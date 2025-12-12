use crate::error::IoError;
use crate::loader::WorkbookLoader;
use crate::traits::{LoadStrategy, SpreadsheetReader, SpreadsheetWriter};
use chrono::Timelike;
use formualizer_common::{
    LiteralValue, RangeAddress,
    error::{ExcelError, ExcelErrorKind},
};
use formualizer_eval::engine::named_range::NamedDefinition;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::ptr;

thread_local! {
    static ACTIVE_WORKBOOK: Cell<*const Workbook> = const { Cell::new(ptr::null()) };
}

/// Minimal resolver for engine-backed workbook (cells/ranges via graph/arrow; functions via registry).
#[derive(Default, Debug, Clone, Copy)]
pub struct WBResolver;

impl formualizer_eval::traits::ReferenceResolver for WBResolver {
    fn resolve_cell_reference(
        &self,
        _sheet: Option<&str>,
        _row: u32,
        _col: u32,
    ) -> Result<LiteralValue, formualizer_common::error::ExcelError> {
        Err(formualizer_common::error::ExcelError::from(
            formualizer_common::error::ExcelErrorKind::NImpl,
        ))
    }
}
impl formualizer_eval::traits::RangeResolver for WBResolver {
    fn resolve_range_reference(
        &self,
        _sheet: Option<&str>,
        _sr: Option<u32>,
        _sc: Option<u32>,
        _er: Option<u32>,
        _ec: Option<u32>,
    ) -> Result<Box<dyn formualizer_eval::traits::Range>, formualizer_common::error::ExcelError>
    {
        Err(formualizer_common::error::ExcelError::from(
            formualizer_common::error::ExcelErrorKind::NImpl,
        ))
    }
}
impl formualizer_eval::traits::NamedRangeResolver for WBResolver {
    fn resolve_named_range_reference(
        &self,
        _name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, formualizer_common::error::ExcelError> {
        ACTIVE_WORKBOOK.with(|cell| {
            let ptr = cell.get();
            if ptr.is_null() {
                return Err(ExcelError::new(ExcelErrorKind::Name));
            }
            let workbook = unsafe { &*ptr };
            if let Some(addr) = workbook.named_range_address(_name) {
                Ok(workbook.read_range(&addr))
            } else {
                Err(ExcelError::new(ExcelErrorKind::Name))
            }
        })
    }
}
impl formualizer_eval::traits::TableResolver for WBResolver {
    fn resolve_table_reference(
        &self,
        _tref: &formualizer_parse::parser::TableReference,
    ) -> Result<Box<dyn formualizer_eval::traits::Table>, formualizer_common::error::ExcelError>
    {
        Err(formualizer_common::error::ExcelError::from(
            formualizer_common::error::ExcelErrorKind::NImpl,
        ))
    }
}
impl formualizer_eval::traits::FunctionProvider for WBResolver {
    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn formualizer_eval::function::Function>> {
        formualizer_eval::function_registry::get(ns, name)
    }
}
impl formualizer_eval::traits::Resolver for WBResolver {}
impl formualizer_eval::traits::EvaluationContext for WBResolver {}

/// Engine-backed workbook facade.
pub struct Workbook {
    engine: formualizer_eval::engine::Engine<WBResolver>,
    enable_changelog: bool,
    log: formualizer_eval::engine::ChangeLog,
    undo: formualizer_eval::engine::graph::editor::undo_engine::UndoEngine,
}

impl Default for Workbook {
    fn default() -> Self {
        Self::new()
    }
}

impl Workbook {
    pub fn new_with_config(config: formualizer_eval::engine::EvalConfig) -> Self {
        let engine = formualizer_eval::engine::Engine::new(WBResolver, config);
        Self {
            engine,
            enable_changelog: false,
            log: formualizer_eval::engine::ChangeLog::new(),
            undo: formualizer_eval::engine::graph::editor::undo_engine::UndoEngine::new(),
        }
    }
    pub fn new() -> Self {
        let mut cfg = formualizer_eval::engine::EvalConfig::default();
        cfg.defer_graph_building = true;
        Self::new_with_config(cfg)
    }

    pub fn engine(&self) -> &formualizer_eval::engine::Engine<WBResolver> {
        &self.engine
    }
    pub fn engine_mut(&mut self) -> &mut formualizer_eval::engine::Engine<WBResolver> {
        &mut self.engine
    }

    // Changelog controls
    pub fn set_changelog_enabled(&mut self, enabled: bool) {
        self.enable_changelog = enabled;
        self.log.set_enabled(enabled);
    }
    pub fn begin_action(&mut self, description: impl Into<String>) {
        if self.enable_changelog {
            self.log.begin_compound(description.into());
        }
    }
    pub fn end_action(&mut self) {
        if self.enable_changelog {
            self.log.end_compound();
        }
    }
    pub fn undo(&mut self) -> Result<(), IoError> {
        if self.enable_changelog {
            self.undo
                .undo(&mut self.engine.graph, &mut self.log)
                .map_err(|e| IoError::from_backend("editor", e))?;
            self.resync_all_overlays();
        }
        Ok(())
    }
    pub fn redo(&mut self) -> Result<(), IoError> {
        if self.enable_changelog {
            self.undo
                .redo(&mut self.engine.graph, &mut self.log)
                .map_err(|e| IoError::from_backend("editor", e))?;
            self.resync_all_overlays();
        }
        Ok(())
    }

    fn resync_all_overlays(&mut self) {
        // Heavy but simple: walk all sheets and rebuild overlay values from graph
        let sheet_names: Vec<String> = self
            .engine
            .sheet_store()
            .sheets
            .iter()
            .map(|s| s.name.as_ref().to_string())
            .collect();
        for s in sheet_names {
            self.resync_overlay_for_sheet(&s);
        }
    }
    fn resync_overlay_for_sheet(&mut self, sheet: &str) {
        if let Some(asheet) = self.engine.sheet_store().sheet(sheet) {
            let rows = asheet.nrows as usize;
            let cols = asheet.columns.len();
            for r0 in 0..rows {
                let r = (r0 as u32) + 1;
                for c0 in 0..cols {
                    let c = (c0 as u32) + 1;
                    let v = self
                        .engine
                        .graph
                        .get_cell_value(sheet, r, c)
                        .unwrap_or(LiteralValue::Empty);
                    self.mirror_value_to_overlay(sheet, r, c, &v);
                }
            }
        }
        // No Arrow sheet: nothing to sync
    }
    fn mirror_value_to_overlay(&mut self, sheet: &str, row: u32, col: u32, value: &LiteralValue) {
        use formualizer_eval::arrow_store::OverlayValue;
        if !(self.engine.config.arrow_storage_enabled && self.engine.config.delta_overlay_enabled) {
            return;
        }
        let date_system = self.engine.config.date_system;
        let asheet = match self.engine.sheet_store_mut().sheet_mut(sheet) {
            Some(s) => s,
            None => return,
        };
        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;
        if col0 >= asheet.columns.len() {
            return;
        }
        if row0 >= asheet.nrows as usize {
            asheet.ensure_row_capacity(row0 + 1);
        }
        if let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) {
            let ov = match value {
                LiteralValue::Empty => OverlayValue::Empty,
                LiteralValue::Int(i) => OverlayValue::Number(*i as f64),
                LiteralValue::Number(n) => OverlayValue::Number(*n),
                LiteralValue::Boolean(b) => OverlayValue::Boolean(*b),
                LiteralValue::Text(s) => OverlayValue::Text(std::sync::Arc::from(s.clone())),
                LiteralValue::Error(e) => {
                    OverlayValue::Error(formualizer_eval::arrow_store::map_error_code(e.kind))
                }
                LiteralValue::Date(d) => {
                    let dt = d.and_hms_opt(0, 0, 0).unwrap();
                    let serial = formualizer_eval::builtins::datetime::datetime_to_serial_for(
                        date_system,
                        &dt,
                    );
                    OverlayValue::Number(serial)
                }
                LiteralValue::DateTime(dt) => {
                    let serial = formualizer_eval::builtins::datetime::datetime_to_serial_for(
                        date_system,
                        dt,
                    );
                    OverlayValue::Number(serial)
                }
                LiteralValue::Time(t) => {
                    let serial = t.num_seconds_from_midnight() as f64 / 86_400.0;
                    OverlayValue::Number(serial)
                }
                LiteralValue::Duration(d) => {
                    let serial = d.num_seconds() as f64 / 86_400.0;
                    OverlayValue::Number(serial)
                }
                LiteralValue::Pending => OverlayValue::Pending,
                LiteralValue::Array(_) => {
                    OverlayValue::Error(formualizer_eval::arrow_store::map_error_code(
                        formualizer_common::ExcelErrorKind::Value,
                    ))
                }
            };
            let colref = &mut asheet.columns[col0];
            let ch = &mut colref.chunks[ch_idx];
            ch.overlay.set(in_off, ov);
            // skip compaction here (optional)
        }
    }

    // Sheets
    pub fn sheet_names(&self) -> Vec<String> {
        self.engine
            .sheet_store()
            .sheets
            .iter()
            .map(|s| s.name.as_ref().to_string())
            .collect()
    }
    /// Return (rows, cols) for a sheet if present in the Arrow store
    pub fn sheet_dimensions(&self, name: &str) -> Option<(u32, u32)> {
        self.engine
            .sheet_store()
            .sheet(name)
            .map(|s| (s.nrows, s.columns.len() as u32))
    }
    pub fn has_sheet(&self, name: &str) -> bool {
        self.engine.graph.sheet_id(name).is_some()
    }
    pub fn add_sheet(&mut self, name: &str) {
        let _ = self.engine.graph.add_sheet(name);
    }
    pub fn delete_sheet(&mut self, name: &str) {
        if let Some(id) = self.engine.graph.sheet_id(name) {
            let _ = self.engine.graph.remove_sheet(id);
        }
        // Remove from Arrow store as well
        self.engine
            .sheet_store_mut()
            .sheets
            .retain(|s| s.name.as_ref() != name);
    }
    pub fn rename_sheet(&mut self, old: &str, new: &str) {
        if let Some(id) = self.engine.graph.sheet_id(old) {
            let _ = self.engine.graph.rename_sheet(id, new);
        }
        if let Some(asheet) = self.engine.sheet_store_mut().sheet_mut(old) {
            asheet.name = std::sync::Arc::<str>::from(new);
        }
    }

    // Cells
    pub fn set_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<(), IoError> {
        if self.enable_changelog {
            // Use VertexEditor with logging for graph, then mirror overlay and mark edited
            let sheet_id = self
                .engine
                .graph
                .sheet_id(sheet)
                .unwrap_or_else(|| self.engine.graph.add_sheet(sheet).unwrap());
            let cell = formualizer_eval::reference::CellRef::new(
                sheet_id,
                formualizer_eval::reference::Coord::from_excel(row, col, true, true),
            );
            {
                let mut editor = formualizer_eval::engine::VertexEditor::with_logger(
                    &mut self.engine.graph,
                    &mut self.log,
                );
                editor.set_cell_value(cell, value.clone());
            }
            self.mirror_value_to_overlay(sheet, row, col, &value);
            self.engine.mark_data_edited();
            Ok(())
        } else {
            self.engine
                .set_cell_value(sheet, row, col, value)
                .map_err(IoError::Engine)
        }
    }
    pub fn set_formula(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        formula: &str,
    ) -> Result<(), IoError> {
        if self.engine.config.defer_graph_building {
            self.engine
                .stage_formula_text(sheet, row, col, formula.to_string());
            Ok(())
        } else {
            let with_eq = if formula.starts_with('=') {
                formula.to_string()
            } else {
                format!("={formula}")
            };
            let ast = formualizer_parse::parser::parse(&with_eq)
                .map_err(|e| IoError::from_backend("parser", e))?;
            if self.enable_changelog {
                let sheet_id = self
                    .engine
                    .graph
                    .sheet_id(sheet)
                    .unwrap_or_else(|| self.engine.graph.add_sheet(sheet).unwrap());
                let cell = formualizer_eval::reference::CellRef::new(
                    sheet_id,
                    formualizer_eval::reference::Coord::from_excel(row, col, true, true),
                );
                {
                    let mut editor = formualizer_eval::engine::VertexEditor::with_logger(
                        &mut self.engine.graph,
                        &mut self.log,
                    );
                    editor.set_cell_formula(cell, ast);
                }
                self.engine.mark_data_edited();
                Ok(())
            } else {
                self.engine
                    .set_cell_formula(sheet, row, col, ast)
                    .map_err(IoError::Engine)
            }
        }
    }
    pub fn get_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        self.engine.get_cell_value(sheet, row, col)
    }
    pub fn get_formula(&self, sheet: &str, row: u32, col: u32) -> Option<String> {
        if let Some(s) = self.engine.get_staged_formula_text(sheet, row, col) {
            return Some(s);
        }
        self.engine
            .get_cell(sheet, row, col)
            .and_then(|(ast, _)| ast.map(|a| formualizer_parse::pretty::canonical_formula(&a)))
    }

    // Ranges
    pub fn read_range(&self, addr: &RangeAddress) -> Vec<Vec<LiteralValue>> {
        let mut out = Vec::with_capacity(addr.height() as usize);
        if let Some(asheet) = self.engine.sheet_store().sheet(&addr.sheet) {
            let sr0 = addr.start_row.saturating_sub(1) as usize;
            let sc0 = addr.start_col.saturating_sub(1) as usize;
            let er0 = addr.end_row.saturating_sub(1) as usize;
            let ec0 = addr.end_col.saturating_sub(1) as usize;
            let view = asheet.range_view(sr0, sc0, er0, ec0);
            let (h, w) = view.dims();
            for rr in 0..h {
                let mut row = Vec::with_capacity(w);
                for cc in 0..w {
                    row.push(view.get_cell(rr, cc));
                }
                out.push(row);
            }
        } else {
            // Fallback: materialize via graph stored values
            for r in addr.start_row..=addr.end_row {
                let mut row = Vec::with_capacity(addr.width() as usize);
                for c in addr.start_col..=addr.end_col {
                    row.push(
                        self.engine
                            .graph
                            .get_cell_value(&addr.sheet, r, c)
                            .unwrap_or(LiteralValue::Empty),
                    );
                }
                out.push(row);
            }
        }
        out
    }
    pub fn write_range(
        &mut self,
        sheet: &str,
        _start: (u32, u32),
        cells: BTreeMap<(u32, u32), crate::traits::CellData>,
    ) -> Result<(), IoError> {
        if self.enable_changelog {
            let sheet_id = self
                .engine
                .graph
                .sheet_id(sheet)
                .unwrap_or_else(|| self.engine.graph.add_sheet(sheet).unwrap());
            let mut overlay_ops: Vec<(u32, u32, LiteralValue)> = Vec::new();
            let mut staged_forms: Vec<(u32, u32, String)> = Vec::new();
            {
                let mut editor = formualizer_eval::engine::VertexEditor::with_logger(
                    &mut self.engine.graph,
                    &mut self.log,
                );
                for ((r, c), d) in cells.into_iter() {
                    let cell = formualizer_eval::reference::CellRef::new(
                        sheet_id,
                        formualizer_eval::reference::Coord::from_excel(r, c, true, true),
                    );
                    if let Some(v) = d.value.clone() {
                        editor.set_cell_value(cell, v.clone());
                        overlay_ops.push((r, c, v));
                    }
                    if let Some(f) = d.formula.as_ref() {
                        if self.engine.config.defer_graph_building {
                            staged_forms.push((r, c, f.clone()));
                        } else {
                            let with_eq = if f.starts_with('=') {
                                f.clone()
                            } else {
                                format!("={f}")
                            };
                            let ast = formualizer_parse::parser::parse(&with_eq)
                                .map_err(|e| IoError::from_backend("parser", e))?;
                            editor.set_cell_formula(cell, ast);
                        }
                    }
                }
            }
            for (r, c, v) in overlay_ops {
                self.mirror_value_to_overlay(sheet, r, c, &v);
            }
            for (r, c, f) in staged_forms {
                self.engine.stage_formula_text(sheet, r, c, f);
            }
            self.engine.mark_data_edited();
            Ok(())
        } else {
            for ((r, c), d) in cells.into_iter() {
                if let Some(v) = d.value.clone() {
                    self.engine
                        .set_cell_value(sheet, r, c, v)
                        .map_err(IoError::Engine)?;
                }
                if let Some(f) = d.formula.as_ref() {
                    if self.engine.config.defer_graph_building {
                        self.engine.stage_formula_text(sheet, r, c, f.clone());
                    } else {
                        let with_eq = if f.starts_with('=') {
                            f.clone()
                        } else {
                            format!("={f}")
                        };
                        let ast = formualizer_parse::parser::parse(&with_eq)
                            .map_err(|e| IoError::from_backend("parser", e))?;
                        self.engine
                            .set_cell_formula(sheet, r, c, ast)
                            .map_err(IoError::Engine)?;
                    }
                }
            }
            Ok(())
        }
    }

    // Batch set values in a rectangle starting at (start_row,start_col)
    pub fn set_values(
        &mut self,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        rows: &[Vec<LiteralValue>],
    ) -> Result<(), IoError> {
        if self.enable_changelog {
            let sheet_id = self
                .engine
                .graph
                .sheet_id(sheet)
                .unwrap_or_else(|| self.engine.graph.add_sheet(sheet).unwrap());
            let mut overlay_ops: Vec<(u32, u32, LiteralValue)> = Vec::new();
            {
                let mut editor = formualizer_eval::engine::VertexEditor::with_logger(
                    &mut self.engine.graph,
                    &mut self.log,
                );
                for (ri, rvals) in rows.iter().enumerate() {
                    let r = start_row + ri as u32;
                    for (ci, v) in rvals.iter().enumerate() {
                        let c = start_col + ci as u32;
                        let cell = formualizer_eval::reference::CellRef::new(
                            sheet_id,
                            formualizer_eval::reference::Coord::new(r, c, true, true),
                        );
                        editor.set_cell_value(cell, v.clone());
                        overlay_ops.push((r, c, v.clone()));
                    }
                }
            }
            for (r, c, v) in overlay_ops {
                self.mirror_value_to_overlay(sheet, r, c, &v);
            }
            self.engine.mark_data_edited();
            Ok(())
        } else {
            for (ri, rvals) in rows.iter().enumerate() {
                let r = start_row + ri as u32;
                for (ci, v) in rvals.iter().enumerate() {
                    let c = start_col + ci as u32;
                    self.engine
                        .set_cell_value(sheet, r, c, v.clone())
                        .map_err(IoError::Engine)?;
                }
            }
            Ok(())
        }
    }

    // Batch set formulas in a rectangle starting at (start_row,start_col)
    pub fn set_formulas(
        &mut self,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        rows: &[Vec<String>],
    ) -> Result<(), IoError> {
        if self.engine.config.defer_graph_building {
            for (ri, rforms) in rows.iter().enumerate() {
                let r = start_row + ri as u32;
                for (ci, f) in rforms.iter().enumerate() {
                    let c = start_col + ci as u32;
                    self.engine.stage_formula_text(sheet, r, c, f.clone());
                }
            }
            Ok(())
        } else if self.enable_changelog {
            let sheet_id = self
                .engine
                .graph
                .sheet_id(sheet)
                .unwrap_or_else(|| self.engine.graph.add_sheet(sheet).unwrap());
            {
                let mut editor = formualizer_eval::engine::VertexEditor::with_logger(
                    &mut self.engine.graph,
                    &mut self.log,
                );
                for (ri, rforms) in rows.iter().enumerate() {
                    let r = start_row + ri as u32;
                    for (ci, f) in rforms.iter().enumerate() {
                        let c = start_col + ci as u32;
                        let cell = formualizer_eval::reference::CellRef::new(
                            sheet_id,
                            formualizer_eval::reference::Coord::from_excel(r, c, true, true),
                        );
                        let with_eq = if f.starts_with('=') {
                            f.clone()
                        } else {
                            format!("={f}")
                        };
                        let ast = formualizer_parse::parser::parse(&with_eq)
                            .map_err(|e| IoError::from_backend("parser", e))?;
                        editor.set_cell_formula(cell, ast);
                    }
                }
            }
            self.engine.mark_data_edited();
            Ok(())
        } else {
            for (ri, rforms) in rows.iter().enumerate() {
                let r = start_row + ri as u32;
                for (ci, f) in rforms.iter().enumerate() {
                    let c = start_col + ci as u32;
                    let with_eq = if f.starts_with('=') {
                        f.clone()
                    } else {
                        format!("={f}")
                    };
                    let ast = formualizer_parse::parser::parse(&with_eq)
                        .map_err(|e| IoError::from_backend("parser", e))?;
                    self.engine
                        .set_cell_formula(sheet, r, c, ast)
                        .map_err(IoError::Engine)?;
                }
            }
            Ok(())
        }
    }

    // Evaluation
    pub fn prepare_graph_all(&mut self) -> Result<(), IoError> {
        self.engine
            .build_graph_all()
            .map_err(|e| IoError::from_backend("parser", e))
    }
    pub fn prepare_graph_for_sheets<'a, I: IntoIterator<Item = &'a str>>(
        &mut self,
        sheets: I,
    ) -> Result<(), IoError> {
        self.engine
            .build_graph_for_sheets(sheets)
            .map_err(|e| IoError::from_backend("parser", e))
    }
    pub fn evaluate_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, IoError> {
        ACTIVE_WORKBOOK.with(|cell| {
            let previous = cell.replace(self as *const _);
            let result = self
                .engine
                .evaluate_cell(sheet, row, col)
                .map_err(IoError::Engine)
                .map(|value| value.unwrap_or(LiteralValue::Empty));
            cell.set(previous);
            result
        })
    }
    pub fn evaluate_cells(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<Vec<LiteralValue>, IoError> {
        ACTIVE_WORKBOOK.with(|cell| {
            let previous = cell.replace(self as *const _);
            let result = self
                .engine
                .evaluate_cells(targets)
                .map_err(IoError::Engine)
                .map(|values| {
                    values
                        .into_iter()
                        .map(|v| v.unwrap_or(LiteralValue::Empty))
                        .collect()
                });
            cell.set(previous);
            result
        })
    }
    pub fn evaluate_all(&mut self) -> Result<formualizer_eval::engine::EvalResult, IoError> {
        ACTIVE_WORKBOOK.with(|cell| {
            let previous = cell.replace(self as *const _);
            let result = self.engine.evaluate_all().map_err(IoError::Engine);
            cell.set(previous);
            result
        })
    }

    pub fn evaluate_with_plan(
        &mut self,
        plan: &formualizer_eval::engine::RecalcPlan,
    ) -> Result<formualizer_eval::engine::EvalResult, IoError> {
        ACTIVE_WORKBOOK.with(|cell| {
            let previous = cell.replace(self as *const _);
            let result = self
                .engine
                .evaluate_recalc_plan(plan)
                .map_err(IoError::Engine);
            cell.set(previous);
            result
        })
    }

    /// Resolve a named range (workbook-scoped or unique sheet-scoped) to an absolute address.
    pub fn named_range_address(&self, name: &str) -> Option<RangeAddress> {
        if let Some((_, named)) = self
            .engine
            .graph
            .named_ranges_iter()
            .find(|(n, _)| n.as_str() == name)
        {
            return self.named_definition_to_address(&named.definition);
        }

        let mut resolved: Option<RangeAddress> = None;
        for ((_sheet_id, candidate), named) in self.engine.graph.sheet_named_ranges_iter() {
            if candidate == name {
                if let Some(address) = self.named_definition_to_address(&named.definition) {
                    if resolved.is_some() {
                        return None; // ambiguous sheet-scoped name
                    }
                    resolved = Some(address);
                }
            }
        }
        resolved
    }

    fn named_definition_to_address(&self, definition: &NamedDefinition) -> Option<RangeAddress> {
        match definition {
            NamedDefinition::Cell(cell) => {
                let sheet = self.engine.graph.sheet_name(cell.sheet_id).to_string();
                let row = cell.coord.row() + 1;
                let col = cell.coord.col() + 1;
                RangeAddress::new(sheet, row, col, row, col).ok()
            }
            NamedDefinition::Range(range) => {
                if range.start.sheet_id != range.end.sheet_id {
                    return None;
                }
                let sheet = self
                    .engine
                    .graph
                    .sheet_name(range.start.sheet_id)
                    .to_string();
                let start_row = range.start.coord.row() + 1;
                let start_col = range.start.coord.col() + 1;
                let end_row = range.end.coord.row() + 1;
                let end_col = range.end.coord.col() + 1;
                RangeAddress::new(sheet, start_row, start_col, end_row, end_col).ok()
            }
            NamedDefinition::Formula { .. } => {
                #[cfg(feature = "tracing")]
                tracing::debug!("formula-backed named ranges are not yet supported");
                None
            }
        }
    }

    // Persistence/transactions via SpreadsheetWriter (self implements writer)
    pub fn begin_tx<'a, W: SpreadsheetWriter>(
        &'a mut self,
        writer: &'a mut W,
    ) -> crate::transaction::WriteTransaction<'a, W> {
        crate::transaction::WriteTransaction::new(writer)
    }

    // Loading via SpreadsheetReader
    pub fn from_reader<B: SpreadsheetReader>(
        backend: B,
        strategy: LoadStrategy,
        mut config: formualizer_eval::engine::EvalConfig,
    ) -> Result<Self, IoError> {
        config.defer_graph_building = true;
        let mut wb = Self::new_with_config(config);
        let mut loader = WorkbookLoader::new(backend, strategy);
        loader.load_into_engine(&mut wb.engine)?;
        Ok(wb)
    }
}

// Implement SpreadsheetWriter so external transactions can target Workbook
impl SpreadsheetWriter for Workbook {
    type Error = IoError;

    fn write_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        data: crate::traits::CellData,
    ) -> Result<(), Self::Error> {
        if let Some(v) = data.value {
            self.set_value(sheet, row, col, v)?;
        }
        if let Some(f) = data.formula {
            self.set_formula(sheet, row, col, &f)?;
        }
        Ok(())
    }
    fn write_range(
        &mut self,
        sheet: &str,
        cells: BTreeMap<(u32, u32), crate::traits::CellData>,
    ) -> Result<(), Self::Error> {
        for ((r, c), d) in cells {
            self.write_cell(sheet, r, c, d)?;
        }
        Ok(())
    }
    fn clear_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<(), Self::Error> {
        for r in start.0..=end.0 {
            for c in start.1..=end.1 {
                self.set_value(sheet, r, c, LiteralValue::Empty)?;
            }
        }
        Ok(())
    }
    fn create_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        self.add_sheet(name);
        Ok(())
    }
    fn delete_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        self.delete_sheet(name);
        Ok(())
    }
    fn rename_sheet(&mut self, old: &str, new: &str) -> Result<(), Self::Error> {
        self.rename_sheet(old, new);
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
