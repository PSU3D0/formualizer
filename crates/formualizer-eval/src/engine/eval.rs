use crate::SheetId;
use crate::arrow_store::SheetStore;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::range_view::RangeView;
use crate::engine::spill::{RegionLockManager, SpillMeta, SpillShape};
use crate::engine::{DependencyGraph, EvalConfig, Scheduler, VertexId, VertexKind};
use crate::interpreter::Interpreter;
use crate::reference::{CellRef, Coord, RangeRef};
use crate::traits::FunctionProvider;
use crate::traits::{EvaluationContext, Resolver};
use chrono::Timelike;
use formualizer_common::{col_letters_from_1based, parse_a1_1based};
use formualizer_parse::parser::ReferenceType;
use formualizer_parse::{ASTNode, ExcelError, ExcelErrorKind, LiteralValue};
use rayon::ThreadPoolBuilder;
use rustc_hash::FxHashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Engine<R> {
    pub(crate) graph: DependencyGraph,
    resolver: R,
    pub config: EvalConfig,
    thread_pool: Option<Arc<rayon::ThreadPool>>,
    pub recalc_epoch: u64,
    snapshot_id: std::sync::atomic::AtomicU64,
    spill_mgr: ShimSpillManager,
    /// Arrow-backed storage for sheet values (Phase A)
    arrow_sheets: SheetStore,
    /// True if any edit after bulk load; disables Arrow reads for parity
    has_edited: bool,
    /// Overlay compaction counter (Phase C instrumentation)
    overlay_compactions: u64,
    // Pass-scoped cache for Arrow used-row bounds per column
    row_bounds_cache: std::sync::RwLock<Option<RowBoundsCache>>,
    /// Staged formulas by sheet when `defer_graph_building` is enabled.
    staged_formulas: std::collections::HashMap<String, Vec<(u32, u32, String)>>,
}

#[derive(Debug)]
pub struct EvalResult {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed: std::time::Duration,
}

/// Cached evaluation schedule that can be replayed across multiple recalculations.
#[derive(Debug)]
pub struct RecalcPlan {
    schedule: crate::engine::Schedule,
}

impl RecalcPlan {
    pub fn layer_count(&self) -> usize {
        self.schedule.layers.len()
    }
}

fn compute_criteria_mask(
    view: &crate::arrow_store::ArrowRangeView<'_>,
    col_in_view: usize,
    pred: &crate::args::CriteriaPredicate,
) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
    use crate::compute_prelude::{boolean, cmp, concat_arrays};
    use arrow::compute::kernels::comparison::{ilike, nilike};
    use arrow_array::{
        Array as _, ArrayRef, BooleanArray, Float64Array, StringArray, builder::BooleanBuilder,
    };

    // Helper: apply a numeric predicate to a single Float64Array chunk
    fn apply_numeric_pred(
        chunk: &Float64Array,
        pred: &crate::args::CriteriaPredicate,
    ) -> Option<BooleanArray> {
        match pred {
            crate::args::CriteriaPredicate::Gt(n) => {
                cmp::gt(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Ge(n) => {
                cmp::gt_eq(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Lt(n) => {
                cmp::lt(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Le(n) => {
                cmp::lt_eq(chunk, &Float64Array::new_scalar(*n)).ok()
            }
            crate::args::CriteriaPredicate::Eq(v) => match v {
                formualizer_common::LiteralValue::Number(x) => {
                    cmp::eq(chunk, &Float64Array::new_scalar(*x)).ok()
                }
                formualizer_common::LiteralValue::Int(i) => {
                    cmp::eq(chunk, &Float64Array::new_scalar(*i as f64)).ok()
                }
                _ => None,
            },
            crate::args::CriteriaPredicate::Ne(v) => match v {
                formualizer_common::LiteralValue::Number(x) => {
                    cmp::neq(chunk, &Float64Array::new_scalar(*x)).ok()
                }
                formualizer_common::LiteralValue::Int(i) => {
                    cmp::neq(chunk, &Float64Array::new_scalar(*i as f64)).ok()
                }
                _ => None,
            },
            _ => None,
        }
    }

    // Check if this is a numeric predicate that can be applied per-chunk
    let is_numeric_pred = matches!(
        pred,
        crate::args::CriteriaPredicate::Gt(_)
            | crate::args::CriteriaPredicate::Ge(_)
            | crate::args::CriteriaPredicate::Lt(_)
            | crate::args::CriteriaPredicate::Le(_)
            | crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Number(_))
            | crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Int(_))
            | crate::args::CriteriaPredicate::Ne(formualizer_common::LiteralValue::Number(_))
            | crate::args::CriteriaPredicate::Ne(formualizer_common::LiteralValue::Int(_))
    );

    // OPTIMIZED PATH: For numeric predicates, apply per-chunk and concatenate boolean masks.
    // This avoids materializing the full numeric column (64-bit per element) and instead
    // concatenates boolean masks (1-bit per element) - a 64x memory reduction.
    if is_numeric_pred {
        let mut bool_parts: Vec<BooleanArray> = Vec::new();
        for (_rs, _rl, cols_seg) in view.numbers_slices() {
            if col_in_view < cols_seg.len() {
                let chunk = cols_seg[col_in_view].as_ref();
                let mask = apply_numeric_pred(chunk, pred)?;
                bool_parts.push(mask);
            }
        }

        if bool_parts.is_empty() {
            return None;
        } else if bool_parts.len() == 1 {
            return Some(std::sync::Arc::new(bool_parts.remove(0)));
        } else {
            // Concatenate boolean masks (much cheaper than concatenating Float64 arrays)
            let anys: Vec<&dyn arrow_array::Array> = bool_parts
                .iter()
                .map(|a| a as &dyn arrow_array::Array)
                .collect();
            let conc: ArrayRef = concat_arrays(&anys).ok()?;
            let ba = conc.as_any().downcast_ref::<BooleanArray>()?.clone();
            return Some(std::sync::Arc::new(ba));
        }
    }

    // FALLBACK PATH: For text predicates, use the pre-lowered text columns
    // (which are already concatenated by the view)
    let lowered_texts: Option<std::sync::Arc<StringArray>> = {
        let cols = view.lowered_text_columns();
        if col_in_view >= cols.len() {
            None
        } else {
            let sa = cols[col_in_view]
                .as_any()
                .downcast_ref::<StringArray>()?
                .clone();
            Some(std::sync::Arc::new(sa))
        }
    };

    let out = match pred {
        crate::args::CriteriaPredicate::Eq(formualizer_common::LiteralValue::Text(t)) => {
            let col = lowered_texts?;
            let pat = StringArray::new_scalar(t.to_ascii_lowercase());
            let mut m = ilike(col.as_ref(), &pat).ok()?;
            if t.is_empty() {
                // Treat nulls as equal to empty string
                let mut bb = BooleanBuilder::with_capacity(col.len());
                for i in 0..col.len() {
                    bb.append_value(col.is_null(i));
                }
                let nulls = bb.finish();
                m = boolean::or_kleene(&m, &nulls).ok()?;
            }
            m
        }
        crate::args::CriteriaPredicate::Ne(formualizer_common::LiteralValue::Text(t)) => {
            let col = lowered_texts?;
            let pat = StringArray::new_scalar(t.to_ascii_lowercase());
            nilike(col.as_ref(), &pat).ok()?
        }
        crate::args::CriteriaPredicate::TextLike {
            pattern,
            case_insensitive,
        } => {
            let col = lowered_texts?;
            let p = if *case_insensitive {
                pattern.to_ascii_lowercase()
            } else {
                pattern.clone()
            };
            let lp = p.replace('*', "%").replace('?', "_");
            let pat = StringArray::new_scalar(lp);
            ilike(col.as_ref(), &pat).ok()?
        }
        // Numeric predicates are handled in the optimized path above
        _ => return None,
    };
    Some(std::sync::Arc::new(out))
}

#[derive(Debug, Clone)]
pub struct LayerInfo {
    pub vertex_count: usize,
    pub parallel_eligible: bool,
    pub sample_cells: Vec<String>, // Sample of up to 5 cell addresses
}

#[derive(Debug, Clone)]
pub struct EvalPlan {
    pub total_vertices_to_evaluate: usize,
    pub layers: Vec<LayerInfo>,
    pub cycles_detected: usize,
    pub dirty_count: usize,
    pub volatile_count: usize,
    pub parallel_enabled: bool,
    pub estimated_parallel_layers: usize,
    pub target_cells: Vec<String>,
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    pub fn new(resolver: R, config: EvalConfig) -> Self {
        crate::builtins::load_builtins();

        // Initialize thread pool based on config
        let thread_pool = if config.enable_parallel {
            let mut builder = ThreadPoolBuilder::new();
            if let Some(max_threads) = config.max_threads {
                builder = builder.num_threads(max_threads);
            }

            match builder.build() {
                Ok(pool) => Some(Arc::new(pool)),
                Err(_) => {
                    // Fall back to sequential evaluation if thread pool creation fails
                    None
                }
            }
        } else {
            None
        };

        let mut engine = Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            thread_pool,
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            spill_mgr: ShimSpillManager::default(),
            arrow_sheets: SheetStore::default(),
            has_edited: false,
            overlay_compactions: 0,
            row_bounds_cache: std::sync::RwLock::new(None),
            staged_formulas: std::collections::HashMap::new(),
        };
        let default_sheet = engine.graph.default_sheet_name().to_string();
        engine.ensure_arrow_sheet(&default_sheet);
        engine
    }

    /// Create an Engine with a custom thread pool (for shared thread pool scenarios)
    pub fn with_thread_pool(
        resolver: R,
        config: EvalConfig,
        thread_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        crate::builtins::load_builtins();
        let mut engine = Self {
            graph: DependencyGraph::new_with_config(config.clone()),
            resolver,
            config,
            thread_pool: Some(thread_pool),
            recalc_epoch: 0,
            snapshot_id: std::sync::atomic::AtomicU64::new(1),
            spill_mgr: ShimSpillManager::default(),
            arrow_sheets: SheetStore::default(),
            has_edited: false,
            overlay_compactions: 0,
            row_bounds_cache: std::sync::RwLock::new(None),
            staged_formulas: std::collections::HashMap::new(),
        };
        let default_sheet = engine.graph.default_sheet_name().to_string();
        engine.ensure_arrow_sheet(&default_sheet);
        engine
    }

    pub fn default_sheet_id(&self) -> SheetId {
        self.graph.default_sheet_id()
    }

    pub fn default_sheet_name(&self) -> &str {
        self.graph.default_sheet_name()
    }

    /// Update the workbook seed for deterministic RNGs in functions.
    pub fn set_workbook_seed(&mut self, seed: u64) {
        self.config.workbook_seed = seed;
    }

    /// Set the volatile level policy (Always/OnRecalc/OnOpen)
    pub fn set_volatile_level(&mut self, level: crate::traits::VolatileLevel) {
        self.config.volatile_level = level;
    }

    pub fn sheet_id(&self, name: &str) -> Option<SheetId> {
        self.graph.sheet_id(name)
    }

    pub fn sheet_id_mut(&mut self, name: &str) -> SheetId {
        self.add_sheet(name)
            .unwrap_or_else(|_| self.graph.sheet_id_mut(name))
    }

    pub fn sheet_name(&self, id: SheetId) -> &str {
        self.graph.sheet_name(id)
    }

    pub fn add_sheet(&mut self, name: &str) -> Result<SheetId, ExcelError> {
        let id = self.graph.add_sheet(name)?;
        self.ensure_arrow_sheet(name);
        Ok(id)
    }

    fn ensure_arrow_sheet(&mut self, name: &str) {
        if self.arrow_sheets.sheet(name).is_some() {
            return;
        }
        self.arrow_sheets
            .sheets
            .push(crate::arrow_store::ArrowSheet {
                name: std::sync::Arc::<str>::from(name),
                columns: Vec::new(),
                nrows: 0,
                chunk_starts: Vec::new(),
            });
    }

    pub fn remove_sheet(&mut self, sheet_id: SheetId) -> Result<(), ExcelError> {
        let name = self.graph.sheet_name(sheet_id).to_string();
        self.graph.remove_sheet(sheet_id)?;
        self.arrow_sheets.sheets.retain(|s| s.name.as_ref() != name);
        Ok(())
    }

    pub fn rename_sheet(&mut self, sheet_id: SheetId, new_name: &str) -> Result<(), ExcelError> {
        let old_name = self.graph.sheet_name(sheet_id).to_string();
        self.graph.rename_sheet(sheet_id, new_name)?;
        self.ensure_arrow_sheet(&old_name);
        if let Some(asheet) = self.arrow_sheets.sheet_mut(&old_name) {
            asheet.name = std::sync::Arc::<str>::from(new_name);
        }
        Ok(())
    }

    pub fn named_ranges_iter(
        &self,
    ) -> impl Iterator<Item = (&String, &crate::engine::named_range::NamedRange)> {
        self.graph.named_ranges_iter()
    }

    pub fn sheet_named_ranges_iter(
        &self,
    ) -> impl Iterator<Item = (&(SheetId, String), &crate::engine::named_range::NamedRange)> {
        self.graph.sheet_named_ranges_iter()
    }

    pub fn resolve_name_entry(
        &self,
        name: &str,
        current_sheet: SheetId,
    ) -> Option<&crate::engine::named_range::NamedRange> {
        self.graph.resolve_name_entry(name, current_sheet)
    }

    pub fn define_name(
        &mut self,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), ExcelError> {
        self.graph.define_name(name, definition, scope)
    }

    pub fn vertex_value(&self, vertex: VertexId) -> Option<LiteralValue> {
        self.graph.get_value(vertex)
    }

    pub fn graph_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        self.graph.get_cell_value(sheet, row, col)
    }

    pub fn vertex_for_cell(&self, cell: &CellRef) -> Option<VertexId> {
        self.graph.get_vertex_for_cell(cell)
    }

    pub fn evaluation_vertices(&self) -> Vec<VertexId> {
        self.graph.get_evaluation_vertices()
    }

    pub fn set_first_load_assume_new(&mut self, enabled: bool) {
        self.graph.set_first_load_assume_new(enabled);
    }

    pub fn reset_ensure_touched(&mut self) {
        self.graph.reset_ensure_touched();
    }

    pub fn finalize_sheet_index(&mut self, sheet: &str) {
        self.graph.finalize_sheet_index(sheet);
    }

    pub fn edit_with_logger<T>(
        &mut self,
        log: &mut crate::engine::ChangeLog,
        f: impl FnOnce(&mut crate::engine::VertexEditor) -> T,
    ) -> T {
        let mut editor = crate::engine::VertexEditor::with_logger(&mut self.graph, log);
        f(&mut editor)
    }

    pub fn undo_logged(
        &mut self,
        undo: &mut crate::engine::graph::editor::undo_engine::UndoEngine,
        log: &mut crate::engine::ChangeLog,
    ) -> Result<(), crate::engine::EditorError> {
        undo.undo(&mut self.graph, log)
    }

    pub fn redo_logged(
        &mut self,
        undo: &mut crate::engine::graph::editor::undo_engine::UndoEngine,
        log: &mut crate::engine::ChangeLog,
    ) -> Result<(), crate::engine::EditorError> {
        undo.redo(&mut self.graph, log)
    }

    pub fn set_default_sheet_by_name(&mut self, name: &str) {
        self.graph.set_default_sheet_by_name(name);
    }

    pub fn set_default_sheet_by_id(&mut self, id: SheetId) {
        self.graph.set_default_sheet_by_id(id);
    }

    pub fn set_sheet_index_mode(&mut self, mode: crate::engine::SheetIndexMode) {
        self.graph.set_sheet_index_mode(mode);
    }

    /// Mark data edited: bump snapshot and set edited flag
    pub fn mark_data_edited(&mut self) {
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
    }

    /// Access Arrow sheet store (read-only)
    pub fn sheet_store(&self) -> &SheetStore {
        &self.arrow_sheets
    }

    /// Access Arrow sheet store (mutable)
    pub fn sheet_store_mut(&mut self) -> &mut SheetStore {
        &mut self.arrow_sheets
    }

    /// Stage a formula text instead of inserting into the graph (used when deferring is enabled).
    pub fn stage_formula_text(&mut self, sheet: &str, row: u32, col: u32, text: String) {
        self.staged_formulas
            .entry(sheet.to_string())
            .or_default()
            .push((row, col, text));
    }

    /// Get a staged formula text for a given cell if present (cloned).
    pub fn get_staged_formula_text(&self, sheet: &str, row: u32, col: u32) -> Option<String> {
        self.staged_formulas.get(sheet).and_then(|v| {
            v.iter()
                .find(|(r, c, _)| *r == row && *c == col)
                .map(|(_, _, s)| s.clone())
        })
    }

    /// Build graph for all staged formulas.
    pub fn build_graph_all(&mut self) -> Result<(), formualizer_parse::ExcelError> {
        if self.staged_formulas.is_empty() {
            return Ok(());
        }
        // Take staged formulas before borrowing graph via builder
        let staged = std::mem::take(&mut self.staged_formulas);
        for sheet in staged.keys() {
            let _ = self.add_sheet(sheet);
        }
        let mut builder = self.begin_bulk_ingest();
        for (sheet, entries) in staged {
            let sid = builder.add_sheet(&sheet);
            // Parse with small cache for shared formulas
            let mut cache: rustc_hash::FxHashMap<String, formualizer_parse::ASTNode> =
                rustc_hash::FxHashMap::default();
            cache.reserve(4096);
            for (row, col, txt) in entries {
                let key = if txt.starts_with('=') {
                    txt
                } else {
                    format!("={txt}")
                };
                let ast = if let Some(p) = cache.get(&key) {
                    p.clone()
                } else {
                    let parsed = formualizer_parse::parser::parse(&key).map_err(|e| {
                        formualizer_parse::ExcelError::new(formualizer_parse::ExcelErrorKind::Value)
                            .with_message(e.to_string())
                    })?;
                    cache.insert(key.clone(), parsed.clone());
                    parsed
                };
                builder.add_formulas(sid, std::iter::once((row, col, ast)));
            }
        }
        let _ = builder.finish();
        Ok(())
    }

    /// Build graph for specific sheets (consuming only those staged entries).
    pub fn build_graph_for_sheets<'a, I: IntoIterator<Item = &'a str>>(
        &mut self,
        sheets: I,
    ) -> Result<(), formualizer_parse::ExcelError> {
        let mut any = false;
        // Collect entries first to avoid aliasing self while builder borrows graph
        struct SheetFormulas<'s> {
            sheet: &'s str,
            entries: Vec<(u32, u32, String)>,
        }
        let mut collected: Vec<SheetFormulas<'_>> = Vec::new();
        for s in sheets {
            if let Some(entries) = self.staged_formulas.remove(s) {
                collected.push(SheetFormulas { sheet: s, entries });
            }
        }
        for SheetFormulas { sheet, .. } in collected.iter() {
            let _ = self.add_sheet(sheet);
        }
        let mut builder = self.begin_bulk_ingest();
        // small parse cache per call
        let mut cache: rustc_hash::FxHashMap<String, formualizer_parse::ASTNode> =
            rustc_hash::FxHashMap::default();
        cache.reserve(4096);
        for SheetFormulas { sheet: s, entries } in collected.into_iter() {
            any = true;
            let sid = builder.add_sheet(s);
            for (row, col, txt) in entries {
                let key = if txt.starts_with('=') {
                    txt
                } else {
                    format!("={txt}")
                };
                let ast = if let Some(p) = cache.get(&key) {
                    p.clone()
                } else {
                    let parsed = formualizer_parse::parser::parse(&key).map_err(|e| {
                        formualizer_parse::ExcelError::new(formualizer_parse::ExcelErrorKind::Value)
                            .with_message(e.to_string())
                    })?;
                    cache.insert(key.clone(), parsed.clone());
                    parsed
                };
                builder.add_formulas(sid, std::iter::once((row, col, ast)));
            }
        }
        if any {
            let _ = builder.finish();
        }
        Ok(())
    }

    /// Begin bulk Arrow ingest for base values (Phase A)
    pub fn begin_bulk_ingest_arrow(
        &mut self,
    ) -> crate::engine::arrow_ingest::ArrowBulkIngestBuilder<'_, R> {
        crate::engine::arrow_ingest::ArrowBulkIngestBuilder::new(self)
    }

    /// Begin bulk updates to Arrow store (Phase C)
    pub fn begin_bulk_update_arrow(
        &mut self,
    ) -> crate::engine::arrow_ingest::ArrowBulkUpdateBuilder<'_, R> {
        crate::engine::arrow_ingest::ArrowBulkUpdateBuilder::new(self)
    }

    /// Insert rows (1-based) and mirror into Arrow store when enabled
    pub fn insert_rows(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.insert_rows(sheet_id, before, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let before0 = before.saturating_sub(1) as usize;
            asheet.insert_rows(before0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }

    /// Delete rows (1-based) and mirror into Arrow store when enabled
    pub fn delete_rows(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.delete_rows(sheet_id, start, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let start0 = start.saturating_sub(1) as usize;
            asheet.delete_rows(start0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }

    /// Insert columns (1-based) and mirror into Arrow store when enabled
    pub fn insert_columns(
        &mut self,
        sheet: &str,
        before: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.insert_columns(sheet_id, before, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let before0 = before.saturating_sub(1) as usize;
            asheet.insert_columns(before0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }

    /// Delete columns (1-based) and mirror into Arrow store when enabled
    pub fn delete_columns(
        &mut self,
        sheet: &str,
        start: u32,
        count: u32,
    ) -> Result<crate::engine::graph::editor::vertex_editor::ShiftSummary, crate::engine::EditorError>
    {
        use crate::engine::graph::editor::vertex_editor::VertexEditor;
        let sheet_id = self.graph.sheet_id(sheet).ok_or(
            crate::engine::graph::editor::vertex_editor::EditorError::InvalidName {
                name: sheet.to_string(),
                reason: "Unknown sheet".to_string(),
            },
        )?;
        let mut editor = VertexEditor::new(&mut self.graph);
        let summary = editor.delete_columns(sheet_id, start, count)?;
        if let Some(asheet) = self.arrow_sheets.sheet_mut(sheet) {
            let start0 = start.saturating_sub(1) as usize;
            asheet.delete_columns(start0, count as usize);
        }
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(summary)
    }
    /// Arrow-backed used row bounds across a column span (1-based inclusive cols).
    fn arrow_used_row_bounds(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        let a = self.sheet_store().sheet(sheet)?;
        if a.columns.is_empty() {
            return None;
        }
        let sc0 = start_col.saturating_sub(1) as usize;
        let ec0 = end_col.saturating_sub(1) as usize;
        let col_hi = a.columns.len().saturating_sub(1);
        if sc0 > col_hi {
            return None;
        }
        let ec0 = ec0.min(col_hi);
        // Pass-scoped cache with snapshot guard
        let snap = self.data_snapshot_id();
        let mut min_r0: Option<usize> = None;
        for ci in sc0..=ec0 {
            let sheet_id = self.graph.sheet_id(sheet)?;
            if let Some((Some(mv), _)) = self.row_bounds_cache.read().ok().and_then(|g| {
                g.as_ref()
                    .and_then(|c| c.get_row_bounds(sheet_id, ci, snap))
            }) {
                let mv = mv as usize;
                min_r0 = Some(min_r0.map(|m| m.min(mv)).unwrap_or(mv));
                continue;
            }
            // Compute and store
            let (min_c, max_c) = Self::scan_column_used_bounds(a, ci);
            if let Ok(mut g) = self.row_bounds_cache.write() {
                g.get_or_insert_with(|| RowBoundsCache::new(snap))
                    .put_row_bounds(sheet_id, ci, snap, (min_c, max_c));
            }
            if let Some(m) = min_c {
                min_r0 = Some(min_r0.map(|mm| mm.min(m as usize)).unwrap_or(m as usize));
            }
        }
        min_r0?;
        let mut max_r0: Option<usize> = None;
        for ci in sc0..=ec0 {
            let sheet_id = self.graph.sheet_id(sheet)?;
            if let Some((_, Some(mv))) = self.row_bounds_cache.read().ok().and_then(|g| {
                g.as_ref()
                    .and_then(|c| c.get_row_bounds(sheet_id, ci, snap))
            }) {
                let mv = mv as usize;
                max_r0 = Some(max_r0.map(|m| m.max(mv)).unwrap_or(mv));
                continue;
            }
            let (_min_c, max_c) = Self::scan_column_used_bounds(a, ci);
            if let Ok(mut g) = self.row_bounds_cache.write() {
                g.get_or_insert_with(|| RowBoundsCache::new(snap))
                    .put_row_bounds(sheet_id, ci, snap, (_min_c, max_c));
            }
            if let Some(m) = max_c {
                max_r0 = Some(max_r0.map(|mm| mm.max(m as usize)).unwrap_or(m as usize));
            }
        }
        match (min_r0, max_r0) {
            (Some(a0), Some(b0)) => Some(((a0 as u32) + 1, (b0 as u32) + 1)),
            _ => None,
        }
    }

    fn scan_column_used_bounds(
        a: &crate::arrow_store::ArrowSheet,
        ci: usize,
    ) -> (Option<u32>, Option<u32>) {
        let col = &a.columns[ci];
        // Min
        let mut min_r0: Option<u32> = None;
        for (chunk_idx, chunk) in col.chunks.iter().enumerate() {
            let tags = chunk.type_tag.values();
            for (off, &t) in tags.iter().enumerate() {
                let overlay_non_empty = chunk
                    .overlay
                    .get(off)
                    .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                    .unwrap_or(false);
                if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                    let row0 = a.chunk_starts[chunk_idx] + off;
                    min_r0 = Some(row0 as u32);
                    break;
                }
            }
            if min_r0.is_some() {
                break;
            }
        }
        // Max
        let mut max_r0: Option<u32> = None;
        for (chunk_rel, chunk) in col.chunks.iter().enumerate().rev() {
            let chunk_idx = chunk_rel;
            let tags = chunk.type_tag.values();
            for (rev_idx, &t) in tags.iter().enumerate().rev() {
                let overlay_non_empty = chunk
                    .overlay
                    .get(rev_idx)
                    .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                    .unwrap_or(false);
                if overlay_non_empty || t != crate::arrow_store::TypeTag::Empty as u8 {
                    let row0 = a.chunk_starts[chunk_idx] + rev_idx;
                    max_r0 = Some(row0 as u32);
                    break;
                }
            }
            if max_r0.is_some() {
                break;
            }
        }
        (min_r0, max_r0)
    }

    /// Arrow-backed used column bounds across a row span (1-based inclusive rows).
    fn arrow_used_col_bounds(
        &self,
        sheet: &str,
        start_row: u32,
        end_row: u32,
    ) -> Option<(u32, u32)> {
        let a = self.sheet_store().sheet(sheet)?;
        if a.columns.is_empty() {
            return None;
        }
        let sr0 = start_row.saturating_sub(1) as usize;
        let er0 = end_row.saturating_sub(1) as usize;
        if sr0 > er0 {
            return None;
        }
        // Map start/end rows into chunk ranges
        // We will scan each column for any non-empty within [sr0..=er0]
        let mut min_c0: Option<usize> = None;
        let mut max_c0: Option<usize> = None;
        // Precompute chunk bounds for row range
        for (ci, col) in a.columns.iter().enumerate() {
            // Skip columns with no chunks (shouldn't happen)
            if col.chunks.is_empty() {
                continue;
            }
            let mut any_in_range = false;
            for (chunk_idx, chunk) in col.chunks.iter().enumerate() {
                let chunk_start = a.chunk_starts[chunk_idx];
                let chunk_len = chunk.type_tag.len();
                let chunk_end = chunk_start + chunk_len.saturating_sub(1);
                // check intersection
                if sr0 > chunk_end || er0 < chunk_start {
                    continue;
                }
                let start_off = sr0.max(chunk_start) - chunk_start;
                let end_off = er0.min(chunk_end) - chunk_start;
                let tags = chunk.type_tag.values();
                for off in start_off..=end_off {
                    let overlay_non_empty = chunk
                        .overlay
                        .get(off)
                        .map(|ov| !matches!(ov, crate::arrow_store::OverlayValue::Empty))
                        .unwrap_or(false);
                    if overlay_non_empty || tags[off] != crate::arrow_store::TypeTag::Empty as u8 {
                        any_in_range = true;
                        break;
                    }
                }
                if any_in_range {
                    break;
                }
            }
            if any_in_range {
                min_c0 = Some(min_c0.map(|m| m.min(ci)).unwrap_or(ci));
                max_c0 = Some(max_c0.map(|m| m.max(ci)).unwrap_or(ci));
            }
        }
        match (min_c0, max_c0) {
            (Some(a0), Some(b0)) => Some(((a0 as u32) + 1, (b0 as u32) + 1)),
            _ => None,
        }
    }

    /// Mirror a single cell value into the Arrow overlay if enabled.
    /// Handles capacity growth, per-chunk overlay set, and heuristic compaction.
    fn mirror_value_to_overlay(&mut self, sheet: &str, row: u32, col: u32, value: &LiteralValue) {
        if !(self.config.arrow_storage_enabled && self.config.delta_overlay_enabled) {
            return;
        }
        if self.arrow_sheets.sheet(sheet).is_none() {
            self.arrow_sheets
                .sheets
                .push(crate::arrow_store::ArrowSheet {
                    name: std::sync::Arc::<str>::from(sheet),
                    columns: Vec::new(),
                    nrows: 0,
                    chunk_starts: Vec::new(),
                });
        }

        let row0 = row.saturating_sub(1) as usize;
        let col0 = col.saturating_sub(1) as usize;

        let asheet = self
            .arrow_sheets
            .sheet_mut(sheet)
            .expect("ArrowSheet must exist");

        let cur_cols = asheet.columns.len();
        if col0 >= cur_cols {
            asheet.insert_columns(cur_cols, (col0 + 1) - cur_cols);
        }

        if row0 >= asheet.nrows as usize {
            if asheet.columns.is_empty() {
                asheet.insert_columns(0, 1);
            }
            asheet.ensure_row_capacity(row0 + 1);
        }
        if let Some((ch_idx, in_off)) = asheet.chunk_of_row(row0) {
            use crate::arrow_store::OverlayValue;
            let ov = match value {
                LiteralValue::Empty => OverlayValue::Empty,
                LiteralValue::Int(i) => OverlayValue::Number(*i as f64),
                LiteralValue::Number(n) => OverlayValue::Number(*n),
                LiteralValue::Boolean(b) => OverlayValue::Boolean(*b),
                LiteralValue::Text(s) => OverlayValue::Text(std::sync::Arc::from(s.clone())),
                LiteralValue::Error(e) => {
                    OverlayValue::Error(crate::arrow_store::map_error_code(e.kind))
                }
                LiteralValue::Date(d) => {
                    let dt = d.and_hms_opt(0, 0, 0).unwrap();
                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                        self.config.date_system,
                        &dt,
                    );
                    OverlayValue::Number(serial)
                }
                LiteralValue::DateTime(dt) => {
                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                        self.config.date_system,
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
                LiteralValue::Array(_) => OverlayValue::Error(crate::arrow_store::map_error_code(
                    formualizer_common::ExcelErrorKind::Value,
                )),
            };
            let colref = &mut asheet.columns[col0];
            let ch = &mut colref.chunks[ch_idx];
            ch.overlay.set(in_off, ov);
            // Heuristic compaction: > len/50 or > 1024
            let abs_threshold = 1024usize;
            let frac_den = 50usize;
            if asheet.maybe_compact_chunk(col0, ch_idx, abs_threshold, frac_den) {
                self.overlay_compactions = self.overlay_compactions.saturating_add(1);
            }
        }
    }

    fn mirror_vertex_value_to_overlay(&mut self, vertex_id: VertexId, value: &LiteralValue) {
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
        {
            return;
        }
        if !matches!(
            self.graph.get_vertex_kind(vertex_id),
            VertexKind::FormulaScalar | VertexKind::FormulaArray
        ) {
            return;
        }
        let Some(cell) = self.graph.get_cell_ref(vertex_id) else {
            return;
        };
        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
        self.mirror_value_to_overlay(
            &sheet_name,
            cell.coord.row() + 1,
            cell.coord.col() + 1,
            value,
        );
    }

    fn resolve_sheet_locator_for_write(
        &mut self,
        loc: formualizer_common::SheetLocator<'_>,
        current_sheet: &str,
    ) -> Result<SheetId, ExcelError> {
        Ok(match loc {
            formualizer_common::SheetLocator::Id(id) => id,
            formualizer_common::SheetLocator::Name(name) => self.graph.sheet_id_mut(name.as_ref()),
            formualizer_common::SheetLocator::Current => self.graph.sheet_id_mut(current_sheet),
        })
    }

    fn resolve_sheet_locator_for_read(
        &self,
        loc: formualizer_common::SheetLocator<'_>,
        current_sheet: &str,
    ) -> Result<SheetId, ExcelError> {
        match loc {
            formualizer_common::SheetLocator::Id(id) => Ok(id),
            formualizer_common::SheetLocator::Name(name) => self
                .graph
                .sheet_id(name.as_ref())
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref)),
            formualizer_common::SheetLocator::Current => self
                .graph
                .sheet_id(current_sheet)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref)),
        }
    }

    /// Set a cell value
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        self.graph.set_cell_value(sheet, row, col, value.clone())?;
        // Mirror into Arrow overlay when enabled
        self.mirror_value_to_overlay(sheet, row, col, &value);
        // Advance snapshot to reflect external mutation
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(())
    }

    pub fn set_cell_value_ref(
        &mut self,
        cell: formualizer_common::SheetCellRef<'_>,
        current_sheet: &str,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = self.resolve_sheet_locator_for_write(owned.sheet, current_sheet)?;
        let sheet_name = self.graph.sheet_name(sheet_id).to_string();
        self.set_cell_value(
            &sheet_name,
            owned.coord.row() + 1,
            owned.coord.col() + 1,
            value,
        )
    }

    pub fn set_cell_formula_ref(
        &mut self,
        cell: formualizer_common::SheetCellRef<'_>,
        current_sheet: &str,
        ast: ASTNode,
    ) -> Result<(), ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = self.resolve_sheet_locator_for_write(owned.sheet, current_sheet)?;
        let sheet_name = self.graph.sheet_name(sheet_id).to_string();
        self.set_cell_formula(
            &sheet_name,
            owned.coord.row() + 1,
            owned.coord.col() + 1,
            ast,
        )
    }

    pub fn get_cell_value_ref(
        &self,
        cell: formualizer_common::SheetCellRef<'_>,
        current_sheet: &str,
    ) -> Result<Option<LiteralValue>, ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = self.resolve_sheet_locator_for_read(owned.sheet, current_sheet)?;
        let sheet_name = self.graph.sheet_name(sheet_id);
        Ok(self.get_cell_value(sheet_name, owned.coord.row() + 1, owned.coord.col() + 1))
    }

    pub fn resolve_range_view_sheet_ref<'c>(
        &'c self,
        r: &formualizer_common::SheetRef<'_>,
        current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        use formualizer_common::SheetLocator;

        let sheet_to_opt_name = |loc: SheetLocator<'_>| -> Result<Option<String>, ExcelError> {
            match loc {
                SheetLocator::Current => Ok(None),
                SheetLocator::Name(name) => Ok(Some(name.as_ref().to_string())),
                SheetLocator::Id(id) => Ok(Some(self.graph.sheet_name(id).to_string())),
            }
        };

        let rt = match r {
            formualizer_common::SheetRef::Cell(cell) => ReferenceType::Cell {
                sheet: sheet_to_opt_name(cell.sheet.clone())?,
                row: cell.coord.row() + 1,
                col: cell.coord.col() + 1,
            },
            formualizer_common::SheetRef::Range(range) => ReferenceType::Range {
                sheet: sheet_to_opt_name(range.sheet.clone())?,
                start_row: range.start_row.map(|b| b.index + 1),
                start_col: range.start_col.map(|b| b.index + 1),
                end_row: range.end_row.map(|b| b.index + 1),
                end_col: range.end_col.map(|b| b.index + 1),
            },
        };

        crate::traits::EvaluationContext::resolve_range_view(self, &rt, current_sheet)
    }

    /// Set a cell formula
    pub fn set_cell_formula(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast: ASTNode,
    ) -> Result<(), ExcelError> {
        let volatile = self.is_ast_volatile_with_provider(&ast);
        self.graph
            .set_cell_formula_with_volatility(sheet, row, col, ast, volatile)?;
        // Advance snapshot to reflect external mutation
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.has_edited = true;
        Ok(())
    }

    /// Bulk set many formulas on a sheet. Skips per-cell snapshot bumping and minimizes edge rebuilds.
    pub fn bulk_set_formulas<I>(&mut self, sheet: &str, items: I) -> Result<usize, ExcelError>
    where
        I: IntoIterator<Item = (u32, u32, ASTNode)>,
    {
        let collected: Vec<(u32, u32, ASTNode)> = items.into_iter().collect();
        let vol_flags: Vec<bool> = collected
            .iter()
            .map(|(_, _, ast)| self.is_ast_volatile_with_provider(ast))
            .collect();
        let n = self
            .graph
            .bulk_set_formulas_with_volatility(sheet, collected, vol_flags)?;
        // Single snapshot bump after batch
        self.snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if n > 0 {
            self.has_edited = true;
        }
        Ok(n)
    }

    /// Get a cell value
    pub fn get_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        // Prefer Arrow for non-formula cells. For formula cells, use graph value.
        if let Some(sheet_id) = self.graph.sheet_id(sheet) {
            // If a formula exists at this address, return graph value
            let coord = Coord::from_excel(row, col, true, true);
            let addr = CellRef::new(sheet_id, coord);
            if let Some(vid) = self.graph.get_vertex_id_for_address(&addr) {
                match self.graph.get_vertex_kind(*vid) {
                    VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                        return self.graph.get_cell_value(sheet, row, col);
                    }
                    _ => {}
                }
            }
        }
        if let Some(asheet) = self.sheet_store().sheet(sheet) {
            let r0 = row.saturating_sub(1) as usize;
            let c0 = col.saturating_sub(1) as usize;
            let av = asheet.range_view(r0, c0, r0, c0);
            let v = av.get_cell(0, 0);
            return Some(v);
        }
        self.graph.get_cell_value(sheet, row, col)
    }

    /// Get formula AST (if any) and current stored value for a cell
    pub fn get_cell(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Option<(Option<formualizer_parse::ASTNode>, Option<LiteralValue>)> {
        let v = self.get_cell_value(sheet, row, col);
        let sheet_id = self.graph.sheet_id(sheet)?;
        let coord = Coord::from_excel(row, col, true, true);
        let cell = CellRef::new(sheet_id, coord);
        let vid = self.graph.get_vertex_for_cell(&cell)?;
        let ast = self.graph.get_formula_id(vid).and_then(|ast_id| {
            self.graph
                .data_store()
                .retrieve_ast(ast_id, self.graph.sheet_reg())
        });
        Some((ast, v))
    }

    /// Begin batch operations - defer CSR rebuilds for better performance
    pub fn begin_batch(&mut self) {
        self.graph.begin_batch();
    }

    /// End batch operations and trigger CSR rebuild
    pub fn end_batch(&mut self) {
        self.graph.end_batch();
    }

    /// Evaluate a single vertex.
    /// This is the core of the sequential evaluation logic for Milestone 3.1.
    pub fn evaluate_vertex(&mut self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }

        // Get vertex kind and check if it needs evaluation
        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        let ast_id = match kind {
            VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                if let Some(ast_id) = self.graph.get_formula_id(vertex_id) {
                    ast_id
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                // Check if there's a value stored
                if let Some(value) = self.graph.get_value(vertex_id) {
                    return Ok(value.clone());
                } else {
                    return Ok(LiteralValue::Int(0)); // Empty cells evaluate to 0
                }
            }
            VertexKind::NamedScalar => {
                let value = self.evaluate_named_scalar(vertex_id, sheet_id)?;
                return Ok(value);
            }
            VertexKind::NamedArray => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message("Array named ranges not yet implemented"));
            }
            VertexKind::InfiniteRange | VertexKind::Range | VertexKind::External => {
                // Not directly evaluatable here; return stored or 0
                if let Some(value) = self.graph.get_value(vertex_id) {
                    return Ok(value.clone());
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
        };

        // The interpreter uses a reference to the engine as the context.
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);

        let result =
            interpreter.evaluate_arena_ast(ast_id, self.graph.data_store(), self.graph.sheet_reg());

        // If array result, perform spill from the anchor cell
        match result {
            Ok(LiteralValue::Array(rows)) => {
                // Update kind to FormulaArray for tracking
                self.graph
                    .set_kind(vertex_id, crate::engine::vertex::VertexKind::FormulaArray);
                // Build target cells rectangle starting from anchor
                let anchor = self
                    .graph
                    .get_cell_ref(vertex_id)
                    .expect("cell ref for vertex");
                let sheet_id = anchor.sheet_id;
                let h = rows.len() as u32;
                let w = rows.first().map(|r| r.len()).unwrap_or(0) as u32;
                // Bounds check to avoid out-of-range writes (align to AbsCoord capacity)
                const PACKED_MAX_ROW: u32 = 1_048_575; // 20-bit max
                const PACKED_MAX_COL: u32 = 16_383; // 14-bit max
                let end_row = anchor.coord.row().saturating_add(h).saturating_sub(1);
                let end_col = anchor.coord.col().saturating_add(w).saturating_sub(1);
                if end_row > PACKED_MAX_ROW || end_col > PACKED_MAX_COL {
                    let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                        .with_message("Spill exceeds sheet bounds")
                        .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                            expected_rows: h,
                            expected_cols: w,
                        });
                    let spill_val = LiteralValue::Error(spill_err.clone());
                    self.graph.update_vertex_value(vertex_id, spill_val.clone());
                    return Ok(spill_val);
                }
                let mut targets = Vec::new();
                for r in 0..h {
                    for c in 0..w {
                        targets.push(self.graph.make_cell_ref_internal(
                            sheet_id,
                            anchor.coord.row() + r,
                            anchor.coord.col() + c,
                        ));
                    }
                }

                // Plan spill via spill manager shim
                match self.spill_mgr.reserve(
                    vertex_id,
                    anchor,
                    SpillShape { rows: h, cols: w },
                    SpillMeta {
                        epoch: self.recalc_epoch,
                        config: self.config.spill,
                    },
                ) {
                    Ok(()) => {
                        // Commit: write values to grid
                        // Default conflict policy is Error + FirstWins; reserve() enforces in-flight locks
                        // and plan_spill_region enforces overlap with committed formulas/spills/values.
                        if let Err(e) =
                            self.commit_spill_and_mirror(vertex_id, &targets, rows.clone())
                        {
                            // If commit fails, mark as error
                            self.graph
                                .update_vertex_value(vertex_id, LiteralValue::Error(e.clone()));
                            return Ok(LiteralValue::Error(e));
                        }
                        // Anchor shows the top-left value, like Excel
                        let top_left = rows
                            .first()
                            .and_then(|r| r.first())
                            .cloned()
                            .unwrap_or(LiteralValue::Empty);
                        self.graph.update_vertex_value(vertex_id, top_left.clone());
                        Ok(top_left)
                    }
                    Err(e) => {
                        let spill_err = ExcelError::new(ExcelErrorKind::Spill)
                            .with_message(e.message.unwrap_or_else(|| "Spill blocked".to_string()))
                            .with_extra(formualizer_common::ExcelErrorExtra::Spill {
                                expected_rows: h,
                                expected_cols: w,
                            });
                        let spill_val = LiteralValue::Error(spill_err.clone());
                        self.graph.update_vertex_value(vertex_id, spill_val.clone());
                        Ok(spill_val)
                    }
                }
            }
            Ok(other) => {
                // Scalar result: store value and ensure any previous spill is cleared
                let spill_cells = self
                    .graph
                    .spill_cells_for_anchor(vertex_id)
                    .map(|cells| cells.to_vec())
                    .unwrap_or_default();
                self.graph.clear_spill_region(vertex_id);
                if self.config.arrow_storage_enabled && self.config.delta_overlay_enabled {
                    let empty = LiteralValue::Empty;
                    for cell in spill_cells.iter() {
                        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                        self.mirror_value_to_overlay(
                            &sheet_name,
                            cell.coord.row() + 1,
                            cell.coord.col() + 1,
                            &empty,
                        );
                    }
                }
                self.graph.update_vertex_value(vertex_id, other.clone());
                // Optionally mirror into Arrow overlay for Arrow-backed reads
                if self.config.arrow_storage_enabled
                    && self.config.delta_overlay_enabled
                    && self.config.write_formula_overlay_enabled
                {
                    let anchor = self
                        .graph
                        .get_cell_ref(vertex_id)
                        .expect("cell ref for vertex");
                    let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
                    self.mirror_value_to_overlay(
                        &sheet_name,
                        anchor.coord.row() + 1,
                        anchor.coord.col() + 1,
                        &other,
                    );
                }
                Ok(other)
            }
            Err(e) => {
                // Runtime Excel error: store as a cell value instead of propagating
                // as an exception so bulk eval paths don't fail the whole pass.
                let spill_cells = self
                    .graph
                    .spill_cells_for_anchor(vertex_id)
                    .map(|cells| cells.to_vec())
                    .unwrap_or_default();
                self.graph.clear_spill_region(vertex_id);
                if self.config.arrow_storage_enabled && self.config.delta_overlay_enabled {
                    let empty = LiteralValue::Empty;
                    for cell in spill_cells.iter() {
                        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                        self.mirror_value_to_overlay(
                            &sheet_name,
                            cell.coord.row() + 1,
                            cell.coord.col() + 1,
                            &empty,
                        );
                    }
                }
                let err_val = LiteralValue::Error(e.clone());
                self.graph.update_vertex_value(vertex_id, err_val.clone());
                if self.config.arrow_storage_enabled
                    && self.config.delta_overlay_enabled
                    && self.config.write_formula_overlay_enabled
                {
                    let anchor = self
                        .graph
                        .get_cell_ref(vertex_id)
                        .expect("cell ref for vertex");
                    let sheet_name = self.graph.sheet_name(anchor.sheet_id).to_string();
                    self.mirror_value_to_overlay(
                        &sheet_name,
                        anchor.coord.row() + 1,
                        anchor.coord.col() + 1,
                        &err_val,
                    );
                }
                Ok(err_val)
            }
        }
    }

    fn evaluate_named_scalar(
        &mut self,
        vertex_id: VertexId,
        sheet_id: SheetId,
    ) -> Result<LiteralValue, ExcelError> {
        let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name)
                .with_message("Named range metadata missing".to_string())
        })?;

        match &named_range.definition {
            NamedDefinition::Cell(cell_ref) => {
                let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                let row = cell_ref.coord.row() + 1;
                let col = cell_ref.coord.col() + 1;

                if let Some(dep_vertex) = self.graph.get_vertex_for_cell(cell_ref)
                    && matches!(
                        self.graph.get_vertex_kind(dep_vertex),
                        VertexKind::FormulaScalar | VertexKind::FormulaArray
                    )
                {
                    let value = if let Some(existing) = self.graph.get_value(dep_vertex) {
                        existing.clone()
                    } else {
                        let computed = self.evaluate_vertex(dep_vertex)?;
                        self.graph.update_vertex_value(dep_vertex, computed.clone());
                        computed
                    };
                    self.graph.update_vertex_value(vertex_id, value.clone());
                    Ok(value)
                } else {
                    let value = self
                        .get_cell_value(sheet_name, row, col)
                        .unwrap_or(LiteralValue::Empty);
                    self.graph.update_vertex_value(vertex_id, value.clone());
                    Ok(value)
                }
            }
            NamedDefinition::Formula { ast, .. } => {
                let context_sheet = match named_range.scope {
                    NameScope::Sheet(id) => id,
                    NameScope::Workbook => sheet_id,
                };
                let sheet_name = self.graph.sheet_name(context_sheet);
                let cell_ref = self
                    .graph
                    .get_cell_ref(vertex_id)
                    .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                match interpreter.evaluate_ast(ast) {
                    Ok(LiteralValue::Array(_)) => {
                        let err = ExcelError::new(ExcelErrorKind::NImpl)
                            .with_message("Array result in scalar named range".to_string());
                        let err_val = LiteralValue::Error(err.clone());
                        self.graph.update_vertex_value(vertex_id, err_val.clone());
                        Ok(err_val)
                    }
                    Ok(value) => {
                        self.graph.update_vertex_value(vertex_id, value.clone());
                        Ok(value)
                    }
                    Err(err) => {
                        let err_val = LiteralValue::Error(err.clone());
                        self.graph.update_vertex_value(vertex_id, err_val.clone());
                        Ok(err_val)
                    }
                }
            }
            NamedDefinition::Range(_) => Err(ExcelError::new(ExcelErrorKind::NImpl)
                .with_message("Array named ranges not yet implemented".to_string())),
        }
    }

    /// Evaluate only the necessary precedents for specific target cells (demand-driven)
    pub fn evaluate_until(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<EvalResult, ExcelError> {
        #[cfg(feature = "tracing")]
        let _span_eval = tracing::info_span!("evaluate_until", targets = targets.len()).entered();
        let start = web_time::Instant::now();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for (sheet, row, col) in targets {
            // For now, assume simple A1-style references on default sheet
            // TODO: Parse complex references with sheets
            let sheet_id = self.graph.sheet_id_mut(sheet);
            let coord = Coord::from_excel(*row, *col, true, true);
            target_addrs.push(CellRef::new(sheet_id, coord));
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Build demand subgraph with virtual edges for compressed ranges
        #[cfg(feature = "tracing")]
        let _span_sub = tracing::info_span!("demand_subgraph_build").entered();
        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);
        #[cfg(feature = "tracing")]
        drop(_span_sub);

        if precedents_to_eval.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Create schedule for the minimal subgraph, honoring virtual edges
        let scheduler = Scheduler::new(&self.graph);
        #[cfg(feature = "tracing")]
        let _span_sched =
            tracing::info_span!("schedule_build", vertices = precedents_to_eval.len()).entered();
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;
        #[cfg(feature = "tracing")]
        drop(_span_sched);

        // Handle cycles first
        let mut cycle_errors = 0;
        for cycle in &schedule.cycles {
            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
            }
        }

        // Evaluate layers (parallel when enabled, mirroring evaluate_all)
        let mut computed_vertices = 0;
        for layer in &schedule.layers {
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel(layer)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential(layer)?;
            }
        }

        // Clear warmup context at end of evaluation

        // Clear dirty flags for evaluated vertices
        self.graph.clear_dirty_flags(&precedents_to_eval);

        // Re-dirty volatile vertices
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Build a reusable evaluation plan that covers every formula vertex in the workbook.
    pub fn build_recalc_plan(&self) -> Result<RecalcPlan, ExcelError> {
        let mut vertices: Vec<VertexId> = self.graph.vertices_with_formulas().collect();
        vertices.sort_unstable();
        if vertices.is_empty() {
            return Ok(RecalcPlan {
                schedule: crate::engine::Schedule {
                    layers: Vec::new(),
                    cycles: Vec::new(),
                },
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&vertices)?;
        Ok(RecalcPlan { schedule })
    }

    /// Evaluate using a previously constructed plan. This avoids rebuilding layer schedules for each run.
    pub fn evaluate_recalc_plan(&mut self, plan: &RecalcPlan) -> Result<EvalResult, ExcelError> {
        if self.config.defer_graph_building {
            self.build_graph_all()?;
        }

        let start = web_time::Instant::now();
        let dirty_vertices = self.graph.get_evaluation_vertices();
        if dirty_vertices.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        let dirty_set: FxHashSet<VertexId> = dirty_vertices.iter().copied().collect();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        if !plan.schedule.cycles.is_empty() {
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for cycle in &plan.schedule.cycles {
                if !cycle.iter().any(|v| dirty_set.contains(v)) {
                    continue;
                }
                cycle_errors += 1;
                for &vertex_id in cycle {
                    if dirty_set.contains(&vertex_id) {
                        self.graph
                            .update_vertex_value(vertex_id, circ_error.clone());
                    }
                }
            }
        }

        for layer in &plan.schedule.layers {
            let work: Vec<VertexId> = layer
                .vertices
                .iter()
                .copied()
                .filter(|v| dirty_set.contains(v))
                .collect();
            if work.is_empty() {
                continue;
            }
            let temp_layer = crate::engine::scheduler::Layer { vertices: work };
            if self.thread_pool.is_some() && temp_layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel(&temp_layer)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential(&temp_layer)?;
            }
        }

        self.graph.clear_dirty_flags(&dirty_vertices);
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Evaluate all dirty/volatile vertices
    pub fn evaluate_all(&mut self) -> Result<EvalResult, ExcelError> {
        if self.config.defer_graph_building {
            // Build graph for all staged formulas before evaluating
            self.build_graph_all()?;
        }
        #[cfg(feature = "tracing")]
        let _span_eval = tracing::info_span!("evaluate_all").entered();
        let start = web_time::Instant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let to_evaluate = self.graph.get_evaluation_vertices();
        if to_evaluate.is_empty() {
            return Ok(EvalResult {
                computed_vertices,
                cycle_errors,
                elapsed: start.elapsed(),
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&to_evaluate)?;

        // Handle cycles first by marking them with #CIRC!
        for cycle in &schedule.cycles {
            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
            }
        }

        // Evaluate acyclic layers (parallel or sequential based on config)
        for layer in &schedule.layers {
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices += self.evaluate_layer_parallel(layer)?;
            } else {
                computed_vertices += self.evaluate_layer_sequential(layer)?;
            }
        }

        // Clear dirty flags for all evaluated vertices (including cycles)
        self.graph.clear_dirty_flags(&to_evaluate);

        // Re-dirty volatile vertices for the next evaluation cycle
        self.graph.redirty_volatiles();

        // Advance recalc epoch after a full evaluation pass finishes
        self.recalc_epoch = self.recalc_epoch.wrapping_add(1);

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Convenience: demand-driven evaluation of a single cell by sheet name and row/col.
    ///
    /// This will evaluate only the minimal set of dirty / volatile precedents required
    /// to bring the target cell up-to-date (as if a user asked for that single value),
    /// rather than scheduling a full workbook recalc. If the cell is already clean and
    /// non-volatile, no vertices will be recomputed.
    ///
    /// Returns the (possibly newly computed) value stored for the cell afterwards.
    /// Empty cells return None. Errors are surfaced via the Result type.
    pub fn evaluate_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Result<Option<LiteralValue>, ExcelError> {
        if row == 0 || col == 0 {
            return Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Row and column must be >= 1".to_string()));
        }

        if self.config.defer_graph_building {
            self.build_graph_for_sheets(std::iter::once(sheet))?;
        }

        let result = self.evaluate_cells(&[(sheet, row, col)])?;

        match result.len() {
            0 => Ok(None),
            1 => {
                let v = result.into_iter().next().unwrap();
                Ok(v)
            }
            _ => unreachable!("evaluate_cells returned unexpected length"),
        }
    }

    /// Convenience: demand-driven evaluation of multiple cells; accepts a slice of
    /// (sheet, row, col) triples. The union of required dirty / volatile precedents
    /// is computed once and evaluated, which is typically faster than calling
    /// `evaluate_cell` repeatedly for a related set of targets.
    ///
    /// Returns the resulting values for each requested target in the same order.
    pub fn evaluate_cells(
        &mut self,
        targets: &[(&str, u32, u32)],
    ) -> Result<Vec<Option<LiteralValue>>, ExcelError> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        if self.config.defer_graph_building {
            let mut sheets: rustc_hash::FxHashSet<&str> = rustc_hash::FxHashSet::default();
            for (s, _, _) in targets.iter() {
                sheets.insert(*s);
            }
            self.build_graph_for_sheets(sheets.iter().cloned())?;
        }
        self.evaluate_until(targets)?;
        Ok(targets
            .iter()
            .map(|(s, r, c)| self.get_cell_value(s, *r, *c))
            .collect())
    }

    /// Get the evaluation plan for target cells without actually evaluating them
    pub fn get_eval_plan(&self, targets: &[(&str, u32, u32)]) -> Result<EvalPlan, ExcelError> {
        if targets.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: Vec::new(),
            });
        }
        if self.config.defer_graph_building {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(
                "Evaluation plan requested with deferred graph; build first or call evaluate_*",
            ));
        }

        // Convert targets to A1 notation for consistency
        let addresses: Vec<String> = targets
            .iter()
            .map(|(s, r, c)| format!("{}!{}{}", s, Self::col_to_letters(*c), r))
            .collect();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for (sheet, row, col) in targets {
            if let Some(sheet_id) = self.graph.sheet_id(sheet) {
                let coord = Coord::from_excel(*row, *col, true, true);
                target_addrs.push(CellRef::new(sheet_id, coord));
            }
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: addresses,
            });
        }

        // Build demand subgraph with virtual edges (same as evaluate_until)
        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);

        if precedents_to_eval.is_empty() {
            return Ok(EvalPlan {
                total_vertices_to_evaluate: 0,
                layers: Vec::new(),
                cycles_detected: 0,
                dirty_count: 0,
                volatile_count: 0,
                parallel_enabled: self.config.enable_parallel && self.thread_pool.is_some(),
                estimated_parallel_layers: 0,
                target_cells: addresses,
            });
        }

        // Count dirty and volatile vertices
        let mut dirty_count = 0;
        let mut volatile_count = 0;
        for &vertex_id in &precedents_to_eval {
            if self.graph.is_dirty(vertex_id) {
                dirty_count += 1;
            }
            if self.graph.is_volatile(vertex_id) {
                volatile_count += 1;
            }
        }

        // Create schedule for the minimal subgraph honoring virtual edges
        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;

        // Build layer information
        let mut layers = Vec::new();
        let mut estimated_parallel_layers = 0;
        let parallel_enabled = self.config.enable_parallel && self.thread_pool.is_some();

        for layer in &schedule.layers {
            let parallel_eligible = parallel_enabled && layer.vertices.len() > 1;
            if parallel_eligible {
                estimated_parallel_layers += 1;
            }

            // Get sample cell addresses (up to 5)
            let sample_cells: Vec<String> = layer
                .vertices
                .iter()
                .take(5)
                .filter_map(|&vertex_id| {
                    self.graph
                        .get_cell_ref_for_vertex(vertex_id)
                        .map(|cell_ref| {
                            let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                            format!(
                                "{}!{}{}",
                                sheet_name,
                                Self::col_to_letters(cell_ref.coord.col()),
                                cell_ref.coord.row() + 1
                            )
                        })
                })
                .collect();

            layers.push(LayerInfo {
                vertex_count: layer.vertices.len(),
                parallel_eligible,
                sample_cells,
            });
        }

        Ok(EvalPlan {
            total_vertices_to_evaluate: precedents_to_eval.len(),
            layers,
            cycles_detected: schedule.cycles.len(),
            dirty_count,
            volatile_count,
            parallel_enabled,
            estimated_parallel_layers,
            target_cells: addresses,
        })
    }

    /// Build a demand-driven subgraph for the given targets, including ephemeral edges for
    /// compressed ranges, and returning the set of dirty/volatile precedents and virtual deps.
    fn build_demand_subgraph(
        &self,
        target_vertices: &[VertexId],
    ) -> (
        Vec<VertexId>,
        rustc_hash::FxHashMap<VertexId, Vec<VertexId>>,
    ) {
        #[cfg(feature = "tracing")]
        let _span =
            tracing::info_span!("demand_subgraph", targets = target_vertices.len()).entered();
        use rustc_hash::{FxHashMap, FxHashSet};

        let mut to_evaluate: FxHashSet<VertexId> = FxHashSet::default();
        let mut visited: FxHashSet<VertexId> = FxHashSet::default();
        let mut stack: Vec<VertexId> = Vec::new();
        let mut vdeps: FxHashMap<VertexId, Vec<VertexId>> = FxHashMap::default(); // incoming deps per vertex

        for &t in target_vertices {
            stack.push(t);
        }

        while let Some(v) = stack.pop() {
            if !visited.insert(v) {
                continue;
            }
            if !self.graph.vertex_exists(v) {
                continue;
            }
            // Only schedule dirty/volatile formulas
            match self.graph.get_vertex_kind(v) {
                VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                    if self.graph.is_dirty(v) || self.graph.is_volatile(v) {
                        to_evaluate.insert(v);
                    }
                }
                _ => {}
            }

            // Explicit dependencies (graph edges)
            for dep in self.graph.get_dependencies(v) {
                if self.graph.vertex_exists(dep) {
                    match self.graph.get_vertex_kind(dep) {
                        VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                            if !visited.contains(&dep) {
                                stack.push(dep);
                            }
                        }
                        _ => {}
                    }
                }
            }

            // Compressed range dependencies → discover formula precedents in used/bounded window
            if let Some(ranges) = self.graph.get_range_dependencies(v) {
                let current_sheet_id = self.graph.get_vertex_sheet_id(v);
                for r in ranges {
                    let sheet_id = match r.sheet {
                        formualizer_common::SheetLocator::Id(id) => id,
                        _ => current_sheet_id,
                    };
                    let sheet_name = self.graph.sheet_name(sheet_id);

                    // Infer bounds like resolve_range_view (r bounds are 0-based)
                    let mut sr = r.start_row.map(|b| b.index + 1);
                    let mut sc = r.start_col.map(|b| b.index + 1);
                    let mut er = r.end_row.map(|b| b.index + 1);
                    let mut ec = r.end_col.map(|b| b.index + 1);

                    if sr.is_none() && er.is_none() {
                        let scv = sc.unwrap_or(1);
                        let ecv = ec.unwrap_or(scv);
                        if let Some((min_r, max_r)) =
                            self.used_rows_for_columns(sheet_name, scv, ecv)
                        {
                            sr = Some(min_r);
                            er = Some(max_r);
                        } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                            sr = Some(1);
                            er = Some(max_rows);
                        }
                    }
                    if sc.is_none() && ec.is_none() {
                        let srv = sr.unwrap_or(1);
                        let erv = er.unwrap_or(srv);
                        if let Some((min_c, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv)
                        {
                            sc = Some(min_c);
                            ec = Some(max_c);
                        } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                            sc = Some(1);
                            ec = Some(max_cols);
                        }
                    }
                    if sr.is_some() && er.is_none() {
                        let scv = sc.unwrap_or(1);
                        let ecv = ec.unwrap_or(scv);
                        if let Some((_, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                            er = Some(max_r);
                        } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                            er = Some(max_rows);
                        }
                    }
                    if er.is_some() && sr.is_none() {
                        let scv = sc.unwrap_or(1);
                        let ecv = ec.unwrap_or(scv);
                        if let Some((min_r, _)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                            sr = Some(min_r);
                        } else {
                            sr = Some(1);
                        }
                    }
                    if sc.is_some() && ec.is_none() {
                        let srv = sr.unwrap_or(1);
                        let erv = er.unwrap_or(srv);
                        if let Some((_, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                            ec = Some(max_c);
                        } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                            ec = Some(max_cols);
                        }
                    }
                    if ec.is_some() && sc.is_none() {
                        let srv = sr.unwrap_or(1);
                        let erv = er.unwrap_or(srv);
                        if let Some((min_c, _)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                            sc = Some(min_c);
                        } else {
                            sc = Some(1);
                        }
                    }

                    let sr = sr.unwrap_or(1);
                    let sc = sc.unwrap_or(1);
                    let er = er.unwrap_or(sr.saturating_sub(1));
                    let ec = ec.unwrap_or(sc.saturating_sub(1));
                    if er < sr || ec < sc {
                        continue;
                    }

                    if let Some(index) = self.graph.sheet_index(sheet_id) {
                        let sr0 = sr.saturating_sub(1);
                        let er0 = er.saturating_sub(1);
                        let sc0 = sc.saturating_sub(1);
                        let ec0 = ec.saturating_sub(1);
                        // enumerate vertices in col range, filter row and kind
                        for u in index.vertices_in_col_range(sc0, ec0) {
                            let pc = self.graph.vertex_coord(u);
                            let row0 = pc.row();
                            if row0 < sr0 || row0 > er0 {
                                continue;
                            }
                            match self.graph.get_vertex_kind(u) {
                                VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                                    // only link and schedule if producer is dirty/volatile
                                    if (self.graph.is_dirty(u) || self.graph.is_volatile(u))
                                        && u != v
                                    {
                                        vdeps.entry(v).or_default().push(u);
                                        if !visited.contains(&u) {
                                            stack.push(u);
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        let mut result: Vec<VertexId> = to_evaluate.into_iter().collect();
        result.sort_unstable();
        // Dedup virtual deps
        for deps in vdeps.values_mut() {
            deps.sort_unstable();
            deps.dedup();
        }
        (result, vdeps)
    }

    /// Helper: convert 1-based column index to Excel-style letters (1 -> A, 27 -> AA)
    fn col_to_letters(col: u32) -> String {
        col_letters_from_1based(col).expect("column index must be >= 1")
    }

    /// Evaluate all dirty/volatile vertices with cancellation support
    pub fn evaluate_all_cancellable(
        &mut self,
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        let start = web_time::Instant::now();
        let mut computed_vertices = 0;
        let mut cycle_errors = 0;

        let to_evaluate = self.graph.get_evaluation_vertices();
        if to_evaluate.is_empty() {
            return Ok(EvalResult {
                computed_vertices,
                cycle_errors,
                elapsed: start.elapsed(),
            });
        }

        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule(&to_evaluate)?;

        // Handle cycles first by marking them with #CIRC!
        for cycle in &schedule.cycles {
            // Check cancellation between cycles
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled during cycle handling".to_string()));
            }

            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
            }
        }

        // Evaluate acyclic layers sequentially with cancellation checks
        for layer in &schedule.layers {
            // Check cancellation between layers
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled between layers".to_string()));
            }

            // Evaluate vertices in this layer (parallel or sequential)
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices +=
                    self.evaluate_layer_parallel_cancellable(layer, cancel_flag)?;
            } else {
                computed_vertices +=
                    self.evaluate_layer_sequential_cancellable(layer, cancel_flag)?;
            }
        }

        // Clear dirty flags for all evaluated vertices (including cycles)
        self.graph.clear_dirty_flags(&to_evaluate);

        // Re-dirty volatile vertices for the next evaluation cycle
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    /// Evaluate only the necessary precedents for specific target cells with cancellation support
    pub fn evaluate_until_cancellable(
        &mut self,
        targets: &[&str],
        cancel_flag: &AtomicBool,
    ) -> Result<EvalResult, ExcelError> {
        let start = web_time::Instant::now();

        // Parse target cell addresses
        let mut target_addrs = Vec::new();
        for target in targets {
            let (sheet, row, col) = self.parse_a1_notation(target)?;
            let sheet_id = self.graph.sheet_id_mut(&sheet);
            let coord = Coord::from_excel(row, col, true, true);
            target_addrs.push(CellRef::new(sheet_id, coord));
        }

        // Find vertex IDs for targets
        let mut target_vertex_ids = Vec::new();
        for addr in &target_addrs {
            if let Some(vertex_id) = self.graph.get_vertex_id_for_address(addr) {
                target_vertex_ids.push(*vertex_id);
            }
        }

        if target_vertex_ids.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Build demand subgraph with virtual edges
        let (precedents_to_eval, vdeps) = self.build_demand_subgraph(&target_vertex_ids);

        if precedents_to_eval.is_empty() {
            return Ok(EvalResult {
                computed_vertices: 0,
                cycle_errors: 0,
                elapsed: start.elapsed(),
            });
        }

        // Create schedule honoring virtual edges
        let scheduler = Scheduler::new(&self.graph);
        let schedule = scheduler.create_schedule_with_virtual(&precedents_to_eval, &vdeps)?;

        // Handle cycles first
        let mut cycle_errors = 0;
        for cycle in &schedule.cycles {
            // Check cancellation between cycles
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                    "Demand-driven evaluation cancelled during cycle handling".to_string(),
                ));
            }

            cycle_errors += 1;
            let circ_error = LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular dependency detected".to_string()),
            );
            for &vertex_id in cycle {
                self.graph
                    .update_vertex_value(vertex_id, circ_error.clone());
            }
        }

        // Evaluate layers with cancellation checks
        let mut computed_vertices = 0;
        for layer in &schedule.layers {
            // Check cancellation between layers
            if cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                    "Demand-driven evaluation cancelled between layers".to_string(),
                ));
            }

            // Evaluate vertices in this layer (parallel or sequential)
            if self.thread_pool.is_some() && layer.vertices.len() > 1 {
                computed_vertices +=
                    self.evaluate_layer_parallel_cancellable(layer, cancel_flag)?;
            } else {
                computed_vertices +=
                    self.evaluate_layer_sequential_cancellable_demand_driven(layer, cancel_flag)?;
            }
        }

        // Clear dirty flags for evaluated vertices
        self.graph.clear_dirty_flags(&precedents_to_eval);

        // Re-dirty volatile vertices
        self.graph.redirty_volatiles();

        Ok(EvalResult {
            computed_vertices,
            cycle_errors,
            elapsed: start.elapsed(),
        })
    }

    fn parse_a1_notation(&self, address: &str) -> Result<(String, u32, u32), ExcelError> {
        let mut parts = address.splitn(2, '!');
        let first = parts.next().unwrap_or_default();
        let remainder = parts.next();

        let (sheet, cell_part) = match remainder {
            Some(cell) => (first.to_string(), cell),
            None => (self.default_sheet_name().to_string(), first),
        };

        let (row, col, _, _) = parse_a1_1based(cell_part).map_err(|err| {
            ExcelError::new(ExcelErrorKind::Ref)
                .with_message(format!("Invalid cell reference `{cell_part}`: {err}"))
        })?;

        Ok((sheet, row, col))
    }

    /// Determine volatility using this engine's FunctionProvider, falling back to global registry.
    fn is_ast_volatile_with_provider(&self, ast: &ASTNode) -> bool {
        use formualizer_parse::parser::ASTNodeType;
        match &ast.node_type {
            ASTNodeType::Function { name, args, .. } => {
                if let Some(func) = self
                    .get_function("", name)
                    .or_else(|| crate::function_registry::get("", name))
                    && func.caps().contains(crate::function::FnCaps::VOLATILE)
                {
                    return true;
                }
                args.iter()
                    .any(|arg| self.is_ast_volatile_with_provider(arg))
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.is_ast_volatile_with_provider(left)
                    || self.is_ast_volatile_with_provider(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.is_ast_volatile_with_provider(expr),
            ASTNodeType::Array(rows) => rows.iter().any(|row| {
                row.iter()
                    .any(|cell| self.is_ast_volatile_with_provider(cell))
            }),
            _ => false,
        }
    }

    /// Find dirty precedents that need evaluation for the given target vertices
    fn find_dirty_precedents(&self, target_vertices: &[VertexId]) -> Vec<VertexId> {
        use rustc_hash::FxHashSet;

        let mut to_evaluate = FxHashSet::default();
        let mut visited = FxHashSet::default();
        let mut stack = Vec::new();

        // Start reverse traversal from target vertices
        for &target in target_vertices {
            stack.push(target);
        }

        while let Some(vertex_id) = stack.pop() {
            if !visited.insert(vertex_id) {
                continue; // Already processed
            }

            if self.graph.vertex_exists(vertex_id) {
                // Check if this vertex needs evaluation
                let kind = self.graph.get_vertex_kind(vertex_id);
                let needs_eval = match kind {
                    super::vertex::VertexKind::FormulaScalar
                    | super::vertex::VertexKind::FormulaArray => {
                        self.graph.is_dirty(vertex_id) || self.graph.is_volatile(vertex_id)
                    }
                    _ => false, // Values and empty cells don't need evaluation
                };

                if needs_eval {
                    to_evaluate.insert(vertex_id);
                }

                // Continue traversal to dependencies (precedents)
                let dependencies = self.graph.get_dependencies(vertex_id);
                for &dep_id in &dependencies {
                    if !visited.contains(&dep_id) {
                        stack.push(dep_id);
                    }
                }
            }
        }

        let mut result: Vec<VertexId> = to_evaluate.into_iter().collect();
        result.sort_unstable();
        result
    }

    /// Evaluate a layer sequentially
    fn evaluate_layer_sequential(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        for &vertex_id in &layer.vertices {
            self.evaluate_vertex(vertex_id)?;
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially with cancellation support
    fn evaluate_layer_sequential_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        for (i, &vertex_id) in layer.vertices.iter().enumerate() {
            // Check cancellation every 256 vertices to balance responsiveness with performance
            if i % 256 == 0 && cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Evaluation cancelled within layer".to_string()));
            }

            self.evaluate_vertex(vertex_id)?;
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer sequentially with more frequent cancellation checks for demand-driven evaluation
    fn evaluate_layer_sequential_cancellable_demand_driven(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        for (i, &vertex_id) in layer.vertices.iter().enumerate() {
            // Check cancellation more frequently for demand-driven evaluation (every 128 vertices)
            if i % 128 == 0 && cancel_flag.load(Ordering::Relaxed) {
                return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                    .with_message("Demand-driven evaluation cancelled within layer".to_string()));
            }

            self.evaluate_vertex(vertex_id)?;
        }
        Ok(layer.vertices.len())
    }

    /// Evaluate a layer in parallel using the thread pool
    fn evaluate_layer_parallel(
        &mut self,
        layer: &super::scheduler::Layer,
    ) -> Result<usize, ExcelError> {
        use rayon::prelude::*;

        let thread_pool = self.thread_pool.as_ref().unwrap();

        // Collect all evaluation results first, then update the graph sequentially
        let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
            thread_pool.install(|| {
                layer
                    .vertices
                    .par_iter()
                    .map(
                        |&vertex_id| match self.evaluate_vertex_immutable(vertex_id) {
                            Ok(v) => Ok((vertex_id, v)),
                            Err(e) => Ok((vertex_id, LiteralValue::Error(e))),
                        },
                    )
                    .collect()
            });

        // Update the graph with results sequentially (thread-safe)
        match results {
            Ok(vertex_results) => {
                for (vertex_id, result) in vertex_results {
                    self.graph.update_vertex_value(vertex_id, result.clone());
                    self.mirror_vertex_value_to_overlay(vertex_id, &result);
                }
                Ok(layer.vertices.len())
            }
            Err(e) => Err(e),
        }
    }

    /// Evaluate a layer in parallel with cancellation support
    fn evaluate_layer_parallel_cancellable(
        &mut self,
        layer: &super::scheduler::Layer,
        cancel_flag: &AtomicBool,
    ) -> Result<usize, ExcelError> {
        use rayon::prelude::*;

        let thread_pool = self.thread_pool.as_ref().unwrap();

        // Check cancellation before starting parallel work
        if cancel_flag.load(Ordering::Relaxed) {
            return Err(ExcelError::new(ExcelErrorKind::Cancelled)
                .with_message("Parallel evaluation cancelled before starting".to_string()));
        }

        // Collect all evaluation results first, then update the graph sequentially
        let results: Result<Vec<(VertexId, LiteralValue)>, ExcelError> =
            thread_pool.install(|| {
                layer
                    .vertices
                    .par_iter()
                    .map(|&vertex_id| {
                        // Check cancellation periodically during parallel work
                        if cancel_flag.load(Ordering::Relaxed) {
                            return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                                "Parallel evaluation cancelled during execution".to_string(),
                            ));
                        }
                        match self.evaluate_vertex_immutable(vertex_id) {
                            Ok(v) => Ok((vertex_id, v)),
                            Err(e) => Ok((vertex_id, LiteralValue::Error(e))),
                        }
                    })
                    .collect()
            });

        // Update the graph with results sequentially (thread-safe)
        match results {
            Ok(vertex_results) => {
                for (vertex_id, result) in vertex_results {
                    self.graph.update_vertex_value(vertex_id, result.clone());
                    self.mirror_vertex_value_to_overlay(vertex_id, &result);
                }
                Ok(layer.vertices.len())
            }
            Err(e) => Err(e),
        }
    }

    /// Evaluate a single vertex without mutating the graph (for parallel evaluation)
    fn evaluate_vertex_immutable(&self, vertex_id: VertexId) -> Result<LiteralValue, ExcelError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(vertex_id) {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Ref)
                .with_message(format!("Vertex not found: {vertex_id:?}")));
        }

        // Get vertex kind and check if it needs evaluation
        let kind = self.graph.get_vertex_kind(vertex_id);
        let sheet_id = self.graph.get_vertex_sheet_id(vertex_id);

        let ast_id = match kind {
            VertexKind::FormulaScalar => {
                if let Some(ast_id) = self.graph.get_formula_id(vertex_id) {
                    ast_id
                } else {
                    return Ok(LiteralValue::Int(0));
                }
            }
            VertexKind::Empty | VertexKind::Cell => {
                // Check if there's a value stored
                if let Some(value) = self.graph.get_value(vertex_id) {
                    return Ok(value.clone());
                } else {
                    return Ok(LiteralValue::Int(0)); // Empty cells evaluate to 0
                }
            }
            VertexKind::NamedScalar => {
                let named_range = self.graph.named_range_by_vertex(vertex_id).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Name)
                        .with_message("Named range metadata missing".to_string())
                })?;

                return match &named_range.definition {
                    NamedDefinition::Cell(cell_ref) => {
                        if let Some(dep_vertex) = self.graph.get_vertex_for_cell(cell_ref)
                            && let Some(existing) = self.graph.get_value(dep_vertex)
                        {
                            return Ok(existing.clone());
                        }
                        let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                        Ok(self
                            .graph
                            .get_cell_value(sheet_name, cell_ref.coord.row(), cell_ref.coord.col())
                            .unwrap_or(LiteralValue::Empty))
                    }
                    NamedDefinition::Formula { ast, .. } => {
                        let context_sheet = match named_range.scope {
                            NameScope::Sheet(id) => id,
                            NameScope::Workbook => sheet_id,
                        };
                        let sheet_name = self.graph.sheet_name(context_sheet);
                        let cell_ref = self
                            .graph
                            .get_cell_ref(vertex_id)
                            .unwrap_or_else(|| self.graph.make_cell_ref(sheet_name, 0, 0));
                        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
                        interpreter.evaluate_ast(ast)
                    }
                    NamedDefinition::Range(_) => Err(ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("Array named ranges not yet implemented".to_string())),
                };
            }
            _ => {
                return Ok(LiteralValue::Error(
                    ExcelError::new(formualizer_common::ExcelErrorKind::Na)
                        .with_message("Array formulas not yet supported".to_string()),
                ));
            }
        };

        // The interpreter uses a reference to the engine as the context
        let sheet_name = self.graph.sheet_name(sheet_id);
        let cell_ref = self
            .graph
            .get_cell_ref(vertex_id)
            .expect("cell ref for vertex");
        let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);

        interpreter.evaluate_arena_ast(ast_id, self.graph.data_store(), self.graph.sheet_reg())
    }

    /// Get access to the shared thread pool for parallel evaluation
    pub fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
    }
}

#[derive(Default)]
struct RowBoundsCache {
    snapshot: u64,
    // key: (sheet_id, col_idx)
    map: rustc_hash::FxHashMap<(u32, usize), (Option<u32>, Option<u32>)>,
}

impl RowBoundsCache {
    fn new(snapshot: u64) -> Self {
        Self {
            snapshot,
            map: Default::default(),
        }
    }
    fn get_row_bounds(
        &self,
        sheet_id: SheetId,
        col_idx: usize,
        snapshot: u64,
    ) -> Option<(Option<u32>, Option<u32>)> {
        if self.snapshot != snapshot {
            return None;
        }
        self.map.get(&(sheet_id as u32, col_idx)).copied()
    }
    fn put_row_bounds(
        &mut self,
        sheet_id: SheetId,
        col_idx: usize,
        snapshot: u64,
        bounds: (Option<u32>, Option<u32>),
    ) {
        if self.snapshot != snapshot {
            self.snapshot = snapshot;
            self.map.clear();
        }
        self.map.insert((sheet_id as u32, col_idx), bounds);
    }
}

// Phase 2 shim: in-process spill manager delegating to current graph methods.
#[derive(Default)]
pub struct ShimSpillManager {
    region_locks: RegionLockManager,
    pub(crate) active_locks: rustc_hash::FxHashMap<VertexId, u64>,
}

impl ShimSpillManager {
    pub(crate) fn reserve(
        &mut self,
        owner: VertexId,
        anchor_cell: CellRef,
        shape: SpillShape,
        _meta: SpillMeta,
    ) -> Result<(), ExcelError> {
        // Derive region from anchor + shape; enforce in-flight exclusivity only.
        let region = crate::engine::spill::Region {
            sheet_id: anchor_cell.sheet_id as u32,
            row_start: anchor_cell.coord.row(),
            row_end: anchor_cell
                .coord
                .row()
                .saturating_add(shape.rows)
                .saturating_sub(1),
            col_start: anchor_cell.coord.col(),
            col_end: anchor_cell
                .coord
                .col()
                .saturating_add(shape.cols)
                .saturating_sub(1),
        };
        match self.region_locks.reserve(region, owner) {
            Ok(id) => {
                if id != 0 {
                    self.active_locks.insert(owner, id);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) fn commit_array(
        &mut self,
        graph: &mut DependencyGraph,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
    ) -> Result<(), ExcelError> {
        // Re-run plan on concrete targets before committing to respect blockers.
        let plan_res = graph.plan_spill_region(anchor_vertex, targets);
        if let Err(e) = plan_res {
            if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                self.region_locks.release(id);
            }
            return Err(e);
        }

        let commit_res = graph.commit_spill_region_atomic_with_fault(
            anchor_vertex,
            targets.to_vec(),
            rows,
            None,
        );
        if let Some(id) = self.active_locks.remove(&anchor_vertex) {
            self.region_locks.release(id);
        }
        commit_res.map(|_| ())
    }

    /// Commit a spill and mirror all written cells into Arrow overlay via the owning engine.
    pub(crate) fn commit_array_with_overlay<R: EvaluationContext>(
        &mut self,
        engine: &mut Engine<R>,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
    ) -> Result<(), ExcelError> {
        // Re-run plan on concrete targets before committing to respect blockers.
        let plan_res = engine.graph.plan_spill_region(anchor_vertex, targets);
        if let Err(e) = plan_res {
            if let Some(id) = self.active_locks.remove(&anchor_vertex) {
                self.region_locks.release(id);
            }
            return Err(e);
        }

        let commit_res = engine.graph.commit_spill_region_atomic_with_fault(
            anchor_vertex,
            targets.to_vec(),
            rows.clone(),
            None,
        );
        if let Some(id) = self.active_locks.remove(&anchor_vertex) {
            self.region_locks.release(id);
        }
        commit_res.map(|_| ())?;

        // Mirror into Arrow overlay when enabled
        if engine.config.arrow_storage_enabled
            && engine.config.delta_overlay_enabled
            && engine.config.write_formula_overlay_enabled
        {
            // Expect targets to be a contiguous rectangle row-major starting at some anchor
            for (idx, cell) in targets.iter().enumerate() {
                let (r_off, c_off) = {
                    if rows.is_empty() || rows[0].is_empty() {
                        (0usize, 0usize)
                    } else {
                        let width = rows[0].len();
                        (idx / width, idx % width)
                    }
                };
                let v = rows
                    .get(r_off)
                    .and_then(|r| r.get(c_off))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty);
                let sheet_name = engine.graph.sheet_name(cell.sheet_id).to_string();
                engine.mirror_value_to_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &v,
                );
            }
        }
        Ok(())
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_shared_ref(
        &self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<formualizer_common::SheetRef<'static>, ExcelError> {
        use formualizer_common::{
            SheetCellRef as SharedCellRef, SheetLocator, SheetRangeRef as SharedRangeRef,
            SheetRef as SharedRef,
        };

        // ReferenceType does not retain $ anchors. For evaluation and identity
        // we treat all coordinates as absolute to match graph cell addressing.
        // Anchor semantics are handled separately by rewrite/adjustment passes.
        let sr = match reference {
            ReferenceType::Cell { sheet, row, col } => {
                let row0 = row
                    .checked_sub(1)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                let col0 = col
                    .checked_sub(1)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                let sheet_loc = match sheet.as_deref() {
                    Some(name) => SheetLocator::from_name(name),
                    None => SheetLocator::Current,
                };
                let coord = formualizer_common::RelativeCoord::new(row0, col0, true, true);
                SharedRef::Cell(SharedCellRef::new(sheet_loc, coord))
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                let sheet_loc = match sheet.as_deref() {
                    Some(name) => SheetLocator::from_name(name),
                    None => SheetLocator::Current,
                };

                fn bound_from_1based(v: Option<u32>) -> Option<formualizer_common::AxisBound> {
                    v.and_then(|x| {
                        x.checked_sub(1)
                            .map(|i| formualizer_common::AxisBound::new(i, true))
                    })
                }

                let sr = bound_from_1based(*start_row);
                if start_row.is_some() && sr.is_none() {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                }
                let sc = bound_from_1based(*start_col);
                if start_col.is_some() && sc.is_none() {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                }
                let er = bound_from_1based(*end_row);
                if end_row.is_some() && er.is_none() {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                }
                let ec = bound_from_1based(*end_col);
                if end_col.is_some() && ec.is_none() {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                }

                let range = SharedRangeRef::from_parts(sheet_loc, sr, sc, er, ec)
                    .map_err(|_| ExcelError::new(ExcelErrorKind::Ref))?;
                SharedRef::Range(range)
            }
            _ => {
                return Err(ExcelError::new(ExcelErrorKind::Ref));
            }
        };

        let current_id = self
            .graph
            .sheet_id(current_sheet)
            .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;

        let resolve_loc = |loc: SheetLocator<'_>| -> Result<SheetLocator<'static>, ExcelError> {
            match loc {
                SheetLocator::Current => Ok(SheetLocator::Id(current_id)),
                SheetLocator::Id(id) => Ok(SheetLocator::Id(id)),
                SheetLocator::Name(name) => {
                    let n = name.as_ref();
                    self.graph
                        .sheet_id(n)
                        .map(SheetLocator::Id)
                        .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))
                }
            }
        };

        match sr {
            SharedRef::Cell(cell) => {
                let owned = cell.into_owned();
                let sheet = resolve_loc(owned.sheet)?;
                Ok(SharedRef::Cell(SharedCellRef::new(sheet, owned.coord)))
            }
            SharedRef::Range(range) => {
                let owned = range.into_owned();
                let sheet = resolve_loc(owned.sheet)?;
                Ok(SharedRef::Range(SharedRangeRef {
                    sheet,
                    start_row: owned.start_row,
                    start_col: owned.start_col,
                    end_row: owned.end_row,
                    end_col: owned.end_col,
                }))
            }
        }
    }
}

// Implement the resolver traits for the Engine.
// This allows the interpreter to resolve references by querying the engine's graph.
impl<R> crate::traits::ReferenceResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        let sheet_name = sheet.unwrap_or_else(|| self.default_sheet_name()); // FIXME: should use formula current-sheet context
        // Prefer engine's unified accessor which consults Arrow store for base values
        // and falls back to graph for formulas and stored values.
        if let Some(v) = self.get_cell_value(sheet_name, row, col) {
            Ok(v)
        } else {
            // Excel semantics: empty cell coerces to 0 in numeric contexts
            Ok(LiteralValue::Int(0))
        }
    }
}

impl<R> crate::traits::RangeResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn crate::traits::Range>, ExcelError> {
        // For now, delegate range resolution to the external resolver.
        // A future optimization could be to handle this within the graph.
        self.resolver.resolve_range_reference(sheet, sr, sc, er, ec)
    }
}

impl<R> crate::traits::NamedRangeResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_named_range_reference(
        &self,
        name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        self.resolver.resolve_named_range_reference(name)
    }
}

impl<R> crate::traits::TableResolver for Engine<R>
where
    R: EvaluationContext,
{
    fn resolve_table_reference(
        &self,
        tref: &formualizer_parse::parser::TableReference,
    ) -> Result<Box<dyn crate::traits::Table>, ExcelError> {
        self.resolver.resolve_table_reference(tref)
    }
}

// The Engine is a Resolver because it implements the constituent traits.
impl<R> crate::traits::Resolver for Engine<R> where R: EvaluationContext {}

// The Engine provides functions by delegating to its internal resolver.
impl<R> crate::traits::FunctionProvider for Engine<R>
where
    R: EvaluationContext,
{
    fn get_function(
        &self,
        prefix: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        self.resolver.get_function(prefix, name)
    }
}

// Override EvaluationContext to provide thread pool access
impl<R> crate::traits::EvaluationContext for Engine<R>
where
    R: EvaluationContext,
{
    fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        self.thread_pool.as_ref()
    }

    fn cancellation_token(&self) -> Option<&std::sync::atomic::AtomicBool> {
        // Engine-wide cancellation is exposed via evaluate_all_cancellable APIs; default None here.
        None
    }

    fn chunk_hint(&self) -> Option<usize> {
        // Use a simple heuristic from configuration (stripe width * height) as a default hint.
        let hint =
            (self.config.stripe_height as usize).saturating_mul(self.config.stripe_width as usize);
        Some(hint.clamp(1024, 1 << 20)) // clamp between 1K and ~1M
    }

    fn volatile_level(&self) -> crate::traits::VolatileLevel {
        self.config.volatile_level
    }

    fn workbook_seed(&self) -> u64 {
        self.config.workbook_seed
    }

    fn recalc_epoch(&self) -> u64 {
        self.recalc_epoch
    }

    fn used_rows_for_columns(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        // Prefer Arrow-backed used-region; fallback to graph if formulas intersect region
        let sheet_id = self.graph.sheet_id(sheet)?;
        let arrow_ok = self.sheet_store().sheet(sheet).is_some();
        if arrow_ok && let Some(bounds) = self.arrow_used_row_bounds(sheet, start_col, end_col) {
            return Some(bounds);
        }
        let sc0 = start_col.saturating_sub(1);
        let ec0 = end_col.saturating_sub(1);
        self.graph
            .used_row_bounds_for_columns(sheet_id, sc0, ec0)
            .map(|(a0, b0)| (a0 + 1, b0 + 1))
    }

    fn used_cols_for_rows(&self, sheet: &str, start_row: u32, end_row: u32) -> Option<(u32, u32)> {
        // Prefer Arrow-backed used-region; fallback to graph if formulas intersect region
        let sheet_id = self.graph.sheet_id(sheet)?;
        let arrow_ok = self.sheet_store().sheet(sheet).is_some();
        if arrow_ok && let Some(bounds) = self.arrow_used_col_bounds(sheet, start_row, end_row) {
            return Some(bounds);
        }
        let sr0 = start_row.saturating_sub(1);
        let er0 = end_row.saturating_sub(1);
        self.graph
            .used_col_bounds_for_rows(sheet_id, sr0, er0)
            .map(|(a0, b0)| (a0 + 1, b0 + 1))
    }

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        let _ = self.graph.sheet_id(sheet)?;
        // Excel-like upper bounds; we expose something finite but large.
        // Backends may override with real bounds.
        Some((1_048_576, 16_384)) // 1048576 rows, 16384 cols (XFD)
    }

    fn data_snapshot_id(&self) -> u64 {
        self.snapshot_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn backend_caps(&self) -> crate::traits::BackendCaps {
        crate::traits::BackendCaps {
            streaming: true,
            used_region: true,
            write: false,
            tables: false,
            async_stream: false,
        }
    }

    // Flats removed

    fn date_system(&self) -> crate::engine::DateSystem {
        self.config.date_system
    }
    /// New: resolve a reference into a RangeView (Phase 2 API)
    fn resolve_range_view<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        match reference {
            ReferenceType::Range { .. } => {
                let shared = self.resolve_shared_ref(reference, current_sheet)?;
                let formualizer_common::SheetRef::Range(range) = shared else {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                };
                let sheet_id = match range.sheet {
                    formualizer_common::SheetLocator::Id(id) => id,
                    _ => return Err(ExcelError::new(ExcelErrorKind::Ref)),
                };
                let sheet_name = self.graph.sheet_name(sheet_id);

                let bounded_range = if range.start_row.is_some()
                    && range.start_col.is_some()
                    && range.end_row.is_some()
                    && range.end_col.is_some()
                {
                    Some(RangeRef::try_from_shared(range.as_ref())?)
                } else {
                    None
                };

                let mut sr = bounded_range
                    .as_ref()
                    .map(|r| r.start.coord.row() + 1)
                    .or_else(|| range.start_row.map(|b| b.index + 1));
                let mut sc = bounded_range
                    .as_ref()
                    .map(|r| r.start.coord.col() + 1)
                    .or_else(|| range.start_col.map(|b| b.index + 1));
                let mut er = bounded_range
                    .as_ref()
                    .map(|r| r.end.coord.row() + 1)
                    .or_else(|| range.end_row.map(|b| b.index + 1));
                let mut ec = bounded_range
                    .as_ref()
                    .map(|r| r.end.coord.col() + 1)
                    .or_else(|| range.end_col.map(|b| b.index + 1));

                if sr.is_none() && er.is_none() {
                    // Full-column reference: anchor at row 1
                    let scv = sc.unwrap_or(1);
                    let ecv = ec.unwrap_or(scv);
                    sr = Some(1);
                    if let Some((_, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                        er = Some(max_r);
                    } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                        er = Some(max_rows);
                    }
                }
                if sc.is_none() && ec.is_none() {
                    // Full-row reference: anchor at column 1
                    let srv = sr.unwrap_or(1);
                    let erv = er.unwrap_or(srv);
                    sc = Some(1);
                    if let Some((_, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                        ec = Some(max_c);
                    } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                        ec = Some(max_cols);
                    }
                }
                if sr.is_some() && er.is_none() {
                    let scv = sc.unwrap_or(1);
                    let ecv = ec.unwrap_or(scv);
                    if let Some((_, max_r)) = self.used_rows_for_columns(sheet_name, scv, ecv) {
                        er = Some(max_r);
                    } else if let Some((max_rows, _)) = self.sheet_bounds(sheet_name) {
                        er = Some(max_rows);
                    }
                }
                if er.is_some() && sr.is_none() {
                    // Open start: anchor at row 1
                    sr = Some(1);
                }
                if sc.is_some() && ec.is_none() {
                    let srv = sr.unwrap_or(1);
                    let erv = er.unwrap_or(srv);
                    if let Some((_, max_c)) = self.used_cols_for_rows(sheet_name, srv, erv) {
                        ec = Some(max_c);
                    } else if let Some((_, max_cols)) = self.sheet_bounds(sheet_name) {
                        ec = Some(max_cols);
                    }
                }
                if ec.is_some() && sc.is_none() {
                    // Open start: anchor at column 1
                    sc = Some(1);
                }

                let sr = sr.unwrap_or(1);
                let sc = sc.unwrap_or(1);
                let er = er.unwrap_or(sr.saturating_sub(1));
                let ec = ec.unwrap_or(sc.saturating_sub(1));

                let Some(asheet) = self.sheet_store().sheet(sheet_name) else {
                    return Ok(RangeView::from_owned_rows(
                        Vec::new(),
                        self.config.date_system,
                    ));
                };

                let av = if er < sr || ec < sc {
                    asheet.range_view(1, 1, 0, 0)
                } else {
                    let sr0 = sr.saturating_sub(1) as usize;
                    let sc0 = sc.saturating_sub(1) as usize;
                    let er0 = er.saturating_sub(1) as usize;
                    let ec0 = ec.saturating_sub(1) as usize;
                    asheet.range_view(sr0, sc0, er0, ec0)
                };

                Ok(RangeView::from_arrow(av))
            }
            ReferenceType::Cell { .. } => {
                let shared = self.resolve_shared_ref(reference, current_sheet)?;
                let formualizer_common::SheetRef::Cell(cell) = shared else {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                };
                let addr = CellRef::try_from_shared(cell)?;
                let sheet_id = addr.sheet_id;
                let sheet_name = self.graph.sheet_name(sheet_id);
                let row = addr.coord.row() + 1;
                let col = addr.coord.col() + 1;

                if let Some(asheet) = self.sheet_store().sheet(sheet_name) {
                    let r0 = row.saturating_sub(1) as usize;
                    let c0 = col.saturating_sub(1) as usize;
                    let av = asheet.range_view(r0, c0, r0, c0);
                    Ok(RangeView::from_arrow(av))
                } else {
                    let v = self
                        .get_cell_value(sheet_name, row, col)
                        .unwrap_or(LiteralValue::Empty);
                    Ok(RangeView::from_owned_rows(
                        vec![vec![v]],
                        self.config.date_system,
                    ))
                }
            }
            ReferenceType::NamedRange(name) => {
                if let Some(current_id) = self.graph.sheet_id(current_sheet)
                    && let Some(named) = self.graph.resolve_name_entry(name, current_id)
                {
                    match &named.definition {
                        NamedDefinition::Cell(cell_ref) => {
                            let sheet_name = self.graph.sheet_name(cell_ref.sheet_id);
                            let asheet = self
                                .sheet_store()
                                .sheet(sheet_name)
                                .expect("Arrow sheet missing for named cell");

                            let r0 = cell_ref.coord.row() as usize;
                            let c0 = cell_ref.coord.col() as usize;
                            let av = asheet.range_view(r0, c0, r0, c0);
                            return Ok(RangeView::from_arrow(av));
                        }
                        NamedDefinition::Range(range_ref) => {
                            let sheet_name = self.graph.sheet_name(range_ref.start.sheet_id);
                            let asheet = self
                                .sheet_store()
                                .sheet(sheet_name)
                                .expect("Arrow sheet missing for named range");

                            let sr0 = range_ref.start.coord.row() as usize;
                            let sc0 = range_ref.start.coord.col() as usize;
                            let er0 = range_ref.end.coord.row() as usize;
                            let ec0 = range_ref.end.coord.col() as usize;
                            let av = asheet.range_view(sr0, sc0, er0, ec0);
                            return Ok(RangeView::from_arrow(av));
                        }
                        NamedDefinition::Formula { .. } => {
                            if let Some(value) = self.graph.get_value(named.vertex) {
                                return Ok(RangeView::from_owned_rows(
                                    vec![vec![value]],
                                    self.config.date_system,
                                ));
                            }
                        }
                    }
                }

                let data = self.resolver.resolve_named_range_reference(name)?;
                Ok(RangeView::from_owned_rows(data, self.config.date_system))
            }
            ReferenceType::Table(tref) => {
                // Materialize via Resolver::resolve_range_like tranche 1
                let boxed = self.resolve_range_like(&ReferenceType::Table(tref.clone()))?;
                let owned = boxed.materialise().into_owned();
                Ok(RangeView::from_owned_rows(owned, self.config.date_system))
            }
        }
    }

    fn build_criteria_mask(
        &self,
        view: &crate::arrow_store::ArrowRangeView<'_>,
        col_in_view: usize,
        pred: &crate::args::CriteriaPredicate,
    ) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
        if view.dims().0 == 0 || view.dims().1 == 0 {
            return None;
        }
        compute_criteria_mask(view, col_in_view, pred)
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// Helper: commit spill via shim and mirror resulting cells into Arrow overlay when enabled.
    fn commit_spill_and_mirror(
        &mut self,
        anchor_vertex: VertexId,
        targets: &[CellRef],
        rows: Vec<Vec<LiteralValue>>,
    ) -> Result<(), ExcelError> {
        let prev_spill_cells = self
            .graph
            .spill_cells_for_anchor(anchor_vertex)
            .map(|cells| cells.to_vec())
            .unwrap_or_default();

        // Commit via shim (releases locks)
        self.spill_mgr
            .commit_array(&mut self.graph, anchor_vertex, targets, rows.clone())?;

        if self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled
        {
            if !prev_spill_cells.is_empty() {
                let target_set: FxHashSet<CellRef> = targets.iter().copied().collect();
                let empty = LiteralValue::Empty;
                for cell in prev_spill_cells.iter() {
                    if !target_set.contains(cell) {
                        let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                        self.mirror_value_to_overlay(
                            &sheet_name,
                            cell.coord.row() + 1,
                            cell.coord.col() + 1,
                            &empty,
                        );
                    }
                }
            }

            for (idx, cell) in targets.iter().enumerate() {
                if rows.is_empty() || rows[0].is_empty() {
                    break;
                }
                let width = rows[0].len();
                let r_off = idx / width;
                let c_off = idx % width;
                let v = rows[r_off][c_off].clone();
                let sheet_name = self.graph.sheet_name(cell.sheet_id).to_string();
                self.mirror_value_to_overlay(
                    &sheet_name,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    &v,
                );
            }
        }
        Ok(())
    }
}
