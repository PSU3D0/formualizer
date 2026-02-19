use crate::engine::VertexId;
use crate::engine::VertexKind;
use crate::engine::eval::Engine;
use crate::traits::{
    EvaluationContext, FunctionProvider, NamedRangeResolver, Range, RangeResolver,
    ReferenceResolver, Resolver, SourceResolver, Table, TableResolver,
};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::{ReferenceType, TableReference};
use rustc_hash::FxHashSet;
use std::sync::Mutex;

use crate::interpreter::Interpreter;

pub struct DynamicRefCollector<'a, R: EvaluationContext> {
    pub engine: &'a Engine<R>,
    pub current_sheet: &'a str,
    pub collected: Mutex<FxHashSet<VertexId>>,
}

impl<'a, R: EvaluationContext> DynamicRefCollector<'a, R> {
    pub fn new(engine: &'a Engine<R>, current_sheet: &'a str) -> Self {
        Self {
            engine,
            current_sheet,
            collected: Mutex::new(FxHashSet::default()),
        }
    }

    fn collect_formula_vertices_in_rect(
        &self,
        sheet_name: &str,
        sr: u32,
        sc: u32,
        er: u32,
        ec: u32,
    ) {
        let Some(sheet_id) = self.engine.graph.sheet_id(sheet_name) else {
            return;
        };
        let Some(index) = self.engine.graph.sheet_index(sheet_id) else {
            return;
        };

        let sr0 = sr.saturating_sub(1);
        let er0 = er.saturating_sub(1);
        let sc0 = sc.saturating_sub(1);
        let ec0 = ec.saturating_sub(1);

        let mut out = self.collected.lock().unwrap();
        for u in index.vertices_in_col_range(sc0, ec0) {
            let row0 = self.engine.graph.vertex_coord(u).row();
            if row0 < sr0 || row0 > er0 {
                continue;
            }
            match self.engine.graph.get_vertex_kind(u) {
                VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                    if self.engine.graph.is_dirty(u) || self.engine.graph.is_volatile(u) {
                        out.insert(u);
                    }
                }
                _ => {}
            }
        }
    }
}

impl<'a, R: EvaluationContext> ReferenceResolver for DynamicRefCollector<'a, R> {
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        let sheet_name = sheet.unwrap_or(self.current_sheet);
        if let Some(&vid) = self
            .engine
            .graph
            .get_vertex_id_for_address(&self.engine.graph.make_cell_ref(sheet_name, row, col))
        {
            self.collected.lock().unwrap().insert(vid);
        }
        self.engine.resolve_cell_reference(sheet, row, col)
    }
}

impl<'a, R: EvaluationContext> RangeResolver for DynamicRefCollector<'a, R> {
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        let sheet_name = sheet.unwrap_or(self.current_sheet);
        let srv = sr.unwrap_or(1u32);
        let scv = sc.unwrap_or(1u32);
        let erv = er.unwrap_or(srv);
        let ecv = ec.unwrap_or(scv);

        self.collect_formula_vertices_in_rect(sheet_name, srv, scv, erv, ecv);

        self.engine.resolve_range_reference(sheet, sr, sc, er, ec)
    }
}

impl<'a, R: EvaluationContext> NamedRangeResolver for DynamicRefCollector<'a, R> {
    fn resolve_named_range_reference(
        &self,
        name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        self.engine.resolve_named_range_reference(name)
    }
}

impl<'a, R: EvaluationContext> TableResolver for DynamicRefCollector<'a, R> {
    fn resolve_table_reference(&self, tref: &TableReference) -> Result<Box<dyn Table>, ExcelError> {
        self.engine.resolve_table_reference(tref)
    }
}

impl<'a, R: EvaluationContext> SourceResolver for DynamicRefCollector<'a, R> {
    fn source_scalar_version(&self, name: &str) -> Option<u64> {
        self.engine.source_scalar_version(name)
    }
    fn resolve_source_scalar(&self, name: &str) -> Result<LiteralValue, ExcelError> {
        self.engine.resolve_source_scalar(name)
    }
    fn source_table_version(&self, name: &str) -> Option<u64> {
        self.engine.source_table_version(name)
    }
    fn resolve_source_table(&self, name: &str) -> Result<Box<dyn Table>, ExcelError> {
        self.engine.resolve_source_table(name)
    }
}

impl<'a, R: EvaluationContext> Resolver for DynamicRefCollector<'a, R> {}

impl<'a, R: EvaluationContext> FunctionProvider for DynamicRefCollector<'a, R> {
    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::traits::Function>> {
        self.engine.get_function(ns, name)
    }
}

impl<'a, R: EvaluationContext> EvaluationContext for DynamicRefCollector<'a, R> {
    fn resolve_range_view<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<crate::engine::range_view::RangeView<'c>, ExcelError> {
        // Collect vertices directly
        match reference {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                self.collect_formula_vertices_in_rect(sheet_name, *row, *col, *row, *col);
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);

                let srv = start_row.unwrap_or(1u32);
                let scv = start_col.unwrap_or(1u32);
                let erv = end_row.unwrap_or(srv);
                let ecv = end_col.unwrap_or(scv);

                self.collect_formula_vertices_in_rect(sheet_name, srv, scv, erv, ecv);
            }
            ReferenceType::NamedRange(name) => {
                let sid = self.engine.sheet_id(current_sheet);
                if let Some(s) = sid
                    && let Some(nr) = self.engine.graph.resolve_name_entry(name, s)
                {
                    let vid = nr.vertex;
                    self.collected.lock().unwrap().insert(vid);
                }
            }
            ReferenceType::Table(_) => {
                // Table references might be tricky, skip for now or resolve from graph if possible
            }
            _ => {}
        }

        self.engine.resolve_range_view(reference, current_sheet)
    }
}

pub struct RangeVirtualDepProvider;

impl RangeVirtualDepProvider {
    pub fn get_virtual_deps<R: EvaluationContext>(
        engine: &Engine<R>,
        v: VertexId,
    ) -> Vec<VertexId> {
        let mut deps = Vec::new();
        if let Some(ranges) = engine.graph.get_range_dependencies(v) {
            let current_sheet_id = engine.graph.get_vertex_sheet_id(v);
            for r in ranges {
                let sheet_id = match r.sheet {
                    formualizer_common::SheetLocator::Id(id) => id,
                    _ => current_sheet_id,
                };
                let sheet_name = engine.graph.sheet_name(sheet_id);

                let mut sr = r.start_row.map(|b| b.index + 1);
                let mut sc = r.start_col.map(|b| b.index + 1);
                let mut er = r.end_row.map(|b| b.index + 1);
                let mut ec = r.end_col.map(|b| b.index + 1);

                if sr.is_none() && er.is_none() {
                    let scv = sc.unwrap_or(1u32);
                    let ecv = ec.unwrap_or(scv);
                    if let Some((min_r, max_r)) = engine.used_rows_for_columns(sheet_name, scv, ecv)
                    {
                        sr = Some(min_r);
                        er = Some(max_r);
                    } else if let Some((_max_rows, _)) = engine.sheet_bounds(sheet_name) {
                        sr = Some(1);
                        er = Some(engine.config.max_open_ended_rows);
                    }
                }
                if sc.is_none() && ec.is_none() {
                    let srv = sr.unwrap_or(1u32);
                    let erv = er.unwrap_or(srv);
                    if let Some((min_c, max_c)) = engine.used_cols_for_rows(sheet_name, srv, erv) {
                        sc = Some(min_c);
                        ec = Some(max_c);
                    } else if let Some((_, _max_cols)) = engine.sheet_bounds(sheet_name) {
                        sc = Some(1);
                        ec = Some(engine.config.max_open_ended_cols);
                    }
                }
                if sr.is_some() && er.is_none() {
                    let scv = sc.unwrap_or(1u32);
                    let ecv = ec.unwrap_or(scv);
                    if let Some((_, max_r)) = engine.used_rows_for_columns(sheet_name, scv, ecv) {
                        er = Some(max_r);
                    } else if let Some((_max_rows, _)) = engine.sheet_bounds(sheet_name) {
                        er = Some(engine.config.max_open_ended_rows);
                    }
                }
                if er.is_some() && sr.is_none() {
                    let scv = sc.unwrap_or(1u32);
                    let ecv = ec.unwrap_or(scv);
                    if let Some((min_r, _)) = engine.used_rows_for_columns(sheet_name, scv, ecv) {
                        sr = Some(min_r);
                    } else {
                        sr = Some(1);
                    }
                }
                if sc.is_some() && ec.is_none() {
                    let srv = sr.unwrap_or(1u32);
                    let erv = er.unwrap_or(srv);
                    if let Some((_, max_c)) = engine.used_cols_for_rows(sheet_name, srv, erv) {
                        ec = Some(max_c);
                    } else if let Some((_, _max_cols)) = engine.sheet_bounds(sheet_name) {
                        ec = Some(engine.config.max_open_ended_cols);
                    }
                }
                if ec.is_some() && sc.is_none() {
                    let srv = sr.unwrap_or(1u32);
                    let erv = er.unwrap_or(srv);
                    if let Some((min_c, _)) = engine.used_cols_for_rows(sheet_name, srv, erv) {
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

                if let Some(index) = engine.graph.sheet_index(sheet_id) {
                    let sr0 = sr.saturating_sub(1);
                    let er0 = er.saturating_sub(1);
                    let sc0 = sc.saturating_sub(1);
                    let ec0 = ec.saturating_sub(1);
                    for u in index.vertices_in_col_range(sc0, ec0) {
                        let pc = engine.graph.vertex_coord(u);
                        let row0 = pc.row();
                        if row0 < sr0 || row0 > er0 {
                            continue;
                        }
                        match engine.graph.get_vertex_kind(u) {
                            VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                                if (engine.graph.is_dirty(u) || engine.graph.is_volatile(u))
                                    && u != v
                                {
                                    deps.push(u);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        deps
    }
}

pub struct VirtualDepBuilder<'a, R: EvaluationContext> {
    engine: &'a Engine<R>,
}

impl<'a, R: EvaluationContext> VirtualDepBuilder<'a, R> {
    pub fn new(engine: &'a Engine<R>) -> Self {
        Self { engine }
    }
    pub fn build(
        &self,
        candidates: &[VertexId],
    ) -> (
        rustc_hash::FxHashMap<VertexId, Vec<VertexId>>,
        Vec<VertexId>,
    ) {
        let mut vdeps: rustc_hash::FxHashMap<VertexId, Vec<VertexId>> =
            rustc_hash::FxHashMap::default();
        let augmented_vertices: Vec<VertexId> = Vec::new(); // Will be populated in Phase 3

        for &v in candidates {
            let mut deps = RangeVirtualDepProvider::get_virtual_deps(self.engine, v);
            let dynamic_deps = DynamicRefVirtualDepProvider::get_virtual_deps(self.engine, v);

            deps.extend(dynamic_deps);
            deps.sort_unstable();
            deps.dedup();

            if !deps.is_empty() {
                vdeps.insert(v, deps);
            }
        }

        (vdeps, augmented_vertices)
    }
}

pub struct DynamicRefVirtualDepProvider;

impl DynamicRefVirtualDepProvider {
    pub fn get_virtual_deps<R: EvaluationContext>(
        engine: &Engine<R>,
        v: VertexId,
    ) -> Vec<VertexId> {
        let mut deps = Vec::new();

        if engine.graph.is_dynamic(v) {
            // Re-evaluating the dynamic formula reference side to find what it references.
            if let Some(ast_id) = engine.graph.get_formula_id(v) {
                let sheet_id = engine.graph.get_vertex_sheet_id(v);
                let sheet_name = engine.graph.sheet_name(sheet_id);

                let collector = DynamicRefCollector::new(engine, sheet_name);

                let cell_ref = engine
                    .graph
                    .get_cell_ref(v)
                    .unwrap_or_else(|| engine.graph.make_cell_ref(sheet_name, 0, 0));

                let interpreter = Interpreter::new_with_cell(&collector, sheet_name, cell_ref);

                // Evaluate the formula. We ignore the result, we only care about the collected vertices!
                let _ = interpreter.evaluate_arena_ast(
                    ast_id,
                    engine.graph.data_store(),
                    engine.graph.sheet_reg(),
                );

                deps.extend(
                    collector
                        .collected
                        .lock()
                        .unwrap()
                        .iter()
                        .copied()
                        .filter(|&u| u != v),
                );
            }
        }

        deps
    }
}
