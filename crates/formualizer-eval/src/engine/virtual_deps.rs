use crate::engine::VertexId;
use crate::engine::VertexKind;
use crate::engine::eval::Engine;
use crate::engine::used_extent::{
    ExtentPolicy, OpenRangeBounds, ResolvedExtent, resolve_used_extent_with_fallback,
};
use crate::formula_plane::region_index::Region;
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
    pub(crate) collected: Mutex<FxHashSet<VertexId>>,
    pub(crate) collected_regions: Mutex<FxHashSet<Region>>,
}

impl<'a, R: EvaluationContext> DynamicRefCollector<'a, R> {
    pub fn new(engine: &'a Engine<R>, current_sheet: &'a str) -> Self {
        Self {
            engine,
            current_sheet,
            collected: Mutex::new(FxHashSet::default()),
            collected_regions: Mutex::new(FxHashSet::default()),
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
        let sr0 = sr.saturating_sub(1);
        let er0 = er.saturating_sub(1);
        let sc0 = sc.saturating_sub(1);
        let ec0 = ec.saturating_sub(1);
        self.collected_regions
            .lock()
            .unwrap()
            .insert(Region::rect(sheet_id, sr0, er0, sc0, ec0).normalized());
        let Some(index) = self.engine.graph.sheet_index(sheet_id) else {
            return;
        };

        let mut out = self.collected.lock().unwrap();
        for u in index.vertices_in_col_range(sc0, ec0) {
            let Some(row0) = self.engine.graph.vertex_grid_addr(u).map(|addr| addr.row()) else {
                continue;
            };
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

    fn collect_formula_vertices_for_range(
        &self,
        sheet_name: &str,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
    ) {
        let Some(extent) = resolve_used_extent_with_fallback(
            OpenRangeBounds {
                start_row,
                start_column: start_col,
                end_row,
                end_column: end_col,
            },
            ExtentPolicy::EvaluationCompat {
                fallback_row: None,
                fallback_column: None,
            },
            || {
                self.engine
                    .sheet_bounds(sheet_name)
                    .map(|_| self.engine.config.max_open_ended_rows)
            },
            || {
                self.engine
                    .sheet_bounds(sheet_name)
                    .map(|_| self.engine.config.max_open_ended_cols)
            },
            |first, last| self.engine.used_rows_for_columns(sheet_name, first, last),
            |first, last| self.engine.used_cols_for_rows(sheet_name, first, last),
        ) else {
            return;
        };

        self.collect_formula_vertices_in_rect(
            sheet_name,
            extent.start_row,
            extent.start_column,
            extent.end_row,
            extent.end_column,
        );
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
        if let Some(sheet_id) = self.engine.graph.sheet_id(sheet_name) {
            self.collected_regions.lock().unwrap().insert(Region::point(
                sheet_id,
                row.saturating_sub(1),
                col.saturating_sub(1),
            ));
        }
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
        self.collect_formula_vertices_for_range(sheet_name, sr, sc, er, ec);
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
    fn planning_semantic_revision(&self) -> Option<u64> {
        self.engine.planning_semantic_revision()
    }

    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::traits::Function>> {
        self.engine.get_function(ns, name)
    }

    fn get_function_for_planning(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::traits::Function>> {
        self.engine.get_function_for_planning(ns, name)
    }
}

impl<'a, R: EvaluationContext> EvaluationContext for DynamicRefCollector<'a, R> {
    fn cancellation_token(&self) -> Option<crate::engine::CancelToken> {
        self.engine.cancellation_token()
    }

    fn resolve_cell_format(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        current_sheet: &str,
    ) -> Option<crate::format::FormatId> {
        self.engine
            .resolve_cell_format(sheet, row, col, current_sheet)
    }

    fn format_class(
        &self,
        format: crate::format::FormatId,
    ) -> Option<formualizer_common::numfmt::FormatClass> {
        self.engine.format_class(format)
    }

    fn record_cell_derived_format(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
        format: Option<crate::format::FormatId>,
    ) {
        self.engine
            .record_cell_derived_format(sheet, row, col, format)
    }

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
                self.collect_formula_vertices_for_range(
                    sheet_name, *start_row, *start_col, *end_row, *end_col,
                );
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
    pub(crate) fn resolve_range<R: EvaluationContext>(
        engine: &Engine<R>,
        sheet_name: &str,
        range: &formualizer_common::SheetRangeRef<'_>,
    ) -> Option<ResolvedExtent> {
        resolve_used_extent_with_fallback(
            OpenRangeBounds {
                start_row: range.start_row.map(|bound| bound.index + 1),
                start_column: range.start_col.map(|bound| bound.index + 1),
                end_row: range.end_row.map(|bound| bound.index + 1),
                end_column: range.end_col.map(|bound| bound.index + 1),
            },
            ExtentPolicy::VirtualDependencyCompat {
                fallback_row: None,
                fallback_column: None,
            },
            || {
                engine
                    .sheet_bounds(sheet_name)
                    .map(|_| engine.config.max_open_ended_rows)
            },
            || {
                engine
                    .sheet_bounds(sheet_name)
                    .map(|_| engine.config.max_open_ended_cols)
            },
            |first, last| engine.used_rows_for_columns(sheet_name, first, last),
            |first, last| engine.used_cols_for_rows(sheet_name, first, last),
        )
    }

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

                let Some(extent) = Self::resolve_range(engine, sheet_name, r) else {
                    continue;
                };
                let sr = extent.start_row;
                let sc = extent.start_column;
                let er = extent.end_row;
                let ec = extent.end_column;

                if let Some(index) = engine.graph.sheet_index(sheet_id) {
                    let sr0 = sr.saturating_sub(1);
                    let er0 = er.saturating_sub(1);
                    let sc0 = sc.saturating_sub(1);
                    let ec0 = ec.saturating_sub(1);
                    for u in index.vertices_in_col_range(sc0, ec0) {
                        let Some(pc) = engine.graph.vertex_grid_addr(u) else {
                            continue;
                        };
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
    fn collect<R: EvaluationContext>(
        engine: &Engine<R>,
        v: VertexId,
    ) -> (Vec<VertexId>, Vec<Region>) {
        if !engine.graph.is_dynamic(v) {
            return (Vec::new(), Vec::new());
        }
        let Some(ast_id) = engine.graph.get_formula_id(v) else {
            return (Vec::new(), Vec::new());
        };
        let sheet_id = engine.graph.get_vertex_sheet_id(v);
        let sheet_name = engine.graph.sheet_name(sheet_id);
        let collector = DynamicRefCollector::new(engine, sheet_name);
        let cell_ref = engine
            .graph
            .get_cell_ref(v)
            .unwrap_or_else(|| engine.graph.make_cell_ref(sheet_name, 0, 0));
        let interpreter = Interpreter::new_with_cell(&collector, sheet_name, cell_ref);
        let _ = interpreter.evaluate_arena_ast(
            ast_id,
            engine.graph.data_store(),
            engine.graph.sheet_reg(),
        );
        let mut deps = collector
            .collected
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|&dependency| dependency != v)
            .collect::<Vec<_>>();
        deps.sort_unstable();
        deps.dedup();
        let mut regions = collector
            .collected_regions
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        regions.sort_by_key(|region| {
            let (rows, cols) = region.axis_ranges();
            (region.sheet_id(), rows.query_bounds(), cols.query_bounds())
        });
        regions.dedup();
        (deps, regions)
    }

    pub fn get_virtual_deps<R: EvaluationContext>(
        engine: &Engine<R>,
        v: VertexId,
    ) -> Vec<VertexId> {
        Self::collect(engine, v).0
    }

    pub(crate) fn get_virtual_regions<R: EvaluationContext>(
        engine: &Engine<R>,
        v: VertexId,
    ) -> Vec<Region> {
        Self::collect(engine, v).1
    }
}
