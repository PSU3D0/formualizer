use crate::engine::graph::DependencyGraph;
use formualizer_common::Coord as AbsCoord;
// use crate::engine::plan::RangeKey; // no longer needed directly here
use crate::engine::EvalConfig;
use crate::engine::arena::AstNodeId;
use crate::engine::ingest_pipeline::{DependencyPlanRow, FormulaAstInput};
use crate::engine::plan::{
    DependencyPlan, F_HAS_NAMES, F_HAS_RANGES, F_HAS_TABLES, F_VOLATILE, RangeKey,
};
use crate::{SheetId, engine::vertex::VertexId};
use formualizer_common::{CoordBuildHasher, ExcelError, PackedSheetCell};
use formualizer_parse::parser::ASTNode;
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::sync::Arc;

/// Summary of bulk ingest
#[derive(Debug, Clone)]
pub struct BulkIngestSummary {
    pub sheets: usize,
    pub vertices: usize,
    pub formulas: usize,
    pub edges: usize,
    pub elapsed: std::time::Duration,
}

enum FormulaAstSource {
    Owned(ASTNode),
    Interned(AstNodeId),
    Planned {
        ast_id: AstNodeId,
        plan: DependencyPlanRow,
    },
}

struct StagedFormula {
    row: u32,
    col: u32,
    ast: FormulaAstSource,
}

struct RegistryFunctionProvider;

impl crate::traits::FunctionProvider for RegistryFunctionProvider {
    fn get_function(&self, ns: &str, name: &str) -> Option<Arc<dyn crate::function::Function>> {
        crate::function_registry::get(ns, name)
    }
}

struct SheetStage {
    name: String,
    id: SheetId,
    formulas: Vec<StagedFormula>,
}

impl SheetStage {
    fn new(name: String, id: SheetId) -> Self {
        Self {
            name,
            id,
            formulas: Vec::new(),
        }
    }
}

fn range_key_from_shared(
    range: &crate::reference::SharedRangeRef<'static>,
    current_sheet: SheetId,
) -> RangeKey {
    let sheet = match range.sheet {
        crate::reference::SharedSheetLocator::Id(id) => id,
        _ => current_sheet,
    };
    match (
        range.start_row,
        range.start_col,
        range.end_row,
        range.end_col,
    ) {
        (Some(sr), Some(sc), Some(er), Some(ec)) => RangeKey::Rect {
            sheet,
            start: AbsCoord::new(sr.index, sc.index),
            end: AbsCoord::new(er.index, ec.index),
        },
        (None, Some(sc), None, Some(ec)) if sc.index == ec.index => RangeKey::WholeCol {
            sheet,
            col: sc.index + 1,
        },
        (Some(sr), None, Some(er), None) if sr.index == er.index => RangeKey::WholeRow {
            sheet,
            row: sr.index + 1,
        },
        _ => RangeKey::OpenRect {
            sheet,
            start: range
                .start_row
                .zip(range.start_col)
                .map(|(r, c)| AbsCoord::new(r.index, c.index)),
            end: range
                .end_row
                .zip(range.end_col)
                .map(|(r, c)| AbsCoord::new(r.index, c.index)),
        },
    }
}

fn dependency_plan_from_rows(
    sheet_id: SheetId,
    rows: &[(u32, u32, DependencyPlanRow)],
) -> DependencyPlan {
    let mut plan = DependencyPlan::default();
    let mut cell_index: HashMap<PackedSheetCell, u32, CoordBuildHasher> =
        HashMap::with_hasher(CoordBuildHasher);
    let mut vertex_pool_index: HashMap<PackedSheetCell, u32, CoordBuildHasher> =
        HashMap::with_hasher(CoordBuildHasher);

    fn ensure_vertex_pool_index(
        plan: &mut DependencyPlan,
        index: &mut HashMap<PackedSheetCell, u32, CoordBuildHasher>,
        key: (SheetId, AbsCoord),
    ) -> u32 {
        let packed = PackedSheetCell::try_new(key.0, key.1.row(), key.1.col())
            .expect("plan vertex pool coordinate must fit PackedSheetCell");
        if let Some(&idx) = index.get(&packed) {
            idx
        } else {
            let idx = plan.vertex_pool.len() as u32;
            plan.vertex_pool.push(key);
            plan.vertex_pool_packed.push(packed);
            index.insert(packed, idx);
            idx
        }
    }

    for (row, col, row_plan) in rows {
        let target = (sheet_id, AbsCoord::from_excel(*row, *col));
        plan.formula_targets.push(target);
        let target_pool_idx = ensure_vertex_pool_index(&mut plan, &mut vertex_pool_index, target);
        plan.formula_target_pool_indices.push(target_pool_idx);

        let mut per_cells = Vec::new();
        for dep in &row_plan.direct_cell_deps {
            let key = (
                dep.sheet_id,
                AbsCoord::new(dep.coord.row(), dep.coord.col()),
            );
            let packed = PackedSheetCell::try_new(key.0, key.1.row(), key.1.col())
                .expect("plan dependency coordinate must fit PackedSheetCell");
            let idx = if let Some(&idx) = cell_index.get(&packed) {
                idx
            } else {
                let idx = plan.global_cells.len() as u32;
                plan.global_cells.push(key);
                cell_index.insert(packed, idx);
                let pool_idx = ensure_vertex_pool_index(&mut plan, &mut vertex_pool_index, key);
                plan.global_cell_pool_indices.push(pool_idx);
                idx
            };
            per_cells.push(idx);
        }

        let flags = (if row_plan.volatile { F_VOLATILE } else { 0 })
            | (if row_plan.range_deps.is_empty() {
                0
            } else {
                F_HAS_RANGES
            })
            | (if row_plan.named_refs.is_empty() && row_plan.resolved_named_refs.is_empty() {
                0
            } else {
                F_HAS_NAMES
            })
            | (if row_plan.table_refs.is_empty() {
                0
            } else {
                F_HAS_TABLES
            });
        plan.per_formula_cells.push(per_cells);
        plan.per_formula_ranges.push(
            row_plan
                .range_deps
                .iter()
                .map(|range| range_key_from_shared(range, sheet_id))
                .collect(),
        );
        let mut names = row_plan.resolved_named_refs.clone();
        names.extend(row_plan.named_refs.clone());
        names.extend(row_plan.source_refs.clone());
        plan.per_formula_names.push(names);
        let mut tables = row_plan.table_refs.clone();
        tables.extend(row_plan.source_refs.clone());
        plan.per_formula_tables.push(tables);
        plan.per_formula_flags.push(flags);
    }
    plan
}

pub struct BulkIngestBuilder<'g> {
    g: &'g mut DependencyGraph,
    sheets: FxHashMap<SheetId, SheetStage>,
    cfg_saved: EvalConfig,
}

impl<'g> BulkIngestBuilder<'g> {
    pub fn new(g: &'g mut DependencyGraph) -> Self {
        let cfg_saved = g.get_config().clone();
        // Respect current sheet index mode (loader may set Lazy to skip index work during ingest)
        Self {
            g,
            sheets: FxHashMap::default(),
            cfg_saved,
        }
    }

    pub fn add_sheet(&mut self, name: &str) -> SheetId {
        let id = self.g.sheet_id(name).unwrap_or_else(|| {
            panic!(
                "BulkIngestBuilder::add_sheet requires pre-existing sheet; call Engine::add_sheet first: {name}"
            )
        });
        self.sheets
            .entry(id)
            .or_insert_with(|| SheetStage::new(name.to_string(), id));
        id
    }

    pub fn add_formulas<I>(&mut self, sheet: SheetId, formulas: I)
    where
        I: IntoIterator<Item = (u32, u32, ASTNode)>,
    {
        let stage = self
            .sheets
            .entry(sheet)
            .or_insert_with(|| SheetStage::new(self.g.sheet_name(sheet).to_string(), sheet));
        for (r, c, ast) in formulas {
            stage.formulas.push(StagedFormula {
                row: r,
                col: c,
                ast: FormulaAstSource::Owned(ast),
            });
        }
    }

    pub fn add_formula_ids<I>(&mut self, sheet: SheetId, formulas: I)
    where
        I: IntoIterator<Item = (u32, u32, AstNodeId)>,
    {
        let stage = self
            .sheets
            .entry(sheet)
            .or_insert_with(|| SheetStage::new(self.g.sheet_name(sheet).to_string(), sheet));
        for (r, c, ast_id) in formulas {
            stage.formulas.push(StagedFormula {
                row: r,
                col: c,
                ast: FormulaAstSource::Interned(ast_id),
            });
        }
    }

    pub(crate) fn add_formula_plans<I>(&mut self, sheet: SheetId, formulas: I)
    where
        I: IntoIterator<Item = (u32, u32, AstNodeId, DependencyPlanRow)>,
    {
        let stage = self
            .sheets
            .entry(sheet)
            .or_insert_with(|| SheetStage::new(self.g.sheet_name(sheet).to_string(), sheet));
        for (r, c, ast_id, plan) in formulas {
            stage.formulas.push(StagedFormula {
                row: r,
                col: c,
                ast: FormulaAstSource::Planned { ast_id, plan },
            });
        }
    }

    pub fn finish(mut self) -> Result<BulkIngestSummary, ExcelError> {
        use crate::instant::FzInstant as Instant;
        let t0 = Instant::now();
        let dbg = std::env::var("FZ_DEBUG_INGEST")
            .ok()
            .is_some_and(|v| v != "0")
            || std::env::var("FZ_DEBUG_LOAD")
                .ok()
                .is_some_and(|v| v != "0");
        let mut total_vertices = 0usize;
        let mut total_formulas = 0usize;
        let mut total_edges = 0usize;

        if dbg {
            eprintln!(
                "[fz][ingest] starting bulk ingest with {} sheets",
                self.sheets.len()
            );
        }

        // Materialize per-sheet to keep caches warm and reduce cross-sheet churn
        // Accumulate a flat adjacency for a single-shot CSR build
        let mut edges_adj: Vec<(u32, Vec<u32>)> = Vec::new();
        let mut coord_accum: Vec<AbsCoord> = Vec::new();
        let mut id_accum: Vec<u32> = Vec::new();
        for (_sid, mut stage) in self.sheets.drain() {
            let t_sheet0 = Instant::now();
            let mut t_plan_ms = 0u128;
            let mut t_ensure_ms = 0u128;
            let mut t_assign_ms = 0u128;
            let mut t_edges_ms = 0u128;
            let mut t_ranges_ms = 0u128;
            let mut n_targets = 0usize;
            let mut n_globals = 0usize;
            let mut n_cell_deps = 0usize;
            let mut n_range_deps = 0usize;
            if dbg {
                eprintln!("[fz][ingest] sheet '{}' begin", stage.name);
            }
            // 1) Build plans for formulas on this sheet in chunks.
            if !stage.formulas.is_empty() {
                let formula_batch_size: usize = std::env::var("FZ_INGEST_FORMULA_BATCH")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .filter(|&n| n > 0)
                    .unwrap_or(10_000);
                let mut batch_count = 0usize;

                for chunk in stage.formulas.chunks_mut(formula_batch_size) {
                    batch_count += 1;

                    let tp0 = Instant::now();
                    let mut prepared: Vec<Option<(AstNodeId, DependencyPlanRow)>> =
                        (0..chunk.len()).map(|_| None).collect();
                    let mut pipeline_inputs = Vec::new();
                    for (idx, formula) in chunk.iter().enumerate() {
                        match &formula.ast {
                            FormulaAstSource::Planned { ast_id, plan } => {
                                prepared[idx] = Some((*ast_id, plan.clone()));
                            }
                            FormulaAstSource::Owned(ast) => {
                                let placement = crate::reference::CellRef::new(
                                    stage.id,
                                    crate::reference::Coord::from_excel(
                                        formula.row,
                                        formula.col,
                                        true,
                                        true,
                                    ),
                                );
                                pipeline_inputs.push((
                                    idx,
                                    FormulaAstInput::Tree(ast.clone()),
                                    placement,
                                ));
                            }
                            FormulaAstSource::Interned(ast_id) => {
                                let placement = crate::reference::CellRef::new(
                                    stage.id,
                                    crate::reference::Coord::from_excel(
                                        formula.row,
                                        formula.col,
                                        true,
                                        true,
                                    ),
                                );
                                pipeline_inputs.push((
                                    idx,
                                    FormulaAstInput::RawArena(*ast_id),
                                    placement,
                                ));
                            }
                        }
                    }
                    if !pipeline_inputs.is_empty() {
                        let indices: Vec<usize> =
                            pipeline_inputs.iter().map(|(idx, _, _)| *idx).collect();
                        let provider = RegistryFunctionProvider;
                        let ingested = {
                            let mut pipeline = self.g.ingest_pipeline(&provider);
                            let inputs = pipeline_inputs
                                .into_iter()
                                .map(|(_, input, placement)| (input, placement, None));
                            pipeline.ingest_batch(inputs)?
                        };
                        for (idx, ingested) in indices.into_iter().zip(ingested) {
                            prepared[idx] = Some((ingested.ast_id, ingested.dep_plan));
                        }
                    }
                    let prepared: Vec<(AstNodeId, DependencyPlanRow)> = prepared
                        .into_iter()
                        .map(|entry| entry.expect("formula must be planned"))
                        .collect();
                    let ast_ids: Vec<AstNodeId> =
                        prepared.iter().map(|(ast_id, _)| *ast_id).collect();
                    let row_plans: Vec<(u32, u32, DependencyPlanRow)> = chunk
                        .iter()
                        .zip(prepared.into_iter())
                        .map(|(formula, (_, plan))| (formula.row, formula.col, plan))
                        .collect();
                    let plan = dependency_plan_from_rows(stage.id, &row_plans);
                    edges_adj.reserve(plan.formula_targets.len());
                    t_plan_ms += tp0.elapsed().as_millis();
                    n_targets += plan.formula_targets.len();
                    n_globals += plan.global_cells.len();

                    // Reserve capacity hints before large ensure / hash-map growth.
                    self.g.reserve_cells(plan.vertex_pool.len());

                    // Ensure targets and referenced cells exist using batch allocation when missing.
                    let te0 = Instant::now();
                    let (all_vids, add_batch) = self
                        .g
                        .ensure_vertices_batch_packed_ordered(&plan.vertex_pool_packed);
                    total_vertices += add_batch.len();
                    if !add_batch.is_empty() {
                        for (pc, id) in &add_batch {
                            coord_accum.push(*pc);
                            id_accum.push(*id);
                        }
                    }
                    t_ensure_ms += te0.elapsed().as_millis();

                    // Assign formula vertices using the canonical AST ids and flags produced by the pipeline.
                    let ta0 = Instant::now();
                    self.g.reserve_formula_metadata(plan.formula_targets.len());

                    let mut dep_vids: Vec<VertexId> = Vec::with_capacity(plan.global_cells.len());
                    for &pos in &plan.global_cell_pool_indices {
                        dep_vids.push(all_vids[pos as usize]);
                    }

                    let mut target_vids: Vec<VertexId> =
                        Vec::with_capacity(plan.formula_targets.len());
                    let load_fast = self.g.first_load_assume_new();
                    for (i, &pos) in plan.formula_target_pool_indices.iter().enumerate() {
                        let vid = all_vids[pos as usize];
                        target_vids.push(vid);
                        let row_plan = &row_plans[i].2;
                        if load_fast {
                            self.g.assign_formula_vertex_load_fast(
                                vid,
                                ast_ids[i],
                                row_plan.volatile,
                                row_plan.dynamic,
                            );
                        } else {
                            self.g.assign_formula_vertex(
                                vid,
                                ast_ids[i],
                                row_plan.volatile,
                                row_plan.dynamic,
                            );
                        }
                    }
                    self.g.mark_vertices_dirty_batch(&target_vids);
                    total_formulas += target_vids.len();
                    t_assign_ms += ta0.elapsed().as_millis();

                    // Collect edges into adjacency rows for a later one-shot CSR build.
                    let ted0 = Instant::now();
                    for (fi, &tvid) in target_vids.iter().enumerate() {
                        let mut row: smallvec::SmallVec<[u32; 8]> = smallvec::SmallVec::new();
                        if let Some(indices) = plan.per_formula_cells.get(fi) {
                            let mut dep_count = 0usize;
                            row.reserve(indices.len());
                            for &idx in indices {
                                let dep_vid = dep_vids[idx as usize];
                                row.push(dep_vid.0);
                                dep_count += 1;
                            }
                            total_edges += dep_count;
                            n_cell_deps += dep_count;
                        }

                        let tr0 = Instant::now();
                        if let Some(rks) = plan.per_formula_ranges.get(fi) {
                            n_range_deps += rks.len();
                            self.g.add_range_deps_from_keys(tvid, rks, stage.id);
                        }
                        t_ranges_ms += tr0.elapsed().as_millis();
                        if let Some(names) = plan.per_formula_names.get(fi)
                            && !names.is_empty()
                        {
                            let mut name_vertices = Vec::new();
                            let (formula_sheet, _) = plan
                                .formula_targets
                                .get(fi)
                                .copied()
                                .unwrap_or((stage.id, AbsCoord::new(1, 1)));
                            for name in names {
                                if let Some(named) = self.g.resolve_name_entry(name, formula_sheet)
                                {
                                    row.push(named.vertex.0);
                                    name_vertices.push(named.vertex);
                                } else if let Some(source) =
                                    self.g.resolve_source_scalar_entry(name)
                                {
                                    row.push(source.vertex.0);
                                } else {
                                    self.g
                                        .record_pending_name_reference(formula_sheet, name, tvid);
                                }
                            }
                            if !name_vertices.is_empty() {
                                self.g.attach_vertex_to_names(tvid, &name_vertices);
                            }
                        }

                        if let Some(tables) = plan.per_formula_tables.get(fi)
                            && !tables.is_empty()
                        {
                            for table_name in tables {
                                if let Some(table) = self.g.resolve_table_entry(table_name) {
                                    row.push(table.vertex.0);
                                } else if let Some(source) =
                                    self.g.resolve_source_table_entry(table_name)
                                {
                                    row.push(source.vertex.0);
                                }
                            }
                        }
                        edges_adj.push((tvid.0, row.into_vec()));
                    }
                    t_edges_ms += ted0.elapsed().as_millis();
                }

                if dbg && batch_count > 1 {
                    eprintln!(
                        "[fz][ingest] sheet '{}' processed in {} formula batches (batch_size={})",
                        stage.name, batch_count, formula_batch_size
                    );
                }
            }
            if dbg {
                eprintln!(
                    "[fz][ingest] sheet '{}' done: plan={}ms ensure={}ms assign={}ms edges={}ms ranges={}ms targets={} globals={} cell_deps={} range_groups={} total={}ms",
                    stage.name,
                    t_plan_ms,
                    t_ensure_ms,
                    t_assign_ms,
                    t_edges_ms,
                    t_ranges_ms,
                    n_targets,
                    n_globals,
                    n_cell_deps,
                    n_range_deps,
                    t_sheet0.elapsed().as_millis()
                );
            }
        }
        if dbg {
            eprintln!("[fz][ingest] beginning finalize");
        }

        // Finalize: pick strategy based on graph size and number of edge rows
        if !edges_adj.is_empty() {
            let rows = edges_adj.len();
            let total_vertices_now = self.g.vertex_count();
            let t_fin0 = Instant::now();
            if dbg {
                eprintln!(
                    "[fz][ingest] finalize: start rows={rows}, vertices={total_vertices_now}"
                );
            }
            // Heuristic: avoid one-shot CSR when vertices are huge and rows are sparse
            let sparse_vs_huge =
                total_vertices_now > 800_000 && (rows as f64) / (total_vertices_now as f64) < 0.05;
            if sparse_vs_huge {
                let t_delta0 = Instant::now();
                if dbg {
                    eprintln!("[fz][ingest] finalize: using delta path (begin)");
                }
                self.g.begin_batch();
                for (tvid_raw, row) in &edges_adj {
                    let tvid = crate::engine::vertex::VertexId(*tvid_raw);
                    if !row.is_empty() {
                        let deps: Vec<crate::engine::vertex::VertexId> = row
                            .iter()
                            .map(|d| crate::engine::vertex::VertexId(*d))
                            .collect();
                        self.g.add_edges_nobatch(tvid, &deps);
                    }
                }
                self.g.end_batch();
                if dbg {
                    eprintln!(
                        "[fz][ingest] finalize: delta done in {} ms (total {} ms)",
                        t_delta0.elapsed().as_millis(),
                        t_fin0.elapsed().as_millis()
                    );
                }
            } else {
                // One-shot CSR build from accumulated adjacency and coords/ids
                let mut t_coords_ms = 0u128;
                if coord_accum.is_empty() || id_accum.is_empty() {
                    if dbg {
                        eprintln!("[fz][ingest] finalize: gathering coords/ids");
                    }
                    let t_coords0 = Instant::now();
                    for vid in self.g.iter_vertex_ids() {
                        coord_accum.push(self.g.vertex_coord(vid));
                        id_accum.push(vid.0);
                    }
                    t_coords_ms = t_coords0.elapsed().as_millis();
                }
                if dbg {
                    eprintln!("[fz][ingest] finalize: building CSR");
                }
                let t_csr0 = Instant::now();
                self.g
                    .build_edges_from_adjacency(edges_adj, coord_accum, id_accum);
                if dbg {
                    eprintln!(
                        "[fz][ingest] finalize: rows={}, gather_coords={} ms, csr_build={} ms, total={} ms",
                        rows,
                        t_coords_ms,
                        t_csr0.elapsed().as_millis(),
                        t_fin0.elapsed().as_millis()
                    );
                }
            }
        }

        // Restore config
        self.g.set_sheet_index_mode(self.cfg_saved.sheet_index_mode);
        Ok(BulkIngestSummary {
            sheets: 0, // could populate later
            vertices: total_vertices,
            formulas: total_formulas,
            edges: total_edges,
            elapsed: t0.elapsed(),
        })
    }
}
