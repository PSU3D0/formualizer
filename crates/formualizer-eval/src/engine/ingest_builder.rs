use crate::engine::graph::DependencyGraph;
use formualizer_common::Coord as AbsCoord;
// use crate::engine::plan::RangeKey; // no longer needed directly here
use crate::engine::EvalConfig;
use crate::engine::arena::{AstNodeData, AstNodeId, DataStore};
use crate::engine::plan::DependencyPlanAst;
use crate::{SheetId, engine::vertex::VertexId};
use formualizer_common::{ExcelError, ExcelErrorKind};
use formualizer_parse::parser::{ASTNode, CollectPolicy};
use rustc_hash::FxHashMap;

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
}

struct StagedFormula {
    row: u32,
    col: u32,
    ast: FormulaAstSource,
    volatile: bool,
}

enum WorkingAst {
    Tree(ASTNode),
    Arena(AstNodeId),
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

pub struct BulkIngestBuilder<'g> {
    g: &'g mut DependencyGraph,
    sheets: FxHashMap<SheetId, SheetStage>,
    cfg_saved: EvalConfig,
    vols_buf: Vec<bool>,
}

impl<'g> BulkIngestBuilder<'g> {
    pub fn new(g: &'g mut DependencyGraph) -> Self {
        let cfg_saved = g.get_config().clone();
        // Respect current sheet index mode (loader may set Lazy to skip index work during ingest)
        Self {
            g,
            sheets: FxHashMap::default(),
            cfg_saved,
            vols_buf: Vec::new(),
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
            let vol = Self::is_ast_volatile(&ast);
            stage.formulas.push(StagedFormula {
                row: r,
                col: c,
                ast: FormulaAstSource::Owned(ast),
                volatile: vol,
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
                volatile: false,
            });
        }
    }

    fn is_ast_volatile(ast: &ASTNode) -> bool {
        use formualizer_parse::parser::ASTNodeType;

        if ast.contains_volatile() {
            return true;
        }

        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                if let Some(func) = crate::function_registry::get("", name)
                    && func.caps().contains(crate::function::FnCaps::VOLATILE)
                {
                    return true;
                }
                args.iter().any(Self::is_ast_volatile)
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                Self::is_ast_volatile(left) || Self::is_ast_volatile(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => Self::is_ast_volatile(expr),
            ASTNodeType::Array(rows) => {
                rows.iter().any(|row| row.iter().any(Self::is_ast_volatile))
            }
            _ => false,
        }
    }

    fn ast_is_volatile_arena(ast_id: AstNodeId, data_store: &DataStore) -> bool {
        let Some(node) = data_store.get_node(ast_id) else {
            return false;
        };
        match node {
            AstNodeData::Function { name_id, .. } => {
                let name = data_store.resolve_ast_string(*name_id);
                if let Some(func) = crate::function_registry::get("", name)
                    && func.caps().contains(crate::function::FnCaps::VOLATILE)
                {
                    return true;
                }
                data_store.get_args(ast_id).is_some_and(|args| {
                    args.iter()
                        .any(|&arg| Self::ast_is_volatile_arena(arg, data_store))
                })
            }
            AstNodeData::BinaryOp {
                left_id, right_id, ..
            } => {
                Self::ast_is_volatile_arena(*left_id, data_store)
                    || Self::ast_is_volatile_arena(*right_id, data_store)
            }
            AstNodeData::UnaryOp { expr_id, .. } => {
                Self::ast_is_volatile_arena(*expr_id, data_store)
            }
            AstNodeData::Array { .. } => {
                data_store
                    .get_array_elems(ast_id)
                    .is_some_and(|(_, _, elems)| {
                        elems
                            .iter()
                            .any(|&elem| Self::ast_is_volatile_arena(elem, data_store))
                    })
            }
            _ => false,
        }
    }

    fn ast_is_dynamic_arena(ast_id: AstNodeId, data_store: &DataStore) -> bool {
        let Some(node) = data_store.get_node(ast_id) else {
            return false;
        };
        match node {
            AstNodeData::Function { name_id, .. } => {
                let name = data_store.resolve_ast_string(*name_id);
                if let Some(func) = crate::function_registry::get("", name)
                    && func
                        .caps()
                        .contains(crate::function::FnCaps::DYNAMIC_DEPENDENCY)
                {
                    return true;
                }
                data_store.get_args(ast_id).is_some_and(|args| {
                    args.iter()
                        .any(|&arg| Self::ast_is_dynamic_arena(arg, data_store))
                })
            }
            AstNodeData::BinaryOp {
                left_id, right_id, ..
            } => {
                Self::ast_is_dynamic_arena(*left_id, data_store)
                    || Self::ast_is_dynamic_arena(*right_id, data_store)
            }
            AstNodeData::UnaryOp { expr_id, .. } => {
                Self::ast_is_dynamic_arena(*expr_id, data_store)
            }
            AstNodeData::Array { .. } => {
                data_store
                    .get_array_elems(ast_id)
                    .is_some_and(|(_, _, elems)| {
                        elems
                            .iter()
                            .any(|&elem| Self::ast_is_dynamic_arena(elem, data_store))
                    })
            }
            _ => false,
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

                    let mut working_asts = Vec::with_capacity(chunk.len());
                    for formula in chunk.iter_mut() {
                        let coord = crate::reference::Coord::from_excel(
                            formula.row,
                            formula.col,
                            true,
                            true,
                        );
                        let cell = crate::reference::CellRef::new(stage.id, coord);
                        let working = match &formula.ast {
                            FormulaAstSource::Owned(ast) => {
                                let mut ast = ast.clone();
                                self.g
                                    .rewrite_structured_references_for_cell(&mut ast, cell)?;
                                formula.volatile = Self::is_ast_volatile(&ast);
                                WorkingAst::Tree(ast)
                            }
                            FormulaAstSource::Interned(ast_id) => {
                                if self.g.data_store().ast_needs_structural_rewrite(*ast_id) {
                                    let mut ast = self
                                        .g
                                        .data_store()
                                        .retrieve_ast(*ast_id, self.g.sheet_reg())
                                        .ok_or_else(|| {
                                            ExcelError::new(ExcelErrorKind::Value)
                                                .with_message("Missing interned formula AST")
                                        })?;
                                    self.g
                                        .rewrite_structured_references_for_cell(&mut ast, cell)?;
                                    formula.volatile = Self::is_ast_volatile(&ast);
                                    let rewritten_id = self.g.store_ast(&ast);
                                    WorkingAst::Arena(rewritten_id)
                                } else {
                                    formula.volatile =
                                        Self::ast_is_volatile_arena(*ast_id, self.g.data_store());
                                    WorkingAst::Arena(*ast_id)
                                }
                            }
                        };
                        working_asts.push(working);
                    }

                    let tp0 = Instant::now();
                    let refs = chunk.iter().zip(working_asts.iter()).map(|(formula, ast)| {
                        let ast = match ast {
                            WorkingAst::Tree(ast) => DependencyPlanAst::Tree(ast),
                            WorkingAst::Arena(ast_id) => DependencyPlanAst::Arena(*ast_id),
                        };
                        (stage.name.as_str(), formula.row, formula.col, ast)
                    });
                    self.vols_buf.clear();
                    self.vols_buf.reserve(chunk.len());
                    for formula in chunk.iter() {
                        self.vols_buf.push(formula.volatile);
                    }
                    let policy = CollectPolicy {
                        expand_small_ranges: true,
                        range_expansion_limit: self.g.range_expansion_limit(),
                        include_names: true,
                    };
                    let plan =
                        self.g
                            .plan_dependencies_mixed(refs, &policy, Some(&self.vols_buf))?;
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

                    // Assign formula vertices. Pre-interned FormulaPlane records reuse their
                    // existing arena ids unless a context-dependent structured reference had to
                    // be rewritten for this cell.
                    let ta0 = Instant::now();
                    self.g.reserve_formula_metadata(plan.formula_targets.len());
                    let ast_ids: Vec<AstNodeId> = working_asts
                        .iter()
                        .map(|working| match working {
                            WorkingAst::Tree(ast) => self.g.store_ast(ast),
                            WorkingAst::Arena(ast_id) => *ast_id,
                        })
                        .collect();

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
                        let dynamic = match &working_asts[i] {
                            WorkingAst::Tree(ast) => self.g.is_ast_dynamic(ast),
                            WorkingAst::Arena(ast_id) => {
                                Self::ast_is_dynamic_arena(*ast_id, self.g.data_store())
                            }
                        };
                        let volatile = chunk[i].volatile;
                        if load_fast {
                            self.g.assign_formula_vertex_load_fast(
                                vid, ast_ids[i], volatile, dynamic,
                            );
                        } else {
                            self.g
                                .assign_formula_vertex(vid, ast_ids[i], volatile, dynamic);
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
