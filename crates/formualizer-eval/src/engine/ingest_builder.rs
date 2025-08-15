use crate::engine::graph::DependencyGraph;
use crate::engine::packed_coord::PackedCoord;
// use crate::engine::plan::RangeKey; // no longer needed directly here
use crate::engine::EvalConfig;
use crate::{SheetId, engine::vertex::VertexId};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_core::parser::{ASTNode, CollectPolicy};
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

struct SheetStage {
    name: String,
    id: SheetId,
    values: Vec<(u32, u32, LiteralValue)>,
    formulas: Vec<(u32, u32, ASTNode, bool)>, // volatile flag
}

impl SheetStage {
    fn new(name: String, id: SheetId) -> Self {
        Self {
            name,
            id,
            values: Vec::new(),
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
        let id = self.g.sheet_id_mut(name);
        self.sheets
            .entry(id)
            .or_insert_with(|| SheetStage::new(name.to_string(), id));
        id
    }

    pub fn add_values<I>(&mut self, sheet: SheetId, values: I)
    where
        I: IntoIterator<Item = (u32, u32, LiteralValue)>,
    {
        let stage = self
            .sheets
            .entry(sheet)
            .or_insert_with(|| SheetStage::new(self.g.sheet_name(sheet).to_string(), sheet));
        stage.values.extend(values);
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
            let vol = ast.contains_volatile();
            stage.formulas.push((r, c, ast, vol));
        }
    }

    pub fn finish(mut self) -> Result<BulkIngestSummary, ExcelError> {
        use std::time::Instant;
        let t0 = Instant::now();
        let dbg = std::env::var("FZ_DEBUG_INGEST")
            .ok()
            .map_or(false, |v| v != "0")
            || std::env::var("FZ_DEBUG_LOAD")
                .ok()
                .map_or(false, |v| v != "0");
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
        let mut coord_accum: Vec<PackedCoord> = Vec::new();
        let mut id_accum: Vec<u32> = Vec::new();
        for (_sid, stage) in self.sheets.drain() {
            let t_sheet0 = Instant::now();
            let mut t_values_ms = 0u128;
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
            // 1) Ensure/allocate values quickly (reuse existing bulk path)
            if !stage.values.is_empty() {
                let tv0 = Instant::now();

                self.g
                    .bulk_insert_values(&stage.name, stage.values.into_iter());
                t_values_ms = tv0.elapsed().as_millis();
            }

            // 2) Build plan for formulas on this sheet
            if !stage.formulas.is_empty() {
                let tp0 = Instant::now();
                let refs = stage
                    .formulas
                    .iter()
                    .map(|(r, c, ast, _)| (stage.name.as_str(), *r, *c, ast));
                // Reuse volatility buffer
                self.vols_buf.clear();
                self.vols_buf.reserve(stage.formulas.len());
                for &(_, _, _, v) in &stage.formulas {
                    self.vols_buf.push(v);
                }
                let policy = CollectPolicy {
                    expand_small_ranges: true,
                    range_expansion_limit: self.g.range_expansion_limit(),
                    include_names: true,
                };
                let plan = self
                    .g
                    .plan_dependencies(refs, &policy, Some(&self.vols_buf))?;
                // Reserve adjacency rows capacity upfront for this sheet
                edges_adj.reserve(plan.formula_targets.len());
                t_plan_ms = tp0.elapsed().as_millis();
                n_targets = plan.formula_targets.len();
                n_globals = plan.global_cells.len();

                // 3) Ensure targets and referenced cells exist using batch allocation when missing
                // Union of targets and global_cells (dedup to cut redundant lookups)
                let mut all_coords: Vec<(SheetId, PackedCoord)> =
                    Vec::with_capacity(plan.formula_targets.len() + plan.global_cells.len());
                all_coords.extend(plan.formula_targets.iter().cloned());
                all_coords.extend(plan.global_cells.iter().cloned());
                // Deduplicate by (SheetId, PackedCoord)
                let mut seen: rustc_hash::FxHashSet<(SheetId, PackedCoord)> =
                    rustc_hash::FxHashSet::default();
                all_coords.retain(|tpl| seen.insert(*tpl));

                // Ensure vertices in batch and also track coords/ids for CSR rebuild
                let te0 = Instant::now();
                let add_batch = self.g.ensure_vertices_batch(&all_coords);
                total_vertices += add_batch.len();
                if !add_batch.is_empty() {
                    for (pc, id) in &add_batch {
                        coord_accum.push(*pc);
                        id_accum.push(*id);
                    }
                }
                t_ensure_ms = te0.elapsed().as_millis();

                // 4) Store ASTs and set kinds/dirty/volatile; map targets to vids
                let ta0 = Instant::now();
                let ast_ids = self
                    .g
                    .store_asts_batch(stage.formulas.iter().map(|(_, _, ast, _)| ast));

                let mut target_vids: Vec<VertexId> = Vec::with_capacity(plan.formula_targets.len());
                for (i, (sid, pc)) in plan.formula_targets.iter().enumerate() {
                    let vid = self.g.vid_for_sid_pc(*sid, *pc).expect("VID must exist");
                    target_vids.push(vid);
                    // Remove old edges if replacing a formula
                    self.g
                        .assign_formula_vertex(vid, ast_ids[i], stage.formulas[i].3);
                }
                total_formulas += target_vids.len();
                t_assign_ms = ta0.elapsed().as_millis();

                // 5) Collect edges into adjacency rows for a later one-shot CSR build
                let ted0 = Instant::now();
                for (fi, &tvid) in target_vids.iter().enumerate() {
                    // Use SmallVec to avoid heap allocs for small dependency counts
                    let mut row: smallvec::SmallVec<[u32; 8]> = smallvec::SmallVec::new();
                    if let Some(indices) = plan.per_formula_cells.get(fi) {
                        let mut dep_count = 0usize;
                        row.reserve(indices.len());
                        for &idx in indices {
                            if let Some(dep_vid) = self.g.vid_for_plan_idx(&plan, idx) {
                                row.push(dep_vid.0);
                                dep_count += 1;
                            }
                        }
                        total_edges += dep_count;
                        n_cell_deps += dep_count;
                    }

                    // Range deps via direct RangeKey path
                    let tr0 = Instant::now();
                    if let Some(rks) = plan.per_formula_ranges.get(fi) {
                        n_range_deps += rks.len();
                        self.g.add_range_deps_from_keys(tvid, rks, stage.id);
                    }
                    t_ranges_ms += tr0.elapsed().as_millis();
                    // Always add adjacency row for target (may be empty)
                    edges_adj.push((tvid.0, row.into_vec()));
                }
                t_edges_ms = ted0.elapsed().as_millis();
            }
            if dbg {
                eprintln!(
                    "[fz][ingest] sheet '{}' done: values={}ms plan={}ms ensure={}ms assign={}ms edges={}ms ranges={}ms targets={} globals={} cell_deps={} range_groups={} total={}ms",
                    stage.name,
                    t_values_ms,
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
                    "[fz][ingest] finalize: start rows={}, vertices={}",
                    rows, total_vertices_now
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
