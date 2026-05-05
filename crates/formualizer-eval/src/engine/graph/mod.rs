use crate::SheetId;
use crate::engine::TombstoneRegistry;
use crate::engine::named_range::{NameScope, NamedDefinition, NamedRange};
use crate::engine::sheet_registry::SheetRegistry;
use crate::formula_plane::authority::FormulaAuthority;
use formualizer_common::{
    CoordBuildHasher, ExcelError, ExcelErrorKind, LiteralValue, PackedSheetCell,
};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use rustc_hash::{FxHashMap, FxHashSet};

#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
#[derive(Debug, Default, Clone)]
pub struct GraphInstrumentation {
    pub edges_added: u64,
    pub stripe_inserts: u64,
    pub stripe_removes: u64,
    pub dependents_scan_fallback_calls: u64,
    pub dependents_scan_vertices_scanned: u64,
}

mod ast_utils;
pub mod editor;
mod formula_analysis;
mod names;
mod range_deps;
mod sheets;
pub mod snapshot;
mod sources;
mod tables;

use super::arena::{AstNodeId, DataStore, ValueRef};
use super::delta_edges::CsrMutableEdges;
use super::ingest_pipeline::{DependencyPlanRow, FormulaAstInput};
use super::sheet_index::SheetIndex;
use super::vertex::{VertexId, VertexKind};
use super::vertex_store::{FIRST_NORMAL_VERTEX, VertexStore};
use crate::engine::topo::{
    GraphAdapter,
    pk::{DynamicTopo, PkConfig},
};
use crate::reference::{CellRef, Coord, SharedRangeRef, SharedRef, SharedSheetLocator};
use formualizer_common::Coord as AbsCoord;
// topo::pk wiring will be integrated behind config.use_dynamic_topo in a follow-up step

struct RegistryFunctionProvider;

impl crate::traits::FunctionProvider for RegistryFunctionProvider {
    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        crate::function_registry::get(ns, name)
    }
}

#[inline]
fn normalize_stored_literal(value: LiteralValue) -> LiteralValue {
    match value {
        // Public contract: store numerics as Number(f64).
        LiteralValue::Int(i) => LiteralValue::Number(i as f64),
        other => other,
    }
}

pub use editor::change_log::{ChangeEvent, ChangeLog};

// ChangeEvent is now imported from change_log module

/// 🔮 Scalability Hook: Dependency reference types for range compression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DependencyRef {
    /// A specific cell dependency
    Cell(VertexId),
    /// A dependency on a finite, rectangular range
    Range {
        sheet: String,
        start_row: u32,
        start_col: u32,
        end_row: u32, // Inclusive
        end_col: u32, // Inclusive
    },
    /// A whole column dependency (A:A) - future range compression
    WholeColumn { sheet: String, col: u32 },
    /// A whole row dependency (1:1) - future range compression  
    WholeRow { sheet: String, row: u32 },
}

/// A key representing a coarse-grained section of a sheet
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct StripeKey {
    pub sheet_id: SheetId,
    pub stripe_type: StripeType,
    pub index: u32, // The index of the row, column, or block stripe
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum StripeType {
    Row,
    Column,
    Block, // For dense, square-like ranges
}

/// Block stripe indexing mathematics
const BLOCK_H: u32 = 256;
const BLOCK_W: u32 = 256;

pub fn block_index(row: u32, col: u32) -> u32 {
    (row / BLOCK_H) << 16 | (col / BLOCK_W)
}

/// A summary of the results of a mutating operation on the graph.
/// This serves as a "changelog" to the application layer.
#[derive(Debug, Clone)]
pub struct OperationSummary {
    /// Vertices whose values have been directly or indirectly affected.
    pub affected_vertices: Vec<VertexId>,
    /// Placeholder cells that were newly created to satisfy dependencies.
    pub created_placeholders: Vec<CellRef>,
}

/// Read-only dependency graph counters used by benchmark/instrumentation tooling.
///
/// These counters are deliberately observational: collecting them must not mutate graph state or
/// alter formula evaluation semantics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphBaselineStats {
    pub graph_vertex_count: usize,
    pub graph_formula_vertex_count: usize,
    pub graph_edge_count: usize,
    pub dirty_vertex_count: usize,
    pub evaluation_vertex_count: usize,
    pub formula_ast_root_count: usize,
    pub formula_ast_node_count: usize,
}

/// SoA-based dependency graph implementation
#[derive(Debug)]
pub struct DependencyGraph {
    // Core columnar storage
    store: VertexStore,

    // Edge storage with delta slab
    edges: CsrMutableEdges,

    // Arena-based value and formula storage
    data_store: DataStore,
    vertex_values: FxHashMap<VertexId, ValueRef>,
    vertex_formulas: FxHashMap<VertexId, AstNodeId>,

    /// Gate for storing grid-backed (cell/formula) LiteralValue payloads inside the dependency graph.
    ///
    /// When `false` (Arrow-canonical mode), the graph does not store values for cell/formula
    /// vertices. Arrow (base + overlays) is the sole value store for sheet cells.
    value_cache_enabled: bool,

    /// Debug-only instrumentation: count attempts to read *cell/formula* graph values while
    /// caching is disabled (canonical mode guard).
    #[cfg(debug_assertions)]
    graph_value_read_attempts: AtomicU64,

    // Address mappings using a hasher tuned for packed Coord / PackedSheetCell
    // keys. FxHasher's weak avalanche produces O(N^2) collision cascades on
    // row-major bulk ingest; CoordBuildHasher keeps these strictly O(N).
    cell_to_vertex: std::collections::HashMap<CellRef, VertexId, CoordBuildHasher>,
    load_packed_to_vertex: std::collections::HashMap<PackedSheetCell, VertexId, CoordBuildHasher>,

    // Scheduling state - using HashSet for O(1) operations
    dirty_vertices: FxHashSet<VertexId>,
    volatile_vertices: FxHashSet<VertexId>,

    /// Vertices explicitly marked as #REF! by structural operations.
    ///
    /// In Arrow-truth mode, the dependency graph does not cache cell/formula values.
    /// We still need a place to record deterministic #REF! invalidations for editor
    /// operations and structural transforms.
    ref_error_vertices: FxHashSet<VertexId>,

    // NEW: Specialized managers for range dependencies (Hybrid Model)
    /// Maps a formula vertex to the ranges it depends on.
    formula_to_range_deps: FxHashMap<VertexId, Vec<SharedRangeRef<'static>>>,

    /// Maps a stripe to formulas that depend on it via a compressed range.
    /// CRITICAL: VertexIds are deduplicated within each stripe to avoid quadratic blow-ups.
    stripe_to_dependents: FxHashMap<StripeKey, FxHashSet<VertexId>>,

    // Sheet-level sparse indexes for O(log n + k) range queries
    /// Maps sheet_id to its interval tree index for efficient row/column operations
    sheet_indexes: FxHashMap<SheetId, SheetIndex>,

    // Sheet name/ID mapping
    sheet_reg: SheetRegistry,
    default_sheet_id: SheetId,

    // Named ranges support
    /// Workbook-scoped named ranges
    named_ranges: FxHashMap<String, NamedRange>,

    /// Normalized-key lookup for workbook-scoped names.
    ///
    /// When `config.case_sensitive_names == false`, keys are ASCII-lowercased.
    /// Values are the canonical (original-cased) name stored in `named_ranges`.
    named_ranges_lookup: FxHashMap<String, String>,

    /// Sheet-scoped named ranges  
    sheet_named_ranges: FxHashMap<(SheetId, String), NamedRange>,

    /// Normalized-key lookup for sheet-scoped names.
    ///
    /// Key is (SheetId, normalized_name_key). Value is the canonical (original-cased)
    /// name stored in `sheet_named_ranges`.
    sheet_named_ranges_lookup: FxHashMap<(SheetId, String), String>,

    /// Reverse mapping: vertex -> names it uses (by vertex id)
    vertex_to_names: FxHashMap<VertexId, Vec<VertexId>>,

    /// Lookup for name vertex -> (scope, name) to avoid map scans
    name_vertex_lookup: FxHashMap<VertexId, (NameScope, String)>,

    /// Pending formula vertices referencing unresolved bare symbolic names.
    ///
    /// Keys are normalized through `name_lookup_key(...)` so workbook names and
    /// source scalars can both wake the same waiting formulas when a symbol appears.
    pending_name_links: FxHashMap<String, FxHashSet<(SheetId, VertexId)>>,

    /// Reverse mapping used to clear stale pending-name registrations when a
    /// formula is edited, overwritten with a value, or otherwise rebuilt.
    vertex_to_pending_names: FxHashMap<VertexId, FxHashSet<String>>,

    // Native workbook tables (ListObjects)
    tables: FxHashMap<String, tables::TableEntry>,
    /// Normalized-key lookup for tables.
    tables_lookup: FxHashMap<String, String>,
    table_vertex_lookup: FxHashMap<VertexId, String>,

    // External sources (SourceVertex)
    source_scalars: FxHashMap<String, sources::SourceScalarEntry>,
    source_tables: FxHashMap<String, sources::SourceTableEntry>,
    source_vertex_lookup: FxHashMap<VertexId, String>,

    /// Monotonic counter to assign synthetic coordinates to name vertices
    name_vertex_seq: u32,

    /// Monotonic counter to assign synthetic coordinates to source vertices
    source_vertex_seq: u32,

    /// Mapping from cell vertices to named range vertices that depend on them
    cell_to_name_dependents: FxHashMap<VertexId, FxHashSet<VertexId>>,
    /// Cached list of cell dependencies per named range vertex (for teardown)
    name_to_cell_dependencies: FxHashMap<VertexId, Vec<VertexId>>,

    // Evaluation configuration
    config: super::EvalConfig,

    // Graph-owned FormulaPlane authority shell. Inert until a later runtime cut-over.
    formula_authority: FormulaAuthority,

    // Dynamic topology orderer (Pearce–Kelly) maintained alongside edges when enabled
    pk_order: Option<DynamicTopo<VertexId>>,

    // Spill registry: anchor -> cells, and reverse mapping for blockers.
    // `spill_cell_to_anchor` is keyed by `CellRef` and uses the tuned hasher
    // for the same reason as `cell_to_vertex`.
    spill_anchor_to_cells: FxHashMap<VertexId, Vec<CellRef>>,
    spill_cell_to_anchor: std::collections::HashMap<CellRef, VertexId, CoordBuildHasher>,

    // Hint: during initial bulk load, many cells are guaranteed new; allow skipping existence checks per-sheet
    first_load_assume_new: bool,
    ensure_touched_sheets: FxHashSet<SheetId>,

    // handled deleted references, in case they are reintroduced.
    pub tombstone_registry: TombstoneRegistry,

    #[cfg(test)]
    instr: std::sync::Mutex<GraphInstrumentation>,
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl DependencyGraph {
    /// Expose range expansion limit for planners
    pub fn range_expansion_limit(&self) -> usize {
        self.config.range_expansion_limit
    }

    pub fn get_config(&self) -> &super::EvalConfig {
        &self.config
    }

    pub(crate) fn formula_authority(&self) -> &FormulaAuthority {
        &self.formula_authority
    }

    pub(crate) fn formula_authority_mut(&mut self) -> &mut FormulaAuthority {
        &mut self.formula_authority
    }

    /// Return read-only baseline counters for FormulaPlane/dispatch benchmarking.
    pub fn baseline_stats(&self) -> GraphBaselineStats {
        let data_stats = self.data_store.memory_usage();
        GraphBaselineStats {
            graph_vertex_count: self.store.len(),
            graph_formula_vertex_count: self.vertex_formulas.len(),
            graph_edge_count: self.edges.num_edges_exact(),
            dirty_vertex_count: self.dirty_vertices.len(),
            evaluation_vertex_count: self.get_evaluation_vertices().len(),
            formula_ast_root_count: self.vertex_formulas.len(),
            formula_ast_node_count: data_stats.total_ast_nodes,
        }
    }

    #[inline]
    pub(crate) fn value_cache_enabled(&self) -> bool {
        self.value_cache_enabled
    }

    /// Debug-only: how many times `get_value`/`get_cell_value` were called while caching is disabled.
    ///
    /// In Arrow-canonical mode this should remain 0 for engine/interpreter reads.
    #[cfg(test)]
    pub fn debug_graph_value_read_attempts(&self) -> u64 {
        #[cfg(debug_assertions)]
        {
            self.graph_value_read_attempts.load(Ordering::Relaxed)
        }
        #[cfg(not(debug_assertions))]
        {
            0
        }
    }

    /// Build a dependency plan for a set of formulas on sheets
    pub fn plan_dependencies<'a, I>(
        &mut self,
        items: I,
        policy: &formualizer_parse::parser::CollectPolicy,
        volatile: Option<&[bool]>,
    ) -> Result<crate::engine::plan::DependencyPlan, formualizer_common::ExcelError>
    where
        I: IntoIterator<Item = (&'a str, u32, u32, &'a formualizer_parse::parser::ASTNode)>,
    {
        crate::engine::plan::build_dependency_plan(
            &mut self.sheet_reg,
            items.into_iter(),
            policy,
            volatile,
        )
    }

    pub fn plan_dependencies_mixed<'a, I>(
        &mut self,
        items: I,
        policy: &formualizer_parse::parser::CollectPolicy,
        volatile: Option<&[bool]>,
    ) -> Result<crate::engine::plan::DependencyPlan, formualizer_common::ExcelError>
    where
        I: IntoIterator<
            Item = (
                &'a str,
                u32,
                u32,
                crate::engine::plan::DependencyPlanAst<'a>,
            ),
        >,
    {
        crate::engine::plan::build_dependency_plan_mixed(
            &mut self.sheet_reg,
            &self.data_store,
            items.into_iter(),
            policy,
            volatile,
        )
    }

    /// Ensure vertices exist for given coords; allocate missing in contiguous batches and add to edges/index.
    /// Returns a list suitable for edges.add_vertices_batch.
    pub fn ensure_vertices_batch(
        &mut self,
        coords: &[(SheetId, AbsCoord)],
    ) -> Vec<(AbsCoord, u32)> {
        self.ensure_vertices_batch_ordered(coords).1
    }

    /// Ensure vertices exist for given packed absolute cells and return vertex ids aligned to the
    /// input order, plus the newly allocated `(coord, raw_vid)` items suitable for edge/index
    /// population.
    pub fn ensure_vertices_batch_packed_ordered(
        &mut self,
        packed_cells: &[PackedSheetCell],
    ) -> (Vec<VertexId>, Vec<(AbsCoord, u32)>) {
        #[cfg(feature = "perf_instrumentation")]
        use crate::instant::FzInstant as PerfInstant;
        use rustc_hash::FxHashMap;

        #[cfg(feature = "perf_instrumentation")]
        let debug = std::env::var("FZ_DEBUG_LOAD")
            .ok()
            .is_some_and(|v| v != "0");
        #[cfg(feature = "perf_instrumentation")]
        let t0 = PerfInstant::now();

        let mut ordered: Vec<Option<VertexId>> = vec![None; packed_cells.len()];
        if packed_cells.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let first_sid = packed_cells[0].sheet_id();
        let single_sheet = packed_cells.iter().all(|cell| cell.sheet_id() == first_sid);
        let mut add_batch: Vec<(AbsCoord, u32)> = Vec::new();

        #[cfg(feature = "perf_instrumentation")]
        let mut packed_hits = 0usize;
        #[cfg(feature = "perf_instrumentation")]
        let mut generic_hits = 0usize;
        #[cfg(feature = "perf_instrumentation")]
        let mut missing = 0usize;
        #[cfg(feature = "perf_instrumentation")]
        let mut t_packed_lookup_us = 0u128;
        #[cfg(feature = "perf_instrumentation")]
        let mut t_generic_lookup_us = 0u128;
        #[cfg(feature = "perf_instrumentation")]
        let mut t_alloc_us = 0u128;
        #[cfg(feature = "perf_instrumentation")]
        let mut t_map_insert_us = 0u128;
        #[cfg(feature = "perf_instrumentation")]
        let mut t_index_insert_us = 0u128;
        #[cfg(feature = "perf_instrumentation")]
        let mut t_edge_register_us = 0u128;

        if single_sheet {
            let sid = first_sid;
            let mut missing_items: Vec<(usize, PackedSheetCell)> =
                Vec::with_capacity(packed_cells.len());

            for (idx, packed) in packed_cells.iter().copied().enumerate() {
                #[cfg(feature = "perf_instrumentation")]
                let tl0 = PerfInstant::now();
                if self.first_load_assume_new
                    && let Some(&existing) = self.load_packed_to_vertex.get(&packed)
                {
                    ordered[idx] = Some(existing);
                    #[cfg(feature = "perf_instrumentation")]
                    {
                        packed_hits += 1;
                        t_packed_lookup_us += tl0.elapsed().as_micros();
                    }
                    continue;
                }
                #[cfg(feature = "perf_instrumentation")]
                {
                    t_packed_lookup_us += tl0.elapsed().as_micros();
                }

                let pc = AbsCoord::new(packed.row0(), packed.col0());
                let addr = CellRef::new(sid, Coord::new(pc.row(), pc.col(), true, true));
                #[cfg(feature = "perf_instrumentation")]
                let tg0 = PerfInstant::now();
                if let Some(&existing) = self.cell_to_vertex.get(&addr) {
                    ordered[idx] = Some(existing);
                    if self.first_load_assume_new {
                        self.load_packed_to_vertex.insert(packed, existing);
                    }
                    #[cfg(feature = "perf_instrumentation")]
                    {
                        generic_hits += 1;
                    }
                } else {
                    missing_items.push((idx, packed));
                    #[cfg(feature = "perf_instrumentation")]
                    {
                        missing += 1;
                    }
                }
                #[cfg(feature = "perf_instrumentation")]
                {
                    t_generic_lookup_us += tg0.elapsed().as_micros();
                }
            }

            if !missing_items.is_empty() {
                self.ensure_touched_sheets.insert(sid);

                let mut pcs: Vec<AbsCoord> = Vec::with_capacity(missing_items.len());
                for (_, packed) in &missing_items {
                    pcs.push(AbsCoord::new(packed.row0(), packed.col0()));
                }

                #[cfg(feature = "perf_instrumentation")]
                let ta0 = PerfInstant::now();
                let vids = self.store.allocate_contiguous(sid, &pcs, 0x00);
                #[cfg(feature = "perf_instrumentation")]
                {
                    t_alloc_us += ta0.elapsed().as_micros();
                }
                add_batch.reserve(missing_items.len());

                match self.config.sheet_index_mode {
                    crate::engine::SheetIndexMode::Eager
                    | crate::engine::SheetIndexMode::FastBatch => {
                        for ((input_idx, packed), vid) in
                            missing_items.into_iter().zip(vids.into_iter())
                        {
                            let pc = AbsCoord::new(packed.row0(), packed.col0());
                            ordered[input_idx] = Some(vid);
                            add_batch.push((pc, vid.0));

                            #[cfg(feature = "perf_instrumentation")]
                            let tm0 = PerfInstant::now();
                            if self.first_load_assume_new {
                                self.load_packed_to_vertex.insert(packed, vid);
                            } else {
                                let addr =
                                    CellRef::new(sid, Coord::new(pc.row(), pc.col(), true, true));
                                self.cell_to_vertex.insert(addr, vid);
                            }
                            #[cfg(feature = "perf_instrumentation")]
                            {
                                t_map_insert_us += tm0.elapsed().as_micros();
                            }

                            #[cfg(feature = "perf_instrumentation")]
                            let ti0 = PerfInstant::now();
                            self.sheet_index_mut(sid).add_vertex(pc, vid);
                            #[cfg(feature = "perf_instrumentation")]
                            {
                                t_index_insert_us += ti0.elapsed().as_micros();
                            }
                        }
                    }
                    crate::engine::SheetIndexMode::Lazy => {
                        for ((input_idx, packed), vid) in
                            missing_items.into_iter().zip(vids.into_iter())
                        {
                            let pc = AbsCoord::new(packed.row0(), packed.col0());
                            ordered[input_idx] = Some(vid);
                            add_batch.push((pc, vid.0));

                            #[cfg(feature = "perf_instrumentation")]
                            let tm0 = PerfInstant::now();
                            if self.first_load_assume_new {
                                self.load_packed_to_vertex.insert(packed, vid);
                            } else {
                                let addr =
                                    CellRef::new(sid, Coord::new(pc.row(), pc.col(), true, true));
                                self.cell_to_vertex.insert(addr, vid);
                            }
                            #[cfg(feature = "perf_instrumentation")]
                            {
                                t_map_insert_us += tm0.elapsed().as_micros();
                            }
                        }
                    }
                }
            }
        } else {
            let mut grouped: FxHashMap<SheetId, Vec<(usize, PackedSheetCell)>> =
                FxHashMap::default();

            for (idx, packed) in packed_cells.iter().copied().enumerate() {
                #[cfg(feature = "perf_instrumentation")]
                let tl0 = PerfInstant::now();
                if self.first_load_assume_new
                    && let Some(&existing) = self.load_packed_to_vertex.get(&packed)
                {
                    ordered[idx] = Some(existing);
                    #[cfg(feature = "perf_instrumentation")]
                    {
                        packed_hits += 1;
                        t_packed_lookup_us += tl0.elapsed().as_micros();
                    }
                    continue;
                }
                #[cfg(feature = "perf_instrumentation")]
                {
                    t_packed_lookup_us += tl0.elapsed().as_micros();
                }

                let sid = packed.sheet_id();
                let pc = AbsCoord::new(packed.row0(), packed.col0());
                let addr = CellRef::new(sid, Coord::new(pc.row(), pc.col(), true, true));
                #[cfg(feature = "perf_instrumentation")]
                let tg0 = PerfInstant::now();
                if let Some(&existing) = self.cell_to_vertex.get(&addr) {
                    ordered[idx] = Some(existing);
                    if self.first_load_assume_new {
                        self.load_packed_to_vertex.insert(packed, existing);
                    }
                    #[cfg(feature = "perf_instrumentation")]
                    {
                        generic_hits += 1;
                    }
                } else {
                    grouped.entry(sid).or_default().push((idx, packed));
                    #[cfg(feature = "perf_instrumentation")]
                    {
                        missing += 1;
                    }
                }
                #[cfg(feature = "perf_instrumentation")]
                {
                    t_generic_lookup_us += tg0.elapsed().as_micros();
                }
            }

            for (sid, items) in grouped {
                if items.is_empty() {
                    continue;
                }
                self.ensure_touched_sheets.insert(sid);

                let mut pcs: Vec<AbsCoord> = Vec::with_capacity(items.len());
                for (_, packed) in &items {
                    pcs.push(AbsCoord::new(packed.row0(), packed.col0()));
                }

                #[cfg(feature = "perf_instrumentation")]
                let ta0 = PerfInstant::now();
                let vids = self.store.allocate_contiguous(sid, &pcs, 0x00);
                #[cfg(feature = "perf_instrumentation")]
                {
                    t_alloc_us += ta0.elapsed().as_micros();
                }

                for ((input_idx, packed), vid) in items.into_iter().zip(vids.into_iter()) {
                    let pc = AbsCoord::new(packed.row0(), packed.col0());
                    ordered[input_idx] = Some(vid);
                    add_batch.push((pc, vid.0));

                    #[cfg(feature = "perf_instrumentation")]
                    let tm0 = PerfInstant::now();
                    if self.first_load_assume_new {
                        self.load_packed_to_vertex.insert(packed, vid);
                    } else {
                        let addr = CellRef::new(sid, Coord::new(pc.row(), pc.col(), true, true));
                        self.cell_to_vertex.insert(addr, vid);
                    }
                    #[cfg(feature = "perf_instrumentation")]
                    {
                        t_map_insert_us += tm0.elapsed().as_micros();
                    }

                    match self.config.sheet_index_mode {
                        crate::engine::SheetIndexMode::Eager
                        | crate::engine::SheetIndexMode::FastBatch => {
                            #[cfg(feature = "perf_instrumentation")]
                            let ti0 = PerfInstant::now();
                            self.sheet_index_mut(sid).add_vertex(pc, vid);
                            #[cfg(feature = "perf_instrumentation")]
                            {
                                t_index_insert_us += ti0.elapsed().as_micros();
                            }
                        }
                        crate::engine::SheetIndexMode::Lazy => {
                            // defer index build
                        }
                    }
                }
            }
        }

        if !add_batch.is_empty() {
            #[cfg(feature = "perf_instrumentation")]
            let te0 = PerfInstant::now();
            self.edges.add_vertices_batch(&add_batch);
            #[cfg(feature = "perf_instrumentation")]
            {
                t_edge_register_us += te0.elapsed().as_micros();
            }
        }

        #[cfg(feature = "perf_instrumentation")]
        if debug {
            eprintln!(
                "[fz][ensure] cells={} single_sheet={} packed_hits={} generic_hits={} missing={} packed_lookup={}us generic_lookup={}us alloc={}us map_insert={}us index_insert={}us edge_register={}us total={}ms",
                packed_cells.len(),
                single_sheet,
                packed_hits,
                generic_hits,
                missing,
                t_packed_lookup_us,
                t_generic_lookup_us,
                t_alloc_us,
                t_map_insert_us,
                t_index_insert_us,
                t_edge_register_us,
                t0.elapsed().as_millis(),
            );
        }

        let ordered = ordered
            .into_iter()
            .map(|vid| vid.expect("ensure_vertices_batch_packed_ordered must resolve every coord"))
            .collect();
        (ordered, add_batch)
    }

    /// Ensure vertices exist for given coords and return vertex ids aligned to the input order,
    /// plus the newly allocated `(coord, raw_vid)` items suitable for edge/index population.
    pub fn ensure_vertices_batch_ordered(
        &mut self,
        coords: &[(SheetId, AbsCoord)],
    ) -> (Vec<VertexId>, Vec<(AbsCoord, u32)>) {
        let mut packed: Vec<PackedSheetCell> = Vec::with_capacity(coords.len());
        for &(sid, coord) in coords {
            packed.push(Self::packed_cell_key(sid, coord));
        }
        self.ensure_vertices_batch_packed_ordered(&packed)
    }

    #[inline]
    fn packed_cell_key(sheet_id: SheetId, coord: AbsCoord) -> PackedSheetCell {
        PackedSheetCell::try_new(sheet_id, coord.row(), coord.col())
            .expect("graph coordinate must fit PackedSheetCell")
    }

    fn flush_load_packed_mappings(&mut self) {
        if self.load_packed_to_vertex.is_empty() {
            return;
        }
        let debug = std::env::var("FZ_DEBUG_LOAD")
            .ok()
            .is_some_and(|v| v != "0");
        let t0 = crate::instant::FzInstant::now();
        let count = self.load_packed_to_vertex.len();
        self.cell_to_vertex.reserve(count);
        for (&packed, &vid) in &self.load_packed_to_vertex {
            let coord = AbsCoord::new(packed.row0(), packed.col0());
            let addr = CellRef::new(
                packed.sheet_id(),
                Coord::new(coord.row(), coord.col(), true, true),
            );
            self.cell_to_vertex.insert(addr, vid);
        }
        self.load_packed_to_vertex.clear();
        if debug {
            eprintln!(
                "[fz][load] flush_load_packed_mappings: {} entries in {:.1} ms",
                count,
                t0.elapsed().as_secs_f64() * 1000.0,
            );
        }
    }

    /// Enable/disable the first-load fast path for value inserts.
    pub fn set_first_load_assume_new(&mut self, enabled: bool) {
        if self.first_load_assume_new && !enabled {
            self.flush_load_packed_mappings();
        } else if enabled {
            self.load_packed_to_vertex.clear();
        }
        self.first_load_assume_new = enabled;
    }

    pub fn first_load_assume_new(&self) -> bool {
        self.first_load_assume_new
    }

    /// Reset the per-sheet ensure touch tracking.
    pub fn reset_ensure_touched(&mut self) {
        self.ensure_touched_sheets.clear();
    }

    /// Store an AST and return its arena id.
    pub fn store_ast(&mut self, ast: &formualizer_parse::parser::ASTNode) -> AstNodeId {
        self.data_store.store_ast(ast, &self.sheet_reg)
    }

    /// Store ASTs in batch and return their arena ids
    pub fn store_asts_batch<'a, I>(&mut self, asts: I) -> Vec<AstNodeId>
    where
        I: IntoIterator<Item = &'a formualizer_parse::parser::ASTNode>,
    {
        self.data_store.store_asts_batch(asts, &self.sheet_reg)
    }

    /// Reserve metadata structures for upcoming formula assignments during bulk load.
    pub fn reserve_formula_metadata(&mut self, additional: usize) {
        self.vertex_formulas.reserve(additional);
        self.dirty_vertices.reserve(additional);
        self.volatile_vertices.reserve(additional);
    }

    /// Lookup VertexId for a (SheetId, AbsCoord)
    pub fn vid_for_sid_pc(&self, sid: SheetId, pc: AbsCoord) -> Option<VertexId> {
        let addr = CellRef::new(sid, Coord::new(pc.row(), pc.col(), true, true));
        self.cell_to_vertex.get(&addr).copied()
    }

    /// Helper to map a global cell index in a plan to a VertexId
    pub fn vid_for_plan_idx(
        &self,
        plan: &crate::engine::plan::DependencyPlan,
        idx: u32,
    ) -> Option<VertexId> {
        let (sid, pc) = plan.global_cells.get(idx as usize).copied()?;
        self.vid_for_sid_pc(sid, pc)
    }
    /// Assign a formula to an existing vertex, removing prior edges and setting flags
    pub fn assign_formula_vertex(
        &mut self,
        vid: VertexId,
        ast_id: AstNodeId,
        volatile: bool,
        dynamic: bool,
    ) {
        if self.vertex_formulas.contains_key(&vid) {
            self.remove_dependent_edges(vid);
        }
        self.store
            .set_kind(vid, crate::engine::vertex::VertexKind::FormulaScalar);
        self.vertex_values.remove(&vid);
        self.vertex_formulas.insert(vid, ast_id);
        self.mark_volatile(vid, volatile);
        self.store.set_dynamic(vid, dynamic);

        // schedule evaluation
        self.mark_vertex_dirty(vid);
    }

    /// Fast path for initial workbook load: assign a formula to a vertex that is known not to
    /// already own dependency edges in the graph. Dirtiness is batched separately.
    pub fn assign_formula_vertex_load_fast(
        &mut self,
        vid: VertexId,
        ast_id: AstNodeId,
        volatile: bool,
        dynamic: bool,
    ) {
        debug_assert!(
            !self.vertex_formulas.contains_key(&vid),
            "load-fast formula assignment expects fresh/non-formula vertices"
        );
        self.store
            .set_kind(vid, crate::engine::vertex::VertexKind::FormulaScalar);
        self.vertex_values.remove(&vid);
        self.vertex_formulas.insert(vid, ast_id);
        self.mark_volatile(vid, volatile);
        self.store.set_dynamic(vid, dynamic);
    }

    /// Public wrapper for adding edges without beginning a batch (caller manages batch)
    pub fn add_edges_nobatch(&mut self, dependent: VertexId, dependencies: &[VertexId]) {
        self.add_dependent_edges_nobatch(dependent, dependencies);
    }

    /// Iterate all normal vertex ids
    pub fn iter_vertex_ids(&self) -> impl Iterator<Item = VertexId> + '_ {
        self.store.all_vertices()
    }

    /// Get current AbsCoord for a vertex
    pub fn vertex_coord(&self, vid: VertexId) -> AbsCoord {
        self.store.coord(vid)
    }

    /// Total number of allocated vertices (including deleted)
    pub fn vertex_count(&self) -> usize {
        self.store.len()
    }

    /// Replace CSR edges in one shot from adjacency and coords
    pub fn build_edges_from_adjacency(
        &mut self,
        adjacency: Vec<(u32, Vec<u32>)>,
        coords: Vec<AbsCoord>,
        vertex_ids: Vec<u32>,
    ) {
        self.edges
            .build_from_adjacency(adjacency, coords, vertex_ids);
    }
    /// Compute min/max used row among vertices within [start_col..=end_col] on a sheet.
    pub fn used_row_bounds_for_columns(
        &self,
        sheet_id: SheetId,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        // Prefer sheet index when available
        if let Some(index) = self.sheet_indexes.get(&sheet_id)
            && !index.is_empty()
        {
            let mut min_r: Option<u32> = None;
            let mut max_r: Option<u32> = None;
            for vid in index.vertices_in_col_range(start_col, end_col) {
                let r = self.store.coord(vid).row();
                min_r = Some(min_r.map(|m| m.min(r)).unwrap_or(r));
                max_r = Some(max_r.map(|m| m.max(r)).unwrap_or(r));
            }
            return match (min_r, max_r) {
                (Some(a), Some(b)) => Some((a, b)),
                _ => None,
            };
        }
        // Fallback: scan cell maps on the fly
        let mut min_r: Option<u32> = None;
        let mut max_r: Option<u32> = None;
        for cref in self.cell_to_vertex.keys() {
            if cref.sheet_id == sheet_id {
                let c = cref.coord.col();
                if c >= start_col && c <= end_col {
                    let r = cref.coord.row();
                    min_r = Some(min_r.map(|m| m.min(r)).unwrap_or(r));
                    max_r = Some(max_r.map(|m| m.max(r)).unwrap_or(r));
                }
            }
        }
        for packed in self.load_packed_to_vertex.keys() {
            if packed.sheet_id() == sheet_id {
                let c = packed.col0();
                if c >= start_col && c <= end_col {
                    let r = packed.row0();
                    min_r = Some(min_r.map(|m| m.min(r)).unwrap_or(r));
                    max_r = Some(max_r.map(|m| m.max(r)).unwrap_or(r));
                }
            }
        }
        match (min_r, max_r) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        }
    }

    /// Build (or rebuild) the sheet index for a given sheet if running in Lazy mode.
    pub fn finalize_sheet_index(&mut self, sheet: &str) {
        let Some(sheet_id) = self.sheet_reg.get_id(sheet) else {
            return;
        };
        // If already present and non-empty, skip
        if let Some(idx) = self.sheet_indexes.get(&sheet_id)
            && !idx.is_empty()
        {
            return;
        }
        let mut idx = SheetIndex::new();
        // Collect coords for this sheet
        let mut batch: Vec<(AbsCoord, VertexId)> =
            Vec::with_capacity(self.cell_to_vertex.len() + self.load_packed_to_vertex.len());
        for (cref, vid) in &self.cell_to_vertex {
            if cref.sheet_id == sheet_id {
                batch.push((AbsCoord::new(cref.coord.row(), cref.coord.col()), *vid));
            }
        }
        for (&packed, &vid) in &self.load_packed_to_vertex {
            if packed.sheet_id() != sheet_id {
                continue;
            }
            let coord = AbsCoord::new(packed.row0(), packed.col0());
            let addr = CellRef::new(sheet_id, Coord::new(coord.row(), coord.col(), true, true));
            if self.cell_to_vertex.contains_key(&addr) {
                continue;
            }
            batch.push((coord, vid));
        }
        // Use batch builder
        idx.add_vertices_batch(&batch);
        self.sheet_indexes.insert(sheet_id, idx);
    }

    pub fn set_sheet_index_mode(&mut self, mode: crate::engine::SheetIndexMode) {
        self.config.sheet_index_mode = mode;
    }

    /// Compute min/max used column among vertices within [start_row..=end_row] on a sheet.
    pub fn used_col_bounds_for_rows(
        &self,
        sheet_id: SheetId,
        start_row: u32,
        end_row: u32,
    ) -> Option<(u32, u32)> {
        if let Some(index) = self.sheet_indexes.get(&sheet_id)
            && !index.is_empty()
        {
            let mut min_c: Option<u32> = None;
            let mut max_c: Option<u32> = None;
            for vid in index.vertices_in_row_range(start_row, end_row) {
                let c = self.store.coord(vid).col();
                min_c = Some(min_c.map(|m| m.min(c)).unwrap_or(c));
                max_c = Some(max_c.map(|m| m.max(c)).unwrap_or(c));
            }
            return match (min_c, max_c) {
                (Some(a), Some(b)) => Some((a, b)),
                _ => None,
            };
        }
        // Fallback: scan cell maps on the fly
        let mut min_c: Option<u32> = None;
        let mut max_c: Option<u32> = None;
        for cref in self.cell_to_vertex.keys() {
            if cref.sheet_id == sheet_id {
                let r = cref.coord.row();
                if r >= start_row && r <= end_row {
                    let c = cref.coord.col();
                    min_c = Some(min_c.map(|m| m.min(c)).unwrap_or(c));
                    max_c = Some(max_c.map(|m| m.max(c)).unwrap_or(c));
                }
            }
        }
        for packed in self.load_packed_to_vertex.keys() {
            if packed.sheet_id() == sheet_id {
                let r = packed.row0();
                if r >= start_row && r <= end_row {
                    let c = packed.col0();
                    min_c = Some(min_c.map(|m| m.min(c)).unwrap_or(c));
                    max_c = Some(max_c.map(|m| m.max(c)).unwrap_or(c));
                }
            }
        }
        match (min_c, max_c) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        }
    }

    /// Returns true if the given sheet currently contains any formula vertices.
    pub fn sheet_has_formulas(&self, sheet_id: SheetId) -> bool {
        // Check vertex_formulas keys; they represent formula vertices
        for &vid in self.vertex_formulas.keys() {
            if self.store.sheet_id(vid) == sheet_id {
                return true;
            }
        }
        false
    }
    pub fn new() -> Self {
        Self::new_with_config(super::EvalConfig::default())
    }

    pub fn new_with_config(config: super::EvalConfig) -> Self {
        let mut sheet_reg = SheetRegistry::new();
        let default_sheet_id = sheet_reg.id_for(&config.default_sheet_name);

        let mut g = Self {
            store: VertexStore::new(),
            edges: CsrMutableEdges::new(),
            data_store: DataStore::new(),
            vertex_values: FxHashMap::default(),
            vertex_formulas: FxHashMap::default(),
            // Phase 1 (ticket 610): Arrow-truth is the only supported mode.
            // The dependency graph does not cache cell/formula literal payloads.
            value_cache_enabled: false,
            #[cfg(debug_assertions)]
            graph_value_read_attempts: AtomicU64::new(0),
            cell_to_vertex: std::collections::HashMap::with_hasher(CoordBuildHasher),
            load_packed_to_vertex: std::collections::HashMap::with_hasher(CoordBuildHasher),
            dirty_vertices: FxHashSet::default(),
            volatile_vertices: FxHashSet::default(),
            ref_error_vertices: FxHashSet::default(),
            formula_to_range_deps: FxHashMap::default(),
            stripe_to_dependents: FxHashMap::default(),
            sheet_indexes: FxHashMap::default(),
            sheet_reg,
            default_sheet_id,
            named_ranges: FxHashMap::default(),
            named_ranges_lookup: FxHashMap::default(),
            sheet_named_ranges: FxHashMap::default(),
            sheet_named_ranges_lookup: FxHashMap::default(),
            vertex_to_names: FxHashMap::default(),
            name_vertex_lookup: FxHashMap::default(),
            pending_name_links: FxHashMap::default(),
            vertex_to_pending_names: FxHashMap::default(),
            tables: FxHashMap::default(),
            tables_lookup: FxHashMap::default(),
            table_vertex_lookup: FxHashMap::default(),
            source_scalars: FxHashMap::default(),
            source_tables: FxHashMap::default(),
            source_vertex_lookup: FxHashMap::default(),
            name_vertex_seq: 0,
            source_vertex_seq: 0,
            cell_to_name_dependents: FxHashMap::default(),
            name_to_cell_dependencies: FxHashMap::default(),
            config: config.clone(),
            formula_authority: FormulaAuthority::default(),
            pk_order: None,
            spill_anchor_to_cells: FxHashMap::default(),
            spill_cell_to_anchor: std::collections::HashMap::with_hasher(CoordBuildHasher),
            first_load_assume_new: false,
            ensure_touched_sheets: FxHashSet::default(),
            tombstone_registry: TombstoneRegistry::default(),
            #[cfg(test)]
            instr: std::sync::Mutex::new(GraphInstrumentation::default()),
        };

        if config.use_dynamic_topo {
            // Seed with currently active vertices (likely empty at startup)
            let nodes = g
                .store
                .all_vertices()
                .filter(|&id| g.store.vertex_exists_active(id));
            let mut pk = DynamicTopo::new(
                nodes,
                PkConfig {
                    visit_budget: config.pk_visit_budget,
                    compaction_interval_ops: config.pk_compaction_interval_ops,
                },
            );
            // Build an initial order using current graph
            let adapter = GraphAdapter { g: &g };
            pk.rebuild_full(&adapter);
            g.pk_order = Some(pk);
        }

        g
    }

    /// When dynamic topology is enabled, compute layers for a subset using PK ordering.
    pub(crate) fn pk_layers_for(&self, subset: &[VertexId]) -> Option<Vec<crate::engine::Layer>> {
        let pk = self.pk_order.as_ref()?;
        let adapter = crate::engine::topo::GraphAdapter { g: self };
        let layers = pk.layers_for(&adapter, subset, self.config.max_layer_width);
        Some(
            layers
                .into_iter()
                .map(|vs| crate::engine::Layer { vertices: vs })
                .collect(),
        )
    }

    #[inline]
    pub(crate) fn dynamic_topo_enabled(&self) -> bool {
        self.pk_order.is_some()
    }

    #[cfg(test)]
    pub fn reset_instr(&mut self) {
        if let Ok(mut g) = self.instr.lock() {
            *g = GraphInstrumentation::default();
        }
    }

    #[cfg(test)]
    pub fn instr(&self) -> GraphInstrumentation {
        self.instr.lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// Begin batch operations - defer CSR rebuilds until end_batch() is called
    pub fn begin_batch(&mut self) {
        self.edges.begin_batch();
    }

    /// End batch operations and trigger CSR rebuild if needed
    pub fn end_batch(&mut self) {
        self.edges.end_batch();
    }

    pub fn default_sheet_id(&self) -> SheetId {
        self.default_sheet_id
    }

    pub fn default_sheet_name(&self) -> &str {
        self.sheet_reg.name(self.default_sheet_id)
    }

    pub fn set_default_sheet_by_name(&mut self, name: &str) {
        self.default_sheet_id = self.sheet_id_mut(name);
    }

    pub fn set_default_sheet_by_id(&mut self, id: SheetId) {
        self.default_sheet_id = id;
    }

    /// Returns the ID for a sheet name, creating one if it doesn't exist.
    pub fn sheet_id_mut(&mut self, name: &str) -> SheetId {
        self.sheet_reg.id_for(name)
    }

    pub fn sheet_id(&self, name: &str) -> Option<SheetId> {
        self.sheet_reg.get_id(name)
    }

    /// Resolve a sheet name to an existing ID or return a #REF! error.
    fn resolve_existing_sheet_id(&self, name: &str) -> Result<SheetId, ExcelError> {
        self.sheet_id(name).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Ref).with_message(format!("Sheet not found: {name}"))
        })
    }

    /// Returns the name of a sheet given its ID.
    pub fn sheet_name(&self, id: SheetId) -> &str {
        self.sheet_reg.name(id)
    }

    /// Access the sheet registry (read-only) for external bindings
    pub fn sheet_reg(&self) -> &SheetRegistry {
        &self.sheet_reg
    }

    pub(crate) fn data_store(&self) -> &DataStore {
        &self.data_store
    }

    pub(crate) fn make_ingest_pipeline<'a>(
        &'a mut self,
        function_provider: &'a dyn crate::traits::FunctionProvider,
        policy: formualizer_parse::parser::CollectPolicy,
    ) -> crate::engine::ingest_pipeline::IngestPipeline<'a> {
        use crate::engine::ingest_pipeline::{
            NameRegistryView, NamedEntryRef, SourceEntryRef, SourceRegistryView,
            TableEntrySnapshot, TableRegistryView,
        };

        let DependencyGraph {
            data_store,
            sheet_reg,
            named_ranges,
            named_ranges_lookup,
            sheet_named_ranges,
            sheet_named_ranges_lookup,
            tables,
            tables_lookup,
            source_scalars,
            source_tables,
            config,
            ..
        } = self;

        let case_sensitive_names = config.case_sensitive_names;
        let names = NameRegistryView::new(move |name, current_sheet| {
            let found = if case_sensitive_names {
                sheet_named_ranges
                    .get(&(current_sheet, name.to_string()))
                    .or_else(|| named_ranges.get(name))
            } else {
                let key = name.to_lowercase();
                sheet_named_ranges_lookup
                    .get(&(current_sheet, key.clone()))
                    .and_then(|canon| sheet_named_ranges.get(&(current_sheet, canon.clone())))
                    .or_else(|| {
                        named_ranges_lookup
                            .get(&key)
                            .and_then(|canon| named_ranges.get(canon))
                    })
            };
            found.map(|entry| NamedEntryRef {
                vertex: entry.vertex,
            })
        });

        let case_sensitive_tables = config.case_sensitive_tables;
        let tables_ref = &*tables;
        let tables_lookup_ref = &*tables_lookup;
        let snapshot_table = |entry: &tables::TableEntry| TableEntrySnapshot {
            name: entry.name.clone(),
            range: entry.range,
            header_row: entry.header_row,
            headers: entry.headers.clone(),
            vertex: entry.vertex,
        };
        let tables_view = TableRegistryView::new(
            move |name| {
                if case_sensitive_tables {
                    tables_ref.get(name).map(snapshot_table)
                } else {
                    let key = name.to_lowercase();
                    tables_lookup_ref
                        .get(&key)
                        .and_then(|canon| tables_ref.get(canon))
                        .map(snapshot_table)
                }
            },
            move |cell| {
                let row0 = cell.coord.row();
                let col0 = cell.coord.col();
                let mut best: Option<&tables::TableEntry> = None;
                let mut best_area = u64::MAX;
                let mut best_name = "";
                for table in tables_ref.values() {
                    if table.sheet_id() != cell.sheet_id {
                        continue;
                    }
                    let sr0 = table.range.start.coord.row();
                    let sc0 = table.range.start.coord.col();
                    let er0 = table.range.end.coord.row();
                    let ec0 = table.range.end.coord.col();
                    if row0 < sr0 || row0 > er0 || col0 < sc0 || col0 > ec0 {
                        continue;
                    }
                    let area = ((er0 - sr0 + 1) as u64).saturating_mul((ec0 - sc0 + 1) as u64);
                    let name = table.name.as_str();
                    if best.is_none() || area < best_area || (area == best_area && name < best_name)
                    {
                        best = Some(table);
                        best_area = area;
                        best_name = name;
                    }
                }
                best.map(snapshot_table)
            },
        );

        let sources = SourceRegistryView::new(
            move |name| {
                source_scalars.get(name).map(|entry| SourceEntryRef {
                    vertex: entry.vertex,
                })
            },
            move |name| {
                source_tables.get(name).map(|entry| SourceEntryRef {
                    vertex: entry.vertex,
                })
            },
        );

        crate::engine::ingest_pipeline::IngestPipeline::new(
            data_store,
            sheet_reg,
            names,
            tables_view,
            sources,
            function_provider,
            policy,
        )
    }

    /// Converts a `CellRef` to a fully qualified A1-style string (e.g., "SheetName!A1").
    pub fn to_a1(&self, cell_ref: CellRef) -> String {
        format!("{}!{}", self.sheet_name(cell_ref.sheet_id), cell_ref.coord)
    }

    pub(crate) fn vertex_len(&self) -> usize {
        self.store.len()
    }

    /// Get mutable access to a sheet's index, creating it if it doesn't exist
    /// This is the primary way VertexEditor and internal operations access the index
    pub fn sheet_index_mut(&mut self, sheet_id: SheetId) -> &mut SheetIndex {
        self.sheet_indexes.entry(sheet_id).or_default()
    }

    /// Get immutable access to a sheet's index, returns None if not initialized
    pub fn sheet_index(&self, sheet_id: SheetId) -> Option<&SheetIndex> {
        self.sheet_indexes.get(&sheet_id)
    }

    /// Set a value in a cell, returns affected vertex IDs
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<OperationSummary, ExcelError> {
        let value = normalize_stored_literal(value);
        let sheet_id = self.sheet_id_mut(sheet);
        // External API is 1-based; store 0-based coords internally.
        let coord = Coord::from_excel(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);
        let mut created_placeholders = Vec::new();

        let vertex_id = if let Some(&existing_id) = self.cell_to_vertex.get(&addr) {
            // Check if it was a formula and remove dependencies
            let is_formula = matches!(
                self.store.kind(existing_id),
                VertexKind::FormulaScalar | VertexKind::FormulaArray
            );

            if is_formula {
                self.remove_dependent_edges(existing_id);
                self.detach_vertex_from_names(existing_id);
                self.clear_pending_name_references(existing_id);
                self.vertex_formulas.remove(&existing_id);
            }

            // Update to value kind
            self.store.set_kind(existing_id, VertexKind::Cell);
            if self.value_cache_enabled {
                let value_ref = self.data_store.store_value(value);
                self.vertex_values.insert(existing_id, value_ref);
            } else {
                // Ensure no stale payload remains if cache is disabled.
                self.vertex_values.remove(&existing_id);
            }
            existing_id
        } else {
            // Create new vertex
            created_placeholders.push(addr);
            let packed_coord = AbsCoord::from_excel(row, col);
            let vertex_id = self.store.allocate(packed_coord, sheet_id, 0x01); // dirty flag

            // Add vertex coordinate for CSR
            self.edges.add_vertex(packed_coord, vertex_id.0);

            // Add to sheet index for O(log n + k) range queries
            self.sheet_index_mut(sheet_id)
                .add_vertex(packed_coord, vertex_id);

            self.store.set_kind(vertex_id, VertexKind::Cell);
            if self.value_cache_enabled {
                let value_ref = self.data_store.store_value(value);
                self.vertex_values.insert(vertex_id, value_ref);
            }
            self.cell_to_vertex.insert(addr, vertex_id);
            vertex_id
        };

        // Cell edits clear any structural #REF! marking for this vertex.
        self.ref_error_vertices.remove(&vertex_id);

        Ok(OperationSummary {
            affected_vertices: self.mark_dirty(vertex_id),
            created_placeholders,
        })
    }

    /// Reserve capacity hints for upcoming bulk cell inserts (values only for now).
    pub fn reserve_cells(&mut self, additional: usize) {
        self.store.reserve(additional);
        if self.value_cache_enabled {
            self.vertex_values.reserve(additional);
        }
        self.cell_to_vertex.reserve(additional);
        // sheet_indexes: cannot easily reserve per-sheet without distribution; skip.
    }

    /// Fast path for initial bulk load of value cells: avoids dirty propagation & dependency work.
    pub fn set_cell_value_bulk_untracked(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) {
        let value = normalize_stored_literal(value);
        let sheet_id = self.sheet_id_mut(sheet);
        let coord = Coord::from_excel(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);
        if let Some(&existing_id) = self.cell_to_vertex.get(&addr) {
            // Overwrite existing value vertex only (ignore formulas in bulk path)
            if matches!(
                self.store.kind(existing_id),
                VertexKind::FormulaScalar | VertexKind::FormulaArray
            ) {
                self.remove_dependent_edges(existing_id);
                self.detach_vertex_from_names(existing_id);
                self.clear_pending_name_references(existing_id);
                self.vertex_formulas.remove(&existing_id);
            }
            if self.value_cache_enabled {
                let value_ref = self.data_store.store_value(value);
                self.vertex_values.insert(existing_id, value_ref);
            } else {
                self.vertex_values.remove(&existing_id);
            }
            self.store.set_kind(existing_id, VertexKind::Cell);
            self.ref_error_vertices.remove(&existing_id);
            return;
        }
        let packed_coord = AbsCoord::from_excel(row, col);
        let vertex_id = self.store.allocate(packed_coord, sheet_id, 0x00); // not dirty
        self.edges.add_vertex(packed_coord, vertex_id.0);
        self.sheet_index_mut(sheet_id)
            .add_vertex(packed_coord, vertex_id);
        self.store.set_kind(vertex_id, VertexKind::Cell);
        self.ref_error_vertices.remove(&vertex_id);
        if self.value_cache_enabled {
            let value_ref = self.data_store.store_value(value);
            self.vertex_values.insert(vertex_id, value_ref);
        }
        self.cell_to_vertex.insert(addr, vertex_id);
    }

    /// Bulk insert a collection of plain value cells (no formulas) more efficiently.
    pub fn bulk_insert_values<I>(&mut self, sheet: &str, cells: I)
    where
        I: IntoIterator<Item = (u32, u32, LiteralValue)>,
    {
        use crate::instant::FzInstant as Instant;
        let t0 = Instant::now();
        // Collect first to know size
        let collected: Vec<(u32, u32, LiteralValue)> = cells.into_iter().collect();
        if collected.is_empty() {
            return;
        }
        let sheet_id = self.sheet_id_mut(sheet);
        self.reserve_cells(collected.len());
        let t_reserve = Instant::now();
        let mut new_vertices: Vec<(AbsCoord, u32)> = Vec::with_capacity(collected.len());
        let mut index_items: Vec<(AbsCoord, VertexId)> = Vec::with_capacity(collected.len());
        // For new allocations, accumulate values and assign after a single batch store
        let mut new_value_coords: Vec<(AbsCoord, VertexId)> = Vec::with_capacity(collected.len());
        let mut new_value_literals: Vec<LiteralValue> = Vec::with_capacity(collected.len());
        // Detect fast path: during initial ingest, caller may guarantee most cells are new.
        let assume_new = self.first_load_assume_new
            && self
                .sheet_id(sheet)
                .map(|sid| !self.ensure_touched_sheets.contains(&sid))
                .unwrap_or(false);

        for (row, col, value) in collected {
            let value = normalize_stored_literal(value);
            let coord = Coord::from_excel(row, col, true, true);
            let addr = CellRef::new(sheet_id, coord);
            if !assume_new && let Some(&existing_id) = self.cell_to_vertex.get(&addr) {
                if matches!(
                    self.store.kind(existing_id),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                ) {
                    self.remove_dependent_edges(existing_id);
                    self.detach_vertex_from_names(existing_id);
                    self.clear_pending_name_references(existing_id);
                    self.vertex_formulas.remove(&existing_id);
                }
                if self.value_cache_enabled {
                    let value_ref = self.data_store.store_value(value);
                    self.vertex_values.insert(existing_id, value_ref);
                } else {
                    self.vertex_values.remove(&existing_id);
                }
                self.store.set_kind(existing_id, VertexKind::Cell);
                continue;
            }
            let packed = AbsCoord::from_excel(row, col);
            let vertex_id = self.store.allocate(packed, sheet_id, 0x00);
            self.store.set_kind(vertex_id, VertexKind::Cell);
            // Defer value arena storage to a single batch
            new_value_coords.push((packed, vertex_id));
            new_value_literals.push(value);
            self.cell_to_vertex.insert(addr, vertex_id);
            new_vertices.push((packed, vertex_id.0));
            index_items.push((packed, vertex_id));
        }
        // Perform a single batch store for newly allocated values
        if self.value_cache_enabled && !new_value_literals.is_empty() {
            let vrefs = self.data_store.store_values_batch(new_value_literals);
            debug_assert_eq!(vrefs.len(), new_value_coords.len());
            for (i, (_pc, vid)) in new_value_coords.iter().enumerate() {
                self.vertex_values.insert(*vid, vrefs[i]);
            }
        }
        let t_after_alloc = Instant::now();
        if !new_vertices.is_empty() {
            let t_edges_start = Instant::now();
            self.edges.add_vertices_batch(&new_vertices);
            let t_edges_done = Instant::now();

            match self.config.sheet_index_mode {
                crate::engine::SheetIndexMode::Eager => {
                    self.sheet_index_mut(sheet_id)
                        .add_vertices_batch(&index_items);
                }
                crate::engine::SheetIndexMode::Lazy => {
                    // Skip building index now; will be built on-demand
                }
                crate::engine::SheetIndexMode::FastBatch => {
                    // FastBatch for now delegates to same batch insert (future: build from sorted arrays)
                    self.sheet_index_mut(sheet_id)
                        .add_vertices_batch(&index_items);
                }
            }
            let t_index_done = Instant::now();
        }
    }

    /// Set a formula in a cell, returns affected vertex IDs
    pub fn set_cell_formula(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast: ASTNode,
    ) -> Result<OperationSummary, ExcelError> {
        self.set_cell_formula_with_volatility(sheet, row, col, ast, false)
    }

    /// Set a formula in a cell. The volatility argument is retained for API compatibility;
    /// dependency flags now come from `IngestPipeline`.
    pub fn set_cell_formula_with_volatility(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast: ASTNode,
        _volatile: bool,
    ) -> Result<OperationSummary, ExcelError> {
        let sheet_id = self.sheet_id_mut(sheet);
        let placement = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
        let provider = RegistryFunctionProvider;
        let ingested = {
            let mut pipeline = self.ingest_pipeline(&provider);
            pipeline.ingest_formula(FormulaAstInput::Tree(ast), placement, None)?
        };
        self.set_cell_formula_with_plan(
            sheet,
            row,
            col,
            ingested.ast_id,
            &ingested.dep_plan,
            ingested.dep_plan.volatile,
            ingested.dep_plan.dynamic,
        )
    }

    pub(crate) fn set_cell_formula_with_plan(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast_id: AstNodeId,
        plan: &DependencyPlanRow,
        volatile: bool,
        dynamic: bool,
    ) -> Result<OperationSummary, ExcelError> {
        let dbg = std::env::var("FZ_DEBUG_LOAD")
            .ok()
            .is_some_and(|v| v != "0");
        let dep_ms_thresh: u128 = std::env::var("FZ_DEBUG_DEP_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let sample_n: usize = std::env::var("FZ_DEBUG_SAMPLE_N")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let t0 = if dbg {
            Some(crate::instant::FzInstant::now())
        } else {
            None
        };
        let sheet_id = self.sheet_id_mut(sheet);
        let coord = Coord::from_excel(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);

        let t_dep0 = if dbg {
            Some(crate::instant::FzInstant::now())
        } else {
            None
        };
        let mut created_placeholders = Vec::new();
        let mut new_dependencies = Vec::with_capacity(plan.direct_cell_deps.len());
        for dep in &plan.direct_cell_deps {
            let dep_vid = self.get_or_create_vertex(dep, &mut created_placeholders);
            if !new_dependencies.contains(&dep_vid) {
                new_dependencies.push(dep_vid);
            }
        }
        let mut named_dependencies = Vec::new();
        let mut unresolved_names = Vec::new();
        for name in plan
            .resolved_named_refs
            .iter()
            .chain(plan.named_refs.iter())
        {
            if let Some(named) = self.resolve_name_entry(name, sheet_id) {
                if !new_dependencies.contains(&named.vertex) {
                    new_dependencies.push(named.vertex);
                }
                if !named_dependencies.contains(&named.vertex) {
                    named_dependencies.push(named.vertex);
                }
            } else if let Some(source) = self.resolve_source_scalar_entry(name) {
                if !new_dependencies.contains(&source.vertex) {
                    new_dependencies.push(source.vertex);
                }
            } else {
                unresolved_names.push(name.clone());
            }
        }
        for source_name in &plan.source_refs {
            if let Some(source) = self.resolve_source_scalar_entry(source_name) {
                if !new_dependencies.contains(&source.vertex) {
                    new_dependencies.push(source.vertex);
                }
            } else if let Some(source) = self.resolve_source_table_entry(source_name)
                && !new_dependencies.contains(&source.vertex)
            {
                new_dependencies.push(source.vertex);
            }
        }
        for table_name in &plan.table_refs {
            if let Some(table) = self.resolve_table_entry(table_name) {
                if !new_dependencies.contains(&table.vertex) {
                    new_dependencies.push(table.vertex);
                }
            } else if let Some(source) = self.resolve_source_table_entry(table_name)
                && !new_dependencies.contains(&source.vertex)
            {
                new_dependencies.push(source.vertex);
            }
        }
        if let (true, Some(t)) = (dbg, t_dep0) {
            let elapsed = t.elapsed().as_millis();
            let do_log = (dep_ms_thresh > 0 && elapsed >= dep_ms_thresh)
                || (sample_n > 0 && (row as usize).is_multiple_of(sample_n));
            if (dep_ms_thresh == 0 && sample_n == 0 && row.is_multiple_of(1000)) || do_log {
                eprintln!(
                    "[fz][dep] {}!{} planned: deps={}, ranges={}, placeholders={}, names={} in {} ms",
                    self.sheet_name(sheet_id),
                    crate::reference::Coord::from_excel(row, col, true, true),
                    new_dependencies.len(),
                    plan.range_deps.len(),
                    created_placeholders.len(),
                    named_dependencies.len(),
                    elapsed
                );
            }
        }

        // Check for self-reference (immediate cycle detection)
        let addr_vertex_id = self.get_or_create_vertex(&addr, &mut created_placeholders);

        // Editing a formula clears any prior structural #REF! marking for this vertex.
        self.ref_error_vertices.remove(&addr_vertex_id);

        if new_dependencies.contains(&addr_vertex_id) {
            return Err(ExcelError::new(ExcelErrorKind::Circ)
                .with_message("Self-reference detected".to_string()));
        }

        for &name_vertex in &named_dependencies {
            let mut visited = FxHashSet::default();
            if self.name_depends_on_vertex(name_vertex, addr_vertex_id, &mut visited) {
                return Err(ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Circular reference through named range".to_string()));
            }
        }

        // Remove old dependencies first
        self.remove_dependent_edges(addr_vertex_id);
        self.detach_vertex_from_names(addr_vertex_id);
        self.clear_pending_name_references(addr_vertex_id);

        // Update vertex properties
        self.store
            .set_kind(addr_vertex_id, VertexKind::FormulaScalar);
        self.vertex_formulas.insert(addr_vertex_id, ast_id);
        self.store.set_dirty(addr_vertex_id, true);

        // Clear any cached value since this is now a formula
        self.vertex_values.remove(&addr_vertex_id);

        self.mark_volatile(addr_vertex_id, volatile);
        self.store.set_dynamic(addr_vertex_id, dynamic);

        if !named_dependencies.is_empty() {
            self.attach_vertex_to_names(addr_vertex_id, &named_dependencies);
        }
        for unresolved_name in &unresolved_names {
            self.record_pending_name_reference(sheet_id, unresolved_name, addr_vertex_id);
        }

        if let (true, Some(t)) = (dbg, t0) {
            let elapsed = t.elapsed().as_millis();
            let log_set = dep_ms_thresh > 0 && elapsed >= dep_ms_thresh;
            if log_set {
                eprintln!(
                    "[fz][set] {}!{} total {} ms",
                    self.sheet_name(sheet_id),
                    crate::reference::Coord::from_excel(row, col, true, true),
                    elapsed
                );
            }
        }

        // Add new dependency edges
        self.add_dependent_edges(addr_vertex_id, &new_dependencies);
        self.add_range_dependent_edges(addr_vertex_id, &plan.range_deps, sheet_id);

        Ok(OperationSummary {
            affected_vertices: self.mark_dirty(addr_vertex_id),
            created_placeholders,
        })
    }

    pub(crate) fn rewrite_structured_references_for_cell(
        &self,
        ast: &mut ASTNode,
        cell: CellRef,
    ) -> Result<bool, ExcelError> {
        self.rewrite_structured_references_node(ast, cell)
    }

    fn rewrite_structured_references_node(
        &self,
        node: &mut ASTNode,
        cell: CellRef,
    ) -> Result<bool, ExcelError> {
        match &mut node.node_type {
            ASTNodeType::Reference { reference, .. } => {
                self.rewrite_structured_reference(reference, cell)
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.rewrite_structured_references_node(expr, cell)
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                let left_rewritten = self.rewrite_structured_references_node(left, cell)?;
                let right_rewritten = self.rewrite_structured_references_node(right, cell)?;
                Ok(left_rewritten || right_rewritten)
            }
            ASTNodeType::Function { args, .. } => {
                let mut rewritten = false;
                for a in args.iter_mut() {
                    rewritten |= self.rewrite_structured_references_node(a, cell)?;
                }
                Ok(rewritten)
            }
            ASTNodeType::Call { callee, args } => {
                let mut rewritten = self.rewrite_structured_references_node(callee, cell)?;
                for a in args.iter_mut() {
                    rewritten |= self.rewrite_structured_references_node(a, cell)?;
                }
                Ok(rewritten)
            }
            ASTNodeType::Array(rows) => {
                let mut rewritten = false;
                for r in rows.iter_mut() {
                    for item in r.iter_mut() {
                        rewritten |= self.rewrite_structured_references_node(item, cell)?;
                    }
                }
                Ok(rewritten)
            }
            ASTNodeType::Literal(_) => Ok(false),
        }
    }

    fn rewrite_structured_reference(
        &self,
        reference: &mut ReferenceType,
        cell: CellRef,
    ) -> Result<bool, ExcelError> {
        use formualizer_parse::parser::{SpecialItem, TableSpecifier};

        let ReferenceType::Table(tref) = reference else {
            return Ok(false);
        };

        // This-row shorthand: parsed as an unnamed table reference with a Combination specifier.
        if !tref.name.is_empty() {
            return Ok(false);
        }

        let col_name = match &tref.specifier {
            Some(TableSpecifier::Combination(parts)) => {
                let mut saw_this_row = false;
                let mut col: Option<&str> = None;
                for p in parts {
                    match p.as_ref() {
                        TableSpecifier::SpecialItem(SpecialItem::ThisRow) => {
                            saw_this_row = true;
                        }
                        TableSpecifier::Column(c) => {
                            if col.is_some() {
                                return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                    "This-row structured reference with multiple columns is not supported"
                                        .to_string(),
                                ));
                            }
                            col = Some(c.as_str());
                        }
                        other => {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                format!(
                                    "Unsupported this-row structured reference component: {other}"
                                ),
                            ));
                        }
                    }
                }
                if !saw_this_row {
                    return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "Unnamed structured reference requires a this-row selector".to_string(),
                    ));
                }
                col.ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "This-row structured reference missing column selector".to_string(),
                    )
                })?
            }
            _ => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                    "Unnamed structured reference form is not supported".to_string(),
                ));
            }
        };

        let Some(table) = self.find_table_containing_cell(cell) else {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("This-row structured reference used outside a table".to_string()));
        };

        let row0 = cell.coord.row();
        let col0 = cell.coord.col();
        let sr0 = table.range.start.coord.row();
        let sc0 = table.range.start.coord.col();
        let er0 = table.range.end.coord.row();
        let ec0 = table.range.end.coord.col();

        if row0 < sr0 || row0 > er0 || col0 < sc0 || col0 > ec0 {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("This-row structured reference used outside a table".to_string()));
        }

        if table.header_row && row0 == sr0 {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                "This-row structured references are not valid in the table header row".to_string(),
            ));
        }

        let data_start = if table.header_row { sr0 + 1 } else { sr0 };
        if row0 < data_start {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                "This-row structured references require a data/totals row context".to_string(),
            ));
        }

        let Some(idx) = table.col_index(col_name) else {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(format!(
                "Unknown table column in this-row reference: {col_name}"
            )));
        };
        let target_col0 = sc0 + (idx as u32);
        let target_row = row0 + 1;
        let target_col = target_col0 + 1;

        *reference = ReferenceType::Cell {
            sheet: None,
            row: target_row,
            col: target_col,
            row_abs: true,
            col_abs: true,
        };

        Ok(true)
    }

    fn find_table_containing_cell(&self, cell: CellRef) -> Option<&tables::TableEntry> {
        let row0 = cell.coord.row();
        let col0 = cell.coord.col();

        let mut best: Option<&tables::TableEntry> = None;
        let mut best_area: u64 = u64::MAX;
        let mut best_name: &str = "";

        for t in self.tables.values() {
            if t.sheet_id() != cell.sheet_id {
                continue;
            }
            let sr0 = t.range.start.coord.row();
            let sc0 = t.range.start.coord.col();
            let er0 = t.range.end.coord.row();
            let ec0 = t.range.end.coord.col();
            if row0 < sr0 || row0 > er0 || col0 < sc0 || col0 > ec0 {
                continue;
            }

            let h = (er0 - sr0 + 1) as u64;
            let w = (ec0 - sc0 + 1) as u64;
            let area = h.saturating_mul(w);
            let name = t.name.as_str();
            let better = match best {
                None => true,
                Some(_) => area < best_area || (area == best_area && name < best_name),
            };
            if better {
                best = Some(t);
                best_area = area;
                best_name = name;
            }
        }

        best
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn fp8_parity_extract_dependencies_with_pending_names(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
    ) -> Result<
        (
            Vec<VertexId>,
            Vec<SharedRangeRef<'static>>,
            Vec<CellRef>,
            Vec<VertexId>,
            Vec<String>,
        ),
        ExcelError,
    > {
        self.extract_dependencies_with_pending_names(ast, current_sheet_id)
    }

    pub(crate) fn fp8_parity_is_ast_volatile(&self, ast: &ASTNode) -> bool {
        self.is_ast_volatile(ast)
    }

    pub fn set_cell_value_ref(
        &mut self,
        cell: formualizer_common::SheetCellRef<'_>,
        value: LiteralValue,
    ) -> Result<OperationSummary, ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = match owned.sheet {
            formualizer_common::SheetLocator::Id(id) => id,
            formualizer_common::SheetLocator::Name(name) => self.sheet_id_mut(name.as_ref()),
            formualizer_common::SheetLocator::Current => self.default_sheet_id,
        };
        let sheet_name = self.sheet_name(sheet_id).to_string();
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
        ast: ASTNode,
    ) -> Result<OperationSummary, ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = match owned.sheet {
            formualizer_common::SheetLocator::Id(id) => id,
            formualizer_common::SheetLocator::Name(name) => self.sheet_id_mut(name.as_ref()),
            formualizer_common::SheetLocator::Current => self.default_sheet_id,
        };
        let sheet_name = self.sheet_name(sheet_id).to_string();
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
    ) -> Option<LiteralValue> {
        let owned = cell.into_owned();
        let sheet_id = match owned.sheet {
            formualizer_common::SheetLocator::Id(id) => id,
            formualizer_common::SheetLocator::Name(name) => self.sheet_id(name.as_ref())?,
            formualizer_common::SheetLocator::Current => self.default_sheet_id,
        };
        let sheet_name = self.sheet_name(sheet_id);
        self.get_cell_value(sheet_name, owned.coord.row() + 1, owned.coord.col() + 1)
    }

    /// Get current value from a cell
    pub fn get_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        if !self.value_cache_enabled {
            #[cfg(debug_assertions)]
            {
                self.graph_value_read_attempts
                    .fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        let sheet_id = self.sheet_reg.get_id(sheet)?;
        let coord = Coord::from_excel(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);

        self.get_vertex_id_for_address(&addr)
            .and_then(|&vertex_id| {
                // Check values hashmap (stores both cell values and formula results)
                self.vertex_values
                    .get(&vertex_id)
                    .map(|&value_ref| self.data_store.retrieve_value(value_ref))
            })
    }

    /// Mark vertex dirty and propagate to dependents
    fn mark_dirty(&mut self, vertex_id: VertexId) -> Vec<VertexId> {
        let mut affected = FxHashSet::default();
        let mut to_visit = Vec::new();
        let mut visited_for_propagation = FxHashSet::default();

        // Only mark the source vertex as dirty if it's a formula
        // Value cells don't get marked dirty themselves but are still affected
        let is_formula = matches!(
            self.store.kind(vertex_id),
            VertexKind::FormulaScalar
                | VertexKind::FormulaArray
                | VertexKind::NamedScalar
                | VertexKind::NamedArray
        );

        if is_formula {
            to_visit.push(vertex_id);
        } else {
            // Value cells are affected (for tracking) but not marked dirty
            affected.insert(vertex_id);
        }

        // Initial propagation from direct and range dependents
        {
            // Get dependents (vertices that depend on this vertex)
            if let Some(dependents) = self.dependents_slice(vertex_id) {
                to_visit.extend(dependents.iter().copied());
            } else {
                let dependents = self.get_dependents(vertex_id);
                to_visit.extend(dependents);
            }

            if let Some(name_set) = self.cell_to_name_dependents.get(&vertex_id) {
                for &name_vertex in name_set {
                    to_visit.push(name_vertex);
                }
            }

            to_visit.extend(self.collect_range_dependents_for_vertex(vertex_id));
        }

        while let Some(id) = to_visit.pop() {
            if !visited_for_propagation.insert(id) {
                continue; // Already processed
            }
            affected.insert(id);

            // Mark vertex as dirty
            self.store.set_dirty(id, true);

            // Add direct dependents to visit list
            if let Some(dependents) = self.dependents_slice(id) {
                to_visit.extend(dependents.iter().copied());
            } else {
                let dependents = self.get_dependents(id);
                to_visit.extend(dependents);
            }
            to_visit.extend(self.collect_range_dependents_for_vertex(id));
        }

        // Add to dirty set
        self.dirty_vertices.extend(&affected);

        // Return as Vec for compatibility
        affected.into_iter().collect()
    }

    /// Get all vertices that need evaluation
    pub fn get_evaluation_vertices(&self) -> Vec<VertexId> {
        let mut combined = FxHashSet::default();
        combined.extend(&self.dirty_vertices);
        combined.extend(&self.volatile_vertices);

        let mut result: Vec<VertexId> = combined
            .into_iter()
            .filter(|&id| {
                // Only include formula vertices
                matches!(
                    self.store.kind(id),
                    VertexKind::FormulaScalar
                        | VertexKind::FormulaArray
                        | VertexKind::NamedScalar
                        | VertexKind::NamedArray
                )
            })
            .collect();
        result.sort_unstable();
        result
    }

    /// Clear dirty flags after successful evaluation
    pub fn clear_dirty_flags(&mut self, vertices: &[VertexId]) {
        for &vertex_id in vertices {
            self.store.set_dirty(vertex_id, false);
            self.dirty_vertices.remove(&vertex_id);
        }
    }

    /// 🔮 Scalability Hook: Clear volatile vertices after evaluation cycle
    pub fn clear_volatile_flags(&mut self) {
        self.volatile_vertices.clear();
    }

    /// Re-marks all volatile vertices as dirty for the next evaluation cycle.
    pub(crate) fn redirty_volatiles(&mut self) {
        let volatile_ids: Vec<VertexId> = self.volatile_vertices.iter().copied().collect();
        for id in volatile_ids {
            self.mark_dirty(id);
        }
    }

    fn get_or_create_vertex(
        &mut self,
        addr: &CellRef,
        created_placeholders: &mut Vec<CellRef>,
    ) -> VertexId {
        if let Some(&vertex_id) = self.cell_to_vertex.get(addr) {
            return vertex_id;
        }

        created_placeholders.push(*addr);
        let packed_coord = AbsCoord::new(addr.coord.row(), addr.coord.col());
        let vertex_id = self.store.allocate(packed_coord, addr.sheet_id, 0x00);

        // Add vertex coordinate for CSR
        self.edges.add_vertex(packed_coord, vertex_id.0);

        // Add to sheet index for O(log n + k) range queries
        self.sheet_index_mut(addr.sheet_id)
            .add_vertex(packed_coord, vertex_id);

        self.store.set_kind(vertex_id, VertexKind::Empty);
        self.cell_to_vertex.insert(*addr, vertex_id);
        vertex_id
    }

    fn add_dependent_edges(&mut self, dependent: VertexId, dependencies: &[VertexId]) {
        // Batch to avoid repeated CSR rebuilds and keep reverse edges current
        self.edges.begin_batch();

        // If PK enabled, update order using a short-lived adapter without holding &mut self
        // Track dependencies that should be skipped if rejecting cycle-creating edges
        let mut skip_deps: rustc_hash::FxHashSet<VertexId> = rustc_hash::FxHashSet::default();
        if self.pk_order.is_some()
            && let Some(mut pk) = self.pk_order.take()
        {
            pk.ensure_nodes(std::iter::once(dependent));
            pk.ensure_nodes(dependencies.iter().copied());
            {
                let adapter = GraphAdapter { g: self };
                for &dep_id in dependencies {
                    match pk.try_add_edge(&adapter, dep_id, dependent) {
                        Ok(_) => {}
                        Err(_cycle) => {
                            if self.config.pk_reject_cycle_edges {
                                skip_deps.insert(dep_id);
                            } else {
                                pk.rebuild_full(&adapter);
                            }
                        }
                    }
                }
            } // drop adapter
            self.pk_order = Some(pk);
        }

        // Now mutate engine edges; if rejecting cycles, re-check and skip those that would create cycles
        for &dep_id in dependencies {
            if self.config.pk_reject_cycle_edges && skip_deps.contains(&dep_id) {
                continue;
            }
            self.edges.add_edge(dependent, dep_id);
            #[cfg(test)]
            {
                if let Ok(mut g) = self.instr.lock() {
                    g.edges_added += 1;
                }
            }
        }

        self.edges.end_batch();
    }

    /// Like add_dependent_edges, but assumes caller is managing edges.begin_batch/end_batch
    fn add_dependent_edges_nobatch(&mut self, dependent: VertexId, dependencies: &[VertexId]) {
        // If PK enabled, update order using a short-lived adapter without holding &mut self
        let mut skip_deps: rustc_hash::FxHashSet<VertexId> = rustc_hash::FxHashSet::default();
        if self.pk_order.is_some()
            && let Some(mut pk) = self.pk_order.take()
        {
            pk.ensure_nodes(std::iter::once(dependent));
            pk.ensure_nodes(dependencies.iter().copied());
            {
                let adapter = GraphAdapter { g: self };
                for &dep_id in dependencies {
                    match pk.try_add_edge(&adapter, dep_id, dependent) {
                        Ok(_) => {}
                        Err(_cycle) => {
                            if self.config.pk_reject_cycle_edges {
                                skip_deps.insert(dep_id);
                            } else {
                                pk.rebuild_full(&adapter);
                            }
                        }
                    }
                }
            }
            self.pk_order = Some(pk);
        }

        for &dep_id in dependencies {
            if self.config.pk_reject_cycle_edges && skip_deps.contains(&dep_id) {
                continue;
            }
            self.edges.add_edge(dependent, dep_id);
            #[cfg(test)]
            {
                if let Ok(mut g) = self.instr.lock() {
                    g.edges_added += 1;
                }
            }
        }
    }

    /// Bulk set formulas on a sheet using a single dependency plan and batched edge updates.
    pub fn bulk_set_formulas<I>(&mut self, sheet: &str, items: I) -> Result<usize, ExcelError>
    where
        I: IntoIterator<Item = (u32, u32, ASTNode)>,
    {
        let collected: Vec<(u32, u32, ASTNode)> = items.into_iter().collect();
        if collected.is_empty() {
            return Ok(0);
        }
        let vol_flags: Vec<bool> = collected
            .iter()
            .map(|(_, _, ast)| self.is_ast_volatile(ast))
            .collect();
        self.bulk_set_formulas_with_volatility(sheet, collected, vol_flags)
    }

    pub fn bulk_set_formulas_with_volatility(
        &mut self,
        sheet: &str,
        collected: Vec<(u32, u32, ASTNode)>,
        _vol_flags: Vec<bool>,
    ) -> Result<usize, ExcelError> {
        let sheet_id = self.sheet_id_mut(sheet);
        if collected.is_empty() {
            return Ok(0);
        }
        let provider = RegistryFunctionProvider;
        let ingested = {
            let mut pipeline = self.ingest_pipeline(&provider);
            let inputs = collected.into_iter().map(|(row, col, ast)| {
                let placement = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
                (FormulaAstInput::Tree(ast), placement, None)
            });
            pipeline.ingest_batch(inputs)?
        };
        let planned = ingested
            .into_iter()
            .map(|formula| {
                (
                    formula.placement.coord.row() + 1,
                    formula.placement.coord.col() + 1,
                    formula.ast_id,
                    formula.dep_plan,
                )
            })
            .collect();
        self.bulk_set_formulas_with_plans(sheet, planned)
    }

    pub(crate) fn bulk_set_formulas_with_plans(
        &mut self,
        sheet: &str,
        planned: Vec<(u32, u32, AstNodeId, DependencyPlanRow)>,
    ) -> Result<usize, ExcelError> {
        let sheet_id = self.sheet_id_mut(sheet);
        if planned.is_empty() {
            return Ok(0);
        }
        let mut created_placeholders: Vec<CellRef> = Vec::new();
        let mut target_vids: Vec<VertexId> = Vec::with_capacity(planned.len());
        for (row, col, _, _) in &planned {
            let addr = CellRef::new(sheet_id, Coord::from_excel(*row, *col, true, true));
            target_vids.push(self.get_or_create_vertex(&addr, &mut created_placeholders));
        }

        for (i, &tvid) in target_vids.iter().enumerate() {
            if self.vertex_formulas.contains_key(&tvid) {
                self.remove_dependent_edges(tvid);
            }
            self.detach_vertex_from_names(tvid);
            self.clear_pending_name_references(tvid);
            self.store.set_kind(tvid, VertexKind::FormulaScalar);
            self.store.set_dirty(tvid, true);
            self.vertex_values.remove(&tvid);
            self.vertex_formulas.insert(tvid, planned[i].2);
            self.mark_volatile(tvid, planned[i].3.volatile);
            self.store.set_dynamic(tvid, planned[i].3.dynamic);
        }
        self.dirty_vertices.extend(target_vids.iter().copied());

        self.edges.begin_batch();
        for (i, tvid) in target_vids.iter().copied().enumerate() {
            let plan = &planned[i].3;
            let mut deps: Vec<VertexId> = Vec::new();
            for cell in &plan.direct_cell_deps {
                let dep_vid = self.get_or_create_vertex(cell, &mut created_placeholders);
                if !deps.contains(&dep_vid) {
                    deps.push(dep_vid);
                }
            }

            let mut name_vertices = Vec::new();
            for name in plan
                .resolved_named_refs
                .iter()
                .chain(plan.named_refs.iter())
            {
                if let Some(named) = self.resolve_name_entry(name, sheet_id) {
                    if !deps.contains(&named.vertex) {
                        deps.push(named.vertex);
                    }
                    if !name_vertices.contains(&named.vertex) {
                        name_vertices.push(named.vertex);
                    }
                } else if let Some(source) = self.resolve_source_scalar_entry(name) {
                    if !deps.contains(&source.vertex) {
                        deps.push(source.vertex);
                    }
                } else {
                    self.record_pending_name_reference(sheet_id, name, tvid);
                }
            }
            for source_name in &plan.source_refs {
                if let Some(source) = self.resolve_source_scalar_entry(source_name) {
                    if !deps.contains(&source.vertex) {
                        deps.push(source.vertex);
                    }
                } else if let Some(source) = self.resolve_source_table_entry(source_name)
                    && !deps.contains(&source.vertex)
                {
                    deps.push(source.vertex);
                }
            }
            for table_name in &plan.table_refs {
                if let Some(table) = self.resolve_table_entry(table_name) {
                    if !deps.contains(&table.vertex) {
                        deps.push(table.vertex);
                    }
                } else if let Some(source) = self.resolve_source_table_entry(table_name)
                    && !deps.contains(&source.vertex)
                {
                    deps.push(source.vertex);
                }
            }
            if !name_vertices.is_empty() {
                self.attach_vertex_to_names(tvid, &name_vertices);
            }
            if !deps.is_empty() {
                self.add_dependent_edges_nobatch(tvid, &deps);
            }
            self.add_range_dependent_edges(tvid, &plan.range_deps, sheet_id);
        }
        self.edges.end_batch();

        Ok(planned.len())
    }

    /// Public (crate) helper to add a single dependency edge (dependent -> dependency) used for restoration/undo.
    pub fn add_dependency_edge(&mut self, dependent: VertexId, dependency: VertexId) {
        if dependent == dependency {
            return;
        }
        // If PK enabled attempt to add maintaining ordering; fallback to rebuild if cycle
        if self.pk_order.is_some()
            && let Some(mut pk) = self.pk_order.take()
        {
            pk.ensure_nodes(std::iter::once(dependent));
            pk.ensure_nodes(std::iter::once(dependency));
            let adapter = GraphAdapter { g: self };
            if pk.try_add_edge(&adapter, dependency, dependent).is_err() {
                // Cycle: rebuild full (conservative)
                pk.rebuild_full(&adapter);
            }
            self.pk_order = Some(pk);
        }
        self.edges.add_edge(dependent, dependency);
        self.store.set_dirty(dependent, true);
        self.dirty_vertices.insert(dependent);
    }

    fn remove_dependent_edges(&mut self, vertex: VertexId) {
        // Remove all outgoing edges from this vertex (its dependencies)
        let dependencies = self.edges.out_edges(vertex);

        self.edges.begin_batch();
        if self.pk_order.is_some()
            && let Some(mut pk) = self.pk_order.take()
        {
            for dep in &dependencies {
                pk.remove_edge(*dep, vertex);
            }
            self.pk_order = Some(pk);
        }
        for dep in dependencies {
            self.edges.remove_edge(vertex, dep);
        }
        self.edges.end_batch();

        // Remove range dependencies and clean up stripes
        if let Some(old_ranges) = self.formula_to_range_deps.remove(&vertex) {
            let old_sheet_id = self.store.sheet_id(vertex);

            for range in &old_ranges {
                let sheet_id = match range.sheet {
                    SharedSheetLocator::Id(id) => id,
                    _ => old_sheet_id,
                };
                let s_row = range.start_row.map(|b| b.index);
                let e_row = range.end_row.map(|b| b.index);
                let s_col = range.start_col.map(|b| b.index);
                let e_col = range.end_col.map(|b| b.index);

                let mut keys_to_clean = FxHashSet::default();

                let col_stripes = (s_row.is_none() && e_row.is_none())
                    || (s_col.is_some() && e_col.is_some() && (s_row.is_none() || e_row.is_none()));
                let row_stripes = (s_col.is_none() && e_col.is_none())
                    || (s_row.is_some() && e_row.is_some() && (s_col.is_none() || e_col.is_none()));

                if col_stripes && !row_stripes {
                    let sc = s_col.unwrap_or(0);
                    let ec = e_col.unwrap_or(sc);
                    for col in sc..=ec {
                        keys_to_clean.insert(StripeKey {
                            sheet_id,
                            stripe_type: StripeType::Column,
                            index: col,
                        });
                    }
                } else if row_stripes && !col_stripes {
                    let sr = s_row.unwrap_or(0);
                    let er = e_row.unwrap_or(sr);
                    for row in sr..=er {
                        keys_to_clean.insert(StripeKey {
                            sheet_id,
                            stripe_type: StripeType::Row,
                            index: row,
                        });
                    }
                } else {
                    let start_row = s_row.unwrap_or(0);
                    let start_col = s_col.unwrap_or(0);
                    let end_row = e_row.unwrap_or(start_row);
                    let end_col = e_col.unwrap_or(start_col);

                    let height = end_row.saturating_sub(start_row) + 1;
                    let width = end_col.saturating_sub(start_col) + 1;

                    if self.config.enable_block_stripes && height > 1 && width > 1 {
                        let start_block_row = start_row / BLOCK_H;
                        let end_block_row = end_row / BLOCK_H;
                        let start_block_col = start_col / BLOCK_W;
                        let end_block_col = end_col / BLOCK_W;

                        for block_row in start_block_row..=end_block_row {
                            for block_col in start_block_col..=end_block_col {
                                keys_to_clean.insert(StripeKey {
                                    sheet_id,
                                    stripe_type: StripeType::Block,
                                    index: block_index(block_row * BLOCK_H, block_col * BLOCK_W),
                                });
                            }
                        }
                    } else if height > width {
                        for col in start_col..=end_col {
                            keys_to_clean.insert(StripeKey {
                                sheet_id,
                                stripe_type: StripeType::Column,
                                index: col,
                            });
                        }
                    } else {
                        for row in start_row..=end_row {
                            keys_to_clean.insert(StripeKey {
                                sheet_id,
                                stripe_type: StripeType::Row,
                                index: row,
                            });
                        }
                    }
                }

                for key in keys_to_clean {
                    if let Some(dependents) = self.stripe_to_dependents.get_mut(&key) {
                        dependents.remove(&vertex);
                        if dependents.is_empty() {
                            self.stripe_to_dependents.remove(&key);
                            #[cfg(test)]
                            {
                                if let Ok(mut g) = self.instr.lock() {
                                    g.stripe_removes += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Removed: vertices() and get_vertex() methods - no longer needed with SoA
    // The old AoS Vertex struct has been eliminated in favor of direct
    // access to columnar data through the VertexStore

    /// Updates the cached value of a formula vertex.
    pub(crate) fn update_vertex_value(&mut self, vertex_id: VertexId, value: LiteralValue) {
        if !self.value_cache_enabled {
            // Canonical mode: cell/formula vertices must not store values in the graph.
            match self.store.kind(vertex_id) {
                VertexKind::Cell
                | VertexKind::FormulaScalar
                | VertexKind::FormulaArray
                | VertexKind::Empty => {
                    self.vertex_values.remove(&vertex_id);
                    return;
                }
                _ => {
                    // Allow non-cell vertices to cache values (e.g. named-range formulas).
                }
            }
        }
        let value_ref = self.data_store.store_value(normalize_stored_literal(value));
        self.vertex_values.insert(vertex_id, value_ref);
    }

    /// Plan a spill region for an anchor; returns #SPILL! if blocked
    pub fn plan_spill_region(
        &self,
        anchor: VertexId,
        target_cells: &[CellRef],
    ) -> Result<(), ExcelError> {
        self.plan_spill_region_allowing_formula_overwrite(anchor, target_cells, None)
    }

    /// Plan a spill region, optionally allowing specific formula vertices to be overwritten.
    ///
    /// This is used by parallel evaluation to allow spill anchors to take precedence over
    /// other formula vertices that are being evaluated in the same layer.
    pub(crate) fn plan_spill_region_allowing_formula_overwrite(
        &self,
        anchor: VertexId,
        target_cells: &[CellRef],
        overwritable_formulas: Option<&rustc_hash::FxHashSet<VertexId>>,
    ) -> Result<(), ExcelError> {
        use formualizer_common::{ExcelErrorExtra, ExcelErrorKind};
        // Compute expected spill shape from the target rectangle for better diagnostics
        let (expected_rows, expected_cols) = if target_cells.is_empty() {
            (0u32, 0u32)
        } else {
            let mut min_r = u32::MAX;
            let mut max_r = 0u32;
            let mut min_c = u32::MAX;
            let mut max_c = 0u32;
            for cell in target_cells {
                let r = cell.coord.row();
                let c = cell.coord.col();
                if r < min_r {
                    min_r = r;
                }
                if r > max_r {
                    max_r = r;
                }
                if c < min_c {
                    min_c = c;
                }
                if c > max_c {
                    max_c = c;
                }
            }
            (
                max_r.saturating_sub(min_r).saturating_add(1),
                max_c.saturating_sub(min_c).saturating_add(1),
            )
        };
        // Allow overlapping with previously owned spill cells by this anchor
        for cell in target_cells {
            // If cell is already owned by this anchor's previous spill, it's allowed.
            let owned_by_anchor = match self.spill_cell_to_anchor.get(cell) {
                Some(&existing_anchor) if existing_anchor == anchor => true,
                Some(_other) => {
                    return Err(ExcelError::new(ExcelErrorKind::Spill)
                        .with_message("BlockedBySpill")
                        .with_extra(ExcelErrorExtra::Spill {
                            expected_rows,
                            expected_cols,
                        }));
                }
                None => false,
            };

            if owned_by_anchor {
                continue;
            }

            // If cell is occupied by another formula anchor, block unless explicitly allowed.
            if let Some(&vid) = self.cell_to_vertex.get(cell)
                && vid != anchor
            {
                // Prevent clobbering formulas (array or scalar) in the target area
                match self.store.kind(vid) {
                    VertexKind::FormulaScalar | VertexKind::FormulaArray => {
                        if let Some(allow) = overwritable_formulas
                            && allow.contains(&vid)
                        {
                            continue;
                        }
                        return Err(ExcelError::new(ExcelErrorKind::Spill)
                            .with_message("BlockedByFormula")
                            .with_extra(ExcelErrorExtra::Spill {
                                expected_rows,
                                expected_cols,
                            }));
                    }
                    _ => {
                        // If a non-empty value exists (and not this anchor), block
                        if let Some(vref) = self.vertex_values.get(&vid) {
                            let v = self.data_store.retrieve_value(*vref);
                            if !matches!(v, LiteralValue::Empty) {
                                return Err(ExcelError::new(ExcelErrorKind::Spill)
                                    .with_message("BlockedByValue")
                                    .with_extra(ExcelErrorExtra::Spill {
                                        expected_rows,
                                        expected_cols,
                                    }));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // Note: non-atomic commit_spill_region has been removed. All callers must use
    // commit_spill_region_atomic_with_fault for atomicity and rollback on failure.

    /// Commit a spill atomically with an internal shadow buffer and optional fault injection.
    /// If a fault is injected partway through, all changes are rolled back to the pre-commit state.
    /// This does not change behavior under normal operation; it's primarily for Phase 3 guarantees and tests.
    pub fn commit_spill_region_atomic_with_fault(
        &mut self,
        anchor: VertexId,
        target_cells: Vec<CellRef>,
        values: Vec<Vec<LiteralValue>>,
        fault_after_ops: Option<usize>,
    ) -> Result<(), ExcelError> {
        // Anchor cell coordinates (0-based) for special-casing writes.
        // We must never overwrite the anchor via set_cell_value(), because that would
        // strip the formula and break incremental recalculation.
        let anchor_cell = self
            .get_cell_ref(anchor)
            .expect("anchor cell ref for spill commit");
        let anchor_sheet_name = self.sheet_name(anchor_cell.sheet_id).to_string();
        let anchor_row = anchor_cell.coord.row();
        let anchor_col = anchor_cell.coord.col();

        // Capture previous owned cells for this anchor
        let prev_cells = self
            .spill_anchor_to_cells
            .get(&anchor)
            .cloned()
            .unwrap_or_default();
        // Use CoordBuildHasher on CellRef keys to avoid FxHasher clustering on
        // packed Coord values.
        let new_set: std::collections::HashSet<CellRef, CoordBuildHasher> =
            target_cells.iter().copied().collect();
        let prev_set: std::collections::HashSet<CellRef, CoordBuildHasher> =
            prev_cells.iter().copied().collect();

        // Compose operation list: clears first (prev - new), then writes for new rectangle
        #[derive(Clone)]
        struct Op {
            sheet: String,
            row: u32,
            col: u32,
            new_value: LiteralValue,
        }
        let mut ops: Vec<Op> = Vec::new();

        // Clears for cells no longer used
        for cell in prev_cells.iter() {
            if !new_set.contains(cell) {
                let sheet = self.sheet_name(cell.sheet_id).to_string();
                ops.push(Op {
                    sheet,
                    row: cell.coord.row(),
                    col: cell.coord.col(),
                    new_value: LiteralValue::Empty,
                });
            }
        }

        // Writes for new values (row-major to match target rectangle)
        if !target_cells.is_empty() {
            let first = target_cells.first().copied().unwrap();
            let row0 = first.coord.row();
            let col0 = first.coord.col();
            let sheet = self.sheet_name(first.sheet_id).to_string();
            for (r_off, row_vals) in values.iter().enumerate() {
                for (c_off, v) in row_vals.iter().enumerate() {
                    ops.push(Op {
                        sheet: sheet.clone(),
                        row: row0 + r_off as u32,
                        col: col0 + c_off as u32,
                        new_value: v.clone(),
                    });
                }
            }
        }

        // Shadow buffer of old values for rollback
        #[derive(Clone)]
        struct OldVal {
            present: bool,
            value: LiteralValue,
        }
        let mut old_values: Vec<((String, u32, u32), OldVal)> = Vec::with_capacity(ops.len());

        // Capture old values before applying
        for op in &ops {
            // op.row/op.col are internal 0-based; get_cell_value is a public 1-based API.
            let old = self
                .get_cell_value(&op.sheet, op.row + 1, op.col + 1)
                .unwrap_or(LiteralValue::Empty);
            let present = true; // unified model: we always treat as present
            old_values.push((
                (op.sheet.clone(), op.row, op.col),
                OldVal {
                    present,
                    value: old,
                },
            ));
        }

        // Apply with optional injected fault
        for (applied, op) in ops.iter().enumerate() {
            if let Some(n) = fault_after_ops
                && applied == n
            {
                for idx in (0..applied).rev() {
                    let ((ref sheet, row, col), ref old) = old_values[idx];
                    if sheet == &anchor_sheet_name && row == anchor_row && col == anchor_col {
                        self.update_vertex_value(anchor, old.value.clone());
                    } else {
                        let _ = self.set_cell_value(sheet, row + 1, col + 1, old.value.clone());
                    }
                }
                return Err(ExcelError::new(ExcelErrorKind::Error)
                    .with_message("Injected persistence fault during spill commit"));
            }
            if op.sheet == anchor_sheet_name && op.row == anchor_row && op.col == anchor_col {
                self.update_vertex_value(anchor, op.new_value.clone());
            } else {
                let _ =
                    self.set_cell_value(&op.sheet, op.row + 1, op.col + 1, op.new_value.clone());
            }
        }

        // Update spill ownership maps only on success
        // Clear previous ownership not reused
        for cell in prev_cells.iter() {
            if !new_set.contains(cell) {
                self.spill_cell_to_anchor.remove(cell);
            }
        }
        // Mark ownership for new rectangle using the declared target cells only
        for cell in &target_cells {
            self.spill_cell_to_anchor.insert(*cell, anchor);
        }
        self.spill_anchor_to_cells.insert(anchor, target_cells);
        Ok(())
    }

    pub(crate) fn spill_cells_for_anchor(&self, anchor: VertexId) -> Option<&[CellRef]> {
        self.spill_anchor_to_cells
            .get(&anchor)
            .map(|v| v.as_slice())
    }

    pub(crate) fn spill_registry_has_anchor(&self, anchor: VertexId) -> bool {
        self.spill_anchor_to_cells.contains_key(&anchor)
    }

    pub(crate) fn spill_registry_anchor_for_cell(&self, cell: CellRef) -> Option<VertexId> {
        self.spill_cell_to_anchor.get(&cell).copied()
    }

    pub(crate) fn spill_registry_counts(&self) -> (usize, usize) {
        (
            self.spill_anchor_to_cells.len(),
            self.spill_cell_to_anchor.len(),
        )
    }

    /// Clear an existing spill region for an anchor (set cells to Empty and forget ownership)
    pub fn clear_spill_region(&mut self, anchor: VertexId) {
        let _ = self.clear_spill_region_bulk(anchor);
    }

    /// Bulk clear an existing spill region for an anchor.
    ///
    /// This avoids calling `set_cell_value()` per spill child (which can trigger O(N*V)
    /// dependent scans when `edges.delta_size() > 0`). Instead, it clears values directly and
    /// performs a single dirty propagation over the affected spill children.
    ///
    /// Returns the previously registered spill cells (including the anchor cell) for callers that
    /// want to mirror/record deltas.
    pub fn clear_spill_region_bulk(&mut self, anchor: VertexId) -> Vec<CellRef> {
        let anchor_cell = self.get_cell_ref(anchor);
        let Some(cells) = self.spill_anchor_to_cells.remove(&anchor) else {
            return Vec::new();
        };

        // Remove ownership for all cells first.
        for cell in cells.iter() {
            self.spill_cell_to_anchor.remove(cell);
        }

        // Prepare a single arena value ref for Empty (only when caching is enabled).
        let empty_ref = if self.value_cache_enabled {
            Some(self.data_store.store_value(LiteralValue::Empty))
        } else {
            None
        };

        // Clear all spill children (excluding the anchor cell).
        let mut changed_vertices: Vec<VertexId> = Vec::new();
        for cell in cells.iter().copied() {
            let is_anchor = anchor_cell.map(|a| a == cell).unwrap_or(false);
            if is_anchor {
                continue;
            }
            let Some(&vid) = self.cell_to_vertex.get(&cell) else {
                continue;
            };
            // Ensure this vertex is a plain value cell.
            if self.vertex_formulas.remove(&vid).is_some() {
                // Be conservative: remove outgoing edges if this was a formula vertex.
                // This should be rare for spill children under normal policies.
                self.remove_dependent_edges(vid);
            }
            self.store.set_kind(vid, VertexKind::Cell);
            if let Some(er) = empty_ref {
                self.vertex_values.insert(vid, er);
            } else {
                self.vertex_values.remove(&vid);
            }
            self.store.set_dirty(vid, false);
            self.dirty_vertices.remove(&vid);
            changed_vertices.push(vid);
        }

        // Single dirty propagation for all changed spill children.
        if !changed_vertices.is_empty() {
            self.mark_dirty_many_value_cells(&changed_vertices);
        }

        cells
    }

    fn mark_dirty_many_value_cells(&mut self, vertex_ids: &[VertexId]) -> Vec<VertexId> {
        if vertex_ids.is_empty() {
            return Vec::new();
        }

        // Ensure reverse edges are usable (delta.in_edges is intentionally not delta-aware).
        if self.edges.delta_size() > 0 {
            self.edges.rebuild();
        }

        let mut affected: FxHashSet<VertexId> = FxHashSet::default();
        let mut to_visit: Vec<VertexId> = Vec::new();
        let mut visited_for_propagation: FxHashSet<VertexId> = FxHashSet::default();

        // Value sources are affected but not marked dirty themselves.
        for &src in vertex_ids {
            affected.insert(src);
        }

        // Collect initial direct dependents and name dependents.
        for &src in vertex_ids {
            to_visit.extend(self.edges.in_edges(src));
            if let Some(name_set) = self.cell_to_name_dependents.get(&src) {
                for &name_vertex in name_set {
                    to_visit.push(name_vertex);
                }
            }
        }

        // Collect range dependents in bulk using spill rect bounds per sheet.
        let mut bounds_by_sheet: FxHashMap<SheetId, (u32, u32, u32, u32)> = FxHashMap::default();
        for &src in vertex_ids {
            let view = self.store.view(src);
            let sid = view.sheet_id();
            let r = view.row();
            let c = view.col();
            bounds_by_sheet
                .entry(sid)
                .and_modify(|b| {
                    b.0 = b.0.min(r);
                    b.1 = b.1.max(r);
                    b.2 = b.2.min(c);
                    b.3 = b.3.max(c);
                })
                .or_insert((r, r, c, c));
        }

        for (sid, (sr, er, sc, ec)) in bounds_by_sheet {
            to_visit.extend(self.collect_range_dependents_for_rect(sid, sr, sc, er, ec));
        }

        while let Some(id) = to_visit.pop() {
            if !visited_for_propagation.insert(id) {
                continue;
            }
            affected.insert(id);
            self.store.set_dirty(id, true);
            to_visit.extend(self.edges.in_edges(id));
            to_visit.extend(self.collect_range_dependents_for_vertex(id));
        }

        self.dirty_vertices.extend(&affected);
        affected.into_iter().collect()
    }

    fn collect_range_dependents_for_vertex(&self, vertex_id: VertexId) -> Vec<VertexId> {
        match self.store.kind(vertex_id) {
            VertexKind::Cell
            | VertexKind::Empty
            | VertexKind::FormulaScalar
            | VertexKind::FormulaArray => {
                let view = self.store.view(vertex_id);
                self.collect_range_dependents_for_rect(
                    view.sheet_id(),
                    view.row(),
                    view.col(),
                    view.row(),
                    view.col(),
                )
            }
            _ => Vec::new(),
        }
    }

    fn collect_range_dependents_for_rect(
        &self,
        sheet_id: SheetId,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    ) -> Vec<VertexId> {
        if self.stripe_to_dependents.is_empty() {
            return Vec::new();
        }
        let mut candidates: FxHashSet<VertexId> = FxHashSet::default();

        for col in start_col..=end_col {
            let key = StripeKey {
                sheet_id,
                stripe_type: StripeType::Column,
                index: col,
            };
            if let Some(deps) = self.stripe_to_dependents.get(&key) {
                candidates.extend(deps);
            }
        }
        for row in start_row..=end_row {
            let key = StripeKey {
                sheet_id,
                stripe_type: StripeType::Row,
                index: row,
            };
            if let Some(deps) = self.stripe_to_dependents.get(&key) {
                candidates.extend(deps);
            }
        }
        if self.config.enable_block_stripes {
            let br0 = start_row / BLOCK_H;
            let br1 = end_row / BLOCK_H;
            let bc0 = start_col / BLOCK_W;
            let bc1 = end_col / BLOCK_W;
            for br in br0..=br1 {
                for bc in bc0..=bc1 {
                    let key = StripeKey {
                        sheet_id,
                        stripe_type: StripeType::Block,
                        index: block_index(br * BLOCK_H, bc * BLOCK_W),
                    };
                    if let Some(deps) = self.stripe_to_dependents.get(&key) {
                        candidates.extend(deps);
                    }
                }
            }
        }

        // Precision check: the dirty rect must overlap at least one of the formula's registered ranges.
        let mut out: Vec<VertexId> = Vec::new();
        for dep_id in candidates {
            let Some(ranges) = self.formula_to_range_deps.get(&dep_id) else {
                continue;
            };
            let mut hit = false;
            for range in ranges {
                let range_sheet_id = match range.sheet {
                    SharedSheetLocator::Id(id) => id,
                    _ => sheet_id,
                };
                if range_sheet_id != sheet_id {
                    continue;
                }
                let sr0 = range.start_row.map(|b| b.index).unwrap_or(0);
                let er0 = range.end_row.map(|b| b.index).unwrap_or(u32::MAX);
                let sc0 = range.start_col.map(|b| b.index).unwrap_or(0);
                let ec0 = range.end_col.map(|b| b.index).unwrap_or(u32::MAX);
                let overlap =
                    sr0 <= end_row && er0 >= start_row && sc0 <= end_col && ec0 >= start_col;
                if overlap {
                    hit = true;
                    break;
                }
            }
            if hit {
                out.push(dep_id);
            }
        }
        out
    }

    /// Check if a vertex exists
    pub(crate) fn vertex_exists(&self, vertex_id: VertexId) -> bool {
        if vertex_id.0 < FIRST_NORMAL_VERTEX {
            return false;
        }
        let index = (vertex_id.0 - FIRST_NORMAL_VERTEX) as usize;
        index < self.store.len()
    }

    /// Get the kind of a vertex
    pub(crate) fn get_vertex_kind(&self, vertex_id: VertexId) -> VertexKind {
        self.store.kind(vertex_id)
    }

    /// Get the sheet ID of a vertex
    pub(crate) fn get_vertex_sheet_id(&self, vertex_id: VertexId) -> SheetId {
        self.store.sheet_id(vertex_id)
    }

    pub fn get_formula_id(&self, vertex_id: VertexId) -> Option<AstNodeId> {
        self.vertex_formulas.get(&vertex_id).copied()
    }

    pub(crate) fn formula_vertices(&self) -> Vec<VertexId> {
        let mut vertices = self.vertex_formulas.keys().copied().collect::<Vec<_>>();
        vertices.sort_unstable();
        vertices
    }

    pub fn get_formula_id_and_volatile(&self, vertex_id: VertexId) -> Option<(AstNodeId, bool)> {
        let ast_id = self.get_formula_id(vertex_id)?;
        Some((ast_id, self.is_volatile(vertex_id)))
    }

    pub fn get_formula_node(&self, vertex_id: VertexId) -> Option<&super::arena::AstNodeData> {
        let ast_id = self.get_formula_id(vertex_id)?;
        self.data_store.get_node(ast_id)
    }

    pub fn get_formula_node_and_volatile(
        &self,
        vertex_id: VertexId,
    ) -> Option<(&super::arena::AstNodeData, bool)> {
        let (ast_id, vol) = self.get_formula_id_and_volatile(vertex_id)?;
        let node = self.data_store.get_node(ast_id)?;
        Some((node, vol))
    }

    /// Get the formula AST for a vertex.
    ///
    /// Not used in hot paths; reconstructs from arena.
    pub fn get_formula(&self, vertex_id: VertexId) -> Option<ASTNode> {
        let ast_id = self.get_formula_id(vertex_id)?;
        self.data_store.retrieve_ast(ast_id, &self.sheet_reg)
    }

    /// Get the value stored for a vertex
    pub fn get_value(&self, vertex_id: VertexId) -> Option<LiteralValue> {
        if !self.value_cache_enabled {
            // In canonical mode, cell/formula values must not be read from the graph.
            // Non-cell vertices (e.g. named ranges, external sources) may still use graph storage.
            match self.store.kind(vertex_id) {
                VertexKind::Cell
                | VertexKind::FormulaScalar
                | VertexKind::FormulaArray
                | VertexKind::Empty => {
                    #[cfg(debug_assertions)]
                    {
                        self.graph_value_read_attempts
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return None;
                }
                _ => {
                    // Allow non-cell vertices to use vertex_values.
                }
            }
        }
        self.vertex_values
            .get(&vertex_id)
            .map(|&value_ref| self.data_store.retrieve_value(value_ref))
    }

    /// Get the cell reference for a vertex
    pub(crate) fn get_cell_ref(&self, vertex_id: VertexId) -> Option<CellRef> {
        let packed_coord = self.store.coord(vertex_id);
        let sheet_id = self.store.sheet_id(vertex_id);
        let coord = Coord::new(packed_coord.row(), packed_coord.col(), true, true);
        Some(CellRef::new(sheet_id, coord))
    }

    /// Create a cell reference (helper for internal use)
    pub(crate) fn make_cell_ref_internal(&self, sheet_id: SheetId, row: u32, col: u32) -> CellRef {
        let coord = Coord::new(row, col, true, true);
        CellRef::new(sheet_id, coord)
    }

    /// Create a cell reference from sheet name and Excel 1-based coordinates.
    pub fn make_cell_ref(&self, sheet_name: &str, row: u32, col: u32) -> CellRef {
        let sheet_id = self.sheet_reg.get_id(sheet_name).unwrap_or(0);
        let coord = Coord::from_excel(row, col, true, true);
        CellRef::new(sheet_id, coord)
    }

    /// Check if a vertex is dirty
    pub(crate) fn is_dirty(&self, vertex_id: VertexId) -> bool {
        self.store.is_dirty(vertex_id)
    }

    /// Check if a vertex is volatile
    pub(crate) fn is_volatile(&self, vertex_id: VertexId) -> bool {
        self.store.is_volatile(vertex_id)
    }

    pub(crate) fn is_dynamic(&self, vertex_id: VertexId) -> bool {
        self.store.is_dynamic(vertex_id)
    }

    /// Get vertex ID for a cell address
    pub fn get_vertex_id_for_address(&self, addr: &CellRef) -> Option<&VertexId> {
        self.cell_to_vertex.get(addr)
    }

    #[cfg(test)]
    pub fn cell_to_vertex(
        &self,
    ) -> &std::collections::HashMap<CellRef, VertexId, CoordBuildHasher> {
        &self.cell_to_vertex
    }

    /// Borrow dependencies of a vertex when no pending edge delta exists.
    ///
    /// This enables zero-allocation traversal in hot scheduler paths.
    #[inline]
    pub(crate) fn dependencies_slice(&self, vertex_id: VertexId) -> Option<&[VertexId]> {
        self.edges.out_edges_ref(vertex_id)
    }

    /// Get the dependencies of a vertex (for scheduler)
    pub(crate) fn get_dependencies(&self, vertex_id: VertexId) -> Vec<VertexId> {
        self.edges.out_edges(vertex_id)
    }

    /// Check if a vertex has a self-loop
    pub(crate) fn has_self_loop(&self, vertex_id: VertexId) -> bool {
        if let Some(deps) = self.dependencies_slice(vertex_id) {
            deps.contains(&vertex_id)
        } else {
            self.edges.out_edges(vertex_id).contains(&vertex_id)
        }
    }

    /// Borrow dependents of a vertex when no pending edge delta exists.
    ///
    /// This enables zero-allocation traversal in hot scheduler paths.
    #[inline]
    pub(crate) fn dependents_slice(&self, vertex_id: VertexId) -> Option<&[VertexId]> {
        self.edges.in_edges_ref(vertex_id)
    }

    /// Get dependents of a vertex (vertices that depend on this vertex)
    /// Uses reverse edges for O(1) lookup when available
    pub(crate) fn get_dependents(&self, vertex_id: VertexId) -> Vec<VertexId> {
        // If there are pending changes in delta, we need to scan
        // Otherwise we can use the fast reverse edges
        if self.edges.delta_size() > 0 {
            #[cfg(test)]
            {
                // This scan is intentionally tracked for perf regression tests.
                // It is expected to be rare in normal operation.
                if let Ok(mut g) = self.instr.lock() {
                    g.dependents_scan_fallback_calls += 1;
                    g.dependents_scan_vertices_scanned += self.cell_to_vertex.len() as u64;
                }
            }
            // Fall back to scanning when delta has changes
            let mut dependents = Vec::new();
            for (&_addr, &vid) in &self.cell_to_vertex {
                let out_edges = self.edges.out_edges(vid);
                if out_edges.contains(&vertex_id) {
                    dependents.push(vid);
                }
            }
            for named in self.named_ranges.values() {
                let vid = named.vertex;
                let out_edges = self.edges.out_edges(vid);
                if out_edges.contains(&vertex_id) {
                    dependents.push(vid);
                }
            }
            for named in self.sheet_named_ranges.values() {
                let vid = named.vertex;
                let out_edges = self.edges.out_edges(vid);
                if out_edges.contains(&vertex_id) {
                    dependents.push(vid);
                }
            }
            dependents
        } else {
            // Fast path: use reverse edges from CSR
            self.edges.in_edges(vertex_id).to_vec()
        }
    }

    // Internal helper methods for Milestone 0.4

    /// Internal: Create a snapshot of vertex state for rollback
    #[doc(hidden)]
    pub fn snapshot_vertex(&self, id: VertexId) -> crate::engine::VertexSnapshot {
        let coord = self.store.coord(id);
        let sheet_id = self.store.sheet_id(id);
        let kind = self.store.kind(id);
        let flags = self.store.flags(id);

        // Get value and formula references
        let value_ref = self.vertex_values.get(&id).copied();
        let formula_ref = self.vertex_formulas.get(&id).copied();

        // Get outgoing edges (dependencies)
        let out_edges = self.get_dependencies(id);

        crate::engine::VertexSnapshot {
            coord,
            sheet_id,
            kind,
            flags,
            value_ref,
            formula_ref,
            out_edges,
        }
    }

    /// Internal: Remove all edges for a vertex
    #[doc(hidden)]
    pub fn remove_all_edges(&mut self, id: VertexId) {
        // Enter batch mode to avoid intermediate rebuilds
        self.edges.begin_batch();

        // Remove outgoing edges (this vertex's dependencies)
        self.remove_dependent_edges(id);

        // Force rebuild to get accurate dependents list
        // This is necessary because get_dependents uses CSR reverse edges
        self.edges.rebuild();

        // Remove incoming edges (vertices that depend on this vertex)
        let dependents = self.get_dependents(id);
        if self.pk_order.is_some()
            && let Some(mut pk) = self.pk_order.take()
        {
            for dependent in &dependents {
                pk.remove_edge(id, *dependent);
            }
            self.pk_order = Some(pk);
        }
        for dependent in dependents {
            self.edges.remove_edge(dependent, id);
        }

        // Exit batch mode and rebuild once with all changes
        self.edges.end_batch();
    }

    /// Internal: Mark vertex as having #REF! error
    #[doc(hidden)]
    pub fn mark_as_ref_error(&mut self, id: VertexId) {
        if !self.value_cache_enabled {
            match self.store.kind(id) {
                VertexKind::Cell
                | VertexKind::FormulaScalar
                | VertexKind::FormulaArray
                | VertexKind::Empty => {
                    self.ref_error_vertices.insert(id);
                    // Canonical-only: graph does not cache cell/formula values.
                    // Ensure the dependent subgraph is dirtied so evaluation updates Arrow truth.
                    self.vertex_values.remove(&id);
                    let _ = self.mark_dirty(id);
                    return;
                }
                _ => {
                    // Allow non-cell vertices to use cached values.
                }
            }
        }
        let error = LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref));
        let value_ref = self.data_store.store_value(error);
        self.vertex_values.insert(id, value_ref);
        let _ = self.mark_dirty(id);
    }

    /// Check if a vertex has a #REF! error
    pub fn is_ref_error(&self, id: VertexId) -> bool {
        if !self.value_cache_enabled {
            match self.store.kind(id) {
                VertexKind::Cell
                | VertexKind::FormulaScalar
                | VertexKind::FormulaArray
                | VertexKind::Empty => {
                    return self.ref_error_vertices.contains(&id);
                }
                _ => {
                    // Non-cell vertices may still have cached values.
                }
            }
        }
        if let Some(value_ref) = self.vertex_values.get(&id) {
            let value = self.data_store.retrieve_value(*value_ref);
            if let LiteralValue::Error(err) = value {
                return err.kind == ExcelErrorKind::Ref;
            }
        }
        false
    }

    /// Internal: Mark all direct dependents as dirty
    #[doc(hidden)]
    pub fn mark_dependents_dirty(&mut self, id: VertexId) {
        let dependents = self.get_dependents(id);
        for dep_id in dependents {
            self.store.set_dirty(dep_id, true);
            self.dirty_vertices.insert(dep_id);
        }
    }

    /// Internal: Mark a vertex as volatile
    #[doc(hidden)]
    pub fn mark_volatile(&mut self, id: VertexId, volatile: bool) {
        self.store.set_volatile(id, volatile);
        if volatile {
            self.volatile_vertices.insert(id);
        } else {
            self.volatile_vertices.remove(&id);
        }
    }

    /// Update vertex coordinate
    #[doc(hidden)]
    pub fn set_coord(&mut self, id: VertexId, coord: AbsCoord) {
        self.store.set_coord(id, coord);
    }

    /// Update edge cache coordinate
    #[doc(hidden)]
    pub fn update_edge_coord(&mut self, id: VertexId, coord: AbsCoord) {
        self.edges.update_coord(id, coord);
    }

    /// Mark vertex as deleted (tombstone)
    #[doc(hidden)]
    pub fn mark_deleted(&mut self, id: VertexId, deleted: bool) {
        self.store.mark_deleted(id, deleted);
    }

    /// Set vertex kind
    #[doc(hidden)]
    pub fn set_kind(&mut self, id: VertexId, kind: VertexKind) {
        self.store.set_kind(id, kind);
    }

    /// Set vertex dirty flag
    #[doc(hidden)]
    pub fn set_dirty(&mut self, id: VertexId, dirty: bool) {
        self.store.set_dirty(id, dirty);
        if dirty {
            self.dirty_vertices.insert(id);
        } else {
            self.dirty_vertices.remove(&id);
        }
    }

    /// Get vertex kind (for testing)
    #[cfg(test)]
    pub(crate) fn get_kind(&self, id: VertexId) -> VertexKind {
        self.store.kind(id)
    }

    /// Get vertex flags (for testing)
    #[cfg(test)]
    pub(crate) fn get_flags(&self, id: VertexId) -> u8 {
        self.store.flags(id)
    }

    /// Check if vertex is deleted (for testing)
    #[cfg(test)]
    pub(crate) fn is_deleted(&self, id: VertexId) -> bool {
        self.store.is_deleted(id)
    }

    /// Force edge rebuild (internal use)
    #[doc(hidden)]
    pub fn rebuild_edges(&mut self) {
        self.edges.rebuild();
    }

    /// Get delta size (internal use)
    #[doc(hidden)]
    pub fn edges_delta_size(&self) -> usize {
        self.edges.delta_size()
    }

    /// Get vertex ID for specific cell address
    pub fn get_vertex_for_cell(&self, addr: &CellRef) -> Option<VertexId> {
        self.cell_to_vertex.get(addr).copied()
    }

    /// Get coord for a vertex (public for VertexEditor)
    pub fn get_coord(&self, id: VertexId) -> AbsCoord {
        self.store.coord(id)
    }

    /// Get sheet_id for a vertex (public for VertexEditor)
    pub fn get_sheet_id(&self, id: VertexId) -> SheetId {
        self.store.sheet_id(id)
    }

    /// Get all vertices in a sheet
    pub fn vertices_in_sheet(&self, sheet_id: SheetId) -> impl Iterator<Item = VertexId> + '_ {
        self.store
            .all_vertices()
            .filter(move |&id| self.vertex_exists(id) && self.store.sheet_id(id) == sheet_id)
    }

    /// Does a vertex have a formula associated
    pub fn vertex_has_formula(&self, id: VertexId) -> bool {
        self.vertex_formulas.contains_key(&id)
    }

    /// Get all vertices with formulas
    pub fn vertices_with_formulas(&self) -> impl Iterator<Item = VertexId> + '_ {
        self.vertex_formulas.keys().copied()
    }

    /// Update a vertex's formula
    pub fn update_vertex_formula(&mut self, id: VertexId, ast: ASTNode) -> Result<(), ExcelError> {
        // Get the sheet_id for this vertex
        let sheet_id = self.store.sheet_id(id);

        // If the adjusted AST contains special #REF markers (from structural edits),
        // treat this as a REF error on the vertex instead of attempting to resolve.
        // This prevents failures when reference_adjuster injected placeholder refs.
        let has_ref_marker = ast.get_dependencies().into_iter().any(|r| {
            matches!(
                r,
                ReferenceType::Cell { sheet: Some(s), .. }
                    | ReferenceType::Range { sheet: Some(s), .. } if s == "#REF"
            )
        });
        if has_ref_marker {
            // Store the adjusted AST for round-tripping/display, but set value state to #REF!
            let ast_id = self.data_store.store_ast(&ast, &self.sheet_reg);
            self.vertex_formulas.insert(id, ast_id);
            self.mark_as_ref_error(id);
            self.store.set_kind(id, VertexKind::FormulaScalar);
            return Ok(());
        }

        // Extract dependencies from AST
        let (new_dependencies, new_range_dependencies, _, named_dependencies) =
            self.extract_dependencies(&ast, sheet_id)?;

        // Remove old dependencies first
        self.remove_dependent_edges(id);
        self.detach_vertex_from_names(id);

        // Store the new formula
        let ast_id = self.data_store.store_ast(&ast, &self.sheet_reg);
        self.vertex_formulas.insert(id, ast_id);

        // Add new dependency edges
        self.add_dependent_edges(id, &new_dependencies);
        self.add_range_dependent_edges(id, &new_range_dependencies, sheet_id);

        if !named_dependencies.is_empty() {
            self.attach_vertex_to_names(id, &named_dependencies);
        }

        // Mark as formula vertex
        self.store.set_kind(id, VertexKind::FormulaScalar);

        Ok(())
    }

    /// Mark a vertex as dirty without propagation (for VertexEditor)
    pub fn mark_vertex_dirty(&mut self, vertex_id: VertexId) {
        self.store.set_dirty(vertex_id, true);
        self.dirty_vertices.insert(vertex_id);
    }

    /// Batch-mark vertices dirty without propagation.
    pub fn mark_vertices_dirty_batch(&mut self, vertices: &[VertexId]) {
        self.dirty_vertices.reserve(vertices.len());
        for &vertex_id in vertices {
            self.store.set_dirty(vertex_id, true);
        }
        self.dirty_vertices.extend(vertices.iter().copied());
    }

    /// Update cell mapping for a vertex (for VertexEditor)
    pub fn update_cell_mapping(
        &mut self,
        id: VertexId,
        old_addr: Option<CellRef>,
        new_addr: CellRef,
    ) {
        // Remove old mapping if it exists
        if let Some(old) = old_addr {
            self.cell_to_vertex.remove(&old);
        }
        // Add new mapping
        self.cell_to_vertex.insert(new_addr, id);
    }

    /// Remove cell mapping (for VertexEditor)
    pub fn remove_cell_mapping(&mut self, addr: &CellRef) {
        self.cell_to_vertex.remove(addr);
    }

    /// Get the cell reference for a vertex
    pub fn get_cell_ref_for_vertex(&self, id: VertexId) -> Option<CellRef> {
        let coord = self.store.coord(id);
        let sheet_id = self.store.sheet_id(id);
        // Find the cell reference in the mapping
        let cell_ref = CellRef::new(sheet_id, Coord::new(coord.row(), coord.col(), true, true));
        // Verify it actually maps to this vertex
        if self.cell_to_vertex.get(&cell_ref) == Some(&id) {
            Some(cell_ref)
        } else {
            None
        }
    }

    /// Rebuild dependency edges/range links for an existing formula vertex after AST changes.
    ///
    /// This intentionally reuses the same extraction and edge wiring machinery as
    /// `set_cell_formula[_with_volatility]` to preserve edge orientation, placeholder
    /// behavior, and name/range dependency semantics.
    pub(crate) fn rebuild_formula_dependencies(&mut self, vertex_id: VertexId, ast: &ASTNode) {
        let sheet_id = self.store.sheet_id(vertex_id);

        // Remove old dependency, name, and pending-name links first.
        self.remove_dependent_edges(vertex_id);
        self.detach_vertex_from_names(vertex_id);
        self.clear_pending_name_references(vertex_id);

        let (
            new_dependencies,
            new_range_dependencies,
            _created_placeholders,
            named_dependencies,
            unresolved_names,
        ) = match self.extract_dependencies_with_pending_names(ast, sheet_id) {
            Ok(v) => v,
            Err(_) => {
                self.mark_as_ref_error(vertex_id);
                return;
            }
        };

        // Self-reference / name-cycle safety parity with set_cell_formula.
        if new_dependencies.contains(&vertex_id) {
            self.mark_as_ref_error(vertex_id);
            return;
        }

        for &name_vertex in &named_dependencies {
            let mut visited = FxHashSet::default();
            if self.name_depends_on_vertex(name_vertex, vertex_id, &mut visited) {
                self.mark_as_ref_error(vertex_id);
                return;
            }
        }

        // Formula is now recoverable again.
        self.ref_error_vertices.remove(&vertex_id);
        self.vertex_values.remove(&vertex_id);

        if !named_dependencies.is_empty() {
            self.attach_vertex_to_names(vertex_id, &named_dependencies);
        }
        for unresolved_name in &unresolved_names {
            self.record_pending_name_reference(sheet_id, unresolved_name, vertex_id);
        }

        self.add_dependent_edges(vertex_id, &new_dependencies);
        self.add_range_dependent_edges(vertex_id, &new_range_dependencies, sheet_id);
        let _ = self.mark_dirty(vertex_id);
    }
}

// ========== Sheet Management Operations ==========
