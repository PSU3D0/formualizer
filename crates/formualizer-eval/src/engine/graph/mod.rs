use crate::SheetId;
use crate::engine::TombstoneRegistry;
use crate::engine::named_range::{NameScope, NamedDefinition, NamedRange};
use crate::engine::sheet_registry::SheetRegistry;
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
pub(crate) mod authority_host;
pub mod editor;
mod extent_record;
mod formula_analysis;
#[cfg(test)]
mod formula_analysis_legacy_tests;
mod formula_dirty;
mod names;
pub(crate) mod prepared_legacy_graph;
mod range_deps;
pub(crate) use range_deps::{StructuralEdit, StructuralOccupancy};

mod sheets;
pub mod snapshot;
mod sources;
mod structural_runs;
mod tables;
pub(crate) mod virtual_members;

pub(crate) use structural_runs::ShiftedRun;
pub(crate) use tables::TableEntry;

use super::addr::{GridAddr, SymbolAddr, VertexAddr};
use super::arena::{AstNodeId, DataStore, ValueRef};
#[cfg(any(test, feature = "legacy_oracle"))]
use super::delta_edges::CsrMutableEdges;
use super::ingest_pipeline::{DependencyPlanRow, FormulaAstInput};
use super::sheet_index::SheetIndex;
use super::vertex::{VertexId, VertexKind};
use super::vertex_store::{FIRST_NORMAL_VERTEX, VertexStore};
#[cfg(any(test, feature = "legacy_oracle"))]
use crate::engine::topo::{
    GraphAdapter,
    pk::{DynamicTopo, PkConfig},
};
use crate::reference::{CellRef, Coord, SharedRangeRef, SharedRef, SharedSheetLocator};
use formualizer_common::Coord as AbsCoord;
use formula_dirty::FormulaDirtyState;
// topo::pk wiring will be integrated behind config.use_dynamic_topo in a follow-up step

struct RegistryFunctionProvider;

impl crate::traits::FunctionProvider for RegistryFunctionProvider {
    fn planning_semantic_revision(&self) -> Option<u64> {
        Some(0)
    }

    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        crate::function_registry::get(ns, name)
    }

    fn get_function_for_planning(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::function::Function>> {
        crate::function_registry::get_for_planning(ns, name)
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

/// How a formula vertex stores its formula (Program 2 compression).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FormulaRef {
    /// The vertex's own arena AST, valid at its cell.
    Own(AstNodeId),
    /// A family member: its formula is `template` (valid at `anchor`,
    /// 0-based) relocated to the member's cell, with the same literals and
    /// reference texts (checked when the member was compressed).
    Member {
        template: AstNodeId,
        anchor: (u32, u32),
    },
}

impl FormulaRef {
    /// The arena root the formula is read from (its own AST, or the shared
    /// template).
    #[inline]
    pub(crate) fn root(self) -> AstNodeId {
        match self {
            FormulaRef::Own(id) | FormulaRef::Member { template: id, .. } => id,
        }
    }

    /// An ingest pipeline result: own AST, or a load-time family member.
    #[inline]
    pub(crate) fn of_ingested(ast_id: AstNodeId, member_anchor: Option<(u32, u32)>) -> Self {
        match member_anchor {
            None => FormulaRef::Own(ast_id),
            Some(anchor) => FormulaRef::Member {
                template: ast_id,
                anchor,
            },
        }
    }

    /// The vertex's own AST, if it has one.
    #[inline]
    pub(crate) fn own(self) -> Option<AstNodeId> {
        match self {
            FormulaRef::Own(id) => Some(id),
            FormulaRef::Member { .. } => None,
        }
    }
}

/// The formula of every formula vertex: a per-vertex map, plus the
/// virtual family members (`virtual_members`), which are in no per-cell
/// map. Writes go through `insert`/`remove`, which also record the touched
/// vertex so the authority can follow formula edits (Program 1 M1a).
/// Compressing a vertex to a family member, or moving a member between the
/// map and a virtual run, is not a formula change and is not recorded.
#[derive(Debug, Default)]
pub(crate) struct FormulaMap {
    map: FxHashMap<VertexId, FormulaRef>,
    virt: virtual_members::VirtualMembers,
    touched: Vec<VertexId>,
}

impl FormulaMap {
    /// The vertex's formula (own AST or family member).
    #[inline]
    pub(crate) fn get(&self, vertex: &VertexId) -> Option<FormulaRef> {
        match self.map.get(vertex) {
            Some(&f) => Some(f),
            None => self.virt.by_vertex(*vertex).map(|m| m.formula),
        }
    }

    #[inline]
    pub(crate) fn contains_key(&self, vertex: &VertexId) -> bool {
        self.map.contains_key(vertex) || self.virt.contains_vertex(*vertex)
    }

    /// Formula vertices (map and virtual).
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.map.len() + self.virt.len()
    }

    /// Every formula vertex with its formula: the map (unordered), then the
    /// virtual members (by id).
    pub(crate) fn iter(&self) -> impl Iterator<Item = (VertexId, FormulaRef)> + '_ {
        self.map
            .iter()
            .map(|(&v, &f)| (v, f))
            .chain(self.virt.iter().map(|m| (m.vertex, m.formula)))
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = VertexId> + '_ {
        self.iter().map(|(v, _)| v)
    }

    /// Formulas held by the per-vertex map only (not virtual members).
    /// Every stored arena root: the map's, and one template per virtual
    /// run.
    pub(crate) fn roots(&self) -> impl Iterator<Item = AstNodeId> + '_ {
        self.map
            .values()
            .map(|f| f.root())
            .chain(self.virt.runs().map(|r| r.template))
    }

    pub(crate) fn map_iter(&self) -> impl Iterator<Item = (VertexId, FormulaRef)> + '_ {
        self.map.iter().map(|(&v, &f)| (v, f))
    }

    #[inline]
    pub(crate) fn virtual_members(&self) -> &virtual_members::VirtualMembers {
        &self.virt
    }

    #[inline]
    pub(crate) fn virtual_members_mut(&mut self) -> &mut virtual_members::VirtualMembers {
        &mut self.virt
    }

    /// Give a virtual member its map entry back (same formula; not
    /// touched). Returns the member (the caller restores its cell maps).
    #[inline]
    pub(crate) fn materialize(
        &mut self,
        vertex: VertexId,
    ) -> Option<virtual_members::VirtualMember> {
        let m = self.virt.take(vertex)?;
        self.map.insert(vertex, m.formula);
        Some(m)
    }

    /// Drop the map entry of a just-materialized load member that is about
    /// to be assigned (not touched).
    pub(crate) fn forget_materialized(&mut self, vertex: &VertexId) {
        self.map.remove(vertex);
    }

    /// Re-insert a drained virtual member's formula (not touched).
    #[inline]
    pub(crate) fn restore(&mut self, vertex: VertexId, formula: FormulaRef) {
        self.map.insert(vertex, formula);
    }

    #[inline]
    pub(crate) fn insert(&mut self, vertex: VertexId, ast: AstNodeId) -> Option<FormulaRef> {
        self.insert_ref(vertex, FormulaRef::Own(ast))
    }

    /// Set a vertex's formula (own AST or family member).
    #[inline]
    pub(crate) fn insert_ref(
        &mut self,
        vertex: VertexId,
        formula: FormulaRef,
    ) -> Option<FormulaRef> {
        debug_assert!(
            !self.virt.contains_vertex(vertex),
            "formula write to a virtual family member (materialize it first)"
        );
        self.touched.push(vertex);
        self.map.insert(vertex, formula)
    }

    /// Replace a vertex's own AST with a family reference (same formula).
    #[inline]
    pub(crate) fn compress(&mut self, vertex: VertexId, template: AstNodeId, anchor: (u32, u32)) {
        if let Some(slot) = self.map.get_mut(&vertex) {
            *slot = FormulaRef::Member { template, anchor };
        }
    }

    /// Replace a member reference with the member's own (instantiated) AST
    /// (same formula; not recorded as touched).
    #[inline]
    pub(crate) fn decompress(&mut self, vertex: VertexId, own: AstNodeId) {
        debug_assert!(!self.virt.contains_vertex(vertex));
        if let Some(slot) = self.map.get_mut(&vertex) {
            *slot = FormulaRef::Own(own);
        }
    }

    /// Remap every stored arena id (after an arena compaction).
    pub(crate) fn remap(&mut self, map: &impl Fn(AstNodeId) -> AstNodeId) {
        for slot in self.map.values_mut() {
            *slot = match *slot {
                FormulaRef::Own(id) => FormulaRef::Own(map(id)),
                FormulaRef::Member { template, anchor } => FormulaRef::Member {
                    template: map(template),
                    anchor,
                },
            };
        }
        self.virt.remap(map);
    }

    #[inline]
    pub(crate) fn remove(&mut self, vertex: &VertexId) -> Option<FormulaRef> {
        debug_assert!(
            !self.virt.contains_vertex(*vertex),
            "formula removal of a virtual family member (materialize it first)"
        );
        let old = self.map.remove(vertex);
        if old.is_some() {
            self.touched.push(*vertex);
        }
        old
    }

    #[inline]
    pub(crate) fn reserve(&mut self, additional: usize) {
        self.map.reserve(additional);
    }

    /// Drop the map entries of vertices that are now virtual members
    /// (`is_virtual`: the store's flag; one pass) and give back the
    /// capacity.
    pub(crate) fn drop_virtual_from_map(&mut self, is_virtual: impl Fn(VertexId) -> bool) {
        debug_assert!(
            self.map
                .keys()
                .all(|&v| is_virtual(v) == self.virt.contains_vertex(v))
        );
        self.map.retain(|&v, _| !is_virtual(v));
        self.map.shrink_to_fit();
    }

    pub(crate) fn map_capacity(&self) -> usize {
        self.map.capacity()
    }

    /// Vertices whose formula changed since the last call.
    pub(crate) fn take_touched(&mut self) -> Vec<VertexId> {
        std::mem::take(&mut self.touched)
    }

    /// Record a vertex whose dependencies were re-derived without a formula
    /// change (a pending symbol became bound).
    pub(crate) fn touch(&mut self, vertex: VertexId) {
        self.touched.push(vertex);
    }

    pub(crate) fn has_touched(&self) -> bool {
        !self.touched.is_empty()
    }
}

/// A formula cell's formula as a template plus offset (design §11).
///
/// Evaluating or rendering `template` with the interpreter's reference
/// offset `(row_delta, col_delta)` yields exactly this cell's formula.
/// `template` alone is the formula of the family's anchor cell and is
/// shared by every member; for a formula stored on its own cell the deltas
/// are zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct FormulaView {
    pub template: AstNodeId,
    pub row_delta: i64,
    pub col_delta: i64,
}

/// SoA-based dependency graph implementation
#[derive(Debug)]
pub struct DependencyGraph {
    // Core columnar storage
    store: VertexStore,

    // Edge storage with delta slab
    #[cfg(any(test, feature = "legacy_oracle"))]
    edges: CsrMutableEdges,
    /// Direct dependency edges legacy would hold (the sum of the per-vertex
    /// counts kept in the vertex store's `edge_offset` column): admission's
    /// `GraphEdges` measure, maintained without the CSR.
    dep_edge_total: usize,
    /// Formulas with `VertexStore::reads_range` set.
    range_reader_count: usize,
    /// Old (lower-cased) name -> sheet, for sheets renamed away from it:
    /// name formulas that still spell it keep their edges (see
    /// `rename_sheet`).
    renamed_sheet_aliases: FxHashMap<String, SheetId>,

    // Arena-based value and formula storage
    data_store: DataStore,
    vertex_values: FxHashMap<VertexId, ValueRef>,
    vertex_formulas: FormulaMap,

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

    /// Vertices removed per cell, revived when replay re-creates the cell
    /// (decision 9 as amended in Program 2: undo restores the id).
    vertex_journal: crate::engine::authority::history::IdJournal,

    // Graph-owned formula dirtiness. Legacy vertices retain their sparse bits
    // and set representation behind this single authority.
    formula_dirty: FormulaDirtyState,
    volatile_vertices: FxHashSet<VertexId>,

    /// Monotonic count of vertices processed by dirty-propagation BFS loops
    /// (`mark_dirty_many` / `mark_dirty_many_value_cells`). Cheap plain
    /// counter used by perf-shape tests to assert propagation work is
    /// O(component), not O(sources × component).
    dirty_propagation_visits: u64,

    /// Nesting depth of active deferred-dirty scopes (`begin_deferred_dirty`
    /// / `end_deferred_dirty`). While > 0, dirty-propagation entry points
    /// queue their sources in `deferred_dirty_pending` instead of running a
    /// BFS per call; the outermost `end_deferred_dirty` flushes the union in
    /// ONE multi-source `mark_dirty_many`.
    deferred_dirty_depth: u32,
    /// Sources queued while a deferred-dirty scope is active.
    deferred_dirty_pending: Vec<VertexId>,
    /// Decision 27 (option B): the retired id of each cell whose formula
    /// was replaced by a value, keyed `(sheet, row0, col0)`. Value ->
    /// formula at the cell takes it back; structural edits shift it; a
    /// delete drops the band's entries into `vertex_journal`.
    retired_ids: std::collections::BTreeMap<(SheetId, u32, u32), VertexId>,
    /// The ids in `retired_ids` (their tombstones stay out of grid scans:
    /// structural edits must not move or log them).
    retired_id_set: FxHashSet<VertexId>,
    /// Entries each journaled structural delete dropped, most recent last
    /// (history is LIFO: an undone delete takes its batch back). A delete
    /// without a change logger cannot be undone and pushes nothing, so the
    /// stack only grows with the history that can pop it.
    retired_dropped: Vec<RetiredBatch>,
    /// Entries each undone insert dropped from its band (a redo of the
    /// insert takes them back).
    retired_dropped_by_undo: Vec<RetiredBatch>,
    /// Cells the legacy graph gave a vertex without a formula (placeholders,
    /// value cells, spill children): the graph's used extent counts them.
    extent_record: extent_record::ExtentRecord,
    /// The extent cells each journaled structural delete dropped, most
    /// recent last: undo of the delete shifts the record back and restores
    /// them (delta-sized: an insert or a delete of an unrecorded band keeps
    /// no cells). Like `retired_dropped`, only journaled deletes push.
    extent_dropped: Vec<Vec<extent_record::ExtentRun>>,
    /// Structural edits whose extent the backward replay already put back
    /// at the edit's end marker (`undo_structural_extent`), innermost last:
    /// their start marker must not shift it again.
    extent_undone: Vec<(u8, SheetId, u32, u32)>,
    /// The same for cell/rectangle seeds (value edits: no vertex, decision 27).
    deferred_dirty_pending_rects: Vec<(u16, crate::engine::authority::geom::Rect)>,

    /// Vertices explicitly marked as #REF! by structural operations.
    ///
    /// In Arrow-truth mode, the dependency graph does not cache cell/formula values.
    /// We still need a place to record deterministic #REF! invalidations for editor
    /// operations and structural transforms.
    ref_error_vertices: FxHashSet<VertexId>,

    // NEW: Specialized managers for range dependencies (Hybrid Model)
    /// Maps a formula vertex to the ranges it depends on.
    #[cfg(any(test, feature = "legacy_oracle"))]
    formula_to_range_deps: FxHashMap<VertexId, Vec<SharedRangeRef<'static>>>,

    /// Maps a stripe to formulas that depend on it via a compressed range.
    /// CRITICAL: VertexIds are deduplicated within each stripe to avoid quadratic blow-ups.
    #[cfg(any(test, feature = "legacy_oracle"))]
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
    #[cfg(any(test, feature = "legacy_oracle"))]
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

    /// Monotonic allocator for the symbol address space.
    ///
    /// Names, tables and external sources are identified by name and have no position, so
    /// they are addressed by a dense index here rather than by fabricated grid coordinates
    /// on a real sheet (#302, #304).
    symbol_vertex_seq: u32,

    /// Mapping from cell vertices to named range vertices that depend on them
    #[cfg(any(test, feature = "legacy_oracle"))]
    cell_to_name_dependents: FxHashMap<VertexId, FxHashSet<VertexId>>,
    /// Cached list of cell dependencies per named range vertex (for teardown)
    #[cfg(any(test, feature = "legacy_oracle"))]
    name_to_cell_dependencies: FxHashMap<VertexId, Vec<VertexId>>,
    /// Oracle edges to cells without a vertex (decision 27): readers per
    /// cell, and cells per reader. A vertex created at such a cell gets its
    /// readers' oracle edges then, as a placeholder used to have them.
    #[cfg(any(test, feature = "legacy_oracle"))]
    oracle_vertexless_readers: FxHashMap<(SheetId, u32, u32), Vec<VertexId>>,
    #[cfg(any(test, feature = "legacy_oracle"))]
    oracle_vertexless_of: FxHashMap<VertexId, Vec<(SheetId, u32, u32)>>,

    // Evaluation configuration
    config: super::EvalConfig,
    /// Low-level monotonic dependency-topology revision used by engine caches.
    topology_revision: u64,
    /// Monotonic name, table, and external-source binding revision.
    symbol_revision: u64,

    /// Program 1 unified authority, maintained beside the legacy graph
    /// while the feature is in development (never default).
    authority: crate::engine::authority::host::AuthorityHost,

    // Dynamic topology orderer (Pearce–Kelly) maintained alongside edges when enabled
    #[cfg(any(test, feature = "legacy_oracle"))]
    pk_order: Option<DynamicTopo<VertexId>>,

    // Spill registry: anchor -> cells, and reverse mapping for blockers.
    // `spill_cell_to_anchor` is keyed by `CellRef` and uses the tuned hasher
    // for the same reason as `cell_to_vertex`.
    spill_anchor_to_cells: FxHashMap<VertexId, Vec<CellRef>>,
    spill_cell_to_anchor: std::collections::HashMap<CellRef, VertexId, CoordBuildHasher>,
    spill_cells_by_sheet: FxHashMap<SheetId, std::collections::BTreeMap<(u32, u32), VertexId>>,

    /// Request-scoped admission budgets used by graph-owned mutation paths.
    admission_budget_override: Option<crate::engine::EvaluationBudgets>,

    // Hint: during initial bulk load, many cells are guaranteed new; allow skipping existence checks per-sheet
    first_load_assume_new: bool,
    ensure_touched_sheets: FxHashSet<SheetId>,

    // handled deleted references, in case they are reintroduced.
    pub tombstone_registry: TombstoneRegistry,

    #[cfg(test)]
    instr: std::sync::Mutex<GraphInstrumentation>,
    #[cfg(test)]
    prepared_legacy_graph_failure_for_test: bool,
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

    /// Formula vertices, virtual members included.
    pub(crate) fn formula_vertex_count(&self) -> usize {
        self.vertex_formulas.len()
    }

    pub(crate) fn clear_formula_vertex_dirty(&mut self, vertex_id: VertexId) {
        self.store.set_dirty(vertex_id, false);
        self.formula_dirty.legacy_remove(&vertex_id);
    }

    /// Return read-only baseline counters for FormulaPlane/dispatch benchmarking.
    pub fn baseline_stats(&self) -> GraphBaselineStats {
        let data_stats = self.data_store.memory_usage();
        GraphBaselineStats {
            graph_vertex_count: self.store.len(),
            graph_formula_vertex_count: self.vertex_formulas.len(),
            graph_edge_count: self.dep_edge_total,
            dirty_vertex_count: self.formula_dirty.legacy_len(),
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
    ) -> Vec<(VertexAddr, u32)> {
        self.ensure_vertices_batch_ordered(coords).1
    }

    /// Ensure vertices exist for given packed absolute cells and return vertex ids aligned to the
    /// input order, plus the newly allocated `(coord, raw_vid)` items suitable for edge/index
    /// population.
    pub fn ensure_vertices_batch_packed_ordered(
        &mut self,
        packed_cells: &[PackedSheetCell],
    ) -> (Vec<VertexId>, Vec<(VertexAddr, u32)>) {
        let mut unmapped = vec![false; packed_cells.len()];
        self.ensure_vertices_batch_packed_ordered_unmapped(packed_cells, &mut unmapped)
    }

    /// [`Self::ensure_vertices_batch_packed_ordered`] where `unmapped[i]`
    /// asks that cell `i`, if it needs a new vertex, get none of the cell
    /// map / sheet index entries (the caller installs it as a virtual
    /// family member, or maps it with [`Self::map_load_unmapped`]). On
    /// return `unmapped[i]` holds exactly for the new vertices left
    /// unmapped.
    pub(crate) fn ensure_vertices_batch_packed_ordered_unmapped(
        &mut self,
        packed_cells: &[PackedSheetCell],
        unmapped: &mut [bool],
    ) -> (Vec<VertexId>, Vec<(VertexAddr, u32)>) {
        debug_assert_eq!(unmapped.len(), packed_cells.len());
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
        let mut add_batch: Vec<(VertexAddr, u32)> = Vec::new();

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
                    unmapped[idx] = false;
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
                if let Some(existing) = self.cell_vertex(&addr) {
                    ordered[idx] = Some(existing);
                    unmapped[idx] = false;
                    // A virtual member stays out of the cell maps.
                    if self.first_load_assume_new && !self.is_virtual_member(existing) {
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

                let mut pcs: Vec<VertexAddr> = Vec::with_capacity(missing_items.len());
                for (_, packed) in &missing_items {
                    pcs.push(GridAddr::new(packed.row0(), packed.col0()).into());
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
                            add_batch.push((VertexAddr::grid(GridAddr::from_coord(pc)), vid.0));
                            if unmapped[input_idx] {
                                continue;
                            }

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
                            self.sheet_index_mut(sid)
                                .add_vertex(GridAddr::from_coord(pc), vid);
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
                            add_batch.push((VertexAddr::grid(GridAddr::from_coord(pc)), vid.0));
                            if unmapped[input_idx] {
                                continue;
                            }

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
                    unmapped[idx] = false;
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
                if let Some(existing) = self.cell_vertex(&addr) {
                    ordered[idx] = Some(existing);
                    unmapped[idx] = false;
                    // A virtual member stays out of the cell maps.
                    if self.first_load_assume_new && !self.is_virtual_member(existing) {
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

                let mut pcs: Vec<VertexAddr> = Vec::with_capacity(items.len());
                for (_, packed) in &items {
                    pcs.push(GridAddr::new(packed.row0(), packed.col0()).into());
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
                    add_batch.push((VertexAddr::grid(GridAddr::from_coord(pc)), vid.0));
                    if unmapped[input_idx] {
                        continue;
                    }

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
                            self.sheet_index_mut(sid)
                                .add_vertex(GridAddr::from_coord(pc), vid);
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
            #[cfg(any(test, feature = "legacy_oracle"))]
            {
                self.edges.add_vertices_batch(&add_batch);
                let created: FxHashSet<u32> = if self.oracle_vertexless_readers.is_empty() {
                    FxHashSet::default()
                } else {
                    add_batch.iter().map(|&(_, raw)| raw).collect()
                };
                for (i, packed) in packed_cells.iter().enumerate() {
                    if let Some(v) = ordered[i]
                        && created.contains(&v.0)
                    {
                        self.oracle_cell_vertex_created(
                            (packed.sheet_id(), packed.row0(), packed.col0()),
                            v,
                        );
                    }
                }
            }
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
    ) -> (Vec<VertexId>, Vec<(VertexAddr, u32)>) {
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
        // Take the map so its allocation is released after the flush: it is
        // only used during a first load and would otherwise keep its capacity
        // (about 60 B per loaded formula) for the life of the graph.
        let packed_mappings = std::mem::replace(
            &mut self.load_packed_to_vertex,
            std::collections::HashMap::with_hasher(CoordBuildHasher),
        );
        for (packed, vid) in packed_mappings {
            let coord = AbsCoord::new(packed.row0(), packed.col0());
            let addr = CellRef::new(
                packed.sheet_id(),
                Coord::new(coord.row(), coord.col(), true, true),
            );
            self.cell_to_vertex.insert(addr, vid);
        }
        if debug {
            eprintln!(
                "[fz][load] flush_load_packed_mappings: {} entries in {:.1} ms",
                count,
                t0.elapsed().as_secs_f64() * 1000.0,
            );
        }
    }

    /// Enable/disable the first-load fast path for value inserts.
    ///
    /// Leaving the load scope builds the dependency authority once (it was
    /// not synced during the load; see `authority_load_skips_closures`).
    pub fn set_first_load_assume_new(&mut self, enabled: bool) {
        let leaving = self.first_load_assume_new && !enabled;
        if leaving {
            self.flush_load_packed_mappings();
            // Loads grow the vertex columns by doubling: drop the slack
            // (up to half of every column) once the load is complete.
            self.store.shrink_to_fit();
            // The load's extent notes (value cells, referenced cells) are
            // folded here, not by whichever edit next crosses the fold
            // threshold (up to one pending cell per run: tens of µs).
            self.extent_record.fold_pending();
        } else if enabled {
            self.load_packed_to_vertex.clear();
        }
        self.first_load_assume_new = enabled;
        if leaving {
            self.authority_sync();
        }
    }

    /// Program 2 compression (`EvalConfig::formula_compression`).
    pub(crate) fn formula_compression_enabled(&self) -> bool {
        self.config.formula_compression
    }

    #[doc(hidden)]
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
        self.formula_dirty.legacy_reserve(additional);
        self.volatile_vertices.reserve(additional);
    }

    /// Lookup VertexId for a (SheetId, AbsCoord)
    pub fn vid_for_sid_pc(&self, sid: SheetId, pc: AbsCoord) -> Option<VertexId> {
        let addr = CellRef::new(sid, Coord::new(pc.row(), pc.col(), true, true));
        self.cell_vertex(&addr)
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
        self.assign_formula_ref(vid, FormulaRef::Own(ast_id), volatile, dynamic);
    }

    /// [`Self::assign_formula_vertex`] for an own AST or a family member.
    pub(crate) fn assign_formula_ref(
        &mut self,
        vid: VertexId,
        formula: FormulaRef,
        volatile: bool,
        dynamic: bool,
    ) {
        self.materialize_vertex(vid);
        if self.vertex_formulas.contains_key(&vid) {
            self.remove_dependent_edges(vid);
        }
        self.store
            .set_kind(vid, crate::engine::vertex::VertexKind::FormulaScalar);
        self.vertex_values.remove(&vid);
        self.vertex_formulas.insert_ref(vid, formula);
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
        self.assign_formula_ref_load_fast(vid, FormulaRef::Own(ast_id), volatile, dynamic);
    }

    /// [`Self::assign_formula_vertex_load_fast`] for an own AST or a family
    /// member.
    pub(crate) fn assign_formula_ref_load_fast(
        &mut self,
        vid: VertexId,
        formula: FormulaRef,
        volatile: bool,
        dynamic: bool,
    ) {
        self.materialize_vertex(vid);
        debug_assert!(
            !self.vertex_formulas.contains_key(&vid),
            "load-fast formula assignment expects fresh/non-formula vertices"
        );
        self.store
            .set_kind(vid, crate::engine::vertex::VertexKind::FormulaScalar);
        self.vertex_values.remove(&vid);
        self.vertex_formulas.insert_ref(vid, formula);
        self.mark_volatile(vid, volatile);
        self.store.set_dynamic(vid, dynamic);
    }

    /// Load-time assignment of a family member whose new vertex was left
    /// unmapped (`ensure_vertices_batch_packed_ordered_unmapped`): kind and
    /// flags as [`Self::assign_formula_ref_load_fast`], the formula held
    /// back for [`Self::install_load_members`].
    pub(crate) fn assign_unmapped_member_load_fast(&mut self, vid: VertexId) {
        self.store
            .set_kind(vid, crate::engine::vertex::VertexKind::FormulaScalar);
        self.store.set_dynamic(vid, false);
        self.vertex_formulas.touch(vid);
    }

    /// First load: give every formula target of `sheet` (1-based row,
    /// col; the load-time family formula when known) its vertex now, in
    /// (col, row) order, so a column's new formula vertices are one id run.
    /// Family members (compression on) become virtual runs with their
    /// formula already set; other targets are mapped as empty vertices and
    /// get their formula when their chunk is planned.
    /// Returns the number of vertices created.
    pub(crate) fn preallocate_load_targets(
        &mut self,
        sheet: SheetId,
        mut targets: Vec<(u32, u32, Option<FormulaRef>)>,
    ) -> usize {
        if targets.is_empty() {
            return 0;
        }
        // (col, row); the last staging of a cell wins.
        targets.sort_by_key(|&(r, c, _)| (c, r));
        let mut dedup: Vec<(u32, u32, Option<FormulaRef>)> = Vec::with_capacity(targets.len());
        for t in targets {
            match dedup.last_mut() {
                Some(last) if (last.0, last.1) == (t.0, t.1) => *last = t,
                _ => dedup.push(t),
            }
        }
        let compress = self.config.formula_compression;
        let mut packed = Vec::with_capacity(dedup.len());
        let mut unmapped = Vec::with_capacity(dedup.len());
        for &(r, c, f) in &dedup {
            let Some(p) = PackedSheetCell::try_from_excel_1based(sheet, r, c) else {
                // Out of range: leave the cell to the chunk (it errors there).
                continue;
            };
            packed.push(p);
            unmapped.push(compress && matches!(f, Some(FormulaRef::Member { .. })));
        }
        let formulas: Vec<Option<FormulaRef>> = dedup
            .iter()
            .filter(|&&(r, c, _)| PackedSheetCell::try_from_excel_1based(sheet, r, c).is_some())
            .map(|&(_, _, f)| f)
            .collect();
        let (vids, created) =
            self.ensure_vertices_batch_packed_ordered_unmapped(&packed, &mut unmapped);
        let mut members = Vec::new();
        for (i, &v) in vids.iter().enumerate() {
            if unmapped[i]
                && let Some(f) = formulas[i]
            {
                members.push((v, sheet, packed[i].row0(), packed[i].col0(), f));
            }
        }
        self.install_preallocated_members(members);
        created.len()
    }

    /// [`Self::install_load_members`] for pre-allocated targets: runs get
    /// their formula and become virtual; the others are mapped without a
    /// formula (their chunk assigns it).
    fn install_preallocated_members(
        &mut self,
        members: Vec<(VertexId, SheetId, u32, u32, FormulaRef)>,
    ) {
        let mut i = 0;
        while i < members.len() {
            let (v, sheet, row, col, f) = members[i];
            let mut len = 1usize;
            while let Some(&(v2, s2, r2, c2, f2)) = members.get(i + len)
                && v2.0 == v.0 + len as u32
                && s2 == sheet
                && c2 == col
                && r2 == row + len as u32
                && f2 == f
            {
                len += 1;
            }
            match f {
                FormulaRef::Member { template, anchor } if len >= 2 => {
                    let run = virtual_members::MemberRun {
                        sheet,
                        col,
                        row0: row,
                        len: len as u32,
                        first: v.0,
                        template,
                        anchor,
                    };
                    for (m, _) in run.members() {
                        self.store
                            .set_kind(m, crate::engine::vertex::VertexKind::FormulaScalar);
                        self.store.set_virtual(m, true);
                        self.vertex_formulas.touch(m);
                    }
                    self.vertex_formulas.virtual_members_mut().insert(run);
                }
                _ => {
                    for &(m, s, r, c, _) in &members[i..i + len] {
                        self.map_load_vertex(m, s, r, c);
                    }
                }
            }
            i += len;
        }
    }

    /// Map a load vertex at its cell as the load's ensure step would.
    fn map_load_vertex(&mut self, v: VertexId, sheet: SheetId, row: u32, col: u32) {
        if self.first_load_assume_new {
            let packed = Self::packed_cell_key(sheet, AbsCoord::new(row, col));
            self.load_packed_to_vertex.insert(packed, v);
        } else {
            self.cell_to_vertex
                .insert(CellRef::new(sheet, Coord::new(row, col, true, true)), v);
        }
        if self.config.sheet_index_mode != crate::engine::SheetIndexMode::Lazy {
            self.sheet_index_mut(sheet)
                .add_vertex(GridAddr::new(row, col), v);
        }
    }

    /// A chunk reaches pre-allocated virtual member `vid` with `formula`: it
    /// stays virtual when that is its run's formula and it needs no
    /// per-vertex state; otherwise it is materialized and assigned as any
    /// loaded formula.
    pub(crate) fn assign_preallocated_member(
        &mut self,
        vid: VertexId,
        formula: FormulaRef,
        volatile: bool,
        dynamic: bool,
    ) {
        if !volatile && !dynamic && self.vertex_formulas.get(&vid) == Some(formula) {
            return;
        }
        self.materialize_vertex(vid);
        self.vertex_formulas.forget_materialized(&vid);
        self.assign_formula_ref_load_fast(vid, formula, volatile, dynamic);
    }

    /// Install the unmapped members of one load chunk: `(vertex, sheet,
    /// row, col, formula)`, 0-based. Runs of consecutive ids down a column
    /// with one template become virtual; any other member is mapped like
    /// every loaded formula (cell map, sheet index, formula map).
    pub(crate) fn install_load_members(
        &mut self,
        mut members: Vec<(VertexId, SheetId, u32, u32, FormulaRef)>,
    ) {
        if members.is_empty() {
            return;
        }
        // The last assignment of a vertex wins (a cell staged twice).
        members.sort_by_key(|m| m.0);
        members.reverse();
        members.dedup_by_key(|m| m.0);
        members.reverse();
        let mut runs: Vec<virtual_members::MemberRun> = Vec::new();
        let mut singles: Vec<usize> = Vec::new();
        let mut i = 0;
        while i < members.len() {
            let (v, sheet, row, col, f) = members[i];
            let mut len = 1usize;
            if let FormulaRef::Member { template, anchor } = f {
                while let Some(&(v2, s2, r2, c2, f2)) = members.get(i + len)
                    && v2.0 == v.0 + len as u32
                    && s2 == sheet
                    && c2 == col
                    && r2 == row + len as u32
                    && f2 == f
                {
                    len += 1;
                }
                if len >= 2 {
                    runs.push(virtual_members::MemberRun {
                        sheet,
                        col,
                        row0: row,
                        len: len as u32,
                        first: v.0,
                        template,
                        anchor,
                    });
                    i += len;
                    continue;
                }
            }
            singles.push(i);
            i += 1;
        }
        for i in singles {
            let (v, sheet, row, col, f) = members[i];
            self.vertex_formulas.restore(v, f);
            if self.first_load_assume_new {
                let packed = Self::packed_cell_key(sheet, AbsCoord::new(row, col));
                self.load_packed_to_vertex.insert(packed, v);
            } else {
                self.cell_to_vertex
                    .insert(CellRef::new(sheet, Coord::new(row, col, true, true)), v);
            }
            if self.config.sheet_index_mode != crate::engine::SheetIndexMode::Lazy {
                self.sheet_index_mut(sheet)
                    .add_vertex(GridAddr::new(row, col), v);
            }
        }
        for r in runs {
            for (v, _) in r.members() {
                self.store.set_virtual(v, true);
            }
            self.vertex_formulas.virtual_members_mut().insert(r);
        }
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Public wrapper for adding edges without beginning a batch (caller manages batch)
    pub fn add_edges_nobatch(&mut self, dependent: VertexId, dependencies: &[VertexId]) {
        self.add_dependent_edges_nobatch(dependent, dependencies);
    }

    /// Iterate all normal vertex ids
    pub fn iter_vertex_ids(&self) -> impl Iterator<Item = VertexId> + '_ {
        self.store.all_vertices()
    }

    /// Get the current address of a vertex: a grid position, or a symbol identity for
    /// names, tables and external sources.
    pub fn vertex_addr(&self, vid: VertexId) -> VertexAddr {
        self.store.addr(vid)
    }

    /// Get the current grid position of a vertex, or `None` when it is a symbol.
    pub fn vertex_grid_addr(&self, vid: VertexId) -> Option<GridAddr> {
        self.store.grid_addr(vid)
    }

    /// Total number of allocated vertices (including deleted)
    pub fn vertex_count(&self) -> usize {
        self.store.len()
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Replace CSR edges in one shot from adjacency and coords
    pub fn build_edges_from_adjacency(
        &mut self,
        adjacency: Vec<(u32, Vec<u32>)>,
        coords: Vec<VertexAddr>,
        vertex_ids: Vec<u32>,
    ) {
        #[cfg(not(any(test, feature = "legacy_oracle")))]
        let _ = (&adjacency, &coords, &vertex_ids);
        #[cfg(any(test, feature = "legacy_oracle"))]
        {
            // Merge in base/delta out-edges for vertices the formula-target
            // adjacency doesn't cover (e.g. named-range pass-through vertices)
            // before handing the final adjacency to the pure builder.
            let adjacency = self.edges.adjacency_with_carried_forward_edges(adjacency);
            self.edges
                .build_from_adjacency(adjacency, coords, vertex_ids);
        }
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
                let Some(r) = self.store.grid_addr(vid).map(|addr| addr.row()) else {
                    continue;
                };
                min_r = Some(min_r.map(|m| m.min(r)).unwrap_or(r));
                max_r = Some(max_r.map(|m| m.max(r)).unwrap_or(r));
            }
            self.virtual_row_bounds(sheet_id, start_col, end_col, &mut min_r, &mut max_r);
            self.extent_row_bounds(sheet_id, start_col, end_col, &mut min_r, &mut max_r);
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
        self.virtual_row_bounds(sheet_id, start_col, end_col, &mut min_r, &mut max_r);
        self.extent_row_bounds(sheet_id, start_col, end_col, &mut min_r, &mut max_r);
        match (min_r, max_r) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        }
    }

    /// Build (or rebuild) the sheet index for a given sheet.
    pub fn finalize_sheet_index(&mut self, sheet: &str) {
        let Some(sheet_id) = self.sheet_reg.get_id(sheet) else {
            return;
        };
        self.rebuild_sheet_index(sheet_id);
    }

    fn rebuild_sheet_index(&mut self, sheet_id: SheetId) {
        let mut idx = SheetIndex::new();
        let mut batch: Vec<(GridAddr, VertexId)> =
            Vec::with_capacity(self.cell_to_vertex.len() + self.load_packed_to_vertex.len());
        for (cref, vid) in &self.cell_to_vertex {
            if cref.sheet_id == sheet_id {
                batch.push((GridAddr::new(cref.coord.row(), cref.coord.col()), *vid));
            }
        }
        for (&packed, &vid) in &self.load_packed_to_vertex {
            if packed.sheet_id() != sheet_id {
                continue;
            }
            let coord = GridAddr::new(packed.row0(), packed.col0());
            let addr = CellRef::new(sheet_id, Coord::new(coord.row(), coord.col(), true, true));
            if self.cell_to_vertex.contains_key(&addr) {
                continue;
            }
            batch.push((coord, vid));
        }
        idx.add_vertices_batch(&batch);
        self.sheet_indexes.insert(sheet_id, idx);
    }

    /// Finalize the queried sheet on demand in Lazy mode. A non-empty Lazy
    /// index can still be partial because incremental edit paths may populate
    /// it after deferred bulk load, so queries rebuild it unconditionally.
    pub(crate) fn prepare_sheet_index_for_query(&mut self, sheet_id: SheetId) {
        if self.config.sheet_index_mode == crate::engine::SheetIndexMode::Lazy {
            self.rebuild_sheet_index(sheet_id);
        }
    }

    pub fn set_sheet_index_mode(&mut self, mode: crate::engine::SheetIndexMode) {
        self.config.sheet_index_mode = mode;
    }

    pub(crate) fn set_evaluation_budgets(&mut self, budgets: crate::engine::EvaluationBudgets) {
        self.config.evaluation_budgets = budgets;
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
                let Some(c) = self.store.grid_addr(vid).map(|addr| addr.col()) else {
                    continue;
                };
                min_c = Some(min_c.map(|m| m.min(c)).unwrap_or(c));
                max_c = Some(max_c.map(|m| m.max(c)).unwrap_or(c));
            }
            self.virtual_col_bounds(sheet_id, start_row, end_row, &mut min_c, &mut max_c);
            self.extent_col_bounds(sheet_id, start_row, end_row, &mut min_c, &mut max_c);
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
        self.virtual_col_bounds(sheet_id, start_row, end_row, &mut min_c, &mut max_c);
        self.extent_col_bounds(sheet_id, start_row, end_row, &mut min_c, &mut max_c);
        match (min_c, max_c) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        }
    }

    /// Widen `min..max` rows by the extent record in columns `c0..=c1`.
    fn extent_row_bounds(
        &self,
        sheet: SheetId,
        c0: u32,
        c1: u32,
        min: &mut Option<u32>,
        max: &mut Option<u32>,
    ) {
        if let Some((a, b)) = self.extent_record.row_bounds_for_cols(sheet, c0, c1) {
            *min = Some(min.map_or(a, |m| m.min(a)));
            *max = Some(max.map_or(b, |m| m.max(b)));
        }
    }

    /// Widen `min..max` columns by the extent record in rows `r0..=r1`.
    fn extent_col_bounds(
        &self,
        sheet: SheetId,
        r0: u32,
        r1: u32,
        min: &mut Option<u32>,
        max: &mut Option<u32>,
    ) {
        if let Some((a, b)) = self.extent_record.col_bounds_for_rows(sheet, r0, r1) {
            *min = Some(min.map_or(a, |m| m.min(a)));
            *max = Some(max.map_or(b, |m| m.max(b)));
        }
    }

    /// Widen `min..max` rows by the virtual members in columns `c0..=c1`.
    fn virtual_row_bounds(
        &self,
        sheet: SheetId,
        c0: u32,
        c1: u32,
        min: &mut Option<u32>,
        max: &mut Option<u32>,
    ) {
        for r in self
            .vertex_formulas
            .virtual_members()
            .runs_in_cols(sheet, c0, c1)
        {
            let (a, b) = (r.row0, r.row0 + r.len - 1);
            *min = Some(min.map_or(a, |m| m.min(a)));
            *max = Some(max.map_or(b, |m| m.max(b)));
        }
    }

    /// Widen `min..max` columns by the virtual members in rows `r0..=r1`.
    fn virtual_col_bounds(
        &self,
        sheet: SheetId,
        r0: u32,
        r1: u32,
        min: &mut Option<u32>,
        max: &mut Option<u32>,
    ) {
        for r in self.vertex_formulas.virtual_members().runs_in_sheet(sheet) {
            if r.row0 <= r1 && r.row0 + r.len > r0 {
                *min = Some(min.map_or(r.col, |m| m.min(r.col)));
                *max = Some(max.map_or(r.col, |m| m.max(r.col)));
            }
        }
    }

    /// Returns true if the given sheet currently contains any formula vertices.
    pub fn sheet_has_formulas(&self, sheet_id: SheetId) -> bool {
        // Check vertex_formulas keys; they represent formula vertices
        for vid in self.vertex_formulas.keys() {
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

        #[cfg_attr(not(any(test, feature = "legacy_oracle")), allow(unused_mut))]
        let mut g = Self {
            store: VertexStore::new(),
            #[cfg(any(test, feature = "legacy_oracle"))]
            edges: CsrMutableEdges::new(),
            dep_edge_total: 0,
            range_reader_count: 0,
            renamed_sheet_aliases: FxHashMap::default(),
            data_store: DataStore::new(),
            vertex_values: FxHashMap::default(),
            vertex_formulas: FormulaMap::default(),
            // Phase 1 (ticket 610): Arrow-truth is the only supported mode.
            // The dependency graph does not cache cell/formula literal payloads.
            value_cache_enabled: false,
            #[cfg(debug_assertions)]
            graph_value_read_attempts: AtomicU64::new(0),
            cell_to_vertex: std::collections::HashMap::with_hasher(CoordBuildHasher),
            vertex_journal: Default::default(),
            load_packed_to_vertex: std::collections::HashMap::with_hasher(CoordBuildHasher),
            formula_dirty: FormulaDirtyState::default(),
            dirty_propagation_visits: 0,
            deferred_dirty_depth: 0,
            deferred_dirty_pending: Vec::new(),
            deferred_dirty_pending_rects: Vec::new(),
            retired_ids: std::collections::BTreeMap::new(),
            retired_id_set: FxHashSet::default(),
            retired_dropped: Vec::new(),
            retired_dropped_by_undo: Vec::new(),
            extent_record: Default::default(),
            extent_dropped: Vec::new(),
            extent_undone: Vec::new(),
            volatile_vertices: FxHashSet::default(),
            ref_error_vertices: FxHashSet::default(),
            #[cfg(any(test, feature = "legacy_oracle"))]
            formula_to_range_deps: FxHashMap::default(),
            #[cfg(any(test, feature = "legacy_oracle"))]
            stripe_to_dependents: FxHashMap::default(),
            sheet_indexes: FxHashMap::default(),
            sheet_reg,
            default_sheet_id,
            named_ranges: FxHashMap::default(),
            named_ranges_lookup: FxHashMap::default(),
            sheet_named_ranges: FxHashMap::default(),
            sheet_named_ranges_lookup: FxHashMap::default(),
            #[cfg(any(test, feature = "legacy_oracle"))]
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
            symbol_vertex_seq: 0,
            #[cfg(any(test, feature = "legacy_oracle"))]
            cell_to_name_dependents: FxHashMap::default(),
            #[cfg(any(test, feature = "legacy_oracle"))]
            name_to_cell_dependencies: FxHashMap::default(),
            #[cfg(any(test, feature = "legacy_oracle"))]
            oracle_vertexless_readers: FxHashMap::default(),
            #[cfg(any(test, feature = "legacy_oracle"))]
            oracle_vertexless_of: FxHashMap::default(),
            config: config.clone(),
            topology_revision: 0,
            symbol_revision: 0,
            authority: Default::default(),
            #[cfg(any(test, feature = "legacy_oracle"))]
            pk_order: None,
            spill_anchor_to_cells: FxHashMap::default(),
            spill_cell_to_anchor: std::collections::HashMap::with_hasher(CoordBuildHasher),
            spill_cells_by_sheet: FxHashMap::default(),
            admission_budget_override: None,
            first_load_assume_new: false,
            ensure_touched_sheets: FxHashSet::default(),
            tombstone_registry: TombstoneRegistry::default(),
            #[cfg(test)]
            instr: std::sync::Mutex::new(GraphInstrumentation::default()),
            #[cfg(test)]
            prepared_legacy_graph_failure_for_test: false,
        };

        #[cfg(any(test, feature = "legacy_oracle"))]
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

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// When dynamic topology is enabled, compute layers for a subset using PK ordering.
    pub(crate) fn pk_layers_for(&self, subset: &[VertexId]) -> Option<Vec<crate::engine::Layer>> {
        let pk = self.pk_order.as_ref()?;
        let adapter = crate::engine::topo::GraphAdapter { g: self };
        let layers = pk.layers_for(&adapter, subset, self.config.max_layer_width);
        Some(layers.into_iter().map(crate::engine::Layer::new).collect())
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
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

    /// Whether legacy's Pearce-Kelly order is maintained (oracle builds with
    /// `use_dynamic_topo`); never at runtime.
    pub(crate) fn pk_active(&self) -> bool {
        #[cfg(any(test, feature = "legacy_oracle"))]
        {
            self.pk_order.is_some()
        }
        #[cfg(not(any(test, feature = "legacy_oracle")))]
        {
            false
        }
    }

    /// Begin batch operations - defer CSR rebuilds until end_batch() is called
    pub fn begin_batch(&mut self) {
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.edges.begin_batch();
    }

    /// End batch operations and trigger CSR rebuild if needed
    pub fn end_batch(&mut self) {
        #[cfg(any(test, feature = "legacy_oracle"))]
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
        if let Some(id) = self.sheet_reg.get_id(name) {
            return id;
        }
        let id = self.sheet_reg.id_for(name);
        self.resolve_pending_symbol("sheet", name);
        id
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
            NameRegistryView, NamedEntryRef, NamedTarget, SourceEntryRef, SourceRegistryView,
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

        let unbound_pending =
            config.preparation_policy == crate::engine::PreparationPolicy::BestEffort;
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
                target: match &entry.definition {
                    crate::engine::named_range::NamedDefinition::Cell(cell) => {
                        NamedTarget::Cell(*cell)
                    }
                    crate::engine::named_range::NamedDefinition::Range(range) => {
                        NamedTarget::Range(*range)
                    }
                    crate::engine::named_range::NamedDefinition::Literal(_)
                    | crate::engine::named_range::NamedDefinition::Formula { .. } => {
                        NamedTarget::Other
                    }
                },
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
        .with_unbound_pending(unbound_pending)
    }

    /// Converts a `CellRef` to a fully qualified A1-style string (e.g., "SheetName!A1").
    pub fn to_a1(&self, cell_ref: CellRef) -> String {
        format!("{}!{}", self.sheet_name(cell_ref.sheet_id), cell_ref.coord)
    }

    pub(crate) fn vertex_len(&self) -> usize {
        self.store.len()
    }

    /// The id the next new vertex gets (tests: fresh ids).
    #[cfg(test)]
    pub(crate) fn next_vertex_id_for_test(&self) -> u32 {
        crate::engine::vertex_store::FIRST_NORMAL_VERTEX + self.store.len() as u32
    }

    pub(crate) fn topology_revision(&self) -> u64 {
        self.topology_revision
    }

    pub(crate) fn bump_topology_revision(&mut self) {
        self.topology_revision = self.topology_revision.wrapping_add(1);
    }

    pub(crate) fn symbol_revision(&self) -> u64 {
        self.symbol_revision
    }

    pub(crate) fn bump_symbol_revision(&mut self) {
        self.symbol_revision = self.symbol_revision.wrapping_add(1);
        // Keep a built authority current, so read-only plans (`&self`) see
        // the new binding; during a load or a structural edit it waits.
        self.authority_sync_if_ready();
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    pub(crate) fn formula_range_dependencies(
        &self,
        vertex: VertexId,
    ) -> Option<&[SharedRangeRef<'static>]> {
        self.formula_to_range_deps.get(&vertex).map(Vec::as_slice)
    }

    pub(crate) fn spill_anchors_in_region(
        &self,
        sheet_id: SheetId,
        start_row0: u32,
        start_col0: u32,
        end_row0: u32,
        end_col0: u32,
    ) -> Vec<VertexId> {
        let mut anchors = self
            .spill_cells_by_sheet
            .get(&sheet_id)
            .into_iter()
            .flat_map(|cells| cells.range((start_row0, 0)..=(end_row0, u32::MAX)))
            .filter_map(|(&(row, col), anchor)| {
                (row <= end_row0 && col >= start_col0 && col <= end_col0).then_some(*anchor)
            })
            .collect::<Vec<_>>();
        anchors.sort_unstable();
        anchors.dedup();
        anchors
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

    pub(crate) fn sheet_index_vertex_count(&self, sheet_id: SheetId) -> usize {
        self.sheet_indexes.get(&sheet_id).map_or(0, SheetIndex::len)
            + self
                .vertex_formulas
                .virtual_members()
                .runs_in_sheet(sheet_id)
                .map(|r| r.len as usize)
                .sum::<usize>()
    }

    pub(crate) fn set_admission_budget_override(
        &mut self,
        budgets: Option<crate::engine::EvaluationBudgets>,
    ) -> Option<crate::engine::EvaluationBudgets> {
        std::mem::replace(&mut self.admission_budget_override, budgets)
    }

    fn self_admission_budgets(&self) -> crate::engine::EvaluationBudgets {
        self.admission_budget_override
            .clone()
            .unwrap_or_else(|| self.config.resolved_evaluation_budgets())
    }

    fn preview_spill_materialization(
        &self,
        target_cells: &[CellRef],
    ) -> Result<crate::engine::resource_ledger::GraphAdmission, ExcelError> {
        let unique = target_cells.iter().copied().collect::<FxHashSet<_>>();
        let added_vertices = unique
            .iter()
            .filter(|cell| self.cell_vertex(cell).is_none())
            .count();
        let stats = self.baseline_stats();
        Ok(crate::engine::resource_ledger::GraphAdmission {
            final_vertices: stats
                .graph_vertex_count
                .checked_add(added_vertices)
                .ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("spill vertex count overflow")
                })?,
            final_edges: stats.graph_edge_count,
            materialization_cells: unique.len() as u64,
            added_vertices,
            added_edges: 0,
        })
    }

    pub(crate) fn preview_value_mutation(
        &self,
        sheet_id: SheetId,
        row: u32,
        col: u32,
    ) -> Result<crate::engine::resource_ledger::GraphAdmission, ExcelError> {
        let cell = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
        let existing = self.cell_vertex(&cell);
        let stats = self.baseline_stats();
        let removed_edges = existing.map_or(0, |vertex| self.store.edge_offset(vertex) as usize);
        // A value cell gets no vertex (decision 27).
        Ok(crate::engine::resource_ledger::GraphAdmission {
            final_vertices: stats.graph_vertex_count,
            final_edges: stats
                .graph_edge_count
                .checked_sub(removed_edges)
                .ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("graph edge count underflow")
                })?,
            materialization_cells: 0,
            added_vertices: 0,
            added_edges: 0,
        })
    }

    pub(crate) fn preview_value_mutations(
        &self,
        sheet_id: SheetId,
        cells: &[(u32, u32)],
    ) -> Result<crate::engine::resource_ledger::GraphAdmission, ExcelError> {
        let mut targets = std::collections::BTreeSet::new();
        let added_vertices = 0usize;
        let mut removed_edges = 0usize;
        for (row, col) in cells {
            let packed = PackedSheetCell::try_from_excel_1based(sheet_id, *row, *col)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
            if !targets.insert(packed) {
                continue;
            }
            let reference = CellRef::new(sheet_id, Coord::from_excel(*row, *col, true, true));
            // A value cell gets no vertex (decision 27).
            if let Some(vertex) = self.cell_vertex(&reference) {
                removed_edges = removed_edges
                    .checked_add(self.store.edge_offset(vertex) as usize)
                    .ok_or_else(|| ExcelError::new(ExcelErrorKind::NImpl))?;
            }
        }
        let stats = self.baseline_stats();
        Ok(crate::engine::resource_ledger::GraphAdmission {
            final_vertices: stats
                .graph_vertex_count
                .checked_add(added_vertices)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::NImpl))?,
            final_edges: stats
                .graph_edge_count
                .checked_sub(removed_edges)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::NImpl))?,
            materialization_cells: 0,
            added_vertices,
            added_edges: 0,
        })
    }

    pub(crate) fn preview_formula_mutations(
        &self,
        plans: &[(SheetId, u32, u32, DependencyPlanRow)],
    ) -> Result<crate::engine::resource_ledger::GraphAdmission, ExcelError> {
        let mut new_cells = std::collections::BTreeSet::new();
        let mut removed_edges = 0usize;
        let mut added_edges = 0usize;
        for (sheet_id, row, col, plan) in plans {
            let target = PackedSheetCell::try_from_excel_1based(*sheet_id, *row, *col)
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
            let target_ref = CellRef::new(*sheet_id, Coord::from_excel(*row, *col, true, true));
            if let Some(vertex) = self.cell_vertex(&target_ref) {
                removed_edges = removed_edges
                    .checked_add(self.store.edge_offset(vertex) as usize)
                    .ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::NImpl)
                            .with_message("graph edge count overflow")
                    })?;
            } else {
                new_cells.insert(target);
            }

            let mut dependencies = std::collections::BTreeSet::new();
            for dependency in &plan.direct_cell_deps {
                let packed = PackedSheetCell::try_new(
                    dependency.sheet_id,
                    dependency.coord.row(),
                    dependency.coord.col(),
                )
                .ok_or_else(|| ExcelError::new(ExcelErrorKind::Ref))?;
                let reference = CellRef::new(dependency.sheet_id, dependency.coord);
                if let Some(vertex) = self.cell_vertex(&reference) {
                    dependencies.insert((0u8, u64::from(vertex.0)));
                } else {
                    new_cells.insert(packed);
                    dependencies.insert((1u8, packed.as_u64()));
                }
            }
            for name in plan.resolved_named_refs.iter().chain(&plan.named_refs) {
                if let Some(entry) = self.resolve_name_entry(name, *sheet_id) {
                    dependencies.insert((0, u64::from(entry.vertex.0)));
                } else if let Some(entry) = self.resolve_source_scalar_entry(name) {
                    dependencies.insert((0, u64::from(entry.vertex.0)));
                }
            }
            for name in &plan.source_refs {
                if let Some(vertex) = self
                    .resolve_source_scalar_entry(name)
                    .map(|entry| entry.vertex)
                    .or_else(|| {
                        self.resolve_source_table_entry(name)
                            .map(|entry| entry.vertex)
                    })
                {
                    dependencies.insert((0, u64::from(vertex.0)));
                }
            }
            for name in &plan.table_refs {
                if let Some(vertex) = self
                    .resolve_table_entry(name)
                    .map(|entry| entry.vertex)
                    .or_else(|| {
                        self.resolve_source_table_entry(name)
                            .map(|entry| entry.vertex)
                    })
                {
                    dependencies.insert((0, u64::from(vertex.0)));
                }
            }
            let target_row = target.row0();
            let target_col = target.col0();
            if plan.range_deps.iter().any(|range| {
                // `Current` is the formula's own sheet.
                let range_sheet = self
                    .sheet_reg
                    .resolve_locator(&range.sheet, *sheet_id)
                    .unwrap_or(*sheet_id);
                range_sheet == *sheet_id
                    && range
                        .start_row
                        .is_none_or(|bound| target_row >= bound.index)
                    && range.end_row.is_none_or(|bound| target_row <= bound.index)
                    && range
                        .start_col
                        .is_none_or(|bound| target_col >= bound.index)
                    && range.end_col.is_none_or(|bound| target_col <= bound.index)
            }) {
                dependencies.insert((1, target.as_u64()));
            }
            added_edges = added_edges.checked_add(dependencies.len()).ok_or_else(|| {
                ExcelError::new(ExcelErrorKind::NImpl).with_message("graph edge count overflow")
            })?;
        }
        let stats = self.baseline_stats();
        Ok(crate::engine::resource_ledger::GraphAdmission {
            final_vertices: stats
                .graph_vertex_count
                .checked_add(new_cells.len())
                .ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("graph vertex count overflow")
                })?,
            final_edges: stats
                .graph_edge_count
                .checked_sub(removed_edges)
                .and_then(|count| count.checked_add(added_edges))
                .ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl).with_message("graph edge count overflow")
                })?,
            materialization_cells: plans.len() as u64,
            added_vertices: new_cells.len(),
            added_edges,
        })
    }

    pub(crate) fn vertices_in_region(
        &self,
        sheet_id: SheetId,
        start_row0: u32,
        end_row0: u32,
        start_col0: u32,
        end_col0: u32,
    ) -> Vec<VertexId> {
        let Some(index) = self.sheet_indexes.get(&sheet_id) else {
            return Vec::new();
        };
        let mut out = index.vertices_in_rect(start_row0, end_row0, start_col0, end_col0);
        // The index holds materialized vertices; virtual family members
        // are indexed by their runs.
        for r in self
            .vertex_formulas
            .virtual_members()
            .runs_in_cols(sheet_id, start_col0, end_col0)
        {
            let lo = r.row0.max(start_row0);
            let hi = (r.row0 + r.len - 1).min(end_row0);
            if lo <= hi {
                out.extend((lo..=hi).map(|row| VertexId(r.first + (row - r.row0))));
            }
        }
        out
    }

    #[cfg(test)]
    pub(crate) fn reset_sheet_index_query_stats(&self) {
        for index in self.sheet_indexes.values() {
            index.reset_query_stats();
        }
    }

    #[cfg(test)]
    pub(crate) fn sheet_index_query_stats(
        &self,
    ) -> crate::engine::sheet_index::SheetIndexQueryStats {
        self.sheet_indexes.values().fold(
            crate::engine::sheet_index::SheetIndexQueryStats::default(),
            |mut total, index| {
                let stats = index.query_stats();
                total.coordinate_nodes_visited = total
                    .coordinate_nodes_visited
                    .saturating_add(stats.coordinate_nodes_visited);
                total.values_visited = total.values_visited.saturating_add(stats.values_visited);
                total
            },
        )
    }

    /// Set a value in a cell, returns affected vertex IDs.
    ///
    /// Decision 27: a value cell has no vertex. A formula it replaces
    /// retires its id (value -> formula at the cell takes it back); the
    /// cell's dependents are dirtied by position. Arrow holds the value.
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<OperationSummary, ExcelError> {
        let _ = normalize_stored_literal(value);
        let sheet_id = self.sheet_id_mut(sheet);
        let budgets = self.self_admission_budgets();
        if crate::engine::resource_ledger::graph_admission_enabled(&budgets) {
            let usage = self.preview_value_mutation(sheet_id, row, col)?;
            crate::engine::resource_ledger::preflight_graph_admission(&budgets, usage, None)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        // External API is 1-based; store 0-based coords internally.
        let coord = Coord::from_excel(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);
        self.vacate_cell(&addr);
        Ok(OperationSummary {
            affected_vertices: self.mark_dirty_cells(&[(sheet_id, coord.row(), coord.col())]),
            created_placeholders: Vec::new(),
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
    /// A value cell gets no vertex (decision 27); a formula there retires.
    pub fn set_cell_value_bulk_untracked(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<(), ExcelError> {
        let _ = normalize_stored_literal(value);
        let sheet_id = self.sheet_id_mut(sheet);
        let budgets = self.self_admission_budgets();
        if crate::engine::resource_ledger::graph_admission_enabled(&budgets) {
            let usage = self.preview_value_mutation(sheet_id, row, col)?;
            crate::engine::resource_ledger::preflight_graph_admission(&budgets, usage, None)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        let coord = Coord::from_excel(row, col, true, true);
        self.vacate_cell(&CellRef::new(sheet_id, coord));
        Ok(())
    }

    /// Bulk insert a collection of plain value cells (no formulas): value
    /// cells get no vertex (decision 27); a formula at such a cell retires.
    /// No dirty propagation (load paths).
    pub fn bulk_insert_values<I>(&mut self, sheet: &str, cells: I) -> Result<(), ExcelError>
    where
        I: IntoIterator<Item = (u32, u32, LiteralValue)>,
    {
        let collected: Vec<(u32, u32, LiteralValue)> = cells.into_iter().collect();
        if collected.is_empty() {
            return Ok(());
        }
        let sheet_id = self.sheet_id_mut(sheet);
        let budgets = self.self_admission_budgets();
        if crate::engine::resource_ledger::graph_admission_enabled(&budgets) {
            let coordinates = collected
                .iter()
                .map(|(row, col, _)| (*row, *col))
                .collect::<Vec<_>>();
            let usage = self.preview_value_mutations(sheet_id, &coordinates)?;
            crate::engine::resource_ledger::preflight_graph_admission(&budgets, usage, None)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        // During initial ingest the caller may guarantee the cells are new.
        let assume_new = self.first_load_assume_new
            && self
                .sheet_id(sheet)
                .map(|sid| !self.ensure_touched_sheets.contains(&sid))
                .unwrap_or(false);
        if assume_new {
            for (row, col, _) in collected {
                self.extent_record
                    .note(sheet_id, row.saturating_sub(1), col.saturating_sub(1));
            }
            return Ok(());
        }
        for (row, col, _) in collected {
            let coord = Coord::from_excel(row, col, true, true);
            self.vacate_cell(&CellRef::new(sheet_id, coord));
        }
        Ok(())
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
        let budgets = self.self_admission_budgets();
        if crate::engine::resource_ledger::graph_admission_enabled(&budgets) {
            let usage = self.preview_formula_mutations(&[(sheet_id, row, col, plan.clone())])?;
            crate::engine::resource_ledger::preflight_graph_admission(&budgets, usage, None)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        let coord = Coord::from_excel(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);

        let t_dep0 = if dbg {
            Some(crate::instant::FzInstant::now())
        } else {
            None
        };
        let mut created_placeholders = Vec::new();
        let (mut new_dependencies, vertexless_deps) =
            self.resolve_direct_deps(&plan.direct_cell_deps);
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
        self.replay_formula_vertex(&addr);
        let addr_vertex_id = self.get_or_create_vertex(&addr, &mut created_placeholders);
        self.materialize_vertex(addr_vertex_id);

        // Editing a formula clears any prior structural #REF! marking for this vertex.
        self.ref_error_vertices.remove(&addr_vertex_id);

        // Under `CyclePolicy::Iterate` (Runtime detection) self-dependencies
        // are accepted, mirroring Excel with iterative calculation enabled:
        // the self-edge forms a single-vertex SCC that the scheduler emits as
        // a Cycle unit and `evaluate_scc_unit` iterates (RFC #113, spec §7.1/
        // §7.6/§7.8). Everywhere else the edit-time rejection stands.
        //
        // Scope note (persistence contract, pinned by
        // `formualizer-workbook/tests/cycle_persistence.rs`): this rejection
        // is an INTERACTIVE-EDIT nicety only. Bulk load paths
        // (`ingest_formula_batches` → `BulkIngestBuilder`, incl. staged
        // `build_graph_all`) intentionally do not perform it, so workbooks
        // saved with self-references under an Iterate config always reload —
        // under any cycle config — and resolve to `#CIRC!`/iteration at
        // evaluation time per the loaded policy.
        let self_reference = new_dependencies.contains(&addr_vertex_id)
            || vertexless_deps.iter().any(|c| same_cell(c, &addr));
        if self_reference && !self.config.cycle.allows_self_dependency() {
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

        // Add new dependency edges (a self-reference whose cell had no
        // vertex before is an edge to the formula's own vertex).
        let vertexless_deps: Vec<CellRef> = vertexless_deps
            .into_iter()
            .filter(|c| {
                if same_cell(c, &addr) {
                    if !new_dependencies.contains(&addr_vertex_id) {
                        new_dependencies.push(addr_vertex_id);
                    }
                    false
                } else {
                    true
                }
            })
            .collect();
        self.add_dependent_edges(addr_vertex_id, &new_dependencies);
        self.note_vertexless_deps(
            addr_vertex_id,
            vertexless_deps
                .iter()
                .map(|c| (c.sheet_id, c.coord.row(), c.coord.col())),
        );
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
            ASTNodeType::Literal(_) | ASTNodeType::Omitted => Ok(false),
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

        self.get_vertex_id_for_address(&addr).and_then(|vertex_id| {
            // Check values hashmap (stores both cell values and formula results)
            self.vertex_values
                .get(&vertex_id)
                .map(|&value_ref| self.data_store.retrieve_value(value_ref))
        })
    }

    /// Mark vertex dirty and propagate to dependents
    fn mark_dirty(&mut self, vertex_id: VertexId) -> Vec<VertexId> {
        self.mark_dirty_many(&[vertex_id])
    }

    /// Multi-source `mark_dirty`: one BFS with a shared seen-set across all
    /// sources, marking exactly the union of per-source `mark_dirty` calls
    /// but visiting every vertex at most once per call.
    ///
    /// Loop-of-`mark_dirty` callers (volatile redirty, iterative-SCC redirty)
    /// pay O(sources × component) without this — measured quadratic by the
    /// iterate edge corpus. A BFS that early-stops at already-`is_dirty`
    /// vertices would also fix that, but it is NOT safe in general: several
    /// call sites set the dirty flag WITHOUT propagating to dependents
    /// (`DependencyGraph::set_dirty`, `mark_dependents_dirty`, names.rs
    /// binding invalidation, eval.rs demand-driven re-marks), so "dirty"
    /// does not imply "my dependents are already dirty". The per-call shared
    /// seen-set needs no such invariant.
    ///
    /// While a deferred-dirty scope is active (`begin_deferred_dirty`), the
    /// call queues its sources for the end-of-scope flush and returns ONLY
    /// the sources as the "affected" set (the full transitive set is
    /// produced once by the flush). Loop-of-edits callers must not rely on
    /// per-edit transitive affected sets inside such a scope.
    pub(crate) fn mark_dirty_many(&mut self, vertex_ids: &[VertexId]) -> Vec<VertexId> {
        if self.deferred_dirty_depth > 0 {
            self.deferred_dirty_pending.extend_from_slice(vertex_ids);
            return vertex_ids.to_vec();
        }
        self.authority_mark_dirty(vertex_ids)
    }

    /// Total vertices processed by dirty-propagation BFS loops since graph
    /// creation (perf-shape observability; see `dirty_propagation_visits`).
    pub(crate) fn dirty_propagation_visits(&self) -> u64 {
        self.dirty_propagation_visits
    }

    /// Begin a deferred-dirty scope for a multi-edit batch.
    ///
    /// While active, `mark_dirty` / `mark_dirty_many` /
    /// `mark_dirty_many_value_cells` queue their sources instead of running a
    /// BFS per call; the outermost `end_deferred_dirty` flushes the queued
    /// union with ONE multi-source `mark_dirty_many`. Union semantics equal
    /// the sequential per-edit calls (pinned by
    /// `mark_dirty_many_equals_sequential_single_source_marks` plus the
    /// deferred-scope tests): any dependent edge removed mid-batch belongs to
    /// a vertex that was itself edited mid-batch, and edited vertices are
    /// themselves pending sources, so the flush covers everything a per-edit
    /// propagation would have reached.
    ///
    /// Nesting is depth-counted. The scope also enters the CSR edge batch
    /// (`begin_batch`) so edge-heavy batches amortize delta rebuilds (#127).
    ///
    /// Callers MUST guarantee `end_deferred_dirty` runs on every exit path
    /// (including `?` early returns): a leaked scope would silently swallow
    /// future propagations. Evaluation entry points `debug_assert` that no
    /// scope is active.
    pub fn begin_deferred_dirty(&mut self) {
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.edges.begin_batch();
        self.deferred_dirty_depth += 1;
    }

    /// End a deferred-dirty scope. When the outermost scope ends, runs ONE
    /// multi-source propagation over every source queued while deferred and
    /// returns its full affected set (sources pointing at vertices deleted
    /// mid-batch are skipped). Inner (nested) ends return an empty set.
    pub fn end_deferred_dirty(&mut self) -> Vec<VertexId> {
        debug_assert!(
            self.deferred_dirty_depth > 0,
            "end_deferred_dirty without matching begin_deferred_dirty"
        );
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.edges.end_batch();
        self.deferred_dirty_depth = self.deferred_dirty_depth.saturating_sub(1);
        if self.deferred_dirty_depth > 0 {
            return Vec::new();
        }
        let pending = std::mem::take(&mut self.deferred_dirty_pending);
        let rects = std::mem::take(&mut self.deferred_dirty_pending_rects);
        let mut affected = self.authority_mark_dirty_rects(&rects);
        if pending.is_empty() {
            return affected;
        }
        let live: Vec<VertexId> = pending
            .into_iter()
            .filter(|&id| self.vertex_exists(id))
            .collect();
        affected.extend(self.mark_dirty_many(&live));
        affected
    }

    /// Dirty the transitive dependents of `cells` (0-based `(sheet, row,
    /// col)`): a value edit, whose cell has no vertex (decision 27).
    pub(crate) fn mark_dirty_cells(
        &mut self,
        cells: &[crate::engine::authority::geom::Cell],
    ) -> Vec<VertexId> {
        let rects: Vec<(SheetId, u32, u32, u32, u32)> =
            cells.iter().map(|&(s, r, c)| (s, r, r, c, c)).collect();
        self.mark_dirty_rects(&rects)
    }

    /// Dirty the transitive dependents of rectangles `(sheet, r0, r1, c0,
    /// c1)` (0-based, inclusive).
    pub(crate) fn mark_dirty_rects(
        &mut self,
        rects: &[(SheetId, u32, u32, u32, u32)],
    ) -> Vec<VertexId> {
        let rects: Vec<(u16, crate::engine::authority::geom::Rect)> = rects
            .iter()
            .map(|&(s, r0, r1, c0, c1)| {
                (s, crate::engine::authority::geom::Rect::new(r0, c0, r1, c1))
            })
            .collect();
        if self.deferred_dirty_depth > 0 {
            self.deferred_dirty_pending_rects.extend_from_slice(&rects);
            return Vec::new();
        }
        self.authority_mark_dirty_rects(&rects)
    }

    /// True while a deferred-dirty scope is active (see
    /// `begin_deferred_dirty`). Evaluation must never start in this state.
    pub fn deferred_dirty_active(&self) -> bool {
        self.deferred_dirty_depth > 0
    }

    /// Get all vertices that need evaluation
    pub fn get_evaluation_vertices(&self) -> Vec<VertexId> {
        // Both sources are sets; sort + dedup instead of a merged hash set
        // (a full recalc lists every formula here).
        let mut result: Vec<VertexId> = self
            .formula_dirty
            .legacy_iter()
            .chain(self.volatile_vertices.iter().copied())
            .filter(|&id| {
                // Only include active formula/name vertices; tombstoned vertices can retain stable
                // IDs in the store, but must never be scheduled for evaluation.
                self.store.vertex_exists_active(id)
                    && matches!(
                        self.store.kind(id),
                        VertexKind::FormulaScalar
                            | VertexKind::FormulaArray
                            | VertexKind::NamedScalar
                            | VertexKind::NamedArray
                    )
            })
            .collect();
        result.sort_unstable();
        result.dedup();
        result
    }

    /// Whether a dirty (not merely volatile) vertex would be scheduled by
    /// [`Self::get_evaluation_vertices`]: the freshness replan condition.
    pub(crate) fn has_dirty_evaluation_vertices(&self) -> bool {
        self.formula_dirty.legacy_iter().any(|id| {
            self.store.vertex_exists_active(id)
                && matches!(
                    self.store.kind(id),
                    VertexKind::FormulaScalar
                        | VertexKind::FormulaArray
                        | VertexKind::NamedScalar
                        | VertexKind::NamedArray
                )
        })
    }

    /// Clear dirty flags after successful evaluation
    pub fn clear_dirty_flags(&mut self, vertices: &[VertexId]) {
        for &vertex_id in vertices {
            self.store.set_dirty(vertex_id, false);
            self.formula_dirty.legacy_remove(&vertex_id);
        }
        self.formula_dirty.legacy_shrink_if_sparse();
        self.authority_observe_clean(vertices);
    }

    /// 🔮 Scalability Hook: Clear volatile vertices after evaluation cycle
    pub fn clear_volatile_flags(&mut self) {
        self.volatile_vertices.clear();
    }

    /// Re-marks all volatile vertices as dirty for the next evaluation cycle.
    /// One multi-source propagation: many volatiles feeding one dependent
    /// component used to pay O(volatiles × component) (a full `mark_dirty`
    /// BFS per volatile); `mark_dirty_many` visits the component once.
    pub(crate) fn redirty_volatiles(&mut self) {
        let volatile_ids: Vec<VertexId> = self.volatile_vertices.iter().copied().collect();
        let _ = self.mark_dirty_many(&volatile_ids);
    }

    /// Re-marks members of iterating SCCs (and, via propagation, their
    /// dependents) dirty for the next evaluation cycle — the volatile-like
    /// redirty that keeps `CyclePolicy::Iterate` cells re-evaluating every
    /// recalc (RFC #113; spec §4/§7.6). Vertices deleted since the recalc
    /// are skipped.
    ///
    /// One multi-source propagation: the old per-member `mark_dirty` loop was
    /// O(|SCC|²) per recalc for a large SCC (a converged 1000-member ring
    /// cost ~42 ms per no-op recalc, release); an interim `!is_dirty` skip
    /// fixed that but leaned on dirty-flag semantics that non-propagating
    /// `set_dirty` callers do not uphold. The shared seen-set in
    /// `mark_dirty_many` is O(component) without any such invariant.
    pub(crate) fn redirty_iterative_members(&mut self, members: &[VertexId]) {
        let live: Vec<VertexId> = members
            .iter()
            .copied()
            .filter(|&id| self.vertex_exists(id))
            .collect();
        let _ = self.mark_dirty_many(&live);
    }

    /// The vertex of a referenced cell, if it has one (Program 3, decision
    /// 27): a value cell or an empty cell has none, and a reference never
    /// creates one. The authority tracks references by position.
    pub(crate) fn dep_vertex(&mut self, addr: &CellRef) -> Option<VertexId> {
        if let Some(vertex_id) = self.cell_vertex(addr) {
            return Some(vertex_id);
        }
        if self.first_load_assume_new {
            let packed = Self::packed_cell_key(
                addr.sheet_id,
                AbsCoord::new(addr.coord.row(), addr.coord.col()),
            );
            if let Some(&existing) = self.load_packed_to_vertex.get(&packed) {
                self.cell_to_vertex.insert(*addr, existing);
                return Some(existing);
            }
        }
        None
    }

    /// Decision 27: a value (or nothing) now fills `addr`, which keeps no
    /// vertex. A formula's id retires into the side table (value -> formula
    /// at the cell takes it back); any other vertex (a value vertex of an
    /// older state, a revived placeholder) is tombstoned. Returns the vertex
    /// that left and whether it held a formula.
    pub(crate) fn vacate_cell(&mut self, addr: &CellRef) -> Option<(VertexId, bool)> {
        // The legacy graph kept (or created) a value vertex here: it counts
        // toward the used extent.
        self.extent_record
            .note(addr.sheet_id, addr.coord.row(), addr.coord.col());
        let v = self.cell_vertex_mut(addr)?;
        let was_formula = matches!(
            self.store.kind(v),
            VertexKind::FormulaScalar | VertexKind::FormulaArray
        ) || self.vertex_formulas.contains_key(&v);
        self.remove_dependent_edges(v);
        self.detach_vertex_from_names(v);
        self.clear_pending_name_references(v);
        self.vertex_formulas.remove(&v);
        self.vertex_values.remove(&v);
        self.ref_error_vertices.remove(&v);
        self.clear_formula_vertex_dirty(v);
        self.mark_volatile(v, false);
        self.store.set_dynamic(v, false);
        self.store.set_kind(v, VertexKind::Empty);
        let key = (addr.sheet_id, addr.coord.row(), addr.coord.col());
        // Oracle builds: readers' edges to the vertex become reads of a
        // cell without one.
        #[cfg(any(test, feature = "legacy_oracle"))]
        {
            let readers = self.get_dependents(v);
            self.remove_all_edges(v);
            for r in readers {
                if !self.store.is_deleted(r) {
                    self.oracle_vertexless_readers
                        .entry(key)
                        .or_default()
                        .push(r);
                    self.oracle_vertexless_of.entry(r).or_default().push(key);
                }
            }
        }
        self.cell_to_vertex.remove(addr);
        if let Some(index) = self.sheet_indexes.get_mut(&addr.sheet_id) {
            index.remove_vertex(GridAddr::new(key.1, key.2), v);
        }
        self.store.mark_deleted(v, true);
        if was_formula {
            self.retired_ids.insert(key, v);
            self.retired_id_set.insert(v);
        }
        Some((v, was_formula))
    }

    /// An empty vertex at 0-based `(row, col)` of `sheet` (the cell's
    /// vertex when it has one): the low-level editor's explicit vertex
    /// creation. References and value edits never create one (decision 27).
    pub(crate) fn add_empty_vertex(
        &mut self,
        sheet: SheetId,
        row: u32,
        col: u32,
    ) -> Result<VertexId, ExcelError> {
        let budgets = self.self_admission_budgets();
        if crate::engine::resource_ledger::graph_admission_enabled(&budgets) {
            let mut usage = self.preview_value_mutation(sheet, row + 1, col + 1)?;
            if self
                .cell_vertex(&CellRef::new(sheet, Coord::new(row, col, true, true)))
                .is_none()
            {
                usage.final_vertices = usage.final_vertices.saturating_add(1);
                usage.added_vertices = 1;
            }
            crate::engine::resource_ledger::preflight_graph_admission(&budgets, usage, None)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        let addr = CellRef::new(sheet, Coord::new(row, col, true, true));
        let mut created = Vec::new();
        let id = self.get_or_create_vertex(&addr, &mut created);
        self.materialize_vertex(id);
        let _ = self.mark_dirty(id);
        Ok(id)
    }

    /// Replay of a cell edit whose prior state was not a formula (undo of a
    /// formula typed into a value or empty cell): the cell loses its vertex
    /// as a value edit does, dependents dirtied by position.
    pub(crate) fn retire_cell_for_replay(&mut self, addr: CellRef) {
        // The cell's value changes either way (Arrow restores it): its
        // readers are dirty even when it had no vertex.
        self.vacate_cell(&addr);
        // Legacy removed the cell's vertex: it leaves the used extent.
        self.forget_extent_cells(
            addr.sheet_id,
            (addr.coord.row(), addr.coord.row()),
            (addr.coord.col(), addr.coord.col()),
        );
        let _ = self.mark_dirty_cells(&[(addr.sheet_id, addr.coord.row(), addr.coord.col())]);
    }

    /// A cell the legacy graph gave a placeholder (a reference to a cell
    /// without a vertex) counts toward the used extent.
    pub(crate) fn note_extent_cell(&mut self, sheet: SheetId, row0: u32, col0: u32) {
        self.extent_record.note(sheet, row0, col0);
    }

    /// Cells of `rows x cols` (0-based, inclusive) leave the extent record:
    /// the legacy graph removed their vertices. Returns the forgotten cells
    /// as `(col, r0, r1)` runs.
    pub(crate) fn forget_extent_cells(
        &mut self,
        sheet: SheetId,
        rows: (u32, u32),
        cols: (u32, u32),
    ) -> Vec<(u32, u32, u32)> {
        self.extent_record.forget_rect(sheet, rows, cols)
    }

    /// Whether the legacy graph held a vertex without a formula at `cell`
    /// (a referenced, value or spill-child cell): the extent record.
    pub(crate) fn had_legacy_cell_vertex(&self, cell: &CellRef) -> bool {
        self.extent_record
            .contains(cell.sheet_id, cell.coord.row(), cell.coord.col())
    }

    /// Columns of `sheet` with a cell in the extent record.
    pub(crate) fn extent_record_columns(&self, sheet: SheetId) -> Vec<u32> {
        self.extent_record.columns(sheet)
    }

    /// Runs in the extent record (tests).
    #[cfg(test)]
    pub(crate) fn extent_record_runs(&self) -> usize {
        self.extent_record.run_count()
    }

    /// The retired id at `addr` comes back (value -> formula, decision 27)
    /// when the cell has no vertex; returns it.
    pub(crate) fn revive_retired_id(&mut self, addr: &CellRef) -> Option<VertexId> {
        if self.retired_ids.is_empty() {
            return None;
        }
        let key = (addr.sheet_id, addr.coord.row(), addr.coord.col());
        let id = *self.retired_ids.get(&key)?;
        let coord = GridAddr::new(key.1, key.2);
        // A live vertex at the cell keeps it (a stale cell-map entry left
        // by legacy move replay does not).
        if let Some(x) = self.cell_vertex(addr)
            && !self.store.is_deleted(x)
            && self.store.grid_addr(x) == Some(coord)
        {
            return None;
        }
        self.retired_ids.remove(&key);
        self.revive_vertex(id, addr.sheet_id, coord).then_some(id)
    }

    /// Retired ids waiting in the side table (tests, accounting).
    pub(crate) fn retired_id_count(&self) -> usize {
        self.retired_ids.len()
    }

    /// Shift the retired-id side table for a structural edit (called with
    /// the pre-edit frame). Entries in a deleted band go to the journal, so
    /// history that brings the cell back revives the id.
    /// `journal`: the edit is logged, so history may undo it (the band's
    /// entries are kept for that); an unlogged delete drops them for good.
    pub(crate) fn shift_retired_ids(
        &mut self,
        op: &crate::engine::graph::editor::reference_adjuster::ShiftOperation,
        journal: bool,
    ) {
        use crate::engine::graph::editor::reference_adjuster::ShiftOperation as Op;
        let dropped = self.extent_record.shift(op);
        if journal && matches!(*op, Op::DeleteRows { .. } | Op::DeleteColumns { .. }) {
            self.extent_dropped.push(dropped);
        }
        self.shift_retired_id_table(op, journal);
    }

    /// The retired-id side table part of [`Self::shift_retired_ids`].
    fn shift_retired_id_table(
        &mut self,
        op: &crate::engine::graph::editor::reference_adjuster::ShiftOperation,
        journal: bool,
    ) {
        use crate::engine::graph::editor::reference_adjuster::ShiftOperation as Op;
        let deleting = matches!(*op, Op::DeleteRows { .. } | Op::DeleteColumns { .. });
        if self.retired_ids.is_empty() {
            if deleting && journal {
                self.retired_dropped.push(Vec::new());
            }
            return;
        }
        let (sheet, rows, start, count, insert) = match *op {
            Op::InsertRows {
                sheet_id,
                before,
                count,
            } => (sheet_id, true, before, count, true),
            Op::DeleteRows {
                sheet_id,
                start,
                count,
            } => (sheet_id, true, start, count, false),
            Op::InsertColumns {
                sheet_id,
                before,
                count,
            } => (sheet_id, false, before, count, true),
            Op::DeleteColumns {
                sheet_id,
                start,
                count,
            } => (sheet_id, false, start, count, false),
        };
        let entries: Vec<((SheetId, u32, u32), VertexId)> = self
            .retired_ids
            .range((sheet, 0, 0)..=(sheet, u32::MAX, u32::MAX))
            .map(|(k, v)| (*k, *v))
            .collect();
        for (key, _) in &entries {
            self.retired_ids.remove(key);
        }
        let mut dropped = Vec::new();
        for ((s, r, c), id) in entries {
            let pos = if rows { r } else { c };
            let moved = if pos < start {
                Some(pos)
            } else if insert {
                pos.checked_add(count)
            } else if pos < start.saturating_add(count) {
                None
            } else {
                Some(pos - count)
            };
            match moved {
                Some(p) => {
                    let key = if rows { (s, p, c) } else { (s, r, p) };
                    self.retired_ids.insert(key, id);
                }
                None => dropped.push(((s, r, c), id)),
            }
        }
        if !insert && journal {
            // Undo of the delete restores them (`replay_structural_marker`).
            self.retired_dropped.push(dropped);
        }
    }

    /// Restore the retired ids a delete dropped (its band is back).
    fn restore_retired_batch(&mut self, batch: RetiredBatch) {
        for (key, id) in batch {
            self.retired_ids.insert(key, id);
        }
    }

    /// Replay of a structural edit's compound marker (its description, as
    /// the editor logs it): history replays the edit per vertex, so the
    /// retired-id side table is shifted here. `forward` for redo, else undo.
    /// Undo of a delete brings the band's dropped entries back (and dirties
    /// the band's readers: its cells reappear, value cells have no vertex
    /// whose revival would dirty them).
    pub(crate) fn replay_structural_marker(&mut self, description: &str, forward: bool) {
        use crate::engine::graph::editor::reference_adjuster::ShiftOperation as Op;
        let Some(op) = parse_structural_description(description) else {
            return;
        };
        if forward {
            self.shift_retired_ids(&op, true);
            if matches!(op, Op::InsertRows { .. } | Op::InsertColumns { .. })
                && let Some(batch) = self.retired_dropped_by_undo.pop()
            {
                // Entries the undo of this insert dropped from its band.
                self.restore_retired_batch(batch);
            }
            return;
        }
        let (inverse, band) = match op {
            Op::InsertRows {
                sheet_id,
                before,
                count,
            } => (
                Op::DeleteRows {
                    sheet_id,
                    start: before,
                    count,
                },
                None,
            ),
            Op::InsertColumns {
                sheet_id,
                before,
                count,
            } => (
                Op::DeleteColumns {
                    sheet_id,
                    start: before,
                    count,
                },
                None,
            ),
            Op::DeleteRows {
                sheet_id,
                start,
                count,
            } => (
                Op::InsertRows {
                    sheet_id,
                    before: start,
                    count,
                },
                Some((sheet_id, true, start, count)),
            ),
            Op::DeleteColumns {
                sheet_id,
                start,
                count,
            } => (
                Op::InsertColumns {
                    sheet_id,
                    before: start,
                    count,
                },
                Some((sheet_id, false, start, count)),
            ),
        };
        // The extent record goes back to the frame before the edit, unless
        // the replay did that at the edit's end marker.
        if self.extent_undone.last() == Some(&shift_key(&op)) {
            self.extent_undone.pop();
        } else {
            self.shift_extent_back(&op, &inverse);
        }
        // Undo of an insert drops the band's entries (retired there after the
        // insert); redo of the insert takes them back.
        self.shift_retired_id_table(&inverse, true);
        if band.is_none() {
            if let Some(batch) = self.retired_dropped.pop() {
                self.retired_dropped_by_undo.push(batch);
            }
            return;
        }
        // Undo of a delete: `inverse` is an insert and recorded nothing.
        if let Some((sheet, rows, start, count)) = band {
            if count == 0 {
                return;
            }
            // Entries the delete dropped come back (LIFO with history).
            if let Some(batch) = self.retired_dropped.pop() {
                self.restore_retired_batch(batch);
            }
            let end = start.saturating_add(count - 1);
            // Excel's grid: 1,048,576 rows by 16,384 columns.
            let rect = if rows {
                (sheet, start, end, 0, 16_383)
            } else {
                (sheet, 0, 1_048_575, start, end)
            };
            let _ = self.mark_dirty_rects(&[rect]);
        }
    }

    /// Backward replay reached the end marker of a compound whose start
    /// marker is `description`: for a structural edit, the extent record
    /// goes back to the frame before the edit now, so the replay of the
    /// edit's events (formulas and vertices restored in that frame) notes
    /// cells in the frame they belong to. The start marker then leaves the
    /// record alone.
    pub(crate) fn undo_structural_extent(&mut self, description: &str) {
        use crate::engine::graph::editor::reference_adjuster::ShiftOperation as Op;
        let Some(op) = parse_structural_description(description) else {
            return;
        };
        let inverse = match op {
            Op::InsertRows {
                sheet_id,
                before,
                count,
            } => Op::DeleteRows {
                sheet_id,
                start: before,
                count,
            },
            Op::InsertColumns {
                sheet_id,
                before,
                count,
            } => Op::DeleteColumns {
                sheet_id,
                start: before,
                count,
            },
            Op::DeleteRows {
                sheet_id,
                start,
                count,
            } => Op::InsertRows {
                sheet_id,
                before: start,
                count,
            },
            Op::DeleteColumns {
                sheet_id,
                start,
                count,
            } => Op::InsertColumns {
                sheet_id,
                before: start,
                count,
            },
        };
        self.shift_extent_back(&op, &inverse);
        self.extent_undone.push(shift_key(&op));
    }

    /// End of a replay: see `authority_set_replay`.
    pub(crate) fn clear_extent_undone(&mut self) {
        self.extent_undone.clear();
    }

    /// Undo of structural edit `op` for the extent record: the inverse
    /// shift, and for a delete the cells it dropped.
    fn shift_extent_back(
        &mut self,
        op: &crate::engine::graph::editor::reference_adjuster::ShiftOperation,
        inverse: &crate::engine::graph::editor::reference_adjuster::ShiftOperation,
    ) {
        use crate::engine::graph::editor::reference_adjuster::ShiftOperation as Op;
        let _ = self.extent_record.shift(inverse);
        if matches!(*op, Op::DeleteRows { .. } | Op::DeleteColumns { .. })
            && let Some(dropped) = self.extent_dropped.pop()
        {
            self.extent_record.restore(dropped);
        }
    }

    /// Extent cells noted but not folded into runs yet (tests).
    #[cfg(test)]
    pub(crate) fn extent_record_pending(&self) -> usize {
        self.extent_record.pending_len()
    }

    /// Extent cells retained for undo of structural deletes: `(entries,
    /// runs)` (tests: history stays delta-sized).
    #[cfg(test)]
    pub(crate) fn extent_history_counts(&self) -> (usize, usize) {
        (
            self.extent_dropped.len(),
            self.extent_dropped.iter().map(Vec::len).sum(),
        )
    }

    /// A sheet is removed: its retired ids go to the journal.
    pub(crate) fn drop_retired_ids_of_sheet(&mut self, sheet: SheetId) {
        self.extent_record.drop_sheet(sheet);
        let keys: Vec<(SheetId, u32, u32)> = self
            .retired_ids
            .range((sheet, 0, 0)..=(sheet, u32::MAX, u32::MAX))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            if let Some(id) = self.retired_ids.remove(&key) {
                self.vertex_journal.retired(key, id.0);
            }
        }
    }

    /// Resolve direct cell dependencies to vertices: `(vertices, cells
    /// without a vertex)`, both deduplicated.
    pub(crate) fn resolve_direct_deps(
        &mut self,
        cells: &[CellRef],
    ) -> (Vec<VertexId>, Vec<CellRef>) {
        let mut vertices: Vec<VertexId> = Vec::with_capacity(cells.len());
        let mut vertexless: Vec<CellRef> = Vec::new();
        for cell in cells {
            match self.dep_vertex(cell) {
                Some(v) => {
                    if !vertices.contains(&v) {
                        vertices.push(v);
                    }
                }
                None => {
                    if !vertexless.iter().any(|c| same_cell(c, cell)) {
                        vertexless.push(*cell);
                    }
                }
            }
        }
        (vertices, vertexless)
    }

    fn get_or_create_vertex(
        &mut self,
        addr: &CellRef,
        created_placeholders: &mut Vec<CellRef>,
    ) -> VertexId {
        if let Some(vertex_id) = self.cell_vertex(addr) {
            return vertex_id;
        }
        // A formula replaced by a value retired its id here: take it back.
        if let Some(vertex_id) = self.revive_retired_id(addr) {
            return vertex_id;
        }

        // During first-load bulk ingest the fast path populates
        // ``load_packed_to_vertex`` but skips ``cell_to_vertex``. Promote
        // the entry into ``cell_to_vertex`` so subsequent lookups are O(1)
        // and consistent across the two maps.
        if self.first_load_assume_new {
            let packed = Self::packed_cell_key(
                addr.sheet_id,
                AbsCoord::new(addr.coord.row(), addr.coord.col()),
            );
            if let Some(&existing) = self.load_packed_to_vertex.get(&packed) {
                self.cell_to_vertex.insert(*addr, existing);
                return existing;
            }
        }

        created_placeholders.push(*addr);
        let position = GridAddr::new(addr.coord.row(), addr.coord.col());
        let vertex_id = self
            .store
            .allocate(VertexAddr::grid(position), addr.sheet_id, 0x00);

        #[cfg(any(test, feature = "legacy_oracle"))]
        {
            self.edges
                .add_vertex(VertexAddr::grid(position), vertex_id.0);
            self.oracle_cell_vertex_created(
                (addr.sheet_id, position.row(), position.col()),
                vertex_id,
            );
        }

        // Add to sheet index for O(log n + k) range queries
        self.sheet_index_mut(addr.sheet_id)
            .add_vertex(position, vertex_id);

        self.store.set_kind(vertex_id, VertexKind::Empty);
        self.cell_to_vertex.insert(*addr, vertex_id);
        vertex_id
    }

    /// Direct dependencies of `dependent` on cells without a vertex: counted
    /// like edges (the count per distinct cell is what a placeholder vertex
    /// per cell gave); oracle builds remember them for the vertex a cell may
    /// get later.
    pub(crate) fn note_vertexless_deps(
        &mut self,
        dependent: VertexId,
        cells: impl IntoIterator<Item = (SheetId, u32, u32)>,
    ) {
        let mut n = 0usize;
        #[cfg(any(test, feature = "legacy_oracle"))]
        let mut keys = Vec::new();
        for cell in cells {
            n += 1;
            // The legacy placeholder counted toward the used extent.
            self.extent_record.note(cell.0, cell.1, cell.2);
            #[cfg(any(test, feature = "legacy_oracle"))]
            {
                self.oracle_vertexless_readers
                    .entry(cell)
                    .or_default()
                    .push(dependent);
                keys.push(cell);
            }
            #[cfg(not(any(test, feature = "legacy_oracle")))]
            let _ = cell;
        }
        #[cfg(any(test, feature = "legacy_oracle"))]
        if !keys.is_empty() {
            self.oracle_vertexless_of
                .entry(dependent)
                .or_default()
                .extend(keys);
        }
        self.note_dep_edges(dependent, n);
    }

    /// Oracle builds: a vertex `v` now exists at `cell`; readers that
    /// referenced the cell while it had none get their oracle edge.
    #[cfg(any(test, feature = "legacy_oracle"))]
    pub(crate) fn oracle_cell_vertex_created(&mut self, cell: (SheetId, u32, u32), v: VertexId) {
        if self.oracle_vertexless_readers.is_empty() {
            return;
        }
        let Some(readers) = self.oracle_vertexless_readers.remove(&cell) else {
            return;
        };
        for reader in readers {
            if let Some(cells) = self.oracle_vertexless_of.get_mut(&reader) {
                cells.retain(|c| *c != cell);
                if cells.is_empty() {
                    self.oracle_vertexless_of.remove(&reader);
                }
            }
            self.oracle_add_dependent_edges(reader, &[v]);
        }
    }

    /// Oracle builds: formulas and names reading `cell`, which has no
    /// vertex.
    #[cfg(any(test, feature = "legacy_oracle"))]
    pub(crate) fn oracle_vertexless_readers_of(
        &self,
        cell: crate::engine::authority::geom::Cell,
    ) -> Vec<VertexId> {
        self.oracle_vertexless_readers
            .get(&(cell.0 as SheetId, cell.1, cell.2))
            .cloned()
            .unwrap_or_default()
    }

    /// Oracle builds: the cells without a vertex that `dependent` reads
    /// directly (its other direct dependencies are oracle edges).
    #[cfg(any(test, feature = "legacy_oracle"))]
    pub(crate) fn oracle_vertexless_cells(&self, dependent: VertexId) -> Vec<CellRef> {
        self.oracle_vertexless_of
            .get(&dependent)
            .map(|cells| {
                cells
                    .iter()
                    .map(|&(s, r, c)| CellRef::new(s, Coord::new(r, c, true, true)))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    fn oracle_forget_vertexless(&mut self, dependent: VertexId) {
        if let Some(cells) = self.oracle_vertexless_of.remove(&dependent) {
            for cell in cells {
                if let Some(readers) = self.oracle_vertexless_readers.get_mut(&cell) {
                    readers.retain(|r| *r != dependent);
                    if readers.is_empty() {
                        self.oracle_vertexless_readers.remove(&cell);
                    }
                }
            }
        }
    }

    /// Record `n` direct dependency edges of `dependent` (admission count).
    pub(crate) fn note_dep_edges(&mut self, dependent: VertexId, n: usize) {
        let now = self.store.edge_offset(dependent) as usize + n;
        self.store
            .set_edge_offset(dependent, u32::try_from(now).unwrap_or(u32::MAX));
        self.dep_edge_total += n;
    }

    /// The sheet a renamed sheet's old name still denotes for name
    /// formulas, unless a live sheet has that name.
    pub(crate) fn renamed_sheet_alias(&self, name: &str) -> Option<SheetId> {
        self.renamed_sheet_aliases
            .get(&name.to_ascii_lowercase())
            .copied()
            .filter(|&id| self.sheet_reg.name(id) != name)
    }

    /// Whether `vertex`'s formula reads a compressed range.
    pub(crate) fn reads_compressed_range(&self, vertex: VertexId) -> bool {
        self.store.reads_range(vertex)
    }

    /// `vertex` reads a compressed range (see `VertexStore::reads_range`).
    pub(crate) fn note_reads_range(&mut self, vertex: VertexId) {
        if !self.store.reads_range(vertex) {
            self.store.set_reads_range(vertex, true);
            self.range_reader_count += 1;
        }
    }

    /// Whether any formula reads a compressed range.
    pub(crate) fn has_compressed_range_readers(&self) -> bool {
        self.range_reader_count > 0
    }

    /// Drop `vertex`'s direct dependency edges from the admission count.
    pub(crate) fn forget_dep_edges(&mut self, vertex: VertexId) {
        let n = self.store.edge_offset(vertex) as usize;
        if n > 0 {
            self.store.set_edge_offset(vertex, 0);
            self.dep_edge_total = self.dep_edge_total.saturating_sub(n);
        }
    }

    fn add_dependent_edges(&mut self, dependent: VertexId, dependencies: &[VertexId]) {
        self.note_dep_edges(dependent, dependencies.len());
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.oracle_add_dependent_edges(dependent, dependencies);
    }

    /// The oracle-only half (test/oracle builds) of the function above.
    #[cfg(any(test, feature = "legacy_oracle"))]
    fn oracle_add_dependent_edges(&mut self, dependent: VertexId, dependencies: &[VertexId]) {
        // Batch to avoid repeated CSR rebuilds and keep reverse edges current
        #[cfg(any(test, feature = "legacy_oracle"))]
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

        #[cfg(any(test, feature = "legacy_oracle"))]
        self.edges.end_batch();
    }

    /// Like add_dependent_edges, but assumes caller is managing edges.begin_batch/end_batch
    fn add_dependent_edges_nobatch(&mut self, dependent: VertexId, dependencies: &[VertexId]) {
        self.note_dep_edges(dependent, dependencies.len());
        #[cfg(any(test, feature = "legacy_oracle"))]
        {
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
        let budgets = self.self_admission_budgets();
        if crate::engine::resource_ledger::graph_admission_enabled(&budgets) {
            let admission_plans = planned
                .iter()
                .map(|(row, col, _, plan)| (sheet_id, *row, *col, plan.clone()))
                .collect::<Vec<_>>();
            let usage = self.preview_formula_mutations(&admission_plans)?;
            crate::engine::resource_ledger::preflight_graph_admission(&budgets, usage, None)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        let mut created_placeholders: Vec<CellRef> = Vec::new();
        let mut target_vids: Vec<VertexId> = Vec::with_capacity(planned.len());
        for (row, col, _, _) in &planned {
            let addr = CellRef::new(sheet_id, Coord::from_excel(*row, *col, true, true));
            let target = self.get_or_create_vertex(&addr, &mut created_placeholders);
            self.materialize_vertex(target);
            target_vids.push(target);
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
        self.formula_dirty
            .legacy_extend(target_vids.iter().copied());

        #[cfg(any(test, feature = "legacy_oracle"))]
        self.edges.begin_batch();
        for (i, tvid) in target_vids.iter().copied().enumerate() {
            let plan = &planned[i].3;
            let (mut deps, vertexless) = self.resolve_direct_deps(&plan.direct_cell_deps);
            self.note_vertexless_deps(
                tvid,
                vertexless
                    .iter()
                    .map(|c| (c.sheet_id, c.coord.row(), c.coord.col())),
            );

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
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.edges.end_batch();

        Ok(planned.len())
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Public (crate) helper to add a single dependency edge (dependent -> dependency) used for restoration/undo.
    pub fn add_dependency_edge(
        &mut self,
        dependent: VertexId,
        dependency: VertexId,
    ) -> Result<(), ExcelError> {
        if dependent == dependency {
            return Ok(());
        }
        let budgets = self.self_admission_budgets();
        if crate::engine::resource_ledger::graph_admission_enabled(&budgets) {
            let stats = self.baseline_stats();
            let added = usize::from(!self.get_dependencies(dependent).contains(&dependency));
            crate::engine::resource_ledger::preflight_graph_admission(
                &budgets,
                crate::engine::resource_ledger::GraphAdmission {
                    final_vertices: stats.graph_vertex_count,
                    final_edges: stats.graph_edge_count.checked_add(added).ok_or_else(|| {
                        ExcelError::new(ExcelErrorKind::NImpl)
                            .with_message("graph edge count overflow")
                    })?,
                    materialization_cells: 0,
                    added_vertices: 0,
                    added_edges: added,
                },
                None,
            )
            .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
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
        self.formula_dirty.legacy_insert(dependent);
        Ok(())
    }

    fn remove_dependent_edges(&mut self, vertex: VertexId) {
        self.forget_dep_edges(vertex);
        if self.store.reads_range(vertex) {
            self.store.set_reads_range(vertex, false);
            self.range_reader_count = self.range_reader_count.saturating_sub(1);
        }
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.oracle_remove_dependent_edges(vertex);
    }

    /// The oracle-only half (test/oracle builds) of the function above.
    #[cfg(any(test, feature = "legacy_oracle"))]
    fn oracle_remove_dependent_edges(&mut self, vertex: VertexId) {
        self.oracle_forget_vertexless(vertex);
        // Remove all outgoing edges from this vertex (its dependencies)
        let dependencies = self.edges.out_edges(vertex);

        #[cfg(any(test, feature = "legacy_oracle"))]
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
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.edges.end_batch();

        // Remove range dependencies and clean up stripes
        if let Some(old_ranges) = self.formula_to_range_deps.remove(&vertex) {
            let old_sheet_id = self.store.sheet_id(vertex);

            for range in &old_ranges {
                // `Current` is the sheet the moved formula used to live on.
                let sheet_id = self
                    .sheet_reg
                    .resolve_locator(&range.sheet, old_sheet_id)
                    .unwrap_or(old_sheet_id);
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
    /// Whether any spill anchor is registered.
    pub(crate) fn has_spill_anchors(&self) -> bool {
        !self.spill_anchor_to_cells.is_empty()
    }

    /// Whether `vertex` anchors a spill.
    pub(crate) fn is_spill_anchor(&self, vertex: VertexId) -> bool {
        self.spill_anchor_to_cells.contains_key(&vertex)
    }

    /// [`Self::update_vertex_value`] that clones the value only when the
    /// graph keeps it (canonical mode keeps none for grid cells).
    pub(crate) fn update_vertex_value_ref(&mut self, vertex_id: VertexId, value: &LiteralValue) {
        if !self.value_cache_enabled && self.is_grid_backed(vertex_id) {
            if !self.vertex_values.is_empty() {
                self.vertex_values.remove(&vertex_id);
            }
            return;
        }
        self.update_vertex_value(vertex_id, value.clone());
    }

    pub(crate) fn update_vertex_value(&mut self, vertex_id: VertexId, value: LiteralValue) {
        if !self.value_cache_enabled && self.is_grid_backed(vertex_id) {
            // Canonical mode: grid-backed vertices must not store values in the graph.
            // Symbols (e.g. named-range formulas) may still cache theirs.
            self.vertex_values.remove(&vertex_id);
            return;
        }
        // A cached value is per-vertex state a virtual member cannot hold.
        self.materialize_vertex(vertex_id);
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
            if let Some(vid) = self.cell_vertex(cell)
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
        self.materialize_vertex(anchor);
        let budgets = self.self_admission_budgets();
        if crate::engine::resource_ledger::graph_admission_enabled(&budgets) {
            let admission = self.preview_spill_materialization(&target_cells)?;
            crate::engine::resource_ledger::preflight_graph_admission(&budgets, admission, None)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }

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
                        self.update_vertex_value_ref(anchor, &old.value);
                    } else {
                        let _ = self.set_cell_value(sheet, row + 1, col + 1, old.value.clone());
                    }
                }
                return Err(ExcelError::new(ExcelErrorKind::Error)
                    .with_message("Injected persistence fault during spill commit"));
            }
            if op.sheet == anchor_sheet_name && op.row == anchor_row && op.col == anchor_col {
                self.update_vertex_value_ref(anchor, &op.new_value);
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
                let remove_sheet = self
                    .spill_cells_by_sheet
                    .get_mut(&cell.sheet_id)
                    .is_some_and(|sheet| {
                        sheet.remove(&(cell.coord.row(), cell.coord.col()));
                        sheet.is_empty()
                    });
                if remove_sheet {
                    self.spill_cells_by_sheet.remove(&cell.sheet_id);
                }
            }
        }
        // Mark ownership for new rectangle using the declared target cells only
        for cell in &target_cells {
            self.spill_cell_to_anchor.insert(*cell, anchor);
            self.spill_cells_by_sheet
                .entry(cell.sheet_id)
                .or_default()
                .insert((cell.coord.row(), cell.coord.col()), anchor);
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
            let remove_sheet = self
                .spill_cells_by_sheet
                .get_mut(&cell.sheet_id)
                .is_some_and(|sheet| {
                    sheet.remove(&(cell.coord.row(), cell.coord.col()));
                    sheet.is_empty()
                });
            if remove_sheet {
                self.spill_cells_by_sheet.remove(&cell.sheet_id);
            }
        }

        // Clear all spill children (excluding the anchor cell). A child is a
        // value cell: no vertex (decision 27); one it still has leaves.
        let mut changed: Vec<crate::engine::authority::geom::Cell> = Vec::new();
        for cell in cells.iter().copied() {
            let is_anchor = anchor_cell.map(|a| a == cell).unwrap_or(false);
            if is_anchor {
                continue;
            }
            self.vacate_cell(&cell);
            changed.push((cell.sheet_id, cell.coord.row(), cell.coord.col()));
        }

        // Single dirty propagation for all changed spill children.
        if !changed.is_empty() {
            let _ = self.mark_dirty_cells(&changed);
        }

        cells
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    fn collect_range_dependents_for_vertex(&self, vertex_id: VertexId) -> Vec<VertexId> {
        // Only a vertex with a position can sit inside a range. A symbol has none.
        let Some(position) = self.store.grid_addr(vertex_id) else {
            return Vec::new();
        };
        self.collect_range_dependents_for_rect(
            self.store.sheet_id(vertex_id),
            position.row(),
            position.col(),
            position.row(),
            position.col(),
        )
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
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
                // `Current` is the dependent formula's own sheet; an
                // unresolvable name keeps the dependent in the candidate set.
                let range_sheet_id = self
                    .sheet_reg
                    .resolve_locator(&range.sheet, self.get_vertex_sheet_id(dep_id))
                    .unwrap_or(sheet_id);
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

    /// Whether `vertex_id` is an existing, non-deleted vertex that still
    /// holds a formula (a cell overwritten with a literal keeps its vertex
    /// but drops its formula).
    pub(crate) fn is_live_formula_vertex(&self, vertex_id: VertexId) -> bool {
        self.store.vertex_exists_active(vertex_id) && self.has_formula(vertex_id)
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

    /// The vertex's own arena formula root. `None` for a compressed family
    /// member (its formula is a shared template at an offset; use
    /// [`Self::formula_view`] or [`Self::get_formula`]).
    pub fn get_formula_id(&self, vertex_id: VertexId) -> Option<AstNodeId> {
        self.vertex_formulas.get(&vertex_id).and_then(|f| f.own())
    }

    /// The formula of a formula vertex as a template plus offset.
    pub fn formula_view(&self, vertex_id: VertexId) -> Option<FormulaView> {
        let f = self.vertex_formulas.get(&vertex_id)?;
        Some(match f {
            FormulaRef::Own(template) => FormulaView {
                template,
                row_delta: 0,
                col_delta: 0,
            },
            FormulaRef::Member { template, anchor } => {
                let addr = self.store.grid_addr(vertex_id)?;
                FormulaView {
                    template,
                    row_delta: i64::from(addr.row()) - i64::from(anchor.0),
                    col_delta: i64::from(addr.col()) - i64::from(anchor.1),
                }
            }
        })
    }

    /// Whether the vertex holds a formula (own or compressed).
    pub(crate) fn has_formula(&self, vertex_id: VertexId) -> bool {
        self.vertex_formulas.contains_key(&vertex_id)
    }

    /// Ensure the vertex stores its own AST (instantiating a compressed
    /// member), and return it. Paths that rewrite one cell's formula use
    /// this before editing.
    pub(crate) fn own_formula_id(&mut self, vertex_id: VertexId) -> Option<AstNodeId> {
        self.materialize_vertex(vertex_id);
        match self.vertex_formulas.get(&vertex_id)? {
            FormulaRef::Own(id) => Some(id),
            FormulaRef::Member { .. } => {
                let ast = self.get_formula(vertex_id)?;
                let id = self.data_store.store_ast(&ast, &self.sheet_reg);
                self.vertex_formulas.decompress(vertex_id, id);
                Some(id)
            }
        }
    }

    /// Formula vertices held by the per-vertex formula map (not virtual
    /// family members), by id.
    pub(crate) fn materialized_formula_vertices_sorted(&self) -> Vec<VertexId> {
        let mut vertices: Vec<VertexId> = self.vertex_formulas.map_iter().map(|(v, _)| v).collect();
        vertices.sort_unstable();
        vertices
    }

    pub(crate) fn formula_vertices(&self) -> Vec<VertexId> {
        let mut vertices = self.vertex_formulas.keys().collect::<Vec<_>>();
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
        let view = self.formula_view(vertex_id)?;
        let ast = self
            .data_store
            .retrieve_ast(view.template, &self.sheet_reg)?;
        if view.row_delta == 0 && view.col_delta == 0 {
            return Some(ast);
        }
        crate::engine::template::relocate::instantiate_member_ast(
            &ast,
            view.row_delta,
            view.col_delta,
        )
        .ok()
    }

    /// Get the value stored for a vertex
    pub fn get_value(&self, vertex_id: VertexId) -> Option<LiteralValue> {
        if !self.value_cache_enabled && self.is_grid_backed(vertex_id) {
            // In canonical mode, grid-backed values must not be read from the graph.
            // Symbols (named ranges, tables, external sources) may still use graph storage.
            #[cfg(debug_assertions)]
            {
                self.graph_value_read_attempts
                    .fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        self.vertex_values
            .get(&vertex_id)
            .map(|&value_ref| self.data_store.retrieve_value(value_ref))
    }

    /// True when the vertex occupies the grid, i.e. it is a cell, formula or empty
    /// placeholder rather than a symbol.
    ///
    /// This replaces the `VertexKind` enumerations that used to spell out the grid-backed
    /// kinds. "Has a position" is now a structural property of the address, so it cannot
    /// drift out of step with the set of kinds.
    #[inline]
    fn is_grid_backed(&self, vertex_id: VertexId) -> bool {
        self.store.grid_addr(vertex_id).is_some()
    }

    /// Get the cell reference for a vertex.
    ///
    /// Returns `None` for symbol vertices (names, tables, external sources): they are
    /// identified by name and have no position, so there is no address to return.
    pub(crate) fn get_cell_ref(&self, vertex_id: VertexId) -> Option<CellRef> {
        let grid = self.store.grid_addr(vertex_id)?;
        let sheet_id = self.store.sheet_id(vertex_id);
        let coord = Coord::new(grid.row(), grid.col(), true, true);
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

    /// Get vertex ID for a cell address.
    ///
    /// Returns the id by value (0.10: it returned `Option<&VertexId>`, a
    /// borrow of the cell map, which no longer holds compressed family
    /// members).
    pub fn get_vertex_id_for_address(&self, addr: &CellRef) -> Option<VertexId> {
        self.cell_vertex(addr)
    }

    #[cfg(test)]
    pub fn cell_to_vertex(
        &self,
    ) -> &std::collections::HashMap<CellRef, VertexId, CoordBuildHasher> {
        &self.cell_to_vertex
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Borrow dependencies of a vertex when no pending edge delta exists.
    ///
    /// This enables zero-allocation traversal in hot scheduler paths.
    #[inline]
    pub(crate) fn dependencies_slice(&self, vertex_id: VertexId) -> Option<&[VertexId]> {
        self.edges.out_edges_ref(vertex_id)
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Get the dependencies of a vertex (for scheduler)
    pub(crate) fn get_dependencies(&self, vertex_id: VertexId) -> Vec<VertexId> {
        self.edges.out_edges(vertex_id)
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Check if a vertex has a self-loop
    pub(crate) fn has_self_loop(&self, vertex_id: VertexId) -> bool {
        if let Some(deps) = self.dependencies_slice(vertex_id) {
            deps.contains(&vertex_id)
        } else {
            self.edges.out_edges(vertex_id).contains(&vertex_id)
        }
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Borrow dependents of a vertex when no pending edge delta exists.
    ///
    /// This enables zero-allocation traversal in hot scheduler paths.
    #[inline]
    pub(crate) fn dependents_slice(&self, vertex_id: VertexId) -> Option<&[VertexId]> {
        self.edges.in_edges_ref(vertex_id)
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Get dependents of a vertex (vertices that depend on this vertex)
    ///
    /// Delta-aware: pending edge mutations that have not been folded into the
    /// CSR base yet are merged in via the delta slab's reverse index, so this
    /// is O(in-degree) even mid-edit (no O(V) scan, no forced rebuild; #125).
    pub(crate) fn get_dependents(&self, vertex_id: VertexId) -> Vec<VertexId> {
        self.edges.in_edges_merged(vertex_id)
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Bounded, delta-aware incoming-edge visitor used by read-only
    /// introspection. Unlike `get_dependents`, this never constructs the full
    /// in-degree before the caller's work limit can stop discovery.
    pub(crate) fn visit_direct_dependents_bounded(
        &self,
        vertex_id: VertexId,
        remaining_work: &mut u64,
        visitor: &mut dyn FnMut(VertexId) -> bool,
    ) -> bool {
        self.edges
            .visit_in_edges_bounded(vertex_id, remaining_work, visitor)
    }

    // Internal helper methods for Milestone 0.4

    /// Internal: Create a snapshot of vertex state for rollback
    #[doc(hidden)]
    pub fn snapshot_vertex(&self, id: VertexId) -> crate::engine::VertexSnapshot {
        let coord = self.store.grid_addr(id).unwrap_or_default();
        let sheet_id = self.store.sheet_id(id);
        let kind = self.store.kind(id);
        let flags = self.store.flags(id) & !crate::engine::vertex_store::VIRTUAL_FLAG;

        // Get value and formula references
        let value_ref = self.vertex_values.get(&id).copied();
        let formula_ref = self.vertex_formulas.get(&id).map(|f| f.root());

        // Outgoing edges (dependencies): legacy's, in oracle builds only.
        #[cfg(any(test, feature = "legacy_oracle"))]
        let out_edges = self.get_dependencies(id);
        #[cfg(not(any(test, feature = "legacy_oracle")))]
        let out_edges = Vec::new();

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
    pub(crate) fn remove_all_edges(&mut self, id: VertexId) {
        self.materialize_vertex(id);
        #[cfg(not(any(test, feature = "legacy_oracle")))]
        self.remove_dependent_edges(id);
        #[cfg(any(test, feature = "legacy_oracle"))]
        {
            // Enter batch mode to avoid intermediate rebuilds
            #[cfg(any(test, feature = "legacy_oracle"))]
            self.edges.begin_batch();

            // Remove outgoing edges (this vertex's dependencies)
            self.remove_dependent_edges(id);

            // Remove incoming edges (vertices that depend on this vertex).
            // get_dependents is delta-aware, so no rebuild is needed here (#125).
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
            #[cfg(any(test, feature = "legacy_oracle"))]
            self.edges.end_batch();
        }
    }

    /// Internal: Mark vertex as having #REF! error
    #[doc(hidden)]
    pub fn mark_as_ref_error(&mut self, id: VertexId) {
        self.materialize_vertex(id);
        if !self.value_cache_enabled && self.is_grid_backed(id) {
            self.ref_error_vertices.insert(id);
            // Canonical-only: graph does not cache grid-backed values.
            // Ensure the dependent subgraph is dirtied so evaluation updates Arrow truth.
            self.vertex_values.remove(&id);
            let _ = self.mark_dirty(id);
            return;
        }
        let error = LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref));
        let value_ref = self.data_store.store_value(error);
        self.vertex_values.insert(id, value_ref);
        let _ = self.mark_dirty(id);
    }

    /// Check if a vertex has a #REF! error
    pub fn is_ref_error(&self, id: VertexId) -> bool {
        if !self.value_cache_enabled && self.is_grid_backed(id) {
            return self.ref_error_vertices.contains(&id);
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
        // Legacy flagged its CSR in-edge readers (cell references, ranges
        // within the expansion limit, names), without propagation. Mid
        // structural edit (or load) the store lags: queue it for the resync.
        if self.authority_defers_marks() {
            self.authority_queue_direct_dirty(id);
            return;
        }
        for dep_id in self.authority_in_edge_readers(id) {
            self.store.set_dirty(dep_id, true);
            self.formula_dirty.legacy_insert(dep_id);
        }
    }

    /// Internal: Mark a vertex as volatile
    #[doc(hidden)]
    pub fn mark_volatile(&mut self, id: VertexId, volatile: bool) {
        if volatile {
            self.materialize_vertex(id);
        }
        self.store.set_volatile(id, volatile);
        if volatile {
            self.volatile_vertices.insert(id);
        } else {
            self.volatile_vertices.remove(&id);
        }
    }

    /// Move a vertex to a new grid position.
    ///
    /// Takes a `GridAddr`, so a symbol vertex cannot be shifted onto the grid by a
    /// structural edit (#304).
    #[doc(hidden)]
    pub fn set_grid_addr(&mut self, id: VertexId, coord: GridAddr) {
        self.materialize_vertex(id);
        self.store.set_addr(id, VertexAddr::grid(coord));
    }

    /// Update edge cache coordinate
    #[doc(hidden)]
    pub(crate) fn update_edge_grid_addr(&mut self, id: VertexId, coord: GridAddr) {
        self.materialize_vertex(id);
        #[cfg(not(any(test, feature = "legacy_oracle")))]
        let _ = (&id, &coord);
        #[cfg(any(test, feature = "legacy_oracle"))]
        {
            self.edges.update_addr(id, VertexAddr::grid(coord));
        }
    }

    /// Mark vertex as deleted (tombstone)
    #[doc(hidden)]
    pub fn mark_deleted(&mut self, id: VertexId, deleted: bool) {
        self.materialize_vertex(id);
        self.store.mark_deleted(id, deleted);
    }

    /// Set vertex kind
    #[doc(hidden)]
    pub fn set_kind(&mut self, id: VertexId, kind: VertexKind) {
        self.materialize_vertex(id);
        self.store.set_kind(id, kind);
    }

    /// Set vertex dirty flag
    #[doc(hidden)]
    pub fn set_dirty(&mut self, id: VertexId, dirty: bool) {
        self.store.set_dirty(id, dirty);
        if dirty {
            self.formula_dirty.legacy_insert(id);
        } else {
            self.formula_dirty.legacy_remove(&id);
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
        self.store.flags(id) & !crate::engine::vertex_store::VIRTUAL_FLAG
    }

    /// The virtual family member at `addr`, if any.
    #[inline]
    pub(crate) fn virtual_member_at(&self, addr: &CellRef) -> Option<VertexId> {
        self.vertex_formulas
            .virtual_members()
            .by_cell(addr.sheet_id, addr.coord.row(), addr.coord.col())
            .map(|m| m.vertex)
    }

    /// Check if vertex is deleted (for testing)
    #[cfg(test)]
    pub(crate) fn is_deleted(&self, id: VertexId) -> bool {
        self.store.is_deleted(id)
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Force edge rebuild (internal use)
    #[doc(hidden)]
    pub fn rebuild_edges(&mut self) {
        self.edges.rebuild();
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Fold pending edge deltas into the CSR base ahead of a read-heavy phase
    /// (scheduling/evaluation), restoring the zero-allocation slice fast
    /// paths. No-op when no deltas are pending. This is the read-side half of
    /// the #125 amortization: writes defer rebuilds, read bursts pay for at
    /// most one.
    pub fn flush_pending_edge_deltas(&mut self) {
        self.edges.rebuild();
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Get delta size (internal use)
    #[doc(hidden)]
    pub fn edges_delta_size(&self) -> usize {
        self.edges.delta_size()
    }

    #[cfg(any(test, feature = "legacy_oracle"))]
    /// Number of full CSR rebuilds performed so far (observability; used by
    /// the #125 rebuild-amortization regression tests).
    #[doc(hidden)]
    pub fn edges_rebuild_count(&self) -> u64 {
        self.edges.rebuild_count()
    }

    /// Get vertex ID for specific cell address
    pub fn get_vertex_for_cell(&self, addr: &CellRef) -> Option<VertexId> {
        self.cell_vertex(addr)
    }

    // ---------------------------------------------------------------
    // Virtual family members (Program 2 compression, P2-M2).

    /// The vertex at `addr`: the cell map, else a virtual family member.
    #[inline]
    pub(crate) fn cell_vertex(&self, addr: &CellRef) -> Option<VertexId> {
        if let Some(&v) = self.cell_to_vertex.get(addr) {
            return Some(v);
        }
        self.vertex_formulas
            .virtual_members()
            .by_cell(addr.sheet_id, addr.coord.row(), addr.coord.col())
            .map(|m| m.vertex)
    }

    /// [`Self::cell_vertex`] for a caller about to mutate the cell or its
    /// vertex: a virtual member is materialized first.
    #[inline]
    pub(crate) fn cell_vertex_mut(&mut self, addr: &CellRef) -> Option<VertexId> {
        if let Some(&v) = self.cell_to_vertex.get(addr) {
            return Some(v);
        }
        let m = self.vertex_formulas.virtual_members().by_cell(
            addr.sheet_id,
            addr.coord.row(),
            addr.coord.col(),
        )?;
        self.materialize_vertex(m.vertex);
        Some(m.vertex)
    }

    /// Whether `v` is a virtual family member.
    #[inline]
    pub(crate) fn is_virtual_member(&self, v: VertexId) -> bool {
        !self.vertex_formulas.virtual_members().is_empty() && self.store.is_virtual(v)
    }

    /// Give virtual member `v` its per-cell map entries back (cell map,
    /// formula map, sheet index). Same formula, same vertex; no-op for any
    /// other vertex. O(log runs).
    pub(crate) fn materialize_vertex(&mut self, v: VertexId) -> bool {
        if !self.is_virtual_member(v) {
            return false;
        }
        let Some(m) = self.vertex_formulas.materialize(v) else {
            return false;
        };
        self.store.ensure_dense(v, 1);
        self.store.set_virtual(v, false);
        let addr = CellRef::new(m.sheet, Coord::new(m.row, m.col, true, true));
        self.cell_to_vertex.insert(addr, v);
        self.sheet_index_mut(m.sheet)
            .add_vertex(GridAddr::new(m.row, m.col), v);
        true
    }

    /// Materialize every virtual member of `sheet`.
    pub(crate) fn materialize_sheet(&mut self, sheet: SheetId) {
        let runs = self
            .vertex_formulas
            .virtual_members_mut()
            .drain_sheet(sheet);
        self.restore_runs(runs);
    }

    /// Materialize every virtual member (structural and sheet-wide
    /// operations, and anything that walks per-cell maps). O(members).
    pub(crate) fn materialize_all(&mut self) {
        if self.vertex_formulas.virtual_members().is_empty() {
            return;
        }
        let runs = self.vertex_formulas.virtual_members_mut().drain();
        self.restore_runs(runs);
    }

    fn restore_runs(&mut self, runs: Vec<virtual_members::MemberRun>) {
        if runs.is_empty() {
            return;
        }
        let n: usize = runs.iter().map(|r| r.len as usize).sum();
        self.cell_to_vertex.reserve(n);
        self.vertex_formulas.reserve(n);
        let mut by_sheet: FxHashMap<SheetId, Vec<(GridAddr, VertexId)>> = FxHashMap::default();
        for r in &runs {
            self.store.ensure_dense(VertexId(r.first), r.len);
            let f = r.formula();
            let batch = by_sheet.entry(r.sheet).or_default();
            for (v, row) in r.members() {
                self.store.set_virtual(v, false);
                self.vertex_formulas.restore(v, f);
                self.cell_to_vertex
                    .insert(CellRef::new(r.sheet, Coord::new(row, r.col, true, true)), v);
                batch.push((GridAddr::new(row, r.col), v));
            }
        }
        for (sheet, batch) in by_sheet {
            self.sheet_index_mut(sheet).add_vertices_batch(&batch);
        }
    }

    /// Formula vertices in columns `c0..=c1` of `sheet` that are virtual
    /// family members, with their rows (sheet-index queries add these: the
    /// index holds only materialized vertices).
    pub(crate) fn virtual_members_in_cols(
        &self,
        sheet: SheetId,
        c0: u32,
        c1: u32,
    ) -> impl Iterator<Item = (VertexId, GridAddr)> + '_ {
        self.vertex_formulas
            .virtual_members()
            .runs_in_cols(sheet, c0, c1)
            .flat_map(|r| {
                r.members()
                    .map(move |(v, row)| (v, GridAddr::new(row, r.col)))
            })
    }

    /// Vertices of `sheet` whose column is in `c0..=c1`: the sheet index
    /// plus virtual members, or (no index) a scan of the vertex rows, which
    /// virtual members keep.
    pub(crate) fn vertices_in_cols(&self, sheet: SheetId, c0: u32, c1: u32) -> Vec<VertexId> {
        match self.sheet_indexes.get(&sheet) {
            Some(index) => {
                let mut out = index.vertices_in_col_range(c0, c1);
                out.extend(self.virtual_members_in_cols(sheet, c0, c1).map(|(v, _)| v));
                out
            }
            None => self
                .grid_vertices_in_sheet(sheet)
                .filter(|(_, a)| a.col() >= c0 && a.col() <= c1)
                .map(|(v, _)| v)
                .collect(),
        }
    }

    /// Vertices of `sheet` whose row is in `r0..=r1` (see
    /// [`Self::vertices_in_cols`]).
    pub(crate) fn vertices_in_rows(&self, sheet: SheetId, r0: u32, r1: u32) -> Vec<VertexId> {
        match self.sheet_indexes.get(&sheet) {
            Some(index) => {
                let mut out = index.vertices_in_row_range(r0, r1);
                for r in self.vertex_formulas.virtual_members().runs_in_sheet(sheet) {
                    let lo = r.row0.max(r0);
                    let hi = (r.row0 + r.len - 1).min(r1);
                    if lo <= hi {
                        out.extend((lo..=hi).map(|row| VertexId(r.first + (row - r.row0))));
                    }
                }
                out
            }
            None => self
                .grid_vertices_in_sheet(sheet)
                .filter(|(_, a)| a.row() >= r0 && a.row() <= r1)
                .map(|(v, _)| v)
                .collect(),
        }
    }

    /// Turn compressed family members into virtual runs: members whose
    /// formula is exactly `Member { template, anchor }` of one run of
    /// consecutive rows and consecutive vertex ids down a column, and that
    /// carry no per-vertex state beyond their row (not volatile, dynamic,
    /// a spill anchor, a #REF! mark, a cached value, pending names or name
    /// links). Returns the number of members made virtual.
    ///
    /// Cost: one scan of the formula map, a sort of the member candidates
    /// by vertex id (so the vertex columns are read in order), one `retain`
    /// of the two maps checked against the new runs, and a rebuild of the
    /// affected sheet indexes.
    pub(crate) fn virtualize_family_members(&mut self) -> usize {
        let mut cand: Vec<(u32, AstNodeId, (u32, u32))> = self
            .vertex_formulas
            .map_iter()
            .filter_map(|(v, f)| match f {
                FormulaRef::Member { template, anchor } => Some((v.0, template, anchor)),
                FormulaRef::Own(_) => None,
            })
            .collect();
        if cand.len() < 2 {
            return 0;
        }
        cand.sort_unstable_by_key(|c| c.0);
        let runs = self.member_runs_in_id_order(&cand);
        drop(cand);
        self.install_virtual_runs(runs)
    }

    /// Maximal runs among `cand` (member vertices sorted by id, with their
    /// template and anchor): consecutive ids, one column, consecutive rows,
    /// one template, each member virtualizable. Runs of one are dropped.
    fn member_runs_in_id_order(
        &self,
        cand: &[(u32, AstNodeId, (u32, u32))],
    ) -> Vec<virtual_members::MemberRun> {
        let mut runs: Vec<virtual_members::MemberRun> = Vec::new();
        let mut cur: Option<virtual_members::MemberRun> = None;
        let close = |cur: &mut Option<virtual_members::MemberRun>,
                     runs: &mut Vec<virtual_members::MemberRun>| {
            if let Some(r) = cur.take()
                && r.len >= 2
            {
                runs.push(r);
            }
        };
        for &(v, template, anchor) in cand {
            let vid = VertexId(v);
            if !self.virtualizable(vid) {
                close(&mut cur, &mut runs);
                continue;
            }
            let Some(addr) = self.store.grid_addr(vid) else {
                close(&mut cur, &mut runs);
                continue;
            };
            let sheet = self.store.sheet_id(vid);
            if let Some(r) = cur.as_mut()
                && r.sheet == sheet
                && r.col == addr.col()
                && r.first + r.len == v
                && r.row0 + r.len == addr.row()
                && r.template == template
                && r.anchor == anchor
            {
                r.len += 1;
                continue;
            }
            close(&mut cur, &mut runs);
            cur = Some(virtual_members::MemberRun {
                sheet,
                col: addr.col(),
                row0: addr.row(),
                len: 1,
                first: v,
                template,
                anchor,
            });
        }
        close(&mut cur, &mut runs);
        runs
    }

    /// Move the members of `runs` (materialized, disjoint) out of the
    /// per-cell maps into virtual runs. Returns the number of members.
    fn install_virtual_runs(&mut self, runs: Vec<virtual_members::MemberRun>) -> usize {
        if runs.is_empty() {
            return 0;
        }
        let mut made = 0usize;
        let mut sheets: Vec<SheetId> = Vec::new();
        for &r in &runs {
            for (v, _) in r.members() {
                self.store.set_virtual(v, true);
            }
            sheets.push(r.sheet);
            made += r.len as usize;
            self.vertex_formulas.virtual_members_mut().insert(r);
        }
        // Drop the members' map entries: one pass over each map, checked
        // against the runs (small and cache-resident) rather than the
        // vertex columns.
        // An entry is a member's own mapping when its vertex is virtual now
        // (flag, one byte per vertex) and sits at that cell (its row).
        let store = &self.store;
        let mut removed: Vec<u32> = Vec::with_capacity(made);
        self.cell_to_vertex.retain(|c, v| {
            let mine = store.is_virtual(*v)
                && store.sheet_id(*v) == c.sheet_id
                && store.grid_addr(*v) == Some(GridAddr::new(c.coord.row(), c.coord.col()));
            if mine {
                removed.push(v.0);
            }
            !mine
        });
        self.cell_to_vertex.shrink_to_fit();
        let store = &self.store;
        self.vertex_formulas
            .drop_virtual_from_map(|v| store.is_virtual(v));
        if removed.len() != made {
            // Some member was not the vertex mapped at its cell (legacy
            // leaves such formula vertices after some undo/redo replays):
            // it stays materialized, cell map untouched.
            removed.sort_unstable();
            let orphans: Vec<VertexId> = runs
                .iter()
                .flat_map(|r| r.members().map(|(v, _)| v))
                .filter(|v| removed.binary_search(&v.0).is_err())
                .collect();
            for v in orphans {
                if self.vertex_formulas.materialize(v).is_some() {
                    self.store.set_virtual(v, false);
                    made -= 1;
                }
            }
        }
        sheets.sort_unstable();
        sheets.dedup();
        self.rebuild_sheet_indexes(&sheets);
        self.virtualize_member_pages();
        made
    }

    /// Drop the vertex rows of every page filled by virtual members (see
    /// `VertexStore::virtualize_member_span`). Returns the pages dropped.
    pub(crate) fn virtualize_member_pages(&mut self) -> usize {
        let runs: Vec<virtual_members::MemberRun> = self
            .vertex_formulas
            .virtual_members()
            .runs()
            .filter(|r| r.len as usize >= 1024)
            .copied()
            .collect();
        runs.iter()
            .map(|r| {
                self.store
                    .virtualize_member_span(VertexId(r.first), r.len, r.sheet, r.col, r.row0)
            })
            .sum()
    }

    /// Whether member vertex `v` may leave the per-cell maps.
    fn virtualizable(&self, v: VertexId) -> bool {
        let flags = self.store.flags(v);
        // deleted | volatile | dynamic | already virtual
        if flags & (0x02 | 0x04 | 0x08 | crate::engine::vertex_store::VIRTUAL_FLAG) != 0
            || self.store.kind(v) != VertexKind::FormulaScalar
        {
            return false;
        }
        let absent = |empty: bool, contains: &dyn Fn() -> bool| empty || !contains();
        absent(self.ref_error_vertices.is_empty(), &|| {
            self.ref_error_vertices.contains(&v)
        }) && absent(self.vertex_values.is_empty(), &|| {
            self.vertex_values.contains_key(&v)
        }) && absent(self.vertex_to_pending_names.is_empty(), &|| {
            self.vertex_to_pending_names.contains_key(&v)
        }) && absent(self.spill_anchor_to_cells.is_empty(), &|| {
            self.spill_anchor_to_cells.contains_key(&v)
        }) && absent(self.name_vertex_lookup.is_empty(), &|| {
            self.name_vertex_lookup.contains_key(&v)
        }) && absent(self.volatile_vertices.is_empty(), &|| {
            self.volatile_vertices.contains(&v)
        })
    }

    /// Drop the virtual members from the (existing) sheet indexes of
    /// `sheets` (the store's virtual flag); every other entry keeps its
    /// indexed position.
    fn rebuild_sheet_indexes(&mut self, sheets: &[SheetId]) {
        let store = &self.store;
        for sheet in sheets {
            if let Some(index) = self.sheet_indexes.get_mut(sheet) {
                index.retain_vertices(|v| !store.is_virtual(v));
            }
        }
    }

    /// Virtual family members and runs (tests, memory probes).
    /// Vertex pages without rows (tests, memory probes).
    pub(crate) fn virtual_vertex_pages(&self) -> usize {
        self.store.virtual_pages()
    }

    pub(crate) fn virtual_member_counts(&self) -> (usize, usize) {
        let m = self.vertex_formulas.virtual_members();
        (m.len(), m.run_count())
    }

    /// Get the grid position of a vertex (public for VertexEditor).
    ///
    /// `None` for symbol vertices, which have no position. Structural operations iterate
    /// grid positions, so this is what keeps them away from names, tables and sources.
    pub fn get_grid_addr(&self, id: VertexId) -> Option<GridAddr> {
        self.store.grid_addr(id)
    }

    /// Get sheet_id for a vertex (public for VertexEditor)
    pub fn get_sheet_id(&self, id: VertexId) -> SheetId {
        self.store.sheet_id(id)
    }

    /// Get every grid-resident vertex on a sheet, paired with its position.
    ///
    /// Symbol vertices (names, tables, external sources) are structurally absent: they have
    /// no grid position, so they cannot be produced here. Structural edits drive off this
    /// iterator, which is why a row or column operation can no longer delete or shift a
    /// name vertex (#302, #304).
    pub fn grid_vertices_in_sheet(
        &self,
        sheet_id: SheetId,
    ) -> impl Iterator<Item = (VertexId, GridAddr)> + '_ {
        self.store.all_vertices().filter_map(move |id| {
            if !self.vertex_exists(id) || self.store.sheet_id(id) != sheet_id {
                return None;
            }
            if !self.retired_id_set.is_empty()
                && self.retired_id_set.contains(&id)
                && self.store.is_deleted(id)
            {
                return None;
            }
            self.store.grid_addr(id).map(|addr| (id, addr))
        })
    }

    /// Does a vertex have a formula associated
    pub fn vertex_has_formula(&self, id: VertexId) -> bool {
        self.vertex_formulas.contains_key(&id)
    }

    /// Get all vertices with formulas
    pub fn vertices_with_formulas(&self) -> impl Iterator<Item = VertexId> + '_ {
        self.vertex_formulas.keys()
    }

    /// Update a vertex's formula
    pub fn update_vertex_formula(&mut self, id: VertexId, ast: ASTNode) -> Result<(), ExcelError> {
        self.materialize_vertex(id);
        // Get the sheet_id for this vertex
        let sheet_id = self.store.sheet_id(id);

        // Extract dependencies from AST, retaining unresolved names for later linking.
        let (
            new_dependencies,
            new_range_dependencies,
            vertexless,
            named_dependencies,
            unresolved_names,
        ) = self.extract_dependencies_with_pending_names(&ast, sheet_id)?;

        let old_kind = self.store.kind(id);

        // Remove all links owned by the previous formula.
        self.remove_dependent_edges(id);
        self.detach_vertex_from_names(id);
        self.clear_pending_name_references(id);

        // Store the new formula
        let ast_id = self.data_store.store_ast(&ast, &self.sheet_reg);
        self.vertex_formulas.insert(id, ast_id);

        // Add new dependency edges
        self.add_dependent_edges(id, &new_dependencies);
        self.note_vertexless_deps(
            id,
            vertexless
                .iter()
                .map(|c| (c.sheet_id, c.coord.row(), c.coord.col())),
        );
        self.add_range_dependent_edges(id, &new_range_dependencies, sheet_id);

        if !named_dependencies.is_empty() {
            self.attach_vertex_to_names(id, &named_dependencies);
        }
        for unresolved_name in &unresolved_names {
            self.record_pending_name_reference(sheet_id, unresolved_name, id);
        }

        // Formula replacement supersedes any structural error/cache state left when a
        // deleted dependency marked this vertex before its AST was rewritten.
        self.ref_error_vertices.remove(&id);
        self.vertex_values.remove(&id);

        // A structural rewrite must not collapse an existing array formula kind.
        self.store.set_kind(
            id,
            if old_kind == VertexKind::FormulaArray {
                VertexKind::FormulaArray
            } else {
                VertexKind::FormulaScalar
            },
        );

        Ok(())
    }

    /// Mark a vertex as dirty without propagation (for VertexEditor)
    pub fn mark_vertex_dirty(&mut self, vertex_id: VertexId) {
        self.store.set_dirty(vertex_id, true);
        self.formula_dirty.legacy_insert(vertex_id);
    }

    /// Batch-mark vertices dirty without propagation.
    pub fn mark_vertices_dirty_batch(&mut self, vertices: &[VertexId]) {
        self.formula_dirty.legacy_reserve(vertices.len());
        for &vertex_id in vertices {
            self.store.set_dirty(vertex_id, true);
        }
        self.formula_dirty.legacy_extend(vertices.iter().copied());
    }

    /// Update cell mapping for a vertex (for VertexEditor)
    pub fn update_cell_mapping(
        &mut self,
        id: VertexId,
        old_addr: Option<CellRef>,
        new_addr: CellRef,
    ) {
        self.materialize_vertex(id);
        if let Some(old) = old_addr {
            self.cell_vertex_mut(&old);
        }
        self.cell_vertex_mut(&new_addr);
        // Remove old mapping if it exists
        if let Some(old) = old_addr {
            self.cell_to_vertex.remove(&old);
        }
        // Add new mapping
        self.cell_to_vertex.insert(new_addr, id);
    }

    /// Remove cell mapping (for VertexEditor)
    pub fn remove_cell_mapping(&mut self, addr: &CellRef) {
        self.cell_vertex_mut(addr);
        self.cell_to_vertex.remove(addr);
    }

    /// Bring back removed vertex `id` at `coord` of `sheet` as an empty
    /// cell (undo of a removal: decision 9, the cell keeps its id). Only a
    /// tombstoned vertex, and only onto a cell without a vertex; returns
    /// whether it did.
    pub(crate) fn revive_vertex(&mut self, id: VertexId, sheet: SheetId, coord: GridAddr) -> bool {
        if !self.store.vertex_exists(id)
            || !self.store.is_deleted(id)
            || self.store.grid_addr(id).is_none()
            || self.store.sheet_id(id) != sheet
        {
            return false;
        }
        let cell = CellRef::new(sheet, Coord::new(coord.row(), coord.col(), true, true));
        // Occupied only by a live vertex that sits at the cell (legacy's
        // move replay can leave stale cell-map entries behind). An empty
        // placeholder (created for a reference while replay restored a
        // reader first) gives way.
        let mut placeholder = None;
        if let Some(x) = self.cell_vertex(&cell)
            && !self.store.is_deleted(x)
            && self.store.grid_addr(x) == Some(coord)
        {
            if !self.is_pure_placeholder(x) {
                return false;
            }
            placeholder = Some(x);
        }
        #[cfg(any(test, feature = "legacy_oracle"))]
        let readers = placeholder
            .map(|x| self.get_dependents(x))
            .unwrap_or_default();
        if let Some(x) = placeholder {
            self.cell_to_vertex.remove(&cell);
            if let Some(index) = self.sheet_indexes.get_mut(&sheet) {
                index.remove_vertex(coord, x);
            }
            self.remove_all_edges(x);
            self.store.mark_deleted(x, true);
        }
        self.retired_id_set.remove(&id);
        self.store.mark_deleted(id, false);
        self.store.set_addr(id, VertexAddr::grid(coord));
        #[cfg(any(test, feature = "legacy_oracle"))]
        self.edges.update_addr(id, VertexAddr::grid(coord));
        self.store.set_kind(id, VertexKind::Empty);
        self.store.set_dynamic(id, false);
        self.store.set_volatile(id, false);
        self.cell_to_vertex.insert(cell, id);
        self.sheet_index_mut(sheet).add_vertex(coord, id);
        self.ref_error_vertices.remove(&id);
        // Legacy's edges from the placeholder's readers now name the
        // revived vertex (oracle builds keep them).
        #[cfg(any(test, feature = "legacy_oracle"))]
        for r in readers {
            if let Some(ast) = self.get_formula(r) {
                self.rebuild_formula_dependencies(r, &ast);
            }
        }
        // As legacy's re-creation (a `set_cell_value` of Empty): the cell
        // changed, its readers are dirty.
        let _ = self.mark_dirty(id);
        true
    }

    /// An empty cell vertex with no state of its own (a reference's
    /// placeholder).
    fn is_pure_placeholder(&self, x: VertexId) -> bool {
        self.store.kind(x) == VertexKind::Empty
            && !self.vertex_formulas.contains_key(&x)
            && !self.vertex_values.contains_key(&x)
            && !self.ref_error_vertices.contains(&x)
            && !self.spill_anchor_to_cells.contains_key(&x)
            && !self.vertex_to_pending_names.contains_key(&x)
            && !self.name_vertex_lookup.contains_key(&x)
    }

    /// The formula of vertex `v` is leaving its cell (formula -> value,
    /// removal): journaled so replay that brings the formula back revives
    /// `v` if the cell lost its vertex meanwhile (see `IdJournal`).
    pub(crate) fn journal_formula_left(&mut self, v: VertexId) {
        if !self.vertex_formulas.contains_key(&v) {
            return;
        }
        if let Some(cell) = self.get_cell_ref(v) {
            self.vertex_journal
                .retired((cell.sheet_id, cell.coord.row(), cell.coord.col()), v.0);
        }
    }

    /// A formula is about to be set at `addr`. During undo/redo, the vertex
    /// whose formula left this cell comes back if the cell has no vertex
    /// now (legacy replay removed it: e.g. undoing a formula typed over a
    /// value, whose value Arrow restores). Outside replay this starts a new
    /// timeline for the cell.
    fn replay_formula_vertex(&mut self, addr: &CellRef) {
        if self.revive_retired_id(addr).is_some() {
            return;
        }
        let cell = (addr.sheet_id, addr.coord.row(), addr.coord.col());
        let Some(id) = self.vertex_journal.created(cell) else {
            return;
        };
        if self.cell_vertex(addr).is_none() {
            let coord = GridAddr::new(addr.coord.row(), addr.coord.col());
            self.revive_vertex(VertexId(id), addr.sheet_id, coord);
        }
    }

    pub(crate) fn set_replay_mode(&mut self, mode: crate::engine::authority::history::Replay) {
        self.vertex_journal.set_mode(mode);
    }

    /// Get the cell reference for a vertex
    pub fn get_cell_ref_for_vertex(&self, id: VertexId) -> Option<CellRef> {
        let coord = self.store.grid_addr(id)?;
        let sheet_id = self.store.sheet_id(id);
        // Find the cell reference in the mapping
        let cell_ref = CellRef::new(sheet_id, Coord::new(coord.row(), coord.col(), true, true));
        // Verify it actually maps to this vertex
        if self.cell_vertex(&cell_ref) == Some(id) {
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
        self.materialize_vertex(vertex_id);
        let sheet_id = self.store.sheet_id(vertex_id);

        // Remove old dependency, name, and pending-name links first.
        self.remove_dependent_edges(vertex_id);
        self.detach_vertex_from_names(vertex_id);
        self.clear_pending_name_references(vertex_id);

        let (
            new_dependencies,
            new_range_dependencies,
            vertexless,
            named_dependencies,
            unresolved_names,
        ) = match self.extract_dependencies_with_pending_names(ast, sheet_id) {
            Ok(v) => v,
            Err(_) => {
                self.mark_as_ref_error(vertex_id);
                return;
            }
        };

        // Self-reference / name-cycle safety parity with set_cell_formula
        // (including the `CyclePolicy::Iterate` self-dependency relaxation).
        if new_dependencies.contains(&vertex_id) && !self.config.cycle.allows_self_dependency() {
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
        self.note_vertexless_deps(
            vertex_id,
            vertexless
                .iter()
                .map(|c| (c.sheet_id, c.coord.row(), c.coord.col())),
        );
        self.add_range_dependent_edges(vertex_id, &new_range_dependencies, sheet_id);
        self.vertex_formulas.touch(vertex_id);
        let _ = self.mark_dirty(vertex_id);
    }
}

// ========== Sheet Management Operations ==========

/// Retired ids a structural edit dropped from the side table.
/// Retired ids a structural delete dropped from the side table.
type RetiredBatch = Vec<((SheetId, u32, u32), VertexId)>;

/// Same sheet and position (reference flags aside).
pub(crate) fn same_cell(a: &CellRef, b: &CellRef) -> bool {
    a.sheet_id == b.sheet_id && a.coord.row() == b.coord.row() && a.coord.col() == b.coord.col()
}

/// The shift operation of a structural edit's compound description, as
/// `VertexEditor` logs it (`InsertRows sheet=S before=B count=N`, ...).
/// The sheet a structural edit applies to.
/// A structural edit as a comparable key (kind, sheet, position, count).
fn shift_key(
    op: &crate::engine::graph::editor::reference_adjuster::ShiftOperation,
) -> (u8, SheetId, u32, u32) {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation as Op;
    match *op {
        Op::InsertRows {
            sheet_id,
            before,
            count,
        } => (0, sheet_id, before, count),
        Op::DeleteRows {
            sheet_id,
            start,
            count,
        } => (1, sheet_id, start, count),
        Op::InsertColumns {
            sheet_id,
            before,
            count,
        } => (2, sheet_id, before, count),
        Op::DeleteColumns {
            sheet_id,
            start,
            count,
        } => (3, sheet_id, start, count),
    }
}

pub(crate) fn parse_structural_description(
    description: &str,
) -> Option<crate::engine::graph::editor::reference_adjuster::ShiftOperation> {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation as Op;
    let mut parts = description.split_whitespace();
    let kind = parts.next()?;
    let mut field = |name: &str| -> Option<u32> {
        parts
            .next()?
            .strip_prefix(name)?
            .strip_prefix('=')?
            .parse()
            .ok()
    };
    let sheet_id = u16::try_from(field("sheet")?).ok()?;
    Some(match kind {
        "InsertRows" => {
            let before = field("before")?;
            Op::InsertRows {
                sheet_id,
                before,
                count: field("count")?,
            }
        }
        "DeleteRows" => {
            let start = field("start")?;
            Op::DeleteRows {
                sheet_id,
                start,
                count: field("count")?,
            }
        }
        "InsertColumns" => {
            let before = field("before")?;
            Op::InsertColumns {
                sheet_id,
                before,
                count: field("count")?,
            }
        }
        "DeleteColumns" => {
            let start = field("start")?;
            Op::DeleteColumns {
                sheet_id,
                start,
                count: field("count")?,
            }
        }
        _ => return None,
    })
}
