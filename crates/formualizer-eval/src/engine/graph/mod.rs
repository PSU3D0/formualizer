use crate::SheetId;
use crate::engine::named_range::{NameScope, NamedDefinition, NamedRange};
use crate::engine::sheet_registry::SheetRegistry;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ReferenceType};
use rustc_hash::{FxHashMap, FxHashSet};

#[cfg(test)]
#[derive(Debug, Default, Clone)]
pub struct GraphInstrumentation {
    pub edges_added: u64,
    pub stripe_inserts: u64,
    pub stripe_removes: u64,
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

    // Address mappings using fast hashing
    cell_to_vertex: FxHashMap<CellRef, VertexId>,

    // Scheduling state - using HashSet for O(1) operations
    dirty_vertices: FxHashSet<VertexId>,
    volatile_vertices: FxHashSet<VertexId>,

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

    /// Sheet-scoped named ranges  
    sheet_named_ranges: FxHashMap<(SheetId, String), NamedRange>,

    /// Reverse mapping: vertex -> names it uses (by vertex id)
    vertex_to_names: FxHashMap<VertexId, Vec<VertexId>>,

    /// Lookup for name vertex -> (scope, name) to avoid map scans
    name_vertex_lookup: FxHashMap<VertexId, (NameScope, String)>,

    /// Pending formula vertices referencing names not yet defined
    pending_name_links: FxHashMap<String, Vec<(SheetId, VertexId)>>,

    // Native workbook tables (ListObjects)
    tables: FxHashMap<String, tables::TableEntry>,
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

    // Dynamic topology orderer (Pearce–Kelly) maintained alongside edges when enabled
    pk_order: Option<DynamicTopo<VertexId>>,

    // Spill registry: anchor -> cells, and reverse mapping for blockers
    spill_anchor_to_cells: FxHashMap<VertexId, Vec<CellRef>>,
    spill_cell_to_anchor: FxHashMap<CellRef, VertexId>,

    // Hint: during initial bulk load, many cells are guaranteed new; allow skipping existence checks per-sheet
    first_load_assume_new: bool,
    ensure_touched_sheets: FxHashSet<SheetId>,

    #[cfg(test)]
    instr: GraphInstrumentation,
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

    /// Ensure vertices exist for given coords; allocate missing in contiguous batches and add to edges/index.
    /// Returns a list suitable for edges.add_vertices_batch.
    pub fn ensure_vertices_batch(
        &mut self,
        coords: &[(SheetId, AbsCoord)],
    ) -> Vec<(AbsCoord, u32)> {
        use rustc_hash::FxHashMap;
        let mut grouped: FxHashMap<SheetId, Vec<AbsCoord>> = FxHashMap::default();
        for (sid, pc) in coords.iter().copied() {
            let addr = CellRef::new(sid, Coord::new(pc.row(), pc.col(), true, true));
            if self.cell_to_vertex.contains_key(&addr) {
                continue;
            }
            grouped.entry(sid).or_default().push(pc);
        }
        let mut add_batch: Vec<(AbsCoord, u32)> = Vec::new();
        for (sid, pcs) in grouped {
            if pcs.is_empty() {
                continue;
            }
            // Mark sheet as touched by ensure to disable fast-path new allocations for its values
            self.ensure_touched_sheets.insert(sid);
            let vids = self.store.allocate_contiguous(sid, &pcs, 0x00);
            for (i, pc) in pcs.iter().enumerate() {
                let vid = vids[i];
                add_batch.push((*pc, vid.0));
                let addr = CellRef::new(sid, Coord::new(pc.row(), pc.col(), true, true));
                self.cell_to_vertex.insert(addr, vid);
                // Respect sheet index mode: skip index updates in Lazy mode during bulk ensure
                match self.config.sheet_index_mode {
                    crate::engine::SheetIndexMode::Eager
                    | crate::engine::SheetIndexMode::FastBatch => {
                        self.sheet_index_mut(sid).add_vertex(*pc, vid);
                    }
                    crate::engine::SheetIndexMode::Lazy => {
                        // defer index build
                    }
                }
            }
        }
        if !add_batch.is_empty() {
            self.edges.add_vertices_batch(&add_batch);
        }
        add_batch
    }

    /// Enable/disable the first-load fast path for value inserts.
    pub fn set_first_load_assume_new(&mut self, enabled: bool) {
        self.first_load_assume_new = enabled;
    }

    /// Reset the per-sheet ensure touch tracking.
    pub fn reset_ensure_touched(&mut self) {
        self.ensure_touched_sheets.clear();
    }

    /// Store ASTs in batch and return their arena ids
    pub fn store_asts_batch<'a, I>(&mut self, asts: I) -> Vec<AstNodeId>
    where
        I: IntoIterator<Item = &'a formualizer_parse::parser::ASTNode>,
    {
        self.data_store.store_asts_batch(asts, &self.sheet_reg)
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
    pub fn assign_formula_vertex(&mut self, vid: VertexId, ast_id: AstNodeId, volatile: bool) {
        if self.vertex_formulas.contains_key(&vid) {
            self.remove_dependent_edges(vid);
        }
        self.store
            .set_kind(vid, crate::engine::vertex::VertexKind::FormulaScalar);
        self.vertex_values.remove(&vid);
        self.vertex_formulas.insert(vid, ast_id);
        self.mark_volatile(vid, volatile);
        // schedule evaluation
        self.mark_vertex_dirty(vid);
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
        // Fallback: scan cell map for bounds on the fly
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
        let mut batch: Vec<(AbsCoord, VertexId)> = Vec::with_capacity(self.cell_to_vertex.len());
        for (cref, vid) in &self.cell_to_vertex {
            if cref.sheet_id == sheet_id {
                batch.push((AbsCoord::new(cref.coord.row(), cref.coord.col()), *vid));
            }
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
        // Fallback: scan cell map on the fly
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
            cell_to_vertex: FxHashMap::default(),
            dirty_vertices: FxHashSet::default(),
            volatile_vertices: FxHashSet::default(),
            formula_to_range_deps: FxHashMap::default(),
            stripe_to_dependents: FxHashMap::default(),
            sheet_indexes: FxHashMap::default(),
            sheet_reg,
            default_sheet_id,
            named_ranges: FxHashMap::default(),
            sheet_named_ranges: FxHashMap::default(),
            vertex_to_names: FxHashMap::default(),
            name_vertex_lookup: FxHashMap::default(),
            pending_name_links: FxHashMap::default(),
            tables: FxHashMap::default(),
            table_vertex_lookup: FxHashMap::default(),
            source_scalars: FxHashMap::default(),
            source_tables: FxHashMap::default(),
            source_vertex_lookup: FxHashMap::default(),
            name_vertex_seq: 0,
            source_vertex_seq: 0,
            cell_to_name_dependents: FxHashMap::default(),
            name_to_cell_dependencies: FxHashMap::default(),
            config: config.clone(),
            pk_order: None,
            spill_anchor_to_cells: FxHashMap::default(),
            spill_cell_to_anchor: FxHashMap::default(),
            first_load_assume_new: false,
            ensure_touched_sheets: FxHashSet::default(),
            #[cfg(test)]
            instr: GraphInstrumentation::default(),
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
        self.instr = GraphInstrumentation::default();
    }

    #[cfg(test)]
    pub fn instr(&self) -> GraphInstrumentation {
        self.instr.clone()
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
                self.vertex_formulas.remove(&existing_id);
            }

            // Update to value kind
            self.store.set_kind(existing_id, VertexKind::Cell);
            let value_ref = self.data_store.store_value(value);
            self.vertex_values.insert(existing_id, value_ref);
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
            let value_ref = self.data_store.store_value(value);
            self.vertex_values.insert(vertex_id, value_ref);
            self.cell_to_vertex.insert(addr, vertex_id);
            vertex_id
        };

        Ok(OperationSummary {
            affected_vertices: self.mark_dirty(vertex_id),
            created_placeholders,
        })
    }

    /// Reserve capacity hints for upcoming bulk cell inserts (values only for now).
    pub fn reserve_cells(&mut self, additional: usize) {
        self.store.reserve(additional);
        self.vertex_values.reserve(additional);
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
        let sheet_id = self.sheet_id_mut(sheet);
        let coord = Coord::from_excel(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);
        if let Some(&existing_id) = self.cell_to_vertex.get(&addr) {
            // Overwrite existing value vertex only (ignore formulas in bulk path)
            let value_ref = self.data_store.store_value(value);
            self.vertex_values.insert(existing_id, value_ref);
            self.store.set_kind(existing_id, VertexKind::Cell);
            return;
        }
        let packed_coord = AbsCoord::from_excel(row, col);
        let vertex_id = self.store.allocate(packed_coord, sheet_id, 0x00); // not dirty
        self.edges.add_vertex(packed_coord, vertex_id.0);
        self.sheet_index_mut(sheet_id)
            .add_vertex(packed_coord, vertex_id);
        self.store.set_kind(vertex_id, VertexKind::Cell);
        let value_ref = self.data_store.store_value(value);
        self.vertex_values.insert(vertex_id, value_ref);
        self.cell_to_vertex.insert(addr, vertex_id);
    }

    /// Bulk insert a collection of plain value cells (no formulas) more efficiently.
    pub fn bulk_insert_values<I>(&mut self, sheet: &str, cells: I)
    where
        I: IntoIterator<Item = (u32, u32, LiteralValue)>,
    {
        use web_time::Instant;
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
            let coord = Coord::from_excel(row, col, true, true);
            let addr = CellRef::new(sheet_id, coord);
            if !assume_new && let Some(&existing_id) = self.cell_to_vertex.get(&addr) {
                let value_ref = self.data_store.store_value(value);
                self.vertex_values.insert(existing_id, value_ref);
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
        if !new_value_literals.is_empty() {
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
        let volatile = self.is_ast_volatile(&ast);
        self.set_cell_formula_with_volatility(sheet, row, col, ast, volatile)
    }

    /// Set a formula in a cell with a known volatility flag (context-scoped detection upstream)
    pub fn set_cell_formula_with_volatility(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast: ASTNode,
        volatile: bool,
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
            Some(web_time::Instant::now())
        } else {
            None
        };
        let sheet_id = self.sheet_id_mut(sheet);
        let coord = Coord::from_excel(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);

        // Extract dependencies from AST, creating placeholders if needed
        let t_dep0 = if dbg {
            Some(web_time::Instant::now())
        } else {
            None
        };
        let (
            new_dependencies,
            new_range_dependencies,
            mut created_placeholders,
            named_dependencies,
        ) = self.extract_dependencies(&ast, sheet_id)?;
        if let (true, Some(t)) = (dbg, t_dep0) {
            let elapsed = t.elapsed().as_millis();
            // Only log if over threshold or sampled
            let do_log = (dep_ms_thresh > 0 && elapsed >= dep_ms_thresh)
                || (sample_n > 0 && (row as usize).is_multiple_of(sample_n));
            if dep_ms_thresh == 0 && sample_n == 0 {
                // default: very light sampling every 1000 rows
                if row.is_multiple_of(1000) {
                    eprintln!(
                        "[fz][dep] {}!{} extracted: deps={}, ranges={}, placeholders={}, names={} in {} ms",
                        self.sheet_name(sheet_id),
                        crate::reference::Coord::from_excel(row, col, true, true),
                        new_dependencies.len(),
                        new_range_dependencies.len(),
                        created_placeholders.len(),
                        named_dependencies.len(),
                        elapsed
                    );
                }
            } else if do_log {
                eprintln!(
                    "[fz][dep] {}!{} extracted: deps={}, ranges={}, placeholders={}, names={} in {} ms",
                    self.sheet_name(sheet_id),
                    crate::reference::Coord::from_excel(row, col, true, true),
                    new_dependencies.len(),
                    new_range_dependencies.len(),
                    created_placeholders.len(),
                    named_dependencies.len(),
                    elapsed
                );
            }
        }

        // Check for self-reference (immediate cycle detection)
        let addr_vertex_id = self.get_or_create_vertex(&addr, &mut created_placeholders);

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

        // Update vertex properties
        self.store
            .set_kind(addr_vertex_id, VertexKind::FormulaScalar);
        let ast_id = self.data_store.store_ast(&ast, &self.sheet_reg);
        self.vertex_formulas.insert(addr_vertex_id, ast_id);
        self.store.set_dirty(addr_vertex_id, true);

        // Clear any cached value since this is now a formula
        self.vertex_values.remove(&addr_vertex_id);

        self.mark_volatile(addr_vertex_id, volatile);

        if !named_dependencies.is_empty() {
            self.attach_vertex_to_names(addr_vertex_id, &named_dependencies);
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
        self.add_range_dependent_edges(addr_vertex_id, &new_range_dependencies, sheet_id);

        Ok(OperationSummary {
            affected_vertices: self.mark_dirty(addr_vertex_id),
            created_placeholders,
        })
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
        let sheet_id = self.sheet_reg.get_id(sheet)?;
        let coord = Coord::from_excel(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);

        self.cell_to_vertex.get(&addr).and_then(|&vertex_id| {
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
            let dependents = self.get_dependents(vertex_id);
            to_visit.extend(&dependents);

            if let Some(name_set) = self.cell_to_name_dependents.get(&vertex_id) {
                for &name_vertex in name_set {
                    to_visit.push(name_vertex);
                }
            }

            // Check range dependencies
            let view = self.store.view(vertex_id);
            let row = view.row();
            let col = view.col();
            let dirty_sheet_id = view.sheet_id();

            // New stripe-based dependents lookup
            let mut potential_dependents = FxHashSet::default();

            // 1. Column stripe lookup
            let column_key = StripeKey {
                sheet_id: dirty_sheet_id,
                stripe_type: StripeType::Column,
                index: col,
            };
            if let Some(dependents) = self.stripe_to_dependents.get(&column_key) {
                potential_dependents.extend(dependents);
            }

            // 2. Row stripe lookup
            let row_key = StripeKey {
                sheet_id: dirty_sheet_id,
                stripe_type: StripeType::Row,
                index: row,
            };
            if let Some(dependents) = self.stripe_to_dependents.get(&row_key) {
                potential_dependents.extend(dependents);
            }

            // 3. Block stripe lookup
            if self.config.enable_block_stripes {
                let block_key = StripeKey {
                    sheet_id: dirty_sheet_id,
                    stripe_type: StripeType::Block,
                    index: block_index(row, col),
                };
                if let Some(dependents) = self.stripe_to_dependents.get(&block_key) {
                    potential_dependents.extend(dependents);
                }
            }

            // Precision check: ensure the dirtied cell is actually within the formula's range
            for &dep_id in &potential_dependents {
                if let Some(ranges) = self.formula_to_range_deps.get(&dep_id) {
                    for range in ranges {
                        let range_sheet_id = match range.sheet {
                            SharedSheetLocator::Id(id) => id,
                            _ => dirty_sheet_id,
                        };
                        if range_sheet_id != dirty_sheet_id {
                            continue;
                        }
                        let sr0 = range.start_row.map(|b| b.index).unwrap_or(0);
                        let er0 = range.end_row.map(|b| b.index).unwrap_or(u32::MAX);
                        let sc0 = range.start_col.map(|b| b.index).unwrap_or(0);
                        let ec0 = range.end_col.map(|b| b.index).unwrap_or(u32::MAX);
                        if row >= sr0 && row <= er0 && col >= sc0 && col <= ec0 {
                            to_visit.push(dep_id);
                            break;
                        }
                    }
                }
            }
        }

        while let Some(id) = to_visit.pop() {
            if !visited_for_propagation.insert(id) {
                continue; // Already processed
            }
            affected.insert(id);

            // Mark vertex as dirty
            self.store.set_dirty(id, true);

            // Add direct dependents to visit list
            let dependents = self.get_dependents(id);
            to_visit.extend(&dependents);
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
                self.instr.edges_added += 1;
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
                self.instr.edges_added += 1;
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
        vol_flags: Vec<bool>,
    ) -> Result<usize, ExcelError> {
        use formualizer_parse::parser::CollectPolicy;
        let sheet_id = self.sheet_id_mut(sheet);

        if collected.is_empty() {
            return Ok(0);
        }

        // 1) Build plan across all formulas (read-only, no graph mutation)
        let tiny_refs = collected.iter().map(|(r, c, ast)| (sheet, *r, *c, ast));
        let policy = CollectPolicy {
            expand_small_ranges: true,
            range_expansion_limit: self.config.range_expansion_limit,
            include_names: true,
        };
        let plan = crate::engine::plan::build_dependency_plan(
            &mut self.sheet_reg,
            tiny_refs,
            &policy,
            Some(&vol_flags),
        )?;

        // 2) Ensure/create target vertices and referenced cells (placeholders) once
        let mut created_placeholders: Vec<CellRef> = Vec::new();

        // Targets
        let mut target_vids: Vec<VertexId> = Vec::with_capacity(plan.formula_targets.len());
        for (sid, pc) in &plan.formula_targets {
            let addr = CellRef::new(*sid, Coord::new(pc.row(), pc.col(), true, true));
            let vid = if let Some(&existing) = self.cell_to_vertex.get(&addr) {
                existing
            } else {
                self.get_or_create_vertex(&addr, &mut created_placeholders)
            };
            target_vids.push(vid);
        }

        // Global referenced cells
        let mut dep_vids: Vec<VertexId> = Vec::with_capacity(plan.global_cells.len());
        for (sid, pc) in &plan.global_cells {
            let addr = CellRef::new(*sid, Coord::new(pc.row(), pc.col(), true, true));
            let vid = if let Some(&existing) = self.cell_to_vertex.get(&addr) {
                existing
            } else {
                self.get_or_create_vertex(&addr, &mut created_placeholders)
            };
            dep_vids.push(vid);
        }

        // 3) Store ASTs in batch and update kinds/flags/value map
        let ast_ids = self
            .data_store
            .store_asts_batch(collected.iter().map(|(_, _, ast)| ast), &self.sheet_reg);

        for (i, &tvid) in target_vids.iter().enumerate() {
            // If this cell already had a formula, remove its edges once here
            if self.vertex_formulas.contains_key(&tvid) {
                self.remove_dependent_edges(tvid);
            }
            self.store.set_kind(tvid, VertexKind::FormulaScalar);
            self.store.set_dirty(tvid, true);
            self.vertex_values.remove(&tvid);
            self.vertex_formulas.insert(tvid, ast_ids[i]);
            self.mark_volatile(tvid, vol_flags.get(i).copied().unwrap_or(false));
        }

        // 4) Add edges in one batch
        self.edges.begin_batch();
        for (i, tvid) in target_vids.iter().copied().enumerate() {
            let mut deps: Vec<VertexId> = Vec::new();

            // Map per-formula indices into dep_vids
            if let Some(indices) = plan.per_formula_cells.get(i) {
                deps.reserve(indices.len());
                for &idx in indices {
                    if let Some(vid) = dep_vids.get(idx as usize) {
                        deps.push(*vid);
                    }
                }
            }

            if let Some(names) = plan.per_formula_names.get(i)
                && !names.is_empty()
            {
                let mut name_vertices = Vec::new();
                let formula_sheet = plan
                    .formula_targets
                    .get(i)
                    .map(|(sid, _)| *sid)
                    .unwrap_or(sheet_id);
                for name in names {
                    if let Some(named) = self.resolve_name_entry(name, formula_sheet) {
                        deps.push(named.vertex);
                        name_vertices.push(named.vertex);
                    } else if let Some(source) = self.resolve_source_scalar_entry(name) {
                        deps.push(source.vertex);
                    } else {
                        self.record_pending_name_reference(formula_sheet, name, tvid);
                    }
                }
                if !name_vertices.is_empty() {
                    self.attach_vertex_to_names(tvid, &name_vertices);
                }
            }

            if let Some(tables) = plan.per_formula_tables.get(i)
                && !tables.is_empty()
            {
                for table_name in tables {
                    if let Some(table) = self.resolve_table_entry(table_name) {
                        deps.push(table.vertex);
                    } else if let Some(source) = self.resolve_source_table_entry(table_name) {
                        deps.push(source.vertex);
                    }
                }
            }

            if !deps.is_empty() {
                self.add_dependent_edges_nobatch(tvid, &deps);
            }

            // Range deps from plan are already compact RangeKeys; register directly.
            if let Some(rks) = plan.per_formula_ranges.get(i) {
                self.add_range_deps_from_keys(tvid, rks, sheet_id);
            }
        }
        self.edges.end_batch();

        Ok(collected.len())
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
                                self.instr.stripe_removes += 1;
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
        let value_ref = self.data_store.store_value(value);
        self.vertex_values.insert(vertex_id, value_ref);
    }

    /// Plan a spill region for an anchor; returns #SPILL! if blocked
    pub fn plan_spill_region(
        &self,
        anchor: VertexId,
        target_cells: &[CellRef],
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

            // If cell is occupied by another formula anchor, block regardless of value visibility
            if let Some(&vid) = self.cell_to_vertex.get(cell)
                && vid != anchor
            {
                // Prevent clobbering formulas (array or scalar) in the target area
                match self.store.kind(vid) {
                    VertexKind::FormulaScalar | VertexKind::FormulaArray => {
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
        use rustc_hash::FxHashSet;

        // Capture previous owned cells for this anchor
        let prev_cells = self
            .spill_anchor_to_cells
            .get(&anchor)
            .cloned()
            .unwrap_or_default();
        let new_set: FxHashSet<CellRef> = target_cells.iter().copied().collect();
        let prev_set: FxHashSet<CellRef> = prev_cells.iter().copied().collect();

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
                    let _ = self.set_cell_value(sheet, row + 1, col + 1, old.value.clone());
                }
                return Err(ExcelError::new(ExcelErrorKind::Error)
                    .with_message("Injected persistence fault during spill commit"));
            }
            let _ = self.set_cell_value(&op.sheet, op.row + 1, op.col + 1, op.new_value.clone());
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

    /// Clear an existing spill region for an anchor (set cells to Empty and forget ownership)
    pub fn clear_spill_region(&mut self, anchor: VertexId) {
        if let Some(cells) = self.spill_anchor_to_cells.remove(&anchor) {
            for cell in cells {
                let sheet = self.sheet_name(cell.sheet_id).to_string();
                let _ = self.set_cell_value(
                    &sheet,
                    cell.coord.row() + 1,
                    cell.coord.col() + 1,
                    LiteralValue::Empty,
                );
                self.spill_cell_to_anchor.remove(&cell);
            }
        }
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

    /// Get vertex ID for a cell address
    pub fn get_vertex_id_for_address(&self, addr: &CellRef) -> Option<&VertexId> {
        self.cell_to_vertex.get(addr)
    }

    #[cfg(test)]
    pub fn cell_to_vertex(&self) -> &FxHashMap<CellRef, VertexId> {
        &self.cell_to_vertex
    }

    /// Get the dependencies of a vertex (for scheduler)
    pub(crate) fn get_dependencies(&self, vertex_id: VertexId) -> Vec<VertexId> {
        self.edges.out_edges(vertex_id)
    }

    /// Check if a vertex has a self-loop
    pub(crate) fn has_self_loop(&self, vertex_id: VertexId) -> bool {
        self.edges.out_edges(vertex_id).contains(&vertex_id)
    }

    /// Get dependents of a vertex (vertices that depend on this vertex)
    /// Uses reverse edges for O(1) lookup when available
    pub(crate) fn get_dependents(&self, vertex_id: VertexId) -> Vec<VertexId> {
        // If there are pending changes in delta, we need to scan
        // Otherwise we can use the fast reverse edges
        if self.edges.delta_size() > 0 {
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
        let error = LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref));
        let value_ref = self.data_store.store_value(error);
        self.vertex_values.insert(id, value_ref);
        self.store.set_dirty(id, true);
        self.dirty_vertices.insert(id);
    }

    /// Check if a vertex has a #REF! error
    pub fn is_ref_error(&self, id: VertexId) -> bool {
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

    // ========== Phase 2: Structural Operations ==========
}

// ========== Sheet Management Operations ==========
