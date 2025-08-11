use crate::SheetId;
use crate::engine::named_range::{NameScope, NamedDefinition, NamedRange};
use crate::engine::sheet_registry::SheetRegistry;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};
use rustc_hash::{FxHashMap, FxHashSet};

#[cfg(test)]
#[derive(Debug, Default, Clone)]
pub struct GraphInstrumentation {
    pub edges_added: u64,
    pub stripe_inserts: u64,
    pub stripe_removes: u64,
}

// Type alias for complex return type
type ExtractDependenciesResult =
    Result<(Vec<VertexId>, Vec<ReferenceType>, Vec<CellRef>), ExcelError>;

pub mod editor;
pub mod snapshot;

use super::arena::{AstNodeId, DataStore, ValueRef};
use super::delta_edges::CsrMutableEdges;
use super::packed_coord::PackedCoord;
use super::sheet_index::SheetIndex;
use super::vertex::{VertexId, VertexKind};
use super::vertex_store::{FIRST_NORMAL_VERTEX, VertexStore};
use crate::engine::topo::{
    GraphAdapter,
    pk::{DynamicTopo, PkConfig},
};
use crate::reference::{CellRef, Coord};
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

/// Validate that a name conforms to Excel naming rules
fn is_valid_excel_name(name: &str) -> bool {
    // Excel name rules:
    // 1. Must start with a letter, underscore, or backslash
    // 2. Can contain letters, numbers, periods, and underscores
    // 3. Cannot be a cell reference (like A1, B2, etc.)
    // 4. Cannot exceed 255 characters
    // 5. Cannot contain spaces

    if name.is_empty() || name.len() > 255 {
        return false;
    }

    // Check if it looks like a cell reference (basic check)
    if is_cell_reference(name) {
        return false;
    }

    let mut chars = name.chars();

    // First character must be letter, underscore, or backslash
    if let Some(first) = chars.next() {
        if !first.is_alphabetic() && first != '_' && first != '\\' {
            return false;
        }
    }

    // Remaining characters must be letters, digits, periods, or underscores
    for c in chars {
        if !c.is_alphanumeric() && c != '.' && c != '_' {
            return false;
        }
    }

    true
}

/// Check if a string looks like a cell reference
fn is_cell_reference(s: &str) -> bool {
    // A cell reference must:
    // 1. Start with optional $, then 1-3 letters (column)
    // 2. Followed by optional $, then digits (row)
    // Examples: A1, $A$1, AB123, $XFD$1048576

    let s = s.trim_start_matches('$');

    // Find where letters end and digits begin
    let letter_end = s.chars().position(|c| !c.is_alphabetic() && c != '$');

    if let Some(pos) = letter_end {
        let (col_part, rest) = s.split_at(pos);

        // Column part must be 1-3 letters
        if col_part.is_empty() || col_part.len() > 3 {
            return false;
        }

        // Check if all are letters
        if !col_part.chars().all(|c| c.is_alphabetic()) {
            return false;
        }

        // Rest must be optional $ followed by digits only
        let row_part = rest.trim_start_matches('$');

        // Must have at least one digit and all must be digits
        !row_part.is_empty() && row_part.chars().all(|c| c.is_ascii_digit())
    } else {
        // No digits found, not a cell reference
        false
    }
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

    // Arena-based value and formula storage (Phase 1 complete)
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
    formula_to_range_deps: FxHashMap<VertexId, Vec<ReferenceType>>,

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

    /// Reverse mapping: vertex -> names it uses
    vertex_to_names: FxHashMap<VertexId, Vec<String>>,

    // Evaluation configuration
    config: super::EvalConfig,

    // Dynamic topology orderer (Pearce–Kelly) maintained alongside edges when enabled
    pk_order: Option<DynamicTopo<VertexId>>,

    // Spill registry: anchor -> cells, and reverse mapping for blockers
    spill_anchor_to_cells: FxHashMap<VertexId, Vec<CellRef>>,
    spill_cell_to_anchor: FxHashMap<CellRef, VertexId>,

    #[cfg(test)]
    instr: GraphInstrumentation,
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl DependencyGraph {
    /// Compute min/max used row among vertices within [start_col..=end_col] on a sheet.
    pub fn used_row_bounds_for_columns(
        &self,
        sheet_id: SheetId,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        let index = self.sheet_indexes.get(&sheet_id)?;
        let mut min_r: Option<u32> = None;
        let mut max_r: Option<u32> = None;
        for vid in index.vertices_in_col_range(start_col, end_col) {
            let r = self.store.coord(vid).row();
            min_r = Some(min_r.map(|m| m.min(r)).unwrap_or(r));
            max_r = Some(max_r.map(|m| m.max(r)).unwrap_or(r));
        }
        match (min_r, max_r) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        }
    }

    /// Compute min/max used column among vertices within [start_row..=end_row] on a sheet.
    pub fn used_col_bounds_for_rows(
        &self,
        sheet_id: SheetId,
        start_row: u32,
        end_row: u32,
    ) -> Option<(u32, u32)> {
        let index = self.sheet_indexes.get(&sheet_id)?;
        let mut min_c: Option<u32> = None;
        let mut max_c: Option<u32> = None;
        for vid in index.vertices_in_row_range(start_row, end_row) {
            let c = self.store.coord(vid).col();
            min_c = Some(min_c.map(|m| m.min(c)).unwrap_or(c));
            max_c = Some(max_c.map(|m| m.max(c)).unwrap_or(c));
        }
        match (min_c, max_c) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        }
    }
    pub fn new() -> Self {
        let mut sheet_reg = SheetRegistry::new();
        let default_sheet_id = sheet_reg.id_for("Sheet1");
        Self {
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
            config: super::EvalConfig::default(),
            pk_order: None,
            spill_anchor_to_cells: FxHashMap::default(),
            spill_cell_to_anchor: FxHashMap::default(),
            #[cfg(test)]
            instr: GraphInstrumentation::default(),
        }
    }

    pub fn new_with_config(config: super::EvalConfig) -> Self {
        let mut g = Self {
            config: config.clone(),
            ..Self::new()
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

    /// Returns the name of a sheet given its ID.
    pub fn sheet_name(&self, id: SheetId) -> &str {
        self.sheet_reg.name(id)
    }

    /// Converts a `CellRef` to a fully qualified A1-style string (e.g., "Sheet1!A1").
    pub fn to_a1(&self, cell_ref: CellRef) -> String {
        format!("{}!{}", self.sheet_name(cell_ref.sheet_id), cell_ref.coord)
    }

    // Named Range Methods

    /// Define a new named range
    pub fn define_name(
        &mut self,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), ExcelError> {
        // Validate name
        if !is_valid_excel_name(name) {
            return Err(
                ExcelError::new(ExcelErrorKind::Name).with_message(format!("Invalid name: {name}"))
            );
        }

        // Check for duplicates
        match scope {
            NameScope::Workbook => {
                if self.named_ranges.contains_key(name) {
                    return Err(ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("Name already exists: {name}")));
                }
            }
            NameScope::Sheet(sheet_id) => {
                if self
                    .sheet_named_ranges
                    .contains_key(&(sheet_id, name.to_string()))
                {
                    return Err(ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("Name already exists in sheet: {name}")));
                }
            }
        }

        // Extract dependencies if formula
        let named_range = match definition {
            NamedDefinition::Formula { ref ast, .. } => {
                let (deps, range_deps, _) = self.extract_dependencies(
                    ast,
                    match scope {
                        NameScope::Sheet(id) => id,
                        NameScope::Workbook => self.default_sheet_id,
                    },
                )?;
                NamedRange {
                    definition: NamedDefinition::Formula {
                        ast: ast.clone(),
                        dependencies: deps,
                        range_deps,
                    },
                    scope,
                    dependents: FxHashSet::default(),
                }
            }
            _ => NamedRange {
                definition,
                scope,
                dependents: FxHashSet::default(),
            },
        };

        // Store
        match scope {
            NameScope::Workbook => {
                self.named_ranges.insert(name.to_string(), named_range);
            }
            NameScope::Sheet(id) => {
                self.sheet_named_ranges
                    .insert((id, name.to_string()), named_range);
            }
        }

        Ok(())
    }

    /// Resolve a named range to its definition
    pub fn resolve_name(&self, name: &str, current_sheet: SheetId) -> Option<&NamedDefinition> {
        // Sheet scope takes precedence
        self.sheet_named_ranges
            .get(&(current_sheet, name.to_string()))
            .or_else(|| self.named_ranges.get(name))
            .map(|nr| &nr.definition)
    }

    /// Update an existing named range definition
    pub fn update_name(
        &mut self,
        name: &str,
        new_definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), ExcelError> {
        // First collect dependents to avoid borrow checker issues
        let dependents_to_dirty = match scope {
            NameScope::Workbook => self
                .named_ranges
                .get(name)
                .map(|nr| nr.dependents.iter().copied().collect::<Vec<_>>()),
            NameScope::Sheet(id) => self
                .sheet_named_ranges
                .get(&(id, name.to_string()))
                .map(|nr| nr.dependents.iter().copied().collect::<Vec<_>>()),
        };

        if let Some(dependents) = dependents_to_dirty {
            // Mark all dependents as dirty
            for vertex_id in dependents {
                self.mark_vertex_dirty(vertex_id);
            }

            // Now update the definition
            let named_range = match scope {
                NameScope::Workbook => self.named_ranges.get_mut(name),
                NameScope::Sheet(id) => self.sheet_named_ranges.get_mut(&(id, name.to_string())),
            };

            if let Some(named_range) = named_range {
                // Update definition
                named_range.definition = new_definition;

                // Clear dependents (will be rebuilt on next evaluation)
                named_range.dependents.clear();
            }

            Ok(())
        } else {
            Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Name not found: {name}")))
        }
    }

    /// Delete a named range
    pub fn delete_name(&mut self, name: &str, scope: NameScope) -> Result<(), ExcelError> {
        let named_range = match scope {
            NameScope::Workbook => self.named_ranges.remove(name),
            NameScope::Sheet(id) => self.sheet_named_ranges.remove(&(id, name.to_string())),
        };

        if let Some(named_range) = named_range {
            // Mark all dependent formulas as needing recalculation with #NAME! error
            for &vertex_id in &named_range.dependents {
                // Mark as dirty to trigger recalculation (will error during eval)
                self.mark_vertex_dirty(vertex_id);
                // Remove from vertex_to_names mapping
                if let Some(names) = self.vertex_to_names.get_mut(&vertex_id) {
                    names.retain(|n| n != name);
                }
            }
            Ok(())
        } else {
            Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Name not found: {name}")))
        }
    }

    #[cfg(test)]
    pub(crate) fn formula_to_range_deps(&self) -> &FxHashMap<VertexId, Vec<ReferenceType>> {
        &self.formula_to_range_deps
    }

    #[cfg(test)]
    pub(crate) fn stripe_to_dependents(&self) -> &FxHashMap<StripeKey, FxHashSet<VertexId>> {
        &self.stripe_to_dependents
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
        let coord = Coord::new(row, col, true, true); // Assuming absolute reference for direct sets
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
            let packed_coord = PackedCoord::new(row, col);
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
        let sheet_id = self.sheet_id_mut(sheet);
        let coord = Coord::new(row, col, true, true);
        let addr = CellRef::new(sheet_id, coord);

        // Extract dependencies from AST, creating placeholders if needed
        let (new_dependencies, new_range_dependencies, mut created_placeholders) =
            self.extract_dependencies(&ast, sheet_id)?;

        // Check for self-reference (immediate cycle detection)
        let addr_vertex_id = self.get_or_create_vertex(&addr, &mut created_placeholders);

        if new_dependencies.contains(&addr_vertex_id) {
            return Err(ExcelError::new(ExcelErrorKind::Circ)
                .with_message("Self-reference detected".to_string()));
        }

        // Remove old dependencies first
        self.remove_dependent_edges(addr_vertex_id);

        // Update vertex properties
        self.store
            .set_kind(addr_vertex_id, VertexKind::FormulaScalar);
        let ast_id = self.data_store.store_ast(&ast, &self.sheet_reg);
        self.vertex_formulas.insert(addr_vertex_id, ast_id);
        self.store.set_dirty(addr_vertex_id, true);

        // Clear any cached value since this is now a formula
        self.vertex_values.remove(&addr_vertex_id);

        if volatile {
            self.store.set_volatile(addr_vertex_id, true);
        }

        // Add new dependency edges
        self.add_dependent_edges(addr_vertex_id, &new_dependencies);
        self.add_range_dependent_edges(addr_vertex_id, &new_range_dependencies, sheet_id);

        // Mark as volatile if needed
        if volatile {
            self.volatile_vertices.insert(addr_vertex_id);
        }

        Ok(OperationSummary {
            affected_vertices: self.mark_dirty(addr_vertex_id),
            created_placeholders,
        })
    }

    /// Get current value from a cell
    pub fn get_cell_value(&self, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
        let sheet_id = self.sheet_reg.get_id(sheet)?;
        let coord = Coord::new(row, col, true, true);
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
            VertexKind::FormulaScalar | VertexKind::FormulaArray
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
                        if let ReferenceType::Range {
                            sheet,
                            start_row,
                            start_col,
                            end_row,
                            end_col,
                        } = range
                        {
                            let range_sheet_name = sheet
                                .as_deref()
                                .unwrap_or_else(|| self.sheet_name(dirty_sheet_id));
                            if let Some(range_sheet_id) = self.sheet_reg.get_id(range_sheet_name) {
                                if range_sheet_id == dirty_sheet_id
                                    && row >= start_row.unwrap_or(1)
                                    && row <= end_row.unwrap_or(u32::MAX)
                                    && col >= start_col.unwrap_or(1)
                                    && col <= end_col.unwrap_or(u32::MAX)
                                {
                                    to_visit.push(dep_id);
                                    break; // Found a matching range
                                }
                            }
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
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
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

    // Helper methods
    fn extract_dependencies(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
    ) -> ExtractDependenciesResult {
        let mut dependencies = FxHashSet::default();
        let mut range_dependencies = Vec::new();
        let mut created_placeholders = Vec::new();
        self.extract_dependencies_recursive(
            ast,
            current_sheet_id,
            &mut dependencies,
            &mut range_dependencies,
            &mut created_placeholders,
        )?;

        // Deduplicate range references
        let mut deduped_ranges = Vec::new();
        for range_ref in range_dependencies {
            if !deduped_ranges.contains(&range_ref) {
                deduped_ranges.push(range_ref);
            }
        }

        Ok((
            dependencies.into_iter().collect(),
            deduped_ranges,
            created_placeholders,
        ))
    }

    fn extract_dependencies_recursive(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
        dependencies: &mut FxHashSet<VertexId>,
        range_dependencies: &mut Vec<ReferenceType>,
        created_placeholders: &mut Vec<CellRef>,
    ) -> Result<(), ExcelError> {
        match &ast.node_type {
            ASTNodeType::Reference { reference, .. } => {
                match reference {
                    ReferenceType::Cell { .. } => {
                        let vertex_id = self.get_or_create_vertex_for_reference(
                            reference,
                            current_sheet_id,
                            created_placeholders,
                        )?;
                        dependencies.insert(vertex_id);
                    }
                    ReferenceType::Range {
                        sheet,
                        start_row,
                        start_col,
                        end_row,
                        end_col,
                    } => {
                        // If any bound is missing (infinite/partial range), always keep compressed
                        let has_unbounded = start_row.is_none()
                            || end_row.is_none()
                            || start_col.is_none()
                            || end_col.is_none();
                        if has_unbounded {
                            range_dependencies.push(reference.clone());
                        } else {
                            let sr = start_row.unwrap();
                            let sc = start_col.unwrap();
                            let er = end_row.unwrap();
                            let ec = end_col.unwrap();

                            if sr > er || sc > ec {
                                return Err(ExcelError::new(ExcelErrorKind::Ref));
                            }

                            let height = er.saturating_sub(sr) + 1;
                            let width = ec.saturating_sub(sc) + 1;
                            let size = (width * height) as usize;

                            if size <= self.config.range_expansion_limit {
                                // Expand to individual cells
                                let sheet_id = match sheet {
                                    Some(name) => self.sheet_id_mut(name),
                                    None => current_sheet_id,
                                };
                                for row in sr..=er {
                                    for col in sc..=ec {
                                        let coord = Coord::new(row, col, true, true);
                                        let addr = CellRef::new(sheet_id, coord);
                                        let vertex_id =
                                            self.get_or_create_vertex(&addr, created_placeholders);
                                        dependencies.insert(vertex_id);
                                    }
                                }
                            } else {
                                // Keep as a compressed range dependency
                                range_dependencies.push(reference.clone());
                            }
                        }
                    }
                    ReferenceType::NamedRange(name) => {
                        // Resolve the named range
                        if let Some(definition) = self.resolve_name(name, current_sheet_id) {
                            // Clone the definition to avoid borrow issues
                            let definition = definition.clone();

                            match definition {
                                NamedDefinition::Cell(cell_ref) => {
                                    let vertex_id =
                                        self.get_or_create_vertex(&cell_ref, created_placeholders);
                                    dependencies.insert(vertex_id);
                                }
                                NamedDefinition::Range(range_ref) => {
                                    // Calculate range size
                                    let height = range_ref
                                        .end
                                        .coord
                                        .row
                                        .saturating_sub(range_ref.start.coord.row)
                                        + 1;
                                    let width = range_ref
                                        .end
                                        .coord
                                        .col
                                        .saturating_sub(range_ref.start.coord.col)
                                        + 1;
                                    let size = (width * height) as usize;

                                    if size <= self.config.range_expansion_limit {
                                        // Expand to individual cells
                                        for row in
                                            range_ref.start.coord.row..=range_ref.end.coord.row
                                        {
                                            for col in
                                                range_ref.start.coord.col..=range_ref.end.coord.col
                                            {
                                                let coord = Coord::new(row, col, true, true);
                                                let addr =
                                                    CellRef::new(range_ref.start.sheet_id, coord);
                                                let vertex_id = self.get_or_create_vertex(
                                                    &addr,
                                                    created_placeholders,
                                                );
                                                dependencies.insert(vertex_id);
                                            }
                                        }
                                    } else {
                                        // Keep as compressed range
                                        let sheet_name = self.sheet_name(range_ref.start.sheet_id);
                                        range_dependencies.push(ReferenceType::Range {
                                            sheet: Some(sheet_name.to_string()),
                                            start_row: Some(range_ref.start.coord.row),
                                            start_col: Some(range_ref.start.coord.col),
                                            end_row: Some(range_ref.end.coord.row),
                                            end_col: Some(range_ref.end.coord.col),
                                        });
                                    }
                                }
                                NamedDefinition::Formula {
                                    dependencies: formula_deps,
                                    range_deps,
                                    ..
                                } => {
                                    // Add pre-computed dependencies
                                    dependencies.extend(formula_deps);
                                    range_dependencies.extend(range_deps);
                                }
                            }

                            // Note: We should track that this vertex uses this name for invalidation
                            // This will be done after the vertex is created in set_cell_formula
                        } else {
                            return Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined name: {name}")));
                        }
                    }
                    _ => {} // Ignore others for now
                }
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.extract_dependencies_recursive(
                    left,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                )?;
                self.extract_dependencies_recursive(
                    right,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                )?;
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.extract_dependencies_recursive(
                    expr,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                )?;
            }
            ASTNodeType::Function { args, .. } => {
                for arg in args {
                    self.extract_dependencies_recursive(
                        arg,
                        current_sheet_id,
                        dependencies,
                        range_dependencies,
                        created_placeholders,
                    )?;
                }
            }
            ASTNodeType::Array(rows) => {
                for row in rows {
                    for cell in row {
                        self.extract_dependencies_recursive(
                            cell,
                            current_sheet_id,
                            dependencies,
                            range_dependencies,
                            created_placeholders,
                        )?;
                    }
                }
            }
            ASTNodeType::Literal(_) => {
                // Literals have no dependencies
            }
        }
        Ok(())
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
        let packed_coord = PackedCoord::new(addr.coord.row, addr.coord.col);
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

    /// Gets the VertexId for a reference, creating a placeholder vertex if it doesn't exist.
    fn get_or_create_vertex_for_reference(
        &mut self,
        reference: &ReferenceType,
        current_sheet_id: SheetId,
        created_placeholders: &mut Vec<CellRef>,
    ) -> Result<VertexId, ExcelError> {
        match reference {
            ReferenceType::Cell { sheet, row, col } => {
                let sheet_id = match sheet {
                    Some(name) => self.sheet_id_mut(name),
                    None => current_sheet_id,
                };
                let coord = Coord::new(*row, *col, true, true);
                let addr = CellRef::new(sheet_id, coord);
                Ok(self.get_or_create_vertex(&addr, created_placeholders))
            }
            _ => Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Expected a cell reference, but got a range or other type.")),
        }
    }

    fn is_ast_volatile(&self, ast: &ASTNode) -> bool {
        match &ast.node_type {
            ASTNodeType::Function { name, args, .. } => {
                if let Some(func) = crate::function_registry::get("", name) {
                    if func.caps().contains(crate::function::FnCaps::VOLATILE) {
                        return true;
                    }
                }
                args.iter().any(|arg| self.is_ast_volatile(arg))
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.is_ast_volatile(left) || self.is_ast_volatile(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.is_ast_volatile(expr),
            ASTNodeType::Array(rows) => rows
                .iter()
                .any(|row| row.iter().any(|cell| self.is_ast_volatile(cell))),
            _ => false,
        }
    }

    fn add_dependent_edges(&mut self, dependent: VertexId, dependencies: &[VertexId]) {
        // Batch to avoid repeated CSR rebuilds and keep reverse edges current
        self.edges.begin_batch();

        // If PK enabled, update order using a short-lived adapter without holding &mut self
        // Track dependencies that should be skipped if rejecting cycle-creating edges
        let mut skip_deps: rustc_hash::FxHashSet<VertexId> = rustc_hash::FxHashSet::default();
        if self.pk_order.is_some() {
            if let Some(mut pk) = self.pk_order.take() {
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

    fn add_range_dependent_edges(
        &mut self,
        dependent: VertexId,
        ranges: &[ReferenceType],
        current_sheet_id: SheetId,
    ) {
        if ranges.is_empty() {
            return;
        }
        self.formula_to_range_deps
            .insert(dependent, ranges.to_vec());

        for range in ranges {
            if let ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } = range
            {
                let sheet_id = match sheet {
                    Some(name) => self.sheet_id_mut(name),
                    None => current_sheet_id,
                };
                let s_row = *start_row;
                let e_row = *end_row;
                let s_col = *start_col;
                let e_col = *end_col;

                // Decide coarse stripes for invalidation
                let col_stripes = (s_row.is_none() && e_row.is_none())
                    || (s_col.is_some() && e_col.is_some() && (s_row.is_none() || e_row.is_none())); // partial rows, fixed columns
                let row_stripes = (s_col.is_none() && e_col.is_none())
                    || (s_row.is_some() && e_row.is_some() && (s_col.is_none() || e_col.is_none())); // partial cols, fixed rows

                if col_stripes && !row_stripes {
                    let sc = s_col.unwrap_or(1);
                    let ec = e_col.unwrap_or(sc);
                    for col in sc..=ec {
                        let key = StripeKey {
                            sheet_id,
                            stripe_type: StripeType::Column,
                            index: col,
                        };
                        self.stripe_to_dependents
                            .entry(key.clone())
                            .or_default()
                            .insert(dependent);
                        #[cfg(test)]
                        {
                            // Count only when inserting into an empty set
                            if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1) {
                                self.instr.stripe_inserts += 1;
                            }
                        }
                    }
                    continue;
                }

                if row_stripes && !col_stripes {
                    let sr = s_row.unwrap_or(1);
                    let er = e_row.unwrap_or(sr);
                    for row in sr..=er {
                        let key = StripeKey {
                            sheet_id,
                            stripe_type: StripeType::Row,
                            index: row,
                        };
                        self.stripe_to_dependents
                            .entry(key.clone())
                            .or_default()
                            .insert(dependent);
                        #[cfg(test)]
                        {
                            if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1) {
                                self.instr.stripe_inserts += 1;
                            }
                        }
                    }
                    continue;
                }

                // Finite rectangle (or ambiguous): fall back to block/row/col heuristic
                let start_row = s_row.unwrap_or(1);
                let start_col = s_col.unwrap_or(1);
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
                            let key = StripeKey {
                                sheet_id,
                                stripe_type: StripeType::Block,
                                index: block_index(block_row * BLOCK_H, block_col * BLOCK_W),
                            };
                            self.stripe_to_dependents
                                .entry(key.clone())
                                .or_default()
                                .insert(dependent);
                            #[cfg(test)]
                            {
                                if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1) {
                                    self.instr.stripe_inserts += 1;
                                }
                            }
                        }
                    }
                } else if height > width {
                    // Tall range
                    for col in start_col..=end_col {
                        let key = StripeKey {
                            sheet_id,
                            stripe_type: StripeType::Column,
                            index: col,
                        };
                        self.stripe_to_dependents
                            .entry(key.clone())
                            .or_default()
                            .insert(dependent);
                        #[cfg(test)]
                        {
                            if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1) {
                                self.instr.stripe_inserts += 1;
                            }
                        }
                    }
                } else {
                    // Wide range
                    for row in start_row..=end_row {
                        let key = StripeKey {
                            sheet_id,
                            stripe_type: StripeType::Row,
                            index: row,
                        };
                        self.stripe_to_dependents
                            .entry(key.clone())
                            .or_default()
                            .insert(dependent);
                        #[cfg(test)]
                        {
                            if self.stripe_to_dependents.get(&key).map(|s| s.len()) == Some(1) {
                                self.instr.stripe_inserts += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    fn remove_dependent_edges(&mut self, vertex: VertexId) {
        // Remove all outgoing edges from this vertex (its dependencies)
        let dependencies = self.edges.out_edges(vertex);

        self.edges.begin_batch();
        if self.pk_order.is_some() {
            if let Some(mut pk) = self.pk_order.take() {
                for dep in &dependencies {
                    pk.remove_edge(*dep, vertex);
                }
                self.pk_order = Some(pk);
            }
        }
        for dep in dependencies {
            self.edges.remove_edge(vertex, dep);
        }
        self.edges.end_batch();

        // Remove range dependencies and clean up stripes
        if let Some(old_ranges) = self.formula_to_range_deps.remove(&vertex) {
            let old_sheet_id = self.store.sheet_id(vertex);

            for range in &old_ranges {
                if let ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                } = range
                {
                    let sheet_id = match sheet {
                        Some(name) => self.sheet_reg.get_id(name).unwrap_or(old_sheet_id),
                        None => old_sheet_id,
                    };
                    let s_row = *start_row;
                    let e_row = *end_row;
                    let s_col = *start_col;
                    let e_col = *end_col;

                    let mut keys_to_clean = FxHashSet::default();

                    let col_stripes = (s_row.is_none() && e_row.is_none())
                        || (s_col.is_some()
                            && e_col.is_some()
                            && (s_row.is_none() || e_row.is_none()));
                    let row_stripes = (s_col.is_none() && e_col.is_none())
                        || (s_row.is_some()
                            && e_row.is_some()
                            && (s_col.is_none() || e_col.is_none()));

                    if col_stripes && !row_stripes {
                        let sc = s_col.unwrap_or(1);
                        let ec = e_col.unwrap_or(sc);
                        for col in sc..=ec {
                            keys_to_clean.insert(StripeKey {
                                sheet_id,
                                stripe_type: StripeType::Column,
                                index: col,
                            });
                        }
                    } else if row_stripes && !col_stripes {
                        let sr = s_row.unwrap_or(1);
                        let er = e_row.unwrap_or(sr);
                        for row in sr..=er {
                            keys_to_clean.insert(StripeKey {
                                sheet_id,
                                stripe_type: StripeType::Row,
                                index: row,
                            });
                        }
                    } else {
                        let start_row = s_row.unwrap_or(1);
                        let start_col = s_col.unwrap_or(1);
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
                                        index: block_index(
                                            block_row * BLOCK_H,
                                            block_col * BLOCK_W,
                                        ),
                                    });
                                }
                            }
                        } else if height > width {
                            // Tall range
                            for col in start_col..=end_col {
                                keys_to_clean.insert(StripeKey {
                                    sheet_id,
                                    stripe_type: StripeType::Column,
                                    index: col,
                                });
                            }
                        } else {
                            // Wide range
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
                let r = cell.coord.row;
                let c = cell.coord.col;
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
            if let Some(&vid) = self.cell_to_vertex.get(cell) {
                if vid != anchor {
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
                    row: cell.coord.row,
                    col: cell.coord.col,
                    new_value: LiteralValue::Empty,
                });
            }
        }

        // Writes for new values (row-major to match target rectangle)
        if !target_cells.is_empty() {
            let first = target_cells.first().copied().unwrap();
            let row0 = first.coord.row;
            let col0 = first.coord.col;
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
            let old = self
                .get_cell_value(&op.sheet, op.row, op.col)
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
        let mut applied = 0usize;
        for op in &ops {
            // Inject fault after N operations
            if let Some(n) = fault_after_ops {
                if applied == n {
                    // Roll back all applied ops
                    for idx in (0..applied).rev() {
                        let ((ref sheet, row, col), ref old) = old_values[idx];
                        let _ = self.set_cell_value(sheet, row, col, old.value.clone());
                    }
                    return Err(ExcelError::new(ExcelErrorKind::Error)
                        .with_message("Injected persistence fault during spill commit"));
                }
            }

            let _ = self.set_cell_value(&op.sheet, op.row, op.col, op.new_value.clone());
            applied += 1;
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

    /// Clear an existing spill region for an anchor (set cells to Empty and forget ownership)
    pub fn clear_spill_region(&mut self, anchor: VertexId) {
        if let Some(cells) = self.spill_anchor_to_cells.remove(&anchor) {
            for cell in cells {
                let sheet = self.sheet_name(cell.sheet_id).to_string();
                let _ = self.set_cell_value(
                    &sheet,
                    cell.coord.row,
                    cell.coord.col,
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

    /// Get the formula AST for a vertex
    pub(crate) fn get_formula(&self, vertex_id: VertexId) -> Option<ASTNode> {
        self.vertex_formulas
            .get(&vertex_id)
            .and_then(|&ast_id| self.data_store.retrieve_ast(ast_id, &self.sheet_reg))
    }

    /// Get the value stored for a vertex
    pub(crate) fn get_value(&self, vertex_id: VertexId) -> Option<LiteralValue> {
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

    /// Create a cell reference from sheet name and coordinates
    pub fn make_cell_ref(&self, sheet_name: &str, row: u32, col: u32) -> CellRef {
        let sheet_id = self.sheet_reg.get_id(sheet_name).unwrap_or(0);
        self.make_cell_ref_internal(sheet_id, row, col)
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
        if self.pk_order.is_some() {
            if let Some(mut pk) = self.pk_order.take() {
                for dependent in &dependents {
                    pk.remove_edge(id, *dependent);
                }
                self.pk_order = Some(pk);
            }
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
    pub fn set_coord(&mut self, id: VertexId, coord: PackedCoord) {
        self.store.set_coord(id, coord);
    }

    /// Update edge cache coordinate
    #[doc(hidden)]
    pub fn update_edge_coord(&mut self, id: VertexId, coord: PackedCoord) {
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
    pub fn get_coord(&self, id: VertexId) -> PackedCoord {
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

    /// Get all vertices with formulas
    pub fn vertices_with_formulas(&self) -> impl Iterator<Item = VertexId> + '_ {
        self.vertex_formulas.keys().copied()
    }

    /// Update a vertex's formula
    pub fn update_vertex_formula(&mut self, id: VertexId, ast: ASTNode) -> Result<(), ExcelError> {
        // Get the sheet_id for this vertex
        let sheet_id = self.store.sheet_id(id);

        // Extract dependencies from AST
        let (new_dependencies, new_range_dependencies, _) =
            self.extract_dependencies(&ast, sheet_id)?;

        // Remove old dependencies first
        self.remove_dependent_edges(id);

        // Store the new formula
        let ast_id = self.data_store.store_ast(&ast, &self.sheet_reg);
        self.vertex_formulas.insert(id, ast_id);

        // Add new dependency edges
        self.add_dependent_edges(id, &new_dependencies);
        self.add_range_dependent_edges(id, &new_range_dependencies, sheet_id);

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

    /// Adjust named ranges during row/column operations
    pub fn adjust_named_ranges(
        &mut self,
        operation: &crate::engine::graph::editor::reference_adjuster::ShiftOperation,
    ) -> Result<(), ExcelError> {
        let adjuster = crate::engine::graph::editor::reference_adjuster::ReferenceAdjuster::new();

        // Adjust workbook-scoped names
        for named_range in self.named_ranges.values_mut() {
            adjust_named_definition(&mut named_range.definition, &adjuster, operation)?;
        }

        // Adjust sheet-scoped names
        for named_range in self.sheet_named_ranges.values_mut() {
            adjust_named_definition(&mut named_range.definition, &adjuster, operation)?;
        }

        Ok(())
    }

    /// Mark a vertex as having a #NAME! error
    pub fn mark_as_name_error(&mut self, vertex_id: VertexId) {
        // Mark the vertex as dirty
        self.mark_vertex_dirty(vertex_id);
        // In a real implementation, we would store the error in the vertex value
        // For now, just mark it dirty so it will error on next evaluation
    }
}

// ========== Sheet Management Operations ==========
// This impl block contains all sheet-level operations like adding, removing,
// renaming, and duplicating sheets. These operations handle the complex task
// of maintaining consistency across all graph structures when sheets change.
impl DependencyGraph {
    /// Add a new sheet to the workbook
    ///
    /// Creates a new sheet with the given name. If a sheet with this name
    /// already exists, returns its ID without error (idempotent operation).
    ///
    /// # Arguments
    /// * `name` - The name for the new sheet
    ///
    /// # Returns
    /// The SheetId of the new or existing sheet
    pub fn add_sheet(&mut self, name: &str) -> Result<SheetId, ExcelError> {
        // Check if sheet already exists
        if let Some(id) = self.sheet_reg.get_id(name) {
            // Sheet already exists, return its ID (idempotent)
            return Ok(id);
        }

        // Create new sheet
        let sheet_id = self.sheet_reg.id_for(name);

        // Initialize sheet index for the new sheet
        self.sheet_indexes.entry(sheet_id).or_default();

        Ok(sheet_id)
    }

    /// Remove a sheet from the workbook
    ///
    /// Removes the specified sheet and all its contents. This operation:
    /// - Deletes all vertices (cells/formulas) in the sheet
    /// - Updates all formulas that reference the deleted sheet to show #REF! errors
    /// - Removes sheet-scoped named ranges
    /// - Cleans up all internal structures
    ///
    /// # Arguments
    /// * `sheet_id` - The ID of the sheet to remove
    ///
    /// # Errors
    /// Returns an error if:
    /// - The sheet doesn't exist
    /// - Trying to remove the last sheet (workbook must have at least one sheet)
    pub fn remove_sheet(&mut self, sheet_id: SheetId) -> Result<(), ExcelError> {
        // Check if sheet exists
        if self.sheet_reg.name(sheet_id).is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Sheet does not exist"));
        }

        // Prevent removing the last sheet
        let sheet_count = self.sheet_reg.all_sheets().len();
        if sheet_count <= 1 {
            return Err(
                ExcelError::new(ExcelErrorKind::Value).with_message("Cannot remove the last sheet")
            );
        }

        // Begin batch operations for efficiency
        self.begin_batch();

        // Collect all vertices in the sheet to be deleted
        let vertices_to_delete: Vec<VertexId> = self.vertices_in_sheet(sheet_id).collect();

        // Find all formulas that reference cells in the deleted sheet
        let mut formulas_to_update = Vec::new();
        for &formula_id in self.vertex_formulas.keys() {
            let deps = self.edges.out_edges(formula_id);
            for dep_id in deps {
                if self.store.sheet_id(dep_id) == sheet_id {
                    formulas_to_update.push(formula_id);
                    break;
                }
            }
        }

        // Mark formulas that reference the deleted sheet with #REF! errors
        for formula_id in formulas_to_update {
            self.mark_as_ref_error(formula_id);
        }

        // Delete all vertices in the sheet
        for vertex_id in vertices_to_delete {
            // Remove from cell mapping
            if let Some(cell_ref) = self.get_cell_ref_for_vertex(vertex_id) {
                self.cell_to_vertex.remove(&cell_ref);
            }

            // Remove all edges
            self.remove_all_edges(vertex_id);

            // Remove from sheet index
            let coord = self.store.coord(vertex_id);
            if let Some(index) = self.sheet_indexes.get_mut(&sheet_id) {
                index.remove_vertex(coord, vertex_id);
            }

            // Remove formula and value data
            self.vertex_formulas.remove(&vertex_id);
            self.vertex_values.remove(&vertex_id);

            // Mark as deleted in store
            self.mark_deleted(vertex_id, true);
        }

        // Remove sheet-scoped named ranges
        let sheet_names_to_remove: Vec<(SheetId, String)> = self
            .sheet_named_ranges
            .keys()
            .filter(|(sid, _)| *sid == sheet_id)
            .cloned()
            .collect();

        for key in sheet_names_to_remove {
            self.sheet_named_ranges.remove(&key);
        }

        // Remove sheet index
        self.sheet_indexes.remove(&sheet_id);

        // If this was the default sheet, pick a new default
        if self.default_sheet_id == sheet_id {
            // Pick the first available sheet as the new default
            if let Some(&new_default) = self.sheet_indexes.keys().next() {
                self.default_sheet_id = new_default;
            }
        }

        // Remove the sheet from the registry
        self.sheet_reg.remove(sheet_id)?;

        // End batch operations
        self.end_batch();

        Ok(())
    }

    /// Rename an existing sheet
    ///
    /// Changes the name of a sheet while preserving all its contents and references.
    /// All formulas that reference the sheet by name are automatically updated.
    ///
    /// # Arguments
    /// * `sheet_id` - The ID of the sheet to rename
    /// * `new_name` - The new name for the sheet
    ///
    /// # Errors
    /// Returns an error if:
    /// - The sheet doesn't exist
    /// - The new name is already used by another sheet
    /// - The new name is invalid (empty or too long)
    pub fn rename_sheet(&mut self, sheet_id: SheetId, new_name: &str) -> Result<(), ExcelError> {
        // Validate new name
        if new_name.is_empty() || new_name.len() > 255 {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Invalid sheet name"));
        }

        // Check if sheet exists
        let old_name = self.sheet_reg.name(sheet_id);
        if old_name.is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Sheet does not exist"));
        }

        // Check if new name is already taken
        if let Some(existing_id) = self.sheet_reg.get_id(new_name) {
            if existing_id != sheet_id {
                return Err(ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("Sheet '{new_name}' already exists")));
            }
            // Same name, nothing to do
            return Ok(());
        }

        // Store old name for formula updates
        let old_name = old_name.to_string();

        // Update sheet registry
        self.sheet_reg.rename(sheet_id, new_name)?;

        // Update all formulas that reference the old sheet name
        // This requires parsing and updating AST nodes
        let formulas_to_update: Vec<VertexId> = self.vertex_formulas.keys().copied().collect();

        for formula_id in formulas_to_update {
            if let Some(ast) = self.get_formula(formula_id) {
                let updated_ast = update_sheet_references_in_ast(&ast, &old_name, new_name);
                if ast != updated_ast {
                    // Formula changed, update it
                    let ast_id = self.data_store.store_ast(&updated_ast, &self.sheet_reg);
                    self.vertex_formulas.insert(formula_id, ast_id);
                    self.mark_vertex_dirty(formula_id);
                }
            }
        }

        Ok(())
    }

    /// Duplicate an existing sheet
    ///
    /// Creates a copy of a sheet with all its contents. The new sheet will have
    /// the specified name and contain copies of all cells, values, and formulas
    /// from the source sheet. Formula references within the sheet are updated
    /// to point to the new sheet.
    ///
    /// # Arguments
    /// * `source_sheet_id` - The ID of the sheet to duplicate
    /// * `new_name` - The name for the new sheet
    ///
    /// # Returns
    /// The SheetId of the newly created sheet
    ///
    /// # Errors
    /// Returns an error if:
    /// - The source sheet doesn't exist
    /// - The new name is already used
    /// - The new name is invalid
    pub fn duplicate_sheet(
        &mut self,
        source_sheet_id: SheetId,
        new_name: &str,
    ) -> Result<SheetId, ExcelError> {
        // Validate new name
        if new_name.is_empty() || new_name.len() > 255 {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Invalid sheet name"));
        }

        // Check if source sheet exists
        let source_name = self.sheet_reg.name(source_sheet_id).to_string();
        if source_name.is_empty() {
            return Err(
                ExcelError::new(ExcelErrorKind::Value).with_message("Source sheet does not exist")
            );
        }

        // Check if new name is already taken
        if self.sheet_reg.get_id(new_name).is_some() {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("Sheet '{new_name}' already exists")));
        }

        // Create the new sheet
        let new_sheet_id = self.add_sheet(new_name)?;

        // Begin batch operations
        self.begin_batch();

        // Collect all vertices in the source sheet
        let source_vertices: Vec<(VertexId, PackedCoord)> = self
            .vertices_in_sheet(source_sheet_id)
            .map(|id| (id, self.store.coord(id)))
            .collect();

        // Map from old vertex IDs to new vertex IDs
        let mut vertex_mapping = FxHashMap::default();

        // First pass: Create all vertices in the new sheet
        for (old_id, coord) in &source_vertices {
            let row = coord.row();
            let col = coord.col();
            let kind = self.store.kind(*old_id);

            // Allocate new vertex
            let new_id = self.store.allocate(*coord, new_sheet_id, 0x01); // Mark as dirty

            // Add to CSR edges
            self.edges.add_vertex(*coord, new_id.0);

            // Add to sheet index
            self.sheet_index_mut(new_sheet_id)
                .add_vertex(*coord, new_id);

            // Set vertex kind
            self.store.set_kind(new_id, kind);

            // Copy value or formula
            if let Some(&value_ref) = self.vertex_values.get(old_id) {
                self.vertex_values.insert(new_id, value_ref);
            }

            // Store mapping
            vertex_mapping.insert(*old_id, new_id);

            // Add to cell mapping
            let cell_ref = CellRef::new(new_sheet_id, Coord::new(row, col, true, true));
            self.cell_to_vertex.insert(cell_ref, new_id);
        }

        // Second pass: Copy formulas and update references
        for (old_id, _) in &source_vertices {
            if let Some(&new_id) = vertex_mapping.get(old_id) {
                if let Some(&ast_id) = self.vertex_formulas.get(old_id) {
                    if let Some(ast) = self.data_store.retrieve_ast(ast_id, &self.sheet_reg) {
                        // Update internal sheet references from source to new sheet
                        let updated_ast = update_internal_sheet_references(
                            &ast,
                            &source_name,
                            new_name,
                            source_sheet_id,
                            new_sheet_id,
                        );

                        // Store updated formula
                        let new_ast_id = self.data_store.store_ast(&updated_ast, &self.sheet_reg);
                        self.vertex_formulas.insert(new_id, new_ast_id);

                        // Extract and add dependencies
                        if let Ok((deps, range_deps, _)) =
                            self.extract_dependencies(&updated_ast, new_sheet_id)
                        {
                            // Map dependencies to new sheet where applicable
                            let mapped_deps: Vec<VertexId> = deps
                                .iter()
                                .map(|&dep_id| {
                                    // If dependency is in the source sheet, map to new vertex
                                    vertex_mapping.get(&dep_id).copied().unwrap_or(dep_id)
                                })
                                .collect();

                            self.add_dependent_edges(new_id, &mapped_deps);
                            self.add_range_dependent_edges(new_id, &range_deps, new_sheet_id);
                        }
                    }
                }
            }
        }

        // Copy sheet-scoped named ranges
        let sheet_names: Vec<(String, NamedRange)> = self
            .sheet_named_ranges
            .iter()
            .filter(|((sid, _), _)| *sid == source_sheet_id)
            .map(|((_, name), range)| (name.clone(), range.clone()))
            .collect();

        for (name, mut named_range) in sheet_names {
            // Update scope to new sheet
            named_range.scope = NameScope::Sheet(new_sheet_id);

            // Update definition references if needed
            match &mut named_range.definition {
                NamedDefinition::Cell(cell_ref) if cell_ref.sheet_id == source_sheet_id => {
                    cell_ref.sheet_id = new_sheet_id;
                }
                NamedDefinition::Range(range_ref) => {
                    if range_ref.start.sheet_id == source_sheet_id {
                        range_ref.start.sheet_id = new_sheet_id;
                        range_ref.end.sheet_id = new_sheet_id;
                    }
                }
                _ => {}
            }

            self.sheet_named_ranges
                .insert((new_sheet_id, name), named_range);
        }

        // End batch operations
        self.end_batch();

        Ok(new_sheet_id)
    }
}

/// Helper function to update sheet references in an AST
fn update_sheet_references_in_ast(ast: &ASTNode, old_name: &str, new_name: &str) -> ASTNode {
    match &ast.node_type {
        ASTNodeType::Reference { reference, .. } => {
            let updated_ref = match reference {
                ReferenceType::Cell { sheet, row, col } => {
                    if sheet.as_deref() == Some(old_name) {
                        ReferenceType::Cell {
                            sheet: Some(new_name.to_string()),
                            row: *row,
                            col: *col,
                        }
                    } else {
                        reference.clone()
                    }
                }
                ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                } => {
                    if sheet.as_deref() == Some(old_name) {
                        ReferenceType::Range {
                            sheet: Some(new_name.to_string()),
                            start_row: *start_row,
                            start_col: *start_col,
                            end_row: *end_row,
                            end_col: *end_col,
                        }
                    } else {
                        reference.clone()
                    }
                }
                _ => reference.clone(),
            };

            ASTNode {
                node_type: ASTNodeType::Reference {
                    original: String::new(),
                    reference: updated_ref,
                },
                source_token: None,
            }
        }
        ASTNodeType::BinaryOp { op, left, right } => ASTNode {
            node_type: ASTNodeType::BinaryOp {
                op: op.clone(),
                left: Box::new(update_sheet_references_in_ast(left, old_name, new_name)),
                right: Box::new(update_sheet_references_in_ast(right, old_name, new_name)),
            },
            source_token: None,
        },
        ASTNodeType::UnaryOp { op, expr } => ASTNode {
            node_type: ASTNodeType::UnaryOp {
                op: op.clone(),
                expr: Box::new(update_sheet_references_in_ast(expr, old_name, new_name)),
            },
            source_token: None,
        },
        ASTNodeType::Function { name, args } => ASTNode {
            node_type: ASTNodeType::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| update_sheet_references_in_ast(arg, old_name, new_name))
                    .collect(),
            },
            source_token: None,
        },
        ASTNodeType::Array(rows) => ASTNode {
            node_type: ASTNodeType::Array(
                rows.iter()
                    .map(|row| {
                        row.iter()
                            .map(|cell| update_sheet_references_in_ast(cell, old_name, new_name))
                            .collect()
                    })
                    .collect(),
            ),
            source_token: None,
        },
        _ => ast.clone(),
    }
}

/// Helper function to update internal sheet references when duplicating a sheet
fn update_internal_sheet_references(
    ast: &ASTNode,
    source_name: &str,
    new_name: &str,
    source_id: SheetId,
    new_id: SheetId,
) -> ASTNode {
    match &ast.node_type {
        ASTNodeType::Reference { reference, .. } => {
            let updated_ref = match reference {
                ReferenceType::Cell { sheet, row, col } => {
                    // Update references without sheet name (internal) or with source sheet name
                    if sheet.is_none() || sheet.as_deref() == Some(source_name) {
                        ReferenceType::Cell {
                            sheet: Some(new_name.to_string()),
                            row: *row,
                            col: *col,
                        }
                    } else {
                        reference.clone()
                    }
                }
                ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                } => {
                    if sheet.is_none() || sheet.as_deref() == Some(source_name) {
                        ReferenceType::Range {
                            sheet: Some(new_name.to_string()),
                            start_row: *start_row,
                            start_col: *start_col,
                            end_row: *end_row,
                            end_col: *end_col,
                        }
                    } else {
                        reference.clone()
                    }
                }
                _ => reference.clone(),
            };

            ASTNode {
                node_type: ASTNodeType::Reference {
                    original: String::new(),
                    reference: updated_ref,
                },
                source_token: None,
            }
        }
        ASTNodeType::BinaryOp { op, left, right } => ASTNode {
            node_type: ASTNodeType::BinaryOp {
                op: op.clone(),
                left: Box::new(update_internal_sheet_references(
                    left,
                    source_name,
                    new_name,
                    source_id,
                    new_id,
                )),
                right: Box::new(update_internal_sheet_references(
                    right,
                    source_name,
                    new_name,
                    source_id,
                    new_id,
                )),
            },
            source_token: None,
        },
        ASTNodeType::UnaryOp { op, expr } => ASTNode {
            node_type: ASTNodeType::UnaryOp {
                op: op.clone(),
                expr: Box::new(update_internal_sheet_references(
                    expr,
                    source_name,
                    new_name,
                    source_id,
                    new_id,
                )),
            },
            source_token: None,
        },
        ASTNodeType::Function { name, args } => ASTNode {
            node_type: ASTNodeType::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| {
                        update_internal_sheet_references(
                            arg,
                            source_name,
                            new_name,
                            source_id,
                            new_id,
                        )
                    })
                    .collect(),
            },
            source_token: None,
        },
        ASTNodeType::Array(rows) => ASTNode {
            node_type: ASTNodeType::Array(
                rows.iter()
                    .map(|row| {
                        row.iter()
                            .map(|cell| {
                                update_internal_sheet_references(
                                    cell,
                                    source_name,
                                    new_name,
                                    source_id,
                                    new_id,
                                )
                            })
                            .collect()
                    })
                    .collect(),
            ),
            source_token: None,
        },
        _ => ast.clone(),
    }
}

/// Helper function to adjust a named definition during structural operations
fn adjust_named_definition(
    definition: &mut NamedDefinition,
    adjuster: &crate::engine::graph::editor::reference_adjuster::ReferenceAdjuster,
    operation: &crate::engine::graph::editor::reference_adjuster::ShiftOperation,
) -> Result<(), ExcelError> {
    match definition {
        NamedDefinition::Cell(cell_ref) => {
            if let Some(adjusted) = adjuster.adjust_cell_ref(cell_ref, operation) {
                *cell_ref = adjusted;
            } else {
                // Cell was deleted, convert to #REF! error
                return Err(ExcelError::new(ExcelErrorKind::Ref));
            }
        }
        NamedDefinition::Range(range_ref) => {
            let adjusted_start = adjuster.adjust_cell_ref(&range_ref.start, operation);
            let adjusted_end = adjuster.adjust_cell_ref(&range_ref.end, operation);

            if let (Some(start), Some(end)) = (adjusted_start, adjusted_end) {
                range_ref.start = start;
                range_ref.end = end;
            } else {
                return Err(ExcelError::new(ExcelErrorKind::Ref));
            }
        }
        NamedDefinition::Formula {
            ast,
            dependencies,
            range_deps,
        } => {
            // Adjust AST references
            let adjusted_ast = adjuster.adjust_ast(ast, operation);
            *ast = adjusted_ast;

            // Dependencies will be recalculated on next use
            dependencies.clear();
            range_deps.clear();
        }
    }
    Ok(())
}
