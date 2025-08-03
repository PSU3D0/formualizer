use crate::{SheetId, SheetRegistry};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};
use rustc_hash::{FxHashMap, FxHashSet};

use super::arena::{AstNodeId, DataStore, ValueRef};
use super::delta_edges::CsrMutableEdges;
use super::packed_coord::PackedCoord;
use super::vertex::{VertexId, VertexKind};
use super::vertex_store::{FIRST_NORMAL_VERTEX, VertexStore};
use crate::reference::{CellRef, Coord};

/// 🔮 Scalability Hook: Change event tracking for future undo/redo support
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    SetValue {
        addr: CellRef,
        old: Option<LiteralValue>,
        new: LiteralValue,
    },
    SetFormula {
        addr: CellRef,
        old: Option<ASTNode>,
        new: ASTNode,
    },
}

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

    // Sheet name/ID mapping
    sheet_reg: SheetRegistry,
    default_sheet_id: SheetId,

    // Evaluation configuration
    config: super::EvalConfig,
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl DependencyGraph {
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
            sheet_reg,
            default_sheet_id,
            config: super::EvalConfig::default(),
        }
    }

    pub fn new_with_config(config: super::EvalConfig) -> Self {
        Self {
            config,
            ..Self::new()
        }
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
            let is_formula = match self.store.kind(existing_id) {
                VertexKind::FormulaScalar | VertexKind::FormulaArray => true,
                _ => false,
            };

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

        // Determine if volatile
        let volatile = self.is_ast_volatile(&ast);

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
    ) -> Result<(Vec<VertexId>, Vec<ReferenceType>, Vec<CellRef>), ExcelError> {
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
                        let start_row = start_row.unwrap_or(1);
                        let start_col = start_col.unwrap_or(1);
                        let end_row = end_row.unwrap_or(1);
                        let end_col = end_col.unwrap_or(1);

                        if start_row > end_row || start_col > end_col {
                            return Err(ExcelError::new(ExcelErrorKind::Ref));
                        }

                        let height = end_row.saturating_sub(start_row) + 1;
                        let width = end_col.saturating_sub(start_col) + 1;
                        let size = (width * height) as usize;

                        if size <= self.config.range_expansion_limit {
                            // Expand to individual cells
                            let sheet_id = match sheet {
                                Some(name) => self.sheet_id_mut(name),
                                None => current_sheet_id,
                            };
                            for row in start_row..=end_row {
                                for col in start_col..=end_col {
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
                    if func.volatile() {
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
        for &dep_id in dependencies {
            // Store edge as dependent -> dependency (what it depends on)
            self.edges.add_edge(dependent, dep_id);
        }
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
                let start_row = start_row.unwrap_or(1);
                let start_col = start_col.unwrap_or(1);
                let end_row = end_row.unwrap_or(1);
                let end_col = end_col.unwrap_or(1);

                let height = end_row - start_row + 1;
                let width = end_col - start_col + 1;

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
                                .entry(key)
                                .or_default()
                                .insert(dependent);
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
                            .entry(key)
                            .or_default()
                            .insert(dependent);
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
                            .entry(key)
                            .or_default()
                            .insert(dependent);
                    }
                }
            }
        }
    }

    fn remove_dependent_edges(&mut self, vertex: VertexId) {
        // Remove all outgoing edges from this vertex (its dependencies)
        let dependencies = self.edges.out_edges(vertex);
        for dep in dependencies {
            self.edges.remove_edge(vertex, dep);
        }

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
                    let start_row = start_row.unwrap_or(1);
                    let start_col = start_col.unwrap_or(1);
                    let end_row = end_row.unwrap_or(1);
                    let end_col = end_col.unwrap_or(1);

                    let height = end_row.saturating_sub(start_row) + 1;
                    let width = end_col.saturating_sub(start_col) + 1;

                    let mut keys_to_clean = FxHashSet::default();

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

                    for key in keys_to_clean {
                        if let Some(dependents) = self.stripe_to_dependents.get_mut(&key) {
                            dependents.remove(&vertex);
                            if dependents.is_empty() {
                                self.stripe_to_dependents.remove(&key);
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
            for (&addr, &vid) in &self.cell_to_vertex {
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
}
