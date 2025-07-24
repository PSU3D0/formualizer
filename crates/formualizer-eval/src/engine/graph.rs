use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::{ASTNode, ReferenceType};
use rustc_hash::{FxHashMap, FxHashSet};

use super::vertex::{Vertex, VertexId, VertexKind};

/// 🔮 Scalability Hook: Change event tracking for future undo/redo support
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    SetValue {
        addr: CellAddr,
        old: Option<LiteralValue>,
        new: LiteralValue,
    },
    SetFormula {
        addr: CellAddr,
        old: Option<ASTNode>,
        new: ASTNode,
    },
}

/// 🔮 Scalability Hook: Dependency reference types for future range compression
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyRef {
    /// A specific cell dependency
    Cell(VertexId),
    /// A whole column dependency (A:A) - future range compression
    WholeColumn { sheet: String, col: u32 },
    /// A whole row dependency (1:1) - future range compression  
    WholeRow { sheet: String, row: u32 },
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CellAddr {
    pub sheet: String,
    pub row: u32,
    pub col: u32,
}

impl CellAddr {
    pub fn new(sheet: String, row: u32, col: u32) -> Self {
        Self { sheet, row, col }
    }
}

/// A summary of the results of a mutating operation on the graph.
/// This serves as a "changelog" to the application layer.
#[derive(Debug, Clone)]
pub struct OperationSummary {
    /// Vertices whose values have been directly or indirectly affected.
    pub affected_vertices: Vec<VertexId>,
    /// Placeholder cells that were newly created to satisfy dependencies.
    pub created_placeholders: Vec<CellAddr>,
}

pub struct DependencyGraph {
    // Core storage - simple arena
    vertices: Vec<Vertex>,

    // Address mappings using fast hashing
    cell_to_vertex: FxHashMap<CellAddr, VertexId>,

    // Scheduling state - using HashSet for O(1) operations
    dirty_vertices: FxHashSet<VertexId>,
    volatile_vertices: FxHashSet<VertexId>,

    // NEW: Specialized managers for range dependencies (Hybrid Model)
    /// Maps a formula vertex to the ranges it depends on.
    formula_to_range_deps: FxHashMap<VertexId, Vec<ReferenceType>>,
    /// A reverse index mapping a cell address to formulas that depend on it via a range.
    cell_to_range_dependents: FxHashMap<CellAddr, Vec<VertexId>>,

    // Default sheet for relative references
    default_sheet: String,
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self {
            vertices: Vec::new(),
            cell_to_vertex: FxHashMap::default(),
            dirty_vertices: FxHashSet::default(),
            volatile_vertices: FxHashSet::default(),
            formula_to_range_deps: FxHashMap::default(),
            cell_to_range_dependents: FxHashMap::default(),
            default_sheet: "Sheet1".to_string(),
        }
    }

    /// Sets the default sheet name for relative references.
    pub fn set_default_sheet(&mut self, sheet_name: &str) {
        self.default_sheet = sheet_name.to_string();
    }

    /// Gets the default sheet name.
    pub fn default_sheet(&self) -> &str {
        &self.default_sheet
    }

    /// Set a value in a cell, returns affected vertex IDs
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<OperationSummary, ExcelError> {
        let addr = CellAddr::new(sheet.to_string(), row, col);
        let mut created_placeholders = Vec::new();

        let vertex_id = if let Some(&existing_id) = self.cell_to_vertex.get(&addr) {
            // Update existing vertex
            if let Some(vertex) = self.vertices.get_mut(existing_id.as_index()) {
                vertex.kind = VertexKind::Value(value);
            }
            existing_id
        } else {
            // Create new vertex
            created_placeholders.push(addr.clone());
            let vertex_id = VertexId::new(self.vertices.len() as u32);
            let vertex = Vertex::new_value(sheet.to_string(), Some(row), Some(col), value);
            self.vertices.push(vertex);
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
        let addr = CellAddr::new(sheet.to_string(), row, col);

        // Extract dependencies from AST, creating placeholders if needed
        let (new_dependencies, new_range_dependencies, mut created_placeholders) =
            self.extract_dependencies(&ast, sheet)?;

        // Check for self-reference (immediate cycle detection)
        let addr_vertex_id = self.get_or_create_vertex(&addr, &mut created_placeholders);

        if new_dependencies.contains(&addr_vertex_id) {
            return Err(ExcelError::new(ExcelErrorKind::Circ)
                .with_message("Self-reference detected".to_string()));
        }

        // Determine if volatile
        let volatile = self.is_ast_volatile(&ast);

        // Remove old dependencies first
        self.remove_dependent_edges(addr_vertex_id, sheet);

        // Update existing vertex
        if let Some(vertex) = self.vertices.get_mut(addr_vertex_id.as_index()) {
            vertex.kind = VertexKind::FormulaScalar {
                ast,
                result: None,
                dirty: true,
                volatile,
            };
            vertex.dependencies = new_dependencies.clone();
        }

        // Add new dependency edges
        self.add_dependent_edges(addr_vertex_id, &new_dependencies);
        self.add_range_dependent_edges(addr_vertex_id, &new_range_dependencies, sheet);

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
        let addr = CellAddr::new(sheet.to_string(), row, col);

        self.cell_to_vertex
            .get(&addr)
            .and_then(|&vertex_id| self.vertices.get(vertex_id.as_index()))
            .and_then(|vertex| match &vertex.kind {
                VertexKind::Value(v) => Some(v.clone()),
                VertexKind::FormulaScalar {
                    result: Some(v), ..
                } => Some(v.clone()),
                _ => None,
            })
    }

    /// Mark vertex dirty and propagate to dependents - O(1) with HashSet
    fn mark_dirty(&mut self, vertex_id: VertexId) -> Vec<VertexId> {
        let mut affected = FxHashSet::default();
        let mut to_visit = vec![vertex_id];

        // Initial propagation from direct and range dependents
        if let Some(vertex) = self.vertices.get(vertex_id.as_index()) {
            to_visit.extend(&vertex.dependents);
            if let (Some(row), Some(col)) = (vertex.row, vertex.col) {
                let addr = CellAddr::new(vertex.sheet.clone(), row, col);
                if let Some(dependents) = self.cell_to_range_dependents.get(&addr) {
                    to_visit.extend(dependents);
                }
            }
        }

        while let Some(id) = to_visit.pop() {
            if !affected.insert(id) {
                continue; // Already processed
            }

            // Mark vertex as dirty
            if let Some(vertex) = self.vertices.get_mut(id.as_index()) {
                match &mut vertex.kind {
                    VertexKind::FormulaScalar { dirty, .. } => *dirty = true,
                    VertexKind::FormulaArray { dirty, .. } => *dirty = true,
                    _ => {}
                }

                // Add direct dependents to visit list
                to_visit.extend(&vertex.dependents);
            }
        }

        // Add to dirty set - O(1) operations
        self.dirty_vertices.extend(&affected);

        // Return as Vec for compatibility
        affected.into_iter().collect()
    }

    /// Get all vertices that need evaluation
    pub fn get_evaluation_vertices(&self) -> Vec<VertexId> {
        let mut combined = FxHashSet::default();
        combined.extend(&self.dirty_vertices);
        combined.extend(&self.volatile_vertices);
        let mut result: Vec<VertexId> = combined.into_iter().collect();
        result.sort_unstable();
        result
    }

    /// Clear dirty flags after successful evaluation
    pub fn clear_dirty_flags(&mut self, vertices: &[VertexId]) {
        for &vertex_id in vertices {
            if let Some(vertex) = self.vertices.get_mut(vertex_id.as_index()) {
                match &mut vertex.kind {
                    VertexKind::FormulaScalar { dirty, .. } => *dirty = false,
                    VertexKind::FormulaArray { dirty, .. } => *dirty = false,
                    _ => {}
                }
            }
        }

        // Remove from dirty set - O(1) per vertex
        for &vertex_id in vertices {
            self.dirty_vertices.remove(&vertex_id);
        }
    }

    /// 🔮 Scalability Hook: Clear volatile vertices after evaluation cycle (prevents accumulation)
    pub fn clear_volatile_flags(&mut self) {
        self.volatile_vertices.clear();
    }

    // Helper methods
    fn extract_dependencies(
        &mut self,
        ast: &ASTNode,
        current_sheet: &str,
    ) -> Result<(Vec<VertexId>, Vec<ReferenceType>, Vec<CellAddr>), ExcelError> {
        let mut dependencies = FxHashSet::default();
        let mut range_dependencies = Vec::new();
        let mut created_placeholders = Vec::new();
        self.extract_dependencies_recursive(
            ast,
            current_sheet,
            &mut dependencies,
            &mut range_dependencies,
            &mut created_placeholders,
        )?;
        Ok((
            dependencies.into_iter().collect(),
            range_dependencies,
            created_placeholders,
        ))
    }

    fn extract_dependencies_recursive(
        &mut self,
        ast: &ASTNode,
        current_sheet: &str,
        dependencies: &mut FxHashSet<VertexId>,
        range_dependencies: &mut Vec<ReferenceType>,
        created_placeholders: &mut Vec<CellAddr>,
    ) -> Result<(), ExcelError> {
        match &ast.node_type {
            formualizer_core::parser::ASTNodeType::Reference { reference, .. } => {
                match reference {
                    ReferenceType::Cell { .. } => {
                        let vertex_id = self.get_or_create_vertex_for_reference(
                            reference,
                            current_sheet,
                            created_placeholders,
                        )?;
                        dependencies.insert(vertex_id);
                    }
                    ReferenceType::Range { .. } => {
                        range_dependencies.push(reference.clone());
                    }
                    _ => {} // Ignore others for now
                }
            }
            formualizer_core::parser::ASTNodeType::BinaryOp { left, right, .. } => {
                self.extract_dependencies_recursive(
                    left,
                    current_sheet,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                )?;
                self.extract_dependencies_recursive(
                    right,
                    current_sheet,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                )?;
            }
            formualizer_core::parser::ASTNodeType::UnaryOp { expr, .. } => {
                self.extract_dependencies_recursive(
                    expr,
                    current_sheet,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                )?;
            }
            formualizer_core::parser::ASTNodeType::Function { args, .. } => {
                for arg in args {
                    self.extract_dependencies_recursive(
                        arg,
                        current_sheet,
                        dependencies,
                        range_dependencies,
                        created_placeholders,
                    )?;
                }
            }
            formualizer_core::parser::ASTNodeType::Array(rows) => {
                for row in rows {
                    for cell in row {
                        self.extract_dependencies_recursive(
                            cell,
                            current_sheet,
                            dependencies,
                            range_dependencies,
                            created_placeholders,
                        )?;
                    }
                }
            }
            formualizer_core::parser::ASTNodeType::Literal(_) => {
                // Literals have no dependencies
            }
        }
        Ok(())
    }

    fn get_or_create_vertex(
        &mut self,
        addr: &CellAddr,
        created_placeholders: &mut Vec<CellAddr>,
    ) -> VertexId {
        if let Some(&vertex_id) = self.cell_to_vertex.get(addr) {
            return vertex_id;
        }

        created_placeholders.push(addr.clone());
        let vertex_id = VertexId::new(self.vertices.len() as u32);
        let vertex = Vertex::new_empty(addr.sheet.clone(), Some(addr.row), Some(addr.col));
        self.vertices.push(vertex);
        self.cell_to_vertex.insert(addr.clone(), vertex_id);
        vertex_id
    }

    /// Gets the VertexId for a reference, creating a placeholder vertex if it doesn't exist.
    fn get_or_create_vertex_for_reference(
        &mut self,
        reference: &formualizer_core::parser::ReferenceType,
        current_sheet: &str,
        created_placeholders: &mut Vec<CellAddr>,
    ) -> Result<VertexId, ExcelError> {
        match reference {
            formualizer_core::parser::ReferenceType::Cell { sheet, row, col } => {
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                let addr = CellAddr::new(sheet_name.to_string(), *row, *col);
                Ok(self.get_or_create_vertex(&addr, created_placeholders))
            }
            _ => Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Expected a cell reference, but got a range or other type.")),
        }
    }

    fn is_ast_volatile(&self, _ast: &ASTNode) -> bool {
        // Check if AST contains volatile functions like RAND(), NOW()
        // TODO: Implement volatile detection
        false
    }

    fn add_dependent_edges(&mut self, dependent: VertexId, dependencies: &[VertexId]) {
        for &dep_id in dependencies {
            if let Some(dep_vertex) = self.vertices.get_mut(dep_id.as_index()) {
                if !dep_vertex.dependents.contains(&dependent) {
                    dep_vertex.dependents.push(dependent);
                }
            }
        }
    }

    fn add_range_dependent_edges(
        &mut self,
        dependent: VertexId,
        ranges: &[ReferenceType],
        current_sheet: &str,
    ) {
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
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                for row in start_row.unwrap_or(1)..=end_row.unwrap_or(1) {
                    for col in start_col.unwrap_or(1)..=end_col.unwrap_or(1) {
                        let addr = CellAddr::new(sheet_name.to_string(), row, col);
                        self.cell_to_range_dependents
                            .entry(addr)
                            .or_default()
                            .push(dependent);
                    }
                }
            }
        }
    }

    fn remove_dependent_edges(&mut self, dependent: VertexId, current_sheet: &str) {
        // Remove direct dependencies
        let old_deps = if let Some(vertex) = self.vertices.get(dependent.as_index()) {
            vertex.dependencies.clone()
        } else {
            return;
        };
        for dep_id in old_deps {
            if let Some(dep_vertex) = self.vertices.get_mut(dep_id.as_index()) {
                dep_vertex.dependents.retain(|&id| id != dependent);
            }
        }

        // Remove range dependencies
        if let Some(old_ranges) = self.formula_to_range_deps.remove(&dependent) {
            for range in old_ranges {
                if let ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                } = range
                {
                    let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                    for row in start_row.unwrap_or(1)..=end_row.unwrap_or(1) {
                        for col in start_col.unwrap_or(1)..=end_col.unwrap_or(1) {
                            let addr = CellAddr::new(sheet_name.to_string(), row, col);
                            if let Some(dependents) = self.cell_to_range_dependents.get_mut(&addr) {
                                dependents.retain(|&id| id != dependent);
                            }
                        }
                    }
                }
            }
        }
    }

    // Accessors for scheduler and tests
    pub(crate) fn vertices(&self) -> &[Vertex] {
        &self.vertices
    }

    /// Gets a reference to a vertex by its ID.
    pub(crate) fn get_vertex(&self, vertex_id: VertexId) -> Option<&Vertex> {
        self.vertices.get(vertex_id.as_index())
    }

    /// Updates the cached value of a formula vertex.
    pub(crate) fn update_vertex_value(&mut self, vertex_id: VertexId, value: LiteralValue) {
        if let Some(vertex) = self.vertices.get_mut(vertex_id.as_index()) {
            match &mut vertex.kind {
                VertexKind::FormulaScalar { result, .. } => {
                    *result = Some(value);
                }
                VertexKind::FormulaArray { results, .. } => {
                    // TODO: Handle array results properly
                    if let LiteralValue::Array(arr) = value {
                        *results = Some(arr);
                    }
                }
                _ => {} // Not a formula, nothing to update
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn cell_to_vertex(&self) -> &FxHashMap<CellAddr, VertexId> {
        &self.cell_to_vertex
    }
}
