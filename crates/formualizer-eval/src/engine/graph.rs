use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::ASTNode;
use rustc_hash::{FxHashMap, FxHashSet};

use super::vertex::{Vertex, VertexId, VertexKind};

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

pub struct DependencyGraph {
    // Core storage - simple arena
    vertices: Vec<Vertex>,

    // Address mappings using fast hashing
    cell_to_vertex: FxHashMap<CellAddr, VertexId>,

    // Scheduling state - using HashSet for O(1) operations
    dirty_vertices: FxHashSet<VertexId>,
    volatile_vertices: FxHashSet<VertexId>,
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self {
            vertices: Vec::new(),
            cell_to_vertex: FxHashMap::default(),
            dirty_vertices: FxHashSet::default(),
            volatile_vertices: FxHashSet::default(),
        }
    }

    /// Set a value in a cell, returns affected vertex IDs
    pub fn set_cell_value(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        value: LiteralValue,
    ) -> Result<Vec<VertexId>, ExcelError> {
        let addr = CellAddr::new(sheet.to_string(), row, col);

        let vertex_id = if let Some(&existing_id) = self.cell_to_vertex.get(&addr) {
            // Update existing vertex
            if let Some(vertex) = self.vertices.get_mut(existing_id.as_index()) {
                vertex.kind = VertexKind::Value(value);
            }
            existing_id
        } else {
            // Create new vertex
            let vertex_id = VertexId::new(self.vertices.len() as u32);
            let vertex = Vertex::new_value(sheet.to_string(), Some(row), Some(col), value);
            self.vertices.push(vertex);
            self.cell_to_vertex.insert(addr, vertex_id);
            vertex_id
        };

        Ok(self.mark_dirty(vertex_id))
    }

    /// Set a formula in a cell, returns affected vertex IDs  
    pub fn set_cell_formula(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        ast: ASTNode,
    ) -> Result<Vec<VertexId>, ExcelError> {
        let addr = CellAddr::new(sheet.to_string(), row, col);

        // Extract dependencies from AST
        let new_dependencies = self.extract_dependencies(&ast)?;

        // Check for self-reference (immediate cycle detection)
        let addr_vertex_id = self.cell_to_vertex.get(&addr).copied();
        if let Some(existing_id) = addr_vertex_id {
            if new_dependencies.contains(&existing_id) {
                return Err(ExcelError::new(ExcelErrorKind::Circ)
                    .with_message("Self-reference detected".to_string()));
            }
        }

        // Determine if volatile
        let volatile = self.is_ast_volatile(&ast);

        let vertex_id = if let Some(&existing_id) = self.cell_to_vertex.get(&addr) {
            // Remove old dependencies first
            self.remove_dependent_edges(existing_id);

            // Update existing vertex
            if let Some(vertex) = self.vertices.get_mut(existing_id.as_index()) {
                // Update vertex
                vertex.kind = VertexKind::FormulaScalar {
                    ast,
                    result: None,
                    dirty: true,
                    volatile,
                };
                vertex.dependencies = new_dependencies.clone();
            }
            existing_id
        } else {
            // Create new vertex
            let vertex_id = VertexId::new(self.vertices.len() as u32);
            let mut vertex =
                Vertex::new_formula_scalar(sheet.to_string(), Some(row), Some(col), ast, volatile);
            vertex.dependencies = new_dependencies.clone();
            self.vertices.push(vertex);
            self.cell_to_vertex.insert(addr, vertex_id);
            vertex_id
        };

        // Add new dependency edges
        self.add_dependent_edges(vertex_id, &new_dependencies);

        // Mark as volatile if needed
        if volatile {
            self.volatile_vertices.insert(vertex_id);
        }

        Ok(self.mark_dirty(vertex_id))
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

                // Add dependents to visit list
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
    fn extract_dependencies(&self, _ast: &ASTNode) -> Result<Vec<VertexId>, ExcelError> {
        // Extract references from AST and map to vertex IDs
        // This is a simplified implementation - expand based on AST structure
        Ok(Vec::new()) // TODO: Implement dependency extraction
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

    fn remove_dependent_edges(&mut self, dependent: VertexId) {
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
    }

    // Testing accessors
    #[cfg(test)]
    pub(crate) fn vertices(&self) -> &[Vertex] {
        &self.vertices
    }

    #[cfg(test)]
    pub(crate) fn cell_to_vertex(&self) -> &FxHashMap<CellAddr, VertexId> {
        &self.cell_to_vertex
    }
}
