use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};
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
    pub sheet: String,
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

#[derive(Debug)]
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

    /// Maps a stripe to formulas that depend on it via a compressed range.
    /// CRITICAL: VertexIds are deduplicated within each stripe to avoid quadratic blow-ups.
    stripe_to_dependents: FxHashMap<StripeKey, FxHashSet<VertexId>>,

    // Default sheet for relative references
    default_sheet: String,
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
        Self {
            vertices: Vec::new(),
            cell_to_vertex: FxHashMap::default(),
            dirty_vertices: FxHashSet::default(),
            volatile_vertices: FxHashSet::default(),
            formula_to_range_deps: FxHashMap::default(),
            stripe_to_dependents: FxHashMap::default(),
            default_sheet: "Sheet1".to_string(),
            config: super::EvalConfig::default(),
        }
    }

    pub fn new_with_config(config: super::EvalConfig) -> Self {
        Self {
            config,
            ..Self::new()
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
        self.remove_dependent_edges(addr_vertex_id);

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
        let mut visited_for_propagation = FxHashSet::default();

        // Initial propagation from direct and range dependents
        if let Some(vertex) = self.vertices.get(vertex_id.as_index()) {
            to_visit.extend(&vertex.dependents);
            if let (Some(row), Some(col)) = (vertex.row, vertex.col) {
                let dirty_addr = CellAddr::new(vertex.sheet.clone(), row, col);

                // New stripe-based dependents lookup (Milestone 5.3)
                let mut potential_dependents = FxHashSet::default();

                // 1. Column stripe lookup
                let column_key = StripeKey {
                    sheet: dirty_addr.sheet.clone(),
                    stripe_type: StripeType::Column,
                    index: dirty_addr.col,
                };
                if let Some(dependents) = self.stripe_to_dependents.get(&column_key) {
                    potential_dependents.extend(dependents);
                }

                // 2. Row stripe lookup
                let row_key = StripeKey {
                    sheet: dirty_addr.sheet.clone(),
                    stripe_type: StripeType::Row,
                    index: dirty_addr.row,
                };
                if let Some(dependents) = self.stripe_to_dependents.get(&row_key) {
                    potential_dependents.extend(dependents);
                }

                // 3. Block stripe lookup
                if self.config.enable_block_stripes {
                    let block_key = StripeKey {
                        sheet: dirty_addr.sheet.clone(),
                        stripe_type: StripeType::Block,
                        index: block_index(dirty_addr.row, dirty_addr.col),
                    };
                    if let Some(dependents) = self.stripe_to_dependents.get(&block_key) {
                        potential_dependents.extend(dependents);
                    }
                }

                // Precision check: ensure the dirtied cell is actually within the formula's range
                for &dep_id in &potential_dependents {
                    if let Some(ranges) = self.formula_to_range_deps.get(&dep_id) {
                        for range in ranges {
                            if range.contains(&dirty_addr.sheet, dirty_addr.row, dirty_addr.col) {
                                to_visit.push(dep_id);
                                break; // Found a matching range, no need to check others for this dependent
                            }
                        }
                    }
                }
            }
        }

        while let Some(id) = to_visit.pop() {
            if !visited_for_propagation.insert(id) {
                continue; // Already processed for propagation
            }
            affected.insert(id);

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

        let mut result: Vec<VertexId> = combined
            .into_iter()
            .filter(|&id: &VertexId| {
                if let Some(vertex) = self.vertices.get(id.as_index()) {
                    matches!(
                        vertex.kind,
                        VertexKind::FormulaScalar { .. } | VertexKind::FormulaArray { .. }
                    )
                } else {
                    false
                }
            })
            .collect();
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

        // Deduplicate range references using manual approach since ReferenceType may not be Ord
        let mut deduped_ranges = Vec::new();
        for range_ref in range_dependencies {
            if !deduped_ranges.contains(&range_ref) {
                deduped_ranges.push(range_ref);
            }
        }
        let range_dependencies = deduped_ranges;

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
                            let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
                            for row in start_row..=end_row {
                                for col in start_col..=end_col {
                                    let addr = CellAddr::new(sheet_name.to_string(), row, col);
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

    fn is_ast_volatile(&self, ast: &ASTNode) -> bool {
        // Recursively check if any part of the AST contains a volatile function.
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
            _ => false, // Literals and references are not volatile themselves.
        }
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
                let sheet_name = sheet.as_deref().unwrap_or(current_sheet);
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
                                sheet: sheet_name.to_string(),
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
                            sheet: sheet_name.to_string(),
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
                            sheet: sheet_name.to_string(),
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

    fn remove_dependent_edges(&mut self, dependent: VertexId) {
        // Remove direct dependencies
        let (old_deps, old_sheet) = if let Some(vertex) = self.vertices.get(dependent.as_index()) {
            (vertex.dependencies.clone(), vertex.sheet.clone())
        } else {
            return;
        };
        for dep_id in old_deps {
            if let Some(dep_vertex) = self.vertices.get_mut(dep_id.as_index()) {
                dep_vertex.dependents.retain(|&id| id != dependent);
            }
        }

        // Remove range dependencies
        if self.formula_to_range_deps.remove(&dependent).is_some() {
            // The stripe cleanup logic below is now the only thing needed.
            // For precision, we could recalculate the old stripes here and remove
            // the dependent from just those, but for now, the global iteration is simpler.
        }

        // Remove stripe dependencies
        // CRITICAL: Remove the VertexId from all stripe entries to prevent stale references
        let mut empty_stripes = Vec::new();
        for (stripe_key, dependents) in self.stripe_to_dependents.iter_mut() {
            dependents.remove(&dependent);
            if dependents.is_empty() {
                empty_stripes.push(stripe_key.clone());
            }
        }

        // Remove empty stripe entries to prevent unbounded map growth
        for stripe_key in empty_stripes {
            self.stripe_to_dependents.remove(&stripe_key);
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

    /// Get vertex ID for a cell address
    pub fn get_vertex_id_for_address(&self, addr: &CellAddr) -> Option<&VertexId> {
        self.cell_to_vertex.get(addr)
    }

    #[cfg(test)]
    pub(crate) fn cell_to_vertex(&self) -> &FxHashMap<CellAddr, VertexId> {
        &self.cell_to_vertex
    }
}
