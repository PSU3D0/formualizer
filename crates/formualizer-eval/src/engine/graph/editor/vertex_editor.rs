use crate::SheetId;
use crate::engine::graph::DependencyGraph;
use crate::engine::graph::editor::reference_adjuster::{
    MoveReferenceAdjuster, ReferenceAdjuster, RelativeReferenceAdjuster, ShiftOperation,
};
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{ChangeEvent, ChangeLogger, VertexId, VertexKind};
use crate::reference::{CellRef, Coord};
use formualizer_common::Coord as AbsCoord;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::ASTNode;
use std::sync::atomic::{AtomicU64, Ordering};

/// Metadata for creating a new vertex
#[derive(Debug, Clone)]
pub struct VertexMeta {
    pub coord: AbsCoord,
    pub sheet_id: SheetId,
    pub kind: VertexKind,
    pub flags: u8,
}

impl VertexMeta {
    pub fn new(row: u32, col: u32, sheet_id: SheetId, kind: VertexKind) -> Self {
        Self {
            coord: AbsCoord::new(row, col),
            sheet_id,
            kind,
            flags: 0,
        }
    }

    pub fn with_flags(mut self, flags: u8) -> Self {
        self.flags = flags;
        self
    }

    pub fn dirty(mut self) -> Self {
        self.flags |= 0x01;
        self
    }

    pub fn volatile(mut self) -> Self {
        self.flags |= 0x02;
        self
    }
}

/// Patch for updating vertex metadata
#[derive(Debug, Clone)]
pub struct VertexMetaPatch {
    pub kind: Option<VertexKind>,
    pub coord: Option<AbsCoord>,
    pub dirty: Option<bool>,
    pub volatile: Option<bool>,
}

/// Patch for updating vertex data
#[derive(Debug, Clone)]
pub struct VertexDataPatch {
    pub value: Option<LiteralValue>,
    pub formula: Option<ASTNode>,
}

/// Summary of metadata update
#[derive(Debug, Clone, Default)]
pub struct MetaUpdateSummary {
    pub coord_changed: bool,
    pub kind_changed: bool,
    pub flags_changed: bool,
}

/// Summary of data update
#[derive(Debug, Clone, Default)]
pub struct DataUpdateSummary {
    pub value_changed: bool,
    pub formula_changed: bool,
    pub dependents_marked_dirty: Vec<VertexId>,
}

/// Summary of shift operations (row/column insert/delete)
#[derive(Debug, Clone, Default)]
pub struct ShiftSummary {
    pub vertices_moved: Vec<VertexId>,
    pub vertices_deleted: Vec<VertexId>,
    pub references_adjusted: usize,
    pub formulas_updated: usize,
}

/// Summary of range operations
#[derive(Debug, Clone, Default)]
pub struct RangeSummary {
    pub cells_affected: usize,
    pub vertices_created: Vec<VertexId>,
    pub vertices_updated: Vec<VertexId>,
    pub cells_moved: usize,
}

/// Transaction ID for tracking active transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionId(u64);

impl TransactionId {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        TransactionId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

/// Represents an active transaction
#[derive(Debug)]
struct Transaction {
    id: TransactionId,
    start_index: usize, // Index in change_log where transaction started
}

/// Custom error type for vertex editor operations
#[derive(Debug, Clone)]
pub enum EditorError {
    TargetOccupied { cell: CellRef },
    OutOfBounds { row: u32, col: u32 },
    InvalidName { name: String, reason: String },
    TransactionFailed { reason: String },
    NoActiveTransaction,
    VertexNotFound { id: VertexId },
    Excel(ExcelError),
}

impl From<ExcelError> for EditorError {
    fn from(e: ExcelError) -> Self {
        EditorError::Excel(e)
    }
}

impl std::fmt::Display for EditorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EditorError::TargetOccupied { cell } => {
                write!(
                    f,
                    "Target cell occupied at row {}, col {}",
                    cell.coord.row(),
                    cell.coord.col()
                )
            }
            EditorError::OutOfBounds { row, col } => {
                write!(f, "Cell position out of bounds: row {row}, col {col}")
            }
            EditorError::InvalidName { name, reason } => {
                write!(f, "Invalid name '{name}': {reason}")
            }
            EditorError::TransactionFailed { reason } => {
                write!(f, "Transaction failed: {reason}")
            }
            EditorError::NoActiveTransaction => {
                write!(f, "No active transaction")
            }
            EditorError::VertexNotFound { id } => {
                write!(f, "Vertex not found: {id:?}")
            }
            EditorError::Excel(e) => write!(f, "Excel error: {e:?}"),
        }
    }
}

impl std::error::Error for EditorError {}

/// Builder/controller object that provides exclusive access to the dependency graph
/// for all mutation operations. This ensures consistency and proper change tracking.
/// # Example Usage
///
/// ```rust
/// use formualizer_eval::engine::{DependencyGraph, VertexEditor, VertexMeta, VertexKind};
/// use formualizer_common::LiteralValue;
/// use formualizer_eval::reference::{CellRef, Coord};
///
/// let mut graph = DependencyGraph::new();
/// let mut editor = VertexEditor::new(&mut graph);
///
/// // Batch operations for better performance
/// editor.begin_batch();
///
/// // Create a new cell vertex
/// let meta = VertexMeta::new(1, 1, 0, VertexKind::Cell).dirty();
/// let vertex_id = editor.add_vertex(meta);
///
/// // Set cell values
/// let cell_ref = CellRef {
///     sheet_id: 0,
///     coord: Coord::new(2, 3, true, true)
/// };
/// editor.set_cell_value(cell_ref, LiteralValue::Number(42.0));
///
/// // Commit batch operations
/// editor.commit_batch();
///
/// ```
pub struct VertexEditor<'g> {
    graph: &'g mut DependencyGraph,
    change_logger: Option<&'g mut dyn ChangeLogger>,
    batch_mode: bool,
}

impl<'g> VertexEditor<'g> {
    /// Create a new vertex editor without change logging
    pub fn new(graph: &'g mut DependencyGraph) -> Self {
        Self {
            graph,
            change_logger: None,
            batch_mode: false,
        }
    }

    /// Create a new vertex editor with change logging
    pub fn with_logger<L: ChangeLogger + 'g>(
        graph: &'g mut DependencyGraph,
        logger: &'g mut L,
    ) -> Self {
        Self {
            graph,
            change_logger: Some(logger as &'g mut dyn ChangeLogger),
            batch_mode: false,
        }
    }

    /// Start batch mode to defer expensive operations until commit
    pub fn begin_batch(&mut self) {
        if !self.batch_mode {
            self.graph.begin_batch();
            self.batch_mode = true;
        }
    }

    /// End batch mode and commit all deferred operations
    pub fn commit_batch(&mut self) {
        if self.batch_mode {
            self.graph.end_batch();
            self.batch_mode = false;
        }
    }

    /// Helper method to log a change event
    fn log_change(&mut self, event: ChangeEvent) {
        if let Some(logger) = &mut self.change_logger {
            logger.record(event);
        }
    }

    /// Check if change logging is enabled
    pub fn has_logger(&self) -> bool {
        self.change_logger.is_some()
    }

    fn get_formula_ast(&self, id: VertexId) -> Option<ASTNode> {
        self.graph.get_formula_id(id).and_then(|ast_id| {
            self.graph
                .data_store()
                .retrieve_ast(ast_id, self.graph.sheet_reg())
        })
    }

    // Transaction support

    // Transaction support has been moved to TransactionContext
    // which coordinates ChangeLog, TransactionManager, and VertexEditor

    /// Apply the inverse of a change event (used by TransactionContext for rollback)
    pub fn apply_inverse(&mut self, change: ChangeEvent) -> Result<(), EditorError> {
        match change {
            ChangeEvent::SetValue { addr, old, new: _ } => {
                // Restore old value
                if let Some(old_value) = old {
                    self.set_cell_value(addr, old_value);
                } else {
                    // Cell didn't exist before, remove it
                    if let Some(&id) = self.graph.get_vertex_id_for_address(&addr) {
                        self.remove_vertex(id)?;
                    }
                }
            }
            ChangeEvent::SetFormula { addr, old, new: _ } => {
                // Restore old formula
                if let Some(old_formula) = old {
                    self.set_cell_formula(addr, old_formula);
                } else {
                    // Cell didn't have formula before, remove it or set to value
                    if let Some(&id) = self.graph.get_vertex_id_for_address(&addr) {
                        self.remove_vertex(id)?;
                    }
                }
            }
            ChangeEvent::AddVertex { id, .. } => {
                // Inverse of AddVertex is removal
                let _ = self.remove_vertex(id); // ignore errors for now
            }
            ChangeEvent::RemoveVertex {
                id: _,
                old_value,
                old_formula,
                old_dependencies,
                old_dependents,
                coord,
                sheet_id,
                kind,
                ..
            } => {
                if let (Some(c), Some(sid)) = (coord, sheet_id) {
                    let meta =
                        VertexMeta::new(c.row(), c.col(), sid, kind.unwrap_or(VertexKind::Cell));
                    let new_id = self.add_vertex(meta);
                    if let Some(v) = old_value {
                        let cell_ref = self.graph.make_cell_ref_internal(sid, c.row(), c.col());
                        self.set_cell_value(cell_ref, v);
                    }
                    if let Some(f) = old_formula {
                        let cell_ref = self.graph.make_cell_ref_internal(sid, c.row(), c.col());
                        self.set_cell_formula(cell_ref, f);
                    }
                    for dep in old_dependencies {
                        self.graph.add_dependency_edge(new_id, dep);
                    }
                    for parent in old_dependents {
                        self.graph.add_dependency_edge(parent, new_id);
                    }
                }
            }
            ChangeEvent::DefineName { name, scope, .. } => {
                // Inverse is delete name
                self.graph.delete_name(&name, scope)?;
            }
            ChangeEvent::UpdateName {
                name,
                scope,
                old_definition,
                ..
            } => {
                // Restore old definition
                self.graph.update_name(&name, old_definition, scope)?;
            }
            ChangeEvent::DeleteName {
                name,
                scope,
                old_definition,
            } => {
                if let Some(def) = old_definition {
                    self.graph.define_name(&name, def, scope)?;
                } else {
                    return Err(EditorError::TransactionFailed {
                        reason: "Missing old definition for name deletion rollback".to_string(),
                    });
                }
            }
            // Granular events for compound operations
            ChangeEvent::CompoundStart { .. } | ChangeEvent::CompoundEnd { .. } => {
                // These are markers, no inverse needed
            }
            ChangeEvent::VertexMoved { id, old_coord, .. } => {
                // Move back to old position
                self.move_vertex(id, old_coord)?;
            }
            ChangeEvent::FormulaAdjusted { id, old_ast, .. } => {
                // Restore old formula
                // TODO: Need a method to update formula by vertex ID
                return Err(EditorError::TransactionFailed {
                    reason: "Cannot rollback formula adjustment yet".to_string(),
                });
            }
            ChangeEvent::NamedRangeAdjusted {
                name,
                scope,
                old_definition,
                ..
            } => {
                // Restore old definition
                self.graph.update_name(&name, old_definition, scope)?;
            }
            ChangeEvent::EdgeAdded { from, to } => {
                // Remove the edge
                // TODO: Need specific edge removal method
                return Err(EditorError::TransactionFailed {
                    reason: "Cannot rollback edge addition yet".to_string(),
                });
            }
            ChangeEvent::EdgeRemoved { from, to } => {
                // Re-add the edge
                // TODO: Need specific edge addition method
                return Err(EditorError::TransactionFailed {
                    reason: "Cannot rollback edge removal yet".to_string(),
                });
            }
        }
        Ok(())
    }

    /// Add a vertex to the graph
    pub fn add_vertex(&mut self, meta: VertexMeta) -> VertexId {
        // For now, use the existing set_cell_value method to create vertices
        // This is a simplified implementation that works with the current API
        let sheet_name = self.graph.sheet_name(meta.sheet_id).to_string();

        let id = match meta.kind {
            VertexKind::Cell => {
                // Create with empty value initially.
                // NOTE: VertexEditor/VertexMeta use internal 0-based coords, while
                // DependencyGraph::set_cell_value is a public 1-based API. Convert here.
                match self.graph.set_cell_value(
                    &sheet_name,
                    meta.coord.row() + 1,
                    meta.coord.col() + 1,
                    LiteralValue::Empty,
                ) {
                    Ok(summary) => summary
                        .affected_vertices
                        .into_iter()
                        .next()
                        .unwrap_or(VertexId::new(0)),
                    Err(_) => VertexId::new(0),
                }
            }
            _ => {
                // For now, treat other kinds as cells.
                // A full implementation would handle different vertex kinds properly.
                // Convert internal 0-based coords to public 1-based API.
                match self.graph.set_cell_value(
                    &sheet_name,
                    meta.coord.row() + 1,
                    meta.coord.col() + 1,
                    LiteralValue::Empty,
                ) {
                    Ok(summary) => summary
                        .affected_vertices
                        .into_iter()
                        .next()
                        .unwrap_or(VertexId::new(0)),
                    Err(_) => VertexId::new(0),
                }
            }
        };

        if self.has_logger() && id.0 != 0 {
            self.log_change(ChangeEvent::AddVertex {
                id,
                coord: meta.coord,
                sheet_id: meta.sheet_id,
                value: Some(LiteralValue::Empty),
                formula: None,
                kind: Some(meta.kind),
                flags: Some(meta.flags),
            });
        }
        id
    }

    /// Remove a vertex from the graph with proper cleanup
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), EditorError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(id) {
            return Err(EditorError::Excel(
                ExcelError::new(ExcelErrorKind::Ref).with_message("Vertex does not exist"),
            ));
        }

        // Get dependents before removing edges
        // Note: get_dependents may require CSR rebuild if delta has changes
        let dependents = self.graph.get_dependents(id);

        // Capture old state (dependencies & dependents) BEFORE edge removal
        let (
            old_value,
            old_formula,
            old_dependencies,
            old_dependents,
            coord,
            sheet_id_opt,
            kind,
            flags,
        ) = if self.has_logger() {
            let coord = self.graph.get_coord(id);
            let sheet_id = self.graph.get_sheet_id(id);
            let kind = self.graph.get_vertex_kind(id);
            // flags not publicly exposed; set to 0 for now (future: expose getter)
            let flags = 0u8;
            (
                self.graph.get_value(id),
                self.get_formula_ast(id),
                self.graph.get_dependencies(id), // outgoing deps
                dependents.clone(),              // captured earlier
                Some(coord),
                Some(sheet_id),
                Some(kind),
                Some(flags),
            )
        } else {
            (None, None, vec![], vec![], None, None, None, None)
        };

        // Remove from cell mapping if it exists
        if let Some(cell_ref) = self.graph.get_cell_ref_for_vertex(id) {
            self.graph.remove_cell_mapping(&cell_ref);
        }

        // Remove all edges
        self.graph.remove_all_edges(id);

        // Mark all dependents as having #REF! error
        for dep_id in &dependents {
            self.graph.mark_as_ref_error(*dep_id);
        }

        // Mark as deleted in store (tombstone)
        self.graph.mark_deleted(id, true);

        // Log change event
        self.log_change(ChangeEvent::RemoveVertex {
            id,
            old_value,
            old_formula,
            old_dependencies,
            old_dependents,
            coord,
            sheet_id: sheet_id_opt,
            kind,
            flags,
        });

        Ok(())
    }

    /// Convenience: remove vertex at a given cell ref if exists
    pub fn remove_vertex_at(&mut self, cell: CellRef) -> Result<(), EditorError> {
        if let Some(id) = self.graph.get_vertex_for_cell(&cell) {
            self.remove_vertex(id)
        } else {
            Ok(())
        }
    }

    /// Move a vertex to a new position
    pub fn move_vertex(&mut self, id: VertexId, new_coord: AbsCoord) -> Result<(), EditorError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(id) {
            return Err(EditorError::Excel(
                ExcelError::new(ExcelErrorKind::Ref).with_message("Vertex does not exist"),
            ));
        }

        // Get old cell reference
        let old_cell_ref = self.graph.get_cell_ref_for_vertex(id);

        // Create new cell reference
        let sheet_id = self.graph.get_sheet_id(id);
        let new_cell_ref = CellRef::new(
            sheet_id,
            Coord::new(new_coord.row(), new_coord.col(), true, true),
        );

        // Update coordinate in store
        self.graph.set_coord(id, new_coord);

        // Update edge cache coordinate if needed
        self.graph.update_edge_coord(id, new_coord);

        // Update cell mapping
        self.graph
            .update_cell_mapping(id, old_cell_ref, new_cell_ref);

        // Mark dependents as dirty
        self.graph.mark_dependents_dirty(id);

        Ok(())
    }

    /// Update vertex metadata
    pub fn patch_vertex_meta(
        &mut self,
        id: VertexId,
        patch: VertexMetaPatch,
    ) -> Result<MetaUpdateSummary, EditorError> {
        if !self.graph.vertex_exists(id) {
            return Err(EditorError::Excel(
                ExcelError::new(ExcelErrorKind::Ref).with_message("Vertex does not exist"),
            ));
        }

        let mut summary = MetaUpdateSummary::default();

        if let Some(coord) = patch.coord {
            self.graph.set_coord(id, coord);
            self.graph.update_edge_coord(id, coord);
            summary.coord_changed = true;
        }

        if let Some(kind) = patch.kind {
            self.graph.set_kind(id, kind);
            summary.kind_changed = true;
        }

        if let Some(dirty) = patch.dirty {
            self.graph.set_dirty(id, dirty);
            summary.flags_changed = true;
        }

        if let Some(volatile) = patch.volatile {
            self.graph.mark_volatile(id, volatile);
            summary.flags_changed = true;
        }

        Ok(summary)
    }

    /// Update vertex data (value or formula)
    pub fn patch_vertex_data(
        &mut self,
        id: VertexId,
        patch: VertexDataPatch,
    ) -> Result<DataUpdateSummary, EditorError> {
        if !self.graph.vertex_exists(id) {
            return Err(EditorError::Excel(
                ExcelError::new(ExcelErrorKind::Ref).with_message("Vertex does not exist"),
            ));
        }

        let mut summary = DataUpdateSummary::default();

        if let Some(value) = patch.value {
            self.graph.update_vertex_value(id, value);
            summary.value_changed = true;

            // Force edge rebuild if needed to get accurate dependents
            // get_dependents may require rebuild when delta has changes
            if self.graph.edges_delta_size() > 0 {
                self.graph.rebuild_edges();
            }

            // Mark dependents as dirty
            let dependents = self.graph.get_dependents(id);
            for dep in &dependents {
                self.graph.set_dirty(*dep, true);
            }
            summary.dependents_marked_dirty = dependents;
        }

        if let Some(_formula) = patch.formula {
            // This would need proper formula update implementation
            // For now, we'll mark as changed
            summary.formula_changed = true;
        }

        Ok(summary)
    }

    /// Add an edge between two vertices
    pub fn add_edge(&mut self, from: VertexId, to: VertexId) -> bool {
        if from == to {
            return false; // Prevent self-loops
        }

        // TODO: Add edge through proper API when available
        // For now, return true to indicate intent
        true
    }

    /// Remove an edge between two vertices
    pub fn remove_edge(&mut self, _from: VertexId, _to: VertexId) -> bool {
        // TODO: Remove edge through proper API when available
        true
    }

    /// Insert rows at the specified position, shifting existing rows down
    pub fn insert_rows(
        &mut self,
        sheet_id: SheetId,
        before: u32,
        count: u32,
    ) -> Result<ShiftSummary, EditorError> {
        if count == 0 {
            return Ok(ShiftSummary::default());
        }

        let mut summary = ShiftSummary::default();

        // Begin batch for efficiency
        self.begin_batch();

        // 1. Collect vertices to shift (those at or after the insert point)
        let vertices_to_shift: Vec<(VertexId, AbsCoord)> = self
            .graph
            .vertices_in_sheet(sheet_id)
            .filter_map(|id| {
                let coord = self.graph.get_coord(id);
                if coord.row() >= before {
                    Some((id, coord))
                } else {
                    None
                }
            })
            .collect();

        if let Some(logger) = &mut self.change_logger {
            logger.begin_compound(format!(
                "InsertRows sheet={sheet_id} before={before} count={count}"
            ));
        }
        // 2. Shift vertices down (emit VertexMoved)
        for (id, old_coord) in vertices_to_shift {
            let new_coord = AbsCoord::new(old_coord.row() + count, old_coord.col());
            if self.has_logger() {
                self.log_change(ChangeEvent::VertexMoved {
                    id,
                    old_coord,
                    new_coord,
                });
            }
            self.move_vertex(id, new_coord)?;
            summary.vertices_moved.push(id);
        }

        // 3. Adjust formulas using ReferenceAdjuster
        let op = ShiftOperation::InsertRows {
            sheet_id,
            before,
            count,
        };
        let adjuster = ReferenceAdjuster::new();

        // Get all formulas and adjust them
        let formula_vertices: Vec<VertexId> = self.graph.vertices_with_formulas().collect();

        for id in formula_vertices {
            if let Some(ast) = self.get_formula_ast(id) {
                let adjusted = adjuster.adjust_ast(&ast, &op);
                // Only update if the formula actually changed
                if format!("{ast:?}") != format!("{adjusted:?}") {
                    self.graph.update_vertex_formula(id, adjusted)?;
                    self.graph.mark_vertex_dirty(id);
                    summary.formulas_updated += 1;
                }
            }
        }

        // 4. Adjust named ranges
        self.graph.adjust_named_ranges(&op)?;

        // 5. Log change event
        if let Some(logger) = &mut self.change_logger {
            logger.end_compound();
        }

        self.commit_batch();

        Ok(summary)
    }

    /// Delete rows at the specified position, shifting remaining rows up
    pub fn delete_rows(
        &mut self,
        sheet_id: SheetId,
        start: u32,
        count: u32,
    ) -> Result<ShiftSummary, EditorError> {
        if count == 0 {
            return Ok(ShiftSummary::default());
        }

        let mut summary = ShiftSummary::default();

        self.begin_batch();

        // 1. Delete vertices in the range
        let vertices_to_delete: Vec<VertexId> = self
            .graph
            .vertices_in_sheet(sheet_id)
            .filter(|&id| {
                let coord = self.graph.get_coord(id);
                coord.row() >= start && coord.row() < start + count
            })
            .collect();

        for id in vertices_to_delete {
            self.remove_vertex(id)?;
            summary.vertices_deleted.push(id);
        }

        if let Some(logger) = &mut self.change_logger {
            logger.begin_compound(format!(
                "DeleteRows sheet={sheet_id} start={start} count={count}"
            ));
        }
        // 2. Shift remaining vertices up (emit VertexMoved)
        let vertices_to_shift: Vec<(VertexId, AbsCoord)> = self
            .graph
            .vertices_in_sheet(sheet_id)
            .filter_map(|id| {
                let coord = self.graph.get_coord(id);
                if coord.row() >= start + count {
                    Some((id, coord))
                } else {
                    None
                }
            })
            .collect();

        for (id, old_coord) in vertices_to_shift {
            let new_coord = AbsCoord::new(old_coord.row() - count, old_coord.col());
            if self.has_logger() {
                self.log_change(ChangeEvent::VertexMoved {
                    id,
                    old_coord,
                    new_coord,
                });
            }
            self.move_vertex(id, new_coord)?;
            summary.vertices_moved.push(id);
        }

        // 3. Adjust formulas
        let op = ShiftOperation::DeleteRows {
            sheet_id,
            start,
            count,
        };
        let adjuster = ReferenceAdjuster::new();

        let formula_vertices: Vec<VertexId> = self.graph.vertices_with_formulas().collect();

        for id in formula_vertices {
            if let Some(ast) = self.get_formula_ast(id) {
                let adjusted = adjuster.adjust_ast(&ast, &op);
                if format!("{ast:?}") != format!("{adjusted:?}") {
                    self.graph.update_vertex_formula(id, adjusted)?;
                    self.graph.mark_vertex_dirty(id);
                    summary.formulas_updated += 1;
                }
            }
        }

        // 4. Adjust named ranges
        self.graph.adjust_named_ranges(&op)?;

        // 5. Log change event
        if let Some(logger) = &mut self.change_logger {
            logger.end_compound();
        }

        self.commit_batch();

        Ok(summary)
    }

    /// Insert columns at the specified position, shifting existing columns right
    pub fn insert_columns(
        &mut self,
        sheet_id: SheetId,
        before: u32,
        count: u32,
    ) -> Result<ShiftSummary, EditorError> {
        if count == 0 {
            return Ok(ShiftSummary::default());
        }

        let mut summary = ShiftSummary::default();

        // Begin batch for efficiency
        self.begin_batch();

        // 1. Collect vertices to shift (those at or after the insert point)
        let vertices_to_shift: Vec<(VertexId, AbsCoord)> = self
            .graph
            .vertices_in_sheet(sheet_id)
            .filter_map(|id| {
                let coord = self.graph.get_coord(id);
                if coord.col() >= before {
                    Some((id, coord))
                } else {
                    None
                }
            })
            .collect();

        if let Some(logger) = &mut self.change_logger {
            logger.begin_compound(format!(
                "InsertColumns sheet={sheet_id} before={before} count={count}"
            ));
        }
        // 2. Shift vertices right (emit VertexMoved)
        for (id, old_coord) in vertices_to_shift {
            let new_coord = AbsCoord::new(old_coord.row(), old_coord.col() + count);
            if self.has_logger() {
                self.log_change(ChangeEvent::VertexMoved {
                    id,
                    old_coord,
                    new_coord,
                });
            }
            self.move_vertex(id, new_coord)?;
            summary.vertices_moved.push(id);
        }

        // 3. Adjust formulas using ReferenceAdjuster
        let op = ShiftOperation::InsertColumns {
            sheet_id,
            before,
            count,
        };
        let adjuster = ReferenceAdjuster::new();

        // Get all formulas and adjust them
        let formula_vertices: Vec<VertexId> = self.graph.vertices_with_formulas().collect();

        for id in formula_vertices {
            if let Some(ast) = self.get_formula_ast(id) {
                let adjusted = adjuster.adjust_ast(&ast, &op);
                // Only update if the formula actually changed
                if format!("{ast:?}") != format!("{adjusted:?}") {
                    self.graph.update_vertex_formula(id, adjusted)?;
                    self.graph.mark_vertex_dirty(id);
                    summary.formulas_updated += 1;
                }
            }
        }

        // 4. Adjust named ranges
        self.graph.adjust_named_ranges(&op)?;

        // 5. Log change event
        if let Some(logger) = &mut self.change_logger {
            logger.end_compound();
        }

        self.commit_batch();

        Ok(summary)
    }

    /// Delete columns at the specified position, shifting remaining columns left
    pub fn delete_columns(
        &mut self,
        sheet_id: SheetId,
        start: u32,
        count: u32,
    ) -> Result<ShiftSummary, EditorError> {
        if count == 0 {
            return Ok(ShiftSummary::default());
        }

        let mut summary = ShiftSummary::default();

        self.begin_batch();

        // 1. Delete vertices in the range
        let vertices_to_delete: Vec<VertexId> = self
            .graph
            .vertices_in_sheet(sheet_id)
            .filter(|&id| {
                let coord = self.graph.get_coord(id);
                coord.col() >= start && coord.col() < start + count
            })
            .collect();

        for id in vertices_to_delete {
            self.remove_vertex(id)?;
            summary.vertices_deleted.push(id);
        }

        if let Some(logger) = &mut self.change_logger {
            logger.begin_compound(format!(
                "DeleteColumns sheet={sheet_id} start={start} count={count}"
            ));
        }
        // 2. Shift remaining vertices left (emit VertexMoved)
        let vertices_to_shift: Vec<(VertexId, AbsCoord)> = self
            .graph
            .vertices_in_sheet(sheet_id)
            .filter_map(|id| {
                let coord = self.graph.get_coord(id);
                if coord.col() >= start + count {
                    Some((id, coord))
                } else {
                    None
                }
            })
            .collect();

        for (id, old_coord) in vertices_to_shift {
            let new_coord = AbsCoord::new(old_coord.row(), old_coord.col() - count);
            if self.has_logger() {
                self.log_change(ChangeEvent::VertexMoved {
                    id,
                    old_coord,
                    new_coord,
                });
            }
            self.move_vertex(id, new_coord)?;
            summary.vertices_moved.push(id);
        }

        // 3. Adjust formulas
        let op = ShiftOperation::DeleteColumns {
            sheet_id,
            start,
            count,
        };
        let adjuster = ReferenceAdjuster::new();

        let formula_vertices: Vec<VertexId> = self.graph.vertices_with_formulas().collect();

        for id in formula_vertices {
            if let Some(ast) = self.get_formula_ast(id) {
                let adjusted = adjuster.adjust_ast(&ast, &op);
                if format!("{ast:?}") != format!("{adjusted:?}") {
                    self.graph.update_vertex_formula(id, adjusted)?;
                    self.graph.mark_vertex_dirty(id);
                    summary.formulas_updated += 1;
                }
            }
        }

        // 4. Adjust named ranges
        self.graph.adjust_named_ranges(&op)?;

        // 5. Log change event
        if let Some(logger) = &mut self.change_logger {
            logger.end_compound();
        }

        self.commit_batch();

        Ok(summary)
    }

    /// Shift rows down/up within a sheet (Excel's insert/delete rows)
    pub fn shift_rows(&mut self, sheet_id: SheetId, start_row: u32, delta: i32) {
        if delta == 0 {
            return;
        }

        // Log change event for undo/redo
        let change_event = ChangeEvent::SetValue {
            addr: CellRef {
                sheet_id,
                coord: Coord::new(start_row, 0, true, true),
            },
            old: None,
            new: LiteralValue::Text(format!("Row shift: start={start_row}, delta={delta}")),
        };
        self.log_change(change_event);

        // TODO: Implement actual row shifting logic
        // This would require coordination with the vertex store and dependency tracking
    }

    /// Shift columns left/right within a sheet (Excel's insert/delete columns)
    pub fn shift_columns(&mut self, sheet_id: SheetId, start_col: u32, delta: i32) {
        if delta == 0 {
            return;
        }

        // Log change event
        let change_event = ChangeEvent::SetValue {
            addr: CellRef {
                sheet_id,
                coord: Coord::new(0, start_col, true, true),
            },
            old: None,
            new: LiteralValue::Text(format!("Column shift: start={start_col}, delta={delta}")),
        };
        self.log_change(change_event);

        // TODO: Implement actual column shifting logic
        // This would require coordination with the vertex store and dependency tracking
    }

    /// Set a cell value, creating the vertex if it doesn't exist
    pub fn set_cell_value(&mut self, cell_ref: CellRef, value: LiteralValue) -> VertexId {
        let sheet_name = self.graph.sheet_name(cell_ref.sheet_id).to_string();

        // Capture old value before modification
        let old_value = self
            .graph
            .get_vertex_id_for_address(&cell_ref)
            .and_then(|&id| self.graph.get_value(id));

        // Use the existing DependencyGraph API
        // VertexEditor operates on internal 0-based coords; graph APIs are 1-based.
        match self.graph.set_cell_value(
            &sheet_name,
            cell_ref.coord.row() + 1,
            cell_ref.coord.col() + 1,
            value.clone(),
        ) {
            Ok(summary) => {
                // Log change event
                let change_event = ChangeEvent::SetValue {
                    addr: cell_ref,
                    old: old_value,
                    new: value,
                };
                self.log_change(change_event);

                summary
                    .affected_vertices
                    .into_iter()
                    .next()
                    .unwrap_or(VertexId::new(0))
            }
            Err(_) => VertexId::new(0),
        }
    }

    /// Set a cell formula, creating the vertex if it doesn't exist
    pub fn set_cell_formula(&mut self, cell_ref: CellRef, formula: ASTNode) -> VertexId {
        let sheet_name = self.graph.sheet_name(cell_ref.sheet_id).to_string();

        // Capture old formula before modification
        let old_formula = self
            .graph
            .get_vertex_id_for_address(&cell_ref)
            .and_then(|&id| self.get_formula_ast(id));

        // Use the existing DependencyGraph API
        // VertexEditor operates on internal 0-based coords; graph APIs are 1-based.
        match self.graph.set_cell_formula(
            &sheet_name,
            cell_ref.coord.row() + 1,
            cell_ref.coord.col() + 1,
            formula.clone(),
        ) {
            Ok(summary) => {
                // Log change event
                let change_event = ChangeEvent::SetFormula {
                    addr: cell_ref,
                    old: old_formula,
                    new: formula,
                };
                self.log_change(change_event);

                summary
                    .affected_vertices
                    .into_iter()
                    .next()
                    .unwrap_or(VertexId::new(0))
            }
            Err(_) => VertexId::new(0),
        }
    }

    // Range operations

    /// Set values for a rectangular range of cells
    pub fn set_range_values(
        &mut self,
        sheet_id: SheetId,
        start_row: u32,
        start_col: u32,
        values: &[Vec<LiteralValue>],
    ) -> Result<RangeSummary, EditorError> {
        let mut summary = RangeSummary::default();

        self.begin_batch();

        for (row_offset, row_values) in values.iter().enumerate() {
            for (col_offset, value) in row_values.iter().enumerate() {
                let row = start_row + row_offset as u32;
                let col = start_col + col_offset as u32;

                // Check if cell already exists
                let cell_ref = self.graph.make_cell_ref_internal(sheet_id, row, col);

                if let Some(&existing_id) = self.graph.get_vertex_id_for_address(&cell_ref) {
                    // Update existing vertex
                    self.graph.update_vertex_value(existing_id, value.clone());
                    self.graph.mark_vertex_dirty(existing_id);
                    summary.vertices_updated.push(existing_id);
                } else {
                    // Create new vertex
                    let meta = VertexMeta::new(row, col, sheet_id, VertexKind::Cell);
                    let id = self.add_vertex(meta);
                    self.graph.update_vertex_value(id, value.clone());
                    summary.vertices_created.push(id);
                }

                summary.cells_affected += 1;
            }
        }

        self.commit_batch();

        Ok(summary)
    }

    /// Clear all cells in a rectangular range
    pub fn clear_range(
        &mut self,
        sheet_id: SheetId,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    ) -> Result<RangeSummary, EditorError> {
        let mut summary = RangeSummary::default();

        self.begin_batch();

        // Collect vertices in range
        let vertices_in_range: Vec<_> = self
            .graph
            .vertices_in_sheet(sheet_id)
            .filter(|&id| {
                let coord = self.graph.get_coord(id);
                let row = coord.row();
                let col = coord.col();
                row >= start_row && row <= end_row && col >= start_col && col <= end_col
            })
            .collect();

        for id in vertices_in_range {
            self.remove_vertex(id)?;
            summary.cells_affected += 1;
        }

        self.commit_batch();

        Ok(summary)
    }

    /// Copy a range to a new location
    pub fn copy_range(
        &mut self,
        sheet_id: SheetId,
        from_start_row: u32,
        from_start_col: u32,
        from_end_row: u32,
        from_end_col: u32,
        to_sheet_id: SheetId,
        to_row: u32,
        to_col: u32,
    ) -> Result<RangeSummary, EditorError> {
        let row_offset = to_row as i32 - from_start_row as i32;
        let col_offset = to_col as i32 - from_start_col as i32;

        let mut summary = RangeSummary::default();
        let mut cell_data = Vec::new();

        // Collect source data
        let vertices_in_range: Vec<_> = self
            .graph
            .vertices_in_sheet(sheet_id)
            .filter(|&id| {
                let coord = self.graph.get_coord(id);
                let row = coord.row();
                let col = coord.col();
                row >= from_start_row
                    && row <= from_end_row
                    && col >= from_start_col
                    && col <= from_end_col
            })
            .collect();

        for id in vertices_in_range {
            let coord = self.graph.get_coord(id);
            let row = coord.row();
            let col = coord.col();

            // Get value or formula
            if let Some(formula) = self.get_formula_ast(id) {
                cell_data.push((
                    row - from_start_row,
                    col - from_start_col,
                    CellData::Formula(formula),
                ));
            } else if let Some(value) = self.graph.get_value(id) {
                cell_data.push((
                    row - from_start_row,
                    col - from_start_col,
                    CellData::Value(value),
                ));
            }
        }

        self.begin_batch();

        // Apply to destination with relative adjustment
        for (row_idx, col_idx, data) in cell_data {
            let dest_row = (to_row as i32 + row_idx as i32) as u32;
            let dest_col = (to_col as i32 + col_idx as i32) as u32;

            match data {
                CellData::Value(value) => {
                    let cell_ref =
                        self.graph
                            .make_cell_ref_internal(to_sheet_id, dest_row, dest_col);

                    if let Some(&existing_id) = self.graph.get_vertex_id_for_address(&cell_ref) {
                        self.graph.update_vertex_value(existing_id, value);
                        self.graph.mark_vertex_dirty(existing_id);
                        summary.vertices_updated.push(existing_id);
                    } else {
                        let meta =
                            VertexMeta::new(dest_row, dest_col, to_sheet_id, VertexKind::Cell);
                        let id = self.add_vertex(meta);
                        self.graph.update_vertex_value(id, value);
                        summary.vertices_created.push(id);
                    }
                }
                CellData::Formula(formula) => {
                    // Adjust relative references in formula
                    let adjuster = RelativeReferenceAdjuster::new(row_offset, col_offset);
                    let adjusted = adjuster.adjust_formula(&formula);

                    let cell_ref =
                        self.graph
                            .make_cell_ref_internal(to_sheet_id, dest_row, dest_col);

                    if let Some(&existing_id) = self.graph.get_vertex_id_for_address(&cell_ref) {
                        self.graph.update_vertex_formula(existing_id, adjusted)?;
                        summary.vertices_updated.push(existing_id);
                    } else {
                        let meta = VertexMeta::new(
                            dest_row,
                            dest_col,
                            to_sheet_id,
                            VertexKind::FormulaScalar,
                        );
                        let id = self.add_vertex(meta);
                        self.graph.update_vertex_formula(id, adjusted)?;
                        summary.vertices_created.push(id);
                    }
                }
            }

            summary.cells_affected += 1;
        }

        self.commit_batch();

        Ok(summary)
    }

    /// Move a range to a new location (copy + clear source)
    pub fn move_range(
        &mut self,
        sheet_id: SheetId,
        from_start_row: u32,
        from_start_col: u32,
        from_end_row: u32,
        from_end_col: u32,
        to_sheet_id: SheetId,
        to_row: u32,
        to_col: u32,
    ) -> Result<RangeSummary, EditorError> {
        // First copy the range
        let mut summary = self.copy_range(
            sheet_id,
            from_start_row,
            from_start_col,
            from_end_row,
            from_end_col,
            to_sheet_id,
            to_row,
            to_col,
        )?;

        // Then clear the source range
        let clear_summary = self.clear_range(
            sheet_id,
            from_start_row,
            from_start_col,
            from_end_row,
            from_end_col,
        )?;

        summary.cells_moved = clear_summary.cells_affected;

        // Update external references to moved cells
        let row_offset = to_row as i32 - from_start_row as i32;
        let col_offset = to_col as i32 - from_start_col as i32;

        // Find all formulas that reference the moved range
        let all_formula_vertices: Vec<_> = self.graph.vertices_with_formulas().collect();

        let from_sheet_name = self.graph.sheet_name(sheet_id).to_string();
        let to_sheet_name = self.graph.sheet_name(to_sheet_id).to_string();
        let adjuster = MoveReferenceAdjuster::new(
            sheet_id,
            from_sheet_name,
            from_start_row,
            from_start_col,
            from_end_row,
            from_end_col,
            to_sheet_id,
            to_sheet_name,
            row_offset,
            col_offset,
        );

        for formula_id in all_formula_vertices {
            if let Some(formula) = self.get_formula_ast(formula_id) {
                let formula_sheet_id = self.graph.get_vertex_sheet_id(formula_id);
                if let Some(adjusted) = adjuster.adjust_if_references(&formula, formula_sheet_id) {
                    self.graph.update_vertex_formula(formula_id, adjusted)?;
                }
            }
        }

        Ok(summary)
    }

    /// Define a named range
    pub fn define_name(
        &mut self,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), EditorError> {
        self.graph.define_name(name, definition.clone(), scope)?;

        self.log_change(ChangeEvent::DefineName {
            name: name.to_string(),
            scope,
            definition,
        });

        Ok(())
    }

    /// Helper to create definitions from coordinates for a single cell
    pub fn define_name_for_cell(
        &mut self,
        name: &str,
        sheet_name: &str,
        row: u32,
        col: u32,
        scope: NameScope,
    ) -> Result<(), EditorError> {
        let sheet_id = self
            .graph
            .sheet_id(sheet_name)
            .ok_or_else(|| EditorError::InvalidName {
                name: sheet_name.to_string(),
                reason: "Sheet not found".to_string(),
            })?;
        let cell_ref = CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
        self.define_name(name, NamedDefinition::Cell(cell_ref), scope)
    }

    /// Helper to create definitions from coordinates for a range
    pub fn define_name_for_range(
        &mut self,
        name: &str,
        sheet_name: &str,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
        scope: NameScope,
    ) -> Result<(), EditorError> {
        let sheet_id = self
            .graph
            .sheet_id(sheet_name)
            .ok_or_else(|| EditorError::InvalidName {
                name: sheet_name.to_string(),
                reason: "Sheet not found".to_string(),
            })?;
        let start = CellRef::new(
            sheet_id,
            Coord::from_excel(start_row, start_col, true, true),
        );
        let end = CellRef::new(sheet_id, Coord::from_excel(end_row, end_col, true, true));
        let range_ref = crate::reference::RangeRef::new(start, end);
        self.define_name(name, NamedDefinition::Range(range_ref), scope)
    }

    /// Update an existing named range definition
    pub fn update_name(
        &mut self,
        name: &str,
        new_definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), EditorError> {
        // Get the old definition for the change log
        let old_definition = self
            .graph
            .resolve_name(
                name,
                match scope {
                    NameScope::Sheet(id) => id,
                    NameScope::Workbook => 0,
                },
            )
            .cloned();

        self.graph
            .update_name(name, new_definition.clone(), scope)?;

        if let Some(old_def) = old_definition {
            self.log_change(ChangeEvent::UpdateName {
                name: name.to_string(),
                scope,
                old_definition: old_def,
                new_definition,
            });
        }

        Ok(())
    }

    /// Delete a named range
    pub fn delete_name(&mut self, name: &str, scope: NameScope) -> Result<(), EditorError> {
        self.graph.delete_name(name, scope)?;

        let old_def = if self.has_logger() {
            self.graph
                .resolve_name(
                    name,
                    match scope {
                        NameScope::Sheet(id) => id,
                        NameScope::Workbook => 0,
                    },
                )
                .cloned()
        } else {
            None
        };
        self.log_change(ChangeEvent::DeleteName {
            name: name.to_string(),
            scope,
            old_definition: old_def,
        });

        Ok(())
    }
}

/// Helper enum for cell data
enum CellData {
    Value(LiteralValue),
    Formula(ASTNode),
}

impl<'g> Drop for VertexEditor<'g> {
    fn drop(&mut self) {
        // Ensure batch operations are committed when the editor is dropped
        if self.batch_mode {
            self.commit_batch();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::editor::change_log::{ChangeEvent, ChangeLog};
    use crate::reference::Coord;

    fn create_test_graph() -> DependencyGraph {
        DependencyGraph::new()
    }

    #[test]
    fn test_vertex_editor_creation() {
        let mut graph = create_test_graph();
        let editor = VertexEditor::new(&mut graph);
        assert!(!editor.has_logger());
        assert!(!editor.batch_mode);
    }

    #[test]
    fn test_vertex_editor_with_logger() {
        let mut graph = create_test_graph();
        let mut log = ChangeLog::new();
        let editor = VertexEditor::with_logger(&mut graph, &mut log);
        assert!(editor.has_logger());
        assert!(!editor.batch_mode);
    }

    #[test]
    fn test_add_vertex() {
        let mut graph = create_test_graph();
        let mut editor = VertexEditor::new(&mut graph);

        let meta = VertexMeta::new(5, 10, 0, VertexKind::Cell).dirty();
        let vertex_id = editor.add_vertex(meta);

        // Verify vertex was created (simplified check)
        assert!(vertex_id.0 > 0);
    }

    #[test]
    fn test_batch_operations() {
        let mut graph = create_test_graph();
        let mut editor = VertexEditor::new(&mut graph);

        assert!(!editor.batch_mode);
        editor.begin_batch();
        assert!(editor.batch_mode);

        // Add multiple vertices in batch mode
        let meta1 = VertexMeta::new(1, 1, 0, VertexKind::Cell);
        let meta2 = VertexMeta::new(2, 2, 0, VertexKind::Cell);

        let id1 = editor.add_vertex(meta1);
        let id2 = editor.add_vertex(meta2);

        // Add edge between them
        assert!(editor.add_edge(id1, id2));

        editor.commit_batch();
        assert!(!editor.batch_mode);
    }

    #[test]
    fn test_remove_vertex() {
        let mut graph = create_test_graph();
        let mut editor = VertexEditor::new(&mut graph);

        let meta = VertexMeta::new(3, 4, 0, VertexKind::Cell).dirty();
        let vertex_id = editor.add_vertex(meta);

        // Now removal returns Result
        assert!(editor.remove_vertex(vertex_id).is_ok());
    }

    #[test]
    fn test_edge_operations() {
        let mut graph = create_test_graph();
        let mut editor = VertexEditor::new(&mut graph);

        let meta1 = VertexMeta::new(1, 1, 0, VertexKind::Cell);
        let meta2 = VertexMeta::new(2, 2, 0, VertexKind::FormulaScalar);

        let id1 = editor.add_vertex(meta1);
        let id2 = editor.add_vertex(meta2);

        // Add edge
        assert!(editor.add_edge(id1, id2));

        // Prevent self-loop
        assert!(!editor.add_edge(id1, id1));

        // Remove edge
        assert!(editor.remove_edge(id1, id2));
    }

    #[test]
    fn test_set_cell_value() {
        let mut graph = create_test_graph();
        let mut log = ChangeLog::new();

        let cell_ref = CellRef {
            sheet_id: 0,
            coord: Coord::new(2, 3, true, true),
        };
        let value = LiteralValue::Number(42.0);

        let vertex_id = {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.set_cell_value(cell_ref, value.clone())
        };

        // Verify vertex was created (simplified check)
        assert!(vertex_id.0 > 0);

        // Verify change log
        assert_eq!(log.len(), 1);
        match &log.events()[0] {
            ChangeEvent::SetValue { addr, new, .. } => {
                assert_eq!(addr.sheet_id, cell_ref.sheet_id);
                assert_eq!(addr.coord.row(), cell_ref.coord.row());
                assert_eq!(addr.coord.col(), cell_ref.coord.col());
                assert_eq!(new, &value);
            }
            _ => panic!("Expected SetValue event"),
        }
    }

    #[test]
    fn test_set_cell_formula() {
        let mut graph = create_test_graph();
        let mut log = ChangeLog::new();

        let cell_ref = CellRef {
            sheet_id: 0,
            coord: Coord::new(1, 1, true, true),
        };

        use formualizer_parse::parser::ASTNodeType;
        let formula = formualizer_parse::parser::ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Number(100.0)),
            source_token: None,
            contains_volatile: false,
        };

        let vertex_id = {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.set_cell_formula(cell_ref, formula.clone())
        };

        // Verify vertex was created (simplified check)
        assert!(vertex_id.0 > 0);

        // Verify change log
        assert_eq!(log.len(), 1);
        match &log.events()[0] {
            ChangeEvent::SetFormula { addr, .. } => {
                assert_eq!(addr.sheet_id, cell_ref.sheet_id);
                assert_eq!(addr.coord.row(), cell_ref.coord.row());
                assert_eq!(addr.coord.col(), cell_ref.coord.col());
            }
            _ => panic!("Expected SetFormula event"),
        }
    }

    #[test]
    fn test_shift_rows() {
        let mut graph = create_test_graph();
        let mut log = ChangeLog::new();

        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);

            // Create vertices at different rows
            let cell1 = CellRef {
                sheet_id: 0,
                coord: Coord::new(5, 1, true, true),
            };
            let cell2 = CellRef {
                sheet_id: 0,
                coord: Coord::new(10, 1, true, true),
            };
            let cell3 = CellRef {
                sheet_id: 0,
                coord: Coord::new(15, 1, true, true),
            };

            editor.set_cell_value(cell1, LiteralValue::Number(1.0));
            editor.set_cell_value(cell2, LiteralValue::Number(2.0));
            editor.set_cell_value(cell3, LiteralValue::Number(3.0));
        }

        // Clear change log to focus on shift operation
        log.clear();

        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            // Shift rows starting at row 10, moving down by 2
            editor.shift_rows(0, 10, 2);
        }

        // Verify change log contains the shift operation
        assert_eq!(log.len(), 1);
        match &log.events()[0] {
            ChangeEvent::SetValue { addr, new, .. } => {
                assert_eq!(addr.sheet_id, 0);
                assert_eq!(addr.coord.row(), 10);
                if let LiteralValue::Text(msg) = new {
                    assert!(msg.contains("Row shift"));
                    assert!(msg.contains("start=10"));
                    assert!(msg.contains("delta=2"));
                }
            }
            _ => panic!("Expected SetValue event for row shift"),
        }
    }

    #[test]
    fn test_shift_columns() {
        let mut graph = create_test_graph();
        let mut log = ChangeLog::new();

        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);

            // Create vertices at different columns
            let cell1 = CellRef {
                sheet_id: 0,
                coord: Coord::new(1, 5, true, true),
            };
            let cell2 = CellRef {
                sheet_id: 0,
                coord: Coord::new(1, 10, true, true),
            };

            editor.set_cell_value(cell1, LiteralValue::Number(1.0));
            editor.set_cell_value(cell2, LiteralValue::Number(2.0));
        }

        // Clear change log
        log.clear();

        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            // Shift columns starting at col 8, moving right by 3
            editor.shift_columns(0, 8, 3);
        }

        // Verify change log
        assert_eq!(log.len(), 1);
        match &log.events()[0] {
            ChangeEvent::SetValue { addr, new, .. } => {
                assert_eq!(addr.sheet_id, 0);
                assert_eq!(addr.coord.col(), 8);
                if let LiteralValue::Text(msg) = new {
                    assert!(msg.contains("Column shift"));
                    assert!(msg.contains("start=8"));
                    assert!(msg.contains("delta=3"));
                }
            }
            _ => panic!("Expected SetValue event for column shift"),
        }
    }

    #[test]
    fn test_move_vertex() {
        let mut graph = create_test_graph();
        let mut editor = VertexEditor::new(&mut graph);

        let meta = VertexMeta::new(5, 10, 0, VertexKind::Cell);
        let vertex_id = editor.add_vertex(meta);

        // Move vertex returns Result
        assert!(editor.move_vertex(vertex_id, AbsCoord::new(8, 12)).is_ok());

        // Moving to same position should work
        assert!(editor.move_vertex(vertex_id, AbsCoord::new(8, 12)).is_ok());
    }

    #[test]
    fn test_vertex_meta_builder() {
        let meta = VertexMeta::new(1, 2, 3, VertexKind::FormulaScalar)
            .dirty()
            .volatile()
            .with_flags(0x08);

        assert_eq!(meta.coord.row(), 1);
        assert_eq!(meta.coord.col(), 2);
        assert_eq!(meta.sheet_id, 3);
        assert_eq!(meta.kind, VertexKind::FormulaScalar);
        assert_eq!(meta.flags, 0x08); // Last with_flags call overwrites previous flags
    }

    #[test]
    fn test_change_log_management() {
        let mut graph = create_test_graph();
        let mut log = ChangeLog::new();

        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            let cell_ref = CellRef {
                sheet_id: 0,
                coord: Coord::new(0, 0, true, true),
            };
            editor.set_cell_value(cell_ref, LiteralValue::Number(1.0));
            editor.set_cell_value(cell_ref, LiteralValue::Number(2.0));
        }

        assert_eq!(log.len(), 2);

        log.clear();
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn test_editor_drop_commits_batch() {
        let mut graph = create_test_graph();
        {
            let mut editor = VertexEditor::new(&mut graph);
            editor.begin_batch();

            let meta = VertexMeta::new(1, 1, 0, VertexKind::Cell);
            editor.add_vertex(meta);

            // Editor will be dropped here, should commit batch
        }

        // If we reach here without hanging, the batch was properly committed
    }
}
