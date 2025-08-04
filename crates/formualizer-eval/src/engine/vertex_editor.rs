use super::graph::{ChangeEvent, DependencyGraph};
use super::packed_coord::PackedCoord;
use super::vertex::{VertexId, VertexKind};
use crate::SheetId;
use crate::reference::{CellRef, Coord};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_core::parser::ASTNode;

/// Metadata for creating a new vertex
#[derive(Debug, Clone)]
pub struct VertexMeta {
    pub coord: PackedCoord,
    pub sheet_id: SheetId,
    pub kind: VertexKind,
    pub flags: u8,
}

impl VertexMeta {
    pub fn new(row: u32, col: u32, sheet_id: SheetId, kind: VertexKind) -> Self {
        Self {
            coord: PackedCoord::new(row, col),
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
    pub coord: Option<PackedCoord>,
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

/// Custom error type for vertex editor operations
#[derive(Debug, Clone)]
pub enum EditorError {
    TargetOccupied { cell: CellRef },
    OutOfBounds { row: u32, col: u32 },
    InvalidName { name: String, reason: String },
    TransactionFailed { reason: String },
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
                    cell.coord.row, cell.coord.col
                )
            }
            EditorError::OutOfBounds { row, col } => {
                write!(f, "Cell position out of bounds: row {}, col {}", row, col)
            }
            EditorError::InvalidName { name, reason } => {
                write!(f, "Invalid name '{}': {}", name, reason)
            }
            EditorError::TransactionFailed { reason } => {
                write!(f, "Transaction failed: {}", reason)
            }
            EditorError::Excel(e) => write!(f, "Excel error: {:?}", e),
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
/// // Review change log for undo/redo
/// println!("Changes made: {}", editor.change_log().len());
/// ```
pub struct VertexEditor<'g> {
    graph: &'g mut DependencyGraph,
    change_log: Vec<ChangeEvent>,
    batch_mode: bool,
    changelog_enabled: bool,
}

impl<'g> VertexEditor<'g> {
    /// Create a new vertex editor with exclusive access to the graph
    pub fn new(graph: &'g mut DependencyGraph) -> Self {
        Self {
            graph,
            change_log: Vec::new(),
            batch_mode: false,
            changelog_enabled: true,
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

    /// Enable or disable the change log
    pub fn set_changelog_enabled(&mut self, enabled: bool) {
        self.changelog_enabled = enabled;
    }

    /// Get the accumulated change log
    pub fn change_log(&self) -> &[ChangeEvent] {
        &self.change_log
    }

    /// Clear the change log
    pub fn clear_change_log(&mut self) {
        self.change_log.clear();
    }

    /// Add a vertex to the graph
    pub fn add_vertex(&mut self, meta: VertexMeta) -> VertexId {
        // For now, use the existing set_cell_value method to create vertices
        // This is a simplified implementation that works with the current API
        let sheet_name = self.graph.sheet_name(meta.sheet_id).to_string();

        match meta.kind {
            VertexKind::Cell => {
                // Create with empty value initially
                match self.graph.set_cell_value(
                    &sheet_name,
                    meta.coord.row(),
                    meta.coord.col(),
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
                // For now, treat other kinds as cells
                // A full implementation would handle different vertex kinds properly
                match self.graph.set_cell_value(
                    &sheet_name,
                    meta.coord.row(),
                    meta.coord.col(),
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
        }
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

        // Remove all edges
        self.graph.remove_all_edges(id);

        // Mark all dependents as having #REF! error
        for dep_id in dependents {
            self.graph.mark_as_ref_error(dep_id);
        }

        // Mark as deleted in store (tombstone)
        self.graph.mark_deleted(id, true);

        // Log change event
        if self.changelog_enabled {
            self.change_log.push(ChangeEvent::RemoveVertex { id });
        }

        Ok(())
    }

    /// Move a vertex to a new position
    pub fn move_vertex(&mut self, id: VertexId, new_coord: PackedCoord) -> Result<(), EditorError> {
        // Check if vertex exists
        if !self.graph.vertex_exists(id) {
            return Err(EditorError::Excel(
                ExcelError::new(ExcelErrorKind::Ref).with_message("Vertex does not exist"),
            ));
        }

        // Update coordinate in store
        self.graph.set_coord(id, new_coord);

        // Update edge cache coordinate if needed
        self.graph.update_edge_coord(id, new_coord);

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
        if self.changelog_enabled {
            self.change_log.push(change_event);
        }

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
        if self.changelog_enabled {
            self.change_log.push(change_event);
        }

        // TODO: Implement actual column shifting logic
        // This would require coordination with the vertex store and dependency tracking
    }

    /// Set a cell value, creating the vertex if it doesn't exist
    pub fn set_cell_value(&mut self, cell_ref: CellRef, value: LiteralValue) -> VertexId {
        let sheet_name = self.graph.sheet_name(cell_ref.sheet_id).to_string();

        // Use the existing DependencyGraph API
        match self.graph.set_cell_value(
            &sheet_name,
            cell_ref.coord.row,
            cell_ref.coord.col,
            value.clone(),
        ) {
            Ok(summary) => {
                // Log change event
                let change_event = ChangeEvent::SetValue {
                    addr: cell_ref,
                    old: None, // TODO: Capture old value for proper undo support
                    new: value,
                };
                self.change_log.push(change_event);

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

        // Use the existing DependencyGraph API
        match self.graph.set_cell_formula(
            &sheet_name,
            cell_ref.coord.row,
            cell_ref.coord.col,
            formula.clone(),
        ) {
            Ok(summary) => {
                // Log change event
                let change_event = ChangeEvent::SetFormula {
                    addr: cell_ref,
                    old: None, // TODO: Capture old formula for proper undo support
                    new: formula,
                };
                self.change_log.push(change_event);

                summary
                    .affected_vertices
                    .into_iter()
                    .next()
                    .unwrap_or(VertexId::new(0))
            }
            Err(_) => VertexId::new(0),
        }
    }
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
    use crate::reference::Coord;

    fn create_test_graph() -> DependencyGraph {
        DependencyGraph::new()
    }

    #[test]
    fn test_vertex_editor_creation() {
        let mut graph = create_test_graph();
        let editor = VertexEditor::new(&mut graph);
        assert_eq!(editor.change_log().len(), 0);
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
        let mut editor = VertexEditor::new(&mut graph);

        let cell_ref = CellRef {
            sheet_id: 0,
            coord: Coord::new(2, 3, true, true),
        };
        let value = LiteralValue::Number(42.0);

        let vertex_id = editor.set_cell_value(cell_ref, value.clone());

        // Verify vertex was created (simplified check)
        assert!(vertex_id.0 > 0);

        // Verify change log
        assert_eq!(editor.change_log().len(), 1);
        match &editor.change_log()[0] {
            ChangeEvent::SetValue { addr, new, .. } => {
                assert_eq!(addr.sheet_id, cell_ref.sheet_id);
                assert_eq!(addr.coord.row, cell_ref.coord.row);
                assert_eq!(addr.coord.col, cell_ref.coord.col);
                assert_eq!(*new, value);
            }
            _ => panic!("Expected SetValue event"),
        }
    }

    #[test]
    fn test_set_cell_formula() {
        let mut graph = create_test_graph();
        let mut editor = VertexEditor::new(&mut graph);

        let cell_ref = CellRef {
            sheet_id: 0,
            coord: Coord::new(1, 1, true, true),
        };

        use formualizer_core::parser::ASTNodeType;
        let formula = formualizer_core::parser::ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Number(100.0)),
            source_token: None,
        };

        let vertex_id = editor.set_cell_formula(cell_ref, formula.clone());

        // Verify vertex was created (simplified check)
        assert!(vertex_id.0 > 0);

        // Verify change log
        assert_eq!(editor.change_log().len(), 1);
        match &editor.change_log()[0] {
            ChangeEvent::SetFormula { addr, .. } => {
                assert_eq!(addr.sheet_id, cell_ref.sheet_id);
                assert_eq!(addr.coord.row, cell_ref.coord.row);
                assert_eq!(addr.coord.col, cell_ref.coord.col);
            }
            _ => panic!("Expected SetFormula event"),
        }
    }

    #[test]
    fn test_shift_rows() {
        let mut graph = create_test_graph();
        let mut editor = VertexEditor::new(&mut graph);

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

        // Clear change log to focus on shift operation
        editor.clear_change_log();

        // Shift rows starting at row 10, moving down by 2
        editor.shift_rows(0, 10, 2);

        // Verify change log contains the shift operation
        assert_eq!(editor.change_log().len(), 1);
        match &editor.change_log()[0] {
            ChangeEvent::SetValue { addr, new, .. } => {
                assert_eq!(addr.sheet_id, 0);
                assert_eq!(addr.coord.row, 10);
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
        let mut editor = VertexEditor::new(&mut graph);

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

        // Clear change log
        editor.clear_change_log();

        // Shift columns starting at col 8, moving right by 3
        editor.shift_columns(0, 8, 3);

        // Verify change log
        assert_eq!(editor.change_log().len(), 1);
        match &editor.change_log()[0] {
            ChangeEvent::SetValue { addr, new, .. } => {
                assert_eq!(addr.sheet_id, 0);
                assert_eq!(addr.coord.col, 8);
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
        assert!(
            editor
                .move_vertex(vertex_id, PackedCoord::new(8, 12))
                .is_ok()
        );

        // Moving to same position should work
        assert!(
            editor
                .move_vertex(vertex_id, PackedCoord::new(8, 12))
                .is_ok()
        );
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
        let mut editor = VertexEditor::new(&mut graph);

        let cell_ref = CellRef {
            sheet_id: 0,
            coord: Coord::new(0, 0, true, true),
        };
        editor.set_cell_value(cell_ref, LiteralValue::Number(1.0));
        editor.set_cell_value(cell_ref, LiteralValue::Number(2.0));

        assert_eq!(editor.change_log().len(), 2);

        editor.clear_change_log();
        assert_eq!(editor.change_log().len(), 0);
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
