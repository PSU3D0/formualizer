//! Transaction orchestration for coordinating graph mutations with rollback support
//!
//! This module provides:
//! - TransactionContext: Orchestrates ChangeLog, TransactionManager, and VertexEditor
//! - Rollback logic for undoing changes
//! - Savepoint support for partial rollback

use crate::engine::graph::editor::transaction_manager::{
    TransactionError, TransactionId, TransactionManager,
};
use crate::engine::graph::editor::{EditorError, VertexEditor};
use crate::engine::graph::DependencyGraph;
use crate::engine::{ChangeEvent, ChangeLog};

/// Orchestrates transactions across graph mutations, change logging, and rollback
pub struct TransactionContext<'g> {
    graph: &'g mut DependencyGraph,
    change_log: ChangeLog,
    tx_manager: TransactionManager,
}

impl<'g> TransactionContext<'g> {
    /// Create a new transaction context for the given graph
    pub fn new(graph: &'g mut DependencyGraph) -> Self {
        Self {
            graph,
            change_log: ChangeLog::new(),
            tx_manager: TransactionManager::new(),
        }
    }

    /// Create a transaction context with custom max transaction size
    pub fn with_max_size(graph: &'g mut DependencyGraph, max_size: usize) -> Self {
        Self {
            graph,
            change_log: ChangeLog::new(),
            tx_manager: TransactionManager::with_max_size(max_size),
        }
    }

    /// Begin a new transaction
    ///
    /// # Returns
    /// The ID of the newly created transaction
    ///
    /// # Errors
    /// Returns `AlreadyActive` if a transaction is already in progress
    pub fn begin(&mut self) -> Result<TransactionId, TransactionError> {
        self.tx_manager.begin(self.change_log.len())
    }

    /// Create an editor that logs changes to this context
    ///
    /// # Safety
    /// This uses unsafe code to work around the borrow checker.
    /// It's safe because:
    /// 1. We control the lifetime of both the graph and change_log
    /// 2. The editor's lifetime is tied to the TransactionContext
    /// 3. We ensure no aliasing occurs
    pub fn editor(&mut self) -> VertexEditor<'_> {
        // We need to create two mutable references: one to graph, one to change_log
        // This is safe because VertexEditor doesn't expose the graph reference
        // and we control the lifetimes
        unsafe {
            let graph_ptr = self.graph as *mut DependencyGraph;
            let logger_ptr = &mut self.change_log as *mut ChangeLog;
            VertexEditor::with_logger(&mut *graph_ptr, &mut *logger_ptr)
        }
    }

    /// Commit the current transaction
    ///
    /// # Returns
    /// The ID of the committed transaction
    ///
    /// # Errors
    /// Returns `NoActiveTransaction` if no transaction is active
    pub fn commit(&mut self) -> Result<TransactionId, TransactionError> {
        // Check size limit before committing
        self.tx_manager.check_size(self.change_log.len())?;
        self.tx_manager.commit()
    }

    /// Rollback the current transaction
    ///
    /// # Errors
    /// Returns `NoActiveTransaction` if no transaction is active
    /// Returns `RollbackFailed` if the rollback operation fails
    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        let (_tx_id, start_index) = self.tx_manager.rollback_info()?;

        // Extract changes to rollback
        let changes = self.change_log.take_from(start_index);

        // Apply inverse operations
        self.apply_rollback(changes)?;

        Ok(())
    }

    /// Add a named savepoint to the current transaction
    ///
    /// # Arguments
    /// * `name` - Name for the savepoint
    ///
    /// # Errors
    /// Returns `NoActiveTransaction` if no transaction is active
    pub fn savepoint(&mut self, name: &str) -> Result<(), TransactionError> {
        self.tx_manager
            .add_savepoint(name.to_string(), self.change_log.len())
    }

    /// Rollback to a named savepoint
    ///
    /// # Arguments
    /// * `name` - Name of the savepoint to rollback to
    ///
    /// # Errors
    /// Returns `NoActiveTransaction` if no transaction is active
    /// Returns `SavepointNotFound` if the savepoint doesn't exist
    /// Returns `RollbackFailed` if the rollback operation fails
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), TransactionError> {
        let savepoint_index = self.tx_manager.get_savepoint(name)?;

        // Extract changes after the savepoint
        let changes = self.change_log.take_from(savepoint_index);

        // Truncate savepoints that are being rolled back
        self.tx_manager.truncate_savepoints(savepoint_index);

        // Apply inverse operations
        self.apply_rollback(changes)?;

        Ok(())
    }

    /// Check if a transaction is currently active
    pub fn is_active(&self) -> bool {
        self.tx_manager.is_active()
    }

    /// Get the ID of the active transaction if any
    pub fn active_id(&self) -> Option<TransactionId> {
        self.tx_manager.active_id()
    }

    /// Get the current size of the change log
    pub fn change_count(&self) -> usize {
        self.change_log.len()
    }

    /// Get reference to the change log (for testing/debugging)
    pub fn change_log(&self) -> &ChangeLog {
        &self.change_log
    }

    /// Clear the change log (useful between transactions)
    pub fn clear_change_log(&mut self) {
        self.change_log.clear();
    }

    /// Apply rollback for a list of changes
    fn apply_rollback(&mut self, changes: Vec<ChangeEvent>) -> Result<(), TransactionError> {
        // Disable logging during rollback to avoid recording rollback operations
        self.change_log.set_enabled(false);

        // Track compound operation depth for proper rollback
        let mut compound_stack = Vec::new();

        // Apply changes in reverse order
        for change in changes.into_iter().rev() {
            match change {
                ChangeEvent::CompoundEnd { depth } => {
                    // Starting to rollback a compound operation (remember, we're going backwards)
                    compound_stack.push(depth);
                }
                ChangeEvent::CompoundStart { depth, .. } => {
                    // Finished rolling back a compound operation
                    if compound_stack.last() == Some(&depth) {
                        compound_stack.pop();
                    }
                }
                _ => {
                    // Apply inverse for actual changes
                    if let Err(e) = self.apply_inverse(change) {
                        self.change_log.set_enabled(true);
                        return Err(TransactionError::RollbackFailed(e.to_string()));
                    }
                }
            }
        }

        self.change_log.set_enabled(true);
        Ok(())
    }

    /// Apply the inverse of a single change event
    fn apply_inverse(&mut self, change: ChangeEvent) -> Result<(), EditorError> {
        let mut editor = VertexEditor::new(self.graph);
        editor.apply_inverse(change)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{reference::Coord, CellRef};
    use formualizer_common::LiteralValue;
    use formualizer_parse::parse;

    fn create_test_graph() -> DependencyGraph {
        DependencyGraph::new()
    }

    fn cell_ref(sheet_id: u16, row: u32, col: u32) -> CellRef {
        // Test helpers use Excel 1-based coords.
        // Graph/editor keys store absolute ("$A$1") coords.
        CellRef::new(sheet_id, Coord::from_excel(row, col, true, true))
    }

    #[test]
    fn test_transaction_context_basic() {
        let mut graph = create_test_graph();
        let mut ctx = TransactionContext::new(&mut graph);

        // Begin transaction
        let tx_id = ctx.begin().unwrap();
        assert!(ctx.is_active());
        assert_eq!(ctx.active_id(), Some(tx_id));

        // Make changes
        {
            let mut editor = ctx.editor();
            editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(42.0));
        }

        // Verify change was logged
        assert_eq!(ctx.change_count(), 1);

        // Commit transaction
        let committed_id = ctx.commit().unwrap();
        assert_eq!(tx_id, committed_id);
        assert!(!ctx.is_active());
    }

    #[test]
    fn test_transaction_context_rollback_new_value() {
        let mut graph = create_test_graph();

        {
            let mut ctx = TransactionContext::new(&mut graph);

            ctx.begin().unwrap();

            // Add a new value
            {
                let mut editor = ctx.editor();
                editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(20.0));
            }

            // Rollback
            ctx.rollback().unwrap();
            assert_eq!(ctx.change_count(), 0);
        }

        // Verify value was removed after context is dropped
        assert!(graph
            .get_vertex_id_for_address(&cell_ref(0, 1, 1))
            .is_none());
    }

    #[test]
    fn test_transaction_context_rollback_value_update() {
        let mut graph = create_test_graph();

        // Set initial value outside transaction
        let _ = graph.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0));

        {
            let mut ctx = TransactionContext::new(&mut graph);
            ctx.begin().unwrap();

            // Update value
            {
                let mut editor = ctx.editor();
                editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(20.0));
            }

            // Rollback
            ctx.rollback().unwrap();
        }

        // Verify original value restored after context is dropped
        assert_eq!(
            graph.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(10.0))
        );
    }

    #[test]
    fn test_transaction_context_multiple_changes() {
        let mut graph = create_test_graph();

        {
            let mut ctx = TransactionContext::new(&mut graph);

            ctx.begin().unwrap();

            // Make multiple changes
            {
                let mut editor = ctx.editor();
                editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(10.0));
                editor.set_cell_value(cell_ref(0, 2, 1), LiteralValue::Number(20.0));
                editor.set_cell_formula(cell_ref(0, 3, 1), parse("=A1+A2").unwrap());
            }

            assert_eq!(ctx.change_count(), 3);

            // Commit
            ctx.commit().unwrap();
        }

        // Changes should persist after context is dropped
        assert_eq!(
            graph.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(10.0))
        );
        assert_eq!(
            graph.get_cell_value("Sheet1", 2, 1),
            Some(LiteralValue::Number(20.0))
        );
        assert!(graph
            .get_vertex_id_for_address(&cell_ref(0, 3, 1))
            .is_some());
    }

    #[test]
    fn test_transaction_context_savepoints() {
        let mut graph = create_test_graph();

        {
            let mut ctx = TransactionContext::new(&mut graph);

            ctx.begin().unwrap();

            // First change
            {
                let mut editor = ctx.editor();
                editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(10.0));
            }

            // Create savepoint
            ctx.savepoint("after_first").unwrap();

            // More changes
            {
                let mut editor = ctx.editor();
                editor.set_cell_value(cell_ref(0, 2, 1), LiteralValue::Number(20.0));
                editor.set_cell_value(cell_ref(0, 3, 1), LiteralValue::Number(30.0));
            }

            assert_eq!(ctx.change_count(), 3);

            // Rollback to savepoint
            ctx.rollback_to_savepoint("after_first").unwrap();

            // First change remains, others rolled back
            assert_eq!(ctx.change_count(), 1);

            // Can still commit the remaining changes
            ctx.commit().unwrap();
        }

        // Verify state after context is dropped
        assert_eq!(
            graph.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(10.0))
        );
        assert!(graph
            .get_vertex_id_for_address(&cell_ref(0, 2, 1))
            .is_none());
        assert!(graph
            .get_vertex_id_for_address(&cell_ref(0, 3, 1))
            .is_none());
    }

    #[test]
    fn test_transaction_context_size_limit() {
        let mut graph = create_test_graph();
        let mut ctx = TransactionContext::with_max_size(&mut graph, 2);

        ctx.begin().unwrap();

        // Add changes up to limit
        {
            let mut editor = ctx.editor();
            editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(1.0));
            editor.set_cell_value(cell_ref(0, 2, 1), LiteralValue::Number(2.0));
        }

        // Should succeed at limit
        assert!(ctx.commit().is_ok());

        ctx.begin().unwrap();

        // Exceed limit
        {
            let mut editor = ctx.editor();
            editor.set_cell_value(cell_ref(0, 3, 1), LiteralValue::Number(3.0));
            editor.set_cell_value(cell_ref(0, 4, 1), LiteralValue::Number(4.0));
            editor.set_cell_value(cell_ref(0, 5, 1), LiteralValue::Number(5.0));
        }

        // Should fail when exceeding limit
        match ctx.commit() {
            Err(TransactionError::TransactionTooLarge { size, max }) => {
                assert_eq!(size, 3);
                assert_eq!(max, 2);
            }
            _ => panic!("Expected TransactionTooLarge error"),
        }
    }

    #[test]
    fn test_transaction_context_no_active_transaction() {
        let mut graph = create_test_graph();
        let mut ctx = TransactionContext::new(&mut graph);

        // Operations without active transaction should fail
        assert!(ctx.commit().is_err());
        assert!(ctx.rollback().is_err());
        assert!(ctx.savepoint("test").is_err());
        assert!(ctx.rollback_to_savepoint("test").is_err());
    }

    #[test]
    fn test_transaction_context_clear_change_log() {
        let mut graph = create_test_graph();
        let mut ctx = TransactionContext::new(&mut graph);

        // Make changes without transaction (for testing)
        {
            let mut editor = ctx.editor();
            editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(1.0));
            editor.set_cell_value(cell_ref(0, 2, 1), LiteralValue::Number(2.0));
        }

        assert_eq!(ctx.change_count(), 2);

        // Clear change log
        ctx.clear_change_log();
        assert_eq!(ctx.change_count(), 0);
    }

    #[test]
    fn test_transaction_context_compound_operations() {
        let mut graph = create_test_graph();
        let mut ctx = TransactionContext::new(&mut graph);

        ctx.begin().unwrap();

        // Simulate a compound operation using the change_log directly
        ctx.change_log.begin_compound("test_compound".to_string());

        {
            let mut editor = ctx.editor();
            editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(1.0));
            editor.set_cell_value(cell_ref(0, 2, 1), LiteralValue::Number(2.0));
        }

        ctx.change_log.end_compound();

        // Should have 4 events: CompoundStart, 2 SetValue, CompoundEnd
        assert_eq!(ctx.change_count(), 4);

        // Rollback should handle compound operations
        ctx.rollback().unwrap();
        assert_eq!(ctx.change_count(), 0);
    }
}
