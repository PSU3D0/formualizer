// TODO: These tests need to be updated to use the new TransactionContext
// architecture once Phase 2 and 3 are implemented. For now, they are
// disabled as the transaction methods have been removed from VertexEditor.
#![cfg(skip_until_transaction_context_impl)]

use crate::SheetId;
use crate::engine::graph::DependencyGraph;
use crate::engine::vertex_editor::VertexEditor;
use crate::reference::{CellRef, Coord};
use formualizer_common::LiteralValue;
use formualizer_parse::parse;

fn create_test_graph() -> DependencyGraph {
    DependencyGraph::new()
}

fn cell_ref(sheet_id: SheetId, row: u32, col: u32) -> CellRef {
    CellRef::new(sheet_id, Coord::new(row, col, false, false))
}

#[test]
fn test_transaction_rollback_single_value() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Initial state
    editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(100.0));

    // Begin transaction
    let tx_id = editor.begin_transaction();

    // Make changes
    editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(200.0));

    // Rollback
    editor.rollback_transaction().unwrap();

    // Verify original state restored
    drop(editor);
    let value = graph.get_cell_value("Sheet1", 1, 1);
    assert_eq!(value, Some(LiteralValue::Number(100.0)));
}

#[test]
fn test_transaction_rollback_multiple_operations() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Initial state
    editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(10.0));
    editor.set_cell_value(cell_ref(0, 2, 1), LiteralValue::Number(20.0));

    // Begin transaction
    editor.begin_transaction();

    // Make multiple changes
    editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(30.0));
    editor.set_cell_value(cell_ref(0, 2, 1), LiteralValue::Number(40.0));
    editor.set_cell_value(cell_ref(0, 3, 1), LiteralValue::Number(50.0)); // New cell

    // Rollback
    editor.rollback_transaction().unwrap();

    // Verify original state restored
    drop(editor);
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        graph.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert_eq!(graph.get_cell_value("Sheet1", 3, 1), None); // Should be removed
}

#[test]
fn test_transaction_commit() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    editor.begin_transaction();

    // Make changes
    editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(10.0));
    editor.set_cell_formula(cell_ref(0, 2, 1), parse("=A1*2").unwrap());

    // Commit
    editor.commit_transaction().unwrap();

    // Changes persisted
    drop(editor);
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    // Formula should exist (we'll check it was created)
    let formula_vertex = graph.get_vertex_id_for_address(&cell_ref(0, 2, 1));
    assert!(formula_vertex.is_some());
}

#[test]
fn test_transaction_rollback_formula_changes() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Initial state with formula
    editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(5.0));
    editor.set_cell_formula(cell_ref(0, 2, 1), parse("=A1*2").unwrap());

    editor.begin_transaction();

    // Change formula
    editor.set_cell_formula(cell_ref(0, 2, 1), parse("=A1*3").unwrap());

    // Rollback
    editor.rollback_transaction().unwrap();

    // Verify original formula restored (checking vertex exists with formula)
    drop(editor);
    let formula_vertex = graph.get_vertex_id_for_address(&cell_ref(0, 2, 1));
    assert!(formula_vertex.is_some());
    // Check that the formula exists (not just a value)
    let formula = graph.get_formula(*formula_vertex.unwrap());
    assert!(formula.is_some());
}

#[test]
fn test_transaction_rollback_row_operations() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Setup: A1=10, A2=20, A3=30
    editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(10.0));
    editor.set_cell_value(cell_ref(0, 2, 1), LiteralValue::Number(20.0));
    editor.set_cell_value(cell_ref(0, 3, 1), LiteralValue::Number(30.0));

    editor.begin_transaction();

    // Insert 2 rows before row 2
    editor.insert_rows(0, 2, 2).unwrap();

    // After insert: A1=10, A2=empty, A3=empty, A4=20, A5=30
    // Verify the shift happened
    drop(editor);
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        graph.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert_eq!(
        graph.get_cell_value("Sheet1", 5, 1),
        Some(LiteralValue::Number(30.0))
    );

    // Now rollback
    let mut editor = VertexEditor::new(&mut graph);
    editor.rollback_transaction().unwrap();

    // Verify original positions restored
    drop(editor);
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        graph.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert_eq!(
        graph.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(30.0))
    );
    assert_eq!(graph.get_cell_value("Sheet1", 4, 1), None);
    assert_eq!(graph.get_cell_value("Sheet1", 5, 1), None);
}

#[test]
fn test_no_active_transaction_error() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Try to rollback without active transaction
    let result = editor.rollback_transaction();
    assert!(result.is_err());

    // Try to commit without active transaction
    let result = editor.commit_transaction();
    assert!(result.is_err());
}

#[test]
fn test_transaction_nested_not_supported() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Begin first transaction
    let tx1 = editor.begin_transaction();

    // Try to begin second transaction should replace the first
    let tx2 = editor.begin_transaction();

    // They should be different IDs
    assert_ne!(tx1, tx2);

    // Only one transaction should be active
    editor.commit_transaction().unwrap();

    // Second commit should fail
    let result = editor.commit_transaction();
    assert!(result.is_err());
}

#[test]
fn test_transaction_with_vertex_removal() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Create vertices
    let id1 = editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(10.0));
    let _id2 = editor.set_cell_formula(cell_ref(0, 2, 1), parse("=A1*2").unwrap());

    editor.begin_transaction();

    // Remove a vertex
    editor.remove_vertex(id1).unwrap();

    // Verify it's gone
    drop(editor);
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), None);

    // Rollback
    let mut editor = VertexEditor::new(&mut graph);
    editor.rollback_transaction().unwrap();

    // Verify it's restored
    drop(editor);
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn test_transaction_rollback_preserves_dependencies() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Setup: A1=10, B1=A1*2
    editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(10.0));
    let _b1_id = editor.set_cell_formula(cell_ref(0, 1, 2), parse("=A1*2").unwrap());

    // Verify dependency exists
    drop(editor);
    let a1_id = *graph.get_vertex_id_for_address(&cell_ref(0, 1, 1)).unwrap();
    let deps = graph.get_dependencies(a1_id);
    assert!(deps.is_empty()); // A1 has no dependencies
    let dependents = graph.get_dependents(a1_id);
    assert_eq!(dependents.len(), 1); // B1 depends on A1

    let mut editor = VertexEditor::new(&mut graph);
    editor.begin_transaction();

    // Change A1 value
    editor.set_cell_value(cell_ref(0, 1, 1), LiteralValue::Number(20.0));

    // Rollback
    editor.rollback_transaction().unwrap();

    // Verify dependencies still intact
    drop(editor);
    let deps_after = graph.get_dependents(a1_id);
    assert_eq!(deps_after.len(), 1);
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
}
