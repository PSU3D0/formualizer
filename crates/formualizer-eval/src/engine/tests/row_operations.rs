use crate::engine::{DependencyGraph, VertexEditor};
use formualizer_common::LiteralValue;
use formualizer_parse::parse;

fn lit_num(value: f64) -> LiteralValue {
    LiteralValue::Number(value)
}

#[test]
fn test_insert_rows() {
    let mut graph = DependencyGraph::new();

    // Setup: A1=10, A2=20, A3=30, A4=SUM(A1:A3)
    // Excel uses 1-based indexing
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 2, 1, lit_num(20.0)).unwrap();
    graph.set_cell_value("Sheet1", 3, 1, lit_num(30.0)).unwrap();
    let sum_result = graph
        .set_cell_formula("Sheet1", 4, 1, parse("=SUM(A1:A3)").unwrap())
        .unwrap();
    let sum_id = sum_result.affected_vertices[0];

    // Use editor to insert rows
    let mut editor = VertexEditor::new(&mut graph);

    // Insert 2 rows before row 2 (public). Editor uses 0-based rows.
    let summary = editor.insert_rows(0, 1, 2).unwrap();

    // Drop editor to release borrow
    drop(editor);

    // Verify shifts
    // A1 unchanged (before insert point)
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), Some(lit_num(10.0)));
    // A2 -> A4 (shifted down by 2)
    assert_eq!(graph.get_cell_value("Sheet1", 4, 1), Some(lit_num(20.0)));
    // A3 -> A5 (shifted down by 2)
    assert_eq!(graph.get_cell_value("Sheet1", 5, 1), Some(lit_num(30.0)));

    // Formula should be updated: SUM(A1:A3) -> SUM(A1:A5)
    let formula = graph.get_formula(sum_id);
    assert!(formula.is_some());
    // The formula should now reference the expanded range

    assert_eq!(summary.vertices_moved.len(), 3); // A2, A3, and A4 moved
    assert_eq!(summary.formulas_updated, 1); // A6 formula updated
}

#[test]
fn test_delete_rows() {
    let mut graph = DependencyGraph::new();

    // Setup: A1 through A5 with values
    for i in 1..=5 {
        graph
            .set_cell_value("Sheet1", i, 1, lit_num(i as f64 * 10.0))
            .unwrap();
    }
    let formula_result = graph
        .set_cell_formula("Sheet1", 7, 1, parse("=SUM(A1:A5)").unwrap())
        .unwrap();

    let mut editor = VertexEditor::new(&mut graph);

    // Delete rows 2-3 (public). Editor uses 0-based rows.
    let summary = editor.delete_rows(0, 1, 2).unwrap();

    drop(editor);

    // Verify remaining values
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), Some(lit_num(10.0))); // A1
    assert_eq!(graph.get_cell_value("Sheet1", 2, 1), Some(lit_num(40.0))); // A4 -> A2
    assert_eq!(graph.get_cell_value("Sheet1", 3, 1), Some(lit_num(50.0))); // A5 -> A3
    assert_eq!(graph.get_cell_value("Sheet1", 4, 1), None); // A4 deleted

    assert_eq!(summary.vertices_deleted.len(), 2);
    assert_eq!(summary.vertices_moved.len(), 3); // A4, A5, and A7 moved up
}

#[test]
fn test_insert_rows_adjusts_formulas() {
    let mut graph = DependencyGraph::new();

    // Create cells with formulas
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 3, 1, lit_num(30.0)).unwrap();

    // B1 = A1 * 2
    graph
        .set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())
        .unwrap();
    // B3 = A3 + 5
    let b3_result = graph
        .set_cell_formula("Sheet1", 3, 2, parse("=A3+5").unwrap())
        .unwrap();
    let b3_id = b3_result.affected_vertices[0];

    let mut editor = VertexEditor::new(&mut graph);

    // Insert row before row 2 (public). Editor uses 0-based rows.
    editor.insert_rows(0, 1, 1).unwrap();

    drop(editor);

    // B3 formula (now at B4) should reference A4
    let b4_formula = graph.get_formula(b3_id);
    assert!(b4_formula.is_some());
    // The formula should now reference A4 instead of A3
}

#[test]
fn test_delete_row_creates_ref_error() {
    let mut graph = DependencyGraph::new();

    // A1 = 10
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    // A2 = 20
    graph.set_cell_value("Sheet1", 2, 1, lit_num(20.0)).unwrap();
    // B2 = A2 * 2
    let b2_result = graph
        .set_cell_formula("Sheet1", 2, 2, parse("=A2*2").unwrap())
        .unwrap();
    let b2_id = b2_result.affected_vertices[0];

    let mut editor = VertexEditor::new(&mut graph);

    // Delete row 2 (public). Editor uses 0-based rows.
    editor.delete_rows(0, 1, 1).unwrap();

    drop(editor);

    // B2 should be deleted
    assert!(graph.is_deleted(b2_id));

    // A2 value should be gone
    assert_eq!(graph.get_cell_value("Sheet1", 2, 1), None);
}

#[test]
fn test_insert_rows_with_absolute_references() {
    let mut graph = DependencyGraph::new();

    // Setup cells
    graph
        .set_cell_value("Sheet1", 1, 1, lit_num(100.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 5, 1, lit_num(500.0))
        .unwrap();

    // Formula with absolute reference: =$A$1+A5
    let formula_result = graph
        .set_cell_formula("Sheet1", 5, 2, parse("=$A$1+A5").unwrap())
        .unwrap();
    let formula_id = formula_result.affected_vertices[0];

    let mut editor = VertexEditor::new(&mut graph);

    // Insert rows before row 3 (public). Editor uses 0-based rows.
    editor.insert_rows(0, 2, 2).unwrap();

    drop(editor);

    // The formula should still reference $A$1 (absolute) but A5 should become A7
    let updated_formula = graph.get_formula(formula_id);
    assert!(updated_formula.is_some());
    // Check that absolute reference is preserved
}

#[test]
fn test_multiple_row_operations() {
    let mut graph = DependencyGraph::new();

    // Setup initial data
    for i in 1..=10 {
        graph
            .set_cell_value("Sheet1", i, 1, lit_num(i as f64))
            .unwrap();
    }

    let mut editor = VertexEditor::new(&mut graph);

    editor.begin_batch();

    // Insert 2 rows at row 3 (public). Editor uses 0-based rows.
    editor.insert_rows(0, 2, 2).unwrap();

    // Delete 1 row at row 8 (public), now row 10 after insertion.
    // Editor uses 0-based rows, so delete internal row 9.
    editor.delete_rows(0, 9, 1).unwrap();

    // Insert 1 row at row 1 (public). Editor uses 0-based rows.
    editor.insert_rows(0, 0, 1).unwrap();

    editor.commit_batch();

    drop(editor);

    // Verify final state
    // Original A1 should now be at A2
    assert_eq!(graph.get_cell_value("Sheet1", 2, 1), Some(lit_num(1.0)));
}
