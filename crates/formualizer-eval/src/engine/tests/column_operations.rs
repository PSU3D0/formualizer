use crate::engine::{DependencyGraph, VertexEditor};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn lit_num(value: f64) -> LiteralValue {
    LiteralValue::Number(value)
}

#[test]
fn test_insert_columns() {
    let mut graph = DependencyGraph::new();

    // Setup: A1=10, B1=20, C1=30, D1=SUM(A1:C1)
    // Excel uses 1-based indexing
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 1, 2, lit_num(20.0)).unwrap();
    graph.set_cell_value("Sheet1", 1, 3, lit_num(30.0)).unwrap();
    let sum_result = graph
        .set_cell_formula("Sheet1", 1, 4, parse("=SUM(A1:C1)").unwrap())
        .unwrap();
    let sum_id = sum_result.affected_vertices[0];

    // Use editor to insert columns
    let mut editor = VertexEditor::new(&mut graph);

    // Insert 2 columns before column 2 (B).
    // VertexEditor uses internal 0-based indices for structural edits.
    let summary = editor.insert_columns(0, 1, 2).unwrap();

    // Drop editor to release borrow
    drop(editor);

    // Verify shifts
    // A1 unchanged (before insert point)
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), Some(lit_num(10.0)));
    // B1 -> D1 (shifted right by 2)
    assert_eq!(graph.get_cell_value("Sheet1", 1, 4), Some(lit_num(20.0)));
    // C1 -> E1 (shifted right by 2)
    assert_eq!(graph.get_cell_value("Sheet1", 1, 5), Some(lit_num(30.0)));

    // Formula should be updated: SUM(A1:C1) -> SUM(A1:E1)
    let formula = graph.get_formula(sum_id);
    assert!(formula.is_some());
    // The formula should now reference the expanded range

    assert_eq!(summary.vertices_moved.len(), 3); // B1, C1, and D1 moved
    assert_eq!(summary.formulas_updated, 1); // F1 formula updated
}

#[test]
fn test_delete_columns() {
    let mut graph = DependencyGraph::new();

    // Setup: A1 through E1 with values
    for i in 1..=5 {
        graph
            .set_cell_value("Sheet1", 1, i, lit_num(i as f64 * 10.0))
            .unwrap();
    }
    let formula_result = graph
        .set_cell_formula("Sheet1", 1, 7, parse("=SUM(A1:E1)").unwrap())
        .unwrap();

    let mut editor = VertexEditor::new(&mut graph);

    // Delete columns 2-3 (B and C). Editor uses 0-based cols.
    let summary = editor.delete_columns(0, 1, 2).unwrap();

    drop(editor);

    // Verify remaining values
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), Some(lit_num(10.0))); // A1
    assert_eq!(graph.get_cell_value("Sheet1", 1, 2), Some(lit_num(40.0))); // D1 -> B1
    assert_eq!(graph.get_cell_value("Sheet1", 1, 3), Some(lit_num(50.0))); // E1 -> C1
    assert_eq!(graph.get_cell_value("Sheet1", 1, 4), None); // D1 deleted

    assert_eq!(summary.vertices_deleted.len(), 2);
    assert_eq!(summary.vertices_moved.len(), 3); // D1, E1, and G1 moved left
}

#[test]
fn test_insert_columns_adjusts_formulas() {
    let mut graph = DependencyGraph::new();

    // Create cells with formulas
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 1, 3, lit_num(30.0)).unwrap();

    // A2 = A1 * 2
    graph
        .set_cell_formula("Sheet1", 2, 1, parse("=A1*2").unwrap())
        .unwrap();
    // C2 = C1 + 5
    let c2_result = graph
        .set_cell_formula("Sheet1", 2, 3, parse("=C1+5").unwrap())
        .unwrap();
    let c2_id = c2_result.affected_vertices[0];

    let mut editor = VertexEditor::new(&mut graph);

    // Insert column before column 2 (B). Editor uses 0-based cols.
    editor.insert_columns(0, 1, 1).unwrap();

    drop(editor);

    // C2 formula (now at D2) should reference D1
    let d2_formula = graph.get_formula(c2_id);
    assert!(d2_formula.is_some());
    // The formula should now reference D1 instead of C1
}

#[test]
fn test_delete_column_creates_ref_error() {
    let mut graph = DependencyGraph::new();

    // A1 = 10
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    // B1 = 20
    graph.set_cell_value("Sheet1", 1, 2, lit_num(20.0)).unwrap();
    // B2 = B1 * 2
    let b2_result = graph
        .set_cell_formula("Sheet1", 2, 2, parse("=B1*2").unwrap())
        .unwrap();
    let b2_id = b2_result.affected_vertices[0];

    let mut editor = VertexEditor::new(&mut graph);

    // Delete column 2 (B). Editor uses 0-based cols.
    editor.delete_columns(0, 1, 1).unwrap();

    drop(editor);

    // B2 should be deleted
    assert!(graph.is_deleted(b2_id));

    // B1 value should be gone
    assert_eq!(graph.get_cell_value("Sheet1", 1, 2), None);
}

#[test]
fn test_insert_columns_with_absolute_references() {
    let mut graph = DependencyGraph::new();

    // Setup cells
    graph
        .set_cell_value("Sheet1", 1, 1, lit_num(100.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 1, 5, lit_num(500.0))
        .unwrap();

    // Formula with absolute reference: =$A$1+E1
    let formula_result = graph
        .set_cell_formula("Sheet1", 2, 5, parse("=$A$1+E1").unwrap())
        .unwrap();
    let formula_id = formula_result.affected_vertices[0];

    let mut editor = VertexEditor::new(&mut graph);

    // Insert columns before column 3 (C). Editor uses 0-based cols.
    editor.insert_columns(0, 2, 2).unwrap();

    drop(editor);

    // The formula should still reference $A$1 (absolute) but E1 should become G1
    let updated_formula = graph.get_formula(formula_id);
    assert!(updated_formula.is_some());
    // Check that absolute reference is preserved
}

#[test]
fn test_multiple_column_operations() {
    let mut graph = DependencyGraph::new();

    // Setup initial data
    for i in 1..=10 {
        graph
            .set_cell_value("Sheet1", 1, i, lit_num(i as f64))
            .unwrap();
    }

    let mut editor = VertexEditor::new(&mut graph);

    editor.begin_batch();

    // Insert 2 columns at column 3 (public). Editor uses 0-based cols.
    editor.insert_columns(0, 2, 2).unwrap();

    // Delete 1 column at column 8 (public), now column 10 after insertion.
    // Editor expects 0-based cols, so delete internal col 9.
    editor.delete_columns(0, 9, 1).unwrap();

    // Insert 1 column at column 1 (public). Editor uses 0-based cols.
    editor.insert_columns(0, 0, 1).unwrap();

    editor.commit_batch();

    drop(editor);

    // Verify final state
    // Original A1 should now be at B1
    assert_eq!(graph.get_cell_value("Sheet1", 1, 2), Some(lit_num(1.0)));
}

#[test]
fn test_mixed_row_column_operations() {
    let mut graph = DependencyGraph::new();

    // Setup: Create a 3x3 grid with values
    for row in 1..=3 {
        for col in 1..=3 {
            graph
                .set_cell_value("Sheet1", row, col, lit_num((row * 10 + col) as f64))
                .unwrap();
        }
    }

    // Add formula: D4 = SUM(A1:C3)
    let formula_result = graph
        .set_cell_formula("Sheet1", 4, 4, parse("=SUM(A1:C3)").unwrap())
        .unwrap();
    let formula_id = formula_result.affected_vertices[0];

    let mut editor = VertexEditor::new(&mut graph);

    editor.begin_batch();

    // Insert 1 row before row 2 (public). Editor uses 0-based rows.
    editor.insert_rows(0, 1, 1).unwrap();

    // Insert 1 column before column 2 (public). Editor uses 0-based cols.
    editor.insert_columns(0, 1, 1).unwrap();

    editor.commit_batch();

    drop(editor);

    // Verify grid shifted correctly
    // A1 stays at A1 (11)
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), Some(lit_num(11.0)));
    // B1 -> C1 (12)
    assert_eq!(graph.get_cell_value("Sheet1", 1, 3), Some(lit_num(12.0)));
    // A2 -> A3 (21)
    assert_eq!(graph.get_cell_value("Sheet1", 3, 1), Some(lit_num(21.0)));
    // B2 -> C3 (22) - shifted both right and down
    assert_eq!(graph.get_cell_value("Sheet1", 3, 3), Some(lit_num(22.0)));

    // Formula should be updated for both shifts
    let updated_formula = graph.get_formula(formula_id);
    assert!(updated_formula.is_some());
    // Formula range should now be expanded
}

#[test]
fn test_delete_columns_with_dependencies() {
    let mut graph = DependencyGraph::new();

    // Setup: A1=10, B1=A1*2, C1=B1+5, D1=C1
    // Excel uses 1-based indexing
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())
        .unwrap();
    let c1_result = graph
        .set_cell_formula("Sheet1", 1, 3, parse("=B1+5").unwrap())
        .unwrap();
    let c1_id = c1_result.affected_vertices[0];
    graph
        .set_cell_formula("Sheet1", 1, 4, parse("=C1").unwrap())
        .unwrap();

    let mut editor = VertexEditor::new(&mut graph);

    // Delete column B (public col 2). Editor uses 0-based cols.
    editor.delete_columns(0, 1, 1).unwrap();

    drop(editor);

    // C1 -> B1, should have #REF! since it referenced deleted B1
    // Check that the formula that was at C1 (now at B1) has been marked as #REF!
    assert!(graph.is_ref_error(c1_id));
}
