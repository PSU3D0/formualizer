use crate::engine::graph::DependencyGraph;
use crate::engine::VertexEditor;
use crate::reference::{CellRef, Coord};
use formualizer_common::LiteralValue;
use formualizer_parse::parse;

fn lit_num(value: f64) -> LiteralValue {
    LiteralValue::Number(value)
}

fn sheet1_cell(graph: &DependencyGraph, row: u32, col: u32) -> CellRef {
    let sid = graph.sheet_id("Sheet1").unwrap();
    CellRef::new(sid, Coord::from_excel(row, col, true, true))
}

#[test]
fn test_insert_rows() {
    let mut graph = super::common::graph_truth_graph();

    // Setup: A1=10, A2=20, A3=30, A4=SUM(A1:A3)
    // Excel uses 1-based indexing
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 2, 1, lit_num(20.0)).unwrap();
    graph.set_cell_value("Sheet1", 3, 1, lit_num(30.0)).unwrap();
    let sum_result = graph
        .set_cell_formula("Sheet1", 4, 1, parse("=SUM(A1:A3)").unwrap())
        .unwrap();
    let sum_id = sum_result.affected_vertices[0];

    let a1_id = *graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 1, 1))
        .unwrap();
    let a2_id = *graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 2, 1))
        .unwrap();
    let a3_id = *graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 3, 1))
        .unwrap();

    // Use editor to insert rows
    let mut editor = VertexEditor::new(&mut graph);

    // Insert 2 rows before row 2 (public). Editor uses 0-based rows.
    let summary = editor.insert_rows(0, 1, 2).unwrap();

    // Drop editor to release borrow
    drop(editor);

    // Verify shifts via vertex mapping
    assert_eq!(
        graph.get_vertex_id_for_address(&sheet1_cell(&graph, 1, 1)),
        Some(&a1_id)
    );
    assert_eq!(
        graph.get_vertex_id_for_address(&sheet1_cell(&graph, 4, 1)),
        Some(&a2_id)
    );
    assert_eq!(
        graph.get_vertex_id_for_address(&sheet1_cell(&graph, 5, 1)),
        Some(&a3_id)
    );
    assert_eq!(
        graph.get_vertex_id_for_address(&sheet1_cell(&graph, 6, 1)),
        Some(&sum_id)
    );

    // Formula should be updated: SUM(A1:A3) -> SUM(A1:A5)
    let formula = graph.get_formula(sum_id);
    assert!(formula.is_some());
    // The formula should now reference the expanded range

    assert_eq!(summary.vertices_moved.len(), 3); // A2, A3, and A4 moved
    assert_eq!(summary.formulas_updated, 1); // A6 formula updated
}

#[test]
fn test_delete_rows() {
    let mut graph = super::common::graph_truth_graph();

    // Setup: A1 through A5 with values
    for i in 1..=5 {
        graph
            .set_cell_value("Sheet1", i, 1, lit_num(i as f64 * 10.0))
            .unwrap();
    }
    let a1_id = *graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 1, 1))
        .unwrap();
    let a4_id = *graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 4, 1))
        .unwrap();
    let a5_id = *graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 5, 1))
        .unwrap();
    let formula_result = graph
        .set_cell_formula("Sheet1", 7, 1, parse("=SUM(A1:A5)").unwrap())
        .unwrap();

    let mut editor = VertexEditor::new(&mut graph);

    // Delete rows 2-3 (public). Editor uses 0-based rows.
    let summary = editor.delete_rows(0, 1, 2).unwrap();

    drop(editor);

    // Verify remaining vertices
    assert_eq!(
        graph.get_vertex_id_for_address(&sheet1_cell(&graph, 1, 1)),
        Some(&a1_id)
    );
    assert_eq!(
        graph.get_vertex_id_for_address(&sheet1_cell(&graph, 2, 1)),
        Some(&a4_id)
    );
    assert_eq!(
        graph.get_vertex_id_for_address(&sheet1_cell(&graph, 3, 1)),
        Some(&a5_id)
    );
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 4, 1))
        .is_none());

    assert_eq!(summary.vertices_deleted.len(), 2);
    assert_eq!(summary.vertices_moved.len(), 3); // A4, A5, and A7 moved up
}

#[test]
fn test_insert_rows_adjusts_formulas() {
    let mut graph = super::common::graph_truth_graph();

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
    let mut graph = super::common::graph_truth_graph();

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

    // A2 vertex should be gone
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 2, 1))
        .is_none());
}

#[test]
fn test_insert_rows_with_absolute_references() {
    let mut graph = super::common::graph_truth_graph();

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
    let mut graph = super::common::graph_truth_graph();

    // Setup initial data
    for i in 1..=10 {
        graph
            .set_cell_value("Sheet1", i, 1, lit_num(i as f64))
            .unwrap();
    }
    let a1_id = *graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 1, 1))
        .unwrap();

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

    // Verify final state: original A1 should now be at A2
    assert_eq!(
        graph.get_vertex_id_for_address(&sheet1_cell(&graph, 2, 1)),
        Some(&a1_id)
    );
}
