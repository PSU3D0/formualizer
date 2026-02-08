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
fn test_set_range_values() {
    let mut graph = super::common::graph_truth_graph();

    // Excel uses 1-based indexing
    let values = vec![
        vec![lit_num(1.0), lit_num(2.0), lit_num(3.0)],
        vec![lit_num(4.0), lit_num(5.0), lit_num(6.0)],
        vec![lit_num(7.0), lit_num(8.0), lit_num(9.0)],
    ];

    let mut editor = VertexEditor::new(&mut graph);

    // Set a 3x3 range starting at A1. Editor uses 0-based coords.
    let summary = editor.set_range_values(0, 0, 0, &values).unwrap();

    drop(editor);

    assert_eq!(summary.cells_affected, 9);
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 1, 1))
        .is_some());
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 1, 2))
        .is_some());
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 2, 1))
        .is_some());
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 3, 3))
        .is_some());
}

#[test]
fn test_clear_range() {
    let mut graph = super::common::graph_truth_graph();

    // Populate a 3x3 range
    for row in 1..=3 {
        for col in 1..=3 {
            graph
                .set_cell_value("Sheet1", row, col, lit_num((row * 10 + col) as f64))
                .unwrap();
        }
    }

    let mut editor = VertexEditor::new(&mut graph);

    // Clear the range (A1:C3). Editor uses 0-based coords.
    let summary = editor.clear_range(0, 0, 0, 2, 2).unwrap();

    drop(editor);

    assert_eq!(summary.cells_affected, 9);
    for row in 1..=3 {
        for col in 1..=3 {
            assert!(graph
                .get_vertex_id_for_address(&sheet1_cell(&graph, row, col))
                .is_none());
        }
    }
}

#[test]
fn test_copy_range() {
    let mut graph = super::common::graph_truth_graph();

    // Source: A1:B2 with values and formulas
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 1, 2, lit_num(20.0)).unwrap();
    graph
        .set_cell_formula("Sheet1", 2, 1, parse("=A1*2").unwrap())
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 2, 2, parse("=B1+A1").unwrap())
        .unwrap();

    let mut editor = VertexEditor::new(&mut graph);

    // Copy A1:B2 to D4. Editor uses 0-based coords.
    let summary = editor.copy_range(0, 0, 0, 1, 1, 0, 3, 3).unwrap();

    drop(editor);

    // In Arrow-truth mode, VertexEditor does not have access to grid values.
    // copy_range therefore only copies formulas/structure.
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 5, 4))
        .is_some());
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 5, 5))
        .is_some());

    // Check if formulas exist at new location (they should be adjusted)
    let d5_value = graph.get_vertex_id_for_address(&graph.make_cell_ref("Sheet1", 5, 4));
    assert!(d5_value.is_some());

    let e5_value = graph.get_vertex_id_for_address(&graph.make_cell_ref("Sheet1", 5, 5));
    assert!(e5_value.is_some());

    assert_eq!(summary.cells_affected, 2);
}

#[test]
fn test_set_range_values_partial_overlap() {
    let mut graph = super::common::graph_truth_graph();

    // Set initial values
    graph
        .set_cell_value("Sheet1", 1, 1, lit_num(100.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 2, 2, lit_num(200.0))
        .unwrap();

    let mut editor = VertexEditor::new(&mut graph);

    let values = vec![
        vec![lit_num(1.0), lit_num(2.0)],
        vec![lit_num(3.0), lit_num(4.0)],
    ];

    // Set A1:B2, should overwrite existing values. Editor uses 0-based coords.
    let summary = editor.set_range_values(0, 0, 0, &values).unwrap();

    drop(editor);

    assert_eq!(summary.cells_affected, 4);
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 1, 1))
        .is_some());
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 2, 2))
        .is_some());
}

#[test]
fn test_copy_range_with_absolute_references() {
    let mut graph = super::common::graph_truth_graph();

    // Setup source with absolute and relative references
    graph
        .set_cell_value("Sheet1", 1, 1, lit_num(100.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 5, 5, lit_num(500.0))
        .unwrap();

    // Formula with mixed references: =$A$1+E5
    graph
        .set_cell_formula("Sheet1", 2, 2, parse("=$A$1+E5").unwrap())
        .unwrap();

    let mut editor = VertexEditor::new(&mut graph);

    // Copy B2 to D4. Editor uses 0-based coords.
    let summary = editor.copy_range(0, 1, 1, 1, 1, 0, 3, 3).unwrap();

    drop(editor);

    // The copied formula at D4 should still reference $A$1 (absolute)
    // but E5 should be adjusted relatively to G7
    let d4_vertex = graph.get_vertex_id_for_address(&graph.make_cell_ref("Sheet1", 4, 4));
    assert!(d4_vertex.is_some());

    assert_eq!(summary.cells_affected, 1);
}

#[test]
fn test_clear_range_with_formulas() {
    let mut graph = super::common::graph_truth_graph();

    // Setup cells with formulas that reference each other
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 3, parse("=B1+5").unwrap())
        .unwrap();

    // D1 references C1 which is in the range to be cleared
    let d1_result = graph
        .set_cell_formula("Sheet1", 1, 4, parse("=C1").unwrap())
        .unwrap();
    let d1_id = d1_result.affected_vertices[0];

    let mut editor = VertexEditor::new(&mut graph);

    // Clear A1:C1. Editor uses 0-based coords.
    let summary = editor.clear_range(0, 0, 0, 0, 2).unwrap();

    drop(editor);

    assert_eq!(summary.cells_affected, 3);

    // D1 should now have #REF! error since C1 was deleted
    assert!(graph.is_ref_error(d1_id));
}

#[test]
fn test_move_range() {
    let mut graph = super::common::graph_truth_graph();

    // Setup source range A1:B2
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 1, 2, lit_num(20.0)).unwrap();
    graph
        .set_cell_formula("Sheet1", 2, 1, parse("=A1*2").unwrap())
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 2, 2, parse("=B1+A1").unwrap())
        .unwrap();

    // C3 references A1 (which will be moved)
    let c3_result = graph
        .set_cell_formula("Sheet1", 3, 3, parse("=A1+10").unwrap())
        .unwrap();
    let c3_id = c3_result.affected_vertices[0];

    let mut editor = VertexEditor::new(&mut graph);

    // Move A1:B2 to D4. Editor uses 0-based coords.
    let summary = editor.move_range(0, 0, 0, 1, 1, 0, 3, 3).unwrap();

    drop(editor);

    // In Arrow-truth mode, VertexEditor does not move grid values; it only moves formulas.
    // Formulas from row 2 should now exist at row 5.
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 5, 4))
        .is_some());
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 5, 5))
        .is_some());

    // C3 formula should be updated to reference D4 instead of A1
    let c3_formula = graph.get_formula(c3_id);
    assert!(c3_formula.is_some());
    // The formula should now reference D4

    assert_eq!(summary.cells_affected, 2);
    // move_range still clears the source addresses, so this counts all moved/cleared cells.
    assert_eq!(summary.cells_moved, 4);
}

#[test]
fn test_set_range_values_large() {
    let mut graph = super::common::graph_truth_graph();

    // Create a 100x100 range
    let mut values = Vec::new();
    for row in 0..100 {
        let mut row_values = Vec::new();
        for col in 0..100 {
            row_values.push(lit_num((row * 100 + col) as f64));
        }
        values.push(row_values);
    }

    let mut editor = VertexEditor::new(&mut graph);

    let summary = editor.set_range_values(0, 0, 0, &values).unwrap();

    drop(editor);

    assert_eq!(summary.cells_affected, 10000);

    // Spot check some vertices exist
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 1, 1))
        .is_some());
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 50, 50))
        .is_some());
    assert!(graph
        .get_vertex_id_for_address(&sheet1_cell(&graph, 100, 100))
        .is_some());
}
