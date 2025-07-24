//! Tests for the hybrid model of range dependency management.
use crate::engine::{DependencyGraph, VertexId};
use formualizer_common::LiteralValue;
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};

/// Helper to create a range reference AST node
fn range_ast(start_row: u32, start_col: u32, end_row: u32, end_col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{}C{}:R{}C{}", start_row, start_col, end_row, end_col),
            reference: ReferenceType::Range {
                sheet: None,
                start_row: Some(start_row),
                start_col: Some(start_col),
                end_row: Some(end_row),
                end_col: Some(end_col),
            },
        },
        source_token: None,
    }
}

#[test]
fn test_range_dependency_dirtiness() {
    let mut graph = DependencyGraph::new();

    // C1 depends on the range A1:A10.
    // We don't need a real SUM function, just the dependency.
    graph
        .set_cell_formula("Sheet1", 1, 3, range_ast(1, 1, 10, 1))
        .unwrap();
    let c1_id = *graph
        .cell_to_vertex()
        .get(&crate::engine::CellAddr::new("Sheet1".to_string(), 1, 3))
        .unwrap();

    // Create a value in the middle of the range, e.g., A5.
    // This will also create placeholder vertices for A1-A4, A6-A10 if they don't exist.
    graph
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(100))
        .unwrap();

    // Clear all dirty flags from the initial setup.
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);
    assert!(graph.get_evaluation_vertices().is_empty());

    // Now, change the value of A5. This should trigger dirty propagation
    // to C1 via the range dependency.
    graph
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(200))
        .unwrap();

    // Check that C1 is now dirty.
    let eval_vertices = graph.get_evaluation_vertices();
    assert!(!eval_vertices.is_empty());
    assert!(eval_vertices.contains(&c1_id));
}

#[test]
fn test_range_dependency_updates_on_formula_change() {
    let mut graph = DependencyGraph::new();

    // B1 = SUM(A1:A2)
    graph
        .set_cell_formula("Sheet1", 1, 2, range_ast(1, 1, 2, 1))
        .unwrap();
    let b1_id = *graph
        .cell_to_vertex()
        .get(&crate::engine::CellAddr::new("Sheet1".to_string(), 1, 2))
        .unwrap();

    // Change A1, B1 should be dirty
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    assert!(graph.get_evaluation_vertices().contains(&b1_id));
    graph.clear_dirty_flags(&[b1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Change A3 (outside the range), B1 should NOT be dirty
    graph
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(30))
        .unwrap();
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Now, update B1 to depend on A1:A5
    graph
        .set_cell_formula("Sheet1", 1, 2, range_ast(1, 1, 5, 1))
        .unwrap();
    graph.clear_dirty_flags(&[b1_id]);

    // Change A3 again (now inside the range), B1 should be dirty
    graph
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(40))
        .unwrap();
    assert!(graph.get_evaluation_vertices().contains(&b1_id));
}
