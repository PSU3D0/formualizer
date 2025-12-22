//! Tests for the hybrid model of range dependency management.
use super::common::{abs_cell_ref, eval_config_with_range_limit};
use crate::engine::{DependencyGraph, VertexId};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

/// Helper to create a range reference AST node
fn range_ast(start_row: u32, start_col: u32, end_row: u32, end_col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{start_row}C{start_col}:R{end_row}C{end_col}"),
            reference: ReferenceType::range(
                None,
                Some(start_row),
                Some(start_col),
                Some(end_row),
                Some(end_col),
            ),
        },
        source_token: None,
        contains_volatile: false,
    }
}

/// Helper to create a SUM(range) AST node
fn sum_ast(start_row: u32, start_col: u32, end_row: u32, end_col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Function {
            name: "SUM".to_string(),
            args: vec![range_ast(start_row, start_col, end_row, end_col)],
        },
        source_token: None,
        contains_volatile: false,
    }
}

fn graph_with_range_limit(limit: usize) -> DependencyGraph {
    DependencyGraph::new_with_config(eval_config_with_range_limit(limit))
}

#[test]
fn test_tiny_range_expands_to_cell_dependencies() {
    let mut graph = graph_with_range_limit(4);

    // C1 = SUM(A1:A4) - size is 4, which is <= limit
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 4, 1))
        .unwrap();

    let c1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 3))
        .unwrap();
    let c1_vertex = graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 3))
        .unwrap();

    let dependencies = graph.get_dependencies(c1_id);

    // Should have 4 direct dependencies
    assert_eq!(
        dependencies.len(),
        4,
        "Should expand to 4 cell dependencies"
    );

    // Should have no compressed range dependencies
    assert!(
        graph.formula_to_range_deps().is_empty(),
        "Should not create a compressed range dependency"
    );

    // Verify the dependencies are correct
    let mut dep_addrs = Vec::new();
    for &dep_id in &dependencies {
        let cell_ref = graph.get_cell_ref(dep_id).unwrap();
        dep_addrs.push((cell_ref.coord.row(), cell_ref.coord.col()));
    }
    dep_addrs.sort();
    let expected_addrs = vec![(0, 0), (1, 0), (2, 0), (3, 0)];
    assert_eq!(dep_addrs, expected_addrs);
}

#[test]
fn test_range_dependency_dirtiness() {
    let mut graph = DependencyGraph::new();

    // C1 depends on the range A1:A10.
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 10, 1))
        .unwrap();
    let c1_id = *graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 3)).unwrap();

    // Create a value in the middle of the range, e.g., A5.
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
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 2, 1))
        .unwrap();
    let b1_id = *graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 2)).unwrap();

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
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 5, 1))
        .unwrap();
    graph.clear_dirty_flags(&[b1_id]);

    // Change A3 again (now inside the range), B1 should be dirty
    graph
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(40))
        .unwrap();
    assert!(graph.get_evaluation_vertices().contains(&b1_id));
}

#[test]
fn test_large_range_creates_single_compressed_ref() {
    let mut graph = graph_with_range_limit(4);

    // C1 = SUM(A1:A100) - size is 100, which is > limit
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 100, 1))
        .unwrap();

    let c1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 3))
        .unwrap();
    let c1_dependencies = graph.get_dependencies(c1_id);

    // Should have no direct dependencies
    assert!(
        c1_dependencies.is_empty(),
        "Should not have any direct cell dependencies"
    );

    // Should have one compressed range dependency
    let range_deps = graph.formula_to_range_deps();
    assert_eq!(
        range_deps.len(),
        1,
        "Should create one compressed range dependency"
    );
    assert!(range_deps.contains_key(&c1_id));
    assert_eq!(range_deps.get(&c1_id).unwrap().len(), 1);
}

#[test]
fn test_duplicate_range_refs_in_formula() {
    let mut graph = graph_with_range_limit(4);
    // B1 = SUM(A1:A100) + COUNT(A1:A100)
    let formula = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(sum_ast(1, 1, 100, 1)),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Function {
                    name: "COUNT".to_string(),
                    args: vec![range_ast(1, 1, 100, 1)],
                },
                source_token: None,
                contains_volatile: false,
            }),
        },
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 1, 2, formula).unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // Should only have one compressed range dependency, not two
    let range_deps = graph.formula_to_range_deps();
    assert_eq!(range_deps.get(&b1_id).unwrap().len(), 1);
}

#[test]
fn test_zero_sized_range_behavior() {
    let mut graph = DependencyGraph::new();
    // B1 = SUM(A1:A0)
    let result = graph.set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 0, 1));
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().kind,
        formualizer_common::ExcelErrorKind::Ref
    );
}
