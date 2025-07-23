//! Tests for topological layer construction (Kahn's algorithm).
use crate::engine::{DependencyGraph, Scheduler, VertexId};
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};

/// Helper to create a cell reference AST node
fn ref_ast(row: u32, col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{}C{}", row, col),
            reference: ReferenceType::Cell {
                sheet: None,
                row,
                col,
            },
        },
        source_token: None,
    }
}

/// Helper to create a binary op AST node
fn op_ast(left: ASTNode, right: ASTNode) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(left),
            right: Box::new(right),
        },
        source_token: None,
    }
}

#[test]
fn test_kahn_topological_layers() {
    let mut graph = DependencyGraph::new();

    // Build a graph with clear layers:
    // Layer 0: A1, B1 (no dependencies)
    // Layer 1: C1 (=A1+B1), D1 (=B1)
    // Layer 2: E1 (=C1+D1)

    // Layer 0
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap(); // A1
    graph
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(20))
        .unwrap(); // B1

    // Layer 1
    graph
        .set_cell_formula("Sheet1", 3, 1, op_ast(ref_ast(1, 1), ref_ast(2, 1)))
        .unwrap(); // C1
    graph
        .set_cell_formula("Sheet1", 4, 1, ref_ast(2, 1))
        .unwrap(); // D1

    // Layer 2
    graph
        .set_cell_formula("Sheet1", 5, 1, op_ast(ref_ast(3, 1), ref_ast(4, 1)))
        .unwrap(); // E1

    let scheduler = Scheduler::new(&graph);
    let all_vertices: Vec<VertexId> = (0..5).map(|i| VertexId::new(i)).collect();

    // We need to get the acyclic components first
    let sccs = scheduler.tarjan_scc(&all_vertices).unwrap();
    let (_, acyclic_sccs) = scheduler.separate_cycles(sccs);

    let layers = scheduler.build_layers(acyclic_sccs).unwrap();

    assert_eq!(layers.len(), 3, "Should be 3 topological layers");

    // Layer 0 should contain A1 and B1
    let layer0_ids: Vec<_> = layers[0].vertices.iter().map(|v| v.as_index()).collect();
    assert_eq!(layer0_ids.len(), 2);
    assert!(layer0_ids.contains(&0)); // A1
    assert!(layer0_ids.contains(&1)); // B1

    // Layer 1 should contain C1 and D1
    let layer1_ids: Vec<_> = layers[1].vertices.iter().map(|v| v.as_index()).collect();
    assert_eq!(layer1_ids.len(), 2);
    assert!(layer1_ids.contains(&2)); // C1
    assert!(layer1_ids.contains(&3)); // D1

    // Layer 2 should contain E1
    let layer2_ids: Vec<_> = layers[2].vertices.iter().map(|v| v.as_index()).collect();
    assert_eq!(layer2_ids.len(), 1);
    assert!(layer2_ids.contains(&4)); // E1
}

#[test]
fn test_layer_parallelism_safety_setup() {
    // This test verifies that our main test case (`test_kahn_topological_layers`)
    // creates a graph where multiple nodes exist in the same layer.
    // This is a prerequisite for testing parallel evaluation in the future.
    let mut graph = DependencyGraph::new();
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(20))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 3, 1, op_ast(ref_ast(1, 1), ref_ast(2, 1)))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 4, 1, ref_ast(2, 1))
        .unwrap();

    let scheduler = Scheduler::new(&graph);
    let all_vertices: Vec<VertexId> = (0..4).map(|i| VertexId::new(i)).collect();
    let sccs = scheduler.tarjan_scc(&all_vertices).unwrap();
    let (_, acyclic_sccs) = scheduler.separate_cycles(sccs);
    let layers = scheduler.build_layers(acyclic_sccs).unwrap();

    assert_eq!(layers.len(), 2);
    assert_eq!(
        layers[0].vertices.len(),
        2,
        "Layer 0 should have 2 vertices, suitable for parallel execution"
    );
    assert_eq!(
        layers[1].vertices.len(),
        2,
        "Layer 1 should have 2 vertices, suitable for parallel execution"
    );
}

#[test]
fn test_empty_layer_handling() {
    let graph = DependencyGraph::new();
    let scheduler = Scheduler::new(&graph);

    let layers = scheduler.build_layers(vec![]).unwrap();
    assert!(
        layers.is_empty(),
        "Building layers from no components should result in no layers"
    );
}

#[test]
fn test_build_layers_with_cycle_errors() {
    let mut graph = DependencyGraph::new();
    // Manually create a graph with a cycle: A1 -> B1 -> A1
    graph
        .set_cell_formula("Sheet1", 1, 1, ref_ast(2, 1))
        .unwrap(); // A1 = B1
    graph
        .set_cell_formula("Sheet1", 2, 1, ref_ast(1, 1))
        .unwrap(); // B1 = A1

    let scheduler = Scheduler::new(&graph);

    // IMPORTANT: We are deliberately passing a cyclic component to `build_layers`
    // to test its internal error handling.
    let cyclic_scc = vec![vec![VertexId::new(0), VertexId::new(1)]];

    let result = scheduler.build_layers(cyclic_scc);

    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.kind, ExcelErrorKind::Circ);
    }
}
