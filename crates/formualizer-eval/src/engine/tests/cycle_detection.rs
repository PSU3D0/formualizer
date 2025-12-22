//! Tests for cycle detection using the Scheduler.
use super::common::abs_cell_ref;
use crate::engine::{DependencyGraph, Scheduler, VertexId};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

/// Helper to create a cell reference AST node
fn ref_ast(row: u32, col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{row}C{col}"),
            reference: ReferenceType::cell(None, row, col),
        },
        source_token: None,
        contains_volatile: false,
    }
}

#[test]
fn test_two_node_cycle_detection() {
    let mut graph = DependencyGraph::new();

    // Create a cycle: A1 -> B1 -> A1
    graph
        .set_cell_formula("Sheet1", 1, 1, ref_ast(1, 2))
        .unwrap(); // A1 = B1
    graph
        .set_cell_formula("Sheet1", 1, 2, ref_ast(1, 1))
        .unwrap(); // B1 = A1

    let scheduler = Scheduler::new(&graph);
    // Get the actual vertex IDs from the graph
    let all_vertices: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    let schedule = scheduler.create_schedule(&all_vertices).unwrap();

    // The schedule should detect one cycle containing both vertices.
    assert_eq!(schedule.cycles.len(), 1);
    assert_eq!(schedule.cycles[0].len(), 2);
    assert!(schedule.layers.is_empty());

    // Get the actual vertex IDs for A1 and B1
    let a1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();
    let b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    let cycle_set: std::collections::HashSet<VertexId> =
        schedule.cycles[0].iter().copied().collect();
    assert!(cycle_set.contains(&a1_id));
    assert!(cycle_set.contains(&b1_id));
}

#[test]
fn test_cycle_with_acyclic_branch() {
    let mut graph = DependencyGraph::new();

    // A1 -> B1 -> A1 (cycle)
    graph
        .set_cell_formula("Sheet1", 1, 1, ref_ast(1, 2))
        .unwrap(); // A1 = B1
    graph
        .set_cell_formula("Sheet1", 1, 2, ref_ast(1, 1))
        .unwrap(); // B1 = A1

    // C1 -> D1 (acyclic)
    graph
        .set_cell_formula("Sheet1", 2, 1, ref_ast(2, 2))
        .unwrap(); // C1 = D1
    graph
        .set_cell_value("Sheet1", 2, 2, formualizer_common::LiteralValue::Int(42))
        .unwrap(); // D1 = 42

    let scheduler = Scheduler::new(&graph);
    // Get the actual vertex IDs from the graph
    let all_vertices: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    let schedule = scheduler.create_schedule(&all_vertices).unwrap();

    // Verify cycle is detected
    assert_eq!(schedule.cycles.len(), 1);
    assert_eq!(schedule.cycles[0].len(), 2);

    // Verify acyclic part is scheduled correctly
    assert_eq!(schedule.layers.len(), 2);
    assert_eq!(schedule.layers[0].vertices.len(), 1); // Layer 0: D1
    assert_eq!(schedule.layers[1].vertices.len(), 1); // Layer 1: C1

    let d1_id = graph.cell_to_vertex().get(&abs_cell_ref(0, 2, 2)).unwrap();
    let c1_id = graph.cell_to_vertex().get(&abs_cell_ref(0, 2, 1)).unwrap();

    assert_eq!(schedule.layers[0].vertices[0], *d1_id);
    assert_eq!(schedule.layers[1].vertices[0], *c1_id);
}
