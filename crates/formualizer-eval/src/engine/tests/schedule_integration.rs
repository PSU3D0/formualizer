//! Tests for the complete scheduling pipeline.
use super::common::get_vertex_ids_in_order;
use crate::engine::{DependencyGraph, Scheduler};
use formualizer_common::LiteralValue;
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
fn test_schedule_creation_end_to_end() {
    let mut graph = DependencyGraph::new();
    // A1 -> B1 -> C1
    // A2 -> B1
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap(); // A1
    graph
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(1))
        .unwrap(); // A2
    graph
        .set_cell_formula("Sheet1", 2, 2, ref_ast(1, 1))
        .unwrap(); // B1 = A1
    graph
        .set_cell_formula("Sheet1", 2, 3, ref_ast(2, 2))
        .unwrap(); // C1 = B1

    let scheduler = Scheduler::new(&graph);
    let all_vertex_ids = get_vertex_ids_in_order(&graph);
    let schedule = scheduler.create_schedule(&all_vertex_ids).unwrap();

    assert!(schedule.cycles.is_empty());
    assert_eq!(schedule.layers.len(), 3);
    assert_eq!(schedule.layers[0].vertices.len(), 2); // A1, A2
    assert_eq!(schedule.layers[1].vertices.len(), 1); // B1
    assert_eq!(schedule.layers[2].vertices.len(), 1); // C1
}

#[test]
fn test_cycle_separation_logic() {
    let mut graph = DependencyGraph::new();
    // Cycle: A1 -> B1 -> A1
    // Acyclic: C1 -> D1
    graph
        .set_cell_formula("Sheet1", 1, 1, ref_ast(1, 2))
        .unwrap(); // A1 = B1
    graph
        .set_cell_formula("Sheet1", 1, 2, ref_ast(1, 1))
        .unwrap(); // B1 = A1
    graph
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(1))
        .unwrap(); // C1
    graph
        .set_cell_formula("Sheet1", 2, 2, ref_ast(2, 1))
        .unwrap(); // D1 = C1

    let scheduler = Scheduler::new(&graph);
    let all_vertex_ids = get_vertex_ids_in_order(&graph);
    let schedule = scheduler.create_schedule(&all_vertex_ids).unwrap();

    assert_eq!(schedule.cycles.len(), 1);
    assert_eq!(schedule.cycles[0].len(), 2);
    assert_eq!(schedule.layers.len(), 2);
    assert_eq!(schedule.layers[0].vertices.len(), 1); // C1
    assert_eq!(schedule.layers[1].vertices.len(), 1); // D1
}

#[test]
fn test_scheduling_with_external_dependencies() {
    let mut graph = DependencyGraph::new();
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap(); // A1
    graph
        .set_cell_formula("Sheet1", 1, 2, ref_ast(1, 1))
        .unwrap(); // B1 = A1

    // Mark only B1 as dirty
    let all_vertex_ids = get_vertex_ids_in_order(&graph);
    let b1_id = all_vertex_ids[1]; // B1 is the second vertex
    let dirty_vertices = vec![b1_id];

    let scheduler = Scheduler::new(&graph);
    let schedule = scheduler.create_schedule(&dirty_vertices).unwrap();

    // The schedule should only contain B1, as A1 is not dirty
    assert!(schedule.cycles.is_empty());
    assert_eq!(schedule.layers.len(), 1);
    assert_eq!(schedule.layers[0].vertices.len(), 1);
    assert_eq!(schedule.layers[0].vertices[0], b1_id);
}
