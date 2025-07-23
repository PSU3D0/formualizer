//! Tests for the hybrid model of range dependency management.
use crate::engine::{DependencyGraph, Scheduler, VertexId};
use formualizer_common::LiteralValue;
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
fn test_range_dependency_creation() {
    let mut graph = DependencyGraph::new();
    // C1 = SUM(A1:B1)
    graph
        .set_cell_formula("Sheet1", 1, 3, range_ast(1, 1, 1, 2))
        .unwrap();

    // TODO: When range dependency management is implemented,
    // this test will check the internal state of the graph's
    // range dependency maps.
}

#[test]
fn test_optimal_recomputation_from_range() {
    let mut graph = DependencyGraph::new();
    // C1 = SUM(A1:A10)
    graph
        .set_cell_formula("Sheet1", 1, 3, range_ast(1, 1, 10, 1))
        .unwrap();

    // Change A5
    let summary = graph
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(100))
        .unwrap();

    // TODO: Assert that C1 is marked as dirty.
    // assert!(summary.affected_vertices.contains(&c1_vertex_id));
}

#[test]
fn test_range_dependency_updates_on_formula_change() {
    let mut graph = DependencyGraph::new();
    // B1 = SUM(A1:A5)
    graph
        .set_cell_formula("Sheet1", 1, 2, range_ast(1, 1, 5, 1))
        .unwrap();

    // Update B1 = SUM(A1:A10)
    graph
        .set_cell_formula("Sheet1", 1, 2, range_ast(1, 1, 10, 1))
        .unwrap();

    // TODO: Verify that dependencies for A6-A10 are added.

    // Update B1 = SUM(A1:A2)
    graph
        .set_cell_formula("Sheet1", 1, 2, range_ast(1, 1, 2, 1))
        .unwrap();

    // TODO: Verify that dependencies for A3-A10 are removed.
}

#[test]
fn test_overlapping_range_dependencies() {
    let mut graph = DependencyGraph::new();
    // C1 = SUM(A1:B1)
    // D1 = SUM(B1:C1)
    graph
        .set_cell_formula("Sheet1", 1, 3, range_ast(1, 1, 1, 2))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 4, range_ast(1, 2, 1, 3))
        .unwrap();

    // Change B1
    let summary = graph
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(100))
        .unwrap();

    // TODO: Assert that both C1 and D1 are marked as dirty.
}

#[test]
fn test_range_dependency_edge_cases() {
    let mut graph = DependencyGraph::new();

    // Single cell range
    graph
        .set_cell_formula("Sheet1", 1, 2, range_ast(1, 1, 1, 1))
        .unwrap();
    // TODO: Verify dependency on A1.

    // Empty range (no cells)
    graph
        .set_cell_formula("Sheet1", 1, 3, range_ast(5, 1, 1, 1))
        .unwrap();
    // TODO: Verify no dependencies are created.
}
