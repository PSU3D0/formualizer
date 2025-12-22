use crate::engine::*;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

// Helper to create a formula that adds two cells
fn sum_formula(row1: u32, col1: u32, row2: u32, col2: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(ASTNode {
                node_type: ASTNodeType::Reference {
                    original: format!("A{row1}"),
                    reference: ReferenceType::cell(None, row1, col1),
                },
                source_token: None,
                contains_volatile: false,
            }),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Reference {
                    original: format!("A{row2}"),
                    reference: ReferenceType::cell(None, row2, col2),
                },
                source_token: None,
                contains_volatile: false,
            }),
        },
        source_token: None,
        contains_volatile: false,
    }
}

#[test]
fn test_sheet_index_updated_on_vertex_creation() {
    let mut graph = DependencyGraph::new();

    // Create a cell value
    let result = graph
        .set_cell_value("Sheet1", 5, 10, LiteralValue::Number(42.0))
        .unwrap();
    let vertex_id = result.affected_vertices[0];

    // Verify the vertex is in the sheet index
    let sheet_id = graph.sheet_id("Sheet1").unwrap();
    let index = graph
        .sheet_index(sheet_id)
        .expect("Sheet index should exist");

    // Query for the exact row
    let row_vertices = index.vertices_in_row_range(4, 4);
    assert!(
        row_vertices.contains(&vertex_id),
        "Vertex should be in row 5"
    );

    // Query for the exact column
    let col_vertices = index.vertices_in_col_range(9, 9);
    assert!(
        col_vertices.contains(&vertex_id),
        "Vertex should be in column 10"
    );

    // Query for a range containing the vertex
    let range_vertices = index.vertices_in_rect(2, 6, 7, 11);
    assert!(
        range_vertices.contains(&vertex_id),
        "Vertex should be in the range"
    );
}

#[test]
fn test_sheet_index_multiple_sheets() {
    let mut graph = DependencyGraph::new();

    // Create cells in different sheets
    let result1 = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    let vertex1 = result1.affected_vertices[0];

    let result2 = graph
        .set_cell_value("Sheet2", 2, 2, LiteralValue::Number(2.0))
        .unwrap();
    let vertex2 = result2.affected_vertices[0];

    let sheet1_id = graph.sheet_id("Sheet1").unwrap();
    let sheet2_id = graph.sheet_id("Sheet2").unwrap();

    // Verify Sheet1 index only contains vertex1
    let index1 = graph
        .sheet_index(sheet1_id)
        .expect("Sheet1 index should exist");
    let sheet1_vertices = index1.vertices_in_row_range(0, u32::MAX);
    assert_eq!(sheet1_vertices.len(), 1);
    assert!(sheet1_vertices.contains(&vertex1));
    assert!(!sheet1_vertices.contains(&vertex2));

    // Verify Sheet2 index only contains vertex2
    let index2 = graph
        .sheet_index(sheet2_id)
        .expect("Sheet2 index should exist");
    let sheet2_vertices = index2.vertices_in_row_range(0, u32::MAX);
    assert_eq!(sheet2_vertices.len(), 1);
    assert!(sheet2_vertices.contains(&vertex2));
    assert!(!sheet2_vertices.contains(&vertex1));
}

#[test]
fn test_sheet_index_range_query_for_shifts() {
    let mut graph = DependencyGraph::new();

    // Create cells that would be affected by "insert rows at row 10"
    let _r5 = graph
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(5.0))
        .unwrap();
    let _r15 = graph
        .set_cell_value("Sheet1", 15, 1, LiteralValue::Number(15.0))
        .unwrap();
    let _r25 = graph
        .set_cell_value("Sheet1", 25, 1, LiteralValue::Number(25.0))
        .unwrap();
    let _r35 = graph
        .set_cell_value("Sheet1", 35, 1, LiteralValue::Number(35.0))
        .unwrap();

    let sheet_id = graph.sheet_id("Sheet1").unwrap();
    let index = graph
        .sheet_index(sheet_id)
        .expect("Sheet index should exist");

    // Query for vertices that would need to shift (row >= 10)
    let vertices_to_shift = index.vertices_in_row_range(10, u32::MAX);
    assert_eq!(vertices_to_shift.len(), 3, "Should find rows 15, 25, 35");

    // Query for vertices that wouldn't shift (row < 10)
    let vertices_unaffected = index.vertices_in_row_range(0, 9);
    assert_eq!(vertices_unaffected.len(), 1, "Should find only row 5");
}

#[test]
fn test_sheet_index_column_operations() {
    let mut graph = DependencyGraph::new();

    // Create cells in various columns
    for col in [0, 5, 10, 15, 20, 25] {
        graph
            .set_cell_value("Sheet1", 1, col + 1, LiteralValue::Number(col as f64))
            .unwrap();
    }

    let sheet_id = graph.sheet_id("Sheet1").unwrap();
    let index = graph
        .sheet_index(sheet_id)
        .expect("Sheet index should exist");

    // Query for columns 10-20 (simulating delete columns operation)
    let vertices_in_range = index.vertices_in_col_range(10, 20);
    assert_eq!(vertices_in_range.len(), 3, "Should find columns 10, 15, 20");

    // Query for columns that would shift after deleting columns 10-20
    let vertices_to_shift = index.vertices_in_col_range(21, u32::MAX);
    assert_eq!(vertices_to_shift.len(), 1, "Should find column 25");
}

#[test]
fn test_sheet_index_rectangular_range() {
    let mut graph = DependencyGraph::new();

    // Create a grid of cells
    for row in 0..10 {
        for col in 0..5 {
            graph
                .set_cell_value(
                    "Sheet1",
                    row + 1,
                    col + 1,
                    LiteralValue::Number((row * 5 + col) as f64),
                )
                .unwrap();
        }
    }

    let sheet_id = graph.sheet_id("Sheet1").unwrap();
    let index = graph
        .sheet_index(sheet_id)
        .expect("Sheet index should exist");

    // Query for a rectangular range (rows 3-6, columns 1-3)
    let rect_vertices = index.vertices_in_rect(3, 6, 1, 3);
    assert_eq!(
        rect_vertices.len(),
        12,
        "Should find 4 rows × 3 columns = 12 vertices"
    );
}

#[test]
fn test_sheet_index_sparse_efficiency() {
    let mut graph = DependencyGraph::new();

    // Create a very sparse sheet - cells at extreme positions
    graph
        .set_cell_value("Sheet1", 100, 5, LiteralValue::Number(1.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 50_000, 10, LiteralValue::Number(2.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 100_000, 15, LiteralValue::Number(3.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 500_000, 20, LiteralValue::Number(4.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 999_999, 25, LiteralValue::Number(5.0))
        .unwrap();

    let sheet_id = graph.sheet_id("Sheet1").unwrap();
    let index = graph
        .sheet_index(sheet_id)
        .expect("Sheet index should exist");

    // This query should be O(log n + k) not O(n)
    let high_rows = index.vertices_in_row_range(99_999, u32::MAX);
    assert_eq!(
        high_rows.len(),
        3,
        "Should efficiently find 3 vertices in high rows"
    );

    // Column range query should also be efficient
    let mid_cols = index.vertices_in_col_range(9, 19);
    assert_eq!(
        mid_cols.len(),
        3,
        "Should efficiently find vertices in columns 10, 15, 20"
    );
}

#[test]
fn test_sheet_index_with_formulas() {
    let mut graph = DependencyGraph::new();

    // Create cells with formulas
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();

    let formula = sum_formula(1, 1, 2, 1); // =A1+A2
    let result = graph.set_cell_formula("Sheet1", 3, 1, formula).unwrap();
    let formula_vertex = result.affected_vertices[0];

    let sheet_id = graph.sheet_id("Sheet1").unwrap();
    let index = graph
        .sheet_index(sheet_id)
        .expect("Sheet index should exist");

    // Formula cell should be in the index
    let row3_vertices = index.vertices_in_row_range(2, 2);
    assert!(
        row3_vertices.contains(&formula_vertex),
        "Formula vertex should be indexed"
    );

    // Total should be at least 3 vertices (may have placeholders)
    assert!(index.len() >= 3, "Should have at least 3 vertices total");
}
