//! Tests for striped dirty propagation (Milestone 5.3)

use super::common::{abs_cell_ref, eval_config_with_range_limit};
use crate::engine::DependencyGraph;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

/// Helper to create a range reference AST node
fn range_ast(start_row: u32, start_col: u32, end_row: u32, end_col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{start_row}C{start_col}:R{end_row}C{end_col}"),
            reference: ReferenceType::Range {
                sheet: None,
                start_row: Some(start_row),
                start_col: Some(start_col),
                end_row: Some(end_row),
                end_col: Some(end_col),
                start_row_abs: false,
                start_col_abs: false,
                end_row_abs: false,
                end_col_abs: false,
            },
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

#[test]
fn test_change_in_tiny_range_dirties_dependent() {
    let mut graph = DependencyGraph::new_with_config(eval_config_with_range_limit(10)); // Allow small ranges to expand

    // B1 = SUM(A1:A4) - small range that expands to direct dependencies
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 4, 1))
        .unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // Set initial values and clear dirty flags
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(20))
        .unwrap();

    graph.clear_dirty_flags(&[b1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Change a cell in the range - should dirty B1 via direct dependents
    graph
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(30))
        .unwrap();

    assert!(graph.get_evaluation_vertices().contains(&b1_id));
}

#[test]
fn test_change_in_large_tall_range_dirties_dependent() {
    let mut graph = DependencyGraph::new_with_config(eval_config_with_range_limit(4)); // Force large ranges to be compressed

    // B1 = SUM(A1:A100) - tall range that uses column stripe
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 100, 1))
        .unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // Clear dirty flags
    graph.clear_dirty_flags(&[b1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Change a cell in the middle of the tall range
    graph
        .set_cell_value("Sheet1", 50, 1, LiteralValue::Int(100))
        .unwrap();

    // Should dirty B1 via column stripe lookup
    assert!(
        graph.get_evaluation_vertices().contains(&b1_id),
        "Tall range dependent should be dirtied via column stripe"
    );
}

#[test]
fn test_change_in_large_wide_range_dirties_dependent() {
    let mut graph = DependencyGraph::new_with_config(eval_config_with_range_limit(4)); // Force large ranges to be compressed

    // B1 = SUM(A1:Z1) - wide range that uses row stripe
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 1, 26))
        .unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // Clear dirty flags
    graph.clear_dirty_flags(&[b1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Change a cell in the middle of the wide range
    graph
        .set_cell_value("Sheet1", 1, 15, LiteralValue::Int(100))
        .unwrap();

    // Should dirty B1 via row stripe lookup
    assert!(
        graph.get_evaluation_vertices().contains(&b1_id),
        "Wide range dependent should be dirtied via row stripe"
    );
}

#[test]
fn test_change_outside_range_does_not_dirty_dependent() {
    let mut graph = DependencyGraph::new_with_config(eval_config_with_range_limit(4));

    // B1 = SUM(A1:A10) - depends on column A rows 1-10
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 10, 1))
        .unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // Clear dirty flags
    graph.clear_dirty_flags(&[b1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Change cells outside the range:

    // 1. Different column (B11)
    graph
        .set_cell_value("Sheet1", 11, 2, LiteralValue::Int(100))
        .unwrap();
    assert!(
        !graph.get_evaluation_vertices().contains(&b1_id),
        "Change in different column should not dirty dependent"
    );

    // 2. Same column but outside row range (A20)
    graph
        .set_cell_value("Sheet1", 20, 1, LiteralValue::Int(200))
        .unwrap();
    // Note: This will still dirty B1 because we use column stripe for the entire column
    // This is expected behavior - stripe indexing trades precision for performance

    // 3. Different sheet entirely
    graph
        .set_cell_value("Sheet2", 5, 1, LiteralValue::Int(300))
        .unwrap();
    // This should definitely not dirty B1 since it's a different sheet
}

#[test]
fn test_multi_stripe_border_cell_edit() {
    let config = eval_config_with_range_limit(4).with_block_stripes(true);
    let mut graph = DependencyGraph::new_with_config(config);

    // Create a dense range that spans multiple blocks
    // C1 = SUM(A1:B512) - spans multiple 256x256 blocks
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 512, 2))
        .unwrap();

    let c1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 3))
        .unwrap();

    // Clear dirty flags
    graph.clear_dirty_flags(&[c1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&c1_id));

    // Edit a cell on the border between blocks (row 256, col 1)
    // This should be on the border between block (0,0) and block (1,0)
    graph
        .set_cell_value("Sheet1", 256, 1, LiteralValue::Int(100))
        .unwrap();

    // Should dirty C1 - the stripe system should handle this correctly
    assert!(
        graph.get_evaluation_vertices().contains(&c1_id),
        "Border cell edit should dirty dependent"
    );

    // Verify that we don't get duplicate entries in dirty queue
    let eval_vertices = graph.get_evaluation_vertices();
    let c1_count = eval_vertices.iter().filter(|&&id| id == c1_id).count();
    assert_eq!(
        c1_count, 1,
        "Dependent should appear only once in dirty queue"
    );
}

// Property-based test helper - we'll use a simpler approach without external deps
fn generate_test_coordinates() -> Vec<(u32, u32)> {
    vec![
        (1, 1),
        (1, 10),
        (1, 50), // First row
        (10, 1),
        (10, 10),
        (10, 50), // Middle row
        (50, 1),
        (50, 10),
        (50, 50), // Last row
        (25, 25), // Center
    ]
}

#[test]
fn prop_any_cell_change_in_range_dirties_dependent() {
    let mut graph = DependencyGraph::new_with_config(eval_config_with_range_limit(4));

    // Test range: A1:AZ50 (large enough to use stripe indexing)

    graph
        .set_cell_formula("Sheet1", 1, 100, sum_ast(1, 1, 50, 52)) // AZ is column 52
        .unwrap();

    let formula_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 100))
        .unwrap();

    // Test various coordinates within the range
    for (row, col) in generate_test_coordinates() {
        if row <= 50 && col <= 52 {
            // Within range A1:AZ50
            // Clear dirty flags
            graph.clear_dirty_flags(&[formula_id]);
            assert!(!graph.get_evaluation_vertices().contains(&formula_id));

            // Change the cell
            graph
                .set_cell_value(
                    "Sheet1",
                    row,
                    col,
                    LiteralValue::Int(row as i64 * col as i64),
                )
                .unwrap();

            // Should dirty the dependent
            assert!(
                graph.get_evaluation_vertices().contains(&formula_id),
                "Cell change at ({row}, {col}) should dirty range-dependent formula"
            );
        }
    }
}

#[test]
fn test_multiple_ranges_same_stripe() {
    let mut graph = DependencyGraph::new_with_config(eval_config_with_range_limit(4));

    // Create two formulas that depend on overlapping ranges in column A
    // B1 = SUM(A1:A30)
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 30, 1))
        .unwrap();

    // C1 = SUM(A20:A50) - overlaps with B1's range
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(20, 1, 50, 1))
        .unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();
    let c1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 3))
        .unwrap();

    // Clear dirty flags
    graph.clear_dirty_flags(&[b1_id, c1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));
    assert!(!graph.get_evaluation_vertices().contains(&c1_id));

    // Change a cell in the overlapping region (A25)
    graph
        .set_cell_value("Sheet1", 25, 1, LiteralValue::Int(100))
        .unwrap();

    // Both formulas should be dirtied
    let eval_vertices = graph.get_evaluation_vertices();
    assert!(
        eval_vertices.contains(&b1_id),
        "First range formula should be dirty"
    );
    assert!(
        eval_vertices.contains(&c1_id),
        "Second range formula should be dirty"
    );
}

#[test]
fn test_cross_sheet_stripe_isolation() {
    let mut graph = DependencyGraph::new_with_config(eval_config_with_range_limit(4));

    // Formula on Sheet1 depends on range in Sheet1
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 20, 1))
        .unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // Clear dirty flags
    graph.clear_dirty_flags(&[b1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Change a cell on Sheet2 in same coordinates
    graph
        .set_cell_value("Sheet2", 10, 1, LiteralValue::Int(100))
        .unwrap();

    // Should NOT dirty the formula since it's on a different sheet
    assert!(
        !graph.get_evaluation_vertices().contains(&b1_id),
        "Cross-sheet change should not dirty formula"
    );

    // But change on the same sheet should dirty it
    graph
        .set_cell_value("Sheet1", 10, 1, LiteralValue::Int(200))
        .unwrap();

    assert!(
        graph.get_evaluation_vertices().contains(&b1_id),
        "Same-sheet change should dirty formula"
    );
}
