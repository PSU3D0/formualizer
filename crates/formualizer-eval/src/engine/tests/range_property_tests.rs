//! Property-based tests for range dependency tracking
//!
//! These tests verify the core property: "For any N-by-M range, if I change any cell (r, c)
//! inside it, the dependent formula must become dirty. If I change a cell outside it, it must not."

use super::common::abs_cell_ref;
use crate::engine::{DependencyGraph, EvalConfig, VertexId};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
// use rustc_hash::FxHashSet; // Not needed for these tests

/// Helper to create a range reference AST node
fn range_ast(
    sheet: Option<&str>,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!(
                "{}R{}C{}:R{}C{}",
                sheet.map(|s| format!("{s}!")).unwrap_or_default(),
                start_row,
                start_col,
                end_row,
                end_col
            ),
            reference: ReferenceType::range(
                sheet.map(|s| s.to_string()),
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
fn sum_range_ast(
    sheet: Option<&str>,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Function {
            name: "SUM".to_string(),
            args: vec![range_ast(sheet, start_row, start_col, end_row, end_col)],
        },
        source_token: None,
        contains_volatile: false,
    }
}

fn config_with_range_limit(limit: usize) -> EvalConfig {
    EvalConfig {
        range_expansion_limit: limit,
        ..Default::default()
    }
}

/// Test the core property for small ranges (expanded to individual cell dependencies)
#[test]
fn test_property_small_range_dependency_tracking() {
    let mut graph = DependencyGraph::new_with_config(config_with_range_limit(100)); // Ensure small ranges are expanded

    // Test range: A1:C3 (3x3 = 9 cells)
    let start_row = 1;
    let start_col = 1;
    let end_row = 3;
    let end_col = 3;

    // Formula at D1 = SUM(A1:C3)
    let formula_row = 1;
    let formula_col = 4;
    graph
        .set_cell_formula(
            "Sheet1",
            formula_row,
            formula_col,
            sum_range_ast(None, start_row, start_col, end_row, end_col),
        )
        .unwrap();

    let formula_addr = abs_cell_ref(0, formula_row, formula_col);
    let formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();

    // Clear initial dirty state
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);
    assert!(
        graph.get_evaluation_vertices().is_empty(),
        "Should start with no dirty vertices"
    );

    // Property test: Every cell inside the range should dirty the formula
    for row in start_row..=end_row {
        for col in start_col..=end_col {
            // Set a value in the cell
            graph
                .set_cell_value("Sheet1", row, col, LiteralValue::Int(42))
                .unwrap();

            // Check that the formula is dirty
            let dirty_vertices = graph.get_evaluation_vertices();
            assert!(
                dirty_vertices.contains(&formula_id),
                "Formula should be dirty when cell ({row}, {col}) changes (inside range {start_row}:{start_col} to {end_row}:{end_col})"
            );

            // Clear dirty state for next iteration
            graph.clear_dirty_flags(&dirty_vertices);
        }
    }

    // Property test: Cells outside the range should NOT dirty the formula
    let outside_cells = vec![
        (start_row - 1, start_col),     // Above
        (end_row + 1, end_col),         // Below
        (start_row, start_col - 1),     // Left
        (end_row, end_col + 1),         // Right
        (start_row - 1, start_col - 1), // Top-left diagonal
        (end_row + 1, end_col + 1),     // Bottom-right diagonal
    ];

    for (row, col) in outside_cells {
        if row > 0 && col > 0 {
            // Skip invalid coordinates
            graph
                .set_cell_value("Sheet1", row, col, LiteralValue::Int(99))
                .unwrap();

            let dirty_vertices = graph.get_evaluation_vertices();
            assert!(
                !dirty_vertices.contains(&formula_id),
                "Formula should NOT be dirty when cell ({row}, {col}) changes (outside range {start_row}:{start_col} to {end_row}:{end_col})"
            );
        }
    }
}

/// Test the core property for large ranges (using stripe-based tracking)
#[test]
fn test_property_large_range_stripe_tracking() {
    let mut graph = DependencyGraph::new_with_config(config_with_range_limit(16)); // Force stripe tracking for larger ranges

    // Test tall range: A1:A1000 (1000 cells)
    let start_row = 1;
    let start_col = 1;
    let end_row = 1000;
    let end_col = 1;

    // Formula at B1 = SUM(A1:A1000)
    let formula_row = 1;
    let formula_col = 2;
    graph
        .set_cell_formula(
            "Sheet1",
            formula_row,
            formula_col,
            sum_range_ast(None, start_row, start_col, end_row, end_col),
        )
        .unwrap();

    let formula_addr = abs_cell_ref(0, formula_row, formula_col);
    let formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();

    // Clear initial dirty state
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Property test: Sample cells inside the range should dirty the formula
    let inside_test_cells = vec![
        (1, 1),    // First cell
        (500, 1),  // Middle cell
        (1000, 1), // Last cell
        (250, 1),  // Random cell
        (750, 1),  // Another random cell
    ];

    for (row, col) in inside_test_cells {
        graph
            .set_cell_value("Sheet1", row, col, LiteralValue::Int(42))
            .unwrap();

        let dirty_vertices = graph.get_evaluation_vertices();
        assert!(
            dirty_vertices.contains(&formula_id),
            "Formula should be dirty when cell ({row}, {col}) changes (inside stripe range A1:A1000)"
        );

        graph.clear_dirty_flags(&dirty_vertices);
    }

    // Property test: Cells outside the range should NOT dirty the formula
    let outside_test_cells = vec![
        (1, 2),    // Same row, different column
        (500, 3),  // Different column
        (1000, 2), // Different column
        (1001, 1), // Below range
        (0, 1),    // Invalid (would be above range if valid)
    ];

    for (row, col) in outside_test_cells {
        if row > 0 && col > 0 {
            graph
                .set_cell_value("Sheet1", row, col, LiteralValue::Int(99))
                .unwrap();

            let dirty_vertices = graph.get_evaluation_vertices();
            assert!(
                !dirty_vertices.contains(&formula_id),
                "Formula should NOT be dirty when cell ({row}, {col}) changes (outside stripe range A1:A1000)"
            );
        }
    }
}

/// Test the core property for wide ranges
#[test]
fn test_property_wide_range_stripe_tracking() {
    let mut graph = DependencyGraph::new_with_config(config_with_range_limit(16));

    // Test wide range: A1:Z1 (26 columns, 1 row)
    let start_row = 1;
    let start_col = 1;
    let end_row = 1;
    let end_col = 26;

    // Formula at A2 = SUM(A1:Z1)
    let formula_row = 2;
    let formula_col = 1;
    graph
        .set_cell_formula(
            "Sheet1",
            formula_row,
            formula_col,
            sum_range_ast(None, start_row, start_col, end_row, end_col),
        )
        .unwrap();

    let formula_addr = abs_cell_ref(0, formula_row, formula_col);
    let formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();

    // Clear initial dirty state
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Property test: Sample cells inside the range should dirty the formula
    let inside_test_cells = vec![
        (1, 1),  // A1
        (1, 13), // M1 (middle)
        (1, 26), // Z1 (last)
        (1, 5),  // E1
        (1, 20), // T1
    ];

    for (row, col) in inside_test_cells {
        graph
            .set_cell_value("Sheet1", row, col, LiteralValue::Int(42))
            .unwrap();

        let dirty_vertices = graph.get_evaluation_vertices();
        assert!(
            dirty_vertices.contains(&formula_id),
            "Formula should be dirty when cell ({row}, {col}) changes (inside row range A1:Z1)"
        );

        graph.clear_dirty_flags(&dirty_vertices);
    }

    // Property test: Cells outside the range should NOT dirty the formula
    let outside_test_cells = vec![
        (2, 1),  // A2 (different row)
        (2, 13), // M2 (different row)
        (0, 13), // Invalid (above)
        (1, 27), // AA1 (beyond range)
        (1, 0),  // Invalid (before range)
    ];

    for (row, col) in outside_test_cells {
        if row > 0 && col > 0 {
            graph
                .set_cell_value("Sheet1", row, col, LiteralValue::Int(99))
                .unwrap();

            let dirty_vertices = graph.get_evaluation_vertices();
            assert!(
                !dirty_vertices.contains(&formula_id),
                "Formula should NOT be dirty when cell ({row}, {col}) changes (outside row range A1:Z1)"
            );
        }
    }
}

/// Test the core property for square/dense ranges with block stripes
#[test]
fn test_property_dense_range_block_stripe_tracking() {
    let mut config = config_with_range_limit(16);
    config = config.with_block_stripes(true);
    let mut graph = DependencyGraph::new_with_config(config);

    // Test dense range: A1:Z26 (676 cells in a 26x26 square)
    let start_row = 1;
    let start_col = 1;
    let end_row = 26;
    let end_col = 26;

    // Formula at AA1 = SUM(A1:Z26)
    let formula_row = 1;
    let formula_col = 27;
    graph
        .set_cell_formula(
            "Sheet1",
            formula_row,
            formula_col,
            sum_range_ast(None, start_row, start_col, end_row, end_col),
        )
        .unwrap();

    let formula_addr = abs_cell_ref(0, formula_row, formula_col);
    let formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();

    // Clear initial dirty state
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Property test: Sample cells inside the range should dirty the formula
    let inside_test_cells = vec![
        (1, 1),   // A1 (corner)
        (1, 26),  // Z1 (corner)
        (26, 1),  // A26 (corner)
        (26, 26), // Z26 (corner)
        (13, 13), // M13 (center)
        (5, 20),  // Random inside
        (20, 5),  // Random inside
    ];

    for (row, col) in inside_test_cells {
        graph
            .set_cell_value("Sheet1", row, col, LiteralValue::Int(42))
            .unwrap();

        let dirty_vertices = graph.get_evaluation_vertices();
        assert!(
            dirty_vertices.contains(&formula_id),
            "Formula should be dirty when cell ({row}, {col}) changes (inside block range A1:Z26)"
        );

        graph.clear_dirty_flags(&dirty_vertices);
    }

    // Property test: Cells outside the range should NOT dirty the formula
    let outside_test_cells = vec![
        (0, 13),  // Invalid row above
        (27, 13), // Row below range
        (13, 0),  // Invalid col before
        (13, 27), // Column after range
        (0, 0),   // Invalid corner
        (27, 27), // Corner outside
        (30, 5),  // Well outside
        (5, 30),  // Well outside
    ];

    for (row, col) in outside_test_cells {
        if row > 0 && col > 0 {
            graph
                .set_cell_value("Sheet1", row, col, LiteralValue::Int(99))
                .unwrap();

            let dirty_vertices = graph.get_evaluation_vertices();
            assert!(
                !dirty_vertices.contains(&formula_id),
                "Formula should NOT be dirty when cell ({row}, {col}) changes (outside block range A1:Z26)"
            );
        }
    }
}

/// Test multiple overlapping ranges
#[test]
fn test_property_multiple_overlapping_ranges() {
    let mut graph = DependencyGraph::new_with_config(config_with_range_limit(16));

    // Formula 1: D1 = SUM(A1:C100) - tall in columns A-C
    graph
        .set_cell_formula("Sheet1", 1, 4, sum_range_ast(None, 1, 1, 100, 3))
        .unwrap();

    // Formula 2: D2 = SUM(A50:C150) - overlaps with above but extends further
    graph
        .set_cell_formula("Sheet1", 2, 4, sum_range_ast(None, 50, 1, 150, 3))
        .unwrap();

    let formula1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 4))
        .unwrap();
    let formula2_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 2, 4))
        .unwrap();

    // Clear initial dirty state
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Test overlap region: Both formulas should be dirty
    graph
        .set_cell_value("Sheet1", 75, 2, LiteralValue::Int(42))
        .unwrap(); // B75 is in both ranges

    let dirty = graph.get_evaluation_vertices();
    assert!(
        dirty.contains(&formula1_id),
        "Formula 1 should be dirty (cell B75 in range A1:C100)"
    );
    assert!(
        dirty.contains(&formula2_id),
        "Formula 2 should be dirty (cell B75 in range A50:C150)"
    );

    graph.clear_dirty_flags(&dirty);

    // Test formula 1 only region: Only formula 1 should be dirty
    graph
        .set_cell_value("Sheet1", 25, 2, LiteralValue::Int(43))
        .unwrap(); // B25 only in first range

    let dirty = graph.get_evaluation_vertices();
    assert!(
        dirty.contains(&formula1_id),
        "Formula 1 should be dirty (cell B25 in range A1:C100)"
    );
    assert!(
        !dirty.contains(&formula2_id),
        "Formula 2 should NOT be dirty (cell B25 not in range A50:C150)"
    );

    graph.clear_dirty_flags(&dirty);

    // Test formula 2 only region: Only formula 2 should be dirty
    graph
        .set_cell_value("Sheet1", 125, 2, LiteralValue::Int(44))
        .unwrap(); // B125 only in second range

    let dirty = graph.get_evaluation_vertices();
    assert!(
        !dirty.contains(&formula1_id),
        "Formula 1 should NOT be dirty (cell B125 not in range A1:C100)"
    );
    assert!(
        dirty.contains(&formula2_id),
        "Formula 2 should be dirty (cell B125 in range A50:C150)"
    );

    graph.clear_dirty_flags(&dirty);

    // Test outside both ranges: Neither should be dirty
    graph
        .set_cell_value("Sheet1", 200, 2, LiteralValue::Int(45))
        .unwrap(); // B200 outside both

    let dirty = graph.get_evaluation_vertices();
    assert!(
        !dirty.contains(&formula1_id),
        "Formula 1 should NOT be dirty (cell B200 outside both ranges)"
    );
    assert!(
        !dirty.contains(&formula2_id),
        "Formula 2 should NOT be dirty (cell B200 outside both ranges)"
    );
}

/// Test cross-sheet range dependencies
#[test]
fn test_property_cross_sheet_ranges() {
    let mut graph = DependencyGraph::new_with_config(config_with_range_limit(16));

    graph.add_sheet("Sheet2").unwrap();

    // Formula on Sheet1: A1 = SUM(Sheet2!A1:A100)
    graph
        .set_cell_formula("Sheet1", 1, 1, sum_range_ast(Some("Sheet2"), 1, 1, 100, 1))
        .unwrap();

    let formula_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();

    // Clear initial dirty state
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Changes on Sheet2 within range should dirty the formula
    graph
        .set_cell_value("Sheet2", 50, 1, LiteralValue::Int(42))
        .unwrap();

    let dirty = graph.get_evaluation_vertices();
    assert!(
        dirty.contains(&formula_id),
        "Formula should be dirty when Sheet2!A50 changes"
    );

    graph.clear_dirty_flags(&dirty);

    // Changes on Sheet2 outside range should NOT dirty the formula
    graph
        .set_cell_value("Sheet2", 50, 2, LiteralValue::Int(43))
        .unwrap(); // Different column

    let dirty = graph.get_evaluation_vertices();
    assert!(
        !dirty.contains(&formula_id),
        "Formula should NOT be dirty when Sheet2!B50 changes"
    );

    // Changes on Sheet1 should NOT dirty the formula (different sheet)
    graph
        .set_cell_value("Sheet1", 50, 1, LiteralValue::Int(44))
        .unwrap();

    let dirty = graph.get_evaluation_vertices();
    assert!(
        !dirty.contains(&formula_id),
        "Formula should NOT be dirty when Sheet1!A50 changes"
    );
}

/// Test edge cases and boundary conditions
#[test]
fn test_property_edge_cases() {
    let mut graph = DependencyGraph::new_with_config(config_with_range_limit(4)); // Small limit to test both expansion and stripe modes

    // Single cell range (edge case)
    graph
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            sum_range_ast(None, 5, 5, 5, 5), // A single cell A5:A5
        )
        .unwrap();

    let formula_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();

    // Clear initial dirty state
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Exact cell should dirty
    graph
        .set_cell_value("Sheet1", 5, 5, LiteralValue::Int(42))
        .unwrap();

    let dirty = graph.get_evaluation_vertices();
    assert!(
        dirty.contains(&formula_id),
        "Formula should be dirty when exact cell (5,5) changes"
    );

    graph.clear_dirty_flags(&dirty);

    // Adjacent cells should NOT dirty
    let adjacent_cells = vec![(4, 5), (6, 5), (5, 4), (5, 6)];
    for (row, col) in adjacent_cells {
        graph
            .set_cell_value("Sheet1", row, col, LiteralValue::Int(99))
            .unwrap();

        let dirty = graph.get_evaluation_vertices();
        assert!(
            !dirty.contains(&formula_id),
            "Formula should NOT be dirty when adjacent cell ({row},{col}) changes"
        );
    }
}

/// Test behavior when formula is replaced (should clean up old dependencies)
#[test]
fn test_property_formula_replacement_cleanup() {
    let mut graph = DependencyGraph::new_with_config(config_with_range_limit(16));

    let formula_row = 1;
    let formula_col = 1;
    let formula_addr = abs_cell_ref(0, formula_row, formula_col);

    // Initial formula: A1 = SUM(B1:B100)
    graph
        .set_cell_formula(
            "Sheet1",
            formula_row,
            formula_col,
            sum_range_ast(None, 1, 2, 100, 2),
        )
        .unwrap();

    let formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();

    // Clear initial dirty state
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Verify old range works
    graph
        .set_cell_value("Sheet1", 50, 2, LiteralValue::Int(42))
        .unwrap(); // B50
    assert!(
        graph.get_evaluation_vertices().contains(&formula_id),
        "Formula should initially be dirty when B50 changes"
    );

    graph.clear_dirty_flags(&graph.get_evaluation_vertices());

    // Replace with new formula: A1 = SUM(C1:C100)
    graph
        .set_cell_formula(
            "Sheet1",
            formula_row,
            formula_col,
            sum_range_ast(None, 1, 3, 100, 3),
        )
        .unwrap();

    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Old range should no longer affect formula
    graph
        .set_cell_value("Sheet1", 50, 2, LiteralValue::Int(99))
        .unwrap(); // B50 (old range)
    assert!(
        !graph.get_evaluation_vertices().contains(&formula_id),
        "Formula should NOT be dirty when old range B50 changes after replacement"
    );

    // New range should affect formula
    graph
        .set_cell_value("Sheet1", 50, 3, LiteralValue::Int(100))
        .unwrap(); // C50 (new range)
    assert!(
        graph.get_evaluation_vertices().contains(&formula_id),
        "Formula should be dirty when new range C50 changes after replacement"
    );
}
