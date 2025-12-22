//! Benchmark tests for mark_dirty performance with thousands of overlapping ranges
//!
//! These tests verify that the stripe model provides significant performance
//! improvements over naive O(n) dependency checking when dealing with large
//! numbers of overlapping range formulas.

use crate::CellRef;
use crate::engine::{DependencyGraph, EvalConfig};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use std::time::Instant;

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
            args: vec![ASTNode {
                node_type: ASTNodeType::Reference {
                    original: format!(
                        "{}R{}C{}:R{}C{}",
                        sheet.map(|s| format!("{}!", s)).unwrap_or_default(),
                        start_row,
                        start_col,
                        end_row,
                        end_col
                    ),
                    reference: ReferenceType::Range {
                        sheet: sheet.map(|s| s.to_string()),
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
            }],
        },
    source_token: None,
    contains_volatile: false,
    }
}

/// Create a graph with many overlapping range formulas to stress-test mark_dirty
fn create_overlapping_ranges_graph(num_formulas: usize, enable_stripes: bool) -> DependencyGraph {
    let mut config = EvalConfig::default();
    if enable_stripes {
        config = config.with_range_expansion_limit(16); // Force stripe usage
    } else {
        config = config.with_range_expansion_limit(1000000); // Force expansion to individual cells
    }
    config = config.with_block_stripes(enable_stripes);

    let mut graph = DependencyGraph::new_with_config(config);

    // Create overlapping column ranges: A1:A1000, A2:A1001, A3:A1002, etc.
    // Each formula is placed in column B to avoid conflicts
    for i in 0..num_formulas {
        let start_row = i as u32 + 1;
        let end_row = start_row + 999; // 1000-cell range
        let formula_row = i as u32 + 1;
        let formula_col = 2; // Column B

        graph
            .set_cell_formula(
                "Sheet1",
                formula_row,
                formula_col,
                sum_range_ast(None, start_row, 1, end_row, 1),
            )
            .unwrap();
    }

    graph
}

#[test]
fn test_mark_dirty_stripe_vs_expansion_performance() {
    const NUM_FORMULAS: usize = 1000;

    println!(
        "\n=== Benchmark: mark_dirty with {} overlapping ranges ===",
        NUM_FORMULAS
    );

    // Test with stripe model (efficient)
    let mut stripe_graph = create_overlapping_ranges_graph(NUM_FORMULAS, true);

    // Clear initial dirty state
    let all_ids: Vec<_> = stripe_graph.cell_to_vertex().values().copied().collect();
    stripe_graph.clear_dirty_flags(&all_ids);

    // Benchmark mark_dirty with stripe model
    let test_row = 500; // Middle of most ranges
    let test_col = 1; // Column A

    let stripe_start = Instant::now();
    stripe_graph
        .set_cell_value("Sheet1", test_row, test_col, LiteralValue::Int(42))
        .unwrap();
    let stripe_duration = stripe_start.elapsed();
    let stripe_dirty_count = stripe_graph.get_evaluation_vertices().len();

    println!(
        "Stripe model: {} ms, {} formulas marked dirty",
        stripe_duration.as_millis(),
        stripe_dirty_count
    );

    // Test with expansion model (less efficient for large ranges)
    let mut expansion_graph = create_overlapping_ranges_graph(
        std::cmp::min(NUM_FORMULAS, 100), // Reduce size to avoid timeout
        false,
    );

    let all_ids: Vec<_> = expansion_graph.cell_to_vertex().values().copied().collect();
    expansion_graph.clear_dirty_flags(&all_ids);

    let expansion_start = Instant::now();
    expansion_graph
        .set_cell_value("Sheet1", test_row, test_col, LiteralValue::Int(42))
        .unwrap();
    let expansion_duration = expansion_start.elapsed();
    let expansion_dirty_count = expansion_graph.get_evaluation_vertices().len();

    println!(
        "Expansion model (100 formulas): {} ms, {} formulas marked dirty",
        expansion_duration.as_millis(),
        expansion_dirty_count
    );

    // Verify correctness: both approaches should mark the same number of formulas dirty
    // (when testing the same number of formulas)
    //let expected_dirty_count = std::cmp::min(NUM_FORMULAS, 100).min(500); // Approximately
    assert!(expansion_dirty_count > 0, "Should mark some formulas dirty");
    assert!(
        stripe_dirty_count >= expansion_dirty_count,
        "Stripe model should find at least as many dependencies"
    );

    // Performance assertion: stripe model should be reasonably fast
    assert!(
        stripe_duration.as_millis() < 1000,
        "Stripe model should complete mark_dirty in under 1 second"
    );

    println!("✓ Stripe model performance test passed");
}

#[test]
fn test_mark_dirty_scaling_with_stripe_model() {
    println!("\n=== Benchmark: mark_dirty scaling test ===");

    let test_sizes = vec![100, 500, 1000, 2000];

    for &size in &test_sizes {
        let mut graph = create_overlapping_ranges_graph(size, true);

        // Clear initial dirty state
        let all_ids: Vec<_> = graph.cell_to_vertex().values().copied().collect();
        graph.clear_dirty_flags(&all_ids);

        // Test mark_dirty performance
        let test_row = size as u32 / 2; // Middle cell
        let test_col = 1;

        let start = Instant::now();
        graph
            .set_cell_value("Sheet1", test_row, test_col, LiteralValue::Int(42))
            .unwrap();
        let duration = start.elapsed();
        let dirty_count = graph.get_evaluation_vertices().len();

        println!(
            "Size {}: {} ms, {} formulas dirty",
            size,
            duration.as_millis(),
            dirty_count
        );

        // Performance should scale reasonably (not quadratically)
        assert!(
            duration.as_millis() < (size as u128 / 10).max(100),
            "mark_dirty should scale sub-linearly with number of formulas"
        );
    }

    println!("✓ Scaling test passed");
}

#[test]
fn test_mark_dirty_hotspot_performance() {
    println!("\n=== Benchmark: mark_dirty hotspot test ===");

    const NUM_FORMULAS: usize = 1000;
    let mut graph = create_overlapping_ranges_graph(NUM_FORMULAS, true);

    // Clear initial dirty state
    let all_ids: Vec<_> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Test hotspot: a cell that affects many formulas
    let hotspot_row = 500; // Should affect most formulas
    let hotspot_col = 1;

    // Warm up
    for _ in 0..3 {
        graph
            .set_cell_value("Sheet1", hotspot_row, hotspot_col, LiteralValue::Int(1))
            .unwrap();
        graph.clear_dirty_flags(&graph.get_evaluation_vertices());
    }

    // Benchmark repeated hits to the hotspot
    let iterations = 10;
    let mut total_duration = std::time::Duration::default();
    let mut total_dirty = 0;

    for i in 0..iterations {
        let start = Instant::now();
        graph
            .set_cell_value(
                "Sheet1",
                hotspot_row,
                hotspot_col,
                LiteralValue::Int(i as i64),
            )
            .unwrap();
        let duration = start.elapsed();
        let dirty_count = graph.get_evaluation_vertices().len();

        total_duration += duration;
        total_dirty += dirty_count;

        graph.clear_dirty_flags(&graph.get_evaluation_vertices());
    }

    let avg_duration = total_duration / iterations;
    let avg_dirty = total_dirty / iterations as usize;

    println!(
        "Hotspot average: {} ms, {} formulas dirty per update",
        avg_duration.as_millis(),
        avg_dirty
    );

    // Should be consistently fast
    assert!(
        avg_duration.as_millis() < 100,
        "Average hotspot mark_dirty should be under 100ms"
    );
    assert!(
        avg_dirty > NUM_FORMULAS / 4,
        "Should affect a significant portion of formulas"
    );

    println!("✓ Hotspot performance test passed");
}

#[test]
fn test_mark_dirty_sparse_vs_dense_ranges() {
    println!("\n=== Benchmark: sparse vs dense range performance ===");

    let mut config = EvalConfig::default();
    config = config.with_range_expansion_limit(16);
    config = config.with_block_stripes(true);

    // Create sparse ranges (single columns)
    let mut sparse_graph = DependencyGraph::new_with_config(config.clone());
    for i in 0..500 {
        let col = (i % 26) + 1; // Spread across columns A-Z
        sparse_graph
            .set_cell_formula(
                "Sheet1",
                i + 1,
                27, // Formulas in column AA
                sum_range_ast(None, 1, col, 1000, col),
            )
            .unwrap();
    }

    // Create dense ranges (square blocks)
    let mut dense_graph = DependencyGraph::new_with_config(config);
    for i in 0..100 {
        let start_row = (i / 10) * 50 + 1;
        let start_col = (i % 10) * 5 + 1;
        dense_graph
            .set_cell_formula(
                "Sheet1",
                i + 1,
                27, // Formulas in column AA
                sum_range_ast(None, start_row, start_col, start_row + 49, start_col + 4),
            )
            .unwrap();
    }

    // Clear dirty state
    let sparse_ids: Vec<_> = sparse_graph.cell_to_vertex().values().copied().collect();
    sparse_graph.clear_dirty_flags(&sparse_ids);

    let dense_ids: Vec<_> = dense_graph.cell_to_vertex().values().copied().collect();
    dense_graph.clear_dirty_flags(&dense_ids);

    // Test sparse performance
    let sparse_start = Instant::now();
    sparse_graph
        .set_cell_value("Sheet1", 500, 1, LiteralValue::Int(42))
        .unwrap(); // Column A
    let sparse_duration = sparse_start.elapsed();
    let sparse_dirty = sparse_graph.get_evaluation_vertices().len();

    // Test dense performance
    let dense_start = Instant::now();
    dense_graph
        .set_cell_value("Sheet1", 25, 3, LiteralValue::Int(42))
        .unwrap(); // Should hit multiple blocks
    let dense_duration = dense_start.elapsed();
    let dense_dirty = dense_graph.get_evaluation_vertices().len();

    println!(
        "Sparse ranges (column stripes): {} ms, {} dirty",
        sparse_duration.as_millis(),
        sparse_dirty
    );
    println!(
        "Dense ranges (block stripes): {} ms, {} dirty",
        dense_duration.as_millis(),
        dense_dirty
    );

    // Both should be reasonably fast
    assert!(
        sparse_duration.as_millis() < 100,
        "Sparse ranges should be fast"
    );
    assert!(
        dense_duration.as_millis() < 100,
        "Dense ranges should be fast"
    );

    println!("✓ Sparse vs dense performance test passed");
}

#[test]
fn test_mark_dirty_cross_sheet_performance() {
    println!("\n=== Benchmark: cross-sheet range performance ===");

    let mut config = EvalConfig::default();
    config = config.with_range_expansion_limit(16);
    let mut graph = DependencyGraph::new_with_config(config);

    // Create formulas on Sheet1 that depend on ranges in Sheet2
    for i in 0..500 {
        graph
            .set_cell_formula(
                "Sheet1",
                i + 1,
                1,
                sum_range_ast(Some("Sheet2"), i + 1, 1, i + 1000, 1),
            )
            .unwrap();
    }

    // Clear dirty state
    let all_ids: Vec<_> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Test cross-sheet mark_dirty performance
    let start = Instant::now();
    graph
        .set_cell_value("Sheet2", 500, 1, LiteralValue::Int(42))
        .unwrap();
    let duration = start.elapsed();
    let dirty_count = graph.get_evaluation_vertices().len();

    println!(
        "Cross-sheet: {} ms, {} formulas dirty",
        duration.as_millis(),
        dirty_count
    );

    // Should handle cross-sheet dependencies efficiently
    assert!(
        duration.as_millis() < 200,
        "Cross-sheet mark_dirty should be reasonably fast"
    );
    assert!(dirty_count > 100, "Should affect many cross-sheet formulas");

    println!("✓ Cross-sheet performance test passed");
}

#[test]
fn test_mark_dirty_memory_efficiency() {
    println!("\n=== Benchmark: memory efficiency test ===");

    const NUM_FORMULAS: usize = 2000;
    let mut graph = create_overlapping_ranges_graph(NUM_FORMULAS, true);

    // Measure stripe storage efficiency
    let stripe_count = graph.stripe_to_dependents().len();
    let total_stripe_entries: usize = graph
        .stripe_to_dependents()
        .values()
        .map(|deps| deps.len())
        .sum();

    println!(
        "Stripes: {} unique stripes, {} total entries",
        stripe_count, total_stripe_entries
    );

    // With 2000 overlapping column ranges, we should have far fewer stripes than
    // we would have individual cell dependencies
    let estimated_cell_deps = NUM_FORMULAS * 1000; // Each formula depends on 1000 cells
    let compression_ratio = estimated_cell_deps as f64 / total_stripe_entries as f64;

    println!(
        "Estimated compression: {:.1}x fewer entries than cell-based tracking",
        compression_ratio
    );

    // Stripe model should provide significant compression
    assert!(
        compression_ratio > 10.0,
        "Stripe model should compress dependencies significantly"
    );
    assert!(
        stripe_count < NUM_FORMULAS,
        "Should have fewer stripes than formulas"
    );

    // Clear and test that memory is actually freed
    let all_ids: Vec<_> = graph.cell_to_vertex().values().copied().collect();
    for &id in &all_ids {
        if let Some(vertex) = graph.get_vertex(id) {
            if vertex.row.is_some() && vertex.col.is_some() && vertex.col.unwrap() == 2 {
                // Remove formula cells (column B)
                drop(graph.set_cell_value(
                    "Sheet1",
                    vertex.row.unwrap(),
                    vertex.col.unwrap(),
                    LiteralValue::Empty,
                ));
            }
        }
    }

    // After removing formulas, stripe count should be much smaller
    let remaining_stripe_count = graph.stripe_to_dependents().len();
    println!("Stripes after cleanup: {}", remaining_stripe_count);

    // Stripe cleanup should reduce memory usage (allow for complete cleanup)
    assert!(
        remaining_stripe_count <= stripe_count / 2,
        "Stripe cleanup should significantly reduce memory usage (from {} to {})",
        stripe_count,
        remaining_stripe_count
    );

    println!("✓ Memory efficiency test passed");
}

#[test]
fn test_mark_dirty_pathological_case() {
    println!("\n=== Benchmark: pathological case test ===");

    // Create a pathological case: one cell affects thousands of formulas
    let mut config = EvalConfig::default();
    config = config.with_range_expansion_limit(16);
    let mut graph = DependencyGraph::new_with_config(config);

    const NUM_FORMULAS: usize = 3000;

    // All formulas depend on A1
    for i in 0..NUM_FORMULAS {
        let formula_row = (i / 100) as u32 + 2; // Spread across rows 2-32
        let formula_col = (i % 100) as u32 + 2; // Spread across columns B-CZ

        graph
            .set_cell_formula(
                "Sheet1",
                formula_row,
                formula_col,
                sum_range_ast(None, 1, 1, 1, 1), // All depend on A1
            )
            .unwrap();
    }

    // Clear dirty state
    let all_ids: Vec<_> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);

    // Test pathological mark_dirty: changing A1 should dirty all formulas
    let start = Instant::now();
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();
    let duration = start.elapsed();
    let dirty_count = graph.get_evaluation_vertices().len();

    println!(
        "Pathological case: {} ms, {} formulas dirty",
        duration.as_millis(),
        dirty_count
    );

    // Should still handle pathological case reasonably well
    assert!(
        duration.as_millis() < 500,
        "Even pathological cases should complete in reasonable time"
    );
    assert_eq!(
        dirty_count, NUM_FORMULAS,
        "Should mark all dependent formulas dirty"
    );

    println!("✓ Pathological case test passed");
}

/// Integration test: verify that stripe-based mark_dirty produces the same
/// results as expansion-based mark_dirty, just more efficiently
#[test]
fn test_mark_dirty_correctness_vs_expansion() {
    println!("\n=== Correctness: stripe vs expansion equivalence ===");

    const TEST_FORMULAS: usize = 50; // Small enough for expansion to handle

    // Create identical graphs with different strategies
    let mut stripe_graph = create_overlapping_ranges_graph(TEST_FORMULAS, true);
    let mut expansion_graph = create_overlapping_ranges_graph(TEST_FORMULAS, false);

    // Test several different cells
    let test_cells = vec![
        (1, 1),    // Affects early formulas
        (25, 1),   // Affects middle formulas
        (49, 1),   // Affects later formulas
        (999, 1),  // Affects most formulas
        (1500, 1), // Affects fewer formulas
    ];

    for (test_row, test_col) in test_cells {
        // Clear both graphs
        let stripe_ids: Vec<_> = stripe_graph.cell_to_vertex().values().copied().collect();
        stripe_graph.clear_dirty_flags(&stripe_ids);

        let expansion_ids: Vec<_> = expansion_graph.cell_to_vertex().values().copied().collect();
        expansion_graph.clear_dirty_flags(&expansion_ids);

        // Make the same change to both
        stripe_graph
            .set_cell_value("Sheet1", test_row, test_col, LiteralValue::Int(42))
            .unwrap();
        expansion_graph
            .set_cell_value("Sheet1", test_row, test_col, LiteralValue::Int(42))
            .unwrap();

        // Compare results
        let stripe_dirty = stripe_graph.get_evaluation_vertices();
        let expansion_dirty = expansion_graph.get_evaluation_vertices();

        println!(
            "Cell ({}, {}): stripe={} dirty, expansion={} dirty",
            test_row,
            test_col,
            stripe_dirty.len(),
            expansion_dirty.len()
        );

        // Should mark the same formulas dirty (or stripe should find more due to precision)
        assert!(
            stripe_dirty.len() >= expansion_dirty.len(),
            "Stripe model should find at least as many dependencies as expansion model"
        );

        // In this test case, they should actually be equal since the ranges are simple
        assert_eq!(
            stripe_dirty.len(),
            expansion_dirty.len(),
            "For simple overlapping ranges, stripe and expansion should find identical dependencies"
        );
    }

    println!("✓ Correctness test passed");
}
