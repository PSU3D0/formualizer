//! Tests for remove_dependent_edges stripe cleanup functionality
//!
//! These tests verify that when formulas are removed or replaced, the stripe
//! dependency tracking is properly cleaned up to prevent memory leaks and
//! stale references.

use crate::CellRef;
use crate::engine::{DependencyGraph, EvalConfig, StripeKey, StripeType};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

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
                        sheet.map(|s| format!("{s}!")).unwrap_or_default(),
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

#[test]
fn test_remove_dependent_edges_cleans_column_stripes() {
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 16; // Force stripe usage
    let mut graph = DependencyGraph::new_with_config(config);

    // Create formula B1 = SUM(A1:A1000) - should create column stripe for column A
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_range_ast(None, 1, 1, 1000, 1))
        .unwrap();

    let formula_addr = CellRef::new_absolute(0, 1, 2);
    let formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();

    // Verify stripe was created
    let column_stripe_key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 1,
    };

    assert!(
        graph
            .stripe_to_dependents()
            .contains_key(&column_stripe_key),
        "Column stripe should be created for column A"
    );
    assert!(
        graph
            .stripe_to_dependents()
            .get(&column_stripe_key)
            .unwrap()
            .contains(&formula_id),
        "Formula should be registered in column stripe"
    );

    // Replace the formula with a value (should trigger remove_dependent_edges)
    graph
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(42))
        .unwrap();

    // Verify stripe entry was cleaned up
    let stripes = graph.stripe_to_dependents();
    if let Some(dependents) = stripes.get(&column_stripe_key) {
        assert!(
            !dependents.contains(&formula_id),
            "Formula should be removed from column stripe after replacement"
        );
        // If no dependents remain, the stripe entry itself should be removed
        if dependents.is_empty() {
            panic!("Empty stripe entry should have been removed entirely");
        }
    }
    // It's also acceptable for the entire stripe key to be removed if it's empty
}

#[test]
fn test_remove_dependent_edges_cleans_row_stripes() {
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 16;
    let mut graph = DependencyGraph::new_with_config(config);

    // Create formula A2 = SUM(A1:Z1) - should create row stripe for row 1
    graph
        .set_cell_formula("Sheet1", 2, 1, sum_range_ast(None, 1, 1, 1, 26))
        .unwrap();

    let formula_addr = CellRef::new_absolute(0, 2, 1);
    let formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();

    // Verify stripe was created
    let row_stripe_key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Row,
        index: 1,
    };

    assert!(
        graph.stripe_to_dependents().contains_key(&row_stripe_key),
        "Row stripe should be created for row 1"
    );
    assert!(
        graph
            .stripe_to_dependents()
            .get(&row_stripe_key)
            .unwrap()
            .contains(&formula_id),
        "Formula should be registered in row stripe"
    );

    // Replace the formula with different formula (should trigger remove_dependent_edges)
    graph
        .set_cell_formula(
            "Sheet1",
            2,
            1,
            sum_range_ast(None, 2, 1, 2, 26), // Now depends on row 2
        )
        .unwrap();

    // Verify old stripe entry was cleaned up
    let stripes = graph.stripe_to_dependents();
    if let Some(dependents) = stripes.get(&row_stripe_key) {
        assert!(
            !dependents.contains(&formula_id),
            "Formula should be removed from old row stripe after replacement"
        );
    }

    // Verify new stripe was created
    let new_row_stripe_key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Row,
        index: 2,
    };
    assert!(
        stripes.contains_key(&new_row_stripe_key),
        "New row stripe should be created for row 2"
    );
    assert!(
        stripes
            .get(&new_row_stripe_key)
            .unwrap()
            .contains(&formula_id),
        "Formula should be registered in new row stripe"
    );
}

#[test]
fn test_remove_dependent_edges_cleans_block_stripes() {
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 16;
    config.enable_block_stripes = true;
    let mut graph = DependencyGraph::new_with_config(config);

    // Create formula AA1 = SUM(A1:Z26) - should create block stripe
    graph
        .set_cell_formula("Sheet1", 1, 27, sum_range_ast(None, 1, 1, 26, 26))
        .unwrap();

    let formula_addr = CellRef::new_absolute(0, 1, 27);
    let formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();

    // Verify block stripe was created
    let block_stripe_key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Block,
        index: crate::engine::graph::block_index(1, 1),
    };

    assert!(
        graph.stripe_to_dependents().contains_key(&block_stripe_key),
        "Block stripe should be created for block containing A1:Z26"
    );
    assert!(
        graph
            .stripe_to_dependents()
            .get(&block_stripe_key)
            .unwrap()
            .contains(&formula_id),
        "Formula should be registered in block stripe"
    );

    // Remove the formula entirely by setting to empty value
    graph
        .set_cell_value("Sheet1", 1, 27, LiteralValue::Empty)
        .unwrap();

    // Verify stripe entry was cleaned up
    let stripes = graph.stripe_to_dependents();
    if let Some(dependents) = stripes.get(&block_stripe_key) {
        assert!(
            !dependents.contains(&formula_id),
            "Formula should be removed from block stripe after removal"
        );
    }
}

#[test]
fn test_remove_dependent_edges_handles_multiple_stripes() {
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 16;
    config.enable_block_stripes = true;
    let mut graph = DependencyGraph::new_with_config(config);

    // Create formula that spans multiple stripes: B100 = SUM(A1:A500)
    // This should create column stripes for column A
    graph
        .set_cell_formula("Sheet1", 100, 2, sum_range_ast(None, 1, 1, 500, 1))
        .unwrap();

    let formula_addr = CellRef::new_absolute(0, 100, 2);
    let formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();

    // Should create column stripe
    let column_stripe_key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 1,
    };

    assert!(
        graph
            .stripe_to_dependents()
            .contains_key(&column_stripe_key),
        "Column stripe should be created"
    );
    assert!(
        graph
            .stripe_to_dependents()
            .get(&column_stripe_key)
            .unwrap()
            .contains(&formula_id),
        "Formula should be in column stripe"
    );

    // Replace with a formula that creates different stripes: B100 = SUM(A1:Z500)
    // This should create both row and column stripes
    graph
        .set_cell_formula("Sheet1", 100, 2, sum_range_ast(None, 1, 1, 500, 26))
        .unwrap();

    // The formula ID should remain the same (same cell)
    let updated_formula_id = *graph.get_vertex_id_for_address(&formula_addr).unwrap();
    assert_eq!(
        formula_id, updated_formula_id,
        "Formula ID should remain the same for same cell"
    );

    let stripes = graph.stripe_to_dependents();

    // The range A1:Z500 (height=500, width=26) with block stripes enabled
    // should create block stripes instead of column stripes

    // Calculate expected block stripes for A1:Z500
    let start_block_row = 1 / 32; // BLOCK_H = 32
    let end_block_row = 500 / 32;
    let start_block_col = 1 / 32; // BLOCK_W = 32
    let end_block_col = 26 / 32;

    // Since both height > 1 and width > 1, and block stripes are enabled,
    // it should create block stripes
    let mut found_formula_in_blocks = false;
    for block_row in start_block_row..=end_block_row {
        for block_col in start_block_col..=end_block_col {
            let block_key = StripeKey {
                sheet_id: 0,
                stripe_type: StripeType::Block,
                index: crate::engine::graph::block_index(block_row * 32, block_col * 32),
            };
            if let Some(deps) = stripes.get(&block_key) {
                if deps.contains(&formula_id) {
                    found_formula_in_blocks = true;
                }
            }
        }
    }

    assert!(
        found_formula_in_blocks,
        "Formula should be found in at least one block stripe for range A1:Z500"
    );
}

#[test]
fn test_empty_stripes_are_removed() {
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 16;
    let mut graph = DependencyGraph::new_with_config(config);

    // Create two formulas that depend on the same column
    // B1 = SUM(A1:A100)
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_range_ast(None, 1, 1, 100, 1))
        .unwrap();

    // C1 = SUM(A1:A200) (overlapping range)
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_range_ast(None, 1, 1, 200, 1))
        .unwrap();

    let formula_b1_id = *graph
        .get_vertex_id_for_address(&CellRef::new_absolute(0, 1, 2))
        .unwrap();
    let formula_c1_id = *graph
        .get_vertex_id_for_address(&CellRef::new_absolute(0, 1, 3))
        .unwrap();

    let column_stripe_key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 1,
    };

    // Both formulas should be in the same column stripe
    let stripes = graph.stripe_to_dependents();
    assert!(
        stripes.contains_key(&column_stripe_key),
        "Column stripe should exist"
    );
    let dependents = stripes.get(&column_stripe_key).unwrap();
    assert!(
        dependents.contains(&formula_b1_id),
        "Formula B1 should be in stripe"
    );
    assert!(
        dependents.contains(&formula_c1_id),
        "Formula C1 should be in stripe"
    );
    assert_eq!(
        dependents.len(),
        2,
        "Stripe should have exactly 2 dependents"
    );

    // Remove first formula
    graph
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(42))
        .unwrap();

    // Stripe should still exist but only have one dependent
    let stripes = graph.stripe_to_dependents();
    assert!(
        stripes.contains_key(&column_stripe_key),
        "Column stripe should still exist"
    );
    let dependents = stripes.get(&column_stripe_key).unwrap();
    assert!(
        !dependents.contains(&formula_b1_id),
        "Formula B1 should be removed from stripe"
    );
    assert!(
        dependents.contains(&formula_c1_id),
        "Formula C1 should still be in stripe"
    );
    assert_eq!(
        dependents.len(),
        1,
        "Stripe should have exactly 1 dependent"
    );

    // Remove second formula
    graph
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(43))
        .unwrap();

    // Now the stripe should be completely removed
    let stripes = graph.stripe_to_dependents();
    assert!(
        !stripes.contains_key(&column_stripe_key),
        "Empty stripe should be completely removed"
    );
}

#[test]
fn test_cross_sheet_stripe_cleanup() {
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 16;
    let mut graph = DependencyGraph::new_with_config(config);

    graph.add_sheet("Sheet2").unwrap();
    graph.add_sheet("Sheet3").unwrap();

    // Create formula on Sheet1 that depends on Sheet2
    // Sheet1!A1 = SUM(Sheet2!A1:A100)
    graph
        .set_cell_formula("Sheet1", 1, 1, sum_range_ast(Some("Sheet2"), 1, 1, 100, 1))
        .unwrap();

    let formula_id = *graph
        .get_vertex_id_for_address(&CellRef::new_absolute(0, 1, 1))
        .unwrap();

    // Should create stripe on Sheet2
    let sheet2_column_stripe_key = StripeKey {
        sheet_id: 1, // Note: different sheet
        stripe_type: StripeType::Column,
        index: 1,
    };

    assert!(
        graph
            .stripe_to_dependents()
            .contains_key(&sheet2_column_stripe_key),
        "Column stripe should be created on Sheet2"
    );
    assert!(
        graph
            .stripe_to_dependents()
            .get(&sheet2_column_stripe_key)
            .unwrap()
            .contains(&formula_id),
        "Formula should be registered in Sheet2 column stripe"
    );

    // Replace with formula depending on different sheet
    // Sheet1!A1 = SUM(Sheet3!A1:A100)
    graph
        .set_cell_formula("Sheet1", 1, 1, sum_range_ast(Some("Sheet3"), 1, 1, 100, 1))
        .unwrap();

    // Old Sheet2 stripe should be cleaned up
    let stripes = graph.stripe_to_dependents();
    if let Some(dependents) = stripes.get(&sheet2_column_stripe_key) {
        assert!(
            !dependents.contains(&formula_id),
            "Formula should be removed from Sheet2 stripe after replacement"
        );
    }

    // New Sheet3 stripe should be created
    let sheet3_column_stripe_key = StripeKey {
        sheet_id: 2,
        stripe_type: StripeType::Column,
        index: 1,
    };
    assert!(
        stripes.contains_key(&sheet3_column_stripe_key),
        "Column stripe should be created on Sheet3"
    );
    assert!(
        stripes
            .get(&sheet3_column_stripe_key)
            .unwrap()
            .contains(&formula_id),
        "Formula should be registered in Sheet3 column stripe"
    );
}

#[test]
fn test_mixed_stripe_types_cleanup() {
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 16;
    config.enable_block_stripes = true;
    let mut graph = DependencyGraph::new_with_config(config);

    // Create formula that generates multiple stripe types
    // A1 = SUM(B1:Z100) - creates row stripes and column stripes
    graph
        .set_cell_formula("Sheet1", 1, 1, sum_range_ast(None, 1, 2, 100, 26))
        .unwrap();

    let formula_id = *graph
        .get_vertex_id_for_address(&CellRef::new_absolute(0, 1, 1))
        .unwrap();

    // Should create block stripes for the range B1:Z100 (since block stripes are enabled)
    // The range spans from (1,2) to (100,26), so it covers block(s) starting at (0,0)
    let mut expected_block_stripes = Vec::new();

    // Calculate which blocks are covered by the range B1:Z100
    let start_block_row = 1 / 32; // BLOCK_H = 32
    let end_block_row = 100 / 32;
    let start_block_col = 2 / 32; // BLOCK_W = 32  
    let end_block_col = 26 / 32;

    for block_row in start_block_row..=end_block_row {
        for block_col in start_block_col..=end_block_col {
            expected_block_stripes.push(StripeKey {
                sheet_id: 0,
                stripe_type: StripeType::Block,
                index: crate::engine::graph::block_index(block_row * 32, block_col * 32),
            });
        }
    }

    let stripes = graph.stripe_to_dependents();

    for stripe_key in &expected_block_stripes {
        assert!(
            stripes.contains_key(stripe_key),
            "Block stripe for index {} should exist",
            stripe_key.index
        );
        assert!(
            stripes.get(stripe_key).unwrap().contains(&formula_id),
            "Formula should be in block stripe {}",
            stripe_key.index
        );
    }

    // Replace with a small formula
    // A1 = SUM(B1:B2) - much smaller range, should clean up most stripes
    graph
        .set_cell_formula("Sheet1", 1, 1, sum_range_ast(None, 1, 2, 2, 2))
        .unwrap();

    let stripes = graph.stripe_to_dependents();

    // The small range B1:B2 (size=2) should be expanded to individual cells since 2 ≤ 16
    // So there should be no stripes for the new formula - it uses direct cell dependencies

    // Verify that no stripes contain our formula after replacement
    for (key, deps) in stripes.iter() {
        assert!(
            !deps.contains(&formula_id),
            "Formula should not be in any stripe after replacement with small range, found in {key:?}"
        );
    }

    // Old block stripes should be gone or not contain our formula
    for stripe_key in &expected_block_stripes {
        if let Some(dependents) = stripes.get(stripe_key) {
            assert!(
                !dependents.contains(&formula_id),
                "Formula should be removed from block stripe {}",
                stripe_key.index
            );
        }
    }
}

#[test]
fn test_formula_replacement_doesnt_affect_other_formulas() {
    let mut config = EvalConfig::default();
    config.range_expansion_limit = 16;
    let mut graph = DependencyGraph::new_with_config(config);

    // Create two formulas using the same column stripe
    // B1 = SUM(A1:A100)
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_range_ast(None, 1, 1, 100, 1))
        .unwrap();

    // B2 = SUM(A1:A200)
    graph
        .set_cell_formula("Sheet1", 2, 2, sum_range_ast(None, 1, 1, 200, 1))
        .unwrap();

    let formula_b1_id = *graph
        .get_vertex_id_for_address(&CellRef::new_absolute(0, 1, 2))
        .unwrap();
    let formula_b2_id = *graph
        .get_vertex_id_for_address(&CellRef::new_absolute(0, 2, 2))
        .unwrap();

    let column_stripe_key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 1,
    };

    // Both should be in the stripe
    let stripes = graph.stripe_to_dependents();
    let dependents = stripes.get(&column_stripe_key).unwrap();
    assert!(
        dependents.contains(&formula_b1_id),
        "B1 should be in stripe"
    );
    assert!(
        dependents.contains(&formula_b2_id),
        "B2 should be in stripe"
    );

    // Replace B1 with a formula that doesn't use column A
    // B1 = SUM(C1:C100)
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_range_ast(None, 1, 3, 100, 3))
        .unwrap();

    // B1 should be removed from column A stripe, but B2 should remain
    let stripes = graph.stripe_to_dependents();
    let dependents = stripes.get(&column_stripe_key).unwrap();
    assert!(
        !dependents.contains(&formula_b1_id),
        "B1 should be removed from column A stripe"
    );
    assert!(
        dependents.contains(&formula_b2_id),
        "B2 should still be in column A stripe"
    );

    // B1 should now be in column C stripe
    let column_c_stripe_key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 3,
    };
    assert!(
        stripes.contains_key(&column_c_stripe_key),
        "Column C stripe should exist"
    );
    assert!(
        stripes
            .get(&column_c_stripe_key)
            .unwrap()
            .contains(&formula_b1_id),
        "B1 should be in column C stripe"
    );
}
