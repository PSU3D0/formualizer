//! Tests for the hybrid model of range dependency management.
use super::common::eval_config_with_range_limit;
use crate::engine::{DependencyGraph, EvalConfig, StripeKey, StripeType, VertexId, block_index};
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
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 3))
        .unwrap();
    let c1_vertex = graph
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 3))
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
        dep_addrs.push((cell_ref.coord.row, cell_ref.coord.col));
    }
    dep_addrs.sort();
    let expected_addrs = vec![(1, 1), (2, 1), (3, 1), (4, 1)];
    assert_eq!(dep_addrs, expected_addrs);
}

#[test]
fn test_range_dependency_dirtiness() {
    let mut graph = DependencyGraph::new();

    // C1 depends on the range A1:A10.
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 10, 1))
        .unwrap();
    let c1_id = *graph
        .cell_to_vertex()
        .get(&crate::CellRef::new_absolute(0, 1, 3))
        .unwrap();

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
    let b1_id = *graph
        .cell_to_vertex()
        .get(&crate::CellRef::new_absolute(0, 1, 2))
        .unwrap();

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
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 3))
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
fn test_tall_range_populates_column_stripe_index() {
    let mut graph = graph_with_range_limit(4);

    // C1 = SUM(A1:A500)
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 500, 1))
        .unwrap();

    let c1_id = *graph
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 3))
        .unwrap();

    let stripes = graph.stripe_to_dependents();
    assert!(!stripes.is_empty(), "Stripes should be created");

    // Check for column stripe
    let key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 1,
    };
    assert!(stripes.contains_key(&key));
    assert!(stripes.get(&key).unwrap().contains(&c1_id));
}

#[test]
fn test_wide_range_populates_row_stripe_index() {
    let mut graph = graph_with_range_limit(4);

    // C1 = SUM(A1:Z1)
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 1, 26))
        .unwrap();

    let c1_id = *graph
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 3))
        .unwrap();

    let stripes = graph.stripe_to_dependents();
    assert!(!stripes.is_empty(), "Stripes should be created");

    // Check for row stripe
    let key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Row,
        index: 1,
    };
    assert!(stripes.contains_key(&key));
    assert!(stripes.get(&key).unwrap().contains(&c1_id));
}

#[test]
fn test_dense_range_populates_block_stripe_index() {
    let config = eval_config_with_range_limit(4).with_block_stripes(true);
    let mut graph = DependencyGraph::new_with_config(config);

    // C1 = SUM(A1:Z26)
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 26, 26))
        .unwrap();

    let c1_id = *graph
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 3))
        .unwrap();

    let stripes = graph.stripe_to_dependents();
    assert!(!stripes.is_empty(), "Stripes should be created");

    // Check for block stripe
    let key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Block,
        index: block_index(1, 1),
    };
    assert!(stripes.contains_key(&key));
    assert!(stripes.get(&key).unwrap().contains(&c1_id));
}

#[test]
fn test_formula_replacement_cleans_stripes() {
    let mut graph = graph_with_range_limit(4);

    // B1 = SUM(A1:A500)
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 500, 1))
        .unwrap();

    let key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 1,
    };
    assert!(graph.stripe_to_dependents().contains_key(&key));

    // Now, change B1 to depend on something else entirely
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 3, 500, 3))
        .unwrap();

    // The old stripe for column A should be gone
    assert!(!graph.stripe_to_dependents().contains_key(&key));

    // The new stripe for column C should exist
    let new_key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 3,
    };
    assert!(graph.stripe_to_dependents().contains_key(&new_key));
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
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 2))
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

#[test]
fn test_cross_sheet_implicit_range_stripes() {
    let mut graph = DependencyGraph::new();
    graph
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            sum_ast(1, 1, 100, 1), // This is on the current sheet
        )
        .unwrap();

    let _stripes = graph.stripe_to_dependents();
    let _key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 1,
    };
    // Note: Cross-sheet handling needs more comprehensive implementation
    // This is a placeholder test for now
}

#[test]
fn test_duplicate_vertex_not_pushed_twice() {
    let mut graph = graph_with_range_limit(4);

    // Create overlapping ranges that hit the same stripe
    // B1 = SUM(A1:A500)
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 500, 1))
        .unwrap();

    // C1 = SUM(A200:A600) - overlaps with B1's range in column A
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(200, 1, 600, 1))
        .unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 2))
        .unwrap();
    let c1_id = *graph
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 3))
        .unwrap();

    // Check that both vertices are in the column stripe for column A
    let key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 1,
    };

    let dependents = graph.stripe_to_dependents().get(&key).unwrap();
    assert!(dependents.contains(&b1_id));
    assert!(dependents.contains(&c1_id));
    assert_eq!(dependents.len(), 2); // Should be exactly 2, not duplicated
}

#[test]
fn test_row_insertion_not_panicking() {
    // Placeholder test for future row/column operations support
    // The hybrid dependency extractor should handle this gracefully
    let mut graph = DependencyGraph::new();

    // This should not panic even if we don't support row operations yet
    graph
        .set_cell_formula("Sheet1", 1, 1, sum_ast(1, 1, 100, 1))
        .unwrap();

    // In the future, row insertions would need to update stripe maps
    // For now, we just ensure the basic setup doesn't panic
    assert!(!graph.stripe_to_dependents().is_empty());
}

#[test]
fn test_threshold_stripe_interplay() {
    let mut graph = DependencyGraph::new_with_config(EvalConfig {
        range_expansion_limit: 5,
        stripe_height: 4,
        stripe_width: 4,
        ..Default::default()
    });

    // Range size = 8, larger than expansion limit (5) - should create compressed dependency
    graph
        .set_cell_formula("Sheet1", 1, 1, sum_ast(1, 1, 8, 1))
        .unwrap();

    let a1_id = *graph
        .get_vertex_id_for_address(&crate::CellRef::new_absolute(0, 1, 1))
        .unwrap();

    // Should have compressed range dependency (not expanded)
    let range_deps = graph.formula_to_range_deps();
    assert!(!range_deps.is_empty());
    assert!(range_deps.contains_key(&a1_id));

    // Should create column stripe
    let key = StripeKey {
        sheet_id: 0,
        stripe_type: StripeType::Column,
        index: 1,
    };
    assert!(graph.stripe_to_dependents().contains_key(&key));
}

#[test]
fn test_overlapping_named_range_deduplication() {
    // Placeholder test for named range support
    // When named ranges are implemented, they should be deduplicated
    // with direct range references if they overlap
    let mut graph = graph_with_range_limit(5); // Make sure range gets compressed

    // For now, just test that the graph handles basic ranges correctly
    // B1 = SUM(A1:A10) (avoid self-reference) - size 10 > limit 5
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 10, 1))
        .unwrap();

    // This is a placeholder - in the future, named ranges like "MyRange"
    // that resolve to A1:A10 should be deduplicated with direct A1:A10 references
    assert!(!graph.stripe_to_dependents().is_empty());
}
