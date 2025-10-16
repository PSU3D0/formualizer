//! Tests for the precision of dirty propagation.

use crate::CellRef;
use crate::engine::DependencyGraph;
use super::common::eval_config_with_range_limit;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

/// Helper to create a SUM(range) AST node
fn sum_ast(start_row: u32, start_col: u32, end_row: u32, end_col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Function {
            name: "SUM".to_string(),
            args: vec![ASTNode {
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
            }],
        },
        source_token: None,
        contains_volatile: false,
    }
}

#[test]
fn test_change_outside_range_in_same_stripe_does_not_dirty() {
    let mut graph = DependencyGraph::new_with_config(eval_config_with_range_limit(4)); // Force compression

    // B1 = SUM(A1:A10)
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 10, 1))
        .unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&CellRef::new_absolute(0, 1, 2))
        .unwrap();

    // Clear dirty flags
    graph.clear_dirty_flags(&[b1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Change A11. This is in the same column stripe as the range A1:A10,
    // but it is outside the actual range. With the precision check in `mark_dirty`,
    // this should NOT trigger a dirty propagation.
    graph
        .set_cell_value("Sheet1", 11, 1, LiteralValue::Int(999))
        .unwrap();

    assert!(
        !graph.get_evaluation_vertices().contains(&b1_id),
        "Change in A11 (outside range A1:A10) should not dirty the dependent formula"
    );

    // For good measure, check that a change *inside* the range *does* dirty it.
    graph
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(123))
        .unwrap();

    assert!(
        graph.get_evaluation_vertices().contains(&b1_id),
        "Change in A5 (inside range A1:A10) should dirty the dependent formula"
    );
}
