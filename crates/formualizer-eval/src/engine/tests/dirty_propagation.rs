use super::common::get_vertex_ids_in_order;
use crate::engine::{DependencyGraph, VertexKind};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

#[test]
fn test_mark_dirty_propagation() {
    let mut graph = DependencyGraph::new();

    // Create dependency chain: A1 → A2 → A3 → A4
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();

    // A2 = A1
    let ast_ref_a1 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::cell(None, 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 2, 1, ast_ref_a1).unwrap();

    // A3 = A2
    let ast_ref_a2 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A2".to_string(),
            reference: ReferenceType::cell(None, 2, 1),
        },
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 3, 1, ast_ref_a2).unwrap();

    // A4 = A3
    let ast_ref_a3 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A3".to_string(),
            reference: ReferenceType::cell(None, 3, 1),
        },
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 4, 1, ast_ref_a3).unwrap();

    // Clear all dirty flags first (they were set during formula creation)
    let all_vertex_ids = get_vertex_ids_in_order(&graph);
    graph.clear_dirty_flags(&all_vertex_ids);

    // Verify all are clean
    for &vertex_id in &all_vertex_ids {
        assert!(
            !graph.is_dirty(vertex_id),
            "Vertex should be clean after clearing"
        );
    }

    // Now change A1 - should propagate to A2, A3, A4
    let summary = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(20))
        .unwrap();

    // All 4 vertices should be affected (A1 changed, A2/A3/A4 became dirty)
    assert_eq!(summary.affected_vertices.len(), 4);

    // Verify dirty flags are set correctly
    let vertex_ids = get_vertex_ids_in_order(&graph);

    // A1 is a value, so no dirty flag to check
    assert!(
        !graph.is_dirty(vertex_ids[0]),
        "A1 should be clean, as it is a value"
    );
    assert!(
        graph.get_vertex_kind(vertex_ids[0]) == VertexKind::Cell,
        "A1 should be a value"
    );

    // A2, A3, A4 should all be dirty
    for (idx, &vertex_id) in vertex_ids.iter().enumerate().skip(1).take(3) {
        assert!(
            graph.is_dirty(vertex_id),
            "A{} should be dirty after A1 changed",
            idx + 1
        );
        assert!(
            graph.get_vertex_kind(vertex_id) == VertexKind::FormulaScalar,
            "A{} should be a formula",
            idx + 1
        );
    }

    // Verify get_evaluation_vertices includes all dirty ones
    let eval_vertices = graph.get_evaluation_vertices();
    assert!(eval_vertices.len() >= 3); // At least A2, A3, A4
}

#[test]
fn test_mark_dirty_diamond_dependency() {
    let mut graph = DependencyGraph::new();

    // Create diamond dependency: A1 → A2, A1 → A3, A2 → A4, A3 → A4
    //     A1
    //    / \
    //   A2  A3
    //    \ /
    //     A4

    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();

    // A2 = A1
    let ast_ref_a1 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::cell(None, 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };
    graph
        .set_cell_formula("Sheet1", 2, 1, ast_ref_a1.clone())
        .unwrap();

    // A3 = A1
    graph.set_cell_formula("Sheet1", 3, 1, ast_ref_a1).unwrap();

    // A4 = A2 + A3
    let ast_sum = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(ASTNode {
                node_type: ASTNodeType::Reference {
                    original: "A2".to_string(),
                    reference: ReferenceType::cell(None, 2, 1),
                },
                source_token: None,
                contains_volatile: false,
            }),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Reference {
                    original: "A3".to_string(),
                    reference: ReferenceType::cell(None, 3, 1),
                },
                source_token: None,
                contains_volatile: false,
            }),
        },
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 4, 1, ast_sum).unwrap();

    // Clear dirty flags
    let all_vertex_ids = get_vertex_ids_in_order(&graph);
    graph.clear_dirty_flags(&all_vertex_ids);

    // Change A1 - should mark A2, A3, A4 as dirty (but A4 only once)
    let summary = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(20))
        .unwrap();

    // Should affect A1, A2, A3, A4 (4 total)
    assert_eq!(summary.affected_vertices.len(), 4);

    // Verify A4 is only marked dirty once despite two paths from A1
    assert!(graph.is_dirty(all_vertex_ids[3]), "A4 should be dirty");
    assert!(
        graph.get_vertex_kind(all_vertex_ids[3]) == VertexKind::FormulaScalar,
        "A4 should be a formula"
    );
}

#[test]
fn test_dirty_flag_clearing() {
    let mut graph = DependencyGraph::new();

    // Create A1 = 10, A2 = A1
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();

    let ast_ref_a1 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::cell(None, 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 2, 1, ast_ref_a1).unwrap();

    // Both should be dirty after creation
    let all_vertex_ids = get_vertex_ids_in_order(&graph);
    assert!(
        graph.is_dirty(all_vertex_ids[1]),
        "A2 should be dirty after creation"
    );
    assert!(
        graph.get_vertex_kind(all_vertex_ids[1]) == VertexKind::FormulaScalar,
        "A2 should be a formula"
    );

    // Clear dirty flags
    let vertex_ids = vec![all_vertex_ids[1]]; // Just A2 (second vertex)
    graph.clear_dirty_flags(&vertex_ids);

    // A2 should no longer be dirty
    assert!(
        !graph.is_dirty(all_vertex_ids[1]),
        "A2 should be clean after clearing"
    );

    // get_evaluation_vertices should not include A2
    let eval_vertices = graph.get_evaluation_vertices();
    let all_vertex_ids = get_vertex_ids_in_order(&graph);
    assert!(!eval_vertices.contains(&all_vertex_ids[1]));

    // But if we change A1 again, A2 should become dirty
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(30))
        .unwrap();

    assert!(
        graph.is_dirty(all_vertex_ids[1]),
        "A2 should be dirty after A1 changed"
    );
}

#[test]
fn test_volatile_vertex_handling() {
    let mut graph = DependencyGraph::new();
    crate::builtins::random::register_builtins(); // Ensure RAND is registered

    // Create a volatile AST: =RAND()
    let volatile_ast = ASTNode {
        node_type: ASTNodeType::Function {
            name: "RAND".to_string(),
            args: vec![],
        },
        source_token: None,
        contains_volatile: true,
    };

    // Set A1 = RAND()
    graph
        .set_cell_formula("Sheet1", 1, 1, volatile_ast)
        .unwrap();

    // The vertex for A1 should be marked as volatile.
    let all_vertex_ids = get_vertex_ids_in_order(&graph);
    let a1_id = all_vertex_ids[0];
    let eval_vertices = graph.get_evaluation_vertices();

    // Volatile vertices are always included for evaluation.
    assert!(eval_vertices.contains(&a1_id));

    // Clear dirty flags, but volatile should remain.
    graph.clear_dirty_flags(&[a1_id]);
    let eval_vertices_after_clear = graph.get_evaluation_vertices();
    assert!(eval_vertices_after_clear.contains(&a1_id));
}

#[test]
fn test_evaluation_vertices_combined() {
    let mut graph = DependencyGraph::new();

    // Create multiple vertices with different states
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap(); // A1 - value

    let ast_literal = ASTNode {
        node_type: ASTNodeType::Literal(LiteralValue::Int(20)),
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 2, 1, ast_literal).unwrap(); // A2 - formula (dirty)

    let ast_ref = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::cell(None, 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 3, 1, ast_ref).unwrap(); // A3 - formula (dirty, depends on A1)

    // Get evaluation vertices (should include dirty formulas)
    let eval_vertices = graph.get_evaluation_vertices();

    // Should include at least the formulas that are dirty
    assert!(eval_vertices.len() >= 2); // A2 and A3 at minimum

    // Results should be sorted for deterministic behavior
    let mut sorted_eval = eval_vertices.clone();
    sorted_eval.sort();
    assert_eq!(eval_vertices, sorted_eval);
}

#[test]
fn test_dirty_propagation_performance() {
    let mut graph = DependencyGraph::new();

    // Create a larger dependency chain to test O(1) operations
    // A1 → A2 → A3 → ... → A20

    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();

    for i in 2..=20 {
        let ast_ref = ASTNode {
            node_type: ASTNodeType::Reference {
                original: format!("A{}", i - 1),
                reference: ReferenceType::cell(None, 1, 1),
            },
            source_token: None,
            contains_volatile: false,
        };
        graph.set_cell_formula("Sheet1", i, 1, ast_ref).unwrap();
    }

    // Clear all dirty flags
    let all_vertex_ids = get_vertex_ids_in_order(&graph);
    graph.clear_dirty_flags(&all_vertex_ids);

    // Time the dirty propagation
    let start = std::time::Instant::now();
    let summary = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(100))
        .unwrap();
    let elapsed = start.elapsed();

    // Should affect all 20 vertices
    assert_eq!(summary.affected_vertices.len(), 20);

    // Performance should be reasonable (this is a rough check)
    // With O(1) HashSet operations, even 20 vertices should be very fast
    assert!(
        elapsed < std::time::Duration::from_millis(10),
        "Dirty propagation took too long: {elapsed:?}"
    );

    // Verify all downstream vertices are dirty
    for (idx, &vertex_id) in all_vertex_ids.iter().enumerate().skip(1) {
        // Skip A1 (it's a value)
        assert!(graph.is_dirty(vertex_id), "A{} should be dirty", idx + 1);
        assert!(
            graph.get_vertex_kind(vertex_id) == VertexKind::FormulaScalar,
            "A{} should be a formula",
            idx + 1
        );
    }
}
