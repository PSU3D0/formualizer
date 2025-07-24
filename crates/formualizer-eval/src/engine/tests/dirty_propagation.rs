use crate::engine::{DependencyGraph, VertexId, VertexKind};
use formualizer_common::LiteralValue;
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};

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
            reference: ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 1,
            },
        },
        source_token: None,
    };
    graph.set_cell_formula("Sheet1", 2, 1, ast_ref_a1).unwrap();

    // A3 = A2
    let ast_ref_a2 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A2".to_string(),
            reference: ReferenceType::Cell {
                sheet: None,
                row: 2,
                col: 1,
            },
        },
        source_token: None,
    };
    graph.set_cell_formula("Sheet1", 3, 1, ast_ref_a2).unwrap();

    // A4 = A3
    let ast_ref_a3 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A3".to_string(),
            reference: ReferenceType::Cell {
                sheet: None,
                row: 3,
                col: 1,
            },
        },
        source_token: None,
    };
    graph.set_cell_formula("Sheet1", 4, 1, ast_ref_a3).unwrap();

    // Clear all dirty flags first (they were set during formula creation)
    let all_vertices: Vec<VertexId> = (0..4).map(|i| VertexId::new(i)).collect();
    graph.clear_dirty_flags(&all_vertices);

    // Verify all are clean
    for vertex in graph.vertices() {
        match &vertex.kind {
            VertexKind::FormulaScalar { dirty, .. } => {
                assert!(!dirty, "Vertex should be clean after clearing")
            }
            VertexKind::FormulaArray { dirty, .. } => {
                assert!(!dirty, "Vertex should be clean after clearing")
            }
            _ => {} // Values don't have dirty flags
        }
    }

    // Now change A1 - should propagate to A2, A3, A4
    let summary = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(20))
        .unwrap();

    // All 4 vertices should be affected (A1 changed, A2/A3/A4 became dirty)
    assert_eq!(summary.affected_vertices.len(), 4);

    // Verify dirty flags are set correctly
    let vertices = graph.vertices();

    // A1 is a value, so no dirty flag to check
    match &vertices[0].kind {
        VertexKind::Value(_) => {} // Expected
        _ => panic!("A1 should be a value"),
    }

    // A2, A3, A4 should all be dirty
    for i in 1..4 {
        match &vertices[i].kind {
            VertexKind::FormulaScalar { dirty, .. } => {
                assert!(*dirty, "A{} should be dirty after A1 changed", i + 1);
            }
            _ => panic!("A{} should be a formula", i + 1),
        }
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
            reference: ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 1,
            },
        },
        source_token: None,
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
                    reference: ReferenceType::Cell {
                        sheet: None,
                        row: 2,
                        col: 1,
                    },
                },
                source_token: None,
            }),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Reference {
                    original: "A3".to_string(),
                    reference: ReferenceType::Cell {
                        sheet: None,
                        row: 3,
                        col: 1,
                    },
                },
                source_token: None,
            }),
        },
        source_token: None,
    };
    graph.set_cell_formula("Sheet1", 4, 1, ast_sum).unwrap();

    // Clear dirty flags
    let vertices = graph.vertices();
    let all_ids: Vec<VertexId> = (0..vertices.len())
        .map(|i| VertexId::new(i as u32))
        .collect();
    graph.clear_dirty_flags(&all_ids);

    // Change A1 - should mark A2, A3, A4 as dirty (but A4 only once)
    let summary = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(20))
        .unwrap();

    // Should affect A1, A2, A3, A4 (4 total)
    assert_eq!(summary.affected_vertices.len(), 4);

    // Verify A4 is only marked dirty once despite two paths from A1
    let vertices = graph.vertices();
    match &vertices[3].kind {
        VertexKind::FormulaScalar { dirty, .. } => {
            assert!(*dirty, "A4 should be dirty");
        }
        _ => panic!("A4 should be a formula"),
    }
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
            reference: ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 1,
            },
        },
        source_token: None,
    };
    graph.set_cell_formula("Sheet1", 2, 1, ast_ref_a1).unwrap();

    // Both should be dirty after creation
    let vertices = graph.vertices();
    match &vertices[1].kind {
        VertexKind::FormulaScalar { dirty, .. } => assert!(*dirty),
        _ => panic!("A2 should be a formula"),
    }

    // Clear dirty flags
    let vertex_ids = vec![VertexId::new(1)]; // Just A2
    graph.clear_dirty_flags(&vertex_ids);

    // A2 should no longer be dirty
    let vertices = graph.vertices();
    match &vertices[1].kind {
        VertexKind::FormulaScalar { dirty, .. } => assert!(!*dirty),
        _ => panic!("A2 should be a formula"),
    }

    // get_evaluation_vertices should not include A2
    let eval_vertices = graph.get_evaluation_vertices();
    assert!(!eval_vertices.contains(&VertexId::new(1)));

    // But if we change A1 again, A2 should become dirty
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(30))
        .unwrap();

    let vertices = graph.vertices();
    match &vertices[1].kind {
        VertexKind::FormulaScalar { dirty, .. } => assert!(*dirty),
        _ => panic!("A2 should be a formula"),
    }
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
    };

    // Set A1 = RAND()
    graph
        .set_cell_formula("Sheet1", 1, 1, volatile_ast)
        .unwrap();

    // The vertex for A1 should be marked as volatile.
    let a1_id = VertexId::new(0);
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
    };
    graph.set_cell_formula("Sheet1", 2, 1, ast_literal).unwrap(); // A2 - formula (dirty)

    let ast_ref = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 1,
            },
        },
        source_token: None,
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
                reference: ReferenceType::Cell {
                    sheet: None,
                    row: i - 1,
                    col: 1,
                },
            },
            source_token: None,
        };
        graph.set_cell_formula("Sheet1", i, 1, ast_ref).unwrap();
    }

    // Clear all dirty flags
    let all_ids: Vec<VertexId> = (0..20).map(|i| VertexId::new(i)).collect();
    graph.clear_dirty_flags(&all_ids);

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
        "Dirty propagation took too long: {:?}",
        elapsed
    );

    // Verify all downstream vertices are dirty
    let vertices = graph.vertices();
    for i in 1..20 {
        // Skip A1 (it's a value)
        match &vertices[i].kind {
            VertexKind::FormulaScalar { dirty, .. } => {
                assert!(*dirty, "A{} should be dirty", i + 1);
            }
            _ => panic!("A{} should be a formula", i + 1),
        }
    }
}
