use super::common::abs_cell_ref;
use crate::engine::{DependencyGraph, VertexKind};
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

#[test]
fn test_dependency_extraction_from_ast() {
    let mut graph = DependencyGraph::new();

    // Create some cells to reference
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Int(20))
        .unwrap();

    // Create a formula that references A1 (Sheet1:1:1)
    let ast_with_ref = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::cell(None, 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };

    graph
        .set_cell_formula("Sheet1", 3, 3, ast_with_ref)
        .unwrap();

    // Verify the dependency was created
    assert_eq!(graph.vertex_len(), 3); // A1, B2, C3

    // Find C3 vertex (should be the last one created)
    let c3_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 3, 3))
        .unwrap();
    assert_eq!(graph.get_dependencies(c3_vertex_id).len(), 1);

    // The dependency should point to A1's vertex
    let a1_addr = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();

    assert_eq!(graph.get_dependencies(c3_vertex_id)[0], a1_addr);

    // A1 should have C3 as a dependent
    let a1_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();
    assert_eq!(graph.get_dependents(a1_vertex_id).len(), 1);

    let c3_addr = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 3, 3))
        .unwrap();

    assert_eq!(graph.get_dependents(a1_vertex_id)[0], c3_addr);
}

#[test]
fn test_dependency_extraction_multiple_references() {
    let mut graph = DependencyGraph::new();

    // Create cells A1 and B1
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))
        .unwrap();

    // Create a binary operation A1 + B1
    let ast_binary = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(ASTNode {
                node_type: ASTNodeType::Reference {
                    original: "A1".to_string(),
                    reference: ReferenceType::cell(None, 1, 1),
                },
                source_token: None,
                contains_volatile: false,
            }),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Reference {
                    original: "B1".to_string(),
                    reference: ReferenceType::cell(None, 1, 2),
                },
                source_token: None,
                contains_volatile: false,
            }),
        },
        source_token: None,
        contains_volatile: false,
    };

    graph.set_cell_formula("Sheet1", 2, 1, ast_binary).unwrap();

    // Verify dependencies were extracted
    let a2_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 2, 1))
        .unwrap();
    let dependencies = graph.get_dependencies(a2_vertex_id);

    assert_eq!(dependencies.len(), 2);

    // Both A1 and B1 should be dependencies
    let a1_addr = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();
    let b1_addr = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // A1 and B1 are value cells, not formulas, so they don't have dependencies
    // We should check if A2's dependencies contain A1 and B1
    assert!(dependencies.contains(&a1_addr));
    assert!(dependencies.contains(&b1_addr));
}

#[test]
fn test_dependency_edge_management() {
    let mut graph = DependencyGraph::new();

    // Create A1
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();

    // Create A2 = A1
    let ast_ref_a1 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::cell(None, 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };

    graph.set_cell_formula("Sheet1", 2, 1, ast_ref_a1).unwrap();

    // Verify initial edges
    let a1_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();
    let a2_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 2, 1))
        .unwrap();

    assert_eq!(graph.get_dependencies(a2_vertex_id).len(), 1);
    assert_eq!(graph.get_dependents(a1_vertex_id).len(), 1);

    // Now update A2 to reference B1 instead
    graph
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))
        .unwrap(); // Create B1

    let ast_ref_b1 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "B1".to_string(),
            reference: ReferenceType::cell(None, 1, 2),
        },
        source_token: None,
        contains_volatile: false,
    };

    graph.set_cell_formula("Sheet1", 2, 1, ast_ref_b1).unwrap();

    // Verify edges were updated
    let a1_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();
    let a2_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 2, 1))
        .unwrap();
    let b1_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // A1 should no longer have A2 as dependent
    assert_eq!(graph.get_dependents(a1_vertex_id).len(), 0);

    // A2 should now depend on B1
    let b1_addr = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    assert_eq!(graph.get_dependencies(a2_vertex_id).len(), 1);
    assert_eq!(graph.get_dependencies(a2_vertex_id)[0], b1_addr);

    // B1 should have A2 as dependent
    assert_eq!(graph.get_dependents(b1_vertex_id).len(), 1);
}

#[test]
fn test_circular_dependency_detection() {
    let mut graph = DependencyGraph::new();

    // Try to create A1 = A1 (self-reference)
    let ast_self_ref = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::cell(None, 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };

    let result = graph.set_cell_formula("Sheet1", 1, 1, ast_self_ref);

    // Should fail with circular reference error
    assert!(result.is_err());
    match result.unwrap_err().kind {
        ExcelErrorKind::Circ => {} // Expected
        other => panic!("Expected circular reference error, got {other:?}"),
    }

    // A1 should be an empty placeholder, not a formula
    assert_eq!(graph.vertex_len(), 1);
    let a1_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();
    match &graph.get_vertex_kind(a1_vertex_id) {
        VertexKind::Empty => {} // Expected
        other => {
            panic!("A1 should be an Empty vertex after failed formula update, but was {other:?}")
        }
    }
}

#[test]
fn test_complex_circular_dependency() {
    let mut graph = DependencyGraph::new();

    // This test will verify more complex circular dependency detection
    // For now, we'll create a simple case and expand later

    // Create A1 = B1, B1 = A1 scenario
    let ast_ref_b1 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "B1".to_string(),
            reference: ReferenceType::cell(None, 1, 2),
        },
        source_token: None,
        contains_volatile: false,
    };

    // Create A1 = B1 (B1 doesn't exist yet, so this should work)
    graph.set_cell_formula("Sheet1", 1, 1, ast_ref_b1).unwrap();

    // Now try to create B1 = A1 (should create a cycle)
    let ast_ref_a1 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::cell(None, 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };

    // For Milestone 1.2, creating this dependency is allowed.
    // The full cycle detection is handled by the Scheduler in Milestone 2.
    let result = graph.set_cell_formula("Sheet1", 1, 2, ast_ref_a1);
    assert!(result.is_ok());

    // Verify the dependency chain was created
    assert_eq!(graph.vertex_len(), 2);

    let a1_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();
    let b1_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // A1 should depend on B1
    assert_eq!(graph.get_dependencies(a1_vertex_id).len(), 1);
    assert_eq!(graph.get_dependencies(b1_vertex_id).len(), 1);

    // B1 should depend on A1
    assert_eq!(graph.get_dependencies(b1_vertex_id).len(), 1);
    assert_eq!(graph.get_dependencies(a1_vertex_id).len(), 1);

    // A1 should have B1 as a dependent
    assert_eq!(graph.get_dependents(a1_vertex_id).len(), 1);
    assert_eq!(graph.get_dependents(b1_vertex_id).len(), 1);

    // B1 should have A1 as a dependent
    assert_eq!(graph.get_dependents(b1_vertex_id).len(), 1);
    assert_eq!(graph.get_dependents(a1_vertex_id).len(), 1);
}

#[test]
fn test_cross_sheet_dependencies() {
    let mut graph = DependencyGraph::new();

    // Create Sheet1!A1 = 10
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();

    // Create Sheet2!A1 = Sheet1!A1
    let ast_cross_sheet = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "Sheet1!A1".to_string(),
            reference: ReferenceType::cell(Some("Sheet1".to_string()), 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };

    graph
        .set_cell_formula("Sheet2", 1, 1, ast_cross_sheet)
        .unwrap();

    // Verify cross-sheet dependency
    assert_eq!(graph.vertex_len(), 2);

    let sheet1_addr = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();

    let sheet2_addr = *graph
        .get_vertex_id_for_address(&abs_cell_ref(1, 1, 1))
        .unwrap();

    let sheet2_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(1, 1, 1))
        .unwrap();
    let sheet1_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 1))
        .unwrap();

    // Sheet2!A1 should depend on Sheet1!A1
    assert_eq!(graph.get_dependencies(sheet2_vertex_id).len(), 1);
    assert_eq!(graph.get_dependencies(sheet2_vertex_id)[0], sheet1_addr);

    // Sheet1!A1 should have Sheet2!A1 as dependent
    assert_eq!(graph.get_dependents(sheet1_vertex_id).len(), 1);
    assert_eq!(graph.get_dependents(sheet1_vertex_id)[0], sheet2_addr);
}

#[test]
fn test_relative_sheet_dependency() {
    let mut graph = DependencyGraph::new();

    // Create Sheet2!A1 = 10
    graph
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Int(10))
        .unwrap();

    // Create Sheet2!B1 = A1 (which should resolve to Sheet2!A1)
    let ast_relative_ref = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: ReferenceType::cell(None, 1, 1),
        },
        source_token: None,
        contains_volatile: false,
    };

    graph
        .set_cell_formula("Sheet2", 1, 2, ast_relative_ref)
        .unwrap();

    // Verify the dependency is within Sheet2
    assert_eq!(graph.vertex_len(), 2);

    let sheet2_a1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(1, 1, 1))
        .unwrap();

    let sheet2_b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(1, 1, 2))
        .unwrap();

    let sheet2_b1_vertex_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(1, 1, 2))
        .unwrap();
    assert_eq!(graph.get_dependencies(sheet2_b1_vertex_id).len(), 1);
    assert_eq!(graph.get_dependencies(sheet2_b1_vertex_id)[0], sheet2_a1_id);
}
