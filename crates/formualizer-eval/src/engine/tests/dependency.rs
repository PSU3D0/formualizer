use crate::engine::{DependencyGraph, VertexKind};
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};

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
            reference: ReferenceType::Cell {
                sheet: None, // Same sheet
                row: 1,
                col: 1,
            },
        },
        source_token: None,
    };

    graph
        .set_cell_formula("Sheet1", 3, 3, ast_with_ref)
        .unwrap();

    // Verify the dependency was created
    let vertices = graph.vertices();
    assert_eq!(vertices.len(), 3); // A1, B2, C3

    // Find C3 vertex (should be the last one created)
    let c3_vertex = &vertices[2];
    assert_eq!(c3_vertex.dependencies.len(), 1);

    // The dependency should point to A1's vertex
    let a1_addr = graph
        .cell_to_vertex()
        .iter()
        .find(|(addr, _)| addr.row == 1 && addr.col == 1 && addr.sheet == "Sheet1")
        .map(|(_, &id)| id)
        .unwrap();

    assert_eq!(c3_vertex.dependencies[0], a1_addr);

    // A1 should have C3 as a dependent
    let a1_vertex = &vertices[a1_addr.as_index()];
    assert_eq!(a1_vertex.dependents.len(), 1);

    let c3_addr = graph
        .cell_to_vertex()
        .iter()
        .find(|(addr, _)| addr.row == 3 && addr.col == 3 && addr.sheet == "Sheet1")
        .map(|(_, &id)| id)
        .unwrap();

    assert_eq!(a1_vertex.dependents[0], c3_addr);
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
                    reference: ReferenceType::Cell {
                        sheet: None,
                        row: 1,
                        col: 1,
                    },
                },
                source_token: None,
            }),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Reference {
                    original: "B1".to_string(),
                    reference: ReferenceType::Cell {
                        sheet: None,
                        row: 1,
                        col: 2,
                    },
                },
                source_token: None,
            }),
        },
        source_token: None,
    };

    graph.set_cell_formula("Sheet1", 2, 1, ast_binary).unwrap();

    // Verify dependencies were extracted
    let vertices = graph.vertices();
    let a2_vertex = &vertices[2]; // Should be the A2 formula

    assert_eq!(a2_vertex.dependencies.len(), 2);

    // Both A1 and B1 should be dependencies
    let a1_addr = graph
        .cell_to_vertex()
        .iter()
        .find(|(addr, _)| addr.row == 1 && addr.col == 1)
        .map(|(_, &id)| id)
        .unwrap();
    let b1_addr = graph
        .cell_to_vertex()
        .iter()
        .find(|(addr, _)| addr.row == 1 && addr.col == 2)
        .map(|(_, &id)| id)
        .unwrap();

    assert!(a2_vertex.dependencies.contains(&a1_addr));
    assert!(a2_vertex.dependencies.contains(&b1_addr));
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
            reference: ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 1,
            },
        },
        source_token: None,
    };

    graph.set_cell_formula("Sheet1", 2, 1, ast_ref_a1).unwrap();

    // Verify initial edges
    let vertices = graph.vertices();
    let a1_vertex = &vertices[0];
    let a2_vertex = &vertices[1];

    assert_eq!(a2_vertex.dependencies.len(), 1);
    assert_eq!(a1_vertex.dependents.len(), 1);

    // Now update A2 to reference B1 instead
    graph
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))
        .unwrap(); // Create B1

    let ast_ref_b1 = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "B1".to_string(),
            reference: ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 2,
            },
        },
        source_token: None,
    };

    graph.set_cell_formula("Sheet1", 2, 1, ast_ref_b1).unwrap();

    // Verify edges were updated
    let vertices = graph.vertices();
    let a1_vertex = &vertices[0];
    let a2_vertex = &vertices[1];
    let b1_vertex = &vertices[2];

    // A1 should no longer have A2 as dependent
    assert_eq!(a1_vertex.dependents.len(), 0);

    // A2 should now depend on B1
    let b1_addr = graph
        .cell_to_vertex()
        .iter()
        .find(|(addr, _)| addr.row == 1 && addr.col == 2)
        .map(|(_, &id)| id)
        .unwrap();

    assert_eq!(a2_vertex.dependencies.len(), 1);
    assert_eq!(a2_vertex.dependencies[0], b1_addr);

    // B1 should have A2 as dependent
    assert_eq!(b1_vertex.dependents.len(), 1);
}

#[test]
fn test_circular_dependency_detection() {
    let mut graph = DependencyGraph::new();

    // Try to create A1 = A1 (self-reference)
    let ast_self_ref = ASTNode {
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

    let result = graph.set_cell_formula("Sheet1", 1, 1, ast_self_ref);

    // Should fail with circular reference error
    assert!(result.is_err());
    match result.unwrap_err().kind {
        ExcelErrorKind::Circ => {} // Expected
        other => panic!("Expected circular reference error, got {:?}", other),
    }

    // A1 should be an empty placeholder, not a formula
    let vertices = graph.vertices();
    assert_eq!(vertices.len(), 1);
    match &vertices[0].kind {
        VertexKind::Empty => {} // Expected
        other => panic!(
            "A1 should be an Empty vertex after failed formula update, but was {:?}",
            other
        ),
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
            reference: ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 2,
            },
        },
        source_token: None,
    };

    // Create A1 = B1 (B1 doesn't exist yet, so this should work)
    graph.set_cell_formula("Sheet1", 1, 1, ast_ref_b1).unwrap();

    // Now try to create B1 = A1 (should create a cycle)
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

    // This should work for now (cycle detection will be enhanced in Phase 2)
    // For basic implementation, we only detect immediate self-references
    let result = graph.set_cell_formula("Sheet1", 1, 2, ast_ref_a1);

    // For Milestone 1.2, this should succeed (we'll add full cycle detection later)
    assert!(result.is_ok());

    // Verify the dependency chain was created
    let vertices = graph.vertices();
    assert_eq!(vertices.len(), 2);

    // Both should be formulas with dependencies
    match &vertices[0].kind {
        VertexKind::FormulaScalar { .. } => {}
        _ => panic!("A1 should be a formula"),
    }

    match &vertices[1].kind {
        VertexKind::FormulaScalar { .. } => {}
        _ => panic!("B1 should be a formula"),
    }
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
            reference: ReferenceType::Cell {
                sheet: Some("Sheet1".to_string()),
                row: 1,
                col: 1,
            },
        },
        source_token: None,
    };

    graph
        .set_cell_formula("Sheet2", 1, 1, ast_cross_sheet)
        .unwrap();

    // Verify cross-sheet dependency
    let vertices = graph.vertices();
    assert_eq!(vertices.len(), 2);

    let sheet1_addr = graph
        .cell_to_vertex()
        .iter()
        .find(|(addr, _)| addr.sheet == "Sheet1")
        .map(|(_, &id)| id)
        .unwrap();

    let sheet2_addr = graph
        .cell_to_vertex()
        .iter()
        .find(|(addr, _)| addr.sheet == "Sheet2")
        .map(|(_, &id)| id)
        .unwrap();

    let sheet2_vertex = &vertices[sheet2_addr.as_index()];
    let sheet1_vertex = &vertices[sheet1_addr.as_index()];

    // Sheet2!A1 should depend on Sheet1!A1
    assert_eq!(sheet2_vertex.dependencies.len(), 1);
    assert_eq!(sheet2_vertex.dependencies[0], sheet1_addr);

    // Sheet1!A1 should have Sheet2!A1 as dependent
    assert_eq!(sheet1_vertex.dependents.len(), 1);
    assert_eq!(sheet1_vertex.dependents[0], sheet2_addr);
}
