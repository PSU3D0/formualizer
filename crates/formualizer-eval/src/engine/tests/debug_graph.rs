use crate::engine::{DependencyGraph, Scheduler, VertexId};
use formualizer_common::LiteralValue;
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};

#[test]
fn debug_cycle_detection() {
    let mut graph = DependencyGraph::new();

    // Create a cycle: A1 → B1 → C1 → A1
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap(); // A1 starts as value

    // A1 = B1 + 1 (this should create dependency A1 → B1)
    let a1_ast = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(ASTNode {
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
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
                source_token: None,
            }),
        },
        source_token: None,
    };
    graph.set_cell_formula("Sheet1", 1, 1, a1_ast).unwrap(); // Now A1 is a formula

    // B1 = C1 * 2 (this should create dependency B1 → C1)
    let b1_ast = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "*".to_string(),
            left: Box::new(ASTNode {
                node_type: ASTNodeType::Reference {
                    original: "C1".to_string(),
                    reference: ReferenceType::Cell {
                        sheet: None,
                        row: 1,
                        col: 3,
                    },
                },
                source_token: None,
            }),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Literal(LiteralValue::Int(2)),
                source_token: None,
            }),
        },
        source_token: None,
    };
    graph.set_cell_formula("Sheet1", 1, 2, b1_ast).unwrap();

    // C1 = A1 - 1 (this should create dependency C1 → A1, closing the cycle)
    let c1_ast = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "-".to_string(),
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
                node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
                source_token: None,
            }),
        },
        source_token: None,
    };
    graph.set_cell_formula("Sheet1", 1, 3, c1_ast).unwrap();

    // Debug: Print the dependency structure
    println!("Graph structure:");
    for (i, vertex) in graph.vertices().iter().enumerate() {
        println!(
            "Vertex {}: dependencies = {:?}, dependents = {:?}",
            i, vertex.dependencies, vertex.dependents
        );
    }

    let scheduler = Scheduler::new(&graph);
    let cycle_vertices: Vec<VertexId> = (0..3).map(|i| VertexId::new(i)).collect();
    let sccs = scheduler.tarjan_scc(&cycle_vertices).unwrap();

    println!("SCCs found: {:?}", sccs);
    println!("Number of SCCs: {}", sccs.len());

    // This test is just for debugging - let's see what the actual structure is
    assert!(sccs.len() >= 1);
}
