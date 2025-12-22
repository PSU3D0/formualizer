use crate::engine::*;
use formualizer_common::Coord as AbsCoord;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType};

// Helper to create a simple formula AST
fn simple_formula(row: u32, col: u32) -> ASTNode {
    // `ReferenceType` uses Excel 1-based coords.
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("A{row}"),
            reference: formualizer_parse::parser::ReferenceType::cell(None, row, col),
        },
        source_token: None,
        contains_volatile: false,
    }
}

#[test]
fn test_snapshot_vertex() {
    let mut graph = DependencyGraph::new();

    // Create a cell with value
    let result = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(42.0))
        .unwrap();
    let vertex_id = result.affected_vertices[0];

    // Take snapshot
    let snapshot = graph.snapshot_vertex(vertex_id);

    // Verify snapshot contents
    assert_eq!(snapshot.coord, AbsCoord::new(0, 0));
    assert_eq!(snapshot.kind, VertexKind::Cell);
    assert_eq!(snapshot.sheet_id, graph.sheet_id("Sheet1").unwrap());

    // Check value was captured (note: value is stored but not directly accessible via snapshot)
    // We verified the snapshot structure above
}

#[test]
fn test_snapshot_vertex_with_formula() {
    let mut graph = DependencyGraph::new();

    // Create dependencies
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    // Create formula cell
    let formula = simple_formula(1, 1);
    let result = graph
        .set_cell_formula("Sheet1", 2, 1, formula.clone())
        .unwrap();
    let formula_vertex = result.affected_vertices[0];

    // Take snapshot
    let snapshot = graph.snapshot_vertex(formula_vertex);

    // Verify formula was captured
    assert_eq!(snapshot.coord, AbsCoord::new(1, 0));
    assert_eq!(snapshot.kind, VertexKind::FormulaScalar);

    // Check dependencies are captured
    assert!(!snapshot.out_edges.is_empty(), "Should have dependencies");
}

#[test]
fn test_mark_as_ref_error() {
    let mut graph = DependencyGraph::new();

    // Create a formula cell
    let formula = simple_formula(1, 1);
    let result = graph.set_cell_formula("Sheet1", 1, 2, formula).unwrap();
    let vertex_id = result.affected_vertices[0];

    // Mark as REF error
    graph.mark_as_ref_error(vertex_id);

    // Verify error value (using internal method)
    let value = graph.get_value(vertex_id).unwrap();
    match value {
        LiteralValue::Error(err) => {
            assert_eq!(err.kind, ExcelErrorKind::Ref);
        }
        _ => panic!("Expected REF error, got {value:?}"),
    }

    // Verify vertex is marked dirty (using internal method)
    assert!(graph.is_dirty(vertex_id));
}

#[test]
fn test_remove_all_edges() {
    let mut graph = DependencyGraph::new();

    // Create a network of dependencies
    // A1 = 10
    // B1 = A1 * 2
    // C1 = B1 + A1
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    let b1_formula = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "*".to_string(),
            left: Box::new(simple_formula(1, 1)),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Literal(LiteralValue::Number(2.0)),
                source_token: None,
                contains_volatile: false,
            }),
        },
        source_token: None,
        contains_volatile: false,
    };
    let b1_result = graph.set_cell_formula("Sheet1", 1, 2, b1_formula).unwrap();
    let b1_vertex = b1_result.affected_vertices[0];

    let c1_formula = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(simple_formula(1, 2)),  // B1
            right: Box::new(simple_formula(1, 1)), // A1
        },
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 1, 3, c1_formula).unwrap();

    // Verify B1 has dependencies and dependents before removal
    let b1_deps = graph.get_dependencies(b1_vertex);
    let b1_dependents = graph.get_dependents(b1_vertex);
    assert!(!b1_deps.is_empty(), "B1 should have dependencies");
    assert!(!b1_dependents.is_empty(), "B1 should have dependents");

    // Remove all edges for B1
    graph.remove_all_edges(b1_vertex);

    // Verify all edges are removed
    let b1_deps_after = graph.get_dependencies(b1_vertex);
    let b1_dependents_after = graph.get_dependents(b1_vertex);
    assert!(b1_deps_after.is_empty(), "B1 should have no dependencies");
    assert!(
        b1_dependents_after.is_empty(),
        "B1 should have no dependents"
    );
}

#[test]
fn test_mark_dependents_dirty() {
    let mut graph = DependencyGraph::new();

    // Create dependency chain: A1 -> B1 -> C1 -> D1
    let a1_result = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    let a1_vertex = a1_result.affected_vertices[0];

    // B1 = A1 * 2
    let b1_formula = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "*".to_string(),
            left: Box::new(simple_formula(1, 1)),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Literal(LiteralValue::Number(2.0)),
                source_token: None,
                contains_volatile: false,
            }),
        },
        source_token: None,
        contains_volatile: false,
    };
    let b1_result = graph.set_cell_formula("Sheet1", 1, 2, b1_formula).unwrap();
    let b1_vertex = b1_result.affected_vertices[0];

    // C1 = B1 + 1
    let c1_formula = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(simple_formula(1, 2)),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Literal(LiteralValue::Number(1.0)),
                source_token: None,
                contains_volatile: false,
            }),
        },
        source_token: None,
        contains_volatile: false,
    };
    let c1_result = graph.set_cell_formula("Sheet1", 1, 3, c1_formula).unwrap();
    let c1_vertex = c1_result.affected_vertices[0];

    // D1 = C1
    let d1_formula = simple_formula(1, 3);
    let d1_result = graph.set_cell_formula("Sheet1", 1, 4, d1_formula).unwrap();
    let d1_vertex = d1_result.affected_vertices[0];

    // Clear dirty flags manually to simulate evaluation completion
    // We need to clear the dirty flags since formulas are marked dirty when created
    graph.clear_dirty_flags(&[b1_vertex, c1_vertex, d1_vertex]);

    // Mark A1's dependents as dirty
    graph.mark_dependents_dirty(a1_vertex);

    // Only B1 should be dirty (direct dependent)
    assert!(graph.is_dirty(b1_vertex), "B1 should be dirty");
    assert!(
        !graph.is_dirty(c1_vertex),
        "C1 should not be dirty (indirect)"
    );
    assert!(
        !graph.is_dirty(d1_vertex),
        "D1 should not be dirty (indirect)"
    );

    // Mark B1's dependents as dirty
    graph.mark_dependents_dirty(b1_vertex);

    // Now C1 should also be dirty
    assert!(graph.is_dirty(c1_vertex), "C1 should be dirty");
    assert!(!graph.is_dirty(d1_vertex), "D1 should still not be dirty");
}

#[test]
fn test_snapshot_preserves_all_state() {
    let mut graph = DependencyGraph::new();

    // Create a complex vertex with all properties
    let formula = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(simple_formula(1, 1)),
            right: Box::new(simple_formula(2, 1)),
        },
        source_token: None,
        contains_volatile: false,
    };

    // First create the dependencies
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();

    let result = graph.set_cell_formula("Sheet1", 3, 1, formula).unwrap();
    let vertex_id = result.affected_vertices[0];

    // Mark as volatile
    graph.mark_volatile(vertex_id, true);

    // Take snapshot
    let snapshot = graph.snapshot_vertex(vertex_id);

    // Verify all state is captured
    assert_eq!(snapshot.coord, AbsCoord::new(2, 0));
    assert_eq!(snapshot.kind, VertexKind::FormulaScalar);
    assert_eq!(snapshot.out_edges.len(), 2, "Should have 2 dependencies");

    // Flags should reflect volatile state (formula cells are dirty by default when created)
    let flags = snapshot.flags;
    assert!(
        flags & 0x01 != 0,
        "Should be marked dirty (formulas are dirty when created)"
    );
    assert!(flags & 0x02 != 0, "Should be marked volatile");
}
