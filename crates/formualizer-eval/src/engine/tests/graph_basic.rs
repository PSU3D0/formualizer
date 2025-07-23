use crate::engine::{CellAddr, DependencyGraph, VertexKind};
use formualizer_common::LiteralValue;

#[test]
fn test_vertex_creation_and_lookup() {
    let mut graph = DependencyGraph::new();

    // Test creating a vertex with set_cell_value
    let affected = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();
    assert_eq!(affected.len(), 1);

    // Test that we can look up the value
    let value = graph.get_cell_value("Sheet1", 1, 1);
    assert_eq!(value, Some(LiteralValue::Int(42)));

    // Test that non-existent cells return None
    let empty_value = graph.get_cell_value("Sheet1", 2, 2);
    assert_eq!(empty_value, None);

    // Test updating an existing cell
    let affected2 = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(3.14))
        .unwrap();
    assert_eq!(affected2.len(), 1);
    assert_eq!(affected[0], affected2[0]); // Same vertex ID

    let updated_value = graph.get_cell_value("Sheet1", 1, 1);
    assert_eq!(updated_value, Some(LiteralValue::Number(3.14)));

    // Verify internal structure
    assert_eq!(graph.vertices().len(), 1);
    let vertex = &graph.vertices()[0];
    assert_eq!(vertex.sheet, "Sheet1");
    assert_eq!(vertex.row, Some(1));
    assert_eq!(vertex.col, Some(1));

    match &vertex.kind {
        VertexKind::Value(v) => assert_eq!(*v, LiteralValue::Number(3.14)),
        _ => panic!("Expected VertexKind::Value"),
    }
}

#[test]
fn test_cell_address_mapping() {
    let mut graph = DependencyGraph::new();

    // Create vertices in different sheets and positions
    let addr1 = CellAddr::new("Sheet1".to_string(), 1, 1);
    let addr2 = CellAddr::new("Sheet1".to_string(), 2, 2);
    let addr3 = CellAddr::new("Sheet2".to_string(), 1, 1);

    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Int(2))
        .unwrap();
    graph
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Int(3))
        .unwrap();

    // Verify all addresses are mapped
    let cell_mappings = graph.cell_to_vertex();
    assert_eq!(cell_mappings.len(), 3);
    assert!(cell_mappings.contains_key(&addr1));
    assert!(cell_mappings.contains_key(&addr2));
    assert!(cell_mappings.contains_key(&addr3));

    // Verify different vertices have different IDs
    let id1 = cell_mappings[&addr1];
    let id2 = cell_mappings[&addr2];
    let id3 = cell_mappings[&addr3];

    assert_ne!(id1, id2);
    assert_ne!(id1, id3);
    assert_ne!(id2, id3);

    // Verify values are correct
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Int(1))
    );
    assert_eq!(
        graph.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Int(2))
    );
    assert_eq!(
        graph.get_cell_value("Sheet2", 1, 1),
        Some(LiteralValue::Int(3))
    );
}

#[test]
fn test_vertex_kind_transitions() {
    let mut graph = DependencyGraph::new();

    // Start with a value
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Int(42))
    );

    // Transition to a formula (we'll use a simple literal AST for now)
    let ast = formualizer_core::parser::ASTNode {
        node_type: formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Int(100)),
        source_token: None,
    };

    graph.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    // After setting formula, value should be None (not evaluated yet)
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), None);

    // Verify the vertex kind changed
    let vertices = graph.vertices();
    assert_eq!(vertices.len(), 1);
    match &vertices[0].kind {
        VertexKind::FormulaScalar {
            dirty,
            volatile,
            result,
            ..
        } => {
            assert!(*dirty);
            assert!(!*volatile);
            assert_eq!(*result, None);
        }
        _ => panic!("Expected VertexKind::FormulaScalar after setting formula"),
    }

    // Transition back to value
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("hello".to_string()))
        .unwrap();
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("hello".to_string()))
    );

    // Verify vertex kind changed back
    let vertices = graph.vertices();
    match &vertices[0].kind {
        VertexKind::Value(v) => assert_eq!(*v, LiteralValue::Text("hello".to_string())),
        _ => panic!("Expected VertexKind::Value after setting value"),
    }
}
