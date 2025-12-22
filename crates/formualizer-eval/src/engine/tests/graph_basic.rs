use super::common::{abs_cell_ref, get_vertex_ids_in_order};
use crate::engine::{DependencyGraph, VertexKind};
use formualizer_common::LiteralValue;

#[test]
fn test_vertex_creation_and_lookup() {
    let mut graph = DependencyGraph::new();

    // Test creating a vertex with set_cell_value
    let summary = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();
    assert_eq!(summary.affected_vertices.len(), 1);
    assert_eq!(summary.created_placeholders.len(), 1);

    // Test that we can look up the value
    let value = graph.get_cell_value("Sheet1", 1, 1);
    assert_eq!(value, Some(LiteralValue::Int(42)));

    // Test that non-existent cells return None
    let empty_value = graph.get_cell_value("Sheet1", 2, 2);
    assert_eq!(empty_value, None);

    // Test updating an existing cell
    let summary2 = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(std::f64::consts::PI))
        .unwrap();
    assert_eq!(summary2.affected_vertices.len(), 1);
    assert_eq!(summary.affected_vertices[0], summary2.affected_vertices[0]); // Same vertex ID
    assert!(summary2.created_placeholders.is_empty()); // Not a new placeholder

    let updated_value = graph.get_cell_value("Sheet1", 1, 1);
    assert_eq!(
        updated_value,
        Some(LiteralValue::Number(std::f64::consts::PI))
    );

    // Verify internal structure
    assert_eq!(graph.vertex_len(), 1); // Only A1 exists
    let vertex_id = *graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 1)).unwrap();
    assert_eq!(graph.get_vertex_sheet_id(vertex_id), 0);
    assert_eq!(graph.get_vertex_kind(vertex_id), VertexKind::Cell);
    assert_eq!(
        graph.get_value(vertex_id),
        Some(LiteralValue::Number(std::f64::consts::PI))
    );
}

#[test]
fn test_cell_address_mapping() {
    let mut graph = DependencyGraph::new();

    // Create vertices in different sheets and positions
    let addr1 = abs_cell_ref(0, 1, 1);
    let addr2 = abs_cell_ref(0, 2, 2);
    let addr3 = abs_cell_ref(1, 1, 1);

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
    let ast = formualizer_parse::parser::ASTNode {
        node_type: formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Int(100)),
        source_token: None,
        contains_volatile: false,
    };

    let summary = graph.set_cell_formula("Sheet1", 1, 1, ast).unwrap();
    assert!(summary.created_placeholders.is_empty());

    // After setting formula, value should be None (not evaluated yet)
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), None);

    // Verify the vertex kind changed
    let vertex_ids = get_vertex_ids_in_order(&graph);
    assert_eq!(vertex_ids.len(), 1);
    assert!(graph.is_dirty(vertex_ids[0]));
    assert!(!graph.is_volatile(vertex_ids[0]));
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), None);

    // Transition back to value
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("hello".to_string()))
        .unwrap();
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("hello".to_string()))
    );

    // Verify vertex kind changed back
    assert_eq!(
        graph.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("hello".to_string()))
    );
}

#[test]
fn test_placeholder_creation() {
    let mut graph = DependencyGraph::new();
    let ast = create_cell_ref_ast(None, 1, 2, "B1"); // A1 = B1
    let summary = graph.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    // A1 and B1 should have been created
    let vertex_ids = get_vertex_ids_in_order(&graph);
    assert_eq!(vertex_ids.len(), 2);
    // Both A1 and B1 are created as placeholders initially
    assert_eq!(summary.created_placeholders.len(), 2);

    let a1_addr = abs_cell_ref(0, 1, 1);
    let b1_addr = abs_cell_ref(0, 1, 2);

    assert!(summary.created_placeholders.contains(&a1_addr));
    assert!(summary.created_placeholders.contains(&b1_addr));

    // Verify B1 is an Empty vertex
    let b1_id = *graph.cell_to_vertex().get(&b1_addr).unwrap();
    assert!(matches!(graph.get_vertex_kind(b1_id), VertexKind::Empty));

    // Verify A1 is a Formula vertex
    let a1_id = *graph.cell_to_vertex().get(&a1_addr).unwrap();
    assert!(matches!(
        graph.get_vertex_kind(a1_id),
        VertexKind::FormulaScalar
    ));
}

#[test]
fn test_default_sheet_handling() {
    let mut graph = DependencyGraph::new();
    assert_eq!(graph.default_sheet_name(), "Sheet1");

    graph.set_default_sheet_by_name("MyCustomSheet");
    assert_eq!(graph.default_sheet_name(), "MyCustomSheet");
}

// Helper to create a cell reference AST node
fn create_cell_ref_ast(
    sheet: Option<&str>,
    row: u32,
    col: u32,
    original: &str,
) -> formualizer_parse::parser::ASTNode {
    formualizer_parse::parser::ASTNode {
        node_type: formualizer_parse::parser::ASTNodeType::Reference {
            original: original.to_string(),
            reference: formualizer_parse::parser::ReferenceType::cell(
                sheet.map(|s| s.to_string()),
                row,
                col,
            ),
        },
        source_token: None,
        contains_volatile: false,
    }
}
