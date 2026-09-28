use super::common::{abs_cell_ref, get_vertex_ids_in_order};
use crate::engine::VertexKind;
use formualizer_common::LiteralValue;

/// Decision 27: a value cell has no vertex, cell-map or sheet-index entry
/// (this test used to check the value vertex's creation and reuse).
#[test]
fn test_vertex_creation_and_lookup() {
    let mut graph = super::common::graph_truth_graph();

    let summary = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();
    assert!(summary.affected_vertices.is_empty());
    assert!(summary.created_placeholders.is_empty());

    let summary2 = graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(std::f64::consts::PI))
        .unwrap();
    assert!(summary2.affected_vertices.is_empty());
    assert!(summary2.created_placeholders.is_empty());

    assert_eq!(graph.vertex_len(), 0);
    assert!(graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 1)).is_none());
}

/// Formula cells map to distinct vertices per address (value cells, which
/// this test used, have none: decision 27).
#[test]
fn test_cell_address_mapping() {
    let mut graph = super::common::graph_truth_graph();

    // Create vertices in different sheets and positions
    let addr1 = abs_cell_ref(0, 1, 1);
    let addr2 = abs_cell_ref(0, 2, 2);
    let addr3 = abs_cell_ref(1, 1, 1);

    for (sheet, row, col, v) in [
        ("Sheet1", 1, 1, 1),
        ("Sheet1", 2, 2, 2),
        ("Sheet2", 1, 1, 3),
    ] {
        graph
            .set_cell_formula(
                sheet,
                row,
                col,
                super::common::literal_ast(LiteralValue::Int(v)),
            )
            .unwrap();
    }
    graph
        .set_cell_value("Sheet2", 5, 5, LiteralValue::Int(4))
        .unwrap();

    // Verify all formula addresses are mapped, and only those
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

    // Values are not cached in the dependency graph in canonical mode.
}

/// value -> formula -> value -> formula: the formula's id retires with the
/// value and comes back with the next formula (decision 27, option B).
#[test]
fn test_vertex_kind_transitions() {
    let mut graph = super::common::graph_truth_graph();

    // Start with a value: no vertex
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), None);
    assert_eq!(graph.vertex_len(), 0);

    let ast = super::common::literal_ast(LiteralValue::Int(100));
    let summary = graph.set_cell_formula("Sheet1", 1, 1, ast.clone()).unwrap();
    assert_eq!(summary.created_placeholders, vec![abs_cell_ref(0, 1, 1)]);

    // After setting formula, value should be None (not evaluated yet)
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), None);

    let vertex_ids = get_vertex_ids_in_order(&graph);
    assert_eq!(vertex_ids.len(), 1);
    let id = vertex_ids[0];
    assert!(graph.is_dirty(id));
    assert!(!graph.is_volatile(id));
    assert_eq!(graph.get_vertex_kind(id), VertexKind::FormulaScalar);

    // Transition back to value: the cell has no vertex, the id retires
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("hello".to_string()))
        .unwrap();
    assert_eq!(graph.get_cell_value("Sheet1", 1, 1), None);
    assert!(graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 1)).is_none());
    assert!(get_vertex_ids_in_order(&graph).is_empty());

    // And to a formula again: the same id
    graph.set_cell_formula("Sheet1", 1, 1, ast).unwrap();
    assert_eq!(
        graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 1)).copied(),
        Some(id)
    );
}

#[test]
fn test_placeholder_creation() {
    let mut graph = super::common::graph_truth_graph();
    let ast = create_cell_ref_ast(None, 1, 2, "B1"); // A1 = B1
    let summary = graph.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    let vertex_ids = get_vertex_ids_in_order(&graph);
    assert_eq!(vertex_ids.len(), 1);
    let a1_addr = abs_cell_ref(0, 1, 1);
    let b1_addr = abs_cell_ref(0, 1, 2);
    assert_eq!(summary.created_placeholders, vec![a1_addr]);
    assert!(graph.cell_to_vertex().get(&b1_addr).is_none());
    assert_eq!(graph.baseline_stats().graph_edge_count, 1);

    // Verify A1 is a Formula vertex
    let a1_id = *graph.cell_to_vertex().get(&a1_addr).unwrap();
    assert!(matches!(
        graph.get_vertex_kind(a1_id),
        VertexKind::FormulaScalar
    ));
}

#[test]
fn test_default_sheet_handling() {
    let mut graph = super::common::graph_truth_graph();
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
