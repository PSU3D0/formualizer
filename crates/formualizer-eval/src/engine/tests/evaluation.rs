//! Tests for the evaluation logic of the engine.
use super::common::get_vertex_ids_in_order;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

fn create_simple_engine() -> Engine<TestWorkbook> {
    let wb = TestWorkbook::new();
    let config = EvalConfig::default();
    Engine::new(wb, config)
}

/// Helper to create a cell reference AST node
fn ref_ast(row: u32, col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{row}C{col}"),
            reference: ReferenceType::cell(None, row, col),
        },
        source_token: None,
        contains_volatile: false,
    }
}

/// Helper to create a binary op AST node
fn op_ast(left: ASTNode, right: ASTNode, op: &str) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
        },
        source_token: None,
        contains_volatile: false,
    }
}

#[test]
fn test_vertex_evaluation_scalar() {
    let mut engine = create_simple_engine();

    // A1 = 1 + 2
    let ast = op_ast(
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(2)), None),
        "+",
    );
    engine
        .set_cell_formula("Sheet1", 1, 1, ast.clone())
        .unwrap();

    // The vertex ID for A1 should be 0.
    let vertex_ids = get_vertex_ids_in_order(&engine.graph);
    let a1_id = vertex_ids[0];
    let result = engine.evaluate_vertex(a1_id).unwrap();

    assert_eq!(result, LiteralValue::Number(3.0));

    // Also verify the value is cached in the graph.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(3.0))
    );
}

#[test]
fn test_evaluation_of_empty_placeholders() {
    // Empty placeholders should evaluate to 0.
    let mut engine = create_simple_engine();

    // A1 = B1, where B1 is empty. This creates two vertices.
    // A1 (ID=0) is the formula, B1 (ID=1) is the empty placeholder.
    let ast = ref_ast(1, 2);
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    // Evaluate A1. The interpreter will ask the engine to resolve B1.
    // The engine's EvaluationContext impl will see B1 is empty and return 0.
    let vertex_ids = get_vertex_ids_in_order(&engine.graph);
    let a1_id = vertex_ids[0];
    let result = engine.evaluate_vertex(a1_id).unwrap();

    // The result of A1 should be 0.
    assert_eq!(result, LiteralValue::Int(0));
}

#[test]
fn test_evaluation_error_handling() {
    let mut engine = create_simple_engine();

    // A1 = 1 / 0
    let ast = op_ast(
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(0)), None),
        "/",
    );
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    let vertex_ids = get_vertex_ids_in_order(&engine.graph);
    let a1_id = vertex_ids[0];
    let result = engine.evaluate_vertex(a1_id).unwrap();

    assert_eq!(result, LiteralValue::Error(ExcelError::new_div()));
}

#[test]
fn test_error_propagation_through_dependencies() {
    let mut engine = create_simple_engine();

    // A1 = 1 / 0
    let div_zero_ast = op_ast(
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(0)), None),
        "/",
    );
    engine
        .set_cell_formula("Sheet1", 1, 1, div_zero_ast)
        .unwrap();

    // A2 = A1
    let ref_a1_ast = ref_ast(1, 1);
    engine.set_cell_formula("Sheet1", 2, 1, ref_a1_ast).unwrap();

    // Evaluate A1, which should result in an error
    let vertex_ids = get_vertex_ids_in_order(&engine.graph);
    let a1_id = vertex_ids[0];
    let a1_result = engine.evaluate_vertex(a1_id).unwrap();
    assert!(matches!(a1_result, LiteralValue::Error(_)));

    // Now, evaluate A2. It should resolve A1 to its cached error value
    // and propagate it.
    let vertex_ids = get_vertex_ids_in_order(&engine.graph);
    let a2_id = vertex_ids[1];
    let a2_result = engine.evaluate_vertex(a2_id).unwrap();

    assert_eq!(a2_result, a1_result);
    assert_eq!(a2_result, LiteralValue::Error(ExcelError::new_div()));
}

#[test]
fn test_sequential_evaluation_of_dependency_chain() {
    let mut engine = create_simple_engine();

    // A1 = 10
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();

    // A2 = A1 + 5
    let a2_ast = op_ast(
        ref_ast(1, 1),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );
    engine.set_cell_formula("Sheet1", 2, 1, a2_ast).unwrap();

    // A3 = A2 * 2
    let a3_ast = op_ast(
        ref_ast(2, 1),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(2)), None),
        "*",
    );
    engine.set_cell_formula("Sheet1", 3, 1, a3_ast).unwrap();

    // Get vertex IDs after all cells are created
    let vertex_ids = get_vertex_ids_in_order(&engine.graph);
    let a2_id = vertex_ids[1];
    let a3_id = vertex_ids[2];

    // Manually evaluate in topological order, simulating the scheduler
    // A1 is a value, no evaluation needed.
    let a2_result = engine.evaluate_vertex(a2_id).unwrap();
    assert_eq!(a2_result, LiteralValue::Number(15.0));

    let a3_result = engine.evaluate_vertex(a3_id).unwrap();
    assert_eq!(a3_result, LiteralValue::Number(30.0));

    // Verify final cached value
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(30.0))
    );
}

#[test]
#[ignore] // Array formulas are not fully implemented yet
fn test_vertex_evaluation_array_stub() {
    // This test will be implemented when array formulas are supported.
}
