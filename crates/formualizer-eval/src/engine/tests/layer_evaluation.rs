//! Tests for the layer-by-layer evaluation logic of the engine.
use super::common::{create_binary_op_ast, create_cell_ref_ast};
use crate::builtins::random::RandFn;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType};

#[test]
fn test_evaluate_linear_chain() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    // A1 = 10
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    // B1 = A1 * 2
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            create_binary_op_ast(
                create_cell_ref_ast(None, 1, 1),
                ASTNode {
                    node_type: ASTNodeType::Literal(LiteralValue::Int(2)),
                    source_token: None,
                    contains_volatile: false,
                },
                "*",
            ),
        )
        .unwrap();
    // C1 = B1 + 5
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            3,
            create_binary_op_ast(
                create_cell_ref_ast(None, 1, 2),
                ASTNode {
                    node_type: ASTNodeType::Literal(LiteralValue::Int(5)),
                    source_token: None,
                    contains_volatile: false,
                },
                "+",
            ),
        )
        .unwrap();

    let result = engine.evaluate_all().unwrap();

    assert_eq!(result.computed_vertices, 2); // B1, C1
    assert_eq!(result.cycle_errors, 0);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(25.0)) // (10 * 2) + 5
    );
}

#[test]
fn test_evaluate_diamond_dependency() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    // A1 = 10
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    // B1 = A1 * 2
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            create_binary_op_ast(
                create_cell_ref_ast(None, 1, 1),
                ASTNode {
                    node_type: ASTNodeType::Literal(LiteralValue::Int(2)),
                    source_token: None,
                    contains_volatile: false,
                },
                "*",
            ),
        )
        .unwrap();
    // C1 = A1 + 3
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            3,
            create_binary_op_ast(
                create_cell_ref_ast(None, 1, 1),
                ASTNode {
                    node_type: ASTNodeType::Literal(LiteralValue::Int(3)),
                    source_token: None,
                    contains_volatile: false,
                },
                "+",
            ),
        )
        .unwrap();
    // D1 = B1 + C1
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            4,
            create_binary_op_ast(
                create_cell_ref_ast(None, 1, 2),
                create_cell_ref_ast(None, 1, 3),
                "+",
            ),
        )
        .unwrap();

    let result = engine.evaluate_all().unwrap();

    assert_eq!(result.computed_vertices, 3); // B1, C1, D1
    assert_eq!(result.cycle_errors, 0);
    // (10 * 2) + (10 + 3) = 20 + 13 = 33
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(33.0))
    );
}

#[test]
fn test_evaluation_with_cycles() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    // A1 = B1
    engine
        .set_cell_formula("Sheet1", 1, 1, create_cell_ref_ast(None, 1, 2))
        .unwrap();
    // B1 = A1
    engine
        .set_cell_formula("Sheet1", 1, 2, create_cell_ref_ast(None, 1, 1))
        .unwrap();
    // C1 = 5 (acyclic part)
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            3,
            ASTNode {
                node_type: ASTNodeType::Literal(LiteralValue::Int(5)),
                source_token: None,
                contains_volatile: false,
            },
        )
        .unwrap();

    let result = engine.evaluate_all().unwrap();

    assert_eq!(result.computed_vertices, 1); // C1
    assert_eq!(result.cycle_errors, 1); // The A1-B1 cycle
    // A1 and B1 should have error values
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Error(
            formualizer_common::ExcelError::new(formualizer_common::ExcelErrorKind::Circ)
                .with_message("Circular dependency detected".to_string())
        ))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Error(
            formualizer_common::ExcelError::new(formualizer_common::ExcelErrorKind::Circ)
                .with_message("Circular dependency detected".to_string())
        ))
    );
    // C1 should be evaluated
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Int(5))
    );
}

#[test]
fn test_volatile_cells_are_always_evaluated() {
    // Register RAND function in global registry so is_ast_volatile can find it
    crate::builtins::random::register_builtins();

    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RandFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // A1 = RAND()
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            ASTNode {
                node_type: ASTNodeType::Function {
                    name: "RAND".to_string(),
                    args: vec![],
                },
                source_token: None,
                contains_volatile: true,
            },
        )
        .unwrap();

    // B1 = A1
    engine
        .set_cell_formula("Sheet1", 1, 2, create_cell_ref_ast(None, 1, 1))
        .unwrap();

    // First evaluation
    engine.evaluate_all().unwrap();
    let first_val = engine.get_cell_value("Sheet1", 1, 2);
    assert!(first_val.is_some());

    // Second evaluation - change the workbook seed to alter RNG composition
    engine.set_workbook_seed(0xDEAD_BEEF_F00D_CAFE);
    engine.evaluate_all().unwrap();
    let second_val = engine.get_cell_value("Sheet1", 1, 2);
    assert!(second_val.is_some());

    // The value of B1 should have changed because RAND() was re-run
    assert_ne!(first_val, second_val, "Volatile cell did not re-evaluate");
}
