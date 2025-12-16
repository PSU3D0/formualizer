use crate::engine::tests::common::{create_binary_op_ast, create_cell_ref_ast};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType};

#[test]
fn test_evaluate_until_single_clean_target() {
    let wb =
        TestWorkbook::new().with_function(std::sync::Arc::new(crate::builtins::random::RandFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Set up: A1 = 10, B1 = A1 + 1, both clean
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    let ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 1),
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
            source_token: None,
            contains_volatile: false,
        },
        "+",
    );
    engine.set_cell_formula("Sheet1", 1, 2, ast).unwrap();

    // Evaluate all to make everything clean
    engine.evaluate_all().unwrap();

    // Test: evaluate_until B1 should do nothing since it's clean
    let result = engine.evaluate_until(&[("Sheet1", 1, 2)]).unwrap();
    assert_eq!(result.computed_vertices, 0);

    // B1 should still have its value
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(11.0))
    );
}

#[test]
fn test_evaluate_until_single_dirty_target() {
    let wb =
        TestWorkbook::new().with_function(std::sync::Arc::new(crate::builtins::random::RandFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Set up: A1 = 10, B1 = A1 + 1
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    let ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 1),
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
            source_token: None,
            contains_volatile: false,
        },
        "+",
    );
    engine.set_cell_formula("Sheet1", 1, 2, ast).unwrap();

    // Change A1 to make B1 dirty
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(20))
        .unwrap();

    // Test: evaluate_until B1 should evaluate B1 only
    let result = engine.evaluate_until(&[("Sheet1", 1, 2)]).unwrap();
    assert_eq!(result.computed_vertices, 1);

    // B1 should have updated value
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(21.0))
    );
}

#[test]
fn test_evaluate_until_dependency_chain() {
    let wb =
        TestWorkbook::new().with_function(std::sync::Arc::new(crate::builtins::random::RandFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Set up chain: A1 = 10, B1 = A1, C1 = B1
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();

    let b1_ast = create_cell_ref_ast(None, 1, 1);
    engine.set_cell_formula("Sheet1", 1, 2, b1_ast).unwrap();

    let c1_ast = create_cell_ref_ast(None, 1, 2);
    engine.set_cell_formula("Sheet1", 1, 3, c1_ast).unwrap();

    // Change A1 to make entire chain dirty
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(30))
        .unwrap();

    // Test: evaluate_until C1 should evaluate A1->B1->C1 chain
    let result = engine.evaluate_until(&[("Sheet1", 1, 3)]).unwrap();
    assert_eq!(result.computed_vertices, 2); // B1 and C1 (A1 is a value, not computed)

    // All values should be updated
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Int(30))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(30.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(30.0))
    );
}

#[test]
fn test_evaluate_until_multiple_targets() {
    let wb =
        TestWorkbook::new().with_function(std::sync::Arc::new(crate::builtins::random::RandFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Set up:
    // A1 = 10
    // B1 = A1 + 1
    // C1 = A1 + 2
    // D1 = B1 + C1 (depends on both B1 and C1)
    // E1 = A1 + 5 (another branch)

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();

    let b1_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 1),
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
            source_token: None,
            contains_volatile: false,
        },
        "+",
    );
    engine.set_cell_formula("Sheet1", 1, 2, b1_ast).unwrap();

    let c1_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 1),
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Int(2)),
            source_token: None,
            contains_volatile: false,
        },
        "+",
    );
    engine.set_cell_formula("Sheet1", 1, 3, c1_ast).unwrap();

    let d1_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 2),
        create_cell_ref_ast(None, 1, 3),
        "+",
    );
    engine.set_cell_formula("Sheet1", 1, 4, d1_ast).unwrap();

    let e1_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 1),
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Int(5)),
            source_token: None,
            contains_volatile: false,
        },
        "+",
    );
    engine.set_cell_formula("Sheet1", 1, 5, e1_ast).unwrap();

    // Change A1 to make everything dirty
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(100))
        .unwrap();

    // Test: evaluate_until C1 and E1 should compute union of precedents
    // Should evaluate B1, C1, E1 (all depend on A1)
    let result = engine
        .evaluate_until(&[("Sheet1", 1, 3), ("Sheet1", 1, 5)])
        .unwrap();
    assert_eq!(result.computed_vertices, 2); // C1 and E1

    // Check that only requested targets and their precedents were computed
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(102.0))
    ); // C1 = A1 + 2 = 102
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 5),
        Some(LiteralValue::Number(105.0))
    ); // E1 = A1 + 5 = 105

    // D1 should still be dirty/old since we didn't request it
    // We can't easily test this without exposing internal state,
    // but we can test that evaluating D1 separately works
    let result2 = engine.evaluate_until(&[("Sheet1", 1, 4)]).unwrap();
    assert_eq!(result2.computed_vertices, 2); // B1 and D1

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(203.0))
    ); // D1 = B1 + C1 = 101 + 102 = 203
}

#[test]
fn test_evaluate_until_volatile_precedent() {
    // Register RAND function in global registry so is_ast_volatile can find it
    crate::builtins::random::register_builtins();

    let wb =
        TestWorkbook::new().with_function(std::sync::Arc::new(crate::builtins::random::RandFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Set up: A1 = RAND(), B1 = A1 + 1
    let rand_ast = ASTNode {
        node_type: ASTNodeType::Function {
            name: "RAND".to_string(),
            args: vec![],
        },
        source_token: None,
        contains_volatile: true,
    };
    engine.set_cell_formula("Sheet1", 1, 1, rand_ast).unwrap();

    let b1_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 1),
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
            source_token: None,
            contains_volatile: false,
        },
        "+",
    );
    engine.set_cell_formula("Sheet1", 1, 2, b1_ast).unwrap();

    // Evaluate all to make everything clean
    engine.evaluate_all().unwrap();

    // Test: evaluate_until B1 should still evaluate A1 because it's volatile
    let result = engine.evaluate_until(&[("Sheet1", 1, 2)]).unwrap();
    assert_eq!(result.computed_vertices, 2); // Both A1 and B1 should be evaluated
}

#[test]
fn test_evaluate_until_target_is_volatile() {
    // Register RAND function in global registry so is_ast_volatile can find it
    crate::builtins::random::register_builtins();

    let wb =
        TestWorkbook::new().with_function(std::sync::Arc::new(crate::builtins::random::RandFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Set up: A1 = RAND()
    let rand_ast = ASTNode {
        node_type: ASTNodeType::Function {
            name: "RAND".to_string(),
            args: vec![],
        },
        source_token: None,
        contains_volatile: true,
    };
    engine.set_cell_formula("Sheet1", 1, 1, rand_ast).unwrap();

    // Evaluate all to make everything clean
    engine.evaluate_all().unwrap();

    // Test: evaluate_until A1 should still evaluate it because it's volatile
    let result = engine.evaluate_until(&[("Sheet1", 1, 1)]).unwrap();
    assert_eq!(result.computed_vertices, 1); // A1 should be evaluated even though "clean"
}

#[test]
fn test_evaluate_until_precedents_include_a_cycle() {
    let wb =
        TestWorkbook::new().with_function(std::sync::Arc::new(crate::builtins::random::RandFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Set up: A1 = B1, B1 = A1 (cycle), C1 = A1 + 1 (depends on cycle)
    let a1_ast = create_cell_ref_ast(None, 1, 2); // A1 = B1
    engine.set_cell_formula("Sheet1", 1, 1, a1_ast).unwrap();

    let b1_ast = create_cell_ref_ast(None, 1, 1); // B1 = A1  
    engine.set_cell_formula("Sheet1", 1, 2, b1_ast).unwrap();

    let c1_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 1),
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
            source_token: None,
            contains_volatile: false,
        },
        "+",
    );
    engine.set_cell_formula("Sheet1", 1, 3, c1_ast).unwrap();

    // Test: evaluate_until C1 should handle the cycle correctly
    let result = engine.evaluate_until(&[("Sheet1", 1, 3)]).unwrap();
    assert_eq!(result.cycle_errors, 1); // Should detect one cycle (A1-B1)
    assert_eq!(result.computed_vertices, 1); // C1 should still be computed

    // Check that cycle members have #CIRC! error
    let a1_value = engine.get_cell_value("Sheet1", 1, 1);
    assert!(matches!(a1_value, Some(LiteralValue::Error(_))));

    let b1_value = engine.get_cell_value("Sheet1", 1, 2);
    assert!(matches!(b1_value, Some(LiteralValue::Error(_))));

    // C1 should still get evaluated even though its precedent has an error
    let c1_value = engine.get_cell_value("Sheet1", 1, 3);
    assert!(c1_value.is_some());
}
