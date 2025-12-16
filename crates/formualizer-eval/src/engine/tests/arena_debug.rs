use crate::builtins::math::SumFn;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn test_simple_sum_with_arena() {
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Set up simple values
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(20))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(30))
        .unwrap();

    // Verify values are stored correctly (Int values should be preserved)
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1).unwrap(),
        LiteralValue::Int(10)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1).unwrap(),
        LiteralValue::Int(20)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1).unwrap(),
        LiteralValue::Int(30)
    );

    // Create a SUM formula
    let formula = "=SUM(A1:A3)";
    let ast = parse(formula).unwrap();
    engine
        .set_cell_formula("Sheet1", 4, 1, ast.clone())
        .unwrap();

    // Check if formula was stored
    let graph = &engine.graph;
    let vertex_id = *graph
        .cell_to_vertex()
        .get(&graph.make_cell_ref("Sheet1", 4, 1))
        .expect("Formula vertex should exist");

    let retrieved_ast = graph.get_formula(vertex_id);
    assert!(retrieved_ast.is_some(), "Formula should be stored");

    // Evaluate
    engine.evaluate_all().unwrap();

    // Get result
    let result = engine.get_cell_value("Sheet1", 4, 1).unwrap();
    assert_eq!(result, LiteralValue::Number(60.0), "SUM should return 60");
}

#[test]
fn test_cross_sheet_simple() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Set value on Sheet2
    engine
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Int(100))
        .unwrap();

    // Create formula on Sheet1 that references Sheet2
    let formula = "=Sheet2!A1";
    let ast = parse(formula).unwrap();
    println!("AST: {ast:?}");
    engine.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    // Check if formula was stored
    let graph = &engine.graph;
    let vertex_id = *graph
        .cell_to_vertex()
        .get(&graph.make_cell_ref("Sheet1", 1, 1))
        .expect("Formula vertex should exist");

    let retrieved_ast = graph.get_formula(vertex_id);
    println!("Retrieved AST: {retrieved_ast:?}");
    assert!(retrieved_ast.is_some(), "Formula should be stored");

    engine.evaluate_all().unwrap();

    let result = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    println!("Result: {result:?}");
    assert_eq!(
        result,
        LiteralValue::Number(100.0),
        "Cross-sheet reference should work"
    );
}
