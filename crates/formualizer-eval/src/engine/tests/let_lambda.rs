use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[test]
fn let_and_lambda_basic_engine_parity() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=LET(x,2,x+3)").unwrap())
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            parse("=LET(inc,LAMBDA(n,n+1),inc(41))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn lambda_closure_capture_and_shadowing_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula(
            "Sheet1",
            2,
            1,
            parse("=LET(k,10,addk,LAMBDA(n,n+k),addk(5))").unwrap(),
        )
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=LET(x,2,LET(x,5,x)+x)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(15.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(7.0))
    );
}

#[test]
fn lambda_errors_surface_in_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=LAMBDA(x,x+1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            3,
            2,
            parse("=LET(inc,LAMBDA(n,n+1),inc(1,2))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 3, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Calc),
        other => panic!("expected #CALC!, got {other:?}"),
    }

    match engine.get_cell_value("Sheet1", 3, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Value),
        other => panic!("expected #VALUE!, got {other:?}"),
    }
}

#[test]
fn let_lambda_case_insensitive_names_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 4, 1, parse("=LET(x,1,X+1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 4, 2, parse("=LET(F,LAMBDA(n,n+1),f(1))").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn let_local_name_shadows_workbook_defined_name_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .define_name(
            "x",
            NamedDefinition::Literal(LiteralValue::Number(100.0)),
            NameScope::Workbook,
        )
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 5, 1, parse("=LET(X,1,x+1)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 1),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn lambda_param_shadowing_and_capture_snapshot_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula(
            "Sheet1",
            6,
            1,
            parse("=LET(n,5,f,LAMBDA(n,n+1),f(10))").unwrap(),
        )
        .unwrap();

    engine
        .set_cell_formula(
            "Sheet1",
            6,
            2,
            parse("=LET(k,1,f,LAMBDA(x,x+k),k,2,f(0))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 6, 1),
        Some(LiteralValue::Number(11.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 6, 2),
        Some(LiteralValue::Number(1.0))
    );
}

#[test]
fn let_undefined_symbol_and_non_invoked_lambda_errors_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    let err = engine
        .set_cell_formula("Sheet1", 7, 1, parse("=LET(x,y,y,2,x)").unwrap())
        .expect_err("undefined y should fail ingest");
    assert_eq!(err.kind, ExcelErrorKind::Name);

    engine
        .set_cell_formula("Sheet1", 7, 2, parse("=LET(f,LAMBDA(x,x+1),f)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 7, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Calc),
        other => panic!("expected #CALC!, got {other:?}"),
    }
}

#[test]
fn nested_let_lambda_dependency_recalc_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            parse("=LET(a,A1,f,LAMBDA(x,LET(y,x+a,y)),f(2))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(12.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(22.0))
    );
}
