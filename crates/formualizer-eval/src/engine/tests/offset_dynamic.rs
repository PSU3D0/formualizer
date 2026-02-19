use crate::engine::{ChangeLog, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::ASTNode;
use formualizer_parse::parser::parse as parse_formula;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

fn parse(formula: &str) -> ASTNode {
    parse_formula(formula).expect("valid formula")
}

#[test]
fn offset_dynamic_ordering_with_dirty_formula_target() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(0.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=C1+1"))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=OFFSET(A1,1,0)"))
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(5.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(6.0))
    );
}

#[test]
fn offset_retarget_via_argument_edit() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(0.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 4, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=OFFSET(A1,D1,0)"))
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(10.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 4, LiteralValue::Number(2.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );
}

#[test]
fn offset_cross_sheet_reference() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(7.0))
        .unwrap();
    engine
        .set_cell_value("Sheet2", 2, 1, LiteralValue::Number(9.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=OFFSET(Sheet2!A1,1,0)"))
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(9.0))
    );
}

#[test]
fn offset_entrypoint_parity() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(0.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=C1+1"))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=OFFSET(A1,1,0)"))
        .unwrap();

    engine.evaluate_all().unwrap();

    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(10.0))
        .unwrap();
    let (_res, _delta) = engine.evaluate_all_with_delta().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(11.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(20.0))
        .unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    engine.evaluate_all_cancellable(cancel).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(21.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(30.0))
        .unwrap();
    let mut log = ChangeLog::new();
    engine.evaluate_all_logged(&mut log).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(31.0))
    );
}

#[test]
fn recalc_plan_with_offset_falls_back_to_dynamic_recalc() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(0.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=C1+1"))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=OFFSET(A1,1,0)"))
        .unwrap();

    let plan = engine.build_recalc_plan().unwrap();
    assert!(plan.has_dynamic_refs());

    engine.evaluate_all().unwrap();
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(9.0))
        .unwrap();

    let before = engine.virtual_dep_fallback_activations();
    engine.evaluate_recalc_plan(&plan).unwrap();
    let after = engine.virtual_dep_fallback_activations();

    assert_eq!(after, before + 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(10.0))
    );
}
