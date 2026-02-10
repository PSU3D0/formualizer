use crate::engine::{EvalConfig, eval::Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn serial_eval_config() -> EvalConfig {
    EvalConfig {
        enable_parallel: false,
        ..Default::default()
    }
}

#[test]
fn spill_projects_array_returning_function() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(2,2)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(4.0))
    );
}

#[test]
fn spill_conflict_clears_previous_projection() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={7,8,9}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(8.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(9.0))
    );

    // Block the next spill with a non-empty value outside the current spill region.
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Text("X".into()))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={10;20}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e, "#SPILL!"),
        v => panic!("expected #SPILL! at A1, got {v:?}"),
    }

    // Previous spill children are cleared; new spill is not projected.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), None);
    assert_eq!(engine.get_cell_value("Sheet1", 1, 3), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Text("X".into()))
    );
}

#[test]
fn spill_anchor_remains_formula_and_recalculates_on_precedent_edit() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(1))
        .unwrap();
    // Row-vector spill: [A2, A2+1]
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(1,2,A2,1)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );

    // Edit precedent; anchor should re-evaluate and update its spill.
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(10))
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(11.0))
    );
}

#[test]
fn spill_max_cells_cap_blocks_and_clears_children() {
    let wb = TestWorkbook::new();
    let mut cfg = serial_eval_config();
    cfg.spill.max_spill_cells = 3;
    let mut engine = Engine::new(wb, cfg);

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2,3}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(3.0))
    );

    // Now exceed cap (4 cells): should be #SPILL! and clear previous children.
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e, "#SPILL!"),
        v => panic!("expected #SPILL! at A1, got {v:?}"),
    }
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), None);
    assert_eq!(engine.get_cell_value("Sheet1", 1, 3), None);
}
