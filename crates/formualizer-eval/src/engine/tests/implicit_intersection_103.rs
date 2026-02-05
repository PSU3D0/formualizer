use crate::engine::{Engine, EvalConfig};
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
fn implicit_intersection_column_vector_selects_by_row() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(42.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 5, 2, parse("=@A1:A10").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn implicit_intersection_row_vector_selects_by_column() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(7.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 3, 3, parse("=@A1:E1").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(7.0))
    );
}

#[test]
fn implicit_intersection_2d_selects_by_row_and_col_cross_sheet() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 3, LiteralValue::Number(123.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet2", 5, 3, parse("=@Sheet1!A1:E10").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet2", 5, 3),
        Some(LiteralValue::Number(123.0))
    );
}

#[test]
fn implicit_intersection_out_of_bounds_is_value_error() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(42.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 20, 2, parse("=@A1:A10").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 20, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.to_string(), "#VALUE!"),
        other => panic!("expected #VALUE!, got {other:?}"),
    }
}

#[test]
fn implicit_intersection_suppresses_spill_from_array_function() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=@SEQUENCE(2,2)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(1.0))
    );

    // No spill should occur.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 5),
        Some(LiteralValue::Empty)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 4),
        Some(LiteralValue::Empty)
    );
}

#[test]
fn implicit_intersection_against_spilled_values_requires_at_for_scalar() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    // A1 spills a 3x1 vector: A1:A3 = 1,2,3
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();

    // B2 uses @ to pick the intersecting element (A2)
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=@A1:A3").unwrap())
        .unwrap();

    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(2.0))
    );

    // B2 should be scalar (no spill).
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Empty)
    );
}
