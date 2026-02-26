use super::common::arrow_eval_config;
use crate::engine::{Engine, RowVisibilitySource};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn assert_num(value: Option<LiteralValue>, expected: f64) {
    match value {
        Some(LiteralValue::Number(n)) => assert!((n - expected).abs() < 1e-9),
        Some(LiteralValue::Int(i)) => assert!(((i as f64) - expected).abs() < 1e-9),
        other => panic!("expected numeric {expected}, got {other:?}"),
    }
}

#[test]
fn subtotal_109_respects_manual_and_filter_hidden_rows() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());

    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(20))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 1, LiteralValue::Int(30))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(100))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUBTOTAL(9,A2:A5)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=SUBTOTAL(109,A2:A5)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 2), 160.0);
    assert_num(engine.get_cell_value("Sheet1", 1, 3), 160.0);

    engine
        .set_row_hidden("Sheet1", 3, true, RowVisibilitySource::Manual)
        .unwrap();
    engine
        .set_row_hidden("Sheet1", 4, true, RowVisibilitySource::Filter)
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 2), 160.0);
    assert_num(engine.get_cell_value("Sheet1", 1, 3), 110.0);

    engine
        .set_row_hidden("Sheet1", 3, false, RowVisibilitySource::Manual)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 3), 130.0);

    engine
        .set_row_hidden("Sheet1", 4, false, RowVisibilitySource::Filter)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 3), 160.0);
}

#[test]
fn subtotal_109_skips_error_when_row_is_hidden() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());

    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_value(
            "Sheet1",
            3,
            1,
            LiteralValue::Error(formualizer_common::ExcelError::new_div()),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 1, LiteralValue::Int(30))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUBTOTAL(109,A2:A4)").unwrap())
        .unwrap();

    engine
        .set_row_hidden("Sheet1", 3, true, RowVisibilitySource::Manual)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 2), 40.0);

    engine
        .set_row_hidden("Sheet1", 3, false, RowVisibilitySource::Manual)
        .unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Div),
        other => panic!("expected #DIV/0!, got {other:?}"),
    }
}
