use chrono::NaiveDate;

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn assert_date_like(v: Option<LiteralValue>, expected: NaiveDate) {
    match v {
        Some(LiteralValue::Date(d)) => assert_eq!(d, expected),
        Some(LiteralValue::DateTime(dt)) => assert_eq!(dt.date(), expected),
        other => panic!("expected date-like {expected:?}, got {other:?}"),
    }
}

#[test]
fn date_plus_number_returns_date() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value(
            "Sheet1",
            1,
            1,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 10, 18).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(14.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=A1+B1").unwrap())
        .unwrap();

    assert_date_like(
        engine.get_cell_value("Sheet1", 1, 1),
        NaiveDate::from_ymd_opt(2024, 10, 18).unwrap(),
    );

    engine.evaluate_all().unwrap();
    assert_date_like(
        engine.get_cell_value("Sheet1", 1, 3),
        NaiveDate::from_ymd_opt(2024, 11, 1).unwrap(),
    );
}

#[test]
fn date_minus_number_returns_date() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value(
            "Sheet1",
            1,
            1,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 11, 1).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(14.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=A1-B1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_date_like(
        engine.get_cell_value("Sheet1", 1, 3),
        NaiveDate::from_ymd_opt(2024, 10, 18).unwrap(),
    );
}

#[test]
fn date_minus_date_returns_number_delta() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value(
            "Sheet1",
            1,
            1,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 11, 1).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value(
            "Sheet1",
            1,
            2,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 10, 18).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=A1-B1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(14.0))
    );
}

#[test]
fn round_days_times_14_preserves_date_tag() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    // Mimic the pattern: C107 + (ROUND(C108,0) * 14)
    engine
        .set_cell_value(
            "Sheet1",
            107,
            3,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 10, 18).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 108, 3, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 109, 3, parse("=C107+(ROUND(C108,0)*14)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_date_like(
        engine.get_cell_value("Sheet1", 109, 3),
        NaiveDate::from_ymd_opt(2024, 11, 1).unwrap(),
    );
}
