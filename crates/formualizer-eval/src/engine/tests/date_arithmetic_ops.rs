use chrono::NaiveDate;

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn assert_serial(v: Option<LiteralValue>, expected: NaiveDate) {
    assert_eq!(
        v,
        Some(LiteralValue::Number(
            formualizer_common::date_to_serial_for(crate::engine::DateSystem::Excel1900, &expected)
        ))
    );
}

#[test]
fn date_plus_number_is_numeric_with_format_separate() {
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

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Date(
            NaiveDate::from_ymd_opt(2024, 10, 18).unwrap()
        ))
    );

    engine.evaluate_all().unwrap();
    assert_serial(
        engine.get_cell_value("Sheet1", 1, 3),
        NaiveDate::from_ymd_opt(2024, 11, 1).unwrap(),
    );
}

#[test]
fn date_minus_number_is_numeric_with_format_separate() {
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
    assert_serial(
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
fn round_days_times_14_does_not_propagate_date_tag() {
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
    assert_serial(
        engine.get_cell_value("Sheet1", 109, 3),
        NaiveDate::from_ymd_opt(2024, 11, 1).unwrap(),
    );
}

#[test]
fn year_accepts_date_and_datetime_cells_in_engine_flow() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    let d = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
    let dt = d.and_hms_opt(8, 30, 0).unwrap();

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Date(d))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::DateTime(dt))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=YEAR(A1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=YEAR(A2)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2024.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(2024.0))
    );
}
