use chrono::NaiveDate;

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn assert_date_like(v: Option<LiteralValue>, expected: NaiveDate) {
    match v {
        Some(LiteralValue::Date(d)) => assert_eq!(d, expected),
        Some(LiteralValue::DateTime(dt)) => assert_eq!(dt.date(), expected),
        Some(LiteralValue::Number(n)) => {
            let got = NaiveDate::from_ymd_opt(1899, 12, 30).unwrap()
                + chrono::Duration::days(n.trunc() as i64);
            assert_eq!(got, expected, "serial {n} did not map to {expected}");
        }
        other => panic!("expected date-like {expected:?}, got {other:?}"),
    }
}

fn run_edate(start: NaiveDate, months: i64) -> Option<LiteralValue> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Date(start))
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            parse(&format!("=EDATE($A$1, {months})")).unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.get_cell_value("Sheet1", 1, 2)
}

#[test]
fn edate_with_date_cell_adds_months_preserving_day() {
    assert_date_like(run_edate(date(2025, 2, 4), 15), date(2026, 5, 4));
}

#[test]
fn edate_with_date_cell_clamps_to_month_end() {
    assert_date_like(run_edate(date(2025, 1, 31), 1), date(2025, 2, 28));
}

#[test]
fn edate_with_date_cell_handles_negative_months() {
    assert_date_like(run_edate(date(2025, 3, 15), -3), date(2024, 12, 15));
}

#[test]
fn eomonth_with_date_cell_returns_month_end() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Date(date(2025, 2, 4)))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=EOMONTH($A$1, 0)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_date_like(engine.get_cell_value("Sheet1", 1, 2), date(2025, 2, 28));
}
