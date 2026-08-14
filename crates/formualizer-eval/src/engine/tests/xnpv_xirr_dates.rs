//! XNPV/XIRR accept date cells in the dates argument.
//!
//! Regression: before the fix, dates were coerced with `coerce_literal_num`,
//! which dropped `Date`/`DateTime` cells from the array/range collection
//! paths, so the date vector ended up shorter than the values vector and the
//! function returned #NUM!.
//!
//! Oracle: LibreOffice 24.2 headless recalculation (values
//! [-1000, 200, 300, 400, 500] on 2024-01-01 .. 2025-01-01 at rate 10%).
use chrono::NaiveDate;

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn xnpv_fixture() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, value) in [(1, -1000), (2, 200), (3, 300), (4, 400), (5, 500)] {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Int(value))
            .unwrap();
    }
    for (row, date) in [
        (1, (2024, 1, 1)),
        (2, (2024, 4, 1)),
        (3, (2024, 7, 1)),
        (4, (2024, 10, 1)),
        (5, (2025, 1, 1)),
    ] {
        engine
            .set_cell_value(
                "Sheet1",
                row,
                2,
                LiteralValue::Date(NaiveDate::from_ymd_opt(date.0, date.1, date.2).unwrap()),
            )
            .unwrap();
    }
    engine
}

fn eval_formula(mut engine: Engine<TestWorkbook>, formula: &str) -> LiteralValue {
    engine
        .set_cell_formula("Sheet1", 1, 5, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value("Sheet1", 1, 5)
        .unwrap_or(LiteralValue::Empty)
}

fn assert_number(formula: &str, expected: f64, tol: f64) {
    match eval_formula(xnpv_fixture(), formula) {
        LiteralValue::Number(actual) => assert!(
            (actual - expected).abs() < tol,
            "{formula}: expected {expected}, got {actual}"
        ),
        other => panic!("{formula}: expected {expected}, got {other:?}"),
    }
}

fn assert_num_error(formula: &str) {
    match eval_formula(xnpv_fixture(), formula) {
        LiteralValue::Error(error) => {
            assert_eq!(error.kind, ExcelErrorKind::Num, "{formula}: {error}")
        }
        other => panic!("{formula}: expected #NUM!, got {other:?}"),
    }
}

#[test]
fn xnpv_accepts_date_cells() {
    assert_number(
        "=XNPV(0.1,A1:A5,B1:B5)",
        308.1871372025822211,
        1e-9,
    );
}

#[test]
fn xnpv_accepts_numeric_serials() {
    // Numeric serial dates must keep working.
    assert_number(
        "=XNPV(0.1,A1:A5,{45292,45383,45474,45566,45658})",
        308.1871372025822211,
        1e-9,
    );
}

#[test]
fn xnpv_accepts_datetime_cells() {
    // First date carries a time fraction: 2024-01-01 12:00 -> serial 45292.5.
    let mut engine = xnpv_fixture();
    engine
        .set_cell_value(
            "Sheet1",
            1,
            2,
            LiteralValue::DateTime(
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap(),
            ),
        )
        .unwrap();
    match eval_formula(engine, "=XNPV(0.1,A1:A5,B1:B5)") {
        LiteralValue::Number(actual) => assert!(
            (actual - 308.3579477383065068).abs() < 1e-9,
            "expected 308.3579477383065068, got {actual}"
        ),
        other => panic!("expected number, got {other:?}"),
    }
}

#[test]
fn xnpv_missing_dates_returns_num_error() {
    // 5 values but only 3 dates must stay #NUM!.
    assert_num_error("=XNPV(0.1,A1:A5,B1:B3)");
}

#[test]
fn xirr_accepts_date_cells() {
    assert_number("=XIRR(A1:A5,B1:B5)", 0.6197585938091048, 1e-6);
}

#[test]
fn xirr_accepts_numeric_serials() {
    assert_number(
        "=XIRR(A1:A5,{45292,45383,45474,45566,45658})",
        0.6197585938091048,
        1e-6,
    );
}
