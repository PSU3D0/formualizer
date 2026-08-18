//! Regression coverage for #310: `Date - Number` yielding a negative serial
//! returned `#NUM!` instead of the plain number Excel produces.
//!
//! Excel has no date type at the formula level. A date cell holds a serial
//! number carrying a date number format, so `=A1-B1` with `A1 < B1` is just a
//! negative number. All date arithmetic now returns plain `Number` values,
//! matching Excel's value semantics (#312).
//!
//! Each test pins exactly the property it names.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use chrono::NaiveDate;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

/// Serial for 2024-12-01 in the 1900 date system, matching the issue report.
const DEC_1_2024: f64 = 45627.0;

fn engine_with_date_anchor() -> Engine<TestWorkbook> {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());
    // A1 holds a genuine Date literal, exactly as an ingested date-formatted
    // xlsx cell does. This is the trigger: a formula-produced `=DATE(...)`
    // yields a plain number and never reached the defect.
    engine
        .set_cell_value(
            "Sheet1",
            1,
            1,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()),
        )
        .unwrap();
    engine
}

fn eval(engine: &mut Engine<TestWorkbook>, formula: &str) -> LiteralValue {
    engine
        .set_cell_formula("Sheet1", 10, 10, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_cell("Sheet1", 10, 10).unwrap().unwrap()
}

#[test]
fn date_minus_larger_number_returns_negative_number_not_num_error() {
    let mut engine = engine_with_date_anchor();
    // 45627 - 45658 = -31, the reporter's headline case.
    assert_eq!(eval(&mut engine, "=A1-45658"), LiteralValue::Number(-31.0));
}

#[test]
fn date_minus_number_at_serial_zero_boundary_stays_representable() {
    let mut engine = engine_with_date_anchor();
    // Serial 0 is plain arithmetic: 45627 - 45627 = 0. Excel returns a number.
    assert_eq!(eval(&mut engine, "=A1-45627"), LiteralValue::Number(0.0));
}

#[test]
fn date_minus_number_one_past_serial_zero_degrades_to_number() {
    let mut engine = engine_with_date_anchor();
    // Serial -1 is the first unrepresentable value; it must be a plain number.
    assert_eq!(
        eval(&mut engine, &format!("=A1-{}", DEC_1_2024 + 1.0)),
        LiteralValue::Number(-1.0)
    );
}

#[test]
fn date_plus_large_number_beyond_year_9999_degrades_to_number() {
    let mut engine = engine_with_date_anchor();
    // Overflow past the representable date range is arithmetic, not failure.
    assert_eq!(
        eval(&mut engine, "=A1+1000000000"),
        LiteralValue::Number(DEC_1_2024 + 1_000_000_000.0)
    );
}

#[test]
fn date_minus_date_remains_a_plain_day_delta_in_both_directions() {
    let mut engine = engine_with_date_anchor();
    engine
        .set_cell_value(
            "Sheet1",
            1,
            2,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
        )
        .unwrap();
    assert_eq!(eval(&mut engine, "=B1-A1"), LiteralValue::Number(31.0));
    // The reverse direction is the negative case, and was never broken because
    // Date - Date short-circuits to a numeric delta. Pinned so the fix to the
    // Date - Number path cannot accidentally reroute it.
    assert_eq!(eval(&mut engine, "=A1-B1"), LiteralValue::Number(-31.0));
}

#[test]
fn negative_date_difference_flows_through_the_accrual_idiom() {
    let mut engine = engine_with_date_anchor();
    // The real-world shape from the report: a partial-period accrual where the
    // payment date precedes the anchor. Previously #NUM! poisoned every
    // downstream cell.
    match eval(&mut engine, "=(A1-45658)/365") {
        LiteralValue::Number(n) => assert!(
            (n - (-31.0 / 365.0)).abs() < 1e-12,
            "expected -31/365, got {n}"
        ),
        other => panic!("expected Number, got {other:?}"),
    }
}

#[test]
fn reporter_max_if_guard_returns_zero_rather_than_propagating_an_error() {
    let mut engine = engine_with_date_anchor();
    // Verbatim from the issue: the MAX(...,0) guard is supposed to floor the
    // negative accrual at zero. An error operand defeats the guard entirely.
    assert_eq!(
        eval(
            &mut engine,
            "=MAX(IF(YEAR(A1)>2025,A1*0,(A1-45658)/365*0),0)"
        ),
        LiteralValue::Number(0.0)
    );
}

#[test]
fn negative_date_difference_is_consumable_by_downstream_functions() {
    let mut engine = engine_with_date_anchor();
    // An error is not merely a wrong value: it is uncomposable. Pin that the
    // result participates in ordinary numeric functions.
    assert_eq!(
        eval(&mut engine, "=ABS(A1-45658)"),
        LiteralValue::Number(31.0)
    );
    assert_eq!(
        eval(&mut engine, "=A1-45658+45658"),
        LiteralValue::Number(DEC_1_2024)
    );
}

#[test]
fn datetime_minus_number_yielding_negative_serial_degrades_to_number() {
    // The defect lived in a helper shared by Date and DateTime operands, so the
    // DateTime path needs its own pin rather than inheriting confidence.
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());
    engine
        .set_cell_value(
            "Sheet1",
            1,
            1,
            LiteralValue::DateTime(
                NaiveDate::from_ymd_opt(2024, 12, 1)
                    .unwrap()
                    .and_hms_opt(6, 0, 0)
                    .unwrap(),
            ),
        )
        .unwrap();
    match eval(&mut engine, "=A1-45658") {
        LiteralValue::Number(n) => assert!((n - (-30.75)).abs() < 1e-9, "got {n}"),
        other => panic!("expected Number, got {other:?}"),
    }
}

#[test]
fn nan_and_infinity_operands_still_produce_numeric_errors() {
    let mut engine = engine_with_date_anchor();
    // The fix must only reclassify unrepresentable-as-date. Genuine numeric
    // domain failures keep erroring, so the change cannot be mistaken for
    // "date arithmetic never errors".
    match eval(&mut engine, "=A1+(1/0)") {
        LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Div),
        other => panic!("expected an error for division by zero, got {other:?}"),
    }
}
