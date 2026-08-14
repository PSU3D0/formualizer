//! XNPV/XIRR accept date cells in the dates argument, and truncate serials.
//!
//! Regression (#328): before the fix, dates were coerced with a local
//! date-blind helper, which dropped `Date`/`DateTime` cells from the
//! array/range collection paths, so the date vector ended up shorter than the
//! values vector and the function returned #NUM!.
//!
//! Follow-up (#328 review §2c): Excel documents that XNPV/XIRR truncate the
//! date serials to whole days ("numbers in dates are truncated to integers"),
//! so a cell holding `2024-01-01 12:00` discounts as day 45292, not 45292.5.
//!
//! Oracle: LibreOffice 24.2.7.2 headless recalculation of the same fixture
//! (values [-1000, 200, 300, 400, 500] on 2024-01-01 .. 2025-01-01 at rate
//! 10%), cross-checked against a 60-digit `decimal` recomputation:
//!
//! ```text
//! XNPV(0.1, A1:A5, B1:B5 as date cells)             = 308.187137202582
//! XIRR(A1:A5, B1:B5 as date cells)                  = 0.619758593809105
//! XNPV(0.1, A1:A5, {45292.5;45383;45474;45566;45658}) = 308.187137202582
//! ```
//!
//! The third line is the truncation control: LibreOffice keeps the .5 in the
//! cell (`=(B1-45292)*24` returns 12) and still discounts from day 45292.
use chrono::NaiveDate;

use crate::engine::{DateSystem, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

/// XNPV of the fixture at rate 10%, day deltas [0, 91, 182, 274, 366].
/// 60-digit truth is 308.18713720258222141746583200819242..., whose
/// correctly-rounded double is this literal.
const XNPV_EXPECTED: f64 = 308.187_137_202_582_2;
/// XIRR of the same fixture (root of XNPV to ~6e-15).
const XIRR_EXPECTED: f64 = 0.6197585938091048;

fn fixture_with_config(config: EvalConfig) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), config);
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

fn xnpv_fixture() -> Engine<TestWorkbook> {
    fixture_with_config(EvalConfig::default())
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

fn assert_number_from(engine: Engine<TestWorkbook>, formula: &str, expected: f64, tol: f64) {
    match eval_formula(engine, formula) {
        LiteralValue::Number(actual) => assert!(
            (actual - expected).abs() < tol,
            "{formula}: expected {expected}, got {actual}"
        ),
        other => panic!("{formula}: expected {expected}, got {other:?}"),
    }
}

fn assert_number(formula: &str, expected: f64, tol: f64) {
    assert_number_from(xnpv_fixture(), formula, expected, tol);
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
    assert_number("=XNPV(0.1,A1:A5,B1:B5)", XNPV_EXPECTED, 1e-12);
}

#[test]
fn xnpv_accepts_numeric_serials() {
    // Numeric serial dates must keep working.
    assert_number(
        "=XNPV(0.1,A1:A5,{45292,45383,45474,45566,45658})",
        XNPV_EXPECTED,
        1e-12,
    );
}

#[test]
fn xnpv_truncates_datetime_cells_to_whole_days() {
    // First date carries a time fraction: 2024-01-01 12:00 -> serial 45292.5.
    // Excel documents that XNPV truncates date serials to integers, and
    // LibreOffice 24.2.7.2 returns the same 308.187137202582 for this exact
    // input, so the time-of-day must not shift the discounting.
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
    assert_number_from(engine, "=XNPV(0.1,A1:A5,B1:B5)", XNPV_EXPECTED, 1e-12);
}

#[test]
fn xnpv_truncates_fractional_serials_to_whole_days() {
    // Same truncation, reached through plain numbers rather than date cells,
    // matching LibreOffice's `{45292.5;45383;45474;45566;45658}` control.
    assert_number(
        "=XNPV(0.1,A1:A5,{45292.5,45383,45474,45566,45658})",
        XNPV_EXPECTED,
        1e-12,
    );
    assert_number(
        "=XNPV(0.1,A1:A5,{45292.9,45383,45474,45566,45658})",
        XNPV_EXPECTED,
        1e-12,
    );
}

#[test]
fn xirr_truncates_datetime_cells_to_whole_days() {
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
    assert_number_from(engine, "=XIRR(A1:A5,B1:B5)", XIRR_EXPECTED, 1e-9);
}

#[test]
fn xnpv_missing_dates_returns_num_error() {
    // 5 values but only 3 dates must stay #NUM!.
    assert_num_error("=XNPV(0.1,A1:A5,B1:B3)");
}

#[test]
fn xirr_accepts_date_cells() {
    assert_number("=XIRR(A1:A5,B1:B5)", XIRR_EXPECTED, 1e-9);
}

#[test]
fn xirr_accepts_numeric_serials() {
    assert_number(
        "=XIRR(A1:A5,{45292,45383,45474,45566,45658})",
        XIRR_EXPECTED,
        1e-9,
    );
}

#[test]
fn xnpv_date_cells_use_the_workbook_date_system() {
    // All-date input: XNPV only looks at serial *differences*, so a 1904
    // workbook must return exactly the 1900 answer.
    let config = EvalConfig {
        date_system: DateSystem::Excel1904,
        ..EvalConfig::default()
    };
    assert_number_from(
        fixture_with_config(config.clone()),
        "=XNPV(0.1,A1:A5,B1:B5)",
        XNPV_EXPECTED,
        1e-12,
    );

    // Mixed input pins *which* serial the date cell produced: B1 is
    // 2024-01-01, which is serial 45292 under the 1900 system and 43830 under
    // the 1904 one, while the literal serials stay put. A conversion that
    // hardcoded 1900 would return XNPV_EXPECTED here instead.
    //
    // Expected value recomputed from the definition at 60 significant digits
    // with day deltas [0, 1553, 1644, 1736, 1828]:
    //   -106.957094440625988464471915527341387070579521682317473951547
    assert_number_from(
        fixture_with_config(config),
        "=XNPV(0.1,A1:A5,{43830,45383,45474,45566,45658})",
        -106.957094440626,
        1e-9,
    );
}
