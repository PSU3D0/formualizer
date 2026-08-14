//! A date cell is a number: date-bearing cells must behave exactly like the
//! numerically identical serial in every builtin that collects numbers.
//!
//! Class regression for the date-blind `coerce_literal_num`/`collect_*`
//! helpers found while reviewing #328. Each case is *differential*: the same
//! formula is evaluated over a range holding `Date`/`DateTime` cells and over
//! a range holding the identical `Int` serials, and the two must agree. Before
//! the fix the date cells were dropped by the collector, which produced either
//! a short vector (`#NUM!`/`#N/A`), a hard `#VALUE!`, or — worst — a different
//! number with no error at all.
//!
//! `SUM`/`AVERAGE`/`COUNT` already agreed (they route through the arrow fold
//! path) and are kept here as controls: the engine must not be internally
//! inconsistent about whether a date cell counts as a number.
use chrono::NaiveDate;

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn date(y: i32, m: u32, d: u32) -> LiteralValue {
    LiteralValue::Date(NaiveDate::from_ymd_opt(y, m, d).unwrap())
}

/// Column layout, 5 rows each:
///   A: pure dates                 B: the identical serials
///   C: 10,20,<date>,40,50         D: 10,20,<serial>,40,50
///   E: -1000,200,300,<date>,500   F: -1000,200,300,<serial>,500
fn fixture() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let dates = [
        (2024, 1, 1),
        (2024, 4, 1),
        (2024, 7, 1),
        (2024, 10, 1),
        (2025, 1, 1),
    ];
    let serials = [45292, 45383, 45474, 45566, 45658];
    for (i, (d, s)) in dates.iter().zip(serials.iter()).enumerate() {
        let row = i as u32 + 1;
        engine
            .set_cell_value("Sheet1", row, 1, date(d.0, d.1, d.2))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Int(*s))
            .unwrap();
    }

    for (i, v) in [10, 20, 30, 40, 50].iter().enumerate() {
        let row = i as u32 + 1;
        engine
            .set_cell_value("Sheet1", row, 3, LiteralValue::Int(*v))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 4, LiteralValue::Int(*v))
            .unwrap();
    }
    engine
        .set_cell_value("Sheet1", 3, 3, date(2024, 7, 1))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 4, LiteralValue::Int(45474))
        .unwrap();

    for (i, v) in [-1000, 200, 300, 400, 500].iter().enumerate() {
        let row = i as u32 + 1;
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Int(*v))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 6, LiteralValue::Int(*v))
            .unwrap();
    }
    engine
        .set_cell_value("Sheet1", 4, 5, date(2024, 10, 1))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 6, LiteralValue::Int(45566))
        .unwrap();

    engine
}

fn eval(engine: &mut Engine<TestWorkbook>, row: u32, formula: &str) -> LiteralValue {
    engine
        .set_cell_formula("Sheet1", row, 10, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value("Sheet1", row, 10)
        .unwrap_or(LiteralValue::Empty)
}

/// Assert that `date_formula` returns the same finite number as the
/// numerically identical `serial_formula`.
fn assert_agrees_with_serial_control(date_formula: &str, serial_formula: &str) {
    let mut engine = fixture();
    let got = eval(&mut engine, 1, date_formula);
    let want = eval(&mut engine, 2, serial_formula);
    match (&got, &want) {
        (LiteralValue::Number(a), LiteralValue::Number(b)) => {
            let tol = 1e-9 * b.abs().max(1.0);
            assert!(
                (a - b).abs() <= tol,
                "{date_formula} = {a} but serial control {serial_formula} = {b}"
            );
        }
        _ => panic!(
            "{date_formula} => {got:?}, serial control {serial_formula} => {want:?} \
             (both must be numbers)"
        ),
    }
}

#[test]
fn controls_already_agreed() {
    // These routed through the arrow fold path and were never affected; they
    // are what made the divergence of the collectors below observable.
    assert_agrees_with_serial_control("=SUM(C1:C5)", "=SUM(D1:D5)");
    assert_agrees_with_serial_control("=AVERAGE(C1:C5)", "=AVERAGE(D1:D5)");
    assert_agrees_with_serial_control("=COUNT(C1:C5)", "=COUNT(D1:D5)");
    assert_agrees_with_serial_control("=MAX(C1:C5)", "=MAX(D1:D5)");
}

#[test]
fn statistical_collectors_include_date_cells() {
    // `collect_numeric_stats` range path: silently dropped the date cell, so
    // MEDIAN/STDEV/... saw 4 values where SUM/COUNT saw 5.
    for (d, s) in [
        ("=MEDIAN(C1:C5)", "=MEDIAN(D1:D5)"),
        ("=STDEV(C1:C5)", "=STDEV(D1:D5)"),
        ("=STDEV.P(C1:C5)", "=STDEV.P(D1:D5)"),
        ("=VAR(C1:C5)", "=VAR(D1:D5)"),
        ("=LARGE(C1:C5,1)", "=LARGE(D1:D5,1)"),
        ("=SMALL(C1:C5,5)", "=SMALL(D1:D5,5)"),
        ("=PRODUCT(C1:C5)", "=PRODUCT(D1:D5)"),
        ("=DEVSQ(C1:C5)", "=DEVSQ(D1:D5)"),
        ("=AVEDEV(C1:C5)", "=AVEDEV(D1:D5)"),
        ("=PERCENTILE(C1:C5,0.5)", "=PERCENTILE(D1:D5,0.5)"),
        ("=QUARTILE(C1:C5,1)", "=QUARTILE(D1:D5,1)"),
    ] {
        assert_agrees_with_serial_control(d, s);
    }
}

#[test]
fn paired_collectors_include_date_cells() {
    // `collect_paired_arrays` built two vectors of unequal length -> #N/A.
    for (d, s) in [
        ("=CORREL(C1:C5,E1:E5)", "=CORREL(D1:D5,F1:F5)"),
        ("=SLOPE(C1:C5,E1:E5)", "=SLOPE(D1:D5,F1:F5)"),
        ("=RSQ(C1:C5,E1:E5)", "=RSQ(D1:D5,F1:F5)"),
        ("=COVARIANCE.P(C1:C5,E1:E5)", "=COVARIANCE.P(D1:D5,F1:F5)"),
    ] {
        assert_agrees_with_serial_control(d, s);
    }
}

#[test]
fn a_variant_collectors_include_date_cells() {
    // `collect_numeric_a`: MAXA/MINA returned 0.0 over an all-date range.
    for (d, s) in [
        ("=MAXA(A1:A5)", "=MAXA(B1:B5)"),
        ("=MINA(A1:A5)", "=MINA(B1:B5)"),
        ("=AVERAGEA(A1:A5)", "=AVERAGEA(B1:B5)"),
        ("=STDEVA(C1:C5)", "=STDEVA(D1:D5)"),
        ("=VARA(C1:C5)", "=VARA(D1:D5)"),
    ] {
        assert_agrees_with_serial_control(d, s);
    }
}

#[test]
fn cashflow_functions_include_date_cells() {
    // IRR/MIRR had no length guard, so a dropped cell silently changed the
    // answer; NPV hard-errored with #VALUE!; XNPV/XIRR `values` went #NUM!.
    for (d, s) in [
        ("=IRR(E1:E5)", "=IRR(F1:F5)"),
        ("=MIRR(E1:E5,0.1,0.12)", "=MIRR(F1:F5,0.1,0.12)"),
        ("=NPV(0.1,E1:E5)", "=NPV(0.1,F1:F5)"),
        ("=XNPV(0.1,E1:E5,B1:B5)", "=XNPV(0.1,F1:F5,B1:B5)"),
        ("=XIRR(E1:E5,B1:B5)", "=XIRR(F1:F5,B1:B5)"),
    ] {
        assert_agrees_with_serial_control(d, s);
    }
}

#[test]
fn bond_functions_accept_date_cells_as_date_arguments() {
    // settlement/maturity/issue are date arguments and a workbook loaded from
    // XLSX holds real Date cells there; every one of these was #VALUE!.
    for (d, s) in [
        (
            "=PRICE(A1,A5,0.05,0.06,100,2,0)",
            "=PRICE(B1,B5,0.05,0.06,100,2,0)",
        ),
        (
            "=YIELD(A1,A5,0.05,95,100,2,0)",
            "=YIELD(B1,B5,0.05,95,100,2,0)",
        ),
        (
            "=ACCRINTM(A1,A5,0.05,10000,0)",
            "=ACCRINTM(B1,B5,0.05,10000,0)",
        ),
        (
            "=ACCRINT(A1,A2,A5,0.05,10000,2,0)",
            "=ACCRINT(B1,B2,B5,0.05,10000,2,0)",
        ),
        // T-bill maturities must be within a year of settlement, so these use
        // A1/A4 (274 days) rather than A1/A5 (366 days).
        ("=TBILLPRICE(A1,A4,0.05)", "=TBILLPRICE(B1,B4,0.05)"),
        ("=TBILLYIELD(A1,A4,98)", "=TBILLYIELD(B1,B4,98)"),
        ("=TBILLEQ(A1,A4,0.05)", "=TBILLEQ(B1,B4,0.05)"),
    ] {
        assert_agrees_with_serial_control(d, s);
    }
}

#[test]
fn financial_scalars_accept_date_cells() {
    // Scalar financial arguments share the same coercion; a date there is
    // meaningless but Excel still treats it as its serial rather than #VALUE!.
    assert_agrees_with_serial_control("=SLN(A5,A1,10)", "=SLN(B5,B1,10)");
}
