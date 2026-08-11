//! Temporal values participate in lookups as ordinary Excel serials.
//!
//! Oracle: Microsoft Excel 16.105.3 (Microsoft 365 for Mac), synthetic
//! workbook, values recalculated in-app (`oracle: excel-verified`).
//!
//! Excel has no date type at the formula level: a date cell holds a serial
//! carrying a date number format. Every lookup family member therefore
//! compares dates against dates, and against plain numerics, exactly as it
//! compares two numbers.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use chrono::{Duration, NaiveDate, NaiveTime};
use formualizer_common::{DateSystem, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn date(y: i32, m: u32, d: u32) -> LiteralValue {
    LiteralValue::Date(NaiveDate::from_ymd_opt(y, m, d).unwrap())
}

/// Serial for 2024-01-01 in the Excel 1900 system.
const JAN_1_2024: f64 = 45292.0;

/// Column A: date-typed keys 2024-01-01..2024-01-05.
/// Column B: numeric payload 10..50.
/// Column C: the same keys written as plain numeric serials.
/// D1: a date-typed needle cell (2024-01-03).
fn build_date_engine_for(date_system: DateSystem) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_date_system(date_system),
    );
    for i in 1..=5u32 {
        engine
            .set_cell_value("Sheet1", i, 1, date(2024, 1, i))
            .unwrap();
        engine
            .set_cell_value("Sheet1", i, 2, LiteralValue::Int((i as i64) * 10))
            .unwrap();
        engine
            .set_cell_value(
                "Sheet1",
                i,
                3,
                LiteralValue::Number(JAN_1_2024 + (i as f64) - 1.0),
            )
            .unwrap();
    }
    engine
        .set_cell_value("Sheet1", 1, 4, date(2024, 1, 3))
        .unwrap();
    engine
}

fn build_date_engine() -> Engine<TestWorkbook> {
    build_date_engine_for(DateSystem::Excel1900)
}

fn eval(engine: &mut Engine<TestWorkbook>, formula: &str) -> Option<LiteralValue> {
    engine
        .set_cell_formula("Sheet1", 1, 10, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.get_cell_value("Sheet1", 1, 10)
}

fn assert_number(value: Option<LiteralValue>, expected: f64, formula: &str) {
    match value {
        Some(LiteralValue::Int(i)) => assert_eq!(i as f64, expected, "{formula}"),
        Some(LiteralValue::Number(n)) => assert!((n - expected).abs() < 1e-9, "{formula} => {n}"),
        other => panic!("{formula}: expected {expected}, got {other:?}"),
    }
}

fn assert_na(value: Option<LiteralValue>, formula: &str) {
    match value {
        Some(LiteralValue::Error(error)) => assert_eq!(error.kind, ExcelErrorKind::Na, "{formula}"),
        other => panic!("{formula}: expected #N/A, got {other:?}"),
    }
}

/// Excel: `=MATCH(D1,A1:A5,0)` => 3 where D1 and A1:A5 are date cells.
#[test]
fn match_exact_finds_a_date_typed_key_with_a_date_typed_needle() {
    let mut engine = build_date_engine();
    assert_number(
        eval(&mut engine, "=MATCH(D1,A1:A5,0)"),
        3.0,
        "MATCH(D1,A1:A5,0)",
    );
}

/// Excel: `=MATCH(DATE(2024,1,3),A1:A5,0)` => 3. A serial-valued needle must
/// find a date-typed cell: both sides are the same number in Excel.
#[test]
fn match_exact_finds_a_date_typed_key_with_a_serial_needle() {
    let mut engine = build_date_engine();
    assert_number(
        eval(&mut engine, "=MATCH(DATE(2024,1,3),A1:A5,0)"),
        3.0,
        "MATCH(DATE(2024,1,3),A1:A5,0)",
    );
}

/// Excel: `=MATCH(D1,C1:C5,0)` => 3. The mirror direction — a date-typed
/// needle against plain numeric serials.
#[test]
fn match_exact_finds_a_numeric_serial_with_a_date_typed_needle() {
    let mut engine = build_date_engine();
    assert_number(
        eval(&mut engine, "=MATCH(D1,C1:C5,0)"),
        3.0,
        "MATCH(D1,C1:C5,0)",
    );
}

/// Excel: `=MATCH(DATE(2024,1,3),A1:A5,1)` => 3 on an ascending date column.
#[test]
fn match_approximate_orders_date_typed_keys() {
    let mut engine = build_date_engine();
    assert_number(
        eval(&mut engine, "=MATCH(DATE(2024,1,3),A1:A5,1)"),
        3.0,
        "MATCH(DATE(2024,1,3),A1:A5,1)",
    );
}

/// Excel: `=MATCH(DATE(2024,1,4)+0.5,A1:A5,1)` => 4. A needle between two
/// date keys selects the largest key not greater than it, which is only
/// possible if dates order against non-integral serials.
#[test]
fn match_approximate_orders_dates_against_fractional_serials() {
    let mut engine = build_date_engine();
    assert_number(
        eval(&mut engine, "=MATCH(DATE(2024,1,4)+0.5,A1:A5,1)"),
        4.0,
        "MATCH(DATE(2024,1,4)+0.5,A1:A5,1)",
    );
}

/// Excel: `=VLOOKUP(D1,A1:B5,2,FALSE)` => 30.
#[test]
fn vlookup_exact_resolves_a_date_typed_key() {
    let mut engine = build_date_engine();
    assert_number(
        eval(&mut engine, "=VLOOKUP(D1,A1:B5,2,FALSE)"),
        30.0,
        "VLOOKUP(D1,A1:B5,2,FALSE)",
    );
}

/// Excel: `=VLOOKUP(DATE(2024,1,3),A1:B5,2,TRUE)` => 30.
#[test]
fn vlookup_approximate_resolves_a_date_typed_key() {
    let mut engine = build_date_engine();
    assert_number(
        eval(&mut engine, "=VLOOKUP(DATE(2024,1,3),A1:B5,2,TRUE)"),
        30.0,
        "VLOOKUP(DATE(2024,1,3),A1:B5,2,TRUE)",
    );
}

/// Control: a date outside the key column is still `#N/A`. Making dates
/// comparable must not make every date match something.
/// Excel: `=MATCH(DATE(2024,1,9),A1:A5,0)` => `#N/A`.
#[test]
fn match_exact_still_reports_na_for_an_absent_date() {
    let mut engine = build_date_engine();
    match eval(&mut engine, "=MATCH(DATE(2024,1,9),A1:A5,0)") {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Na),
        other => panic!("expected #N/A, got {other:?}"),
    }
}

/// A `Time` cell is the fractional part of a serial and orders the same way.
/// Excel: with A=06:00,12:00,18:00 written as times, `=MATCH(0.5,A1:A3,0)`
/// => 2 (12:00 is serial fraction 0.5).
#[test]
fn match_exact_finds_a_time_typed_key_by_its_serial_fraction() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, hour) in [(1u32, 6u32), (2, 12), (3, 18)] {
        engine
            .set_cell_value(
                "Sheet1",
                row,
                1,
                LiteralValue::Time(NaiveTime::from_hms_opt(hour, 0, 0).unwrap()),
            )
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=MATCH(0.5,A1:A3,0)"),
        2.0,
        "MATCH(0.5,A1:A3,0)",
    );
}

#[test]
fn temporal_lookup_semantics_follow_both_workbook_date_systems() {
    for (system, correct_serial, wrong_serial) in [
        (DateSystem::Excel1900, 45294.0, 43832.0),
        (DateSystem::Excel1904, 43832.0, 45294.0),
    ] {
        let mut engine = build_date_engine_for(system);
        engine
            .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(wrong_serial))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 2, 6, LiteralValue::Number(correct_serial))
            .unwrap();

        for (formula, expected) in [
            ("=MATCH(D1,A1:A5,0)", 3.0),
            ("=MATCH(D1,A1:A5,1)", 3.0),
            ("=VLOOKUP(D1,A1:B5,2,FALSE)", 30.0),
            ("=VLOOKUP(D1,A1:B5,2,TRUE)", 30.0),
            ("=XLOOKUP(D1,A1:A5,B1:B5)", 30.0),
            ("=MATCH(D1,F1:F2,0)", 2.0),
        ] {
            assert_number(eval(&mut engine, formula), expected, formula);
        }
        assert_na(
            eval(&mut engine, "=MATCH(D1,F1:F1,0)"),
            "date-system false-positive guard",
        );
    }
}

#[test]
fn approximate_temporal_needles_preserve_time_fractions() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, hour) in [(1, 6), (2, 12), (3, 18)] {
        engine
            .set_cell_value(
                "Sheet1",
                row,
                6,
                LiteralValue::Time(NaiveTime::from_hms_opt(hour, 0, 0).unwrap()),
            )
            .unwrap();
    }
    engine
        .set_cell_value(
            "Sheet1",
            1,
            4,
            LiteralValue::Time(NaiveTime::from_hms_opt(14, 24, 0).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value(
            "Sheet1",
            3,
            4,
            LiteralValue::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value(
            "Sheet1",
            2,
            4,
            LiteralValue::DateTime(
                NaiveDate::from_ymd_opt(2024, 1, 3)
                    .unwrap()
                    .and_hms_opt(10, 10, 10)
                    .unwrap(),
            ),
        )
        .unwrap();
    engine
        .set_cell_value(
            "Sheet1",
            4,
            6,
            LiteralValue::DateTime(
                NaiveDate::from_ymd_opt(2024, 1, 3)
                    .unwrap()
                    .and_hms_opt(10, 10, 10)
                    .unwrap(),
            ),
        )
        .unwrap();
    for (row, serial) in [(1, 0.25), (2, 0.5), (3, 0.75)] {
        engine
            .set_cell_value("Sheet1", row, 7, LiteralValue::Number(serial))
            .unwrap();
    }

    assert_number(
        eval(&mut engine, "=MATCH(D1,F1:F3,1)"),
        2.0,
        "MATCH temporal Time needle approximate",
    );
    assert_number(
        eval(&mut engine, "=MATCH(D2,F4:F4,1)"),
        1.0,
        "MATCH temporal DateTime needle approximate",
    );
    assert_number(
        eval(&mut engine, "=MATCH(D3,G1:G3,0)"),
        2.0,
        "MATCH fractional temporal needle exact view path",
    );
}

#[test]
fn warm_lookup_hash_index_keys_fractional_temporal_serials() {
    for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig::default().with_date_system(system),
        );
        let start = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 30, 0)
            .unwrap();
        let target = start + Duration::hours(149);
        for row in 1..=200u32 {
            engine
                .set_cell_value(
                    "Sheet1",
                    row,
                    1,
                    LiteralValue::DateTime(start + Duration::hours((row - 1) as i64)),
                )
                .unwrap();
            engine
                .set_cell_value("Sheet1", row, 2, LiteralValue::Int(row as i64 * 10))
                .unwrap();
        }
        let target_serial = LiteralValue::DateTime(target)
            .as_serial_number_for(system)
            .unwrap();
        engine
            .set_cell_value("Sheet1", 1, 4, LiteralValue::Number(target_serial))
            .unwrap();
        for row in 1..=8u32 {
            engine
                .set_cell_formula(
                    "Sheet1",
                    row,
                    5,
                    parse("=VLOOKUP($D$1,$A$1:$B$200,2,FALSE)").unwrap(),
                )
                .unwrap();
        }
        engine.evaluate_all().unwrap();
        let cache = engine.last_lookup_index_cache_report();
        assert!(
            cache.builds >= 1,
            "lookup hash index did not warm: {cache:?}"
        );
        assert!(cache.hits >= 1, "lookup hash index was not used: {cache:?}");
        for row in 1..=8u32 {
            assert_number(
                engine.get_cell_value("Sheet1", row, 5),
                1500.0,
                "warm hash-index VLOOKUP",
            );
        }
    }
}

#[test]
fn date_vlookup_with_a_blank_tail_returns_the_date_rows_payload() {
    let mut engine = build_date_engine();
    assert_number(
        eval(&mut engine, "=VLOOKUP(D1,A1:B10,2,TRUE)"),
        30.0,
        "VLOOKUP date column with blank tail",
    );
}
