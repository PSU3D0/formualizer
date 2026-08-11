//! Approximate lookup ignores entries it cannot compare against the needle.
//!
//! Oracle: Microsoft Excel 16.105.3 (Microsoft 365 for Mac), synthetic
//! workbook, values recalculated in-app (`oracle: excel-verified`).
//!
//! Excel's legacy approximate match (`MATCH` with `match_type` 1/-1,
//! `VLOOKUP`/`HLOOKUP` with `range_lookup` TRUE) searches only the entries
//! belonging to the needle's value class. Blank cells and entries of another
//! class -- a text header above a numeric column, a stray number inside a
//! text column -- are skipped. They neither make the vector look unsorted nor
//! occupy a matchable position, and the returned index is still the position
//! in the *original* range, not in the compacted one.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn eval(engine: &mut Engine<TestWorkbook>, formula: &str) -> Option<LiteralValue> {
    engine
        .set_cell_formula("Sheet1", 1, 20, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.get_cell_value("Sheet1", 1, 20)
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
        Some(LiteralValue::Error(e)) => {
            assert_eq!(e.kind, ExcelErrorKind::Na, "{formula}")
        }
        other => panic!("{formula}: expected #N/A, got {other:?}"),
    }
}

/// A: 1..5 in rows 1-5, rows 6-10 never written (blank).
/// C: "Header" in row 1, then 1..5 in rows 2-6.
/// E: 5..1 descending in rows 1-5, rows 6-10 blank.
/// G: 1, 2, blank, 4, 5 -- an interior blank.
/// I: "apple", 1, 2, "zebra" -- both classes interleaved.
/// K: "alpha", "beta", "gamma" in rows 1-3, rows 4-10 blank.
/// M: 3, 1, 5, 2, 4 -- genuinely unsorted control.
fn build_engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    fn num(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, v: i64) {
        engine
            .set_cell_value("Sheet1", row, col, LiteralValue::Int(v))
            .unwrap();
    }
    for i in 1..=5i64 {
        num(&mut engine, i as u32, 1, i);
        num(&mut engine, i as u32, 5, 6 - i);
    }
    // Column C: text header then ascending numbers.
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Text("Header".into()))
        .unwrap();
    for i in 1..=5i64 {
        num(&mut engine, (i as u32) + 1, 3, i);
    }
    // Column G: interior blank at row 3.
    num(&mut engine, 1, 7, 1);
    num(&mut engine, 2, 7, 2);
    num(&mut engine, 4, 7, 4);
    num(&mut engine, 5, 7, 5);
    // Column I: mixed classes.
    engine
        .set_cell_value("Sheet1", 1, 9, LiteralValue::Text("apple".into()))
        .unwrap();
    num(&mut engine, 2, 9, 1);
    num(&mut engine, 3, 9, 2);
    engine
        .set_cell_value("Sheet1", 4, 9, LiteralValue::Text("zebra".into()))
        .unwrap();
    // Column K: ascending text with a blank tail.
    for (row, word) in [(1u32, "alpha"), (2, "beta"), (3, "gamma")] {
        engine
            .set_cell_value("Sheet1", row, 11, LiteralValue::Text(word.into()))
            .unwrap();
    }
    // Column M: unsorted control.
    for (row, v) in [(1u32, 3i64), (2, 1), (3, 5), (4, 2), (5, 4)] {
        num(&mut engine, row, 13, v);
    }
    // Column B: payload for VLOOKUP against column A.
    for i in 1..=5i64 {
        num(&mut engine, i as u32, 2, i * 10);
    }
    engine
}

/// Excel: `=MATCH(3,A1:A10,1)` => 3. An over-wide range whose tail is blank
/// is still sorted; the blanks are not out-of-order data.
#[test]
fn blank_tail_does_not_make_an_ascending_range_unsorted() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(3,A1:A10,1)"),
        3.0,
        "MATCH(3,A1:A10,1)",
    );
}

/// Excel: `=MATCH(3.5,A1:A10,1)` => 3. A needle between keys still selects
/// the largest key not greater than it, not a trailing blank.
#[test]
fn blank_tail_is_never_the_selected_approximate_position() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(3.5,A1:A10,1)"),
        3.0,
        "MATCH(3.5,A1:A10,1)",
    );
}

/// Excel: `=MATCH(6,A1:A10,1)` => 5. A needle past the last key selects the
/// last populated row, not the last row of the range.
#[test]
fn needle_past_the_last_key_selects_the_last_populated_row() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(6,A1:A10,1)"),
        5.0,
        "MATCH(6,A1:A10,1)",
    );
}

/// Excel: `=VLOOKUP(3,A1:B10,2,TRUE)` => 30.
#[test]
fn vlookup_approximate_tolerates_a_blank_tail() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=VLOOKUP(3,A1:B10,2,TRUE)"),
        30.0,
        "VLOOKUP(3,A1:B10,2,TRUE)",
    );
}

/// Excel: `=MATCH(4,G1:G5,1)` => 4 and `=MATCH(3,G1:G5,1)` => 2, where G3 is
/// blank. Interior blanks are skipped, and the result is the position in the
/// original range.
#[test]
fn interior_blank_is_skipped_and_positions_stay_original() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(4,G1:G5,1)"),
        4.0,
        "MATCH(4,G1:G5,1)",
    );
    assert_number(
        eval(&mut engine, "=MATCH(3,G1:G5,1)"),
        2.0,
        "MATCH(3,G1:G5,1)",
    );
}

/// Excel: `=MATCH(3,C1:C6,1)` => 4, where C1 is the text "Header". The header
/// is skipped rather than treated as out-of-order data, and the answer is
/// still counted from the top of the range.
#[test]
fn text_header_above_a_numeric_column_is_skipped() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(3,C1:C6,1)"),
        4.0,
        "MATCH(3,C1:C6,1)",
    );
}

/// Excel: `=MATCH(3.5,C1:C6,1)` => 4.
#[test]
fn text_header_is_skipped_for_a_between_keys_needle() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(3.5,C1:C6,1)"),
        4.0,
        "MATCH(3.5,C1:C6,1)",
    );
}

/// Excel: `=MATCH(2,I1:I4,1)` => 3, where I1 and I4 are text. Numbers are the
/// needle's class; the surrounding text is skipped in both directions.
#[test]
fn numeric_needle_skips_text_entries_on_both_sides() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(2,I1:I4,1)"),
        3.0,
        "MATCH(2,I1:I4,1)",
    );
}

/// Excel: `=MATCH("m",I1:I4,1)` => 1. The mirror direction: a text needle
/// searches only the text entries.
#[test]
fn text_needle_skips_numeric_entries() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(\"m\",I1:I4,1)"),
        1.0,
        "MATCH(\"m\",I1:I4,1)",
    );
}

/// Excel: `=MATCH("beta",K1:K10,1)` => 2. Text vectors get the same blank
/// tolerance as numeric ones.
#[test]
fn text_vector_with_a_blank_tail_matches() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(\"beta\",K1:K10,1)"),
        2.0,
        "MATCH(\"beta\",K1:K10,1)",
    );
}

/// Excel: `=MATCH(3,E1:E10,-1)` => 3 on a descending column with a blank
/// tail. Descending mode gets the same treatment as ascending.
#[test]
fn blank_tail_does_not_make_a_descending_range_unsorted() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(3,E1:E10,-1)"),
        3.0,
        "MATCH(3,E1:E10,-1)",
    );
}

/// Control: genuinely unsorted data is still `#N/A`. Ignoring blanks must not
/// weaken the sortedness guard.
/// Excel: `=MATCH(2,M1:M5,1)` => `#N/A`.
#[test]
fn genuinely_unsorted_data_is_still_na() {
    let mut engine = build_engine();
    assert_na(eval(&mut engine, "=MATCH(2,M1:M5,1)"), "MATCH(2,M1:M5,1)");
}

/// Control: a needle below every key is still `#N/A`.
/// Excel: `=MATCH(0.5,A1:A10,1)` => `#N/A`.
#[test]
fn needle_below_every_key_is_still_na() {
    let mut engine = build_engine();
    assert_na(
        eval(&mut engine, "=MATCH(0.5,A1:A10,1)"),
        "MATCH(0.5,A1:A10,1)",
    );
}

/// Control: exact match over the same blank-tailed range is unaffected.
/// Excel: `=MATCH(3,A1:A10,0)` => 3.
///
/// Note: `=MATCH(0,A1:A10,0)` is `#N/A` in Excel but returns 6 here, because
/// the exact path coerces a blank cell to numeric zero. That is a separate
/// defect on a separate code path and is filed on its own; this PR neither
/// fixes nor worsens it.
#[test]
fn exact_match_over_a_blank_tail_is_unaffected() {
    let mut engine = build_engine();
    assert_number(
        eval(&mut engine, "=MATCH(3,A1:A10,0)"),
        3.0,
        "MATCH(3,A1:A10,0)",
    );
}
