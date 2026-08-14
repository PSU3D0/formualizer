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
//!
//! Error cells are skipped on exactly the same terms (issue #326, oracle rows
//! reproduced in `issue_326_error_skip_oracle.rs`): they are projected out of
//! the search, cannot be returned, never break sortedness, and are not
//! propagated even when a bisection probe lands on one. A range with nothing
//! searchable left yields #N/A.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use chrono::NaiveDate;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
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

fn assert_error(value: Option<LiteralValue>, expected: ExcelErrorKind, formula: &str) {
    match value {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, expected, "{formula}"),
        other => panic!("{formula}: expected {expected:?}, got {other:?}"),
    }
}

fn assert_na(value: Option<LiteralValue>, formula: &str) {
    assert_error(value, ExcelErrorKind::Na, formula);
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

#[test]
fn approximate_lookups_skip_error_entries_in_any_lookup_position() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let error = LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div));

    // A = 1, 2, #DIV/0!, 9 with B = 10, 20, 30, 40.
    for (row, value) in [
        LiteralValue::Int(1),
        LiteralValue::Int(2),
        error.clone(),
        LiteralValue::Int(9),
    ]
    .into_iter()
    .enumerate()
    {
        engine
            .set_cell_value("Sheet1", row as u32 + 1, 1, value)
            .unwrap();
        engine
            .set_cell_value(
                "Sheet1",
                row as u32 + 1,
                2,
                LiteralValue::Int((row as i64 + 1) * 10),
            )
            .unwrap();
    }
    // The error sits where the search decides. Excel projects it out and
    // answers from the surviving entries: the largest value <= 5 is the 2 in
    // row 2, counted in the original range.
    assert_number(
        eval(&mut engine, "=MATCH(5,A1:A4,1)"),
        2.0,
        "MATCH ascending skips a deciding error",
    );
    assert_number(
        eval(&mut engine, "=VLOOKUP(5,A1:B4,2,TRUE)"),
        20.0,
        "VLOOKUP skips a deciding error",
    );

    // Move the error past a key that already disqualifies the final row: the
    // answer is unchanged, so skipping is not search-path dependent.
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(9))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 1, error.clone())
        .unwrap();
    assert_number(
        eval(&mut engine, "=MATCH(5,A1:A4,1)"),
        2.0,
        "MATCH ascending skips a non-deciding error",
    );
    assert_number(
        eval(&mut engine, "=VLOOKUP(5,A1:B4,2,TRUE)"),
        20.0,
        "VLOOKUP skips a non-deciding error",
    );

    for (col, value) in [
        LiteralValue::Int(1),
        LiteralValue::Int(2),
        LiteralValue::Int(9),
        error.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        engine
            .set_cell_value("Sheet1", 10, col as u32 + 1, value)
            .unwrap();
        engine
            .set_cell_value(
                "Sheet1",
                11,
                col as u32 + 1,
                LiteralValue::Int((col as i64 + 1) * 10),
            )
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=HLOOKUP(5,A10:D11,2,TRUE)"),
        20.0,
        "HLOOKUP skips a non-deciding error",
    );

    // Descending: 9, #DIV/0!, 2, 1. The smallest entry >= 5 is the 9 in row 1.
    for (row, value) in [
        LiteralValue::Int(9),
        error,
        LiteralValue::Int(2),
        LiteralValue::Int(1),
    ]
    .into_iter()
    .enumerate()
    {
        engine
            .set_cell_value("Sheet1", row as u32 + 1, 5, value)
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=MATCH(5,E1:E4,-1)"),
        1.0,
        "MATCH descending skips a deciding error",
    );

    // An all-error range has nothing searchable: #N/A, not the error.
    for row in 1..=3u32 {
        engine
            .set_cell_value(
                "Sheet1",
                row,
                6,
                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
            )
            .unwrap();
    }
    assert_na(eval(&mut engine, "=MATCH(5,F1:F3,1)"), "all-error range");

    // Exact mode never matches an error cell.
    assert_na(
        eval(&mut engine, "=MATCH(5,F1:F3,0)"),
        "exact mode over errors",
    );
}

#[test]
fn approximate_match_skips_materialized_pre_1900_date_error() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, date) in [(1899, 1, 1), (1899, 6, 1), (1900, 3, 1), (2024, 1, 1)]
        .into_iter()
        .enumerate()
    {
        engine
            .set_cell_value(
                "Sheet1",
                row as u32 + 1,
                1,
                LiteralValue::Date(NaiveDate::from_ymd_opt(date.0, date.1, date.2).unwrap()),
            )
            .unwrap();
    }
    // The three pre-1900 dates materialize as #NUM!. Excel projects error cells
    // out of an approximate search, so the only searchable entry is the 2024
    // date in row 4, and the needle lands on it.
    assert_number(
        eval(&mut engine, "=MATCH(50000,A1:A4,1)"),
        4.0,
        "MATCH skips pre-1900 date errors",
    );
}

#[test]
fn ascending_duplicates_are_sorted_and_return_the_last_original_position() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, value) in [(1, 1), (2, 2), (4, 2), (6, 2), (8, 5)] {
        engine
            .set_cell_value("Sheet1", row, 8, LiteralValue::Int(value))
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=MATCH(3,H1:H8,1)"),
        6.0,
        "MATCH duplicate ascending keys",
    );
}

#[test]
fn vlookup_maps_a_projected_match_back_to_the_original_row() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("Key".into()))
        .unwrap();
    for value in 1..=5i64 {
        engine
            .set_cell_value("Sheet1", value as u32 + 1, 1, LiteralValue::Int(value))
            .unwrap();
        engine
            .set_cell_value("Sheet1", value as u32 + 1, 2, LiteralValue::Int(value * 10))
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=VLOOKUP(3.5,A1:B6,2,TRUE)"),
        30.0,
        "VLOOKUP original-position remap",
    );
}

#[test]
fn searchable_count_selects_the_small_descending_linear_path() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, value) in (1..=5i64).rev().enumerate() {
        engine
            .set_cell_value("Sheet1", row as u32 + 1, 5, LiteralValue::Int(value))
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=MATCH(3.5,E1:E10,-1)"),
        2.0,
        "MATCH threshold uses searchable count",
    );
}

#[test]
fn descending_binary_search_returns_the_last_qualifying_position() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, value) in (1..=10i64).rev().enumerate() {
        engine
            .set_cell_value("Sheet1", row as u32 + 1, 1, LiteralValue::Int(value))
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=MATCH(3.5,A1:A10,-1)"),
        7.0,
        "MATCH descending binary boundary",
    );
    assert_number(
        eval(&mut engine, "=MATCH(3,A1:A10,-1)"),
        8.0,
        "MATCH descending exact-hit masking control",
    );
}

#[test]
fn descending_binary_search_skips_interior_blanks_without_shifting_the_answer() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, value) in [
        (1, 9),
        (2, 8),
        (3, 7),
        (5, 6),
        (6, 5),
        (7, 4),
        (9, 3),
        (10, 2),
        (11, 1),
    ] {
        engine
            .set_cell_value("Sheet1", row, 7, LiteralValue::Int(value))
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=MATCH(5.5,G1:G11,-1)"),
        5.0,
        "MATCH descending with interior blanks",
    );
}

#[test]
fn searchable_count_keeps_duplicate_tie_break_on_small_descending_data() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, value) in [(1, 5), (2, 4), (3, 4), (4, 3), (5, 2)] {
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Int(value))
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=MATCH(4,E1:E10,-1)"),
        2.0,
        "MATCH threshold preserves descending duplicate tie-break",
    );
}

#[test]
fn searchable_count_not_range_extent_controls_descending_tie_break() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, value) in [(1, 5), (3, 4), (4, 4), (6, 3), (8, 2)] {
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Int(value))
            .unwrap();
    }
    for row in [2, 5, 7, 9, 10] {
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Text("skip".into()))
            .unwrap();
    }
    assert_number(
        eval(&mut engine, "=MATCH(4,E1:E10,-1)"),
        3.0,
        "MATCH threshold uses projected length, not range extent",
    );
}
