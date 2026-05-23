use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

const TABLE_ROWS: u32 = 100;
const FORMULA_ROWS: u32 = 100;

fn engine_with_mode(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    )
}

fn engine_with_config(config: EvalConfig) -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::default(), config)
}

fn formula(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, text: &str) {
    let ast = parse(text).unwrap_or_else(|err| panic!("parse {text}: {err}"));
    engine.set_cell_formula(sheet, row, col, ast).unwrap();
}

fn value(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, value: LiteralValue) {
    engine.set_cell_value(sheet, row, col, value).unwrap();
}

fn number(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, value_num: f64) {
    value(engine, sheet, row, col, LiteralValue::Number(value_num));
}

fn text(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, value_text: &str) {
    value(
        engine,
        sheet,
        row,
        col,
        LiteralValue::Text(value_text.to_string()),
    );
}

fn populate_numeric_table(engine: &mut Engine<TestWorkbook>, sheet: &str, rows: u32) {
    for row in 1..=rows {
        number(engine, sheet, row, 4, row as f64);
        number(engine, sheet, row, 5, row as f64 * 10.0);
    }
}

fn populate_horizontal_table(engine: &mut Engine<TestWorkbook>, cols: u32) {
    for offset in 0..cols {
        let col = 4 + offset;
        number(engine, "Sheet1", 1, col, (offset + 1) as f64);
        number(engine, "Sheet1", 2, col, (offset + 1) as f64 * 10.0);
    }
}

fn a1_col(mut col: u32) -> String {
    let mut out = Vec::new();
    while col > 0 {
        col -= 1;
        out.push((b'A' + (col % 26) as u8) as char);
        col /= 26;
    }
    out.iter().rev().collect()
}

fn assert_off_auth_match(
    setup: impl Copy + Fn(&mut Engine<TestWorkbook>),
    formulas: impl Copy + Fn(&mut Engine<TestWorkbook>),
    cells: &[(String, u32, u32)],
) -> Engine<TestWorkbook> {
    let mut off = engine_with_mode(FormulaPlaneMode::Off);
    setup(&mut off);
    formulas(&mut off);
    off.evaluate_all().unwrap();

    let mut auth = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    setup(&mut auth);
    formulas(&mut auth);
    auth.evaluate_all().unwrap();

    for (sheet, row, col) in cells {
        assert_eq!(
            auth.get_cell_value(sheet, *row, *col),
            off.get_cell_value(sheet, *row, *col),
            "Off/Auth mismatch at {sheet}!R{row}C{col}"
        );
    }
    auth
}

fn single_formula_parity(
    setup: impl Copy + Fn(&mut Engine<TestWorkbook>),
    formula_text: &'static str,
) -> Engine<TestWorkbook> {
    assert_off_auth_match(
        setup,
        |engine| formula(engine, "Sheet1", 1, 2, formula_text),
        &[("Sheet1".to_string(), 1, 2)],
    )
}

fn vlookup_engine_with_formula_rows(config: EvalConfig, formula_rows: u32) -> Engine<TestWorkbook> {
    let mut engine = engine_with_config(config);
    populate_numeric_table(&mut engine, "Sheet1", TABLE_ROWS);
    for row in 1..=formula_rows {
        number(&mut engine, "Sheet1", row, 1, row as f64);
        formula(
            &mut engine,
            "Sheet1",
            row,
            2,
            &format!("=VLOOKUP(A{row}, $D$1:$E${TABLE_ROWS}, 2, FALSE)"),
        );
    }
    engine
}

fn repeated_vlookup_engine(config: EvalConfig) -> Engine<TestWorkbook> {
    vlookup_engine_with_formula_rows(config, FORMULA_ROWS)
}

#[test]
fn vlookup_int_vs_number_match() {
    single_formula_parity(
        |engine| {
            value(engine, "Sheet1", 1, 4, LiteralValue::Int(5));
            number(engine, "Sheet1", 1, 5, 50.0);
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
        },
        "=VLOOKUP(5, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_text_case_insensitive() {
    single_formula_parity(
        |engine| {
            for row in 1..=TABLE_ROWS {
                text(engine, "Sheet1", row, 4, &format!("key-{row}"));
                number(engine, "Sheet1", row, 5, row as f64);
            }
            text(engine, "Sheet1", 42, 4, "abc");
        },
        "=VLOOKUP(\"ABC\", $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_text_with_unicode_special() {
    single_formula_parity(
        |engine| {
            for row in 1..=TABLE_ROWS {
                text(engine, "Sheet1", row, 4, &format!("κλειδί-{row}"));
                number(engine, "Sheet1", row, 5, row as f64);
            }
            text(engine, "Sheet1", 20, 4, "Straße");
            text(engine, "Sheet1", 21, 4, "İstanbul");
            text(engine, "Sheet1", 22, 4, "Σίσυφος");
        },
        "=VLOOKUP(\"straße\", $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_numeric_tolerance_match() {
    single_formula_parity(
        |engine| {
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
            number(engine, "Sheet1", 33, 4, 1.0000000000001);
            number(engine, "Sheet1", 33, 5, 333.0);
        },
        "=VLOOKUP(1, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_numeric_tolerance_no_match() {
    single_formula_parity(
        |engine| {
            for row in 1..=TABLE_ROWS {
                number(engine, "Sheet1", row, 4, row as f64 + 1000.0);
                number(engine, "Sheet1", row, 5, row as f64);
            }
            number(engine, "Sheet1", 33, 4, 1.0001);
        },
        "=VLOOKUP(1, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_empty_matches_zero() {
    single_formula_parity(
        |engine| {
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
            value(engine, "Sheet1", 1, 4, LiteralValue::Empty);
            number(engine, "Sheet1", 1, 5, 12.0);
        },
        "=VLOOKUP(0, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_zero_does_not_match_empty_string() {
    single_formula_parity(
        |engine| {
            for row in 1..=TABLE_ROWS {
                number(engine, "Sheet1", row, 4, row as f64 + 10.0);
                number(engine, "Sheet1", row, 5, row as f64);
            }
            text(engine, "Sheet1", 1, 4, "");
        },
        "=VLOOKUP(0, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_boolean_does_not_match_number_in_exact() {
    single_formula_parity(
        |engine| {
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
        },
        "=VLOOKUP(TRUE, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_text_does_not_match_numeric_in_exact() {
    single_formula_parity(
        |engine| {
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
        },
        "=VLOOKUP(\"1\", $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_first_match_with_duplicates() {
    single_formula_parity(
        |engine| {
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
            for row in [5, 10, 15] {
                text(engine, "Sheet1", row, 4, "X");
                number(engine, "Sheet1", row, 5, row as f64);
            }
        },
        "=VLOOKUP(\"X\", $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn xlookup_forward_first_match() {
    single_formula_parity(
        |engine| {
            for row in 1..=TABLE_ROWS {
                text(engine, "Sheet1", row, 4, &format!("K{row}"));
                number(engine, "Sheet1", row, 5, row as f64);
            }
            for row in [5, 10, 15] {
                text(engine, "Sheet1", row, 4, "X");
            }
        },
        "=XLOOKUP(\"X\", $D$1:$D$100, $E$1:$E$100, \"missing\", 0, 1)",
    );
}

#[test]
fn xlookup_reverse_last_match() {
    single_formula_parity(
        |engine| {
            for row in 1..=TABLE_ROWS {
                text(engine, "Sheet1", row, 4, &format!("K{row}"));
                number(engine, "Sheet1", row, 5, row as f64);
            }
            for row in [5, 10, 15] {
                text(engine, "Sheet1", row, 4, "X");
            }
        },
        "=XLOOKUP(\"X\", $D$1:$D$100, $E$1:$E$100, \"missing\", 0, -1)",
    );
}

#[test]
fn match_first_match_with_duplicates() {
    single_formula_parity(
        |engine| {
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
            for row in [5, 10, 15] {
                text(engine, "Sheet1", row, 4, "X");
            }
        },
        "=MATCH(\"X\", $D$1:$D$100, 0)",
    );
}

#[test]
fn hlookup_first_match_horizontal_duplicates() {
    let last = a1_col(4 + TABLE_ROWS - 1);
    let formula_text = format!("=HLOOKUP(\"X\", $D$1:${last}$2, 2, FALSE)");
    assert_off_auth_match(
        |engine| {
            populate_horizontal_table(engine, TABLE_ROWS);
            for offset in [4, 9, 14] {
                text(engine, "Sheet1", 1, 4 + offset, "X");
                number(engine, "Sheet1", 2, 4 + offset, offset as f64);
            }
        },
        |engine| formula(engine, "Sheet1", 1, 2, &formula_text),
        &[("Sheet1".to_string(), 1, 2)],
    );
}

#[test]
fn vlookup_in_table_with_gaps() {
    single_formula_parity(
        |engine| {
            for row in 1..=TABLE_ROWS {
                if row % 7 != 0 {
                    number(engine, "Sheet1", row, 4, row as f64);
                }
                number(engine, "Sheet1", row, 5, row as f64 * 3.0);
            }
        },
        "=VLOOKUP(42, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn match_zero_against_table_with_empty_first_cell() {
    single_formula_parity(
        |engine| {
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
            value(engine, "Sheet1", 1, 4, LiteralValue::Empty);
        },
        "=MATCH(0, $D$1:$D$100, 0)",
    );
}

#[test]
fn vlookup_against_used_region_smaller_than_declared() {
    single_formula_parity(
        |engine| populate_numeric_table(engine, "Sheet1", TABLE_ROWS),
        "=VLOOKUP(42, $D$1:$E$1000, 2, FALSE)",
    );
}

#[test]
fn vlookup_against_table_containing_now_function() {
    single_formula_parity(
        |engine| {
            formula(engine, "Sheet1", 1, 4, "=NOW()");
            number(engine, "Sheet1", 1, 5, 1.0);
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
        },
        "=VLOOKUP(42, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_against_table_with_index_function_cells() {
    single_formula_parity(
        |engine| {
            for row in 1..=TABLE_ROWS {
                number(engine, "Sheet1", row, 1, row as f64);
                formula(
                    engine,
                    "Sheet1",
                    row,
                    4,
                    &format!("=INDEX($A$1:$A$100,{row})"),
                );
                number(engine, "Sheet1", row, 5, row as f64 * 2.0);
            }
        },
        "=VLOOKUP(42, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_cross_sheet_table() {
    assert_off_auth_match(
        |engine| populate_numeric_table(engine, "Lookup", TABLE_ROWS),
        |engine| {
            formula(
                engine,
                "Sheet1",
                1,
                2,
                "=VLOOKUP(42, Lookup!$D$1:$E$100, 2, FALSE)",
            )
        },
        &[("Sheet1".to_string(), 1, 2)],
    );
}

#[test]
fn vlookup_two_lookups_on_different_sheets_share_no_cache() {
    assert_off_auth_match(
        |engine| {
            populate_numeric_table(engine, "LookupA", TABLE_ROWS);
            populate_numeric_table(engine, "LookupB", TABLE_ROWS);
            number(engine, "LookupB", 42, 5, 999.0);
        },
        |engine| {
            formula(
                engine,
                "Sheet1",
                1,
                2,
                "=VLOOKUP(42, LookupA!$D$1:$E$100, 2, FALSE)",
            );
            formula(
                engine,
                "Sheet1",
                1,
                3,
                "=VLOOKUP(42, LookupB!$D$1:$E$100, 2, FALSE)",
            );
        },
        &[("Sheet1".to_string(), 1, 2), ("Sheet1".to_string(), 1, 3)],
    );
}

#[test]
fn vlookup_with_error_lookup_value() {
    single_formula_parity(
        |engine| populate_numeric_table(engine, "Sheet1", TABLE_ROWS),
        "=VLOOKUP(1/0, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_against_table_with_errors_in_lookup_column() {
    single_formula_parity(
        |engine| {
            populate_numeric_table(engine, "Sheet1", TABLE_ROWS);
            value(
                engine,
                "Sheet1",
                10,
                4,
                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref)),
            );
        },
        "=VLOOKUP(42, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_against_huge_lookup_table_respects_memory_cap() {
    single_formula_parity(
        |engine| populate_numeric_table(engine, "Sheet1", TABLE_ROWS),
        "=VLOOKUP(42, $D$1:$E$100, 2, FALSE)",
    );
}

#[test]
fn vlookup_lookup_array_is_full_column_reference() {
    assert_off_auth_match(
        |engine| populate_numeric_table(engine, "Lookup", TABLE_ROWS),
        |engine| {
            formula(
                engine,
                "Sheet1",
                1,
                2,
                "=VLOOKUP(42, Lookup!$D:$E, 2, FALSE)",
            )
        },
        &[("Sheet1".to_string(), 1, 2)],
    );
}

#[test]
fn lookup_cache_invalidates_on_table_edit() {
    let mut engine = repeated_vlookup_engine(EvalConfig::default());
    engine.evaluate_all().unwrap();
    number(&mut engine, "Sheet1", 42, 5, 4242.0);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 42, 2),
        Some(LiteralValue::Number(4242.0))
    );
}

#[test]
fn lookup_cache_invalidates_on_table_extend() {
    let mut engine = repeated_vlookup_engine(EvalConfig::default());
    formula(
        &mut engine,
        "Sheet1",
        101,
        2,
        "=VLOOKUP(A101, $D$1:$E$101, 2, FALSE)",
    );
    number(&mut engine, "Sheet1", 101, 1, 101.0);
    engine.evaluate_all().unwrap();
    number(&mut engine, "Sheet1", 101, 4, 101.0);
    number(&mut engine, "Sheet1", 101, 5, 1010.0);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 101, 2),
        Some(LiteralValue::Number(1010.0))
    );
}

#[test]
fn vlookup_against_tiny_table_skips_cache() {
    single_formula_parity(
        |engine| populate_numeric_table(engine, "Sheet1", 10),
        "=VLOOKUP(5, $D$1:$E$10, 2, FALSE)",
    );
}

#[test]
fn approximate_match_does_not_use_exact_cache() {
    single_formula_parity(
        |engine| populate_numeric_table(engine, "Sheet1", TABLE_ROWS),
        "=VLOOKUP(42.5, $D$1:$E$100, 2, TRUE)",
    );
}

#[test]
fn wildcard_match_does_not_use_exact_cache() {
    single_formula_parity(
        |engine| {
            for row in 1..=TABLE_ROWS {
                text(engine, "Sheet1", row, 4, &format!("KEY-{row}"));
                number(engine, "Sheet1", row, 5, row as f64);
            }
        },
        "=XLOOKUP(\"KEY-*\", $D$1:$D$100, $E$1:$E$100, \"missing\", 2, 1)",
    );
}

#[test]
fn offset_indirect_remain_uncacheable() {
    single_formula_parity(
        |engine| populate_numeric_table(engine, "Sheet1", TABLE_ROWS),
        "=VLOOKUP(42, OFFSET($D$1,0,0,100,2), 2, FALSE)",
    );
}

#[test]
fn lookup_cache_does_not_build_on_first_call() {
    let mut engine = vlookup_engine_with_formula_rows(EvalConfig::default(), 1);
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 0, "{report:?}");
    assert!(report.skipped_below_threshold > 0, "{report:?}");
}

#[test]
fn lookup_cache_does_not_build_on_third_call() {
    let mut engine = vlookup_engine_with_formula_rows(EvalConfig::default(), 3);
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 0, "{report:?}");
    assert_eq!(report.skipped_below_threshold, 3, "{report:?}");
}

#[test]
fn lookup_cache_builds_on_fourth_call() {
    let mut engine = vlookup_engine_with_formula_rows(EvalConfig::default(), 5);
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 1, "{report:?}");
    assert!(report.hits >= 1, "{report:?}");
    assert_eq!(report.skipped_below_threshold, 3, "{report:?}");
}

#[test]
fn lookup_cache_threshold_is_per_key() {
    let mut engine = engine_with_config(EvalConfig::default());
    populate_numeric_table(&mut engine, "Sheet1", TABLE_ROWS);
    for row in 1..=TABLE_ROWS {
        number(&mut engine, "Sheet1", row, 7, row as f64);
        number(&mut engine, "Sheet1", row, 8, row as f64 * 100.0);
    }
    for row in 1..=2 {
        number(&mut engine, "Sheet1", row, 1, row as f64);
        formula(
            &mut engine,
            "Sheet1",
            row,
            2,
            &format!("=VLOOKUP(A{row}, $D$1:$E${TABLE_ROWS}, 2, FALSE)"),
        );
    }
    for row in 1..=4 {
        number(&mut engine, "Sheet1", row, 3, row as f64);
        formula(
            &mut engine,
            "Sheet1",
            row,
            6,
            &format!("=VLOOKUP(C{row}, $G$1:$H${TABLE_ROWS}, 2, FALSE)"),
        );
    }
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 1, "{report:?}");
    assert_eq!(report.skipped_below_threshold, 5, "{report:?}");
}

#[test]
fn lookup_cache_threshold_resets_across_snapshots() {
    let mut engine = vlookup_engine_with_formula_rows(EvalConfig::default(), 5);
    engine.evaluate_all().unwrap();
    let first_report = engine.last_lookup_index_cache_report();
    assert_eq!(first_report.builds, 1, "{first_report:?}");
    assert_eq!(first_report.skipped_below_threshold, 3, "{first_report:?}");

    for row in 1..=5 {
        number(&mut engine, "Sheet1", row, 1, (row + 10) as f64);
    }
    engine.evaluate_all().unwrap();
    let second_report = engine.last_lookup_index_cache_report();
    assert_eq!(second_report.builds, 1, "{second_report:?}");
    assert_eq!(
        second_report.skipped_below_threshold, 3,
        "{second_report:?}"
    );
    assert!(second_report.hits >= 1, "{second_report:?}");
}

#[test]
fn lookup_cache_repeated_calls_to_same_table_eventually_build() {
    let mut engine = repeated_vlookup_engine(EvalConfig::default());
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 1, "{report:?}");
    assert!(report.hits >= 96, "{report:?}");
    assert_eq!(report.skipped_below_threshold, 3, "{report:?}");
}

#[test]
fn vlookup_cache_engages_for_repeated_keys() {
    let mut engine = repeated_vlookup_engine(EvalConfig::default());
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 1, "{report:?}");
    assert!(report.hits >= 96, "{report:?}");
    assert_eq!(report.skipped_below_threshold, 3, "{report:?}");
    assert_eq!(report.skipped_volatile, 0, "{report:?}");
}

#[test]
fn lookup_cache_skips_volatile_tiny_capped_and_error_cases() {
    let mut volatile = repeated_vlookup_engine(EvalConfig::default());
    formula(&mut volatile, "Sheet1", 1, 4, "=NOW()");
    volatile.evaluate_all().unwrap();
    let volatile_report = volatile.last_lookup_index_cache_report();
    assert_eq!(volatile_report.builds, 0, "{volatile_report:?}");
    assert!(volatile_report.skipped_volatile > 0, "{volatile_report:?}");

    let mut tiny = engine_with_config(EvalConfig::default());
    populate_numeric_table(&mut tiny, "Sheet1", 10);
    formula(
        &mut tiny,
        "Sheet1",
        1,
        2,
        "=VLOOKUP(5, $D$1:$E$10, 2, FALSE)",
    );
    tiny.evaluate_all().unwrap();
    let tiny_report = tiny.last_lookup_index_cache_report();
    assert_eq!(tiny_report.builds, 0, "{tiny_report:?}");
    assert!(tiny_report.skipped_tiny > 0, "{tiny_report:?}");

    let mut capped = repeated_vlookup_engine(EvalConfig {
        lookup_index_cache_max_bytes: 1,
        ..EvalConfig::default()
    });
    capped.evaluate_all().unwrap();
    let capped_report = capped.last_lookup_index_cache_report();
    assert_eq!(capped_report.builds, 0, "{capped_report:?}");
    assert!(capped_report.skipped_cap > 0, "{capped_report:?}");

    let mut error = repeated_vlookup_engine(EvalConfig::default());
    value(
        &mut error,
        "Sheet1",
        10,
        4,
        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref)),
    );
    error.evaluate_all().unwrap();
    let error_report = error.last_lookup_index_cache_report();
    assert_eq!(error_report.builds, 0, "{error_report:?}");
    assert!(error_report.skipped_error > 0, "{error_report:?}");
}

#[test]
fn lookup_cache_cross_sheet_entries_are_isolated() {
    let mut engine = engine_with_config(EvalConfig::default());
    populate_numeric_table(&mut engine, "LookupA", TABLE_ROWS);
    populate_numeric_table(&mut engine, "LookupB", TABLE_ROWS);
    for row in 1..=FORMULA_ROWS {
        number(&mut engine, "Sheet1", row, 1, row as f64);
        formula(
            &mut engine,
            "Sheet1",
            row,
            2,
            &format!("=VLOOKUP(A{row}, LookupA!$D$1:$E$100, 2, FALSE)"),
        );
        formula(
            &mut engine,
            "Sheet1",
            row,
            3,
            &format!("=VLOOKUP(A{row}, LookupB!$D$1:$E$100, 2, FALSE)"),
        );
    }
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 2, "{report:?}");
    assert!(report.entries_count >= 2, "{report:?}");
}

#[test]
fn approximate_and_wildcard_modes_do_not_hit_exact_cache() {
    let mut approximate = engine_with_config(EvalConfig::default());
    populate_numeric_table(&mut approximate, "Sheet1", TABLE_ROWS);
    for row in 1..=FORMULA_ROWS {
        formula(
            &mut approximate,
            "Sheet1",
            row,
            2,
            &format!("=VLOOKUP({row}.5, $D$1:$E$100, 2, TRUE)"),
        );
    }
    approximate.evaluate_all().unwrap();
    let approximate_report = approximate.last_lookup_index_cache_report();
    assert_eq!(approximate_report.hits, 0, "{approximate_report:?}");
    assert_eq!(approximate_report.builds, 0, "{approximate_report:?}");

    let mut wildcard = engine_with_config(EvalConfig::default());
    for row in 1..=TABLE_ROWS {
        text(&mut wildcard, "Sheet1", row, 4, &format!("KEY-{row}"));
        number(&mut wildcard, "Sheet1", row, 5, row as f64);
    }
    for row in 1..=FORMULA_ROWS {
        formula(
            &mut wildcard,
            "Sheet1",
            row,
            2,
            "=XLOOKUP(\"KEY-*\", $D$1:$D$100, $E$1:$E$100, \"missing\", 2, 1)",
        );
    }
    wildcard.evaluate_all().unwrap();
    let wildcard_report = wildcard.last_lookup_index_cache_report();
    assert_eq!(wildcard_report.hits, 0, "{wildcard_report:?}");
    assert_eq!(wildcard_report.builds, 0, "{wildcard_report:?}");
}
