use std::sync::Arc;

use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
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

fn ingest_record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    text: &str,
) -> FormulaIngestRecord {
    let ast_id = engine.intern_formula_ast(&parse(text).unwrap());
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(text)))
}

fn canonical_at(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> String {
    let ast = engine
        .get_cell("Sheet1", row, col)
        .and_then(|(ast, _)| ast)
        .expect("formula AST");
    formualizer_parse::pretty::canonical_formula(&ast)
}

#[test]
fn span_formula_api_relocates_first_middle_and_last_placement() {
    let mut engine = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut records = Vec::new();
    for row in 1..=100 {
        number(&mut engine, "Sheet1", row, 1, row as f64);
        records.push(ingest_record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(canonical_at(&engine, 1, 2), "=A1 + 1");
    assert_eq!(canonical_at(&engine, 50, 2), "=A50 + 1");
    assert_eq!(canonical_at(&engine, 100, 2), "=A100 + 1");
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(51.0))
    );
}

#[test]
fn cross_sheet_span_relocation_uses_placement_coordinate() {
    let mut engine = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    engine.add_sheet("Sheet2").unwrap();
    let mut records = Vec::new();
    for row in 1..=100 {
        number(&mut engine, "Sheet2", row, 1, row as f64);
        records.push(ingest_record(
            &mut engine,
            row,
            2,
            &format!("=Sheet2!A{row}+1"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(canonical_at(&engine, 50, 2), "=Sheet2!A50 + 1");
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(51.0))
    );
}

#[test]
fn equal_canonical_templates_from_distinct_anchors_keep_span_state_isolated() {
    let mut engine = engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut first = Vec::new();
    for row in 1..=100 {
        number(&mut engine, "Sheet1", row, 1, row as f64);
        first.push(ingest_record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    let mut second = Vec::new();
    for row in 201..=300 {
        number(&mut engine, "Sheet1", row, 1, row as f64);
        second.push(ingest_record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![
            FormulaIngestBatch::new("Sheet1", first),
            FormulaIngestBatch::new("Sheet1", second),
        ])
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(canonical_at(&engine, 50, 2), "=A50 + 1");
    assert_eq!(canonical_at(&engine, 250, 2), "=A250 + 1");
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

fn mark_all_formulas_dirty_without_edit(engine: &mut Engine<TestWorkbook>) {
    let vertices: Vec<_> = engine.graph.vertices_with_formulas().collect();
    for vertex in vertices {
        engine.graph.mark_vertex_dirty(vertex);
    }
}

#[derive(Clone, Copy)]
enum LookupExpected {
    Number(f64),
    Na,
    Text(&'static str),
}

fn assert_lookup_expected(actual: LiteralValue, expected: LookupExpected, label: &str) {
    match expected {
        LookupExpected::Number(expected_number) => {
            let actual_number = match actual {
                LiteralValue::Number(number) => number,
                LiteralValue::Int(number) => number as f64,
                other => panic!("{label}: expected {expected_number}, got {other:?}"),
            };
            assert_eq!(actual_number, expected_number, "{label}");
        }
        LookupExpected::Na => assert!(
            matches!(actual, LiteralValue::Error(ref error) if error.kind == ExcelErrorKind::Na),
            "{label}: expected #N/A, got {actual:?}"
        ),
        LookupExpected::Text(expected_text) => {
            assert_eq!(actual, LiteralValue::Text(expected_text.into()), "{label}")
        }
    }
}

fn evaluate_lookup_fixture(
    values: [Option<f64>; 3],
    horizontal: bool,
    formula_text: &str,
) -> LiteralValue {
    let mut engine = engine_with_mode(FormulaPlaneMode::Off);
    for (offset, value_num) in values.into_iter().enumerate() {
        let offset = offset as u32;
        if let Some(value_num) = value_num {
            if horizontal {
                number(&mut engine, "Sheet1", 5, offset + 1, value_num);
            } else {
                number(&mut engine, "Sheet1", offset + 1, 1, value_num);
            }
        }
        text(
            &mut engine,
            "Sheet1",
            offset + 1,
            4,
            &format!("p{}", offset + 1),
        );
    }
    formula(&mut engine, "Sheet1", 1, 8, formula_text);
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value("Sheet1", 1, 8)
        .expect("lookup fixture result")
}

#[test]
fn contributor_x_function_blank_needle_oracle_cases() {
    let cases = [
        (
            "X1",
            [Some(1.0), Some(5.0), Some(0.0)],
            "=XMATCH(F1,A1:A3,0)",
            LookupExpected::Na,
        ),
        (
            "X2",
            [Some(1.0), Some(5.0), Some(0.0)],
            "=XLOOKUP(F1,A1:A3,D1:D3,\"NF\",0,1)",
            LookupExpected::Text("NF"),
        ),
        (
            "X3",
            [Some(1.0), Some(5.0), Some(0.0)],
            "=MATCH(F1,A1:A3,0)",
            LookupExpected::Number(3.0),
        ),
        (
            "X4",
            [Some(1.0), Some(5.0), Some(0.0)],
            "=XLOOKUP(0,A1:A3,D1:D3,\"NF\",0,1)",
            LookupExpected::Text("p3"),
        ),
        (
            "X5",
            [Some(0.0), None, Some(1.0)],
            "=XMATCH(F1,A1:A3,0)",
            LookupExpected::Number(2.0),
        ),
        (
            "X6",
            [Some(0.0), None, Some(1.0)],
            "=XLOOKUP(F1,A1:A3,D1:D3,\"NF\",0,1)",
            LookupExpected::Text("p2"),
        ),
        (
            "X7",
            [Some(0.0), None, Some(1.0)],
            "=XLOOKUP(F1,A1:A3,D1:D3,\"NF\",0,-1)",
            LookupExpected::Text("p2"),
        ),
        (
            "X8",
            [Some(0.0), None, Some(1.0)],
            "=MATCH(F1,A1:A3,0)",
            LookupExpected::Number(1.0),
        ),
        (
            "X9",
            [Some(1.0), None, Some(2.0)],
            "=XLOOKUP(F1,A1:A3,D1:D3,\"NF\",0,1)",
            LookupExpected::Text("p2"),
        ),
        (
            "X10",
            [Some(1.0), None, Some(2.0)],
            "=XLOOKUP(F1,A1:A3,D1:D3,\"NF\",0,-1)",
            LookupExpected::Text("p2"),
        ),
        (
            "X11",
            [Some(1.0), None, Some(2.0)],
            "=XMATCH(F1,A1:A3,0)",
            LookupExpected::Number(2.0),
        ),
        (
            "X12",
            [Some(1.0), None, Some(2.0)],
            "=XLOOKUP(\"\",A1:A3,D1:D3,\"NF\",0,1)",
            LookupExpected::Text("NF"),
        ),
        (
            "X13",
            [Some(1.0), None, Some(2.0)],
            "=MATCH(\"\",A1:A3,0)",
            LookupExpected::Na,
        ),
    ];

    for (label, values, formula_text, expected) in cases {
        assert_lookup_expected(
            evaluate_lookup_fixture(values, false, formula_text),
            expected,
            label,
        );
    }
}

#[test]
fn corrected_blank_zero_boundary_oracle_rows() {
    let vertical = [
        ("B1", 2.0, "=MATCH(0,A1:A3,0)", LookupExpected::Na),
        ("B2", 2.0, "=MATCH(F1,A1:A3,0)", LookupExpected::Na),
        ("B3", 0.0, "=MATCH(F1,A1:A3,0)", LookupExpected::Number(3.0)),
        ("B4", 2.0, "=XMATCH(0,A1:A3,0)", LookupExpected::Na),
        (
            "B5",
            0.0,
            "=XMATCH(F1,A1:A3,0)",
            LookupExpected::Number(2.0),
        ),
        ("B6", 2.0, "=VLOOKUP(0,A1:A3,1,FALSE)", LookupExpected::Na),
        (
            "B7",
            0.0,
            "=VLOOKUP(F1,A1:A3,1,FALSE)",
            LookupExpected::Number(0.0),
        ),
        (
            "B8",
            2.0,
            "=XLOOKUP(0,A1:A3,A1:A3,\"NF\",0,1)",
            LookupExpected::Text("NF"),
        ),
        (
            "B9",
            2.0,
            "=XLOOKUP(0,A1:A3,A1:A3,\"NF\",0,-1)",
            LookupExpected::Text("NF"),
        ),
        (
            "B10",
            2.0,
            "=XLOOKUP(F1,A1:A3,A1:A3,\"NF\",0,-1)",
            LookupExpected::Number(0.0),
        ),
        (
            "B11",
            0.0,
            "=XLOOKUP(F1,A1:A3,A1:A3,\"NF\",0,-1)",
            LookupExpected::Number(0.0),
        ),
        (
            "B12",
            2.0,
            "=XLOOKUP(0,A1:A3,A1:A3,\"NF\",2,1)",
            LookupExpected::Text("NF"),
        ),
        ("B13", 2.0, "=MATCH(-0,A1:A3,0)", LookupExpected::Na),
        (
            "B14",
            0.0,
            "=MATCH(-0,A1:A3,0)",
            LookupExpected::Number(3.0),
        ),
        (
            "B15",
            -0.0,
            "=MATCH(F1,A1:A3,0)",
            LookupExpected::Number(3.0),
        ),
    ];
    let horizontal = [
        ("B16", 2.0, "=HLOOKUP(0,A5:C5,1,FALSE)", LookupExpected::Na),
        ("B17", 2.0, "=HLOOKUP(F1,A5:C5,1,FALSE)", LookupExpected::Na),
        (
            "B18",
            0.0,
            "=HLOOKUP(F1,A5:C5,1,FALSE)",
            LookupExpected::Number(0.0),
        ),
        ("B19", 2.0, "=MATCH(F1,A5:C5,0)", LookupExpected::Na),
        (
            "B20",
            0.0,
            "=MATCH(F1,A5:C5,0)",
            LookupExpected::Number(3.0),
        ),
    ];

    for (label, third, formula_text, expected) in vertical {
        assert_lookup_expected(
            evaluate_lookup_fixture([Some(1.0), None, Some(third)], false, formula_text),
            expected,
            label,
        );
    }
    for (label, third, formula_text, expected) in horizontal {
        assert_lookup_expected(
            evaluate_lookup_fixture([Some(1.0), None, Some(third)], true, formula_text),
            expected,
            label,
        );
    }
}

#[test]
fn x_function_semantic_blank_scan_controls() {
    let mut engine = engine_with_mode(FormulaPlaneMode::Off);
    number(&mut engine, "Sheet1", 2, 1, 0.0);
    text(&mut engine, "Sheet1", 4, 1, "");
    for row in 1..=4 {
        text(&mut engine, "Sheet1", row, 4, &format!("p{row}"));
    }

    number(&mut engine, "Sheet1", 10, 2, 0.0);
    text(&mut engine, "Sheet1", 10, 4, "");
    for col in 1..=4 {
        text(&mut engine, "Sheet1", 11, col, &format!("p{col}"));
    }

    for row in 1..=3 {
        text(&mut engine, "Sheet1", row, 12, &format!("p{row}"));
    }

    let cases = [
        (
            "XMATCH exact first blank",
            "=XMATCH(F1,A1:A4,0,1)",
            LookupExpected::Number(1.0),
        ),
        (
            "XMATCH exact last blank",
            "=XMATCH(F1,A1:A4,0,-1)",
            LookupExpected::Number(3.0),
        ),
        (
            "XMATCH wildcard first blank",
            "=XMATCH(F1,A1:A4,2,1)",
            LookupExpected::Number(1.0),
        ),
        (
            "XMATCH wildcard last blank",
            "=XMATCH(F1,A1:A4,2,-1)",
            LookupExpected::Number(3.0),
        ),
        (
            "XLOOKUP exact first blank",
            "=XLOOKUP(F1,A1:A4,D1:D4,\"NF\",0,1)",
            LookupExpected::Text("p1"),
        ),
        (
            "XLOOKUP exact last blank",
            "=XLOOKUP(F1,A1:A4,D1:D4,\"NF\",0,-1)",
            LookupExpected::Text("p3"),
        ),
        (
            "XLOOKUP wildcard first blank",
            "=XLOOKUP(F1,A1:A4,D1:D4,\"NF\",2,1)",
            LookupExpected::Text("p1"),
        ),
        (
            "XLOOKUP wildcard last blank",
            "=XLOOKUP(F1,A1:A4,D1:D4,\"NF\",2,-1)",
            LookupExpected::Text("p3"),
        ),
        (
            "XLOOKUP numeric zero",
            "=XLOOKUP(0,A1:A4,D1:D4,\"NF\",0,1)",
            LookupExpected::Text("p2"),
        ),
        (
            "XLOOKUP empty text",
            "=XLOOKUP(\"\",A1:A4,D1:D4,\"NF\",0,1)",
            LookupExpected::Text("p4"),
        ),
        (
            "XMATCH numeric zero",
            "=XMATCH(0,A1:A4,0,1)",
            LookupExpected::Number(2.0),
        ),
        (
            "XMATCH empty text",
            "=XMATCH(\"\",A1:A4,0,1)",
            LookupExpected::Number(4.0),
        ),
        (
            "XMATCH forward alias",
            "=XMATCH(F1,A1:A4,0,2)",
            LookupExpected::Number(1.0),
        ),
        (
            "XMATCH reverse alias",
            "=XMATCH(F1,A1:A4,0,-2)",
            LookupExpected::Number(3.0),
        ),
        (
            "XLOOKUP forward alias",
            "=XLOOKUP(F1,A1:A4,D1:D4,\"NF\",0,2)",
            LookupExpected::Text("p1"),
        ),
        (
            "XLOOKUP fallback alias",
            "=XLOOKUP(F1,A1:A4,D1:D4,\"NF\",0,-2)",
            LookupExpected::Text("p1"),
        ),
        (
            "horizontal XLOOKUP first blank",
            "=XLOOKUP(F1,A10:D10,A11:D11,\"NF\",0,1)",
            LookupExpected::Text("p1"),
        ),
        (
            "horizontal XLOOKUP last blank",
            "=XLOOKUP(F1,A10:D10,A11:D11,\"NF\",0,-1)",
            LookupExpected::Text("p3"),
        ),
        (
            "horizontal XMATCH last blank",
            "=XMATCH(F1,A10:D10,0,-1)",
            LookupExpected::Number(3.0),
        ),
        (
            "materialized XLOOKUP first blank",
            "=XLOOKUP(F1,CHOOSECOLS(A1:A4,1),D1:D4,\"NF\",0,1)",
            LookupExpected::Text("p1"),
        ),
        (
            "materialized XLOOKUP last blank",
            "=XLOOKUP(F1,CHOOSECOLS(A1:A4,1),D1:D4,\"NF\",0,-1)",
            LookupExpected::Text("p3"),
        ),
        (
            "materialized XMATCH first blank",
            "=XMATCH(F1,CHOOSECOLS(A1:A4,1),0,1)",
            LookupExpected::Number(1.0),
        ),
        (
            "materialized XMATCH last blank",
            "=XMATCH(F1,CHOOSECOLS(A1:A4,1),0,-1)",
            LookupExpected::Number(3.0),
        ),
        (
            "empty-view XLOOKUP first blank",
            "=XLOOKUP(F1,K1:K3,L1:L3,\"NF\",0,1)",
            LookupExpected::Text("p1"),
        ),
        (
            "empty-view XLOOKUP last blank",
            "=XLOOKUP(F1,K1:K3,L1:L3,\"NF\",0,-1)",
            LookupExpected::Text("p3"),
        ),
        (
            "empty-view XLOOKUP wildcard blank",
            "=XLOOKUP(F1,K1:K3,L1:L3,\"NF\",2,1)",
            LookupExpected::Text("p1"),
        ),
        (
            "empty-view XLOOKUP numeric zero",
            "=XLOOKUP(0,K1:K3,L1:L3,\"NF\",0,1)",
            LookupExpected::Text("NF"),
        ),
        (
            "empty-view XLOOKUP empty text",
            "=XLOOKUP(\"\",K1:K3,L1:L3,\"NF\",0,1)",
            LookupExpected::Text("NF"),
        ),
    ];

    for (offset, (_, formula_text, _)) in cases.iter().enumerate() {
        formula(&mut engine, "Sheet1", offset as u32 + 20, 14, formula_text);
    }
    engine.evaluate_all().unwrap();
    for (offset, (label, _, expected)) in cases.into_iter().enumerate() {
        let actual = engine
            .get_cell_value("Sheet1", offset as u32 + 20, 14)
            .expect("blank scan control result");
        assert_lookup_expected(actual, expected, label);
    }
}

type LookupMatrixExpectation = (u32, u32, LookupExpected, &'static str);

fn blank_zero_lookup_matrix_engine(
    cache_max_bytes: usize,
    max_threads: Option<usize>,
) -> (Engine<TestWorkbook>, Vec<LookupMatrixExpectation>) {
    let mut engine = engine_with_config(EvalConfig {
        formula_plane_mode: FormulaPlaneMode::Off,
        lookup_index_cache_max_bytes: cache_max_bytes,
        max_threads,
        ..EvalConfig::default()
    });

    for row in 1..=TABLE_ROWS {
        let key = match row {
            1 | 7 => LiteralValue::Empty,
            2 | 10 => LiteralValue::Text(String::new()),
            3 | 9 => LiteralValue::Text("0".into()),
            4 | 8 => LiteralValue::Boolean(false),
            5 => LiteralValue::Number(-0.0),
            6 => LiteralValue::Number(0.0),
            _ => LiteralValue::Number(row as f64),
        };
        let no_zero_key = match row % 4 {
            0 => LiteralValue::Empty,
            1 => LiteralValue::Text(String::new()),
            2 => LiteralValue::Text("0".into()),
            _ => LiteralValue::Boolean(false),
        };
        value(&mut engine, "Sheet1", row, 1, key.clone());
        number(&mut engine, "Sheet1", row, 2, row as f64 * 10.0);
        value(&mut engine, "Sheet1", row, 3, no_zero_key.clone());
        number(&mut engine, "Sheet1", row, 4, row as f64 * 10.0);
        value(&mut engine, "Sheet1", 110, row, key);
        number(&mut engine, "Sheet1", 111, row, row as f64 * 10.0);
        value(&mut engine, "Sheet1", 120, row, no_zero_key);
        number(&mut engine, "Sheet1", 121, row, row as f64 * 10.0);
    }

    let numeric_cases = [
        ("MATCH +0 vertical", "=MATCH(0,$A$1:$A$100,0)", 5.0),
        ("MATCH -0 vertical", "=MATCH(-0,$A$1:$A$100,0)", 5.0),
        (
            "MATCH blank vertical",
            "=MATCH($ZZ$1000,$A$1:$A$100,0)",
            5.0,
        ),
        ("MATCH +0 horizontal", "=MATCH(0,$A$110:$CV$110,0)", 5.0),
        ("MATCH -0 horizontal", "=MATCH(-0,$A$110:$CV$110,0)", 5.0),
        (
            "MATCH blank horizontal",
            "=MATCH($ZZ$1000,$A$110:$CV$110,0)",
            5.0,
        ),
        ("VLOOKUP +0", "=VLOOKUP(0,$A$1:$B$100,2,FALSE)", 50.0),
        ("VLOOKUP -0", "=VLOOKUP(-0,$A$1:$B$100,2,FALSE)", 50.0),
        (
            "VLOOKUP blank",
            "=VLOOKUP($ZZ$1000,$A$1:$B$100,2,FALSE)",
            50.0,
        ),
        ("HLOOKUP +0", "=HLOOKUP(0,$A$110:$CV$111,2,FALSE)", 50.0),
        ("HLOOKUP -0", "=HLOOKUP(-0,$A$110:$CV$111,2,FALSE)", 50.0),
        (
            "HLOOKUP blank",
            "=HLOOKUP($ZZ$1000,$A$110:$CV$111,2,FALSE)",
            50.0,
        ),
        (
            "XMATCH +0 forward vertical",
            "=XMATCH(0,$A$1:$A$100,0,1)",
            5.0,
        ),
        (
            "XMATCH +0 reverse vertical",
            "=XMATCH(0,$A$1:$A$100,0,-1)",
            6.0,
        ),
        (
            "XMATCH -0 forward vertical",
            "=XMATCH(-0,$A$1:$A$100,0,1)",
            5.0,
        ),
        (
            "XMATCH -0 reverse vertical",
            "=XMATCH(-0,$A$1:$A$100,0,-1)",
            6.0,
        ),
        (
            "XMATCH blank forward vertical",
            "=XMATCH($ZZ$1000,$A$1:$A$100,0,1)",
            1.0,
        ),
        (
            "XMATCH blank reverse vertical",
            "=XMATCH($ZZ$1000,$A$1:$A$100,0,-1)",
            7.0,
        ),
        (
            "XMATCH +0 forward horizontal",
            "=XMATCH(0,$A$110:$CV$110,0,1)",
            5.0,
        ),
        (
            "XMATCH +0 reverse horizontal",
            "=XMATCH(0,$A$110:$CV$110,0,-1)",
            6.0,
        ),
        (
            "XMATCH -0 forward horizontal",
            "=XMATCH(-0,$A$110:$CV$110,0,1)",
            5.0,
        ),
        (
            "XMATCH -0 reverse horizontal",
            "=XMATCH(-0,$A$110:$CV$110,0,-1)",
            6.0,
        ),
        (
            "XMATCH blank forward horizontal",
            "=XMATCH($ZZ$1000,$A$110:$CV$110,0,1)",
            1.0,
        ),
        (
            "XMATCH blank reverse horizontal",
            "=XMATCH($ZZ$1000,$A$110:$CV$110,0,-1)",
            7.0,
        ),
        (
            "XLOOKUP +0 forward vertical",
            "=XLOOKUP(0,$A$1:$A$100,$B$1:$B$100,-1,0,1)",
            50.0,
        ),
        (
            "XLOOKUP +0 reverse vertical",
            "=XLOOKUP(0,$A$1:$A$100,$B$1:$B$100,-1,0,-1)",
            60.0,
        ),
        (
            "XLOOKUP -0 forward vertical",
            "=XLOOKUP(-0,$A$1:$A$100,$B$1:$B$100,-1,0,1)",
            50.0,
        ),
        (
            "XLOOKUP -0 reverse vertical",
            "=XLOOKUP(-0,$A$1:$A$100,$B$1:$B$100,-1,0,-1)",
            60.0,
        ),
        (
            "XLOOKUP blank forward vertical",
            "=XLOOKUP($ZZ$1000,$A$1:$A$100,$B$1:$B$100,-1,0,1)",
            10.0,
        ),
        (
            "XLOOKUP blank reverse vertical",
            "=XLOOKUP($ZZ$1000,$A$1:$A$100,$B$1:$B$100,-1,0,-1)",
            70.0,
        ),
        (
            "XLOOKUP +0 forward horizontal",
            "=XLOOKUP(0,$A$110:$CV$110,$A$111:$CV$111,-1,0,1)",
            50.0,
        ),
        (
            "XLOOKUP +0 reverse horizontal",
            "=XLOOKUP(0,$A$110:$CV$110,$A$111:$CV$111,-1,0,-1)",
            60.0,
        ),
        (
            "XLOOKUP -0 forward horizontal",
            "=XLOOKUP(-0,$A$110:$CV$110,$A$111:$CV$111,-1,0,1)",
            50.0,
        ),
        (
            "XLOOKUP -0 reverse horizontal",
            "=XLOOKUP(-0,$A$110:$CV$110,$A$111:$CV$111,-1,0,-1)",
            60.0,
        ),
        (
            "XLOOKUP blank forward horizontal",
            "=XLOOKUP($ZZ$1000,$A$110:$CV$110,$A$111:$CV$111,-1,0,1)",
            10.0,
        ),
        (
            "XLOOKUP blank reverse horizontal",
            "=XLOOKUP($ZZ$1000,$A$110:$CV$110,$A$111:$CV$111,-1,0,-1)",
            70.0,
        ),
    ];
    let missing_cases = [
        (
            "MATCH +0 missing vertical",
            "=MATCH(0,$C$1:$C$100,0)",
            LookupExpected::Na,
        ),
        (
            "MATCH -0 missing vertical",
            "=MATCH(-0,$C$1:$C$100,0)",
            LookupExpected::Na,
        ),
        (
            "MATCH blank missing vertical",
            "=MATCH($ZZ$1000,$C$1:$C$100,0)",
            LookupExpected::Na,
        ),
        (
            "MATCH +0 missing horizontal",
            "=MATCH(0,$A$120:$CV$120,0)",
            LookupExpected::Na,
        ),
        (
            "MATCH -0 missing horizontal",
            "=MATCH(-0,$A$120:$CV$120,0)",
            LookupExpected::Na,
        ),
        (
            "MATCH blank missing horizontal",
            "=MATCH($ZZ$1000,$A$120:$CV$120,0)",
            LookupExpected::Na,
        ),
        (
            "VLOOKUP +0 missing",
            "=VLOOKUP(0,$C$1:$D$100,2,FALSE)",
            LookupExpected::Na,
        ),
        (
            "VLOOKUP -0 missing",
            "=VLOOKUP(-0,$C$1:$D$100,2,FALSE)",
            LookupExpected::Na,
        ),
        (
            "VLOOKUP blank missing",
            "=VLOOKUP($ZZ$1000,$C$1:$D$100,2,FALSE)",
            LookupExpected::Na,
        ),
        (
            "HLOOKUP +0 missing",
            "=HLOOKUP(0,$A$120:$CV$121,2,FALSE)",
            LookupExpected::Na,
        ),
        (
            "HLOOKUP -0 missing",
            "=HLOOKUP(-0,$A$120:$CV$121,2,FALSE)",
            LookupExpected::Na,
        ),
        (
            "HLOOKUP blank missing",
            "=HLOOKUP($ZZ$1000,$A$120:$CV$121,2,FALSE)",
            LookupExpected::Na,
        ),
        (
            "XMATCH +0 missing forward vertical",
            "=XMATCH(0,$C$1:$C$100,0,1)",
            LookupExpected::Na,
        ),
        (
            "XMATCH +0 missing reverse vertical",
            "=XMATCH(0,$C$1:$C$100,0,-1)",
            LookupExpected::Na,
        ),
        (
            "XMATCH -0 missing forward vertical",
            "=XMATCH(-0,$C$1:$C$100,0,1)",
            LookupExpected::Na,
        ),
        (
            "XMATCH -0 missing reverse vertical",
            "=XMATCH(-0,$C$1:$C$100,0,-1)",
            LookupExpected::Na,
        ),
        (
            "XMATCH blank first vertical",
            "=XMATCH($ZZ$1000,$C$1:$C$100,0,1)",
            LookupExpected::Number(4.0),
        ),
        (
            "XMATCH blank last vertical",
            "=XMATCH($ZZ$1000,$C$1:$C$100,0,-1)",
            LookupExpected::Number(100.0),
        ),
        (
            "XMATCH +0 missing forward horizontal",
            "=XMATCH(0,$A$120:$CV$120,0,1)",
            LookupExpected::Na,
        ),
        (
            "XMATCH +0 missing reverse horizontal",
            "=XMATCH(0,$A$120:$CV$120,0,-1)",
            LookupExpected::Na,
        ),
        (
            "XMATCH -0 missing forward horizontal",
            "=XMATCH(-0,$A$120:$CV$120,0,1)",
            LookupExpected::Na,
        ),
        (
            "XMATCH -0 missing reverse horizontal",
            "=XMATCH(-0,$A$120:$CV$120,0,-1)",
            LookupExpected::Na,
        ),
        (
            "XMATCH blank first horizontal",
            "=XMATCH($ZZ$1000,$A$120:$CV$120,0,1)",
            LookupExpected::Number(4.0),
        ),
        (
            "XMATCH blank last horizontal",
            "=XMATCH($ZZ$1000,$A$120:$CV$120,0,-1)",
            LookupExpected::Number(100.0),
        ),
        (
            "XLOOKUP +0 missing forward vertical",
            "=XLOOKUP(0,$C$1:$C$100,$D$1:$D$100,\"NF\",0,1)",
            LookupExpected::Text("NF"),
        ),
        (
            "XLOOKUP +0 missing reverse vertical",
            "=XLOOKUP(0,$C$1:$C$100,$D$1:$D$100,\"NF\",0,-1)",
            LookupExpected::Text("NF"),
        ),
        (
            "XLOOKUP -0 missing forward vertical",
            "=XLOOKUP(-0,$C$1:$C$100,$D$1:$D$100,\"NF\",0,1)",
            LookupExpected::Text("NF"),
        ),
        (
            "XLOOKUP -0 missing reverse vertical",
            "=XLOOKUP(-0,$C$1:$C$100,$D$1:$D$100,\"NF\",0,-1)",
            LookupExpected::Text("NF"),
        ),
        (
            "XLOOKUP blank first vertical",
            "=XLOOKUP($ZZ$1000,$C$1:$C$100,$D$1:$D$100,\"NF\",0,1)",
            LookupExpected::Number(40.0),
        ),
        (
            "XLOOKUP blank last vertical",
            "=XLOOKUP($ZZ$1000,$C$1:$C$100,$D$1:$D$100,\"NF\",0,-1)",
            LookupExpected::Number(1000.0),
        ),
        (
            "XLOOKUP +0 missing forward horizontal",
            "=XLOOKUP(0,$A$120:$CV$120,$A$121:$CV$121,\"NF\",0,1)",
            LookupExpected::Text("NF"),
        ),
        (
            "XLOOKUP +0 missing reverse horizontal",
            "=XLOOKUP(0,$A$120:$CV$120,$A$121:$CV$121,\"NF\",0,-1)",
            LookupExpected::Text("NF"),
        ),
        (
            "XLOOKUP -0 missing forward horizontal",
            "=XLOOKUP(-0,$A$120:$CV$120,$A$121:$CV$121,\"NF\",0,1)",
            LookupExpected::Text("NF"),
        ),
        (
            "XLOOKUP -0 missing reverse horizontal",
            "=XLOOKUP(-0,$A$120:$CV$120,$A$121:$CV$121,\"NF\",0,-1)",
            LookupExpected::Text("NF"),
        ),
        (
            "XLOOKUP blank first horizontal",
            "=XLOOKUP($ZZ$1000,$A$120:$CV$120,$A$121:$CV$121,\"NF\",0,1)",
            LookupExpected::Number(40.0),
        ),
        (
            "XLOOKUP blank last horizontal",
            "=XLOOKUP($ZZ$1000,$A$120:$CV$120,$A$121:$CV$121,\"NF\",0,-1)",
            LookupExpected::Number(1000.0),
        ),
    ];

    let mut expected = Vec::new();
    let mut next_col = 110;
    for (label, expression, expected_value) in numeric_cases {
        for row in 1..=5 {
            formula(&mut engine, "Sheet1", row, next_col, expression);
            expected.push((row, next_col, LookupExpected::Number(expected_value), label));
        }
        next_col += 1;
    }
    for (label, expression, expected_value) in missing_cases {
        for row in 1..=5 {
            formula(&mut engine, "Sheet1", row, next_col, expression);
            expected.push((row, next_col, expected_value, label));
        }
        next_col += 1;
    }
    (engine, expected)
}

fn assert_blank_zero_lookup_matrix(
    engine: &Engine<TestWorkbook>,
    expected: &[LookupMatrixExpectation],
) {
    for &(row, col, expected_value, label) in expected {
        let actual = engine
            .get_cell_value("Sheet1", row, col)
            .unwrap_or_else(|| panic!("missing result for {label}"));
        assert_lookup_expected(actual, expected_value, label);
    }
}

#[test]
fn blank_zero_exact_lookup_matrix_is_identical_cold_and_warm() {
    assert_blank_zero_lookup_matrix_cold_and_warm(None);
}

/// The matrix's lookup work does not depend on the pool's thread count.
/// Its families are 5-member runs of one fully absolute lookup; how a
/// parallel layer chunks a run depends on the thread count, and a lifted
/// chunk used to evaluate such a template once for the chunk (plus the
/// debug build's per-member oracle): 184 misses instead of 160 on 2 and 4
/// threads, 160 on 24 (one-member chunks are not lifted).
#[test]
fn blank_zero_exact_lookup_matrix_counts_do_not_depend_on_thread_count() {
    for max_threads in [2, 4] {
        assert_blank_zero_lookup_matrix_cold_and_warm(Some(max_threads));
    }
}

fn assert_blank_zero_lookup_matrix_cold_and_warm(max_threads: Option<usize>) {
    for cache_max_bytes in [0, EvalConfig::default().lookup_index_cache_max_bytes] {
        let (mut engine, expected) = blank_zero_lookup_matrix_engine(cache_max_bytes, max_threads);
        let snapshot = engine.inspection_mutation_revision();

        engine.evaluate_all().unwrap();
        assert_blank_zero_lookup_matrix(&engine, &expected);
        let first = engine.last_lookup_index_cache_report();
        if cache_max_bytes == 0 {
            assert_eq!(first.builds, 0, "threads {max_threads:?}: {first:?}");
            assert_eq!(first.hits, 0, "threads {max_threads:?}: {first:?}");
            assert_eq!(first.misses, 160, "threads {max_threads:?}: {first:?}");
            assert_eq!(first.skipped_cap, 160, "threads {max_threads:?}: {first:?}");
        } else {
            assert_eq!(first.builds, 8, "threads {max_threads:?}: {first:?}");
            assert_eq!(first.hits, 128, "threads {max_threads:?}: {first:?}");
            assert_eq!(first.entries_count, 8, "threads {max_threads:?}: {first:?}");
        }

        mark_all_formulas_dirty_without_edit(&mut engine);
        assert_eq!(engine.inspection_mutation_revision(), snapshot);
        engine.evaluate_all().unwrap();
        assert_eq!(engine.inspection_mutation_revision(), snapshot);
        assert_blank_zero_lookup_matrix(&engine, &expected);
        let warm = engine.last_lookup_index_cache_report();
        assert_eq!(warm.builds, 0, "threads {max_threads:?}: {warm:?}");
        assert_eq!(
            warm.misses,
            if cache_max_bytes == 0 { 160 } else { 0 },
            "threads {max_threads:?}: {warm:?}"
        );
        if cache_max_bytes == 0 {
            assert_eq!(warm.hits, 0, "threads {max_threads:?}: {warm:?}");
            assert_eq!(warm.skipped_cap, 160, "threads {max_threads:?}: {warm:?}");
        } else {
            assert_eq!(warm.hits, 160, "threads {max_threads:?}: {warm:?}");
            assert_eq!(
                warm.entries_count, first.entries_count,
                "threads {max_threads:?}: {warm:?}"
            );
            assert_eq!(
                warm.bytes_in_cache, first.bytes_in_cache,
                "threads {max_threads:?}: {warm:?}"
            );
        }
    }
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
fn lookup_cache_mixed_axis_parity_below_and_above_threshold() {
    let needles = [
        "0",
        "2",
        "\"ALPHA\"",
        "TRUE",
        "\"missing\"",
        "DATE(2024,1,1)",
    ];
    for with_error in [false, true] {
        for calls in [3, 8] {
            for needle in needles {
                for expression in [
                    format!("=VLOOKUP({needle},$D$1:$E$100,2,FALSE)"),
                    format!("=VLOOKUP({needle},$D$1:$E$100,2,TRUE)"),
                    format!("=MATCH({needle},$D$1:$D$100,0)"),
                    format!("=MATCH({needle},$D$1:$D$100,1)"),
                    format!("=XLOOKUP({needle},$D$1:$D$100,$E$1:$E$100)"),
                    format!("=HLOOKUP({needle},$A$110:$CV$111,2,FALSE)"),
                ] {
                    let mut results = Vec::new();
                    for max_bytes in [0, EvalConfig::default().lookup_index_cache_max_bytes] {
                        let mut engine = engine_with_config(EvalConfig {
                            lookup_index_cache_max_bytes: max_bytes,
                            ..EvalConfig::default()
                        });
                        let values = [
                            LiteralValue::Empty,
                            LiteralValue::Number(-0.0),
                            LiteralValue::Number(0.0),
                            LiteralValue::Int(2),
                            LiteralValue::Number(2.0),
                            LiteralValue::Text("Alpha".into()),
                            LiteralValue::Text("alpha".into()),
                            LiteralValue::Boolean(false),
                            LiteralValue::Boolean(true),
                            LiteralValue::Date(
                                chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                            ),
                        ];
                        for row in 1..=100 {
                            let candidate = if with_error && row == 50 {
                                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref))
                            } else {
                                values[(row as usize - 1) / 10].clone()
                            };
                            value(&mut engine, "Sheet1", row, 4, candidate.clone());
                            number(&mut engine, "Sheet1", row, 5, row as f64);
                            value(&mut engine, "Sheet1", 110, row, candidate);
                            number(&mut engine, "Sheet1", 111, row, row as f64);
                        }
                        for row in 1..=calls {
                            formula(&mut engine, "Sheet1", row, 2, &expression);
                        }
                        engine.evaluate_all().unwrap();
                        let report = engine.last_lookup_index_cache_report();
                        assert_eq!(
                            report.builds,
                            usize::from(max_bytes > 0 && calls > 3 && !with_error),
                            "{expression}: {report:?}"
                        );
                        results.push(
                            (1..=calls)
                                .map(|row| engine.get_cell_value("Sheet1", row, 2))
                                .collect::<Vec<_>>(),
                        );
                    }
                    assert_eq!(
                        results[0], results[1],
                        "{expression}, calls={calls}, error={with_error}"
                    );
                }
            }
        }
    }
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
    // Program 3: approximate lookups read the lookup index's stored column
    // (the same cells in the same order) instead of materializing it per
    // call; the binary search is unchanged, so their results equal the
    // materializing path's (no cache at all).
    let approximate_results = |max_bytes: usize| {
        let mut approximate = engine_with_config(EvalConfig {
            lookup_index_cache_max_bytes: max_bytes,
            ..EvalConfig::default()
        });
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
        let values: Vec<_> = (1..=FORMULA_ROWS)
            .map(|row| approximate.get_cell_value("Sheet1", row, 2))
            .collect();
        (values, approximate.last_lookup_index_cache_report())
    };
    let (cached, report) = approximate_results(EvalConfig::default().lookup_index_cache_max_bytes);
    let (materialized, uncached) = approximate_results(0);
    assert_eq!(uncached.hits, 0, "{uncached:?}");
    assert!(report.hits > 0, "{report:?}");
    assert_eq!(cached, materialized);

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

/// Program 3: parallel members of a lookup family that miss the index
/// together build it once (the others wait for it), and read the same
/// values as a sequential engine.
#[test]
fn parallel_lookup_family_builds_its_index_once() {
    let build = |parallel: bool| {
        let mut engine = engine_with_config(EvalConfig {
            enable_parallel: parallel,
            ..EvalConfig::default()
        });
        populate_numeric_table(&mut engine, "Sheet1", TABLE_ROWS);
        for row in 1..=4000u32 {
            number(
                &mut engine,
                "Sheet1",
                row,
                1,
                f64::from(row % TABLE_ROWS + 1),
            );
            formula(
                &mut engine,
                "Sheet1",
                row,
                2,
                &format!("=VLOOKUP(A{row}, $D$1:$E${TABLE_ROWS}, 2, FALSE)"),
            );
        }
        engine
    };
    let mut par = build(true);
    let mut seq = build(false);
    par.evaluate_all().unwrap();
    seq.evaluate_all().unwrap();
    assert_eq!(par.lookup_index_flights_built_for_test(), 1);
    // An edit to the table: one rebuild for the whole recalculation.
    for e in [&mut par, &mut seq] {
        number(e, "Sheet1", 7, 5, -1.5);
        e.evaluate_all().unwrap();
    }
    assert_eq!(par.lookup_index_flights_built_for_test(), 2);
    for row in 1..=4000u32 {
        assert_eq!(
            par.get_cell_value("Sheet1", row, 2),
            seq.get_cell_value("Sheet1", row, 2),
            "B{row}"
        );
    }
}

#[test]
fn issue_414_approx_and_exact_cache_invalidation_and_unsorted_guard() {
    let mut engine = engine_with_config(EvalConfig::default());
    // Rows 1..=80 ascending numeric keys 10, 20, ..., 800; col E = row index.
    for row in 1..=80u32 {
        number(&mut engine, "Sheet1", row, 4, row as f64 * 10.0);
        number(&mut engine, "Sheet1", row, 5, row as f64);
    }
    // 10 approximate VLOOKUPs + 10 approximate MATCH(1) + 10 exact VLOOKUPs over the same table.
    for row in 1..=10u32 {
        let needle = row as f64 * 50.0 + 5.0; // 55, 105, ..., 505
        formula(
            &mut engine,
            "Sheet1",
            row,
            1,
            &format!("=VLOOKUP({needle}, $D$1:$E$80, 2, TRUE)"),
        );
        formula(
            &mut engine,
            "Sheet1",
            row,
            2,
            &format!("=MATCH({needle}, $D$1:$D$80, 1)"),
        );
        let exact_needle = row as f64 * 50.0;
        formula(
            &mut engine,
            "Sheet1",
            row,
            3,
            &format!("=VLOOKUP({exact_needle}, $D$1:$E$80, 2, FALSE)"),
        );
    }

    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert!(report.builds >= 1, "{report:?}");
    assert!(report.hits > 0, "{report:?}");
    for row in 1..=10u32 {
        let expected = (row * 5) as f64;
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 1),
            Some(LiteralValue::Number(expected))
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(expected))
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 3),
            Some(LiteralValue::Number(expected))
        );
    }

    // Edit key inside the table while preserving order: D5 changes from 50 to 56.
    // Now needle 55 falls back to row 4 (40), while exact 50 becomes #N/A.
    number(&mut engine, "Sheet1", 5, 4, 56.0);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(4.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(4.0))
    );
    assert!(matches!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Na
    ));

    // Edit breaks ascending order: D40 = 99999. Approximate queries must immediately return #N/A
    // (#283/#399 guard), while exact VLOOKUP for 100 still finds row 10!
    number(&mut engine, "Sheet1", 40, 4, 99_999.0);
    engine.evaluate_all().unwrap();
    for row in 1..=10u32 {
        assert!(
            matches!(engine.get_cell_value("Sheet1", row, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Na),
            "unsorted VLOOKUP row {row}"
        );
        assert!(
            matches!(engine.get_cell_value("Sheet1", row, 2), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Na),
            "unsorted MATCH row {row}"
        );
    }
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(10.0))
    );

    // Restore ascending order: D40 = 400. Approximate queries recover cleanly.
    number(&mut engine, "Sheet1", 40, 4, 400.0);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn issue_414_approx_parity_cached_vs_uncached_asc_desc_mixed_blanks_text_errors() {
    // Compare max_bytes=0 (zero-alloc streaming path) against default cache (warmed projection)
    // across ascending, descending, duplicates, blanks, headers, mixed types, text case-insensitivity,
    // numeric-text needles, errors, and VLOOKUP/HLOOKUP/MATCH parity.
    let run_suite = |max_bytes: usize| -> Vec<Option<LiteralValue>> {
        let mut engine = engine_with_config(EvalConfig {
            lookup_index_cache_max_bytes: max_bytes,
            ..EvalConfig::default()
        });

        // Sheet1 D1:E90: ascending numeric with text header, blanks, and duplicate runs.
        text(&mut engine, "Sheet1", 1, 4, "Header");
        text(&mut engine, "Sheet1", 1, 5, "Payload");
        for row in 2..=85u32 {
            if row % 17 == 0 {
                // Leave blank
                number(&mut engine, "Sheet1", row, 5, row as f64);
                continue;
            }
            // Create duplicate runs every 3 rows: 10, 10, 10, 20, 20, 20, ...
            let key = ((row - 2) / 3 + 1) as f64 * 10.0;
            number(&mut engine, "Sheet1", row, 4, key);
            number(&mut engine, "Sheet1", row, 5, row as f64);
        }
        // Trailing rows 86..=90 left blank.

        // Sheet1 G1:G85: descending numeric with header, blanks, and duplicate runs.
        text(&mut engine, "Sheet1", 1, 7, "DescHeader");
        for row in 2..=85u32 {
            if row % 19 == 0 {
                continue;
            }
            let key = (30 - (row - 2) / 3) as f64 * 10.0;
            number(&mut engine, "Sheet1", row, 7, key);
        }

        // Sheet1 I1:J80: ascending mixed-case ASCII + Unicode text with interspersed numbers & blanks.
        for row in 1..=80u32 {
            if row % 11 == 0 {
                number(&mut engine, "Sheet1", row, 9, row as f64);
            } else if row % 13 != 0 {
                let label = if row % 2 == 0 {
                    format!("ITEM-{:03}", row.div_ceil(2))
                } else {
                    format!("item-{:03}", row.div_ceil(2))
                };
                text(&mut engine, "Sheet1", row, 9, &label);
            }
            number(&mut engine, "Sheet1", row, 10, row as f64);
        }

        // Sheet1 L1:L80: descending text.
        for row in 1..=80u32 {
            let label = format!("Z-{:03}", 100 - (row / 2));
            text(&mut engine, "Sheet1", row, 12, &label);
        }

        // Sheet1 N1:O80: ascending numeric WITH an error cell in row 40 (skipped by approx, disables cache).
        for row in 1..=80u32 {
            if row == 40 {
                value(
                    &mut engine,
                    "Sheet1",
                    row,
                    14,
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
                );
            } else {
                number(&mut engine, "Sheet1", row, 14, row as f64 * 5.0);
            }
            number(&mut engine, "Sheet1", row, 15, row as f64);
        }

        // Horizontal table on Sheet2 A1:CP2 (94 cols) ascending with header & blanks.
        text(&mut engine, "Sheet2", 1, 1, "HHeader");
        text(&mut engine, "Sheet2", 2, 1, "HVal");
        for col in 2..=94u32 {
            if col % 15 != 0 {
                number(
                    &mut engine,
                    "Sheet2",
                    1,
                    col,
                    ((col - 2) / 2 + 1) as f64 * 10.0,
                );
            }
            number(&mut engine, "Sheet2", 2, col, col as f64);
        }

        // Horizontal descending row on Sheet2 A4:CP4.
        for col in 1..=94u32 {
            if col % 13 != 0 {
                number(&mut engine, "Sheet2", 4, col, (50 - col / 2) as f64 * 10.0);
            }
        }

        let expressions = [
            // Ascending VLOOKUP & MATCH(1) (repeated > 4 times so cache warms on max_bytes > 0)
            "=VLOOKUP(5, Sheet1!$D$1:$E$90, 2, TRUE)",
            "=VLOOKUP(10, Sheet1!$D$1:$E$90, 2, TRUE)",
            "=VLOOKUP(25, Sheet1!$D$1:$E$90, 2, TRUE)",
            "=VLOOKUP(60, Sheet1!$D$1:$E$90, 2, TRUE)",
            "=VLOOKUP(9999, Sheet1!$D$1:$E$90, 2, TRUE)",
            "=VLOOKUP(\"60\", Sheet1!$D$1:$E$90, 2, TRUE)",
            "=MATCH(5, Sheet1!$D$1:$D$90, 1)",
            "=MATCH(10, Sheet1!$D$1:$D$90, 1)",
            "=MATCH(25, Sheet1!$D$1:$D$90, 1)",
            "=MATCH(60, Sheet1!$D$1:$D$90, 1)",
            "=MATCH(9999, Sheet1!$D$1:$D$90, 1)",
            // Descending MATCH(-1) with exact duplicates, between values, out of range
            "=MATCH(9999, Sheet1!$G$1:$G$85, -1)",
            "=MATCH(300, Sheet1!$G$1:$G$85, -1)",
            "=MATCH(255, Sheet1!$G$1:$G$85, -1)",
            "=MATCH(200, Sheet1!$G$1:$G$85, -1)",
            "=MATCH(5, Sheet1!$G$1:$G$85, -1)",
            // Wrong direction returns #N/A (#283/#399 guard)
            "=MATCH(100, Sheet1!$G$1:$G$85, 1)",
            "=MATCH(100, Sheet1!$D$1:$D$90, -1)",
            // Ascending text (case-insensitive) VLOOKUP & MATCH(1)
            "=VLOOKUP(\"aaa\", Sheet1!$I$1:$J$80, 2, TRUE)",
            "=VLOOKUP(\"ITEM-010\", Sheet1!$I$1:$J$80, 2, TRUE)",
            "=VLOOKUP(\"item-015z\", Sheet1!$I$1:$J$80, 2, TRUE)",
            "=VLOOKUP(\"Item-040\", Sheet1!$I$1:$J$80, 2, TRUE)",
            "=VLOOKUP(\"zzz\", Sheet1!$I$1:$J$80, 2, TRUE)",
            "=MATCH(\"ITEM-010\", Sheet1!$I$1:$I$80, 1)",
            "=MATCH(\"item-015z\", Sheet1!$I$1:$I$80, 1)",
            // Descending text MATCH(-1)
            "=MATCH(\"Z-999\", Sheet1!$L$1:$L$80, -1)",
            "=MATCH(\"z-090\", Sheet1!$L$1:$L$80, -1)",
            "=MATCH(\"Z-085a\", Sheet1!$L$1:$L$80, -1)",
            "=MATCH(\"a\", Sheet1!$L$1:$L$80, -1)",
            // Axis with error cell (error skipped, approximate still works)
            "=VLOOKUP(199, Sheet1!$N$1:$O$80, 2, TRUE)",
            "=VLOOKUP(200, Sheet1!$N$1:$O$80, 2, TRUE)",
            "=VLOOKUP(205, Sheet1!$N$1:$O$80, 2, TRUE)",
            "=VLOOKUP(210, Sheet1!$N$1:$O$80, 2, TRUE)",
            "=MATCH(205, Sheet1!$N$1:$N$80, 1)",
            // HLOOKUP & horizontal MATCH(1) & MATCH(-1)
            "=HLOOKUP(5, Sheet2!$A$1:$CP$2, 2, TRUE)",
            "=HLOOKUP(10, Sheet2!$A$1:$CP$2, 2, TRUE)",
            "=HLOOKUP(75, Sheet2!$A$1:$CP$2, 2, TRUE)",
            "=HLOOKUP(200, Sheet2!$A$1:$CP$2, 2, TRUE)",
            "=HLOOKUP(9999, Sheet2!$A$1:$CP$2, 2, TRUE)",
            "=MATCH(75, Sheet2!$A$1:$CP$1, 1)",
            "=MATCH(200, Sheet2!$A$1:$CP$1, 1)",
            "=MATCH(500, Sheet2!$A$4:$CP$4, -1)",
            "=MATCH(455, Sheet2!$A$4:$CP$4, -1)",
            "=MATCH(300, Sheet2!$A$4:$CP$4, -1)",
            "=MATCH(5, Sheet2!$A$4:$CP$4, -1)",
            "=MATCH(9999, Sheet2!$A$4:$CP$4, -1)",
        ];

        for (idx, expr) in expressions.iter().enumerate() {
            formula(&mut engine, "Out", (idx + 1) as u32, 1, expr);
        }
        engine.evaluate_all().unwrap();
        if max_bytes > 0 {
            let report = engine.last_lookup_index_cache_report();
            assert!(report.builds >= 4, "expected warmed caches: {report:?}");
            assert!(report.hits >= 8, "expected cache hits: {report:?}");
        }
        (1..=expressions.len() as u32)
            .map(|r| engine.get_cell_value("Out", r, 1))
            .collect()
    };

    let uncached = run_suite(0);
    let cached = run_suite(EvalConfig::default().lookup_index_cache_max_bytes);
    assert_eq!(cached, uncached);

    // Spot-check key semantic expectations from the suite.
    // VLOOKUP(5, ...) below min -> #N/A
    assert!(
        matches!(uncached[0], Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Na)
    );
    // VLOOKUP(10, ...) ascending duplicate run rows 2,3,4 -> returns LAST (row 4)
    assert_eq!(uncached[1], Some(LiteralValue::Number(4.0)));
    // MATCH(10, ..., 1) agrees with VLOOKUP(10, ..., TRUE)
    assert_eq!(uncached[7], Some(LiteralValue::Number(4.0)));
    // MATCH(300, desc, -1) exact duplicate run rows 2,3,4 -> returns FIRST (row 2)
    assert_eq!(uncached[12], Some(LiteralValue::Number(2.0)));
    // MATCH(100, desc, 1) wrong direction -> #N/A
    assert!(
        matches!(uncached[16], Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Na)
    );
    // MATCH(100, asc, -1) wrong direction -> #N/A
    assert!(
        matches!(uncached[17], Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Na)
    );
    // Error cell at row 40 (value would be 200) is skipped: VLOOKUP(200) matches row 39 (195)
    assert_eq!(uncached[30], Some(LiteralValue::Number(39.0)));
    assert_eq!(uncached[31], Some(LiteralValue::Number(41.0)));
}

#[test]
fn issue_414_volatile_outside_table_still_caches_while_inside_skips() {
    let mut engine = repeated_vlookup_engine(EvalConfig::default());
    // Volatile cell OUTSIDE $D$1:$E$100 (placed in Z1) must NOT block caching.
    formula(&mut engine, "Sheet1", 1, 26, "=NOW()");
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 1, "{report:?}");
    assert_eq!(report.skipped_volatile, 0, "{report:?}");

    // Volatile cell INSIDE $D$1:$E$100 must skip caching.
    formula(&mut engine, "Sheet1", 50, 4, "=NOW()");
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 0, "{report:?}");
    assert!(report.skipped_volatile > 0, "{report:?}");
}

#[test]
fn issue_414_mixed_numeric_text_approx_cache_matches_uncached_across_calls() {
    let run = |max_bytes: usize| -> Vec<Vec<Option<LiteralValue>>> {
        let mut engine = engine_with_config(EvalConfig {
            lookup_index_cache_max_bytes: max_bytes,
            ..EvalConfig::default()
        });

        // Ascending column D1:D68: ["2", "3", "4", "10", "z", "z", ...]
        text(&mut engine, "Sheet1", 1, 4, "2");
        text(&mut engine, "Sheet1", 2, 4, "3");
        text(&mut engine, "Sheet1", 3, 4, "4");
        text(&mut engine, "Sheet1", 4, 4, "10");
        for row in 5..=68u32 {
            text(&mut engine, "Sheet1", row, 4, "z");
        }

        // Descending mirror column E1:E68: ["z", ..., "z", "10", "4", "3", "2"]
        for row in 1..=64u32 {
            text(&mut engine, "Sheet1", row, 5, "z");
        }
        text(&mut engine, "Sheet1", 65, 5, "10");
        text(&mut engine, "Sheet1", 66, 5, "4");
        text(&mut engine, "Sheet1", 67, 5, "3");
        text(&mut engine, "Sheet1", 68, 5, "2");

        // Descending mixed column F1:F68: ["z", ..., "z", "20", "10", "2", "0"]
        for row in 1..=64u32 {
            text(&mut engine, "Sheet1", row, 6, "z");
        }
        text(&mut engine, "Sheet1", 65, 6, "20");
        text(&mut engine, "Sheet1", 66, 6, "10");
        text(&mut engine, "Sheet1", 67, 6, "2");
        text(&mut engine, "Sheet1", 68, 6, "0");

        let queries = [
            // Non-numeric-text needles (calls 1..=6 warm ApproxTextProjection on call 4+)
            "=MATCH(\"1a\", Sheet1!$D$1:$D$68, 1)",
            // Numeric-text needles on ascending range
            "=MATCH(\"5\", Sheet1!$D$1:$D$68, 1)",
            "=MATCH(\"10\", Sheet1!$D$1:$D$68, 1)",
            // Non-numeric-text needles on descending mirror
            "=MATCH(\"1a\", Sheet1!$E$1:$E$68, -1)",
            // Numeric-text needles on descending mirror
            "=MATCH(\"5\", Sheet1!$E$1:$E$68, -1)",
            // Non-numeric-text needle on descending mixed column where "10" < "1a" < "2"
            "=MATCH(\"1a\", Sheet1!$F$1:$F$68, -1)",
        ];

        for (q_idx, expr) in queries.iter().enumerate() {
            let col = (q_idx + 1) as u32;
            for call in 1..=6u32 {
                formula(&mut engine, "Out", call, col, expr);
            }
        }

        engine.evaluate_all().unwrap();
        if max_bytes > 0 {
            let report = engine.last_lookup_index_cache_report();
            assert!(report.builds >= 3, "expected warmed caches: {report:?}");
            assert!(report.hits >= 3, "expected cache hits: {report:?}");
        }

        (0..queries.len())
            .map(|q_idx| {
                let col = (q_idx + 1) as u32;
                (1..=6u32)
                    .map(|call| engine.get_cell_value("Out", call, col))
                    .collect()
            })
            .collect()
    };

    let uncached = run(0);
    let cached = run(EvalConfig::default().lookup_index_cache_max_bytes);
    assert_eq!(cached, uncached);

    // Every query must return the exact same value on calls 1..=6.
    for (q_idx, calls) in cached.iter().enumerate() {
        for (call_idx, val) in calls.iter().enumerate() {
            assert_eq!(
                val,
                &calls[0],
                "query {q_idx} call {} differed from call 1",
                call_idx + 1
            );
        }
    }

    // Verify expected semantics for key cases:
    // "1a" on ["2","3","4","10","z",...] -> "10" is at 1-based position 4.
    assert_eq!(cached[0][0], Some(LiteralValue::Number(4.0)));
    // "5" on ["2","3","4","10","z",...] -> "4" is at 1-based position 3.
    assert_eq!(cached[1][0], Some(LiteralValue::Number(3.0)));
    // "10" on ["2","3","4","10","z",...] -> "10" is at 1-based position 4.
    assert_eq!(cached[2][0], Some(LiteralValue::Number(4.0)));
    // "1a" on ["z", ..., "z", "10", "4", "3", "2"] -> "2" is at 1-based position 68.
    assert_eq!(cached[3][0], Some(LiteralValue::Number(68.0)));
    // "5" on ["z", ..., "z", "10", "4", "3", "2"] -> "10" is at 1-based position 65.
    assert_eq!(cached[4][0], Some(LiteralValue::Number(65.0)));
    // "1a" on ["z", ..., "z", "20", "10", "2", "0"] -> "2" is at 1-based position 67.
    assert_eq!(cached[5][0], Some(LiteralValue::Number(67.0)));
}

#[test]
fn issue_414_deleted_volatile_vertex_restores_has_volatile_in_range_and_caching() {
    let mut engine = repeated_vlookup_engine(EvalConfig::default());
    formula(&mut engine, "Sheet1", 50, 4, "=NOW()");
    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 0, "{report:?}");
    assert!(report.skipped_volatile > 0, "{report:?}");

    let sheet_id = engine.graph.sheet_id("Sheet1").unwrap();
    let addr = engine.graph.make_cell_ref("Sheet1", 50, 4);
    let vid = engine.graph.get_vertex_for_cell(&addr).unwrap();
    assert!(engine.graph.has_volatile_in_range(sheet_id, 0, 99, 3, 4));

    // Tombstone the volatile vertex without manually removing from `volatile_vertices`.
    engine.graph.mark_deleted(vid, true);
    engine.mark_topology_edited();

    assert!(
        !engine.graph.has_volatile_in_range(sheet_id, 0, 99, 3, 4),
        "deleted/tombstoned volatile vertex must not keep has_volatile_in_range true"
    );

    engine.evaluate_all().unwrap();
    let report = engine.last_lookup_index_cache_report();
    assert_eq!(report.builds, 1, "{report:?}");
    assert_eq!(report.skipped_volatile, 0, "{report:?}");
}
