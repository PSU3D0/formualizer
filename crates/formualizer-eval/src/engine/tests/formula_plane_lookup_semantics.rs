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
