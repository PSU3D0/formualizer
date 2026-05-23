use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;

const FORMULA_ROWS: u32 = 200;
const SAMPLES: [u32; 6] = [1, 5, 10, 50, 100, 200];

fn engine_with_mode(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    )
}

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn ingest(engine: &mut Engine<TestWorkbook>, formulas: Vec<FormulaIngestRecord>) {
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("ingest formulas");
}

fn build_family(
    mode: FormulaPlaneMode,
    result_col: u32,
    setup: impl Fn(&mut Engine<TestWorkbook>),
    formula: impl Fn(u32) -> String,
) -> Engine<TestWorkbook> {
    let mut engine = engine_with_mode(mode);
    setup(&mut engine);

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(&mut engine, row, result_col, &formula(row)));
    }
    ingest(&mut engine, formulas);
    engine
}

fn assert_span_count(engine: &Engine<TestWorkbook>, expected: usize) {
    assert_eq!(
        engine.baseline_stats().formula_plane_active_span_count,
        expected,
        "ingest report: {:?}",
        engine.last_formula_ingest_report()
    );
}

fn cell_value(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> LiteralValue {
    engine
        .get_cell_value("Sheet1", row, col)
        .unwrap_or_else(|| panic!("missing Sheet1!R{row}C{col}"))
}

fn numeric_value(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> f64 {
    match cell_value(engine, row, col) {
        LiteralValue::Int(value) => value as f64,
        LiteralValue::Number(value) => value,
        value => panic!("expected numeric Sheet1!R{row}C{col}, got {value:?}"),
    }
}

fn assert_off_auth_match(
    result_col: u32,
    setup: impl Copy + Fn(&mut Engine<TestWorkbook>),
    formula: impl Copy + Fn(u32) -> String,
) -> Engine<TestWorkbook> {
    let mut off = build_family(FormulaPlaneMode::Off, result_col, setup, formula);
    off.evaluate_all().unwrap();

    let mut auth = build_family(
        FormulaPlaneMode::AuthoritativeExperimental,
        result_col,
        setup,
        formula,
    );
    assert_span_count(&auth, 1);
    auth.evaluate_all().unwrap();

    for row in SAMPLES {
        assert_eq!(
            cell_value(&auth, row, result_col),
            cell_value(&off, row, result_col),
            "Off/Auth mismatch at row {row}"
        );
    }

    auth
}

fn populate_a_with_row_times_100(engine: &mut Engine<TestWorkbook>) {
    for row in 1..=FORMULA_ROWS {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64 * 100.0))
            .unwrap();
    }
}

fn populate_numeric_lookup_table(engine: &mut Engine<TestWorkbook>) {
    for row in 1..=FORMULA_ROWS {
        engine
            .set_cell_value("Sheet1", row, 4, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Number(table_value(row)))
            .unwrap();
    }
}

fn populate_constant_text_lookup_table(engine: &mut Engine<TestWorkbook>) {
    engine
        .set_cell_value("Sheet1", 1, 4, LiteralValue::Text("X".to_string()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 5, LiteralValue::Number(4_242.0))
        .unwrap();
    for row in 2..=FORMULA_ROWS {
        engine
            .set_cell_value("Sheet1", row, 4, LiteralValue::Text(format!("Y{row}")))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Number(row as f64))
            .unwrap();
    }
}

fn table_value(row: u32) -> f64 {
    row as f64 * 10.0 + 5.0
}

#[test]
fn per_placement_literal_substitution_basic() {
    let auth = assert_off_auth_match(2, populate_a_with_row_times_100, |row| {
        format!("=A{row}+{row}")
    });

    for row in SAMPLES {
        assert_eq!(numeric_value(&auth, row, 2), row as f64 * 101.0);
    }
}

#[test]
fn per_placement_literal_substitution_in_sum() {
    let auth = assert_off_auth_match(2, populate_a_with_row_times_100, |row| {
        format!("=SUM(A{row}, {row})")
    });

    for row in SAMPLES {
        assert_eq!(numeric_value(&auth, row, 2), row as f64 * 101.0);
    }
}

#[test]
fn per_placement_literal_substitution_in_mod() {
    let auth = assert_off_auth_match(2, |_| {}, |row| format!("=MOD({row}, 2)"));

    for (row, expected) in [
        (1, 1.0),
        (5, 1.0),
        (10, 0.0),
        (50, 0.0),
        (100, 0.0),
        (200, 0.0),
    ] {
        assert_eq!(numeric_value(&auth, row, 2), expected);
    }
}

#[test]
fn per_placement_literal_in_vlookup_key() {
    let auth = assert_off_auth_match(2, populate_numeric_lookup_table, |row| {
        format!("=VLOOKUP({row}, $D$1:$E$200, 2, FALSE)")
    });

    for row in SAMPLES {
        assert_eq!(numeric_value(&auth, row, 2), table_value(row));
    }
}

#[test]
fn per_placement_literal_in_nested_if_chain() {
    let auth = assert_off_auth_match(2, populate_a_with_row_times_100, |row| {
        format!(
            "=IF({row}<10, IF({row}>0, A{row}+{row}, -1), IF({row}<100, IF(MOD({row}, 2)=0, A{row}+{row}*2, A{row}+{row}*3), IF({row}<150, A{row}-{row}, A{row}+{row}/2)))"
        )
    });

    for row in SAMPLES {
        assert_eq!(
            numeric_value(&auth, row, 2),
            nested_if_expected(row),
            "unexpected nested IF value at row {row}"
        );
    }
}

#[test]
fn per_placement_literal_with_text_concat() {
    let auth = assert_off_auth_match(2, |_| {}, |row| format!("=LEN(\"row-\" & {row})"));

    for (row, expected) in [
        (1, 5.0),
        (5, 5.0),
        (10, 6.0),
        (50, 6.0),
        (100, 7.0),
        (200, 7.0),
    ] {
        assert_eq!(numeric_value(&auth, row, 2), expected);
    }
}

#[test]
fn per_placement_literal_substitution_does_not_break_constant_broadcast() {
    let auth = assert_off_auth_match(2, populate_constant_text_lookup_table, |_| {
        "=VLOOKUP(\"X\", $D$1:$E$200, 2, FALSE)".to_string()
    });

    let report = auth.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.span_eval_placement_count, FORMULA_ROWS as u64);
    assert_eq!(report.transient_ast_relocation_count, 1);
    for row in SAMPLES {
        assert_eq!(numeric_value(&auth, row, 2), 4_242.0);
    }
}

fn nested_if_expected(row: u32) -> f64 {
    if row < 10 {
        row as f64 * 101.0
    } else if row < 100 {
        if row.is_multiple_of(2) {
            row as f64 * 102.0
        } else {
            row as f64 * 103.0
        }
    } else if row < 150 {
        row as f64 * 99.0
    } else {
        row as f64 * 100.5
    }
}
