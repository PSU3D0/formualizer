use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;

const FORMULA_ROWS: u32 = 200;
const TABLE_ROWS: u32 = 1_000;

fn engine_with_mode(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    )
}

fn authoritative_engine() -> Engine<TestWorkbook> {
    engine_with_mode(FormulaPlaneMode::AuthoritativeExperimental)
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

fn assert_span_count(engine: &Engine<TestWorkbook>, expected: usize) {
    assert_eq!(
        engine.baseline_stats().formula_plane_active_span_count,
        expected,
        "ingest report: {:?}",
        engine.last_formula_ingest_report()
    );
}

fn assert_number(engine: &Engine<TestWorkbook>, row: u32, col: u32, expected: f64) {
    assert_eq!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Number(expected))
    );
}

fn populate_numeric_keys(engine: &mut Engine<TestWorkbook>, rows: u32) {
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }
}

fn populate_numeric_lookup_table(engine: &mut Engine<TestWorkbook>) {
    for row in 1..=TABLE_ROWS {
        engine
            .set_cell_value("Sheet1", row, 4, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Number(table_value(row)))
            .unwrap();
    }
}

fn populate_text_lookup_table(engine: &mut Engine<TestWorkbook>) {
    for row in 1..=TABLE_ROWS {
        engine
            .set_cell_value("Sheet1", row, 4, LiteralValue::Text(format!("KEY_{row}")))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Number(table_value(row)))
            .unwrap();
    }
}

fn table_value(row: u32) -> f64 {
    row as f64 * 10.0 + 5.0
}

fn hlookup_value(position: u32) -> f64 {
    position as f64 * 100.0 + 7.0
}

fn col_letters(mut col: u32) -> String {
    let mut out = Vec::new();
    while col > 0 {
        col -= 1;
        out.push((b'A' + (col % 26) as u8) as char);
        col /= 26;
    }
    out.iter().rev().collect()
}

#[test]
fn vlookup_exact_relative_key_promotes() {
    let mut engine = authoritative_engine();
    populate_numeric_keys(&mut engine, FORMULA_ROWS);
    populate_numeric_lookup_table(&mut engine);

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=VLOOKUP(A{row}, $D$1:$E$1000, 2, FALSE)"),
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    for row in [1, 50, 100, 200] {
        assert_number(&engine, row, 2, table_value(row));
    }
}

#[test]
fn vlookup_constant_key_broadcasts() {
    let mut engine = authoritative_engine();
    populate_text_lookup_table(&mut engine);

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(
            &mut engine,
            row,
            2,
            "=VLOOKUP(\"KEY_42\", $D$1:$E$1000, 2, FALSE)",
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.span_eval_placement_count, FORMULA_ROWS as u64);
    assert_eq!(report.transient_ast_relocation_count, 1);
    for row in [1, 50, 100, 200] {
        assert_number(&engine, row, 2, table_value(42));
    }
}

#[test]
fn hlookup_exact_promotes() {
    let mut engine = authoritative_engine();
    populate_numeric_keys(&mut engine, FORMULA_ROWS);

    let table_cols = TABLE_ROWS;
    let first_table_col = 4;
    for offset in 0..table_cols {
        let col = first_table_col + offset;
        let position = offset + 1;
        engine
            .set_cell_value("Sheet1", 1, col, LiteralValue::Number(position as f64))
            .unwrap();
        engine
            .set_cell_value(
                "Sheet1",
                2,
                col,
                LiteralValue::Number(hlookup_value(position)),
            )
            .unwrap();
    }
    let last_col = col_letters(first_table_col + table_cols - 1);

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=HLOOKUP(A{row}, $D$1:${last_col}$2, 2, FALSE)"),
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    for row in [1, 50, 100, 200] {
        assert_number(&engine, row, 2, hlookup_value(row));
    }
}

#[test]
fn match_exact_promotes() {
    let mut engine = authoritative_engine();
    populate_numeric_keys(&mut engine, FORMULA_ROWS);
    for row in 1..=TABLE_ROWS {
        engine
            .set_cell_value("Sheet1", row, 4, LiteralValue::Number(row as f64))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=MATCH(A{row}, $D$1:$D$1000, 0)"),
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    for row in [1, 50, 100, 200] {
        assert_number(&engine, row, 2, row as f64);
    }
}

#[test]
fn xlookup_exact_scalar_promotes() {
    let mut engine = authoritative_engine();
    populate_numeric_keys(&mut engine, FORMULA_ROWS);
    populate_numeric_lookup_table(&mut engine);

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=XLOOKUP(A{row}, $D$1:$D$1000, $E$1:$E$1000, \"NF\", 0, 1)"),
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    for row in [1, 50, 100, 200] {
        assert_number(&engine, row, 2, table_value(row));
    }
}

#[test]
fn xlookup_if_not_found_ref_is_value_slot() {
    let mut engine = authoritative_engine();
    populate_numeric_lookup_table(&mut engine);
    for row in 1..=FORMULA_ROWS {
        let key = if row.is_multiple_of(2) {
            TABLE_ROWS + row
        } else {
            row
        };
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(key as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 3, LiteralValue::Number(-(row as f64)))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=XLOOKUP(A{row}, $D$1:$D$1000, $E$1:$E$1000, C{row}, 0, 1)"),
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();
    assert_number(&engine, 1, 2, table_value(1));
    assert_number(&engine, 2, 2, -2.0);
    assert_number(&engine, 4, 2, -4.0);

    engine
        .set_cell_value("Sheet1", 2, 3, LiteralValue::Number(-999.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_number(&engine, 1, 2, table_value(1));
    assert_number(&engine, 2, 2, -999.0);
    assert_number(&engine, 4, 2, -4.0);
}

#[test]
fn lookup_table_edit_marks_dirty() {
    let mut engine = authoritative_engine();
    populate_numeric_lookup_table(&mut engine);
    for row in 1..=FORMULA_ROWS {
        let key = ((row - 1) % 10) + 1;
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(key as f64))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=VLOOKUP(A{row}, $D$1:$E$1000, 2, FALSE)"),
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();
    assert_number(&engine, 5, 2, table_value(5));
    assert_number(&engine, 15, 2, table_value(5));
    assert_number(&engine, 6, 2, table_value(6));

    engine
        .set_cell_value("Sheet1", 5, 5, LiteralValue::Number(9_999.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    for row in [5, 15, 25, 195] {
        assert_number(&engine, row, 2, 9_999.0);
    }
    assert_number(&engine, 6, 2, table_value(6));
}

#[test]
fn xlookup_multi_cell_return_parity_guard() {
    let mut values = Vec::new();
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut engine = engine_with_mode(mode);
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(2.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(11.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(12.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(21.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 2, 3, LiteralValue::Number(22.0))
            .unwrap();
        let formula = record(&mut engine, 1, 4, "=XLOOKUP(2, $A$1:$A$2, $B$1:$C$2)");
        ingest(&mut engine, vec![formula]);
        engine.evaluate_all().unwrap();
        values.push(engine.get_cell_value("Sheet1", 1, 4));
    }

    assert_eq!(values[0], values[1]);
    assert_eq!(values[0], Some(LiteralValue::Number(21.0)));
}

#[test]
fn mixed_lookup_aggregate_logical_promotes() {
    let mut engine = authoritative_engine();
    populate_numeric_keys(&mut engine, FORMULA_ROWS);
    populate_numeric_lookup_table(&mut engine);

    let mut formulas = Vec::new();
    for row in 1..=FORMULA_ROWS {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!(
                "=VLOOKUP(A{row}, $D$1:$E$1000, 2, FALSE) + IFERROR(VLOOKUP(A{row}+1, $D$1:$E$1000, 2, FALSE), 0) + IF(MOD(A{row},2)=0, 100, 200) + LEN(\"row-\" & A{row})"
            ),
        ));
    }
    ingest(&mut engine, formulas);

    assert_span_count(&engine, 1);
    engine.evaluate_all().unwrap();

    for row in [1, 50, 100, 200] {
        let expected = table_value(row)
            + table_value(row + 1)
            + if row.is_multiple_of(2) { 100.0 } else { 200.0 }
            + format!("row-{row}").len() as f64;
        assert_number(&engine, row, 2, expected);
    }
}
