use super::common::arrow_eval_config;
use crate::engine::{Engine, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const SHEET: &str = "Sheet1";

fn number(value: f64) -> LiteralValue {
    LiteralValue::Number(value)
}

fn assert_number(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, expected: f64) {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(actual)) => {
            assert!((actual - expected).abs() < 1e-9, "{sheet}!R{row}C{col}");
        }
        other => panic!("expected {sheet}!R{row}C{col} = {expected}, got {other:?}"),
    }
}

fn assert_empty(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) {
    assert_eq!(
        engine.get_cell_value(sheet, row, col),
        None,
        "expected {sheet}!R{row}C{col} to be empty"
    );
}

fn engine_with_mode(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut cfg = arrow_eval_config();
    cfg.formula_plane_mode = mode;
    Engine::new(TestWorkbook::new(), cfg)
}

fn off_engine() -> Engine<TestWorkbook> {
    engine_with_mode(FormulaPlaneMode::Off)
}

fn populate_row_formulas(engine: &mut Engine<TestWorkbook>, sheet: &str) {
    for row in 5..=10 {
        engine
            .set_cell_value(sheet, row, 1, number(row as f64))
            .unwrap();
        engine
            .set_cell_formula(sheet, row, 2, parse(format!("=A{row}*2")).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
}

fn populate_column_formulas(engine: &mut Engine<TestWorkbook>, sheet: &str) {
    engine.set_cell_value(sheet, 1, 1, number(10.0)).unwrap();
    for col in 5..=10 {
        engine
            .set_cell_formula(sheet, 1, col, parse(format!("=$A$1+{col}")).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
}

#[test]
fn insert_rows_clears_computed_values_in_affected_region() {
    let mut engine = off_engine();
    populate_row_formulas(&mut engine, SHEET);

    engine.insert_rows(SHEET, 7, 2).unwrap();

    assert_number(&engine, SHEET, 5, 2, 10.0);
    assert_number(&engine, SHEET, 6, 2, 12.0);
    for row in 7..=12 {
        assert_empty(&engine, SHEET, row, 2);
    }
}

#[test]
fn delete_rows_clears_computed_values_in_affected_region() {
    let mut engine = off_engine();
    populate_row_formulas(&mut engine, SHEET);

    engine.delete_rows(SHEET, 7, 2).unwrap();

    assert_number(&engine, SHEET, 5, 2, 10.0);
    assert_number(&engine, SHEET, 6, 2, 12.0);
    assert_empty(&engine, SHEET, 7, 2);
    assert_empty(&engine, SHEET, 8, 2);
}

#[test]
fn insert_columns_clears_computed_values() {
    let mut engine = off_engine();
    populate_column_formulas(&mut engine, SHEET);

    engine.insert_columns(SHEET, 7, 2).unwrap();

    assert_number(&engine, SHEET, 1, 5, 15.0);
    assert_number(&engine, SHEET, 1, 6, 16.0);
    for col in 7..=12 {
        assert_empty(&engine, SHEET, 1, col);
    }
}

#[test]
fn delete_columns_clears_computed_values() {
    let mut engine = off_engine();
    populate_column_formulas(&mut engine, SHEET);

    engine.delete_columns(SHEET, 7, 2).unwrap();

    assert_number(&engine, SHEET, 1, 5, 15.0);
    assert_number(&engine, SHEET, 1, 6, 16.0);
    assert_empty(&engine, SHEET, 1, 7);
    assert_empty(&engine, SHEET, 1, 8);
}

#[test]
fn add_sheet_clears_all_sheets_computed_values() {
    let mut engine = off_engine();
    engine.add_sheet("Sheet2").unwrap();
    engine.set_cell_value(SHEET, 1, 1, number(2.0)).unwrap();
    engine
        .set_cell_formula(SHEET, 1, 2, parse("=A1*3").unwrap())
        .unwrap();
    engine.set_cell_value("Sheet2", 1, 1, number(4.0)).unwrap();
    engine
        .set_cell_formula("Sheet2", 1, 2, parse("=A1*5").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_number(&engine, SHEET, 1, 2, 6.0);
    assert_number(&engine, "Sheet2", 1, 2, 20.0);

    engine.add_sheet("NewSheet").unwrap();

    assert_empty(&engine, SHEET, 1, 2);
    assert_empty(&engine, "Sheet2", 1, 2);
}

#[test]
fn remove_sheet_clears_remaining_sheets_computed_values() {
    let mut engine = off_engine();
    let doomed = engine.add_sheet("Doomed").unwrap();
    engine.add_sheet("Keep").unwrap();
    engine.set_cell_value(SHEET, 1, 1, number(2.0)).unwrap();
    engine
        .set_cell_formula(SHEET, 1, 2, parse("=A1*3").unwrap())
        .unwrap();
    engine.set_cell_value("Keep", 1, 1, number(4.0)).unwrap();
    engine
        .set_cell_formula("Keep", 1, 2, parse("=A1*5").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_number(&engine, SHEET, 1, 2, 6.0);
    assert_number(&engine, "Keep", 1, 2, 20.0);

    engine.remove_sheet(doomed).unwrap();

    assert_empty(&engine, SHEET, 1, 2);
    assert_empty(&engine, "Keep", 1, 2);
}

#[test]
fn structural_op_clear_works_in_off_mode() {
    let mut engine = off_engine();
    populate_row_formulas(&mut engine, SHEET);

    engine.insert_rows(SHEET, 6, 1).unwrap();

    assert_number(&engine, SHEET, 5, 2, 10.0);
    assert_empty(&engine, SHEET, 6, 2);
    assert_empty(&engine, SHEET, 7, 2);
}

#[test]
fn structural_op_then_evaluate_recovers_values() {
    let mut engine = off_engine();
    populate_row_formulas(&mut engine, SHEET);

    engine.insert_rows(SHEET, 7, 2).unwrap();

    assert_empty(&engine, SHEET, 9, 2);
    engine.evaluate_all().unwrap();
    assert_number(&engine, SHEET, 9, 2, 14.0);
    assert_number(&engine, SHEET, 10, 2, 16.0);
}
