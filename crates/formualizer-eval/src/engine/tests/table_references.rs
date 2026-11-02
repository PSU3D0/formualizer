use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn number(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, value: f64) {
    engine
        .set_cell_value("Sheet1", row, col, LiteralValue::Number(value))
        .unwrap();
}

fn formula(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, text: &str) {
    engine
        .set_cell_formula("Sheet1", row, col, parse(text).unwrap())
        .unwrap();
}

fn table(engine: &mut Engine<TestWorkbook>, rows: u32, cols: u32, headers: &[&str], totals: bool) {
    let sheet = engine.sheet_id("Sheet1").unwrap();
    let start = CellRef::new(sheet, Coord::from_excel(1, 1, true, true));
    let end = CellRef::new(sheet, Coord::from_excel(rows, cols, true, true));
    engine
        .define_table(
            "Sales",
            RangeRef::new(start, end),
            true,
            headers.iter().map(|h| h.to_string()).collect(),
            totals,
        )
        .unwrap();
}

fn new_engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();
    engine
}

#[test]
fn a_totals_row_reading_its_own_table_is_not_a_cycle() {
    let mut engine = new_engine();
    number(&mut engine, 2, 2, 10.0);
    number(&mut engine, 3, 2, 20.0);
    table(&mut engine, 4, 2, &["Item", "Cost"], true);
    formula(&mut engine, 4, 2, "=SUBTOTAL(109,Sales[Cost])");
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(30.0))
    );

    number(&mut engine, 3, 2, 25.0);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(35.0))
    );
}

#[test]
fn a_reader_of_the_totals_row_follows_it() {
    let mut engine = new_engine();
    number(&mut engine, 2, 2, 10.0);
    number(&mut engine, 3, 2, 20.0);
    table(&mut engine, 4, 2, &["Item", "Cost"], true);
    formula(&mut engine, 4, 2, "=SUBTOTAL(109,Sales[Cost])");
    formula(&mut engine, 1, 4, "=Sales[[#Totals],[Cost]]*2");
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(60.0))
    );

    number(&mut engine, 2, 2, 11.0);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(62.0))
    );
}

#[test]
fn a_named_this_row_reference_reads_its_row() {
    let mut engine = new_engine();
    number(&mut engine, 2, 1, 1.0);
    number(&mut engine, 2, 2, 2.0);
    number(&mut engine, 3, 1, 3.0);
    number(&mut engine, 3, 2, 4.0);
    table(&mut engine, 3, 4, &["Q1", "Q2", "Sum", "Twice"], false);
    for row in 2..=3 {
        formula(&mut engine, row, 3, "=SUM(Sales[[#This Row],[Q1]:[Q2]])");
        formula(&mut engine, row, 4, "=Sales[[#This Row],[Sum]]*2");
    }
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 4),
        Some(LiteralValue::Number(14.0))
    );

    number(&mut engine, 3, 1, 10.0);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 4),
        Some(LiteralValue::Number(28.0))
    );
}

#[test]
fn a_column_name_with_a_trailing_space_still_resolves() {
    let mut engine = new_engine();
    number(&mut engine, 2, 1, 4.0);
    number(&mut engine, 3, 1, 6.0);
    table(&mut engine, 3, 2, &["Cost ", "Twice"], false);
    for row in 2..=3 {
        formula(&mut engine, row, 2, "=Sales[[#This Row],[Cost ]]*2");
    }
    formula(&mut engine, 1, 4, "=SUM(Sales[Cost ])");
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(12.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn an_unknown_column_is_an_error_in_its_own_cell_only() {
    let mut engine = new_engine();
    number(&mut engine, 2, 1, 4.0);
    table(&mut engine, 2, 2, &["Cost", "Twice"], false);
    formula(&mut engine, 2, 2, "=Sales[[#This Row],[Nope]]*2");
    formula(&mut engine, 1, 4, "=SUM(Sales[Cost])");
    engine.evaluate_all().unwrap();
    assert!(matches!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Error(_))
    ));
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(4.0))
    );
}
