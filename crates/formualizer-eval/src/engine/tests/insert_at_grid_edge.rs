//! Inserting rows or columns near the grid edge: references pushed off the
//! grid become #REF!, range ends stop at the edge, and an insert that would
//! push content off the sheet is refused without changing anything. Each
//! case used to panic.
use crate::engine::{EditorError, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parse;

const LAST_ROW: u32 = 1_048_576;
const LAST_COL: u32 = 16_384;

fn engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

fn formula(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, text: &str) {
    engine
        .set_cell_formula("Sheet1", row, col, parse(text).unwrap())
        .unwrap();
}

fn formula_text(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> String {
    let (ast, _) = engine.get_cell("Sheet1", row, col).unwrap();
    formualizer_parse::pretty::canonical_formula(&ast.expect("formula"))
}

fn is_ref_error(value: Option<LiteralValue>) -> bool {
    matches!(value, Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Ref)
}

fn refused(result: Result<impl std::fmt::Debug, EditorError>) -> bool {
    matches!(result, Err(EditorError::Excel(e)) if e.kind == ExcelErrorKind::Ref)
}

#[test]
fn references_pushed_off_the_grid_become_ref_errors() {
    for (text, expected) in [
        (format!("=A{LAST_ROW}"), "=#REF!"),
        (format!("=SUM(A{LAST_ROW}:B{LAST_ROW})"), "=SUM(#REF!)"),
    ] {
        let mut e = engine();
        formula(&mut e, 1, 2, &text);
        e.evaluate_all().unwrap();
        e.insert_rows("Sheet1", 2, 1).unwrap();
        e.evaluate_all().unwrap();
        assert_eq!(formula_text(&e, 1, 2), expected, "{text}");
        assert!(is_ref_error(e.get_cell_value("Sheet1", 1, 2)), "{text}");
    }
    let mut e = engine();
    formula(&mut e, 2, 1, "=XFD1");
    e.evaluate_all().unwrap();
    e.insert_columns("Sheet1", 2, 1).unwrap();
    e.evaluate_all().unwrap();
    assert!(is_ref_error(e.get_cell_value("Sheet1", 2, 1)));
}

#[test]
fn range_ends_stop_at_the_grid_edge() {
    let mut e = engine();
    e.set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();
    formula(&mut e, 1, 2, &format!("=SUM(A2:A{LAST_ROW})"));
    formula(&mut e, 1, 3, "=SUM(D1:XFD1)");
    e.evaluate_all().unwrap();
    e.insert_rows("Sheet1", 2, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(formula_text(&e, 1, 2), format!("=SUM(A3:A{LAST_ROW})"));
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(5.0))
    );
    e.insert_columns("Sheet1", 4, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(formula_text(&e, 1, 3), "=SUM(E1:XFD1)");
}

#[test]
fn insert_that_would_push_content_off_the_sheet_is_refused() {
    let mut e = engine();
    e.set_cell_value("Sheet1", LAST_ROW, 1, LiteralValue::Number(1.0))
        .unwrap();
    formula(&mut e, 1, 2, &format!("=A{LAST_ROW}"));
    e.evaluate_all().unwrap();
    assert!(refused(e.insert_rows("Sheet1", 2, 1)));
    e.evaluate_all().unwrap();
    assert_eq!(formula_text(&e, 1, 2), format!("=A{LAST_ROW}"));
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(1.0))
    );

    let mut e = engine();
    formula(&mut e, LAST_ROW, 2, "=A1");
    assert!(refused(e.insert_rows("Sheet1", 2, 1)));

    let mut e = engine();
    e.set_cell_value("Sheet1", 1, LAST_COL, LiteralValue::Number(1.0))
        .unwrap();
    assert!(refused(e.insert_columns("Sheet1", 2, 1)));

    // Room for exactly the inserted rows is not a refusal.
    let mut e = engine();
    e.set_cell_value("Sheet1", LAST_ROW - 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    e.insert_rows("Sheet1", 2, 1).unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", LAST_ROW, 1),
        Some(LiteralValue::Number(1.0))
    );
}
