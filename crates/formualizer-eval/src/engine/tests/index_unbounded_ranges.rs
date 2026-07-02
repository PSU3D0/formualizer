//! INDEX/OFFSET over unbounded whole-column/whole-row ranges (issue #162).
//!
//! INDEX (and OFFSET) used to bail with #REF! whenever the array argument had
//! any unbounded dimension (B:B, 2:2, Data!$A:$C, Data!1:2). These tests pin
//! the fixed behavior: unbounded dimensions are clamped to the used region via
//! `resolve_range_view`, exactly like MATCH/VLOOKUP.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn new_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

fn assert_number(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, expected: f64) {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => {
            assert!(
                (n - expected).abs() < 1e-9,
                "{sheet}!R{row}C{col}: expected {expected}, got {n}"
            )
        }
        Some(LiteralValue::Int(i)) => {
            assert_eq!(i as f64, expected, "{sheet}!R{row}C{col}")
        }
        other => panic!("{sheet}!R{row}C{col}: expected {expected}, got {other:?}"),
    }
}

#[test]
fn index_whole_column_same_sheet() {
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Int(42))
        .unwrap();
    // Formula placed outside column B so the whole-column reference is not
    // self-inclusive.
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=INDEX(B:B,2,1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_number(&engine, "Sheet1", 1, 4, 42.0);
}

#[test]
fn index_whole_row_same_sheet() {
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Int(42))
        .unwrap();
    // Formula placed outside row 2 so the whole-row reference is not
    // self-inclusive.
    engine
        .set_cell_formula("Sheet1", 5, 4, parse("=INDEX(2:2,1,2)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_number(&engine, "Sheet1", 5, 4, 42.0);
}

#[test]
fn index_whole_column_cross_sheet() {
    let mut engine = new_engine();
    engine.add_sheet("Data").unwrap();
    engine
        .set_cell_value("Data", 2, 2, LiteralValue::Int(42))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDEX(Data!B:B,2,1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_number(&engine, "Sheet1", 1, 1, 42.0);
}

#[test]
fn index_multi_whole_row_cross_sheet() {
    let mut engine = new_engine();
    engine.add_sheet("Data").unwrap();
    engine
        .set_cell_value("Data", 2, 2, LiteralValue::Int(42))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDEX(Data!1:2,2,2)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_number(&engine, "Sheet1", 1, 1, 42.0);
}

#[test]
fn index_unbounded_with_match_row_and_col() {
    // The exact shape from issue #162:
    // =INDEX(Data!$A:$C, MATCH("row",Data!$A:$A,0), MATCH("col",Data!$1:$1,0))
    let mut engine = new_engine();
    engine.add_sheet("Data").unwrap();
    engine
        .set_cell_value("Data", 1, 2, LiteralValue::Text("col".into()))
        .unwrap();
    engine
        .set_cell_value("Data", 2, 1, LiteralValue::Text("row".into()))
        .unwrap();
    engine
        .set_cell_value("Data", 2, 2, LiteralValue::Int(42))
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            parse("=INDEX(Data!$A:$C, MATCH(\"row\",Data!$A:$A,0), MATCH(\"col\",Data!$1:$1,0))")
                .unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_number(&engine, "Sheet1", 1, 1, 42.0);
}

#[test]
fn index_whole_column_zero_row_returns_entire_used_column() {
    // Interaction with INDEX(range, 0, c) from PR #156: row_num == 0 over an
    // unbounded column yields the clamped whole column.
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Int(42))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 2, LiteralValue::Int(8))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=SUM(INDEX(B:B,0,1))").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_number(&engine, "Sheet1", 1, 4, 50.0);
}

#[test]
fn index_whole_column_out_of_range_is_ref_error() {
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Int(42))
        .unwrap();
    // Used region of column B ends at row 2; asking for row 5 is out of range.
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=INDEX(B:B,5,1)").unwrap())
        .unwrap();
    // Negative index is always #REF!.
    engine
        .set_cell_formula("Sheet1", 2, 4, parse("=INDEX(B:B,-1,1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    for row in [1u32, 2u32] {
        match engine.get_cell_value("Sheet1", row, 4) {
            Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
            other => panic!("Sheet1!R{row}C4: expected #REF!, got {other:?}"),
        }
    }
}

#[test]
fn offset_whole_column_and_row_clamped() {
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Int(42))
        .unwrap();
    // OFFSET(B:B,1,0,1,1) -> B2
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=OFFSET(B:B,1,0,1,1)").unwrap())
        .unwrap();
    // OFFSET(2:2,0,1,1,1) -> B2
    engine
        .set_cell_formula("Sheet1", 5, 4, parse("=OFFSET(2:2,0,1,1,1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_number(&engine, "Sheet1", 1, 4, 42.0);
    assert_number(&engine, "Sheet1", 5, 4, 42.0);
}

#[test]
fn offset_whole_column_default_size_sums_used_region() {
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Int(41))
        .unwrap();
    // Height defaults to the clamped used height of B:B (rows 1..2), shifted
    // one column right onto C. C1:C2 holds 2 and 40.
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(2))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 3, LiteralValue::Int(40))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 5, parse("=SUM(OFFSET(B:B,0,1))").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_number(&engine, "Sheet1", 1, 5, 42.0);
}
