//! Excel-parity semantics for XLOOKUP no-match and exact-match class rules.
//!
//! - An omitted-in-place `if_not_found` slot (`XLOOKUP(v,l,r,,mode)`) is not a
//!   supplied argument: a no-match returns #N/A, never the slot's implicit 0.
//! - Exact match never crosses value classes: a text needle does not find a
//!   number, whichever search direction or storage path is used.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn numeric_grid_engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, v) in [(1u32, 10.0), (2, 20.0), (3, 30.0)] {
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(v))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 3, LiteralValue::Number(v * 10.0))
            .unwrap();
    }
    engine
}

fn assert_na(engine: &Engine<TestWorkbook>, row: u32, col: u32) {
    match engine.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Na),
        other => panic!("expected #N/A, got {other:?}"),
    }
}

#[test]
fn xlookup_omitted_if_not_found_returns_na_on_approximate_no_match() {
    let mut engine = numeric_grid_engine();
    engine
        .set_cell_formula(
            "Sheet1",
            10,
            1,
            parse("=XLOOKUP(5,B1:B3,C1:C3,,-1)").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            11,
            1,
            parse("=XLOOKUP(35,B1:B3,C1:C3,,1)").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            12,
            1,
            parse("=XLOOKUP(35,B1:B3,C1:C3,\"none\",1)").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_na(&engine, 10, 1);
    assert_na(&engine, 11, 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 12, 1),
        Some(LiteralValue::Text("none".into()))
    );
}

#[test]
fn xlookup_text_needle_does_not_coerce_to_number() {
    let mut engine = numeric_grid_engine();
    engine
        .set_cell_formula(
            "Sheet1",
            10,
            1,
            parse("=XLOOKUP(\"20\",B1:B3,C1:C3)").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            11,
            1,
            parse("=XLOOKUP(\"20\",B1:B3,C1:C3,,0,-1)").unwrap(),
        )
        .unwrap();
    // Control: a real numeric needle still matches.
    engine
        .set_cell_formula("Sheet1", 12, 1, parse("=XLOOKUP(20,B1:B3,C1:C3)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_na(&engine, 10, 1);
    assert_na(&engine, 11, 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 12, 1),
        Some(LiteralValue::Number(200.0))
    );
}
