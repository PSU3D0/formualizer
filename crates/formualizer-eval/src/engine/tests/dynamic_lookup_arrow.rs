use crate::engine::{EvalConfig, eval::Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn xlookup_whole_column_empty_lookup_matches_first_cell() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Make the return range non-empty so B:B is trimmed to its used region.
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(42))
        .unwrap();

    // Lookup column A has no used rows; XLOOKUP should still be able to resolve A:A without
    // materializing the entire million-row range.
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=XLOOKUP(0,A:A,B:B,\"NF\")").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn take_whole_column_returns_single_cell_without_materializing() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=TAKE(A:A,1)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Empty)
    );
}

#[test]
fn drop_whole_column_can_return_last_cell_without_materializing() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // DROP(A:A,1048575) returns the last row of A:A.
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=DROP(A:A,1048575)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Empty)
    );
}
