//! Dispatch-level regressions for bounded lookup and rounding semantics.
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn evaluate(formula: &str) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 20, 10, LiteralValue::Number(-1.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

fn assert_grid(formula: &str, expected: &[&[f64]]) {
    let engine = evaluate(formula);
    for (r, row) in expected.iter().enumerate() {
        for (c, expected) in row.iter().enumerate() {
            let actual = engine.get_cell_value("Sheet1", r as u32 + 1, c as u32 + 1);
            let number = match actual {
                Some(LiteralValue::Number(n)) => n,
                Some(LiteralValue::Int(n)) => n as f64,
                other => panic!("{formula} at ({r},{c}): expected {expected}, got {other:?}"),
            };
            assert_eq!(
                number.to_bits(),
                expected.to_bits(),
                "{formula} at ({r},{c})"
            );
        }
    }
}

fn assert_error(formula: &str, kind: ExcelErrorKind) {
    match evaluate(formula).get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, kind, "{formula}"),
        other => panic!("{formula}: expected {kind:?}, got {other:?}"),
    }
}

#[test]
fn sortby_multiple_criteria_ties_and_last_default() {
    assert_grid(
        "=SORTBY({10;20;30;40},{1;1;2;1},1,{2;1;0;1},-1)",
        &[&[10.], &[20.], &[40.], &[30.]],
    );
    assert_grid(
        "=SORTBY({10;20;30;40},{1;1;2;1},1,{2;1;0;1})",
        &[&[20.], &[40.], &[10.], &[30.]],
    );
    assert_grid(
        "=SORTBY({10;20;30;40},{1;1;2;1},,{2;1;0;1},J20)",
        &[&[10.], &[20.], &[40.], &[30.]],
    );
}

#[test]
fn sortby_horizontal_columns_and_ties() {
    assert_grid(
        "=SORTBY({10,20,30;100,200,300},{3,1,2},1)",
        &[&[20., 30., 10.], &[200., 300., 100.]],
    );
    assert_grid(
        "=SORTBY({10,20,30},{1,1,2},1,{1,2,0},-1)",
        &[&[20., 10., 30.]],
    );
}

#[test]
fn sortby_invalid_order_and_axes() {
    for formula in [
        "=SORTBY({1;2},{2;1},0)",
        "=SORTBY({1;2},{2;1},2)",
        "=SORTBY({1;2},{2;1},1.5)",
        "=SORTBY({1;2},{2;1},1,{1,2},1)",
        "=SORTBY({1,2;3,4},{2;1},1,{1,2},1)",
        "=SORTBY({1;2},{1;2;3},1)",
        "=SORTBY({1,2;3,4},{1,2;3,4},1)",
    ] {
        assert_error(formula, ExcelErrorKind::Value);
    }
}

#[test]
fn sortby_supported_scalar_reference_and_errors() {
    assert_grid("=SORTBY({10;20;30},{3;1;2},J20)", &[&[10.], &[30.], &[20.]]);
    assert_grid("=SORTBY({10,20},1,-1)", &[&[10., 20.]]);
    assert_grid("=SORTBY(7,1)", &[&[7.]]);
    assert_error("=SORTBY({1;2},{1;2},1/0)", ExcelErrorKind::Div);
}

#[test]
fn ceiling_both_negative_rounds_away_from_zero() {
    assert_grid("=CEILING(-2.5,-2)", &[&[-4.]]);
    assert_grid("=CEILING(-0.6,-0.25)", &[&[-0.75]]);
    assert_grid("=CEILING(-4,-2)", &[&[-4.]]);
}

#[test]
fn ceiling_supported_sign_fraction_zero_and_error_controls() {
    for (formula, expected) in [
        ("=CEILING(-2.5,2)", -2.),
        ("=CEILING(2.5,2)", 4.),
        ("=CEILING(-4,2)", -4.),
        ("=CEILING(0.6,0.25)", 0.75),
        ("=CEILING(-0.6,0.25)", -0.5),
        ("=CEILING(0,-2)", 0.),
        ("=CEILING(-0.0,2)", -0.0),
        ("=CEILING(-0.0,-2)", -0.0),
        // Retain the existing positive-number/negative-significance behavior.
        ("=CEILING(2.5,-2)", 4.),
    ] {
        assert_grid(formula, &[&[expected]]);
    }
    assert_error("=CEILING(2,0)", ExcelErrorKind::Div);
    assert_error("=CEILING(2,-0.0)", ExcelErrorKind::Div);
    assert_error("=CEILING(\"bad\",2)", ExcelErrorKind::Value);
    assert_error("=CEILING(2,\"bad\")", ExcelErrorKind::Value);
    assert_error("=CEILING(1/0,-2)", ExcelErrorKind::Div);
    assert_error("=CEILING(-2,1/0)", ExcelErrorKind::Div);
}
