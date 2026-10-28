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
fn ceiling_both_negative_decimal_exact_multiples() {
    // Decimal exact multiples must retain the original value, even when
    // binary division lands just beyond the integral quotient.
    let mut failures = Vec::new();
    for (number, significance, expected) in [
        ("-0.07", "-0.01", -0.07_f64),
        ("-0.14", "-0.01", -0.14),
        ("-0.28", "-0.01", -0.28),
        ("-0.7", "-0.01", -0.7),
        ("-0.9", "-0.3", -0.9),
        ("-0.21", "-0.03", -0.21),
        ("-7e-20", "-1e-20", -7e-20),
    ] {
        let formula = format!("=CEILING({number},{significance})");
        let actual = evaluate(&formula).get_cell_value("Sheet1", 1, 1).unwrap();
        if !matches!(actual, LiteralValue::Number(n) if n.to_bits() == expected.to_bits()) {
            failures.push(format!("{formula}: expected {expected}, got {actual:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn ceiling_both_negative_decimal_nearby_nonmultiples() {
    // Distinct values on either side remain distinct at the engine's
    // 15-significant-digit decimal precision; do not snap their quotients.
    for (number, significance, expected) in [
        ("-0.0699999999999999", "-0.01", -0.07),
        ("-0.0700000000000001", "-0.01", -0.08),
        ("-0.139999999999999", "-0.01", -0.14),
        ("-0.140000000000001", "-0.01", -0.15),
        ("-0.279999999999999", "-0.01", -0.28),
        ("-0.280000000000001", "-0.01", -0.29),
        ("-0.899999999999999", "-0.3", -3.0 * 0.3),
        ("-0.900000000000001", "-0.3", -1.2),
    ] {
        // Preserve the existing binary multiplication for nonmultiples;
        // the -0.3 control expects exactly the product of -3 and 0.3.
        assert_grid(
            &format!("=CEILING({number},{significance})"),
            &[&[expected]],
        );
    }
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

#[test]
fn xlookup_descending_approximate_smaller_larger_and_endpoints() {
    assert_grid("=XLOOKUP(2.5,{3;2;1},{30;20;10},,-1,-2)", &[&[20.]]);
    assert_grid("=XLOOKUP(2.5,{3;2;1},{30;20;10},,1,-2)", &[&[30.]]);
    assert_grid("=XLOOKUP(2.5,{3,2,1},{30,20,10},,-1,-2)", &[&[20.]]);
    assert_grid("=XLOOKUP(4,{3;2;1},{30;20;10},,-1,-2)", &[&[30.]]);
    assert_grid("=XLOOKUP(0,{3;2;1},{30;20;10},,1,-2)", &[&[10.]]);
    assert_error("=XLOOKUP(0,{3;2;1},{30;20;10},,-1,-2)", ExcelErrorKind::Na);
    assert_error("=XLOOKUP(4,{3;2;1},{30;20;10},,1,-2)", ExcelErrorKind::Na);
}

#[test]
fn xlookup_scan_direction_and_duplicates() {
    // Binary-intent modes retain this provider's linear scan/tie policy;
    // these assertions establish result semantics, not the search algorithm.
    for mode in [0, -1, 1] {
        assert_grid(
            &format!("=XLOOKUP(2,{{3;2;2;1}},{{30;21;22;10}},,{mode},-2)"),
            &[&[if mode == 0 { 21. } else { 22. }]],
        );
        assert_grid(
            &format!("=XLOOKUP(2,{{1;2;2;3}},{{10;21;22;30}},,{mode},2)"),
            &[&[21.]],
        );
        assert_grid(
            &format!("=XLOOKUP(2,{{1;2;2;3}},{{10;21;22;30}},,{mode},1)"),
            &[&[21.]],
        );
        assert_grid(
            &format!("=XLOOKUP(2,{{1;2;2;3}},{{10;21;22;30}},,{mode},-1)"),
            &[&[22.]],
        );
    }
    assert_grid("=XLOOKUP(2.5,{3;2;2;1},{30;21;22;10},,-1,-2)", &[&[22.]]);
    assert_grid("=XLOOKUP(1.5,{3;2;2;1},{30;21;22;10},,1,-2)", &[&[22.]]);
    assert_grid("=XLOOKUP(2.5,{1;2;2;3},{10;21;22;30},,-1,1)", &[&[21.]]);
    assert_grid("=XLOOKUP(2.5,{1;2;2;3},{10;21;22;30},,-1,-1)", &[&[22.]]);
}

#[test]
fn xlookup_exact_ascending_approximate_and_error_controls() {
    assert_grid("=XLOOKUP(2,{3;2;1},{30;20;10},,0,-2)", &[&[20.]]);
    assert_grid("=XLOOKUP(2.5,{1;2;3},{10;20;30},,-1,2)", &[&[20.]]);
    assert_grid("=XLOOKUP(2.5,{1;2;3},{10;20;30},,1,2)", &[&[30.]]);
    assert_grid("=XLOOKUP(0,{3;2;1},{30;20;10},99,-1,-2)", &[&[99.]]);
    assert_error("=XLOOKUP(2,{3;2;1},{30;20},,-1,-2)", ExcelErrorKind::Value);
    assert_error(
        "=XLOOKUP(2,{3;2;1},{30;20;10},,3,-2)",
        ExcelErrorKind::Value,
    );
    assert_error(
        "=XLOOKUP(2,{3;2;1},{30;20;10},,-1,3)",
        ExcelErrorKind::Value,
    );
    assert_error(
        "=XLOOKUP(1/0,{3;2;1},{30;20;10},,-1,-2)",
        ExcelErrorKind::Div,
    );
}
