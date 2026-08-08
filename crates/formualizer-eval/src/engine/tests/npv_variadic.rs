//! NPV variadic-argument semantics (#293).
//!
//! Oracle: LibreOffice 24.2.7 headless recalculation (`oracle: lo-verified`), except logical
//! values in references, where the tests follow Microsoft's documented Excel behavior.
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn engine_with_npv_fixture() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(0.1))
        .unwrap();
    for (row, value) in [(1, -100), (2, 50), (3, 60), (4, 70)] {
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Int(value))
            .unwrap();
    }
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(-100))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 3, LiteralValue::Text("n/a".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 3, LiteralValue::Int(60))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 3, LiteralValue::Int(70))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 5, LiteralValue::Int(-100))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 5, LiteralValue::Int(60))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 5, LiteralValue::Int(70))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 6, LiteralValue::Int(-100))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 6, parse("=1/0").unwrap())
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 6, LiteralValue::Int(60))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 6, LiteralValue::Int(70))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 7, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 7, LiteralValue::Int(2))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 8, LiteralValue::Int(3))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 8, LiteralValue::Int(4))
        .unwrap();

    engine
        .set_cell_value("Sheet1", 1, 11, LiteralValue::Text("n/a".into()))
        .unwrap();
    // L1 is deliberately blank.
    engine
        .set_cell_value("Sheet1", 1, 13, LiteralValue::Boolean(true))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 14, LiteralValue::Int(100))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 15, parse("=1/0").unwrap())
        .unwrap();
    for (row, value) in [
        (1, LiteralValue::Int(-100)),
        (2, LiteralValue::Boolean(true)),
        (3, LiteralValue::Int(60)),
        (4, LiteralValue::Int(70)),
    ] {
        engine.set_cell_value("Sheet1", row, 16, value).unwrap();
    }
    engine
}

fn eval_formula(formula: &str) -> LiteralValue {
    let mut engine = engine_with_npv_fixture();
    engine
        .set_cell_formula("Sheet1", 1, 10, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value("Sheet1", 1, 10)
        .unwrap_or(LiteralValue::Empty)
}

fn assert_number(formula: &str, expected: f64) {
    match eval_formula(formula) {
        LiteralValue::Number(actual) => assert!(
            (actual - expected).abs() < 1e-12,
            "{formula}: expected {expected}, got {actual}"
        ),
        other => panic!("{formula}: expected {expected}, got {other:?}"),
    }
}

fn assert_error(formula: &str, expected: ExcelErrorKind) {
    match eval_formula(formula) {
        LiteralValue::Error(error) => assert_eq!(error.kind, expected, "{formula}"),
        other => panic!("{formula}: expected {expected:?}, got {other:?}"),
    }
}

#[test]
fn npv_variadic_oracle_table() {
    let cases = [
        ("=NPV(A1,B1:B4)", 43.30305307014546),
        ("=NPV(A1,-100,50,60,70)", 43.30305307014546),
        ("=NPV(A1,B1,B2,B3,B4)", 43.30305307014546),
        ("=NPV(A1,B1:B4,C1:C4)", 51.00042484360604),
        ("=NPV(A1,C1:C4)", 11.269722013523648),
        ("=NPV(A1,E1:E4)", 11.269722013523648),
        ("=NPV(A1,-100,,60)", -45.8302028549963),
        ("=NPV(A1,G1:G2,H1:H2)", 7.54798169523939),
        ("=NPV(A1,H1:H2,G1:G2)", 8.15039956287139),
        ("=NPV(A1,{-100,50},{60,70})", 43.30305307014546),
        ("=NPV(0,B1:B4)", 80.0),
        ("=NPV(-0.5,B1:B4)", 1600.0),
        ("=NPV(B5,B1:B4)", 80.0),
    ];

    for (formula, expected) in cases {
        assert_number(formula, expected);
    }

    for (formula, expected) in [
        ("=NPV(A1,-100,\"n/a\",60)", ExcelErrorKind::Value),
        ("=NPV(A1,B1:B4,1/0)", ExcelErrorKind::Div),
        ("=NPV(A1,F1:F4)", ExcelErrorKind::Div),
        ("=NPV(A1,{-100,1/0,60})", ExcelErrorKind::Div),
        ("=NPV(A1,{-100,\"n/a\",60})", ExcelErrorKind::Value),
        ("=NPV(-1,B1:B4)", ExcelErrorKind::Num),
        ("=NPV(\"\",B1:B4)", ExcelErrorKind::Value),
    ] {
        assert_error(formula, expected);
    }
}

#[test]
fn npv_negative_controls_preserve_range_and_rate_semantics() {
    assert_number("=NPV(A1,B1:B4)", 43.30305307014546);
    assert_number("=NPV(A1,C1:C4)", 11.269722013523648);
    assert_number("=NPV(A1,E1:E4)", 11.269722013523648);
    assert_error("=NPV(\"\",B1:B4)", ExcelErrorKind::Value);
}

#[test]
fn npv_reference_and_one_cell_range_have_identical_semantics() {
    // oracle: lo-verified for text, blank, number, and error references.
    for (cell, range) in [
        ("K1", "K1:K1"),
        ("L1", "L1:L1"),
        ("N1", "N1:N1"),
        ("O1", "O1:O1"),
    ] {
        let scalar = eval_formula(&format!("=NPV(A1,{cell},100)"));
        let one_cell_range = eval_formula(&format!("=NPV(A1,{range},100)"));
        assert_eq!(
            scalar, one_cell_range,
            "reference invariant failed for {cell} vs {range}"
        );
    }

    // Microsoft documents logical values in references as ignored. LibreOffice 24.2.7 instead
    // counts them as 1; Excel semantics still require M1 and M1:M1 to agree.
    assert_number("=NPV(A1,M1,100)", 90.9090909090909);
    assert_number("=NPV(A1,M1:M1,100)", 90.9090909090909);

    assert_number("=NPV(A1,K1)", 0.0);
    assert_number("=NPV(A1,L1)", 0.0);
    assert_number("=NPV(A1,K1,100)", 90.9090909090909);
    assert_number("=NPV(A1,L1,100)", 90.9090909090909);
    assert_number("=NPV(A1,N1)", 90.9090909090909);
    assert_number("=NPV(A1,OFFSET(K1,0,0))", 0.0);
    assert_error("=NPV(A1,O1)", ExcelErrorKind::Div);
    assert_error("=NPV(A1,O1:O1)", ExcelErrorKind::Div);
}

#[test]
fn npv_logical_values_distinguish_direct_values_from_references() {
    // oracle: lo-verified for direct scalar and array-literal logical values.
    assert_number("=NPV(A1,TRUE)", 0.9090909090909091);
    assert_number("=NPV(A1,{TRUE})", 0.9090909090909091);
    assert_number("=NPV(A1,{-100,TRUE,60})", -45.00375657400452);

    // Microsoft-documented Excel behavior: logical cells in references are ignored. LO 24.2.7
    // diverges by counting TRUE as 1, so these rows are intentionally not LO expectations.
    assert_number("=NPV(A1,M1)", 0.0);
    assert_number("=NPV(A1,M1:M1)", 0.0);
    assert_number("=NPV(A1,P1:P4)", 11.269722013523648);
}

#[test]
fn npv_computed_arrays_use_value_semantics() {
    // oracle: lo-verified. TRANSPOSE returns CalcValue::Range, but it is a computed value rather
    // than a reference; embedded text therefore makes NPV fail instead of being skipped.
    assert_error("=NPV(A1,TRANSPOSE(C1:C4))", ExcelErrorKind::Value);
    assert_error(
        "=NPV(A1,IF({1,1,1},{-100,\"n/a\",60}))",
        ExcelErrorKind::Value,
    );
    assert_number("=NPV(A1,TRANSPOSE(B1:B4))", 43.30305307014546);
}

#[test]
fn npv_enforces_excel_total_argument_limit() {
    let accepted = format!("=NPV(A1,{})", vec!["1"; 254].join(","));
    assert!(
        matches!(eval_formula(&accepted), LiteralValue::Number(_)),
        "255 total arguments must be accepted"
    );

    let rejected = format!("=NPV(A1,{})", vec!["1"; 255].join(","));
    assert_error(&rejected, ExcelErrorKind::Value);
}
