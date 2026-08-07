//! NPV variadic-argument semantics (#293).
//!
//! Oracle: LibreOffice 24.2.7 headless recalculation (`oracle: lo-verified`).
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
            "{formula} (oracle: lo-verified): expected {expected}, got {actual}"
        ),
        other => panic!("{formula} (oracle: lo-verified): expected {expected}, got {other:?}"),
    }
}

fn assert_error(formula: &str, expected: ExcelErrorKind) {
    match eval_formula(formula) {
        LiteralValue::Error(error) => assert_eq!(error.kind, expected, "{formula}"),
        other => panic!("{formula} (oracle: lo-verified): expected {expected:?}, got {other:?}"),
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
