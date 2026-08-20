//! CELL / HYPERLINK builtins.
//!
//! CELL answers `contents`, `address`, `col`, `row` and `type` for a reference;
//! HYPERLINK returns its friendly name or link location as text. Both are
//! stored with the `_xlfn.` prefix in real workbooks, which the registry
//! resolves transparently.
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn new_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

fn eval_formula(formula: &str) -> LiteralValue {
    let mut engine = new_engine();
    engine
        .set_cell_formula("Sheet1", 1, 20, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value("Sheet1", 1, 20)
        .unwrap_or(LiteralValue::Empty)
}

fn assert_text(formula: &str, expected: &str) {
    match eval_formula(formula) {
        LiteralValue::Text(actual) => {
            assert_eq!(
                actual, expected,
                "{formula}: expected {expected:?}, got {actual:?}"
            )
        }
        other => panic!("{formula}: expected {expected:?}, got {other:?}"),
    }
}

fn assert_int(formula: &str, expected: i64) {
    match eval_formula(formula) {
        LiteralValue::Int(actual) => assert_eq!(actual, expected, "{formula}"),
        LiteralValue::Number(actual) => {
            assert_eq!(actual as i64, expected, "{formula}")
        }
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
fn hyperlink_returns_friendly_name() {
    assert_text(r#"=HYPERLINK("https://example.com","Example")"#, "Example");
    assert_text(r#"=HYPERLINK("https://example.com","")"#, "");
}

#[test]
fn hyperlink_returns_link_location_without_name() {
    assert_text(
        r#"=HYPERLINK("https://example.com")"#,
        "https://example.com",
    );
    // Numbers are text-coerced, like Excel's friendly-name fallback.
    assert_text("=HYPERLINK(42)", "42");
}

#[test]
fn hyperlink_xlfn_spelling_resolves() {
    assert_text(r#"=_xlfn.HYPERLINK("https://example.com","link")"#, "link");
}

#[test]
fn hyperlink_propagates_argument_errors() {
    assert_error("=HYPERLINK(1/0)", ExcelErrorKind::Div);
    assert_error(r#"=HYPERLINK("x",1/0)"#, ExcelErrorKind::Div);
}

#[test]
fn cell_contents_returns_top_left_value() {
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(r#"=CELL("contents",$A$2)"#).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 1).unwrap() {
        LiteralValue::Int(10) | LiteralValue::Number(10.0) => {}
        other => panic!("CELL contents: expected 10, got {other:?}"),
    }
}

#[test]
fn cell_contents_on_range_uses_first_cell() {
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(20))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(r#"=CELL("contents",A2:A3)"#).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 1).unwrap() {
        LiteralValue::Int(10) | LiteralValue::Number(10.0) => {}
        other => panic!("CELL contents: expected 10, got {other:?}"),
    }
}

#[test]
fn cell_address_col_row() {
    assert_text(r#"=CELL("address",A1)"#, "$A$1");
    assert_text(r#"=CELL("address",B3)"#, "$B$3");
    assert_text(r#"=CELL("address",AA100)"#, "$AA$100");
    assert_int(r#"=CELL("col",B3)"#, 2);
    assert_int(r#"=CELL("row",B3)"#, 3);
}

#[test]
fn cell_type_classifies_value() {
    assert_text(r#"=CELL("type",Z99)"#, "b");
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Text("x".into()))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(r#"=CELL("type",A2)"#).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1).unwrap(),
        LiteralValue::Text("l".into())
    );
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(5))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1).unwrap(),
        LiteralValue::Text("v".into())
    );
}

#[test]
fn cell_unsupported_info_type_is_value_error() {
    assert_error(r#"=CELL("format",A1)"#, ExcelErrorKind::Value);
    assert_error(r#"=CELL("filename",A1)"#, ExcelErrorKind::Value);
    assert_error(r#"=CELL("protect",A1)"#, ExcelErrorKind::Value);
    assert_error(r#"=CELL("width",A1)"#, ExcelErrorKind::Value);
}

#[test]
fn cell_requires_reference_argument() {
    // Missing reference: Excel reports on the last-changed cell, which is not
    // reproducible, so we surface #VALUE!.
    assert_error(r#"=CELL("contents")"#, ExcelErrorKind::Value);
    // A scalar value cannot be inspected as a reference.
    assert_error(r#"=CELL("address",42)"#, ExcelErrorKind::Value);
}

#[test]
fn cell_non_text_info_type_is_value_error() {
    assert_error("=CELL(42,A1)", ExcelErrorKind::Value);
    assert_error("=CELL(TRUE,A1)", ExcelErrorKind::Value);
}

#[test]
fn cell_extra_arguments_rejected() {
    assert_error(r#"=CELL("address",A1,A1)"#, ExcelErrorKind::Value);
}
