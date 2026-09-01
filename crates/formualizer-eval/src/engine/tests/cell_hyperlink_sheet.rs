//! CELL / HYPERLINK builtins.
//!
//! CELL answers `contents`, `address`, `col`, `row` and `type` for a reference;
//! HYPERLINK returns its friendly name or link location as text.
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
    // HYPERLINK is an Excel-97-era function and is never written with the
    // `_xlfn.` prefix in real files; the registry strips the prefix
    // generically, so the spelling still has to resolve.
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
    // Blank cells classify as "b". NOTE: this is provisional pending #319/#285
    // blank-coercion adjudication; the variant study may change what a blank
    // cell yields, so this assertion should be revisited rather than preserved.
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
fn cell_contents_on_blank_is_empty_in_graph() {
    // diverges from Excel: Excel returns numeric 0; tracked in #333, resolve
    // with the #319 variant study. In-graph the result is Empty, so ISBLANK
    // sees a blank; egress coerces the same result to 0.
    assert_eq!(
        eval_formula(r#"=ISBLANK(CELL("contents",Z99))"#),
        LiteralValue::Boolean(true)
    );
    assert_eq!(
        eval_formula(r#"=CELL("contents",Z99)"#),
        LiteralValue::Number(0.0)
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
    // A scalar value cannot be inspected as a reference by the metadata info
    // types; #VALUE! is reserved for exactly this case.
    assert_error(r#"=CELL("address",42)"#, ExcelErrorKind::Value);
    assert_error(r#"=CELL("col",42)"#, ExcelErrorKind::Value);
    assert_error(r#"=CELL("row","x")"#, ExcelErrorKind::Value);
}

#[test]
fn cell_propagates_reference_argument_errors() {
    // Excel evaluates the argument first, so its error wins over #VALUE!.
    assert_error(r#"=CELL("row",1/0)"#, ExcelErrorKind::Div);
    assert_error(r#"=CELL("address",1/0)"#, ExcelErrorKind::Div);
    assert_error(r#"=CELL("contents",NA())"#, ExcelErrorKind::Na);
    assert_error(r#"=CELL("type",NA())"#, ExcelErrorKind::Na);
}

#[test]
fn cell_propagates_ref_error_after_row_delete() {
    // Deleting the referenced row invalidates the reference; CELL must report
    // #REF!, not a blanket #VALUE!.
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(7))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 20, parse(r#"=CELL("row",A5)"#).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 20).unwrap(),
        LiteralValue::Number(5.0)
    );
    engine.delete_rows("Sheet1", 5, 1).unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 20).unwrap() {
        LiteralValue::Error(error) => assert_eq!(error.kind, ExcelErrorKind::Ref),
        other => panic!("expected #REF! after delete_rows, got {other:?}"),
    }
}

#[test]
fn cell_value_info_types_accept_a_literal() {
    // Excel's `contents` and `type` read a value, so a literal in the reference
    // position is legitimate.
    assert_text(r#"=CELL("type","")"#, "l");
    assert_text(r#"=CELL("type","x")"#, "l");
    assert_text(r#"=CELL("type",5)"#, "v");
    assert_int(r#"=CELL("contents",5)"#, 5);
    assert_text(r#"=CELL("contents","abc")"#, "abc");
}

#[test]
fn cell_rejects_3d_references() {
    // Excel's CELL does not accept 3-D references; it must not silently report
    // on the first sheet of the span.
    let mut engine = new_engine();
    engine.graph.add_sheet("Sheet2").unwrap();
    for formula in [
        r#"=CELL("address",Sheet1:Sheet2!A1)"#,
        r#"=CELL("row",Sheet1:Sheet1!A1)"#,
        r#"=CELL("contents",Sheet1:Sheet2!A1)"#,
    ] {
        engine
            .set_cell_formula("Sheet1", 1, 20, parse(formula).unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        match engine.get_cell_value("Sheet1", 1, 20).unwrap() {
            LiteralValue::Error(error) => {
                assert_eq!(error.kind, ExcelErrorKind::Value, "{formula}")
            }
            other => panic!("{formula}: expected #VALUE!, got {other:?}"),
        }
    }
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

#[test]
fn cell_address_qualifies_other_sheet() {
    let mut engine = new_engine();
    engine.graph.add_sheet("Sheet2").unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            20,
            parse(r#"=CELL("address",Sheet2!A1)"#).unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 20).unwrap() {
        LiteralValue::Text(actual) => assert_eq!(actual, "Sheet2!$A$1"),
        other => panic!("expected \"Sheet2!$A$1\", got {other:?}"),
    }
    // Same-sheet references stay unqualified.
    engine
        .set_cell_formula("Sheet1", 1, 20, parse(r#"=CELL("address",A1)"#).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 20).unwrap() {
        LiteralValue::Text(actual) => assert_eq!(actual, "$A$1"),
        other => panic!("expected \"$A$1\", got {other:?}"),
    }
}

#[test]
fn cell_address_qualifies_off_sheet_with_quoted_name() {
    // A cross-sheet reference whose sheet name needs quoting is qualified with
    // the quoted name, per Excel, and the coordinates come from the reference.
    let mut engine = new_engine();
    engine.graph.add_sheet("My Sheet").unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            20,
            parse(r#"=CELL("address",'My Sheet'!B7)"#).unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 20).unwrap() {
        LiteralValue::Text(actual) => assert_eq!(actual, "'My Sheet'!$B$7"),
        other => panic!("expected \"'My Sheet'!$B$7\", got {other:?}"),
    }
}

#[test]
fn cell_address_on_ranges_and_whole_columns() {
    // The address is the top-left cell of a range reference.
    assert_text(r#"=CELL("address",B2:C3)"#, "$B$2");
    // Whole-column/row references resolve to a top-left address without
    // materializing the referenced axis's values.
    assert_text(r#"=CELL("address",A:A)"#, "$A$1");
    assert_text(r#"=CELL("address",2:2)"#, "$A$2");
}

#[test]
fn hyperlink_rejects_multi_cell_arguments() {
    let mut engine = new_engine();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(2))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 20, parse("=HYPERLINK(A2:A3)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 20).unwrap() {
        LiteralValue::Error(e) => {
            assert_eq!(e.kind, ExcelErrorKind::Value, "range link_location")
        }
        other => panic!("expected #VALUE! for range link_location, got {other:?}"),
    }
    // Array literals behave the same, except 1x1 arrays collapse.
    assert_error("=HYPERLINK({1,2})", ExcelErrorKind::Value);
    assert_error(
        r#"=HYPERLINK("https://example.com",{1,2})"#,
        ExcelErrorKind::Value,
    );
    assert_text("=HYPERLINK({1})", "1");
}
