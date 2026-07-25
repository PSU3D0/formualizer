//! Scalar-to-1x1 promotion for range-consuming builtins (issue #224).
//!
//! Excel treats a scalar handed to a range-consuming function as a 1x1 array.
//! Functions that treated a resolution failure as an error opt into that via
//! `ArgumentHandle::range_view_or_scalar`.
//!
//! The second half of this file is the more important half. Several builtins use
//! a `range_view` failure as *type dispatch*, where "scalar" and "1x1 range" mean
//! different things. Promoting inside `range_view` itself would silently change
//! those answers, and nothing pinned them before, so a future attempt would have
//! passed CI. These tests exist to make that attempt fail loudly.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn eval(formula: &str) -> LiteralValue {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine.add_sheet("Sheet1").ok();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(formula).unwrap())
        .unwrap();
    engine
        .evaluate_cell("Sheet1", 1, 1)
        .unwrap()
        .unwrap_or(LiteralValue::Empty)
}

#[test]
fn dynamic_array_builtins_accept_a_scalar_as_a_1x1_array() {
    for formula in [
        "=TRANSPOSE(2)",
        "=SORT(2)",
        "=UNIQUE(2)",
        "=TAKE(2,1)",
        "=DROP(2,0)",
    ] {
        assert_eq!(
            eval(formula),
            LiteralValue::Number(2.0),
            "{formula} should treat the scalar as a 1x1 array"
        );
    }
}

#[test]
fn promotion_preserves_the_arguments_own_error_instead_of_masking_it() {
    // Previously #REF!, because the scalar was rejected for not being a range
    // before its own error was ever looked at.
    for (formula, kind) in [
        ("=TRANSPOSE(NA())", ExcelErrorKind::Na),
        ("=SORT(NA())", ExcelErrorKind::Na),
        ("=TRANSPOSE(1/0)", ExcelErrorKind::Div),
    ] {
        match eval(formula) {
            LiteralValue::Error(error) => assert_eq!(error.kind, kind, "{formula}"),
            other => panic!("{formula} expected {kind:?}, got {other:?}"),
        }
    }
}

#[test]
fn a_lambda_is_not_promoted_to_a_1x1_array() {
    match eval("=TRANSPOSE(LAMBDA(x,x))") {
        LiteralValue::Error(error) => assert_eq!(error.kind, ExcelErrorKind::Ref),
        other => panic!("expected #REF!, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Guards: builtins that must keep distinguishing a scalar from a 1x1 range.
// ---------------------------------------------------------------------------

/// A direct scalar argument is numerically coerced; a range cell of the same
/// type is skipped. Promoting inside `range_view` would move these arguments
/// onto the range path and silently change ~54 statistical functions.
#[test]
fn statistical_functions_still_coerce_a_direct_scalar_argument() {
    assert_eq!(eval("=MEDIAN(TRUE)"), LiteralValue::Number(1.0));
    assert_eq!(eval("=PRODUCT(\"3\")"), LiteralValue::Number(3.0));
    assert_eq!(eval("=AVERAGEA(TRUE)"), LiteralValue::Number(1.0));
    match eval("=STDEV.S(1,2,TRUE)") {
        LiteralValue::Number(value) => {
            assert!(
                (value - 0.577_350_269_189_625_7).abs() < 1e-12,
                "got {value}"
            )
        }
        other => panic!("expected a number, got {other:?}"),
    }
}

/// A scalar criteria argument is an error, not an empty criteria block. An
/// empty criteria list matches every row, so promoting here would turn a
/// rejected formula into a plausible-looking wrong number.
#[test]
fn d_functions_still_reject_a_scalar_criteria_argument() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine.add_sheet("Sheet1").ok();
    for (row, (label, amount)) in [("a", 1.0), ("b", 2.0), ("c", 4.0)].iter().enumerate() {
        let row = row as u32 + 2;
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Text((*label).to_string()))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(*amount))
            .unwrap();
    }
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("Label".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("Amount".into()))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 5, parse("=DSUM(A1:B4,\"Amount\",5)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 5) {
        Some(LiteralValue::Error(error)) => assert_eq!(error.kind, ExcelErrorKind::Value),
        // 7.0 would mean the scalar became an empty criteria block that matched
        // every row.
        other => panic!("expected #VALUE!, got {other:?}"),
    }
}

/// ARRAYTOTEXT returns an error argument as an error, not as its own text.
#[test]
fn arraytotext_still_propagates_an_error_argument() {
    match eval("=ARRAYTOTEXT(NA())") {
        LiteralValue::Error(error) => assert_eq!(error.kind, ExcelErrorKind::Na),
        other => panic!("expected #N/A, got {other:?}"),
    }
}
