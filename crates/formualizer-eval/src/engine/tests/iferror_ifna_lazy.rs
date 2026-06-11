//! IFERROR/IFNA semantics pins + lazy-dispatch polarity tests.
//!
//! IFERROR and IFNA are `SHORT_CIRCUIT` functions (same defect class as the
//! IF bug fixed in #118): the fallback argument must not be evaluated unless
//! the value argument produced an error (for IFNA: only `#N/A`). The first
//! half of this file pins the value/array semantics that must be identical
//! before and after the lazy-dispatch change; the second half proves
//! laziness with an evaluation-counting function.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn wb_with_builtins() -> TestWorkbook {
    TestWorkbook::new()
        .with_function(Arc::new(crate::builtins::logical_ext::IfErrorFn))
        .with_function(Arc::new(crate::builtins::logical_ext::IfNaFn))
}

fn eval_formula(formula: &str) -> LiteralValue {
    let wb = wb_with_builtins();
    let interp = wb.interpreter();
    let ast = parse(formula).unwrap();
    interp.evaluate_ast(&ast).unwrap().into_literal()
}

/* ───────────────────── value-semantics pins (pre/post identical) ───────────────────── */

#[test]
fn iferror_catches_every_error_kind() {
    for err in [
        "#DIV/0!", "#N/A", "#NAME?", "#NULL!", "#NUM!", "#REF!", "#VALUE!",
    ] {
        let v = eval_formula(&format!("=IFERROR({err},42)"));
        assert_eq!(v, LiteralValue::Number(42.0), "IFERROR must catch {err}");
    }
}

#[test]
fn ifna_catches_only_na() {
    assert_eq!(eval_formula("=IFNA(#N/A,42)"), LiteralValue::Number(42.0));
    for err in ["#DIV/0!", "#NAME?", "#NULL!", "#NUM!", "#REF!", "#VALUE!"] {
        match eval_formula(&format!("=IFNA({err},42)")) {
            LiteralValue::Error(e) => {
                assert_eq!(e.to_string(), err, "IFNA must pass through {err}")
            }
            other => panic!("IFNA({err},42) must pass the error through, got {other:?}"),
        }
    }
}

#[test]
fn iferror_text_and_number_passthrough() {
    assert_eq!(
        eval_formula("=IFERROR(\"ok\",\"fb\")"),
        LiteralValue::Text("ok".into())
    );
    assert_eq!(eval_formula("=IFERROR(0,99)"), LiteralValue::Number(0.0));
    assert_eq!(
        eval_formula("=IFNA(\"ok\",\"fb\")"),
        LiteralValue::Text("ok".into())
    );
}

#[test]
fn iferror_blank_value_arg_passes_through_as_empty() {
    // A1 is blank; IFERROR(A1, 42) yields the blank (Empty), not the fallback.
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=IFERROR(A1,42)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=IFNA(A1,42)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    let v = engine.get_cell_value("Sheet1", 1, 2);
    let v2 = engine.get_cell_value("Sheet1", 2, 2);
    // Blank arg0 is not an error: it passes through as Empty (engine stores
    // an Empty result as an empty cell), NOT as the fallback.
    assert!(
        matches!(v, None | Some(LiteralValue::Empty)),
        "blank passthrough changed: {v:?}"
    );
    assert_eq!(v, v2, "IFERROR and IFNA must agree on blank passthrough");
}

#[test]
fn iferror_arity_errors_are_value() {
    // Too many args: #VALUE! (was enforced by eager validation, now by eval body).
    for f in ["=IFERROR(1,2,3)", "=IFNA(1,2,3)"] {
        match eval_formula(f) {
            LiteralValue::Error(e) => {
                assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Value)
            }
            other => panic!("{f} must be #VALUE!, got {other:?}"),
        }
    }
}

/* ───────────────────── array/spill semantics pin ─────────────────────
 *
 * IFERROR is whole-value in this engine: when arg0 evaluates to an array
 * (e.g. a broadcast division over ranges), the array passes through as-is —
 * element-wise errors inside the array are NOT replaced by the fallback.
 * The fallback is used only when arg0 itself is a (scalar) error / eval
 * failure. This pin guards that the lazy-dispatch change does not alter the
 * array path. (Laziness applies to the same `args[0].value()` call the eval
 * body already made; arrays were never materialized differently.)
 */

#[test]
fn iferror_array_arg_passes_array_through_with_elementwise_errors() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());
    // A1:A3 = 1,2,3 ; B1=1, B2=0 (div error), B3=3
    for (r, v) in [(1, 1), (2, 2), (3, 3)] {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Int(v))
            .unwrap();
    }
    for (r, v) in [(1, 1), (2, 0), (3, 3)] {
        engine
            .set_cell_value("Sheet1", r, 2, LiteralValue::Int(v))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=IFERROR(A1:A3/B1:B3,0)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(1.0))
    );
    // Element-wise error inside the array is NOT replaced by the fallback.
    match engine.get_cell_value("Sheet1", 2, 3) {
        Some(LiteralValue::Error(e)) => {
            assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Div)
        }
        other => panic!("expected #DIV/0! spilled at C2, got {other:?}"),
    }
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(1.0))
    );
}

/* ───────────────────── laziness polarity tests (#118 pattern) ───────────────────── */

#[derive(Debug)]
struct CountFn(Arc<AtomicUsize>);
impl crate::function::Function for CountFn {
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "COUNTING"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(7)))
    }
}

fn harness() -> (TestWorkbook, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let wb = wb_with_builtins().with_function(Arc::new(CountFn(counter.clone())));
    (wb, counter)
}

fn eval_in(wb: &TestWorkbook, formula: &str) -> LiteralValue {
    let interp = wb.interpreter();
    let ast = parse(formula).unwrap();
    interp.evaluate_ast(&ast).unwrap().into_literal()
}

#[test]
fn iferror_ok_value_does_not_evaluate_fallback() {
    let (wb, counter) = harness();
    let v = eval_in(&wb, "=IFERROR(1,COUNTING())");
    assert_eq!(v, LiteralValue::Number(1.0));
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "IFERROR fallback must not be evaluated when the value arg succeeds"
    );
}

#[test]
fn iferror_error_value_evaluates_fallback_exactly_once() {
    let (wb, counter) = harness();
    let v = eval_in(&wb, "=IFERROR(1/0,COUNTING())");
    assert_eq!(v, LiteralValue::Int(7));
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "IFERROR fallback must be evaluated exactly once on error"
    );
}

#[test]
fn ifna_fallback_only_for_na_other_errors_pass_through_unevaluated() {
    let (wb, counter) = harness();

    // #N/A: fallback taken, evaluated once.
    let v = eval_in(&wb, "=IFNA(#N/A,COUNTING())");
    assert_eq!(v, LiteralValue::Int(7));
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // Non-NA error: passes through, fallback NOT evaluated.
    let v = eval_in(&wb, "=IFNA(1/0,COUNTING())");
    match v {
        LiteralValue::Error(e) => assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Div),
        other => panic!("expected #DIV/0! passthrough, got {other:?}"),
    }
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "IFNA fallback must not be evaluated for non-#N/A errors"
    );

    // Plain value: fallback NOT evaluated.
    let v = eval_in(&wb, "=IFNA(5,COUNTING())");
    assert_eq!(v, LiteralValue::Number(5.0));
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn nested_iferror_only_evaluates_needed_arms() {
    let (wb, counter) = harness();
    // Inner IFERROR succeeds -> outer sees a non-error -> no COUNTING anywhere.
    let v = eval_in(&wb, "=IFERROR(IFERROR(3,COUNTING()),COUNTING())");
    assert_eq!(v, LiteralValue::Number(3.0));
    assert_eq!(counter.load(Ordering::SeqCst), 0);

    // Inner errors -> inner fallback evaluated once; outer sees non-error.
    let v = eval_in(&wb, "=IFERROR(IFERROR(1/0,COUNTING()),COUNTING())");
    assert_eq!(v, LiteralValue::Int(7));
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
