//! Elementwise IFERROR/IFNA over arrays and ranges.
//!
//! Contract under test:
//! - an array or range `value` has each matching error element replaced by
//!   the corresponding element of the fallback (IFERROR: any error kind,
//!   IFNA: only `#N/A`); non-matching elements pass through unchanged;
//! - the output shape follows the engine's operator broadcast rule
//!   (`broadcast.rs`): singleton axes broadcast, incompatible axes are
//!   `#VALUE!`;
//! - when no element matches, the fallback is never evaluated and the value
//!   is returned untouched (a sheet range stays a borrowed view);
//! - when some element matches, the fallback is evaluated exactly once;
//! - the materialized output is bounded by the shared generated-array cap
//!   (`#NUM!`), checked before allocation and only when materializing;
//! - live cancellation and resource failures raised while evaluating either
//!   argument, or observed while scanning/copying, propagate as `Err` rather
//!   than being converted into spreadsheet fallback values.

use crate::engine::{CancelToken, Engine, EvalConfig};
use crate::function::{FnCaps, Function};
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{
    ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue, ResourceExhaustionDetail,
    ResourceExhaustionReason,
};
use formualizer_parse::parser::parse;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/* ───────────────────────────── helpers ───────────────────────────── */

/// Renders a value for readable comparison: numbers as `f64`, errors by
/// their spreadsheet code, arrays row by row.
fn norm(v: &LiteralValue) -> String {
    match v {
        LiteralValue::Number(n) => format!("{n}"),
        LiteralValue::Int(i) => format!("{}", *i as f64),
        LiteralValue::Error(e) => e.kind.to_string(),
        LiteralValue::Text(s) => format!("{s:?}"),
        LiteralValue::Boolean(b) => b.to_string().to_uppercase(),
        LiteralValue::Empty => "<empty>".into(),
        LiteralValue::Array(rows) => {
            let rows: Vec<String> = rows
                .iter()
                .map(|r| r.iter().map(norm).collect::<Vec<_>>().join(","))
                .collect();
            format!("[{}]", rows.join(";"))
        }
        other => format!("{other:?}"),
    }
}

type Behavior = fn(&dyn FunctionContext<'_>, usize) -> Result<CalcValue<'static>, ExcelError>;

/// Test UDF: counts calls and delegates to a behavior function that may
/// flip the context's cancellation token or return a live failure.
#[derive(Debug)]
struct Probe {
    name: &'static str,
    calls: Arc<AtomicUsize>,
    behavior: Behavior,
}

impl Function for Probe {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        self.name
    }
    fn min_args(&self) -> usize {
        0
    }
    // Host functions that propagate their own annotation (as IFERROR does)
    // can hand an annotated array to the guards.
    fn propagate_format(&self, result: &CalcValue<'_>) -> Option<crate::format::FormatId> {
        result.format_id()
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        (self.behavior)(ctx, call)
    }
}

fn cancel_ctx(ctx: &dyn FunctionContext<'_>) {
    ctx.cancellation_token()
        .expect("test context must provide a cancellation token")
        .cancel();
}

fn resource_error() -> ExcelError {
    ExcelError::new(ExcelErrorKind::NImpl).with_extra(ExcelErrorExtra::Resource {
        detail: Box::new(ResourceExhaustionDetail {
            reason: ResourceExhaustionReason::ScratchMemory,
            limit: 1,
            observed: 2,
            request_id: None,
        }),
    })
}

fn err(kind: ExcelErrorKind) -> LiteralValue {
    LiteralValue::Error(ExcelError::new(kind))
}

/// Registry of probes shared by the interpreter-level tests.
struct Harness {
    wb: TestWorkbook,
    token: CancelToken,
    counters: std::collections::HashMap<&'static str, Arc<AtomicUsize>>,
}

impl Harness {
    fn new() -> Self {
        crate::builtins::load_builtins();
        let token = CancelToken::new();
        let mut wb = TestWorkbook::new().with_cancellation_token(token.clone());
        let mut counters = std::collections::HashMap::new();
        let probes: [(&'static str, Behavior); 9] = [
            // Plain value; the laziness counter.
            ("COUNTING", |_, _| {
                Ok(CalcValue::Scalar(LiteralValue::Int(7)))
            }),
            // Live cancellation raised by the argument itself.
            ("CANCEL_ERR", |ctx, _| {
                cancel_ctx(ctx);
                Err(ExcelError::new(ExcelErrorKind::Cancelled))
            }),
            // Live resource failure raised by the argument itself.
            ("RESOURCE_ERR", |_, _| Err(resource_error())),
            // An ordinary evaluation failure surfaced through `Err`.
            ("VALUE_ERR", |_, _| Err(ExcelError::new_value())),
            // Returns an array containing an error, but cancels the request
            // first: the scan must observe the cancellation.
            ("CANCEL_ARRAY", |ctx, _| {
                cancel_ctx(ctx);
                Ok(CalcValue::Scalar(LiteralValue::Array(vec![vec![
                    err(ExcelErrorKind::Div),
                    LiteralValue::Number(4.0),
                ]])))
            }),
            // Returns a clean owned range with no attached token, but cancels
            // the request first: a clean probe must not report success.
            ("CANCEL_RANGE", |ctx, _| {
                cancel_ctx(ctx);
                Ok(CalcValue::Range(
                    crate::engine::range_view::RangeView::from_owned_rows(
                        vec![
                            vec![LiteralValue::Number(1.0)],
                            vec![LiteralValue::Number(2.0)],
                        ],
                        crate::engine::DateSystem::Excel1900,
                    ),
                ))
            }),
            // Owned range with a stored error and no attached token.
            ("ERROR_RANGE", |_, _| {
                Ok(CalcValue::Range(
                    crate::engine::range_view::RangeView::from_owned_rows(
                        vec![
                            vec![LiteralValue::Number(1.0)],
                            vec![err(ExcelErrorKind::Ref)],
                            vec![err(ExcelErrorKind::Na)],
                        ],
                        crate::engine::DateSystem::Excel1900,
                    ),
                ))
            }),
            // Fallback that succeeds but cancels the request while doing so:
            // the copy loop must observe the cancellation.
            ("CANCEL_SEVEN", |ctx, _| {
                cancel_ctx(ctx);
                Ok(CalcValue::Scalar(LiteralValue::Int(7)))
            }),
            // A wide single-row array (1 x 16384) whose first element is an
            // error; cancels the request before returning.
            ("CANCEL_WIDE", |ctx, _| {
                cancel_ctx(ctx);
                let mut row = vec![LiteralValue::Number(1.0); 16_384];
                row[0] = err(ExcelErrorKind::Div);
                Ok(CalcValue::Scalar(LiteralValue::Array(vec![row])))
            }),
        ];
        for (name, behavior) in probes {
            let calls = Arc::new(AtomicUsize::new(0));
            counters.insert(name, calls.clone());
            wb = wb.with_function(Arc::new(Probe {
                name,
                calls,
                behavior,
            }));
        }
        Self {
            wb,
            token,
            counters,
        }
    }

    fn calls(&self, name: &str) -> usize {
        self.counters[name].load(Ordering::SeqCst)
    }

    fn eval(&self, formula: &str) -> Result<CalcValue<'_>, ExcelError> {
        let interp = self.wb.interpreter();
        let ast = parse(formula).unwrap();
        interp.evaluate_ast(&ast)
    }

    fn literal(&self, formula: &str) -> String {
        match self.eval(formula) {
            Ok(v) => norm(&v.into_literal()),
            Err(e) => panic!("{formula} returned Err({e:?})"),
        }
    }
}

fn engine_with(values: &[(u32, u32, LiteralValue)]) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (r, c, v) in values {
        engine.set_cell_value("Sheet1", *r, *c, v.clone()).unwrap();
    }
    engine
}

fn column(engine: &Engine<TestWorkbook>, col: u32, rows: std::ops::RangeInclusive<u32>) -> String {
    rows.map(|r| match engine.get_cell_value("Sheet1", r, col) {
        Some(v) => norm(&v),
        None => "<none>".into(),
    })
    .collect::<Vec<_>>()
    .join(",")
}

/* ───────────────────────── elementwise selection ───────────────────────── */

#[test]
fn iferror_replaces_error_elements_of_computed_array() {
    let h = Harness::new();
    assert_eq!(h.literal("=IFERROR(1/{0,4},8)"), "[8,0.25]");
    assert_eq!(h.literal("=IFERROR({1;#N/A;3},\"x\")"), "[1;\"x\";3]");
}

#[test]
fn ifna_replaces_only_na_elements() {
    let h = Harness::new();
    assert_eq!(h.literal("=IFNA({#N/A,#DIV/0!,3},0)"), "[0,#DIV/0!,3]");
    // No #N/A element: array passes through, fallback not evaluated.
    assert_eq!(h.literal("=IFNA(1/{0,4},COUNTING())"), "[#DIV/0!,0.25]");
    assert_eq!(h.calls("COUNTING"), 0);
}

#[test]
fn iferror_catches_every_stored_error_kind_per_element() {
    let kinds = [
        ExcelErrorKind::Div,
        ExcelErrorKind::Na,
        ExcelErrorKind::Name,
        ExcelErrorKind::Null,
        ExcelErrorKind::Num,
        ExcelErrorKind::Ref,
        ExcelErrorKind::Value,
    ];
    let mut values = vec![(1, 1, LiteralValue::Number(1.0))];
    for (i, kind) in kinds.iter().enumerate() {
        values.push((i as u32 + 2, 1, err(*kind)));
    }
    let mut engine = engine_with(&values);
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=IFERROR(A1:A8,0)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=IFNA(A1:A8,0)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(column(&engine, 2, 1..=8), "1,0,0,0,0,0,0,0");
    assert_eq!(
        column(&engine, 3, 1..=8),
        "1,#DIV/0!,0,#NAME?,#NULL!,#NUM!,#REF!,#VALUE!"
    );
}

#[test]
fn iferror_over_range_division_and_reduction() {
    let mut engine = engine_with(&[
        (1, 1, LiteralValue::Number(1.0)),
        (2, 1, LiteralValue::Number(0.0)),
        (3, 1, LiteralValue::Number(2.0)),
        (1, 2, LiteralValue::Number(10.0)),
        (2, 2, LiteralValue::Number(20.0)),
        (3, 2, LiteralValue::Number(30.0)),
    ]);
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=IFERROR(1/A1:A3,-1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=IFERROR(1/A1:A3,B1:B3)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 5, parse("=SUM(IFERROR(1/A1:A3,0))").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(column(&engine, 3, 1..=3), "1,-1,0.5");
    assert_eq!(column(&engine, 4, 1..=3), "1,20,0.5");
    assert_eq!(column(&engine, 5, 1..=1), "1.5");
}

#[test]
fn iferror_replaces_formula_produced_errors_in_referenced_range() {
    // Errors computed by formulas live in the computed overlay, not the base
    // lanes; the probe must see them.
    let mut engine = engine_with(&[(1, 1, LiteralValue::Number(3.0))]);
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=1/0").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=NA()").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=IFERROR(A1:A3,5)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=IFNA(A1:A3,5)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(column(&engine, 2, 1..=3), "3,5,5");
    assert_eq!(column(&engine, 3, 1..=3), "3,#DIV/0!,5");

    // Edit clears the error: the guard recomputes to a clean passthrough.
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=2").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(column(&engine, 2, 1..=3), "3,2,5");
}

#[test]
fn iferror_fallback_errors_propagate_positionally() {
    let h = Harness::new();
    assert_eq!(h.literal("=IFERROR(1/{0,4},1/{0,0})"), "[#DIV/0!,0.25]");
    assert_eq!(h.literal("=IFERROR(1/{0,4},NA())"), "[#N/A,0.25]");
    // An ordinary `Err` from the fallback becomes that error per element.
    assert_eq!(h.literal("=IFERROR(1/{0,4},VALUE_ERR())"), "[#VALUE!,0.25]");
}

#[test]
fn iferror_output_shape_follows_operator_broadcast() {
    let h = Harness::new();
    // 1x2 value with 2x1 fallback broadcasts to 2x2.
    assert_eq!(h.literal("=IFERROR({#N/A,2},{1;2})"), "[1,2;2,2]");
    // Fallback column paired positionally with a column value.
    assert_eq!(h.literal("=IFERROR({#N/A;2;#REF!},{7;8;9})"), "[7;2;9]");
    // Incompatible non-singleton axes follow the operator rule: #VALUE!.
    assert_eq!(h.literal("=IFERROR({#N/A,#N/A,#N/A},{8,9})"), "#VALUE!");
    assert_eq!(h.literal("=IFNA({#N/A,1,2},{8,9})"), "#VALUE!");
}

#[test]
fn iferror_scalar_error_with_array_fallback_is_unchanged() {
    let h = Harness::new();
    assert_eq!(h.literal("=IFERROR(1/0,{5,6})"), "[5,6]");
}

/* ─────────────────────────────── laziness ─────────────────────────────── */

#[test]
fn clean_array_does_not_evaluate_fallback() {
    let h = Harness::new();
    assert_eq!(h.literal("=IFERROR({1,4},COUNTING())"), "[1,4]");
    assert_eq!(h.literal("=IFNA({1,#DIV/0!},COUNTING())"), "[1,#DIV/0!]");
    assert_eq!(h.calls("COUNTING"), 0);
}

#[test]
fn array_with_errors_evaluates_fallback_exactly_once() {
    let h = Harness::new();
    assert_eq!(
        h.literal("=IFERROR({#DIV/0!,4;#N/A,#REF!},COUNTING())"),
        "[7,4;7,7]"
    );
    assert_eq!(h.calls("COUNTING"), 1);
    assert_eq!(
        h.literal("=IFNA({#N/A,#N/A;1,#N/A},COUNTING())"),
        "[7,7;1,7]"
    );
    assert_eq!(h.calls("COUNTING"), 2);
}

#[test]
fn clean_value_keeps_larger_dormant_fallback_unevaluated() {
    let h = Harness::new();
    // A fallback that would be over the generated-array cap is never touched.
    assert_eq!(h.literal("=IFERROR({1,2},SEQUENCE(1000000000))"), "[1,2]");
    assert_eq!(h.literal("=IFERROR({1,2},{9;8;7})"), "[1,2]");
}

#[test]
fn clean_sheet_range_is_returned_as_the_same_borrowed_view() {
    let calls = Arc::new(AtomicUsize::new(0));
    let wb = TestWorkbook::new().with_function(Arc::new(Probe {
        name: "COUNTING",
        calls: calls.clone(),
        behavior: |_, _| Ok(CalcValue::Scalar(LiteralValue::Int(7))),
    }));
    let mut engine = Engine::new(wb, EvalConfig::default());
    for r in 1..=3 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    let interp = crate::interpreter::Interpreter::new(&engine, "Sheet1");
    for formula in ["=IFERROR(A1:A3,COUNTING())", "=IFNA(A1:A3,COUNTING())"] {
        match interp.evaluate_ast(&parse(formula).unwrap()).unwrap() {
            CalcValue::Range(view) => {
                assert!(view.is_sheet_backed(), "{formula} must stay zero-copy");
                assert_eq!(view.dims(), (3, 1));
            }
            other => panic!("{formula} must return the original range, got {other:?}"),
        }
    }
    // A clean view larger than the materialization cap is still passed
    // through: the cap applies only when a new array is built.
    match interp
        .evaluate_ast(&parse("=IFERROR(A1:XFD1025,COUNTING())").unwrap())
        .unwrap()
    {
        CalcValue::Range(view) => {
            assert!(view.is_sheet_backed());
            assert_eq!(view.dims(), (1025, 16_384));
        }
        other => panic!("clean oversized range must pass through, got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

/* ─────────────────────────── materialization cap ─────────────────────────── */

#[test]
fn materialized_output_over_shared_cap_is_num_before_allocation() {
    let h = Harness::new();
    // 4097 x 1 value with an error, broadcast against a 1 x 4097 fallback:
    // 16,785,409 output cells exceeds the 2^24 generated-array cap.
    assert_eq!(
        h.literal("=IFERROR(1/(SEQUENCE(4097)-1),SEQUENCE(1,4097))"),
        "#NUM!"
    );
    // The cap applies to the broadcast output, not to either input alone.
    // (Exact boundaries are unit-tested on the shared guard.)
    assert_eq!(
        h.literal("=ROWS(IFERROR(1/(SEQUENCE(4097)-1),SEQUENCE(1,2)))"),
        "4097"
    );
}

/* ─────────────────────── out-of-band failure polarity ─────────────────────── */

#[test]
fn value_cancellation_propagates_instead_of_selecting_fallback() {
    for formula in [
        "=IFERROR(CANCEL_ERR(),COUNTING())",
        "=IFNA(CANCEL_ERR(),COUNTING())",
    ] {
        let h = Harness::new();
        let e = h.eval(formula).expect_err(formula);
        assert_eq!(e.kind, ExcelErrorKind::Cancelled, "{formula}");
        assert_eq!(h.calls("COUNTING"), 0, "{formula}");
        assert!(h.token.is_cancelled());
    }
}

#[test]
fn value_resource_failure_propagates_instead_of_selecting_fallback() {
    for formula in [
        "=IFERROR(RESOURCE_ERR(),COUNTING())",
        "=IFNA(RESOURCE_ERR(),COUNTING())",
    ] {
        let h = Harness::new();
        let e = h.eval(formula).expect_err(formula);
        assert!(
            matches!(e.extra, ExcelErrorExtra::Resource { .. }),
            "{formula}: {e:?}"
        );
        assert_eq!(h.calls("COUNTING"), 0, "{formula}");
    }
}

#[test]
fn ordinary_value_failure_still_selects_fallback() {
    let h = Harness::new();
    assert_eq!(h.literal("=IFERROR(VALUE_ERR(),COUNTING())"), "7");
    assert_eq!(h.calls("COUNTING"), 1);
}

#[test]
fn fallback_live_failures_propagate() {
    for formula in [
        "=IFERROR(1/{0,4},CANCEL_ERR())",
        "=IFERROR(1/0,CANCEL_ERR())",
        "=IFNA({#N/A,1},CANCEL_ERR())",
    ] {
        let h = Harness::new();
        let e = h.eval(formula).expect_err(formula);
        assert_eq!(e.kind, ExcelErrorKind::Cancelled, "{formula}");
    }
    for formula in [
        "=IFERROR(1/{0,4},RESOURCE_ERR())",
        "=IFERROR(1/0,RESOURCE_ERR())",
    ] {
        let h = Harness::new();
        let e = h.eval(formula).expect_err(formula);
        assert!(
            matches!(e.extra, ExcelErrorExtra::Resource { .. }),
            "{formula}: {e:?}"
        );
    }
}

#[test]
fn cancellation_observed_during_array_scan_is_not_a_result() {
    for formula in [
        "=IFERROR(CANCEL_ARRAY(),COUNTING())",
        "=IFNA(CANCEL_ARRAY(),COUNTING())",
        "=IFERROR(CANCEL_WIDE(),COUNTING())",
    ] {
        let h = Harness::new();
        let e = h.eval(formula).expect_err(formula);
        assert_eq!(e.kind, ExcelErrorKind::Cancelled, "{formula}");
        assert_eq!(h.calls("COUNTING"), 0, "{formula}");
    }
}

#[test]
fn cancellation_observed_during_clean_range_probe_is_not_success() {
    for formula in ["=IFERROR(CANCEL_RANGE(),0)", "=IFNA(CANCEL_RANGE(),0)"] {
        let h = Harness::new();
        let e = h.eval(formula).expect_err(formula);
        assert_eq!(e.kind, ExcelErrorKind::Cancelled, "{formula}");
    }
}

#[test]
fn cancellation_observed_while_copying_is_not_a_result() {
    for formula in [
        "=IFERROR(1/{0,4},CANCEL_SEVEN())",
        "=IFERROR(ERROR_RANGE(),CANCEL_SEVEN())",
        "=IFNA(ERROR_RANGE(),CANCEL_SEVEN())",
    ] {
        let h = Harness::new();
        let e = h.eval(formula).expect_err(formula);
        assert_eq!(e.kind, ExcelErrorKind::Cancelled, "{formula}");
        assert_eq!(h.calls("CANCEL_SEVEN"), 1, "{formula}");
    }
}

#[test]
fn owned_range_values_are_selected_elementwise_without_cancellation() {
    let h = Harness::new();
    assert_eq!(h.literal("=IFERROR(ERROR_RANGE(),0)"), "[1;0;0]");
    assert_eq!(h.literal("=IFNA(ERROR_RANGE(),0)"), "[1;#REF!;0]");
    assert!(!h.token.is_cancelled());
}

/* ─────────────────────── engine cancel and retry ─────────────────────── */

/// The dependent `A3` puts a layer boundary after the guarded cell, where the
/// engine checks the request's cancellation token. (Which transient value a
/// cancelled vertex leaves before retry is engine-owned; this test only
/// requires that it is never the spreadsheet fallback.)
#[test]
fn engine_cancel_does_not_commit_fallback_and_retry_recomputes() {
    let calls = Arc::new(AtomicUsize::new(0));
    let wb = TestWorkbook::new().with_function(Arc::new(Probe {
        name: "CANCEL_ONCE",
        calls: calls.clone(),
        behavior: |ctx, call| {
            if call == 0 {
                cancel_ctx(ctx);
                return Err(ExcelError::new(ExcelErrorKind::Cancelled));
            }
            Ok(CalcValue::Scalar(LiteralValue::Array(vec![vec![
                err(ExcelErrorKind::Div),
                LiteralValue::Number(4.0),
            ]])))
        },
    }));
    let mut engine = Engine::new(wb, EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=IFERROR(CANCEL_ONCE(),-1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=A1+100").unwrap())
        .unwrap();

    let token = CancelToken::new();
    let outcome = engine.evaluate_all_cancellable(token.clone());
    assert_eq!(
        outcome.expect_err("cancelled request").kind,
        ExcelErrorKind::Cancelled
    );
    assert_ne!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(-1.0)),
        "a cancelled value must not be committed as the fallback"
    );

    engine.evaluate_all().unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "retry re-evaluates the guard"
    );
    assert_eq!(column(&engine, 1, 1..=1), "-1");
    assert_eq!(column(&engine, 2, 1..=1), "4");
    assert_eq!(column(&engine, 1, 3..=3), "99");
}

/* ─────────────────────── annotated host arrays ─────────────────────── */

/// Host functions may return an array carrying a format annotation
/// (`CalcValue::with_format`); it must be matched like any other array.
fn annotated_harness() -> (TestWorkbook, Arc<AtomicUsize>) {
    crate::builtins::load_builtins();
    let counter = Arc::new(AtomicUsize::new(0));
    let probes: [(&'static str, Behavior); 4] = [
        ("COUNTING", |_, _| {
            Ok(CalcValue::Scalar(LiteralValue::Int(7)))
        }),
        ("DATED_ERRORS", |_, _| {
            Ok(CalcValue::Scalar(LiteralValue::Array(vec![vec![
                err(ExcelErrorKind::Na),
                LiteralValue::Number(45000.0),
            ]]))
            .with_format(Some(crate::format::FormatId::DATE)))
        }),
        ("DATED_CLEAN", |_, _| {
            Ok(CalcValue::Scalar(LiteralValue::Array(vec![vec![
                LiteralValue::Number(1.0),
                LiteralValue::Number(2.0),
            ]]))
            .with_format(Some(crate::format::FormatId::DATE)))
        }),
        ("DATED_FALLBACK", |_, _| {
            Ok(CalcValue::Scalar(LiteralValue::Array(vec![vec![
                LiteralValue::Number(10.0),
                LiteralValue::Number(20.0),
            ]]))
            .with_format(Some(crate::format::FormatId::DATE)))
        }),
    ];
    let mut wb = TestWorkbook::new();
    for (name, behavior) in probes {
        let calls = if name == "COUNTING" {
            counter.clone()
        } else {
            Arc::new(AtomicUsize::new(0))
        };
        wb = wb.with_function(Arc::new(Probe {
            name,
            calls,
            behavior,
        }));
    }
    (wb, counter)
}

#[test]
fn annotated_array_values_are_matched_elementwise() {
    let (wb, counter) = annotated_harness();
    let interp = wb.interpreter();
    let eval = |f: &str| interp.evaluate_ast(&parse(f).unwrap()).unwrap();

    // The helper really produces an annotated array.
    assert!(matches!(
        eval("=DATED_ERRORS()"),
        CalcValue::AnnotatedScalar(LiteralValue::Array(_), _)
    ));
    assert_eq!(
        norm(&eval("=IFERROR(DATED_ERRORS(),0)").into_literal()),
        "[0,45000]"
    );
    assert_eq!(
        norm(&eval("=IFNA(DATED_ERRORS(),0)").into_literal()),
        "[0,45000]"
    );

    // Clean annotated array: returned as-is (annotation kept), fallback lazy.
    for f in [
        "=IFERROR(DATED_CLEAN(),COUNTING())",
        "=IFNA(DATED_CLEAN(),COUNTING())",
    ] {
        let out = eval(f);
        assert_eq!(out.format_id(), Some(crate::format::FormatId::DATE), "{f}");
        assert_eq!(norm(&out.into_literal()), "[1,2]", "{f}");
    }
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

#[test]
fn annotated_array_fallback_is_paired_not_nested() {
    let (wb, _) = annotated_harness();
    let interp = wb.interpreter();
    let eval = |f: &str| {
        norm(
            &interp
                .evaluate_ast(&parse(f).unwrap())
                .unwrap()
                .into_literal(),
        )
    };
    assert_eq!(eval("=IFERROR(1/{0,0},DATED_FALLBACK())"), "[10,20]");
    assert_eq!(
        eval("=IFERROR(DATED_ERRORS(),DATED_FALLBACK())"),
        "[10,45000]"
    );
    assert_eq!(eval("=IFERROR({#N/A;1},DATED_FALLBACK())"), "[10,20;1,1]");
}
