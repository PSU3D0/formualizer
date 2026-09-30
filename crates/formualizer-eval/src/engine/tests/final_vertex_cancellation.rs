//! Live cancellation raised by the work of the last unit a request runs.
//!
//! A function that observes the request's cancellation token returns
//! `Err(Cancelled)`, which the evaluator turns into a `#CANCELLED` value.
//! The engine used to check the token only before units, before layers and
//! every 256 vertices, so when the cancelling vertex was the last unit of a
//! request the request returned `Ok`, the cell kept `#CANCELLED`, and a
//! retry did not recompute it (the vertex had been committed clean).
//!
//! Contract pinned here: work evaluated across a live cancellation is not
//! committed; the request reports `Cancelled` with its dirty state retained,
//! and a retry recomputes. A `#CANCELLED` value produced with no live
//! cancellation is ordinary data. All seams are counter UDFs that flip the
//! context token on a chosen call; there are no timers or threads.

use crate::engine::{
    CancelToken, CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig,
    EvaluationRequestOutcome, EvaluationTarget, FormulaDirtyLeaseOutcome, TargetEvalOptions,
};
use crate::function::{FnCaps, Function};
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const NEVER: usize = usize::MAX;

/// What the probe returns on the call that cancels the request.
#[derive(Clone, Copy, Debug)]
enum OnCancel {
    /// `Err(Cancelled)`: the function observed the token.
    Err,
    /// An ordinary scalar computed across the signal.
    Value,
    /// An ordinary array (a spill) computed across the signal.
    Array,
}

/// `PROBE()`: counts calls; on call `cancel_at` it cancels the request's
/// token and returns per `on_cancel`, otherwise 10 (or `{1;2;3}` with
/// `array`).
#[derive(Debug)]
struct Probe {
    calls: Arc<AtomicUsize>,
    cancel_at: Arc<AtomicUsize>,
    on_cancel: OnCancel,
    array: bool,
}

fn column3() -> LiteralValue {
    LiteralValue::Array(vec![
        vec![LiteralValue::Number(1.0)],
        vec![LiteralValue::Number(2.0)],
        vec![LiteralValue::Number(3.0)],
    ])
}

impl Function for Probe {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "PROBE"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == self.cancel_at.load(Ordering::SeqCst) {
            ctx.cancellation_token()
                .expect("a cancellable request exposes its token")
                .cancel();
            return match self.on_cancel {
                OnCancel::Err => Err(ExcelError::new(ExcelErrorKind::Cancelled)),
                OnCancel::Value => Ok(CalcValue::Scalar(LiteralValue::Number(10.0))),
                OnCancel::Array => Ok(CalcValue::Scalar(column3())),
            };
        }
        Ok(CalcValue::Scalar(if self.array {
            column3()
        } else {
            LiteralValue::Number(10.0)
        }))
    }
}

/// `STORED_CANCELLED()`: returns a `#CANCELLED` value without cancelling.
#[derive(Debug)]
struct StoredCancelled {
    calls: Arc<AtomicUsize>,
}

impl Function for StoredCancelled {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "STORED_CANCELLED"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new(
            ExcelErrorKind::Cancelled,
        ))))
    }
}

struct Harness {
    engine: Engine<TestWorkbook>,
    calls: Arc<AtomicUsize>,
    cancel_at: Arc<AtomicUsize>,
}

impl Harness {
    fn new(config: EvalConfig, cancel_at: usize, on_cancel: OnCancel, array: bool) -> Self {
        crate::builtins::load_builtins();
        let calls = Arc::new(AtomicUsize::new(0));
        let cancel_at = Arc::new(AtomicUsize::new(cancel_at));
        let wb = TestWorkbook::new().with_function(Arc::new(Probe {
            calls: calls.clone(),
            cancel_at: cancel_at.clone(),
            on_cancel,
            array,
        }));
        Self {
            engine: Engine::new(wb, config),
            calls,
            cancel_at,
        }
    }

    fn sequential(cancel_at: usize, on_cancel: OnCancel) -> Self {
        Self::new(sequential_config(), cancel_at, on_cancel, false)
    }

    fn formula(&mut self, row: u32, col: u32, src: &str) {
        self.engine
            .set_cell_formula("Sheet1", row, col, parse(src).unwrap())
            .unwrap();
    }

    fn value(&mut self, row: u32, col: u32, value: f64) {
        self.engine
            .set_cell_value("Sheet1", row, col, LiteralValue::Number(value))
            .unwrap();
    }

    fn get(&self, row: u32, col: u32) -> Option<LiteralValue> {
        match self.engine.get_cell_value("Sheet1", row, col) {
            Some(LiteralValue::Int(i)) => Some(LiteralValue::Number(i as f64)),
            Some(LiteralValue::Empty) | None => None,
            other => other,
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// The last request reported `Cancelled` and kept its dirty lease.
    fn assert_cancelled(&self, result: Result<crate::engine::EvalResult, ExcelError>) {
        let error = result.expect_err("live cancellation must fail the request");
        assert_eq!(error.kind, ExcelErrorKind::Cancelled, "{error:?}");
        let stats = self
            .engine
            .last_evaluation_resource_request_stats()
            .expect("request stats");
        assert_eq!(stats.outcome, EvaluationRequestOutcome::Cancelled);
        assert_ne!(stats.dirty_lease, FormulaDirtyLeaseOutcome::Acknowledged);
    }

    /// The cell holds no out-of-band `#CANCELLED`.
    fn assert_not_cancelled_value(&self, row: u32, col: u32) {
        assert!(
            !matches!(
                self.get(row, col),
                Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Cancelled
            ),
            "R{row}C{col} published a live cancellation as a value: {:?}",
            self.get(row, col)
        );
    }
}

fn sequential_config() -> EvalConfig {
    EvalConfig {
        enable_parallel: false,
        ..Default::default()
    }
}

fn parallel_config() -> EvalConfig {
    EvalConfig {
        enable_parallel: true,
        max_threads: Some(2),
        ..Default::default()
    }
}

fn n(x: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(x))
}

/* ───────────────────────── singleton final vertex ───────────────────────── */

#[test]
fn singleton_final_vertex_err_cancelled_fails_request_and_retry_recomputes() {
    let mut h = Harness::sequential(0, OnCancel::Err);
    h.formula(1, 1, "=PROBE()");

    let result = h.engine.evaluate_all_cancellable(CancelToken::new());
    h.assert_cancelled(result);
    assert_eq!(h.calls(), 1);
    assert_eq!(h.get(1, 1), None, "the cancelled vertex is not committed");

    // Nothing else changed: a plain retry must recompute the vertex.
    h.engine.evaluate_all().unwrap();
    assert_eq!(h.calls(), 2, "retry re-evaluates the cancelled vertex");
    assert_eq!(h.get(1, 1), n(10.0));
}

/// The FORM203 reproduction: IFERROR now propagates the live cancellation
/// of its value argument; the engine must not publish it.
#[test]
fn singleton_final_iferror_over_cancelled_value_retries() {
    let mut h = Harness::sequential(0, OnCancel::Err);
    h.formula(1, 1, "=IFERROR(PROBE(),-1)");

    let result = h.engine.evaluate_all_cancellable(CancelToken::new());
    h.assert_cancelled(result);
    h.assert_not_cancelled_value(1, 1);
    assert_ne!(h.get(1, 1), n(-1.0), "the fallback is not committed");

    h.engine.evaluate_all().unwrap();
    assert_eq!(h.calls(), 2);
    assert_eq!(h.get(1, 1), n(10.0));
}

/// A result computed across the signal is not a finished result either,
/// even when it is an ordinary value.
#[test]
fn singleton_final_value_computed_across_cancellation_is_not_committed() {
    let mut h = Harness::sequential(0, OnCancel::Value);
    h.formula(1, 1, "=PROBE()+1");

    let result = h.engine.evaluate_all_cancellable(CancelToken::new());
    h.assert_cancelled(result);
    assert_eq!(h.get(1, 1), None);

    h.engine.evaluate_all().unwrap();
    assert_eq!(h.calls(), 2);
    assert_eq!(h.get(1, 1), n(11.0));
}

/// A `#CANCELLED` value with no live cancellation is ordinary data: it
/// commits, the request succeeds and the cell is clean.
#[test]
fn stored_cancelled_literal_without_live_cancellation_commits() {
    crate::builtins::load_builtins();
    let calls = Arc::new(AtomicUsize::new(0));
    let wb = TestWorkbook::new().with_function(Arc::new(StoredCancelled {
        calls: calls.clone(),
    }));
    let mut engine = Engine::new(wb, sequential_config());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=STORED_CANCELLED()").unwrap())
        .unwrap();

    let token = CancelToken::new();
    engine.evaluate_all_cancellable(token.clone()).unwrap();
    assert!(!token.is_cancelled());
    assert!(matches!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Cancelled
    ));
    assert_eq!(
        engine
            .last_evaluation_resource_request_stats()
            .unwrap()
            .outcome,
        EvaluationRequestOutcome::Success
    );
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the stored value is clean");
}

/* ───────────────────────── final vertex within a layer ─────────────────── */

/// Four independent cells in one layer (distinct formulas, so no family
/// run); the third cancels. No later layer exists. The cancelling cell and
/// the one after it are not committed, and the failed pass leaves every
/// scheduled vertex dirty (`freshness_abort_pass`), so the retry recomputes
/// the whole layer.
#[test]
fn final_layer_in_layer_cancellation_fails_request_and_retry_recomputes() {
    let mut h = Harness::sequential(2, OnCancel::Err);
    for row in 1..=4 {
        h.formula(row, 1, &format!("=PROBE()+{row}"));
    }

    let result = h.engine.evaluate_all_cancellable(CancelToken::new());
    h.assert_cancelled(result);
    assert_eq!(h.calls(), 3, "the unit after the cancelling one never runs");
    h.assert_not_cancelled_value(3, 1);
    assert_eq!(h.get(3, 1), None);
    assert_eq!(h.get(4, 1), None);

    h.engine.evaluate_all().unwrap();
    assert_eq!(h.calls(), 7, "retry recomputes every scheduled vertex");
    for row in 1..=4 {
        assert_eq!(h.get(row, 1), n(10.0 + row as f64));
    }
}

/// The cancelling cell is the last vertex of a multi-layer request.
#[test]
fn final_dependent_layer_cancellation_retries() {
    let mut h = Harness::sequential(NEVER, OnCancel::Err);
    h.value(1, 1, 5.0);
    h.formula(1, 2, "=A1*2");
    h.formula(1, 3, "=B1+PROBE()");
    h.engine.evaluate_all().unwrap();
    assert_eq!(h.get(1, 3), n(20.0));

    h.value(1, 1, 6.0);
    h.cancel_at.store(1, Ordering::SeqCst);
    let result = h.engine.evaluate_all_cancellable(CancelToken::new());
    h.assert_cancelled(result);
    assert_eq!(
        h.get(1, 3),
        n(20.0),
        "the previous value is not overwritten"
    );

    h.engine.evaluate_all().unwrap();
    assert_eq!(h.calls(), 3);
    assert_eq!(h.get(1, 2), n(12.0));
    assert_eq!(h.get(1, 3), n(22.0));
}

/* ───────────────────────── spill ───────────────────────── */

#[test]
fn final_spill_computed_across_cancellation_is_not_committed() {
    let mut h = Harness::new(sequential_config(), 0, OnCancel::Array, true);
    h.formula(1, 1, "=PROBE()");

    let result = h.engine.evaluate_all_cancellable(CancelToken::new());
    h.assert_cancelled(result);
    for row in 1..=3 {
        assert_eq!(h.get(row, 1), None, "no spill cell is projected");
    }

    h.engine.evaluate_all().unwrap();
    assert_eq!(h.calls(), 2);
    for row in 1..=3 {
        assert_eq!(h.get(row, 1), n(row as f64));
    }
}

/* ───────────────────────── dynamic reference ───────────────────────── */

#[test]
fn final_dynamic_reader_cancellation_retries() {
    // Measure the calls a dynamic reader makes in one request (the dynamic
    // pre-probe may evaluate it too), then cancel on the last of them.
    let setup = |h: &mut Harness| {
        h.value(1, 2, 7.0);
        h.formula(1, 1, "=PROBE()+INDIRECT(\"B1\")");
    };
    let mut dry = Harness::sequential(NEVER, OnCancel::Err);
    setup(&mut dry);
    dry.engine
        .evaluate_all_cancellable(CancelToken::new())
        .unwrap();
    let per_request = dry.calls();
    assert!(per_request >= 1);
    assert_eq!(dry.get(1, 1), n(17.0));

    let mut h = Harness::sequential(per_request - 1, OnCancel::Err);
    setup(&mut h);
    let result = h.engine.evaluate_all_cancellable(CancelToken::new());
    h.assert_cancelled(result);
    h.assert_not_cancelled_value(1, 1);

    h.engine.evaluate_all().unwrap();
    assert_eq!(h.get(1, 1), n(17.0), "retry recomputes the dynamic reader");
}

/* ───────────────────────── parallel layer / family run ─────────────────── */

/// A parallel group is one commit unit: a live cancellation raised by any
/// member keeps the whole group uncommitted. The probe cancels on the last
/// of the 32 calls, so every task has already passed its pre-task check.
#[test]
fn final_parallel_layer_cancellation_commits_nothing_and_retries() {
    let mut h = Harness::new(parallel_config(), 31, OnCancel::Err, false);
    for row in 1..=32 {
        h.formula(row, 1, &format!("=PROBE()+{row}"));
    }

    let result = h.engine.evaluate_all_cancellable(CancelToken::new());
    h.assert_cancelled(result);
    assert_eq!(h.calls(), 32);
    for row in 1..=32 {
        assert_eq!(h.get(row, 1), None, "row {row}: the group is not committed");
    }

    h.engine.evaluate_all().unwrap();
    for row in 1..=32 {
        assert_eq!(h.get(row, 1), n(10.0 + row as f64));
    }
}

fn family_rows(h: &mut Harness, rows: u32) {
    for row in 1..=rows {
        h.value(row, 1, row as f64);
        h.formula(row, 2, &format!("=A{row}*2+PROBE()"));
    }
}

#[test]
fn final_family_run_member_cancellation_retries() {
    for parallel in [false, true] {
        const ROWS: u32 = 16;
        let config = || EvalConfig {
            family_execution: true,
            enable_parallel: parallel,
            max_threads: Some(2),
            ..super::common::arrow_eval_config()
        };
        // Cancel on the last member evaluated.
        let mut dry = Harness::new(config(), NEVER, OnCancel::Err, false);
        family_rows(&mut dry, ROWS);
        dry.engine
            .evaluate_all_cancellable(CancelToken::new())
            .unwrap();
        let per_request = dry.calls();
        assert!(per_request >= 1);

        let mut h = Harness::new(config(), per_request - 1, OnCancel::Err, false);
        family_rows(&mut h, ROWS);
        let result = h.engine.evaluate_all_cancellable(CancelToken::new());
        h.assert_cancelled(result);
        for row in 1..=ROWS {
            h.assert_not_cancelled_value(row, 2);
        }

        h.engine.evaluate_all().unwrap();
        for row in 1..=ROWS {
            assert_eq!(
                h.get(row, 2),
                n(row as f64 * 2.0 + 10.0),
                "parallel={parallel} row={row}"
            );
        }
    }
}

/* ───────────────────────── runtime SCC ───────────────────────── */

/// A phantom SCC (static cycle, live-acyclic) is the request's last unit;
/// its member cancels. The task must not complete as a success.
#[test]
fn final_runtime_scc_member_cancellation_retries() {
    let config = EvalConfig::default().with_cycle(CycleConfig {
        detection: CycleDetection::Runtime,
        policy: CyclePolicy::Error,
    });
    let mut h = Harness::new(config, 0, OnCancel::Err, false);
    h.engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Boolean(true))
        .unwrap();
    h.formula(2, 1, "=IF(A1,PROBE(),A3)");
    h.formula(3, 1, "=IF(A1,A2,999)");

    let result = h.engine.evaluate_all_cancellable(CancelToken::new());
    h.assert_cancelled(result);

    h.engine.evaluate_all().unwrap();
    assert_eq!(h.get(2, 1), n(10.0));
    assert_eq!(h.get(3, 1), n(10.0));
}

/* ───────────────────────── targeted public paths ───────────────────────── */

#[test]
fn final_target_vertex_cancellation_via_evaluate_until_cancellable() {
    let mut h = Harness::sequential(0, OnCancel::Err);
    h.formula(1, 1, "=PROBE()");

    let result = h
        .engine
        .evaluate_until_cancellable(&["A1"], CancelToken::new());
    h.assert_cancelled(result);
    assert_eq!(h.get(1, 1), None);

    h.engine.evaluate_all().unwrap();
    assert_eq!(h.calls(), 2);
    assert_eq!(h.get(1, 1), n(10.0));
}

#[test]
fn final_target_vertex_cancellation_via_target_options() {
    let mut h = Harness::sequential(0, OnCancel::Err);
    h.formula(1, 1, "=PROBE()");
    let target = EvaluationTarget::Cell {
        sheet: "Sheet1".to_string(),
        row: 1,
        col: 1,
    };

    let result = h.engine.evaluate_targets_with_options(
        std::slice::from_ref(&target),
        TargetEvalOptions {
            cancel: Some(CancelToken::new()),
            ..Default::default()
        },
    );
    h.assert_cancelled(result);
    assert_eq!(h.get(1, 1), None);

    h.engine
        .evaluate_targets_with_options(&[target], TargetEvalOptions::default())
        .unwrap();
    assert_eq!(h.calls(), 2);
    assert_eq!(h.get(1, 1), n(10.0));
}

#[test]
fn final_vertex_cancellation_via_recalc_plan_controls() {
    let mut h = Harness::sequential(0, OnCancel::Err);
    h.formula(1, 1, "=PROBE()");
    let plan = h.engine.build_recalc_plan().unwrap();

    let result = h
        .engine
        .evaluate_recalc_plan_with_controls(&plan, Some(CancelToken::new()), None);
    h.assert_cancelled(result);
    assert_eq!(h.get(1, 1), None);

    h.engine.evaluate_all().unwrap();
    assert_eq!(h.calls(), 2);
    assert_eq!(h.get(1, 1), n(10.0));
}
