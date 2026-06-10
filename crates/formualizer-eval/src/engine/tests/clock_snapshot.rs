//! Per-recalc volatile clock snapshot (spec
//! `formualizer-cycle-semantics-spec.md` §7.11).
//!
//! Excel samples the clock ONCE per recalculation: `NOW()`/`TODAY()` agree
//! across every cell of one recalc — including all iteration passes of an
//! iterating SCC — and only advance on the NEXT recalc. These tests drive the
//! engine with a `TickingClock` that advances on every `now()` call, so any
//! per-call sampling (the old raw-`SystemClock` behaviour) is caught
//! deterministically.

use crate::engine::{CycleConfig, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use crate::timezone::{ClockProvider, TimeZoneSpec};
use chrono::{Duration, NaiveDateTime, TimeZone};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Adversarial clock: every `now()` call advances by `step`. If volatile
/// builtins sample per call instead of per recalc, two reads disagree.
#[derive(Debug)]
struct TickingClock {
    start: NaiveDateTime,
    step: Duration,
    calls: AtomicU64,
    timezone: TimeZoneSpec,
}

impl TickingClock {
    fn new(start: NaiveDateTime, step: Duration) -> Self {
        Self {
            start,
            step,
            calls: AtomicU64::new(0),
            timezone: TimeZoneSpec::Utc,
        }
    }
}

impl ClockProvider for TickingClock {
    fn timezone(&self) -> &TimeZoneSpec {
        &self.timezone
    }

    fn now(&self) -> NaiveDateTime {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) as i32;
        self.start + self.step * n
    }
}

fn start_instant() -> NaiveDateTime {
    chrono::Utc
        .with_ymd_and_hms(2025, 6, 1, 12, 0, 0)
        .single()
        .expect("valid timestamp")
        .naive_utc()
}

fn set_formula(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, f: &str) {
    engine
        .set_cell_formula(sheet, row, col, parse(f).expect("parse"))
        .expect("set formula");
}

fn num(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> f64 {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        other => panic!("expected number at {sheet} r{row}c{col}, got {other:?}"),
    }
}

/// Two NOW() cells in one acyclic recalc observe the same instant, even when
/// the underlying clock advances between calls (spec §7.11: one sample per
/// recalc). A second recalc observes a LATER instant (NOW stays volatile
/// across recalcs).
#[test]
fn now_agrees_across_cells_within_one_recalc_and_advances_across_recalcs() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    // Each now() call advances a full day, far above any rounding noise.
    engine.set_clock(Arc::new(TickingClock::new(
        start_instant(),
        Duration::days(1),
    )));

    set_formula(&mut engine, "Sheet1", 1, 1, "=NOW()");
    set_formula(&mut engine, "Sheet1", 2, 1, "=NOW()");
    engine.evaluate_all().unwrap();

    let a1_first = num(&engine, "Sheet1", 1, 1);
    let a2_first = num(&engine, "Sheet1", 2, 1);
    assert_eq!(
        a1_first, a2_first,
        "two NOW() cells in one recalc must observe the same clock sample"
    );

    // NOW() is volatile: the next recalc re-samples and must see a later time.
    engine.evaluate_all().unwrap();
    let a1_second = num(&engine, "Sheet1", 1, 1);
    assert!(
        a1_second > a1_first,
        "NOW() must advance across recalcs (got {a1_first} then {a1_second})"
    );
    assert_eq!(num(&engine, "Sheet1", 1, 1), num(&engine, "Sheet1", 2, 1));
}

/// NOW() inside an iterating SCC observes the same instant on every settle
/// pass of one recalc (spec §7.11: iteration passes reuse the sample).
///
/// `A1 = A1*0 + NOW()` is a direct self-reference: pass 1 computes NOW, pass
/// 2 recomputes and the convergence test compares the two passes. With the
/// clock stepping a full day per call (serial Δ = 1.0 ≫ max_change = 0.001),
/// per-pass sampling can never converge and the SCC caps out; a per-recalc
/// snapshot converges on pass 2 with Δ = 0.
#[test]
fn now_is_stable_across_iteration_passes_within_one_recalc() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_cycle(CycleConfig::iterate(100, 0.001)),
    );
    engine.set_clock(Arc::new(TickingClock::new(
        start_instant(),
        Duration::days(1),
    )));

    set_formula(&mut engine, "Sheet1", 1, 1, "=A1*0+NOW()");
    engine.evaluate_all().unwrap();

    let t = engine.last_cycle_telemetry();
    assert_eq!(
        t.iterated_sccs, 1,
        "self-referent NOW() cell must enter iterative calculation"
    );
    assert_eq!(
        t.converged_sccs, 1,
        "NOW() must be frozen within the recalc so the SCC converges \
         (telemetry: {t:?})"
    );
    assert_eq!(t.capped_sccs, 0, "no pass cap expected (telemetry: {t:?})");
    assert_eq!(
        t.max_abs_delta_at_stop, 0.0,
        "both passes must observe the identical clock sample"
    );

    let first = num(&engine, "Sheet1", 1, 1);

    // Iterating SCC members are redirtied volatile-like: the next recalc
    // re-samples the clock and must observe a strictly later NOW().
    engine.evaluate_all().unwrap();
    let t = engine.last_cycle_telemetry();
    assert_eq!(
        (t.iterated_sccs, t.converged_sccs, t.capped_sccs),
        (1, 1, 0)
    );
    let second = num(&engine, "Sheet1", 1, 1);
    assert!(
        second > first,
        "NOW() must advance across recalcs (got {first} then {second})"
    );
}

/// TODAY() shares the per-recalc sample with NOW() (both read the engine
/// clock snapshot): a midnight-straddling drift between two cells of one
/// recalc is impossible.
#[test]
fn today_and_now_share_one_sample_per_recalc() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    // Start just before midnight; one clock step crosses the date boundary.
    let start = chrono::Utc
        .with_ymd_and_hms(2025, 6, 1, 23, 59, 59)
        .single()
        .unwrap()
        .naive_utc();
    engine.set_clock(Arc::new(TickingClock::new(start, Duration::hours(1))));

    set_formula(&mut engine, "Sheet1", 1, 1, "=TODAY()");
    set_formula(&mut engine, "Sheet1", 2, 1, "=INT(NOW())");
    engine.evaluate_all().unwrap();

    assert_eq!(
        num(&engine, "Sheet1", 1, 1),
        num(&engine, "Sheet1", 2, 1),
        "TODAY() and INT(NOW()) must agree within one recalc"
    );
}
