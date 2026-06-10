//! Stage 3 — `CyclePolicy::Iterate`, Excel-style iterative calculation
//! (RFC #113; contract spec `formualizer-cycle-semantics-spec.md` §2 config,
//! §3.5 procedure, §4 seeding/persistence, §6 convergence, §7 cases).
//!
//! The §6 convergence matrix itself is unit-tested in
//! `engine::convergence`; this file covers the workbook-level behaviors:
//! pass counting, capping, persistence + the volatile-like redirty that
//! re-fires iterating SCCs every recalc, telemetry, replan interaction,
//! cancellation, and determinism.

use crate::engine::{CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn iterate_cfg(max_iterations: u32, max_change: f64) -> EvalConfig {
    EvalConfig::default().with_cycle(CycleConfig::iterate(max_iterations, max_change))
}

fn iterate_engine(max_iterations: u32, max_change: f64) -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), iterate_cfg(max_iterations, max_change))
}

fn set_formula(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, f: &str) {
    engine
        .set_cell_formula(sheet, row, col, parse(f).expect("parse"))
        .expect("set formula");
}

fn set_value(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, v: LiteralValue) {
    engine
        .set_cell_value(sheet, row, col, v)
        .expect("set value");
}

fn num(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> f64 {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet} r{row}c{col}, got {other:?}"),
    }
}

fn err_kind(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> ExcelErrorKind {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Error(e)) => e.kind,
        other => panic!("expected error at {sheet} r{row}c{col}, got {other:?}"),
    }
}

/* ───────────────────────── §2 config validation ──────────────────────── */

#[test]
fn cycle_config_validation_rules() {
    // Valid combos.
    assert!(CycleConfig::iterate(1, 0.0).validate().is_ok());
    assert!(CycleConfig::iterate_excel_defaults().validate().is_ok());
    assert_eq!(
        CyclePolicy::iterate_excel_defaults(),
        CyclePolicy::Iterate {
            max_iterations: 100,
            max_change: 0.001
        }
    );
    // Error policy is unconstrained.
    assert!(
        CycleConfig {
            detection: CycleDetection::Static,
            policy: CyclePolicy::Error,
        }
        .validate()
        .is_ok()
    );

    // max_iterations == 0.
    assert!(CycleConfig::iterate(0, 0.001).validate().is_err());
    // max_change negative / non-finite.
    assert!(CycleConfig::iterate(100, -0.001).validate().is_err());
    assert!(CycleConfig::iterate(100, f64::NAN).validate().is_err());
    assert!(CycleConfig::iterate(100, f64::INFINITY).validate().is_err());
    assert!(
        CycleConfig::iterate(100, f64::NEG_INFINITY)
            .validate()
            .is_err()
    );
    // Iterate with Static detection.
    assert!(
        CycleConfig {
            detection: CycleDetection::Static,
            policy: CyclePolicy::iterate_excel_defaults(),
        }
        .validate()
        .is_err()
    );
}

#[test]
#[should_panic(expected = "invalid CycleConfig")]
fn with_cycle_rejects_zero_iterations_at_build() {
    let _ = EvalConfig::default().with_cycle(CycleConfig::iterate(0, 0.001));
}

#[test]
#[should_panic(expected = "invalid CycleConfig")]
fn with_cycle_rejects_negative_max_change_at_build() {
    let _ = EvalConfig::default().with_cycle(CycleConfig::iterate(100, -1.0));
}

#[test]
#[should_panic(expected = "invalid CycleConfig")]
fn with_cycle_rejects_iterate_under_static_detection() {
    let _ = EvalConfig::default().with_cycle(CycleConfig {
        detection: CycleDetection::Static,
        policy: CyclePolicy::iterate_excel_defaults(),
    });
}

#[test]
#[should_panic(expected = "invalid CycleConfig")]
fn engine_new_revalidates_struct_literal_configs() {
    // Bypasses `with_cycle`; `Engine::new` must still reject it.
    let cfg = EvalConfig {
        cycle: CycleConfig::iterate(0, 0.001),
        ..EvalConfig::default()
    };
    let _ = Engine::new(TestWorkbook::new(), cfg);
}

/* ────────────── ingest: self-dependencies are Iterate-only ───────────── */

#[test]
fn self_reference_ingest_is_accepted_only_under_iterate() {
    // Iterate: `=A1+1` in A1 and a dense range covering its own cell are
    // accepted (Excel parity with iterative calculation on).
    let mut engine = iterate_engine(100, 0.001);
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=A1+1").unwrap())
        .expect("self-reference accepted under Iterate");
    engine
        .set_cell_formula("Sheet1", 5, 1, parse("=SUM(A2:A10)").unwrap())
        .expect("dense self-covering range accepted under Iterate");

    // Runtime + Error policy: edit-time rejection stands.
    let cfg = EvalConfig::default().with_cycle(CycleConfig {
        detection: CycleDetection::Runtime,
        policy: CyclePolicy::Error,
    });
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    let err = engine
        .set_cell_formula("Sheet1", 1, 1, parse("=A1+1").unwrap())
        .unwrap_err();
    assert_eq!(err.kind, ExcelErrorKind::Circ);
}

/* ───────────────────────── §7.1 self-reference ───────────────────────── */

#[test]
fn self_reference_caps_at_max_iterations_with_exact_value() {
    // `=A1+1` increments once per pass from the Empty→0 seed; the SCC never
    // converges and stops at the cap: value after one recalc == cap.
    let mut engine = iterate_engine(7, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=A1+1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 7.0);

    let t = engine.last_cycle_telemetry();
    assert_eq!(t.static_sccs, 1);
    assert_eq!(t.phantom_sccs, 0);
    assert_eq!(t.live_cycles_witnessed, 1);
    assert_eq!(t.circ_cells_stamped, 0);
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 0);
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.settle_passes_total, 7);
    assert_eq!(t.max_passes_single_scc, 7);
    // Final-round residual: each pass adds exactly 1.
    assert_eq!(t.max_abs_delta_at_stop, 1.0);
    assert_eq!(t.nan_converged, 0);

    // §4 persistence + per-recalc re-fire: the next recalc (no edits at
    // all) seeds from 7 and adds the cap again.
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 14.0);
    assert_eq!(engine.last_cycle_telemetry().capped_sccs, 1);
}

#[test]
fn self_reference_small_caps_verified_exactly() {
    for cap in [1u32, 2, 3, 5] {
        let mut engine = iterate_engine(cap, 0.001);
        set_formula(&mut engine, "Sheet1", 1, 1, "=A1+1");
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 1, 1), cap as f64, "cap {cap}");
        assert_eq!(
            engine.last_cycle_telemetry().settle_passes_total,
            cap as usize
        );
    }
}

/* ───────────── §7.4 arithmetic routing: converging pair ──────────────── */

#[test]
fn arithmetic_routing_converges_to_the_closed_form_fixed_point() {
    // B1 = g·X + (1−g)·C1, C1 = g·B1 + (1−g)·Y with g = 0.5, X = 10, Y = 20.
    // Fixed point: b = 5 + 0.5c, c = 0.5b + 10 ⟹ b = 40/3, c = 50/3.
    // Gauss–Seidel contraction L = g(1−g) = 0.25; at stop the per-pass
    // delta is < max_change, so |value − fixed point| ≤ L/(1−L)·max_change
    // < max_change.
    let max_change = 0.001;
    let mut engine = iterate_engine(100, max_change);
    set_formula(&mut engine, "Sheet1", 1, 2, "=0.5*10+0.5*C1"); // B1
    set_formula(&mut engine, "Sheet1", 1, 3, "=0.5*B1+0.5*20"); // C1
    engine.evaluate_all().unwrap();

    let b = num(&engine, "Sheet1", 1, 2);
    let c = num(&engine, "Sheet1", 1, 3);
    assert!((b - 40.0 / 3.0).abs() < max_change, "B1 = {b}");
    assert!((c - 50.0 / 3.0).abs() < max_change, "C1 = {c}");

    let t = engine.last_cycle_telemetry();
    assert_eq!(t.static_sccs, 1);
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(t.capped_sccs, 0);
    assert_eq!(t.live_cycles_witnessed, 1);
    assert!(
        t.max_abs_delta_at_stop < max_change && t.max_abs_delta_at_stop > 0.0,
        "converged residual must be the sub-threshold final round, got {}",
        t.max_abs_delta_at_stop
    );
    assert!(
        t.settle_passes_total > 2 && t.settle_passes_total < 100,
        "got {} passes",
        t.settle_passes_total
    );
}

/* ───────────────────── §7.5 genuinely divergent pair ─────────────────── */

#[test]
fn divergent_pair_caps_with_deterministic_values() {
    // A1 = A2+1, A2 = A1+1 in member order (A1 first): pass k commits
    // A1 = 2k−1, A2 = 2k. Never converges; capped; NOT an error.
    let mut engine = iterate_engine(10, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=A2+1");
    set_formula(&mut engine, "Sheet1", 2, 1, "=A1+1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 19.0);
    assert_eq!(num(&engine, "Sheet1", 2, 1), 20.0);

    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 0);
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.circ_cells_stamped, 0, "capping is not an error");
    assert_eq!(t.settle_passes_total, 10);
    // Final round: both members moved by 2.
    assert_eq!(t.max_abs_delta_at_stop, 2.0);
}

/* ──────────── §7.6 accumulator (`max_iterations: 1`, §4 seed) ─────────── */

#[test]
fn accumulator_adds_input_exactly_once_per_recalc() {
    // B1 = B1 + A1 with A1 = 5: three recalcs from a fresh workbook → 15.
    // No convergence test runs (`max_iterations: 1` ⇒ exactly one pass).
    let mut engine = iterate_engine(1, 0.001);
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Number(5.0)); // A1
    set_formula(&mut engine, "Sheet1", 1, 2, "=B1+A1"); // B1

    for (recalc, expected) in [(1u32, 5.0), (2, 10.0), (3, 15.0)] {
        engine.evaluate_all().unwrap();
        assert_eq!(
            num(&engine, "Sheet1", 1, 2),
            expected,
            "after recalc {recalc}"
        );
        let t = engine.last_cycle_telemetry();
        assert_eq!(t.static_sccs, 1, "recalc {recalc} must re-fire the SCC");
        assert_eq!(t.iterated_sccs, 1);
        assert_eq!(t.converged_sccs, 0);
        assert_eq!(t.capped_sccs, 1, "stopping at the pass budget is a cap");
        assert_eq!(t.settle_passes_total, 1, "exactly one pass per recalc");
        assert_eq!(
            t.max_abs_delta_at_stop, 0.0,
            "no convergence comparison ever ran"
        );
    }
}

#[test]
fn accumulator_member_evaluates_exactly_max_iterations_times_per_recalc() {
    use crate::args::ArgSchema;
    use crate::function::{FnCaps, Function};
    use crate::traits::{ArgumentHandle, FunctionContext};

    /// Returns 0 and counts invocations — a direct probe of the
    /// pass-counting contract.
    #[derive(Debug)]
    struct CountFn(Arc<AtomicUsize>);
    impl Function for CountFn {
        fn caps(&self) -> FnCaps {
            FnCaps::empty()
        }
        fn name(&self) -> &'static str {
            "COUNTEVALS"
        }
        fn arg_schema(&self) -> &'static [ArgSchema] {
            &[]
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
        }
    }

    for cap in [1usize, 3] {
        let count = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new().with_function(Arc::new(CountFn(count.clone())));
        let mut engine = Engine::new(wb, iterate_cfg(cap as u32, 0.001));
        set_formula(&mut engine, "Sheet1", 1, 1, "=A1+1+COUNTEVALS()");
        engine.evaluate_all().unwrap();
        assert_eq!(
            count.load(Ordering::Relaxed),
            cap,
            "cap {cap}: one evaluation per pass"
        );
        engine.evaluate_all().unwrap();
        assert_eq!(
            count.load(Ordering::Relaxed),
            2 * cap,
            "cap {cap}: same again on the next recalc"
        );
    }
}

#[test]
fn accumulator_works_through_the_demand_path_too() {
    // evaluate_cell shares the unit walk + end-of-recalc redirty.
    let mut engine = iterate_engine(1, 0.001);
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Number(5.0));
    set_formula(&mut engine, "Sheet1", 1, 2, "=B1+A1");
    for expected in [5.0, 10.0, 15.0] {
        engine.evaluate_cell("Sheet1", 1, 2).unwrap();
        assert_eq!(num(&engine, "Sheet1", 1, 2), expected);
    }
}

#[test]
fn iterating_scc_changes_propagate_downstream_each_recalc() {
    // D1 reads the accumulator from outside the SCC: the volatile-like
    // redirty must propagate to dependents so downstream stays fresh.
    let mut engine = iterate_engine(1, 0.001);
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Number(5.0));
    set_formula(&mut engine, "Sheet1", 1, 2, "=B1+A1");
    set_formula(&mut engine, "Sheet1", 1, 4, "=B1*2");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 4), 10.0);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 4), 20.0);
}

#[test]
fn breaking_the_cycle_stops_the_per_recalc_redirty() {
    // Overwrite the accumulator with a literal: the SCC dissolves, values
    // freeze, and no work is scheduled on later recalcs (the redirty chain
    // is per-recalc and self-healing).
    let mut engine = iterate_engine(1, 0.001);
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Number(5.0));
    set_formula(&mut engine, "Sheet1", 1, 2, "=B1+A1");
    engine.evaluate_all().unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 2), 10.0);

    set_value(&mut engine, "Sheet1", 1, 2, LiteralValue::Number(42.0));
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 2), 42.0);
    assert_eq!(engine.last_cycle_telemetry().static_sccs, 0);

    // And the recalc after that schedules nothing cycle-related at all.
    let res = engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().static_sccs, 0);
    assert_eq!(res.computed_vertices, 0, "no perpetual redirty leak");
    assert_eq!(num(&engine, "Sheet1", 1, 2), 42.0);
}

/* ───────────── §7.7 self-referential timestamp + §7.11 volatiles ──────── */

fn deterministic_iterate_cfg(max_iterations: u32, max_change: f64) -> EvalConfig {
    use crate::engine::DeterministicMode;
    use crate::timezone::TimeZoneSpec;
    let mut cfg = iterate_cfg(max_iterations, max_change);
    cfg.deterministic_mode = DeterministicMode::Enabled {
        timestamp_utc: chrono::DateTime::parse_from_rfc3339("2026-06-09T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        timezone: TimeZoneSpec::Utc,
    };
    cfg
}

#[test]
fn timestamp_pattern_stamps_once_and_is_preserved() {
    // B1 = IF(A1="", "", IF(B1="", NOW(), B1)) with a fixed clock.
    let mut engine = Engine::new(TestWorkbook::new(), deterministic_iterate_cfg(100, 0.001));
    set_formula(
        &mut engine,
        "Sheet1",
        1,
        2,
        "=IF(A1=\"\",\"\",IF(B1=\"\",NOW(),B1))",
    );
    engine.evaluate_all().unwrap();
    // A1 empty: the guarded branch returns "" without reading B1 — phantom.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Text(String::new()))
    );
    assert_eq!(engine.last_cycle_telemetry().iterated_sccs, 0);

    // A1 becomes non-empty: NOW() stamps once; the second pass re-reads the
    // stamp through the taken branch and converges (Δ = 0 — the volatile
    // sample is per-recalc stable, §7.11).
    set_value(
        &mut engine,
        "Sheet1",
        1,
        1,
        LiteralValue::Text("x".to_string()),
    );
    engine.evaluate_all().unwrap();
    let stamped = num(&engine, "Sheet1", 1, 2);
    assert!(stamped > 40000.0, "expected a date serial, got {stamped}");
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(t.capped_sccs, 0);

    // Later recalcs preserve the stamp (B1 reads its own previous value).
    for _ in 0..2 {
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 1, 2), stamped);
        assert_eq!(engine.last_cycle_telemetry().converged_sccs, 1);
    }
}

#[test]
fn volatile_inside_cycle_is_pass_stable_and_redirties_next_recalc() {
    // B1 = NOW() + 0·C1, C1 = B1: NOW() must read identically in every pass
    // of one recalc (fixed clock ⇒ converges with Δ = 0 on pass 2), and the
    // volatile member re-fires the SCC on the next recalc.
    let mut engine = Engine::new(TestWorkbook::new(), deterministic_iterate_cfg(100, 0.001));
    set_formula(&mut engine, "Sheet1", 1, 2, "=NOW()+0*C1");
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1");
    engine.evaluate_all().unwrap();
    let stamped = num(&engine, "Sheet1", 1, 2);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(
        t.settle_passes_total, 2,
        "pass 2 must observe the same NOW() sample and converge immediately"
    );
    assert_eq!(t.max_abs_delta_at_stop, 0.0);

    // Volatile redirty: the SCC re-evaluates on the next recalc (and the
    // fixed clock keeps the value identical).
    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().iterated_sccs, 1);
    assert_eq!(num(&engine, "Sheet1", 1, 2), stamped);
    assert_eq!(num(&engine, "Sheet1", 1, 3), stamped);
}

/* ─────────────────────── §7.8 range self-inclusion ───────────────────── */

#[test]
fn range_self_inclusion_grows_per_pass_and_caps() {
    // B2 = SUM(B1:B3) with B1 = 1, B3 = 2: each pass re-adds the previous
    // total, so the value grows by 3 per pass and never converges.
    let mut engine = iterate_engine(5, 0.001);
    set_value(&mut engine, "Sheet1", 1, 2, LiteralValue::Number(1.0));
    set_value(&mut engine, "Sheet1", 3, 2, LiteralValue::Number(2.0));
    set_formula(&mut engine, "Sheet1", 2, 2, "=SUM(B1:B3)");
    engine.evaluate_all().unwrap();
    // p1: 3, p2: 6, p3: 9, p4: 12, p5: 15.
    assert_eq!(num(&engine, "Sheet1", 2, 2), 15.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.settle_passes_total, 5);
    assert_eq!(t.max_abs_delta_at_stop, 3.0);

    // Persistence: the next recalc keeps growing from 15.
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 2), 30.0);
}

#[test]
fn whole_column_self_inclusion_iterates_via_the_stripe_path() {
    // `=SUM(B:B)` in B1 (single-vertex SCC from whole-axis self-inclusion,
    // #129): grows by the column total each pass, caps.
    let mut engine = iterate_engine(4, 0.001);
    set_value(&mut engine, "Sheet1", 4, 2, LiteralValue::Number(2.0));
    set_value(&mut engine, "Sheet1", 5, 2, LiteralValue::Number(3.0));
    set_formula(&mut engine, "Sheet1", 1, 2, "=SUM(B:B)");
    engine.evaluate_all().unwrap();
    // p1: 5, p2: 10, p3: 15, p4: 20.
    assert_eq!(num(&engine, "Sheet1", 1, 2), 20.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.settle_passes_total, 4);
}

/* ─────────────────────── §7.10 errors inside cycles ──────────────────── */

#[test]
fn error_in_cycle_propagates_and_converges_on_same_kind() {
    // B1 = 1/C1, C1 = B1 with C1 seeded Empty→0: pass 1 yields #DIV/0!,
    // pass 2 reproduces it, same-kind errors converge (§6). No #CIRC is
    // synthesized.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 2, "=1/C1");
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1");
    engine.evaluate_all().unwrap();
    assert_eq!(err_kind(&engine, "Sheet1", 1, 2), ExcelErrorKind::Div);
    assert_eq!(err_kind(&engine, "Sheet1", 1, 3), ExcelErrorKind::Div);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(t.capped_sccs, 0);
    assert_eq!(t.circ_cells_stamped, 0);
    assert_eq!(t.settle_passes_total, 2);
}

#[test]
fn cycle_escapes_the_error_when_the_outside_input_changes() {
    // B1 = IFERROR(0*C1, 0) + 1/D1, C1 = B1. D1 = 0 errors the cycle;
    // setting D1 = 4 lets iteration escape to a clean fixed point.
    let mut engine = iterate_engine(100, 0.001);
    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(0.0)); // D1
    set_formula(&mut engine, "Sheet1", 1, 2, "=IFERROR(0*C1,0)+1/D1");
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1");
    engine.evaluate_all().unwrap();
    assert_eq!(err_kind(&engine, "Sheet1", 1, 2), ExcelErrorKind::Div);
    assert_eq!(err_kind(&engine, "Sheet1", 1, 3), ExcelErrorKind::Div);
    assert_eq!(engine.last_cycle_telemetry().converged_sccs, 1);

    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(4.0));
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 2), 0.25);
    assert_eq!(num(&engine, "Sheet1", 1, 3), 0.25);
    assert_eq!(engine.last_cycle_telemetry().converged_sccs, 1);
}

/* ──────────────── §6 type transitions at workbook level ──────────────── */

#[test]
fn type_oscillation_never_converges_and_caps() {
    // A1 flips Number↔Text depending on B1's type; B1 = A1. Every pass is a
    // type transition for one of them, so the SCC caps (even a huge
    // max_change can't make transitions converge).
    let mut engine = iterate_engine(9, 1.0e12);
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(ISNUMBER(B1),\"t\",1)");
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1");
    engine.evaluate_all().unwrap();
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 0);
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.settle_passes_total, 9);
}

#[test]
fn text_cycle_converges_on_exact_equality() {
    // B1 = IF(C1="", "x", C1), C1 = B1: stabilizes on the text "x".
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 2, "=IF(C1=\"\",\"x\",C1)");
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1");
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Text("x".to_string()))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Text("x".to_string()))
    );
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(t.capped_sccs, 0);
}

/* ─────────────── §7.3 guard flips / phantom parity ───────────────────── */

#[test]
fn guard_flip_inside_iteration_converges_after_the_branch_change() {
    // A1 = IF(B1>2, 7, B1+1), B1 = A1: the else-branch climbs 1, 2, 3, the
    // guard flips on pass 4 (7), pass 5 confirms — converged, never capped.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(B1>2,7,B1+1)");
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 7.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 7.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(t.capped_sccs, 0);
    assert_eq!(t.settle_passes_total, 5);
}

#[test]
fn live_cycle_appearing_mid_settle_switches_into_iteration() {
    // Stage-2's branch-flip shape: pass 1 is acyclic; A1's settle re-eval
    // flips onto A3, closing a live cycle A1↔A3 that only classification-
    // after-settle sees. Under Iterate it must iterate (and here it
    // stabilizes immediately) instead of stamping #CIRC.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(A2=999,A3,7)");
    set_formula(&mut engine, "Sheet1", 2, 1, "=IF(TRUE,999,A1)");
    set_formula(&mut engine, "Sheet1", 3, 1, "=IF(TRUE,A1,8)");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 7.0);
    assert_eq!(num(&engine, "Sheet1", 2, 1), 999.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 7.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.circ_cells_stamped, 0);
    assert_eq!(t.live_cycles_witnessed, 1);
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(t.capped_sccs, 0);
}

#[test]
fn phantom_sccs_behave_identically_under_both_policies() {
    // The #99 guarded pair: phantom under Error AND under Iterate — no
    // iteration, identical values and settle telemetry.
    fn run(policy: CyclePolicy) -> (f64, f64, crate::engine::CycleTelemetry) {
        let cfg = EvalConfig::default().with_cycle(CycleConfig {
            detection: CycleDetection::Runtime,
            policy,
        });
        let mut engine = Engine::new(TestWorkbook::new(), cfg);
        set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Boolean(true));
        set_formula(&mut engine, "Sheet1", 2, 1, "=IF(A1,555,A3)");
        set_formula(&mut engine, "Sheet1", 3, 1, "=IF(A1,A2,999)");
        engine.evaluate_all().unwrap();
        let mut t = engine.last_cycle_telemetry().clone();
        t.elapsed_ms = 0;
        (
            num(&engine, "Sheet1", 2, 1),
            num(&engine, "Sheet1", 3, 1),
            t,
        )
    }
    let error = run(CyclePolicy::Error);
    let iterate = run(CyclePolicy::iterate_excel_defaults());
    assert_eq!(error, iterate);
    assert_eq!(iterate.0, 555.0);
    assert_eq!(iterate.2.phantom_sccs, 1);
    assert_eq!(iterate.2.iterated_sccs, 0);

    // And a phantom SCC under Iterate never registers the per-recalc
    // redirty: a second recalc with no edits schedules no SCC task.
    let cfg = EvalConfig::default().with_cycle(CycleConfig::iterate_excel_defaults());
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Boolean(true));
    set_formula(&mut engine, "Sheet1", 2, 1, "=IF(A1,555,A3)");
    set_formula(&mut engine, "Sheet1", 3, 1, "=IF(A1,A2,999)");
    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().phantom_sccs, 1);
    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().static_sccs, 0);
}

/* ─────────────── §7.12 / G12: INDIRECT inside an iterating SCC ────────── */

#[test]
fn indirect_in_iterating_scc_completes_within_bounded_replan() {
    // A1 = INDIRECT(D1)+1 with D1 → "B1", B1 = A1+1: the virtual edge closes
    // the SCC; under Iterate the divergent ladder caps inside ONE replan
    // iteration (total work ≤ MAX_REPLAN × max_iterations passes).
    let cfg = iterate_cfg(5, 0.001).with_virtual_dep_telemetry(true);
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    set_value(
        &mut engine,
        "Sheet1",
        1,
        4,
        LiteralValue::Text("B1".to_string()),
    );
    set_formula(&mut engine, "Sheet1", 1, 1, "=INDIRECT(D1)+1");
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1+1");
    engine.evaluate_all().unwrap();

    // Member order (A1, B1); pass k commits A1 = 2k−1, B1 = 2k; cap 5.
    assert_eq!(num(&engine, "Sheet1", 1, 1), 9.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 10.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.capped_sccs, 1);
    let vt = engine.last_virtual_dep_telemetry().clone();
    const MAX_REPLAN: usize = 5;
    assert!(vt.replan_iterations <= MAX_REPLAN);
    assert!(
        t.settle_passes_total <= (1 + MAX_REPLAN) * 5,
        "combined replan × iteration bound, got {} passes",
        t.settle_passes_total
    );

    // Re-point the dynamic ref: the cycle dissolves; plain values.
    set_value(&mut engine, "Sheet1", 3, 1, LiteralValue::Number(10.0)); // A3
    set_value(
        &mut engine,
        "Sheet1",
        1,
        4,
        LiteralValue::Text("A3".to_string()),
    );
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 11.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 12.0);
    // …and the per-recalc redirty stops once nothing iterates.
    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().iterated_sccs, 0);
    assert_eq!(num(&engine, "Sheet1", 1, 1), 11.0);
}

/* ────────────── property test: random linear fixed points ─────────────── */

/// Tiny deterministic PRNG (xorshift64*) — no external entropy.
struct XorShift64(u64);
impl XorShift64 {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    /// Integer in [lo, hi] inclusive.
    fn int_in(&mut self, lo: i64, hi: i64) -> i64 {
        let span = (hi - lo + 1) as u64;
        lo + (self.next_u64() % span) as i64
    }
}

/// Solve `M x = b` by Gaussian elimination with partial pivoting.
fn solve_linear(mut m: Vec<Vec<f64>>, mut b: Vec<f64>) -> Vec<f64> {
    let n = b.len();
    for col in 0..n {
        let pivot = (col..n)
            .max_by(|&a, &b2| m[a][col].abs().partial_cmp(&m[b2][col].abs()).unwrap())
            .unwrap();
        m.swap(col, pivot);
        b.swap(col, pivot);
        assert!(m[col][col].abs() > 1e-12, "singular test matrix");
        for row in (col + 1)..n {
            let f = m[row][col] / m[col][col];
            let pivot_row = m[col].clone();
            for (k, pv) in pivot_row.iter().enumerate().skip(col) {
                m[row][k] -= f * pv;
            }
            b[row] -= f * b[col];
        }
    }
    let mut x = vec![0.0; n];
    for row in (0..n).rev() {
        let mut acc = b[row];
        for k in (row + 1)..n {
            acc -= m[row][k] * x[k];
        }
        x[row] = acc / m[row][row];
    }
    x
}

const COLS: [&str; 6] = ["A", "B", "C", "D", "E", "F"];

#[test]
fn random_contractive_linear_systems_converge_to_the_solution() {
    // x = A·x + b with ‖A‖∞ ≤ 0.75 by construction (coefficients are
    // dyadic multiples of 1/64 so the formula text round-trips exactly).
    for seed in [1u64, 7, 42, 1234, 987_654_321] {
        let mut rng = XorShift64(seed);
        let n = rng.int_in(3, 6) as usize;
        let mut a = vec![vec![0.0f64; n]; n];
        let mut b = vec![0.0f64; n];
        for (row, bi) in a.iter_mut().zip(b.iter_mut()) {
            for aij in row.iter_mut() {
                *aij = rng.int_in(-8, 8) as f64 * 0.015625; // |aij| ≤ 0.125
            }
            *bi = rng.int_in(-40, 40) as f64 * 0.25;
        }

        let max_change = 1e-9;
        let mut engine = iterate_engine(1000, max_change);
        for i in 0..n {
            let mut f = format!("={}", b[i]);
            for j in 0..n {
                f.push_str(&format!("+({})*{}1", a[i][j], COLS[j]));
            }
            set_formula(&mut engine, "Sheet1", 1, (i + 1) as u32, &f);
        }
        engine.evaluate_all().unwrap();

        // Closed form: (I − A) x = b.
        let mut m = vec![vec![0.0f64; n]; n];
        for i in 0..n {
            for j in 0..n {
                m[i][j] = if i == j { 1.0 - a[i][j] } else { -a[i][j] };
            }
        }
        let expected = solve_linear(m, b);
        for (i, want) in expected.iter().enumerate() {
            let got = num(&engine, "Sheet1", 1, (i + 1) as u32);
            assert!(
                (got - want).abs() < 1e-6,
                "seed {seed} x{i}: got {got}, want {want}"
            );
        }
        let t = engine.last_cycle_telemetry();
        assert_eq!(t.iterated_sccs, 1, "seed {seed}");
        assert_eq!(t.converged_sccs, 1, "seed {seed}");
        assert_eq!(t.capped_sccs, 0, "seed {seed}");
        assert!(t.max_abs_delta_at_stop < max_change, "seed {seed}");
    }
}

#[test]
fn random_expansive_linear_systems_cap_without_error() {
    // Ring systems with |coefficient| = 1.5 ⇒ spectral radius 1.5 > 1:
    // never converges, caps cleanly, values stay finite numbers.
    for seed in [3u64, 99, 2026] {
        let mut rng = XorShift64(seed);
        let n = rng.int_in(3, 6) as usize;
        let cap = 40u32;
        let mut engine = iterate_engine(cap, 0.001);
        for i in 0..n {
            let j = (i + 1) % n;
            let sign = if rng.int_in(0, 1) == 0 { "" } else { "-" };
            let f = format!("=1+({sign}1.5)*{}1", COLS[j]);
            set_formula(&mut engine, "Sheet1", 1, (i + 1) as u32, &f);
        }
        engine.evaluate_all().unwrap();
        let t = engine.last_cycle_telemetry();
        assert_eq!(t.iterated_sccs, 1, "seed {seed}");
        assert_eq!(t.converged_sccs, 0, "seed {seed}");
        assert_eq!(t.capped_sccs, 1, "seed {seed}");
        assert_eq!(t.circ_cells_stamped, 0, "seed {seed}: capping ≠ error");
        assert_eq!(t.settle_passes_total, cap as usize, "seed {seed}");
        for i in 0..n {
            let v = num(&engine, "Sheet1", 1, (i + 1) as u32);
            assert!(v.is_finite(), "seed {seed} x{i} = {v}");
        }
    }
}

/* ───────────────────────── side effects & deltas ──────────────────────── */

#[test]
fn one_delta_per_member_per_recalc_under_iteration() {
    use formualizer_common::PackedSheetCell;

    // Divergent pair: both members change every recalc → one delta each,
    // with the FINAL value only (never per pass).
    let mut engine = iterate_engine(10, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=A2+1");
    set_formula(&mut engine, "Sheet1", 2, 1, "=A1+1");
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let mut expected = vec![
        PackedSheetCell::try_new(sheet_id, 0, 0).unwrap(), // A1 (0-based)
        PackedSheetCell::try_new(sheet_id, 1, 0).unwrap(), // A2
    ];
    expected.sort_unstable();
    assert_eq!(delta.changed_cells, expected);
    assert_eq!(num(&engine, "Sheet1", 1, 1), 19.0);

    // Next recalc: values move again → deltas again.
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    assert_eq!(delta.changed_cells, expected);
    assert_eq!(num(&engine, "Sheet1", 1, 1), 39.0);

    // A converged-and-stable SCC re-runs each recalc but produces NO deltas
    // when the final values don't move.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(B1>2,7,B1+1)");
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1");
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    assert_eq!(delta.changed_cells.len(), 2);
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    assert_eq!(engine.last_cycle_telemetry().iterated_sccs, 1);
    assert!(
        delta.changed_cells.is_empty(),
        "stable values must not re-delta: {:?}",
        delta.changed_cells
    );
}

/* ───────────────────────── cancellation mid-iteration ─────────────────── */

#[test]
fn cancellation_is_honored_between_iteration_passes() {
    use crate::args::ArgSchema;
    use crate::function::{FnCaps, Function};
    use crate::traits::{ArgumentHandle, FunctionContext};

    // TRIPCANCEL() sets the cancel flag during pass 1 of a divergent pair;
    // the per-pass check must stop iteration before pass 2.
    #[derive(Debug)]
    struct TripFn(Arc<AtomicBool>);
    impl Function for TripFn {
        fn caps(&self) -> FnCaps {
            FnCaps::empty()
        }
        fn name(&self) -> &'static str {
            "TRIPCANCEL"
        }
        fn arg_schema(&self) -> &'static [ArgSchema] {
            &[]
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            self.0.store(true, Ordering::Relaxed);
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
        }
    }

    let flag = Arc::new(AtomicBool::new(false));
    let wb = TestWorkbook::new().with_function(Arc::new(TripFn(flag.clone())));
    let mut engine = Engine::new(wb, iterate_cfg(1000, 0.001));
    set_formula(&mut engine, "Sheet1", 1, 1, "=A2+1+TRIPCANCEL()");
    set_formula(&mut engine, "Sheet1", 2, 1, "=A1+1");
    let err = engine.evaluate_all_cancellable(flag).unwrap_err();
    assert_eq!(err.kind, ExcelErrorKind::Cancelled);
    assert!(
        err.message.as_deref().unwrap_or("").contains("SCC"),
        "cancellation must come from the SCC pass boundary, got {err:?}"
    );
}

/* ─────────────────────────── determinism ──────────────────────────────── */

#[test]
fn iterate_is_deterministic_across_thread_counts_and_repeats() {
    fn build_and_run(
        threads: usize,
    ) -> Vec<(Vec<Option<LiteralValue>>, crate::engine::CycleTelemetry)> {
        let cfg = EvalConfig {
            max_threads: Some(threads),
            enable_parallel: threads > 1,
            ..iterate_cfg(10, 0.001)
        };
        let mut engine = Engine::new(TestWorkbook::new(), cfg);
        // Mixed workbook: converging pair, divergent pair, accumulator,
        // phantom pair, and downstream readers.
        set_formula(&mut engine, "Sheet1", 1, 1, "=0.5*10+0.5*B1");
        set_formula(&mut engine, "Sheet1", 1, 2, "=0.5*A1+0.5*20");
        set_formula(&mut engine, "Sheet1", 2, 1, "=B2+1");
        set_formula(&mut engine, "Sheet1", 2, 2, "=A2+1");
        set_value(&mut engine, "Sheet1", 3, 1, LiteralValue::Number(5.0));
        set_formula(&mut engine, "Sheet1", 3, 2, "=B3+A3");
        set_value(&mut engine, "Sheet1", 4, 1, LiteralValue::Boolean(true));
        set_formula(&mut engine, "Sheet1", 4, 2, "=IF(A4,555,C4)");
        set_formula(&mut engine, "Sheet1", 4, 3, "=IF(A4,B4,999)");
        set_formula(&mut engine, "Sheet1", 5, 1, "=A1+A2+B3");

        // Two recalcs: the second exercises the per-recalc redirty schedule.
        let mut out = Vec::new();
        for _ in 0..2 {
            engine.evaluate_all().unwrap();
            let mut values = Vec::new();
            for r in 1..=5u32 {
                for c in 1..=3u32 {
                    values.push(engine.get_cell_value("Sheet1", r, c));
                }
            }
            let mut telemetry = engine.last_cycle_telemetry().clone();
            telemetry.elapsed_ms = 0;
            out.push((values, telemetry));
        }
        out
    }

    let baseline = build_and_run(1);
    for threads in [1usize, 2, 8] {
        for run in 0..2 {
            let out = build_and_run(threads);
            assert_eq!(out, baseline, "threads={threads} run={run}");
        }
    }
}
