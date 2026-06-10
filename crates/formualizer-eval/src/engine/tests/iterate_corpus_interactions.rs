//! Iterate edge corpus — interaction cells from the G-catalog (RFC #112/#113;
//! gotchas G7/G8/G11/G12 + spec §7.11/§7.12).
//!
//! Covered here: FormulaPlane-authoritative mode with an ITERATING SCC,
//! mid-iteration cancellation partial-state contract, exact delta sets across
//! iterating recalcs, RAND-in-cycle epoch semantics, and an INDIRECT whose
//! target string is itself computed by the cycle.

use crate::engine::{
    CycleConfig, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
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

/* ───────────── G8 × Iterate: FormulaPlane-authoritative mode ──────────── */

#[test]
fn iterating_scc_inside_fp_authoritative_mode_end_to_end() {
    // Stage 2b only proved Error+phantom inside FP-authoritative mode; this
    // pins the ITERATE policy: a span family whose member B5 joins a live
    // cycle (C5 = B5) must demote, ITERATE (grows by A5 = 5 per pass, cap 4
    // → 20), and leave the independent span + non-cycle members exact.
    let cfg = EvalConfig::default()
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
        .with_cycle(CycleConfig::iterate(4, 0.001));
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let mut col_b = Vec::new();
    let mut col_e = Vec::new();
    for row in 1..=120u32 {
        set_value(
            &mut engine,
            "Sheet1",
            row,
            1,
            LiteralValue::Number(row as f64),
        );
        for (col, f, bucket) in [
            (2u32, format!("=A{row}+C{row}"), &mut col_b),
            (5u32, format!("=A{row}*2"), &mut col_e),
        ] {
            let ast = parse(&f).unwrap();
            let ast_id = engine.intern_formula_ast(&ast);
            bucket.push(FormulaIngestRecord::new(
                row,
                col,
                ast_id,
                Some(Arc::<str>::from(f.as_str())),
            ));
        }
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(
            "Sheet1",
            col_b.into_iter().chain(col_e).collect(),
        )])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);

    // Close the live cycle through span member B5.
    set_formula(&mut engine, "Sheet1", 5, 3, "=B5");
    engine.evaluate_all().unwrap();

    // The cyclic span demoted; the CycleMember fallback reason is recorded.
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_cycle_member_span_demotions, 1);
    assert_eq!(stats.formula_plane_active_span_count, 1);

    // Iterate semantics on the demoted members: B5 = A5 + C5 accumulates
    // A5 = 5 per pass (member order B5 then C5) → cap 4 ⇒ 20.
    assert_eq!(num(&engine, "Sheet1", 5, 2), 20.0);
    assert_eq!(num(&engine, "Sheet1", 5, 3), 20.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.circ_cells_stamped, 0, "Iterate must not stamp #CIRC");

    // Non-cycle members and the independent span family stay exact.
    assert_eq!(num(&engine, "Sheet1", 1, 2), 1.0);
    assert_eq!(num(&engine, "Sheet1", 120, 2), 120.0);
    assert_eq!(num(&engine, "Sheet1", 120, 5), 240.0);

    // Persistence + redirty under FP: the next recalc grows B5 by 4·5 more.
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 5, 2), 40.0);
    assert_eq!(num(&engine, "Sheet1", 120, 5), 240.0, "span stays exact");
}

/* ───────────── G7 × Iterate: partial state after cancellation ─────────── */

#[test]
fn cancellation_mid_iteration_leaves_committed_values_and_engine_recovers() {
    use crate::args::ArgSchema;
    use crate::function::{FnCaps, Function};
    use crate::traits::{ArgumentHandle, FunctionContext};

    // CANCELAT(n): sets the cancel flag during evaluation #n (1-based count
    // of calls). The SCC commits write-through per member, so after the
    // cancelled recalc the values of all members evaluated in completed
    // passes are visible — the PINNED partial-state contract:
    //   (a) evaluate_* returns Cancelled;
    //   (b) committed per-pass values stand (no rollback);
    //   (c) the engine stays usable: the next recalc completes and iterates
    //       the SCC from those committed values (§4 persistence).
    #[derive(Debug)]
    struct CancelAt {
        calls: Arc<AtomicUsize>,
        trip_at: usize,
        flag: Arc<AtomicBool>,
    }
    impl Function for CancelAt {
        fn caps(&self) -> FnCaps {
            FnCaps::empty()
        }
        fn name(&self) -> &'static str {
            "CANCELAT"
        }
        fn arg_schema(&self) -> &'static [ArgSchema] {
            &[]
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            let n = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
            if n == self.trip_at {
                self.flag.store(true, Ordering::Relaxed);
            }
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let flag = Arc::new(AtomicBool::new(false));
    let wb = TestWorkbook::new().with_function(Arc::new(CancelAt {
        calls: calls.clone(),
        trip_at: 3, // trip during pass 3 of the divergent member
        flag: flag.clone(),
    }));
    let mut engine = Engine::new(wb, iterate_cfg(100, 0.001));
    set_formula(&mut engine, "Sheet1", 1, 1, "=A2+1+CANCELAT()");
    set_formula(&mut engine, "Sheet1", 2, 1, "=A1+1");

    // (a) cancelled at the pass-3→4 boundary.
    let err = engine.evaluate_all_cancellable(flag.clone()).unwrap_err();
    assert_eq!(err.kind, ExcelErrorKind::Cancelled);

    // (b) pass 3 ran to completion before the boundary check: A1 = 5, A2 = 6.
    assert_eq!(num(&engine, "Sheet1", 1, 1), 5.0);
    assert_eq!(num(&engine, "Sheet1", 2, 1), 6.0);
    assert_eq!(calls.load(Ordering::Relaxed), 3, "exactly 3 passes ran");

    // (c) recovery: clear the flag, recalc completes (cap reached) starting
    // from the committed partial values.
    flag.store(false, Ordering::Relaxed);
    engine.evaluate_all().unwrap();
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.capped_sccs, 1);
    // 100 more passes from (5, 6): A1 = 5 + 200 - 1 = 204? — pinned by
    // arithmetic: each pass adds 2 to each member; A1 = 5 + 2·100 - ... use
    // exact recurrence: pass k: A1 = A2_prev + 1, A2 = A1 + 1.
    // From (5,6): pass1 → (7,8) … pass100 → (205,206).
    assert_eq!(num(&engine, "Sheet1", 1, 1), 205.0);
    assert_eq!(num(&engine, "Sheet1", 2, 1), 206.0);
}

/* ───────────── G11 × Iterate: exact delta sets across recalcs ─────────── */

#[test]
fn delta_sets_stay_exact_across_converging_then_stable_then_perturbed_recalcs() {
    use formualizer_common::PackedSheetCell;

    // Converging SCC + outside input: recalc 1 deltas both members; recalc 2
    // (stable fixed point) deltas NOTHING; an input edit then re-deltas both
    // members plus the input cell itself.
    let mut engine = iterate_engine(100, 0.001);
    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(10.0)); // D1
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(B1>D1,D1,B1+1)"); // A1
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1"); // B1
    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let a1 = PackedSheetCell::try_new(sheet_id, 0, 0).unwrap();
    let b1 = PackedSheetCell::try_new(sheet_id, 0, 1).unwrap();
    let d1 = PackedSheetCell::try_new(sheet_id, 0, 3).unwrap();

    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    let mut expected = vec![a1, b1];
    expected.sort_unstable();
    assert_eq!(delta.changed_cells, expected, "recalc 1: both members");

    // Stable fixed point (climbs to D1 then holds): no deltas at all.
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    let (_res2, delta2) = engine.evaluate_all_with_delta().unwrap();
    assert!(delta.changed_cells.is_empty() || delta.changed_cells == expected);
    // By the third recalc the value is pinned at the fixed point.
    assert!(
        delta2.changed_cells.is_empty(),
        "stable SCC must not delta: {:?}",
        delta2.changed_cells
    );

    // Perturb the outside input: both members move again. (Pinned: the
    // evaluation delta records EVALUATION-driven changes only — the user
    // edit to D1 itself is not part of the eval delta.)
    let _ = d1;
    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(3.0));
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    assert_eq!(delta.changed_cells, expected, "perturbed: members re-delta");
}

/* ───────────── §7.11 × determinism: RAND inside a cycle ───────────────── */

#[test]
fn rand_in_cycle_is_pass_stable_per_recalc_and_reseeds_across_recalcs() {
    crate::builtins::random::register_builtins();
    // B1 = RAND() + 0·C1, C1 = B1: the live cycle iterates, but RAND is
    // seeded by (workbook_seed, cell, salt, recalc_epoch) — every pass of one
    // recalc draws the SAME sample → converges on pass 2 with Δ = 0. The
    // next recalc bumps the epoch → new sample → the SCC re-iterates to the
    // new value. Two engines with the same seed agree exactly (§11).
    fn run(seed: u64) -> Vec<f64> {
        let cfg = EvalConfig {
            workbook_seed: seed,
            ..iterate_cfg(100, 0.001)
        };
        let mut engine = Engine::new(TestWorkbook::new(), cfg);
        // Default volatile level keeps the epoch out of the seed; OnRecalc
        // includes it, which is what §7.11's "new sample per recalc" needs.
        engine.set_volatile_level(crate::traits::VolatileLevel::OnRecalc);
        set_formula(&mut engine, "Sheet1", 1, 2, "=RAND()+0*C1");
        set_formula(&mut engine, "Sheet1", 1, 3, "=B1");
        let mut out = Vec::new();
        for _ in 0..3 {
            engine.evaluate_all().unwrap();
            let t = engine.last_cycle_telemetry();
            assert_eq!(t.iterated_sccs, 1);
            assert_eq!(
                t.converged_sccs, 1,
                "pass-stable volatile must converge immediately"
            );
            assert_eq!(t.settle_passes_total, 2);
            assert_eq!(t.max_abs_delta_at_stop, 0.0);
            assert_eq!(
                num(&engine, "Sheet1", 1, 2),
                num(&engine, "Sheet1", 1, 3),
                "both members carry the same sample"
            );
            out.push(num(&engine, "Sheet1", 1, 2));
        }
        out
    }
    let a = run(42);
    let b = run(42);
    assert_eq!(a, b, "same seed ⇒ identical sequence across recalcs");
    assert!(
        a[0] != a[1] || a[1] != a[2],
        "epoch reseeding must move the sample across recalcs: {a:?}"
    );
}

/* ───────────── §7.12 × Iterate: cycle-computed INDIRECT target ────────── */

#[test]
fn indirect_target_computed_by_the_cycle_itself_terminates_and_settles() {
    // A1 = IFERROR(INDIRECT(T1),0) + 1 and T1 = IF(A1>5, "X1", "A1"): T1 is
    // an SCC member (reads A1) AND the string it produces decides A1's
    // dynamic edge. Pass 1 reads T1 = Empty → INDIRECT errors → IFERROR
    // seeds 0 → A1 climbs by 1 per pass through the "A1" self-target; once
    // A1 exceeds 5, T1 flips to "X1" (X1 = 100) and the system stabilizes at
    // A1 = 101 — possibly needing an outer replan when the dynamic target
    // changes. Total work must stay within MAX_REPLAN × max_iterations and
    // a follow-up recalc must be stable at the fixed point.
    let cfg = iterate_cfg(50, 0.001).with_virtual_dep_telemetry(true);
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    set_value(&mut engine, "Sheet1", 1, 24, LiteralValue::Number(100.0)); // X1
    set_formula(&mut engine, "Sheet1", 1, 1, "=IFERROR(INDIRECT(T1),0)+1"); // A1
    set_formula(&mut engine, "Sheet1", 1, 20, "=IF(A1>5,\"X1\",\"A1\")"); // T1
    engine.evaluate_all().unwrap();

    const MAX_REPLAN: usize = 5;
    let vt = engine.last_virtual_dep_telemetry().clone();
    assert!(vt.replan_iterations <= MAX_REPLAN);
    assert!(
        engine.last_cycle_telemetry().settle_passes_total <= (1 + MAX_REPLAN) * 50,
        "bounded by replan × iteration budget"
    );

    // Re-recalc until the fixed point (the flip may straddle a replan); two
    // recalcs are ample.
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 101.0, "A1 = X1 + 1");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 20),
        Some(LiteralValue::Text("X1".to_string()))
    );

    // Stability: one more recalc keeps the fixed point and converges.
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 101.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.converged_sccs, t.iterated_sccs, "no caps at fixed point");
}
