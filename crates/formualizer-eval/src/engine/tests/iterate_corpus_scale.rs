//! Iterate edge corpus — scale shapes (perf-signal, not perf gates;
//! RFC #112/#113).
//!
//! Correctness is asserted via telemetry and EVAL COUNTS (never wall time);
//! shape timings are printed with eprintln for the findings report. Run the
//! timing-heavy comparisons with `--ignored` in release for real numbers.

use crate::engine::{CycleConfig, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn iterate_cfg(max_iterations: u32, max_change: f64) -> EvalConfig {
    EvalConfig::default().with_cycle(CycleConfig::iterate(max_iterations, max_change))
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

/// Counting function: returns 0, counts invocations.
#[derive(Debug)]
struct CountFn(Arc<AtomicUsize>);
impl crate::function::Function for CountFn {
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::empty()
    }
    fn name(&self) -> &'static str {
        "COUNTEVALS"
    }
    fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
        &[]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
    }
}

/* ───────────── downstream of an SCC evaluates exactly once ────────────── */

#[test]
fn wide_downstream_of_iterating_scc_evaluates_once_per_recalc() {
    // SCC (B1, C1) iterates many passes; 200 downstream readers of B1 must
    // each evaluate exactly ONCE per recalc (spec §3.6: dependents see final
    // values only — pass-driven re-evaluation would show up as count > N).
    let count = Arc::new(AtomicUsize::new(0));
    let wb = TestWorkbook::new().with_function(Arc::new(CountFn(count.clone())));
    let mut engine = Engine::new(wb, iterate_cfg(100, 0.001));
    set_formula(&mut engine, "Sheet1", 1, 2, "=0.5*10+0.5*C1"); // B1
    set_formula(&mut engine, "Sheet1", 1, 3, "=0.5*B1+0.5*20"); // C1
    let n = 200u32;
    for row in 2..2 + n {
        set_formula(&mut engine, "Sheet1", row, 4, "=B1+COUNTEVALS()");
    }
    engine.evaluate_all().unwrap();
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert!(t.settle_passes_total > 2, "the SCC really iterated");
    assert_eq!(
        count.load(Ordering::Relaxed),
        n as usize,
        "each downstream dependent evaluates exactly once per recalc"
    );

    // Second recalc: the iterative redirty re-fires the SCC AND its
    // dependents — but still exactly once each.
    engine.evaluate_all().unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 2 * n as usize);
}

/* ───────────── recalc-after-converged-recalc dirty footprint ──────────── */

#[test]
fn stable_iterating_scc_recalc_does_not_touch_unrelated_workbook() {
    // 50 iterating pairs + 2000 unrelated acyclic formulas. After the first
    // recalc, a no-edit recalc must re-evaluate ONLY the SCC members (the
    // per-recalc redirty is O(SCC members), not O(workbook)).
    let mut engine = Engine::new(TestWorkbook::new(), iterate_cfg(100, 0.001));
    let pairs = 50u32;
    for i in 0..pairs {
        let r = 1 + i * 2;
        set_formula(
            &mut engine,
            "Sheet1",
            r,
            1,
            &format!("=0.5*10+0.5*A{}", r + 1),
        );
        set_formula(&mut engine, "Sheet1", r + 1, 1, &format!("=0.5*A{r}"));
    }
    let unrelated = 2000u32;
    set_value(&mut engine, "Sheet1", 1, 9, LiteralValue::Number(1.0)); // I1
    for row in 1..=unrelated {
        set_formula(&mut engine, "Sheet1", row, 10, "=$I$1*2");
    }
    let res1 = engine.evaluate_all().unwrap();
    // SCC-member commits are not counted in `computed_vertices` (they bypass
    // the layer pipeline) — count the acyclic plane only.
    assert!(res1.computed_vertices >= unrelated as usize);
    assert_eq!(engine.last_cycle_telemetry().iterated_sccs, pairs as usize);

    let res2 = engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().iterated_sccs, pairs as usize);
    assert!(
        res2.computed_vertices <= (pairs * 2) as usize,
        "no-edit recalc must touch only SCC members (got {} > {})",
        res2.computed_vertices,
        pairs * 2
    );
}

/* ───────────── SCC granularity shapes: 1000×2 vs 10×200 vs 1×2000 ─────── */

fn build_independent_pairs(engine: &mut Engine<TestWorkbook>, count: u32) {
    for i in 0..count {
        let r = 1 + i * 2;
        set_formula(engine, "Sheet1", r, 1, &format!("=0.5*10+0.5*A{}", r + 1));
        set_formula(engine, "Sheet1", r + 1, 1, &format!("=0.5*A{r}"));
    }
}

/// Ring of `size` members: A1 = 0.5·A{size} + 1, A_k = A_{k−1} (k ≥ 2).
/// Gauss–Seidel propagates fully within one pass (member order ascending
/// row), so the contraction converges in a handful of passes at any size.
fn build_ring(engine: &mut Engine<TestWorkbook>, col: u32, size: u32) {
    let col_letter = match col {
        1 => "A",
        2 => "B",
        _ => unreachable!(),
    };
    set_formula(
        engine,
        "Sheet1",
        1,
        col,
        &format!("=0.5*{col_letter}{size}+1"),
    );
    for r in 2..=size {
        set_formula(engine, "Sheet1", r, col, &format!("={col_letter}{}", r - 1));
    }
}

#[test]
fn granularity_shapes_converge_with_expected_telemetry() {
    // 1000 × 2-member SCCs.
    let t0 = std::time::Instant::now();
    let mut engine = Engine::new(TestWorkbook::new(), iterate_cfg(100, 0.001));
    build_independent_pairs(&mut engine, 1000);
    engine.evaluate_all().unwrap();
    let many_small = t0.elapsed();
    let t = engine.last_cycle_telemetry().clone();
    assert_eq!(t.static_sccs, 1000);
    assert_eq!(t.iterated_sccs, 1000);
    assert_eq!(t.converged_sccs, 1000);
    assert_eq!(t.capped_sccs, 0);
    let recalc0 = std::time::Instant::now();
    engine.evaluate_all().unwrap(); // converged-stable recalc
    let many_small_recalc = recalc0.elapsed();

    // 1 × 500-member SCC (ring). NOT 2000: the scheduler's recursive Tarjan
    // overflows the 2 MiB test-thread stack well before that depth in debug —
    // see the `tarjan_recursion_overflows_*` repros below.
    let t1 = std::time::Instant::now();
    let mut engine = Engine::new(TestWorkbook::new(), iterate_cfg(100, 0.001));
    build_ring(&mut engine, 1, 500);
    engine.evaluate_all().unwrap();
    let one_large = t1.elapsed();
    let t = engine.last_cycle_telemetry().clone();
    assert_eq!(t.static_sccs, 1);
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 1);
    assert!(
        (num(&engine, "Sheet1", 1, 1) - 2.0).abs() < 0.01,
        "x = 0.5x+1 ⇒ 2"
    );
    let recalc1 = std::time::Instant::now();
    engine.evaluate_all().unwrap();
    let one_large_recalc = recalc1.elapsed();
    let one_large_recalc_passes = engine.last_cycle_telemetry().settle_passes_total;

    // 10 × 200-member SCCs (rings at row offsets).
    let t2 = std::time::Instant::now();
    let mut engine = Engine::new(TestWorkbook::new(), iterate_cfg(100, 0.001));
    for block in 0..10u32 {
        let base = block * 200;
        set_formula(
            &mut engine,
            "Sheet1",
            base + 1,
            1,
            &format!("=0.5*A{}+1", base + 200),
        );
        for r in (base + 2)..=(base + 200) {
            set_formula(&mut engine, "Sheet1", r, 1, &format!("=A{}", r - 1));
        }
    }
    engine.evaluate_all().unwrap();
    let ten_medium = t2.elapsed();
    let t = engine.last_cycle_telemetry().clone();
    assert_eq!(t.static_sccs, 10);
    assert_eq!(t.converged_sccs, 10);

    eprintln!(
        "[iterate-scale] 1000×2: {many_small:?} (stable recalc {many_small_recalc:?}) | \
         1×500: {one_large:?} (stable recalc {one_large_recalc:?}, \
         {one_large_recalc_passes} passes) | 10×200: {ten_medium:?}"
    );
}

/* ───────────── BUG REPRO: recursive Tarjan stack overflow ─────────────── */
//
// `Scheduler::tarjan_visit` (scheduler.rs) recurses once per dependency hop.
// Any SCC of size k forces recursion depth k, and — independent of cycles —
// any dependency chain visited AGAINST vertex-id order does too (the root
// loop walks vertices in id order; a chain whose dependencies point at
// higher ids recurses its full length). Debug builds abort (SIGABRT, stack
// overflow) around depth ~1500 on the default 2 MiB test stack; release
// stacks die at larger but still-reachable depths. Real-world trigger:
// "totals above data" sheets (formulas referencing rows below) and any
// large iterating SCC. Fix direction: iterative DFS like
// `live_graph::analyze_live_graph` (already iterative). The fix touches
// both `tarjan_visit` and `tarjan_visit_with_virtual` and must preserve
// SCC emission order (schedule determinism) — left for a follow-up, with
// these repros pinning it.

#[test]
fn forward_built_deep_chain_is_fine_control() {
    // Control: same 2000 depth, dependencies pointing at LOWER ids — the
    // id-order root loop visits dependencies first, recursion stays shallow.
    let mut engine = Engine::new(TestWorkbook::new(), iterate_cfg(100, 0.001));
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Number(1.0));
    for r in 2..=2000u32 {
        set_formula(&mut engine, "Sheet1", r, 1, &format!("=A{}+1", r - 1));
    }
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2000, 1), 2000.0);
}

#[test]
#[ignore = "BUG (pre-existing, scheduler): recursive tarjan_visit overflows the stack on a 2000-member SCC — SIGABRT in debug; see module comment"]
fn tarjan_recursion_overflows_on_2000_member_scc() {
    let mut engine = Engine::new(TestWorkbook::new(), iterate_cfg(100, 0.001));
    build_ring(&mut engine, 1, 2000);
    engine.evaluate_all().unwrap();
    assert!((num(&engine, "Sheet1", 1, 1) - 2.0).abs() < 0.01);
}

#[test]
#[ignore = "BUG (pre-existing, scheduler): recursive tarjan_visit overflows the stack on a reverse-built 2000-deep ACYCLIC chain — not cycle-specific; see module comment"]
fn tarjan_recursion_overflows_on_reverse_built_acyclic_chain() {
    let mut engine = Engine::new(TestWorkbook::new(), iterate_cfg(100, 0.001));
    for r in 1..2000u32 {
        set_formula(&mut engine, "Sheet1", r, 1, &format!("=A{}+1", r + 1));
    }
    set_value(&mut engine, "Sheet1", 2000, 1, LiteralValue::Number(1.0));
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 2000.0);
}

#[test]
fn ring_stable_recalc_cost_scales_linearly_probe() {
    // Perf-shape probe (printed, not gated): a converged ring's no-edit
    // recalc must scale ~linearly in SCC size. Before the
    // `redirty_iterative_members` already-dirty skip this was quadratic
    // (each member's `mark_dirty` re-walked the whole component): release
    // numbers were 12 ms / 42 ms / 83 ms for 500/1000/1500 members; after
    // the fix ~0.6 ms / 1.2 ms / 2.4 ms. Sizes stay below the recursive-
    // Tarjan debug stack limit (see the overflow repros above).
    for size in [200u32, 400, 800] {
        let mut engine = Engine::new(TestWorkbook::new(), iterate_cfg(100, 0.001));
        build_ring(&mut engine, 1, size);
        engine.evaluate_all().unwrap();
        let t = std::time::Instant::now();
        engine.evaluate_all().unwrap();
        let tel = engine.last_cycle_telemetry();
        assert_eq!(tel.iterated_sccs, 1, "size {size}");
        assert_eq!(tel.converged_sccs, 1, "size {size}");
        assert_eq!(
            tel.settle_passes_total, 2,
            "stable ring reconverges in 2 passes"
        );
        eprintln!(
            "[iterate-scale] ring {size} stable recalc: {:?} ({} passes)",
            t.elapsed(),
            tel.settle_passes_total
        );
    }
}
