//! Iterate edge corpus — numeric non-finite shapes (RFC #112/#113).
//!
//! The ±inf convergence wart (Stage-3c bench observation): what actually
//! happens when a divergent factor overflows mid-iteration.
//!
//! Findings pinned here:
//! - Operator arithmetic sanitizes overflow to `#NUM!` (Excel parity), so a
//!   divergent product chain "converges" via the §6 Error-vs-Error rule —
//!   spec-compliant, matches Excel's stabilized `#NUM!`.
//! - Aggregates (SUM/MAX) do NOT sanitize, so `Number(inf)` can leak into a
//!   cycle. The §6 comparator is correct: |inf − inf| = NaN, NaN < max_change
//!   is false → never converges → permanent cap on every recalc (perf trap,
//!   documented divergence from Excel, which cannot represent inf at all).

use crate::engine::convergence::values_converged;
use crate::engine::{CycleConfig, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{DateSystem, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

const DS: DateSystem = DateSystem::Excel1900;

fn iterate_engine(max_iterations: u32, max_change: f64) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_cycle(CycleConfig::iterate(max_iterations, max_change)),
    )
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

fn err_kind(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> ExcelErrorKind {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Error(e)) => e.kind,
        other => panic!("expected error at {sheet} r{row}c{col}, got {other:?}"),
    }
}

/* ─────────────── §6 comparator unit pins for non-finite pairs ─────────── */

#[test]
fn comparator_inf_vs_inf_does_not_converge() {
    // |inf − inf| = NaN; NaN < max_change is false. Spec-§6 literal rule.
    let inf = LiteralValue::Number(f64::INFINITY);
    let out = values_converged(&inf, &inf.clone(), 0.001, DS);
    assert!(!out.converged, "inf vs inf must NOT converge (spec §6)");
    assert!(
        !out.nan_converged,
        "the NaN flag is for NaN values, not inf"
    );
    // Even a huge max_change cannot make it converge.
    assert!(!values_converged(&inf, &inf.clone(), f64::MAX, DS).converged);
}

#[test]
fn comparator_neg_inf_vs_neg_inf_does_not_converge() {
    let ninf = LiteralValue::Number(f64::NEG_INFINITY);
    assert!(!values_converged(&ninf, &ninf.clone(), 0.001, DS).converged);
}

#[test]
fn comparator_inf_vs_neg_inf_does_not_converge() {
    // |−inf − inf| = inf; inf < max_change is false for any finite bound.
    let inf = LiteralValue::Number(f64::INFINITY);
    let ninf = LiteralValue::Number(f64::NEG_INFINITY);
    assert!(!values_converged(&inf, &ninf, f64::MAX, DS).converged);
    assert!(!values_converged(&ninf, &inf, f64::MAX, DS).converged);
}

#[test]
fn comparator_inf_vs_finite_does_not_converge() {
    let inf = LiteralValue::Number(f64::INFINITY);
    let big = LiteralValue::Number(f64::MAX);
    assert!(!values_converged(&big, &inf, f64::MAX, DS).converged);
    assert!(!values_converged(&inf, &big, f64::MAX, DS).converged);
}

#[test]
fn comparator_f64_max_vs_f64_max_converges_with_zero_delta() {
    // The largest finite value still compares exactly: Δ = 0 < max_change.
    let big = LiteralValue::Number(f64::MAX);
    let out = values_converged(&big, &big.clone(), 0.001, DS);
    assert!(out.converged);
    assert_eq!(out.abs_delta, Some(0.0));
}

/* ───────────── overflow mid-iteration at the workbook level ───────────── */

#[test]
fn operator_overflow_mid_iteration_stabilizes_as_num_error_and_converges() {
    // B1 = C1*1e100, C1 = B1*1e100+1: the product ladder overflows on pass 3
    // (1e400 → `#NUM!` via sanitize_numeric — Excel parity), the error then
    // propagates around the cycle, and `#NUM!` vs `#NUM!` converges (§6
    // Error-vs-Error). This is the Stage-3c "divergence converges" shape: it
    // is NOT the comparator falsely accepting inf — operator arithmetic never
    // commits an inf, it commits `#NUM!`, exactly like Excel.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 2, "=C1*1E100"); // B1
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1*1E100+1"); // C1
    engine.evaluate_all().unwrap();

    assert_eq!(err_kind(&engine, "Sheet1", 1, 2), ExcelErrorKind::Num);
    assert_eq!(err_kind(&engine, "Sheet1", 1, 3), ExcelErrorKind::Num);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 1, "same-kind errors converge (§6)");
    assert_eq!(t.capped_sccs, 0);
    assert_eq!(t.circ_cells_stamped, 0, "no #CIRC is synthesized");
    assert!(
        t.settle_passes_total <= 6,
        "overflow → error → convergence must be quick, got {} passes",
        t.settle_passes_total
    );
}

#[test]
fn aggregate_leaked_infinity_pins_the_scc_at_a_permanent_cap() {
    // SUM does not sanitize overflow, so `Number(inf)` leaks into the cycle:
    // B1 = MAX(SUM(A1:A2), C1), C1 = B1 with A1 = A2 = 1e308. Both members
    // commit +inf on pass 1 and never change again — but |inf − inf| = NaN
    // fails the §6 numeric test, so the SCC caps at max_iterations, and does
    // so again on EVERY subsequent recalc (the per-recalc redirty re-fires
    // iterating SCCs). Pinned as spec-§6-correct; flagged in the findings
    // report as a perf trap + Excel divergence (Excel would show #NUM!).
    let mut engine = iterate_engine(7, 0.001);
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Number(1.0e308));
    set_value(&mut engine, "Sheet1", 2, 1, LiteralValue::Number(1.0e308));
    set_formula(&mut engine, "Sheet1", 1, 2, "=MAX(SUM(A1:A2),C1)"); // B1
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1"); // C1
    engine.evaluate_all().unwrap();

    // Documents the leak itself: the committed value really is +inf.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(f64::INFINITY)),
        "SUM/MAX leak unsanitized +inf (Excel would have produced #NUM!)"
    );
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 0, "inf vs inf must not converge (§6)");
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.settle_passes_total, 7, "runs the full pass budget");

    // Permanent: a no-edit recalc burns the full budget again.
    engine.evaluate_all().unwrap();
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.settle_passes_total, 7);
}

#[test]
fn sign_oscillating_infinity_caps_deterministically() {
    // B1 = -C1, C1 = MAX(B1, SUM(A1:A2)) with the SUM forcing +inf into the
    // cycle: C1 pins at +inf, B1 at −inf (unary negation of inf is not an
    // operator overflow — the value is already non-finite... but `-C1`
    // sanitizes too, so expect #NUM! if it does). This test pins whichever
    // behavior is real: see asserts below (unary minus DOES sanitize).
    let mut engine = iterate_engine(5, 0.001);
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Number(1.0e308));
    set_value(&mut engine, "Sheet1", 2, 1, LiteralValue::Number(1.0e308));
    set_formula(&mut engine, "Sheet1", 1, 2, "=-C1"); // B1
    set_formula(&mut engine, "Sheet1", 1, 3, "=MAX(B1,SUM(A1:A2))"); // C1
    engine.evaluate_all().unwrap();

    // Unary minus routes through numeric sanitization: −inf → #NUM!.
    assert_eq!(err_kind(&engine, "Sheet1", 1, 2), ExcelErrorKind::Num);
    // C1 = MAX(#NUM!, inf) propagates the error.
    assert_eq!(err_kind(&engine, "Sheet1", 1, 3), ExcelErrorKind::Num);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(
        t.converged_sccs, 1,
        "once both members are #NUM!, error convergence applies"
    );
}

#[test]
fn near_overflow_divergent_growth_caps_without_error() {
    // Stays finite for the whole budget (×10 per pass from 1): no overflow,
    // plain §7.5 divergence semantics at large magnitudes.
    let mut engine = iterate_engine(50, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 2, "=C1*10"); // B1
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1*10+1"); // C1
    engine.evaluate_all().unwrap();
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.converged_sccs, 0);
    let b = match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Number(n)) => n,
        other => panic!("expected number, got {other:?}"),
    };
    assert!(b.is_finite() && b > 1e90, "finite huge growth, got {b}");
}
