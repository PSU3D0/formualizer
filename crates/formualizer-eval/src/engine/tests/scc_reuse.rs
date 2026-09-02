//! #368 — retaining exactly converged iterative SCCs across recalcs.
//!
//! Under `CyclePolicy::Iterate` every iterating SCC used to be redirtied at
//! the end of each recalc (Excel's "circular cells recalculate every time"
//! contract, spec §4/§7.6). Now an SCC that stopped on an *exact* fixed point — every member reproduced its previous
//! value bit-for-bit, before the `max_iterations` cap, no NaN identity, no
//! volatile or dynamic member — is left clean instead. Running it again with
//! the same inputs cannot change anything, so the dirty graph alone decides
//! when it runs, plus a config fingerprint for knobs that live outside the
//! graph.
//!
//! This file is the behavior matrix: where reuse happens, where it must not,
//! and every invalidation door that must re-run a retained SCC.

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{
    CycleConfig, CyclePolicy, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord,
    FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;

/* ────────────────────────────── helpers ──────────────────────────────── */

fn iterate_cfg(max_iterations: u32, max_change: f64) -> EvalConfig {
    EvalConfig {
        temporal_egress: crate::engine::TemporalEgress::Serial,
        ..EvalConfig::default().with_cycle(CycleConfig::iterate(max_iterations, max_change))
    }
}

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

/// `(iterated_sccs, reused_sccs, reused_scc_members)` of the last request.
fn reuse_telemetry(engine: &Engine<TestWorkbook>) -> (usize, usize, usize) {
    let t = engine.last_cycle_telemetry();
    (t.iterated_sccs, t.reused_sccs, t.reused_scc_members)
}

/// The canonical exactly-converging pair: `A1 = IF(B1>2,7,B1+1)`, `B1 = A1`
/// climbs 1, 2, 3, flips the guard to 7 and reproduces 7 exactly on the
/// confirming pass (5 passes, converged, never capped).
fn build_exact_pair(engine: &mut Engine<TestWorkbook>) {
    set_formula(engine, "Sheet1", 1, 1, "=IF(B1>2,7,B1+1)");
    set_formula(engine, "Sheet1", 1, 2, "=A1");
}

/// A cycle whose exact fixed point is the value of an outside input cell:
/// `A{r} = IF(B{r}>=D{r}, D{r}, B{r}+1)`, `B{r} = A{r}`; `D{r}` is a plain
/// value cell. Converges exactly on `D{r}` for any positive integer input.
fn build_input_driven_pair(engine: &mut Engine<TestWorkbook>, row: u32, input: f64) {
    set_value(engine, "Sheet1", row, 4, LiteralValue::Number(input));
    set_formula(
        engine,
        "Sheet1",
        row,
        1,
        &format!("=IF(B{row}>=D{row},D{row},B{row}+1)"),
    );
    set_formula(engine, "Sheet1", row, 2, &format!("=A{row}"));
}

/// Function registrations are process-global. A registration invalidates
/// only the retained SCCs whose members call the changed function (an
/// incomplete change log invalidates all of them), so tests here are not
/// affected by registrations elsewhere in the binary in practice; the lock
/// keeps the exact-telemetry assertions independent of registration order
/// anyway. Retention tests take it shared; the registry test takes it
/// exclusively.
static EPOCH_LOCK: std::sync::RwLock<()> = std::sync::RwLock::new(());

fn epoch_stable() -> std::sync::RwLockReadGuard<'static, ()> {
    EPOCH_LOCK.read().unwrap_or_else(|e| e.into_inner())
}

fn epoch_bumper() -> std::sync::RwLockWriteGuard<'static, ()> {
    EPOCH_LOCK.write().unwrap_or_else(|e| e.into_inner())
}

/* ═════════════════════ 1–7: reuse happens (knob on) ═══════════════════ */

#[test]
fn exact_fixed_point_is_retained_and_reused_on_the_next_recalc() {
    let _epoch = epoch_stable();
    let mut engine = iterate_engine(100, 0.001);
    build_exact_pair(&mut engine);

    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 7.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 7.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(t.capped_sccs, 0);
    assert_eq!(t.settle_passes_total, 5);
    assert_eq!(t.reused_sccs, 0, "nothing was retained before this recalc");
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.settle_passes_total, 0, "no pass may run");
    assert_eq!(t.converged_sccs, 0);
    assert_eq!(t.capped_sccs, 0);
    assert_eq!(num(&engine, "Sheet1", 1, 1), 7.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 7.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);
}

#[test]
fn editing_one_sccs_input_reruns_only_that_scc() {
    let _epoch = epoch_stable();
    // Two independent exact SCCs (rows 1 and 3), each pinned to its own
    // input cell. Editing D1 must re-run SCC #1 only; SCC #3 stays reused.
    let mut engine = iterate_engine(100, 0.001);
    build_input_driven_pair(&mut engine, 1, 3.0);
    build_input_driven_pair(&mut engine, 3, 4.0);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 4.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 4);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 2, 4));

    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(6.0));
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 1, 2));
    assert_eq!(num(&engine, "Sheet1", 1, 1), 6.0, "SCC #1 re-converged");
    assert_eq!(num(&engine, "Sheet1", 1, 2), 6.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 4.0, "SCC #3 untouched");
    assert_eq!(num(&engine, "Sheet1", 3, 2), 4.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 4);

    // Both retained again.
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 2, 4));
}

#[test]
fn error_valued_fixed_point_is_retained_and_reused() {
    let _epoch = epoch_stable();
    // B1 = 1/C1, C1 = B1: #DIV/0! reproduces itself exactly (§6 same-kind
    // error rule) — an exact fixed point like any other.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 2, "=1/C1");
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1");
    engine.evaluate_all().unwrap();
    assert_eq!(err_kind(&engine, "Sheet1", 1, 2), ExcelErrorKind::Div);
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
    assert_eq!(err_kind(&engine, "Sheet1", 1, 2), ExcelErrorKind::Div);
    assert_eq!(err_kind(&engine, "Sheet1", 1, 3), ExcelErrorKind::Div);
}

#[test]
fn text_fixed_point_is_retained_and_reused() {
    let _epoch = epoch_stable();
    // B1 = IF(C1="","x",C1), C1 = B1 stabilizes on the text "x" (identity,
    // not tolerance).
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 2, "=IF(C1=\"\",\"x\",C1)");
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1");
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Text("x".to_string()))
    );
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Text("x".to_string()))
    );
}

#[test]
fn boolean_fixed_point_is_retained_and_reused() {
    let _epoch = epoch_stable();
    // B1 = OR(C1,TRUE), C1 = B1: TRUE on every pass from the FALSE seed.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 2, "=OR(C1,TRUE)");
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1");
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Boolean(true))
    );
}

#[test]
fn demand_driven_requests_serve_retained_members_without_scc_work() {
    let _epoch = epoch_stable();
    let mut engine = iterate_engine(100, 0.001);
    build_exact_pair(&mut engine);
    set_formula(&mut engine, "Sheet1", 1, 4, "=B1*3"); // acyclic dependent
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 4), 21.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);

    // Demand on a retained member itself.
    engine.evaluate_cell("Sheet1", 1, 1).unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
    assert_eq!(num(&engine, "Sheet1", 1, 1), 7.0);

    // Demand on the acyclic dependent: still no SCC work, right value.
    engine.evaluate_cell("Sheet1", 1, 4).unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
    assert_eq!(num(&engine, "Sheet1", 1, 4), 21.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);
}

#[test]
fn a_hundred_no_change_recalcs_do_no_work_and_keep_the_values() {
    let _epoch = epoch_stable();
    let mut engine = iterate_engine(100, 0.001);
    build_exact_pair(&mut engine);
    engine.evaluate_all().unwrap();
    let retained = engine.baseline_stats().retained_scc_members;
    assert_eq!(retained, 2);

    for i in 0..100 {
        engine.evaluate_all().unwrap();
        let t = engine.last_cycle_telemetry();
        assert_eq!(t.settle_passes_total, 0, "recalc {i}");
        assert_eq!(t.iterated_sccs, 0, "recalc {i}");
        assert_eq!(t.reused_sccs, 1, "recalc {i}");
        assert_eq!(num(&engine, "Sheet1", 1, 1), 7.0, "recalc {i}");
        assert_eq!(num(&engine, "Sheet1", 1, 2), 7.0, "recalc {i}");
        assert_eq!(
            engine.baseline_stats().retained_scc_members,
            retained,
            "recalc {i}"
        );
    }
}

#[test]
fn retained_scc_feeds_a_formula_plane_span_family_in_authoritative_mode() {
    let _epoch = epoch_stable();
    // FormulaPlane-authoritative: a 120-row span family reads the retained
    // SCC's output cell E1 through an absolute reference.
    let cfg = iterate_cfg(100, 0.001)
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    // SCC {E1, F1} pinned to the input G1.
    set_value(&mut engine, "Sheet1", 1, 7, LiteralValue::Number(3.0));
    set_formula(&mut engine, "Sheet1", 1, 5, "=IF(F1>=G1,G1,F1+1)");
    set_formula(&mut engine, "Sheet1", 1, 6, "=E1");
    // H1 is an ordinary (acyclic) reader of the SCC output; the span family
    // reads H1 so the span itself never touches a cycle member.
    set_formula(&mut engine, "Sheet1", 1, 8, "=E1");

    let mut records = Vec::new();
    let mut independent = Vec::new();
    for row in 1..=120u32 {
        set_value(
            &mut engine,
            "Sheet1",
            row,
            1,
            LiteralValue::Number(row as f64),
        );
        set_value(&mut engine, "Sheet1", row, 2, LiteralValue::Number(2.0));
        let f = format!("=B{row}*2+A{row}+$H$1");
        let ast = parse(&f).unwrap();
        let ast_id = engine.intern_formula_ast(&ast);
        records.push(FormulaIngestRecord::new(
            row,
            3,
            ast_id,
            Some(Arc::<str>::from(f.as_str())),
        ));
        let g = format!("=A{row}*2");
        let ast = parse(&g).unwrap();
        let ast_id = engine.intern_formula_ast(&ast);
        independent.push(FormulaIngestRecord::new(
            row,
            10,
            ast_id,
            Some(Arc::<str>::from(g.as_str())),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(
            "Sheet1",
            records.into_iter().chain(independent).collect(),
        )])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);

    engine.evaluate_all().unwrap();
    let st = engine.baseline_stats();
    // A span family that transitively reads a cycle member is demoted to
    // legacy vertices (G8); the independent family keeps span treatment.
    assert_eq!(st.formula_plane_cycle_member_span_demotions, 1);
    assert_eq!(st.formula_plane_active_span_count, 1);
    assert_eq!(num(&engine, "Sheet1", 120, 10), 240.0);
    assert_eq!(num(&engine, "Sheet1", 1, 5), 3.0);
    assert_eq!(num(&engine, "Sheet1", 1, 3), 8.0); // 2*2 + 1 + 3
    assert_eq!(num(&engine, "Sheet1", 120, 3), 4.0 + 120.0 + 3.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);

    // No-change recalc: the SCC is reused.
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
    assert_eq!(num(&engine, "Sheet1", 120, 3), 4.0 + 120.0 + 3.0);

    // Edit the SCC input: the SCC re-runs and the span cells follow.
    set_value(&mut engine, "Sheet1", 1, 7, LiteralValue::Number(9.0));
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 0, 0));
    assert_eq!(num(&engine, "Sheet1", 1, 5), 9.0);
    assert_eq!(num(&engine, "Sheet1", 1, 6), 9.0);
    assert_eq!(num(&engine, "Sheet1", 1, 3), 4.0 + 1.0 + 9.0);
    assert_eq!(num(&engine, "Sheet1", 120, 3), 4.0 + 120.0 + 9.0);
}

/* ═══════════════ 8–13: reuse must NOT happen ═══════════════════════════ */

/// Runs `build` and asserts that the recalc after the first one re-runs the
/// SCC (no retention).
fn assert_never_retained(name: &str, build: impl Fn(&mut Engine<TestWorkbook>), cfg: EvalConfig) {
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    build(&mut engine);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.baseline_stats().retained_scc_members,
        0,
        "{name}: nothing may be retained"
    );
    engine.evaluate_all().unwrap();
    assert_eq!(
        reuse_telemetry(&engine),
        (1, 0, 0),
        "{name}: the SCC must re-run"
    );
}

#[test]
fn tolerance_only_convergence_is_not_retained() {
    let _epoch = epoch_stable();
    // A1 = 0.5*B1+10, B1 = A1 converges geometrically on 20 but stops as
    // soon as |Δ| < max_change — the last delta is nonzero.
    assert_never_retained(
        "tolerance-only",
        |engine| {
            set_formula(engine, "Sheet1", 1, 1, "=0.5*B1+10");
            set_formula(engine, "Sheet1", 1, 2, "=A1");
        },
        iterate_cfg(100, 0.001),
    );

    // …and the residual really is sub-tolerance but nonzero.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=0.5*B1+10");
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1");
    engine.evaluate_all().unwrap();
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.converged_sccs, 1);
    assert!(
        t.max_abs_delta_at_stop > 0.0 && t.max_abs_delta_at_stop < 0.001,
        "residual {}",
        t.max_abs_delta_at_stop
    );
    assert!((num(&engine, "Sheet1", 1, 1) - 20.0).abs() < 0.001);
}

#[test]
fn capped_divergent_pair_is_not_retained() {
    let _epoch = epoch_stable();
    assert_never_retained(
        "divergent pair",
        |engine| {
            set_formula(engine, "Sheet1", 1, 1, "=A2+1");
            set_formula(engine, "Sheet1", 2, 1, "=A1+1");
        },
        iterate_cfg(10, 0.001),
    );
}

#[test]
fn accumulator_still_adds_its_input_exactly_once_per_recalc() {
    let _epoch = epoch_stable();
    // The `max_iterations: 1` accumulator caps by construction, so it can
    // never be retained — the §7.6 contract is unchanged by retention.
    let mut engine = iterate_engine(1, 0.001);
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Number(5.0));
    set_formula(&mut engine, "Sheet1", 1, 2, "=B1+A1");
    for (recalc, expected) in [(1u32, 5.0), (2, 10.0), (3, 15.0)] {
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 1, 2), expected, "recalc {recalc}");
        assert_eq!(reuse_telemetry(&engine), (1, 0, 0), "recalc {recalc}");
        assert_eq!(engine.baseline_stats().retained_scc_members, 0);
    }
}

#[test]
fn volatile_member_is_never_retained_even_on_an_exact_fixed_point() {
    let _epoch = epoch_stable();
    // RAND() is neutralized to a constant 0, so the SCC converges with
    // Δ = 0 — but a volatile member disqualifies retention.
    assert_never_retained(
        "RAND in cycle",
        |engine| {
            set_formula(engine, "Sheet1", 1, 2, "=0*RAND()+0*C1");
            set_formula(engine, "Sheet1", 1, 3, "=B1");
        },
        iterate_cfg(100, 0.001),
    );

    // NOW() under the deterministic clock: the value is literally identical
    // on every recalc and still must not be retained.
    assert_never_retained(
        "NOW in cycle (deterministic)",
        |engine| {
            set_formula(engine, "Sheet1", 1, 2, "=NOW()+0*C1");
            set_formula(engine, "Sheet1", 1, 3, "=B1");
        },
        deterministic_iterate_cfg(100, 0.001),
    );
}

#[test]
fn dynamic_reference_member_is_never_retained() {
    let _epoch = epoch_stable();
    // INDIRECT closes the cycle through a virtual edge; the member is
    // `is_dynamic`, so the SCC keeps the per-recalc redirty.
    assert_never_retained(
        "INDIRECT in cycle",
        |engine| {
            set_formula(engine, "Sheet1", 1, 1, "=0*INDIRECT(\"B1\")");
            set_formula(engine, "Sheet1", 1, 2, "=A1");
        },
        iterate_cfg(100, 0.001),
    );

    // OFFSET is dynamic too.
    assert_never_retained(
        "OFFSET in cycle",
        |engine| {
            set_formula(engine, "Sheet1", 1, 1, "=0*OFFSET(A1,0,1)");
            set_formula(engine, "Sheet1", 1, 2, "=A1");
        },
        iterate_cfg(100, 0.001),
    );
}

#[test]
fn nan_identity_convergence_is_not_retained() {
    let _epoch = epoch_stable();
    use crate::args::ArgSchema;
    use crate::function::{FnCaps, Function};
    use crate::traits::{ArgumentHandle, FunctionContext};

    // No built-in path leaks a `Number(NaN)` into a cell any more (SUM and
    // the arithmetic operators both sanitize non-finite results to `#NUM!`,
    // see `iterate_corpus_numeric`). NANPROBE(x) reads its argument — so the
    // live cycle is witnessed — and returns NaN, which is the only shape
    // that exercises the spec-§6 identical-bit NaN rule end to end.
    #[derive(Debug)]
    struct NanProbeFn;
    impl Function for NanProbeFn {
        fn caps(&self) -> FnCaps {
            FnCaps::empty()
        }
        fn name(&self) -> &'static str {
            "NANPROBE"
        }
        fn arg_schema(&self) -> &'static [ArgSchema] {
            static SCHEMA: std::sync::LazyLock<Vec<ArgSchema>> =
                std::sync::LazyLock::new(|| vec![ArgSchema::any()]);
            &SCHEMA
        }
        fn eval<'a, 'b, 'c>(
            &self,
            args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            let _ = args[0].value()?;
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                f64::NAN,
            )))
        }
    }

    let wb = TestWorkbook::new().with_function(Arc::new(NanProbeFn));
    let mut engine = Engine::new(wb, iterate_cfg(100, 0.001));
    set_formula(&mut engine, "Sheet1", 1, 1, "=NANPROBE(B1)");
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1");
    engine.evaluate_all().unwrap();

    let t = engine.last_cycle_telemetry();
    assert!(
        t.nan_converged > 0,
        "expected NaN identity, telemetry {t:?}"
    );
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(t.capped_sccs, 0);
    assert_eq!(
        engine.baseline_stats().retained_scc_members,
        0,
        "NaN identity is explicitly excluded from retention"
    );

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 0, 0));
}

#[test]
fn array_result_member_excluded_mid_iteration_is_not_retained() {
    let _epoch = epoch_stable();
    // §7.9: a member that would become a spill anchor inside the SCC is
    // stamped #CIRC and excluded instead of spilling.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=SEQUENCE(2)+0*A2");
    set_formula(&mut engine, "Sheet1", 2, 1, "=A1");
    engine.evaluate_all().unwrap();
    assert_eq!(err_kind(&engine, "Sheet1", 1, 1), ExcelErrorKind::Circ);
    assert_eq!(err_kind(&engine, "Sheet1", 2, 1), ExcelErrorKind::Circ);
    assert_eq!(
        engine.baseline_stats().retained_scc_members,
        0,
        "an excluded member never reaches the retention check"
    );

    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().reused_sccs, 0);
    assert_eq!(err_kind(&engine, "Sheet1", 1, 1), ExcelErrorKind::Circ);
}

/* ═══════════════ 14–22: invalidation doors ════════════════════════════ */

/// Builds the input-driven pair on row 1, runs to retention, and asserts a
/// no-change recalc reuses it. Leaves the engine ready for a door test.
fn retained_engine(input: f64) -> Engine<TestWorkbook> {
    let mut engine = iterate_engine(100, 0.001);
    build_input_driven_pair(&mut engine, 1, input);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), input);
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
    engine
}

#[test]
fn external_precedent_edit_reruns_the_retained_scc() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(8.0));
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 0, 0));
    assert_eq!(num(&engine, "Sheet1", 1, 1), 8.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 8.0);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2), "retained again");
}

#[test]
fn same_value_rewrite_of_a_precedent_still_reruns_the_scc() {
    let _epoch = epoch_stable();
    // Today's dirty semantics: writing a cell dirties its dependents even if
    // the value is unchanged. Retention follows the dirty graph, so the SCC
    // re-runs (and lands on the same fixed point).
    let mut engine = retained_engine(3.0);
    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(3.0));
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 0, 0));
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
}

#[test]
fn formula_change_inside_a_member_reruns_the_retained_scc() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    // B1 = A1 becomes B1 = A1*10: still a member, different formula.
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1*10");
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 0, 0));
    // A1 = IF(B1>=D1,D1,B1+1) with B1 = 10*A1 and D1 = 3: A1 = 3 (guard
    // holds because B1 = 30 ≥ 3), B1 = 30 — an exact fixed point again.
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 30.0);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
}

#[test]
fn overwriting_a_member_with_a_literal_dissolves_the_retained_scc() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    set_value(&mut engine, "Sheet1", 1, 2, LiteralValue::Number(42.0));
    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().static_sccs, 0);
    assert_eq!(engine.last_cycle_telemetry().iterated_sccs, 0);
    // A1 = IF(42>=3,3,43) = 3, B1 is the literal.
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 42.0);

    // …and no work is scheduled ever again (the pre-#368
    // `breaking_the_cycle_stops_the_per_recalc_redirty` contract).
    for _ in 0..3 {
        let res = engine.evaluate_all().unwrap();
        assert_eq!(res.computed_vertices, 0, "no perpetual redirty leak");
        assert_eq!(engine.last_cycle_telemetry().static_sccs, 0);
        assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
        assert_eq!(num(&engine, "Sheet1", 1, 2), 42.0);
    }
}

#[test]
fn overwriting_a_member_with_a_literal_prunes_the_retained_set() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    set_value(&mut engine, "Sheet1", 1, 2, LiteralValue::Number(42.0));
    engine.evaluate_all().unwrap();
    assert_eq!(engine.baseline_stats().retained_scc_members, 0);

    for _ in 0..3 {
        engine.evaluate_all().unwrap();
        assert_eq!(engine.baseline_stats().retained_scc_members, 0);
        assert_eq!(engine.last_cycle_telemetry().reused_sccs, 0);
    }
}

#[test]
fn a_new_formula_joining_the_scc_reruns_the_enlarged_scc() {
    let _epoch = epoch_stable();
    // SCC {A1, B1} with B1 = MAX(A1, C1) and C1 empty. Giving C1 a formula
    // that reads B1 makes it a third member.
    let mut engine = iterate_engine(100, 0.001);
    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(3.0));
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(B1>=D1,D1,B1+1)");
    set_formula(&mut engine, "Sheet1", 1, 2, "=MAX(A1,C1)");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));

    set_formula(&mut engine, "Sheet1", 1, 3, "=B1*0");
    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().iterated_sccs, 1);
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 3.0);
    assert_eq!(num(&engine, "Sheet1", 1, 3), 0.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 3);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 3));
}

#[test]
fn defined_name_edits_reach_retained_members_through_the_name_edge() {
    let _epoch = epoch_stable();
    // The cap lives in a workbook-scoped name instead of a cell (#365).
    let mut engine = iterate_engine(100, 0.001);
    engine
        .define_name(
            "Cap",
            NamedDefinition::Literal(LiteralValue::Number(3.0)),
            NameScope::Workbook,
        )
        .unwrap();
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(B1>=Cap,Cap,B1+1)");
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));

    engine
        .update_name(
            "Cap",
            NamedDefinition::Literal(LiteralValue::Number(6.0)),
            NameScope::Workbook,
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 0, 0));
    assert_eq!(num(&engine, "Sheet1", 1, 1), 6.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 6.0);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));

    engine.delete_name("Cap", NameScope::Workbook).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().reused_sccs, 0);
    assert_eq!(err_kind(&engine, "Sheet1", 1, 1), ExcelErrorKind::Name);
}

#[test]
fn downstream_retained_scc_reruns_when_the_upstream_one_moves() {
    let _epoch = epoch_stable();
    // Upstream SCC {A1, B1} pinned to D1; downstream SCC {A3, B3} pinned to
    // the upstream output B1.
    let mut engine = iterate_engine(100, 0.001);
    build_input_driven_pair(&mut engine, 1, 3.0);
    set_formula(&mut engine, "Sheet1", 3, 1, "=IF(B3>=B1,B1,B3+1)");
    set_formula(&mut engine, "Sheet1", 3, 2, "=A3");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 3.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 4);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 2, 4));

    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(7.0));
    engine.evaluate_all().unwrap();
    assert_eq!(
        reuse_telemetry(&engine),
        (2, 0, 0),
        "both SCCs must re-run: the downstream one reads the upstream output"
    );
    assert_eq!(num(&engine, "Sheet1", 1, 1), 7.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 7.0);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 2, 4));
}

#[test]
fn structural_edits_reach_retained_members_and_preserve_their_values() {
    let _epoch = epoch_stable();
    // Insert a row ABOVE the SCC: members shift to row 2, values survive
    // (no restart from the Empty→0 seed).
    let mut engine = retained_engine(3.0);
    engine.insert_rows("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 3.0);
    assert_eq!(num(&engine, "Sheet1", 2, 2), 3.0);
    assert_eq!(num(&engine, "Sheet1", 2, 4), 3.0);

    // Delete the inserted row again: back to row 1, still exact.
    engine.delete_rows("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 3.0);

    // An unrelated row below is deleted: the SCC stays retained/reused.
    set_value(&mut engine, "Sheet1", 20, 1, LiteralValue::Number(1.0));
    engine.evaluate_all().unwrap();
    engine.delete_rows("Sheet1", 20, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    assert_eq!(num(&engine, "Sheet1", 1, 2), 3.0);
}

#[test]
fn row_insert_inside_a_retained_members_range_extends_it_and_keeps_values() {
    let _epoch = epoch_stable();
    // Self-loop member reading a range: A1 = IF(A1>=SUM(B1:B3),SUM(B1:B3),A1+1)
    // climbs 1, 2, 3 and reproduces 3 exactly (SUM(B1:B3) = 1 + 0 + 2).
    let mut engine = iterate_engine(100, 0.001);
    set_value(&mut engine, "Sheet1", 1, 2, LiteralValue::Number(1.0));
    set_value(&mut engine, "Sheet1", 2, 2, LiteralValue::Number(0.0));
    set_value(&mut engine, "Sheet1", 3, 2, LiteralValue::Number(2.0));
    set_formula(
        &mut engine,
        "Sheet1",
        1,
        1,
        "=IF(A1>=SUM(B1:B3),SUM(B1:B3),A1+1)",
    );
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 1);
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 1));

    // Insert a row INSIDE the member's range: B1:B3 stretches to B1:B4 with
    // the same contents (a new blank B2), the member stays at A1 and keeps
    // its value — no restart from the Empty→0 seed.
    engine.insert_rows("Sheet1", 2, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        reuse_telemetry(&engine),
        (1, 0, 0),
        "the range edit re-runs it"
    );
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 1));
}

#[test]
fn timestamp_pattern_keeps_its_stamp_across_a_row_insert() {
    let _epoch = epoch_stable();
    // B1 = IF(A1="","",IF(B1="",NOW(),B1)) with a fixed clock: once A1 is
    // non-empty the stamp is written once and re-read forever, including
    // across a structural shift.
    let mut engine = Engine::new(TestWorkbook::new(), deterministic_iterate_cfg(100, 0.001));
    set_formula(
        &mut engine,
        "Sheet1",
        1,
        2,
        "=IF(A1=\"\",\"\",IF(B1=\"\",NOW(),B1))",
    );
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

    engine.insert_rows("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        num(&engine, "Sheet1", 2, 2),
        stamped,
        "the stamp must survive the row insert"
    );
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 2), stamped);
}

/* ───────────────────── config-fingerprint doors ───────────────────────── */

#[test]
fn switching_the_cycle_policy_to_error_reruns_retained_members_as_circ() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    engine.config.cycle.policy = CyclePolicy::Error;
    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().reused_sccs, 0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 0);
    assert_eq!(err_kind(&engine, "Sheet1", 1, 1), ExcelErrorKind::Circ);
    assert_eq!(err_kind(&engine, "Sheet1", 1, 2), ExcelErrorKind::Circ);
}

#[test]
fn changing_max_change_reruns_retained_members() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    engine.config.cycle.policy = CyclePolicy::Iterate {
        max_iterations: 100,
        max_change: 0.5,
    };
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 0, 0));
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);

    // Retained again under the new fingerprint.
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
}

#[test]
fn changing_the_date_system_reruns_retained_members() {
    let _epoch = epoch_stable();
    use formualizer_common::DateSystem;
    let mut engine = retained_engine(3.0);
    let flipped = match engine.config.date_system {
        DateSystem::Excel1900 => DateSystem::Excel1904,
        _ => DateSystem::Excel1900,
    };
    engine.config.date_system = flipped;
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 0, 0));
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);

    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
}

#[test]
fn registry_changes_rerun_only_retained_members_that_call_a_changed_function() {
    let _epoch = epoch_bumper();
    use crate::args::ArgSchema;
    use crate::function::{FnCaps, Function};
    use crate::traits::{ArgumentHandle, FunctionContext};

    /// `SCCREUSEPROBE(x)` reads its argument (so the live cycle is
    /// witnessed) and returns `x + delta`; re-registering it with a new
    /// `delta` is a semantic change to a function a member calls.
    #[derive(Debug)]
    struct SccReuseProbe {
        delta: f64,
    }
    impl Function for SccReuseProbe {
        fn caps(&self) -> FnCaps {
            FnCaps::empty()
        }
        fn name(&self) -> &'static str {
            "SCCREUSEPROBE"
        }
        fn arg_schema(&self) -> &'static [ArgSchema] {
            static SCHEMA: std::sync::LazyLock<Vec<ArgSchema>> =
                std::sync::LazyLock::new(|| vec![ArgSchema::any()]);
            &SCHEMA
        }
        fn eval<'a, 'b, 'c>(
            &self,
            args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            let x = match args[0].value()?.into_literal() {
                LiteralValue::Number(n) => n,
                LiteralValue::Int(i) => i as f64,
                _ => 0.0,
            };
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                x + self.delta,
            )))
        }
    }

    /// An unrelated function nobody calls.
    #[derive(Debug)]
    struct SccReuseEpochProbe;
    impl Function for SccReuseEpochProbe {
        fn caps(&self) -> FnCaps {
            FnCaps::empty()
        }
        fn name(&self) -> &'static str {
            "SCCREUSEEPOCHPROBE"
        }
        fn arg_schema(&self) -> &'static [ArgSchema] {
            &[]
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
        }
    }

    // A1 = IF(B1>=D1, D1, SCCREUSEPROBE(B1)) climbs by `delta` until it
    // reaches D1 and then holds: an exact fixed point at D1.
    crate::function_registry::register_function(Arc::new(SccReuseProbe { delta: 1.0 }));
    let mut engine = iterate_engine(100, 0.001);
    set_value(&mut engine, "Sheet1", 1, 4, LiteralValue::Number(3.0));
    set_formula(
        &mut engine,
        "Sheet1",
        1,
        1,
        "=IF(B1>=D1,D1,SCCREUSEPROBE(B1))",
    );
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));

    // Registering a function no member calls does not disturb retention.
    let before = crate::function_registry::semantic_epoch();
    crate::function_registry::register_function(Arc::new(SccReuseEpochProbe));
    assert!(crate::function_registry::semantic_epoch() > before);
    engine.evaluate_all().unwrap();
    assert_eq!(
        reuse_telemetry(&engine),
        (0, 1, 2),
        "unrelated registration"
    );
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);

    // Replacing the function a member calls re-runs the SCC under the new
    // semantics: with delta 0 the climb stalls and the pair settles where
    // it stands (3.0 still satisfies the guard, so the value holds), but the
    // SCC must actually run to find that out.
    crate::function_registry::register_function(Arc::new(SccReuseProbe { delta: 0.0 }));
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (1, 0, 0), "changed callee");
    assert_eq!(num(&engine, "Sheet1", 1, 1), 3.0);

    // Retained again afterwards.
    engine.evaluate_all().unwrap();
    assert_eq!(reuse_telemetry(&engine), (0, 1, 2));
}

#[test]
fn deleting_retained_members_stops_all_scc_work_without_panicking() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    assert_eq!(engine.baseline_stats().retained_scc_members, 2);

    // Delete the whole row: both members and the input vanish.
    engine.delete_rows("Sheet1", 1, 1).unwrap();
    for _ in 0..3 {
        let res = engine.evaluate_all().unwrap();
        assert_eq!(res.computed_vertices, 0, "no perpetual redirty leak");
        assert_eq!(engine.last_cycle_telemetry().iterated_sccs, 0);
        assert_eq!(engine.get_cell_value("Sheet1", 1, 1), None);
        assert_eq!(engine.get_cell_value("Sheet1", 1, 2), None);
    }
}

#[test]
fn deleting_retained_members_prunes_the_retained_set() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    engine.delete_rows("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.baseline_stats().retained_scc_members, 0);
    assert_eq!(engine.last_cycle_telemetry().reused_sccs, 0);

    engine.evaluate_all().unwrap();
    assert_eq!(engine.baseline_stats().retained_scc_members, 0);
    assert_eq!(engine.last_cycle_telemetry().reused_sccs, 0);
}

#[test]
fn clearing_a_retained_member_dissolves_the_cycle_and_keeps_values_correct() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    set_value(&mut engine, "Sheet1", 1, 2, LiteralValue::Empty);
    engine.evaluate_all().unwrap();
    // A1 = IF(B1>=D1,D1,B1+1) with B1 blank → IF(0>=3,3,1) = 1.
    assert_eq!(num(&engine, "Sheet1", 1, 1), 1.0);
    assert_eq!(engine.last_cycle_telemetry().iterated_sccs, 0);
    for _ in 0..2 {
        let res = engine.evaluate_all().unwrap();
        assert_eq!(res.computed_vertices, 0, "no perpetual redirty leak");
        assert_eq!(num(&engine, "Sheet1", 1, 1), 1.0);
    }
}

#[test]
fn clearing_a_retained_member_prunes_the_retained_set() {
    let _epoch = epoch_stable();
    let mut engine = retained_engine(3.0);
    set_value(&mut engine, "Sheet1", 1, 2, LiteralValue::Empty);
    engine.evaluate_all().unwrap();
    assert_eq!(engine.baseline_stats().retained_scc_members, 0);
    for _ in 0..2 {
        engine.evaluate_all().unwrap();
        assert_eq!(engine.baseline_stats().retained_scc_members, 0);
        assert_eq!(engine.last_cycle_telemetry().reused_sccs, 0);
    }
}
