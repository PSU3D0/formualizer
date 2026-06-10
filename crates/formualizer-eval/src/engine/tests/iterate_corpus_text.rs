//! Iterate edge corpus — text accumulation and type-oscillation cycles
//! (RFC #112/#113, spec §6 type rules).
//!
//! Text never "converges" while it keeps changing (exact-equality rule), so
//! string-growth cycles always run to the cap — including on every later
//! recalc (perf note for the report: O(cap² · seed) bytes copied per recalc
//! for a self-concat with a long seed).

use crate::engine::{CycleConfig, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

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

fn text(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> String {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Text(s)) => s,
        other => panic!("expected text at {sheet} r{row}c{col}, got {other:?}"),
    }
}

fn num(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> f64 {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet} r{row}c{col}, got {other:?}"),
    }
}

/* ───────────────────────── string growth cycles ───────────────────────── */

#[test]
fn self_concat_grows_one_char_per_pass_and_always_caps() {
    // A1 = A1 & "x": Empty seeds as "" in text context; every pass appends
    // one char, text never repeats → capped, length == cap.
    let mut engine = iterate_engine(10, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=A1&\"x\"");
    engine.evaluate_all().unwrap();
    assert_eq!(text(&engine, "Sheet1", 1, 1), "x".repeat(10));
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(t.converged_sccs, 0);
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.settle_passes_total, 10);

    // §4 persistence: the next recalc keeps appending to the previous text.
    engine.evaluate_all().unwrap();
    assert_eq!(text(&engine, "Sheet1", 1, 1), "x".repeat(20));
    assert_eq!(engine.last_cycle_telemetry().capped_sccs, 1);
}

#[test]
fn self_concat_with_long_seed_allocates_linearly_per_pass_and_survives() {
    // A1 = A1 & B1 with an 8 KiB seed: pass k holds k·8 KiB; at cap 64 the
    // final string is 512 KiB and total bytes copied ≈ cap²/2 · seed
    // (~16 MiB) — memory-sane, but quadratic in the cap (report flag).
    let seed = "s".repeat(8 * 1024);
    let cap = 64u32;
    let mut engine = iterate_engine(cap, 0.001);
    set_value(
        &mut engine,
        "Sheet1",
        1,
        2,
        LiteralValue::Text(seed.clone()),
    );
    set_formula(&mut engine, "Sheet1", 1, 1, "=A1&B1");
    engine.evaluate_all().unwrap();
    let got = text(&engine, "Sheet1", 1, 1);
    assert_eq!(got.len(), cap as usize * seed.len());
    assert!(got.starts_with(&seed) && got.ends_with(&seed));
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.capped_sccs, 1);
    assert_eq!(t.settle_passes_total, cap as usize);
}

#[test]
fn concat_growth_feeds_downstream_len_with_final_value_only() {
    // B1 = LEN(A1) sits outside the SCC: it must see only the post-cap final
    // string each recalc (spec §3.6), and the per-recalc redirty must keep it
    // fresh on the second recalc.
    let mut engine = iterate_engine(5, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=CONCAT(A1,\"ab\")");
    set_formula(&mut engine, "Sheet1", 1, 2, "=LEN(A1)");
    engine.evaluate_all().unwrap();
    assert_eq!(
        num(&engine, "Sheet1", 1, 2),
        10.0,
        "LEN sees the capped final value (5 passes × 2 chars)"
    );
    engine.evaluate_all().unwrap();
    assert_eq!(
        num(&engine, "Sheet1", 1, 2),
        20.0,
        "second recalc: downstream stays fresh via the iterative redirty"
    );
}

/* ─────────────────────── type-oscillation cycles ──────────────────────── */

#[test]
fn three_way_number_text_error_oscillation_caps_without_panic() {
    // A1 cycles Error → Number → Text with period 3 driven by B1's type:
    //   B1 error → 1 (Number); B1 numeric → "t" (Text); anything else →
    //   1/0 (#DIV/0!). B1 = A1 echoes within the same pass (member order
    //   A1 then B1). Every pass is a type transition for both members →
    //   caps, never converges, never panics — across several cap parities
    //   so each phase of the period-3 orbit is the stopping point at least
    //   once.
    for cap in [3u32, 4, 5, 6, 7] {
        let mut engine = iterate_engine(cap, f64::MAX / 2.0);
        set_formula(
            &mut engine,
            "Sheet1",
            1,
            1,
            "=IF(ISERROR(B1),1,IF(ISNUMBER(B1),\"t\",1/0))",
        );
        set_formula(&mut engine, "Sheet1", 1, 2, "=A1");
        engine.evaluate_all().unwrap();
        let t = engine.last_cycle_telemetry();
        assert_eq!(t.iterated_sccs, 1, "cap {cap}");
        assert_eq!(t.converged_sccs, 0, "cap {cap}: must never converge");
        assert_eq!(t.capped_sccs, 1, "cap {cap}");
        assert_eq!(t.settle_passes_total, cap as usize, "cap {cap}");
        // The cell holds one of the three oscillation phases — never #CIRC.
        let v = engine.get_cell_value("Sheet1", 1, 1).unwrap();
        let phase_ok = matches!(
            &v,
            LiteralValue::Int(1) | LiteralValue::Number(_) | LiteralValue::Text(_)
        ) || matches!(&v, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Div);
        assert!(phase_ok, "cap {cap}: unexpected phase value {v:?}");
    }
}

#[test]
fn boolean_not_cycle_oscillates_and_stops_on_cap_parity() {
    // A1 = NOT(B1), B1 = A1: NOT(Empty) = TRUE, then both flip every pass
    // (Boolean≠Boolean never converges). Final value is determined by cap
    // parity — pinned for determinism (§11).
    for (cap, expected) in [(1u32, true), (2, false), (3, true), (4, false)] {
        let mut engine = iterate_engine(cap, 0.001);
        set_formula(&mut engine, "Sheet1", 1, 1, "=NOT(B1)");
        set_formula(&mut engine, "Sheet1", 1, 2, "=A1");
        engine.evaluate_all().unwrap();
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Boolean(expected)),
            "cap {cap}"
        );
        let t = engine.last_cycle_telemetry();
        assert_eq!(t.capped_sccs, 1, "cap {cap}");
        assert_eq!(t.converged_sccs, 0, "cap {cap}");
    }
}

#[test]
fn empty_producing_cycle_member_converges_on_empty_vs_empty() {
    // B1 = IF(C1=0, "", C1), C1 = B1*1: B1 commits "" (Text) on pass 1...
    // pinned: the IF taken-branch literal "" is Text, so convergence runs on
    // the Text rule, not Empty-vs-Empty. C1 = ""*1 is a #VALUE! error which
    // then feeds B1=IF(#VALUE!...) → error propagation. Pass 3 reproduces
    // pass 2 exactly → converged on stable (Text, Error) values.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 2, "=IF(C1=0,\"\",C1)");
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1*1");
    engine.evaluate_all().unwrap();
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 1);
    assert_eq!(
        t.converged_sccs, 1,
        "stable mixed-type fixed point must converge"
    );
    assert_eq!(t.capped_sccs, 0);
}
