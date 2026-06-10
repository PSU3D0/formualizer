//! Stage 2 — SCC evaluation under `CycleDetection::Runtime` (RFC #112).
//!
//! Test inventory from `formualizer-stage2-scc-evaluation-design.md` §5 and
//! the spec's §7 Error-policy subset: phantom (guarded) cycles produce
//! values, live cycles produce `#CIRC!` with live-cycle-only blast radius,
//! and `CycleDetection::Static` (the default) stays byte-for-byte today's
//! behavior.

use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue, PackedSheetCell};
use formualizer_parse::parser::parse;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn runtime_cycle() -> CycleConfig {
    CycleConfig {
        detection: CycleDetection::Runtime,
        policy: CyclePolicy::Error,
    }
}

fn runtime_cfg() -> EvalConfig {
    EvalConfig::default()
        .with_cycle(runtime_cycle())
        .with_virtual_dep_telemetry(true)
}

fn runtime_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), runtime_cfg())
}

fn static_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
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

fn is_circ(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> bool {
    matches!(
        engine.get_cell_value(sheet, row, col),
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Circ
    )
}

/// Build the discussion-#99 guarded pair: A1 guard, A2/A3 the static SCC.
fn build_99_pair(engine: &mut Engine<TestWorkbook>, guard: bool) {
    set_value(engine, "Sheet1", 1, 1, LiteralValue::Boolean(guard));
    set_formula(engine, "Sheet1", 2, 1, "=IF(A1,555,A3)");
    set_formula(engine, "Sheet1", 3, 1, "=IF(A1,A2,999)");
}

/* ───────────────────────── 7.1 self-reference ───────────────────────── */

/// Engine rule that PRE-EMPTS spec §7.1's eval-time `#CIRC!`: a direct
/// self-reference (`=A1+1` in A1, or an expanded range containing the cell)
/// is rejected when the formula is SET ("Self-reference detected"). Runtime
/// mode must not change that edit-time rule. Eval-time self-loop handling is
/// still exercised through dynamic refs (see the INDIRECT tests) and the
/// `live_graph` unit tests.
#[test]
fn direct_self_reference_rejected_at_ingest_in_both_modes() {
    for cfg in [EvalConfig::default(), runtime_cfg()] {
        let mut engine = Engine::new(TestWorkbook::new(), cfg);
        let err = engine
            .set_cell_formula("Sheet1", 1, 1, parse("=A1+1").unwrap())
            .unwrap_err();
        assert_eq!(err.kind, ExcelErrorKind::Circ);
        // Dense range covering the cell expands to direct deps → same rule.
        let err = engine
            .set_cell_formula("Sheet1", 5, 1, parse("=SUM(A1:A10)").unwrap())
            .unwrap_err();
        assert_eq!(err.kind, ExcelErrorKind::Circ);
    }
}

#[test]
fn live_two_cycle_is_circ_via_evaluate_all_and_evaluate_cell() {
    let mut engine = runtime_engine();
    set_formula(&mut engine, "Sheet1", 1, 1, "=B1+1");
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1+1");
    let res = engine.evaluate_all().unwrap();
    assert!(is_circ(&engine, "Sheet1", 1, 1));
    assert!(is_circ(&engine, "Sheet1", 1, 2));
    assert_eq!(res.cycle_errors, 1);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.static_sccs, 1);
    assert_eq!(t.phantom_sccs, 0);
    assert_eq!(t.live_cycles_witnessed, 1);
    assert_eq!(t.circ_cells_stamped, 2);

    let mut engine = runtime_engine();
    set_formula(&mut engine, "Sheet1", 1, 1, "=B1+1");
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1+1");
    let v = engine.evaluate_cell("Sheet1", 1, 1).unwrap();
    assert!(
        matches!(v, Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Circ),
        "got {v:?}"
    );
}

/* ──────────────── 7.2 #99 guarded pair — both polarities ─────────────── */

#[test]
fn guarded_pair_99_evaluates_to_values_under_runtime() {
    let mut engine = runtime_engine();
    build_99_pair(&mut engine, true);
    let res = engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 555.0);
    assert_eq!(res.cycle_errors, 0, "phantom SCC must not count as a cycle");
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.static_sccs, 1);
    assert_eq!(t.phantom_sccs, 1);
    assert_eq!(t.live_cycles_witnessed, 0);
    assert_eq!(t.circ_cells_stamped, 0);
    // A2 is ordered first and A3 reads it fresh: exactly one pass.
    assert_eq!(t.settle_passes_total, 1);
}

#[test]
fn guarded_pair_99_opposite_polarity_settles_to_values() {
    // Same pair but the live edge points at the *later*-ordered member:
    // A2 = IF(A1, A3, 999), A3 = IF(A1, 555, A2). With A1=TRUE the live
    // edge is A2→A3 and A2 runs first, so one settle re-eval is needed.
    let mut engine = runtime_engine();
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Boolean(true));
    set_formula(&mut engine, "Sheet1", 2, 1, "=IF(A1,A3,999)");
    set_formula(&mut engine, "Sheet1", 3, 1, "=IF(A1,555,A2)");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 555.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.phantom_sccs, 1);
    assert_eq!(t.settle_passes_total, 2, "polarity flip costs one settle");
}

#[test]
fn guarded_pair_99_guard_flip_between_recalcs_reverses_values() {
    let mut engine = runtime_engine();
    build_99_pair(&mut engine, true);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 555.0);

    // Flip the guard: live edges reverse, values follow.
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Boolean(false));
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 999.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 999.0);
    assert_eq!(engine.last_cycle_telemetry().phantom_sccs, 1);

    // And back.
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Boolean(true));
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 555.0);
}

#[test]
fn guarded_pair_99_via_evaluate_cell_both_polarities() {
    // Demand path (evaluate_until under the hood): the demand closure pulls
    // the whole SCC via static deps and evaluates it as one task.
    let mut engine = runtime_engine();
    build_99_pair(&mut engine, true);
    let v = engine.evaluate_cell("Sheet1", 2, 1).unwrap();
    assert_eq!(v, Some(LiteralValue::Number(555.0)));
    assert_eq!(num(&engine, "Sheet1", 3, 1), 555.0);

    let mut engine = runtime_engine();
    build_99_pair(&mut engine, false);
    let v = engine.evaluate_cell("Sheet1", 3, 1).unwrap();
    assert_eq!(v, Some(LiteralValue::Number(999.0)));
    assert_eq!(num(&engine, "Sheet1", 2, 1), 999.0);
}

#[test]
fn guarded_chains_three_and_five_cells_mixed_polarity() {
    // 3-cell guarded ring: every static edge exists, live edges form a chain.
    let mut engine = runtime_engine();
    set_value(&mut engine, "Sheet1", 1, 7, LiteralValue::Boolean(true)); // G1
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(G1,100,A2)");
    set_formula(&mut engine, "Sheet1", 2, 1, "=IF(G1,A1,A3)");
    set_formula(&mut engine, "Sheet1", 3, 1, "=IF(G1,A2,100)");
    engine.evaluate_all().unwrap();
    for r in 1..=3 {
        assert_eq!(num(&engine, "Sheet1", r, 1), 100.0, "row {r}");
    }
    assert_eq!(engine.last_cycle_telemetry().phantom_sccs, 1);

    // Flip: live edges reverse direction (reads now go down the column,
    // against member order → settling required).
    set_value(&mut engine, "Sheet1", 1, 7, LiteralValue::Boolean(false));
    engine.evaluate_all().unwrap();
    for r in 1..=3 {
        assert_eq!(num(&engine, "Sheet1", r, 1), 100.0, "row {r} after flip");
    }

    // 5-cell ring with mixed polarity guards.
    let mut engine = runtime_engine();
    set_value(&mut engine, "Sheet1", 1, 7, LiteralValue::Boolean(true));
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(G1,42,B5)");
    set_formula(&mut engine, "Sheet1", 2, 1, "=IF(G1,A1,B1)"); // reads up (fresh)
    set_formula(&mut engine, "Sheet1", 3, 1, "=IF(NOT(G1),B1,A2)"); // reads up (fresh)
    set_formula(&mut engine, "Sheet1", 4, 1, "=IF(G1,A5,A3)"); // reads DOWN (stale)
    set_formula(&mut engine, "Sheet1", 5, 1, "=IF(G1,A3,A4)"); // reads up
    // Static ring closure: B5 referenced by A1's untaken branch.
    set_formula(&mut engine, "Sheet1", 5, 2, "=A1");
    engine.evaluate_all().unwrap();
    for r in 1..=5 {
        assert_eq!(num(&engine, "Sheet1", r, 1), 42.0, "row {r}");
    }
}

/* ─────────────── 7.3 guard inside the cycle (always live) ────────────── */

#[test]
fn guard_reading_cycle_member_is_a_live_cycle() {
    // Spec §7.3's literal example (`=IF(A1, A2+1, 5)` in A2) contains a
    // direct self-reference and is rejected at ingest; the same semantics —
    // a guard read that is itself a live edge into the cycle — is built with
    // two cells: A1's GUARD always reads A2, and A2 always reads A1, so the
    // live subgraph is cyclic in every guard state.
    for guard_seed in [0.0, 9.0] {
        let mut engine = runtime_engine();
        set_value(
            &mut engine,
            "Sheet1",
            1,
            2,
            LiteralValue::Number(guard_seed),
        );
        set_formula(&mut engine, "Sheet1", 1, 1, "=IF(A2>0,A2+1,5)");
        set_formula(&mut engine, "Sheet1", 2, 1, "=A1+B1");
        engine.evaluate_all().unwrap();
        assert!(is_circ(&engine, "Sheet1", 1, 1), "seed {guard_seed}");
        assert!(is_circ(&engine, "Sheet1", 2, 1), "seed {guard_seed}");
        assert_eq!(engine.last_cycle_telemetry().live_cycles_witnessed, 1);
    }
}

/* ─────────────── 7.4 arithmetic routing — always live ────────────────── */

#[test]
fn arithmetic_routing_stays_circ() {
    // Both operands of `+`/`*` always evaluate: the reads genuinely occur,
    // so the cycle is live regardless of the mixing weight g (A1).
    let mut engine = runtime_engine();
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Number(0.0)); // g
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1*99+(1-A1)*C1"); // B1
    set_formula(&mut engine, "Sheet1", 1, 3, "=A1*B1+(1-A1)*7"); // C1
    engine.evaluate_all().unwrap();
    assert!(is_circ(&engine, "Sheet1", 1, 2));
    assert!(is_circ(&engine, "Sheet1", 1, 3));
}

/* ─────────────────────── 7.8 range self-inclusion ────────────────────── */

#[test]
fn range_mediated_live_cycle_is_circ() {
    // A5 ranges over B1:B10 which contains B5 = A5: the rect read records a
    // live edge into B5 and the cycle is witnessed. (Direct self-inclusion —
    // `=SUM(A1:A10)` in A5 — is rejected at ingest; see the §7.1 test.)
    let mut engine = runtime_engine();
    for r in 1..=10u32 {
        if r != 5 {
            set_value(&mut engine, "Sheet1", r, 2, LiteralValue::Number(r as f64));
        }
    }
    set_formula(&mut engine, "Sheet1", 5, 1, "=SUM(B1:B10)");
    set_formula(&mut engine, "Sheet1", 5, 2, "=A5");
    engine.evaluate_all().unwrap();
    assert!(is_circ(&engine, "Sheet1", 5, 1));
    assert!(is_circ(&engine, "Sheet1", 5, 2));
    assert_eq!(engine.last_cycle_telemetry().live_cycles_witnessed, 1);
}

#[test]
fn range_mediated_guarded_cycle_is_phantom_until_guard_flips() {
    // Same shape but the range read sits in a guarded branch: phantom while
    // the guard holds (Static would stamp #CIRC — documented diff), live
    // cycle when it flips.
    let mut engine = runtime_engine();
    set_value(&mut engine, "Sheet1", 1, 7, LiteralValue::Boolean(true)); // G1
    for r in 1..=10u32 {
        if r != 5 {
            set_value(&mut engine, "Sheet1", r, 2, LiteralValue::Number(1.0));
        }
    }
    set_formula(&mut engine, "Sheet1", 5, 1, "=IF(G1,5,SUM(B1:B10))");
    set_formula(&mut engine, "Sheet1", 5, 2, "=A5");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 5, 1), 5.0);
    assert_eq!(num(&engine, "Sheet1", 5, 2), 5.0);
    assert_eq!(engine.last_cycle_telemetry().phantom_sccs, 1);

    set_value(&mut engine, "Sheet1", 1, 7, LiteralValue::Boolean(false));
    engine.evaluate_all().unwrap();
    assert!(is_circ(&engine, "Sheet1", 5, 1));
    assert!(is_circ(&engine, "Sheet1", 5, 2));
}

#[test]
fn named_range_covering_the_cell_rejected_at_ingest_in_both_modes() {
    // A name whose region covers the formula's own cell is rejected when the
    // formula is set ("Circular reference through named range") — existing
    // engine rule, identical under Runtime.
    for cfg in [EvalConfig::default(), runtime_cfg()] {
        let mut engine = Engine::new(TestWorkbook::new(), cfg);
        let sheet_id = engine.sheet_id("Sheet1").unwrap();
        let nr = RangeRef::new(
            CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true)),
            CellRef::new(sheet_id, Coord::from_excel(10, 1, true, true)),
        );
        engine
            .define_name("COVER", NamedDefinition::Range(nr), NameScope::Workbook)
            .unwrap();
        let err = engine
            .set_cell_formula("Sheet1", 5, 1, parse("=SUM(COVER)").unwrap())
            .unwrap_err();
        assert_eq!(err.kind, ExcelErrorKind::Circ);
    }
}

/// KNOWN GAP (pre-existing, pre-dates #112 — static dependency extraction,
/// not Runtime evaluation): whole-column self-inclusion `=SUM(B:B)` in B1 is
/// not detected as a static SCC, so no cycle unit exists for either mode to
/// handle. Spec §7.8's stripe-path `#CIRC!` is therefore NOT yet delivered;
/// this test pins that Runtime at least matches Static exactly (no silent
/// divergence) until the detection gap is fixed.
#[test]
fn whole_column_self_inclusion_gap_runtime_matches_static() {
    let run = |cfg: EvalConfig| {
        let mut engine = Engine::new(TestWorkbook::new(), cfg);
        for r in 2..=4u32 {
            set_value(&mut engine, "Sheet1", r, 2, LiteralValue::Number(r as f64));
        }
        set_formula(&mut engine, "Sheet1", 1, 2, "=SUM(B:B)");
        let res = engine.evaluate_all().unwrap();
        (engine.get_cell_value("Sheet1", 1, 2), res.cycle_errors)
    };
    let static_out = run(EvalConfig::default());
    let runtime_out = run(runtime_cfg());
    assert_eq!(static_out, runtime_out);
    // Document today's (incorrect per spec §7.8) value so a future detection
    // fix must consciously update this pin.
    assert_eq!(static_out.0, Some(LiteralValue::Number(9.0)));
    assert_eq!(static_out.1, 0);
}

/* ──────────────────── 7.9 spill anchor inside an SCC ─────────────────── */

#[test]
fn spill_anchor_in_scc_is_circ_and_region_freed() {
    let mut engine = runtime_engine();
    // Prime: B1 spills SEQUENCE(C1) with C1 = 3 → B1:B3.
    set_value(&mut engine, "Sheet1", 1, 3, LiteralValue::Number(3.0));
    set_formula(&mut engine, "Sheet1", 1, 2, "=SEQUENCE(C1)");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 2), 2.0);
    assert_eq!(num(&engine, "Sheet1", 3, 2), 3.0);

    // Introduce the cycle: C1 = B1 + 1 ⇒ static SCC {B1, C1}.
    set_formula(&mut engine, "Sheet1", 1, 3, "=B1+1");
    engine.evaluate_all().unwrap();

    // Anchor pre-stamped #CIRC with spill teardown (spec §7.9, #115).
    assert!(is_circ(&engine, "Sheet1", 1, 2));
    for r in 2..=3 {
        assert!(
            matches!(
                engine.get_cell_value("Sheet1", r, 2),
                None | Some(LiteralValue::Empty)
            ),
            "spilled B{r} must be cleared, got {:?}",
            engine.get_cell_value("Sheet1", r, 2)
        );
    }
    // C1 reads the stamped anchor: #CIRC propagates through arithmetic.
    assert!(is_circ(&engine, "Sheet1", 1, 3));

    // Region freed: a new spill into the former region succeeds.
    set_formula(&mut engine, "Sheet1", 2, 2, "=SEQUENCE(2)");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 2), 1.0);
    assert_eq!(num(&engine, "Sheet1", 3, 2), 2.0);
}

#[test]
fn array_result_first_produced_inside_scc_is_stamped() {
    // A member that would *become* a spill anchor during the SCC task gets
    // the conservative §7.9 verdict instead of spilling.
    let mut engine = runtime_engine();
    set_formula(&mut engine, "Sheet1", 1, 1, "=SEQUENCE(2)+0*A2");
    set_formula(&mut engine, "Sheet1", 2, 1, "=A1");
    engine.evaluate_all().unwrap();
    assert!(is_circ(&engine, "Sheet1", 1, 1));
    assert!(
        is_circ(&engine, "Sheet1", 2, 1),
        "readers see propagated #CIRC"
    );
}

/* ───────────── 7.13 cross-sheet and named-formula members ────────────── */

#[test]
fn cross_sheet_phantom_scc_produces_values() {
    let mut engine = runtime_engine();
    engine.add_sheet("Sheet2").unwrap();
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(TRUE,5,Sheet2!A1)");
    set_formula(&mut engine, "Sheet2", 1, 1, "=Sheet1!A1+1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 5.0);
    assert_eq!(num(&engine, "Sheet2", 1, 1), 6.0);
    assert_eq!(engine.last_cycle_telemetry().phantom_sccs, 1);
}

/// Entering a cell formula that reads a name already depending on that cell
/// is rejected at ingest in both modes (existing rule).
#[test]
fn named_formula_cycle_rejected_at_ingest_in_both_modes() {
    for cfg in [EvalConfig::default(), runtime_cfg()] {
        let mut engine = Engine::new(TestWorkbook::new(), cfg);
        engine
            .define_name(
                "N",
                NamedDefinition::Formula {
                    ast: parse("=A1+1").unwrap(),
                    dependencies: Vec::new(),
                    range_deps: Vec::new(),
                },
                NameScope::Workbook,
            )
            .unwrap();
        let err = engine
            .set_cell_formula("Sheet1", 1, 1, parse("=N").unwrap())
            .unwrap_err();
        assert_eq!(err.kind, ExcelErrorKind::Circ);
    }
}

/// Direct SCC-task test for name-vertex members (spec §7.13): the scheduler
/// cannot currently produce a {cell, name} SCC (no static name-cycle edges),
/// so drive `evaluate_scc_unit` with the membership directly. Pass 1 records
/// the cell's read OF the name (by folded key) and the name's read of the
/// cell — a witnessed live cycle stamping both.
#[test]
fn named_formula_member_live_cycle_is_circ_in_scc_task() {
    let mut engine = runtime_engine();
    engine
        .define_name(
            "N",
            NamedDefinition::Literal(LiteralValue::Number(1.0)),
            NameScope::Workbook,
        )
        .unwrap();
    set_formula(&mut engine, "Sheet1", 1, 1, "=N");
    engine.evaluate_all().unwrap();
    // Re-point the name at a formula reading A1 (allowed via update path).
    engine
        .graph
        .update_name(
            "N",
            NamedDefinition::Formula {
                ast: parse("=A1+1").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let a1 = *engine
        .graph
        .get_vertex_id_for_address(&CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true)))
        .unwrap();
    let n = engine
        .graph
        .resolve_name_entry("N", sheet_id)
        .expect("name entry")
        .vertex;

    let stamped = engine.evaluate_scc_unit(&[a1, n], None, None).unwrap();
    assert_eq!(stamped, 2, "both the cell and the name member are stamped");
    assert!(is_circ(&engine, "Sheet1", 1, 1));
    assert!(matches!(
        engine.graph.get_value(n),
        Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Circ
    ));
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.live_cycles_witnessed, 1);
    assert_eq!(t.circ_cells_stamped, 2);
}

/// Phantom counterpart: the cell's guarded branch never reads the name, so
/// the SCC task produces values for both members.
#[test]
fn named_formula_member_phantom_produces_values_in_scc_task() {
    let mut engine = runtime_engine();
    engine
        .define_name(
            "N",
            NamedDefinition::Literal(LiteralValue::Number(1.0)),
            NameScope::Workbook,
        )
        .unwrap();
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(TRUE,2,N)");
    engine.evaluate_all().unwrap();
    engine
        .graph
        .update_name(
            "N",
            NamedDefinition::Formula {
                ast: parse("=A1+1").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let a1 = *engine
        .graph
        .get_vertex_id_for_address(&CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true)))
        .unwrap();
    let n = engine
        .graph
        .resolve_name_entry("N", sheet_id)
        .expect("name entry")
        .vertex;

    let stamped = engine.evaluate_scc_unit(&[a1, n], None, None).unwrap();
    assert_eq!(stamped, 0);
    assert_eq!(num(&engine, "Sheet1", 1, 1), 2.0);
    assert_eq!(engine.graph.get_value(n), Some(LiteralValue::Number(3.0)));
    assert_eq!(engine.last_cycle_telemetry().phantom_sccs, 1);
}

/* ───────────── 7.14 user value over a formula member ─────────────────── */

#[test]
fn user_value_over_formula_removes_it_from_the_scc() {
    let mut engine = runtime_engine();
    build_99_pair(&mut engine, true);
    engine.evaluate_all().unwrap();
    assert_eq!(engine.last_cycle_telemetry().static_sccs, 1);

    // Overwrite A3 with a literal: its formula is removed (engine rule), so
    // no static SCC remains.
    set_value(&mut engine, "Sheet1", 3, 1, LiteralValue::Number(777.0));
    let res = engine.evaluate_all().unwrap();
    assert_eq!(res.cycle_errors, 0);
    assert_eq!(engine.last_cycle_telemetry().static_sccs, 0);
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 777.0);
}

/* ─────────────────────────── blast radius ────────────────────────────── */

#[test]
fn blast_radius_only_live_cycle_members_are_stamped() {
    // 10-member static ring C1..C10 (each row r references row r+1 in an
    // untaken branch; C10 closes the ring to C1). Only C5↔C6 is live.
    let mut engine = runtime_engine();
    for r in 1..=10u32 {
        let f = match r {
            5 => "=IF(TRUE,C6,C6)".to_string(), // live edge C5→C6 (both arms)
            6 => "=IF(TRUE,C5,C7)".to_string(), // live edge C6→C5
            10 => "=IF(TRUE,100,C1)".to_string(),
            _ => format!("=IF(TRUE,{},C{})", r * 10, r + 1),
        };
        set_formula(&mut engine, "Sheet1", r, 3, &f);
    }
    let res = engine.evaluate_all().unwrap();

    assert!(is_circ(&engine, "Sheet1", 5, 3), "C5 on the live cycle");
    assert!(is_circ(&engine, "Sheet1", 6, 3), "C6 on the live cycle");
    for r in [1u32, 2, 3, 4, 7, 8, 9] {
        assert_eq!(num(&engine, "Sheet1", r, 3), (r * 10) as f64, "C{r}");
    }
    assert_eq!(num(&engine, "Sheet1", 10, 3), 100.0);

    assert_eq!(res.cycle_errors, 1);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.static_sccs, 1);
    assert_eq!(t.live_cycles_witnessed, 1);
    assert_eq!(t.circ_cells_stamped, 2, "exactly the live-cycle members");
    assert_eq!(t.phantom_sccs, 0);
}

/* ─────────────────────────── settle mechanics ────────────────────────── */

#[test]
fn engineered_stale_reader_settles_to_exact_values() {
    // A1 (ordered first) live-reads A2 (ordered later): pass 1 sees A2's
    // pre-task value, the settle pass fixes it.
    let mut engine = runtime_engine();
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(TRUE,A2,0)");
    set_formula(&mut engine, "Sheet1", 2, 1, "=IF(TRUE,7,A1)");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 7.0);
    assert_eq!(num(&engine, "Sheet1", 2, 1), 7.0);
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.phantom_sccs, 1);
    assert_eq!(t.settle_passes_total, 2);
    assert_eq!(t.max_passes_single_scc, 2);
}

#[test]
fn branch_flip_during_settle_creating_live_cycle_is_circ() {
    // Pass 1 is acyclic; C1's settle re-eval flips its branch onto C3,
    // closing a live cycle C1↔C3 that only classification-after-settle sees.
    let mut engine = runtime_engine();
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(A2=999,A3,7)");
    set_formula(&mut engine, "Sheet1", 2, 1, "=IF(TRUE,999,A1)");
    set_formula(&mut engine, "Sheet1", 3, 1, "=IF(TRUE,A1,8)");
    engine.evaluate_all().unwrap();
    assert!(is_circ(&engine, "Sheet1", 1, 1), "A1 joins the live cycle");
    assert!(is_circ(&engine, "Sheet1", 3, 1), "A3 joins the live cycle");
    assert_eq!(num(&engine, "Sheet1", 2, 1), 999.0, "A2 keeps its value");
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.live_cycles_witnessed, 1);
    assert_eq!(t.circ_cells_stamped, 2);
    assert_eq!(t.capped_sccs, 0);
}

/* ───────────────────────────── side effects ──────────────────────────── */

#[test]
fn one_delta_per_member_per_recalc() {
    let mut engine = runtime_engine();
    build_99_pair(&mut engine, true);
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let mut expected = vec![
        PackedSheetCell::try_new(sheet_id, 1, 0).unwrap(), // A2 (0-based r1c0)
        PackedSheetCell::try_new(sheet_id, 2, 0).unwrap(), // A3
    ];
    expected.sort_unstable();
    assert_eq!(delta.changed_cells, expected);

    // No changes → no deltas (values persist, the SCC isn't re-stamped).
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    assert!(
        delta.changed_cells.is_empty(),
        "got {:?}",
        delta.changed_cells
    );

    // Guard flip: exactly one delta per member again.
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Boolean(false));
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    assert_eq!(delta.changed_cells, expected);
}

#[test]
fn evaluation_writes_do_not_hit_the_changelog() {
    // G11 confirm: `evaluate_all_logged` records only spill events for
    // computed results; Runtime SCC commits must not add anything either.
    use crate::engine::ChangeLog;
    use crate::engine::graph::editor::change_log::ChangeEvent;

    let mut engine = runtime_engine();
    build_99_pair(&mut engine, true);
    let mut log = ChangeLog::new();
    engine.evaluate_all_logged(&mut log).unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    for ev in log.events() {
        assert!(
            matches!(
                ev,
                ChangeEvent::CompoundStart { .. } | ChangeEvent::CompoundEnd { .. }
            ),
            "unexpected changelog event from SCC evaluation: {ev:?}"
        );
    }
}

#[test]
fn undo_of_the_triggering_edit_restores_pre_recalc_values() {
    let mut engine = runtime_engine();
    let mut undo = UndoEngine::new();

    let (_v, journal) = engine
        .action_atomic_journal("seed".to_string(), |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Boolean(true))?;
            tx.set_cell_formula("Sheet1", 2, 1, parse("=IF(A1,555,A3)").unwrap())?;
            tx.set_cell_formula("Sheet1", 3, 1, parse("=IF(A1,A2,999)").unwrap())?;
            Ok(())
        })
        .unwrap();
    undo.push_action(journal);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);

    let (_v, journal) = engine
        .action_atomic_journal("flip".to_string(), |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Boolean(false))?;
            Ok(())
        })
        .unwrap();
    undo.push_action(journal);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 999.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 999.0);

    // Undo the flip and recalc: pre-flip values come back.
    engine.undo_action(&mut undo).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 555.0);
}

#[test]
fn cancellation_is_honored_at_settle_pass_boundaries() {
    use crate::args::ArgSchema;
    use crate::function::{FnCaps, Function};
    use crate::traits::{ArgumentHandle, FunctionContext};

    // TRIP() sets the cancel flag as a side effect of pass 1; the SCC task's
    // per-settle-pass check must observe it before re-evaluating.
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
    let mut engine = Engine::new(wb, runtime_cfg());
    // Stale-reader shape: A1 needs a settle pass, A2's pass-1 evaluation
    // trips the flag.
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(TRUE,A2,0)");
    set_formula(&mut engine, "Sheet1", 2, 1, "=IF(TRUE,7+TRIPCANCEL(),A1)");
    let err = engine.evaluate_all_cancellable(flag).unwrap_err();
    assert_eq!(err.kind, ExcelErrorKind::Cancelled);
    assert!(
        err.message.as_deref().unwrap_or("").contains("SCC"),
        "cancellation must come from the SCC pass boundary, got {err:?}"
    );
}

/* ───────────────────── G12: INDIRECT inside an SCC ───────────────────── */

#[test]
fn indirect_cycle_through_replan_then_target_change_breaks_it() {
    // A1 = INDIRECT(D1)+1 with D1 → "B1" and B1 = A1+1: the virtual edge
    // closes a 2-vertex SCC (tarjan_scc_with_virtual); both members are
    // live (arithmetic) → #CIRC. (A single-vertex virtual SELF-edge does not
    // form a cycle unit today — pre-existing scheduler behavior, identical
    // under Static — so the pair shape is the canonical G12 case.)
    let mut engine = runtime_engine();
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
    assert!(is_circ(&engine, "Sheet1", 1, 1));
    assert!(is_circ(&engine, "Sheet1", 1, 2));

    // Re-point the dynamic ref: the outer replan loop drops the virtual
    // edge and both cells evaluate normally.
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
}

#[test]
fn indirect_in_untaken_branch_is_phantom() {
    // The virtual-dep builder registers INDIRECT targets statically, so the
    // self-edge exists in the schedule; at runtime the branch never executes
    // and the SCC is phantom.
    let mut engine = runtime_engine();
    set_value(
        &mut engine,
        "Sheet1",
        1,
        4,
        LiteralValue::Text("A1".to_string()),
    );
    set_formula(&mut engine, "Sheet1", 1, 1, "=IF(TRUE,1,INDIRECT(D1))");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 1.0);
}

/* ───────────────── recalc-plan dirty quirk under Runtime ─────────────── */

#[test]
fn recalc_plan_skips_clean_cycles_and_evaluates_dirty_ones_whole() {
    let mut engine = runtime_engine();
    build_99_pair(&mut engine, true);
    set_formula(&mut engine, "Sheet1", 1, 5, "=1+1"); // unrelated formula E1

    let plan = engine.build_recalc_plan().unwrap();
    engine.evaluate_recalc_plan(&plan).unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    assert_eq!(engine.last_cycle_telemetry().static_sccs, 1);

    // Dirty only the unrelated formula: the clean SCC must be skipped
    // entirely (values stand, no task runs).
    set_formula(&mut engine, "Sheet1", 1, 5, "=2+2");
    engine.evaluate_recalc_plan(&plan).unwrap();
    assert_eq!(engine.last_cycle_telemetry().static_sccs, 0);
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 555.0);

    // Dirty a cycle member (via its guard): the whole SCC evaluates.
    set_value(&mut engine, "Sheet1", 1, 1, LiteralValue::Boolean(false));
    engine.evaluate_recalc_plan(&plan).unwrap();
    assert_eq!(engine.last_cycle_telemetry().static_sccs, 1);
    assert_eq!(num(&engine, "Sheet1", 2, 1), 999.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 999.0);
}

/* ──────────────────────── compat & determinism ───────────────────────── */

#[test]
fn static_default_keeps_stamping_the_99_pair() {
    // Golden dual-mode pin: the only documented diff (spec §8) is values vs
    // #CIRC for the guarded phantom pair.
    let mut engine = static_engine();
    build_99_pair(&mut engine, true);
    let res = engine.evaluate_all().unwrap();
    assert!(is_circ(&engine, "Sheet1", 2, 1));
    assert!(is_circ(&engine, "Sheet1", 3, 1));
    assert_eq!(res.cycle_errors, 1);
    // Telemetry stays default-zero in Static mode.
    assert_eq!(
        engine.last_cycle_telemetry(),
        &crate::engine::CycleTelemetry::default()
    );
}

#[test]
fn dual_mode_corpus_documented_diffs_only() {
    // For each scenario: build identically under both modes; live cycles
    // must be #CIRC in both, phantoms differ exactly as documented.
    type Builder = fn(&mut Engine<TestWorkbook>);
    type Scenario = (Builder, &'static [(u32, u32)], &'static [(u32, u32, f64)]);
    let scenarios: Vec<Scenario> = vec![
        // (build, circ-in-both, runtime-values)
        (
            |e| {
                set_value(e, "Sheet1", 1, 7, LiteralValue::Boolean(true));
                set_value(e, "Sheet1", 1, 2, LiteralValue::Number(1.0));
                set_formula(e, "Sheet1", 5, 1, "=IF(G1,5,SUM(B1:B10))");
                set_formula(e, "Sheet1", 5, 2, "=A5");
            },
            &[][..],
            &[(5, 1, 5.0), (5, 2, 5.0)][..],
        ),
        (
            |e| build_99_pair(e, true),
            &[][..],
            &[(2, 1, 555.0), (3, 1, 555.0)][..],
        ),
        (
            |e| {
                set_formula(e, "Sheet1", 1, 1, "=B1+1");
                set_formula(e, "Sheet1", 1, 2, "=A1+1");
            },
            &[(1, 1), (1, 2)][..],
            &[][..],
        ),
    ];

    for (build, circ_both, runtime_values) in scenarios {
        let mut st = static_engine();
        build(&mut st);
        st.evaluate_all().unwrap();
        let mut rt = runtime_engine();
        build(&mut rt);
        rt.evaluate_all().unwrap();

        for &(r, c) in circ_both {
            assert!(is_circ(&st, "Sheet1", r, c), "static r{r}c{c}");
            assert!(is_circ(&rt, "Sheet1", r, c), "runtime r{r}c{c}");
        }
        for &(r, c, v) in runtime_values {
            assert!(
                is_circ(&st, "Sheet1", r, c),
                "static stamps phantoms r{r}c{c}"
            );
            assert_eq!(num(&rt, "Sheet1", r, c), v, "runtime value r{r}c{c}");
        }
    }
}

#[test]
fn deterministic_across_thread_counts_and_repeats() {
    fn build_and_run(threads: usize) -> (Vec<Option<LiteralValue>>, crate::engine::CycleTelemetry) {
        let cfg = EvalConfig {
            max_threads: Some(threads),
            enable_parallel: threads > 1,
            ..runtime_cfg()
        };
        let mut engine = Engine::new(TestWorkbook::new(), cfg);
        // Mixed workbook: phantom pair + live pair + blast-radius ring +
        // downstream readers.
        build_99_pair(&mut engine, true);
        set_formula(&mut engine, "Sheet1", 1, 2, "=B2+1");
        set_formula(&mut engine, "Sheet1", 2, 2, "=B1+1");
        for r in 1..=10u32 {
            let f = match r {
                5 => "=IF(TRUE,C6,C6)".to_string(),
                6 => "=IF(TRUE,C5,C7)".to_string(),
                10 => "=IF(TRUE,100,C1)".to_string(),
                _ => format!("=IF(TRUE,{},C{})", r * 10, r + 1),
            };
            set_formula(&mut engine, "Sheet1", r, 3, &f);
        }
        set_formula(&mut engine, "Sheet1", 1, 4, "=A2+C1"); // downstream
        engine.evaluate_all().unwrap();

        let mut values = Vec::new();
        for r in 1..=10u32 {
            for c in 1..=4u32 {
                values.push(engine.get_cell_value("Sheet1", r, c));
            }
        }
        let mut telemetry = engine.last_cycle_telemetry().clone();
        telemetry.elapsed_ms = 0; // wall clock is the only nondeterministic field
        (values, telemetry)
    }

    let baseline = build_and_run(1);
    for threads in [1usize, 2, 8] {
        for run in 0..2 {
            let out = build_and_run(threads);
            assert_eq!(out, baseline, "threads={threads} run={run}");
        }
    }
}

/* ──────────────── journaled-effects site under Runtime ───────────────── */

#[test]
fn evaluate_all_logged_handles_runtime_cycles_directly() {
    use crate::engine::ChangeLog;

    // Phantom and live cycles through the ChangeLog-threaded path produce
    // identical results to plain evaluate_all (the journal only ever records
    // spill events, which SCC tasks never produce).
    let mut engine = runtime_engine();
    build_99_pair(&mut engine, true);
    set_formula(&mut engine, "Sheet1", 1, 2, "=B2+1");
    set_formula(&mut engine, "Sheet1", 2, 2, "=B1+1");
    let mut log = ChangeLog::new();
    let res = engine.evaluate_all_logged(&mut log).unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), 555.0);
    assert_eq!(num(&engine, "Sheet1", 3, 1), 555.0);
    assert!(is_circ(&engine, "Sheet1", 1, 2));
    assert!(is_circ(&engine, "Sheet1", 2, 2));
    assert_eq!(res.cycle_errors, 1);
}
