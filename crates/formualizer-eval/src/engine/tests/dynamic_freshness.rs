//! Freshness of dynamic reads (Program 1 M1c, design §8.2/§8.5).
//!
//! Value-level versions of the §8.5 named cases. They run on the legacy
//! graph (the oracle) and under `unified_authority`: whatever the plan and
//! its hints order first, a dynamic reader of a dirty cell must never leave
//! a stale value behind, and cycles first discovered through a dynamic read
//! must be reported as cycles.
//!
//! Not here: a self read first discovered through INDIRECT (`A1 =
//! INDIRECT("A1")+1`) evaluates to 1 in legacy and under the authority,
//! where design §8.2 case 2 expects #CIRC; that is a planned Δ of the
//! freshness recorder, not current behavior.

use crate::engine::{Engine, EvalConfig, EvaluationTarget};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

fn num(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> Option<f64> {
    match engine.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Number(n)) => Some(n),
        Some(LiteralValue::Int(n)) => Some(n as f64),
        _ => None,
    }
}

fn set(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, v: f64) {
    engine
        .set_cell_value("Sheet1", row, col, LiteralValue::Number(v))
        .unwrap();
}

fn formula(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, f: &str) {
    engine
        .set_cell_formula("Sheet1", row, col, parse(f).unwrap())
        .unwrap();
}

fn is_circ(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> bool {
    matches!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Circ
    )
}

/// §8.5(1): first evaluation, INDIRECT to a dirty formula ordered later.
#[test]
fn indirect_to_dirty_formula_first_evaluation() {
    let mut e = engine();
    set(&mut e, 1, 1, 3.0);
    formula(&mut e, 1, 2, "=INDIRECT(\"C1\")*2");
    formula(&mut e, 1, 3, "=A1+1");
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 2), Some(8.0));
    set(&mut e, 1, 1, 10.0);
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 3), Some(11.0));
    assert_eq!(num(&e, 1, 2), Some(22.0));
}

/// §8.5(2): the INDIRECT target changes between recalcs.
#[test]
fn indirect_target_changes_between_recalcs() {
    let mut e = engine();
    set(&mut e, 1, 1, 1.0);
    formula(&mut e, 2, 1, "=A1*10");
    formula(&mut e, 3, 1, "=A1*100");
    e.engine_set_text("D1", "A2");
    formula(&mut e, 1, 2, "=INDIRECT(D1)+1");
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 2), Some(11.0));
    e.engine_set_text("D1", "A3");
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 2), Some(101.0));
    set(&mut e, 1, 1, 2.0);
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 2), Some(201.0));
}

/// §8.5(11a): X dynamically reads dirty Y outside the static closure; static
/// Z reads X. Z must end fresh.
#[test]
fn dynamic_to_static_chain_no_stale_clean() {
    let mut e = engine();
    set(&mut e, 1, 1, 1.0);
    formula(&mut e, 5, 1, "=A1+100"); // Y
    formula(&mut e, 1, 2, "=OFFSET(A1,4,0)*2"); // X reads Y dynamically
    formula(&mut e, 1, 3, "=B1+1"); // Z
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 3), Some(203.0));
    set(&mut e, 1, 1, 5.0);
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 5, 1), Some(105.0));
    assert_eq!(num(&e, 1, 2), Some(210.0));
    assert_eq!(num(&e, 1, 3), Some(211.0));
}

/// §8.5(11c): a two-cycle first discovered through a dynamic read, with no
/// external dirty read.
#[test]
fn first_discovered_dynamic_two_cycle_is_a_cycle() {
    let mut e = engine();
    formula(&mut e, 1, 1, "=INDIRECT(\"B1\")+1");
    formula(&mut e, 1, 2, "=A1+1");
    e.evaluate_all().unwrap();
    assert!(is_circ(&e, 1, 1) && is_circ(&e, 1, 2));
}

/// §8.5(6): a targeted evaluation whose dynamic precedent lies outside the
/// initial static closure.
#[test]
fn demand_late_precedent_outside_closure() {
    let mut e = engine();
    set(&mut e, 1, 1, 2.0);
    formula(&mut e, 7, 1, "=A1*3");
    formula(&mut e, 1, 2, "=INDIRECT(\"A7\")+1");
    e.evaluate_all().unwrap();
    set(&mut e, 1, 1, 4.0);
    e.evaluate_targets(&[EvaluationTarget::Cell {
        sheet: "Sheet1".into(),
        row: 1,
        col: 2,
    }])
    .unwrap();
    assert_eq!(num(&e, 7, 1), Some(12.0));
    assert_eq!(num(&e, 1, 2), Some(13.0));
}

trait SetText {
    fn engine_set_text(&mut self, a1: &str, text: &str);
}

impl SetText for Engine<TestWorkbook> {
    fn engine_set_text(&mut self, a1: &str, text: &str) {
        let col = u32::from(a1.as_bytes()[0] - b'A') + 1;
        let row: u32 = a1[1..].parse().unwrap();
        self.set_cell_value("Sheet1", row, col, LiteralValue::Text(text.into()))
            .unwrap();
    }
}

/// Expected Δ, legacy wrong: an open column read over a spill that commits
/// earlier in the same pass. Legacy's range virtual deps resolve the column's
/// used extent at plan time, which fills the per-snapshot used-bounds cache
/// before the spill exists; the reader then sees only the anchor row (legacy
/// gives SUM 1 / COUNT 1, and keeps it on later recalcs). The authority
/// orders the anchor by its static range edge and probes nothing at plan
/// time, so the reader sees the committed spill (Excel: 6 / 3).
#[test]
fn open_column_reader_sees_spill_committed_earlier_in_pass() {
    let mut engine = engine();
    formula(&mut engine, 10, 3, "=SEQUENCE(3)");
    formula(&mut engine, 1, 1, "=SUM(C:C)");
    formula(&mut engine, 2, 1, "=COUNT(C:C)");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, 1, 1), Some(6.0));
    assert_eq!(num(&engine, 2, 1), Some(3.0));
    set(&mut engine, 1, 5, 1.0);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, 1, 1), Some(6.0));
}

/// Expected Δ, legacy wrong (design §8.2 case 1, FR3/FR4): the pre-probe
/// cannot see the new target. X = INDIRECT(D1) where D1's address moves to
/// A4 at the end of a dirty chain, so X runs before A4 in the first pass.
/// Legacy publishes the stale X, lets its static readers Z and W clear with
/// it, then re-dirties only X: Z = 5 and W = 10 stay wrong. The recorder
/// sees X read dirty A4, drops X's result, stops the pass after X's layer
/// and replans with the hint A4 → X.
#[test]
fn fr_dynamic_reader_of_moved_dirty_target_never_publishes_stale() {
    for (targeted, parallel) in [(false, false), (true, false), (false, true), (true, true)] {
        // Parallel: X shares its layer with S = LEN(D1)*10; the group is one
        // commit unit, so S is dropped with the stale X and re-runs.
        let mut e = if parallel {
            Engine::new(
                TestWorkbook::new(),
                EvalConfig {
                    enable_parallel: true,
                    max_threads: Some(4),
                    ..EvalConfig::default()
                },
            )
        } else {
            engine()
        };
        formula(&mut e, 1, 6, "=LEN(D1)*10"); // S
        set(&mut e, 1, 1, 1.0);
        formula(&mut e, 2, 1, "=A1+1");
        formula(&mut e, 3, 1, "=A2+1");
        formula(&mut e, 4, 1, "=A3+1");
        formula(&mut e, 1, 4, "=IF(A1>1,\"A4\",\"A1\")");
        formula(&mut e, 1, 2, "=INDIRECT(D1)"); // X
        formula(&mut e, 1, 3, "=B1+1"); // Z
        formula(&mut e, 1, 5, "=C1*2"); // W
        e.evaluate_all().unwrap();
        assert_eq!(
            (num(&e, 1, 2), num(&e, 1, 3), num(&e, 1, 5)),
            (Some(1.0), Some(2.0), Some(4.0))
        );
        set(&mut e, 1, 1, 5.0);
        if targeted {
            e.evaluate_targets(&[EvaluationTarget::Cell {
                sheet: "Sheet1".into(),
                row: 1,
                col: 5,
            }])
            .unwrap();
        } else {
            e.evaluate_all().unwrap();
        }
        assert_eq!(num(&e, 4, 1), Some(8.0), "targeted={targeted}");
        assert_eq!(num(&e, 1, 2), Some(8.0), "targeted={targeted}");
        assert_eq!(num(&e, 1, 3), Some(9.0), "targeted={targeted}");
        assert_eq!(num(&e, 1, 5), Some(18.0), "targeted={targeted}");
        if !targeted {
            assert_eq!(num(&e, 1, 6), Some(20.0), "parallel={parallel}");
        }
        let (stale, stops) = e.freshness_counters_for_test();
        assert!(stale >= 1 && stops >= 1, "stale={stale} stops={stops}");
    }
}

/// Expected Δ, legacy wrong (design §8.2 FR5 / DirtyExtents): a static
/// reader of a spill child. C5 = B5*2 has an edge to B5 only; the anchor B1
/// sits deeper in a dirty chain, so nothing orders it first. Legacy
/// evaluates C5 before the spill commits and its end-of-pass clear erases
/// the re-dirty from the spill write: C5 = 0 while B5 shows 5, on the first
/// spill and on every later recalc. Under the authority the spill commit's
/// re-dirty survives (commit-time clearing) and the loop replans; once the
/// extent is known it is ordered first by a plan hint.
#[test]
fn fr_static_reader_of_spill_child_follows_the_spill() {
    // Not (first spill, targeted): C5's target closure never reaches the
    // anchor while it has no extent, and C5 is not dirty, so a targeted
    // request cannot know about the first spill (the same in legacy).
    for (grow, targeted) in [(false, false), (true, false), (true, true)] {
        let mut e = engine();
        set(&mut e, 1, 1, if grow { 2.0 } else { 0.0 });
        formula(&mut e, 5, 3, "=B5*2");
        formula(&mut e, 6, 3, "=C5+1");
        formula(&mut e, 1, 4, "=A1+1");
        formula(&mut e, 2, 4, "=D1+1");
        formula(&mut e, 3, 4, "=D2+1");
        formula(&mut e, 1, 2, "=IF(D3>3,SEQUENCE(5),D3)");
        e.evaluate_all().unwrap();
        if grow {
            assert_eq!((num(&e, 5, 2), num(&e, 5, 3)), (Some(5.0), Some(10.0)));
        }
        set(&mut e, 1, 1, 5.0);
        if targeted {
            e.evaluate_targets(&[EvaluationTarget::Cell {
                sheet: "Sheet1".into(),
                row: 6,
                col: 3,
            }])
            .unwrap();
        } else {
            e.evaluate_all().unwrap();
        }
        let got = (num(&e, 5, 2), num(&e, 5, 3), num(&e, 6, 3));
        assert_eq!(
            got,
            (Some(5.0), Some(10.0), Some(11.0)),
            "grow={grow} targeted={targeted}"
        );
    }
}

/// rdi_dyn (design §8.2 OR1, §8.4): a fresh dynamic reader's reads become
/// its observed set; later plans use it instead of a pre-probe, so steady
/// recalcs with a stable target hit the static schedule cache, keyed on
/// rev.dyn. Moving the target changes the observed set and stays correct.
#[test]
fn observed_reads_plan_dynamic_readers_and_key_the_schedule_cache() {
    let mut e = engine();
    set(&mut e, 1, 1, 1.0);
    formula(&mut e, 3, 1, "=A1*10"); // A3
    formula(&mut e, 4, 1, "=A1*100"); // A4
    e.engine_set_text("D1", "A3");
    formula(&mut e, 1, 2, "=INDIRECT(D1)+1"); // X
    formula(&mut e, 1, 3, "=B1*2"); // Z
    e.evaluate_all().unwrap();
    assert_eq!((num(&e, 1, 2), num(&e, 1, 3)), (Some(11.0), Some(22.0)));
    let x = e
        .graph
        .get_vertex_for_cell(&crate::reference::CellRef::new(
            e.sheet_id("Sheet1").unwrap(),
            crate::reference::Coord::new(0, 1, true, true),
        ))
        .unwrap();
    let observed = e.graph.authority_host().observed(x).map(<[_]>::to_vec);
    assert!(
        observed
            .as_ref()
            .is_some_and(|r| r.contains(&(e.sheet_id("Sheet1").unwrap(), 2, 0, 2, 0))),
        "X observed A3: {observed:?}"
    );
    let rev = e.graph.authority_host().rev_dyn();
    // Stable target: the second value-only recalc reuses the schedule.
    set(&mut e, 1, 1, 2.0);
    e.evaluate_all().unwrap();
    set(&mut e, 1, 1, 3.0);
    e.reset_recalc_reuse_probe();
    e.evaluate_all().unwrap();
    assert_eq!((num(&e, 1, 2), num(&e, 1, 3)), (Some(31.0), Some(62.0)));
    assert_eq!(
        e.graph.authority_host().rev_dyn(),
        rev,
        "same reads, same rev.dyn"
    );
    assert!(e.recalc_reuse_probe().schedule_cache_hits >= 1);
    // Moving the target: the observed set (and rev.dyn) changes, values
    // stay right.
    e.engine_set_text("D1", "A4");
    e.evaluate_all().unwrap();
    assert_eq!((num(&e, 1, 2), num(&e, 1, 3)), (Some(301.0), Some(602.0)));
    assert!(e.graph.authority_host().rev_dyn() > rev);
    set(&mut e, 1, 1, 4.0);
    e.evaluate_all().unwrap();
    assert_eq!((num(&e, 1, 2), num(&e, 1, 3)), (Some(401.0), Some(802.0)));
    // Editing the reader drops its observed set.
    formula(&mut e, 1, 2, "=INDIRECT(D1)+2");
    assert!(e.graph.authority().is_ok());
    assert!(e.graph.authority_host().observed(x).is_none());
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 2), Some(402.0));
}

/// Program 3 (plan reuse): recalculations alternating between a few inputs
/// reuse their schedules (a small bounded set of recent ones), and a formula
/// edit invalidates them all.
#[test]
fn alternating_value_edits_reuse_recent_schedules() {
    let mut e = engine();
    for r in 1..=3u32 {
        set(&mut e, r, 1, f64::from(r));
    }
    // Three independent chains.
    for r in 1..=3u32 {
        formula(&mut e, r, 2, &format!("=A{r}*2"));
        formula(&mut e, r, 3, &format!("=B{r}+1"));
    }
    e.evaluate_all().unwrap();
    // Warm one schedule per input.
    for r in 1..=3u32 {
        set(&mut e, r, 1, 10.0 + f64::from(r));
        e.evaluate_all().unwrap();
    }
    e.reset_recalc_reuse_probe();
    for round in 0..2u32 {
        for r in 1..=3u32 {
            let v = 100.0 * f64::from(round + 1) + f64::from(r);
            set(&mut e, r, 1, v);
            e.evaluate_all().unwrap();
            assert_eq!(num(&e, r, 3), Some(v * 2.0 + 1.0));
        }
    }
    let probe = e.recalc_reuse_probe();
    assert_eq!(probe.schedule_cache_hits, 6, "{probe:?}");
    // A formula edit: every retained schedule is stale.
    formula(&mut e, 2, 3, "=B2+5");
    e.evaluate_all().unwrap();
    e.reset_recalc_reuse_probe();
    set(&mut e, 1, 1, 7.0);
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 3), Some(15.0));
    assert_eq!(e.recalc_reuse_probe().schedule_cache_hits, 0);
}

/// Program 3 audit B1: `Out!A10 = SUM(OFFSET(Data!A1,0,0,20,2))` after
/// twelve mutations (column/row inserts and deletes, formulas over values,
/// a value edit and its undo). The last column insert dirties three Data
/// formulas and the readers in one pass. The reader's structural edit
/// cleared its observed reads, and its pre-probe (the sheet index, which
/// structural moves do not update) finds no formula in its target, so it
/// shares a layer with two of them. That layer buffers its writes: a
/// member's dirty flag is cleared at commit and its value written at the
/// flush, so the reader's freshness check saw clean targets holding their
/// pre-formula values (210 instead of 185). Members committed but not yet
/// written now count as dirty for that check: the reader re-plans after
/// them. `extra_reader` adds a static reader that makes main's
/// layer as wide (main fails that variant the same way).
fn offset_after_history(
    parallel: bool,
    deferred: bool,
    fast: bool,
    extra_reader: bool,
) -> (f64, f64) {
    use crate::engine::ChangeLog;
    use crate::engine::graph::editor::undo_engine::UndoEngine;
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: fast,
            family_kernels: fast,
            family_lift: fast,
            formula_compression: fast,
            enable_parallel: parallel,
            defer_graph_building: deferred,
            ..super::common::arrow_eval_config()
        },
    );
    for r in 1..=30u32 {
        e.set_cell_value("Data", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
        e.set_cell_formula("Data", r, 2, parse(format!("=A{r}*2")).unwrap())
            .unwrap();
    }
    let mut readers = vec![
        "=SUM(Data!A1:B4)",
        "=SUM(Data!A:A)",
        "=COUNTBLANK(Data!A:A)",
        "=COUNTIF(Data!C:C,\"\")",
        "=Data!A7+Data!B8",
        "=SUM(Data!A20:B30)",
        "=SUM(Data!A1:A64)",
        "=SUM(Data!A1:A65)",
        "=INDIRECT(\"Data!A7\")",
        "=SUM(OFFSET(Data!A1,0,0,20,2))",
        "=ROWS(Data!C:C*1)",
        "=Data!C30",
    ];
    if extra_reader {
        readers.push("=Data!A2");
    }
    for (i, f) in readers.iter().enumerate() {
        e.set_cell_formula("Out", i as u32 + 1, 1, parse(f).unwrap())
            .unwrap();
    }
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();
    e.evaluate_all().unwrap();
    let formula_at = |e: &mut Engine<TestWorkbook>, log: &mut ChangeLog, row: u32, f: &str| {
        e.action_with_logger(log, "f", |a| {
            a.set_cell_formula("Data", row, 2, parse(f).unwrap())
        })
        .unwrap();
    };
    let insert_col = |e: &mut Engine<TestWorkbook>, log: &mut ChangeLog| {
        e.action_with_logger(log, "ic", |a| a.insert_columns("Data", 1, 1))
            .unwrap();
    };
    // Each mutation is followed by a recalculation.
    insert_col(&mut e, &mut log);
    e.evaluate_all().unwrap();
    for _ in 0..2 {
        e.delete_columns("Data", 3, 1).unwrap();
        (log, undo) = (ChangeLog::new(), UndoEngine::new());
        e.evaluate_all().unwrap();
    }
    formula_at(&mut e, &mut log, 8, "=A9+1");
    e.evaluate_all().unwrap();
    e.action_with_logger(&mut log, "ir", |a| a.insert_rows("Data", 7, 1))
        .unwrap();
    e.evaluate_all().unwrap();
    e.action_with_logger(&mut log, "v", |a| {
        a.set_cell_value("Data", 15, 1, LiteralValue::Number(49.0))
    })
    .unwrap();
    e.evaluate_all().unwrap();
    e.undo_logged(&mut undo, &mut log).unwrap();
    e.evaluate_all().unwrap();
    e.delete_rows("Data", 7, 1).unwrap();
    (log, undo) = (ChangeLog::new(), UndoEngine::new());
    e.evaluate_all().unwrap();
    formula_at(&mut e, &mut log, 19, "=A20+1");
    e.evaluate_all().unwrap();
    insert_col(&mut e, &mut log);
    e.evaluate_all().unwrap();
    formula_at(&mut e, &mut log, 14, "=A15+1");
    e.evaluate_all().unwrap();
    insert_col(&mut e, &mut log);
    e.evaluate_all().unwrap();
    let _ = undo;
    let value = |e: &Engine<TestWorkbook>, col: u32| match e.get_cell_value("Out", 10, col) {
        Some(LiteralValue::Number(n)) => n,
        other => panic!("Out!R10C{col}: {other:?}"),
    };
    let reader = value(&e, 1);
    // A fresh copy of the reader's current (structurally adjusted) formula.
    let Some((Some(ast), _)) = e.get_cell("Out", 10, 1) else {
        panic!("reader lost its formula");
    };
    e.set_cell_formula("Out", 10, 2, ast).unwrap();
    e.evaluate_all().unwrap();
    (reader, value(&e, 2))
}

#[test]
fn offset_reader_after_structural_edits_and_undo_is_fresh() {
    for extra_reader in [false, true] {
        for parallel in [false, true] {
            for deferred in [false, true] {
                for fast in [false, true] {
                    let got = offset_after_history(parallel, deferred, fast, extra_reader);
                    assert_eq!(
                        got,
                        (185.0, 185.0),
                        "(reader, fresh copy): parallel={parallel} deferred={deferred} \
                         fast={fast} extra_reader={extra_reader}"
                    );
                }
            }
        }
    }
}
