//! Program 2 (contract decision 20.5): row/column inserts and deletes
//! shift virtual family member runs as blocks and log their per-member
//! `FormulaAdjusted` events as run records, expanded only when read.
//!
//! An engine with compression must be observably identical to one without
//! (`formula_compression = false`, where every member takes the per-cell
//! path): values, formula texts, vertex ids, the change log (events,
//! metadata, sequence numbers, groups), undo/redo through the change log
//! and through action journals, sequential and parallel, over random
//! structural edits on both sheets with edits between.
use super::common::arrow_eval_config;
use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{ChangeEvent, ChangeLog, Engine, EvalConfig};
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const ROWS: u32 = 72;

fn engine(compress: bool, parallel: bool) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        formula_compression: compress,
        enable_parallel: parallel,
        ..arrow_eval_config()
    };
    let mut e = Engine::new(TestWorkbook::new(), config);
    for r in 1..=ROWS {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
        e.set_cell_value("Sheet1", r, 2, LiteralValue::Number(f64::from(r % 5)))
            .unwrap();
        e.set_cell_value("Sheet2", r, 1, LiteralValue::Number(f64::from(r) * 0.25))
            .unwrap();
    }
    type Column = (&'static str, u32, u32, fn(u32) -> String);
    let cols: [Column; 10] = [
        ("Sheet1", 3, 1, |r| format!("=A{r}*2+B{r}")),
        ("Sheet1", 4, 2, |r| format!("=D{}+C{r}", r - 1)),
        ("Sheet1", 5, 1, |r| format!("=SUM($A$1:A{r})")),
        ("Sheet1", 6, 4, |r| format!("=SUM(A{}:A{r})", r - 3)),
        ("Sheet1", 7, 1, |r| format!("=Sheet2!A{r}+E{r}")),
        ("Sheet1", 8, 1, |r| format!("=$A$1*C{r}")),
        ("Sheet1", 9, 1, |r| format!("=IF(B{r}>2,C{r},-C{r})")),
        ("Sheet2", 2, 1, |r| format!("=Sheet1!C{r}*2")),
        ("Sheet2", 3, 1, |r| format!("=B{r}+A{r}+Sheet1!$B$3")),
        ("Sheet2", 4, 1, |r| format!("=SUM(Sheet1!A{r}:B{r})")),
    ];
    // Column by column, so each column's vertices are one id run.
    for (sheet, c, from, f) in cols {
        if from > 1 {
            e.set_cell_value(sheet, 1, c, LiteralValue::Number(1.0))
                .unwrap();
            for r in 2..from {
                e.set_cell_formula(sheet, r, c, parse(format!("=A{r}")).unwrap())
                    .unwrap();
            }
        }
        for r in from..=ROWS {
            e.set_cell_formula(sheet, r, c, parse(f(r)).unwrap())
                .unwrap();
        }
    }
    e
}

/// Bit-level key of a value.
fn key(v: Option<LiteralValue>) -> String {
    match v {
        Some(LiteralValue::Number(x)) => format!("N{:016x}", x.to_bits()),
        Some(LiteralValue::Error(e)) => format!("E{:?}", e.kind),
        other => format!("{other:?}"),
    }
}

/// Every cell of the used block: value, formula text and vertex id.
fn state(e: &Engine<TestWorkbook>) -> Vec<String> {
    let mut out = Vec::new();
    for sheet in ["Sheet1", "Sheet2"] {
        let Some(sid) = e.graph.sheet_id(sheet) else {
            continue;
        };
        for r in 1..=ROWS + 16 {
            for c in 1..=14 {
                let v = key(e.get_cell_value(sheet, r, c));
                let cell = CellRef::new(sid, Coord::from_excel(r, c, true, true));
                let vid = e.graph.get_vertex_id_for_address(&cell);
                let f = vid
                    .and_then(|v| e.graph.get_formula(v))
                    .map(|a| a.to_string());
                out.push(format!("{sheet}!{r},{c}: {v} {f:?} {vid:?}"));
            }
        }
    }
    out
}

fn assert_same(a: &Engine<TestWorkbook>, b: &Engine<TestWorkbook>, ctx: &str) {
    let (sa, sb) = (state(a), state(b));
    for (x, y) in sa.iter().zip(&sb) {
        assert_eq!(x, y, "{ctx}");
    }
    assert_eq!(sa.len(), sb.len(), "{ctx}");
}

fn assert_same_log(la: &ChangeLog, lb: &ChangeLog, ctx: &str) {
    assert_eq!(la.len(), lb.len(), "{ctx}: log length");
    assert_eq!(la.events(), lb.events(), "{ctx}: change log");
    for i in 0..la.len() {
        assert_eq!(la.meta(i), lb.meta(i), "{ctx}: seq/group {i}");
        assert_eq!(la.event_meta(i), lb.event_meta(i), "{ctx}: meta {i}");
    }
    assert_eq!(la.last_group_indices(), lb.last_group_indices(), "{ctx}");
}

fn virtual_count(e: &Engine<TestWorkbook>) -> usize {
    e.graph.virtual_member_counts().0
}

/// A small deterministic generator.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) as u32
    }
    fn below(&mut self, n: u32) -> u32 {
        self.next() % n
    }
}

#[derive(Clone, Copy, Debug)]
enum Op {
    InsertRows(&'static str, u32, u32),
    DeleteRows(&'static str, u32, u32),
    InsertCols(&'static str, u32, u32),
    DeleteCols(&'static str, u32, u32),
    Value(&'static str, u32, u32, f64),
    Formula(&'static str, u32, u32),
}

fn random_op(g: &mut Lcg) -> Op {
    let sheet = if g.below(4) == 0 { "Sheet2" } else { "Sheet1" };
    match g.below(6) {
        0 => Op::InsertRows(sheet, g.below(ROWS + 4), 1 + g.below(3)),
        1 => Op::DeleteRows(sheet, 2 + g.below(ROWS - 4), 1 + g.below(2)),
        2 => Op::InsertCols(sheet, 1 + g.below(9), 1),
        3 => Op::DeleteCols(sheet, 2 + g.below(9), 1),
        4 => Op::Value(
            sheet,
            1 + g.below(ROWS),
            1 + g.below(9),
            f64::from(g.below(50)),
        ),
        _ => Op::Formula(sheet, 2 + g.below(ROWS - 2), 3 + g.below(6)),
    }
}

/// Apply `op` through the change log (editor-level, 0-based structural
/// positions as the editor takes them).
fn apply_logged(e: &mut Engine<TestWorkbook>, log: &mut ChangeLog, op: Op) {
    let sid = |e: &Engine<TestWorkbook>, s: &str| e.graph.sheet_id(s).unwrap();
    match op {
        Op::InsertRows(s, at, n) => {
            let s = sid(e, s);
            e.edit_with_logger(log, |ed| ed.insert_rows(s, at, n).map(|_| ()))
                .unwrap()
                .unwrap();
        }
        Op::DeleteRows(s, at, n) => {
            let s = sid(e, s);
            e.edit_with_logger(log, |ed| ed.delete_rows(s, at, n).map(|_| ()))
                .unwrap()
                .unwrap();
        }
        Op::InsertCols(s, at, n) => {
            let s = sid(e, s);
            e.edit_with_logger(log, |ed| ed.insert_columns(s, at, n).map(|_| ()))
                .unwrap()
                .unwrap();
        }
        Op::DeleteCols(s, at, n) => {
            let s = sid(e, s);
            e.edit_with_logger(log, |ed| ed.delete_columns(s, at, n).map(|_| ()))
                .unwrap()
                .unwrap();
        }
        Op::Value(s, r, c, v) => {
            let cell = e.graph.make_cell_ref_internal(sid(e, s), r - 1, c - 1);
            log.begin_compound("value".into());
            e.edit_with_logger(log, |ed| ed.set_cell_value(cell, LiteralValue::Number(v)))
                .unwrap();
            log.end_compound();
        }
        Op::Formula(s, r, c) => {
            let cell = e.graph.make_cell_ref_internal(sid(e, s), r - 1, c - 1);
            let f = parse(format!("=A{r}*3+B{}", r - 1)).unwrap();
            log.begin_compound("formula".into());
            e.edit_with_logger(log, |ed| ed.set_cell_formula(cell, f))
                .unwrap();
            log.end_compound();
        }
    }
}

/// Random logged structural edits (both sheets, with value and formula
/// edits between) on a compressed and an uncompressed engine: identical
/// after every step, through undo of every step and redo of every step.
#[test]
fn block_shifts_match_per_cell_path_through_logged_history() {
    for (seed, parallel) in [(1u64, false), (2, true), (3, false), (4, false)] {
        let mut g = Lcg(seed);
        let mut a = engine(true, parallel);
        let mut b = engine(false, parallel);
        a.evaluate_all().unwrap();
        b.evaluate_all().unwrap();
        assert!(virtual_count(&a) > 6 * ROWS as usize);
        assert_same(&a, &b, "first eval");
        let (mut la, mut lb) = (ChangeLog::new(), ChangeLog::new());
        let mut steps = 0;
        let mut lazy_seen = 0;
        for step in 0..14 {
            let op = random_op(&mut g);
            let ctx = format!("seed {seed} step {step} {op:?}");
            let before = virtual_count(&a);
            apply_logged(&mut a, &mut la, op);
            apply_logged(&mut b, &mut lb, op);
            if matches!(op, Op::InsertRows(..) | Op::InsertCols(..)) && before > 0 {
                // The block path left most members virtual (no
                // materialize-everything) and kept their events lazy.
                assert!(
                    virtual_count(&a) * 2 > before,
                    "{ctx}: {} of {before} still virtual",
                    virtual_count(&a)
                );
            }
            lazy_seen += la.unexpanded_run_records();
            steps += 1;
            // Read the logs only every other step: expansion must also
            // work when records pile up.
            if step % 2 == 1 {
                assert_same_log(&la, &lb, &ctx);
            }
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &ctx);
        }
        assert!(lazy_seen > 0, "seed {seed}: no run record was kept lazy");
        assert_same_log(&la, &lb, "after edits");
        let (mut ua, mut ub) = (UndoEngine::new(), UndoEngine::new());
        for i in 0..steps {
            a.undo_logged(&mut ua, &mut la).unwrap();
            b.undo_logged(&mut ub, &mut lb).unwrap();
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("seed {seed} undo {i}"));
            assert_same_log(&la, &lb, &format!("seed {seed} undo {i}"));
        }
        for i in 0..steps {
            a.redo_logged(&mut ua, &mut la).unwrap();
            b.redo_logged(&mut ub, &mut lb).unwrap();
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("seed {seed} redo {i}"));
        }
        assert_same_log(&la, &lb, "after history");
    }
}

/// The same through atomic action journals (the journal is a public value:
/// its events are expanded), undo and redo of every action, with the
/// states checked against the forward states.
#[test]
fn block_shifts_match_per_cell_path_through_action_journals() {
    for (seed, parallel) in [(11u64, false), (12, true), (13, false)] {
        let mut g = Lcg(seed);
        let mut a = engine(true, parallel);
        let mut b = engine(false, parallel);
        a.evaluate_all().unwrap();
        b.evaluate_all().unwrap();
        let (mut ua, mut ub) = (UndoEngine::new(), UndoEngine::new());
        let mut states = vec![state(&a)];
        let mut n = 0;
        for step in 0..12 {
            // Actions take 1-based positions and support inserts (deletes
            // are refused inside atomic actions).
            let op = random_op(&mut g);
            type Act = Box<
                dyn Fn(
                    &mut crate::engine::EngineAction<'_, TestWorkbook>,
                ) -> Result<(), crate::engine::EditorError>,
            >;
            let act: Act = match op {
                Op::InsertRows(s, at, k) | Op::DeleteRows(s, at, k) => {
                    Box::new(move |tx| tx.insert_rows(s, at + 1, k).map(|_| ()))
                }
                Op::InsertCols(s, at, k) | Op::DeleteCols(s, at, k) => {
                    Box::new(move |tx| tx.insert_columns(s, at + 1, k).map(|_| ()))
                }
                Op::Value(s, r, c, v) => {
                    Box::new(move |tx| tx.set_cell_value(s, r, c, LiteralValue::Number(v)))
                }
                Op::Formula(s, r, c) => Box::new(move |tx| {
                    tx.set_cell_formula(s, r, c, parse(format!("=A{r}*3")).unwrap())
                }),
            };
            let (_, ja) = a.action_atomic_journal(format!("a{step}"), &act).unwrap();
            let (_, jb) = b.action_atomic_journal(format!("a{step}"), &act).unwrap();
            assert_eq!(
                ja.graph.events, jb.graph.events,
                "seed {seed} step {step} {op:?}: journal"
            );
            ua.push_action(ja);
            ub.push_action(jb);
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("seed {seed} action {step} {op:?}"));
            states.push(state(&a));
            n += 1;
        }
        for i in (0..n).rev() {
            a.undo_action(&mut ua).unwrap();
            b.undo_action(&mut ub).unwrap();
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("seed {seed} undo action {i}"));
            assert_eq!(
                state(&a),
                states[i],
                "seed {seed}: undo action {i} restores"
            );
        }
        for i in 0..n {
            a.redo_action(&mut ua).unwrap();
            b.redo_action(&mut ub).unwrap();
            a.evaluate_all().unwrap();
            b.evaluate_all().unwrap();
            assert_same(&a, &b, &format!("seed {seed} redo action {i}"));
        }
    }
}

/// `action_with_logger` publishes the capture's run records to the log
/// unexpanded; the log reads back exactly as the per-cell engine's.
#[test]
fn action_with_logger_keeps_run_records_lazy() {
    let mut a = engine(true, false);
    let mut b = engine(false, false);
    a.evaluate_all().unwrap();
    b.evaluate_all().unwrap();
    let (mut la, mut lb) = (ChangeLog::new(), ChangeLog::new());
    for (e, log) in [(&mut a, &mut la), (&mut b, &mut lb)] {
        e.action_with_logger(log, "insert", |tx| {
            tx.insert_rows("Sheet1", 30, 2)?;
            tx.set_cell_value("Sheet1", 31, 1, LiteralValue::Number(5.0))?;
            tx.insert_columns("Sheet1", 2, 1).map(|_| ())
        })
        .unwrap();
        e.evaluate_all().unwrap();
    }
    assert!(la.unexpanded_run_records() > 0, "records stay lazy");
    assert_eq!(lb.unexpanded_run_records(), 0);
    assert_same_log(&la, &lb, "action log");
    assert_eq!(la.unexpanded_run_records(), 0, "read expands");
    assert_same(&a, &b, "after action");
    // Appends after a read go to the expanded log.
    for (e, log) in [(&mut a, &mut la), (&mut b, &mut lb)] {
        let s1 = e.graph.sheet_id("Sheet1").unwrap();
        e.edit_with_logger(log, |ed| ed.insert_rows(s1, 10, 1).map(|_| ()))
            .unwrap()
            .unwrap();
        e.evaluate_all().unwrap();
    }
    assert_same_log(&la, &lb, "after a second edit");
    let (mut ua, mut ub) = (UndoEngine::new(), UndoEngine::new());
    for i in 0..2 {
        a.undo_logged(&mut ua, &mut la).unwrap();
        b.undo_logged(&mut ub, &mut lb).unwrap();
        a.evaluate_all().unwrap();
        b.evaluate_all().unwrap();
        assert_same(&a, &b, &format!("undo {i}"));
    }
}

/// A capped change log counts a run record as its events (eviction by
/// events, not records), and truncation cuts inside a record.
#[test]
fn capped_and_truncated_logs_count_run_events() {
    let run = |cap: Option<usize>| {
        let mut e = engine(true, false);
        e.evaluate_all().unwrap();
        let mut log = ChangeLog::new();
        if let Some(cap) = cap {
            log.set_max_changelog_events(Some(cap));
        }
        let s1 = e.graph.sheet_id("Sheet1").unwrap();
        e.edit_with_logger(&mut log, |ed| ed.insert_rows(s1, 20, 2).map(|_| ()))
            .unwrap()
            .unwrap();
        log
    };
    let full = run(None);
    let n = full.len();
    assert!(n > 100);
    let all: Vec<ChangeEvent> = full.events().to_vec();
    let capped = run(Some(n - 37));
    assert_eq!(capped.len(), n - 37);
    assert_eq!(capped.events(), &all[37..]);
    let mut t = run(None);
    t.truncate(n / 2);
    assert_eq!(t.events(), &all[..n / 2]);
    let mut t = run(None);
    let tail = t.take_from(n - 10);
    assert_eq!(tail, all[n - 10..].to_vec());
    assert_eq!(t.len(), n - 10);
    let mut t = run(None);
    t.clear();
    assert!(t.is_empty());
}

/// Reading the log after every structural edit expands each run record
/// once, in place: the history already expanded by an earlier read is
/// neither cloned nor expanded again (red team B1: the lazy log used to
/// rebuild, and so clone, the whole retained history on every read after
/// an append; alternating insert/delete with a read after each edit cloned
/// 12,848 / 50,784 / 201,920 / 805,248 events at 16 / 32 / 64 / 128 edits).
///
/// Expansion work (events written by expansion: expanded members plus
/// plain events moved behind them) must grow linearly with the edits (at
/// most 2.2x per doubling), the per-cell path does none, and both logs
/// read back identically.
#[test]
fn changelog_reads_after_each_structural_edit_do_linear_work() {
    let build = |on: bool| {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution: on,
                family_kernels: on,
                family_lift: on,
                formula_compression: on,
                enable_parallel: false,
                ..arrow_eval_config()
            },
        );
        for r in 1..=32 {
            e.set_cell_value("S", r, 1, LiteralValue::Number(f64::from(r)))
                .unwrap();
        }
        for r in 1..=32 {
            e.set_cell_formula("S", r, 2, parse(format!("=A{r}*2")).unwrap())
                .unwrap();
        }
        e.evaluate_all().unwrap();
        e
    };
    let mut work = Vec::new();
    for n in [16u32, 32, 64, 128] {
        let (mut a, mut b) = (build(true), build(false));
        let (mut la, mut lb) = (ChangeLog::new(), ChangeLog::new());
        for step in 0..n {
            for (e, log) in [(&mut a, &mut la), (&mut b, &mut lb)] {
                let sid = e.graph.sheet_id("S").unwrap();
                e.edit_with_logger(log, |ed| {
                    if step % 2 == 0 {
                        ed.insert_rows(sid, 0, 1).map(|_| ())
                    } else {
                        ed.delete_rows(sid, 0, 1).map(|_| ())
                    }
                })
                .unwrap()
                .unwrap();
                std::hint::black_box(log.events());
            }
            assert_eq!(la.unexpanded_run_records(), 0, "read expands");
        }
        assert_same_log(&la, &lb, &format!("{n} edits"));
        assert_eq!(lb.expansion_work(), 0, "per-cell path");
        println!(
            "edits={n} retained={} expansion_work={}",
            la.len(),
            la.expansion_work()
        );
        work.push(la.expansion_work());
    }
    assert!(work[0] > 0, "run records were logged lazily: {work:?}");
    for w in work.windows(2) {
        assert!(
            w[1] * 5 <= w[0] * 11,
            "super-linear changelog work for doubling edits: {work:?}"
        );
    }
}
