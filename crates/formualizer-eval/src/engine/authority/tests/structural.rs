//! M3: structural edits, sheets and history keep the authority usable and
//! its identities stable (decision 9, design §6). After every edit the
//! resynced store answers the naive oracle, equals the legacy relation and
//! a rebuild (`compare_all`); moved formulas keep their ids, destroyed ones
//! retire theirs for good, and undo/redo restore the ids of every state.

use super::super::geom::Cell;
use super::super::identity::Vid;
use super::engine_relation::{compare_all, compare_oracle};
use super::fixtures::*;
use super::support::Rng;
use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{ChangeLog, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parse;
use std::collections::BTreeMap;

type Ids = BTreeMap<Cell, Vid>;

fn ids(e: &mut Engine<TestWorkbook>) -> Ids {
    let host = e.graph.authority().expect("authority ready");
    let mut m = Ids::new();
    for (_, run) in host.store().ids().live_runs() {
        for i in 0..run.len {
            m.insert((run.sheet, run.row_start + i, run.col), run.first_id + i);
        }
    }
    m
}

fn assert_ids(got: &Ids, want: &Ids, ctx: &str) {
    let diff: Vec<_> = got
        .iter()
        .map(|(c, v)| (*c, Some(*v), want.get(c).copied()))
        .chain(
            want.iter()
                .map(|(c, v)| (*c, got.get(c).copied(), Some(*v))),
        )
        .filter(|(_, a, b)| a != b)
        .collect();
    assert!(diff.is_empty(), "{ctx}: (cell, got, want) {diff:?}");
}

/// Legacy's formula text per cell (the reference state of §6.4).
fn texts(e: &Engine<TestWorkbook>) -> BTreeMap<(String, u32, u32), String> {
    let mut m = BTreeMap::new();
    for sheet in ["Sheet1", "Data"] {
        for r in 1..=ROWS + 12 {
            for c in 1..=COLS + 4 {
                if let Some((Some(ast), _)) = e.get_cell(sheet, r, c) {
                    m.insert((sheet.to_string(), r, c), format!("{ast}"));
                }
            }
        }
    }
    m
}

fn next_id(e: &mut Engine<TestWorkbook>) -> Vid {
    e.graph.authority().unwrap().store().ids().next_id()
}

fn engine(seed: u64) -> Engine<TestWorkbook> {
    let mut e = fixture_engine();
    populate(&mut e, &mut Rng(seed));
    e
}

/// Every id of `after` is either carried from `before` through `map`
/// (same id) or fresh (at or above `counter`); ids of `before` whose cell
/// maps to `None` are live nowhere.
fn assert_carried(
    before: &Ids,
    after: &Ids,
    counter: Vid,
    map: impl Fn(Cell) -> Option<Cell>,
    ctx: &str,
) {
    let live: BTreeMap<Vid, Cell> = after.iter().map(|(&c, &v)| (v, c)).collect();
    assert_eq!(live.len(), after.len(), "{ctx}: an id is live twice");
    for (&cell, &id) in before {
        match map(cell) {
            Some(to) => assert_eq!(after.get(&to), Some(&id), "{ctx}: {cell:?} -> {to:?}"),
            None => assert!(!live.contains_key(&id), "{ctx}: retired id {id} is live"),
        }
    }
    let carried: std::collections::BTreeSet<Vid> = before.values().copied().collect();
    for (&cell, &id) in after {
        assert!(
            carried.contains(&id) || id >= counter,
            "{ctx}: {cell:?} has id {id}, neither carried nor fresh"
        );
    }
}

#[test]
fn structural_edits_carry_identities_and_match_the_oracle() {
    for seed in [0x5eed_0001u64, 0x5eed_0002, 0x5eed_0003] {
        let mut e = engine(seed);
        compare_all(&mut e, "populate");
        let s1 = e.graph.sheet_id("Sheet1").unwrap();
        let data = e.graph.sheet_id("Data").unwrap();
        let builds = e.graph.authority_host().builds();

        // Insert 3 rows before 0-based row 4 of Sheet1.
        let (b0, c0) = (ids(&mut e), next_id(&mut e));
        e.edit_with_logger(&mut ChangeLog::new(), |ed| ed.insert_rows(s1, 4, 3))
            .unwrap()
            .unwrap();
        compare_all(&mut e, "insert rows");
        let a = ids(&mut e);
        assert_carried(
            &b0,
            &a,
            c0,
            |(s, r, c)| Some((s, if s == s1 && r >= 4 { r + 3 } else { r }, c)),
            "insert rows",
        );

        // Delete 2 rows at 0-based 10 of Sheet1: the band retires.
        let (b0, c0) = (a, next_id(&mut e));
        e.edit_with_logger(&mut ChangeLog::new(), |ed| ed.delete_rows(s1, 10, 2))
            .unwrap()
            .unwrap();
        compare_all(&mut e, "delete rows");
        let a = ids(&mut e);
        assert_carried(
            &b0,
            &a,
            c0,
            |(s, r, c)| match (s == s1, r) {
                (true, 10..=11) => None,
                (true, r) if r >= 12 => Some((s, r - 2, c)),
                _ => Some((s, r, c)),
            },
            "delete rows",
        );

        // Delete column E (0-based 4) and insert one before D on Data.
        let (b0, c0) = (a, next_id(&mut e));
        e.edit_with_logger(&mut ChangeLog::new(), |ed| ed.delete_columns(s1, 4, 1))
            .unwrap()
            .unwrap();
        compare_all(&mut e, "delete columns");
        let a = ids(&mut e);
        assert_carried(
            &b0,
            &a,
            c0,
            |(s, r, c)| match (s == s1, c) {
                (true, 4) => None,
                (true, c) if c > 4 => Some((s, r, c - 1)),
                _ => Some((s, r, c)),
            },
            "delete columns",
        );
        let (b0, c0) = (a, next_id(&mut e));
        e.edit_with_logger(&mut ChangeLog::new(), |ed| ed.insert_columns(data, 3, 2))
            .unwrap()
            .unwrap();
        compare_all(&mut e, "insert columns");
        let a = ids(&mut e);
        assert_carried(
            &b0,
            &a,
            c0,
            |(s, r, c)| Some((s, r, if s == data && c >= 3 { c + 2 } else { c })),
            "insert columns",
        );

        // Every structural edit was a resync, not a failure.
        assert!(e.graph.authority_host().builds() >= builds + 4);
        assert!(e.graph.authority_host().structural_rebuilds() >= 4);

        // A new formula never takes a retired id.
        let c0 = next_id(&mut e);
        e.set_cell_formula("Sheet1", 40, 3, parse("=A1+1").unwrap())
            .unwrap();
        let id = ids(&mut e)[&(s1, 39, 2)];
        assert!(id >= c0, "fresh formula reused id {id} < {c0}");
    }
}

#[test]
fn sheet_operations_carry_identities_and_rebind_on_readd() {
    let mut e = engine(0x5eed_0101);
    let s1 = e.graph.sheet_id("Sheet1").unwrap();
    let data = e.graph.sheet_id("Data").unwrap();

    let (b0, c0) = (ids(&mut e), next_id(&mut e));
    e.rename_sheet(data, "Facts").unwrap();
    compare_all_named(&mut e, "rename");
    assert_carried(&b0, &ids(&mut e), c0, Some, "rename");

    // Removing the sheet retires its formulas' ids; the rest keep theirs.
    let (b0, c0) = (ids(&mut e), next_id(&mut e));
    e.remove_sheet(data).unwrap();
    let a = ids(&mut e);
    assert_carried(&b0, &a, c0, |c| (c.0 != data).then_some(c), "remove");
    assert!(a.keys().all(|c| c.0 == s1));

    // Re-adding it heals the orphans; the healed readers bind to the new
    // sheet (edges to it, equal to a rebuild and to legacy).
    e.add_sheet("Facts").unwrap();
    e.set_cell_value("Facts", 3, 1, LiteralValue::Number(7.0))
        .unwrap();
    let facts = e.graph.sheet_id("Facts").unwrap();
    let host = e.graph.authority().expect("authority ready after re-add");
    let bound = host
        .store()
        .edge_groups()
        .any(|(k, rects)| !rects.is_empty() && k.proj.sheet == facts);
    let readers = b0.keys().filter(|c| c.0 == s1).count();
    assert!(
        readers == 0 || bound,
        "no reader re-bound to the re-added sheet"
    );
    let rebuilt = super::fixtures::rebuild_from_graph(&e);
    let host = e.graph.authority().unwrap();
    assert_eq!(host.store().digest(), rebuilt.digest(), "re-add != rebuild");
}

/// `compare_all` enumerates the fixture's sheet names; after a rename only
/// the rebuild equality applies.
fn compare_all_named(e: &mut Engine<TestWorkbook>, ctx: &str) {
    e.graph.authority().expect("authority ready");
    let rebuilt = super::fixtures::rebuild_from_graph(e);
    let store = e.graph.authority_host().store();
    store.check().unwrap_or_else(|m| panic!("{ctx}: {m}"));
    assert_eq!(
        store.digest(),
        rebuilt.digest(),
        "{ctx}: maintained != rebuild"
    );
}

fn assert_legacy(
    got: &BTreeMap<(String, u32, u32), String>,
    want: &BTreeMap<(String, u32, u32), String>,
    ctx: &str,
) {
    let d: Vec<_> = got
        .iter()
        .filter(|(k, v)| want.get(*k) != Some(*v))
        .chain(want.iter().filter(|(k, v)| got.get(*k) != Some(*v)))
        .collect();
    assert!(d.is_empty(), "{ctx}: legacy formula text differs {d:?}");
}

/// Design §6.4 `undo_redo_chain_50_ids_stable` through the action journal
/// (the Engine's Arrow-consistent history path): a chain of row/column
/// inserts and cell edits, undone to the start and redone to the end,
/// shows exactly each state's legacy text and ids. Deletes are refused
/// inside atomic actions; they are covered through the ChangeLog below.
#[test]
fn undo_redo_chain_ids_stable() {
    let seed: u64 = std::env::var("FZ_M3_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5eed_0201);
    let mut e = engine(seed);
    let mut undo = UndoEngine::new();
    let mut g = Rng(seed ^ 0x77);
    let mut states = vec![(texts(&e), ids(&mut e))];
    for step in 0..50 {
        let sheet = if g.chance(70) { "Sheet1" } else { "Data" };
        let (at, col, n) = (1 + g.below(ROWS), 3 + g.below(7), 1 + g.below(3));
        let (_, journal) = e
            .action_atomic_journal(format!("step {step}"), |tx| match step % 4 {
                0 => tx.insert_rows(sheet, at, n).map(|_| ()),
                1 => tx.insert_columns(sheet, col, 1).map(|_| ()),
                2 => tx.set_cell_value(sheet, at, col, LiteralValue::Number(1.0)),
                _ => tx.set_cell_formula(sheet, at, col, parse("=A1*3").unwrap()),
            })
            .unwrap();
        undo.push_action(journal);
        states.push((texts(&e), ids(&mut e)));
    }
    for i in (0..50).rev() {
        e.undo_action(&mut undo).unwrap();
        assert_legacy(&texts(&e), &states[i].0, &format!("undo to state {i}"));
        assert_ids(&ids(&mut e), &states[i].1, &format!("undo to state {i}"));
    }
    compare_oracle(&mut e, "after undo chain");
    // Legacy's redo leg is not exact: after a few steps it re-creates
    // formulas at wrong cells or leaves a deleted vertex visible at its
    // cell (the default build does the same). Ids are checked for every
    // state legacy reproduces (text and live formula cells), up to its
    // first divergence.
    let mut verified = 0;
    for (i, state) in states.iter().enumerate().skip(1) {
        e.redo_action(&mut undo).unwrap();
        let mut live: Vec<Cell> = e
            .graph
            .authority_build_input()
            .into_iter()
            .map(|(c, _)| c)
            .collect();
        live.sort_unstable();
        if texts(&e) != state.0 || !live.iter().eq(state.1.keys()) {
            eprintln!("legacy redo diverges at state {i}");
            break;
        }
        assert_ids(&ids(&mut e), &state.1, &format!("redo to state {i}"));
        if i % 4 == 1 {
            compare_oracle(&mut e, &format!("redo to state {i}"));
        }
        verified += 1;
    }
    assert!(verified >= 5, "only {verified} redo states verified");
}

/// Structural deletes through the ChangeLog: the deleted band's formulas
/// come back with their retired ids on undo, retire again on redo, and
/// come back again on a second undo (legacy re-creates them on new
/// vertices every time).
#[test]
fn undo_redo_of_logged_deletes_revives_retired_ids() {
    for (seed, rows) in [(0x5eed_0301u64, true), (0x5eed_0302, false)] {
        let mut e = engine(seed);
        let s1 = e.graph.sheet_id("Sheet1").unwrap();
        let (t0, i0) = (texts(&e), ids(&mut e));
        let mut log = ChangeLog::new();
        let mut undo = UndoEngine::new();
        e.edit_with_logger(&mut log, |ed| {
            if rows {
                ed.delete_rows(s1, 6, 3).map(|_| ())
            } else {
                ed.delete_columns(s1, 4, 2).map(|_| ())
            }
        })
        .unwrap()
        .unwrap();
        let (t1, i1) = (texts(&e), ids(&mut e));
        assert!(i1.len() < i0.len(), "the delete destroyed formulas");
        for round in 0..2 {
            e.undo_logged(&mut undo, &mut log).unwrap();
            assert_legacy(&texts(&e), &t0, &format!("undo {round}"));
            assert_ids(&ids(&mut e), &i0, &format!("undo {round}"));
            e.redo_logged(&mut undo, &mut log).unwrap();
            assert_legacy(&texts(&e), &t1, &format!("redo {round}"));
            assert_ids(&ids(&mut e), &i1, &format!("redo {round}"));
            compare_oracle(&mut e, "after redo");
        }
    }
}

fn form117_engine() -> Engine<TestWorkbook> {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for r in 1..=10 {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
    }
    e.set_cell_formula("Sheet1", 1, 3, parse("=A5*2").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 8, 3, parse("=SUM(A1:A10)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    e
}

/// Undo/redo of an action-journal insert restores and re-applies ids, and
/// the authority's relation after the undo is the naive oracle's.
#[test]
fn undo_structural_insert_restores_ids_and_relation() {
    let mut e = form117_engine();
    let s1 = e.graph.sheet_id("Sheet1").unwrap();
    let before = ids(&mut e);
    let mut undo = UndoEngine::new();
    let (_, journal) = e
        .action_atomic_journal("insert".to_string(), |tx| tx.insert_rows("Sheet1", 3, 2))
        .unwrap();
    undo.push_action(journal);
    e.evaluate_all().unwrap();
    assert_eq!(ids(&mut e)[&(s1, 9, 2)], before[&(s1, 7, 2)]);
    e.undo_action(&mut undo).unwrap();
    assert_eq!(ids(&mut e), before);
    let mut direct = Vec::new();
    e.graph.authority().unwrap().store().direct_dependents(
        s1,
        &super::super::geom::Rect::cell(4, 0),
        super::super::store::TagFilter::All,
        &mut direct,
    );
    let readers: Vec<Cell> = {
        let mut cover = super::super::geom::Cover::new();
        for (s, r) in direct {
            cover.insert_rect(s, &r);
        }
        cover.cells()
    };
    assert_eq!(
        readers,
        vec![(s1, 0, 2), (s1, 7, 2)],
        "A5's readers after undo"
    );
    e.redo_action(&mut undo).unwrap();
    assert_eq!(ids(&mut e)[&(s1, 9, 2)], before[&(s1, 7, 2)]);
}

/// Design §6.4 FORM-117 pattern: after undoing a structural insert, an
/// edit to a precedent recalculates its dependents. Legacy fails it (the
/// default build computes the same stale C1 = 10): runtime dirtying still
/// follows legacy's edges, which the undo leaves stale. The authority's
/// relation is right (`undo_structural_insert_restores_ids_and_relation`);
/// this passes once dirtying comes from the authority.
#[test]
#[ignore = "FORM-117: legacy dirtying after structural undo; needs the authority dirty-closure cutover"]
fn undo_structural_insert_then_edit_precedent_recalcs() {
    let mut e = form117_engine();
    let mut undo = UndoEngine::new();
    let (_, journal) = e
        .action_atomic_journal("insert".to_string(), |tx| tx.insert_rows("Sheet1", 3, 2))
        .unwrap();
    undo.push_action(journal);
    e.evaluate_all().unwrap();
    e.undo_action(&mut undo).unwrap();
    e.evaluate_all().unwrap();
    e.set_cell_value("Sheet1", 5, 1, LiteralValue::Number(100.0))
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(200.0))
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 8, 3),
        Some(LiteralValue::Number(150.0))
    );
}

#[test]
fn undo_of_formula_to_value_edit_revives_the_id_incrementally() {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 2, 2, parse("=B1+1").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let s1 = e.graph.sheet_id("Sheet1").unwrap();
    let before = ids(&mut e);
    let builds = e.graph.authority_host().builds();
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();
    let b1 = e.graph.make_cell_ref_internal(s1, 0, 1);
    e.edit_with_logger(&mut log, |ed| {
        ed.set_cell_value(b1, LiteralValue::Number(5.0))
    })
    .unwrap();
    let mid = ids(&mut e);
    assert!(!mid.contains_key(&(s1, 0, 1)));
    e.edit_with_logger(&mut log, |ed| {
        ed.set_cell_formula(b1, parse("=A1*3").unwrap())
    })
    .unwrap();
    let fresh = ids(&mut e)[&(s1, 0, 1)];
    assert!(fresh > before[&(s1, 0, 1)], "a fresh edit takes a fresh id");
    e.undo_logged(&mut undo, &mut log).unwrap();
    assert_eq!(ids(&mut e), mid);
    e.undo_logged(&mut undo, &mut log).unwrap();
    assert_eq!(ids(&mut e), before);
    e.redo_logged(&mut undo, &mut log).unwrap();
    assert_eq!(ids(&mut e), mid);
    e.redo_logged(&mut undo, &mut log).unwrap();
    assert_eq!(ids(&mut e)[&(s1, 0, 1)], fresh);
    // No structural edit: all of this was incremental.
    assert_eq!(e.graph.authority_host().builds(), builds);
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(7.0))
    );
}

/// Two structural operations with no sync between them: each records
/// the ids it retires in its own pre-operation frame (the frame its undo
/// restores). Checked on the journal: legacy's logged undo cannot replay
/// this pair (the first delete's `VertexMoved` events name vertices the
/// second destroyed, which its undo re-creates on new vertices).
#[test]
fn back_to_back_deletes_retire_in_their_own_frames() {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for r in 1..=20 {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
        e.set_cell_formula("Sheet1", r, 3, parse(&format!("=A{r}*2")).unwrap())
            .unwrap();
    }
    let s1 = e.graph.sheet_id("Sheet1").unwrap();
    let i0 = ids(&mut e);
    let mut log = ChangeLog::new();
    e.edit_with_logger(&mut log, |ed| ed.delete_rows(s1, 3, 2).map(|_| ()))
        .unwrap()
        .unwrap();
    e.edit_with_logger(&mut log, |ed| ed.delete_rows(s1, 9, 3).map(|_| ()))
        .unwrap()
        .unwrap();
    let i2 = ids(&mut e);
    let journal = e.graph.authority_host().journal();
    // First delete: 0-based rows 3..=4 in the original frame.
    for r in 3..=4 {
        assert_eq!(journal.undo_target((s1, r, 2)), Some(i0[&(s1, r, 2)]));
    }
    // Second delete: rows 9..=11 after the first, i.e. original 11..=13.
    for r in 9..=11 {
        assert_eq!(journal.undo_target((s1, r, 2)), Some(i0[&(s1, r + 2, 2)]));
    }
    assert_eq!(i2.len(), i0.len() - 5);
}
