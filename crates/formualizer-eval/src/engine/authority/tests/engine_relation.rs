//! Engine fixtures: the authority built from real engine formulas answers
//! R-1 exactly on R1 edges and R-1X on all edges (naive oracle), equals the
//! legacy graph's direct dependents (Δ(e)) and dirty closure (Δ(a)), and
//! stays equal to a rebuild under formula and value edits.

use super::super::store::TagFilter;
use super::fixtures::*;
use super::oracle::Naive;
use super::support::Rng;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parse;

/// Full comparison of one engine state. Returns the number of queries.
pub fn compare_all(e: &mut Engine<TestWorkbook>, ctx: &str) -> usize {
    compare_with(e, ctx, true)
}

/// `compare_all` without Δ(e)/Δ(a): for states where legacy's own edges
/// are known stale (FORM-117: after structural undo).
pub fn compare_oracle(e: &mut Engine<TestWorkbook>, ctx: &str) -> usize {
    compare_with(e, ctx, false)
}

fn compare_with(e: &mut Engine<TestWorkbook>, ctx: &str, legacy: bool) -> usize {
    let naive = Naive::build(&e.graph);
    let cells = query_cells(e);
    e.graph.authority().expect("authority ready");
    let store = e.graph.authority_host().store();
    store.check().unwrap_or_else(|m| panic!("{ctx}: {m}"));
    let mut n = 0;
    for &q in &cells {
        assert_eq!(
            store_direct(store, q, TagFilter::R1Only),
            naive.direct(q, true),
            "{ctx}: R-1 direct {q:?}"
        );
        assert_eq!(
            store_direct(store, q, TagFilter::All),
            naive.direct(q, false),
            "{ctx}: R-1X direct {q:?}"
        );
        assert_eq!(
            store_closure(store, q, TagFilter::R1Only),
            naive.closure(q, true),
            "{ctx}: R-1 closure {q:?}"
        );
        assert_eq!(
            store_closure(store, q, TagFilter::All),
            naive.closure(q, false),
            "{ctx}: R-1X closure {q:?}"
        );
        n += 4;
    }
    for (cell, _) in &naive.formulas {
        assert_eq!(
            store_precedent_cells(store, *cell, TagFilter::R1Only),
            naive.precedent_cells(*cell, true, ROWS, COLS),
            "{ctx}: R-1 precedents {cell:?}"
        );
        assert_eq!(
            store_precedent_cells(store, *cell, TagFilter::All),
            naive.precedent_cells(*cell, false, ROWS, COLS),
            "{ctx}: R-1X precedents {cell:?}"
        );
        n += 2;
    }
    // Δ(e) and Δ(a) against the legacy graph (the runtime authority).
    let digest = store.digest();
    for &q in cells.iter().filter(|_| legacy) {
        let mine = store_direct(store, q, TagFilter::All);
        assert_eq!(
            mine,
            e.graph.legacy_direct_dependent_cells(q),
            "{ctx}: Δ(e) {q:?}"
        );
        let mine = store_closure(store, q, TagFilter::All);
        assert_eq!(
            mine,
            e.graph.legacy_closure_cells(&[q]),
            "{ctx}: Δ(a) {q:?}"
        );
        n += 2;
    }
    // Maintained == rebuild.
    let rebuilt = rebuild_from_graph(e);
    assert_eq!(digest, rebuilt.digest(), "{ctx}: maintained != rebuild");
    n
}

#[test]
fn engine_fixtures_r1_r1x_oracles_delta_and_rebuild() {
    let mut total = 0;
    for seed in 0..12u64 {
        let mut g = Rng(0xA1 + seed * 104_729);
        let mut e = fixture_engine();
        populate(&mut e, &mut g);
        total += compare_all(&mut e, &format!("seed {seed} build"));
        // Formula and value edits through the engine: maintained, not rebuilt.
        let builds = e.graph.authority_host().builds();
        for _ in 0..40 {
            let sheet = if g.chance(70) { "Sheet1" } else { "Data" };
            let (r, c) = (g.below(ROWS) + 1, g.below(8) + 1);
            if g.chance(35) {
                let _ = e.set_cell_value(sheet, r, c, LiteralValue::Number(7.0));
            } else {
                let t = MENU[g.below(MENU.len() as u32) as usize];
                let _ = e.set_cell_formula(sheet, r, c, parse(text(t, r)).unwrap());
            }
        }
        total += compare_all(&mut e, &format!("seed {seed} edits"));
        assert_eq!(
            e.graph.authority_host().builds(),
            builds,
            "edits must not rebuild"
        );
        assert!(e.graph.authority_host().incremental_mutations() > 0);
    }
    assert!(total > 100_000, "{total} queries");
}

// ---------------------------------------------------------------- M1a correction B4, B5(b)

/// Review B4 repro: a symbol-revision rebuild keeps every live id.
#[test]
fn review_symbol_rebuild_preserves_ids() {
    use crate::engine::EvalConfig;
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.set_cell_formula("Sheet1", 2, 1, parse("=1").unwrap())
        .unwrap();
    e.graph.authority().unwrap();
    e.set_cell_formula("Sheet1", 1, 1, parse("=2").unwrap())
        .unwrap();
    e.graph.authority().unwrap();
    let sid = e.graph.sheet_id("Sheet1").unwrap();
    let a1 = (sid, 0, 0);
    let a2 = (sid, 1, 0);
    let before = (
        e.graph.authority_host().store().ids().id_of(a1),
        e.graph.authority_host().store().ids().id_of(a2),
    );
    let applied = |e: &Engine<TestWorkbook>| {
        e.graph.authority_host().builds() + e.graph.authority_host().symbol_incremental()
    };
    let before_revisions = applied(&e);
    e.define_name(
        "UnrelatedReviewName",
        NamedDefinition::Literal(LiteralValue::Number(7.0)),
        NameScope::Workbook,
    )
    .unwrap();
    e.graph.authority().unwrap();
    // Reclassified (internal representation): name revisions apply
    // incrementally now (must-fix 1); what matters is that one was applied.
    assert!(
        applied(&e) > before_revisions,
        "the symbol revision was applied"
    );
    let after = (
        e.graph.authority_host().store().ids().id_of(a1),
        e.graph.authority_host().store().ids().id_of(a2),
    );
    assert_eq!(before, after, "symbol rebuild renumbered live formulas");
}

/// Executable names receive real non-grid IDs in the maintained host, sharing
/// the formula counter and surviving definition-only rebuilds.
/// Program 2 (one id space): a formula cell's id is its executor vertex,
/// and symbol binding identities come from the store's counter, from
/// `HOST_SYMBOL_ID_BASE` up, so the two never alias. A symbol keeps its id
/// across redefinition; a deleted and re-created name gets a new one;
/// cell edits never move the symbol counter. (Program 1: cells and symbols
/// shared one counter.)
#[test]
fn host_symbol_ids_never_alias_cell_ids_and_survive_redefinition() {
    use crate::engine::EvalConfig;
    use crate::engine::authority::identity::HOST_SYMBOL_ID_BASE;
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.set_cell_formula("Sheet1", 1, 1, parse("=1").unwrap())
        .unwrap();
    e.define_name(
        "Tracked",
        NamedDefinition::Literal(LiteralValue::Number(7.0)),
        NameScope::Workbook,
    )
    .unwrap();
    e.graph.authority().unwrap();
    let symbol = e
        .graph
        .iter_vertex_ids()
        .find_map(|v| e.graph.vertex_addr(v).as_symbol())
        .unwrap();
    let sid = e.graph.sheet_id("Sheet1").unwrap();
    let a1 = e
        .graph
        .get_vertex_for_cell(&e.graph.make_cell_ref_internal(sid, 0, 0))
        .unwrap();
    let store = e.graph.authority_host().store();
    let cell_id = store.ids().id_of((sid, 0, 0)).unwrap();
    assert_eq!(cell_id, a1.0, "a formula cell's id is its vertex");
    let symbol_id = store.symbol_id(symbol).unwrap();
    assert!(symbol_id >= HOST_SYMBOL_ID_BASE && cell_id < HOST_SYMBOL_ID_BASE);
    assert_eq!(store.ids().locate(symbol_id), None);
    let next = store.ids().next_id();
    e.update_name(
        "Tracked",
        NamedDefinition::Literal(LiteralValue::Number(9.0)),
        NameScope::Workbook,
    )
    .unwrap();
    e.graph.authority().unwrap();
    let store = e.graph.authority_host().store();
    assert_eq!(store.symbol_id(symbol), Some(symbol_id));
    assert_eq!(store.ids().id_of((sid, 0, 0)), Some(cell_id));
    assert_eq!(store.ids().next_id(), next);
    store.check().unwrap();
    e.set_cell_formula("Sheet1", 2, 1, parse("=2").unwrap())
        .unwrap();
    e.graph.authority().unwrap();
    let a2 = e
        .graph
        .get_vertex_for_cell(&e.graph.make_cell_ref_internal(sid, 1, 0))
        .unwrap();
    let store = e.graph.authority_host().store();
    assert_eq!(store.ids().id_of((sid, 1, 0)), Some(a2.0));
    assert_eq!(store.ids().next_id(), next, "cells do not draw the counter");
    assert_eq!(store.symbol_id(symbol), Some(symbol_id));
    assert_eq!(store.heap_bytes(), store.census_heap_bytes());
    let high_water = store.ids().next_id();
    e.delete_name("Tracked", NameScope::Workbook).unwrap();
    e.graph.authority().unwrap();
    assert_eq!(e.graph.authority_host().store().symbol_id(symbol), None);
    e.define_name(
        "Tracked",
        NamedDefinition::Literal(LiteralValue::Number(11.0)),
        NameScope::Workbook,
    )
    .unwrap();
    e.graph.authority().unwrap();
    let recreated = e
        .graph
        .resolve_name_entry_in_scope("Tracked", NameScope::Workbook)
        .unwrap()
        .vertex;
    let recreated_symbol = e.graph.vertex_addr(recreated).as_symbol().unwrap();
    assert_ne!(recreated_symbol, symbol);
    // The recreated name's binding identity is the next counter id (its
    // symbol-plane node takes the name vertex's id).
    assert_eq!(
        e.graph.authority_host().store().symbol_id(recreated_symbol),
        Some(high_water)
    );
    assert_eq!(
        e.graph.authority_host().store().ids().id_of((
            super::super::geom::SYMBOL_SHEET,
            e.graph.authority_host().symbols().slot(recreated).unwrap(),
            0
        )),
        Some(recreated.0)
    );
}

/// Review B5(b) repro: a rebuild obeys the retained budget.
#[test]
fn review_symbol_rebuild_obeys_retained_budget() {
    use crate::engine::EvalConfig;
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.set_cell_formula("Sheet1", 1, 1, parse("=1").unwrap())
        .unwrap();
    e.graph.authority().unwrap();
    e.graph.authority_host_mut().store.budget.retained = Some(0);
    e.define_name(
        "UnrelatedReviewBudgetName",
        NamedDefinition::Literal(LiteralValue::Number(7.0)),
        NameScope::Workbook,
    )
    .unwrap();
    match e.graph.authority() {
        Err(super::super::store::AuthorityError::Admission { resource, .. }) => {
            assert_eq!(resource, "retained")
        }
        other => panic!("rebuild ignored retained limit: {:?}", other.map(|_| ())),
    }
}

/// Re-review R3: fixed workbook names used across many (sheet, name)
/// contexts, one live formula at a time, no symbol change. Each context
/// interns a new LK key; the store compacts the directory once dead keys
/// dominate it (B-23), so it stays bounded instead of growing with the
/// contexts ever used, and no rebuild runs.
#[test]
fn fixed_names_across_many_contexts_keep_the_lk_directory_bounded() {
    use crate::engine::EvalConfig;
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use crate::reference::CellRef;
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sheets: Vec<String> = (0..8).map(|i| format!("Ctx{i}")).collect();
    for s in &sheets {
        e.add_sheet(s).unwrap();
    }
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    let target = e.graph.sheet_id("Sheet1").unwrap();
    for k in 0..8 {
        e.define_name(
            &format!("LibName_k{k}"),
            NamedDefinition::Cell(CellRef::new_absolute(target, 0, 0)),
            NameScope::Workbook,
        )
        .unwrap();
    }
    e.set_cell_formula("Sheet1", 2, 1, parse("=1").unwrap())
        .unwrap();
    e.graph.authority().unwrap();
    let anchor = (target, 1, 0);
    let anchor_id = e.graph.authority_host().store().ids().id_of(anchor);
    assert!(anchor_id.is_some());
    let builds = e.graph.authority_host().builds();
    let mut max_lk = 0;
    let mut contexts = 0;
    for s in &sheets {
        for k in 0..8 {
            e.set_cell_formula(s, 1, 1, parse(format!("=LibName_k{k}")).unwrap())
                .unwrap();
            e.graph.authority().unwrap();
            max_lk = max_lk.max(e.graph.authority_host().store().lk_len());
            e.set_cell_value(s, 1, 1, LiteralValue::Number(0.0))
                .unwrap();
            e.graph.authority().unwrap();
            max_lk = max_lk.max(e.graph.authority_host().store().lk_len());
            contexts += 1;
        }
    }
    let host = e.graph.authority_host();
    let stats = &host.store().stats;
    eprintln!(
        "R3: {contexts} contexts, max LK keys {max_lk}, compactions {}",
        stats.lk_compactions
    );
    assert!(stats.lk_compactions > 0, "no compaction ran");
    assert_eq!(stats.lk_compaction_skips, 0);
    assert_eq!(host.builds(), builds, "a compaction must not rebuild");
    // One live name formula plus the anchor: the bound is 2·(groups +
    // formulas + 8) + 1 with a handful of live groups.
    // Reclassified (symbol nodes, design §4.1): the 8 names are now 8 live
    // symbol nodes, each holding its own LK key, so the live bound gains
    // 2·8 keys. It still does not grow with the 64 contexts used.
    assert!(
        max_lk <= 32 + 2 * 8,
        "LK directory grew with history: {max_lk}"
    );
    assert_eq!(
        host.store().ids().id_of(anchor),
        anchor_id,
        "anchor renumbered"
    );
    host.store().check().unwrap();
}

/// Decision 9 under host rebuilds: random engine edits interleaved with
/// forced rebuilds (symbol revision, and real `define_name` calls). At
/// every sync, a cell that had a formula at the previous sync and still has
/// one keeps its id; an id is never seen at two different cells (never
/// reused for another cell, never renumbered); the counter never goes
/// back. Program 2 amendment: a formula cell's id is its vertex's, and a
/// value edit keeps the cell's vertex, so a formula set again at that cell
/// gets the same id (Program 1 gave it a fresh one).
#[test]
fn ids_are_stable_across_edits_and_forced_rebuilds() {
    use super::super::geom::Cell;
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use rustc_hash::{FxHashMap, FxHashSet};
    let mut rebuilds = 0u64;
    let mut kept = 0u64;
    let mut fresh = 0u64;
    for seed in 0..16u64 {
        let mut g = Rng(0x1D5 + seed * 7919);
        let mut e = fixture_engine();
        populate(&mut e, &mut g);
        e.graph.authority().expect("authority ready");
        let mut live: FxHashMap<Cell, u32> = FxHashMap::default();
        // Every id seen, with the cell it was seen at.
        let mut ever: FxHashMap<u32, Cell> = FxHashMap::default();
        let mut next = 0u32;
        let snapshot = |e: &Engine<TestWorkbook>| -> (FxHashMap<Cell, u32>, u32) {
            let s = e.graph.authority_host().store();
            let m = s
                .formula_cells()
                .into_iter()
                .map(|c| (c, s.ids().id_of(c).expect("formula cell id")))
                .collect();
            (m, s.ids().next_id())
        };
        for step in 0..120 {
            // Cells whose formula a value edit deleted in this batch: the
            // host syncs at every dirty propagation, so a formula set after
            // that in the same batch is a genuine creation (fresh id).
            let mut deleted: FxHashSet<Cell> = FxHashSet::default();
            for _ in 0..=g.below(4) {
                let sheet = if g.chance(75) { "Sheet1" } else { "Data" };
                let (r, c) = (g.below(ROWS) + 1, g.below(8) + 1);
                if g.chance(30) {
                    if e.set_cell_value(sheet, r, c, LiteralValue::Number(3.0))
                        .is_ok()
                    {
                        let sid = e.graph.sheet_id(sheet).expect("sheet");
                        deleted.insert((sid, r - 1, c - 1));
                    }
                } else {
                    let t = MENU[g.below(MENU.len() as u32) as usize];
                    let _ = e.set_cell_formula(sheet, r, c, parse(text(t, r)).unwrap());
                }
            }
            let builds = e.graph.authority_host().builds();
            if g.chance(20) {
                e.graph.authority_host_mut().symbol_rev = u64::MAX;
            } else if g.chance(5) {
                let _ = e.define_name(
                    &format!("IdProbeName{step}"),
                    NamedDefinition::Literal(LiteralValue::Number(1.0)),
                    NameScope::Workbook,
                );
            }
            e.graph.authority().expect("authority ready");
            rebuilds += e.graph.authority_host().builds() - builds;
            let (now, counter) = snapshot(&e);
            assert!(
                counter >= next,
                "seed {seed} step {step}: counter went back"
            );
            for (cell, id) in &now {
                match live.get(cell) {
                    Some(old) => {
                        assert_eq!(old, id, "seed {seed} step {step}: {cell:?} renumbered");
                        kept += 1;
                    }
                    None => {
                        assert!(
                            ever.get(id).is_none_or(|c| c == cell),
                            "seed {seed} step {step}: {cell:?} reused id {id} of {:?}",
                            ever.get(id)
                        );
                        fresh += 1;
                    }
                }
            }
            e.graph.authority_host().store().check().unwrap();
            ever.extend(now.iter().map(|(&c, &v)| (v, c)));
            live = now;
            next = counter;
        }
        let rebuilt = rebuild_from_graph(&e);
        assert_eq!(
            e.graph.authority_host().store().digest(),
            rebuilt.digest(),
            "seed {seed}: maintained != rebuild"
        );
    }
    assert!(
        rebuilds > 100 && kept > 10_000 && fresh > 100,
        "{rebuilds} {kept} {fresh}"
    );
}
