//! Dirty store as a cover (design §4.4): set semantics against a cell-set
//! model, and engine marking equal to the legacy dirty closure.

use super::super::dirty::DirtyStore;
use super::super::geom::{Cell, Rect};
use super::support::Rng;
use super::support::{Model, build_from, rel};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parse;
use rustc_hash::FxHashSet;

#[test]
fn dirty_store_matches_a_cell_set() {
    let mut d = DirtyStore::default();
    let mut model: FxHashSet<Cell> = FxHashSet::default();
    let mut g = Rng(0xD1);
    for step in 0..3000 {
        let s = g.below(2) as u16;
        let (r0, c0) = (g.below(50), g.below(8));
        let r = Rect::new(r0, c0, r0 + g.below(6), c0 + g.below(3));
        let cells: Vec<Cell> = (r.c0..=r.c1)
            .flat_map(|c| (r.r0..=r.r1).map(move |row| (s, row, c)))
            .collect();
        if g.chance(60) {
            d.mark_rect(s, &r);
            model.extend(cells);
        } else if g.chance(50) {
            d.clean(s, &r);
            for c in cells {
                model.remove(&c);
            }
        } else {
            let want = cells.iter().any(|c| model.contains(c));
            assert_eq!(d.any_dirty(s, &r), want, "step {step}");
        }
        if step % 50 == 0 {
            let mut want: Vec<Cell> = model.iter().copied().collect();
            want.sort_unstable();
            assert_eq!(d.cells(), want, "step {step}");
            assert_eq!(d.cell_count(), want.len() as u64);
        }
    }
}

/// Marking a closure marks exactly the model's transitive dependents.
#[test]
fn mark_closure_equals_the_model_closure() {
    let mut m = Model::default();
    let f = super::support::Formula {
        refs: vec![rel(-1, 0, 0)],
        l: 1,
        literal: 1,
    };
    for r in 1..30u32 {
        m.cells.insert((0, r, 2), f.clone());
    }
    let s = build_from(&m);
    let mut d = DirtyStore::default();
    let n = d.mark_closure(&s, &[(0, Rect::cell(10, 2))]);
    assert_eq!(d.cells(), m.dependents((0, 10, 2)));
    assert_eq!(n, 19);
    d.clean(0, &Rect::new(0, 2, 20, 2));
    assert_eq!(d.cell_count(), 9);
    assert!(d.is_dirty((0, 21, 2)) && !d.is_dirty((0, 20, 2)));
}

// Reclassified (M5): dirty propagation is the authority's now, and its
// marking is the engine's dirty flags; the observational `DirtyStore` cover
// these tests read is no longer maintained. They check the flags against
// legacy's mirror closure instead.
#[test]
fn engine_edits_mark_the_legacy_closure_and_evaluation_cleans() {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for r in 1..=20u32 {
        e.set_cell_value("Sheet1", r, 1, LiteralValue::Number(f64::from(r)))
            .unwrap();
        e.set_cell_formula("Sheet1", r, 2, parse(format!("=A{r}*2")).unwrap())
            .unwrap();
        e.set_cell_formula("Sheet1", r, 3, parse(format!("=SUM($B$1:B{r})")).unwrap())
            .unwrap();
    }
    e.set_cell_formula("Sheet1", 1, 5, parse("=SUM(C:C)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    assert!(
        legacy_dirty_flags(&e).is_empty(),
        "evaluation cleans the cover"
    );
    e.set_cell_value("Sheet1", 7, 1, LiteralValue::Number(70.0))
        .unwrap();
    let dirty = legacy_dirty_flags(&e);
    let legacy = e.graph.legacy_closure_cells(&[(0, 6, 0)]);
    assert_eq!(dirty, legacy);
    // B7, C7..C20 and E1: one interval per column in the cover.
    assert_eq!(dirty.len(), 1 + 14 + 1);
    e.evaluate_all().unwrap();
    assert!(legacy_dirty_flags(&e).is_empty());
}

// ---------------------------------------------------------------- M1a correction B6

/// Every legacy formula vertex whose dirty flag is set, as sorted cells.
fn legacy_dirty_flags(e: &Engine<TestWorkbook>) -> Vec<Cell> {
    let mut v: Vec<Cell> = e
        .graph
        .vertices_with_formulas()
        .filter(|&v| e.graph.is_dirty(v))
        .filter_map(|v| e.graph.get_cell_ref(v))
        .map(|c| (c.sheet_id, c.coord.row(), c.coord.col()))
        .collect();
    v.sort_unstable();
    v
}

/// Review B6 repro: a re-edited standalone formula is dirty in legacy and
/// must be dirty in the authority cover.
#[test]
fn review_formula_seed_is_dirty_in_both_authorities() {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    e.set_cell_formula("Sheet1", 1, 1, parse("=1").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    e.set_cell_formula("Sheet1", 1, 1, parse("=2").unwrap())
        .unwrap();
    let v = e.graph.vertices_with_formulas().next().unwrap();
    let affected = e.graph.mark_dirty_many(&[v]);
    assert!(affected.contains(&v));
    assert!(e.graph.is_dirty(v));
    let sid = e.graph.sheet_id("Sheet1").unwrap();
    // The relation closure has no positive-length path from A1: this is
    // why the old mirror missed the dirty source.
    assert!(e.graph.legacy_closure_cells(&[(sid, 0, 0)]).is_empty());
    e.graph.authority().unwrap();
    assert!(
        legacy_dirty_flags(&e).contains(&(sid, 0, 0)),
        "edited formula is dirty in legacy but absent from authority cover"
    );
}

/// Isolated formula create, formula edit and multi-source propagation:
/// the authority's dirty cover equals legacy's dirty formula cells, both
/// for one propagation (the Δ(a) comparator) and as the whole cover.
#[test]
fn formula_seeds_create_edit_and_multi_source_match_legacy_dirty() {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sid = e.graph.sheet_id_mut("Sheet1");
    // Create: a standalone formula, and one with a dependent.
    e.set_cell_formula("Sheet1", 1, 1, parse("=1").unwrap())
        .unwrap();
    e.graph.authority().unwrap();
    assert!(legacy_dirty_flags(&e).contains(&(sid, 0, 0)));
    assert_eq!(legacy_dirty_flags(&e), legacy_dirty_flags(&e));
    e.set_cell_value("Sheet1", 1, 3, LiteralValue::Number(3.0))
        .unwrap();
    e.set_cell_formula("Sheet1", 2, 1, parse("=A1+C1").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 3, 1, parse("=A2*2").unwrap())
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 5, parse("=SUM(C1:C9)").unwrap())
        .unwrap();
    e.graph.authority().unwrap();
    assert_eq!(legacy_dirty_flags(&e), legacy_dirty_flags(&e));
    e.evaluate_all().unwrap();
    assert!(legacy_dirty_flags(&e).is_empty());

    // Edit: A1 := 2 dirties A1 (the seed) and A2, A3.
    e.set_cell_formula("Sheet1", 1, 1, parse("=2").unwrap())
        .unwrap();
    e.graph.authority().unwrap();
    let dirty = legacy_dirty_flags(&e);
    assert_eq!(dirty, vec![(sid, 0, 0), (sid, 1, 0), (sid, 2, 0)]);
    assert_eq!(dirty, legacy_dirty_flags(&e));
    e.evaluate_all().unwrap();

    // One propagation, several sources: a formula seed, a value seed with
    // range and cell readers, and a formula seed with no dependents.
    let seeds = [(sid, 0, 0), (sid, 0, 2), (sid, 2, 0)];
    let (legacy, mine) = e.graph.dirty_propagation_pair(&seeds).unwrap();
    assert_eq!(mine, legacy);
    assert_eq!(
        mine,
        vec![(sid, 0, 0), (sid, 0, 4), (sid, 1, 0), (sid, 2, 0)],
        "A1, E1 (reads C1), A2 (reads A1, C1), A3 (seed)"
    );
    // A value seed alone is affected but not dirty.
    e.evaluate_all().unwrap();
    let (legacy, mine) = e.graph.dirty_propagation_pair(&[(sid, 0, 2)]).unwrap();
    assert_eq!(mine, legacy);
    assert!(!mine.contains(&(sid, 0, 2)));
}
