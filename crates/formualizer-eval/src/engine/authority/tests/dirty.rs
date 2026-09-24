//! Dirty store as a cover (design §4.4): set semantics against a cell-set
//! model, and engine marking equal to the legacy dirty closure.

use super::super::dirty::DirtyStore;
use super::super::geom::{Cell, Rect};
use super::support::Rng;
use super::support::{Model, build_from, rel};
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
