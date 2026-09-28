//! Maintenance traces (SP-2 properties 3, 6, 7, 9 carried to the
//! production store): after every mutation the invariants hold, the dry run
//! equals the committed counts and bytes exactly, the observed index
//! transient is within the admitted one, and the relation equals an
//! independent brute-force model. At the end maintained == rebuild.

use super::super::geom::{Cell, Rect};
use super::super::store::Store;
use super::support::*;

const ROWS: u32 = 40;

fn initial(g: &mut Rng) -> Model {
    let mut m = Model::default();
    // A few filled-down families plus scattered singletons.
    for col in 2..6u32 {
        let f = random_formula(g, ROWS);
        let start = g.below(8) + 2;
        for r in start..start + g.below(25) + 5 {
            let cell = (0u16, r, col);
            if Model::valid(&f, cell) {
                m.cells.insert(cell, f.clone());
            }
        }
    }
    for _ in 0..15 {
        let f = random_formula(g, ROWS);
        let cell = (g.below(2) as u16, g.below(ROWS), 2 + g.below(6));
        if Model::valid(&f, cell) {
            m.cells.insert(cell, f);
        }
    }
    m
}

fn compare(store: &Store, m: &Model, g: &mut Rng, step: usize) {
    for _ in 0..6 {
        let q: Cell = (g.below(2) as u16, g.below(ROWS + 3), g.below(9));
        assert_eq!(
            store_direct(store, q),
            m.direct_dependents(q),
            "direct {q:?} step {step}"
        );
        assert_eq!(
            store_closure(store, q),
            m.dependents(q),
            "closure {q:?} step {step}"
        );
    }
}

/// One random mutation; returns the mutation's report check.
fn step(store: &mut Store, m: &mut Model, g: &mut Rng, step: usize) {
    let cell: Cell = (g.below(2) as u16, g.below(ROWS), 2 + g.below(6));
    let report = match g.below(100) {
        // Punch: formula → value.
        0..=29 => {
            m.cells.remove(&cell);
            store.clear_cell(cell).expect("unbounded budget")
        }
        // Refill from the neighbour above or left (re-forms families).
        30..=59 => {
            let src = if g.chance(50) && cell.1 > 0 {
                (cell.0, cell.1 - 1, cell.2)
            } else {
                (cell.0, cell.1, cell.2.saturating_sub(1))
            };
            let f = m
                .cells
                .get(&src)
                .cloned()
                .unwrap_or_else(|| random_formula(g, ROWS));
            if !Model::valid(&f, cell) {
                return;
            }
            m.cells.insert(cell, f.clone());
            store
                .set_formula(cell, &f.facts())
                .expect("unbounded budget")
        }
        // Another formula (formula → formula keeps the id).
        60..=84 => {
            let f = random_formula(g, ROWS);
            if !Model::valid(&f, cell) {
                return;
            }
            let before = store.ids().id_of(cell);
            m.cells.insert(cell, f.clone());
            let r = store
                .set_formula(cell, &f.facts())
                .expect("unbounded budget");
            if let Some(id) = before {
                assert_eq!(store.ids().id_of(cell), Some(id), "ID6 at {cell:?}");
            }
            r
        }
        // Range clear.
        _ => {
            let q = Rect::new(
                cell.1,
                cell.2,
                (cell.1 + g.below(6)).min(ROWS),
                cell.2 + g.below(3),
            );
            m.cells
                .retain(|c, _| !(c.0 == cell.0 && q.contains(c.1, c.2)));
            store.clear_rect(cell.0, q).expect("unbounded budget")
        }
    };
    assert_eq!(
        report.predicted, report.actual,
        "dry run != actual at step {step}"
    );
    assert!(
        report.observed_index_transient <= report.predicted_transient,
        "index transient above the admitted peak at step {step}"
    );
    if let Err(e) = store.check() {
        panic!("invariant at step {step}: {e}");
    }
}

#[test]
fn traces_keep_invariants_dry_run_exact_and_relation_equal() {
    let mut commits = 0;
    for seed in 0..40u64 {
        let mut g = Rng(0x5202 + seed * 7919);
        let mut m = initial(&mut g);
        let mut store = build_from(&m);
        store.check().unwrap();
        compare(&store, &m, &mut g, 0);
        for i in 0..150 {
            step(&mut store, &mut m, &mut g, i);
            if i % 5 == 0 {
                compare(&store, &m, &mut g, i);
            }
        }
        commits += store.stats.repartitions_committed;
        if seed == 0 {
            eprintln!(
                "seed 0 stats: {:?} counts {:?}",
                store.stats,
                store.counts()
            );
        }
        // Maintained == rebuild (canonical per-group cell sets, Lemma C1).
        let rebuilt = build_from(&m);
        assert_eq!(store.digest(), rebuilt.digest(), "seed {seed}");
    }
    assert!(commits > 0, "the traces never repartitioned");
}
