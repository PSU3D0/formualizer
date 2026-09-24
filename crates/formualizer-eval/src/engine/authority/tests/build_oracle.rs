//! Stage 3: the canon build answers R-1 exactly on R1 edges and R-1X on all
//! edges (independent naive oracle), on synthetic models and on engine
//! fixtures (every reference form, names, tables), and is deterministic.

use super::super::geom::Cell;
use super::super::store::TagFilter;
use super::fixtures::{
    COLS, ROWS, fixture_engine, populate, query_cells, rebuild_from_graph, store_closure,
    store_direct, store_precedent_cells,
};
use super::oracle::Naive;
use super::support::{self, Model, Rng, build_from, random_formula};

#[test]
fn synthetic_build_equals_the_model() {
    for seed in 0..30u64 {
        let mut g = Rng(0xB1 + seed * 7);
        let mut m = Model::default();
        for col in 2..8u32 {
            let f = random_formula(&mut g, 40);
            for r in 0..40u32 {
                if g.chance(70) && Model::valid(&f, (0, r, col)) {
                    m.cells.insert((0, r, col), f.clone());
                }
            }
        }
        let s = build_from(&m);
        s.check().unwrap();
        for r in 0..44u32 {
            for c in 0..9u32 {
                for sh in 0..2u16 {
                    let q: Cell = (sh, r, c);
                    assert_eq!(support::store_direct(&s, q), m.direct_dependents(q));
                    assert_eq!(support::store_closure(&s, q), m.dependents(q));
                }
            }
        }
        // Lemma C1: the build is a function of the cell sets.
        assert_eq!(s.digest(), build_from(&m).digest());
    }
}

#[test]
fn engine_build_answers_r1_and_r1x_exactly() {
    let mut total = 0;
    let mut x_total = 0;
    for seed in 0..12u64 {
        let mut g = Rng(0xA1 + seed * 104_729);
        let mut e = fixture_engine();
        populate(&mut e, &mut g);
        let store = rebuild_from_graph(&e);
        store.check().unwrap();
        let naive = Naive::build(&e.graph);
        for q in query_cells(&e) {
            assert_eq!(
                store_direct(&store, q, TagFilter::R1Only),
                naive.direct(q, true),
                "R-1 {q:?}"
            );
            assert_eq!(
                store_direct(&store, q, TagFilter::All),
                naive.direct(q, false),
                "R-1X {q:?}"
            );
            assert_eq!(
                store_closure(&store, q, TagFilter::R1Only),
                naive.closure(q, true)
            );
            assert_eq!(
                store_closure(&store, q, TagFilter::All),
                naive.closure(q, false)
            );
            total += 4;
        }
        for (cell, _) in &naive.formulas {
            assert_eq!(
                store_precedent_cells(&store, *cell, TagFilter::R1Only),
                naive.precedent_cells(*cell, true, ROWS, COLS)
            );
            assert_eq!(
                store_precedent_cells(&store, *cell, TagFilter::All),
                naive.precedent_cells(*cell, false, ROWS, COLS)
            );
            total += 2;
        }
        // X edges exist (names, tables) and are excluded from R-1.
        let x_groups = store
            .edge_groups()
            .filter(|(k, _)| k.tag == super::super::store::Tag::X)
            .count();
        x_total += x_groups;
    }
    assert!(x_total > 0, "fixtures have no X edges");
    assert!(total > 30_000, "{total} queries");
}
