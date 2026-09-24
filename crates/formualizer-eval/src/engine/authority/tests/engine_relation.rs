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
    for &q in &cells {
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
