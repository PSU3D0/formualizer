use super::alloc::measure;
use super::support::{Formula, Model, Rng, abs, column, rel, window};
use crate::engine::authority::arc_emit::EmitError;
use crate::engine::authority::arc_sweep::SweepError;
use crate::engine::authority::arc_topology::TopologyError;
use crate::engine::authority::geom::{Cover, Rect};
use crate::engine::authority::plan_graph::GraphError;
use crate::engine::authority::planner::prepare;
use crate::engine::authority::store::{AuthorityError, Store};

#[test]
fn planner_generated_store_topology_matches_raw_cell_piece_oracle() {
    for seed in 1..=64 {
        let mut rng = Rng(seed);
        let mut model = Model::default();
        let mut cover = Cover::new();
        for sheet in 0..2 {
            for col in 0..3 {
                for row in 2..10 {
                    let refs = match rng.below(5) {
                        0 => vec![],
                        1 => vec![rel(-1, 0, sheet)],
                        2 => vec![window(-2, 0, col, sheet)],
                        3 => vec![column(col, sheet)],
                        _ => vec![abs(rng.below(10), rng.below(3), rng.below(2) as u16)],
                    };
                    let cell = (sheet, row, col);
                    if rng.chance(80) {
                        cover.insert_rect(sheet, &Rect::cell(row, col));
                    }
                    if rng.chance(80) {
                        model.cells.insert(
                            cell,
                            Formula {
                                refs,
                                l: 0,
                                literal: row,
                            },
                        );
                    }
                }
            }
        }
        let mut store = Store::build(model.cells.iter().map(|(&c, f)| (c, f.facts())).collect());
        for sheet in 0..2 {
            let cell = (sheet, 5, 1);
            let f = Formula {
                refs: vec![abs(4, 0, 1 - sheet)],
                l: 7,
                literal: 0,
            };
            store.set_formula(cell, &f.facts()).unwrap();
            model.cells.insert(cell, f);
        }
        let out = prepare(&store, &cover, None, None, None).unwrap();
        let input = &out.input;
        let n = input.slices.len();
        let mut membership = std::collections::BTreeMap::new();
        let mut expected_r = 0;
        for (piece, (s, id)) in input.slices.iter().zip(&input.identities).enumerate() {
            for row in s.r0..=s.r1 {
                let cell = (s.sheet, row, s.col);
                assert!(membership.insert(cell, piece).is_none());
                assert_eq!(
                    store.ids().lookup(cell).unwrap().0,
                    id.first_id + row - s.r0
                );
                assert_eq!(store.owner_at(cell), Some(id.owner));
                expected_r += model.cells[&cell].refs.len() as u64;
            }
        }
        assert_eq!(input.cells, membership.len() as u64);
        assert_eq!(input.references, expected_r);
        let mut reach = vec![vec![false; n]; n];
        // Model expands raw bound endpoints, not production projections/images.
        for (&cell, &from) in &membership {
            for reader in model.direct_dependents(cell) {
                if let Some(&to) = membership.get(&reader) {
                    reach[from][to] = true;
                }
            }
        }
        for (i, row) in reach.iter_mut().enumerate() {
            assert_eq!(out.topology.emission.selfdep[i] != 0, row[i]);
            row[i] = true;
        }
        for via in 0..n {
            for from in 0..n {
                for to in 0..n {
                    reach[from][to] |= reach[from][via] && reach[via][to];
                }
            }
        }
        let comp = &out.topology.components.component_of;
        for (a, row) in reach.iter().enumerate() {
            for (b, reverse) in reach.iter().enumerate() {
                assert_eq!(comp[a] == comp[b], row[b] && reverse[a], "seed={seed}");
            }
        }
        assert_eq!(input.edges.len(), input.probes.len());
        for (q, edge) in input.probes.iter().zip(&input.edges) {
            assert_eq!(q.sheet, edge.proj.sheet);
        }
    }
}

fn fixture(n: u32) -> (Store, Cover) {
    let store = Store::build(
        (0..n)
            .map(|row| {
                (
                    (0, row, 0),
                    Formula {
                        refs: vec![column(0, 0)],
                        l: u64::from(row % 2),
                        literal: 0,
                    }
                    .facts(),
                )
            })
            .collect(),
    );
    let mut cover = Cover::new();
    cover.insert_rect(0, &Rect::new(0, 0, n - 1, 0));
    (store, cover)
}

fn authority_error(error: TopologyError) -> AuthorityError {
    match error {
        TopologyError::Authority(e)
        | TopologyError::Sweep(SweepError::Authority(e))
        | TopologyError::Emit(EmitError::Authority(e))
        | TopologyError::Graph(GraphError::Authority(e)) => e,
        e => panic!("unexpected: {e:?}"),
    }
}

#[test]
fn planner_assembly_fail_every_allocation_and_exact_simultaneous_peak() {
    let (store, cover) = fixture(31);
    let (result, m) = measure(None, || prepare(&store, &cover, None, None, None));
    let out = result.unwrap();
    assert_eq!(m.peak as u64, out.peak_heap_bytes);
    assert_eq!(m.net as u64, out.heap_bytes());
    for nth in 0..m.allocs {
        let (result, failed) = measure(Some(nth), || prepare(&store, &cover, None, None, None));
        assert!(failed.failed, "allocation {nth}");
        assert_eq!(authority_error(result.unwrap_err()), AuthorityError::Alloc);
        assert_eq!(failed.net, 0);
    }
    for limit in [0, out.peak_heap_bytes - 1] {
        let (result, failed) = measure(None, || prepare(&store, &cover, Some(limit), None, None));
        assert!(matches!(
            authority_error(result.unwrap_err()),
            AuthorityError::Admission { .. }
        ));
        assert_eq!(failed.net, 0);
    }
    prepare(
        &store,
        &cover,
        Some(out.peak_heap_bytes),
        Some(out.topology.emission.charged),
        Some(out.topology.sweep.hits.len() as u64),
    )
    .unwrap();
    println!(
        "PLANNER_ASSEMBLY_ALLOC allocations={} peak={} retained={} work={}",
        m.allocs,
        m.peak,
        m.net,
        out.total_work()
    );
}

#[test]
fn planner_assembly_counted_dense_vertical_scaling() {
    let mut prior = 0;
    for n in [1024, 4096, 16384] {
        let (store, cover) = fixture(n);
        let out = prepare(&store, &cover, None, None, None).unwrap();
        assert_eq!(out.input.cells, u64::from(n));
        assert_eq!(out.input.references, u64::from(n));
        assert_eq!(
            out.topology.emission.pairwise_equivalent_hits,
            u64::from(n).pow(2)
        );
        assert!(out.total_work() <= 12000 * u64::from(n) + 16384);
        if prior > 0 {
            assert!(out.total_work() <= 4 * prior);
        }
        prior = out.total_work();
        println!(
            "PLANNER_ASSEMBLY n={n} R={} work={} peak={} H={}",
            out.input.references,
            out.total_work(),
            out.peak_heap_bytes,
            out.topology.emission.pairwise_equivalent_hits
        );
    }
}
