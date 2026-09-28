use super::alloc::measure;
use super::support::{Formula, Model, Rng, abs, column, rel, window};
use crate::engine::authority::arc_emit::EmitError;
use crate::engine::authority::arc_sweep::SweepError;
use crate::engine::authority::arc_topology::TopologyError;
use crate::engine::authority::geom::{Cover, Rect};
use crate::engine::authority::plan_graph::GraphError;
use crate::engine::authority::planner::{plan, prepare};
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
        for (&cell, &from) in &membership {
            for reader in model.direct_dependents(cell) {
                let Some(&to) = membership.get(&reader) else {
                    continue;
                };
                let cid = out.topology.components.component_of[from];
                if cid != out.topology.components.component_of[to] {
                    continue;
                }
                if let crate::engine::authority::planner::Class::Affine {
                    direction,
                    k,
                    min,
                    max,
                } = out.classification.components[cid].class
                {
                    let theta = |c: (u16, u32, u32), p: usize| {
                        k * (direction.0 * i64::from(c.1) + direction.1 * i64::from(c.2))
                            + out.classification.sigma[p]
                    };
                    assert!(theta(reader, to) > theta(cell, from));
                    assert!((min..=max).contains(&theta(cell, from)));
                    assert!((min..=max).contains(&theta(reader, to)));
                }
            }
        }
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
        let ordered = plan(&store, &cover, None, None, None).unwrap();
        let positions: std::collections::BTreeMap<_, _> = ordered
            .cells
            .iter()
            .enumerate()
            .map(|(i, c)| ((c.sheet, c.row, c.col), i))
            .collect();
        assert_eq!(positions.len(), membership.len());
        let o3 = oracle_work(&model, &membership);
        let top_h = ordered.prepared.topology.emission.pairwise_equivalent_hits;
        assert!(top_h <= o3);
        assert!(ordered.fallback_hits <= o3);
        assert!(top_h + ordered.fallback_hits <= 2 * o3);
        assert!(ordered.prepared.topology.emission.reader_arcs <= top_h);
        assert!(ordered.fallback_reader_arcs <= ordered.fallback_hits);
        assert!(ordered.fallback_references <= ordered.prepared.input.references);
        assert!(ordered.prepared.topology.sweep.hits.len() as u64 <= o3);
        assert!(ordered.discoveries <= 2 * o3);
        let n = ordered.cells.len();
        let mut reach = vec![vec![false; n]; n];
        let mut selfdep = vec![false; n];
        for (i, c) in ordered.cells.iter().enumerate() {
            let cell = (c.sheet, c.row, c.col);
            assert!(membership.contains_key(&cell));
            assert_eq!(store.ids().lookup(cell).unwrap().0, c.id);
            assert_eq!(store.owner_at(cell), Some(c.owner));
            for reader in model.direct_dependents(cell) {
                if let Some(&j) = positions.get(&reader) {
                    reach[i][j] = true;
                    selfdep[i] |= i == j;
                    let r = ordered.cells[j];
                    // A chain unit's cells share a layer and run in row
                    // order: an arc inside it goes to a later row.
                    let chained = c.chain
                        && r.chain
                        && c.layer == r.layer
                        && (c.sheet, c.col, c.owner) == (r.sheet, r.col, r.owner)
                        && c.row < r.row;
                    if (c.cycle.is_none() || c.cycle != r.cycle) && !chained {
                        assert!(c.layer < r.layer, "seed={seed} {cell:?}->{reader:?}");
                    }
                }
            }
            reach[i][i] = true;
        }
        for via in 0..n {
            for i in 0..n {
                for j in 0..n {
                    reach[i][j] |= reach[i][via] && reach[via][j];
                }
            }
        }
        for (i, a) in ordered.cells.iter().enumerate() {
            let cyclic = selfdep[i] || (0..n).any(|j| i != j && reach[i][j] && reach[j][i]);
            assert_eq!(a.cycle.is_some(), cyclic, "seed={seed}");
            for (j, b) in ordered.cells.iter().enumerate() {
                if i != j {
                    assert_eq!(
                        a.cycle.is_some() && a.cycle == b.cycle,
                        reach[i][j] && reach[j][i]
                    );
                }
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

#[test]
fn planner_classifies_affine_span_unknown_and_reuses_cell_sccs() {
    use crate::engine::authority::planner::Class;
    use crate::engine::authority::proj::{AxisMap, Bound, RefProj};
    let projection = RefProj {
        sheet: 0,
        rows: AxisMap {
            lo: Bound::Abs(0),
            hi: Bound::Rel(-1),
        },
        cols: AxisMap::point(Bound::Abs(0)),
    };
    let f = Formula {
        refs: vec![projection],
        l: 0,
        literal: 0,
    };
    let store = Store::build((1..1000).map(|r| ((0, r, 0), f.facts())).collect());
    let mut cover = Cover::new();
    cover.insert_rect(0, &Rect::new(1, 0, 999, 0));
    let out = prepare(&store, &cover, None, None, None).unwrap();
    assert_eq!(out.input.slices.len(), 1);
    assert_eq!(
        out.classification.components[0].class,
        Class::Affine {
            direction: (1, 0),
            k: 2,
            min: 2,
            max: 1998
        }
    );
    // SP-1 F2: literal theta span is 1997, NOT number of cells (999).
    assert_eq!(out.input.references, 999);
    assert!(out.classification.affine_work > 0);

    let f = Formula {
        refs: vec![column(0, 0)],
        l: 0,
        literal: 0,
    };
    let store = Store::build((1..1000).map(|r| ((0, r, 0), f.facts())).collect());
    let out = prepare(&store, &cover, None, None, None).unwrap();
    assert_eq!(out.classification.components[0].class, Class::Refine);
    // It may contain real cell cycles, but no cycle claim before exact fallback.
    assert_eq!(out.classification.reused_cell_sccs, 0);

    let (store, cover) = fixture(127);
    let out = prepare(&store, &cover, None, None, None).unwrap();
    assert_eq!(out.classification.components.len(), 1);
    assert!(out.classification.components[0].aux);
    assert_eq!(
        out.classification.components[0].class,
        Class::Cell {
            genuine_cycle: true
        }
    );
    assert_eq!(out.classification.reused_cell_sccs, 1);
    assert_eq!(out.classification.affine_work, 0);
    // ARC top-level graph is already the exact cell SCC: no duplicate graph.
}

#[test]
fn planner_coupled_family_affine_offsets_are_legal() {
    use crate::engine::authority::planner::Class;
    let mut inputs = Vec::new();
    for row in 2..20 {
        inputs.push((
            (0, row, 0),
            Formula {
                refs: vec![rel(-1, 1, 0)],
                l: 0,
                literal: 0,
            }
            .facts(),
        ));
        inputs.push((
            (0, row, 1),
            Formula {
                refs: vec![rel(0, -1, 0)],
                l: 1,
                literal: 0,
            }
            .facts(),
        ));
    }
    let store = Store::build(inputs);
    let mut cover = Cover::new();
    cover.insert_rect(0, &Rect::new(2, 0, 19, 1));
    let out = prepare(&store, &cover, None, None, None).unwrap();
    assert_eq!(out.input.slices.len(), 2);
    let Class::Affine {
        direction,
        k,
        min,
        max,
    } = out.classification.components[0].class
    else {
        panic!("not affine");
    };
    let theta = |r: u32, c: u32| {
        k * (direction.0 * i64::from(r) + direction.1 * i64::from(c))
            + out.classification.sigma[c as usize]
    };
    for r in 2..20 {
        assert!(theta(r, 1) > theta(r, 0));
        if r > 2 {
            assert!(theta(r, 0) > theta(r - 1, 1));
        }
        for c in 0..2 {
            assert!((min..=max).contains(&theta(r, c)));
        }
    }
    let (result, m) = measure(None, || prepare(&store, &cover, None, None, None));
    let measured = result.unwrap();
    assert_eq!(m.peak as u64, measured.peak_heap_bytes);
    assert_eq!(m.net as u64, measured.heap_bytes());
    for nth in 0..m.allocs {
        let (result, failed) = measure(Some(nth), || prepare(&store, &cover, None, None, None));
        assert!(failed.failed);
        assert_eq!(authority_error(result.unwrap_err()), AuthorityError::Alloc);
        assert_eq!(failed.net, 0);
    }
    for limit in [0, measured.peak_heap_bytes - 1] {
        let (result, failed) = measure(None, || prepare(&store, &cover, Some(limit), None, None));
        assert!(matches!(
            authority_error(result.unwrap_err()),
            AuthorityError::Admission { .. }
        ));
        assert_eq!(failed.net, 0);
    }
    prepare(&store, &cover, Some(measured.peak_heap_bytes), None, None).unwrap();
    println!(
        "PLANNER_AFFINE_ALLOC allocations={} peak={} retained={}",
        m.allocs, m.peak, m.net
    );
    println!(
        "PLANNER_AFFINE work={} affine={} span={}",
        out.total_work(),
        out.classification.affine_work,
        max - min + 1
    );
}

#[test]
fn planner_full_fallback_allocation_and_shared_caps() {
    for family in [false, true] {
        let store = Store::build(
            (0..31)
                .map(|row| {
                    (
                        (0, row, 0),
                        Formula {
                            refs: vec![column(0, 0)],
                            l: if family { 0 } else { u64::from(row % 2) },
                            literal: 0,
                        }
                        .facts(),
                    )
                })
                .collect(),
        );
        let mut cover = Cover::new();
        cover.insert_rect(0, &Rect::new(0, 0, 30, 0));
        let (result, m) = measure(None, || plan(&store, &cover, None, None, None));
        let out = result.unwrap();
        assert_eq!(out.cells.len(), 31);
        assert_eq!(out.fallback_components, u64::from(family));
        assert_eq!(m.peak as u64, out.peak_heap_bytes);
        assert_eq!(m.net as u64, out.heap_bytes());
        assert!(out.cells.iter().all(|c| c.layer == 0 && c.cycle == Some(0)));
        for nth in 0..m.allocs {
            let (result, failed) = measure(Some(nth), || plan(&store, &cover, None, None, None));
            assert!(failed.failed);
            assert_eq!(authority_error(result.unwrap_err()), AuthorityError::Alloc);
            assert_eq!(failed.net, 0);
        }
        for limit in [0, out.peak_heap_bytes - 1] {
            let (result, failed) = measure(None, || plan(&store, &cover, Some(limit), None, None));
            assert!(matches!(
                authority_error(result.unwrap_err()),
                AuthorityError::Admission { .. }
            ));
            assert_eq!(failed.net, 0);
        }
        plan(
            &store,
            &cover,
            Some(out.peak_heap_bytes),
            Some(out.charged),
            Some(out.discoveries),
        )
        .unwrap();
        assert!(matches!(
            plan(&store, &cover, None, Some(out.charged - 1), None).unwrap_err(),
            TopologyError::Emit(EmitError::SuperLinearWork { .. })
        ));
        assert!(matches!(
            plan(&store, &cover, None, None, Some(out.discoveries - 1)).unwrap_err(),
            TopologyError::Sweep(SweepError::DiscoveryLimit { .. })
        ));
        println!(
            "FULL_PLAN_ALLOC family={family} allocations={} peak={} retained={} work={} charged={} discovery={}",
            m.allocs, m.peak, m.net, out.work, out.charged, out.discoveries
        );
    }
}

#[test]
fn planner_full_dense_counted_scaling_and_singleton_reuse() {
    for family in [false, true] {
        let mut previous = 0;
        for n in [1024, 4096, 16384] {
            let store = Store::build(
                (0..n)
                    .map(|row| {
                        (
                            (0, row, 0),
                            Formula {
                                refs: vec![column(0, 0)],
                                l: if family { 0 } else { u64::from(row % 2) },
                                literal: 0,
                            }
                            .facts(),
                        )
                    })
                    .collect(),
            );
            let mut cover = Cover::new();
            cover.insert_rect(0, &Rect::new(0, 0, n - 1, 0));
            let out = plan(&store, &cover, None, None, None).unwrap();
            assert_eq!(out.cells.len(), n as usize);
            assert_eq!(out.fallback_components, u64::from(family));
            assert!(out.cells.iter().all(|c| c.cycle == Some(0) && c.layer == 0));
            assert!(out.work <= 12000 * u64::from(n) + 32768);
            if previous != 0 {
                assert!(out.work <= previous * 4);
            }
            previous = out.work;
            println!(
                "FULL_PLAN n={n} family={family} work={} fallback_work={} peak={} charges={} discovery={}",
                out.work, out.fallback_work, out.peak_heap_bytes, out.charged, out.discoveries
            );
        }
    }
}

// SP-1 O3′ with the spike's 64-cell expansion threshold. Raw endpoints and
// candidate cells only; no production image/projection/sweep calls.
fn oracle_work(
    model: &Model,
    candidates: &std::collections::BTreeMap<(u16, u32, u32), usize>,
) -> u64 {
    use crate::engine::authority::proj::Bound;
    let end = |b: Bound, x: u32, open: i64| match b {
        Bound::Abs(v) => i64::from(v),
        Bound::Rel(v) => i64::from(x) + i64::from(v),
        Bound::Open => open,
    };
    let columns: std::collections::BTreeSet<_> =
        candidates.keys().map(|&(s, _, c)| (s, c)).collect();
    let mut work = 0;
    for &(s, r, c) in candidates.keys() {
        for p in &model.cells[&(s, r, c)].refs {
            let (r0, r1, c0, c1) = (
                end(p.rows.lo, r, 0),
                end(p.rows.hi, r, 1_048_575),
                end(p.cols.lo, c, 0),
                end(p.cols.hi, c, 16_383),
            );
            assert!(r0 >= 0 && c0 >= 0 && r0 <= r1 && c0 <= c1);
            let area = ((r1 - r0 + 1) * (c1 - c0 + 1)) as u64;
            if p.rows.lo == p.rows.hi
                && p.cols.lo == p.cols.hi
                && p.rows.lo != Bound::Open
                && p.cols.lo != Bound::Open
            {
                work += 1;
            } else if area <= 64 {
                work += area;
            } else {
                for &(sheet, col) in &columns {
                    if sheet != p.sheet || i64::from(col) < c0 || i64::from(col) > c1 {
                        continue;
                    }
                    let hits = candidates
                        .keys()
                        .filter(|&&(s, r, c)| {
                            s == sheet && c == col && r0 <= i64::from(r) && i64::from(r) <= r1
                        })
                        .count();
                    work += (hits as u64).max(1);
                }
            }
        }
    }
    work
}
