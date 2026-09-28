use super::alloc::measure;
use super::support::{Formula, abs};
use crate::engine::authority::arc_emit::EmitError;
use crate::engine::authority::arc_emit::emit;
use crate::engine::authority::arc_sweep::{Probe, Slice, SweepError, sweep};
use crate::engine::authority::arc_topology::{TopologyError, topology};
use crate::engine::authority::candidates::discover;
use crate::engine::authority::geom::{Cover, MAX_COL, MAX_ROW, Rect};
use crate::engine::authority::plan_graph::GraphError;
use crate::engine::authority::store::{AuthorityError, Store};

fn check(slices: &[Slice], probes: &[Probe]) {
    let topo = topology(slices, probes, None, None, None).unwrap();
    let out = &topo.sweep;
    let mut actual = vec![vec![false; slices.len()]; probes.len()];
    for (hit, &q) in out.hits.iter().zip(&out.probes) {
        assert_eq!(hit.reader, probes[q].reader);
        actual[q][hit.start..hit.end].fill(true);
    }
    let mut expected = vec![vec![false; slices.len()]; probes.len()];
    for (i, q) in probes.iter().enumerate() {
        for (j, s) in slices.iter().enumerate() {
            expected[i][j] = q.sheet == s.sheet
                && q.image.c0 <= s.col
                && s.col <= q.image.c1
                && q.image.r0 <= s.r1
                && s.r0 <= q.image.r1;
        }
    }
    assert_eq!(actual, expected);
    let emission = &topo.emission;
    assert_eq!(
        emission.pairwise_equivalent_hits,
        expected.iter().flatten().filter(|&&v| v).count() as u64
    );
    let graph = &topo.graph;
    let components = &topo.components;
    let n = slices.len();
    let mut reach = vec![vec![false; n]; n];
    for (q, row) in probes.iter().zip(&expected) {
        for (from, &hit) in row.iter().enumerate() {
            reach[from][q.reader] |= hit;
        }
    }
    for (i, row) in reach.iter_mut().enumerate() {
        assert_eq!(emission.selfdep[i] != 0, row[i]);
        row[i] = true;
    }
    for via in 0..n {
        for from in 0..n {
            for to in 0..n {
                reach[from][to] |= reach[from][via] && reach[via][to];
            }
        }
    }
    for (from, row) in reach.iter().enumerate() {
        let mut seen = vec![false; emission.nodes];
        let mut stack = vec![from];
        while let Some(node) = stack.pop() {
            if seen[node] {
                continue;
            }
            seen[node] = true;
            stack.extend(graph.successors(node));
        }
        assert_eq!(&seen[..n], row);
        for (to, reverse) in reach.iter().enumerate() {
            assert_eq!(
                components.component_of[from] == components.component_of[to],
                row[to] && reverse[from]
            );
        }
    }
}

#[test]
fn arc_sweep_pinned_ties_holes_sheets_and_grid_edges() {
    let slices = [
        Slice {
            sheet: 0,
            col: 0,
            r0: 0,
            r1: 3,
        },
        Slice {
            sheet: 0,
            col: 0,
            r0: 5,
            r1: 9,
        },
        Slice {
            sheet: 0,
            col: MAX_COL,
            r0: MAX_ROW,
            r1: MAX_ROW,
        },
        Slice {
            sheet: 1,
            col: 0,
            r0: 0,
            r1: 3,
        },
    ];
    let probes = [
        Probe {
            reader: 3,
            sheet: 0,
            image: Rect::new(3, 0, 5, 0),
        },
        Probe {
            reader: 3,
            sheet: 0,
            image: Rect::cell(4, 0),
        },
        Probe {
            reader: 0,
            sheet: 0,
            image: Rect::cell(MAX_ROW, MAX_COL),
        },
        Probe {
            reader: 0,
            sheet: 1,
            image: Rect::cell(0, 0),
        },
        Probe {
            reader: 0,
            sheet: 2,
            image: Rect::new(0, 0, MAX_ROW, MAX_COL),
        },
    ];
    check(&slices, &probes);
    check(&[], &[]);
    check(&slices, &[]);
}

#[test]
fn arc_sweep_generated_rectangles_equal_independent_geometry_and_sccs() {
    for seed in 1..=128u64 {
        let mut state = seed;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state >> 32
        };
        let mut slices = Vec::new();
        for sheet in 0..2 {
            for col in [0, 2, 30] {
                for row in 0..24 {
                    if next() % 3 != 0 {
                        slices.push(Slice {
                            sheet,
                            col,
                            r0: row * 3,
                            r1: row * 3 + 1,
                        });
                    }
                }
            }
        }
        let mut probes = Vec::new();
        for reader in 0..slices.len() {
            let (a, b) = ((next() % 76) as u32, (next() % 76) as u32);
            let (c, d) = ((next() % 32) as u32, (next() % 32) as u32);
            probes.push(Probe {
                reader,
                sheet: (next() % 3) as u16,
                image: Rect::new(a.min(b), c.min(d), a.max(b), c.max(d)),
            });
        }
        check(&slices, &probes);
    }
}

#[test]
fn arc_sweep_production_independent_partitions_feed_the_topology_kernels() {
    let facts = |l| {
        Formula {
            refs: vec![abs(3, 1, 0)],
            l,
            literal: 0,
        }
        .facts()
    };
    let mut inputs = Vec::new();
    for col in 0..3 {
        for row in 2..6 {
            inputs.push(((0, row, col), facts(1)));
        }
    }
    for row in 0..2 {
        inputs.push(((0, row, 0), facts(2)));
    }
    let store = Store::build(inputs);
    let owner = store.owner_at((0, 2, 0)).unwrap();
    let (_, domain, _) = store.owner_dom(owner);
    assert_eq!(domain, Rect::new(2, 0, 5, 2));
    let mut partial = 0;
    store.visit_plan_edges(0, &domain, &mut |_, edge_domain| {
        assert!(!edge_domain.contains_rect(&domain));
        partial += 1;
    });
    assert_eq!(partial, 2);
    let mut slices = Vec::new();
    let mut probes = Vec::new();
    let mut cover = Cover::new();
    cover.insert_rect(0, &domain);
    let candidates = discover(&store, &cover, None).unwrap();
    assert_eq!(candidates.cells, 12);
    for candidate in candidates.slices {
        let col = candidate.col;
        let refined = store
            .refine_owner_column(candidate.owner, col, candidate.r0, candidate.r1, None)
            .unwrap();
        assert_eq!(refined.cell_references, 4);
        for piece in &refined.pieces {
            let reader = slices.len();
            slices.push(Slice {
                sheet: piece.sheet,
                col,
                r0: piece.domain.r0,
                r1: piece.domain.r1,
            });
            for edge in &refined.edges[piece.edge_start..piece.edge_end] {
                let image = edge.proj.forward(&piece.domain).unwrap();
                assert_eq!(image, Rect::cell(3, 1)); // independent raw written reference
                probes.push(Probe {
                    reader,
                    sheet: edge.proj.sheet,
                    image,
                });
            }
        }
    }
    check(&slices, &probes);
    let swept = sweep(&slices, &probes, None, None).unwrap();
    let out = emit(slices.len(), &swept.columns, &swept.hits, None, None).unwrap();
    assert_eq!(out.selfdep, [0, 1, 0]);
    assert_eq!(out.arcs, [(1, 0), (1, 2)]);
}

#[test]
fn arc_sweep_fail_every_allocation_and_peak_admission() {
    let slices: Vec<_> = (0..80)
        .map(|r| Slice {
            sheet: 0,
            col: 0,
            r0: r * 2,
            r1: r * 2,
        })
        .collect();
    let probes: Vec<_> = (0..80)
        .map(|reader| Probe {
            reader,
            sheet: 0,
            image: Rect::new(0, 0, 158, 0),
        })
        .collect();
    let (result, m) = measure(None, || sweep(&slices, &probes, None, None));
    let out = result.unwrap();
    assert_eq!(m.allocs, 8);
    assert_eq!(m.peak as u64, out.peak_heap_bytes);
    assert_eq!(m.net as u64, out.heap_bytes());
    for nth in 0..m.allocs {
        let (result, failed) = measure(Some(nth), || sweep(&slices, &probes, None, None));
        assert!(failed.failed);
        assert_eq!(
            result.unwrap_err(),
            SweepError::Authority(AuthorityError::Alloc)
        );
        assert_eq!(failed.net, 0);
    }
    for limit in [0, out.peak_heap_bytes - 1] {
        let (result, rejected) = measure(None, || sweep(&slices, &probes, Some(limit), None));
        assert!(matches!(
            result.unwrap_err(),
            SweepError::Authority(AuthorityError::Admission {
                resource: "scratch",
                ..
            })
        ));
        assert_eq!(rejected.net, 0);
    }
    sweep(&slices, &probes, Some(out.peak_heap_bytes), None).unwrap();
    println!(
        "ARC_SWEEP allocations={} peak={} retained={}",
        m.allocs, m.peak, m.net
    );
}

#[test]
fn arc_sweep_counted_sparse_scaling_and_invalid_order() {
    for n in [1024, 4096, 16384] {
        let slices: Vec<_> = (0..n)
            .map(|r| Slice {
                sheet: 0,
                col: 0,
                r0: r as u32 * 2,
                r1: r as u32 * 2,
            })
            .collect();
        // One real hit per probe; the gaps must not create false arcs.
        let probes: Vec<_> = (0..n)
            .map(|reader| Probe {
                reader,
                sheet: 0,
                image: Rect::cell(reader as u32 * 2, 0),
            })
            .collect();
        let topo = topology(&slices, &probes, None, None, None).unwrap();
        assert_eq!(topo.total_work(), 8198 + 156 * n as u64);
        assert_eq!(topo.components.len(), n);
        println!(
            "ARC_TOPOLOGY_SPARSE n={n} work={} peak={}",
            topo.total_work(),
            topo.peak_heap_bytes
        );
        let out = &topo.sweep;
        assert_eq!(out.hits.len(), n);
        assert!(out.hits.iter().all(|h| h.end - h.start == 1));
        assert_eq!(out.work.total(), 8193 + 141 * n as u64);
        println!(
            "ARC_SWEEP n={n} pairs={} work={} peak={}",
            out.hits.len(),
            out.work.total(),
            out.peak_heap_bytes
        );
    }
    let bad = [
        Slice {
            sheet: 0,
            col: 0,
            r0: 0,
            r1: 3,
        },
        Slice {
            sheet: 0,
            col: 0,
            r0: 3,
            r1: 5,
        },
    ];
    assert_eq!(
        sweep(&bad, &[], None, None).unwrap_err(),
        SweepError::InvalidInput
    );
}

#[test]
fn arc_sweep_discovery_cap_precedes_quadratic_allocation_even_without_hits() {
    for n in [64usize, 256, 1024] {
        let slices: Vec<_> = (0..n)
            .map(|col| Slice {
                sheet: 0,
                col: col as u32,
                r0: 0,
                r1: 0,
            })
            .collect();
        // Wide probes miss every row. Emission charges zero, so its cap
        // cannot protect the preceding n*n candidate-column discovery.
        let probes: Vec<_> = (0..n)
            .map(|reader| Probe {
                reader,
                sheet: 0,
                image: Rect::new(1, 0, 1, n as u32 - 1),
            })
            .collect();
        let cap = 3 * n as u64;
        let (result, measured) = measure(None, || {
            topology(&slices, &probes, None, Some(0), Some(cap))
        });
        let TopologyError::Sweep(SweepError::DiscoveryLimit {
            needed,
            limit,
            work,
        }) = result.unwrap_err()
        else {
            panic!("discovery must reject before emission");
        };
        assert_eq!((needed, limit, work.pairs), (cap + 1, cap, cap + 1));
        assert_eq!(measured.allocs, 4); // no pair/key output allocations
        assert_eq!(measured.net, 0);
        assert!(work.total() <= 4096 + 34 * n as u64);
        println!(
            "ARC_DISCOVERY_CAP n={n} pairs={} work={} peak={}",
            work.pairs,
            work.total(),
            measured.peak
        );
        if n == 64 {
            let exact = n as u64 * n as u64;
            let out = topology(&slices, &probes, None, Some(0), Some(exact)).unwrap();
            assert_eq!(out.sweep.hits.len() as u64, exact);
            assert_eq!(out.sweep.work.pairs, 2 * exact);
            assert_eq!(out.emission.charged, 0);
            for budget in [0, exact - 1] {
                let err = sweep(&slices, &probes, None, Some(budget)).unwrap_err();
                assert!(
                    matches!(err, SweepError::DiscoveryLimit { needed, limit, .. }
                    if needed == budget + 1 && limit == budget)
                );
            }
        }
    }
    let empty = sweep(&[], &[], None, Some(0)).unwrap();
    assert_eq!(empty.work.pairs, 0);
}

#[test]
fn arc_topology_control_cancels_inside_every_stage_and_charges_exact_work() {
    use crate::engine::authority::arc_topology::topology_controlled;
    use formualizer_common::{ExcelError, ExcelErrorKind};
    let n = 1024;
    let slices: Vec<_> = (0..n)
        .map(|r| Slice {
            sheet: 0,
            col: 0,
            r0: r as u32,
            r1: r as u32,
        })
        .collect();
    let probes: Vec<_> = (0..n)
        .map(|reader| Probe {
            reader,
            sheet: 0,
            image: Rect::new(0, 0, n as u32 - 1, 0),
        })
        .collect();
    let mut calls = 0;
    let mut charged = 0;
    let mut entries = 0;
    let out = topology_controlled(&slices, &probes, None, None, None, |delta| {
        assert!(delta <= 4096);
        calls += 1;
        charged += delta;
        entries += usize::from(delta == 0);
        Ok(())
    })
    .unwrap();
    assert_eq!(charged, out.total_work());
    assert_eq!(entries, 4);
    let error = ExcelError::new(ExcelErrorKind::Value);
    let mut stages = [false; 3];
    for fail_at in 0..calls {
        let mut seen = 0;
        let (result, heap) = measure(None, || {
            topology_controlled(&slices, &probes, None, None, None, |_| {
                let fail = seen == fail_at;
                seen += 1;
                if fail { Err(error.clone()) } else { Ok(()) }
            })
        });
        let returned = match result.unwrap_err() {
            TopologyError::Sweep(SweepError::Runtime(e)) => {
                stages[0] = true;
                e
            }
            TopologyError::Emit(EmitError::Runtime(e)) => {
                stages[1] = true;
                e
            }
            TopologyError::Graph(GraphError::Runtime(e)) => {
                stages[2] = true;
                e
            }
            e => panic!("unexpected error: {e:?}"),
        };
        assert_eq!(returned, error);
        assert_eq!(seen, fail_at + 1);
        assert_eq!(heap.net, 0);
        if fail_at == 0 {
            assert_eq!(heap.allocs, 0);
        }
    }
    assert_eq!(stages, [true; 3]);
}

#[test]
fn arc_topology_all_stage_allocations_and_simultaneous_peaks() {
    let n = 127;
    let slices: Vec<_> = (0..n)
        .map(|r| Slice {
            sheet: 0,
            col: 0,
            r0: r as u32,
            r1: r as u32,
        })
        .collect();
    let mut probes: Vec<_> = (0..n)
        .map(|reader| Probe {
            reader,
            sheet: 0,
            image: Rect::new(0, 0, 126, 0),
        })
        .collect();
    probes.push(Probe {
        reader: 0,
        sheet: 0,
        image: Rect::cell(0, 0),
    });
    let (result, m) = measure(None, || topology(&slices, &probes, None, None, None));
    let out = result.unwrap();
    assert_eq!(m.allocs, 23); // sweep 8, emission 4, CSR 3, Tarjan 8
    assert_eq!(m.peak as u64, out.peak_heap_bytes);
    assert_eq!(m.net as u64, out.heap_bytes());
    assert_eq!(out.components.len(), 1);
    for nth in 0..m.allocs {
        let (result, failed) = measure(Some(nth), || topology(&slices, &probes, None, None, None));
        assert!(failed.failed);
        assert!(matches!(
            result.unwrap_err(),
            TopologyError::Authority(AuthorityError::Alloc)
                | TopologyError::Sweep(SweepError::Authority(AuthorityError::Alloc))
                | TopologyError::Emit(EmitError::Authority(AuthorityError::Alloc))
                | TopologyError::Graph(GraphError::Authority(AuthorityError::Alloc))
        ));
        assert_eq!(failed.net, 0);
    }
    for limit in [0, out.peak_heap_bytes - 1] {
        let (result, failed) =
            measure(None, || topology(&slices, &probes, Some(limit), None, None));
        assert!(matches!(
            result.unwrap_err(),
            TopologyError::Authority(AuthorityError::Admission { .. })
                | TopologyError::Sweep(SweepError::Authority(AuthorityError::Admission { .. }))
                | TopologyError::Emit(EmitError::Authority(AuthorityError::Admission { .. }))
                | TopologyError::Graph(GraphError::Authority(AuthorityError::Admission { .. }))
        ));
        assert_eq!(failed.net, 0);
    }
    topology(
        &slices,
        &probes,
        Some(out.peak_heap_bytes),
        Some(out.emission.charged),
        Some(out.sweep.hits.len() as u64),
    )
    .unwrap();
    let (result, failed) = measure(None, || topology(&slices, &probes, None, Some(0), None));
    assert!(matches!(
        result.unwrap_err(),
        TopologyError::Emit(EmitError::SuperLinearWork { .. })
    ));
    assert_eq!(failed.net, 0);
    println!(
        "ARC_TOPOLOGY allocations={} peak={} retained={} work={}",
        m.allocs,
        m.peak,
        m.net,
        out.total_work()
    );
}
