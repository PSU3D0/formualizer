use super::alloc::measure;
use crate::engine::authority::arc_emit::{Column, EmitError, Hit, emit};
use crate::engine::authority::plan_graph::PlanGraph;
use crate::engine::authority::store::AuthorityError;

// Independent direct-hit reachability oracle; no tree, SCC or CSR algorithm.
fn check(n: usize, columns: &[Column], hits: &[Hit]) {
    let out = emit(n, columns, hits, None, None).unwrap();
    let graph = PlanGraph::build(out.nodes, &out.arcs, None).unwrap();
    let scc = graph.components(None).unwrap();
    let mut reach = vec![vec![false; n]; n];
    let mut selfdep = vec![0; n];
    let mut h = 0;
    for hit in hits {
        for (precedent, row) in reach.iter_mut().enumerate().take(hit.end).skip(hit.start) {
            row[hit.reader] = true;
            h += 1;
            if precedent == hit.reader {
                selfdep[hit.reader] = 1;
            }
        }
    }
    for (i, row) in reach.iter_mut().enumerate() {
        row[i] = true;
    }
    for via in 0..n {
        for from in 0..n {
            for to in 0..n {
                reach[from][to] |= reach[from][via] && reach[via][to];
            }
        }
    }
    assert_eq!(out.selfdep, selfdep);
    assert_eq!(out.pairwise_equivalent_hits, h);
    assert!(out.reader_arcs <= h);
    assert!(out.structural_arcs + out.aux_nodes <= 5 * n as u64);
    // Compare all real reachability, not just SCC equality: an extra one-way
    // path can leave SCCs unchanged but make the eventual schedule illegal.
    for (from, expected) in reach.iter().enumerate() {
        let mut seen = vec![false; out.nodes];
        let mut stack = vec![from];
        while let Some(node) = stack.pop() {
            if seen[node] {
                continue;
            }
            seen[node] = true;
            stack.extend(graph.successors(node));
        }
        assert_eq!(&seen[..n], expected);
        for (to, reverse) in reach.iter().enumerate() {
            assert_eq!(
                scc.component_of[from] == scc.component_of[to],
                expected[to] && reverse[from]
            );
        }
    }
    for component in 0..scc.len() {
        let members = scc.members(component);
        assert!(
            members.len() == 1 || members.iter().any(|&m| m < n),
            "aux-only cycle"
        );
    }
    for w in &out.pair_witnesses {
        let hit = &hits[w.hit];
        assert!((hit.start..hit.end).contains(&w.precedent));
    }
    assert_eq!(
        out.charged,
        out.reader_arcs + out.structural_arcs + out.aux_nodes
    );
}

#[test]
fn arc_emit_generated_hits_equal_direct_reachability() {
    for seed in 0..128u64 {
        let mut state = seed + 1;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state >> 32
        };
        let mut columns = Vec::new();
        let mut n = 0;
        for _ in 0..1 + next() % 3 {
            let len = 1 + (next() % 37) as usize;
            columns.push(Column {
                start: n,
                end: n + len,
            });
            n += len;
        }
        let mut hits = Vec::new();
        for reader in 0..n {
            for _ in 0..next() % 4 {
                let column = next() as usize % columns.len();
                let c = columns[column];
                let a = c.start + next() as usize % (c.end - c.start + 1);
                let b = c.start + next() as usize % (c.end - c.start + 1);
                hits.push(Hit {
                    reader,
                    column,
                    start: a.min(b),
                    end: a.max(b),
                });
            }
        }
        check(n, &columns, &hits);
    }
}

#[test]
fn arc_emit_arbitrary_tree_sizes_all_intervals_and_ownership() {
    check(0, &[], &[]);
    check(
        1,
        &[Column { start: 0, end: 1 }],
        &[Hit {
            reader: 0,
            column: 0,
            start: 0,
            end: 1,
        }],
    );
    // Exhaustively check every interval in power-of-two and non-power-of-two
    // trees. Reader is isolated in another column, so there is no cycle and
    // exactly the selected leaves can reach it.
    for n in [17, 24, 31, 32, 33] {
        let columns = [
            Column { start: 0, end: n },
            Column {
                start: n,
                end: n + 1,
            },
        ];
        for start in 0..=n {
            for end in start..=n {
                check(
                    n + 1,
                    &columns,
                    &[Hit {
                        reader: n,
                        column: 0,
                        start,
                        end,
                    }],
                );
            }
        }
    }
}

#[test]
fn arc_emit_invalid_intervals_never_become_cycles() {
    let cols = [Column { start: 0, end: 8 }];
    for hit in [
        Hit {
            reader: 8,
            column: 0,
            start: 0,
            end: 8,
        },
        Hit {
            reader: 0,
            column: 1,
            start: 0,
            end: 8,
        },
        Hit {
            reader: 0,
            column: 0,
            start: 0,
            end: 9,
        },
        Hit {
            reader: 0,
            column: 0,
            start: 4,
            end: 3,
        },
    ] {
        assert_eq!(
            emit(8, &cols, &[hit], None, None).unwrap_err(),
            EmitError::InvalidInput
        );
    }
    assert_eq!(
        emit(8, &[Column { start: 1, end: 8 }], &[], None, None).unwrap_err(),
        EmitError::InvalidInput
    );
    assert_eq!(
        emit(8, &[], &[], None, None).unwrap_err(),
        EmitError::InvalidInput
    );
}

#[test]
fn arc_emit_every_allocation_cap_and_exact_admission() {
    let n = 127;
    let columns = [Column { start: 0, end: n }];
    let mut hits: Vec<_> = (0..n)
        .map(|reader| Hit {
            reader,
            column: 0,
            start: 0,
            end: n,
        })
        .collect();
    hits.push(Hit {
        reader: 0,
        column: 0,
        start: 0,
        end: 3,
    });
    let (result, m) = measure(None, || emit(n, &columns, &hits, None, None));
    let out = result.unwrap();
    assert_eq!(m.allocs, 4);
    assert_eq!(m.peak as u64, out.peak_heap_bytes);
    assert_eq!(m.net as u64, out.heap_bytes());
    for nth in 0..m.allocs {
        let (result, failed) = measure(Some(nth), || emit(n, &columns, &hits, None, None));
        assert!(failed.failed);
        assert_eq!(
            result.unwrap_err(),
            EmitError::Authority(AuthorityError::Alloc)
        );
        assert_eq!(failed.net, 0);
    }
    for limit in [0, out.peak_heap_bytes - 1] {
        let (result, rejected) = measure(None, || emit(n, &columns, &hits, Some(limit), None));
        assert!(matches!(
            result.unwrap_err(),
            EmitError::Authority(AuthorityError::Admission {
                resource: "scratch",
                ..
            })
        ));
        assert_eq!(rejected.net, 0);
    }
    emit(
        n,
        &columns,
        &hits,
        Some(out.peak_heap_bytes),
        Some(out.charged),
    )
    .unwrap();
    for limit in [0, out.charged - 1] {
        let (result, rejected) = measure(None, || emit(n, &columns, &hits, None, Some(limit)));
        let EmitError::SuperLinearWork {
            charged,
            limit: actual,
            work,
        } = result.unwrap_err()
        else {
            panic!("expected typed cap error")
        };
        assert!(charged > limit);
        assert_eq!(actual, limit);
        assert!(work.total() > 0);
        assert_eq!(rejected.net, 0);
        assert_eq!(rejected.allocs, 1); // output is never allocated on cap failure
    }
    println!(
        "ARC_EMIT allocations={} peak={} retained={} charged={}",
        m.allocs, m.peak, m.net, out.charged
    );
}

#[test]
fn arc_emit_dense_vertical_counted_scaling() {
    for n in [1024, 4096, 16384] {
        let columns = [Column { start: 0, end: n }];
        let hits: Vec<_> = (0..n)
            .map(|reader| Hit {
                reader,
                column: 0,
                start: 0,
                end: n,
            })
            .collect();
        let out = emit(n, &columns, &hits, None, None).unwrap();
        assert_eq!(out.reader_arcs, n as u64);
        assert_eq!(out.aux_nodes, (2 * n - 1) as u64);
        assert_eq!(out.structural_arcs, (3 * n - 2) as u64);
        assert_eq!(out.charged, (6 * n - 3) as u64);
        assert_eq!(out.pairwise_equivalent_hits, (n * n) as u64);
        // Full-column queries select the root directly, even for arbitrary
        // tree sizes. This is emitter-only, not whole-planner evidence.
        assert_eq!(out.work.total(), 1 + 8 * n as u64);
        let graph = PlanGraph::build(out.nodes, &out.arcs, None).unwrap();
        let scc = graph.components(None).unwrap();
        assert_eq!(scc.len(), 1);
        println!(
            "ARC_EMIT n={n} H={} arcs={} aux={} charged={} work={} peak={}",
            out.pairwise_equivalent_hits,
            out.arcs.len(),
            out.aux_nodes,
            out.charged,
            out.work.total(),
            out.peak_heap_bytes
        );
    }
}
