use super::alloc::measure;
use crate::engine::authority::plan_graph::{GraphError, PlanGraph};
use crate::engine::authority::store::AuthorityError;

/// Independent reachability oracle (no DFS, CSR, or SCC implementation shared).
fn check(nodes: usize, arcs: &[(usize, usize)]) {
    let graph = PlanGraph::build(nodes, arcs, None).unwrap();
    let components = graph.components(None).unwrap();
    let mut reach = vec![vec![false; nodes]; nodes];
    for (i, row) in reach.iter_mut().enumerate() {
        row[i] = true;
    }
    for &(from, to) in arcs {
        reach[from][to] = true;
    }
    for via in 0..nodes {
        for from in 0..nodes {
            for to in 0..nodes {
                reach[from][to] |= reach[from][via] && reach[via][to];
            }
        }
    }
    for (from, forward) in reach.iter().enumerate() {
        for (to, reverse) in reach.iter().enumerate() {
            assert_eq!(
                components.component_of[from] == components.component_of[to],
                forward[to] && reverse[from],
                "{nodes} nodes, arcs {arcs:?}, pair ({from}, {to})"
            );
        }
    }
    let mut seen = vec![false; nodes];
    for component in 0..components.len() {
        assert!(!components.members(component).is_empty());
        for &member in components.members(component) {
            assert!(!seen[member]);
            seen[member] = true;
            assert_eq!(components.component_of[member], component);
        }
    }
    assert!(seen.iter().all(|&v| v));
    for &(from, to) in arcs {
        assert!(components.component_of[from] >= components.component_of[to]);
    }
    for node in 0..nodes {
        let expected: Vec<_> = arcs
            .iter()
            .filter_map(|&(from, to)| (from == node).then_some(to))
            .collect();
        assert_eq!(graph.successors(node), expected);
    }
    // Exact loop counts, not an asymptotic estimate assigned after the run.
    assert_eq!(graph.work.total(), (3 * nodes + 3 * arcs.len() + 1) as u64);
    assert_eq!(
        components.work.total(),
        (7 * nodes + 2 * arcs.len() + 1) as u64
    );
}

#[test]
fn scc_generated_graphs_equal_independent_reachability() {
    for seed in 0..256u64 {
        let mut state = seed + 1;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            state >> 32
        };
        let nodes = (next() % 25) as usize;
        let mut arcs = Vec::new();
        for from in 0..nodes {
            for to in 0..nodes {
                if next() % 7 == 0 {
                    arcs.push((from, to));
                    if next() % 3 == 0 {
                        arcs.push((from, to));
                    }
                }
            }
        }
        check(nodes, &arcs);
    }
}

#[test]
fn scc_handles_empty_isolated_self_duplicate_and_finished_cross_edges() {
    check(0, &[]);
    check(4, &[]);
    check(1, &[(0, 0), (0, 0)]);
    check(
        7,
        &[
            (0, 1),
            (1, 0),
            (2, 1),
            (2, 3),
            (3, 4),
            (4, 3),
            (5, 6),
            (6, 5),
            (5, 3),
        ],
    );
    assert_eq!(
        PlanGraph::build(1, &[(0, 1)], None).unwrap_err(),
        GraphError::InvalidEndpoint
    );
    assert_eq!(
        PlanGraph::build(0, &[(0, 0)], None).unwrap_err(),
        GraphError::InvalidEndpoint
    );
    assert_eq!(
        PlanGraph::build(usize::MAX, &[], None).unwrap_err(),
        GraphError::Authority(AuthorityError::Alloc)
    );
}

#[test]
fn scc_arc_ownership_is_one_way_and_aux_paths_preserve_real_components() {
    // Four real cells, leaves 4..8, internal nodes 8..11. Arcs only go
    // owner -> leaf -> parent. A root -> reader probe makes 0 and 1 cyclic
    // only when both readers read the root; 2/3 stay external precedents.
    let mut arcs = vec![
        (0, 4),
        (1, 5),
        (2, 6),
        (3, 7),
        (4, 8),
        (5, 8),
        (6, 9),
        (7, 9),
        (8, 10),
        (9, 10),
    ];
    let graph = PlanGraph::build(11, &arcs, None).unwrap();
    assert_eq!(graph.components(None).unwrap().len(), 11);
    arcs.extend([(10, 0), (10, 1)]);
    check(11, &arcs);
    let graph = PlanGraph::build(11, &arcs, None).unwrap();
    let components = graph.components(None).unwrap();
    assert_eq!(components.component_of[0], components.component_of[1]);
    assert_ne!(components.component_of[0], components.component_of[2]);
    assert_ne!(components.component_of[0], components.component_of[3]);
    assert_ne!(components.component_of[2], components.component_of[3]);
}

#[test]
fn graph_and_scc_every_allocation_is_fallible_and_peak_is_exact() {
    let arcs: Vec<_> = (0..128).map(|i| (i, (i + 1) % 128)).collect();
    let (result, graph_measure) = measure(None, || PlanGraph::build(128, &arcs, None));
    let graph = result.unwrap();
    assert_eq!(graph_measure.peak as u64, graph.peak_heap_bytes);
    assert_eq!(graph_measure.net as u64, graph.heap_bytes());
    assert_eq!(graph_measure.allocs, 3);
    for nth in 0..graph_measure.allocs {
        let (failed, m) = measure(Some(nth), || PlanGraph::build(128, &arcs, None));
        assert!(m.failed);
        assert_eq!(
            failed.unwrap_err(),
            GraphError::Authority(AuthorityError::Alloc)
        );
        assert_eq!(m.net, 0);
    }
    let (result, scc_measure) = measure(None, || graph.components(None));
    let components = result.unwrap();
    assert_eq!(scc_measure.peak as u64, components.peak_heap_bytes);
    assert_eq!(scc_measure.net as u64, components.heap_bytes());
    assert_eq!(scc_measure.allocs, 8);
    for nth in 0..scc_measure.allocs {
        let (failed, m) = measure(Some(nth), || graph.components(None));
        assert!(m.failed);
        assert_eq!(
            failed.unwrap_err(),
            GraphError::Authority(AuthorityError::Alloc)
        );
        assert_eq!(m.net, 0);
    }
    for limit in [0, graph.peak_heap_bytes - 1] {
        let (failed, m) = measure(None, || PlanGraph::build(128, &arcs, Some(limit)));
        assert_eq!(
            failed.unwrap_err(),
            GraphError::Authority(AuthorityError::Admission {
                resource: "scratch",
                needed: graph.peak_heap_bytes,
                limit,
            })
        );
        assert_eq!(m.allocs, 0);
    }
    for limit in [0, components.peak_heap_bytes - 1] {
        let (failed, m) = measure(None, || graph.components(Some(limit)));
        assert_eq!(
            failed.unwrap_err(),
            GraphError::Authority(AuthorityError::Admission {
                resource: "scratch",
                needed: components.peak_heap_bytes,
                limit,
            })
        );
        assert_eq!(m.allocs, 0);
    }
    PlanGraph::build(128, &arcs, Some(graph.peak_heap_bytes)).unwrap();
    graph.components(Some(components.peak_heap_bytes)).unwrap();
    // Combined peak includes graph output coexisting with Tarjan, not just
    // the larger of the two helper peaks.
    let (result, combined) = measure(None, || {
        let graph = PlanGraph::build(128, &arcs, None)?;
        let components = graph.components(None)?;
        Ok::<_, GraphError>((graph, components))
    });
    let (g, c) = result.unwrap();
    assert_eq!(
        combined.peak as u64,
        g.peak_heap_bytes.max(g.heap_bytes() + c.peak_heap_bytes)
    );
    assert_eq!(combined.net as u64, g.heap_bytes() + c.heap_bytes());
    println!(
        "PLAN_GRAPH allocations={}+{} graph_peak={} scc_peak={} combined_peak={} retained={}",
        graph_measure.allocs,
        scc_measure.allocs,
        g.peak_heap_bytes,
        c.peak_heap_bytes,
        combined.peak,
        combined.net
    );
}

#[test]
fn graph_control_cancels_every_checkpoint_without_retaining_scratch() {
    use formualizer_common::{ExcelError, ExcelErrorKind};
    let arcs: Vec<_> = (0..8192).map(|i| (i, (i + 1) % 8192)).collect();
    let error = ExcelError::new(ExcelErrorKind::Value);
    for components in [false, true] {
        let graph = PlanGraph::build(8192, &arcs, None).unwrap();
        let mut calls = 0;
        let mut charged = 0;
        let mut checkpoint = |delta| {
            assert!(delta <= 4096);
            if calls == 0 {
                assert_eq!(delta, 0);
            }
            calls += 1;
            charged += delta;
            Ok(())
        };
        let expected = if components {
            graph
                .components_controlled(None, &mut checkpoint)
                .unwrap()
                .work
                .total()
        } else {
            PlanGraph::build_controlled(8192, &arcs, None, &mut checkpoint)
                .unwrap()
                .work
                .total()
        };
        assert_eq!(charged, expected);
        assert!(calls > 3);
        for fail_at in 0..calls {
            let mut seen = 0;
            let mut checkpoint = |_| {
                let fail = seen == fail_at;
                seen += 1;
                if fail { Err(error.clone()) } else { Ok(()) }
            };
            let (failed, heap) = measure(None, || {
                if components {
                    graph
                        .components_controlled(None, &mut checkpoint)
                        .map(|_| ())
                } else {
                    PlanGraph::build_controlled(8192, &arcs, None, &mut checkpoint).map(|_| ())
                }
            });
            assert_eq!(failed, Err(GraphError::Runtime(error.clone())));
            assert_eq!(seen, fail_at + 1);
            assert_eq!(heap.net, 0);
            if fail_at == 0 {
                assert_eq!(heap.allocs, 0);
            }
        }
    }
}

#[test]
fn graph_control_charges_actual_engine_work_ledger() {
    use crate::engine::resource_ledger::{EvaluationBudgets, ResourceLedger};
    use formualizer_common::{ExcelErrorExtra, ResourceExhaustionReason};
    let arcs: Vec<_> = (0..8192).map(|i| (i, (i + 1) % 8192)).collect();
    let graph = PlanGraph::build(8192, &arcs, None).unwrap();
    let total = graph.work.total() + graph.components(None).unwrap().work.total();
    for limit in [0, 4095, total - 1, total] {
        let mut budgets = EvaluationBudgets::default();
        budgets.work.max_work_units = Some(limit);
        let mut ledger = ResourceLedger::new(Some(172), budgets);
        let ((), heap) = measure(None, || {
            let result = (|| {
                let graph = PlanGraph::build_controlled(8192, &arcs, None, |delta| {
                    ledger.charge_work(delta).map_err(|e| e.into_excel_error())
                })?;
                graph.components_controlled(None, |delta| {
                    ledger.charge_work(delta).map_err(|e| e.into_excel_error())
                })?;
                Ok::<_, GraphError>(())
            })();
            if limit == total {
                result.unwrap();
                assert_eq!(ledger.snapshot().work_charged, total);
            } else {
                let GraphError::Runtime(error) = result.unwrap_err() else {
                    panic!("wrong error")
                };
                let ExcelErrorExtra::Resource { detail } = error.extra else {
                    panic!("missing resource detail")
                };
                assert_eq!(detail.reason, ResourceExhaustionReason::WorkUnits);
                assert_eq!(detail.limit, limit);
                assert_eq!(detail.request_id, Some(172));
                assert!(detail.observed > limit);
                assert!(detail.observed - limit <= 4096);
            }
        });
        assert_eq!(heap.net, 0);
    }
}

#[test]
fn scc_counted_scaling_and_deep_graphs_use_no_call_stack() {
    for nodes in [1024, 4096, 16384] {
        for cycle in [false, true] {
            let mut arcs: Vec<_> = (1..nodes).map(|i| (i - 1, i)).collect();
            if cycle {
                arcs.push((nodes - 1, 0));
            }
            let graph = PlanGraph::build(nodes, &arcs, None).unwrap();
            let components = graph.components(None).unwrap();
            assert_eq!(components.len(), if cycle { 1 } else { nodes });
            let total = graph.work.total() + components.work.total();
            assert_eq!(total, (10 * nodes + 5 * arcs.len() + 2) as u64);
            assert!(total <= (15 * nodes + 2) as u64);
            println!(
                "PLAN_GRAPH nodes={nodes} cycle={cycle} edges={} csr_work={} scc_work={} total={total} combined_peak={}",
                arcs.len(),
                graph.work.total(),
                components.work.total(),
                graph
                    .peak_heap_bytes
                    .max(graph.heap_bytes() + components.peak_heap_bytes)
            );
        }
    }
}
