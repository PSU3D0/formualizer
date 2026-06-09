//! Tests for condensation-ordered `Schedule::units` (Stage 0 of the
//! cycle-evaluation work; pre-work for RFC #112).
//!
//! `Schedule::units` is the canonical walk order: each cyclic SCC is a
//! `ScheduleUnit::Cycle` super-node positioned after all units containing its
//! external dependencies and before all units containing its external
//! dependents. Acyclic vertices are grouped into `ScheduleUnit::Layer` waves
//! exactly as before.

use super::common::get_vertex_ids_in_order;
use crate::engine::scheduler::ScheduleUnit;
use crate::engine::{DependencyGraph, Scheduler, VertexId};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use rustc_hash::FxHashSet;

/// Helper to create a cell reference AST node (row/col are 1-based).
fn ref_ast(row: u32, col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{row}C{col}"),
            reference: ReferenceType::cell(None, row, col),
        },
        source_token: None,
        contains_volatile: false,
    }
}

/// Build `=ref1 + ref2 + ...` as a chain of binary ops.
fn sum_refs_ast(refs: &[(u32, u32)]) -> ASTNode {
    assert!(!refs.is_empty());
    let mut iter = refs.iter();
    let first = iter.next().unwrap();
    let mut ast = ref_ast(first.0, first.1);
    for &(r, c) in iter {
        ast = ASTNode {
            node_type: ASTNodeType::BinaryOp {
                op: "+".to_string(),
                left: Box::new(ast),
                right: Box::new(ref_ast(r, c)),
            },
            source_token: None,
            contains_volatile: false,
        };
    }
    ast
}

fn vertex_id_at(graph: &DependencyGraph, row: u32, col: u32) -> VertexId {
    *graph
        .cell_to_vertex()
        .get(&super::common::abs_cell_ref(0, row, col))
        .unwrap()
}

/// Map each scheduled vertex to the index of the unit that contains it.
fn unit_positions(units: &[ScheduleUnit]) -> rustc_hash::FxHashMap<VertexId, usize> {
    let mut pos = rustc_hash::FxHashMap::default();
    for (i, unit) in units.iter().enumerate() {
        let members: &[VertexId] = match unit {
            ScheduleUnit::Layer(layer) => &layer.vertices,
            ScheduleUnit::Cycle(cycle) => cycle,
        };
        for &v in members {
            let prev = pos.insert(v, i);
            assert!(prev.is_none(), "vertex {v:?} appears in more than one unit");
        }
    }
    pos
}

/// U -> SCC{A,B} -> D: the cycle unit must sit between its upstream layer and
/// its dependent's layer.
#[test]
fn units_position_cycle_between_upstream_and_dependent() {
    let mut graph = DependencyGraph::new();

    // U: A1 = 42 (value, upstream of the cycle)
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();
    // SCC: B1 = A1 + C1, C1 = B1
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_refs_ast(&[(1, 1), (1, 3)]))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 3, ref_ast(1, 2))
        .unwrap();
    // D: D1 = B1 (dependent of the cycle)
    graph
        .set_cell_formula("Sheet1", 1, 4, ref_ast(1, 2))
        .unwrap();

    let u_id = vertex_id_at(&graph, 1, 1);
    let b_id = vertex_id_at(&graph, 1, 2);
    let c_id = vertex_id_at(&graph, 1, 3);
    let d_id = vertex_id_at(&graph, 1, 4);

    let scheduler = Scheduler::new(&graph);
    let all = get_vertex_ids_in_order(&graph);
    let schedule = scheduler.create_schedule(&all).unwrap();

    assert_eq!(schedule.units.len(), 3, "units: {:?}", schedule.units);
    match &schedule.units[0] {
        ScheduleUnit::Layer(l) => assert_eq!(l.vertices, vec![u_id]),
        other => panic!("unit 0 should be Layer{{U}}, got {other:?}"),
    }
    match &schedule.units[1] {
        ScheduleUnit::Cycle(c) => {
            let set: FxHashSet<VertexId> = c.iter().copied().collect();
            assert_eq!(set.len(), 2);
            assert!(set.contains(&b_id) && set.contains(&c_id));
        }
        other => panic!("unit 1 should be Cycle{{B,C}}, got {other:?}"),
    }
    match &schedule.units[2] {
        ScheduleUnit::Layer(l) => assert_eq!(l.vertices, vec![d_id]),
        other => panic!("unit 2 should be Layer{{D}}, got {other:?}"),
    }

    // Compatibility views are preserved.
    assert_eq!(schedule.cycles.len(), 1);
    assert_eq!(schedule.layers.len(), 2);
    assert_eq!(schedule.layers[0].vertices, vec![u_id]);
    assert_eq!(schedule.layers[1].vertices, vec![d_id]);
}

/// SCC1 -> mid vertex -> SCC2: condensation order chains across SCCs.
#[test]
fn units_order_multi_scc_chain() {
    let mut graph = DependencyGraph::new();

    // SCC1: A1 = B1, B1 = A1
    graph
        .set_cell_formula("Sheet1", 1, 1, ref_ast(1, 2))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 2, ref_ast(1, 1))
        .unwrap();
    // Mid: C1 = A1
    graph
        .set_cell_formula("Sheet1", 1, 3, ref_ast(1, 1))
        .unwrap();
    // SCC2: D1 = C1 + E1, E1 = D1
    graph
        .set_cell_formula("Sheet1", 1, 4, sum_refs_ast(&[(1, 3), (1, 5)]))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 5, ref_ast(1, 4))
        .unwrap();

    let a_id = vertex_id_at(&graph, 1, 1);
    let b_id = vertex_id_at(&graph, 1, 2);
    let c_id = vertex_id_at(&graph, 1, 3);
    let d_id = vertex_id_at(&graph, 1, 4);
    let e_id = vertex_id_at(&graph, 1, 5);

    let scheduler = Scheduler::new(&graph);
    let all = get_vertex_ids_in_order(&graph);
    let schedule = scheduler.create_schedule(&all).unwrap();

    assert_eq!(schedule.units.len(), 3, "units: {:?}", schedule.units);
    match &schedule.units[0] {
        ScheduleUnit::Cycle(c) => {
            let set: FxHashSet<VertexId> = c.iter().copied().collect();
            assert!(set.contains(&a_id) && set.contains(&b_id));
        }
        other => panic!("unit 0 should be Cycle{{A,B}}, got {other:?}"),
    }
    match &schedule.units[1] {
        ScheduleUnit::Layer(l) => assert_eq!(l.vertices, vec![c_id]),
        other => panic!("unit 1 should be Layer{{C}}, got {other:?}"),
    }
    match &schedule.units[2] {
        ScheduleUnit::Cycle(c) => {
            let set: FxHashSet<VertexId> = c.iter().copied().collect();
            assert!(set.contains(&d_id) && set.contains(&e_id));
        }
        other => panic!("unit 2 should be Cycle{{D,E}}, got {other:?}"),
    }
    assert_eq!(schedule.cycles.len(), 2);
    assert_eq!(schedule.layers.len(), 1);
}

/// Two independent SCCs land in the same Kahn wave; their relative order is
/// deterministic: ascending by smallest member VertexId.
#[test]
fn independent_cycles_same_wave_sorted_by_smallest_member() {
    let mut graph = DependencyGraph::new();

    // SCC-a: A1 = B1, B1 = A1 (created first => smaller vertex ids)
    graph
        .set_cell_formula("Sheet1", 1, 1, ref_ast(1, 2))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 2, ref_ast(1, 1))
        .unwrap();
    // SCC-b: C1 = D1, D1 = C1
    graph
        .set_cell_formula("Sheet1", 1, 3, ref_ast(1, 4))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 4, ref_ast(1, 3))
        .unwrap();

    let a_id = vertex_id_at(&graph, 1, 1);
    let c_id = vertex_id_at(&graph, 1, 3);
    assert!(a_id < c_id, "test setup expects A1 created before C1");

    let scheduler = Scheduler::new(&graph);
    let all = get_vertex_ids_in_order(&graph);
    let schedule = scheduler.create_schedule(&all).unwrap();

    assert_eq!(schedule.units.len(), 2, "units: {:?}", schedule.units);
    match (&schedule.units[0], &schedule.units[1]) {
        (ScheduleUnit::Cycle(first), ScheduleUnit::Cycle(second)) => {
            assert!(first.contains(&a_id), "smaller-id SCC must come first");
            assert!(second.contains(&c_id));
        }
        other => panic!("expected two Cycle units, got {other:?}"),
    }
    assert!(schedule.layers.is_empty());
    assert_eq!(schedule.cycles.len(), 2);
}

/// Cycle-free schedules must be byte-for-byte the legacy layer construction:
/// `units` is exactly the old layers wrapped, and `layers` matches
/// `build_layers` output.
#[test]
fn fast_path_units_match_legacy_layers() {
    let mut graph = DependencyGraph::new();

    // Diamond: A1, A2 values; B1 = A1 + A2; C1 = B1.
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(2))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_refs_ast(&[(1, 1), (2, 1)]))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 3, ref_ast(1, 2))
        .unwrap();

    let scheduler = Scheduler::new(&graph);
    let all = get_vertex_ids_in_order(&graph);
    let schedule = scheduler.create_schedule(&all).unwrap();
    assert!(schedule.cycles.is_empty());

    // Reconstruct layers the pre-change way: tarjan -> separate -> build_layers.
    let sccs = scheduler.tarjan_scc(&all).unwrap();
    let (cycles, acyclic_sccs) = scheduler.separate_cycles(sccs);
    assert!(cycles.is_empty());
    let legacy_layers = scheduler.build_layers(acyclic_sccs).unwrap();

    assert_eq!(schedule.layers.len(), legacy_layers.len());
    for (got, want) in schedule.layers.iter().zip(legacy_layers.iter()) {
        assert_eq!(got.vertices, want.vertices);
    }

    // Units are exactly those layers, wrapped, in order.
    assert_eq!(schedule.units.len(), legacy_layers.len());
    for (unit, want) in schedule.units.iter().zip(legacy_layers.iter()) {
        match unit {
            ScheduleUnit::Layer(l) => assert_eq!(l.vertices, want.vertices),
            other => panic!("fast path must emit only Layer units, got {other:?}"),
        }
    }
}

/// Randomized invariant: every Cycle unit appears strictly after all units
/// containing its external dependencies and strictly before all units
/// containing its external dependents.
#[test]
fn cycle_units_respect_condensation_invariant_random() {
    for seed in 1u64..=24 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut next = || {
            // xorshift64*
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            rng.wrapping_mul(0x2545_F491_4F6C_DD1D)
        };

        const N: u32 = 12;
        let mut graph = DependencyGraph::new();

        // Random forward DAG edges j -> i (i < j), plus a few back edges
        // i -> j (i < j) to create cycles. Cell (row r, col 1) for r in 1..=N.
        let mut deps: Vec<Vec<u32>> = vec![Vec::new(); (N + 1) as usize];
        for j in 2..=N {
            for i in 1..j {
                if next() % 100 < 30 {
                    deps[j as usize].push(i);
                }
            }
        }
        for _ in 0..3 {
            let i = 1 + (next() % (N as u64 - 1)) as u32; // 1..N
            let j = i + 1 + (next() % ((N - i) as u64)) as u32; // i+1..=N
            deps[i as usize].push(j);
        }

        for r in 1..=N {
            let d = &deps[r as usize];
            if d.is_empty() {
                graph
                    .set_cell_value("Sheet1", r, 1, LiteralValue::Int(r as i64))
                    .unwrap();
            } else {
                let refs: Vec<(u32, u32)> = d.iter().map(|&dr| (dr, 1)).collect();
                graph
                    .set_cell_formula("Sheet1", r, 1, sum_refs_ast(&refs))
                    .unwrap();
            }
        }

        let scheduler = Scheduler::new(&graph);
        let all = get_vertex_ids_in_order(&graph);
        let scheduled: FxHashSet<VertexId> = all.iter().copied().collect();
        let schedule = scheduler.create_schedule(&all).unwrap();

        let pos = unit_positions(&schedule.units);
        assert_eq!(
            pos.len(),
            all.len(),
            "seed {seed}: every scheduled vertex must appear in exactly one unit"
        );

        for (idx, unit) in schedule.units.iter().enumerate() {
            let ScheduleUnit::Cycle(members) = unit else {
                continue;
            };
            let member_set: FxHashSet<VertexId> = members.iter().copied().collect();
            for &v in members {
                for dep in graph.get_dependencies(v) {
                    if scheduled.contains(&dep) && !member_set.contains(&dep) {
                        assert!(
                            pos[&dep] < idx,
                            "seed {seed}: external dependency {dep:?} of cycle at unit {idx} \
                             is at unit {} (must be earlier)",
                            pos[&dep]
                        );
                    }
                }
                for dependent in graph.get_dependents(v) {
                    if scheduled.contains(&dependent) && !member_set.contains(&dependent) {
                        assert!(
                            pos[&dependent] > idx,
                            "seed {seed}: external dependent {dependent:?} of cycle at unit {idx} \
                             is at unit {} (must be later)",
                            pos[&dependent]
                        );
                    }
                }
            }
        }
    }
}
