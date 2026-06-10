//! Differential proof for the iterative Tarjan rewrite (scheduler.rs).
//!
//! The scheduler's recursive `tarjan_visit`/`tarjan_visit_with_virtual` were
//! rewritten with an explicit work stack (stack-overflow fix: a ~2000-deep
//! reverse-built chain or SCC SIGABRTed debug builds). Schedules must stay
//! deterministic, so the rewrite's HARD CONSTRAINT is byte-identical output:
//! same SCC emission order, same within-SCC member order.
//!
//! This module keeps the recursive implementation alive behind `#[cfg(test)]`
//! (`Scheduler::tarjan_scc_recursive_reference{,_with_virtual}`) and asserts
//! `Vec<Vec<VertexId>>` equality over seeded random graphs — including deep
//! chains (both directions), rings/self-loops (via virtual deps), and
//! multi-SCC meshes — plus randomized root-iteration orders. Everything is
//! seeded (xorshift64*), no wall-clock anywhere. The recursive reference runs
//! inside a 64 MiB thread so the *reference's* depth limit doesn't constrain
//! the shapes we can compare.

use crate::engine::{DependencyGraph, Scheduler, VertexId};
use formualizer_parse::parser::parse;
use rustc_hash::FxHashMap;

/* ─────────────────────────── seeded rng ──────────────────────────── */

struct XorShift64Star(u64);

impl XorShift64Star {
    fn new(seed: u64) -> Self {
        // Avoid the all-zero fixed point.
        Self(seed.wrapping_mul(2685821657736338717).max(1))
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(2685821657736338717)
    }
    /// Uniform-ish in `0..bound` (bound > 0).
    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            items.swap(i, self.below(i + 1));
        }
    }
}

/* ─────────────────────── graph construction ──────────────────────── */

/// Build a `DependencyGraph` whose vertex `i` (cell `A{i+1}`) depends on
/// `deps[i]` (other vertex indices; `i == j` is skipped — direct self-refs
/// are rejected at edit time, self-loops are exercised via virtual deps).
/// Returns the graph and the vertex ids in index order.
fn build_graph(n: usize, deps: &[Vec<usize>]) -> (DependencyGraph, Vec<VertexId>) {
    use super::common::abs_cell_ref;
    assert_eq!(deps.len(), n);
    let mut graph = DependencyGraph::new();
    for (i, dep_rows) in deps.iter().enumerate() {
        let row = (i + 1) as u32;
        let refs: Vec<String> = dep_rows
            .iter()
            .filter(|&&j| j != i)
            .map(|&j| format!("A{}", j + 1))
            .collect();
        if refs.is_empty() {
            graph
                .set_cell_formula("Sheet1", row, 1, parse("=1").unwrap())
                .unwrap();
        } else {
            let formula = format!("={}", refs.join("+"));
            graph
                .set_cell_formula("Sheet1", row, 1, parse(&formula).unwrap())
                .unwrap();
        }
    }
    let ids: Vec<VertexId> = (0..n)
        .map(|i| {
            *graph
                .cell_to_vertex()
                .get(&abs_cell_ref(0, (i + 1) as u32, 1))
                .expect("vertex for cell")
        })
        .collect();
    (graph, ids)
}

/// Assert byte-identical SCC output between the recursive reference and the
/// iterative implementation, for both the plain and the virtual-deps entry.
fn assert_differential(
    graph: &DependencyGraph,
    roots: &[VertexId],
    vdeps: &FxHashMap<VertexId, Vec<VertexId>>,
    label: &str,
) {
    let scheduler = Scheduler::new(graph);

    let reference = scheduler
        .tarjan_scc_recursive_reference(roots)
        .expect("recursive tarjan");
    let iterative = scheduler.tarjan_scc(roots).expect("iterative tarjan");
    assert_eq!(
        reference, iterative,
        "[{label}] plain tarjan_scc diverged from the recursive reference"
    );

    let reference_v = scheduler
        .tarjan_scc_with_virtual_recursive_reference(roots, vdeps)
        .expect("recursive tarjan (virtual)");
    let iterative_v = scheduler
        .tarjan_scc_with_virtual_for_tests(roots, vdeps)
        .expect("iterative tarjan (virtual)");
    assert_eq!(
        reference_v, iterative_v,
        "[{label}] tarjan_scc_with_virtual diverged from the recursive reference"
    );
}

/* ───────────────────────────── the test ──────────────────────────── */

fn run_random_differential() {
    // 220 seeded random graphs: mixed density, virtual deps (incl.
    // self-loops), and randomized root orders.
    for seed in 0..220u64 {
        let mut rng = XorShift64Star::new(seed.wrapping_add(0x9E3779B97F4A7C15));
        let n = 2 + rng.below(60);

        // Random adjacency: 0..=3 deps per vertex, pointing anywhere
        // (higher ids included — the reverse-direction shape that used to
        // overflow). Duplicate edges are kept: the dependency extractor may
        // dedup, but both implementations see the same final adjacency.
        let mut deps: Vec<Vec<usize>> = Vec::with_capacity(n);
        for _ in 0..n {
            let k = rng.below(4);
            deps.push((0..k).map(|_| rng.below(n)).collect());
        }
        let (graph, ids) = build_graph(n, &deps);

        // Virtual deps over ~1/3 of vertices; ~1/4 of those get a self-loop
        // (only reachable through vdeps — edit-time checks reject direct
        // self-references).
        let mut vdeps: FxHashMap<VertexId, Vec<VertexId>> = FxHashMap::default();
        for (i, &id) in ids.iter().enumerate() {
            if rng.below(3) == 0 {
                let mut extra: Vec<VertexId> =
                    (0..1 + rng.below(2)).map(|_| ids[rng.below(n)]).collect();
                if rng.below(4) == 0 {
                    extra.push(ids[i]); // self-loop
                }
                vdeps.insert(id, extra);
            }
        }

        // Root order 1: ascending vertex id (the production order).
        let mut roots = ids.clone();
        roots.sort_unstable();
        assert_differential(&graph, &roots, &vdeps, &format!("seed {seed} sorted"));

        // Root order 2: seeded shuffle (the contract must hold for any
        // caller-supplied order).
        rng.shuffle(&mut roots);
        assert_differential(&graph, &roots, &vdeps, &format!("seed {seed} shuffled"));
    }
}

fn run_deep_shape_differential() {
    let no_vdeps: FxHashMap<VertexId, Vec<VertexId>> = FxHashMap::default();

    // Reverse-built 3000-deep acyclic chain (deps point at HIGHER ids):
    // the shape that crashed the recursive scheduler at production depth.
    let n = 3000;
    let deps: Vec<Vec<usize>> = (0..n)
        .map(|i| if i + 1 < n { vec![i + 1] } else { vec![] })
        .collect();
    let (graph, mut roots) = build_graph(n, &deps);
    roots.sort_unstable();
    assert_differential(&graph, &roots, &no_vdeps, "reverse chain 3000");

    // Forward-built chain (control: shallow even for the reference).
    let deps: Vec<Vec<usize>> = (0..n)
        .map(|i| if i > 0 { vec![i - 1] } else { vec![] })
        .collect();
    let (graph, mut roots) = build_graph(n, &deps);
    roots.sort_unstable();
    assert_differential(&graph, &roots, &no_vdeps, "forward chain 3000");

    // One 2500-member ring (single deep SCC).
    let n = 2500;
    let deps: Vec<Vec<usize>> = (0..n).map(|i| vec![(i + 1) % n]).collect();
    let (graph, mut roots) = build_graph(n, &deps);
    roots.sort_unstable();
    assert_differential(&graph, &roots, &no_vdeps, "ring 2500");

    // Multi-SCC mesh: three 400-member rings bridged in a chain, plus a
    // 400-deep reverse tail feeding the first ring.
    let ring = 400;
    let n = 4 * ring;
    let mut deps: Vec<Vec<usize>> = vec![Vec::new(); n];
    for block in 0..3 {
        let base = block * ring;
        for i in 0..ring {
            deps[base + i].push(base + (i + 1) % ring);
        }
        if block > 0 {
            // Bridge: this ring's first member depends on the previous ring.
            deps[base].push(base - 1);
        }
    }
    for (i, dep) in deps.iter_mut().enumerate().take(n).skip(3 * ring) {
        // Reverse tail: each tail vertex depends on the NEXT tail vertex;
        // the last tail vertex depends into ring 0.
        if i + 1 < n {
            dep.push(i + 1);
        } else {
            dep.push(0);
        }
    }
    let (graph, mut roots) = build_graph(n, &deps);
    roots.sort_unstable();
    // Self-loop on one singleton via vdeps for good measure.
    let mut vdeps: FxHashMap<VertexId, Vec<VertexId>> = FxHashMap::default();
    vdeps.insert(roots[0], vec![roots[0]]);
    assert_differential(&graph, &roots, &vdeps, "3 rings + reverse tail");
}

#[test]
fn iterative_tarjan_is_byte_identical_to_recursive_reference() {
    // The recursive REFERENCE needs headroom for the deep shapes (it
    // overflows ~1500 frames on a default 2 MiB test stack), so the whole
    // differential runs on a 64 MiB thread.
    std::thread::Builder::new()
        .name("tarjan-differential".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            run_random_differential();
            run_deep_shape_differential();
        })
        .expect("spawn differential thread")
        .join()
        .expect("differential thread panicked");
}
