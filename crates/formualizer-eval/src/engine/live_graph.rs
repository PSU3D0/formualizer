//! Local SCC analysis over a recorded live-edge list (Stage 2 of the
//! runtime-cycle-verdicts work, RFC #112).
//!
//! `evaluate_scc_unit` records live edges as `(from_idx, to_idx)` pairs over
//! the member index space of one statically-cyclic SCC (`from` *reads* `to`,
//! i.e. `from` depends on `to`). This module classifies that small index
//! graph:
//!
//! * which members sit on a **live cycle** (SCC of size > 1, or a self-loop),
//! * a deterministic **live-topological order** (dependencies before
//!   dependents) used for stale-reader settling and the post-stamp
//!   consistency pass.
//!
//! The scheduler's Tarjan reads dependency-graph adjacency, not edge lists,
//! so this is a separate ~80-line iterative implementation. It is
//! deterministic given a deterministic edge list: callers must pass sorted
//! edges (the collector hands out a hash set; sort before calling).

/// Classification of the live index graph of one SCC task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveGraphAnalysis {
    /// `in_cycle[i]` — member `i` lies on a live cycle (its live SCC has
    /// size > 1, or it has a live self-edge).
    pub in_cycle: Vec<bool>,
    /// Number of distinct live cycles (cyclic live SCCs).
    pub cycle_count: usize,
    /// All member indices in live-topological order: every member appears
    /// after all members it has a live edge *to* (its live dependencies).
    /// Members of one cyclic SCC appear contiguously (their internal order is
    /// deterministic but otherwise meaningless).
    pub topo: Vec<u32>,
}

impl LiveGraphAnalysis {
    /// `pos[i]` = position of member `i` in `topo` (for ordering subsets).
    pub fn topo_positions(&self) -> Vec<u32> {
        let mut pos = vec![0u32; self.topo.len()];
        for (p, &i) in self.topo.iter().enumerate() {
            pos[i as usize] = p as u32;
        }
        pos
    }
}

/// Iterative Tarjan over `n` nodes and directed `edges` (`from` depends on
/// `to`). `edges` must be sorted and deduplicated for deterministic output.
pub(crate) fn analyze_live_graph(n: usize, edges: &[(u32, u32)]) -> LiveGraphAnalysis {
    debug_assert!(
        edges.is_sorted(),
        "live edges must be sorted for determinism"
    );

    // CSR adjacency.
    let mut adj_start = vec![0usize; n + 1];
    for &(from, _) in edges {
        adj_start[from as usize + 1] += 1;
    }
    for i in 0..n {
        adj_start[i + 1] += adj_start[i];
    }
    // `edges` is sorted by `from`, so the slice for node i is contiguous.
    let adj = |i: usize| -> &[(u32, u32)] { &edges[adj_start[i]..adj_start[i + 1]] };

    const UNVISITED: u32 = u32::MAX;
    let mut index = vec![UNVISITED; n];
    let mut lowlink = vec![0u32; n];
    let mut on_stack = vec![false; n];
    let mut stack: Vec<u32> = Vec::new();
    let mut next_index = 0u32;

    let mut in_cycle = vec![false; n];
    let mut cycle_count = 0usize;
    // Tarjan emits an SCC only after all SCCs it depends on were emitted, so
    // emission order == live-topological order (dependencies first).
    let mut topo: Vec<u32> = Vec::with_capacity(n);

    // Explicit DFS frames: (node, next-edge-offset within its adjacency).
    let mut frames: Vec<(u32, usize)> = Vec::new();
    for root in 0..n as u32 {
        if index[root as usize] != UNVISITED {
            continue;
        }
        frames.push((root, 0));
        index[root as usize] = next_index;
        lowlink[root as usize] = next_index;
        next_index += 1;
        stack.push(root);
        on_stack[root as usize] = true;

        while let Some(&(v, next_edge)) = frames.last() {
            let vu = v as usize;
            if let Some(&(_, w)) = adj(vu).get(next_edge) {
                frames.last_mut().expect("frame exists").1 += 1;
                let wu = w as usize;
                if index[wu] == UNVISITED {
                    index[wu] = next_index;
                    lowlink[wu] = next_index;
                    next_index += 1;
                    stack.push(w);
                    on_stack[wu] = true;
                    frames.push((w, 0));
                } else if on_stack[wu] {
                    lowlink[vu] = lowlink[vu].min(index[wu]);
                }
            } else {
                // v is exhausted: maybe emit an SCC, then propagate lowlink.
                if lowlink[vu] == index[vu] {
                    let scc_start = topo.len();
                    loop {
                        let w = stack.pop().expect("tarjan stack underflow");
                        on_stack[w as usize] = false;
                        topo.push(w);
                        if w == v {
                            break;
                        }
                    }
                    let members = &mut topo[scc_start..];
                    // Deterministic intra-SCC order (pop order depends on DFS).
                    members.sort_unstable();
                    let cyclic = members.len() > 1 || adj(vu).iter().any(|&(_, w)| w == v); // self-loop
                    if cyclic {
                        cycle_count += 1;
                        for &m in topo[scc_start..].iter() {
                            in_cycle[m as usize] = true;
                        }
                    }
                }
                frames.pop();
                if let Some(&(parent, _)) = frames.last() {
                    let pu = parent as usize;
                    lowlink[pu] = lowlink[pu].min(lowlink[vu]);
                }
            }
        }
    }

    LiveGraphAnalysis {
        in_cycle,
        cycle_count,
        topo,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyze(n: usize, mut edges: Vec<(u32, u32)>) -> LiveGraphAnalysis {
        edges.sort_unstable();
        edges.dedup();
        analyze_live_graph(n, &edges)
    }

    fn assert_topo_consistent(a: &LiveGraphAnalysis, n: usize, edges: &[(u32, u32)]) {
        assert_eq!(a.topo.len(), n);
        let pos = a.topo_positions();
        for &(from, to) in edges {
            if from == to {
                continue;
            }
            // Within a cyclic SCC ordering constraints don't apply.
            if a.in_cycle[from as usize] && a.in_cycle[to as usize] {
                continue;
            }
            assert!(
                pos[to as usize] < pos[from as usize],
                "dependency {to} must precede reader {from} in topo {:?}",
                a.topo
            );
        }
    }

    #[test]
    fn empty_graph_is_acyclic() {
        let a = analyze(3, vec![]);
        assert_eq!(a.cycle_count, 0);
        assert_eq!(a.in_cycle, vec![false; 3]);
        assert_eq!(a.topo.len(), 3);
    }

    #[test]
    fn self_loop_is_a_cycle() {
        let a = analyze(2, vec![(0, 0)]);
        assert_eq!(a.cycle_count, 1);
        assert_eq!(a.in_cycle, vec![true, false]);
    }

    #[test]
    fn two_cycle_detected() {
        let edges = vec![(0, 1), (1, 0)];
        let a = analyze(2, edges.clone());
        assert_eq!(a.cycle_count, 1);
        assert_eq!(a.in_cycle, vec![true, true]);
        assert_topo_consistent(&a, 2, &edges);
    }

    #[test]
    fn chain_is_acyclic_with_deps_first_topo() {
        // 0 reads 1, 1 reads 2: topo must be [2, 1, 0].
        let edges = vec![(0, 1), (1, 2)];
        let a = analyze(3, edges.clone());
        assert_eq!(a.cycle_count, 0);
        assert_eq!(a.in_cycle, vec![false; 3]);
        assert_eq!(a.topo, vec![2, 1, 0]);
        assert_topo_consistent(&a, 3, &edges);
    }

    #[test]
    fn disjoint_components_cycle_and_chain() {
        // Component A: 0 <-> 1 (cycle). Component B: 2 reads 3 (chain).
        // Node 4 isolated.
        let edges = vec![(0, 1), (1, 0), (2, 3)];
        let a = analyze(5, edges.clone());
        assert_eq!(a.cycle_count, 1);
        assert_eq!(a.in_cycle, vec![true, true, false, false, false]);
        assert_topo_consistent(&a, 5, &edges);
    }

    #[test]
    fn cycle_with_downstream_reader() {
        // 2 reads the cycle {0,1}; 3 reads 2.
        let edges = vec![(0, 1), (1, 0), (2, 0), (3, 2)];
        let a = analyze(4, edges.clone());
        assert_eq!(a.cycle_count, 1);
        assert_eq!(a.in_cycle, vec![true, true, false, false]);
        let pos = a.topo_positions();
        assert!(pos[0] < pos[2] && pos[1] < pos[2] && pos[2] < pos[3]);
    }

    #[test]
    fn two_distinct_cycles_counted() {
        let edges = vec![(0, 1), (1, 0), (2, 2)];
        let a = analyze(3, edges);
        assert_eq!(a.cycle_count, 2);
        assert_eq!(a.in_cycle, vec![true, true, true]);
    }

    #[test]
    fn deterministic_for_same_input() {
        let edges = vec![(0, 2), (1, 2), (2, 3), (4, 0), (4, 1)];
        let a1 = analyze(5, edges.clone());
        let a2 = analyze(5, edges);
        assert_eq!(a1, a2);
    }
}
