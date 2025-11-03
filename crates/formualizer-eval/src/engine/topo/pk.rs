use rustc_hash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;

/// GraphView abstracts the conceptual DAG over which we maintain order.
/// Implementations should provide successors (dependents) and predecessors (dependencies)
/// for a node, using the engine's storage as the source of truth. The provided Vecs are
/// scratch buffers owned by the caller; implementors should push into them without heap churn.
pub trait GraphView<N: Copy + Eq + std::hash::Hash> {
    fn successors(&self, n: N, out: &mut Vec<N>);
    fn predecessors(&self, n: N, out: &mut Vec<N>);
    fn exists(&self, n: N) -> bool;
}

#[derive(Debug, Clone)]
pub struct Cycle<N> {
    pub path: Vec<N>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PkStats {
    pub relabeled: usize,
    pub dfs_visited: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct PkConfig {
    pub visit_budget: usize,
    pub compaction_interval_ops: u64,
}

impl Default for PkConfig {
    fn default() -> Self {
        Self {
            visit_budget: 50_000,
            compaction_interval_ops: 100_000,
        }
    }
}

/// DynamicTopo maintains a deterministic total order (pos) consistent with the conceptual DAG.
#[derive(Debug)]
pub struct DynamicTopo<N: Copy + Eq + std::hash::Hash + Ord> {
    pos: FxHashMap<N, u32>,
    order: Vec<N>,
    op_count: u64,
    cfg: PkConfig,
    // scratch
    succ_buf: Vec<N>,
    pred_buf: Vec<N>,
}

impl<N> DynamicTopo<N>
where
    N: Copy + Eq + std::hash::Hash + Ord,
{
    pub fn new(nodes: impl IntoIterator<Item = N>, cfg: PkConfig) -> Self {
        let mut order: Vec<N> = nodes.into_iter().collect();
        order.sort(); // stable deterministic seed order
        let mut pos = FxHashMap::default();
        for (i, n) in order.iter().enumerate() {
            pos.insert(*n, i as u32);
        }
        Self {
            pos,
            order,
            op_count: 0,
            cfg,
            succ_buf: Vec::new(),
            pred_buf: Vec::new(),
        }
    }

    /// Full rebuild via Kahn-style topological sort. Breaks ties by node Ord.
    pub fn rebuild_full<G: GraphView<N>>(&mut self, graph: &G) {
        // Collect existing nodes; drop non-existent
        let mut nodes: Vec<N> = self
            .order
            .iter()
            .copied()
            .filter(|n| graph.exists(*n))
            .collect();
        nodes.sort();
        let set: FxHashSet<N> = nodes.iter().copied().collect();

        // Compute in-degrees within set
        let mut indeg: FxHashMap<N, usize> = nodes.iter().map(|&n| (n, 0usize)).collect();
        for &n in &nodes {
            self.pred_buf.clear();
            graph.predecessors(n, &mut self.pred_buf);
            for &p in &self.pred_buf {
                if set.contains(&p) {
                    *indeg.get_mut(&n).unwrap() += 1;
                }
            }
        }

        let mut zero: Vec<N> = indeg
            .iter()
            .filter_map(|(&n, &d)| if d == 0 { Some(n) } else { None })
            .collect();
        // Sort descending so that pop() returns the smallest first
        zero.sort_by(|a, b| b.cmp(a));

        let mut out: Vec<N> = Vec::with_capacity(nodes.len());
        while let Some(n) = zero.pop() {
            out.push(n);
            self.succ_buf.clear();
            graph.successors(n, &mut self.succ_buf);
            // limited to subset
            for &s in &self.succ_buf {
                if let Some(d) = indeg.get_mut(&s) {
                    *d -= 1;
                    if *d == 0 {
                        zero.push(s);
                    }
                }
            }
            zero.sort_by(|a, b| b.cmp(a)); // maintain pop as smallest
        }
        // If out length < nodes, there is a cycle in source graph; keep relative sorted order
        if out.len() != nodes.len() {
            out = nodes; // fallback deterministic
        }
        self.order = out;
        self.pos.clear();
        for (i, n) in self.order.iter().enumerate() {
            self.pos.insert(*n, i as u32);
        }
    }

    pub fn try_add_edge<G: GraphView<N>>(
        &mut self,
        graph: &G,
        x: N,
        y: N,
    ) -> Result<PkStats, Cycle<N>> {
        if x == y {
            return Err(Cycle { path: vec![x, y] });
        }
        let px = match self.pos.get(&x).copied() {
            Some(v) => v,
            None => self.add_missing(x),
        };
        let py = match self.pos.get(&y).copied() {
            Some(v) => v,
            None => self.add_missing(y),
        };
        if px < py {
            return Ok(PkStats::default());
        }

        // Limited DFS from y through successors with pos <= px
        let mut stack: Vec<N> = vec![y];
        let mut parent: FxHashMap<N, N> = FxHashMap::default();
        let mut visited: FxHashSet<N> = FxHashSet::default();
        let mut affected: Vec<N> = Vec::new();
        let mut visited_cnt = 0usize;

        while let Some(u) = stack.pop() {
            if !visited.insert(u) {
                continue;
            }
            visited_cnt += 1;
            if visited_cnt > self.cfg.visit_budget {
                self.rebuild_full(graph);
                self.op_count += 1;
                // After rebuild, re-check quickly, recurse once
                return self.try_add_edge(graph, x, y);
            }
            if u == x {
                // build path from y -> ... -> x
                let mut path = vec![x];
                let mut cur = x;
                while cur != y {
                    cur = *parent.get(&cur).unwrap();
                    path.push(cur);
                }
                path.reverse();
                return Err(Cycle { path });
            }
            affected.push(u);
            self.succ_buf.clear();
            graph.successors(u, &mut self.succ_buf);
            for &s in &self.succ_buf {
                if let Some(&ps) = self.pos.get(&s)
                    && ps <= px
                    && !visited.contains(&s)
                {
                    parent.insert(s, u);
                    stack.push(s);
                }
            }
        }

        // splice affected block to just after px in global order, maintaining relative order
        let relabeled = self.splice_after(px as usize, &affected);

        self.op_count += 1;
        if self
            .op_count
            .is_multiple_of(self.cfg.compaction_interval_ops)
        {
            self.compact_ranks();
        }
        Ok(PkStats {
            relabeled,
            dfs_visited: visited_cnt,
        })
    }

    pub fn remove_edge(&mut self, _x: N, _y: N) {
        // PK does not require reorder on deletion.
        self.op_count += 1;
        if self
            .op_count
            .is_multiple_of(self.cfg.compaction_interval_ops)
        {
            self.compact_ranks();
        }
    }

    pub fn apply_bulk<G: GraphView<N>>(
        &mut self,
        graph: &G,
        removes: &[(N, N)],
        adds: &[(N, N)],
    ) -> Result<PkStats, Cycle<N>> {
        for &(x, y) in removes {
            let _ = (x, y);
            self.remove_edge(x, y);
        }
        let mut stats = PkStats::default();
        for &(x, y) in adds {
            match self.try_add_edge(graph, x, y) {
                Ok(s) => {
                    stats.relabeled += s.relabeled;
                    stats.dfs_visited += s.dfs_visited;
                }
                Err(c) => {
                    return Err(c);
                }
            }
        }
        Ok(stats)
    }

    #[inline]
    pub fn topo_order(&self) -> &[N] {
        &self.order
    }

    pub fn compact_ranks(&mut self) {
        // Re-impose deterministic order: stable sort by (current pos, then N)
        self.order
            .sort_by(|a, b| match self.pos[a].cmp(&self.pos[b]) {
                Ordering::Equal => a.cmp(b),
                o => o,
            });
        self.pos.clear();
        for (i, n) in self.order.iter().enumerate() {
            self.pos.insert(*n, i as u32);
        }
    }

    /// Build parallel-ready layers for a subset, using maintained order for tie-breaks.
    pub fn layers_for<G: GraphView<N>>(
        &self,
        graph: &G,
        subset: &[N],
        max_layer_width: Option<usize>,
    ) -> Vec<Vec<N>> {
        if subset.is_empty() {
            return Vec::new();
        }
        let subset_set: FxHashSet<N> = subset.iter().copied().collect();
        let mut indeg: FxHashMap<N, usize> = subset.iter().map(|&n| (n, 0usize)).collect();
        let mut pred_buf = Vec::new();
        for &n in subset {
            pred_buf.clear();
            // SAFETY: We don't have &mut self graph; create a temp adapter using trait buffers
            // but GraphView requires &self; we only need predecessors, pass scratch buffer.
            // Here we can't call self.graph.predecessors directly because we don't have &mut; it's &self fine.
            // But GraphView signature already takes &self and &mut Vec.
            graph.predecessors(n, &mut pred_buf);
            for &p in &pred_buf {
                if subset_set.contains(&p) {
                    *indeg.get_mut(&n).unwrap() += 1;
                }
            }
        }
        let mut zero: Vec<N> = indeg
            .iter()
            .filter_map(|(&n, &d)| if d == 0 { Some(n) } else { None })
            .collect();
        // Deterministic: by current position, then N
        zero.sort_by(|a, b| self.pos[a].cmp(&self.pos[b]).then_with(|| a.cmp(b)));

        let mut layers: Vec<Vec<N>> = Vec::new();
        let mut succ_buf = Vec::new();
        while !zero.is_empty() {
            let mut layer = Vec::new();
            let cap = max_layer_width.unwrap_or(usize::MAX);
            for _ in 0..zero.len().min(cap) {
                layer.push(zero.remove(0));
            }
            // within layer, sort deterministically
            layer.sort_by(|a, b| self.pos[a].cmp(&self.pos[b]).then_with(|| a.cmp(b)));

            for &u in &layer {
                succ_buf.clear();
                graph.successors(u, &mut succ_buf);
                for &v in &succ_buf {
                    if let Some(d) = indeg.get_mut(&v) {
                        *d -= 1;
                        if *d == 0 {
                            zero.push(v);
                        }
                    }
                }
            }
            zero.sort_by(|a, b| self.pos[a].cmp(&self.pos[b]).then_with(|| a.cmp(b)));
            layers.push(layer);
        }
        // Any remaining indeg>0 would be a logic bug (cycles should be caught earlier).
        layers
    }

    #[inline]
    fn add_missing(&mut self, n: N) -> u32 {
        let idx = self.order.len() as u32;
        self.order.push(n);
        self.pos.insert(n, idx);
        idx
    }

    /// Ensure that all provided nodes exist in the ordering; append any missing at the end.
    pub fn ensure_nodes(&mut self, nodes: impl IntoIterator<Item = N>) {
        for n in nodes {
            if !self.pos.contains_key(&n) {
                self.add_missing(n);
            }
        }
    }

    /// Move all nodes in `affected` to just after index `after_pos`, preserving their internal order.
    /// Returns number of nodes relabeled.
    fn splice_after(&mut self, after_pos: usize, affected: &[N]) -> usize {
        if affected.is_empty() {
            return 0;
        }
        let mark: FxHashSet<N> = affected.iter().copied().collect();
        // Extract unaffected and affected in original order
        let mut left: Vec<N> = Vec::with_capacity(self.order.len() - affected.len());
        let mut block: Vec<N> = Vec::with_capacity(affected.len());
        for &n in &self.order {
            if mark.contains(&n) {
                block.push(n);
            } else {
                left.push(n);
            }
        }
        // Build new order by placing block after after_pos in the left vector
        let mut new_order: Vec<N> = Vec::with_capacity(self.order.len());
        let mut i = 0usize;
        while i < left.len() {
            new_order.push(left[i]);
            i += 1;
            if i - 1 == after_pos {
                new_order.extend(block.iter().copied());
            }
        }
        if after_pos >= left.len() {
            new_order.extend(block.iter().copied());
        }
        self.order = new_order;
        // Recompute positions for moved nodes only; but for simplicity recompute all
        for (i, &n) in self.order.iter().enumerate() {
            self.pos.insert(n, i as u32);
        }
        affected.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustc_hash::FxHashMap;

    #[derive(Default)]
    struct SimpleGraph {
        succ: FxHashMap<u32, Vec<u32>>, // x -> [y]
        pred: FxHashMap<u32, Vec<u32>>, // y -> [x]
    }

    impl SimpleGraph {
        fn add_edge(&mut self, x: u32, y: u32) {
            self.succ.entry(x).or_default().push(y);
            self.pred.entry(y).or_default().push(x);
        }

        fn remove_edge(&mut self, x: u32, y: u32) {
            if let Some(v) = self.succ.get_mut(&x) {
                v.retain(|&t| t != y);
            }
            if let Some(v) = self.pred.get_mut(&y) {
                v.retain(|&s| s != x);
            }
        }
    }

    impl GraphView<u32> for SimpleGraph {
        fn successors(&self, n: u32, out: &mut Vec<u32>) {
            out.clear();
            if let Some(v) = self.succ.get(&n) {
                out.extend(v.iter().copied());
            }
        }
        fn predecessors(&self, n: u32, out: &mut Vec<u32>) {
            out.clear();
            if let Some(v) = self.pred.get(&n) {
                out.extend(v.iter().copied());
            }
        }
        fn exists(&self, _n: u32) -> bool {
            true
        }
    }

    fn idx(order: &[u32], n: u32) -> usize {
        order.iter().position(|&x| x == n).unwrap()
    }

    #[test]
    fn rebuild_full_basic_chain() {
        let mut g = SimpleGraph::default();
        g.add_edge(1, 2);
        g.add_edge(2, 3);
        let nodes = [1, 2, 3, 4];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        let layers = pk.layers_for(&g, &nodes, None);
        // Expect chain 1->2->3 and 4 independent; first layer contains 1 and 4
        assert!(!layers.is_empty());
        assert!(layers[0].contains(&1));
        // 2 must come after 1, 3 after 2 in order
        let order = pk.topo_order().to_vec();
        assert!(idx(&order, 1) < idx(&order, 2));
        assert!(idx(&order, 2) < idx(&order, 3));
    }

    #[test]
    fn add_edge_forward_no_relabel() {
        let g = SimpleGraph::default();
        let nodes = [1, 2, 3, 4];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        let stats = pk.try_add_edge(&g, 1, 3).unwrap();
        assert_eq!(stats.relabeled, 0);
        // Order remains a valid topo order: 1 before 3
        let order = pk.topo_order();
        assert!(idx(order, 1) < idx(order, 3));
    }

    #[test]
    fn add_edge_backedge_splices_without_cycle() {
        // No existing path from 2 to 3, so adding 3->2 should reorder placing 2 after 3
        let g = SimpleGraph::default();
        let nodes = [1, 2, 3, 4];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        let _ = pk.try_add_edge(&g, 3, 2).unwrap();
        let order = pk.topo_order();
        assert!(idx(order, 3) < idx(order, 2));
    }

    #[test]
    fn detect_cycle_on_add() {
        let mut g = SimpleGraph::default();
        g.add_edge(2, 3);
        g.add_edge(3, 4);
        let nodes = [1, 2, 3, 4];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        // Adding 4->2 creates a cycle 2->3->4->2
        let res = pk.try_add_edge(&g, 4, 2);
        assert!(res.is_err());
    }

    #[test]
    fn layers_with_width_cap() {
        let mut g = SimpleGraph::default();
        // 1->3, 2->3, 4 independent
        g.add_edge(1, 3);
        g.add_edge(2, 3);
        let nodes = [1, 2, 3, 4];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        let layers = pk.layers_for(&g, &nodes, Some(1));
        // Each layer must have at most 1 due to cap
        assert!(layers.iter().all(|layer| layer.len() <= 1));
        // Union of all layers equals the subset
        let mut all: Vec<u32> = layers.into_iter().flatten().collect();
        all.sort();
        assert_eq!(all, vec![1, 2, 3, 4]);
    }

    #[test]
    fn layers_unbounded_expected_first_layer() {
        let mut g = SimpleGraph::default();
        g.add_edge(1, 3);
        g.add_edge(2, 3); // zeros: 1,2,4
        let nodes = [1, 2, 3, 4];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        let layers = pk.layers_for(&g, &nodes, None);
        assert!(!layers.is_empty());
        // First layer should contain exactly {1,2,4} in some deterministic order
        let mut got = layers[0].clone();
        got.sort();
        assert_eq!(got, vec![1, 2, 4]);
        assert_eq!(layers[1], vec![3]);
    }

    #[test]
    fn apply_bulk_adds_then_layers() {
        let mut g = SimpleGraph::default();
        let nodes = [1, 2, 3];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        g.add_edge(1, 2);
        g.add_edge(2, 3);
        let _stats = pk.apply_bulk(&g, &[], &[(1, 2), (2, 3)]).unwrap();
        let layers = pk.layers_for(&g, &nodes, None);
        assert_eq!(layers.len(), 3);
    }

    #[test]
    fn budget_config_is_respected_in_api_surface() {
        let g = SimpleGraph::default();
        let mut pk = DynamicTopo::new(
            [1u32, 2, 3, 4, 5],
            PkConfig {
                visit_budget: 1,
                compaction_interval_ops: 10,
            },
        );
        pk.rebuild_full(&g);
        assert_eq!(pk.cfg.visit_budget, 1);
    }

    #[test]
    fn ensure_nodes_appends_missing() {
        let g = SimpleGraph::default();
        let mut pk = DynamicTopo::new([1u32], PkConfig::default());
        pk.ensure_nodes([2u32, 3u32]);
        let order = pk.topo_order();
        assert_eq!(order.len(), 3);
        assert!(idx(order, 1) < idx(order, 2));
        assert!(idx(order, 2) < idx(order, 3));
    }

    #[test]
    fn compact_ranks_keeps_nodes() {
        let g = SimpleGraph::default();
        let nodes = [3, 1, 4, 2];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        pk.compact_ranks();
        let mut order = pk.topo_order().to_vec();
        order.sort();
        assert_eq!(order, vec![1, 2, 3, 4]);
    }

    #[test]
    fn remove_edge_does_not_change_order() {
        let mut g = SimpleGraph::default();
        g.add_edge(1, 3);
        let nodes = [1, 2, 3];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        let before = pk.topo_order().to_vec();
        pk.remove_edge(2, 1); // unrelated removal
        let after = pk.topo_order().to_vec();
        assert_eq!(before, after);
    }

    #[test]
    fn apply_bulk_mixed_removes_and_adds() {
        // Start with a chain 1->2->3; then remove (1,2) and add (1,3)
        let mut g = SimpleGraph::default();
        g.add_edge(1, 2);
        g.add_edge(2, 3);
        let nodes = [1, 2, 3, 4];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);

        // Prime PK with current edges
        let _ = pk.apply_bulk(&g, &[], &[(1, 2), (2, 3)]).unwrap();

        // Mutate graph: remove (1,2), add (1,3)
        g.remove_edge(1, 2);
        g.add_edge(1, 3);
        let stats = pk.apply_bulk(&g, &[(1, 2)], &[(1, 3)]).unwrap();
        // After changes, at minimum 1 must precede 3; 2 can be anywhere relative to 1
        let order = pk.topo_order().to_vec();
        assert!(idx(&order, 1) < idx(&order, 3));
        // stats existence was verified by unwrap(); nothing else to assert here.
    }

    #[test]
    fn layers_for_subset_only() {
        // Full graph: 1->2, 2->3, 4->5; subset: [2,3,5]
        let mut g = SimpleGraph::default();
        g.add_edge(1, 2);
        g.add_edge(2, 3);
        g.add_edge(4, 5);
        let nodes = [1, 2, 3, 4, 5];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        // Build layers only for subset; expect 2 before 3; 5 has indegree 1 from 4 (outside subset) so indegree 0 in subset
        let subset = vec![2, 3, 5];
        let layers = pk.layers_for(&g, &subset, None);
        // Flatten and compare membership equals subset
        let mut flat: Vec<u32> = layers.iter().flatten().copied().collect();
        flat.sort();
        assert_eq!(flat, vec![2, 3, 5]);
        // 2 should be in a layer before 3
        let pos2 = layers.iter().position(|lay| lay.contains(&2)).unwrap();
        let pos3 = layers.iter().position(|lay| lay.contains(&3)).unwrap();
        assert!(pos2 < pos3);
        // 5 should be in the first layer (no in-subset predecessors)
        assert!(layers[0].contains(&5));
    }

    #[test]
    fn compact_ranks_repeated_stability() {
        // After establishing some ordering pressure, repeated compactions shouldn't change order
        let mut g = SimpleGraph::default();
        g.add_edge(1, 3);
        g.add_edge(2, 3);
        let nodes = [1, 2, 3, 4];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        let _ = pk.try_add_edge(&g, 4, 2); // introduce a back-edge reorder (4 before 2)
        let baseline = pk.topo_order().to_vec();
        for _ in 0..10 {
            pk.compact_ranks();
            assert_eq!(baseline, pk.topo_order());
        }
    }

    #[test]
    fn compact_ranks_is_stable_repeated() {
        let g = SimpleGraph::default();
        let nodes = [1, 2, 3, 4];
        let mut pk = DynamicTopo::new(nodes, PkConfig::default());
        pk.rebuild_full(&g);
        // Create a back-edge to reorder: 3->2
        let _ = pk.try_add_edge(&g, 3, 2).unwrap();
        let before = pk.topo_order().to_vec();
        for _ in 0..5 {
            pk.compact_ranks();
            assert_eq!(pk.topo_order(), &before);
        }
    }
}
