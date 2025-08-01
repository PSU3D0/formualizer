use super::csr_edges::CsrEdges;
use super::packed_coord::PackedCoord;
use super::vertex::VertexId;
use rustc_hash::{FxHashMap, FxHashSet};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::packed_coord::PackedCoord;

    #[test]
    fn test_delta_slab_add_edge() {
        let csr = CsrEdges::from_adjacency(
            vec![(0u32, vec![1u32])],
            &[
                PackedCoord::new(0, 0),
                PackedCoord::new(0, 1),
                PackedCoord::new(0, 2),
            ],
        );
        let mut delta = DeltaEdgeSlab::new();

        delta.add_edge(VertexId(0), VertexId(2));

        let merged = delta.merged_view(&csr, VertexId(0));
        assert_eq!(merged, vec![VertexId(1), VertexId(2)]);
    }

    #[test]
    fn test_delta_slab_remove_edge() {
        let csr = CsrEdges::from_adjacency(
            vec![(0u32, vec![1u32, 2u32, 3u32])],
            &[
                PackedCoord::new(0, 0),
                PackedCoord::new(0, 1),
                PackedCoord::new(0, 2),
                PackedCoord::new(0, 3),
            ],
        );
        let mut delta = DeltaEdgeSlab::new();

        delta.remove_edge(VertexId(0), VertexId(2));

        let merged = delta.merged_view(&csr, VertexId(0));
        assert_eq!(merged, vec![VertexId(1), VertexId(3)]);
    }

    #[test]
    fn test_delta_slab_rebuild_threshold() {
        let mut edges = CsrMutableEdges::new();

        // Add 1000 edges through delta slab
        for i in 0..1000 {
            edges.add_edge(VertexId(i), VertexId(i + 1));
        }

        // Should trigger rebuild at threshold
        assert!(edges.delta_size() < 100); // Delta cleared after rebuild
    }

    #[test]
    fn test_delta_slab_multiple_operations() {
        let csr = CsrEdges::from_adjacency(
            vec![(0u32, vec![1u32, 2u32]), (1u32, vec![3u32])],
            &[
                PackedCoord::new(0, 0),
                PackedCoord::new(0, 1),
                PackedCoord::new(0, 2),
                PackedCoord::new(1, 0),
            ],
        );
        let mut delta = DeltaEdgeSlab::new();

        // Multiple operations on same vertex
        delta.add_edge(VertexId(0), VertexId(3));
        delta.remove_edge(VertexId(0), VertexId(1));
        delta.add_edge(VertexId(0), VertexId(4));

        let merged = delta.merged_view(&csr, VertexId(0));
        assert_eq!(merged, vec![VertexId(2), VertexId(3), VertexId(4)]);
    }

    #[test]
    fn test_delta_slab_empty_base() {
        let csr = CsrEdges::empty();
        let mut delta = DeltaEdgeSlab::new();

        delta.add_edge(VertexId(0), VertexId(1));
        delta.add_edge(VertexId(0), VertexId(2));

        let merged = delta.merged_view(&csr, VertexId(0));
        assert_eq!(merged, vec![VertexId(1), VertexId(2)]);
    }

    #[test]
    fn test_delta_slab_remove_nonexistent() {
        let csr = CsrEdges::from_adjacency(
            vec![(0u32, vec![1u32])],
            &[PackedCoord::new(0, 0), PackedCoord::new(0, 1)],
        );
        let mut delta = DeltaEdgeSlab::new();

        // Remove edge that doesn't exist
        delta.remove_edge(VertexId(0), VertexId(2));

        let merged = delta.merged_view(&csr, VertexId(0));
        assert_eq!(merged, vec![VertexId(1)]); // No change
    }

    #[test]
    fn test_delta_slab_apply_to_csr() {
        let csr = CsrEdges::from_adjacency(
            vec![(0u32, vec![1u32]), (1u32, vec![2u32]), (2u32, vec![])],
            &[
                PackedCoord::new(0, 0),
                PackedCoord::new(0, 1),
                PackedCoord::new(1, 0),
            ],
        );

        let mut delta = DeltaEdgeSlab::new();
        delta.add_edge(VertexId(0), VertexId(2));
        delta.remove_edge(VertexId(1), VertexId(2));
        delta.add_edge(VertexId(2), VertexId(0));

        // Apply delta and get new CSR
        let coords = vec![
            PackedCoord::new(0, 0),
            PackedCoord::new(0, 1),
            PackedCoord::new(1, 0),
        ];
        let vertex_ids = vec![0u32, 1u32, 2u32];
        let new_csr = delta.apply_to_csr(&csr, &coords, &vertex_ids);

        assert_eq!(new_csr.out_edges(VertexId(0)), &[VertexId(1), VertexId(2)]);
        assert_eq!(new_csr.out_edges(VertexId(1)), &[]);
        assert_eq!(new_csr.out_edges(VertexId(2)), &[VertexId(0)]);
    }

    #[test]
    fn test_mutable_edges_auto_rebuild() {
        let mut edges = CsrMutableEdges::with_coords(vec![
            PackedCoord::new(0, 0),
            PackedCoord::new(0, 1),
            PackedCoord::new(1, 0),
        ]);

        // Add initial edges
        edges.add_edge(VertexId(0), VertexId(1));
        edges.add_edge(VertexId(1), VertexId(2));

        // Perform many operations to trigger rebuild
        for _ in 0..500 {
            edges.add_edge(VertexId(2), VertexId(0));
            edges.remove_edge(VertexId(2), VertexId(0));
        }

        // Check that rebuild happened (delta is small)
        assert!(edges.delta_size() < 50);

        // Verify edges are still correct
        assert_eq!(edges.out_edges(VertexId(0)), vec![VertexId(1)]);
        assert_eq!(edges.out_edges(VertexId(1)), vec![VertexId(2)]);
    }

    #[test]
    fn test_mutable_edges_with_offset_vertex_ids() {
        use crate::engine::vertex_store::FIRST_NORMAL_VERTEX;

        let mut edges = CsrMutableEdges::new();

        // Add vertices with IDs starting at FIRST_NORMAL_VERTEX (1024)
        let base_id = FIRST_NORMAL_VERTEX;
        edges.add_vertex(PackedCoord::new(0, 0), base_id);
        edges.add_vertex(PackedCoord::new(0, 1), base_id + 1);
        edges.add_vertex(PackedCoord::new(1, 0), base_id + 2);

        // Add edges using offset IDs
        edges.add_edge(VertexId(base_id), VertexId(base_id + 1));
        edges.add_edge(VertexId(base_id + 1), VertexId(base_id + 2));
        edges.add_edge(VertexId(base_id + 2), VertexId(base_id));

        // Verify edges work correctly
        assert_eq!(
            edges.out_edges(VertexId(base_id)),
            vec![VertexId(base_id + 1)]
        );
        assert_eq!(
            edges.out_edges(VertexId(base_id + 1)),
            vec![VertexId(base_id + 2)]
        );
        assert_eq!(
            edges.out_edges(VertexId(base_id + 2)),
            vec![VertexId(base_id)]
        );

        // Force rebuild and verify again
        edges.rebuild();
        assert_eq!(
            edges.out_edges(VertexId(base_id)),
            vec![VertexId(base_id + 1)]
        );
        assert_eq!(
            edges.out_edges(VertexId(base_id + 1)),
            vec![VertexId(base_id + 2)]
        );
        assert_eq!(
            edges.out_edges(VertexId(base_id + 2)),
            vec![VertexId(base_id)]
        );
    }
}

/// Delta slab for accumulating edge mutations between CSR rebuilds
///
/// Provides O(1) edge mutations by tracking additions and removals
/// separately, merging them with the base CSR on read.
#[derive(Debug)]
pub struct DeltaEdgeSlab {
    /// New edges to add, grouped by source vertex
    additions: FxHashMap<VertexId, Vec<VertexId>>,

    /// Edges to remove, stored as sets for O(1) lookup
    removals: FxHashMap<VertexId, FxHashSet<VertexId>>,

    /// Total operation count for rebuild threshold
    op_count: usize,
}

impl DeltaEdgeSlab {
    /// Create a new empty delta slab
    pub fn new() -> Self {
        Self {
            additions: FxHashMap::default(),
            removals: FxHashMap::default(),
            op_count: 0,
        }
    }

    /// Add an edge from source to target
    pub fn add_edge(&mut self, from: VertexId, to: VertexId) {
        self.additions.entry(from).or_default().push(to);
        self.op_count += 1;
    }

    /// Remove an edge from source to target
    pub fn remove_edge(&mut self, from: VertexId, to: VertexId) {
        self.removals.entry(from).or_default().insert(to);
        self.op_count += 1;
    }

    /// Get a merged view of edges for a vertex, combining CSR and delta
    pub fn merged_view(&self, csr: &CsrEdges, v: VertexId) -> Vec<VertexId> {
        // CSR stores the edges, it handles vertex ID mapping internally
        let mut result: Vec<_> = csr.out_edges(v).to_vec();

        // Remove edges marked for deletion
        if let Some(removes) = self.removals.get(&v) {
            result.retain(|e| !removes.contains(e));
        }

        // Add new edges
        if let Some(adds) = self.additions.get(&v) {
            result.extend_from_slice(adds);
        }

        result
    }

    /// Check if the delta needs to be applied (rebuild threshold reached)
    pub fn needs_rebuild(&self) -> bool {
        self.op_count >= 1000
    }

    /// Get the current operation count
    pub fn op_count(&self) -> usize {
        self.op_count
    }

    /// Clear the delta slab
    pub fn clear(&mut self) {
        self.additions.clear();
        self.removals.clear();
        self.op_count = 0;
    }

    /// Apply delta to CSR, creating a new CSR structure
    pub fn apply_to_csr(
        &self,
        base: &CsrEdges,
        coords: &[PackedCoord],
        vertex_ids: &[u32],
    ) -> CsrEdges {
        let mut adjacency = Vec::with_capacity(vertex_ids.len());

        // Build new adjacency list by merging base and delta
        for &vid in vertex_ids {
            let v = VertexId(vid);
            let merged = self.merged_view(base, v);

            // Convert to u32 for adjacency format
            let targets: Vec<u32> = merged.into_iter().map(|id| id.0).collect();

            adjacency.push((vid, targets));
        }

        CsrEdges::from_adjacency(adjacency, coords)
    }
}

impl Default for DeltaEdgeSlab {
    fn default() -> Self {
        Self::new()
    }
}

/// Mutable edge storage combining CSR base with delta slab
///
/// Provides efficient edge mutations with automatic rebuild when
/// delta grows too large.
#[derive(Debug)]
pub struct CsrMutableEdges {
    /// Base CSR structure (immutable between rebuilds)
    base: CsrEdges,

    /// Delta slab for mutations
    delta: DeltaEdgeSlab,

    /// Vertex coordinates for deterministic ordering
    coords: Vec<PackedCoord>,

    /// Vertex IDs corresponding to coords array
    vertex_ids: Vec<u32>,

    /// Batch mode flag - when true, skip automatic rebuilds
    batch_mode: bool,
}

impl CsrMutableEdges {
    /// Create new mutable edges with empty base
    pub fn new() -> Self {
        Self {
            base: CsrEdges::empty(),
            delta: DeltaEdgeSlab::new(),
            coords: Vec::new(),
            vertex_ids: Vec::new(),
            batch_mode: false,
        }
    }

    /// Create with initial vertex coordinates
    pub fn with_coords(coords: Vec<PackedCoord>) -> Self {
        let num_vertices = coords.len();
        let vertex_ids: Vec<u32> = (0..num_vertices as u32).collect();
        let adjacency: Vec<_> = vertex_ids.iter().map(|&id| (id, Vec::new())).collect();

        Self {
            base: CsrEdges::from_adjacency(adjacency, &coords),
            delta: DeltaEdgeSlab::new(),
            coords,
            vertex_ids,
            batch_mode: false,
        }
    }

    /// Add an edge, rebuilding if threshold reached
    pub fn add_edge(&mut self, from: VertexId, to: VertexId) {
        self.delta.add_edge(from, to);
        self.maybe_rebuild();
    }

    /// Remove an edge, rebuilding if threshold reached
    pub fn remove_edge(&mut self, from: VertexId, to: VertexId) {
        self.delta.remove_edge(from, to);
        self.maybe_rebuild();
    }

    /// Get outgoing edges for a vertex (merged view)
    pub fn out_edges(&self, v: VertexId) -> Vec<VertexId> {
        self.delta.merged_view(&self.base, v)
    }

    /// Get incoming edges from base CSR (delta not applied for performance)
    /// After rebuild, this will include all changes
    pub fn in_edges(&self, v: VertexId) -> &[VertexId] {
        self.base.in_edges(v)
    }

    /// Get the current delta size
    pub fn delta_size(&self) -> usize {
        self.delta.op_count()
    }

    /// Force a rebuild of the CSR structure
    pub fn rebuild(&mut self) {
        if self.delta.op_count() > 0 {
            self.base = self
                .delta
                .apply_to_csr(&self.base, &self.coords, &self.vertex_ids);
            self.delta.clear();
        }
    }

    /// Check and perform rebuild if threshold reached
    fn maybe_rebuild(&mut self) {
        if !self.batch_mode && self.delta.needs_rebuild() {
            self.rebuild();
        }
    }

    /// Enter batch mode - defer rebuilds until end_batch() is called
    pub fn begin_batch(&mut self) {
        self.batch_mode = true;
    }

    /// Exit batch mode and rebuild if needed
    pub fn end_batch(&mut self) {
        self.batch_mode = false;
        if self.delta.op_count() > 0 {
            self.rebuild();
        }
    }

    /// Add a new vertex with its coordinate and ID
    pub fn add_vertex(&mut self, coord: PackedCoord, vertex_id: u32) -> usize {
        let idx = self.coords.len();
        self.coords.push(coord);
        self.vertex_ids.push(vertex_id);

        // Rebuild base to include new vertex
        // This is necessary to maintain CSR structure consistency
        self.rebuild();

        idx
    }
}

impl Default for CsrMutableEdges {
    fn default() -> Self {
        Self::new()
    }
}
