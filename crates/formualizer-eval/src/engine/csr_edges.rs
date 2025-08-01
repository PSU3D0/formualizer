use super::packed_coord::PackedCoord;
use super::vertex::VertexId;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csr_construction() {
        let edges = vec![(0, vec![1, 2]), (1, vec![2, 3]), (2, vec![3]), (3, vec![])];

        let coords = vec![
            PackedCoord::new(0, 0),
            PackedCoord::new(0, 1),
            PackedCoord::new(1, 0),
            PackedCoord::new(1, 1),
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert_eq!(csr.out_edges(VertexId(0)), &[VertexId(1), VertexId(2)]);
        assert_eq!(csr.out_edges(VertexId(1)), &[VertexId(2), VertexId(3)]);
        assert_eq!(csr.out_edges(VertexId(3)), &[]);
    }

    #[test]
    fn test_csr_memory_efficiency() {
        // 10K vertices, average 4 edges each
        let mut edges = Vec::new();
        let mut coords = Vec::new();

        for i in 0..10_000 {
            let targets: Vec<_> = (0..4).map(|j| (i + j + 1) % 10_000).collect();
            edges.push((i, targets));
            coords.push(PackedCoord::new(i as u32, i as u32));
        }

        let csr = CsrEdges::from_adjacency(edges, &coords);

        // Should use ~200KB (40k edges × 4B + 10k vertices × 4B)
        assert!(csr.memory_usage() < 210_000);
    }

    #[test]
    fn test_csr_edge_ordering() {
        // Test that edges are sorted by (row, col, id) for determinism
        let edges = vec![
            (0, vec![3, 1, 2]), // Unsorted input
        ];

        let coords = vec![
            PackedCoord::new(0, 0), // vertex 0
            PackedCoord::new(0, 5), // vertex 1
            PackedCoord::new(0, 3), // vertex 2
            PackedCoord::new(1, 0), // vertex 3
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        // Should be sorted by row first, then col: [1(0,5), 2(0,3), 3(1,0)]
        // But row 0 comes before row 1, so order is: 2(0,3), 1(0,5), 3(1,0)
        assert_eq!(
            csr.out_edges(VertexId(0)),
            &[VertexId(2), VertexId(1), VertexId(3)]
        );
    }

    #[test]
    fn test_csr_empty_graph() {
        let edges: Vec<(usize, Vec<usize>)> = vec![];
        let coords: Vec<PackedCoord> = vec![];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert_eq!(csr.num_vertices(), 0);
        assert_eq!(csr.num_edges(), 0);
        // Empty graph has one offset entry (0) = 4 bytes
        assert_eq!(csr.memory_usage(), 4);
    }

    #[test]
    fn test_csr_single_vertex() {
        let edges = vec![(0, vec![])];
        let coords = vec![PackedCoord::new(0, 0)];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert_eq!(csr.num_vertices(), 1);
        assert_eq!(csr.num_edges(), 0);
        assert_eq!(csr.out_edges(VertexId(0)), &[]);
    }

    #[test]
    fn test_csr_self_loop() {
        let edges = vec![(0, vec![0])]; // Self loop
        let coords = vec![PackedCoord::new(0, 0)];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert_eq!(csr.out_edges(VertexId(0)), &[VertexId(0)]);
        assert_eq!(csr.num_edges(), 1);
    }

    #[test]
    fn test_csr_duplicate_edges() {
        // CSR should preserve duplicates (formulas can reference same cell multiple times)
        let edges = vec![(0, vec![1, 1, 2, 1])];
        let coords = vec![
            PackedCoord::new(0, 0),
            PackedCoord::new(0, 1),
            PackedCoord::new(0, 2),
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        // Should preserve all edges, sorted by target coords
        assert_eq!(
            csr.out_edges(VertexId(0)),
            &[VertexId(1), VertexId(1), VertexId(1), VertexId(2)]
        );
    }

    #[test]
    fn test_degree_calculation() {
        let edges = vec![
            (0, vec![1, 2, 3]),
            (1, vec![2]),
            (2, vec![]),
            (3, vec![0, 1]),
        ];

        let coords = vec![
            PackedCoord::new(0, 0),
            PackedCoord::new(0, 1),
            PackedCoord::new(1, 0),
            PackedCoord::new(1, 1),
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert_eq!(csr.out_degree(VertexId(0)), 3);
        assert_eq!(csr.out_degree(VertexId(1)), 1);
        assert_eq!(csr.out_degree(VertexId(2)), 0);
        assert_eq!(csr.out_degree(VertexId(3)), 2);
    }

    #[test]
    #[should_panic]
    fn test_out_of_bounds_access() {
        let edges = vec![(0, vec![])];
        let coords = vec![PackedCoord::new(0, 0)];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        // Should panic - only vertex 0 exists
        csr.out_edges(VertexId(1));
    }

    #[test]
    fn test_csr_iterator() {
        let edges = vec![(0, vec![1, 2]), (1, vec![3]), (2, vec![1, 3]), (3, vec![])];

        let coords = vec![
            PackedCoord::new(0, 0),
            PackedCoord::new(0, 1),
            PackedCoord::new(1, 0),
            PackedCoord::new(1, 1),
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        let collected: Vec<_> = csr.iter().collect();
        assert_eq!(collected.len(), 4);
        assert_eq!(collected[0].0, VertexId(0));
        assert_eq!(collected[0].1, &[VertexId(1), VertexId(2)]);
        assert_eq!(collected[3].1, &[]);
    }

    #[test]
    fn test_has_edge() {
        let edges = vec![
            (0, vec![1, 2]),
            (1, vec![3]),
            (2, vec![]),
            (3, vec![0]), // Back edge
        ];

        let coords = vec![
            PackedCoord::new(0, 0),
            PackedCoord::new(0, 1),
            PackedCoord::new(1, 0),
            PackedCoord::new(1, 1),
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert!(csr.has_edge(VertexId(0), VertexId(1)));
        assert!(csr.has_edge(VertexId(0), VertexId(2)));
        assert!(!csr.has_edge(VertexId(0), VertexId(3)));
        assert!(csr.has_edge(VertexId(3), VertexId(0))); // Back edge exists
        assert!(!csr.has_edge(VertexId(2), VertexId(0))); // No edge
    }

    #[test]
    fn test_csr_with_vertex_store_integration() {
        use crate::engine::vertex_store::{FIRST_NORMAL_VERTEX, VertexStore};

        // Create vertices in store
        let mut store = VertexStore::new();
        let v0 = store.allocate(PackedCoord::new(0, 0), 0, 0);
        let v1 = store.allocate(PackedCoord::new(0, 1), 0, 0);
        let v2 = store.allocate(PackedCoord::new(1, 0), 0, 0);
        let v3 = store.allocate(PackedCoord::new(1, 1), 0, 0);

        // Build CSR edges
        let mut builder = CsrBuilder::new();

        // Add vertices with their coordinates
        for i in 0..4 {
            let id = VertexId(i + FIRST_NORMAL_VERTEX);
            builder.add_vertex(store.coord(id));
        }

        // Add edges (using indices, not VertexIds)
        builder.add_edge(0, 1); // v0 -> v1
        builder.add_edge(0, 2); // v0 -> v2
        builder.add_edge(1, 3); // v1 -> v3
        builder.add_edge(2, 3); // v2 -> v3

        let csr = builder.build();

        // Verify edges
        assert_eq!(csr.out_edges(VertexId(0)), &[VertexId(1), VertexId(2)]);
        assert_eq!(csr.out_edges(VertexId(1)), &[VertexId(3)]);
        assert_eq!(csr.out_edges(VertexId(2)), &[VertexId(3)]);
        assert_eq!(csr.out_edges(VertexId(3)), &[]);

        // Update edge offsets in store
        store.set_edge_offset(v0, 0);
        store.set_edge_offset(v1, csr.out_degree(VertexId(0)) as u32);
        store.set_edge_offset(
            v2,
            (csr.out_degree(VertexId(0)) + csr.out_degree(VertexId(1))) as u32,
        );
        store.set_edge_offset(
            v3,
            (csr.out_degree(VertexId(0))
                + csr.out_degree(VertexId(1))
                + csr.out_degree(VertexId(2))) as u32,
        );
    }
}

/// Compressed Sparse Row (CSR) format for edge storage
///
/// Replaces Vec<VertexId> per vertex with two arrays:
/// - offsets: Start index for each vertex's edges
/// - edges: All edges concatenated
///
/// Memory usage: O(V + E) instead of O(V * avg_degree * vec_overhead)
#[derive(Debug, Clone)]
pub struct CsrEdges {
    /// Offsets into the edges array. Length = num_vertices + 1
    /// offset[i] = start index of vertex i's edges
    /// offset[i+1] - offset[i] = number of edges for vertex i
    offsets: Vec<u32>,

    /// All edges concatenated, sorted within each vertex's section
    edges: Vec<VertexId>,
}

impl CsrEdges {
    /// Create CSR from adjacency list representation
    ///
    /// # Arguments
    /// - adj: Vector of (vertex_index, outgoing_edges)
    /// - coords: Packed coordinates for each vertex (used for deterministic ordering)
    ///
    /// # Edge Ordering
    /// Edges are sorted by (row, col, vertex_id) to ensure deterministic
    /// evaluation order for formulas (important for functions with side effects)
    pub fn from_adjacency(adj: Vec<(usize, Vec<usize>)>, coords: &[PackedCoord]) -> Self {
        let num_vertices = adj.len();
        let mut offsets = Vec::with_capacity(num_vertices + 1);
        let mut edges = Vec::new();

        offsets.push(0);

        for (_vertex_idx, mut targets) in adj {
            // Sort targets by their coordinates for deterministic ordering
            targets.sort_by_key(|&t| {
                if t < coords.len() {
                    let coord = coords[t];
                    (coord.row(), coord.col(), t)
                } else {
                    // Handle out-of-bounds gracefully for construction
                    (u32::MAX, u32::MAX, t)
                }
            });

            edges.extend(targets.into_iter().map(|t| VertexId(t as u32)));
            offsets.push(edges.len() as u32);
        }

        Self { offsets, edges }
    }

    /// Get outgoing edges for a vertex
    #[inline]
    pub fn out_edges(&self, v: VertexId) -> &[VertexId] {
        let idx = v.0 as usize;
        assert!(idx < self.offsets.len() - 1, "Vertex {:?} out of bounds", v);

        let start = self.offsets[idx] as usize;
        let end = self.offsets[idx + 1] as usize;
        &self.edges[start..end]
    }

    /// Get the out-degree of a vertex
    #[inline]
    pub fn out_degree(&self, v: VertexId) -> usize {
        let idx = v.0 as usize;
        if idx >= self.offsets.len() - 1 {
            return 0;
        }

        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];
        (end - start) as usize
    }

    /// Number of vertices in the graph
    #[inline]
    pub fn num_vertices(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Total number of edges in the graph
    #[inline]
    pub fn num_edges(&self) -> usize {
        self.edges.len()
    }

    /// Memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.offsets.len() * std::mem::size_of::<u32>()
            + self.edges.len() * std::mem::size_of::<VertexId>()
    }

    /// Create an empty CSR graph
    pub fn empty() -> Self {
        Self {
            offsets: vec![0],
            edges: Vec::new(),
        }
    }

    /// Builder pattern for incremental construction
    pub fn builder() -> CsrBuilder {
        CsrBuilder::new()
    }

    /// Iterate over all vertices and their outgoing edges
    pub fn iter(&self) -> CsrIterator {
        CsrIterator {
            csr: self,
            current_vertex: 0,
        }
    }

    /// Check if the graph has a specific edge
    pub fn has_edge(&self, from: VertexId, to: VertexId) -> bool {
        self.out_edges(from).contains(&to)
    }
}

/// Iterator over vertices and their edges
pub struct CsrIterator<'a> {
    csr: &'a CsrEdges,
    current_vertex: usize,
}

impl<'a> Iterator for CsrIterator<'a> {
    type Item = (VertexId, &'a [VertexId]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_vertex >= self.csr.num_vertices() {
            return None;
        }

        let vertex_id = VertexId(self.current_vertex as u32);
        let edges = self.csr.out_edges(vertex_id);
        self.current_vertex += 1;

        Some((vertex_id, edges))
    }
}

/// Builder for incremental CSR construction
pub struct CsrBuilder {
    adjacency: Vec<Vec<usize>>,
    coords: Vec<PackedCoord>,
}

impl CsrBuilder {
    pub fn new() -> Self {
        Self {
            adjacency: Vec::new(),
            coords: Vec::new(),
        }
    }

    /// Add a vertex with its coordinate
    pub fn add_vertex(&mut self, coord: PackedCoord) -> usize {
        let idx = self.adjacency.len();
        self.adjacency.push(Vec::new());
        self.coords.push(coord);
        idx
    }

    /// Add an edge from source to target
    pub fn add_edge(&mut self, from: usize, to: usize) {
        if from < self.adjacency.len() {
            self.adjacency[from].push(to);
        }
    }

    /// Build the final CSR structure
    pub fn build(self) -> CsrEdges {
        let adj: Vec<_> = self.adjacency.into_iter().enumerate().collect();
        CsrEdges::from_adjacency(adj, &self.coords)
    }
}
