use super::vertex::VertexId;
use formualizer_common::Coord as AbsCoord;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csr_construction() {
        let edges = vec![
            (0u32, vec![1u32, 2u32]),
            (1u32, vec![2u32, 3u32]),
            (2u32, vec![3u32]),
            (3u32, vec![]),
        ];

        let coords = vec![
            AbsCoord::new(0, 0),
            AbsCoord::new(0, 1),
            AbsCoord::new(1, 0),
            AbsCoord::new(1, 1),
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

        for i in 0..10_000u32 {
            let targets: Vec<_> = (0..4).map(|j| (i + j + 1) % 10_000).collect();
            edges.push((i, targets));
            coords.push(AbsCoord::new(i, i));
        }

        let csr = CsrEdges::from_adjacency(edges, &coords);

        // Should use ~200KB (40k edges × 4B + 10k vertices × 4B)
        assert!(csr.memory_usage() < 410_000, "{}", csr.memory_usage());
    }

    #[test]
    fn test_csr_edge_ordering() {
        // Test that edges are sorted by (row, col, id) for determinism
        let edges = vec![
            (0u32, vec![3u32, 1u32, 2u32]), // Unsorted input
        ];

        let coords = vec![
            AbsCoord::new(0, 0), // vertex 0
            AbsCoord::new(0, 5), // vertex 1
            AbsCoord::new(0, 3), // vertex 2
            AbsCoord::new(1, 0), // vertex 3
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
        let edges: Vec<(u32, Vec<u32>)> = vec![];
        let coords: Vec<AbsCoord> = vec![];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert_eq!(csr.num_vertices(), 0);
        assert_eq!(csr.num_edges(), 0);
        // Empty graph has one offset entry (0) = 4 bytes
        assert_eq!(csr.memory_usage(), 8);
    }

    #[test]
    fn test_csr_single_vertex() {
        let edges = vec![(0u32, vec![])];
        let coords = vec![AbsCoord::new(0, 0)];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert_eq!(csr.num_vertices(), 1);
        assert_eq!(csr.num_edges(), 0);
        assert_eq!(csr.out_edges(VertexId(0)), &[]);
    }

    #[test]
    fn test_csr_self_loop() {
        let edges = vec![(0u32, vec![0u32])]; // Self loop
        let coords = vec![AbsCoord::new(0, 0)];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert_eq!(csr.out_edges(VertexId(0)), &[VertexId(0)]);
        assert_eq!(csr.num_edges(), 1);
    }

    #[test]
    fn test_csr_duplicate_edges() {
        // CSR should preserve duplicates (formulas can reference same cell multiple times)
        let edges = vec![(0u32, vec![1u32, 1u32, 2u32, 1u32])];
        let coords = vec![
            AbsCoord::new(0, 0),
            AbsCoord::new(0, 1),
            AbsCoord::new(0, 2),
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
            (0u32, vec![1u32, 2u32, 3u32]),
            (1u32, vec![2u32]),
            (2u32, vec![]),
            (3u32, vec![0u32, 1u32]),
        ];

        let coords = vec![
            AbsCoord::new(0, 0),
            AbsCoord::new(0, 1),
            AbsCoord::new(1, 0),
            AbsCoord::new(1, 1),
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert_eq!(csr.out_degree(VertexId(0)), 3);
        assert_eq!(csr.out_degree(VertexId(1)), 1);
        assert_eq!(csr.out_degree(VertexId(2)), 0);
        assert_eq!(csr.out_degree(VertexId(3)), 2);
    }

    #[test]
    fn test_out_of_bounds_access() {
        let edges = vec![(0u32, vec![])];
        let coords = vec![AbsCoord::new(0, 0)];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        // Should return empty slice - only vertex 0 exists
        assert_eq!(csr.out_edges(VertexId(1)), &[]);
    }

    #[test]
    fn test_csr_iterator() {
        let edges = vec![
            (0u32, vec![1u32, 2u32]),
            (1u32, vec![3u32]),
            (2u32, vec![1u32, 3u32]),
            (3u32, vec![]),
        ];

        let coords = vec![
            AbsCoord::new(0, 0),
            AbsCoord::new(0, 1),
            AbsCoord::new(1, 0),
            AbsCoord::new(1, 1),
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
            (0u32, vec![1u32, 2u32]),
            (1u32, vec![3u32]),
            (2u32, vec![]),
            (3u32, vec![0u32]), // Back edge
        ];

        let coords = vec![
            AbsCoord::new(0, 0),
            AbsCoord::new(0, 1),
            AbsCoord::new(1, 0),
            AbsCoord::new(1, 1),
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        assert!(csr.has_edge(VertexId(0), VertexId(1)));
        assert!(csr.has_edge(VertexId(0), VertexId(2)));
        assert!(!csr.has_edge(VertexId(0), VertexId(3)));
        assert!(csr.has_edge(VertexId(3), VertexId(0))); // Back edge exists
        assert!(!csr.has_edge(VertexId(2), VertexId(0))); // No edge
    }

    #[test]
    fn test_csr_with_offset_vertex_ids() {
        // Test CSR with vertex IDs starting at 1024 (FIRST_NORMAL_VERTEX)
        let base_id = 1024u32;
        let edges = vec![
            (base_id, vec![base_id + 1, base_id + 2]),
            (base_id + 1, vec![base_id + 3]),
            (base_id + 2, vec![base_id + 3]),
            (base_id + 3, vec![]),
        ];

        let coords = vec![
            AbsCoord::new(0, 0),
            AbsCoord::new(0, 1),
            AbsCoord::new(1, 0),
            AbsCoord::new(1, 1),
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        // Verify min vertex ID
        assert_eq!(csr.min_vertex_id, base_id);

        // Verify edges work with offset IDs
        assert_eq!(
            csr.out_edges(VertexId(base_id)),
            &[VertexId(base_id + 1), VertexId(base_id + 2)]
        );
        assert_eq!(
            csr.out_edges(VertexId(base_id + 1)),
            &[VertexId(base_id + 3)]
        );
        assert_eq!(
            csr.out_edges(VertexId(base_id + 2)),
            &[VertexId(base_id + 3)]
        );
        assert_eq!(csr.out_edges(VertexId(base_id + 3)), &[]);

        // Verify out of bounds returns empty
        assert_eq!(csr.out_edges(VertexId(0)), &[]); // Before min
        assert_eq!(csr.out_edges(VertexId(base_id + 100)), &[]); // After max
    }

    #[test]
    fn test_csr_with_sparse_vertex_ids() {
        // Test CSR with sparse vertex IDs (gaps in numbering)
        let edges = vec![
            (100u32, vec![300u32, 500u32]),
            (300u32, vec![500u32]),
            (500u32, vec![100u32]), // Back edge
        ];

        let coords = vec![
            AbsCoord::new(0, 0), // For vertex 100 (index 0)
            AbsCoord::new(0, 0), // Padding (index 100-199)
            AbsCoord::new(1, 0), // For vertex 300 (index 200)
            AbsCoord::new(0, 0), // Padding (index 300-399)
            AbsCoord::new(2, 0), // For vertex 500 (index 400)
        ];

        let csr = CsrEdges::from_adjacency(edges, &coords);

        // Verify min vertex ID
        assert_eq!(csr.min_vertex_id, 100);

        // Verify edges work
        assert_eq!(
            csr.out_edges(VertexId(100)),
            &[VertexId(300), VertexId(500)]
        );
        assert_eq!(csr.out_edges(VertexId(300)), &[VertexId(500)]);
        assert_eq!(csr.out_edges(VertexId(500)), &[VertexId(100)]);

        // Non-existent vertices return empty
        assert_eq!(csr.out_edges(VertexId(200)), &[]);
        assert_eq!(csr.out_edges(VertexId(400)), &[]);
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

    /// Reverse edges: offsets for incoming edges
    reverse_offsets: Vec<u32>,

    /// All incoming edges concatenated
    reverse_edges: Vec<VertexId>,

    /// Minimum vertex ID in the graph (for offset calculation)
    min_vertex_id: u32,
}

impl CsrEdges {
    /// Create CSR from adjacency list representation
    ///
    /// # Arguments
    /// - adj: Vector of (vertex_id, outgoing_edges) where vertex_id is the actual VertexId value
    /// - coords: Packed coordinates for each vertex (used for deterministic ordering)
    ///
    /// # Edge Ordering
    /// Edges are sorted by (row, col, vertex_id) to ensure deterministic
    /// evaluation order for formulas (important for functions with side effects)
    pub fn from_adjacency(adj: Vec<(u32, Vec<u32>)>, coords: &[AbsCoord]) -> Self {
        if adj.is_empty() {
            return Self {
                offsets: vec![0],
                edges: Vec::new(),
                reverse_offsets: vec![0],
                reverse_edges: Vec::new(),
                min_vertex_id: 0,
            };
        }

        // Find min and max vertex IDs
        let mut min_id = u32::MAX;
        let mut max_id = 0;
        for &(vid, ref targets) in &adj {
            min_id = min_id.min(vid);
            max_id = max_id.max(vid);
            for &target in targets {
                min_id = min_id.min(target);
                max_id = max_id.max(target);
            }
        }

        // If no vertices, return empty
        if min_id == u32::MAX {
            return Self {
                offsets: vec![0],
                edges: Vec::new(),
                reverse_offsets: vec![0],
                reverse_edges: Vec::new(),
                min_vertex_id: 0,
            };
        }

        let num_vertices = (max_id - min_id + 1) as usize;
        let mut offsets = vec![0u32; num_vertices + 1];
        let mut edges = Vec::new();

        // Build adjacency data indexed by offset
        let mut adj_by_offset: Vec<Vec<u32>> = vec![Vec::new(); num_vertices];
        for (vid, targets) in adj {
            let offset_idx = (vid - min_id) as usize;
            adj_by_offset[offset_idx] = targets;
        }

        // Build forward edges
        for (idx, mut targets) in adj_by_offset.clone().into_iter().enumerate() {
            // Sort targets by their coordinates for deterministic ordering
            targets.sort_by_key(|&t| {
                // Convert vertex ID to index in coords array
                let coord_idx = (t - min_id) as usize;
                if coord_idx < coords.len() {
                    let coord = coords[coord_idx];
                    (coord.row(), coord.col(), t)
                } else {
                    // Handle out-of-bounds gracefully for construction
                    (u32::MAX, u32::MAX, t)
                }
            });

            edges.extend(targets.into_iter().map(VertexId));
            offsets[idx + 1] = edges.len() as u32;
        }

        // Build reverse edges (incoming edges for each vertex)
        let mut reverse_offsets = vec![0u32; num_vertices + 1];
        let mut reverse_edges = Vec::new();
        let mut reverse_adj: Vec<Vec<u32>> = vec![Vec::new(); num_vertices];

        // Collect reverse edges
        for (idx, targets) in adj_by_offset.into_iter().enumerate() {
            let source = min_id + idx as u32;
            for target in targets {
                let target_idx = (target - min_id) as usize;
                if target_idx < num_vertices {
                    reverse_adj[target_idx].push(source);
                }
            }
        }

        // Build reverse CSR
        for (idx, mut sources) in reverse_adj.into_iter().enumerate() {
            // Sort sources by their coordinates for deterministic ordering
            sources.sort_by_key(|&s| {
                let coord_idx = (s - min_id) as usize;
                if coord_idx < coords.len() {
                    let coord = coords[coord_idx];
                    (coord.row(), coord.col(), s)
                } else {
                    (u32::MAX, u32::MAX, s)
                }
            });

            reverse_edges.extend(sources.into_iter().map(VertexId));
            reverse_offsets[idx + 1] = reverse_edges.len() as u32;
        }

        Self {
            offsets,
            edges,
            reverse_offsets,
            reverse_edges,
            min_vertex_id: min_id,
        }
    }

    /// Get outgoing edges for a vertex
    #[inline]
    pub fn out_edges(&self, v: VertexId) -> &[VertexId] {
        // Handle empty graph
        if self.offsets.len() <= 1 {
            return &[];
        }

        // Convert vertex ID to offset index
        if v.0 < self.min_vertex_id {
            return &[];
        }

        let idx = (v.0 - self.min_vertex_id) as usize;
        if idx >= self.offsets.len() - 1 {
            return &[];
        }

        let start = self.offsets[idx] as usize;
        let end = self.offsets[idx + 1] as usize;
        &self.edges[start..end]
    }

    /// Get incoming edges for a vertex (who depends on this vertex)
    #[inline]
    pub fn in_edges(&self, v: VertexId) -> &[VertexId] {
        // Handle empty graph
        if self.reverse_offsets.len() <= 1 {
            return &[];
        }

        // Convert vertex ID to offset index
        if v.0 < self.min_vertex_id {
            return &[];
        }

        let idx = (v.0 - self.min_vertex_id) as usize;
        if idx >= self.reverse_offsets.len() - 1 {
            return &[];
        }

        let start = self.reverse_offsets[idx] as usize;
        let end = self.reverse_offsets[idx + 1] as usize;
        &self.reverse_edges[start..end]
    }

    /// Get the out-degree of a vertex
    #[inline]
    pub fn out_degree(&self, v: VertexId) -> usize {
        // Handle empty graph
        if self.offsets.len() <= 1 {
            return 0;
        }

        // Convert vertex ID to offset index
        if v.0 < self.min_vertex_id {
            return 0;
        }

        let idx = (v.0 - self.min_vertex_id) as usize;
        if idx >= self.offsets.len() - 1 {
            return 0;
        }

        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];
        (end - start) as usize
    }

    /// Get the in-degree of a vertex
    #[inline]
    pub fn in_degree(&self, v: VertexId) -> usize {
        self.in_edges(v).len()
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
            + self.reverse_offsets.len() * std::mem::size_of::<u32>()
            + self.reverse_edges.len() * std::mem::size_of::<VertexId>()
    }

    /// Create an empty CSR graph
    pub fn empty() -> Self {
        Self {
            offsets: vec![0],
            edges: Vec::new(),
            reverse_offsets: vec![0],
            reverse_edges: Vec::new(),
            min_vertex_id: 0,
        }
    }

    /// Builder pattern for incremental construction
    pub fn builder() -> CsrBuilder {
        CsrBuilder::new()
    }

    /// Iterate over all vertices and their outgoing edges
    pub fn iter(&'_ self) -> CsrIterator<'_> {
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

        let vertex_id = VertexId(self.current_vertex as u32 + self.csr.min_vertex_id);
        let edges = self.csr.out_edges(vertex_id);
        self.current_vertex += 1;

        Some((vertex_id, edges))
    }
}

/// Builder for incremental CSR construction
pub struct CsrBuilder {
    adjacency: Vec<Vec<usize>>,
    coords: Vec<AbsCoord>,
}

impl Default for CsrBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CsrBuilder {
    pub fn new() -> Self {
        Self {
            adjacency: Vec::new(),
            coords: Vec::new(),
        }
    }

    /// Add a vertex with its coordinate
    pub fn add_vertex(&mut self, coord: AbsCoord) -> usize {
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
        // Convert to (vertex_id, edges) format starting from vertex ID 0
        let adj: Vec<_> = self
            .adjacency
            .into_iter()
            .enumerate()
            .map(|(idx, edges)| (idx as u32, edges.into_iter().map(|e| e as u32).collect()))
            .collect();
        CsrEdges::from_adjacency(adj, &self.coords)
    }
}
