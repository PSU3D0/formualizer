use super::interval_tree::IntervalTree;
use super::vertex::VertexId;
use formualizer_common::Coord as AbsCoord;
use std::collections::HashSet;

/// Sheet-level sparse index for efficient range queries on vertex positions.
///
/// ## Why SheetIndex with interval trees?
///
/// While `cell_to_vertex: HashMap<CellRef, VertexId>` provides O(1) exact lookups,
/// structural operations (insert/delete rows/columns) need to find ALL vertices
/// in a given range, which would require O(n) full scans of the hash map.
///
/// ### Performance comparison:
///
/// | Operation | Hash map only | With SheetIndex |
/// |-----------|---------------|-----------------|
/// | Insert 100 rows at row 20,000 | O(total cells) | O(log n + k)* |
/// | Delete columns B:D | O(total cells) | O(log n + k) |
/// | Viewport query (visible cells) | O(total cells) | O(log n + k) |
///
/// *where n = number of indexed vertices, k = vertices actually affected
///
/// ### Memory efficiency:
///
/// - Each interval is just 2×u32 + Vec<VertexId> pointer
/// - Spreadsheets are extremely sparse (1M row sheet typically has <10K cells)
/// - Point intervals (single cells) are the common case
/// - Trees stay small and cache-friendly
///
/// ### Future benefits:
///
/// 1. **Virtual scrolling** - fetch viewport cells in microseconds
/// 2. **Lazy evaluation** - mark row blocks dirty without scanning
/// 3. **Concurrent reads** - trees are read-mostly, perfect for RwLock
/// 4. **Minimal undo/redo** - know exactly which vertices were touched
#[derive(Debug, Default)]
pub struct SheetIndex {
    /// Row interval tree: maps row ranges → vertices in those rows
    /// For a cell at (r,c), we store the point interval [r,r] → VertexId
    row_tree: IntervalTree<VertexId>,

    /// Column interval tree: maps column ranges → vertices in those columns  
    /// For a cell at (r,c), we store the point interval [c,c] → VertexId
    col_tree: IntervalTree<VertexId>,
}

impl SheetIndex {
    /// Create a new empty sheet index
    pub fn new() -> Self {
        Self {
            row_tree: IntervalTree::new(),
            col_tree: IntervalTree::new(),
        }
    }

    /// Fast path build from sorted coordinates. Assumes items are row-major sorted.
    pub fn build_from_sorted(&mut self, items: &[(AbsCoord, VertexId)]) {
        self.add_vertices_batch(items);
    }

    /// Add a vertex at the given coordinate to the index.
    ///
    /// ## Complexity
    /// O(log n) where n is the number of vertices in the index
    pub fn add_vertex(&mut self, coord: AbsCoord, vertex_id: VertexId) {
        let row = coord.row();
        let col = coord.col();

        // Add to row tree - point interval [row, row]
        self.row_tree
            .entry(row, row)
            .or_insert_with(HashSet::new)
            .insert(vertex_id);

        // Add to column tree - point interval [col, col]
        self.col_tree
            .entry(col, col)
            .or_insert_with(HashSet::new)
            .insert(vertex_id);
    }

    /// Add many vertices in a single pass. Assumes coords belong to same sheet index.
    pub fn add_vertices_batch(&mut self, items: &[(AbsCoord, VertexId)]) {
        if items.is_empty() {
            return;
        }
        // If trees are empty we can bulk build from sorted points in O(n log n) with better constants.
        if self.row_tree.is_empty() && self.col_tree.is_empty() {
            // Build row points
            let mut row_items: Vec<(u32, HashSet<VertexId>)> = Vec::with_capacity(items.len());
            let mut col_items: Vec<(u32, HashSet<VertexId>)> = Vec::with_capacity(items.len());
            // Use temp hash maps for merging duplicates
            use rustc_hash::FxHashMap;
            let mut row_map: FxHashMap<u32, HashSet<VertexId>> = FxHashMap::default();
            let mut col_map: FxHashMap<u32, HashSet<VertexId>> = FxHashMap::default();
            for (coord, vid) in items {
                row_map.entry(coord.row()).or_default().insert(*vid);
                col_map.entry(coord.col()).or_default().insert(*vid);
            }
            row_items.reserve(row_map.len());
            for (k, v) in row_map.into_iter() {
                row_items.push((k, v));
            }
            col_items.reserve(col_map.len());
            for (k, v) in col_map.into_iter() {
                col_items.push((k, v));
            }
            self.row_tree.bulk_build_points(row_items);
            self.col_tree.bulk_build_points(col_items);
            return;
        }
        // Fallback: incremental for already populated index
        for (coord, vid) in items {
            let row = coord.row();
            let col = coord.col();
            self.row_tree
                .entry(row, row)
                .or_insert_with(HashSet::new)
                .insert(*vid);
            self.col_tree
                .entry(col, col)
                .or_insert_with(HashSet::new)
                .insert(*vid);
        }
    }

    /// Remove a vertex from the index.
    ///
    /// ## Complexity
    /// O(log n) where n is the number of vertices in the index
    pub fn remove_vertex(&mut self, coord: AbsCoord, vertex_id: VertexId) {
        let row = coord.row();
        let col = coord.col();

        // Remove from row tree
        if let Some(vertices) = self.row_tree.get_mut(row, row) {
            vertices.remove(&vertex_id);
            // Note: We could remove empty intervals, but keeping them is fine
            // as they take minimal space and may be reused
        }

        // Remove from column tree
        if let Some(vertices) = self.col_tree.get_mut(col, col) {
            vertices.remove(&vertex_id);
        }
    }

    /// Update a vertex's position in the index (move operation).
    ///
    /// ## Complexity
    /// O(log n) for removal + O(log n) for insertion = O(log n)
    pub fn update_vertex(&mut self, old_coord: AbsCoord, new_coord: AbsCoord, vertex_id: VertexId) {
        self.remove_vertex(old_coord, vertex_id);
        self.add_vertex(new_coord, vertex_id);
    }

    /// Query all vertices in the given row range.
    ///
    /// ## Complexity
    /// O(log n + k) where k is the number of vertices in the range
    pub fn vertices_in_row_range(&self, start: u32, end: u32) -> Vec<VertexId> {
        let mut result = HashSet::new();

        for (_low, _high, values) in self.row_tree.query(start, end) {
            result.extend(values.iter());
        }

        result.into_iter().collect()
    }

    /// Query all vertices in the given column range.
    ///
    /// ## Complexity
    /// O(log n + k) where k is the number of vertices in the range
    pub fn vertices_in_col_range(&self, start: u32, end: u32) -> Vec<VertexId> {
        let mut result = HashSet::new();

        for (_low, _high, values) in self.col_tree.query(start, end) {
            result.extend(values.iter());
        }

        result.into_iter().collect()
    }

    /// Query all vertices in a rectangular range.
    ///
    /// ## Complexity
    /// O(log n + k) where k is the number of vertices in the range
    pub fn vertices_in_rect(
        &self,
        start_row: u32,
        end_row: u32,
        start_col: u32,
        end_col: u32,
    ) -> Vec<VertexId> {
        // Get vertices in row range
        let row_vertices: HashSet<_> = self
            .vertices_in_row_range(start_row, end_row)
            .into_iter()
            .collect();

        // Get vertices in column range
        let col_vertices: HashSet<_> = self
            .vertices_in_col_range(start_col, end_col)
            .into_iter()
            .collect();

        // Return intersection - vertices that are in both ranges
        row_vertices.intersection(&col_vertices).copied().collect()
    }

    /// Get the total number of indexed vertices (may count duplicates if vertex appears at multiple positions).
    pub fn len(&self) -> usize {
        let mut unique: HashSet<VertexId> = HashSet::new();

        // Collect all unique vertices from row tree
        for (_low, _high, values) in self.row_tree.query(0, u32::MAX) {
            unique.extend(values.iter());
        }

        unique.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.row_tree.is_empty()
    }

    /// Clear all entries from the index.
    pub fn clear(&mut self) {
        self.row_tree = IntervalTree::new();
        self.col_tree = IntervalTree::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_query_single_vertex() {
        let mut index = SheetIndex::new();
        let coord = AbsCoord::new(5, 10);
        let vertex_id = VertexId(1024);

        index.add_vertex(coord, vertex_id);

        // Query exact row
        let row_results = index.vertices_in_row_range(5, 5);
        assert_eq!(row_results, vec![vertex_id]);

        // Query exact column
        let col_results = index.vertices_in_col_range(10, 10);
        assert_eq!(col_results, vec![vertex_id]);

        // Query range containing the vertex
        let row_results = index.vertices_in_row_range(3, 7);
        assert_eq!(row_results, vec![vertex_id]);
    }

    #[test]
    fn test_remove_vertex() {
        let mut index = SheetIndex::new();
        let coord = AbsCoord::new(5, 10);
        let vertex_id = VertexId(1024);

        index.add_vertex(coord, vertex_id);
        assert_eq!(index.len(), 1);

        index.remove_vertex(coord, vertex_id);
        assert_eq!(index.len(), 0);

        // Should return empty after removal
        let row_results = index.vertices_in_row_range(5, 5);
        assert!(row_results.is_empty());
    }

    #[test]
    fn test_update_vertex_position() {
        let mut index = SheetIndex::new();
        let old_coord = AbsCoord::new(5, 10);
        let new_coord = AbsCoord::new(15, 20);
        let vertex_id = VertexId(1024);

        index.add_vertex(old_coord, vertex_id);
        index.update_vertex(old_coord, new_coord, vertex_id);

        // Should not be at old position
        let old_row_results = index.vertices_in_row_range(5, 5);
        assert!(old_row_results.is_empty());

        // Should be at new position
        let new_row_results = index.vertices_in_row_range(15, 15);
        assert_eq!(new_row_results, vec![vertex_id]);

        let new_col_results = index.vertices_in_col_range(20, 20);
        assert_eq!(new_col_results, vec![vertex_id]);
    }

    #[test]
    fn test_range_queries() {
        let mut index = SheetIndex::new();

        // Add vertices in a pattern
        for row in 0..10 {
            for col in 0..5 {
                let coord = AbsCoord::new(row, col);
                let vertex_id = VertexId(1024 + row * 5 + col);
                index.add_vertex(coord, vertex_id);
            }
        }

        // Query rows 3-5 (should get 3 rows × 5 cols = 15 vertices)
        let row_results = index.vertices_in_row_range(3, 5);
        assert_eq!(row_results.len(), 15);

        // Query columns 1-2 (should get 10 rows × 2 cols = 20 vertices)
        let col_results = index.vertices_in_col_range(1, 2);
        assert_eq!(col_results.len(), 20);

        // Query rectangle (rows 3-5, cols 1-2) should get 3 × 2 = 6 vertices
        let rect_results = index.vertices_in_rect(3, 5, 1, 2);
        assert_eq!(rect_results.len(), 6);
    }

    #[test]
    fn test_sparse_sheet_efficiency() {
        let mut index = SheetIndex::new();

        // Simulate sparse sheet - only a few cells in a million-row range
        index.add_vertex(AbsCoord::new(100, 5), VertexId(1024));
        index.add_vertex(AbsCoord::new(50_000, 10), VertexId(1025));
        index.add_vertex(AbsCoord::new(100_000, 15), VertexId(1026));
        index.add_vertex(AbsCoord::new(500_000, 20), VertexId(1027));
        index.add_vertex(AbsCoord::new(999_999, 25), VertexId(1028));

        assert_eq!(index.len(), 5);

        // Query for rows >= 100_000 (should find 3 vertices efficiently)
        let high_rows = index.vertices_in_row_range(100_000, u32::MAX);
        assert_eq!(high_rows.len(), 3);

        // Query for specific column range
        let col_range = index.vertices_in_col_range(10, 20);
        assert_eq!(col_range.len(), 3); // columns 10, 15, 20
    }

    #[test]
    fn test_shift_operation_query() {
        let mut index = SheetIndex::new();

        // Setup: cells at rows 10, 20, 30, 40, 50
        for row in [10, 20, 30, 40, 50] {
            index.add_vertex(AbsCoord::new(row, 0), VertexId(1024 + row));
        }

        // Simulate "insert 5 rows at row 25" - need to find all vertices with row >= 25
        let vertices_to_shift = index.vertices_in_row_range(25, u32::MAX);
        assert_eq!(vertices_to_shift.len(), 3); // rows 30, 40, 50

        // Simulate "delete columns B:D" - need to find all vertices in columns 1-3
        for col in 1..=3 {
            index.add_vertex(AbsCoord::new(5, col), VertexId(2000 + col));
        }

        let vertices_to_delete = index.vertices_in_col_range(1, 3);
        assert_eq!(vertices_to_delete.len(), 3);
    }

    #[test]
    fn test_viewport_query() {
        let mut index = SheetIndex::new();

        // Simulate a spreadsheet with scattered data
        for row in (0..10000).step_by(100) {
            for col in 0..10 {
                index.add_vertex(AbsCoord::new(row, col), VertexId(row * 10 + col));
            }
        }

        // Query viewport: rows 500-1500, columns 2-7
        let viewport = index.vertices_in_rect(500, 1500, 2, 7);

        // Should find 11 rows (500, 600, ..., 1500) × 6 columns (2-7) = 66 vertices
        assert_eq!(viewport.len(), 66);
    }
}
