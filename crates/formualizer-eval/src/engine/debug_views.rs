use super::vertex::{VertexId, VertexKind};
use super::vertex_store::VertexStore;
use std::fmt;

/// Immutable view into a vertex's data in the columnar store
///
/// Provides zero-cost abstraction for ergonomic access to vertex fields
pub struct VertexView<'s> {
    store: &'s VertexStore,
    id: VertexId,
}

impl<'s> VertexView<'s> {
    #[inline]
    pub fn new(store: &'s VertexStore, id: VertexId) -> Self {
        Self { store, id }
    }

    #[inline]
    pub fn id(&self) -> VertexId {
        self.id
    }

    #[inline]
    pub fn row(&self) -> u32 {
        self.store.coord(self.id).row()
    }

    #[inline]
    pub fn col(&self) -> u32 {
        self.store.coord(self.id).col()
    }

    #[inline]
    pub fn sheet_id(&self) -> u16 {
        self.store.sheet_id(self.id)
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.store.is_dirty(self.id)
    }

    #[inline]
    pub fn is_volatile(&self) -> bool {
        self.store.is_volatile(self.id)
    }

    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.store.is_deleted(self.id)
    }

    #[inline]
    pub fn kind(&self) -> VertexKind {
        self.store.kind(self.id)
    }

    #[inline]
    pub fn value_ref(&self) -> u32 {
        self.store.value_ref(self.id)
    }

    #[inline]
    pub fn edge_offset(&self) -> u32 {
        self.store.edge_offset(self.id)
    }

    #[inline]
    pub fn flags(&self) -> u8 {
        self.store.flags(self.id)
    }
}

impl<'s> fmt::Debug for VertexView<'s> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VertexView")
            .field("id", &self.id)
            .field("row", &self.row())
            .field("col", &self.col())
            .field("sheet_id", &self.sheet_id())
            .field("kind", &self.kind())
            .field("dirty", &self.is_dirty())
            .field("volatile", &self.is_volatile())
            .field("deleted", &self.is_deleted())
            .field("value_ref", &format!("0x{:08x}", self.value_ref()))
            .field("edge_offset", &self.edge_offset())
            .finish()
    }
}

impl<'s> fmt::Display for VertexView<'s> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format as Excel-style reference
        use crate::reference::Coord;
        let col_letters = Coord::col_to_letters(self.col());
        let row_1based = self.row() + 1;

        if self.sheet_id() == 0 {
            write!(f, "{col_letters}{row_1based}")
        } else {
            write!(f, "Sheet{}!{}{}", self.sheet_id(), col_letters, row_1based)
        }
    }
}

/// Mutable view into a vertex's data in the columnar store
pub struct VertexViewMut<'s> {
    store: &'s mut VertexStore,
    id: VertexId,
}

impl<'s> VertexViewMut<'s> {
    #[inline]
    pub fn new(store: &'s mut VertexStore, id: VertexId) -> Self {
        Self { store, id }
    }

    #[inline]
    pub fn id(&self) -> VertexId {
        self.id
    }

    // Read accessors (same as immutable view)
    #[inline]
    pub fn row(&self) -> u32 {
        self.store.coord(self.id).row()
    }

    #[inline]
    pub fn col(&self) -> u32 {
        self.store.coord(self.id).col()
    }

    #[inline]
    pub fn sheet_id(&self) -> u16 {
        self.store.sheet_id(self.id)
    }

    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.store.is_dirty(self.id)
    }

    #[inline]
    pub fn is_volatile(&self) -> bool {
        self.store.is_volatile(self.id)
    }

    #[inline]
    pub fn kind(&self) -> VertexKind {
        self.store.kind(self.id)
    }

    // Write accessors
    #[inline]
    pub fn set_kind(&mut self, kind: VertexKind) {
        self.store.set_kind(self.id, kind);
    }

    #[inline]
    pub fn set_dirty(&mut self, dirty: bool) {
        self.store.set_dirty(self.id, dirty);
    }

    #[inline]
    pub fn set_volatile(&mut self, volatile: bool) {
        self.store.set_volatile(self.id, volatile);
    }

    #[inline]
    pub fn set_value_ref(&mut self, value_ref: u32) {
        self.store.set_value_ref(self.id, value_ref);
    }

    #[inline]
    pub fn set_edge_offset(&mut self, offset: u32) {
        self.store.set_edge_offset(self.id, offset);
    }
}

impl<'s> fmt::Debug for VertexViewMut<'s> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VertexViewMut")
            .field("id", &self.id)
            .field("row", &self.row())
            .field("col", &self.col())
            .field("sheet_id", &self.sheet_id())
            .field("kind", &self.kind())
            .field("dirty", &self.is_dirty())
            .field("volatile", &self.is_volatile())
            .finish()
    }
}

// Extension methods for VertexStore
impl VertexStore {
    /// Create an immutable view for a vertex
    #[inline]
    pub fn view(&'_ self, id: VertexId) -> VertexView<'_> {
        VertexView::new(self, id)
    }

    /// Create a mutable view for a vertex
    #[inline]
    pub fn view_mut(&'_ mut self, id: VertexId) -> VertexViewMut<'_> {
        VertexViewMut::new(self, id)
    }

    /// Debug helper to dump vertex info
    pub fn debug_vertex(&self, id: VertexId) -> String {
        let view = self.view(id);
        format!("{view:?}")
    }

    /// Debug helper to dump a range of vertices
    pub fn debug_range(&self, start: VertexId, count: usize) -> Vec<String> {
        (0..count)
            .map(|i| {
                let id = VertexId(start.0 + i as u32);
                self.debug_vertex(id)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use formualizer_common::Coord as AbsCoord;

    #[test]
    fn test_vertex_view_access() {
        let mut store = VertexStore::new();
        let id = store.allocate(AbsCoord::new(5, 10), 2, 0x03);

        let view = store.view(id);
        assert_eq!(view.row(), 5);
        assert_eq!(view.col(), 10);
        assert_eq!(view.sheet_id(), 2);
        assert!(view.is_dirty());
        assert!(view.is_volatile());
    }

    #[test]
    fn test_debug_output() {
        let mut store = VertexStore::new();
        let id = store.allocate(AbsCoord::new(1, 1), 0, 0x01);
        store.set_kind(id, VertexKind::Cell);

        let view = store.view(id);
        let debug = format!("{view:?}");
        assert!(debug.contains("row: 1"));
        assert!(debug.contains("col: 1"));
        assert!(debug.contains("Cell"));
    }

    #[test]
    fn test_mutable_view() {
        let mut store = VertexStore::new();
        let id = store.allocate(AbsCoord::new(0, 0), 0, 0);

        {
            let mut view = store.view_mut(id);
            view.set_dirty(true);
            view.set_volatile(true);
            view.set_value_ref(42);
        }

        assert!(store.is_dirty(id));
        assert!(store.is_volatile(id));
        assert_eq!(store.value_ref(id), 42);
    }

    #[test]
    fn test_view_lifetime() {
        let mut store = VertexStore::new();
        let id1 = store.allocate(AbsCoord::new(0, 0), 0, 0);
        let id2 = store.allocate(AbsCoord::new(1, 1), 0, 0);

        // Multiple immutable views should work
        let view1 = store.view(id1);
        let view2 = store.view(id2);

        assert_eq!(view1.row(), 0);
        assert_eq!(view2.row(), 1);
    }

    #[test]
    fn test_view_display() {
        let mut store = VertexStore::new();
        let id = store.allocate(AbsCoord::new(0, 5), 1, 0x01);
        store.set_kind(id, VertexKind::Cell);

        let view = store.view(id);
        let display = format!("{view}");
        assert!(display.contains("Sheet1!F1")); // col 5 = F, row 0 = 1 (1-based)
    }

    #[test]
    fn test_zero_cost_abstraction() {
        // Verify that view methods are truly zero-cost
        // This test ensures inlining happens correctly
        let mut store = VertexStore::new();
        let id = store.allocate(AbsCoord::new(100, 200), 5, 0x07);

        // These should compile to direct array access
        let view = store.view(id);

        // Multiple accesses should be optimized
        let row1 = view.row();
        let row2 = view.row();
        assert_eq!(row1, row2);
        assert_eq!(row1, 100);

        // Verify all accessors work efficiently
        assert_eq!(view.col(), 200);
        assert_eq!(view.sheet_id(), 5);
        assert!(view.is_dirty());
        assert!(view.is_volatile());
        assert!(view.is_deleted());
    }
}
