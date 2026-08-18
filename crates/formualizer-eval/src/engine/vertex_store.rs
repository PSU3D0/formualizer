use super::addr::{GridAddr, VertexAddr};
use super::vertex::{VertexId, VertexKind};
use crate::SheetId;
use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::addr::SymbolAddr;

    fn grid(row: u32, col: u32) -> VertexAddr {
        VertexAddr::grid(GridAddr::new(row, col))
    }

    #[test]
    fn test_vertex_store_allocation() {
        let mut store = VertexStore::new();
        let id = store.allocate(grid(10, 20), 1, 0x01);
        assert_eq!(store.addr(id), grid(10, 20));
        assert_eq!(store.sheet_id(id), 1);
        assert_eq!(store.flags(id), 0x01);
    }

    #[test]
    fn prepared_batch_vertex_overflow_is_checked_before_mutation() {
        let mut store = VertexStore::new();
        store.len = u32::MAX as usize - FIRST_NORMAL_VERTEX as usize + 1;
        let before = (
            store.coords.len(),
            store.sheet_kind.len(),
            store.flags.len(),
        );
        assert_eq!(
            store.try_allocate_batch(&[(grid(0, 0), 0, 0)], &[VertexId(FIRST_NORMAL_VERTEX)],),
            Err(VertexBatchAllocationError::IdExhausted)
        );
        assert_eq!(
            before,
            (
                store.coords.len(),
                store.sheet_kind.len(),
                store.flags.len()
            )
        );
    }

    #[test]
    fn prepared_batch_reserved_id_mismatch_is_checked_before_mutation() {
        let mut store = VertexStore::new();
        let before = (store.len(), store.coords.len(), store.flags.len());
        assert_eq!(
            store.try_allocate_batch(&[(grid(0, 0), 0, 0)], &[VertexId(FIRST_NORMAL_VERTEX + 1)],),
            Err(VertexBatchAllocationError::ReservedIdsMismatch)
        );
        assert_eq!(before, (store.len(), store.coords.len(), store.flags.len()));
    }

    #[test]
    fn test_vertex_store_grow() {
        let mut store = VertexStore::with_capacity(1000);
        for i in 0..10_000 {
            store.allocate(grid(i, i), 0, 0);
        }
        assert_eq!(store.len(), 10_000);
        // Note: While VertexStore itself is 64-byte aligned,
        // the Vec allocations inside may not be. This is fine
        // as the important thing is data locality, not alignment.
    }

    #[test]
    fn test_vertex_store_capacity() {
        let store = VertexStore::with_capacity(100);
        assert!(store.coords.capacity() >= 100);
        assert!(store.sheet_kind.capacity() >= 100);
        assert!(store.flags.capacity() >= 100);
        assert!(store.value_ref.capacity() >= 100);
        assert!(store.edge_offset.capacity() >= 100);
    }

    #[test]
    fn test_vertex_store_accessors() {
        let mut store = VertexStore::new();
        let id = store.allocate(grid(5, 10), 3, 0x03);

        // Test coord access
        let position = store
            .grid_addr(id)
            .expect("cell vertices carry a grid position");
        assert_eq!(position.row(), 5);
        assert_eq!(position.col(), 10);

        // Test sheet_id access
        assert_eq!(store.sheet_id(id), 3);

        // Test flags access
        assert_eq!(store.flags(id), 0x03);
        assert!(store.is_dirty(id));
        assert!(store.is_volatile(id));

        // Test kind access/update
        store.set_kind(id, VertexKind::Cell);
        assert_eq!(store.kind(id), VertexKind::Cell);
    }

    #[test]
    fn symbol_vertices_have_no_grid_position() {
        let mut store = VertexStore::new();
        let cell = store.allocate(grid(3, 4), 0, 0);
        let symbol = store.allocate(VertexAddr::symbol(SymbolAddr::new(0)), 0, 0);

        assert_eq!(store.grid_addr(cell), Some(GridAddr::new(3, 4)));
        assert_eq!(store.grid_addr(symbol), None);
        assert!(store.addr(symbol).is_symbol());
        assert_eq!(store.addr(symbol).as_symbol(), Some(SymbolAddr::new(0)));
    }

    /// The edge coordinate arrays and the vertex store hold one address per vertex,
    /// parallel to adjacency. Tagging the symbol space must not widen that slot.
    #[test]
    fn stored_address_element_size_is_unchanged() {
        assert_eq!(std::mem::size_of::<VertexAddr>(), 8);
        assert_eq!(
            std::mem::size_of::<VertexAddr>(),
            std::mem::size_of::<formualizer_common::Coord>(),
        );
    }

    #[test]
    fn test_reserved_vertex_range() {
        let mut store = VertexStore::new();
        // First allocation should be >= FIRST_NORMAL_VERTEX
        let id = store.allocate(grid(0, 0), 0, 0);
        assert!(id.0 >= FIRST_NORMAL_VERTEX);
    }

    #[test]
    fn test_atomic_flag_operations() {
        let mut store = VertexStore::new();
        let id = store.allocate(grid(0, 0), 0, 0);

        // Test atomic flag updates
        store.set_dirty(id, true);
        assert!(store.is_dirty(id));

        store.set_dirty(id, false);
        assert!(!store.is_dirty(id));

        store.set_volatile(id, true);
        assert!(store.is_volatile(id));
    }

    #[test]
    fn test_vertex_store_set_addr() {
        let mut store = VertexStore::new();
        let id = store.allocate(grid(1, 1), 0, 0);

        // Update coordinate
        store.set_addr(id, grid(5, 10));
        assert_eq!(store.addr(id), grid(5, 10));
    }

    #[test]
    fn test_vertex_store_atomic_flags() {
        let mut store = VertexStore::new();
        let id = store.allocate(grid(0, 0), 0, 0);

        // Test atomic flag operations
        store.set_dirty(id, true);
        assert!(store.is_dirty(id));

        store.set_volatile(id, true);
        assert!(store.is_volatile(id));

        // Mark as deleted (tombstone)
        store.mark_deleted(id, true);
        assert!(store.is_deleted(id));
    }

    #[test]
    fn test_reserved_id_range_preserved() {
        let mut store = VertexStore::new();

        // Verify first allocation is >= FIRST_NORMAL_VERTEX
        let id = store.allocate(grid(0, 0), 0, 0);
        assert!(id.0 >= FIRST_NORMAL_VERTEX);

        // Verify deletion uses tombstone, not physical removal
        store.mark_deleted(id, true);
        assert!(store.vertex_exists(id));
        assert!(store.is_deleted(id));
    }
}

/// Reserved vertex ID range constants
pub const FIRST_NORMAL_VERTEX: u32 = 1024;
pub const RANGE_VERTEX_START: u32 = 0;
pub const EXTERNAL_VERTEX_START: u32 = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VertexBatchAllocationError {
    IdExhausted,
    ReservedIdsMismatch,
}

/// Core columnar storage for vertices in Struct-of-Arrays layout
///
/// Memory layout optimized for cache efficiency:
/// - 21B logical per vertex (no struct padding)
/// - Dense columnar arrays for hot data
/// - Atomic flags for lock-free operations
#[repr(C, align(64))]
#[derive(Debug)]
pub struct VertexStore {
    // Dense columnar arrays - 21B per vertex logical
    coords: Vec<VertexAddr>, // 8B (packed grid position or symbol identity)
    sheet_kind: Vec<u32>,    // 4B (16-bit sheet, 8-bit kind, 8-bit reserved)
    flags: Vec<AtomicU8>,    // 1B (dirty|volatile|deleted|...)
    value_ref: Vec<u32>,     // 4B (2-bit tag, 4-bit error, 26-bit index)
    edge_offset: Vec<u32>,   // 4B (CSR offset)

    // Length tracking
    len: usize,
}

impl Default for VertexStore {
    fn default() -> Self {
        Self::new()
    }
}

impl VertexStore {
    pub fn new() -> Self {
        Self {
            coords: Vec::new(),
            sheet_kind: Vec::new(),
            flags: Vec::new(),
            value_ref: Vec::new(),
            edge_offset: Vec::new(),
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            coords: Vec::with_capacity(capacity),
            sheet_kind: Vec::with_capacity(capacity),
            flags: Vec::with_capacity(capacity),
            value_ref: Vec::with_capacity(capacity),
            edge_offset: Vec::with_capacity(capacity),
            len: 0,
        }
    }

    /// Reserve additional capacity for upcoming vertex allocations.
    pub fn reserve(&mut self, additional: usize) {
        if additional == 0 {
            return;
        }
        // Ensure each column has enough spare capacity
        let target = self.len + additional;
        if self.coords.capacity() < target {
            self.coords.reserve(additional);
        }
        if self.sheet_kind.capacity() < target {
            self.sheet_kind.reserve(additional);
        }
        if self.flags.capacity() < target {
            self.flags.reserve(additional);
        }
        if self.value_ref.capacity() < target {
            self.value_ref.reserve(additional);
        }
        if self.edge_offset.capacity() < target {
            self.edge_offset.reserve(additional);
        }
    }

    /// Allocate a new vertex, returning its ID
    /// IDs start at FIRST_NORMAL_VERTEX to reserve 0-1023 for special vertices
    pub fn allocate(&mut self, addr: VertexAddr, sheet: SheetId, flags: u8) -> VertexId {
        let id = VertexId(self.len as u32 + FIRST_NORMAL_VERTEX);
        debug_assert!(id.0 >= FIRST_NORMAL_VERTEX);

        self.coords.push(addr);
        self.sheet_kind.push((sheet as u32) << 16);
        self.flags.push(AtomicU8::new(flags));
        self.value_ref.push(0);
        self.edge_offset.push(0);
        self.len += 1;

        id
    }

    pub(crate) fn try_allocate_batch(
        &mut self,
        vertices: &[(VertexAddr, SheetId, u8)],
        expected_ids: &[VertexId],
    ) -> Result<Vec<VertexId>, VertexBatchAllocationError> {
        if vertices.len() != expected_ids.len() {
            return Err(VertexBatchAllocationError::ReservedIdsMismatch);
        }
        let start = u32::try_from(self.len)
            .map_err(|_| VertexBatchAllocationError::IdExhausted)?
            .checked_add(FIRST_NORMAL_VERTEX)
            .ok_or(VertexBatchAllocationError::IdExhausted)?;
        let count =
            u32::try_from(vertices.len()).map_err(|_| VertexBatchAllocationError::IdExhausted)?;
        if count != 0 {
            start
                .checked_add(count - 1)
                .ok_or(VertexBatchAllocationError::IdExhausted)?;
        }
        let ids: Vec<_> = (0..count).map(|offset| VertexId(start + offset)).collect();
        if ids != expected_ids {
            return Err(VertexBatchAllocationError::ReservedIdsMismatch);
        }
        self.reserve(vertices.len());
        for &(addr, sheet, flags) in vertices {
            self.coords.push(addr);
            self.sheet_kind.push((u32::from(sheet)) << 16);
            self.flags.push(AtomicU8::new(flags));
            self.value_ref.push(0);
            self.edge_offset.push(0);
            self.len += 1;
        }
        Ok(ids)
    }

    /// Allocate a batch whose identifiers were checked against the current
    /// store length by an exclusively-held prepared transaction.
    pub(crate) fn allocate_prevalidated_batch(&mut self, vertices: &[(VertexAddr, SheetId, u8)]) {
        self.reserve(vertices.len());
        for &(addr, sheet, flags) in vertices {
            self.allocate(addr, sheet, flags);
        }
    }

    /// Allocate many vertices contiguously in the current store order.
    /// Returns the assigned VertexIds in the same order as input coords.
    pub fn allocate_contiguous(
        &mut self,
        sheet: SheetId,
        addrs: &[VertexAddr],
        flags: u8,
    ) -> Vec<VertexId> {
        if addrs.is_empty() {
            return Vec::new();
        }
        self.reserve(addrs.len());
        let mut ids = Vec::with_capacity(addrs.len());
        for &addr in addrs {
            ids.push(self.allocate(addr, sheet, flags));
        }
        ids
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Convert vertex ID to index, returning None if invalid
    #[inline]
    fn vertex_id_to_index(&self, id: VertexId) -> Option<usize> {
        if id.0 < FIRST_NORMAL_VERTEX {
            return None;
        }
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        if idx >= self.len {
            return None;
        }
        Some(idx)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    // Accessors
    /// The vertex's address: a grid position for cells and formulas, a symbol identity
    /// for names, tables and external sources.
    #[inline]
    pub fn addr(&self, id: VertexId) -> VertexAddr {
        if let Some(idx) = self.vertex_id_to_index(id) {
            self.coords[idx]
        } else {
            VertexAddr::INVALID // Invalid vertices have no address at all
        }
    }

    /// The vertex's grid position, or `None` when it is a symbol.
    ///
    /// Grid-keyed structures go through this, so a symbol cannot reach them.
    #[inline]
    pub fn grid_addr(&self, id: VertexId) -> Option<GridAddr> {
        self.addr(id).as_grid()
    }

    #[inline]
    pub fn sheet_id(&self, id: VertexId) -> SheetId {
        if let Some(idx) = self.vertex_id_to_index(id) {
            (self.sheet_kind[idx] >> 16) as SheetId
        } else {
            0 // Default sheet ID for invalid vertices
        }
    }

    #[inline]
    pub fn kind(&self, id: VertexId) -> VertexKind {
        if let Some(idx) = self.vertex_id_to_index(id) {
            let tag = ((self.sheet_kind[idx] >> 8) & 0xFF) as u8;
            VertexKind::from_tag(tag)
        } else {
            VertexKind::Empty // Default kind for invalid vertices
        }
    }

    #[inline]
    pub fn set_kind(&mut self, id: VertexId, kind: VertexKind) {
        if let Some(idx) = self.vertex_id_to_index(id) {
            let sheet_bits = self.sheet_kind[idx] & 0xFFFF0000;
            self.sheet_kind[idx] = sheet_bits | ((kind.to_tag() as u32) << 8);
        }
    }

    #[inline]
    pub fn flags(&self, id: VertexId) -> u8 {
        if let Some(idx) = self.vertex_id_to_index(id) {
            self.flags[idx].load(Ordering::Acquire)
        } else {
            0 // Default flags for invalid vertices
        }
    }

    #[inline]
    pub fn is_dirty(&self, id: VertexId) -> bool {
        self.flags(id) & 0x01 != 0
    }

    #[inline]
    pub fn is_volatile(&self, id: VertexId) -> bool {
        self.flags(id) & 0x02 != 0
    }

    #[inline]
    pub fn is_deleted(&self, id: VertexId) -> bool {
        self.flags(id) & 0x04 != 0
    }

    #[inline]
    pub fn is_dynamic(&self, id: VertexId) -> bool {
        self.flags(id) & 0x08 != 0
    }

    #[inline]
    pub fn set_dirty(&self, id: VertexId, dirty: bool) {
        if id.0 < FIRST_NORMAL_VERTEX {
            return; // Skip invalid vertex IDs
        }
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        if idx >= self.flags.len() {
            return; // Out of bounds
        }
        if dirty {
            self.flags[idx].fetch_or(0x01, Ordering::Release);
        } else {
            self.flags[idx].fetch_and(!0x01, Ordering::Release);
        }
    }

    #[inline]
    pub fn set_volatile(&self, id: VertexId, volatile: bool) {
        if id.0 < FIRST_NORMAL_VERTEX {
            return;
        }
        if let Some(idx) = self.vertex_id_to_index(id) {
            if volatile {
                self.flags[idx].fetch_or(0x02, std::sync::atomic::Ordering::Release);
            } else {
                self.flags[idx].fetch_and(!0x02, std::sync::atomic::Ordering::Release);
            }
        }
    }

    #[inline]
    pub fn set_dynamic(&self, id: VertexId, dynamic: bool) {
        if id.0 < FIRST_NORMAL_VERTEX {
            return;
        }
        if let Some(idx) = self.vertex_id_to_index(id) {
            if dynamic {
                self.flags[idx].fetch_or(0x08, std::sync::atomic::Ordering::Release);
            } else {
                self.flags[idx].fetch_and(!0x08, std::sync::atomic::Ordering::Release);
            }
        }
    }

    #[inline]
    pub fn value_ref(&self, id: VertexId) -> u32 {
        if let Some(idx) = self.vertex_id_to_index(id) {
            self.value_ref[idx]
        } else {
            0 // Default value ref for invalid vertices
        }
    }

    #[inline]
    pub fn set_value_ref(&mut self, id: VertexId, value_ref: u32) {
        if let Some(idx) = self.vertex_id_to_index(id) {
            self.value_ref[idx] = value_ref;
        }
    }

    #[inline]
    pub fn edge_offset(&self, id: VertexId) -> u32 {
        if let Some(idx) = self.vertex_id_to_index(id) {
            self.edge_offset[idx]
        } else {
            0 // Default edge offset for invalid vertices
        }
    }

    #[inline]
    pub fn set_edge_offset(&mut self, id: VertexId, offset: u32) {
        if let Some(idx) = self.vertex_id_to_index(id) {
            self.edge_offset[idx] = offset;
        }
    }

    /// Update the address of a vertex
    /// # Safety
    /// Caller must ensure CSR edge cache is updated via CsrMutableEdges::update_addr
    #[doc(hidden)]
    pub fn set_addr(&mut self, id: VertexId, addr: VertexAddr) {
        if let Some(idx) = self.vertex_id_to_index(id) {
            self.coords[idx] = addr;
        }
    }

    /// Mark vertex as deleted (tombstone strategy)
    pub fn mark_deleted(&self, id: VertexId, deleted: bool) {
        if let Some(idx) = self.vertex_id_to_index(id) {
            if deleted {
                self.flags[idx].fetch_or(0x04, Ordering::Release);
            } else {
                self.flags[idx].fetch_and(!0x04, Ordering::Release);
            }
        }
    }

    /// Check if vertex exists (may be deleted/tombstoned)
    pub fn vertex_exists(&self, id: VertexId) -> bool {
        self.vertex_id_to_index(id).is_some()
    }

    /// Check if vertex exists and is not deleted
    pub fn vertex_exists_active(&self, id: VertexId) -> bool {
        self.vertex_id_to_index(id)
            .map(|_| !self.is_deleted(id))
            .unwrap_or(false)
    }

    /// Get an iterator over all vertex IDs (including deleted ones)
    pub fn all_vertices(&self) -> impl Iterator<Item = VertexId> + '_ {
        (0..self.len).map(|i| VertexId((i as u32) + FIRST_NORMAL_VERTEX))
    }
}
