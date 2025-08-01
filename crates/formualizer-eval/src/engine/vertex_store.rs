use super::packed_coord::PackedCoord;
use super::vertex::{VertexId, VertexKind};
use crate::SheetId;
use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vertex_store_allocation() {
        let mut store = VertexStore::new();
        let id = store.allocate(PackedCoord::new(10, 20), 1, 0x01);
        assert_eq!(store.coord(id), PackedCoord::new(10, 20));
        assert_eq!(store.sheet_id(id), 1);
        assert_eq!(store.flags(id), 0x01);
    }

    #[test]
    fn test_vertex_store_grow() {
        let mut store = VertexStore::with_capacity(1000);
        for i in 0..10_000 {
            store.allocate(PackedCoord::new(i, i), 0, 0);
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
        let id = store.allocate(PackedCoord::new(5, 10), 3, 0x03);

        // Test coord access
        assert_eq!(store.coord(id).row(), 5);
        assert_eq!(store.coord(id).col(), 10);

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
    fn test_reserved_vertex_range() {
        let mut store = VertexStore::new();
        // First allocation should be >= FIRST_NORMAL_VERTEX
        let id = store.allocate(PackedCoord::new(0, 0), 0, 0);
        assert!(id.0 >= FIRST_NORMAL_VERTEX);
    }

    #[test]
    fn test_atomic_flag_operations() {
        let mut store = VertexStore::new();
        let id = store.allocate(PackedCoord::new(0, 0), 0, 0);

        // Test atomic flag updates
        store.set_dirty(id, true);
        assert!(store.is_dirty(id));

        store.set_dirty(id, false);
        assert!(!store.is_dirty(id));

        store.set_volatile(id, true);
        assert!(store.is_volatile(id));
    }
}

/// Reserved vertex ID range constants
pub const FIRST_NORMAL_VERTEX: u32 = 1024;
pub const RANGE_VERTEX_START: u32 = 0;
pub const EXTERNAL_VERTEX_START: u32 = 256;

/// Core columnar storage for vertices in Struct-of-Arrays layout
///
/// Memory layout optimized for cache efficiency:
/// - 21B logical per vertex (no struct padding)
/// - Dense columnar arrays for hot data
/// - Atomic flags for lock-free operations
#[repr(C, align(64))]
pub struct VertexStore {
    // Dense columnar arrays - 21B per vertex logical
    coords: Vec<PackedCoord>, // 8B (packed row/col)
    sheet_kind: Vec<u32>,     // 4B (16-bit sheet, 8-bit kind, 8-bit reserved)
    flags: Vec<AtomicU8>,     // 1B (dirty|volatile|deleted|...)
    value_ref: Vec<u32>,      // 4B (2-bit tag, 4-bit error, 26-bit index)
    edge_offset: Vec<u32>,    // 4B (CSR offset)

    // Length tracking
    len: usize,
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

    /// Allocate a new vertex, returning its ID
    /// IDs start at FIRST_NORMAL_VERTEX to reserve 0-1023 for special vertices
    pub fn allocate(&mut self, coord: PackedCoord, sheet: SheetId, flags: u8) -> VertexId {
        let id = VertexId(self.len as u32 + FIRST_NORMAL_VERTEX);
        debug_assert!(id.0 >= FIRST_NORMAL_VERTEX);

        self.coords.push(coord);
        self.sheet_kind.push((sheet as u32) << 16);
        self.flags.push(AtomicU8::new(flags));
        self.value_ref.push(0);
        self.edge_offset.push(0);
        self.len += 1;

        id
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    // Accessors
    #[inline]
    pub fn coord(&self, id: VertexId) -> PackedCoord {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        self.coords[idx]
    }

    #[inline]
    pub fn sheet_id(&self, id: VertexId) -> SheetId {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        (self.sheet_kind[idx] >> 16) as SheetId
    }

    #[inline]
    pub fn kind(&self, id: VertexId) -> VertexKind {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        let tag = ((self.sheet_kind[idx] >> 8) & 0xFF) as u8;
        VertexKind::from_tag(tag)
    }

    #[inline]
    pub fn set_kind(&mut self, id: VertexId, kind: VertexKind) {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        let sheet_bits = self.sheet_kind[idx] & 0xFFFF0000;
        self.sheet_kind[idx] = sheet_bits | ((kind.to_tag() as u32) << 8);
    }

    #[inline]
    pub fn flags(&self, id: VertexId) -> u8 {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        self.flags[idx].load(Ordering::Acquire)
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
    pub fn set_dirty(&self, id: VertexId, dirty: bool) {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        if dirty {
            self.flags[idx].fetch_or(0x01, Ordering::Release);
        } else {
            self.flags[idx].fetch_and(!0x01, Ordering::Release);
        }
    }

    #[inline]
    pub fn set_volatile(&self, id: VertexId, volatile: bool) {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        if volatile {
            self.flags[idx].fetch_or(0x02, Ordering::Release);
        } else {
            self.flags[idx].fetch_and(!0x02, Ordering::Release);
        }
    }

    #[inline]
    pub fn value_ref(&self, id: VertexId) -> u32 {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        self.value_ref[idx]
    }

    #[inline]
    pub fn set_value_ref(&mut self, id: VertexId, value_ref: u32) {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        self.value_ref[idx] = value_ref;
    }

    #[inline]
    pub fn edge_offset(&self, id: VertexId) -> u32 {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        self.edge_offset[idx]
    }

    #[inline]
    pub fn set_edge_offset(&mut self, id: VertexId, offset: u32) {
        let idx = (id.0 - FIRST_NORMAL_VERTEX) as usize;
        self.edge_offset[idx] = offset;
    }
}
