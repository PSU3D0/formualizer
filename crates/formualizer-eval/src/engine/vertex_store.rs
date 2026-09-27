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
        let before = (store.cold_rows(), store.flags.len());
        assert_eq!(
            store.try_allocate_batch(&[(grid(0, 0), 0, 0)], &[VertexId(FIRST_NORMAL_VERTEX)],),
            Err(VertexBatchAllocationError::IdExhausted)
        );
        assert_eq!(before, (store.cold_rows(), store.flags.len()));
    }

    #[test]
    fn prepared_batch_reserved_id_mismatch_is_checked_before_mutation() {
        let mut store = VertexStore::new();
        let before = (store.len(), store.cold_rows(), store.flags.len());
        assert_eq!(
            store.try_allocate_batch(&[(grid(0, 0), 0, 0)], &[VertexId(FIRST_NORMAL_VERTEX + 1)],),
            Err(VertexBatchAllocationError::ReservedIdsMismatch)
        );
        assert_eq!(before, (store.len(), store.cold_rows(), store.flags.len()));
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
        assert!(store.cold_capacity() >= 100);
        assert!(store.flags.capacity() >= 100);
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
    fn member_pages_drop_their_rows_and_come_back() {
        let mut store = VertexStore::new();
        // 3000 formula members down column 4 of sheet 2, then one more cell.
        let first = store.allocate(grid(5, 4), 2, 0);
        for r in 6..5 + 3000 {
            store.allocate(grid(r, 4), 2, 0);
        }
        store.allocate(grid(0, 0), 2, 0);
        for i in 0..3000 {
            let v = VertexId(first.0 + i);
            store.set_kind(v, VertexKind::FormulaScalar);
            store.set_edge_offset(v, 2);
        }
        let dense = store.authority_gate_heap_bytes();
        // Ids 1024.. fill pages 0..2 completely; page 2 also holds the cell.
        assert_eq!(store.virtualize_member_span(first, 3000, 2, 4, 5), 2);
        assert_eq!(store.virtual_pages(), 2);
        assert!(store.authority_gate_heap_bytes() < dense);
        for i in [0u32, 1023, 1024, 2047, 2999] {
            let v = VertexId(first.0 + i);
            assert_eq!(store.grid_addr(v), Some(GridAddr::new(5 + i, 4)));
            assert_eq!(store.sheet_id(v), 2);
            assert_eq!(store.kind(v), VertexKind::FormulaScalar);
            assert_eq!(store.edge_offset(v), 2);
            assert_eq!(store.value_ref(v), 0);
        }
        // Materializing one member rebuilds its page, unchanged.
        store.ensure_dense(VertexId(first.0 + 1500), 1);
        assert_eq!(store.virtual_pages(), 1);
        store.set_kind(VertexId(first.0 + 1500), VertexKind::Cell);
        assert_eq!(store.kind(VertexId(first.0 + 1500)), VertexKind::Cell);
        assert_eq!(
            store.grid_addr(VertexId(first.0 + 1501)),
            Some(GridAddr::new(1506, 4))
        );
        assert_eq!(store.edge_offset(VertexId(first.0 + 1501)), 2);
        // A span whose rows disagree (another kind) keeps its page.
        assert_eq!(store.virtualize_member_span(first, 3000, 2, 4, 5), 0);
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

/// Flag bit of a virtual family member (see [`VertexStore::is_virtual`]).
pub(crate) const VIRTUAL_FLAG: u8 = 0x20;

/// Largest vertex id (Program 2): a formula cell's `VertexId` is its
/// authority id, and the authority allocates symbol binding identities
/// from `HOST_SYMBOL_ID_BASE` (2^31) up.
pub const MAX_VERTEX_ID: u32 = crate::engine::authority::identity::HOST_SYMBOL_ID_BASE - 1;
pub const RANGE_VERTEX_START: u32 = 0;
pub const EXTERNAL_VERTEX_START: u32 = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VertexBatchAllocationError {
    IdExhausted,
    ReservedIdsMismatch,
}

/// Vertices per cold page (see [`VertexStore`]).
const COLD_PAGE_BITS: usize = 10;
const COLD_PAGE: usize = 1 << COLD_PAGE_BITS;
const COLD_MASK: usize = COLD_PAGE - 1;

/// The cold columns of one page of vertices.
#[derive(Debug, Default)]
struct ColdCols {
    coords: Vec<VertexAddr>, // 8B (packed grid position or symbol identity)
    sheet_kind: Vec<u32>,    // 4B (16-bit sheet, 8-bit kind, 8-bit reserved)
    value_ref: Vec<u32>,     // 4B (2-bit tag, 4-bit error, 26-bit index)
    edge_offset: Vec<u32>,   // 4B (direct dependency edge count)
}

impl ColdCols {
    fn with_capacity(n: usize) -> Self {
        Self {
            coords: Vec::with_capacity(n),
            sheet_kind: Vec::with_capacity(n),
            value_ref: Vec::with_capacity(n),
            edge_offset: Vec::with_capacity(n),
        }
    }

    fn heap_bytes(&self) -> usize {
        self.coords.capacity() * size_of::<VertexAddr>()
            + (self.sheet_kind.capacity() + self.value_ref.capacity() + self.edge_offset.capacity())
                * 4
    }

    fn shrink_to_fit(&mut self) {
        self.coords.shrink_to_fit();
        self.sheet_kind.shrink_to_fit();
        self.value_ref.shrink_to_fit();
        self.edge_offset.shrink_to_fit();
    }
}

/// A whole page of virtual family members (Program 2): offsets
/// `start..start + len` of the page are rows `row0..` of column `col` on
/// `sheet`, formula vertices with no cached value and `edge_offset` direct
/// edges each. The spans of a virtual page cover it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct VirtualSpan {
    start: u32,
    len: u32,
    sheet: SheetId,
    col: u32,
    row0: u32,
    edge_offset: u32,
}

#[derive(Debug)]
enum ColdPage {
    Dense(ColdCols),
    Virtual(Box<[VirtualSpan]>),
}

impl ColdPage {
    #[inline]
    fn span(spans: &[VirtualSpan], off: usize) -> &VirtualSpan {
        let off = off as u32;
        spans
            .iter()
            .find(|s| off >= s.start && off < s.start + s.len)
            .expect("a virtual page's spans cover it")
    }

    #[inline]
    fn addr(&self, off: usize) -> VertexAddr {
        match self {
            ColdPage::Dense(c) => c.coords[off],
            ColdPage::Virtual(spans) => {
                let s = Self::span(spans, off);
                VertexAddr::grid(GridAddr::new(s.row0 + (off as u32 - s.start), s.col))
            }
        }
    }

    #[inline]
    fn sheet_kind(&self, off: usize) -> u32 {
        match self {
            ColdPage::Dense(c) => c.sheet_kind[off],
            ColdPage::Virtual(spans) => {
                let s = Self::span(spans, off);
                ((s.sheet as u32) << 16) | ((VertexKind::FormulaScalar.to_tag() as u32) << 8)
            }
        }
    }

    #[inline]
    fn value_ref(&self, off: usize) -> u32 {
        match self {
            ColdPage::Dense(c) => c.value_ref[off],
            ColdPage::Virtual(_) => 0,
        }
    }

    #[inline]
    fn edge_offset(&self, off: usize) -> u32 {
        match self {
            ColdPage::Dense(c) => c.edge_offset[off],
            ColdPage::Virtual(spans) => Self::span(spans, off).edge_offset,
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            ColdPage::Dense(c) => c.heap_bytes(),
            ColdPage::Virtual(spans) => spans.len() * size_of::<VirtualSpan>(),
        }
    }
}

/// Core columnar storage for vertices in Struct-of-Arrays layout
///
/// - Flags (1B per vertex, atomic) are one dense column.
/// - The cold columns (position 8B, sheet and kind 4B, value ref 4B, direct
///   edge count 4B) are kept in pages of 1024 vertices. A page whose
///   vertices are all virtual family members (Program 2) keeps no rows: its
///   values are derived from the members' runs (`virtualize_member_span`),
///   and the page is rebuilt when a member is materialized.
#[repr(C, align(64))]
#[derive(Debug)]
pub struct VertexStore {
    pages: Vec<ColdPage>,
    flags: Vec<AtomicU8>, // 1B (dirty|volatile|deleted|dynamic|reads range|virtual)

    // Length tracking
    len: usize,

    /// Some dirty flag was cleared since creation (nothing has been clean
    /// before the first evaluation): lets load skip dependency closures,
    /// which cannot change anything while every formula is dirty.
    dirty_cleared: std::sync::atomic::AtomicBool,
}

impl Default for VertexStore {
    fn default() -> Self {
        Self::new()
    }
}

impl VertexStore {
    pub fn new() -> Self {
        Self {
            pages: Vec::new(),
            flags: Vec::new(),
            len: 0,
            dirty_cleared: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut store = Self::new();
        store.reserve(capacity);
        store
    }

    /// Reserve additional capacity for upcoming vertex allocations.
    pub fn reserve(&mut self, additional: usize) {
        if additional == 0 {
            return;
        }
        let target = self.len + additional;
        if self.flags.capacity() < target {
            self.flags.reserve(additional);
        }
        // The current last page up to its end (later pages are created at
        // full size).
        let page = self.len >> COLD_PAGE_BITS;
        let room = (COLD_PAGE - (self.len & COLD_MASK)).min(additional);
        if let Some(ColdPage::Dense(c)) = self.pages.get_mut(page) {
            let want = (self.len & COLD_MASK) + room;
            if c.coords.capacity() < want {
                let extra = want - c.coords.len();
                c.coords.reserve(extra);
                c.sheet_kind.reserve(extra);
                c.value_ref.reserve(extra);
                c.edge_offset.reserve(extra);
            }
        } else if page == self.pages.len() {
            self.pages
                .push(ColdPage::Dense(ColdCols::with_capacity(room)));
        }
    }

    #[inline]
    fn push_row(&mut self, addr: VertexAddr, sheet: SheetId, flags: u8) {
        let page = self.len >> COLD_PAGE_BITS;
        if page == self.pages.len() {
            // A first page grows with the store; later ones are created at
            // full size (more vertices are coming).
            let cap = if page == 0 { 0 } else { COLD_PAGE };
            self.pages
                .push(ColdPage::Dense(ColdCols::with_capacity(cap)));
        }
        let ColdPage::Dense(c) = &mut self.pages[page] else {
            unreachable!("the page being filled is dense");
        };
        c.coords.push(addr);
        c.sheet_kind.push((u32::from(sheet)) << 16);
        c.value_ref.push(0);
        c.edge_offset.push(0);
        self.flags.push(AtomicU8::new(flags));
        self.len += 1;
    }

    /// Allocate a new vertex, returning its ID
    /// IDs start at FIRST_NORMAL_VERTEX to reserve 0-1023 for special vertices
    pub fn allocate(&mut self, addr: VertexAddr, sheet: SheetId, flags: u8) -> VertexId {
        let id = VertexId(self.len as u32 + FIRST_NORMAL_VERTEX);
        debug_assert!(id.0 >= FIRST_NORMAL_VERTEX);
        assert!(
            id.0 <= MAX_VERTEX_ID,
            "vertex ids exhausted (formula ids share the authority's id space below its symbol range)"
        );
        self.push_row(addr, sheet, flags);
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
            let last = start
                .checked_add(count - 1)
                .ok_or(VertexBatchAllocationError::IdExhausted)?;
            if last > MAX_VERTEX_ID {
                return Err(VertexBatchAllocationError::IdExhausted);
            }
        }
        let ids: Vec<_> = (0..count).map(|offset| VertexId(start + offset)).collect();
        if ids != expected_ids {
            return Err(VertexBatchAllocationError::ReservedIdsMismatch);
        }
        self.reserve(vertices.len());
        for &(addr, sheet, flags) in vertices {
            self.push_row(addr, sheet, flags);
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

    /// Give back the growth slack of every column (end of a bulk load).
    pub(crate) fn shrink_to_fit(&mut self) {
        self.flags.shrink_to_fit();
        self.pages.shrink_to_fit();
        if let Some(ColdPage::Dense(c)) = self.pages.last_mut() {
            c.shrink_to_fit();
        }
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
    fn page(&self, idx: usize) -> &ColdPage {
        &self.pages[idx >> COLD_PAGE_BITS]
    }

    /// The dense columns of `idx`'s page (a virtual page is rebuilt).
    #[inline]
    fn dense_mut(&mut self, idx: usize) -> (&mut ColdCols, usize) {
        let page = idx >> COLD_PAGE_BITS;
        self.densify_page(page);
        let ColdPage::Dense(c) = &mut self.pages[page] else {
            unreachable!("densified");
        };
        (c, idx & COLD_MASK)
    }

    fn densify_page(&mut self, page: usize) {
        let ColdPage::Virtual(spans) = &self.pages[page] else {
            return;
        };
        let spans = spans.clone();
        let mut c = ColdCols::with_capacity(COLD_PAGE);
        let tmp = ColdPage::Virtual(spans);
        for off in 0..COLD_PAGE {
            c.coords.push(tmp.addr(off));
            c.sheet_kind.push(tmp.sheet_kind(off));
            c.value_ref.push(0);
            c.edge_offset.push(tmp.edge_offset(off));
        }
        self.pages[page] = ColdPage::Dense(c);
    }

    /// Give the pages holding vertices `first..first + len` their rows back
    /// (a member of theirs is being materialized).
    pub(crate) fn ensure_dense(&mut self, first: VertexId, len: u32) {
        let Some(a) = self.vertex_id_to_index(first) else {
            return;
        };
        let b = (a + len as usize - 1).min(self.len - 1);
        for page in (a >> COLD_PAGE_BITS)..=(b >> COLD_PAGE_BITS) {
            self.densify_page(page);
        }
    }

    /// Vertices `first..first + len` are virtual family members at rows
    /// `row0..` of `col` on `sheet`: drop the rows of every page they fill
    /// completely whose rows say exactly that (formula kind, no cached
    /// value, one direct-edge count). Returns the pages dropped.
    pub(crate) fn virtualize_member_span(
        &mut self,
        first: VertexId,
        len: u32,
        sheet: SheetId,
        col: u32,
        row0: u32,
    ) -> usize {
        let Some(a) = self.vertex_id_to_index(first) else {
            return 0;
        };
        let end = a + len as usize; // exclusive
        if end > self.len {
            return 0;
        }
        let formula = ((sheet as u32) << 16) | ((VertexKind::FormulaScalar.to_tag() as u32) << 8);
        let mut dropped = 0;
        let first_page = a.div_ceil(COLD_PAGE);
        let last_page_end = end >> COLD_PAGE_BITS; // pages [first_page, last_page_end)
        for page in first_page..last_page_end {
            let ColdPage::Dense(c) = &self.pages[page] else {
                continue;
            };
            if c.coords.len() != COLD_PAGE {
                continue;
            }
            let base = page << COLD_PAGE_BITS;
            let page_row0 = row0 + (base - a) as u32;
            let edges = c.edge_offset[0];
            let exact = (0..COLD_PAGE).all(|off| {
                c.coords[off] == VertexAddr::grid(GridAddr::new(page_row0 + off as u32, col))
                    && c.sheet_kind[off] == formula
                    && c.value_ref[off] == 0
                    && c.edge_offset[off] == edges
            });
            if !exact {
                continue;
            }
            self.pages[page] = ColdPage::Virtual(Box::new([VirtualSpan {
                start: 0,
                len: COLD_PAGE as u32,
                sheet,
                col,
                row0: page_row0,
                edge_offset: edges,
            }]));
            dropped += 1;
        }
        dropped
    }

    /// Virtual family members `first..first + len` moved by `(dr, dc)`
    /// with their run (Program 2 row/column shifts): whole virtual pages
    /// shift their spans, other rows their coordinates. No
    /// materialization: the members stay virtual.
    pub(crate) fn shift_member_addrs(&mut self, first: VertexId, len: u32, dr: i64, dc: i64) {
        let Some(a) = self.vertex_id_to_index(first) else {
            return;
        };
        let end = (a + len as usize).min(self.len);
        let shift = |g: GridAddr| {
            GridAddr::new(
                (i64::from(g.row()) + dr) as u32,
                (i64::from(g.col()) + dc) as u32,
            )
        };
        let mut idx = a;
        while idx < end {
            let page = idx >> COLD_PAGE_BITS;
            let page_start = page << COLD_PAGE_BITS;
            let hi = end.min(page_start + COLD_PAGE);
            let whole = idx == page_start && hi == page_start + COLD_PAGE;
            match &mut self.pages[page] {
                ColdPage::Virtual(spans) if whole => {
                    for sp in spans.iter_mut() {
                        let g = shift(GridAddr::new(sp.row0, sp.col));
                        sp.row0 = g.row();
                        sp.col = g.col();
                    }
                }
                _ => {
                    self.densify_page(page);
                    let ColdPage::Dense(c) = &mut self.pages[page] else {
                        unreachable!("densified");
                    };
                    for off in (idx - page_start)..(hi - page_start) {
                        if let Some(g) = c.coords[off].as_grid() {
                            c.coords[off] = VertexAddr::grid(shift(g));
                        }
                    }
                }
            }
            idx = hi;
        }
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
            self.page(idx).addr(idx & COLD_MASK)
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
            (self.page(idx).sheet_kind(idx & COLD_MASK) >> 16) as SheetId
        } else {
            0 // Default sheet ID for invalid vertices
        }
    }

    #[inline]
    pub fn kind(&self, id: VertexId) -> VertexKind {
        if let Some(idx) = self.vertex_id_to_index(id) {
            let tag = ((self.page(idx).sheet_kind(idx & COLD_MASK) >> 8) & 0xFF) as u8;
            VertexKind::from_tag(tag)
        } else {
            VertexKind::Empty // Default kind for invalid vertices
        }
    }

    #[inline]
    pub fn set_kind(&mut self, id: VertexId, kind: VertexKind) {
        debug_assert!(
            !self.is_virtual(id),
            "virtual family member {id:?} mutated without materializing"
        );
        if let Some(idx) = self.vertex_id_to_index(id) {
            let (c, off) = self.dense_mut(idx);
            let sheet_bits = c.sheet_kind[off] & 0xFFFF0000;
            c.sheet_kind[off] = sheet_bits | ((kind.to_tag() as u32) << 8);
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
    /// Whether any dirty flag was ever cleared (see the field).
    pub(crate) fn dirty_ever_cleared(&self) -> bool {
        self.dirty_cleared.load(Ordering::Relaxed)
    }

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
            let before = self.flags[idx].fetch_and(!0x01, Ordering::Release);
            if before & 0x01 != 0 {
                self.dirty_cleared.store(true, Ordering::Relaxed);
            }
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

    /// The formula reads a compressed range (one legacy kept as range
    /// dependencies instead of expanded edges): flush pending writes before
    /// evaluating it, and the structural-occupancy shortcut.
    #[inline]
    pub fn reads_range(&self, id: VertexId) -> bool {
        self.flags(id) & 0x10 != 0
    }

    #[inline]
    pub fn set_reads_range(&self, id: VertexId, on: bool) {
        if id.0 < FIRST_NORMAL_VERTEX {
            return;
        }
        if let Some(idx) = self.vertex_id_to_index(id) {
            if on {
                self.flags[idx].fetch_or(0x10, Ordering::Release);
            } else {
                self.flags[idx].fetch_and(!0x10, Ordering::Release);
            }
        }
    }

    /// The vertex is a virtual family member (Program 2): its formula and
    /// cell mapping live in a member run, not in the graph's per-cell maps.
    /// Debug builds check that no position, kind or deletion change reaches
    /// such a vertex (the graph materializes it first).
    #[inline]
    pub(crate) fn is_virtual(&self, id: VertexId) -> bool {
        self.flags(id) & VIRTUAL_FLAG != 0
    }

    #[inline]
    pub(crate) fn set_virtual(&self, id: VertexId, on: bool) {
        if let Some(idx) = self.vertex_id_to_index(id) {
            if on {
                self.flags[idx].fetch_or(VIRTUAL_FLAG, Ordering::Release);
            } else {
                self.flags[idx].fetch_and(!VIRTUAL_FLAG, Ordering::Release);
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
            self.page(idx).value_ref(idx & COLD_MASK)
        } else {
            0 // Default value ref for invalid vertices
        }
    }

    #[inline]
    pub fn set_value_ref(&mut self, id: VertexId, value_ref: u32) {
        if let Some(idx) = self.vertex_id_to_index(id) {
            let (c, off) = self.dense_mut(idx);
            c.value_ref[off] = value_ref;
        }
    }

    #[inline]
    pub fn edge_offset(&self, id: VertexId) -> u32 {
        if let Some(idx) = self.vertex_id_to_index(id) {
            self.page(idx).edge_offset(idx & COLD_MASK)
        } else {
            0 // Default edge offset for invalid vertices
        }
    }

    #[inline]
    pub fn set_edge_offset(&mut self, id: VertexId, offset: u32) {
        if let Some(idx) = self.vertex_id_to_index(id) {
            if self.page(idx).edge_offset(idx & COLD_MASK) == offset {
                return;
            }
            let (c, off) = self.dense_mut(idx);
            c.edge_offset[off] = offset;
        }
    }

    /// Update the address of a vertex
    /// # Safety
    /// Caller must ensure CSR edge cache is updated via CsrMutableEdges::update_addr
    #[doc(hidden)]
    pub fn set_addr(&mut self, id: VertexId, addr: VertexAddr) {
        debug_assert!(
            !self.is_virtual(id),
            "virtual family member {id:?} mutated without materializing"
        );
        if let Some(idx) = self.vertex_id_to_index(id) {
            let (c, off) = self.dense_mut(idx);
            c.coords[off] = addr;
        }
    }

    /// Mark vertex as deleted (tombstone strategy)
    pub fn mark_deleted(&self, id: VertexId, deleted: bool) {
        debug_assert!(
            !self.is_virtual(id),
            "virtual family member {id:?} mutated without materializing"
        );
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

/// Heap bytes of the vertex columns (Program 1 memory gate; feature-gated).
impl VertexStore {
    pub(crate) fn authority_gate_heap_bytes(&self) -> usize {
        self.pages.capacity() * size_of::<ColdPage>()
            + self.pages.iter().map(ColdPage::heap_bytes).sum::<usize>()
            + self.flags.capacity()
    }

    #[cfg(test)]
    fn cold_rows(&self) -> usize {
        self.pages
            .iter()
            .map(|p| match p {
                ColdPage::Dense(c) => c.coords.len(),
                ColdPage::Virtual(_) => COLD_PAGE,
            })
            .sum()
    }

    #[cfg(test)]
    fn cold_capacity(&self) -> usize {
        self.pages
            .iter()
            .map(|p| match p {
                ColdPage::Dense(c) => c.coords.capacity(),
                ColdPage::Virtual(_) => COLD_PAGE,
            })
            .sum()
    }

    /// Pages holding no rows (tests, memory probes).
    pub(crate) fn virtual_pages(&self) -> usize {
        self.pages
            .iter()
            .filter(|p| matches!(p, ColdPage::Virtual(_)))
            .count()
    }
}
