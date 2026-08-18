//! Vertex address space.
//!
//! The graph holds two disjoint kinds of vertex.
//!
//! * **Grid vertices** — cells, formulas, empty placeholders. Their address *is* their
//!   identity: a `(SheetId, row, col)` position that users can see, reference, and shift
//!   with structural edits.
//! * **Symbol vertices** — defined names, tables, and external sources. They are identified
//!   by name and have no position at all.
//!
//! Before this module existed, both kinds shared one address space: symbols were handed
//! fabricated `(row, col)` coordinates on a real user-visible sheet, and the only thing that
//! kept them from behaving like cells was their deliberate absence from `cell_to_vertex` —
//! a convention any code path could break. Issues #302 and #304 are two paths that broke it.
//!
//! [`GridAddr`] and [`SymbolAddr`] make the distinction a type. [`VertexAddr`] is the tagged
//! union actually stored in the vertex store and the edge coordinate arrays. Structures that
//! are keyed by grid position take a `GridAddr`, which a symbol cannot produce, so inserting
//! a symbol into a grid structure is a compile error rather than a runtime guard.
//!
//! # Representation
//!
//! [`VertexAddr`] is exactly 8 bytes — the same width as the [`AbsCoord`] it replaces. The
//! coordinate encoding saturates rows (20 bits) and columns (14 bits) at Excel's limits but
//! leaves the top 20 bits (`0xFFFFF000_00000000`) reserved and always zero for a real
//! position, with `u64::MAX` already reserved as the invalid sentinel. Symbols live in that
//! niche: bit 63 set with the rest of the reserved field clear. The edge coordinate arrays
//! are `Vec` parallel to adjacency, so widening them to `Option<AbsCoord>` (16 bytes) would
//! double hot memory; using the existing niche keeps the address free.

use formualizer_common::Coord as AbsCoord;
use std::fmt;

/// The reserved high field of a packed coordinate. Zero for every real `(row, col)`.
const RESERVED_HIGH_MASK: u64 = 0xFFFFF000_00000000;

/// Tag written into the reserved high field to mark a symbol address.
const SYMBOL_TAG: u64 = 1 << 63;

/// Payload area available to a symbol address (44 bits; `u32` indices fit trivially).
const SYMBOL_PAYLOAD_MASK: u64 = !RESERVED_HIGH_MASK;

/// A real grid position.
///
/// Only vertices that live on a sheet's grid have one. Grid-keyed structures take this
/// type so a symbol cannot be inserted into them.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct GridAddr(AbsCoord);

impl Default for GridAddr {
    #[inline]
    fn default() -> Self {
        Self::new(0, 0)
    }
}

impl Ord for GridAddr {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.row(), self.col()).cmp(&(other.row(), other.col()))
    }
}

impl PartialOrd for GridAddr {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl GridAddr {
    /// Construct from zero-based row/column, panicking beyond Excel's limits.
    #[inline]
    pub fn new(row: u32, col: u32) -> Self {
        Self(AbsCoord::new(row, col))
    }

    /// Wrap an already-packed coordinate.
    #[inline]
    pub const fn from_coord(coord: AbsCoord) -> Self {
        Self(coord)
    }

    /// The packed coordinate.
    #[inline]
    pub const fn coord(self) -> AbsCoord {
        self.0
    }

    #[inline]
    pub fn row(self) -> u32 {
        self.0.row()
    }

    #[inline]
    pub fn col(self) -> u32 {
        self.0.col()
    }
}

impl From<AbsCoord> for GridAddr {
    #[inline]
    fn from(coord: AbsCoord) -> Self {
        Self(coord)
    }
}

impl From<GridAddr> for AbsCoord {
    #[inline]
    fn from(addr: GridAddr) -> Self {
        addr.0
    }
}

/// A dense symbol identity.
///
/// Symbols have no position. The index is an allocation counter and carries no meaning
/// beyond distinguishing one symbol vertex from another; nothing may derive a row, a
/// column, or a sheet from it.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SymbolAddr(u32);

impl SymbolAddr {
    #[inline]
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    #[inline]
    pub const fn index(self) -> u32 {
        self.0
    }
}

/// The address of a vertex: a grid position or a symbol identity, in 8 bytes.
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct VertexAddr(u64);

impl VertexAddr {
    /// The invalid sentinel. Neither a grid position nor a symbol.
    pub const INVALID: Self = Self(u64::MAX);

    /// Address of a vertex that occupies a grid position.
    #[inline]
    pub fn grid(addr: GridAddr) -> Self {
        Self(addr.0.as_u64())
    }

    /// Address of a vertex identified by name rather than position.
    #[inline]
    pub const fn symbol(addr: SymbolAddr) -> Self {
        Self(SYMBOL_TAG | (addr.0 as u64))
    }

    /// The grid position, or `None` for a symbol (or the invalid sentinel).
    ///
    /// This is the only route from a stored vertex address to a `(row, col)`, which is what
    /// keeps symbols out of grid-keyed structures.
    #[inline]
    pub fn as_grid(self) -> Option<GridAddr> {
        (self.0 & RESERVED_HIGH_MASK == 0).then(|| GridAddr(unsafe_coord_from_raw(self.0)))
    }

    /// The symbol identity, or `None` for a grid position.
    #[inline]
    pub fn as_symbol(self) -> Option<SymbolAddr> {
        (self.0 & RESERVED_HIGH_MASK == SYMBOL_TAG)
            .then_some(SymbolAddr((self.0 & SYMBOL_PAYLOAD_MASK) as u32))
    }

    #[inline]
    pub fn is_symbol(self) -> bool {
        self.0 & RESERVED_HIGH_MASK == SYMBOL_TAG
    }

    #[inline]
    pub fn is_grid(self) -> bool {
        self.0 & RESERVED_HIGH_MASK == 0
    }

    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Total order used to keep edge lists deterministic.
    ///
    /// Grid vertices order by `(row, col)` exactly as they did when the arrays held bare
    /// coordinates. Symbols have no position, so they sort after every grid vertex, by
    /// allocation index.
    #[inline]
    pub fn order_key(self) -> (u32, u32) {
        match (self.as_grid(), self.as_symbol()) {
            (Some(grid), _) => (grid.row(), grid.col()),
            (_, Some(symbol)) => (u32::MAX, symbol.index()),
            _ => (u32::MAX, u32::MAX),
        }
    }
}

/// Rebuild a `Coord` from raw bits already known to have a clear reserved field.
#[inline]
fn unsafe_coord_from_raw(raw: u64) -> AbsCoord {
    debug_assert!(raw & RESERVED_HIGH_MASK == 0);
    // `Coord::from_raw` rejects reserved low bits too; a stored grid address never has them,
    // and falling back to the packed row/col keeps this total rather than panicking.
    AbsCoord::from_raw(raw).unwrap_or_else(|_| AbsCoord::new(0, 0))
}

impl From<GridAddr> for VertexAddr {
    #[inline]
    fn from(addr: GridAddr) -> Self {
        Self::grid(addr)
    }
}

impl From<SymbolAddr> for VertexAddr {
    #[inline]
    fn from(addr: SymbolAddr) -> Self {
        Self::symbol(addr)
    }
}

impl From<AbsCoord> for VertexAddr {
    #[inline]
    fn from(coord: AbsCoord) -> Self {
        Self::grid(GridAddr(coord))
    }
}

impl Default for VertexAddr {
    #[inline]
    fn default() -> Self {
        Self::grid(GridAddr::default())
    }
}

impl fmt::Debug for VertexAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(grid) = self.as_grid() {
            write!(f, "Grid(r{}, c{})", grid.row(), grid.col())
        } else if let Some(symbol) = self.as_symbol() {
            write!(f, "Symbol({})", symbol.index())
        } else {
            write!(f, "VertexAddr::INVALID")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vertex_addr_is_eight_bytes() {
        assert_eq!(std::mem::size_of::<VertexAddr>(), 8);
        assert_eq!(std::mem::size_of::<VertexAddr>(), size_of::<AbsCoord>());
        assert_eq!(std::mem::align_of::<VertexAddr>(), align_of::<AbsCoord>());
        assert_eq!(size_of::<GridAddr>(), 8);
    }

    #[test]
    fn grid_addresses_round_trip_and_are_never_symbols() {
        for (row, col) in [(0, 0), (1, 1), (1_048_575, 16_383), (7, 0), (0, 16_383)] {
            let addr = VertexAddr::grid(GridAddr::new(row, col));
            assert!(addr.is_grid());
            assert!(!addr.is_symbol());
            assert_eq!(addr.as_symbol(), None);
            let grid = addr.as_grid().expect("grid address must decode");
            assert_eq!((grid.row(), grid.col()), (row, col));
            assert_eq!(addr.order_key(), (row, col));
        }
    }

    #[test]
    fn symbol_addresses_round_trip_and_are_never_grid() {
        for index in [0u32, 1, 16_384, u32::MAX] {
            let addr = VertexAddr::symbol(SymbolAddr::new(index));
            assert!(addr.is_symbol());
            assert!(!addr.is_grid());
            assert_eq!(addr.as_grid(), None);
            assert_eq!(addr.as_symbol(), Some(SymbolAddr::new(index)));
            assert_eq!(addr.order_key(), (u32::MAX, index));
        }
    }

    #[test]
    fn invalid_sentinel_is_neither_grid_nor_symbol() {
        assert!(!VertexAddr::INVALID.is_grid());
        assert!(!VertexAddr::INVALID.is_symbol());
        assert_eq!(VertexAddr::INVALID.as_grid(), None);
        assert_eq!(VertexAddr::INVALID.as_symbol(), None);
    }

    #[test]
    fn symbols_order_after_every_grid_position() {
        let last_cell = VertexAddr::grid(GridAddr::new(1_048_575, 16_383));
        let first_symbol = VertexAddr::symbol(SymbolAddr::new(0));
        assert!(last_cell.order_key() < first_symbol.order_key());
    }
}
