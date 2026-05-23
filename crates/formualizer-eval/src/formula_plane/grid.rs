//! Bounded and unbounded grid-shape descriptors for future storage planning.
//!
//! See FORMULA_PLANE_V2.md, VIRTUAL_REFERENCES.md, and PHASE_8_COMPATIBILITY_NOTES.md.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Bounded grid shape in rows and columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct GridShape {
    pub rows: u32,
    pub cols: u32,
}

/// Extent descriptor for a future grid or external range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum GridExtent {
    Bounded(GridShape),
    UnboundedRows { cols: u32 },
    UnboundedCols { rows: u32 },
    Unknown,
}

/// Cardinality descriptor for a future scalar or range reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum RangeCardinality {
    Scalar,
    Bounded { rows: u32, cols: u32 },
    UnboundedRows { cols: u32 },
    UnboundedCols { rows: u32 },
    Unknown,
}
