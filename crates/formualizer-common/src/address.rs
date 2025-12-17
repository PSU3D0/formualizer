//! Sheet-scoped reference helpers shared across the workspace.

use std::borrow::Cow;
use std::error::Error;
use std::fmt;

use crate::coord::{A1ParseError, CoordError, RelativeCoord};

/// Stable sheet identifier used across the workspace.
pub type SheetId = u16;

/// Compact, stable packed address for an absolute grid cell: `(SheetId, row0, col0)`.
///
/// This is intended for high-volume, allocation-free data paths (e.g. evaluation deltas,
/// dependency attribution, UI invalidation, FFI).
///
/// Bit layout (low → high):
/// - `row0`: 20 bits (0..=1_048_575)
/// - `col0`: 14 bits (0..=16_383)
/// - `sheet_id`: 16 bits
///
/// This packing is a public contract. Do not change the bit layout without a major
/// version bump.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PackedSheetCell(u64);

impl PackedSheetCell {
    const ROW_BITS: u32 = 20;
    const COL_BITS: u32 = 14;
    const SHEET_BITS: u32 = 16;

    const COL_SHIFT: u32 = Self::ROW_BITS;
    const SHEET_SHIFT: u32 = Self::ROW_BITS + Self::COL_BITS;

    const ROW_MASK: u64 = (1u64 << Self::ROW_BITS) - 1;
    const COL_MASK: u64 = (1u64 << Self::COL_BITS) - 1;
    const SHEET_MASK: u64 = (1u64 << Self::SHEET_BITS) - 1;

    pub const MAX_ROW0: u32 = Self::ROW_MASK as u32;
    pub const MAX_COL0: u32 = Self::COL_MASK as u32;
    const USED_BITS: u32 = Self::ROW_BITS + Self::COL_BITS + Self::SHEET_BITS;
    const USED_MASK: u64 = (1u64 << Self::USED_BITS) - 1;

    /// Construct from a resolved sheet id and 0-based row/col indices.
    ///
    /// Returns `None` if indices exceed Excel's packed bounds.
    pub const fn try_new(sheet_id: SheetId, row0: u32, col0: u32) -> Option<Self> {
        if row0 > Self::MAX_ROW0 || col0 > Self::MAX_COL0 {
            return None;
        }
        let packed = (row0 as u64)
            | ((col0 as u64) << Self::COL_SHIFT)
            | ((sheet_id as u64) << Self::SHEET_SHIFT);
        Some(Self(packed))
    }

    /// Return the packed representation as a `u64` (stable ABI for FFI/serialization).
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Construct from a packed `u64` representation.
    ///
    /// Returns `None` if the upper unused bits are set, or if row/col exceed bounds.
    pub const fn try_from_u64(raw: u64) -> Option<Self> {
        if (raw & !Self::USED_MASK) != 0 {
            return None;
        }
        let row0 = (raw & Self::ROW_MASK) as u32;
        let col0 = ((raw >> Self::COL_SHIFT) & Self::COL_MASK) as u32;
        if row0 > Self::MAX_ROW0 || col0 > Self::MAX_COL0 {
            return None;
        }
        Some(Self(raw))
    }

    /// Construct from Excel-style 1-based row/col indices.
    pub fn try_from_excel_1based(sheet_id: SheetId, row: u32, col: u32) -> Option<Self> {
        let row0 = row.checked_sub(1)?;
        let col0 = col.checked_sub(1)?;
        Self::try_new(sheet_id, row0, col0)
    }

    pub const fn sheet_id(self) -> SheetId {
        ((self.0 >> Self::SHEET_SHIFT) & Self::SHEET_MASK) as SheetId
    }

    pub const fn row0(self) -> u32 {
        (self.0 & Self::ROW_MASK) as u32
    }

    pub const fn col0(self) -> u32 {
        ((self.0 >> Self::COL_SHIFT) & Self::COL_MASK) as u32
    }

    pub const fn to_excel_1based(self) -> (SheetId, u32, u32) {
        (self.sheet_id(), self.row0() + 1, self.col0() + 1)
    }
}

/// Errors that can occur while constructing sheet-scoped references.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SheetAddressError {
    /// Encountered a 0 or underflowed 1-based index when converting to 0-based.
    ZeroIndex,
    /// Start/end coordinates were not ordered (start <= end).
    RangeOrder,
    /// Attempted to combine references with different sheet locators.
    MismatchedSheets,
    /// Requested operation requires a sheet name but only an id/current was supplied.
    MissingSheetName,
    /// Attempted to convert an unbounded range into a bounded representation.
    UnboundedRange,
    /// Wrapped [`CoordError`] that originated from `RelativeCoord`.
    Coord(CoordError),
    /// Wrapped [`A1ParseError`] originating from A1 parsing.
    Parse(A1ParseError),
}

impl fmt::Display for SheetAddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SheetAddressError::ZeroIndex => {
                write!(f, "row and column indices must be 1-based (>= 1)")
            }
            SheetAddressError::RangeOrder => {
                write!(
                    f,
                    "range must be ordered so the start is above/left of the end"
                )
            }
            SheetAddressError::MismatchedSheets => {
                write!(f, "range bounds refer to different sheets")
            }
            SheetAddressError::MissingSheetName => {
                write!(f, "sheet name required to materialise textual address")
            }
            SheetAddressError::UnboundedRange => {
                write!(f, "range requires explicit bounds")
            }
            SheetAddressError::Coord(err) => err.fmt(f),
            SheetAddressError::Parse(err) => err.fmt(f),
        }
    }
}

impl Error for SheetAddressError {}

impl From<CoordError> for SheetAddressError {
    fn from(value: CoordError) -> Self {
        SheetAddressError::Coord(value)
    }
}

impl From<A1ParseError> for SheetAddressError {
    fn from(value: A1ParseError) -> Self {
        SheetAddressError::Parse(value)
    }
}

/// Sheet locator that can carry either a resolved id, a name, or the current sheet.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum SheetLocator<'a> {
    /// Reference is scoped to the sheet containing the formula.
    #[default]
    Current,
    /// Resolved sheet id.
    Id(SheetId),
    /// Unresolved sheet name (borrowed or owned).
    Name(Cow<'a, str>),
}

impl<'a> SheetLocator<'a> {
    /// Construct a locator for the current sheet.
    pub const fn current() -> Self {
        SheetLocator::Current
    }

    /// Construct from a resolved sheet id.
    pub const fn from_id(id: SheetId) -> Self {
        SheetLocator::Id(id)
    }

    /// Construct from a sheet name (borrowed or owned).
    pub fn from_name(name: impl Into<Cow<'a, str>>) -> Self {
        SheetLocator::Name(name.into())
    }

    /// Returns the sheet id if present.
    pub const fn id(&self) -> Option<SheetId> {
        match self {
            SheetLocator::Id(id) => Some(*id),
            SheetLocator::Current | SheetLocator::Name(_) => None,
        }
    }

    /// Returns the sheet name if present.
    pub fn name(&self) -> Option<&str> {
        match self {
            SheetLocator::Name(name) => Some(name.as_ref()),
            SheetLocator::Current | SheetLocator::Id(_) => None,
        }
    }

    /// Returns true if this locator refers to the current sheet.
    pub const fn is_current(&self) -> bool {
        matches!(self, SheetLocator::Current)
    }

    /// Borrow the locator, ensuring any owned name is exposed by reference.
    pub fn as_ref(&self) -> SheetLocator<'_> {
        match self {
            SheetLocator::Current => SheetLocator::Current,
            SheetLocator::Id(id) => SheetLocator::Id(*id),
            SheetLocator::Name(name) => SheetLocator::Name(Cow::Borrowed(name.as_ref())),
        }
    }

    /// Convert the locator into an owned `'static` form.
    pub fn into_owned(self) -> SheetLocator<'static> {
        match self {
            SheetLocator::Current => SheetLocator::Current,
            SheetLocator::Id(id) => SheetLocator::Id(id),
            SheetLocator::Name(name) => SheetLocator::Name(Cow::Owned(name.into_owned())),
        }
    }
}

impl<'a> From<SheetId> for SheetLocator<'a> {
    fn from(value: SheetId) -> Self {
        SheetLocator::from_id(value)
    }
}

impl<'a> From<&'a str> for SheetLocator<'a> {
    fn from(value: &'a str) -> Self {
        SheetLocator::from_name(value)
    }
}

impl<'a> From<String> for SheetLocator<'a> {
    fn from(value: String) -> Self {
        SheetLocator::from_name(value)
    }
}

/// Bound on a single axis (row or column).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct AxisBound {
    /// 0-based index.
    pub index: u32,
    /// True if anchored with '$'.
    pub abs: bool,
}

impl AxisBound {
    pub const fn new(index: u32, abs: bool) -> Self {
        AxisBound { index, abs }
    }

    /// Construct from an Excel 1-based index.
    pub fn from_excel_1based(index: u32, abs: bool) -> Result<Self, SheetAddressError> {
        let index0 = index.checked_sub(1).ok_or(SheetAddressError::ZeroIndex)?;
        Ok(AxisBound::new(index0, abs))
    }

    /// Convert to Excel 1-based index.
    pub const fn to_excel_1based(self) -> u32 {
        self.index + 1
    }
}

/// Sheet-scoped cell reference that retains relative/absolute anchors.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SheetCellRef<'a> {
    pub sheet: SheetLocator<'a>,
    pub coord: RelativeCoord,
}

impl<'a> SheetCellRef<'a> {
    pub const fn new(sheet: SheetLocator<'a>, coord: RelativeCoord) -> Self {
        SheetCellRef { sheet, coord }
    }

    /// Construct from Excel 1-based coordinates with anchor flags.
    pub fn from_excel(
        sheet: SheetLocator<'a>,
        row: u32,
        col: u32,
        row_abs: bool,
        col_abs: bool,
    ) -> Result<Self, SheetAddressError> {
        let row0 = row.checked_sub(1).ok_or(SheetAddressError::ZeroIndex)?;
        let col0 = col.checked_sub(1).ok_or(SheetAddressError::ZeroIndex)?;
        let coord = RelativeCoord::try_new(row0, col0, row_abs, col_abs)?;
        Ok(SheetCellRef::new(sheet, coord))
    }

    /// Parse an A1-style reference for this sheet.
    pub fn try_from_a1(
        sheet: SheetLocator<'a>,
        reference: &str,
    ) -> Result<Self, SheetAddressError> {
        let coord = RelativeCoord::try_from_a1(reference)?;
        Ok(SheetCellRef::new(sheet, coord))
    }

    /// Borrowing variant that preserves the lifetime of the sheet locator.
    pub fn as_ref(&self) -> SheetCellRef<'_> {
        SheetCellRef {
            sheet: self.sheet.as_ref(),
            coord: self.coord,
        }
    }

    /// Convert into an owned `'static` reference.
    pub fn into_owned(self) -> SheetCellRef<'static> {
        SheetCellRef {
            sheet: self.sheet.into_owned(),
            coord: self.coord,
        }
    }
}

/// Sheet-scoped range reference. Bounds are inclusive; None indicates an unbounded side.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SheetRangeRef<'a> {
    pub sheet: SheetLocator<'a>,
    pub start_row: Option<AxisBound>,
    pub start_col: Option<AxisBound>,
    pub end_row: Option<AxisBound>,
    pub end_col: Option<AxisBound>,
}

impl<'a> SheetRangeRef<'a> {
    pub const fn new(
        sheet: SheetLocator<'a>,
        start_row: Option<AxisBound>,
        start_col: Option<AxisBound>,
        end_row: Option<AxisBound>,
        end_col: Option<AxisBound>,
    ) -> Self {
        SheetRangeRef {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
        }
    }

    /// Construct a range from two cell references, ensuring sheet/order validity.
    pub fn from_cells(
        start: SheetCellRef<'a>,
        end: SheetCellRef<'a>,
    ) -> Result<Self, SheetAddressError> {
        if start.sheet != end.sheet {
            return Err(SheetAddressError::MismatchedSheets);
        }
        let sr = AxisBound::new(start.coord.row(), start.coord.row_abs());
        let sc = AxisBound::new(start.coord.col(), start.coord.col_abs());
        let er = AxisBound::new(end.coord.row(), end.coord.row_abs());
        let ec = AxisBound::new(end.coord.col(), end.coord.col_abs());
        SheetRangeRef::from_parts(start.sheet, Some(sr), Some(sc), Some(er), Some(ec))
    }

    /// Construct from Excel 1-based bounds and anchor flags.
    #[allow(clippy::too_many_arguments)]
    pub fn from_excel_rect(
        sheet: SheetLocator<'a>,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
        start_row_abs: bool,
        start_col_abs: bool,
        end_row_abs: bool,
        end_col_abs: bool,
    ) -> Result<Self, SheetAddressError> {
        let sr = AxisBound::from_excel_1based(start_row, start_row_abs)?;
        let sc = AxisBound::from_excel_1based(start_col, start_col_abs)?;
        let er = AxisBound::from_excel_1based(end_row, end_row_abs)?;
        let ec = AxisBound::from_excel_1based(end_col, end_col_abs)?;
        SheetRangeRef::from_parts(sheet, Some(sr), Some(sc), Some(er), Some(ec))
    }

    /// Helper to build a range from raw bounds, validating ordering when bounded.
    pub fn from_parts(
        sheet: SheetLocator<'a>,
        start_row: Option<AxisBound>,
        start_col: Option<AxisBound>,
        end_row: Option<AxisBound>,
        end_col: Option<AxisBound>,
    ) -> Result<Self, SheetAddressError> {
        if let (Some(sr), Some(er)) = (start_row, end_row)
            && sr.index > er.index
        {
            return Err(SheetAddressError::RangeOrder);
        }
        if let (Some(sc), Some(ec)) = (start_col, end_col)
            && sc.index > ec.index
        {
            return Err(SheetAddressError::RangeOrder);
        }
        Ok(SheetRangeRef::new(
            sheet, start_row, start_col, end_row, end_col,
        ))
    }

    /// Borrowing variant preserving the sheet locator lifetime.
    pub fn as_ref(&self) -> SheetRangeRef<'_> {
        SheetRangeRef {
            sheet: self.sheet.as_ref(),
            start_row: self.start_row,
            start_col: self.start_col,
            end_row: self.end_row,
            end_col: self.end_col,
        }
    }

    /// Convert into an owned `'static` range.
    pub fn into_owned(self) -> SheetRangeRef<'static> {
        SheetRangeRef {
            sheet: self.sheet.into_owned(),
            start_row: self.start_row,
            start_col: self.start_col,
            end_row: self.end_row,
            end_col: self.end_col,
        }
    }
}

/// Sheet-scoped grid reference (cell or range).
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SheetRef<'a> {
    Cell(SheetCellRef<'a>),
    Range(SheetRangeRef<'a>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sheet_locator_roundtrip() {
        let loc = SheetLocator::from_id(7);
        assert_eq!(loc.id(), Some(7));
        assert_eq!(loc.name(), None);
        assert_eq!(loc.as_ref(), SheetLocator::Id(7));

        let name = SheetLocator::from_name("Data");
        assert_eq!(name.id(), None);
        assert_eq!(name.name(), Some("Data"));
        let owned = name.clone().into_owned();
        assert_eq!(owned.name(), Some("Data"));
        assert_eq!(name, owned.as_ref());

        let current = SheetLocator::current();
        assert!(current.is_current());
        assert_eq!(current.id(), None);
    }

    #[test]
    fn cell_from_excel_preserves_flags() {
        let a1 = SheetCellRef::from_excel(SheetLocator::from_name("Sheet1"), 1, 1, false, false)
            .expect("valid cell");
        assert_eq!(a1.coord.row(), 0);
        assert_eq!(a1.coord.col(), 0);
        assert!(!a1.coord.row_abs());
        assert!(!a1.coord.col_abs());

        let abs = SheetCellRef::from_excel(SheetLocator::from_name("Sheet1"), 3, 2, true, false)
            .expect("valid absolute cell");
        assert_eq!(abs.coord.row(), 2);
        assert!(abs.coord.row_abs());
        assert!(!abs.coord.col_abs());
    }

    #[test]
    fn cell_from_excel_rejects_zero() {
        let err = SheetCellRef::from_excel(SheetLocator::from_name("Sheet1"), 0, 1, false, false)
            .unwrap_err();
        assert_eq!(err, SheetAddressError::ZeroIndex);
    }

    #[test]
    fn range_from_cells_validates_sheet_and_order() {
        let sheet = SheetLocator::from_name("Sheet1");
        let start = SheetCellRef::try_from_a1(sheet.as_ref(), "A1").unwrap();
        let end = SheetCellRef::try_from_a1(sheet.as_ref(), "$B$3").unwrap();
        let range = SheetRangeRef::from_cells(start.clone(), end.clone()).unwrap();
        assert_eq!(range.start_row.unwrap().index, 0);
        assert_eq!(range.end_row.unwrap().index, 2);

        let other_sheet =
            SheetCellRef::try_from_a1(SheetLocator::from_name("Other"), "C2").unwrap();
        assert_eq!(
            SheetRangeRef::from_cells(start, other_sheet).unwrap_err(),
            SheetAddressError::MismatchedSheets
        );

        let inverted = SheetRangeRef::from_parts(
            SheetLocator::from_name("Sheet1"),
            Some(AxisBound::new(end.coord.row(), end.coord.row_abs())),
            Some(AxisBound::new(end.coord.col(), end.coord.col_abs())),
            Some(AxisBound::new(0, false)),
            Some(AxisBound::new(0, false)),
        );
        assert_eq!(inverted.unwrap_err(), SheetAddressError::RangeOrder);
    }

    #[test]
    fn packed_sheet_cell_roundtrip() {
        let packed = PackedSheetCell::try_new(7, 10, 8).unwrap();
        assert_eq!(packed.sheet_id(), 7);
        assert_eq!(packed.row0(), 10);
        assert_eq!(packed.col0(), 8);
        assert_eq!(packed.to_excel_1based(), (7, 11, 9));
        assert_eq!(
            PackedSheetCell::try_from_excel_1based(7, 11, 9),
            Some(packed)
        );
        assert_eq!(PackedSheetCell::try_from_excel_1based(7, 0, 1), None);
        assert_eq!(PackedSheetCell::try_from_u64(packed.as_u64()), Some(packed));
        assert_eq!(
            PackedSheetCell::try_from_u64(packed.as_u64() | (1u64 << 63)),
            None
        );
    }
}
