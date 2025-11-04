//! Sheet-scoped coordinate helpers shared across the engine and bindings.

use std::borrow::Cow;
use std::error::Error;
use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::coord::{A1ParseError, CoordError, RelativeCoord};

/// Stable sheet identifier used across the workspace.
pub type SheetId = u16;

/// Errors that can occur while constructing sheet-scoped addresses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SheetAddressError {
    /// Encountered a 0 or underflowed 1-based index when converting to 0-based.
    ZeroIndex,
    /// Start/end coordinates were not ordered (start <= end).
    RangeOrder,
    /// Attempted to combine addresses with different sheet locators.
    MismatchedSheets,
    /// Requested operation requires a sheet name but only an id was supplied.
    MissingSheetName,
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

/// Sheet locator that can carry either a resolved id or a name.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SheetLocator<'a> {
    Id(SheetId),
    Name(Cow<'a, str>),
}

impl<'a> SheetLocator<'a> {
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
            SheetLocator::Name(_) => None,
        }
    }

    /// Returns the sheet name if present.
    pub fn name(&self) -> Option<&str> {
        match self {
            SheetLocator::Id(_) => None,
            SheetLocator::Name(name) => Some(name.as_ref()),
        }
    }

    /// Borrow the locator, ensuring any owned name is exposed by reference.
    pub fn as_ref(&self) -> SheetLocator<'_> {
        match self {
            SheetLocator::Id(id) => SheetLocator::Id(*id),
            SheetLocator::Name(name) => SheetLocator::Name(Cow::Borrowed(name.as_ref())),
        }
    }

    /// Convert the locator into an owned `'static` form.
    pub fn into_owned(self) -> SheetLocator<'static> {
        match self {
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

/// Sheet-scoped cell reference that retains relative/absolute anchors.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SheetCellAddress<'a> {
    pub sheet: SheetLocator<'a>,
    pub coord: RelativeCoord,
}

impl<'a> SheetCellAddress<'a> {
    pub const fn new(sheet: SheetLocator<'a>, coord: RelativeCoord) -> Self {
        SheetCellAddress { sheet, coord }
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
        Ok(SheetCellAddress::new(sheet, coord))
    }

    /// Parse an A1-style reference for this sheet.
    pub fn try_from_a1(
        sheet: SheetLocator<'a>,
        reference: &str,
    ) -> Result<Self, SheetAddressError> {
        let coord = RelativeCoord::try_from_a1(reference)?;
        Ok(SheetCellAddress::new(sheet, coord))
    }

    /// Borrowing variant that preserves the lifetime of the sheet locator.
    pub fn as_ref(&self) -> SheetCellAddress<'_> {
        SheetCellAddress {
            sheet: self.sheet.as_ref(),
            coord: self.coord,
        }
    }

    /// Convert into an owned `'static` address.
    pub fn into_owned(self) -> SheetCellAddress<'static> {
        SheetCellAddress {
            sheet: self.sheet.into_owned(),
            coord: self.coord,
        }
    }
}

/// Inclusive rectangular range with sheet context.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SheetRangeAddress<'a> {
    pub sheet: SheetLocator<'a>,
    pub start: RelativeCoord,
    pub end: RelativeCoord,
}

impl<'a> SheetRangeAddress<'a> {
    pub const fn new(
        sheet: SheetLocator<'a>,
        start: RelativeCoord,
        end: RelativeCoord,
    ) -> SheetRangeAddress<'a> {
        SheetRangeAddress { sheet, start, end }
    }

    /// Construct a range from two cell addresses, ensuring sheet/order validity.
    pub fn from_cells(
        start: SheetCellAddress<'a>,
        end: SheetCellAddress<'a>,
    ) -> Result<Self, SheetAddressError> {
        if start.sheet != end.sheet {
            return Err(SheetAddressError::MismatchedSheets);
        }
        SheetRangeAddress::from_parts(start.sheet, start.coord, end.coord)
    }

    /// Construct from Excel 1-based bounds and anchor flags.
    pub fn from_excel(
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
        let start = SheetCellAddress::from_excel(
            sheet.as_ref(),
            start_row,
            start_col,
            start_row_abs,
            start_col_abs,
        )?
        .coord;
        let end = SheetCellAddress::from_excel(
            sheet.as_ref(),
            end_row,
            end_col,
            end_row_abs,
            end_col_abs,
        )?
        .coord;
        SheetRangeAddress::from_parts(sheet, start, end)
    }

    /// Helper to build a range from raw coordinates.
    pub fn from_parts(
        sheet: SheetLocator<'a>,
        start: RelativeCoord,
        end: RelativeCoord,
    ) -> Result<Self, SheetAddressError> {
        if start.row() > end.row() || start.col() > end.col() {
            return Err(SheetAddressError::RangeOrder);
        }
        Ok(SheetRangeAddress::new(sheet, start, end))
    }

    /// Borrowing variant preserving the sheet locator lifetime.
    pub fn as_ref(&self) -> SheetRangeAddress<'_> {
        SheetRangeAddress {
            sheet: self.sheet.as_ref(),
            start: self.start,
            end: self.end,
        }
    }

    /// Convert into an owned `'static` range.
    pub fn into_owned(self) -> SheetRangeAddress<'static> {
        SheetRangeAddress {
            sheet: self.sheet.into_owned(),
            start: self.start,
            end: self.end,
        }
    }

    /// Width of the range in cells (inclusive bounds).
    pub fn width(&self) -> u32 {
        self.end.col() - self.start.col() + 1
    }

    /// Height of the range in cells (inclusive bounds).
    pub fn height(&self) -> u32 {
        self.end.row() - self.start.row() + 1
    }

    /// Decompose into top-left and bottom-right cell addresses.
    pub fn bounds(&self) -> (SheetCellAddress<'_>, SheetCellAddress<'_>) {
        (
            SheetCellAddress::new(self.sheet.as_ref(), self.start),
            SheetCellAddress::new(self.sheet.as_ref(), self.end),
        )
    }
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
    }

    #[test]
    fn cell_from_excel_preserves_flags() {
        let a1 =
            SheetCellAddress::from_excel(SheetLocator::from_name("Sheet1"), 1, 1, false, false)
                .expect("valid cell");
        assert_eq!(a1.coord.row(), 0);
        assert_eq!(a1.coord.col(), 0);
        assert!(!a1.coord.row_abs());
        assert!(!a1.coord.col_abs());

        let abs =
            SheetCellAddress::from_excel(SheetLocator::from_name("Sheet1"), 3, 2, true, false)
                .expect("valid absolute cell");
        assert_eq!(abs.coord.row(), 2);
        assert!(abs.coord.row_abs());
        assert!(!abs.coord.col_abs());
    }

    #[test]
    fn cell_from_excel_rejects_zero() {
        let err =
            SheetCellAddress::from_excel(SheetLocator::from_name("Sheet1"), 0, 1, false, false)
                .unwrap_err();
        assert_eq!(err, SheetAddressError::ZeroIndex);
    }

    #[test]
    fn range_from_cells_validates_sheet_and_order() {
        let sheet = SheetLocator::from_name("Sheet1");
        let start = SheetCellAddress::try_from_a1(sheet.as_ref(), "A1").unwrap();
        let end = SheetCellAddress::try_from_a1(sheet.as_ref(), "$B$3").unwrap();
        let range = SheetRangeAddress::from_cells(start.clone(), end.clone()).unwrap();
        assert_eq!(range.width(), 2);
        assert_eq!(range.height(), 3);
        let (tl, br) = range.bounds();
        assert_eq!(tl.coord.to_string(), "A1");
        assert_eq!(br.coord.to_string(), "$B$3");

        let other_sheet = SheetCellAddress::try_from_a1(SheetLocator::from_name("Other"), "C2")
            .expect("valid cell");
        assert_eq!(
            SheetRangeAddress::from_cells(start, other_sheet).unwrap_err(),
            SheetAddressError::MismatchedSheets
        );

        let inverted = SheetRangeAddress::from_parts(
            SheetLocator::from_name("Sheet1"),
            end.coord,
            RelativeCoord::try_from_a1("A1").unwrap(),
        );
        assert_eq!(
            inverted.unwrap_err(),
            SheetAddressError::RangeOrder,
            "start must be above/left of end"
        );
    }
}
