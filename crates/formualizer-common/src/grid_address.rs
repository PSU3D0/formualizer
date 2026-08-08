//! Owned, binding-neutral spreadsheet addresses.
//!
//! These absolute, 1-based addresses are intended for public reports and
//! binding boundaries. Their public fields form a wire shape that is validated
//! by the checked constructors; direct field construction and serde
//! deserialization are intentionally unvalidated. [`CellAddress`]'s display is
//! total even for such unchecked values.
//!
//! Sheet names carry engine-canonical casing verbatim. Equality and hashing are
//! case-sensitive. Producers must use registry-canonical casing within a report;
//! consumers comparing addresses across sources must normalize names themselves.
//!
//! These types deliberately differ from engine `CellRef`, which uses a numeric
//! sheet id and packed 0-based coordinates, and from [`crate::SheetCellRef`] /
//! [`crate::SheetRangeRef`], which retain locators, relative anchors, and
//! borrowing lifetimes for parsing and evaluation.

use std::fmt;

use crate::{RangeAddress, SheetAddressError, format_a1_sheet_name};

const MAX_ROW: u32 = 1_048_576;
const MAX_COLUMN: u32 = 16_384;

fn validate_row(row: u32) -> Result<(), SheetAddressError> {
    match row {
        0 => Err(SheetAddressError::ZeroIndex),
        1..=MAX_ROW => Ok(()),
        _ => Err(SheetAddressError::RowOutOfBounds),
    }
}

fn validate_column(column: u32) -> Result<(), SheetAddressError> {
    match column {
        0 => Err(SheetAddressError::ZeroIndex),
        1..=MAX_COLUMN => Ok(()),
        _ => Err(SheetAddressError::ColumnOutOfBounds),
    }
}

fn unbounded_column_letters(mut column: u32) -> String {
    let mut reversed = Vec::new();
    while column > 0 {
        column -= 1;
        reversed.push(char::from(b'A' + (column % 26) as u8));
        column /= 26;
    }
    reversed.into_iter().rev().collect()
}

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// An owned, absolute cell address with 1-based coordinates.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CellAddress {
    pub sheet: String,
    pub row: u32,
    pub column: u32,
}

impl CellAddress {
    /// Construct a validated in-grid 1-based address.
    pub fn new(sheet: impl Into<String>, row: u32, column: u32) -> Result<Self, SheetAddressError> {
        validate_row(row)?;
        validate_column(column)?;
        Ok(Self {
            sheet: sheet.into(),
            row,
            column,
        })
    }

    /// Convert this cell to a finite one-cell range.
    pub fn to_finite(&self) -> RangeAddress {
        RangeAddress {
            sheet: self.sheet.clone(),
            start_row: self.row,
            start_col: self.column,
            end_row: self.row,
            end_col: self.column,
        }
    }

    /// Convert a finite one-cell range to a cell address.
    pub fn from_finite(range: &RangeAddress) -> Option<Self> {
        (range.start_row == range.end_row && range.start_col == range.end_col).then(|| Self {
            sheet: range.sheet.clone(),
            row: range.start_row,
            column: range.start_col,
        })
    }
}

impl fmt::Display for CellAddress {
    /// Render a total, sheet-qualified, relative-form A1 label.
    ///
    /// This output omits `$` markers and is not intended for reinsertion into
    /// formula text. Zero coordinates render as `Sheet!#REF!`; positive columns
    /// beyond the spreadsheet grid use unbounded bijective base-26.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sheet = format_a1_sheet_name(&self.sheet);
        if self.row == 0 || self.column == 0 {
            return write!(f, "{sheet}!#REF!");
        }
        write!(
            f,
            "{sheet}!{}{}",
            unbounded_column_letters(self.column),
            self.row
        )
    }
}

impl From<CellAddress> for RangeAddress {
    fn from(value: CellAddress) -> Self {
        Self {
            sheet: value.sheet,
            start_row: value.row,
            start_col: value.column,
            end_row: value.row,
            end_col: value.column,
        }
    }
}

impl TryFrom<RangeAddress> for CellAddress {
    type Error = SheetAddressError;

    fn try_from(value: RangeAddress) -> Result<Self, Self::Error> {
        if value.start_row != value.end_row || value.start_col != value.end_col {
            return Err(SheetAddressError::NonSingleCellRange);
        }
        Self::new(value.sheet, value.start_row, value.start_col)
    }
}

/// An owned, absolute range area with optional, inclusive 1-based bounds.
///
/// `None` represents an open side on that axis.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RangeArea {
    pub sheet: String,
    pub start_row: Option<u32>,
    pub start_column: Option<u32>,
    pub end_row: Option<u32>,
    pub end_column: Option<u32>,
}

impl RangeArea {
    /// Construct an area, rejecting out-of-grid coordinates and inverted finite axes.
    pub fn new(
        sheet: impl Into<String>,
        start_row: Option<u32>,
        start_column: Option<u32>,
        end_row: Option<u32>,
        end_column: Option<u32>,
    ) -> Result<Self, SheetAddressError> {
        for row in [start_row, end_row].into_iter().flatten() {
            validate_row(row)?;
        }
        for column in [start_column, end_column].into_iter().flatten() {
            validate_column(column)?;
        }
        if start_row
            .zip(end_row)
            .is_some_and(|(start, end)| start > end)
            || start_column
                .zip(end_column)
                .is_some_and(|(start, end)| start > end)
        {
            return Err(SheetAddressError::RangeOrder);
        }
        Ok(Self {
            sheet: sheet.into(),
            start_row,
            start_column,
            end_row,
            end_column,
        })
    }

    /// Copy a finite range into the open-area representation.
    pub fn from_finite(range: &RangeAddress) -> Self {
        Self {
            sheet: range.sheet.clone(),
            start_row: Some(range.start_row),
            start_column: Some(range.start_col),
            end_row: Some(range.end_row),
            end_column: Some(range.end_col),
        }
    }

    /// Convert to a finite range when all four bounds are present.
    pub fn to_finite(&self) -> Option<RangeAddress> {
        Some(RangeAddress {
            sheet: self.sheet.clone(),
            start_row: self.start_row?,
            start_col: self.start_column?,
            end_row: self.end_row?,
            end_col: self.end_column?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_display_uses_canonical_sheet_quoting() {
        assert_eq!(
            CellAddress::new("Data", 12, 28).unwrap().to_string(),
            "Data!AB12"
        );
        assert_eq!(
            CellAddress::new("My Sheet", 1, 1).unwrap().to_string(),
            "'My Sheet'!A1"
        );
        assert_eq!(
            CellAddress::new("O'Brien", 2, 3).unwrap().to_string(),
            "'O''Brien'!C2"
        );
        assert_eq!(
            CellAddress::new("TRUE", 3, 2).unwrap().to_string(),
            "'TRUE'!B3"
        );
    }

    #[test]
    fn display_is_total_for_checked_and_unchecked_coordinates() {
        let display = |row, column| {
            CellAddress {
                sheet: "Sheet".to_string(),
                row,
                column,
            }
            .to_string()
        };
        assert_eq!(display(1, 16_384), "Sheet!XFD1");
        assert_eq!(display(1, 16_385), "Sheet!XFE1");
        assert_eq!(display(1, u32::MAX), "Sheet!MWLQKWU1");
        assert_eq!(display(1, 0), "Sheet!#REF!");
        assert_eq!(display(0, 1), "Sheet!#REF!");
        assert_eq!(display(0, 0), "Sheet!#REF!");
    }

    #[test]
    fn constructors_validate_grid_bounds_and_order() {
        assert_eq!(
            CellAddress::new("S", 0, 1).unwrap_err(),
            SheetAddressError::ZeroIndex
        );
        assert_eq!(
            CellAddress::new("S", MAX_ROW + 1, 1).unwrap_err(),
            SheetAddressError::RowOutOfBounds
        );
        assert_eq!(
            CellAddress::new("S", 1, MAX_COLUMN + 1).unwrap_err(),
            SheetAddressError::ColumnOutOfBounds
        );
        assert!(CellAddress::new("S", MAX_ROW, MAX_COLUMN).is_ok());
        assert_eq!(
            RangeArea::new("S", Some(4), Some(1), Some(3), Some(2)).unwrap_err(),
            SheetAddressError::RangeOrder
        );
        assert_eq!(
            RangeArea::new("S", None, Some(0), None, Some(2)).unwrap_err(),
            SheetAddressError::ZeroIndex
        );
        assert_eq!(
            RangeArea::new("S", Some(MAX_ROW + 1), None, None, None).unwrap_err(),
            SheetAddressError::RowOutOfBounds
        );
        assert_eq!(
            RangeArea::new("S", None, None, None, Some(MAX_COLUMN + 1)).unwrap_err(),
            SheetAddressError::ColumnOutOfBounds
        );
        assert!(RangeArea::new("S", None, Some(2), Some(9), None).is_ok());
    }

    #[test]
    fn finite_conversions_round_trip() {
        let finite = RangeAddress::new("S", 2, 3, 5, 7).unwrap();
        assert_eq!(RangeArea::from_finite(&finite).to_finite(), Some(finite));

        let cell = CellAddress::new("S", 8, 9).unwrap();
        assert_eq!(
            CellAddress::from_finite(&cell.to_finite()),
            Some(cell.clone())
        );
        assert_eq!(
            CellAddress::try_from(RangeAddress::from(cell.clone())),
            Ok(cell)
        );
        assert_eq!(
            CellAddress::try_from(RangeAddress::new("S", 1, 1, 2, 1).unwrap()),
            Err(SheetAddressError::NonSingleCellRange)
        );
    }
}
