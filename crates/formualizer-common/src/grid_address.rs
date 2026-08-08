//! Owned, binding-neutral spreadsheet addresses.
//!
//! These absolute, 1-based addresses are intended for public reports and
//! binding boundaries. They deliberately differ from engine `CellRef`, which
//! uses a numeric sheet id and packed 0-based coordinates, and from
//! [`crate::SheetCellRef`] / [`crate::SheetRangeRef`], which retain locators,
//! relative anchors, and borrowing lifetimes for parsing and evaluation.

use std::fmt;

use crate::{RangeAddress, SheetAddressError, col_letters_from_1based, format_a1_sheet_name};

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
    /// Construct a validated 1-based address.
    pub fn new(sheet: impl Into<String>, row: u32, column: u32) -> Result<Self, SheetAddressError> {
        if row == 0 || column == 0 {
            return Err(SheetAddressError::ZeroIndex);
        }
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let column = col_letters_from_1based(self.column).map_err(|_| fmt::Error)?;
        write!(
            f,
            "{}!{column}{}",
            format_a1_sheet_name(&self.sheet),
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
    type Error = RangeAddress;

    fn try_from(value: RangeAddress) -> Result<Self, Self::Error> {
        if value.start_row != value.end_row || value.start_col != value.end_col {
            return Err(value);
        }
        Ok(Self {
            sheet: value.sheet,
            row: value.start_row,
            column: value.start_col,
        })
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
    /// Construct an area, rejecting zero coordinates and inverted finite axes.
    pub fn new(
        sheet: impl Into<String>,
        start_row: Option<u32>,
        start_column: Option<u32>,
        end_row: Option<u32>,
        end_column: Option<u32>,
    ) -> Result<Self, SheetAddressError> {
        if [start_row, start_column, end_row, end_column]
            .into_iter()
            .flatten()
            .any(|coordinate| coordinate == 0)
        {
            return Err(SheetAddressError::ZeroIndex);
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
    fn constructors_validate_one_based_ordered_coordinates() {
        assert_eq!(
            CellAddress::new("S", 0, 1).unwrap_err(),
            SheetAddressError::ZeroIndex
        );
        assert_eq!(
            RangeArea::new("S", Some(4), Some(1), Some(3), Some(2)).unwrap_err(),
            SheetAddressError::RangeOrder
        );
        assert_eq!(
            RangeArea::new("S", None, Some(0), None, Some(2)).unwrap_err(),
            SheetAddressError::ZeroIndex
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
        assert!(CellAddress::try_from(RangeAddress::new("S", 1, 1, 2, 1).unwrap()).is_err());
    }
}
