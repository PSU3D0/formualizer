use crate::address::{AxisBound, SheetAddressError, SheetLocator, SheetRangeRef};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RangeAddress {
    pub sheet: String,
    pub start_row: u32,
    pub start_col: u32,
    pub end_row: u32,
    pub end_col: u32,
}

impl RangeAddress {
    pub fn new(
        sheet: impl Into<String>,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    ) -> Result<Self, &'static str> {
        if start_row == 0 || start_col == 0 || end_row == 0 || end_col == 0 {
            return Err("Row and column indices must be 1-based");
        }
        if start_row > end_row || start_col > end_col {
            return Err("Range must be ordered: start <= end");
        }
        Ok(Self {
            sheet: sheet.into(),
            start_row,
            start_col,
            end_row,
            end_col,
        })
    }

    /// Validate the invariants enforced by [`RangeAddress::new`].
    ///
    /// [`RangeAddress`] has public fields and derives `Deserialize`, so values
    /// reaching the engine from FFI/JSON/CBOR payloads have *not* necessarily
    /// gone through the checked constructor. Untrusted input should be passed
    /// through this method before use.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.start_row == 0 || self.start_col == 0 || self.end_row == 0 || self.end_col == 0 {
            return Err("Row and column indices must be 1-based");
        }
        if self.start_row > self.end_row || self.start_col > self.end_col {
            return Err("Range must be ordered: start <= end");
        }
        Ok(())
    }

    /// Number of columns spanned by this range.
    ///
    /// Saturating: a malformed (unvalidated) range with `end_col < start_col`
    /// yields 0 rather than underflowing and panicking.
    pub fn width(&self) -> u32 {
        (self.end_col.saturating_sub(self.start_col)).saturating_add(1)
    }

    /// Number of rows spanned by this range. Saturating; see [`Self::width`].
    pub fn height(&self) -> u32 {
        (self.end_row.saturating_sub(self.start_row)).saturating_add(1)
    }

    /// Convert into the richer [`SheetRangeRef`] representation.
    pub fn to_sheet_range(&self) -> SheetRangeRef<'_> {
        let sheet = SheetLocator::from_name(self.sheet.as_str());
        // Saturating conversion to 0-based: a 0 index in an unvalidated range
        // would otherwise underflow to u32::MAX in release builds.
        let start_row = Some(AxisBound::new(self.start_row.saturating_sub(1), true));
        let start_col = Some(AxisBound::new(self.start_col.saturating_sub(1), true));
        let end_row = Some(AxisBound::new(self.end_row.saturating_sub(1), true));
        let end_col = Some(AxisBound::new(self.end_col.saturating_sub(1), true));
        SheetRangeRef::new(sheet, start_row, start_col, end_row, end_col)
    }
}

impl<'a> TryFrom<SheetRangeRef<'a>> for RangeAddress {
    type Error = SheetAddressError;

    fn try_from(value: SheetRangeRef<'a>) -> Result<Self, Self::Error> {
        let sheet = value
            .sheet
            .name()
            .ok_or(SheetAddressError::MissingSheetName)?;
        let (sr, sc, er, ec) = match (
            value.start_row,
            value.start_col,
            value.end_row,
            value.end_col,
        ) {
            (Some(sr), Some(sc), Some(er), Some(ec)) => (sr, sc, er, ec),
            _ => return Err(SheetAddressError::UnboundedRange),
        };
        if sr.index > er.index || sc.index > ec.index {
            return Err(SheetAddressError::RangeOrder);
        }
        Ok(RangeAddress {
            sheet: sheet.to_owned(),
            start_row: sr.to_excel_1based(),
            start_col: sc.to_excel_1based(),
            end_row: er.to_excel_1based(),
            end_col: ec.to_excel_1based(),
        })
    }
}

impl<'a> From<&'a RangeAddress> for SheetRangeRef<'a> {
    fn from(value: &'a RangeAddress) -> Self {
        value.to_sheet_range()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_well_formed_ranges() {
        let range = RangeAddress::new("Sheet1", 1, 1, 3, 4).unwrap();
        assert!(range.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_based_indices() {
        // Fields are public and `Deserialize` is derived, so this state is
        // reachable from untrusted JSON/CBOR without going through `new`.
        let range = RangeAddress {
            sheet: "Sheet1".to_string(),
            start_row: 0,
            start_col: 1,
            end_row: 1,
            end_col: 1,
        };
        assert!(range.validate().is_err());
    }

    #[test]
    fn validate_rejects_inverted_ranges() {
        let range = RangeAddress {
            sheet: "Sheet1".to_string(),
            start_row: 10,
            start_col: 1,
            end_row: 2,
            end_col: 1,
        };
        assert!(range.validate().is_err());
    }

    #[test]
    fn width_and_height_saturate_on_malformed_ranges() {
        // Previously `end - start + 1` underflowed to ~4 billion, which then
        // drove `Vec::with_capacity` and aborted on allocation failure.
        let range = RangeAddress {
            sheet: "Sheet1".to_string(),
            start_row: 10,
            start_col: 10,
            end_row: 2,
            end_col: 2,
        };
        assert_eq!(range.height(), 1);
        assert_eq!(range.width(), 1);
    }

    #[test]
    fn to_sheet_range_saturates_zero_indices() {
        let range = RangeAddress {
            sheet: "Sheet1".to_string(),
            start_row: 0,
            start_col: 0,
            end_row: 0,
            end_col: 0,
        };
        let sheet_range = range.to_sheet_range();
        assert_eq!(sheet_range.start_row.unwrap().index, 0);
        assert_eq!(sheet_range.start_col.unwrap().index, 0);
    }

    #[test]
    fn convert_to_sheet_range() {
        let range = RangeAddress::new("Sheet1", 1, 1, 3, 4).unwrap();
        let sheet_range = range.to_sheet_range();
        assert_eq!(sheet_range.start_col.unwrap().index, 0);
        assert_eq!(sheet_range.end_col.unwrap().index, 3);
        assert_eq!(sheet_range.start_row.unwrap().index, 0);
        assert_eq!(sheet_range.end_row.unwrap().index, 2);
        assert_eq!(sheet_range.sheet.name(), Some("Sheet1"));
        assert!(sheet_range.start_row.unwrap().abs);
        assert!(sheet_range.start_col.unwrap().abs);
    }

    #[test]
    fn convert_from_sheet_range_requires_name() {
        let owned = RangeAddress::new("Sheet1", 2, 2, 2, 5).unwrap();
        let sheet_range = owned.to_sheet_range();
        let reconstructed = RangeAddress::try_from(sheet_range.clone()).unwrap();
        assert_eq!(owned, reconstructed);

        let without_name = SheetRangeRef::new(
            SheetLocator::from_id(3),
            sheet_range.start_row,
            sheet_range.start_col,
            sheet_range.end_row,
            sheet_range.end_col,
        );
        let err = RangeAddress::try_from(without_name).unwrap_err();
        assert_eq!(err, SheetAddressError::MissingSheetName);
    }
}
