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

    pub fn width(&self) -> u32 {
        self.end_col - self.start_col + 1
    }

    pub fn height(&self) -> u32 {
        self.end_row - self.start_row + 1
    }

    /// Convert into the richer [`SheetRangeRef`] representation.
    pub fn to_sheet_range(&self) -> SheetRangeRef<'_> {
        let sheet = SheetLocator::from_name(self.sheet.as_str());
        let start_row = Some(AxisBound::new(self.start_row - 1, true));
        let start_col = Some(AxisBound::new(self.start_col - 1, true));
        let end_row = Some(AxisBound::new(self.end_row - 1, true));
        let end_col = Some(AxisBound::new(self.end_col - 1, true));
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
