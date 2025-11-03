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
}
