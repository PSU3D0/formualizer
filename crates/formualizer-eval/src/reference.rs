//! Cell, coordinate, and range reference utilities for a spreadsheet engine.
//!
//! ## Design goals
//! * **Compact**: small, `Copy`‑able types (12–16 bytes) that can be placed in large
//!   dependency graphs without GC/heap pressure.
//! * **Excel‑compatible semantics**: four anchoring modes (`A1`, `$A1`, `A$1`, `$A$1`)
//!   plus optional sheet scoping.
//! * **Utility helpers**: rebasing, offsetting, (de)serialising, and pretty `Display`.
//!
//! ----
//!
//! ```text
//! ┌──────────┐    1) Parser/loader creates         ┌─────────────┐
//! │  Coord   │────┐                                 │   CellRef   │
//! └──────────┘    └──────┐      2) Linker inserts ─▶└─────────────┘
//!  row, col, flags        │      SheetId + range
//!                         ▼
//!                ┌────────────────┐   (RangeRef = 2×CellRef)
//!                │ Evaluation IR  │  (row/col absolute, flags dropped)
//!                └────────────────┘
//! ```

use core::fmt;

use crate::engine::sheet_registry::SheetRegistry; // `no_std`‑friendly; swap for `std::fmt` if you prefer
use formualizer_common::{
    ExcelError, ExcelErrorKind, RelativeCoord, SheetCellRef as CommonSheetCellRef,
    SheetId as CommonSheetId, SheetLocator as CommonSheetLocator,
    SheetRangeRef as CommonSheetRangeRef, SheetRef as CommonSheetRef,
};
use formualizer_parse::parser::ReferenceType;

//------------------------------------------------------------------------------
// Shared ref aliases (Phase 3.2 staging)
//------------------------------------------------------------------------------

pub type SharedSheetId = CommonSheetId;
pub type SharedSheetLocator<'a> = CommonSheetLocator<'a>;
pub type SharedCellRef<'a> = CommonSheetCellRef<'a>;
pub type SharedRangeRef<'a> = CommonSheetRangeRef<'a>;
pub type SharedRef<'a> = CommonSheetRef<'a>;

//------------------------------------------------------------------------------
// Coord
//------------------------------------------------------------------------------

/// One 2‑D grid coordinate (row, column) **plus** absolute/relative flags.
///
/// Internally delegates to `RelativeCoord` from `formualizer-common`, adding the
/// historical API surface used throughout the evaluator.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Coord(RelativeCoord);

impl Coord {
    #[inline]
    pub fn new(row: u32, col: u32, row_abs: bool, col_abs: bool) -> Self {
        Self(RelativeCoord::new(row, col, row_abs, col_abs))
    }

    #[inline]
    pub fn from_excel(row: u32, col: u32, row_abs: bool, col_abs: bool) -> Self {
        let row0 = row.saturating_sub(1);
        let col0 = col.saturating_sub(1);
        Self(RelativeCoord::new(row0, col0, row_abs, col_abs))
    }

    #[inline]
    pub fn row(self) -> u32 {
        self.0.row()
    }

    #[inline]
    pub fn col(self) -> u32 {
        self.0.col()
    }

    #[inline]
    pub fn row_abs(self) -> bool {
        self.0.row_abs()
    }

    #[inline]
    pub fn col_abs(self) -> bool {
        self.0.col_abs()
    }

    #[inline]
    pub fn with_row_abs(self, abs: bool) -> Self {
        Self(self.0.with_row_abs(abs))
    }

    #[inline]
    pub fn with_col_abs(self, abs: bool) -> Self {
        Self(self.0.with_col_abs(abs))
    }

    #[inline]
    pub fn offset(self, drow: i32, dcol: i32) -> Self {
        Self(self.0.offset(drow, dcol))
    }

    #[inline]
    pub fn rebase(self, origin: Coord, target: Coord) -> Self {
        Self(self.0.rebase(origin.0, target.0))
    }

    #[inline]
    pub fn into_inner(self) -> RelativeCoord {
        self.0
    }

    pub fn col_to_letters(col: u32) -> String {
        RelativeCoord::col_to_letters(col)
    }

    pub fn letters_to_col(s: &str) -> Option<u32> {
        RelativeCoord::letters_to_col(s)
    }
}

type SheetBounds = (Option<String>, (u32, u32, u32, u32));

/// Combine two references with the range operator ':'
/// Supports combining Cell:Cell, Cell:Range (and Range:Cell), and Range:Range on the same sheet.
/// Returns #REF! for cross-sheet combinations or incompatible shapes.
pub fn combine_references(
    a: &ReferenceType,
    b: &ReferenceType,
) -> Result<ReferenceType, ExcelError> {
    // Extract sheet and bounds as (sheet, (sr, sc, er, ec))
    fn to_bounds(r: &ReferenceType) -> Option<SheetBounds> {
        match r {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => Some((sheet.clone(), (*row, *col, *row, *col))),
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                let (sr, sc, er, ec) = match (start_row, start_col, end_row, end_col) {
                    (Some(sr), Some(sc), Some(er), Some(ec)) => (*sr, *sc, *er, *ec),
                    _ => return None,
                };
                Some((sheet.clone(), (sr, sc, er, ec)))
            }
            _ => None,
        }
    }

    let (sheet_a, (a_sr, a_sc, a_er, a_ec)) = to_bounds(a).ok_or_else(|| {
        ExcelError::new(ExcelErrorKind::Ref).with_message("Unsupported reference for ':'")
    })?;
    let (sheet_b, (b_sr, b_sc, b_er, b_ec)) = to_bounds(b).ok_or_else(|| {
        ExcelError::new(ExcelErrorKind::Ref).with_message("Unsupported reference for ':'")
    })?;

    // Sheets must match (both None or equal Some)
    if sheet_a != sheet_b {
        return Err(ExcelError::new(ExcelErrorKind::Ref)
            .with_message("Cannot combine references across sheets"));
    }

    let sr = a_sr.min(b_sr);
    let sc = a_sc.min(b_sc);
    let er = a_er.max(b_er);
    let ec = a_ec.max(b_ec);

    Ok(ReferenceType::Range {
        sheet: sheet_a,
        start_row: Some(sr),
        start_col: Some(sc),
        end_row: Some(er),
        end_col: Some(ec),
        start_row_abs: false,
        start_col_abs: false,
        end_row_abs: false,
        end_col_abs: false,
    })
}

impl fmt::Display for Coord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.col_abs() {
            write!(f, "$")?;
        }
        write!(f, "{}", Self::col_to_letters(self.col()))?;
        if self.row_abs() {
            write!(f, "$")?;
        }
        // rows are 1‑based in A1 notation
        write!(f, "{}", self.row() + 1)
    }
}

//------------------------------------------------------------------------------
// CellRef
//------------------------------------------------------------------------------

/// Sheet identifier inside a workbook.
///
/// Sheet ids are assigned by the engine/registry and have no sentinel values.
pub type SheetId = u16; // 65,535 sheets should be enough for anyone.

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct CellRef {
    pub sheet_id: SheetId,
    pub coord: Coord,
}

impl CellRef {
    #[inline]
    pub const fn new(sheet_id: SheetId, coord: Coord) -> Self {
        Self { sheet_id, coord }
    }

    #[inline]
    pub fn new_absolute(sheet_id: SheetId, row: u32, col: u32) -> Self {
        Self {
            sheet_id,
            coord: Coord::new(row, col, true, true),
        }
    }

    /// Rebase using underlying `Coord` logic.
    #[inline]
    pub fn rebase(self, origin: Coord, target: Coord) -> Self {
        Self {
            sheet_id: self.sheet_id,
            coord: self.coord.rebase(origin, target),
        }
    }

    #[inline]
    pub fn sheet_name<'a>(&self, sheet_reg: &'a SheetRegistry) -> &'a str {
        sheet_reg.name(self.sheet_id)
    }

    #[inline]
    pub fn to_shared(self) -> SharedCellRef<'static> {
        SharedCellRef::new(
            SharedSheetLocator::Id(self.sheet_id),
            self.coord.into_inner(),
        )
    }

    pub fn try_from_shared(cell: SharedCellRef<'_>) -> Result<Self, ExcelError> {
        let owned = cell.into_owned();
        let sheet_id = match owned.sheet {
            SharedSheetLocator::Id(id) => id,
            _ => return Err(ExcelError::new(ExcelErrorKind::Ref)),
        };
        Ok(Self::new(sheet_id, Coord(owned.coord)))
    }
}

impl fmt::Display for CellRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Always include the sheet id; there is no longer a "current sheet" sentinel.
        write!(f, "Sheet{}!", self.sheet_id)?;
        write!(f, "{}", self.coord)
    }
}

//------------------------------------------------------------------------------
// RangeRef (half‑open range helper)
//------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RangeRef {
    pub start: CellRef,
    pub end: CellRef, // inclusive like Excel: A1:B5 covers both corners
}

impl RangeRef {
    #[inline]
    pub const fn new(start: CellRef, end: CellRef) -> Self {
        Self { start, end }
    }

    pub fn try_to_shared(self) -> Result<SharedRangeRef<'static>, ExcelError> {
        if self.start.sheet_id != self.end.sheet_id {
            return Err(ExcelError::new(ExcelErrorKind::Ref));
        }
        let sheet = SharedSheetLocator::Id(self.start.sheet_id);
        let sr =
            formualizer_common::AxisBound::new(self.start.coord.row(), self.start.coord.row_abs());
        let sc =
            formualizer_common::AxisBound::new(self.start.coord.col(), self.start.coord.col_abs());
        let er = formualizer_common::AxisBound::new(self.end.coord.row(), self.end.coord.row_abs());
        let ec = formualizer_common::AxisBound::new(self.end.coord.col(), self.end.coord.col_abs());
        SharedRangeRef::from_parts(sheet, Some(sr), Some(sc), Some(er), Some(ec))
            .map_err(|_| ExcelError::new(ExcelErrorKind::Ref))
    }

    pub fn try_from_shared(range: SharedRangeRef<'_>) -> Result<Self, ExcelError> {
        let owned = range.into_owned();
        let sheet_id = match owned.sheet {
            SharedSheetLocator::Id(id) => id,
            _ => return Err(ExcelError::new(ExcelErrorKind::Ref)),
        };
        let (sr, sc, er, ec) = match (
            owned.start_row,
            owned.start_col,
            owned.end_row,
            owned.end_col,
        ) {
            (Some(sr), Some(sc), Some(er), Some(ec)) => (sr, sc, er, ec),
            _ => return Err(ExcelError::new(ExcelErrorKind::Ref)),
        };
        let start = CellRef::new(sheet_id, Coord::new(sr.index, sc.index, sr.abs, sc.abs));
        let end = CellRef::new(sheet_id, Coord::new(er.index, ec.index, er.abs, ec.abs));
        Ok(Self::new(start, end))
    }
}

impl fmt::Display for RangeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start.sheet_id == self.end.sheet_id {
            // Single sheet: prefix once
            write!(f, "{}:{}", self.start, self.end.coord)
        } else {
            // Different sheets: print fully.
            write!(f, "{}:{}", self.start, self.end)
        }
    }
}

//------------------------------------------------------------------------------
// Tests
//------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_coord() {
        let c = Coord::new(0, 0, false, false);
        assert_eq!(c.to_string(), "A1");
        let c = Coord::new(7, 27, true, true); // row 8, col 28 == AB
        assert_eq!(c.to_string(), "$AB$8");
    }

    #[test]
    fn test_rebase() {
        let origin = Coord::new(0, 0, false, false);
        let target = Coord::new(1, 1, false, false);
        let formula_coord = Coord::new(2, 0, false, true); // A3 with absolute col
        let rebased = formula_coord.rebase(origin, target);
        // Should move down 1 row, col stays because absolute
        assert_eq!(rebased, Coord::new(3, 0, false, true));
    }

    #[test]
    fn test_range_display() {
        let a1 = CellRef::new(0, Coord::new(0, 0, false, false));
        let b2 = CellRef::new(0, Coord::new(1, 1, false, false));
        let r = RangeRef::new(a1, b2);
        assert_eq!(r.to_string(), "Sheet0!A1:B2");
    }
}
