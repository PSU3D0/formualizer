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

//------------------------------------------------------------------------------
// Coord
//------------------------------------------------------------------------------

/// One 2‑D grid coordinate (row, column) **plus** absolute/relative flags.
///
/// * `row` and `col` are *zero‑based* indices.
/// * `flags` is a 2‑bit field: `bit0 = row_abs`, `bit1 = col_abs`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Coord {
    pub row: u32,
    pub col: u32,
    flags: u8,
}

impl Coord {
    /// Creates a new coordinate.
    #[inline]
    pub const fn new(row: u32, col: u32, row_abs: bool, col_abs: bool) -> Self {
        let flags = (row_abs as u8) | ((col_abs as u8) << 1);
        Self { row, col, flags }
    }

    /// Absolute/relative accessors.
    #[inline]
    pub const fn row_abs(self) -> bool {
        self.flags & 0b01 != 0
    }
    #[inline]
    pub const fn col_abs(self) -> bool {
        self.flags & 0b10 != 0
    }

    /// Returns a copy with modified row anchor.
    #[inline]
    pub const fn with_row_abs(mut self, abs: bool) -> Self {
        if abs {
            self.flags |= 0b01
        } else {
            self.flags &= !0b01;
        }
        self
    }
    /// Returns a copy with modified col anchor.
    #[inline]
    pub const fn with_col_abs(mut self, abs: bool) -> Self {
        if abs {
            self.flags |= 0b10
        } else {
            self.flags &= !0b10;
        }
        self
    }

    /// Offset by signed deltas *ignoring* anchor flags (internal helper).
    #[inline]
    pub const fn offset(self, drow: i32, dcol: i32) -> Self {
        Self {
            row: ((self.row as i32) + drow) as u32,
            col: ((self.col as i32) + dcol) as u32,
            flags: self.flags,
        }
    }

    /// Re‐base this coordinate as if the *formula containing it* was copied
    /// from `origin` to `target`.
    #[inline]
    pub fn rebase(self, origin: Coord, target: Coord) -> Self {
        let drow = target.row as i32 - origin.row as i32;
        let dcol = target.col as i32 - origin.col as i32;
        let new_row = if self.row_abs() {
            self.row
        } else {
            ((self.row as i32) + drow) as u32
        };
        let new_col = if self.col_abs() {
            self.col
        } else {
            ((self.col as i32) + dcol) as u32
        };
        Self {
            row: new_row,
            col: new_col,
            flags: self.flags,
        }
    }

    // ---- helpers for column letter ↔ index -------------------------------------------------- //

    /// Convert `col` into Excel‑style letters (0‑based ⇒ A, B, …, AA…).
    pub fn col_to_letters(mut col: u32) -> String {
        // worst‑case 16,384 cols ⇒ "XFD" (3 chars); keep 8‑char buffer for safety.
        let mut buf = String::new();
        loop {
            let rem = (col % 26) as u8;
            buf.push(char::from(b'A' + rem));
            col /= 26;
            if col == 0 {
                break;
            }
            col -= 1; // shift because Excel letters are 1‑based internally
        }
        buf.chars().rev().collect()
    }

    /// Convert Excel letters (e.g., "AA") back to 0‑based column index.
    pub fn letters_to_col(s: &str) -> Option<u32> {
        let mut col: u32 = 0;
        for (i, ch) in s.bytes().enumerate() {
            if !ch.is_ascii_uppercase() {
                return None;
            }
            let val = (ch - b'A') as u32;
            col = col * 26 + val;
            if i != s.len() - 1 {
                col += 1; // inverse of the post‑decrement above
            }
        }
        Some(col)
    }
}

impl fmt::Display for Coord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.col_abs() {
            write!(f, "$")?;
        }
        write!(f, "{}", Self::col_to_letters(self.col))?;
        if self.row_abs() {
            write!(f, "$")?;
        }
        // rows are 1‑based in A1 notation
        write!(f, "{}", self.row + 1)
    }
}

//------------------------------------------------------------------------------
// CellRef
//------------------------------------------------------------------------------

/// Sheet identifier inside a workbook.
///
/// A `SheetId` of `0` is a special value representing the sheet
/// that contains the reference itself.
pub type SheetId = u16; // 65,535 sheets should be enough for anyone.

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct CellRef {
    pub sheet_id: SheetId, // 0 == current sheet fast‑path
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
}

impl fmt::Display for CellRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.sheet_id != 0 {
            write!(f, "Sheet{}!", self.sheet_id)?; // caller can map id→name if needed
        }
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
        assert_eq!(r.to_string(), "A1:B2");
    }
}
