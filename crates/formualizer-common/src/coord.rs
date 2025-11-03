//! Compact coordinate representations shared across the engine and bindings.
//!
//! `Coord` encodes an absolute cell position (row, column) in 64 bits with the same
//! limits as Excel: 1,048,576 rows × 16,384 columns. `RelativeCoord` extends that
//! layout with anchor flags that preserve the `$A$1` semantics needed while parsing
//! and adjusting formulas.

use core::fmt;

const ROW_BITS: u32 = 20;
const COL_BITS: u32 = 14;
const ROW_MAX: u32 = (1 << ROW_BITS) - 1;
const COL_MAX: u32 = (1 << COL_BITS) - 1;

const ROW_SHIFT: u32 = 24;
const COL_SHIFT: u32 = 10;

const ROW_MASK: u64 = (ROW_MAX as u64) << ROW_SHIFT;
const COL_MASK: u64 = (COL_MAX as u64) << COL_SHIFT;
const RESERVED_HIGH_MASK: u64 = 0xFFFFF00000000000;
const RESERVED_LOW_MASK: u64 = 0x3FF;

const ROW_ABS_BIT: u64 = 1;
const COL_ABS_BIT: u64 = 1 << 1;
const RELATIVE_RESERVED_LOW_MASK: u64 = RESERVED_LOW_MASK & !((ROW_ABS_BIT | COL_ABS_BIT) as u64);

/// Errors returned when constructing coordinates from unchecked inputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordError {
    RowOverflow(i64),
    ColOverflow(i64),
    NegativeRow(i64),
    NegativeCol(i64),
    ReservedBitsSet(u64),
}

impl fmt::Display for CoordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoordError::RowOverflow(row) => write!(f, "row {row} exceeds {MAX}", MAX = ROW_MAX),
            CoordError::ColOverflow(col) => write!(f, "col {col} exceeds {MAX}", MAX = COL_MAX),
            CoordError::NegativeRow(row) => write!(f, "row {row} is negative"),
            CoordError::NegativeCol(col) => write!(f, "col {col} is negative"),
            CoordError::ReservedBitsSet(bits) => {
                write!(f, "coordinate contains reserved bits: {bits:#x}")
            }
        }
    }
}

/// Absolute grid coordinate (row, column) with Excel-compatible bounds.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Coord(u64);

impl Coord {
    pub const INVALID: Self = Self(u64::MAX);

    const RESERVED_MASK: u64 = RESERVED_HIGH_MASK | RESERVED_LOW_MASK;

    /// Construct a coordinate, panicking if values exceed the supported limits.
    pub fn new(row: u32, col: u32) -> Self {
        assert!(row <= ROW_MAX, "Row {row} exceeds 20 bits");
        assert!(col <= COL_MAX, "Col {col} exceeds 14 bits");
        Self(((row as u64) << ROW_SHIFT) | ((col as u64) << COL_SHIFT))
    }

    /// Construct from Excel 1-based coordinates.
    #[inline(always)]
    pub fn from_excel(row: u32, col: u32) -> Self {
        let row0 = row.saturating_sub(1);
        let col0 = col.saturating_sub(1);
        Self::new(row0, col0)
    }

    /// Fallible constructor that reports overflow rather than panicking.
    pub fn try_new(row: u32, col: u32) -> Result<Self, CoordError> {
        if row > ROW_MAX {
            return Err(CoordError::RowOverflow(row as i64));
        }
        if col > COL_MAX {
            return Err(CoordError::ColOverflow(col as i64));
        }
        Ok(Self::new(row, col))
    }

    /// Reconstruct from a raw packed value, ensuring reserved bits stay zero.
    pub fn from_raw(raw: u64) -> Result<Self, CoordError> {
        if raw == u64::MAX {
            return Ok(Self::INVALID);
        }
        if raw & Self::RESERVED_MASK != 0 {
            return Err(CoordError::ReservedBitsSet(raw & Self::RESERVED_MASK));
        }
        Ok(Self(raw))
    }

    #[inline(always)]
    pub fn row(self) -> u32 {
        ((self.0 & ROW_MASK) >> ROW_SHIFT) as u32
    }

    #[inline(always)]
    pub fn col(self) -> u32 {
        ((self.0 & COL_MASK) >> COL_SHIFT) as u32
    }

    #[inline(always)]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    #[inline(always)]
    pub fn is_valid(self) -> bool {
        self.0 != u64::MAX
    }

    /// Clear any reserved bits in-place. Useful before serialisation.
    #[inline(always)]
    pub fn normalize(self) -> Self {
        Self(self.0 & !Self::RESERVED_MASK)
    }

    /// Convert to a relative coordinate with absolute anchors on both axes.
    #[inline(always)]
    pub fn into_relative(self) -> RelativeCoord {
        RelativeCoord::new(self.row(), self.col(), true, true)
    }
}

impl From<Coord> for (u32, u32) {
    fn from(coord: Coord) -> Self {
        (coord.row(), coord.col())
    }
}

impl TryFrom<(u32, u32)> for Coord {
    type Error = CoordError;

    fn try_from(value: (u32, u32)) -> Result<Self, Self::Error> {
        Self::try_new(value.0, value.1)
    }
}

impl TryFrom<(i64, i64)> for Coord {
    type Error = CoordError;

    fn try_from(value: (i64, i64)) -> Result<Self, Self::Error> {
        let (row, col) = value;
        if row < 0 {
            return Err(CoordError::NegativeRow(row));
        }
        if col < 0 {
            return Err(CoordError::NegativeCol(col));
        }
        let row = row as u32;
        let col = col as u32;
        Self::try_new(row, col)
    }
}

impl From<RelativeCoord> for Coord {
    fn from(value: RelativeCoord) -> Self {
        Self::new(value.row(), value.col())
    }
}

/// Relative coordinate (row, column) with anchor flags.
///
/// Anchor bits mirror Excel semantics:
/// * `row_abs = true` keeps the row fixed during rebasing.
/// * `col_abs = true` keeps the column fixed during rebasing.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct RelativeCoord(u64);

impl RelativeCoord {
    const RESERVED_MASK: u64 = RESERVED_HIGH_MASK | RELATIVE_RESERVED_LOW_MASK;

    pub fn new(row: u32, col: u32, row_abs: bool, col_abs: bool) -> Self {
        assert!(row <= ROW_MAX, "Row {row} exceeds 20 bits");
        assert!(col <= COL_MAX, "Col {col} exceeds 14 bits");
        let mut raw = ((row as u64) << ROW_SHIFT) | ((col as u64) << COL_SHIFT);
        if row_abs {
            raw |= ROW_ABS_BIT;
        }
        if col_abs {
            raw |= COL_ABS_BIT;
        }
        Self(raw)
    }

    pub fn try_new(row: u32, col: u32, row_abs: bool, col_abs: bool) -> Result<Self, CoordError> {
        if row > ROW_MAX {
            return Err(CoordError::RowOverflow(row as i64));
        }
        if col > COL_MAX {
            return Err(CoordError::ColOverflow(col as i64));
        }
        Ok(Self::new(row, col, row_abs, col_abs))
    }

    pub fn from_raw(raw: u64) -> Result<Self, CoordError> {
        if raw & Self::RESERVED_MASK != 0 {
            return Err(CoordError::ReservedBitsSet(raw & Self::RESERVED_MASK));
        }
        Ok(Self(raw))
    }

    #[inline(always)]
    pub fn row(self) -> u32 {
        ((self.0 & ROW_MASK) >> ROW_SHIFT) as u32
    }

    #[inline(always)]
    pub fn col(self) -> u32 {
        ((self.0 & COL_MASK) >> COL_SHIFT) as u32
    }

    #[inline(always)]
    pub fn row_abs(self) -> bool {
        self.0 & ROW_ABS_BIT != 0
    }

    #[inline(always)]
    pub fn col_abs(self) -> bool {
        self.0 & COL_ABS_BIT != 0
    }

    #[inline(always)]
    pub fn with_row_abs(mut self, abs: bool) -> Self {
        if abs {
            self.0 |= ROW_ABS_BIT;
        } else {
            self.0 &= !ROW_ABS_BIT;
        }
        self
    }

    #[inline(always)]
    pub fn with_col_abs(mut self, abs: bool) -> Self {
        if abs {
            self.0 |= COL_ABS_BIT;
        } else {
            self.0 &= !COL_ABS_BIT;
        }
        self
    }

    /// Offset by signed deltas, ignoring anchor flags (matching legacy behaviour).
    #[inline(always)]
    pub fn offset(self, drow: i32, dcol: i32) -> Self {
        let row = ((self.row() as i32) + drow) as u32;
        let col = ((self.col() as i32) + dcol) as u32;
        Self::new(row, col, self.row_abs(), self.col_abs())
    }

    /// Rebase as if the enclosing formula moved from `origin` to `target`.
    #[inline(always)]
    pub fn rebase(self, origin: RelativeCoord, target: RelativeCoord) -> Self {
        let drow = target.row() as i32 - origin.row() as i32;
        let dcol = target.col() as i32 - origin.col() as i32;
        let new_row = if self.row_abs() {
            self.row()
        } else {
            ((self.row() as i32) + drow) as u32
        };
        let new_col = if self.col_abs() {
            self.col()
        } else {
            ((self.col() as i32) + dcol) as u32
        };
        Self::new(new_row, new_col, self.row_abs(), self.col_abs())
    }

    #[inline(always)]
    pub fn into_absolute(self) -> Coord {
        Coord::new(self.row(), self.col())
    }

    #[inline(always)]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn col_to_letters(col: u32) -> String {
        column_to_letters(col)
    }

    pub fn letters_to_col(s: &str) -> Option<u32> {
        letters_to_column_index(s)
    }
}

impl fmt::Display for RelativeCoord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.col_abs() {
            write!(f, "$")?;
        }
        write!(f, "{}", column_to_letters(self.col()))?;
        if self.row_abs() {
            write!(f, "$")?;
        }
        write!(f, "{}", self.row() + 1)
    }
}

impl From<Coord> for RelativeCoord {
    fn from(coord: Coord) -> Self {
        Self::new(coord.row(), coord.col(), true, true)
    }
}

impl TryFrom<(u32, u32, bool, bool)> for RelativeCoord {
    type Error = CoordError;

    fn try_from(value: (u32, u32, bool, bool)) -> Result<Self, Self::Error> {
        Self::try_new(value.0, value.1, value.2, value.3)
    }
}

fn column_to_letters(mut col: u32) -> String {
    let mut buf = Vec::new();
    loop {
        let rem = (col % 26) as u8;
        buf.push(b'A' + rem);
        col /= 26;
        if col == 0 {
            break;
        }
        col -= 1;
    }
    buf.reverse();
    String::from_utf8(buf).expect("only ASCII A-Z")
}

fn letters_to_column_index(s: &str) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut col: u32 = 0;
    for (idx, ch) in s.bytes().enumerate() {
        if !ch.is_ascii_uppercase() {
            return None;
        }
        let val = (ch - b'A') as u32;
        col = col.checked_mul(26)?;
        col = col.checked_add(val)?;
        if idx != s.len() - 1 {
            col = col.checked_add(1)?;
        }
    }
    Some(col)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_roundtrip() {
        let coord = Coord::new(1_048_575, 16_383);
        assert_eq!(coord.row(), 1_048_575);
        assert_eq!(coord.col(), 16_383);
        let expected = (0xFFFFF_u64 << ROW_SHIFT) | (0x3FFF_u64 << COL_SHIFT);
        assert_eq!(coord.as_u64(), expected);
    }

    #[test]
    fn absolute_invalid_const() {
        let invalid = Coord::INVALID;
        assert!(!invalid.is_valid());
        assert_eq!(invalid.as_u64(), u64::MAX);
    }

    #[test]
    fn absolute_try_new() {
        assert!(Coord::try_new(ROW_MAX, COL_MAX).is_ok());
        assert_eq!(
            Coord::try_new(ROW_MAX + 1, 0),
            Err(CoordError::RowOverflow((ROW_MAX + 1) as i64))
        );
        assert_eq!(
            Coord::try_new(0, COL_MAX + 1),
            Err(CoordError::ColOverflow((COL_MAX + 1) as i64))
        );
    }

    #[test]
    fn relative_flags() {
        let coord = RelativeCoord::new(0, 0, true, false);
        assert!(coord.row_abs());
        assert!(!coord.col_abs());
        let toggled = coord.with_col_abs(true);
        assert!(toggled.col_abs());
    }

    #[test]
    fn relative_display() {
        let coord = RelativeCoord::new(5, 27, true, false);
        assert_eq!(coord.to_string(), "$AB6");
        let coord = RelativeCoord::new(0, 0, false, false);
        assert_eq!(coord.to_string(), "A1");
    }

    #[test]
    fn rebase_behaviour() {
        let origin = RelativeCoord::new(0, 0, false, false);
        let target = RelativeCoord::new(1, 1, false, false);
        let formula = RelativeCoord::new(2, 0, false, true);
        let rebased = formula.rebase(origin, target);
        assert_eq!(rebased, RelativeCoord::new(3, 0, false, true));
    }

    #[test]
    fn column_letter_roundtrip() {
        let letters = RelativeCoord::col_to_letters(27);
        assert_eq!(letters, "AB");
        let idx = RelativeCoord::letters_to_col(&letters).unwrap();
        assert_eq!(idx, 27);
        assert!(RelativeCoord::letters_to_col("a1").is_none());
    }
}
