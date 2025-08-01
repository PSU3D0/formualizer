/// Bit-packed coordinate representation for memory efficiency
///
/// Layout (64 bits):
/// [63:44] Reserved (20 bits) - MUST BE ZERO
/// [43:24] Row (20 bits) - 0 to 1,048,575 (zero-based)
/// [23:10] Col (14 bits) - 0 to 16,383 (zero-based)
/// [9:0]   Reserved (10 bits) - MUST BE ZERO
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PackedCoord(u64);

impl PackedCoord {
    pub const INVALID: Self = Self(u64::MAX);
    pub const RESERVED_MASK: u64 = 0xFFFFF00000000000 | 0x3FF; // Bits [63:44] and [9:0]

    /// Creates a new PackedCoord from row and column indices
    ///
    /// # Safety Invariants
    /// - Row must be <= 1,048,575 (20 bits)
    /// - Column must be <= 16,383 (14 bits)
    /// - Reserved bits [63:44] and [9:0] MUST remain zero
    pub fn new(row: u32, col: u32) -> Self {
        assert!(row <= 0x000FFFFF, "Row {} exceeds 20 bits", row);
        assert!(col <= 0x00003FFF, "Col {} exceeds 14 bits", col);
        Self((row as u64) << 24 | (col as u64) << 10)
    }

    #[inline(always)]
    pub fn row(self) -> u32 {
        ((self.0 >> 24) & 0x000FFFFF) as u32
    }

    #[inline(always)]
    pub fn col(self) -> u32 {
        ((self.0 >> 10) & 0x00003FFF) as u32
    }

    #[inline(always)]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn is_valid(self) -> bool {
        self.0 != u64::MAX
    }

    // For serialization - ensures reserved bits are zero
    pub fn normalize(self) -> Self {
        Self(self.0 & !Self::RESERVED_MASK)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packed_coord_roundtrip() {
        let coord = PackedCoord::new(1_048_575, 16_383); // Max Excel coords
        assert_eq!(coord.row(), 1_048_575);
        assert_eq!(coord.col(), 16_383);
        // Row: 0xFFFFF << 24, Col: 0x3FFF << 10
        let expected = (0xFFFFF_u64 << 24) | (0x3FFF_u64 << 10);
        assert_eq!(coord.as_u64(), expected);
    }

    #[test]
    fn test_packed_coord_zero() {
        let coord = PackedCoord::new(0, 0);
        assert_eq!(coord.row(), 0);
        assert_eq!(coord.col(), 0);
        assert_eq!(coord.as_u64(), 0);
    }

    #[test]
    fn test_packed_coord_invalid() {
        let invalid = PackedCoord::INVALID;
        assert!(!invalid.is_valid());
        assert_eq!(invalid.as_u64(), u64::MAX);
    }

    #[test]
    fn test_packed_coord_normalize() {
        let coord = PackedCoord::new(100, 200);
        let normalized = coord.normalize();
        assert_eq!(normalized.row(), 100);
        assert_eq!(normalized.col(), 200);
        // Verify reserved bits are zero
        assert_eq!(normalized.as_u64() & PackedCoord::RESERVED_MASK, 0);
    }

    #[test]
    #[should_panic(expected = "Row 1048576 exceeds 20 bits")]
    fn test_packed_coord_row_overflow() {
        PackedCoord::new(1_048_576, 0); // 2^20
    }

    #[test]
    #[should_panic(expected = "Col 16384 exceeds 14 bits")]
    fn test_packed_coord_col_overflow() {
        PackedCoord::new(0, 16_384); // 2^14
    }
}
