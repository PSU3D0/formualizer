/// Unified value reference type with bit-packed layout
///
/// Bit layout (32 bits):
/// [31:28] Value Type (4 bits)
/// [27:0]  Payload (28 bits)
use std::fmt;

/// Type of value stored in the reference
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Empty = 0,
    SmallInt = 1, // Inline i28
    LargeInt = 2, // Index into scalar arena (integers)
    Number = 3,   // Index into scalar arena (floats)
    String = 4,   // Index into string interner
    Boolean = 5,  // Inline boolean (bit 0)
    Error = 6,    // Error code in bits [27:24]
    Array = 7,    // Index into array arena
    DateTime = 8, // Serial number in scalar arena
    Duration = 9, // Nanoseconds in scalar arena
    Pending = 10, // Pending evaluation marker
    FormulaAst = 11, // Index into AST arena
                  // 12-15 reserved for future use
}

impl ValueType {
    fn from_bits(bits: u8) -> Option<Self> {
        match bits {
            0 => Some(ValueType::Empty),
            1 => Some(ValueType::SmallInt),
            2 => Some(ValueType::LargeInt),
            3 => Some(ValueType::Number),
            4 => Some(ValueType::String),
            5 => Some(ValueType::Boolean),
            6 => Some(ValueType::Error),
            7 => Some(ValueType::Array),
            8 => Some(ValueType::DateTime),
            9 => Some(ValueType::Duration),
            10 => Some(ValueType::Pending),
            11 => Some(ValueType::FormulaAst),
            _ => None,
        }
    }
}

/// Unified reference to any value type
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct ValueRef {
    raw: u32,
}

impl ValueRef {
    // Bit masks
    const TYPE_SHIFT: u32 = 28;
    const TYPE_MASK: u32 = 0xF0000000;
    const PAYLOAD_MASK: u32 = 0x0FFFFFFF;

    // Small int constants
    const SMALL_INT_MAX: i32 = (1 << 27) - 1; // 2^27 - 1
    const SMALL_INT_MIN: i32 = -(1 << 27); // -2^27
    const SMALL_INT_SIGN_BIT: u32 = 1 << 27; // Sign bit for i28

    /// Create an empty/uninitialized reference
    pub const fn empty() -> Self {
        Self { raw: 0 }
    }

    /// Create a reference from raw bits (for testing)
    #[cfg(test)]
    pub const fn from_raw(raw: u32) -> Self {
        Self { raw }
    }

    /// Get the raw bits
    pub const fn as_raw(self) -> u32 {
        self.raw
    }

    /// Get the value type
    pub fn value_type(self) -> ValueType {
        let type_bits = ((self.raw & Self::TYPE_MASK) >> Self::TYPE_SHIFT) as u8;
        ValueType::from_bits(type_bits).unwrap_or(ValueType::Empty)
    }

    /// Get the payload (lower 28 bits)
    fn payload(self) -> u32 {
        self.raw & Self::PAYLOAD_MASK
    }

    /// Check if this is an empty reference
    pub fn is_empty(self) -> bool {
        self.raw == 0
    }

    /// Create a small integer reference (fits in 28 bits with sign)
    pub fn small_int(value: i32) -> Option<Self> {
        if (Self::SMALL_INT_MIN..=Self::SMALL_INT_MAX).contains(&value) {
            // Pack the signed integer into 28 bits
            let payload = (value as u32) & Self::PAYLOAD_MASK;
            Some(Self {
                raw: (ValueType::SmallInt as u32) << Self::TYPE_SHIFT | payload,
            })
        } else {
            None
        }
    }

    /// Extract a small integer value
    pub fn as_small_int(self) -> Option<i32> {
        if self.value_type() == ValueType::SmallInt {
            let payload = self.payload();
            // Sign-extend from 28 bits to 32 bits
            if payload & Self::SMALL_INT_SIGN_BIT != 0 {
                // Negative number - set upper bits
                Some((payload | !Self::PAYLOAD_MASK) as i32)
            } else {
                // Positive number
                Some(payload as i32)
            }
        } else {
            None
        }
    }

    /// Create a boolean reference
    pub fn boolean(value: bool) -> Self {
        Self {
            raw: (ValueType::Boolean as u32) << Self::TYPE_SHIFT | (value as u32),
        }
    }

    /// Extract a boolean value
    pub fn as_boolean(self) -> Option<bool> {
        if self.value_type() == ValueType::Boolean {
            Some(self.payload() & 1 != 0)
        } else {
            None
        }
    }

    /// Create an error reference with an ErrorRef index
    pub fn error(error_ref: u32) -> Self {
        assert!(
            error_ref <= Self::PAYLOAD_MASK,
            "ErrorRef must fit in 28 bits"
        );
        Self {
            raw: (ValueType::Error as u32) << Self::TYPE_SHIFT | error_ref,
        }
    }

    /// Extract an error reference
    pub fn as_error_ref(self) -> Option<u32> {
        if self.value_type() == ValueType::Error {
            Some(self.payload())
        } else {
            None
        }
    }

    /// Create a pending reference
    pub fn pending() -> Self {
        Self {
            raw: (ValueType::Pending as u32) << Self::TYPE_SHIFT,
        }
    }

    /// Create a reference to a large integer in the scalar arena
    pub fn large_int(index: u32) -> Self {
        assert!(index <= Self::PAYLOAD_MASK, "Large int index overflow");
        Self {
            raw: (ValueType::LargeInt as u32) << Self::TYPE_SHIFT | index,
        }
    }

    /// Create a reference to a number in the scalar arena
    pub fn number(index: u32) -> Self {
        assert!(index <= Self::PAYLOAD_MASK, "Number index overflow");
        Self {
            raw: (ValueType::Number as u32) << Self::TYPE_SHIFT | index,
        }
    }

    /// Create a reference to a string in the interner
    pub fn string(index: u32) -> Self {
        assert!(index <= Self::PAYLOAD_MASK, "String index overflow");
        Self {
            raw: (ValueType::String as u32) << Self::TYPE_SHIFT | index,
        }
    }

    /// Create a reference to an array
    pub fn array(index: u32) -> Self {
        assert!(index <= Self::PAYLOAD_MASK, "Array index overflow");
        Self {
            raw: (ValueType::Array as u32) << Self::TYPE_SHIFT | index,
        }
    }

    /// Create a reference to a date/time serial number
    pub fn date_time(index: u32) -> Self {
        assert!(index <= Self::PAYLOAD_MASK, "DateTime index overflow");
        Self {
            raw: (ValueType::DateTime as u32) << Self::TYPE_SHIFT | index,
        }
    }

    /// Create a reference to a duration
    pub fn duration(index: u32) -> Self {
        assert!(index <= Self::PAYLOAD_MASK, "Duration index overflow");
        Self {
            raw: (ValueType::Duration as u32) << Self::TYPE_SHIFT | index,
        }
    }

    /// Create a reference to an AST node
    pub fn formula_ast(index: u32) -> Self {
        assert!(index <= Self::PAYLOAD_MASK, "AST index overflow");
        Self {
            raw: (ValueType::FormulaAst as u32) << Self::TYPE_SHIFT | index,
        }
    }

    /// Get the arena index for types that use external storage
    pub fn arena_index(self) -> Option<u32> {
        match self.value_type() {
            ValueType::LargeInt
            | ValueType::Number
            | ValueType::String
            | ValueType::Array
            | ValueType::DateTime
            | ValueType::Duration
            | ValueType::FormulaAst => Some(self.payload()),
            _ => None,
        }
    }
}

impl fmt::Debug for ValueRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value_type() {
            ValueType::Empty => write!(f, "Empty"),
            ValueType::SmallInt => {
                if let Some(v) = self.as_small_int() {
                    write!(f, "SmallInt({v})")
                } else {
                    write!(f, "SmallInt(?)")
                }
            }
            ValueType::Boolean => {
                if let Some(v) = self.as_boolean() {
                    write!(f, "Boolean({v})")
                } else {
                    write!(f, "Boolean(?)")
                }
            }
            ValueType::Error => {
                if let Some(code) = self.as_error_ref() {
                    write!(f, "Error(code={code})")
                } else {
                    write!(f, "Error(?)")
                }
            }
            ValueType::Pending => write!(f, "Pending"),
            vt => write!(f, "{:?}(idx={})", vt, self.payload()),
        }
    }
}

impl Default for ValueRef {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_ref_empty() {
        let vref = ValueRef::empty();
        assert!(vref.is_empty());
        assert_eq!(vref.value_type(), ValueType::Empty);
        assert_eq!(vref.as_raw(), 0);
    }

    #[test]
    fn test_value_ref_small_int() {
        // Test positive small int
        let vref = ValueRef::small_int(42).unwrap();
        assert_eq!(vref.value_type(), ValueType::SmallInt);
        assert_eq!(vref.as_small_int(), Some(42));

        // Test negative small int
        let vref = ValueRef::small_int(-100).unwrap();
        assert_eq!(vref.as_small_int(), Some(-100));

        // Test boundary values
        let vref = ValueRef::small_int(ValueRef::SMALL_INT_MAX).unwrap();
        assert_eq!(vref.as_small_int(), Some(ValueRef::SMALL_INT_MAX));

        let vref = ValueRef::small_int(ValueRef::SMALL_INT_MIN).unwrap();
        assert_eq!(vref.as_small_int(), Some(ValueRef::SMALL_INT_MIN));

        // Test overflow
        assert!(ValueRef::small_int(ValueRef::SMALL_INT_MAX + 1).is_none());
        assert!(ValueRef::small_int(ValueRef::SMALL_INT_MIN - 1).is_none());
    }

    #[test]
    fn test_value_ref_boolean() {
        let true_ref = ValueRef::boolean(true);
        assert_eq!(true_ref.value_type(), ValueType::Boolean);
        assert_eq!(true_ref.as_boolean(), Some(true));

        let false_ref = ValueRef::boolean(false);
        assert_eq!(false_ref.value_type(), ValueType::Boolean);
        assert_eq!(false_ref.as_boolean(), Some(false));
    }

    #[test]
    fn test_value_ref_error() {
        let error_ref = ValueRef::error(5);
        assert_eq!(error_ref.value_type(), ValueType::Error);
        assert_eq!(error_ref.as_error_ref(), Some(5));

        // Test larger values within 28-bit range
        let error_ref = ValueRef::error(1000);
        assert_eq!(error_ref.as_error_ref(), Some(1000));
    }

    #[test]
    fn test_value_ref_pending() {
        let pending = ValueRef::pending();
        assert_eq!(pending.value_type(), ValueType::Pending);
        assert!(!pending.is_empty());
    }

    #[test]
    fn test_value_ref_arena_types() {
        let large_int = ValueRef::large_int(100);
        assert_eq!(large_int.value_type(), ValueType::LargeInt);
        assert_eq!(large_int.arena_index(), Some(100));

        let number = ValueRef::number(200);
        assert_eq!(number.value_type(), ValueType::Number);
        assert_eq!(number.arena_index(), Some(200));

        let string = ValueRef::string(300);
        assert_eq!(string.value_type(), ValueType::String);
        assert_eq!(string.arena_index(), Some(300));

        let array = ValueRef::array(400);
        assert_eq!(array.value_type(), ValueType::Array);
        assert_eq!(array.arena_index(), Some(400));

        let ast = ValueRef::formula_ast(500);
        assert_eq!(ast.value_type(), ValueType::FormulaAst);
        assert_eq!(ast.arena_index(), Some(500));
    }

    #[test]
    fn test_value_ref_type_checking() {
        let int_ref = ValueRef::small_int(42).unwrap();
        assert!(int_ref.as_boolean().is_none());
        assert!(int_ref.as_error_ref().is_none());

        let bool_ref = ValueRef::boolean(true);
        assert!(bool_ref.as_small_int().is_none());
        assert!(bool_ref.as_error_ref().is_none());
    }

    #[test]
    fn test_value_ref_debug() {
        assert_eq!(format!("{:?}", ValueRef::empty()), "Empty");
        assert_eq!(
            format!("{:?}", ValueRef::small_int(42).unwrap()),
            "SmallInt(42)"
        );
        assert_eq!(format!("{:?}", ValueRef::boolean(true)), "Boolean(true)");
        assert_eq!(format!("{:?}", ValueRef::error(5)), "Error(code=5)");
        assert_eq!(format!("{:?}", ValueRef::pending()), "Pending");
        assert_eq!(format!("{:?}", ValueRef::number(100)), "Number(idx=100)");
    }

    #[test]
    fn test_value_ref_sign_extension() {
        // Test that negative numbers are properly sign-extended
        let neg_one = ValueRef::small_int(-1).unwrap();
        assert_eq!(neg_one.as_small_int(), Some(-1));

        let large_neg = ValueRef::small_int(-1000000).unwrap();
        assert_eq!(large_neg.as_small_int(), Some(-1000000));

        // Test edge case near sign bit
        let near_boundary = ValueRef::small_int((1 << 26) - 1).unwrap();
        assert_eq!(near_boundary.as_small_int(), Some((1 << 26) - 1));
    }

    #[test]
    #[should_panic(expected = "Array index overflow")]
    fn test_value_ref_index_overflow() {
        ValueRef::array(0x10000000); // 28 bits + 1
    }
}
