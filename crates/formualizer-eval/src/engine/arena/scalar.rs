/// Scalar arena for efficient storage of numeric values
/// Stores f64 numbers and i64 integers in separate dense arrays
use std::fmt;

/// Reference to a value in the scalar arena
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ScalarRef {
    /// Bit 31: 0 = float, 1 = integer
    /// Bits 30-0: Index into respective array
    pub raw: u32,
}

impl ScalarRef {
    const TYPE_MASK: u32 = 1 << 31;
    const INDEX_MASK: u32 = !Self::TYPE_MASK;

    fn float(index: u32) -> Self {
        debug_assert!(index <= Self::INDEX_MASK, "Float index overflow");
        Self { raw: index }
    }

    fn integer(index: u32) -> Self {
        debug_assert!(index <= Self::INDEX_MASK, "Integer index overflow");
        Self {
            raw: index | Self::TYPE_MASK,
        }
    }

    pub fn from_integer_index(index: u32) -> Self {
        Self {
            raw: index | Self::TYPE_MASK,
        }
    }

    pub fn from_float_index(index: u32) -> Self {
        Self { raw: index }
    }

    pub fn is_float(self) -> bool {
        self.raw & Self::TYPE_MASK == 0
    }

    pub fn is_integer(self) -> bool {
        !self.is_float()
    }

    fn index(self) -> usize {
        (self.raw & Self::INDEX_MASK) as usize
    }
}

/// Arena for storing scalar values (numbers and integers)
#[derive(Debug)]
pub struct ScalarArena {
    floats: Vec<f64>,
    integers: Vec<i64>,
}

impl ScalarArena {
    pub fn new() -> Self {
        Self {
            floats: Vec::new(),
            integers: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            floats: Vec::with_capacity(cap / 2),
            integers: Vec::with_capacity(cap / 2),
        }
    }

    /// Insert a floating-point number
    pub fn insert_float(&mut self, value: f64) -> ScalarRef {
        let index = self.floats.len() as u32;
        if index > ScalarRef::INDEX_MASK {
            panic!("Scalar arena float overflow: too many values");
        }
        self.floats.push(value);
        ScalarRef::float(index)
    }

    /// Insert an integer
    pub fn insert_integer(&mut self, value: i64) -> ScalarRef {
        let index = self.integers.len() as u32;
        if index > ScalarRef::INDEX_MASK {
            panic!("Scalar arena integer overflow: too many values");
        }
        self.integers.push(value);
        ScalarRef::integer(index)
    }

    /// Get a float value by reference
    #[inline]
    pub fn get_float(&self, r: ScalarRef) -> Option<f64> {
        if r.is_float() {
            self.floats.get(r.index()).copied()
        } else {
            None
        }
    }

    /// Get an integer value by reference
    #[inline]
    pub fn get_integer(&self, r: ScalarRef) -> Option<i64> {
        if r.is_integer() {
            self.integers.get(r.index()).copied()
        } else {
            None
        }
    }

    /// Get any scalar value as f64 (integers are converted)
    #[inline]
    pub fn get_as_float(&self, r: ScalarRef) -> Option<f64> {
        if r.is_float() {
            self.floats.get(r.index()).copied()
        } else {
            self.integers.get(r.index()).map(|i| *i as f64)
        }
    }

    /// Returns the total number of scalars stored
    pub fn len(&self) -> usize {
        self.floats.len() + self.integers.len()
    }

    /// Returns true if the arena is empty
    pub fn is_empty(&self) -> bool {
        self.floats.is_empty() && self.integers.is_empty()
    }

    /// Returns memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.floats.capacity() * std::mem::size_of::<f64>()
            + self.integers.capacity() * std::mem::size_of::<i64>()
    }

    /// Clear all values from the arena
    pub fn clear(&mut self) {
        self.floats.clear();
        self.integers.clear();
    }
}

impl Default for ScalarArena {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ScalarRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_float() {
            write!(f, "Float({})", self.index())
        } else {
            write!(f, "Int({})", self.index())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_arena_float_alloc() {
        let mut arena = ScalarArena::new();
        let ref1 = arena.insert_float(42.0);
        let ref2 = arena.insert_float(std::f64::consts::PI);

        assert_eq!(arena.get_float(ref1), Some(42.0));
        assert_eq!(arena.get_float(ref2), Some(std::f64::consts::PI));
        assert_eq!(arena.len(), 2);
    }

    #[test]
    fn test_scalar_arena_integer_alloc() {
        let mut arena = ScalarArena::new();
        let ref1 = arena.insert_integer(42);
        let ref2 = arena.insert_integer(-100);

        assert_eq!(arena.get_integer(ref1), Some(42));
        assert_eq!(arena.get_integer(ref2), Some(-100));
        assert_eq!(arena.len(), 2);
    }

    #[test]
    fn test_scalar_arena_mixed_types() {
        let mut arena = ScalarArena::new();
        let float_ref = arena.insert_float(std::f64::consts::PI);
        let int_ref = arena.insert_integer(42);

        assert!(float_ref.is_float());
        assert!(!float_ref.is_integer());
        assert!(int_ref.is_integer());
        assert!(!int_ref.is_float());

        assert_eq!(arena.get_float(float_ref), Some(std::f64::consts::PI));
        assert_eq!(arena.get_integer(int_ref), Some(42));

        // Wrong type access returns None
        assert_eq!(arena.get_float(int_ref), None);
        assert_eq!(arena.get_integer(float_ref), None);
    }

    #[test]
    fn test_scalar_arena_capacity() {
        let mut arena = ScalarArena::with_capacity(1000);
        let refs: Vec<_> = (0..10_000)
            .map(|i| {
                if i % 2 == 0 {
                    arena.insert_float(i as f64)
                } else {
                    arena.insert_integer(i as i64)
                }
            })
            .collect();

        // Verify all values retained
        for (i, r) in refs.iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(arena.get_float(*r), Some(i as f64));
            } else {
                assert_eq!(arena.get_integer(*r), Some(i as i64));
            }
        }

        assert_eq!(arena.len(), 10_000);
    }

    #[test]
    fn test_scalar_arena_get_as_float() {
        let mut arena = ScalarArena::new();
        let float_ref = arena.insert_float(std::f64::consts::PI);
        let int_ref = arena.insert_integer(42);

        assert_eq!(arena.get_as_float(float_ref), Some(std::f64::consts::PI));
        assert_eq!(arena.get_as_float(int_ref), Some(42.0));
    }

    #[test]
    fn test_scalar_arena_memory_usage() {
        let mut arena = ScalarArena::new();
        let initial_memory = arena.memory_usage();

        for i in 0..100 {
            arena.insert_float(i as f64);
            arena.insert_integer(i);
        }

        let final_memory = arena.memory_usage();
        assert!(final_memory > initial_memory);

        // Should be at least 200 * 8 bytes for the values
        assert!(final_memory >= 1600);
    }

    #[test]
    fn test_scalar_ref_display() {
        let mut arena = ScalarArena::new();
        let float_ref = arena.insert_float(std::f64::consts::PI);
        let int_ref = arena.insert_integer(42);

        assert_eq!(format!("{float_ref}"), "Float(0)");
        assert_eq!(format!("{int_ref}"), "Int(0)");
    }

    #[test]
    fn test_scalar_arena_clear() {
        let mut arena = ScalarArena::new();
        arena.insert_float(std::f64::consts::PI);
        arena.insert_integer(42);

        assert_eq!(arena.len(), 2);
        arena.clear();
        assert_eq!(arena.len(), 0);
        assert!(arena.is_empty());
    }

    #[test]
    #[should_panic(expected = "Float index overflow")]
    fn test_scalar_arena_float_overflow() {
        // This test is theoretical since we can't actually allocate 2^31 values
        // But we can test the assertion
        let _ = ScalarRef::float(ScalarRef::INDEX_MASK + 1);
    }
}
