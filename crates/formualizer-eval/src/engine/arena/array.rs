/// Array arena for efficient storage of 2D arrays
/// Arrays are stored in flattened form with separate dimension tracking
use super::value_ref::ValueRef;
use std::fmt;

/// Reference to an array in the arena
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ArrayRef(pub u32);

impl ArrayRef {
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ArrayRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArrayRef({})", self.0)
    }
}

/// Arena for storing 2D arrays
#[derive(Debug)]
pub struct ArrayArena {
    /// Array dimensions (rows, cols)
    dimensions: Vec<(u32, u32)>,
    /// Flattened array elements (all arrays concatenated)
    elements: Vec<ValueRef>,
    /// Offset into elements for each array
    offsets: Vec<u32>,
}

impl ArrayArena {
    pub fn new() -> Self {
        Self {
            dimensions: Vec::new(),
            elements: Vec::new(),
            offsets: vec![0], // Start with offset 0
        }
    }

    pub fn with_capacity(array_count: usize) -> Self {
        Self {
            dimensions: Vec::with_capacity(array_count),
            elements: Vec::with_capacity(array_count * 10), // Assume avg 10 elements per array
            offsets: {
                let mut v = Vec::with_capacity(array_count + 1);
                v.push(0);
                v
            },
        }
    }

    /// Insert a 2D array, returning its reference
    pub fn insert(&mut self, rows: u32, cols: u32, elements: Vec<ValueRef>) -> ArrayRef {
        let expected_len = (rows * cols) as usize;
        assert_eq!(
            elements.len(),
            expected_len,
            "Array dimensions {}x{} don't match element count {}",
            rows,
            cols,
            elements.len()
        );

        let index = self.dimensions.len() as u32;
        if index == u32::MAX {
            panic!("Array arena overflow: too many arrays");
        }

        // Store dimensions
        self.dimensions.push((rows, cols));

        // Store elements
        self.elements.extend(elements);

        // Update offsets
        let new_offset = self.elements.len() as u32;
        self.offsets.push(new_offset);

        ArrayRef(index)
    }

    /// Insert a 2D array from nested vectors
    pub fn insert_2d(&mut self, array: Vec<Vec<ValueRef>>) -> ArrayRef {
        if array.is_empty() {
            return self.insert(0, 0, Vec::new());
        }

        let rows = array.len() as u32;
        let cols = array[0].len() as u32;

        // Verify all rows have same length
        for (i, row) in array.iter().enumerate() {
            assert_eq!(
                row.len(),
                cols as usize,
                "Row {} has {} columns, expected {}",
                i,
                row.len(),
                cols
            );
        }

        // Flatten the array
        let elements: Vec<ValueRef> = array.into_iter().flatten().collect();
        self.insert(rows, cols, elements)
    }

    /// Get array dimensions
    #[inline]
    pub fn dimensions(&self, r: ArrayRef) -> Option<(u32, u32)> {
        self.dimensions.get(r.0 as usize).copied()
    }

    /// Get array elements as a slice
    #[inline]
    pub fn elements(&self, r: ArrayRef) -> Option<&[ValueRef]> {
        let index = r.0 as usize;
        if index >= self.dimensions.len() {
            return None;
        }

        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        Some(&self.elements[start..end])
    }

    /// Get a specific element from an array
    #[inline]
    pub fn get_element(&self, r: ArrayRef, row: u32, col: u32) -> Option<ValueRef> {
        let (rows, cols) = self.dimensions(r)?;
        if row >= rows || col >= cols {
            return None;
        }

        let elements = self.elements(r)?;
        let index = (row * cols + col) as usize;
        elements.get(index).copied()
    }

    /// Reconstruct a 2D array from its reference
    pub fn get_2d(&self, r: ArrayRef) -> Option<Vec<Vec<ValueRef>>> {
        let (rows, cols) = self.dimensions(r)?;
        let elements = self.elements(r)?;

        if rows == 0 || cols == 0 {
            return Some(Vec::new());
        }

        let mut result = Vec::with_capacity(rows as usize);
        for row in 0..rows {
            let start = (row * cols) as usize;
            let end = start + cols as usize;
            result.push(elements[start..end].to_vec());
        }

        Some(result)
    }

    /// Returns the number of arrays stored
    pub fn len(&self) -> usize {
        self.dimensions.len()
    }

    /// Returns true if the arena is empty
    pub fn is_empty(&self) -> bool {
        self.dimensions.is_empty()
    }

    /// Returns the total number of elements across all arrays
    pub fn total_elements(&self) -> usize {
        self.elements.len()
    }

    /// Returns memory usage in bytes (approximate)
    pub fn memory_usage(&self) -> usize {
        self.dimensions.capacity() * std::mem::size_of::<(u32, u32)>()
            + self.elements.capacity() * std::mem::size_of::<ValueRef>()
            + self.offsets.capacity() * std::mem::size_of::<u32>()
    }

    /// Clear all arrays from the arena
    pub fn clear(&mut self) {
        self.dimensions.clear();
        self.elements.clear();
        self.offsets.clear();
        self.offsets.push(0);
    }
}

impl Default for ArrayArena {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_arena_basic() {
        let mut arena = ArrayArena::new();

        // Create dummy ValueRefs for testing
        let elements = vec![
            ValueRef::from_raw(1),
            ValueRef::from_raw(2),
            ValueRef::from_raw(3),
            ValueRef::from_raw(4),
        ];

        let array_ref = arena.insert(2, 2, elements);

        assert_eq!(arena.dimensions(array_ref), Some((2, 2)));
        assert_eq!(arena.len(), 1);
        assert_eq!(arena.total_elements(), 4);
    }

    #[test]
    fn test_array_arena_2d_insert() {
        let mut arena = ArrayArena::new();

        let array = vec![
            vec![
                ValueRef::from_raw(1),
                ValueRef::from_raw(2),
                ValueRef::from_raw(3),
            ],
            vec![
                ValueRef::from_raw(4),
                ValueRef::from_raw(5),
                ValueRef::from_raw(6),
            ],
        ];

        let array_ref = arena.insert_2d(array);

        assert_eq!(arena.dimensions(array_ref), Some((2, 3)));

        // Check individual elements
        assert_eq!(
            arena.get_element(array_ref, 0, 0),
            Some(ValueRef::from_raw(1))
        );
        assert_eq!(
            arena.get_element(array_ref, 0, 2),
            Some(ValueRef::from_raw(3))
        );
        assert_eq!(
            arena.get_element(array_ref, 1, 0),
            Some(ValueRef::from_raw(4))
        );
        assert_eq!(
            arena.get_element(array_ref, 1, 2),
            Some(ValueRef::from_raw(6))
        );
    }

    #[test]
    fn test_array_arena_get_2d() {
        let mut arena = ArrayArena::new();

        let original = vec![
            vec![ValueRef::from_raw(1), ValueRef::from_raw(2)],
            vec![ValueRef::from_raw(3), ValueRef::from_raw(4)],
            vec![ValueRef::from_raw(5), ValueRef::from_raw(6)],
        ];

        let array_ref = arena.insert_2d(original.clone());
        let retrieved = arena.get_2d(array_ref).unwrap();

        assert_eq!(retrieved, original);
    }

    #[test]
    fn test_array_arena_multiple_arrays() {
        let mut arena = ArrayArena::new();

        let arr1 = vec![vec![ValueRef::from_raw(1), ValueRef::from_raw(2)]];
        let arr2 = vec![
            vec![ValueRef::from_raw(3)],
            vec![ValueRef::from_raw(4)],
            vec![ValueRef::from_raw(5)],
        ];
        let arr3 = vec![
            vec![
                ValueRef::from_raw(6),
                ValueRef::from_raw(7),
                ValueRef::from_raw(8),
            ],
            vec![
                ValueRef::from_raw(9),
                ValueRef::from_raw(10),
                ValueRef::from_raw(11),
            ],
        ];

        let ref1 = arena.insert_2d(arr1.clone());
        let ref2 = arena.insert_2d(arr2.clone());
        let ref3 = arena.insert_2d(arr3.clone());

        assert_eq!(arena.len(), 3);
        assert_eq!(arena.total_elements(), 11);

        assert_eq!(arena.get_2d(ref1), Some(arr1));
        assert_eq!(arena.get_2d(ref2), Some(arr2));
        assert_eq!(arena.get_2d(ref3), Some(arr3));
    }

    #[test]
    fn test_array_arena_empty_array() {
        let mut arena = ArrayArena::new();

        let array_ref = arena.insert(0, 0, Vec::new());

        assert_eq!(arena.dimensions(array_ref), Some((0, 0)));
        assert_eq!(arena.elements(array_ref), Some(&[][..]));
        assert_eq!(arena.get_2d(array_ref), Some(Vec::new()));
    }

    #[test]
    fn test_array_arena_single_element() {
        let mut arena = ArrayArena::new();

        let array_ref = arena.insert(1, 1, vec![ValueRef::from_raw(42)]);

        assert_eq!(arena.dimensions(array_ref), Some((1, 1)));
        assert_eq!(
            arena.get_element(array_ref, 0, 0),
            Some(ValueRef::from_raw(42))
        );
        assert_eq!(
            arena.get_2d(array_ref),
            Some(vec![vec![ValueRef::from_raw(42)]])
        );
    }

    #[test]
    fn test_array_arena_out_of_bounds() {
        let mut arena = ArrayArena::new();

        let array_ref = arena.insert(2, 3, vec![ValueRef::from_raw(0); 6]);

        assert_eq!(
            arena.get_element(array_ref, 0, 0),
            Some(ValueRef::from_raw(0))
        );
        assert_eq!(
            arena.get_element(array_ref, 1, 2),
            Some(ValueRef::from_raw(0))
        );
        assert_eq!(arena.get_element(array_ref, 2, 0), None); // Row out of bounds
        assert_eq!(arena.get_element(array_ref, 0, 3), None); // Col out of bounds
    }

    #[test]
    #[should_panic(expected = "Array dimensions 2x3 don't match element count 5")]
    fn test_array_arena_dimension_mismatch() {
        let mut arena = ArrayArena::new();
        arena.insert(2, 3, vec![ValueRef::from_raw(0); 5]);
    }

    #[test]
    #[should_panic(expected = "Row 1 has 2 columns, expected 3")]
    fn test_array_arena_inconsistent_rows() {
        let mut arena = ArrayArena::new();

        let array = vec![
            vec![
                ValueRef::from_raw(1),
                ValueRef::from_raw(2),
                ValueRef::from_raw(3),
            ],
            vec![ValueRef::from_raw(4), ValueRef::from_raw(5)], // Wrong length
        ];

        arena.insert_2d(array);
    }

    #[test]
    fn test_array_arena_clear() {
        let mut arena = ArrayArena::new();

        arena.insert_2d(vec![vec![ValueRef::from_raw(1), ValueRef::from_raw(2)]]);
        arena.insert_2d(vec![vec![ValueRef::from_raw(3), ValueRef::from_raw(4)]]);

        assert_eq!(arena.len(), 2);
        assert_eq!(arena.total_elements(), 4);

        arena.clear();

        assert_eq!(arena.len(), 0);
        assert_eq!(arena.total_elements(), 0);
        assert!(arena.is_empty());
    }

    #[test]
    fn test_array_arena_memory_usage() {
        let mut arena = ArrayArena::with_capacity(10);

        let initial_memory = arena.memory_usage();

        for i in 0..10 {
            arena.insert(10, 10, vec![ValueRef::from_raw(i); 100]);
        }

        let final_memory = arena.memory_usage();
        assert!(final_memory >= initial_memory);
    }
}
