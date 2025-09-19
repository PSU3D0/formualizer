//! Mask API for criteria evaluation (Phase 3)

use std::ops::Range;
use std::sync::Arc;

/// Dense bitmask for row selection
#[derive(Debug, Clone)]
pub struct DenseMask {
    bits: Arc<Vec<u64>>,
    len: usize,
}

impl DenseMask {
    /// Create a new mask with the given length (in bits)
    pub fn new(len: usize) -> Self {
        let n_words = len.div_ceil(64);
        Self {
            bits: Arc::new(vec![0u64; n_words]),
            len,
        }
    }

    /// Create a mask with all bits set
    pub fn all_ones(len: usize) -> Self {
        let n_words = len.div_ceil(64);
        let mut bits = vec![!0u64; n_words];

        // Clear unused bits in the last word
        let remainder = len % 64;
        if remainder > 0 && n_words > 0 {
            bits[n_words - 1] = (1u64 << remainder) - 1;
        }

        Self {
            bits: Arc::new(bits),
            len,
        }
    }

    /// Get the length in bits
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Set a bit at the given index (requires mutable Arc)
    pub fn set(&mut self, index: usize, value: bool) {
        if index >= self.len {
            return;
        }

        let bits = Arc::make_mut(&mut self.bits);
        let word_idx = index / 64;
        let bit_idx = index % 64;

        if value {
            bits[word_idx] |= 1u64 << bit_idx;
        } else {
            bits[word_idx] &= !(1u64 << bit_idx);
        }
    }

    /// Get a bit at the given index
    pub fn get(&self, index: usize) -> bool {
        if index >= self.len {
            return false;
        }

        let word_idx = index / 64;
        let bit_idx = index % 64;

        (self.bits[word_idx] & (1u64 << bit_idx)) != 0
    }

    /// Count the number of set bits
    pub fn count_ones(&self) -> u64 {
        self.bits.iter().map(|w| w.count_ones() as u64).sum()
    }

    /// Calculate density (ratio of set bits to total bits)
    pub fn density(&self) -> f64 {
        if self.len == 0 {
            return 0.0;
        }
        self.count_ones() as f64 / self.len as f64
    }

    /// In-place AND with another mask
    pub fn and_inplace(&mut self, other: &DenseMask) {
        let bits = Arc::make_mut(&mut self.bits);
        let min_words = bits.len().min(other.bits.len());

        for (dst, src) in bits.iter_mut().zip(other.bits.iter()).take(min_words) {
            *dst &= *src;
        }

        // Clear any remaining words if other is shorter
        for slot in bits.iter_mut().skip(min_words) {
            *slot = 0;
        }
    }

    /// In-place OR with another mask
    pub fn or_inplace(&mut self, other: &DenseMask) {
        let bits = Arc::make_mut(&mut self.bits);
        let min_words = bits.len().min(other.bits.len());

        for (dst, src) in bits.iter_mut().zip(other.bits.iter()).take(min_words) {
            *dst |= *src;
        }
    }

    /// In-place NOT within used rows range
    pub fn not_inplace(&mut self, used_rows: Range<u32>) {
        let bits = Arc::make_mut(&mut self.bits);
        let start = used_rows.start as usize;
        let end = used_rows.end.min(self.len as u32) as usize;

        // Flip bits in the used range
        for i in start..end {
            let word_idx = i / 64;
            let bit_idx = i % 64;
            bits[word_idx] ^= 1u64 << bit_idx;
        }
    }

    /// Iterate over set bit positions
    pub fn iter_ones(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        Box::new(DenseMaskIterator {
            mask: self,
            word_idx: 0,
            current_word: if self.bits.is_empty() {
                0
            } else {
                self.bits[0]
            },
            base_index: 0,
        })
    }

    /// Create from a slice of row indices
    pub fn from_indices(indices: &[u32], len: usize) -> Self {
        let mut mask = Self::new(len);
        for &idx in indices {
            if (idx as usize) < len {
                mask.set(idx as usize, true);
            }
        }
        mask
    }

    /// Apply to select values from a slice
    pub fn select<'a, T>(&self, values: &'a [T]) -> Vec<&'a T> {
        let min_len = self.len.min(values.len());
        let mut result = Vec::new();

        // Choose iteration strategy based on density
        if self.density() < 0.1 {
            // Sparse: iterate set bits
            for idx in self.iter_ones() {
                if (idx as usize) < min_len {
                    result.push(&values[idx as usize]);
                }
            }
        } else {
            // Dense: linear scan
            for (idx, value) in values.iter().take(min_len).enumerate() {
                if self.get(idx) {
                    result.push(value);
                }
            }
        }

        result
    }
}

/// Iterator for set bit positions in a DenseMask
struct DenseMaskIterator<'a> {
    mask: &'a DenseMask,
    word_idx: usize,
    current_word: u64,
    base_index: u32,
}

impl<'a> Iterator for DenseMaskIterator<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Find next set bit in current word
            if self.current_word != 0 {
                let bit_idx = self.current_word.trailing_zeros();
                let index = self.base_index + bit_idx;

                // Clear the found bit
                self.current_word &= self.current_word - 1;

                // Check if within bounds
                if index < self.mask.len as u32 {
                    return Some(index);
                }
            }

            // Move to next word
            self.word_idx += 1;
            if self.word_idx >= self.mask.bits.len() {
                return None;
            }

            self.current_word = self.mask.bits[self.word_idx];
            self.base_index = (self.word_idx * 64) as u32;
        }
    }
}
