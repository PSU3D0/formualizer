// no imports required

/// Typed numeric stripe passed to reductions and maps
pub struct NumericChunk<'a> {
    pub data: &'a [f64],
    pub validity: Option<ValidityMask<'a>>, // None => all valid
}

/// Validity bitmask for elementwise operations
pub enum ValidityMask<'a> {
    Bits(&'a [u64]),
    Bools(&'a [bool]),
}

/// Heterogeneous cell chunk for elementwise mapping.
pub enum CellChunk<'a> {
    /// Mixed values provided as `LiteralValue` slice
    Mixed(&'a [formualizer_common::LiteralValue]),
    /// Numeric-only chunk (optional fast path for elementwise numeric)
    Numbers(&'a [f64]),
}

impl<'a> NumericChunk<'a> {
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_numeric_chunk_properties() {
        let buf = [1.0, 2.0, 3.0];
        let ch = NumericChunk {
            data: &buf,
            validity: None,
        };
        assert_eq!(ch.len(), 3);
        assert!(!ch.is_empty());
    }
}
