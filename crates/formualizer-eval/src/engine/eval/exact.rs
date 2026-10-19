//! Excel-order reductions over borrowed lanes. Carry the accumulator across
//! chunks and arguments: adding independently computed subtotals is not exact.
use arrow_buffer::bit_chunk_iterator::BitChunks;

#[derive(Clone, Copy)]
pub(crate) struct LaneSlice<'a, T> {
    pub values: &'a [T],
    pub validity: Option<(&'a [u8], usize)>,
}

impl<T> LaneSlice<'_, T> {
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }
    pub(crate) fn null_count(&self) -> usize {
        match self.validity {
            None => 0,
            Some((bytes, offset)) => {
                self.len()
                    - BitChunks::new(bytes, offset, self.len())
                        .iter_padded()
                        .map(|c| c.count_ones() as usize)
                        .sum::<usize>()
            }
        }
    }
    pub(crate) fn first_valid(&self) -> Option<usize> {
        match self.validity {
            None => (!self.values.is_empty()).then_some(0),
            Some((bytes, offset)) => {
                for (i, chunk) in BitChunks::new(bytes, offset, self.len())
                    .iter_padded()
                    .enumerate()
                {
                    if chunk != 0 {
                        let idx = i * 64 + chunk.trailing_zeros() as usize;
                        return (idx < self.len()).then_some(idx);
                    }
                }
                None
            }
        }
    }
    #[inline]
    pub(crate) fn is_valid(&self, i: usize) -> bool {
        match self.validity {
            None => true,
            Some((bytes, offset)) => {
                let bit = offset + i;
                bytes[bit / 8] >> (bit % 8) & 1 == 1
            }
        }
    }
}

pub(crate) fn lane_slice<P: arrow_array::ArrowPrimitiveType>(
    array: &arrow_array::PrimitiveArray<P>,
    offset: usize,
    len: usize,
) -> LaneSlice<'_, P::Native> {
    use arrow_array::Array;
    LaneSlice {
        values: &array.values()[offset..offset + len],
        validity: array
            .nulls()
            .map(|n| (n.buffer().as_slice(), n.offset() + offset)),
    }
}

/// Sequentially add valid values, without resetting at chunk boundaries
/// ([`crate::engine::NumericProfile::ExcelCompatible`] order).
/// Starting at +0, the running sum cannot become -0 under round-to-nearest;
/// selecting +0 for a null is therefore equivalent to skipping it.
pub(crate) fn add_f64(total: &mut f64, slice: LaneSlice<'_, f64>) {
    if slice.validity.is_none() {
        for &value in slice.values {
            *total += value;
        }
    } else {
        for (i, &value) in slice.values.iter().enumerate() {
            *total += if slice.is_valid(i) { value } else { 0.0 };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array};

    #[test]
    fn sequential_seed_and_chunk_boundaries() {
        let array = Float64Array::from(vec![Some(1e16), None, Some(1.0), Some(-1e16), Some(1.0)]);
        let mut total = 0.0;
        add_f64(&mut total, lane_slice(&array, 0, 3));
        add_f64(&mut total, lane_slice(&array, 3, 2));
        assert_eq!(total, 1.0);
    }

    #[test]
    fn sequential_slices_match_scalar_oracle() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for round in 0..60 {
            let values: Vec<Option<f64>> = (0..280)
                .map(|i| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let value = match state % 12 {
                        0 => -0.0,
                        1 => f64::NAN,
                        2 => f64::INFINITY,
                        3 => f64::NEG_INFINITY,
                        4 => 1e308,
                        _ => (state % 2_000_000) as f64 / 7.0 - 100_000.0,
                    };
                    (round % 4 == 0 || i % 4 >= round % 4).then_some(value)
                })
                .collect();
            let array = Float64Array::from(values);
            for array in [&array, &array.slice(5, 250)] {
                for offset in [0, 1, 7, 63, 64, 65] {
                    for len in [0, 1, 3, 63, 64, 65, 129] {
                        let sliced = array.slice(offset, len);
                        let expected = sliced.iter().flatten().fold(0.0, |a, b| a + b);
                        let slice = lane_slice(array, offset, len);
                        let mut got = 0.0;
                        add_f64(&mut got, slice);
                        assert!(
                            expected.is_nan() && got.is_nan()
                                || expected.to_bits() == got.to_bits()
                        );
                        assert_eq!(slice.null_count(), sliced.null_count());
                        assert_eq!(slice.first_valid(), (0..len).find(|&i| sliced.is_valid(i)));
                    }
                }
            }
        }
        let array = Float64Array::new_null(100);
        let mut total = 0.0;
        add_f64(&mut total, lane_slice(&array, 0, 100));
        assert_eq!(total.to_bits(), 0.0f64.to_bits());
    }
}
