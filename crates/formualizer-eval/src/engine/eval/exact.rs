//! Exact replicas of the Arrow reductions the scalar builtins call, over
//! borrowed lane slices (no per-call array construction).
//!
//! `sum_f64` reproduces `arrow_arith::aggregate::sum::<Float64Type>`
//! (arrow-arith 59.2) operation for operation: the lane count depends on the
//! same compile-time vector width, the nullable path consumes validity in
//! 64-bit chunks aligned to the slice start, and accumulators reduce as the
//! same tree. The result is bit-identical (`-0.0`, infinities, every
//! finite value) to calling Arrow on the equivalent sliced array; NaN is NaN
//! (payloads are not stable across compilations, LLVM may commute `fadd`,
//! and callers map every non-finite result to `#NUM!`). `tests` checks this
//! differentially.

use arrow_buffer::bit_chunk_iterator::BitChunks;

const PREFERRED_VECTOR_SIZE: usize =
    if cfg!(all(target_arch = "x86_64", target_feature = "avx512f")) {
        64
    } else if cfg!(all(target_arch = "x86_64", target_feature = "avx")) {
        32
    } else {
        16
    };
const PREFERRED_VECTOR_SIZE_NON_NULL: usize = PREFERRED_VECTOR_SIZE * 2;

/// A borrowed slice of a Float64 lane: values plus optional validity bits
/// (`bytes`, bit offset of the first value).
#[derive(Clone, Copy)]
pub(crate) struct LaneSlice<'a, T> {
    pub values: &'a [T],
    pub validity: Option<(&'a [u8], usize)>,
}

impl<'a, T> LaneSlice<'a, T> {
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    /// Number of null slots.
    pub(crate) fn null_count(&self) -> usize {
        match self.validity {
            None => 0,
            Some((bytes, offset)) => {
                let chunks = BitChunks::new(bytes, offset, self.values.len());
                self.values.len()
                    - chunks
                        .iter_padded()
                        .map(|c| c.count_ones() as usize)
                        .sum::<usize>()
            }
        }
    }

    /// Index of the first valid slot.
    pub(crate) fn first_valid(&self) -> Option<usize> {
        match self.validity {
            None => (!self.values.is_empty()).then_some(0),
            Some((bytes, offset)) => {
                let chunks = BitChunks::new(bytes, offset, self.values.len());
                for (i, chunk) in chunks.iter_padded().enumerate() {
                    if chunk != 0 {
                        let idx = i * 64 + chunk.trailing_zeros() as usize;
                        return (idx < self.values.len()).then_some(idx);
                    }
                }
                None
            }
        }
    }

    /// Validity of slot `i`.
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

/// The lane slice `[offset, offset + len)` of a primitive array.
pub(crate) fn lane_slice<P: arrow_array::ArrowPrimitiveType>(
    array: &arrow_array::PrimitiveArray<P>,
    offset: usize,
    len: usize,
) -> LaneSlice<'_, P::Native> {
    use arrow_array::Array;
    let values = &array.values()[offset..offset + len];
    let validity = array
        .nulls()
        .map(|n| (n.buffer().as_slice(), n.offset() + offset));
    LaneSlice { values, validity }
}

#[inline(always)]
fn reduce<const LANES: usize>(mut acc: [f64; LANES]) -> f64 {
    let mut len = LANES;
    while len >= 2 {
        let mid = len / 2;
        let (h, t) = acc[..len].split_at_mut(mid);
        for i in 0..mid {
            h[i] += t[i];
        }
        len /= 2;
    }
    acc[0]
}

#[inline(never)]
fn nonnull_lanes<const LANES: usize>(values: &[f64]) -> f64 {
    let mut acc = [0.0f64; LANES];
    let mut chunks = values.chunks_exact(LANES);
    for chunk in chunks.by_ref() {
        for i in 0..LANES {
            acc[i] += chunk[i];
        }
    }
    let remainder = chunks.remainder();
    for i in 0..remainder.len() {
        acc[i] += remainder[i];
    }
    reduce(acc)
}

fn nonnull_simple(values: &[f64]) -> f64 {
    values.iter().copied().fold(0.0f64, |a, b| a + b)
}

#[inline(always)]
fn nullable_chunk<const LANES: usize>(acc: &mut [f64; LANES], values: &[f64], validity: u64) {
    let mut bit = 1u64;
    for i in 0..LANES {
        let sum = acc[i];
        let added = sum + values[i];
        acc[i] = if validity & bit != 0 { added } else { sum };
        bit <<= 1;
    }
}

#[inline(never)]
fn nullable_lanes<const LANES: usize>(values: &[f64], bytes: &[u8], offset: usize) -> f64 {
    let mut acc = [0.0f64; LANES];
    let mut values_chunks = values.chunks_exact(64);
    let validity_chunks = BitChunks::new(bytes, offset, values.len());
    let mut validity_iter = validity_chunks.iter();
    for chunk in values_chunks.by_ref() {
        let mut validity = validity_iter.next().unwrap_or(0);
        for lane_chunk in chunk.chunks_exact(LANES) {
            nullable_chunk(&mut acc, lane_chunk, validity);
            validity >>= LANES;
        }
    }
    let remainder = values_chunks.remainder();
    if !remainder.is_empty() {
        let mut validity = validity_chunks.remainder_bits();
        let mut remainder_chunks = remainder.chunks_exact(LANES);
        for lane_chunk in remainder_chunks.by_ref() {
            nullable_chunk(&mut acc, lane_chunk, validity);
            validity >>= LANES;
        }
        let rest = remainder_chunks.remainder();
        let mut bit = 1u64;
        for i in 0..rest.len() {
            let sum = acc[i];
            let added = sum + rest[i];
            acc[i] = if validity & bit != 0 { added } else { sum };
            bit <<= 1;
        }
    }
    reduce(acc)
}

/// `arrow::compute::sum` of the Float64 lane slice.
pub(crate) fn sum_f64(slice: LaneSlice<'_, f64>) -> Option<f64> {
    let len = slice.len();
    let null_count = slice.null_count();
    if null_count == len {
        return None;
    }
    let values = slice.values;
    match slice.validity {
        Some((bytes, offset)) if null_count > 0 => Some(match PREFERRED_VECTOR_SIZE / 8 {
            8 => nullable_lanes::<8>(values, bytes, offset),
            4 => nullable_lanes::<4>(values, bytes, offset),
            2 => nullable_lanes::<2>(values, bytes, offset),
            _ => nullable_lanes::<1>(values, bytes, offset),
        }),
        _ => Some(match PREFERRED_VECTOR_SIZE_NON_NULL / 8 {
            16 => nonnull_lanes::<16>(values),
            8 => nonnull_lanes::<8>(values),
            4 => nonnull_lanes::<4>(values),
            2 => nonnull_lanes::<2>(values),
            _ => nonnull_simple(values),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array};

    /// xorshift, deterministic.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn value(rng: &mut Rng) -> f64 {
        match rng.next() % 16 {
            0 => -0.0,
            1 => 0.0,
            2 => f64::NAN,
            3 => f64::from_bits(0x7ff8_0000_0000_1234),
            4 => f64::INFINITY,
            5 => f64::NEG_INFINITY,
            6 => 1e308,
            7 => -1e-300,
            8 => 0.1,
            _ => (rng.next() % 2_000_000) as f64 / 7.0 - 100_000.0,
        }
    }

    fn check(array: &Float64Array) {
        for offset in [0usize, 1, 3, 7, 63, 64, 65] {
            for len in [
                0usize, 1, 2, 3, 5, 8, 15, 31, 63, 64, 65, 127, 128, 129, 200,
            ] {
                if offset + len > array.len() {
                    continue;
                }
                let sliced = array.slice(offset, len);
                let expected = arrow::compute::kernels::aggregate::sum(&sliced);
                let got = sum_f64(lane_slice(array, offset, len));
                // NaN payloads are not stable across compilations (LLVM may
                // commute `fadd`); callers map every non-finite sum to #NUM!.
                let key =
                    |v: Option<f64>| v.map(|x| if x.is_nan() { u64::MAX } else { x.to_bits() });
                assert_eq!(key(expected), key(got), "offset {offset} len {len}");
                assert_eq!(
                    lane_slice(array, offset, len).null_count(),
                    sliced.null_count()
                );
                let first = (0..len).find(|&i| sliced.is_valid(i));
                assert_eq!(lane_slice(array, offset, len).first_valid(), first);
            }
        }
    }

    #[test]
    fn exact_sum_matches_arrow_bit_for_bit() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for round in 0..60 {
            let n = 280;
            let null_rate = round % 4; // 0: none, 1..3: increasing
            let values: Vec<Option<f64>> = (0..n)
                .map(|_| {
                    let v = value(&mut rng);
                    (null_rate == 0 || rng.next() % 4 >= null_rate as u64).then_some(v)
                })
                .collect();
            let array = Float64Array::from(values);
            check(&array);
            // No null buffer at all.
            let dense = Float64Array::from((0..n).map(|_| value(&mut rng)).collect::<Vec<f64>>());
            check(&dense);
            // Arrays that are themselves slices (nonzero buffer offsets).
            check(&array.slice(5, 250));
        }
        let all_null = Float64Array::new_null(100);
        check(&all_null);
    }
}
