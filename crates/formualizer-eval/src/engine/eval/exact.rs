//! Compensated floating-point reductions, shared by every path that sums a
//! Float64 lane: the scalar builtins (`SUM`, `AVERAGE`, `SUMIF(S)`, …), the
//! tier-3 range kernels, and the criteria kernels.
//!
//! Plain accumulation loses about half an ulp per addition, and the loss
//! grows with the row count: 500 cells of `19.99` sum to 9994.99999999998,
//! which is inside the 15 significant digits a spreadsheet displays. Excel's
//! `SUM` does not drift, and neither does this: [`CompensatedSum`] is
//! Kahan–Babuška (Neumaier) summation, carrying the rounding error of every
//! addition in a second `f64` so the result is within one ulp of the exact
//! sum of the inputs regardless of length.
//!
//! Slices accumulate into `LANES` independent compensated partials (the same
//! shape as Arrow's `sum` kernel, so the loop stays data-parallel) and the
//! partials merge at the end; merging is itself compensated. As with any
//! reassociated reduction, a partial can overflow where the sequential sum
//! would not (two lanes of 1e308 meet before the −1e308); that needs values
//! within a factor of `LANES` of `f64::MAX`. The tier-3
//! kernels reduce a range in the scalar builtin's segment order, so the two
//! paths agree bit for bit. NaN payloads are not stable across
//! compilations; callers map every non-finite result to `#NUM!`.
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

    /// Nulls in the slice (0 without a validity buffer).
    pub(crate) fn null_count(&self) -> usize {
        match self.validity {
            None => 0,
            Some((bytes, offset)) => {
                let len = self.values.len();
                let set = BitChunks::new(bytes, offset, len)
                    .iter()
                    .map(|c| c.count_ones() as usize)
                    .sum::<usize>()
                    + BitChunks::new(bytes, offset, len)
                        .remainder_bits()
                        .count_ones() as usize;
                len - set
            }
        }
    }

    /// Index of the first valid value.
    pub(crate) fn first_valid(&self) -> Option<usize> {
        match self.validity {
            None => (!self.values.is_empty()).then_some(0),
            Some((bytes, offset)) => (0..self.values.len()).find(|&i| {
                let bit = offset + i;
                bytes[bit / 8] & (1 << (bit % 8)) != 0
            }),
        }
    }

    pub(crate) fn is_valid(&self, i: usize) -> bool {
        match self.validity {
            None => true,
            Some((bytes, offset)) => {
                let bit = offset + i;
                bytes[bit / 8] & (1 << (bit % 8)) != 0
            }
        }
    }
}

/// Borrow `len` values of `array` from `offset` (relative to the array's own
/// offset) without copying.
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

/// Running sum with its accumulated rounding error (Kahan–Babuška /
/// Neumaier). `total()` is within one ulp of the exact sum of everything
/// added. Adding `0.0` is a no-op, so blanks may be fed as zeros.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CompensatedSum {
    sum: f64,
    compensation: f64,
}

impl CompensatedSum {
    #[inline(always)]
    pub fn add(&mut self, value: f64) {
        let t = self.sum + value;
        // Whichever operand is larger lost nothing; recover the smaller one's
        // dropped low-order bits.
        let lost = if self.sum.abs() >= value.abs() {
            (self.sum - t) + value
        } else {
            (value - t) + self.sum
        };
        self.compensation += lost;
        self.sum = t;
    }

    /// Add `a * b`, carrying the product's own rounding error as well
    /// (two-product via fused multiply-add), so `SUMSQ`/`SUMPRODUCT` of
    /// inexact decimals land on the exact result like `SUM` does.
    #[inline(always)]
    pub fn add_product(&mut self, a: f64, b: f64) {
        let product = a * b;
        let product_error = a.mul_add(b, -product);
        self.add(product);
        self.compensation += product_error;
    }

    /// Fold another partial in; the combined error term is kept.
    #[inline(always)]
    pub fn merge(&mut self, other: CompensatedSum) {
        self.add(other.sum);
        self.compensation += other.compensation;
    }

    /// Add every valid value of the slice.
    pub fn add_slice(&mut self, slice: LaneSlice<'_, f64>) {
        let values = slice.values;
        let partial = match slice.validity {
            Some((bytes, offset)) if slice.null_count() > 0 => match PREFERRED_VECTOR_SIZE / 8 {
                8 => nullable_lanes::<8>(values, bytes, offset),
                4 => nullable_lanes::<4>(values, bytes, offset),
                2 => nullable_lanes::<2>(values, bytes, offset),
                _ => nullable_lanes::<1>(values, bytes, offset),
            },
            _ => match PREFERRED_VECTOR_SIZE_NON_NULL / 8 {
                16 => nonnull_lanes::<16>(values),
                8 => nonnull_lanes::<8>(values),
                4 => nonnull_lanes::<4>(values),
                2 => nonnull_lanes::<2>(values),
                _ => nonnull_lanes::<1>(values),
            },
        };
        self.merge(partial);
    }

    /// Add every non-null value of the array.
    pub fn add_array(&mut self, array: &arrow_array::Float64Array) {
        self.add_slice(lane_slice(array, 0, array.len()));
    }

    #[inline(always)]
    pub fn total(&self) -> f64 {
        // Once the sum overflows, the compensation is `inf - inf`; the sum
        // alone carries the right non-finite answer.
        if self.sum.is_finite() {
            self.sum + self.compensation
        } else {
            self.sum
        }
    }
}

/// Running product with its accumulated rounding error (Ogita–Rump–Oishi
/// compensated product). Each multiply's error, `fma(p, x, -p·x)`, is
/// carried in a second `f64` that is itself scaled by every later factor,
/// so `total()` is within a couple of ulps of the exact product regardless
/// of length; a plain fold loses about an ulp per factor.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompensatedProduct {
    product: f64,
    error: f64,
}

impl Default for CompensatedProduct {
    fn default() -> Self {
        Self {
            product: 1.0,
            error: 0.0,
        }
    }
}

impl CompensatedProduct {
    #[inline(always)]
    pub fn mul(&mut self, factor: f64) {
        let next = self.product * factor;
        let lost = self.product.mul_add(factor, -next);
        self.error = self.error * factor + lost;
        self.product = next;
    }

    #[inline(always)]
    pub fn total(&self) -> f64 {
        if self.product.is_finite() {
            self.product + self.error
        } else {
            self.product
        }
    }
}

/// Compensated product of an iterator of values; the empty product is `1.0`.
pub fn compensated_product(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut acc = CompensatedProduct::default();
    for v in values {
        acc.mul(v);
    }
    acc.total()
}

/// Compensated sum of an iterator of values. The empty sum is `0.0`
/// (`Iterator::sum` of no `f64` values is `-0.0`, which Excel has no
/// representation for).
pub fn compensated_sum(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut acc = CompensatedSum::default();
    for v in values {
        acc.add(v);
    }
    acc.total()
}

/// Compensated arithmetic mean. The caller guarantees `values` is non-empty.
pub fn compensated_mean(values: &[f64]) -> f64 {
    compensated_sum(values.iter().copied()) / values.len() as f64
}

/// Compensated `Σ aᵢ·bᵢ`, each product's rounding error folded in.
pub fn compensated_dot(a: impl IntoIterator<Item = f64>, b: impl IntoIterator<Item = f64>) -> f64 {
    let mut acc = CompensatedSum::default();
    for (x, y) in a.into_iter().zip(b) {
        acc.add_product(x, y);
    }
    acc.total()
}

/// Compensated `Σ (x − center)²`, the error-free square folded in. This is
/// the core of every variance, standard deviation and related moment.
pub fn compensated_sum_sq_dev(values: &[f64], center: f64) -> f64 {
    let mut acc = CompensatedSum::default();
    for &v in values {
        let d = v - center;
        acc.add_product(d, d);
    }
    acc.total()
}

/// Compensated sum of the slice, or `None` when every value is null (the
/// convention of Arrow's `sum`, which the callers' empty-range semantics
/// follow).
pub(crate) fn sum_f64(slice: LaneSlice<'_, f64>) -> Option<f64> {
    if slice.null_count() == slice.len() {
        return None;
    }
    let mut acc = CompensatedSum::default();
    acc.add_slice(slice);
    Some(acc.total())
}

#[inline(always)]
fn reduce<const LANES: usize>(mut acc: [CompensatedSum; LANES]) -> CompensatedSum {
    let mut len = LANES;
    while len >= 2 {
        let mid = len / 2;
        let (head, tail) = acc[..len].split_at_mut(mid);
        for i in 0..mid {
            head[i].merge(tail[i]);
        }
        len = mid;
    }
    acc[0]
}

#[inline(never)]
fn nonnull_lanes<const LANES: usize>(values: &[f64]) -> CompensatedSum {
    let mut acc = [CompensatedSum::default(); LANES];
    let (chunks, remainder) = values.as_chunks::<LANES>();
    for chunk in chunks {
        for i in 0..LANES {
            acc[i].add(chunk[i]);
        }
    }
    for (i, v) in remainder.iter().enumerate() {
        acc[i].add(*v);
    }
    reduce(acc)
}

#[inline(always)]
fn nullable_chunk<const LANES: usize>(
    acc: &mut [CompensatedSum; LANES],
    values: &[f64],
    validity: u64,
) {
    let mut bit = 1u64;
    for i in 0..LANES {
        // A masked value adds 0.0, which leaves sum and compensation intact.
        let value = if validity & bit != 0 { values[i] } else { 0.0 };
        acc[i].add(value);
        bit <<= 1;
    }
}

#[inline(never)]
fn nullable_lanes<const LANES: usize>(
    values: &[f64],
    bytes: &[u8],
    offset: usize,
) -> CompensatedSum {
    let mut acc = [CompensatedSum::default(); LANES];
    let (values_chunks, remainder) = values.as_chunks::<64>();
    let validity_chunks = BitChunks::new(bytes, offset, values.len());
    let mut validity_iter = validity_chunks.iter();
    for chunk in values_chunks {
        let mut validity = validity_iter.next().unwrap_or(0);
        for lane_chunk in chunk.as_chunks::<LANES>().0 {
            nullable_chunk(&mut acc, lane_chunk, validity);
            validity >>= LANES;
        }
    }
    if !remainder.is_empty() {
        let mut validity = validity_chunks.remainder_bits();
        let (remainder_chunks, rest) = remainder.as_chunks::<LANES>();
        for lane_chunk in remainder_chunks {
            nullable_chunk(&mut acc, lane_chunk, validity);
            validity >>= LANES;
        }
        let mut bit = 1u64;
        for (i, v) in rest.iter().enumerate() {
            let value = if validity & bit != 0 { *v } else { 0.0 };
            acc[i].add(value);
            bit <<= 1;
        }
    }
    reduce(acc)
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
            2 => 1e308,
            3 => -1e-300,
            4 => 0.1,
            5 => 19.99,
            _ => (rng.next() % 2_000_000) as f64 / 7.0 - 100_000.0,
        }
    }

    /// Shewchuk's exactly-rounded sum (Python's `math.fsum`): the partials
    /// are non-overlapping, so the result is the exact sum up to one final
    /// rounding.
    fn exact_sum(values: impl Iterator<Item = f64>) -> f64 {
        let mut partials: Vec<f64> = Vec::new();
        for mut x in values {
            let mut i = 0;
            for &y in partials.clone().iter() {
                let (mut hi, mut lo) = (x, y);
                if hi.abs() < lo.abs() {
                    std::mem::swap(&mut hi, &mut lo);
                }
                let s = hi + lo;
                let err = lo - (s - hi);
                if err != 0.0 {
                    partials[i] = err;
                    i += 1;
                }
                x = s;
            }
            partials.truncate(i);
            partials.push(x);
        }
        partials.iter().fold(0.0, |a, b| a + b)
    }

    fn valid_values(array: &Float64Array) -> impl Iterator<Item = f64> + '_ {
        array.iter().flatten()
    }

    /// Every slicing of the array sums to the exactly-rounded result (within
    /// an ulp), whether reduced in one go or segment by segment.
    fn check(array: &Float64Array) {
        for offset in [0usize, 1, 3, 7, 63, 64, 65] {
            for len in [
                0usize, 1, 2, 3, 5, 8, 15, 31, 63, 64, 65, 127, 128, 129, 200,
            ] {
                if offset + len > array.len() {
                    continue;
                }
                let sliced = array.slice(offset, len);
                let slice = lane_slice(array, offset, len);
                assert_eq!(slice.null_count(), sliced.null_count());
                let first = (0..len).find(|&i| sliced.is_valid(i));
                assert_eq!(slice.first_valid(), first);

                let got = sum_f64(slice);
                let valid = sliced.len() - sliced.null_count();
                assert_eq!(got.is_some(), valid > 0, "offset {offset} len {len}");
                let Some(got) = got else { continue };
                let expected = exact_sum(valid_values(&sliced));
                // The oracle overflows with the inputs; so must we.
                let close = |v: f64| {
                    if expected.is_finite() {
                        v == expected || (v - expected).abs() <= expected.abs() * f64::EPSILON
                    } else {
                        !v.is_finite()
                    }
                };
                assert!(
                    close(got),
                    "offset {offset} len {len}: got {got:e}, exact {expected:e}"
                );

                let mut chunked = CompensatedSum::default();
                for (s, e) in [(0, len / 3), (len / 3, 2 * len / 3), (2 * len / 3, len)] {
                    chunked.add_slice(lane_slice(array, offset + s, e - s));
                }
                assert!(
                    close(chunked.total()),
                    "offset {offset} len {len}: chunked {:e}, exact {expected:e}",
                    chunked.total()
                );
            }
        }
    }

    #[test]
    fn compensated_sum_is_exact_and_chunking_invariant() {
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

    #[test]
    fn long_decimal_column_sums_exactly() {
        for &(v, n, exact) in &[
            (19.99, 500usize, 9995.0),
            (0.07, 500, 35.0),
            (0.3, 300, 90.0),
            (1.1, 500, 550.0),
            (12.34, 10_000, 123_400.0),
            (0.1, 100_000, 10_000.0),
        ] {
            let array = Float64Array::from(vec![v; n]);
            let mut acc = CompensatedSum::default();
            acc.add_array(&array);
            assert_eq!(acc.total(), exact, "{n} × {v}");
            let scalar = (0..n).fold(CompensatedSum::default(), |mut a, _| {
                a.add(v);
                a
            });
            assert_eq!(scalar.total(), exact, "scalar {n} × {v}");
        }
    }

    /// Classic Kahan, for contrast: its compensation term breaks when a
    /// value much larger than the running sum arrives.
    fn classic_kahan(values: &[f64]) -> f64 {
        let (mut sum, mut c) = (0.0f64, 0.0f64);
        for &v in values {
            let y = v - c;
            let t = sum + y;
            c = (t - sum) - y;
            sum = t;
        }
        sum
    }

    /// The Neumaier (KBN) step keeps the small terms when a huge one swamps
    /// the running sum, in either order, in scalar and lane form, and
    /// across merged partials.
    #[test]
    fn mixed_magnitudes_do_not_cancel() {
        let cases: &[(&[f64], f64)] = &[
            (&[1.0, 1e100, 1.0, -1e100], 2.0),
            (&[1e100, 1.0, -1e100, 1.0], 2.0),
            (&[1.0, -1e100, 1.0, 1e100], 2.0),
            (&[1e300, 1.0, -1e300, 1.0, 1e300, -1e300], 2.0),
            (&[1e-300, 1e300, 1e-300, -1e300], 2e-300),
        ];
        for (values, exact) in cases {
            assert_ne!(
                classic_kahan(values),
                *exact,
                "Kahan should fail on {values:?}"
            );
            assert_eq!(
                compensated_sum(values.iter().copied()),
                *exact,
                "{values:?}"
            );
            let mut lanes = CompensatedSum::default();
            lanes.add_array(&Float64Array::from(values.to_vec()));
            assert_eq!(lanes.total(), *exact, "lanes {values:?}");
        }

        // One huge term in a long run of small ones, across lanes and
        // segments: the small ones survive. While the huge term occupies the
        // running sum the small ones land in the compensation term, which is
        // itself a plain sum — KBN is exact to second order, not exactly
        // rounded — so this holds to ~1e-13 relative, where classic Kahan
        // and a plain fold lose the small terms outright.
        let mut values = vec![0.1; 1000];
        values.insert(333, 1e100);
        values.push(-1e100);
        let exact = 100.0;
        let close = |v: f64| (v - exact).abs() <= exact * 1e-13;
        let array = Float64Array::from(values.clone());
        assert!(close(compensated_sum(values.iter().copied())));
        let mut whole = CompensatedSum::default();
        whole.add_array(&array);
        assert!(close(whole.total()), "{}", whole.total());
        let mut segmented = CompensatedSum::default();
        for (start, len) in [(0, 300), (300, 100), (400, 602)] {
            segmented.add_slice(lane_slice(&array, start, len));
        }
        assert!(close(segmented.total()), "{}", segmented.total());
        assert!(!close(classic_kahan(&values)));
        assert!(!close(values.iter().sum::<f64>()));
    }

    #[test]
    fn long_products_are_correctly_rounded() {
        // (value, count, correctly rounded value^count)
        for &(v, n, exact) in &[
            (1.1, 200usize, 189_905_276.460_464_9),
            (1.01, 500, 144.772_772_432_573_96),
            (0.99, 500, 0.006_570_483_042_414_603),
            (1.0001, 2000, 1.221_390_545_007_830_6),
            (2.5, 100, 6.223_015_277_861_142e39),
        ] {
            let got = compensated_product(std::iter::repeat_n(v, n));
            assert_eq!(got, exact, "{v}^{n}");
        }
        assert_eq!(compensated_product(std::iter::empty()), 1.0);
        assert_eq!(compensated_product([2.0, 0.0, 3.0]), 0.0);
        assert_eq!(compensated_product([1e200, 1e200]), f64::INFINITY);
    }

    #[test]
    fn non_finite_inputs_propagate() {
        let mut acc = CompensatedSum::default();
        acc.add_array(&Float64Array::from(vec![1.0, f64::INFINITY, 2.0]));
        assert_eq!(acc.total(), f64::INFINITY);
        let mut acc = CompensatedSum::default();
        acc.add_array(&Float64Array::from(vec![f64::INFINITY, f64::NEG_INFINITY]));
        assert!(acc.total().is_nan());
        let mut acc = CompensatedSum::default();
        acc.add_array(&Float64Array::from(vec![1.0, f64::NAN]));
        assert!(acc.total().is_nan());
    }
}
