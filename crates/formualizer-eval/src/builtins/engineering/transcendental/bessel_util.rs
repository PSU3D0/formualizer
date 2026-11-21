pub(crate) const HUGE: f64 = 1e300;
pub(crate) const FRAC_2_SQRT_PI: f64 = 5.641_895_835_477_563e-1;

/// Work limit for recurrence-based Bessel evaluation, matching `bessel_jn_yn.rs`.
pub(crate) const MAX_RECURRENCE_ORDER: i32 = 1_000_000;

/// Exact IEEE-754 scaling `x * 2^exp` in pure Rust (portable across native & WASM).
///
/// Produces bit-exact results for normal outputs and single-rounded IEEE-754
/// results for subnormal outputs without intermediate overflow or underflow.
#[inline]
pub(crate) fn ldexp(mut x: f64, exp: i32) -> f64 {
    if x == 0.0 || !x.is_finite() || exp == 0 {
        return x;
    }
    let mut e = i64::from(exp);
    let mut bits = x.to_bits();
    let sign = bits & (1u64 << 63);
    let mut biased = ((bits >> 52) & 0x7ff) as i64;

    if biased == 0 {
        // 2^54 brings every non-zero subnormal into normal range exactly.
        const TWO_54: f64 = f64::from_bits((1023 + 54) << 52);
        x *= TWO_54;
        bits = x.to_bits();
        biased = ((bits >> 52) & 0x7ff) as i64;
        e -= 54;
    }

    let target_biased = biased + e;
    if target_biased >= 0x7ff {
        return f64::from_bits(sign | (0x7ffu64 << 52));
    }
    if target_biased > 0 {
        let mantissa = bits & 0x000f_ffff_ffff_ffff;
        return f64::from_bits(sign | ((target_biased as u64) << 52) | mantissa);
    }
    if target_biased < -52 {
        return f64::from_bits(sign);
    }
    // Subnormal output: place at minimum normal exponent (2^-1022, biased = 1),
    // then multiply once by 2^(target_biased - 1) for exact single IEEE rounding.
    let mantissa = bits & 0x000f_ffff_ffff_ffff;
    let min_normal = f64::from_bits(sign | (1u64 << 52) | mantissa);
    let shift = (target_biased - 1) as i32;
    let scale = f64::from_bits(((1023 + shift) as u64) << 52);
    min_normal * scale
}

/// Computes `m * exp(x) * 2^pow2` without premature overflow or underflow.
///
/// Uses a Cody-Waite high/low split of `ln(2)` so `x = k * ln(2) + r` with
/// `|r| <= ln(2) / 2` preserves full `f64` precision across large exponents.
#[inline]
pub(crate) fn scale_exp_pow2(m: f64, x: f64, pow2: i32) -> f64 {
    if m == 0.0 || !m.is_finite() {
        return m;
    }
    if pow2 == 0 && x.abs() <= 700.0 {
        return m * x.exp();
    }
    // Quick overflow/underflow guard before integer conversion of `k`.
    let est_exp2 = x * std::f64::consts::LOG2_E + f64::from(pow2);
    if est_exp2 > 2200.0 {
        return f64::INFINITY.copysign(m);
    }
    if est_exp2 < -2200.0 {
        return 0.0_f64.copysign(m);
    }

    // Bottom 24 mantissa bits of LN2_HI are zero so `k * LN2_HI` is exact for |k| < 2^29.
    const LN2_HI: f64 = f64::from_bits(0x3fe6_2e42_fe00_0000);
    const LN2_LO: f64 = 1.820_635_998_504_146_2e-9;

    let k = (x * std::f64::consts::LOG2_E).round() as i64;
    let kf = k as f64;
    let r = (x - kf * LN2_HI) - kf * LN2_LO;
    let total_pow2 = k + i64::from(pow2);
    let val = m * r.exp();
    if total_pow2 > 2100 {
        return f64::INFINITY.copysign(val);
    }
    if total_pow2 < -2100 {
        return 0.0_f64.copysign(val);
    }
    ldexp(val, total_pow2 as i32)
}

/// Returns the high (most significant) 32 bits of an IEEE-754 `binary64` value,
/// reinterpreted as a signed integer.
///
/// The ported openlibm algorithms in this module inspect the raw exponent/sign
/// bits of a double via its "high word". This must be derived from the IEEE
/// bit pattern (`f64::to_bits`) rather than from the platform's native byte
/// order: `to_ne_bytes` places the high word at index 0 on big-endian targets
/// and at index 4 on little-endian ones, so indexing a fixed byte range yields
/// the *low* word (and therefore wrong Bessel results) on big-endian hosts.
#[inline]
pub(crate) fn high_word(x: f64) -> i32 {
    (x.to_bits() >> 32) as u32 as i32
}

/// Returns the `(low, high)` 32-bit halves of an IEEE-754 `binary64` value.
///
/// Note the tuple order is `(low, high)` to match the call sites ported from
/// openlibm's `EXTRACT_WORDS(hx, lx, x)` usage. Like [`high_word`], this is
/// defined purely in terms of the IEEE bit pattern and is therefore
/// endianness-independent.
#[inline]
pub(crate) fn split_words(x: f64) -> (i32, i32) {
    let bits = x.to_bits();
    let low = bits as u32 as i32;
    let high = (bits >> 32) as u32 as i32;
    (low, high)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn high_word_matches_ieee_layout() {
        // 1.0 == 0x3FF0_0000_0000_0000
        assert_eq!(high_word(1.0), 0x3FF0_0000);
        assert_eq!(split_words(1.0), (0, 0x3FF0_0000));
    }

    #[test]
    fn high_word_carries_sign_bit() {
        // -1.0 == 0xBFF0_0000_0000_0000; as i32 the high word is negative.
        assert!(high_word(-1.0) < 0);
        assert_eq!(high_word(-1.0) & 0x7fff_ffff, 0x3FF0_0000);
    }

    #[test]
    fn zero_and_infinity_are_recognised() {
        let (lx, hx) = split_words(0.0);
        assert_eq!(lx | hx, 0, "+0.0 must have all-zero words");

        let (lx, hx) = split_words(f64::INFINITY);
        assert_eq!(hx & 0x7fff_ffff, 0x7ff0_0000);
        assert_eq!(lx, 0);
    }

    #[test]
    fn split_words_roundtrips_low_half() {
        let x = f64::from_bits(0x3FF0_0000_DEAD_BEEF);
        let (lx, hx) = split_words(x);
        assert_eq!(lx as u32, 0xDEAD_BEEF);
        assert_eq!(hx, 0x3FF0_0000);
    }

    #[test]
    fn ldexp_handles_normal_subnormal_overflow_and_underflow() {
        assert_eq!(ldexp(1.5, 3), 12.0);
        assert_eq!(ldexp(-1.5, -2), -0.375);
        assert_eq!(ldexp(0.0, 500), 0.0);
        assert!(ldexp(-0.0, 500).is_sign_negative());
        assert!(ldexp(f64::NAN, 10).is_nan());
        assert_eq!(ldexp(f64::INFINITY, -100), f64::INFINITY);
        assert_eq!(ldexp(1.0, 1024), f64::INFINITY);
        assert_eq!(ldexp(-1.0, 1024), f64::NEG_INFINITY);
        // Subnormal output & exact single rounding
        let min_sub = f64::from_bits(1);
        assert_eq!(ldexp(1.0, -1074), min_sub);
        assert_eq!(ldexp(1.0, -1075), 0.0);
        assert_eq!(ldexp(1.5, -1075), min_sub);
        // Subnormal input scaled back up
        assert_eq!(ldexp(min_sub, 1074), 1.0);
        assert_eq!(ldexp(min_sub, 2000), ldexp(1.0, 926));
    }
}
