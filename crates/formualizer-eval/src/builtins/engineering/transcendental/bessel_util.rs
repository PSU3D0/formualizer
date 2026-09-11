pub(crate) const HUGE: f64 = 1e300;
pub(crate) const FRAC_2_SQRT_PI: f64 = 5.641_895_835_477_563e-1;

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
}
