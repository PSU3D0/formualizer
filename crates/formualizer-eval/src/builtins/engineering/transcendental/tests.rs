use super::{
    bessel_i::bessel_i,
    bessel_j0_y0::{j0, y0},
    bessel_j1_y1::j1,
    bessel_jn_yn::{jn, yn},
    bessel_k::bessel_k,
};

const EPS: f64 = 1e-13;
const EPS_LOW: f64 = 1e-6;

// Known values computed with Arb via Nemo.jl in Julia
// You can also use Mathematica
// But please do not use Excel or any other software without arbitrary precision

fn numbers_are_close(a: f64, b: f64) -> bool {
    if a == b {
        // avoid underflow if a = b = 0.0
        return true;
    }
    (a - b).abs() / ((a * a + b * b).sqrt()) < EPS
}

fn numbers_are_somewhat_close(a: f64, b: f64) -> bool {
    if a == b {
        // avoid underflow if a = b = 0.0
        return true;
    }
    (a - b).abs() / ((a * a + b * b).sqrt()) < EPS_LOW
}

#[test]
fn bessel_j0_known_values() {
    let cases = [
        (2.4, 0.002507683297243813),
        (0.5, 0.9384698072408129),
        (1.0, 0.7651976865579666),
        (1.12345, 0.7084999488947348),
        (27.0, 0.07274191800588709),
        (33.0, 0.09727067223550946),
        (2e-4, 0.9999999900000001),
        (0.0, 1.0),
        (1e10, 2.175591750246892e-6),
    ];
    for (value, known) in cases {
        let f = j0(value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for j0({value})"
        );
    }
}

#[test]
fn bessel_y0_known_values() {
    let cases = [
        (2.4, 0.5104147486657438),
        (0.5, -0.4445187335067065),
        (1.0, 0.08825696421567692),
        (1.12345, 0.1783162909790613),
        (27.0, 0.1352149762078722),
        (33.0, 0.0991348255208796),
        (2e-4, -5.496017824512429),
        (1e10, -7.676508175792937e-6),
        (1e-300, -439.8351636227653),
    ];
    for (value, known) in cases {
        let f = y0(value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for y0({value})"
        );
    }
    assert!(y0(0.0).is_infinite());
}

#[test]
fn bessel_j1_known_values() {
    // Values computed with Maxima, the computer algebra system
    // TODO: Recompute
    let cases = [
        (2.4, 0.5201852681819311),
        (0.5, 0.2422684576748738),
        (1.0, 0.4400505857449335),
        (1.17232, 0.4910665691824317),
        (27.5, 0.1521418932046569),
        (42.0, -0.04599388822188721),
        (3e-5, 1.499999999831249E-5),
        (350.0, -0.02040531295214455),
        (0.0, 0.0),
        (1e12, -7.913802683850441e-7),
    ];
    for (value, known) in cases {
        let f = j1(value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for j1({value})"
        );
    }
}

#[test]
fn bessel_jn_known_values() {
    // Values computed with Maxima, the computer algebra system
    // TODO: Recompute
    let cases = [
        (3, 0.5, 0.002_563_729_994_587_244),
        (4, 0.5, 0.000_160_736_476_364_287_6),
        (-3, 0.5, -0.002_563_729_994_587_244),
        (-4, 0.5, 0.000_160_736_476_364_287_6),
        (3, 30.0, 0.129211228759725),
        (-3, 30.0, -0.129211228759725),
        (4, 30.0, -0.052609000321320355),
        (20, 30.0, 0.0048310199934040645),
        (7, 0.0, 0.0),
    ];
    for (n, value, known) in cases {
        let f = jn(n, value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for jn({n}, {value})"
        );
    }
}

#[test]
fn bessel_yn_known_values() {
    let cases = [
        (3, 0.5, -42.059494304723883),
        (4, 0.5, -499.272_560_819_512_3),
        (-3, 0.5, 42.059494304723883),
        (-4, 0.5, -499.272_560_819_512_3),
        (3, 35.0, -0.13191405300596323),
        (-12, 12.2, -0.310438011314211),
        (7, 1e12, 1.016_712_505_197_956_3e-7),
        (35, 3.0, -6.895_879_073_343_495e31),
    ];
    for (n, value, known) in cases {
        let f = yn(n, value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for yn({n}, {value})"
        );
    }
}

#[test]
fn bessel_in_known_values() {
    let cases = [
        (1, 0.5, 0.2578943053908963),
        (3, 0.5, 0.002645111968990286),
        (7, 0.2, 1.986608521182497e-11),
        (7, 0.0, 0.0),
        (0, -0.5, 1.0634833707413236),
        // worse case scenario
        (0, 3.7499, 9.118167894541882),
        (0, 3.7501, 9.119723897590003),
    ];
    for (n, value, known) in cases {
        let f = bessel_i(n, value);
        assert!(
            numbers_are_somewhat_close(f, known),
            "Got: {f}, expected: {known} for in({n}, {value})"
        );
    }
}

#[test]
fn bessel_kn_known_values() {
    let cases = [
        (1, 0.5, 1.656441120003301),
        (0, 0.5, 0.9244190712276659),
        (3, 0.5, 62.05790952993026),
    ];
    for (n, value, known) in cases {
        let f = bessel_k(n, value);
        assert!(
            numbers_are_somewhat_close(f, known),
            "Got: {f}, expected: {known} for kn({n}, {value})"
        );
    }
}

/// Regression: the tiny-`x` Taylor branch of `jn` accumulated `n!` in an `i32`.
/// `13!` already overflows `i32`, so any order in `13..=33` combined with a
/// very small `x` panicked in debug builds (and silently wrapped, producing
/// garbage, in release builds).
#[test]
fn bessel_jn_tiny_x_high_order_does_not_overflow() {
    // x < 2^-29 selects the Taylor branch.
    let x = 1e-12;
    for n in 2..=33 {
        let f = jn(n, x);
        assert!(f.is_finite(), "jn({n}, {x}) must be finite, got {f}");
        // (x/2)^n / n! underflows to +0 well before n = 33.
        assert!(f >= 0.0, "jn({n}, {x}) must be non-negative, got {f}");
    }
}

/// Regression: `jn`/`yn` negated `n` with `-n`, which overflows for
/// `i32::MIN`, and `jn` negated the high word with `-hx`, which overflows for
/// the high word of `-0.0`.
#[test]
fn bessel_extreme_orders_do_not_panic() {
    for x in [0.5f64, 1.0, 30.0] {
        let jn_result = jn(i32::MIN, x);
        let yn_result = yn(i32::MIN, x);
        assert!(
            jn_result.is_nan(),
            "excessive recurrence must fail explicitly"
        );
        assert!(
            yn_result.is_nan(),
            "excessive recurrence must fail explicitly"
        );
    }
}

/// Regression: `jn` used `-hx` on the high word to flip the sign of `x`.
/// For `x == -0.0` the high word is `i32::MIN`, whose arithmetic negation
/// overflows.
#[test]
fn bessel_jn_handles_negative_zero() {
    for (n, x, negative) in [
        (2, -0.0, false),
        (3, -0.0, true),
        (-3, 0.0, true),
        (-3, -0.0, false),
        (i32::MIN, -0.0, false),
    ] {
        let result = jn(n, x);
        assert_eq!(result, 0.0);
        assert_eq!(result.is_sign_negative(), negative);
    }
}

/// `y0`/`yn` rely on the *high* word of the IEEE bit pattern to detect zero,
/// infinity and the sign. Sanity-check those boundaries, which silently broke
/// when the words were extracted via native byte order.
#[test]
fn bessel_y_special_values() {
    assert!(yn(2, 0.0).is_infinite() && yn(2, 0.0) < 0.0);
    assert!(yn(2, -1.0).is_nan());
    assert_eq!(yn(2, f64::INFINITY), 0.0);
    assert!(yn(2, f64::NAN).is_nan());
}

/// Genuine underflow/overflow inside the admitted recurrence still has its
/// ordinary IEEE result. This is not a reason to synthesize limits elsewhere.
#[test]
fn bessel_moderate_orders_run_real_recurrence() {
    let j = jn(2000, 100.0);
    let y = yn(2000, 100.0);
    // J underflows toward zero from above: a non-negative, non-NaN result.
    assert!(!j.is_nan(), "jn(2000, 100) must not be NaN, got {j}");
    assert!(j >= 0.0, "jn(2000, 100) = {j} must be non-negative");
    // Y diverges negative: either a large finite negative or -inf, never NaN/positive.
    assert!(!y.is_nan(), "yn(2000, 100) must not be NaN, got {y}");
    assert!(y < 0.0, "yn(2000, 100) = {y} must be negative");
}

#[test]
fn bessel_excessive_recurrence_returns_failure_not_fabricated_limits() {
    for n in [1_000_001, i32::MAX, i32::MIN] {
        for x in [1.0, 3.5, 3e9] {
            assert!(jn(n, x).is_nan());
            assert!(yn(n, x).is_nan());
        }
    }
}

#[test]
fn bessel_constant_time_paths_precede_work_limit_and_preserve_parity() {
    for n in [i32::MIN, i32::MAX, 2_000_000] {
        assert_eq!(jn(n, f64::INFINITY), 0.0);
        assert_eq!(jn(n, f64::NEG_INFINITY), 0.0);
        assert_eq!(yn(n, f64::INFINITY), 0.0);
        assert!(jn(n, f64::NAN).is_nan());
        assert!(yn(n, f64::NAN).is_nan());
        assert!(yn(n, -1.0).is_nan());
        // The existing tiny-x bound is distinct from an order-only heuristic.
        assert_eq!(jn(n, 1e-12), 0.0);
        // Huge x uses the existing constant-time phase approximation. With
        // i32::MIN the magnitude is even, not the odd saturated i32::MAX.
        let parity = n.unsigned_abs() % 4;
        let sign = if n < 0 && parity % 2 != 0 { -1.0 } else { 1.0 };
        assert_eq!(jn(n, 1e100), sign * jn(parity as i32, 1e100));
        assert_eq!(yn(n, 1e100), sign * yn(parity as i32, 1e100));
    }
    assert_eq!(yn(-3, 0.0), f64::INFINITY);
    assert_eq!(yn(-2, -0.0), f64::NEG_INFINITY);
    assert_eq!(yn(i32::MIN, 0.0), f64::NEG_INFINITY);
}

#[test]
fn bessel_representable_values_are_not_discarded() {
    // mpmath 80-digit references; compare ratios to retain subnormal sensitivity
    // and avoid overflowing a subtraction of large opposite-sign values.
    for (actual, expected) in [
        (jn(100, 0.06), 5.522_273_948_726_5e-311),
        (yn(100, 0.06), -5.76410997427483e307),
        (jn(13, 1e-12), 1.9603324996120135e-170),
    ] {
        assert!(
            (actual / expected - 1.0).abs() < 2e-12,
            "{actual:e} vs {expected:e}"
        );
    }
    // Independent SciPy 1.18.1/AMOS references around the old cutoff and at
    // the new work boundary. These assert values, not merely finite/nonzero.
    for (n, x, j, y) in [
        (
            200_000,
            200_000.0,
            0.007648847543722423,
            -0.013248192594800937,
        ),
        (
            200_001,
            200_000.0,
            0.007528721867113492,
            -0.013456282866366336,
        ),
        (
            250_000,
            250_000.0,
            0.00710056107183997,
            -0.012298532559165735,
        ),
        (
            1_000_000,
            1_000_000.0,
            0.004473073183377776,
            -0.007747590021617347,
        ),
    ] {
        assert!((jn(n, x) / j - 1.0).abs() < 1e-10);
        assert!((yn(n, x) / y - 1.0).abs() < 1e-10);
    }
    // Large oscillatory arguments retain the existing recurrence. The wider
    // tolerance acknowledges disagreement with AMOS here, not a precision fix.
    assert!((jn(300_000, 1e8) / 2.6539905910125003e-5 - 1.0).abs() < 1e-6);
    assert!((yn(300_000, 1e8) / -7.524532638187288e-5 - 1.0).abs() < 1e-6);
}
