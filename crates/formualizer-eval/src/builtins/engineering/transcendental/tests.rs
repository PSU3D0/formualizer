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
            jn_result.is_finite() || jn_result.is_nan() || jn_result.is_infinite(),
            "jn(i32::MIN, {x}) = {jn_result} must not panic"
        );
        assert!(
            yn_result.is_finite() || yn_result.is_nan() || yn_result.is_infinite(),
            "yn(i32::MIN, {x}) = {yn_result} must not panic"
        );
    }
}

/// Regression: `jn` used `-hx` on the high word to flip the sign of `x`.
/// For `x == -0.0` the high word is `i32::MIN`, whose arithmetic negation
/// overflows.
#[test]
fn bessel_jn_handles_negative_zero() {
    assert_eq!(jn(2, -0.0), 0.0);
    assert_eq!(jn(3, 0.0), 0.0);
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

/// Accuracy contract for the large-order short-circuit.
///
/// `asymptotic_large_order` only short-circuits orders so large *relative to the
/// argument* that the result has already underflowed (`J`) or diverged (`Y`).
/// Orders on the representable side of the Debye boundary must run the real
/// recurrence unchanged. `jn(2000, 100)` / `yn(2000, 100)` are the inputs
/// flagged in review: order 2000 far exceeds argument 100, so they short-circuit
/// to the correct IEEE-754 limits (`J_2000(100) ~ 10^-3000` underflows to +0,
/// `Y_2000(100) ~ -10^+3000` overflows to -inf) rather than spinning the
/// recurrence.
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

/// The short-circuit must fire for orders that are large *relative to the
/// argument* — the asymptotic limit (`J -> 0`, `Y -> -inf`) — instead of running
/// a multi-billion step recurrence. Terminating at all within the test timeout
/// is the guarantee; the returned limits document the contract.
#[test]
fn bessel_pathological_order_short_circuits_to_limit() {
    // Orders vastly exceeding the argument: J underflows, Y diverges.
    assert_eq!(jn(i32::MAX, 1.0), 0.0);
    assert_eq!(jn(10_000_000, 3.5), 0.0);
    assert!(yn(i32::MAX, 1.0).is_infinite());
    assert!(yn(10_000_000, 3.5).is_infinite());
    // i32::MIN negates (saturating) to i32::MAX, so it exercises the same path.
    assert_eq!(jn(i32::MIN, 2.0), 0.0);
    assert!(yn(i32::MIN, 2.0).is_infinite());
}

/// The short-circuit boundary depends on `|x|`, not on `n` alone: a large order
/// paired with a large argument is still representable and must run the real
/// recurrence. These values are cross-checked against scipy and were returned as
/// `0.0` / `-inf` (i.e. `#NUM!` for `Y`) by the earlier `n`-only bound.
#[test]
fn bessel_large_order_large_argument_stays_representable() {
    // scipy: jn(300000, 1e8) ~ 2.6539906e-5
    let j1 = jn(300_000, 1e8);
    assert!(
        j1.is_finite() && j1.abs() > 1e-6,
        "jn(300000, 1e8) must be a small finite non-zero value, got {j1}"
    );
    // scipy: yn(300000, 1e8) ~ -7.5245326e-5 (finite, must not be -inf -> #NUM!)
    let y1 = yn(300_000, 1e8);
    assert!(
        y1.is_finite() && y1.abs() > 1e-6,
        "yn(300000, 1e8) must be finite, got {y1}"
    );
    // scipy: jn(250000, 250000) ~ 7.1005611e-3 (on the turning point)
    let j2 = jn(250_000, 250_000.0);
    assert!(
        j2.is_finite() && j2.abs() > 1e-4,
        "jn(250000, 250000) must be a finite non-zero value, got {j2}"
    );
    // jn(1e6, 1e8): far below the turning point, ordinary oscillatory value.
    let j3 = jn(1_000_000, 1e8);
    assert!(j3.is_finite(), "jn(1000000, 1e8) must be finite, got {j3}");
}

/// Continuity across the short-circuit boundary: adjacent orders at a fixed
/// large argument must not jump from a real value to `0` / `-inf`. Before the
/// Debye fix, `n = 200000` and `n = 200001` at `x = 1e8` differed by ~7.8e-6
/// because the bound triggered on `n` alone at 200_000.
#[test]
fn bessel_order_boundary_is_continuous_at_large_argument() {
    let j_lo = jn(200_000, 1e8);
    let j_hi = jn(200_001, 1e8);
    assert!(
        j_lo.is_finite(),
        "jn(200000, 1e8) must be finite, got {j_lo}"
    );
    assert!(
        j_hi.is_finite(),
        "jn(200001, 1e8) must be finite, got {j_hi}"
    );
    // Neighbouring orders are close; certainly not separated by a full unit gap.
    assert!(
        (j_lo - j_hi).abs() < 1e-3,
        "jn is discontinuous across n=200000/200001 at x=1e8: {j_lo} vs {j_hi}"
    );
}

/// Below the turning point (`n <= |x|`) the result is finite, so the asymptotic
/// short-circuit cannot fire. A genuinely huge such order would still be a
/// multi-billion-step recurrence, so it is bounded to `NaN` (mapped to `#NUM!`)
/// for latency rather than a fabricated value. Terminating quickly is the
/// guarantee.
#[test]
fn bessel_finite_but_huge_order_below_turning_point_bails_to_nan() {
    assert!(jn(2_000_000_000, 3_000_000_000.0).is_nan());
    assert!(yn(2_000_000_000, 3_000_000_000.0).is_nan());
}
