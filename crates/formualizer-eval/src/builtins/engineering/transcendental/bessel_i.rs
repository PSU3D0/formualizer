//! Full double-precision modified Bessel functions of the first kind `I_n(x)`.
//!
//! - For `n = 0, 1`: positive-term power series for `|x| <= 25` and asymptotic
//!   expansion for `|x| > 25`, with `scale_exp_pow2` avoiding premature overflow
//!   near the `f64` boundary (`|x| in [709.78, 713.98]`).
//! - For `n >= 2`: parity `I_n(-x) = (-1)^n I_n(x)`, Debye uniform asymptotic
//!   bounds for constant-time overflow/underflow detection, modified Lentz
//!   continued fraction (CF1) for `I_n(|x|) / I_{n-1}(|x|)`, and strictly
//!   positive downward recurrence with power-of-2 rescaling normalized against
//!   `e^{-|x|} I_0(|x|)`.

use super::bessel_util::{MAX_RECURRENCE_ORDER, scale_exp_pow2};

/// `1 / sqrt(2 * pi)`.
const INV_SQRT_2PI: f64 = 0.398_942_280_401_432_7;

/// Crossover between power series and asymptotic expansion for `I_0` and `I_1`.
const SERIES_ASYMP_CUTOFF: f64 = 25.0;

/// Direct positive-term power series for `I_0(ax)` (`ax >= 0`).
#[inline]
fn i0_series(ax: f64) -> f64 {
    let t = 0.25 * ax * ax;
    let mut sum = 1.0;
    let mut term = 1.0;
    let mut k = 1.0;
    loop {
        term *= t / (k * k);
        sum += term;
        if term <= sum * 1e-17 {
            break;
        }
        k += 1.0;
    }
    sum
}

/// Direct positive-term power series for `I_1(ax)` (`ax >= 0`).
#[inline]
fn i1_series(ax: f64) -> f64 {
    let t = 0.25 * ax * ax;
    let mut sum = 1.0;
    let mut term = 1.0;
    let mut k = 1.0;
    loop {
        term *= t / (k * (k + 1.0));
        sum += term;
        if term <= sum * 1e-17 {
            break;
        }
        k += 1.0;
    }
    0.5 * ax * sum
}

/// Exponentially scaled `e^{-ax} I_0(ax)` for `ax >= 0`.
///
/// Always finite and strictly positive for finite `ax >= 0`; shared with
/// `bessel_k` for Wronskian normalization.
pub(crate) fn bessel_i0_scaled(ax: f64) -> f64 {
    if ax <= SERIES_ASYMP_CUTOFF {
        i0_series(ax) * (-ax).exp()
    } else {
        let inv_8x = 1.0 / (8.0 * ax);
        let mut sum = 1.0;
        let mut term = 1.0;
        for k in 1..100 {
            let kf = k as f64;
            let odd = 2.0 * kf - 1.0;
            let next_term = term * (odd * odd) * inv_8x / kf;
            if next_term > term {
                break;
            }
            term = next_term;
            sum += term;
            if term <= sum * 1e-17 {
                break;
            }
        }
        sum * INV_SQRT_2PI / ax.sqrt()
    }
}

/// Exponentially scaled `e^{-ax} I_1(ax)` for `ax >= 0`.
///
/// Always finite and non-negative for finite `ax >= 0`; shared with `bessel_k`
/// for Wronskian normalization.
pub(crate) fn bessel_i1_scaled(ax: f64) -> f64 {
    if ax == 0.0 {
        return 0.0;
    }
    if ax <= SERIES_ASYMP_CUTOFF {
        i1_series(ax) * (-ax).exp()
    } else {
        let inv_8x = 1.0 / (8.0 * ax);
        let mut sum = 1.0;
        let mut term = 1.0;
        for k in 1..100 {
            let kf = k as f64;
            let odd = 2.0 * kf - 1.0;
            let next_term = term * (odd * odd - 4.0) * inv_8x / kf;
            if k > 2 && next_term.abs() > term.abs() {
                break;
            }
            term = next_term;
            sum += term;
            if term.abs() <= sum.abs() * 1e-17 {
                break;
            }
        }
        sum * INV_SQRT_2PI / ax.sqrt()
    }
}

pub(crate) fn bessel_i0(x: f64) -> f64 {
    if x.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 {
        return 1.0;
    }
    if x.is_infinite() {
        return f64::INFINITY;
    }
    let ax = x.abs();
    if ax <= SERIES_ASYMP_CUTOFF {
        i0_series(ax)
    } else {
        scale_exp_pow2(bessel_i0_scaled(ax), ax, 0)
    }
}

pub(crate) fn bessel_i1(x: f64) -> f64 {
    if x.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 {
        return 0.0;
    }
    if x.is_infinite() {
        return f64::INFINITY.copysign(x);
    }
    let ax = x.abs();
    let res = if ax <= SERIES_ASYMP_CUTOFF {
        i1_series(ax)
    } else {
        scale_exp_pow2(bessel_i1_scaled(ax), ax, 0)
    };
    if x < 0.0 { -res } else { res }
}

/// Leading Debye uniform asymptotic approximation to `ln(I_n(ax))` (`n >= 2`, `ax > 0`).
///
/// Accurate to within ~0.02 across all `n >= 2, ax > 0`, allowing constant-time
/// guaranteed overflow (`> 715`) and underflow (`< -750`) detection.
#[inline]
fn debye_log_i(n: i32, ax: f64) -> f64 {
    let nf = f64::from(n);
    let r = nf.hypot(ax);
    r + nf * (ax.ln() - (nf + r).ln()) - 0.5 * (2.0 * std::f64::consts::PI * r).ln()
}

/// Evaluates `I_n(ax) / I_{n-1}(ax)` via modified Lentz on the strictly-positive
/// continued fraction CF1: `1 / (b_1 + 1 / (b_2 + ...))`, `b_k = 2(n + k - 1) / ax`.
fn cf1_ratio(n: i32, ax: f64) -> f64 {
    const TINY: f64 = 1e-300;
    let two_over_x = 2.0 / ax;
    let b1 = f64::from(n) * two_over_x;
    let mut f = b1;
    let mut c = b1;
    let mut d = 0.0;
    let mut k = 1u32;
    loop {
        let bk = (f64::from(n) + f64::from(k)) * two_over_x;
        d += bk;
        if d.abs() < TINY {
            d = TINY;
        }
        c = bk + 1.0 / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let delta = c * d;
        f *= delta;
        if (delta - 1.0).abs() < 1e-16 || k >= 200_000 {
            break;
        }
        k += 1;
    }
    1.0 / f
}

pub(crate) fn bessel_i(n: i32, x: f64) -> f64 {
    if n < 0 || x.is_nan() {
        return f64::NAN;
    }
    if n == 0 {
        return bessel_i0(x);
    }
    if x == 0.0 {
        return 0.0;
    }
    if x.is_infinite() {
        return if x < 0.0 && (n & 1) == 1 {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    if n == 1 {
        return bessel_i1(x);
    }
    if n > MAX_RECURRENCE_ORDER {
        return f64::NAN;
    }

    let ax = x.abs();
    let est_ln = debye_log_i(n, ax);
    if est_ln > 715.0 {
        return if x < 0.0 && (n & 1) == 1 {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    if est_ln < -750.0 {
        return 0.0;
    }

    let r = cf1_ratio(n, ax);
    if r == 0.0 {
        return 0.0;
    }

    // Strictly positive downward recurrence from n down to 0 with exact power-of-2 rescaling.
    const TWO_512: f64 = f64::from_bits((1023 + 512) << 52);
    const TWO_M512: f64 = f64::from_bits((1023 - 512) << 52);
    let two_over_x = 2.0 / ax;
    let mut y_next = r;
    let mut y_curr = 1.0;
    let mut scale_exp: i32 = 0;

    for k in (1..n).rev() {
        let y_prev = (f64::from(k) * two_over_x) * y_curr + y_next;
        y_next = y_curr;
        y_curr = y_prev;
        if y_curr > TWO_512 {
            y_curr *= TWO_M512;
            y_next *= TWO_M512;
            scale_exp += 512;
        }
    }

    // I_n(ax) / I_0(ax) = (r / y_curr) * 2^(-scale_exp)
    let mut res = if ax <= SERIES_ASYMP_CUTOFF && scale_exp == 0 {
        i0_series(ax) * (r / y_curr)
    } else {
        scale_exp_pow2(bessel_i0_scaled(ax) * (r / y_curr), ax, -scale_exp)
    };

    if x < 0.0 && (n & 1) == 1 {
        res = -res;
    }
    res
}
