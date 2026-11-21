//! Full double-precision modified Bessel functions of the second kind `K_n(x)`.
//!
//! - For `0 < x <= 1`: direct power series for `K_0(x)` and `K_1(x)` using the
//!   Euler-Mascheroni constant and harmonic numbers (zero cancellation for `K_0`
//!   since `ln(x/2) + gamma < 0`, and `< 1` bit cancellation for `K_1`).
//! - For `x > 1`: modified Lentz continued fraction (CF2) for `K_1(x) / K_0(x)`
//!   combined with the strictly positive Wronskian identity
//!   `e^x K_0(x) = 1 / (x * (e^{-x} I_0(x) * r + e^{-x} I_1(x)))`.
//! - For `n >= 2`: Debye uniform asymptotic bounds for constant-time overflow
//!   and underflow detection, followed by the stable strictly-positive upward
//!   recurrence `K_{j+1}(x) = K_{j-1}(x) + (2j / x) K_j(x)` (exponentially
//!   scaled with power-of-2 rescaling for `x > 1` to avoid premature underflow
//!   or overflow).

use super::bessel_i::{bessel_i0_scaled, bessel_i1_scaled};
use super::bessel_util::{MAX_RECURRENCE_ORDER, scale_exp_pow2};

/// Euler-Mascheroni constant `gamma` (MSRV 1.93 predates `f64::consts::EULER_GAMMA`).
#[allow(clippy::approx_constant)]
const EULER_GAMMA: f64 = 0.577_215_664_901_532_9;

/// Evaluates `(K_0(x), K_1(x))` via direct power series for `0 < x <= 1.0`.
fn bessel_k0_k1_small(x: f64) -> (f64, f64) {
    let t = 0.25 * x * x;
    // Compute `ln(x) - ln(2)` instead of `(x / 2).ln()` so subnormal `x` does not underflow.
    let log_term = x.ln() - std::f64::consts::LN_2 + EULER_GAMMA;

    let mut i0_sum = 1.0;
    let mut k0_harm = 0.0;
    let mut i1_sum = 1.0;
    let mut k1_harm = 1.0; // k = 0 term: H_0 + H_1 = 1
    let mut term0 = 1.0;
    let mut term1 = 1.0;
    let mut h_k = 0.0;
    let mut k = 1.0;

    loop {
        term0 *= t / (k * k);
        h_k += 1.0 / k;
        i0_sum += term0;
        k0_harm += h_k * term0;

        term1 *= t / (k * (k + 1.0));
        let h_kp1 = h_k + 1.0 / (k + 1.0);
        i1_sum += term1;
        k1_harm += (h_k + h_kp1) * term1;

        if term0 <= i0_sum * 1e-17 && term1 <= i1_sum * 1e-17 {
            break;
        }
        k += 1.0;
    }

    let k0 = -log_term * i0_sum + k0_harm;
    let k1 = 1.0 / x + log_term * (0.5 * x * i1_sum) - 0.25 * x * k1_harm;
    (k0, k1)
}

/// Evaluates exponentially scaled `(e^x K_0(x), e^x K_1(x))` for `x > 1.0`
/// using CF2 for `K_1(x) / K_0(x)` and the Wronskian with `e^{-x} I_0, e^{-x} I_1`.
fn bessel_k0_k1_scaled(x: f64) -> (f64, f64) {
    const TINY: f64 = 1e-300;
    let a1 = -0.25;
    let b1 = 2.0 * (x + 1.0);
    let mut f = b1;
    let mut c = b1;
    let mut d = 0.0;

    for k in 2..10_000 {
        let km = f64::from(k) - 0.5;
        let ak = -(km * km);
        let bk = 2.0 * (x + f64::from(k));
        d = bk + ak * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = bk + ak / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let delta = c * d;
        f *= delta;
        if (delta - 1.0).abs() < 1e-16 {
            break;
        }
    }

    let ratio = (x + 0.5 + a1 / f) / x;
    let i0e = bessel_i0_scaled(x);
    let i1e = bessel_i1_scaled(x);
    let k0e = 1.0 / (x * (i0e * ratio + i1e));
    let k1e = ratio * k0e;
    (k0e, k1e)
}

/// Leading Debye uniform asymptotic approximation to `ln(K_n(x))` (`n >= 2`, `x > 0`).
///
/// Accurate to within ~0.02 across `n >= 2, x > 0`, allowing constant-time
/// guaranteed overflow (`> 715`) and underflow (`< -750`) detection.
#[inline]
fn debye_log_k(n: i32, x: f64) -> f64 {
    let nf = f64::from(n);
    let r = nf.hypot(x);
    -r - nf * (x.ln() - (nf + r).ln()) + 0.5 * (std::f64::consts::FRAC_PI_2 / r).ln()
}

pub(crate) fn bessel_k(n: i32, x: f64) -> f64 {
    if n < 0 || x.is_nan() || x <= 0.0 {
        return f64::NAN;
    }
    if x.is_infinite() {
        return 0.0;
    }
    if n > MAX_RECURRENCE_ORDER {
        return f64::NAN;
    }

    if x <= 1.0 {
        if n >= 2 && debye_log_k(n, x) > 715.0 {
            return f64::INFINITY;
        }
        let (k0, k1) = bessel_k0_k1_small(x);
        if n == 0 {
            return k0;
        }
        if n == 1 {
            return k1;
        }
        let two_over_x = 2.0 / x;
        let mut bkm = k0;
        let mut bk = k1;
        for j in 1..n {
            let bkp = bkm + (f64::from(j) * two_over_x) * bk;
            bkm = bk;
            bk = bkp;
            if !bk.is_finite() {
                return f64::INFINITY;
            }
        }
        return bk;
    }

    if n <= 1 {
        if x > 750.0 {
            return 0.0;
        }
        let (k0e, k1e) = bessel_k0_k1_scaled(x);
        return scale_exp_pow2(if n == 0 { k0e } else { k1e }, -x, 0);
    }

    let est_ln = debye_log_k(n, x);
    if est_ln > 715.0 {
        return f64::INFINITY;
    }
    if est_ln < -750.0 {
        return 0.0;
    }

    let (k0e, k1e) = bessel_k0_k1_scaled(x);
    const TWO_512: f64 = f64::from_bits((1023 + 512) << 52);
    const TWO_M512: f64 = f64::from_bits((1023 - 512) << 52);
    let two_over_x = 2.0 / x;
    let mut bkm = k0e;
    let mut bk = k1e;
    let mut scale_exp: i32 = 0;

    for j in 1..n {
        let bkp = bkm + (f64::from(j) * two_over_x) * bk;
        bkm = bk;
        bk = bkp;
        if bk > TWO_512 {
            bk *= TWO_M512;
            bkm *= TWO_M512;
            scale_exp += 512;
        }
    }

    scale_exp_pow2(bk, -x, scale_exp)
}
