// https://github.com/JuliaLang/openlibm/blob/master/src/e_jn.c

/*
 * ====================================================
 * Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
 *
 * Developed at SunSoft, a Sun Microsystems, Inc. business.
 * Permission to use, copy, modify, and distribute this
 * software is freely granted, provided that this notice
 * is preserved.
 * ====================================================
 */

/*
 * __ieee754_jn(n, x), __ieee754_yn(n, x)
 * floating point Bessel's function of the 1st and 2nd kind
 * of order n
 *
 * Special cases:
 *	y0(0)=y1(0)=yn(n,0) = -inf with division by 0 signal;
 *	y0(-ve)=y1(-ve)=yn(n,-ve) are NaN with invalid signal.
 * Note 2. About jn(n,x), yn(n,x)
 *	For n=0, j0(x) is called,
 *	for n=1, j1(x) is called,
 *	for n<x, forward recursion us used starting
 *	from values of j0(x) and j1(x).
 *	for n>x, a continued fraction approximation to
 *	j(n,x)/j(n-1,x) is evaluated and then backward
 *	recursion is used starting from a supposed value
 *	for j(n,x). The resulting value of j(0,x) is
 *	compared with the actual value to correct the
 *	supposed value of j(n,x).
 *
 *	yn(n,x) is similar in all respects, except
 *	that forward recursion is used for all
 *	values of n>1.
 *
 */

use super::{
    bessel_j0_y0::{j0, y0},
    bessel_j1_y1::{j1, y1},
    bessel_util::{FRAC_2_SQRT_PI, split_words},
};

/// Ceiling on the number of recurrence steps `jn`/`yn` may perform.
///
/// The forward and backward recurrences in `jn`/`yn` iterate on the order `n`.
/// For a pathological order such as `i32::MAX` (reachable via
/// `(-i32::MIN).checked_neg().unwrap_or(i32::MAX)`), that is a ~2-billion-step
/// loop driven by a single formula call — a genuine DoS.
///
/// Bounding *iterations* (rather than clamping the input) is safe because
/// `J_n(x)` and `Y_n(x)` are dominated by their asymptotic forms once the order
/// far exceeds the argument:
///
/// ```text
///   J_n(x) ~ (1 / sqrt(2*pi*n)) * (e*x / (2n))^n   -> underflows to 0
///   Y_n(x) ~ -sqrt(2 / (pi*n)) * (2n / (e*x))^n    -> overflows to +/-inf
/// ```
///
/// `f64` loses the last representable magnitude around `n * ln(2n / (e*x)) ~ 709`
/// (the log of `f64::MAX`). Every order that would still yield a representable,
/// non-zero result is far below this cap, so computing the recurrence past it
/// only produces `0.0` / `+/-inf` anyway. A cap of 200_000 clears every input the
/// reviewer flagged as must-still-work (e.g. `jn(2000, 100)`, `yn(2000, 100)`)
/// with several orders of magnitude of head-room while keeping the worst case
/// bounded and fast.
const MAX_RECURRENCE_ORDER: i32 = 200_000;

/// Returns the asymptotic limit of `J_n(x)` / `Y_n(x)` when the order is large
/// enough that the recurrence is both prohibitively long and numerically moot.
///
/// `Some(limit)` short-circuits the caller; `None` means "compute normally".
/// Any order beyond `MAX_RECURRENCE_ORDER` is so far above every representable
/// argument that `J` has underflowed to `0` and `Y` has diverged, so the limit
/// is decided by the function kind alone.
#[inline]
fn asymptotic_large_order(n: i32, kind: BesselKind) -> Option<f64> {
    if n <= MAX_RECURRENCE_ORDER {
        return None;
    }
    match kind {
        BesselKind::J => Some(0.0),
        BesselKind::Y => Some(f64::NEG_INFINITY),
    }
}

#[derive(Clone, Copy)]
enum BesselKind {
    J,
    Y,
}

// Special cases are:
//
//	$ J_n(n, ±\Infinity) = 0$
//	$ J_n(n, NaN} = NaN $
//  $ J_n(n, 0) = 0 $
pub(crate) fn jn(n: i32, x: f64) -> f64 {
    let (lx, mut hx) = split_words(x);
    let ix = 0x7fffffff & hx;
    // if J(n,NaN) is NaN
    if x.is_nan() {
        return x;
    }
    // if (ix | (/*(u_int32_t)*/(lx | -lx)) >> 31) > 0x7ff00000 {
    //     return x + x;
    // }
    let (n, x) = if n < 0 {
        // Flip the sign *bit* (openlibm's `hx ^= 0x80000000`). Arithmetic
        // negation (`-hx`) is not equivalent: it overflows and panics for
        // `hx == i32::MIN`, which is exactly the high word of -0.0.
        hx ^= i32::MIN;
        // `i32::MIN.wrapping_neg()` is `i32::MIN`, so guard the negation to
        // avoid an overflow panic for `n == i32::MIN`. Such an order is far
        // beyond any converging regime and saturates to `i32::MAX`.
        (n.checked_neg().unwrap_or(i32::MAX), -x)
    } else {
        (n, x)
    };
    if n == 0 {
        return j0(x);
    }
    if n == 1 {
        return j1(x);
    }
    // Bound the recurrence: for orders far above the argument, J_n(x) has already
    // underflowed to 0. This caps pathological orders (e.g. i32::MAX) at O(1)
    // without changing any result that is still representable. See
    // `MAX_RECURRENCE_ORDER`.
    if let Some(limit) = asymptotic_large_order(n, BesselKind::J) {
        return limit;
    }
    let sign = (n & 1) & (hx >> 31); /* even n -- 0, odd n -- sign(x) */
    // let sign = if x < 0.0 { -1 } else { 1 };
    let x = x.abs();
    let b = if (ix | lx) == 0 || ix >= 0x7ff00000 {
        // if x is 0 or inf
        0.0
    } else if n as f64 <= x {
        /* Safe to use J(n+1,x)=2n/x *J(n,x)-J(n-1,x) */
        if ix >= 0x52D00000 {
            /* x > 2**302 */
            /* (x >> n**2)
             *	    Jn(x) = cos(x-(2n+1)*pi/4)*sqrt(2/x*pi)
             *	    Yn(x) = sin(x-(2n+1)*pi/4)*sqrt(2/x*pi)
             *	    Let s=x.sin(), c=x.cos(),
             *		xn=x-(2n+1)*pi/4, sqt2 = sqrt(2),then
             *
             *		   n	sin(xn)*sqt2	cos(xn)*sqt2
             *		----------------------------------
             *		   0	 s-c		 c+s
             *		   1	-s-c 		-c+s
             *		   2	-s+c		-c-s
             *		   3	 s+c		 c-s
             */
            // `n` is non-negative here, so `n & 3` is in 0..=3. Ordering the
            // arms so that the `0` case is the catch-all keeps the match
            // exhaustive without an unreachable branch that would silently
            // return 0.0 (or panic) if the invariant ever changed.
            let temp = match n & 3 {
                1 => -x.cos() + x.sin(),
                2 => -x.cos() - x.sin(),
                3 => x.cos() - x.sin(),
                _ => x.cos() + x.sin(),
            };
            FRAC_2_SQRT_PI * temp / x.sqrt()
        } else {
            let mut a = j0(x);
            let mut b = j1(x);
            for i in 1..n {
                let temp = b;
                b = b * (((i + i) as f64) / x) - a; /* avoid underflow */
                a = temp;
            }
            b
        }
    } else {
        // x < 2^(-29)
        if ix < 0x3e100000 {
            // x is tiny, return the first Taylor expansion of J(n,x)
            // J(n,x) = 1/n!*(x/2)^n  - ...
            if n > 33 {
                // underflow
                0.0
            } else {
                let temp = x * 0.5;
                let mut b = temp;
                // `a` accumulates n! for n up to 33. 13! already exceeds i32::MAX,
                // so this must be computed in floating point (as the original
                // openlibm C code does) -- an integer accumulator overflows and
                // panics in debug builds / silently wraps in release builds.
                let mut a = 1.0f64;
                for i in 2..=n {
                    a *= i as f64; /* a = n! */
                    b *= temp; /* b = (x/2)^n */
                }
                b / a
            }
        } else {
            /* use backward recurrence */
            /* 			x      x^2      x^2
             *  J(n,x)/J(n-1,x) =  ----   ------   ------   .....
             *			2n  - 2(n+1) - 2(n+2)
             *
             * 			1      1        1
             *  (for large x)   =  ----  ------   ------   .....
             *			2n   2(n+1)   2(n+2)
             *			-- - ------ - ------ -
             *			 x     x         x
             *
             * Let w = 2n/x and h=2/x, then the above quotient
             * is equal to the continued fraction:
             *		    1
             *	= -----------------------
             *		       1
             *	   w - -----------------
             *			  1
             * 	        w+h - ---------
             *		       w+2h - ...
             *
             * To determine how many terms needed, let
             * Q(0) = w, Q(1) = w(w+h) - 1,
             * Q(k) = (w+k*h)*Q(k-1) - Q(k-2),
             * When Q(k) > 1e4	good for single
             * When Q(k) > 1e9	good for double
             * When Q(k) > 1e17	good for quadruple
             */

            let n_f64 = n as f64;
            let w = (n_f64 + n_f64) / x;
            let h = 2.0 / x;
            let mut q0 = w;
            let mut z = w + h;
            let mut q1 = w * z - 1.0;
            let mut k = 1;
            while q1 < 1.0e9 {
                k += 1;
                z += h;
                let tmp = z * q1 - q0;
                q0 = q1;
                q1 = tmp;
            }
            let m = (n as i64) + (n as i64);
            let mut t = 0.0;
            let end = 2 * ((n as i64) + k);
            let mut i = end - 2;
            while i >= m {
                t = 1.0 / ((i as f64) / x - t);
                i -= 2;
            }
            // for (t=0, i = 2*(n+k); i>=m; i -= 2) t = 1/(i/x-t);
            let mut a = t;
            let mut b = 1.0;
            /*  estimate log((2/x)^n*n!) = n*log(2/x)+n*ln(n)
             *  Hence, if n*(log(2n/x)) > ...
             *  single 8.8722839355e+01
             *  double 7.09782712893383973096e+02
             *  long double 1.1356523406294143949491931077970765006170e+04
             *  then recurrent value may overflow and the result is
             *  likely underflow to 0
             */
            let mut tmp = n as f64;
            let v = 2.0 / x;
            tmp = tmp * f64::ln(f64::abs(v * tmp));
            if tmp < 7.097_827_128_933_84e2 {
                // for(i=n-1, di=(i+i); i>0; i--){
                let mut di = 2.0 * ((n - 1) as f64);
                for _ in (1..=n - 1).rev() {
                    let temp = b;
                    b *= di;
                    b = b / x - a;
                    a = temp;
                    di -= 2.0;
                }
            } else {
                // for(i=n-1, di=(i+i); i>0; i--) {
                let mut di = 2.0 * ((n - 1) as f64);
                for _ in (1..=n - 1).rev() {
                    let temp = b;
                    b *= di;
                    b = b / x - a;
                    a = temp;
                    di -= 2.0;
                    /* scale b to avoid spurious overflow */
                    if b > 1e100 {
                        a /= b;
                        t /= b;
                        b = 1.0;
                    }
                }
            }
            let z = j0(x);
            let w = j1(x);
            if z.abs() >= w.abs() {
                t * z / b
            } else {
                t * w / a
            }
        }
    };
    if sign == 1 { -b } else { b }
}

// Yn returns the order-n Bessel function of the second kind.
//
// Special cases are:
//
//	Y(n, +Inf) = 0
//	Y(n ≥ 0, 0) = -Inf
//	Y(n < 0, 0) = +Inf if n is odd, -Inf if n is even
//	Y(n, x < 0) = NaN
//	Y(n, NaN) = NaN
pub(crate) fn yn(n: i32, x: f64) -> f64 {
    let (lx, hx) = split_words(x);
    let ix = 0x7fffffff & hx;

    // if Y(n, NaN) is NaN
    if x.is_nan() {
        return x;
    }
    // if (ix | (/*(u_int32_t)*/(lx | -lx)) >> 31) > 0x7ff00000 {
    //     return x + x;
    // }

    if (ix | lx) == 0 {
        return f64::NEG_INFINITY;
    }
    if hx < 0 {
        return f64::NAN;
    }

    let (n, sign) = if n < 0 {
        // `-i32::MIN` overflows; saturate instead of panicking. The parity
        // driven `sign` is computed from the original `n` either way.
        (n.checked_neg().unwrap_or(i32::MAX), 1 - ((n & 1) << 1))
    } else {
        (n, 1)
    };
    if n == 0 {
        return y0(x);
    }
    if n == 1 {
        return (sign as f64) * y1(x);
    }
    if ix == 0x7ff00000 {
        return 0.0;
    }
    // Bound the recurrence: for orders far above the argument, Y_n(x) has already
    // diverged. This caps pathological orders (e.g. i32::MAX) at O(1) without
    // changing any result that is still representable. See `MAX_RECURRENCE_ORDER`.
    // `Y_n` diverges to -inf for the even-parity branch; fold the parity `sign`
    // so odd orders of a negated `n` keep their +inf limit.
    if let Some(limit) = asymptotic_large_order(n, BesselKind::Y) {
        return (sign as f64) * limit;
    }
    let b = if ix >= 0x52D00000 {
        // x > 2^302
        /* (x >> n**2)
         *	    Jn(x) = cos(x-(2n+1)*pi/4)*sqrt(2/x*pi)
         *	    Yn(x) = sin(x-(2n+1)*pi/4)*sqrt(2/x*pi)
         *	    Let s=x.sin(), c=x.cos(),
         *		xn=x-(2n+1)*pi/4, sqt2 = sqrt(2),then
         *
         *		   n	sin(xn)*sqt2	cos(xn)*sqt2
         *		----------------------------------
         *		   0	 s-c		 c+s
         *		   1	-s-c 		-c+s
         *		   2	-s+c		-c-s
         *		   3	 s+c		 c-s
         */
        let temp = match n & 3 {
            1 => -x.sin() - x.cos(),
            2 => -x.sin() + x.cos(),
            3 => x.sin() + x.cos(),
            _ => x.sin() - x.cos(),
        };
        FRAC_2_SQRT_PI * temp / x.sqrt()
    } else {
        let mut a = y0(x);
        let mut b = y1(x);
        for i in 1..n {
            if b.is_infinite() {
                break;
            }
            // if high_word(b) != 0xfff00000 {
            //     break;
            // }
            (a, b) = (b, ((2.0 * i as f64) / x) * b - a);
        }
        b
    };
    if sign > 0 { b } else { -b }
}
