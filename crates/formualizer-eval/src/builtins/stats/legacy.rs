//! Excel 2007 compatibility names for the statistical distributions.
//!
//! Excel 2010 replaced these functions with dotted names (`NORM.S.DIST`,
//! `T.DIST.2T`, `CHISQ.DIST.RT`, ...) but still evaluates the old names, and
//! workbooks written by older versions use them. Where the old function has
//! the same argument list and domain rules as its replacement, the replacement
//! carries the old name as an alias. The functions here differ: the old
//! argument list is shorter (`NORMSDIST(z)` is always cumulative), a tail is
//! chosen by an argument (`TDIST`), degrees of freedom are truncated to
//! integers, or the domain errors differ (`HYPGEOMDIST` rejects an infeasible
//! sample count with `#NUM!` instead of returning 0).
//!
//! Each function follows its own Microsoft support page, for example
//! <https://support.microsoft.com/en-us/excel/functions/tdist-function>.

use super::{
    beta_i, beta_inv_helper, chisq_inv, coerce_num, f_inv, gamma_cf, gamma_series, hypgeom_pmf,
    ln_binom, scalar_like_value, std_norm_cdf, t_inv,
};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/// Degrees of freedom at or above this are rejected by CHIDIST, FDIST and FINV.
const MAX_DEG_FREEDOM: f64 = 1e10;

fn number(args: &[ArgumentHandle<'_, '_>], i: usize) -> Result<f64, ExcelError> {
    coerce_num(&scalar_like_value(&args[i])?)
}

fn optional_number(
    args: &[ArgumentHandle<'_, '_>],
    i: usize,
    default: f64,
) -> Result<f64, ExcelError> {
    if args.len() > i {
        number(args, i)
    } else {
        Ok(default)
    }
}

fn finish<'b>(result: Result<f64, ExcelError>) -> Result<CalcValue<'b>, ExcelError> {
    Ok(CalcValue::Scalar(match result {
        Ok(n) if n.is_finite() => LiteralValue::Number(n),
        Ok(_) => LiteralValue::Error(ExcelError::new_num()),
        Err(e) => LiteralValue::Error(e),
    }))
}

fn num_error<T>() -> Result<T, ExcelError> {
    Err(ExcelError::new_num())
}

fn scalar_schema(n: usize) -> Vec<ArgSchema> {
    (0..n).map(|_| ArgSchema::number_lenient_scalar()).collect()
}

/// Upper regularized incomplete gamma `Q(a, x) = 1 - P(a, x)`, computed on the
/// side that does not cancel.
fn gamma_q(a: f64, x: f64) -> f64 {
    if x <= 0.0 {
        1.0
    } else if x < a + 1.0 {
        1.0 - gamma_series(a, x)
    } else {
        gamma_cf(a, x)
    }
}

/// `P(T > x)` for `x >= 0`, from `I_{df/(df+x^2)}(df/2, 1/2) = P(|T| > x)`.
fn t_upper_tail(x: f64, df: f64) -> f64 {
    0.5 * beta_i(df / (df + x * x), df / 2.0, 0.5)
}

/// `P(F > x)` for `x >= 0`, using the complementary beta argument.
fn f_upper_tail(x: f64, d1: f64, d2: f64) -> f64 {
    if x <= 0.0 {
        return 1.0;
    }
    beta_i(d2 / (d2 + d1 * x), d2 / 2.0, d1 / 2.0)
}

/* ─────────────────────────── NORMSDIST ──────────────────────────── */

/// Returns the standard normal cumulative distribution at `z` (legacy name).
///
/// `NORMSDIST(z)` is `NORM.S.DIST(z, TRUE)`: the compatibility form has no
/// `cumulative` argument and always returns the cumulative probability.
///
/// # Remarks
/// - A non-numeric `z` returns `#VALUE!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=NORMSDIST(1.333333)"
/// expected: 0.908788726
/// ```
#[derive(Debug)]
pub struct NormSDistLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: NORMSDIST
/// Type: NormSDistLegacyFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: NORMSDIST(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NormSDistLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NORMSDIST"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(1));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish(number(args, 0).map(std_norm_cdf))
    }
}

/* ─────────────────────────── TDIST ──────────────────────────── */

/// Returns the right-tailed or two-tailed Student's t probability (legacy name).
///
/// `TDIST(x, deg_freedom, 1)` is `P(T > x)` (`T.DIST.RT`) and
/// `TDIST(x, deg_freedom, 2)` is `P(|T| > x)` (`T.DIST.2T`).
///
/// # Remarks
/// - `deg_freedom` and `tails` are truncated to integers.
/// - Returns `#NUM!` when `x < 0`, `deg_freedom < 1`, or `tails` is not 1 or 2.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Two-tailed probability"
/// formula: "=TDIST(1.959999998,60,2)"
/// expected: 0.05464493
/// ```
#[derive(Debug)]
pub struct TDistLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: TDIST
/// Type: TDistLegacyFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: TDIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TDistLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TDIST"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(3));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            let x = number(args, 0)?;
            let df = number(args, 1)?.trunc();
            let tails = number(args, 2)?.trunc();
            if x < 0.0 || df < 1.0 || (tails != 1.0 && tails != 2.0) {
                return num_error();
            }
            Ok(tails * t_upper_tail(x, df))
        })())
    }
}

/* ─────────────────────────── TINV ──────────────────────────── */

/// Returns the two-tailed inverse of the Student's t distribution (legacy name).
///
/// `TINV(probability, deg_freedom)` is the `t` with `P(|T| > t) = probability`,
/// the same as `T.INV.2T`.
///
/// # Remarks
/// - `deg_freedom` is truncated to an integer.
/// - Returns `#NUM!` when `probability <= 0`, `probability > 1`, or `deg_freedom < 1`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Two-tailed critical value"
/// formula: "=TINV(0.05,10)"
/// expected: 2.2281388519649385
/// ```
#[derive(Debug)]
pub struct TInvLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: TINV
/// Type: TInvLegacyFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: TINV(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TInvLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TINV"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(2));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            let p = number(args, 0)?;
            let df = number(args, 1)?.trunc();
            if p <= 0.0 || p > 1.0 || df < 1.0 {
                return num_error();
            }
            if p == 1.0 {
                return Ok(0.0);
            }
            t_inv(1.0 - p / 2.0, df).map_or_else(num_error, Ok)
        })())
    }
}

/* ─────────────────────────── CHIDIST ──────────────────────────── */

/// Returns the right-tailed chi-squared probability (legacy name).
///
/// `CHIDIST(x, deg_freedom)` is `P(X > x)`, the same as `CHISQ.DIST.RT`.
///
/// # Remarks
/// - `deg_freedom` is truncated to an integer.
/// - Returns `#NUM!` when `x < 0`, `deg_freedom < 1`, or `deg_freedom > 10^10`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=CHIDIST(18.307,10)"
/// expected: 0.0500006
/// ```
#[derive(Debug)]
pub struct ChiDistLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: CHIDIST
/// Type: ChiDistLegacyFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: CHIDIST(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ChiDistLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CHIDIST"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(2));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            let x = number(args, 0)?;
            let df = number(args, 1)?.trunc();
            if x < 0.0 || !(1.0..=MAX_DEG_FREEDOM).contains(&df) {
                return num_error();
            }
            Ok(gamma_q(df / 2.0, x / 2.0))
        })())
    }
}

/* ─────────────────────────── CHIINV ──────────────────────────── */

/// Returns the inverse of the right-tailed chi-squared probability (legacy name).
///
/// `CHIINV(probability, deg_freedom)` is the `x` with `CHIDIST(x, deg_freedom) =
/// probability`, the same as `CHISQ.INV.RT`.
///
/// # Remarks
/// - `deg_freedom` is truncated to an integer.
/// - Returns `#NUM!` when `probability` is outside `[0, 1]` or `deg_freedom < 1`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=CHIINV(0.050001,10)"
/// expected: 18.306973
/// ```
#[derive(Debug)]
pub struct ChiInvLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: CHIINV
/// Type: ChiInvLegacyFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: CHIINV(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ChiInvLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CHIINV"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(2));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            let p = number(args, 0)?;
            let df = number(args, 1)?.trunc();
            if !(0.0..=1.0).contains(&p) || !(1.0..=MAX_DEG_FREEDOM).contains(&df) {
                return num_error();
            }
            if p == 1.0 {
                return Ok(0.0);
            }
            // P(X > x) = 0 has no finite solution.
            chisq_inv(1.0 - p, df).map_or_else(num_error, Ok)
        })())
    }
}

/* ─────────────────────────── FDIST ──────────────────────────── */

/// Returns the right-tailed F probability (legacy name).
///
/// `FDIST(x, deg_freedom1, deg_freedom2)` is `P(F > x)`, the same as `F.DIST.RT`.
///
/// # Remarks
/// - Both degrees of freedom are truncated to integers.
/// - Returns `#NUM!` when `x < 0` or either degrees of freedom is `< 1` or `>= 10^10`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=FDIST(15.20686486,6,4)"
/// expected: 0.01
/// ```
#[derive(Debug)]
pub struct FDistLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: FDIST
/// Type: FDistLegacyFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: FDIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FDistLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FDIST"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(3));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            let x = number(args, 0)?;
            let d1 = number(args, 1)?.trunc();
            let d2 = number(args, 2)?.trunc();
            if x < 0.0
                || !(1.0..MAX_DEG_FREEDOM).contains(&d1)
                || !(1.0..MAX_DEG_FREEDOM).contains(&d2)
            {
                return num_error();
            }
            Ok(f_upper_tail(x, d1, d2))
        })())
    }
}

/* ─────────────────────────── FINV ──────────────────────────── */

/// Returns the inverse of the right-tailed F probability (legacy name).
///
/// `FINV(probability, deg_freedom1, deg_freedom2)` is the `x` with
/// `FDIST(x, deg_freedom1, deg_freedom2) = probability`, the same as `F.INV.RT`.
///
/// # Remarks
/// - Both degrees of freedom are truncated to integers.
/// - Returns `#NUM!` when `probability` is outside `[0, 1]` or either degrees of freedom is
///   `< 1` or `>= 10^10`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=FINV(0.01,6,4)"
/// expected: 15.206865
/// ```
#[derive(Debug)]
pub struct FInvLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: FINV
/// Type: FInvLegacyFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: FINV(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FInvLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FINV"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(3));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            let p = number(args, 0)?;
            let d1 = number(args, 1)?.trunc();
            let d2 = number(args, 2)?.trunc();
            if !(0.0..=1.0).contains(&p)
                || !(1.0..MAX_DEG_FREEDOM).contains(&d1)
                || !(1.0..MAX_DEG_FREEDOM).contains(&d2)
            {
                return num_error();
            }
            if p == 1.0 {
                return Ok(0.0);
            }
            f_inv(1.0 - p, d1, d2).map_or_else(num_error, Ok)
        })())
    }
}

/* ─────────────────────────── BETADIST ──────────────────────────── */

/// Returns the cumulative beta distribution (legacy name).
///
/// `BETADIST(x, alpha, beta, [A], [B])` is `BETA.DIST(x, alpha, beta, TRUE, A, B)`: the
/// compatibility form is always cumulative and takes the bounds as its 4th and 5th arguments.
///
/// # Remarks
/// - `A` and `B` default to `0` and `1`.
/// - Returns `#NUM!` when `alpha <= 0`, `beta <= 0`, `x < A`, `x > B`, or `A = B`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=BETADIST(2,8,10,1,3)"
/// expected: 0.6854706
/// ```
#[derive(Debug)]
pub struct BetaDistLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: BETADIST
/// Type: BetaDistLegacyFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: BETADIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BetaDistLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BETADIST"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(5));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            if args.len() > 5 {
                return Err(ExcelError::new_value());
            }
            let x = number(args, 0)?;
            let alpha = number(args, 1)?;
            let beta = number(args, 2)?;
            let lower = optional_number(args, 3, 0.0)?;
            let upper = optional_number(args, 4, 1.0)?;
            if alpha <= 0.0 || beta <= 0.0 || x < lower || x > upper || lower == upper {
                return num_error();
            }
            Ok(beta_i((x - lower) / (upper - lower), alpha, beta))
        })())
    }
}

/* ─────────────────────────── BETAINV ──────────────────────────── */

/// Returns the inverse of the cumulative beta distribution (legacy name).
///
/// `BETAINV(probability, alpha, beta, [A], [B])` is the `x` with
/// `BETADIST(x, alpha, beta, A, B) = probability`.
///
/// # Remarks
/// - `A` and `B` default to `0` and `1`.
/// - Returns `#NUM!` when `alpha <= 0`, `beta <= 0`, `probability <= 0`, `probability > 1`,
///   or `A >= B`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=BETAINV(0.685470581,8,10,1,3)"
/// expected: 2
/// ```
#[derive(Debug)]
pub struct BetaInvLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: BETAINV
/// Type: BetaInvLegacyFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: BETAINV(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BetaInvLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BETAINV"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(5));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            if args.len() > 5 {
                return Err(ExcelError::new_value());
            }
            let p = number(args, 0)?;
            let alpha = number(args, 1)?;
            let beta = number(args, 2)?;
            let lower = optional_number(args, 3, 0.0)?;
            let upper = optional_number(args, 4, 1.0)?;
            if alpha <= 0.0 || beta <= 0.0 || p <= 0.0 || p > 1.0 || lower >= upper {
                return num_error();
            }
            let standard = beta_inv_helper(p, alpha, beta).map_or_else(num_error, Ok)?;
            Ok(lower + standard * (upper - lower))
        })())
    }
}

/* ─────────────────────────── LOGNORMDIST ──────────────────────────── */

/// Returns the cumulative log-normal distribution (legacy name).
///
/// `LOGNORMDIST(x, mean, standard_dev)` is `LOGNORM.DIST(x, mean, standard_dev, TRUE)`: the
/// compatibility form has no `cumulative` argument.
///
/// # Remarks
/// - Returns `#NUM!` when `x <= 0` or `standard_dev <= 0`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=LOGNORMDIST(4,3.5,1.2)"
/// expected: 0.0390836
/// ```
#[derive(Debug)]
pub struct LognormDistLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: LOGNORMDIST
/// Type: LognormDistLegacyFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: LOGNORMDIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for LognormDistLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LOGNORMDIST"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(3));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            let x = number(args, 0)?;
            let mean = number(args, 1)?;
            let sd = number(args, 2)?;
            if x <= 0.0 || sd <= 0.0 {
                return num_error();
            }
            Ok(std_norm_cdf((x.ln() - mean) / sd))
        })())
    }
}

/* ─────────────────────────── HYPGEOMDIST ──────────────────────────── */

/// Returns the hypergeometric probability of exactly `sample_s` successes (legacy name).
///
/// `HYPGEOMDIST(sample_s, number_sample, population_s, number_pop)` is the probability mass
/// form of `HYPGEOM.DIST`.
///
/// # Remarks
/// - All arguments are truncated to integers.
/// - Returns `#NUM!` when `sample_s` is negative, above `MIN(number_sample, population_s)`, or
///   below `MAX(0, number_sample - number_pop + population_s)`.
/// - Returns `#NUM!` when `number_sample <= 0`, `number_sample > number_pop`,
///   `population_s <= 0`, `population_s > number_pop`, or `number_pop <= 0`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=HYPGEOMDIST(1,4,8,20)"
/// expected: 0.3632610939112487
/// ```
#[derive(Debug)]
pub struct HypgeomDistLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: HYPGEOMDIST
/// Type: HypgeomDistLegacyFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: HYPGEOMDIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for HypgeomDistLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "HYPGEOMDIST"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(4));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            let sample_s = number(args, 0)?.trunc();
            let number_sample = number(args, 1)?.trunc();
            let population_s = number(args, 2)?.trunc();
            let number_pop = number(args, 3)?.trunc();
            if number_pop <= 0.0
                || number_sample <= 0.0
                || number_sample > number_pop
                || population_s <= 0.0
                || population_s > number_pop
                || sample_s < 0.0
                || sample_s > number_sample.min(population_s)
                || sample_s < (number_sample - number_pop + population_s).max(0.0)
            {
                return num_error();
            }
            Ok(hypgeom_pmf(
                sample_s as i64,
                number_sample as i64,
                population_s as i64,
                number_pop as i64,
            ))
        })())
    }
}

/* ─────────────────────────── NEGBINOMDIST ──────────────────────────── */

/// Returns the negative binomial probability of `number_f` failures (legacy name).
///
/// `NEGBINOMDIST(number_f, number_s, probability_s)` is the probability mass form of
/// `NEGBINOM.DIST`: the chance of exactly `number_f` failures before the `number_s`-th success.
///
/// # Remarks
/// - `number_f` and `number_s` are truncated to integers.
/// - Returns `#NUM!` when `probability_s < 0`, `probability_s > 1`, `number_f < 0`, or
///   `number_s < 1`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Documented example"
/// formula: "=NEGBINOMDIST(10,5,0.25)"
/// expected: 0.05504866
/// ```
#[derive(Debug)]
pub struct NegbinomDistLegacyFn;
/// [formualizer-docgen:schema:start]
/// Name: NEGBINOMDIST
/// Type: NegbinomDistLegacyFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: NEGBINOMDIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NegbinomDistLegacyFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NEGBINOMDIST"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| scalar_schema(3));
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        finish((|| {
            let failures = number(args, 0)?.trunc();
            let successes = number(args, 1)?.trunc();
            let p = number(args, 2)?;
            if !(0.0..=1.0).contains(&p) || failures < 0.0 || successes < 1.0 {
                return num_error();
            }
            // p^s (1-p)^f with the 0^0 = 1 convention at the endpoints.
            let (f, s) = (failures as i64, successes as i64);
            let ln_p = if p == 0.0 { f64::NEG_INFINITY } else { p.ln() };
            let ln_q_term = if failures == 0.0 {
                0.0
            } else if p == 1.0 {
                f64::NEG_INFINITY
            } else {
                failures * (1.0 - p).ln()
            };
            Ok((ln_binom(f + s - 1, s - 1) + successes * ln_p + ln_q_term).exp())
        })())
    }
}

pub(super) fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_builtin(Arc::new(NormSDistLegacyFn));
    crate::function_registry::register_builtin(Arc::new(TDistLegacyFn));
    crate::function_registry::register_builtin(Arc::new(TInvLegacyFn));
    crate::function_registry::register_builtin(Arc::new(ChiDistLegacyFn));
    crate::function_registry::register_builtin(Arc::new(ChiInvLegacyFn));
    crate::function_registry::register_builtin(Arc::new(FDistLegacyFn));
    crate::function_registry::register_builtin(Arc::new(FInvLegacyFn));
    crate::function_registry::register_builtin(Arc::new(BetaDistLegacyFn));
    crate::function_registry::register_builtin(Arc::new(BetaInvLegacyFn));
    crate::function_registry::register_builtin(Arc::new(LognormDistLegacyFn));
    crate::function_registry::register_builtin(Arc::new(HypgeomDistLegacyFn));
    crate::function_registry::register_builtin(Arc::new(NegbinomDistLegacyFn));
}
