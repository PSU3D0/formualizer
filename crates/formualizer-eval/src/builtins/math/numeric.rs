use super::super::utils::{
    ARG_NUM_LENIENT_ONE, ARG_NUM_LENIENT_TWO, ARG_RANGE_NUM_LENIENT_ONE, coerce_num,
};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

#[derive(Debug)]
pub struct AbsFn;
/// Returns the absolute value of a number.
///
/// # Remarks
/// - Negative numbers are returned as positive values.
/// - Zero and positive numbers are unchanged.
/// - Errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Absolute value of a negative number"
/// formula: "=ABS(-12.5)"
/// expected: 12.5
/// ```
///
/// ```yaml,sandbox
/// title: "Absolute value from a cell reference"
/// grid:
///   A1: -42
/// formula: "=ABS(A1)"
/// expected: 42
/// ```
///
/// ```yaml,docs
/// related:
///   - SIGN
///   - INT
///   - MOD
/// faq:
///   - q: "How does ABS handle errors or non-numeric text?"
///     a: "Input errors propagate, and non-coercible text returns a coercion error."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ABS
/// Type: AbsFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ABS(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for AbsFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ABS"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let v = args[0].value()?.into_literal();
        match v {
            LiteralValue::Error(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            other => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                coerce_num(&other)?.abs(),
            ))),
        }
    }
}

#[derive(Debug)]
pub struct SignFn;
/// Returns the sign of a number as -1, 0, or 1.
///
/// # Remarks
/// - Returns `1` for positive numbers.
/// - Returns `-1` for negative numbers.
/// - Returns `0` when input is zero.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Positive input"
/// formula: "=SIGN(12)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Negative input"
/// formula: "=SIGN(-12)"
/// expected: -1
/// ```
///
/// ```yaml,docs
/// related:
///   - ABS
///   - INT
///   - IF
/// faq:
///   - q: "Can SIGN return anything other than -1, 0, or 1?"
///     a: "No. After numeric coercion, the output is always exactly -1, 0, or 1."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: SIGN
/// Type: SignFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: SIGN(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for SignFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SIGN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let v = args[0].value()?.into_literal();
        match v {
            LiteralValue::Error(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            other => {
                let n = coerce_num(&other)?;
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                    if n > 0.0 {
                        1.0
                    } else if n < 0.0 {
                        -1.0
                    } else {
                        0.0
                    },
                )))
            }
        }
    }
}

#[derive(Debug)]
pub struct IntFn; // floor toward -inf
/// Rounds a number down to the nearest integer.
///
/// `INT` uses floor semantics, so negative values move farther from zero.
///
/// # Remarks
/// - Equivalent to mathematical floor (`floor(x)`).
/// - Coercion is lenient for numeric-like inputs; invalid values return an error.
/// - Input errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Drop decimal digits from a positive number"
/// formula: "=INT(8.9)"
/// expected: 8
/// ```
///
/// ```yaml,sandbox
/// title: "Floor a negative number"
/// formula: "=INT(-8.9)"
/// expected: -9
/// ```
///
/// ```yaml,docs
/// related:
///   - TRUNC
///   - ROUNDDOWN
///   - FLOOR
/// faq:
///   - q: "Why is INT(-8.9) equal to -9 instead of -8?"
///     a: "INT uses floor semantics, so negative values round toward negative infinity."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: INT
/// Type: IntFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: INT(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IntFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "INT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let v = args[0].value()?.into_literal();
        match v {
            LiteralValue::Error(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            other => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                coerce_num(&other)?.floor(),
            ))),
        }
    }
}

#[derive(Debug)]
pub struct TruncFn; // truncate toward zero
/// Truncates a number toward zero, optionally at a specified digit position.
///
/// # Remarks
/// - If `num_digits` is omitted, truncation is to an integer.
/// - Positive `num_digits` keeps decimal places; negative values zero places to the left.
/// - Passing more than two arguments returns `#VALUE!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Truncate to two decimal places"
/// formula: "=TRUNC(12.3456,2)"
/// expected: 12.34
/// ```
///
/// ```yaml,sandbox
/// title: "Truncate toward zero at the hundreds place"
/// formula: "=TRUNC(-987.65,-2)"
/// expected: -900
/// ```
///
/// ```yaml,docs
/// related:
///   - INT
///   - ROUND
///   - ROUNDDOWN
/// faq:
///   - q: "How does TRUNC differ from INT for negative numbers?"
///     a: "TRUNC removes digits toward zero, while INT floors toward negative infinity."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: TRUNC
/// Type: TruncFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: TRUNC(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TruncFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TRUNC"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() || args.len() > 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let mut n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let digits: i32 = if args.len() == 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)? as i32,
            }
        } else {
            0
        };
        if digits >= 0 {
            let f = 10f64.powi(digits);
            n = (n * f).trunc() / f;
        } else {
            let f = 10f64.powi(-digits);
            n = (n / f).trunc() * f;
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(n)))
    }
}

#[derive(Debug)]
pub struct RoundFn; // ROUND(number, digits)
/// Rounds a number to a specified number of digits.
///
/// # Remarks
/// - Positive `digits` rounds to the right of the decimal point.
/// - Negative `digits` rounds to the left of the decimal point.
/// - Uses standard half-up style rounding from Rust's `round` behavior.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Round to two decimals"
/// formula: "=ROUND(3.14159,2)"
/// expected: 3.14
/// ```
///
/// ```yaml,sandbox
/// title: "Round to nearest hundred"
/// formula: "=ROUND(1234,-2)"
/// expected: 1200
/// ```
///
/// ```yaml,docs
/// related:
///   - ROUNDUP
///   - ROUNDDOWN
///   - MROUND
/// faq:
///   - q: "What does a negative digits argument do in ROUND?"
///     a: "It rounds digits to the left of the decimal point (for example, tens or hundreds)."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ROUND
/// Type: RoundFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: ROUND(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for RoundFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ROUND"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let digits = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)? as i32,
        };
        let f = 10f64.powi(digits.abs());
        let out = if digits >= 0 {
            (n * f).round() / f
        } else {
            (n / f).round() * f
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(out)))
    }
}

#[derive(Debug)]
pub struct RoundDownFn; // toward zero
/// Rounds a number toward zero to a specified number of digits.
///
/// # Remarks
/// - Positive `num_digits` affects decimals; negative values affect digits left of the decimal.
/// - Always reduces magnitude toward zero (unlike `INT` for negatives).
/// - Input errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Trim decimals without rounding up"
/// formula: "=ROUNDDOWN(3.14159,3)"
/// expected: 3.141
/// ```
///
/// ```yaml,sandbox
/// title: "Round down a negative value at the hundreds place"
/// formula: "=ROUNDDOWN(-987.65,-2)"
/// expected: -900
/// ```
///
/// ```yaml,docs
/// related:
///   - ROUND
///   - ROUNDUP
///   - TRUNC
/// faq:
///   - q: "Does ROUNDDOWN always move toward negative infinity?"
///     a: "No. It moves toward zero, which is different from FLOOR-style behavior on negatives."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ROUNDDOWN
/// Type: RoundDownFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: ROUNDDOWN(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for RoundDownFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ROUNDDOWN"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let digits = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)? as i32,
        };
        let f = 10f64.powi(digits.abs());
        let out = if digits >= 0 {
            (n * f).trunc() / f
        } else {
            (n / f).trunc() * f
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(out)))
    }
}

#[derive(Debug)]
pub struct RoundUpFn; // away from zero
/// Rounds a number away from zero to a specified number of digits.
///
/// # Remarks
/// - Positive `num_digits` affects decimals; negative values affect digits left of the decimal.
/// - Any discarded non-zero part increases the magnitude of the result.
/// - Input errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Round up decimals away from zero"
/// formula: "=ROUNDUP(3.14159,3)"
/// expected: 3.142
/// ```
///
/// ```yaml,sandbox
/// title: "Round up a negative value at the hundreds place"
/// formula: "=ROUNDUP(-987.65,-2)"
/// expected: -1000
/// ```
///
/// ```yaml,docs
/// related:
///   - ROUND
///   - ROUNDDOWN
///   - CEILING
/// faq:
///   - q: "What does ROUNDUP do when discarded digits are already zero?"
///     a: "It leaves the value unchanged because no non-zero discarded part remains."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ROUNDUP
/// Type: RoundUpFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: ROUNDUP(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for RoundUpFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ROUNDUP"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let digits = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)? as i32,
        };
        let f = 10f64.powi(digits.abs());
        let mut scaled = if digits >= 0 { n * f } else { n / f };
        if scaled > 0.0 {
            scaled = scaled.ceil();
        } else {
            scaled = scaled.floor();
        }
        let out = if digits >= 0 { scaled / f } else { scaled * f };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(out)))
    }
}

#[derive(Debug)]
pub struct ModFn; // MOD(a,b)
/// Returns the remainder after division, with the sign of the divisor.
///
/// # Remarks
/// - If divisor is `0`, returns `#DIV/0!`.
/// - Result sign follows Excel-style MOD semantics (sign of divisor).
/// - Errors in either argument are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Positive divisor"
/// formula: "=MOD(10,3)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Negative dividend"
/// formula: "=MOD(-3,2)"
/// expected: 1
/// ```
///
/// ```yaml,docs
/// related:
///   - QUOTIENT
///   - INT
///   - GCD
/// faq:
///   - q: "Why can MOD return a positive value for a negative dividend?"
///     a: "MOD follows the sign of the divisor, matching Excel's modulo semantics."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: MOD
/// Type: ModFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: MOD(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ModFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "MOD"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let y = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        if y == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::from_error_string("#DIV/0!"),
            )));
        }
        let m = x % y;
        let mut r = if m == 0.0 {
            0.0
        } else if (y > 0.0 && m < 0.0) || (y < 0.0 && m > 0.0) {
            m + y
        } else {
            m
        };
        if r == -0.0 {
            r = 0.0;
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(r)))
    }
}

/* ───────────────────── Additional Math / Rounding ───────────────────── */

#[derive(Debug)]
pub struct CeilingFn; // CEILING(number, [significance]) legacy semantics simplified
/// Rounds a number up to the nearest multiple of a significance.
///
/// This implementation defaults significance to `1` and normalizes negative significance to positive.
///
/// # Remarks
/// - If `significance` is omitted, `1` is used.
/// - `significance = 0` returns `#DIV/0!`.
/// - Negative significance is treated as its absolute value in this fallback behavior.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Round up to the nearest multiple"
/// formula: "=CEILING(5.1,2)"
/// expected: 6
/// ```
///
/// ```yaml,sandbox
/// title: "Round a negative number toward positive infinity"
/// formula: "=CEILING(-5.1,2)"
/// expected: -4
/// ```
///
/// ```yaml,docs
/// related:
///   - CEILING.MATH
///   - FLOOR
///   - ROUNDUP
/// faq:
///   - q: "What happens if CEILING significance is 0?"
///     a: "It returns #DIV/0! because a zero multiple is invalid."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: CEILING
/// Type: CeilingFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: CEILING(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for CeilingFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CEILING"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() || args.len() > 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let mut sig = if args.len() == 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)?,
            }
        } else {
            1.0
        };
        if sig == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::from_error_string("#DIV/0!"),
            )));
        }
        if sig < 0.0 {
            sig = sig.abs(); /* Excel nuances: #NUM! when sign mismatch; simplified TODO */
        }
        let k = (n / sig).ceil();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            k * sig,
        )))
    }
}

#[derive(Debug)]
pub struct CeilingMathFn; // CEILING.MATH(number,[significance],[mode])
/// Rounds a number up to the nearest integer or multiple using `CEILING.MATH` rules.
///
/// # Remarks
/// - If `significance` is omitted (or passed as `0`), the function uses `1`.
/// - `significance` is treated as a positive magnitude.
/// - For negative numbers, non-zero `mode` rounds away from zero; otherwise it rounds toward positive infinity.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Default behavior for a positive number"
/// formula: "=CEILING.MATH(24.3,5)"
/// expected: 25
/// ```
///
/// ```yaml,sandbox
/// title: "Use mode to round a negative number away from zero"
/// formula: "=CEILING.MATH(-24.3,5,1)"
/// expected: -25
/// ```
///
/// ```yaml,docs
/// related:
///   - CEILING
///   - FLOOR.MATH
///   - ROUNDUP
/// faq:
///   - q: "How does mode affect negative numbers in CEILING.MATH?"
///     a: "With non-zero mode, negatives round away from zero; otherwise they round toward +infinity."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: CEILING.MATH
/// Type: CeilingMathFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: CEILING.MATH(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for CeilingMathFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CEILING.MATH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    } // allow up to 3 handled manually
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() || args.len() > 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let sig = if args.len() >= 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => {
                    let v = coerce_num(&other)?;
                    if v == 0.0 { 1.0 } else { v.abs() }
                }
            }
        } else {
            1.0
        };
        let mode_nonzero = if args.len() == 3 {
            match args[2].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)? != 0.0,
            }
        } else {
            false
        };
        let result = if n >= 0.0 {
            (n / sig).ceil() * sig
        } else if mode_nonzero {
            (n / sig).floor() * sig /* away from zero */
        } else {
            (n / sig).ceil() * sig /* toward +inf (less negative) */
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

#[derive(Debug)]
pub struct FloorFn; // FLOOR(number,[significance])
/// Rounds a number down to the nearest multiple of a significance.
///
/// This implementation defaults significance to `1` and normalizes negative significance to positive.
///
/// # Remarks
/// - If `significance` is omitted, `1` is used.
/// - `significance = 0` returns `#DIV/0!`.
/// - Negative significance is treated as its absolute value in this fallback behavior.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Round down to the nearest multiple"
/// formula: "=FLOOR(5.9,2)"
/// expected: 4
/// ```
///
/// ```yaml,sandbox
/// title: "Round a negative number to a lower multiple"
/// formula: "=FLOOR(-5.9,2)"
/// expected: -6
/// ```
///
/// ```yaml,docs
/// related:
///   - FLOOR.MATH
///   - CEILING
///   - ROUNDDOWN
/// faq:
///   - q: "Why does FLOOR move negative values farther from zero?"
///     a: "FLOOR rounds down to a lower multiple, which is more negative for negative inputs."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: FLOOR
/// Type: FloorFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: FLOOR(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FloorFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FLOOR"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() || args.len() > 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let mut sig = if args.len() == 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)?,
            }
        } else {
            1.0
        };
        if sig == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::from_error_string("#DIV/0!"),
            )));
        }
        if sig < 0.0 {
            sig = sig.abs();
        }
        let k = (n / sig).floor();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            k * sig,
        )))
    }
}

#[derive(Debug)]
pub struct FloorMathFn; // FLOOR.MATH(number,[significance],[mode])
/// Rounds a number down to the nearest integer or multiple using `FLOOR.MATH` rules.
///
/// # Remarks
/// - If `significance` is omitted (or passed as `0`), the function uses `1`.
/// - `significance` is treated as a positive magnitude.
/// - For negative numbers, non-zero `mode` rounds toward zero; otherwise it rounds away from zero.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Default behavior for a positive number"
/// formula: "=FLOOR.MATH(24.3,5)"
/// expected: 20
/// ```
///
/// ```yaml,sandbox
/// title: "Use mode to round a negative number toward zero"
/// formula: "=FLOOR.MATH(-24.3,5,1)"
/// expected: -20
/// ```
///
/// ```yaml,docs
/// related:
///   - FLOOR
///   - CEILING.MATH
///   - ROUNDDOWN
/// faq:
///   - q: "How does mode affect negative numbers in FLOOR.MATH?"
///     a: "With non-zero mode, negatives round toward zero; otherwise they round away from zero."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: FLOOR.MATH
/// Type: FloorMathFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: FLOOR.MATH(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FloorMathFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FLOOR.MATH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() || args.len() > 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let sig = if args.len() >= 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => {
                    let v = coerce_num(&other)?;
                    if v == 0.0 { 1.0 } else { v.abs() }
                }
            }
        } else {
            1.0
        };
        let mode_nonzero = if args.len() == 3 {
            match args[2].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)? != 0.0,
            }
        } else {
            false
        };
        let result = if n >= 0.0 {
            (n / sig).floor() * sig
        } else if mode_nonzero {
            (n / sig).ceil() * sig
        } else {
            (n / sig).floor() * sig
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

#[derive(Debug)]
pub struct SqrtFn; // SQRT(number)
/// Returns the positive square root of a number.
///
/// # Remarks
/// - Input must be greater than or equal to zero.
/// - Negative input returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Square root of a perfect square"
/// formula: "=SQRT(144)"
/// expected: 12
/// ```
///
/// ```yaml,sandbox
/// title: "Square root from a reference"
/// grid:
///   A1: 2
/// formula: "=SQRT(A1)"
/// expected: 1.4142135623730951
/// ```
///
/// ```yaml,docs
/// related:
///   - POWER
///   - SQRTPI
///   - EXP
/// faq:
///   - q: "When does SQRT return #NUM!?"
///     a: "It returns #NUM! for negative inputs because real square roots are undefined there."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: SQRT
/// Type: SqrtFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: SQRT(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for SqrtFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SQRT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        if n < 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            n.sqrt(),
        )))
    }
}

#[derive(Debug)]
pub struct PowerFn; // POWER(number, power)
/// Raises a base number to a specified power.
///
/// # Remarks
/// - Equivalent to exponentiation (`base^exponent`).
/// - Negative bases with fractional exponents return `#NUM!`.
/// - Errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Integer exponent"
/// formula: "=POWER(2,10)"
/// expected: 1024
/// ```
///
/// ```yaml,sandbox
/// title: "Fractional exponent"
/// formula: "=POWER(9,0.5)"
/// expected: 3
/// ```
///
/// ```yaml,docs
/// related:
///   - SQRT
///   - EXP
///   - LN
/// faq:
///   - q: "Why can POWER return #NUM! for negative bases?"
///     a: "Negative bases with fractional exponents are rejected to avoid complex-number results."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: POWER
/// Type: PowerFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: POWER(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for PowerFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "POWER"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let base = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let expv = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        if base < 0.0 && (expv.fract().abs() > 1e-12) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            base.powf(expv),
        )))
    }
}

#[derive(Debug)]
pub struct ExpFn; // EXP(number)
/// Returns Euler's number `e` raised to the given power.
///
/// `EXP` is the inverse of `LN` for positive-domain values.
///
/// # Remarks
/// - Computes `e^x` using floating-point math.
/// - Very large positive inputs may overflow to infinity.
/// - Input errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Compute e to the first power"
/// formula: "=EXP(1)"
/// expected: 2.718281828459045
/// ```
///
/// ```yaml,sandbox
/// title: "Invert LN"
/// formula: "=EXP(LN(5))"
/// expected: 5
/// ```
///
/// ```yaml,docs
/// related:
///   - LN
///   - LOG
///   - LOG10
/// faq:
///   - q: "Can EXP overflow?"
///     a: "Yes. Very large positive inputs can overflow floating-point range."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: EXP
/// Type: ExpFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: EXP(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ExpFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "EXP"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            n.exp(),
        )))
    }
}

#[derive(Debug)]
pub struct LnFn; // LN(number)
/// Returns the natural logarithm of a positive number.
///
/// # Remarks
/// - `number` must be greater than `0`; otherwise the function returns `#NUM!`.
/// - `LN(EXP(x))` returns `x` up to floating-point precision.
/// - Input errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Natural log of e cubed"
/// formula: "=LN(EXP(3))"
/// expected: 3
/// ```
///
/// ```yaml,sandbox
/// title: "Natural log of a fraction"
/// formula: "=LN(0.5)"
/// expected: -0.6931471805599453
/// ```
///
/// ```yaml,docs
/// related:
///   - EXP
///   - LOG
///   - LOG10
/// faq:
///   - q: "Why does LN return #NUM! for 0 or negatives?"
///     a: "Natural logarithm is only defined for strictly positive inputs."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: LN
/// Type: LnFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: LN(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for LnFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        if n <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            n.ln(),
        )))
    }
}

#[derive(Debug)]
pub struct LogFn; // LOG(number,[base]) default base 10
/// Returns the logarithm of a number for a specified base.
///
/// # Remarks
/// - If `base` is omitted, base 10 is used.
/// - `number` must be positive.
/// - `base` must be positive and not equal to 1.
/// - Invalid domains return `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Base-10 logarithm"
/// formula: "=LOG(1000)"
/// expected: 3
/// ```
///
/// ```yaml,sandbox
/// title: "Base-2 logarithm"
/// formula: "=LOG(8,2)"
/// expected: 3
/// ```
///
/// ```yaml,docs
/// related:
///   - LN
///   - LOG10
///   - EXP
/// faq:
///   - q: "Which base values are invalid for LOG?"
///     a: "Base must be positive and not equal to 1; otherwise LOG returns #NUM!."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: LOG
/// Type: LogFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: LOG(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for LogFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LOG"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() || args.len() > 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let base = if args.len() == 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)?,
            }
        } else {
            10.0
        };
        if n <= 0.0 || base <= 0.0 || (base - 1.0).abs() < 1e-12 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            n.log(base),
        )))
    }
}

#[derive(Debug)]
pub struct Log10Fn; // LOG10(number)
/// Returns the base-10 logarithm of a positive number.
///
/// # Remarks
/// - `number` must be greater than `0`; otherwise the function returns `#NUM!`.
/// - `LOG10(POWER(10,x))` returns `x` up to floating-point precision.
/// - Input errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Power of ten to exponent"
/// formula: "=LOG10(1000)"
/// expected: 3
/// ```
///
/// ```yaml,sandbox
/// title: "Log base 10 of a decimal"
/// formula: "=LOG10(0.01)"
/// expected: -2
/// ```
///
/// ```yaml,docs
/// related:
///   - LOG
///   - LN
///   - EXP
/// faq:
///   - q: "When does LOG10 return #NUM!?"
///     a: "It returns #NUM! for non-positive input values."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: LOG10
/// Type: Log10Fn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: LOG10(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Log10Fn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LOG10"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        if n <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            n.log10(),
        )))
    }
}

fn factorial_checked(n: i64) -> Option<f64> {
    if !(0..=170).contains(&n) {
        return None;
    }
    let mut out = 1.0;
    for i in 2..=n {
        out *= i as f64;
    }
    Some(out)
}

#[derive(Debug)]
pub struct QuotientFn;
/// Returns the integer portion of a division result, truncated toward zero.
///
/// # Remarks
/// - Fractional remainder is discarded without rounding.
/// - Dividing by `0` returns `#DIV/0!`.
/// - Input errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Positive quotient"
/// formula: "=QUOTIENT(10,3)"
/// expected: 3
/// ```
///
/// ```yaml,sandbox
/// title: "Negative quotient truncates toward zero"
/// formula: "=QUOTIENT(-10,3)"
/// expected: -3
/// ```
///
/// ```yaml,docs
/// related:
///   - MOD
///   - INT
///   - TRUNC
/// faq:
///   - q: "How is QUOTIENT different from regular division?"
///     a: "It truncates the fractional part toward zero instead of returning a decimal result."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: QUOTIENT
/// Type: QuotientFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: QUOTIENT(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for QuotientFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "QUOTIENT"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let d = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        if d == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (n / d).trunc(),
        )))
    }
}

#[derive(Debug)]
pub struct EvenFn;
/// Rounds a number away from zero to the nearest even integer.
///
/// # Remarks
/// - Values already equal to an even integer stay unchanged.
/// - Positive and negative values both move away from zero.
/// - `0` returns `0`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Round a positive number to even"
/// formula: "=EVEN(3)"
/// expected: 4
/// ```
///
/// ```yaml,sandbox
/// title: "Round a negative number away from zero"
/// formula: "=EVEN(-1.1)"
/// expected: -2
/// ```
///
/// ```yaml,docs
/// related:
///   - ODD
///   - ROUNDUP
///   - MROUND
/// faq:
///   - q: "Does EVEN ever round toward zero?"
///     a: "No. It always rounds away from zero to the nearest even integer."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: EVEN
/// Type: EvenFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: EVEN(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for EvenFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "EVEN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let number = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        if number == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
        }

        let sign = number.signum();
        let mut v = number.abs().ceil() as i64;
        if v % 2 != 0 {
            v += 1;
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            sign * v as f64,
        )))
    }
}

#[derive(Debug)]
pub struct OddFn;
/// Rounds a number away from zero to the nearest odd integer.
///
/// # Remarks
/// - Values already equal to an odd integer stay unchanged.
/// - Positive and negative values both move away from zero.
/// - `0` returns `1`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Round a positive number to odd"
/// formula: "=ODD(2)"
/// expected: 3
/// ```
///
/// ```yaml,sandbox
/// title: "Round a negative number away from zero"
/// formula: "=ODD(-1.1)"
/// expected: -3
/// ```
///
/// ```yaml,docs
/// related:
///   - EVEN
///   - ROUNDUP
///   - INT
/// faq:
///   - q: "Why does ODD(0) return 1?"
///     a: "ODD rounds away from zero to the nearest odd integer, so zero maps to positive one."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ODD
/// Type: OddFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ODD(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for OddFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ODD"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let number = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };

        let sign = if number < 0.0 { -1.0 } else { 1.0 };
        let mut v = number.abs().ceil() as i64;
        if v % 2 == 0 {
            v += 1;
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            sign * v as f64,
        )))
    }
}

#[derive(Debug)]
pub struct SqrtPiFn;
/// Returns the square root of a number multiplied by pi.
///
/// # Remarks
/// - Computes `SQRT(number * PI())`.
/// - `number` must be greater than or equal to `0`; otherwise returns `#NUM!`.
/// - Input errors are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Square root of pi"
/// formula: "=SQRTPI(1)"
/// expected: 1.772453850905516
/// ```
///
/// ```yaml,sandbox
/// title: "Scale before taking square root"
/// formula: "=SQRTPI(4)"
/// expected: 3.544907701811032
/// ```
///
/// ```yaml,docs
/// related:
///   - SQRT
///   - PI
///   - POWER
/// faq:
///   - q: "When does SQRTPI return #NUM!?"
///     a: "It returns #NUM! when the input is negative, because number*PI must be non-negative."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: SQRTPI
/// Type: SqrtPiFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: SQRTPI(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for SqrtPiFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SQRTPI"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        if n < 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (n * std::f64::consts::PI).sqrt(),
        )))
    }
}

#[derive(Debug)]
pub struct MultinomialFn;
/// Returns the multinomial coefficient for one or more values.
///
/// # Remarks
/// - Each input is truncated toward zero before factorial is applied.
/// - Any negative term returns `#NUM!`.
/// - Values that require factorials outside `0..=170` return `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Compute a standard multinomial coefficient"
/// formula: "=MULTINOMIAL(2,3,4)"
/// expected: 1260
/// ```
///
/// ```yaml,sandbox
/// title: "Non-integers are truncated first"
/// formula: "=MULTINOMIAL(1.9,2.2)"
/// expected: 3
/// ```
///
/// ```yaml,docs
/// related:
///   - FACT
///   - COMBIN
///   - PERMUT
/// faq:
///   - q: "Why does MULTINOMIAL return #NUM! for large terms?"
///     a: "If any required factorial falls outside 0..=170, the function returns #NUM!."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: MULTINOMIAL
/// Type: MultinomialFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: MULTINOMIAL(arg1...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for MultinomialFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "MULTINOMIAL"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut values: Vec<i64> = Vec::new();
        for arg in args {
            for value in arg.lazy_values_owned()? {
                let n = match value {
                    LiteralValue::Error(e) => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                    }
                    other => coerce_num(&other)?.trunc() as i64,
                };
                if n < 0 {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new_num(),
                    )));
                }
                values.push(n);
            }
        }

        let sum: i64 = values.iter().sum();
        let num = match factorial_checked(sum) {
            Some(v) => v,
            None => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };

        let mut den = 1.0;
        for n in values {
            let fact = match factorial_checked(n) {
                Some(v) => v,
                None => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new_num(),
                    )));
                }
            };
            den *= fact;
        }

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (num / den).round(),
        )))
    }
}

#[derive(Debug)]
pub struct SeriesSumFn;
/// Evaluates a power series from coefficients, start power, and step.
///
/// # Remarks
/// - Computes `sum(c_i * x^(n + i*m))` in coefficient order.
/// - Coefficients may be supplied as a scalar, array literal, or range.
/// - Errors in `x`, `n`, `m`, or coefficient values are propagated.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Series from an array literal"
/// formula: "=SERIESSUM(2,0,1,{1,2,3})"
/// expected: 17
/// ```
///
/// ```yaml,sandbox
/// title: "Series from worksheet coefficients"
/// grid:
///   A1: 1
///   A2: -1
///   A3: 0.5
/// formula: "=SERIESSUM(0.5,1,2,A1:A3)"
/// expected: 0.390625
/// ```
///
/// ```yaml,docs
/// related:
///   - SUMPRODUCT
///   - POWER
///   - EXP
/// faq:
///   - q: "In what order are SERIESSUM coefficients applied?"
///     a: "Coefficients are consumed in input order as c_i*x^(n+i*m)."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: SERIESSUM
/// Type: SeriesSumFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: SERIESSUM(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for SeriesSumFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SERIESSUM"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::any(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let n = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let m = match args[2].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };

        let mut coeffs: Vec<f64> = Vec::new();
        if let Ok(view) = args[3].range_view() {
            view.for_each_cell(&mut |cell| {
                match cell {
                    LiteralValue::Error(e) => return Err(e.clone()),
                    other => coeffs.push(coerce_num(other)?),
                }
                Ok(())
            })?;
        } else {
            match args[3].value()?.into_literal() {
                LiteralValue::Array(rows) => {
                    for row in rows {
                        for cell in row {
                            match cell {
                                LiteralValue::Error(e) => {
                                    return Ok(crate::traits::CalcValue::Scalar(
                                        LiteralValue::Error(e),
                                    ));
                                }
                                other => coeffs.push(coerce_num(&other)?),
                            }
                        }
                    }
                }
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coeffs.push(coerce_num(&other)?),
            }
        }

        let mut sum = 0.0;
        for (i, c) in coeffs.into_iter().enumerate() {
            sum += c * x.powf(n + (i as f64) * m);
        }

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(sum)))
    }
}

#[derive(Debug)]
pub struct SumsqFn;
/// Returns the sum of squares of supplied numbers.
///
/// # Remarks
/// - Accepts one or more scalar values, arrays, or ranges.
/// - For ranges, non-numeric cells are ignored while errors are propagated.
/// - Date/time-like values in ranges are converted to numeric serial values before squaring.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Sum squares of scalar arguments"
/// formula: "=SUMSQ(3,4)"
/// expected: 25
/// ```
///
/// ```yaml,sandbox
/// title: "Ignore text cells in a range"
/// grid:
///   A1: 1
///   A2: "x"
///   A3: 2
/// formula: "=SUMSQ(A1:A3)"
/// expected: 5
/// ```
///
/// ```yaml,docs
/// related:
///   - SUM
///   - PRODUCT
///   - SUMPRODUCT
/// faq:
///   - q: "How does SUMSQ treat text cells in ranges?"
///     a: "Non-numeric range cells are ignored, while explicit errors are propagated."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: SUMSQ
/// Type: SumsqFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: SUMSQ(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for SumsqFn {
    func_caps!(PURE, REDUCTION, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "SUMSQ"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut total = 0.0;
        for arg in args {
            if let Ok(view) = arg.range_view() {
                view.for_each_cell(&mut |cell| {
                    match cell {
                        LiteralValue::Error(e) => return Err(e.clone()),
                        LiteralValue::Number(n) => total += n * n,
                        LiteralValue::Int(i) => {
                            let n = *i as f64;
                            total += n * n;
                        }
                        LiteralValue::Date(d) => {
                            let n = crate::builtins::datetime::date_to_serial(d);
                            total += n * n;
                        }
                        LiteralValue::DateTime(dt) => {
                            let n = crate::builtins::datetime::datetime_to_serial(dt);
                            total += n * n;
                        }
                        LiteralValue::Time(t) => {
                            let n = crate::builtins::datetime::time_to_fraction(t);
                            total += n * n;
                        }
                        LiteralValue::Duration(d) => {
                            let n = d.num_seconds() as f64 / 86_400.0;
                            total += n * n;
                        }
                        _ => {}
                    }
                    Ok(())
                })?;
            } else {
                let v = arg.value()?.into_literal();
                match v {
                    LiteralValue::Error(e) => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                    }
                    other => {
                        let n = coerce_num(&other)?;
                        total += n * n;
                    }
                }
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            total,
        )))
    }
}

#[derive(Debug)]
pub struct MroundFn;
/// Rounds a number to the nearest multiple.
///
/// # Remarks
/// - Returns `0` when `multiple` is `0`.
/// - If `number` and `multiple` have different signs, returns `#NUM!`.
/// - Midpoints are rounded away from zero.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Round to nearest 5"
/// formula: "=MROUND(17,5)"
/// expected: 15
/// ```
///
/// ```yaml,sandbox
/// title: "Round negative value"
/// formula: "=MROUND(-17,-5)"
/// expected: -15
/// ```
///
/// ```yaml,docs
/// related:
///   - ROUND
///   - CEILING
///   - FLOOR
/// faq:
///   - q: "Why does MROUND return #NUM! for mixed signs?"
///     a: "If number and multiple have different signs (excluding zero), MROUND returns #NUM!."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: MROUND
/// Type: MroundFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: MROUND(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for MroundFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "MROUND"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let number = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let multiple = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };

        if multiple == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
        }
        if number != 0.0 && number.signum() != multiple.signum() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let m = multiple.abs();
        let scaled = number.abs() / m;
        let rounded = (scaled + 0.5 + 1e-12).floor();
        let out = rounded * m * number.signum();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(out)))
    }
}

fn roman_classic(mut n: u32) -> String {
    let table = [
        (1000, "M"),
        (900, "CM"),
        (500, "D"),
        (400, "CD"),
        (100, "C"),
        (90, "XC"),
        (50, "L"),
        (40, "XL"),
        (10, "X"),
        (9, "IX"),
        (5, "V"),
        (4, "IV"),
        (1, "I"),
    ];

    let mut out = String::new();
    for (value, glyph) in table {
        while n >= value {
            n -= value;
            out.push_str(glyph);
        }
    }
    out
}

fn roman_apply_form(classic: String, form: i64) -> String {
    match form {
        0 => classic,
        1 => classic
            .replace("CM", "LM")
            .replace("CD", "LD")
            .replace("XC", "VL")
            .replace("XL", "VL")
            .replace("IX", "IV"),
        2 => roman_apply_form(classic, 1)
            .replace("LD", "XD")
            .replace("LM", "XM")
            .replace("VLIV", "IX"),
        3 => roman_apply_form(classic, 2)
            .replace("XD", "VD")
            .replace("XM", "VM")
            .replace("IX", "IV"),
        4 => roman_apply_form(classic, 3)
            .replace("VDIV", "ID")
            .replace("VMIV", "IM"),
        _ => classic,
    }
}

#[derive(Debug)]
pub struct RomanFn;
/// Converts an Arabic number to a Roman numeral string.
///
/// # Remarks
/// - Accepts integer values in the range `0..=3999`.
/// - `0` returns an empty string.
/// - Optional `form` controls output compactness (`0` classic through `4` simplified).
/// - Out-of-range values return `#VALUE!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Classic Roman numeral"
/// formula: "=ROMAN(1999)"
/// expected: "MCMXCIX"
/// ```
///
/// ```yaml,sandbox
/// title: "Another conversion"
/// formula: "=ROMAN(44)"
/// expected: "XLIV"
/// ```
///
/// ```yaml,docs
/// related:
///   - ARABIC
///   - TEXT
/// faq:
///   - q: "What input range does ROMAN support?"
///     a: "ROMAN accepts truncated integers from 0 through 3999; outside that range it returns #VALUE!."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ROMAN
/// Type: RomanFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: ROMAN(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for RomanFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ROMAN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() > 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let number = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?.trunc() as i64,
        };

        if !(0..=3999).contains(&number) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        if number == 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
                "".to_string(),
            )));
        }

        let form = if args.len() >= 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Boolean(b) => {
                    if b {
                        0
                    } else {
                        4
                    }
                }
                LiteralValue::Number(n) => n.trunc() as i64,
                LiteralValue::Int(i) => i,
                LiteralValue::Empty => 0,
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new_value(),
                    )));
                }
            }
        } else {
            0
        };

        if !(0..=4).contains(&form) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let classic = roman_classic(number as u32);
        let text = roman_apply_form(classic, form);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(text)))
    }
}

fn roman_digit_value(ch: char) -> Option<i64> {
    match ch {
        'I' => Some(1),
        'V' => Some(5),
        'X' => Some(10),
        'L' => Some(50),
        'C' => Some(100),
        'D' => Some(500),
        'M' => Some(1000),
        _ => None,
    }
}

#[derive(Debug)]
pub struct ArabicFn;
/// Converts a Roman numeral string to its Arabic numeric value.
///
/// # Remarks
/// - Accepts text input containing Roman symbols (`I,V,X,L,C,D,M`).
/// - Surrounding whitespace is trimmed.
/// - Empty text returns `0`.
/// - Invalid Roman syntax returns `#VALUE!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Roman to Arabic"
/// formula: "=ARABIC(\"MCMXCIX\")"
/// expected: 1999
/// ```
///
/// ```yaml,sandbox
/// title: "Trimmed input"
/// formula: "=ARABIC(\"  XLIV  \")"
/// expected: 44
/// ```
///
/// ```yaml,docs
/// related:
///   - ROMAN
///   - VALUE
/// faq:
///   - q: "What causes ARABIC to return #VALUE!?"
///     a: "Invalid Roman symbols/syntax, non-text input, or overlength text produce #VALUE!."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ARABIC
/// Type: ArabicFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ARABIC(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ArabicFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ARABIC"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| vec![ArgSchema::any()]);
        &ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let raw = match args[0].value()?.into_literal() {
            LiteralValue::Text(s) => s,
            LiteralValue::Empty => String::new(),
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        };

        let mut text = raw.trim().to_uppercase();
        if text.len() > 255 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        if text.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
        }

        let sign = if text.starts_with('-') {
            text.remove(0);
            -1.0
        } else {
            1.0
        };

        if text.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let mut total = 0i64;
        let mut prev = 0i64;
        for ch in text.chars().rev() {
            let v = match roman_digit_value(ch) {
                Some(v) => v,
                None => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new_value(),
                    )));
                }
            };
            if v < prev {
                total -= v;
            } else {
                total += v;
                prev = v;
            }
        }

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            sign * total as f64,
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(AbsFn));
    crate::function_registry::register_function(Arc::new(SignFn));
    crate::function_registry::register_function(Arc::new(IntFn));
    crate::function_registry::register_function(Arc::new(TruncFn));
    crate::function_registry::register_function(Arc::new(RoundFn));
    crate::function_registry::register_function(Arc::new(RoundDownFn));
    crate::function_registry::register_function(Arc::new(RoundUpFn));
    crate::function_registry::register_function(Arc::new(ModFn));
    crate::function_registry::register_function(Arc::new(CeilingFn));
    crate::function_registry::register_function(Arc::new(CeilingMathFn));
    crate::function_registry::register_function(Arc::new(FloorFn));
    crate::function_registry::register_function(Arc::new(FloorMathFn));
    crate::function_registry::register_function(Arc::new(SqrtFn));
    crate::function_registry::register_function(Arc::new(PowerFn));
    crate::function_registry::register_function(Arc::new(ExpFn));
    crate::function_registry::register_function(Arc::new(LnFn));
    crate::function_registry::register_function(Arc::new(LogFn));
    crate::function_registry::register_function(Arc::new(Log10Fn));
    crate::function_registry::register_function(Arc::new(QuotientFn));
    crate::function_registry::register_function(Arc::new(EvenFn));
    crate::function_registry::register_function(Arc::new(OddFn));
    crate::function_registry::register_function(Arc::new(SqrtPiFn));
    crate::function_registry::register_function(Arc::new(MultinomialFn));
    crate::function_registry::register_function(Arc::new(SeriesSumFn));
    crate::function_registry::register_function(Arc::new(SumsqFn));
    crate::function_registry::register_function(Arc::new(MroundFn));
    crate::function_registry::register_function(Arc::new(RomanFn));
    crate::function_registry::register_function(Arc::new(ArabicFn));
}

#[cfg(test)]
mod tests_numeric {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    // ABS
    #[test]
    fn abs_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AbsFn));
        let ctx = interp(&wb);
        let n = lit(LiteralValue::Number(-5.5));
        let f = ctx.context.get_function("", "ABS").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(5.5)
        );
    }
    #[test]
    fn abs_error_passthrough() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AbsFn));
        let ctx = interp(&wb);
        let e = lit(LiteralValue::Error(ExcelError::from_error_string(
            "#VALUE!",
        )));
        let f = ctx.context.get_function("", "ABS").unwrap();
        match f
            .dispatch(
                &[ArgumentHandle::new(&e, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(er) => assert_eq!(er, "#VALUE!"),
            _ => panic!(),
        }
    }

    // SIGN
    #[test]
    fn sign_neg_zero_pos() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SignFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "SIGN").unwrap();
        let neg = lit(LiteralValue::Number(-3.2));
        let zero = lit(LiteralValue::Int(0));
        let pos = lit(LiteralValue::Int(9));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&neg, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(-1.0)
        );
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&zero, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(0.0)
        );
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&pos, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(1.0)
        );
    }
    #[test]
    fn sign_error_passthrough() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SignFn));
        let ctx = interp(&wb);
        let e = lit(LiteralValue::Error(ExcelError::from_error_string(
            "#DIV/0!",
        )));
        let f = ctx.context.get_function("", "SIGN").unwrap();
        match f
            .dispatch(
                &[ArgumentHandle::new(&e, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(er) => assert_eq!(er, "#DIV/0!"),
            _ => panic!(),
        }
    }

    // INT
    #[test]
    fn int_floor_negative() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IntFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "INT").unwrap();
        let n = lit(LiteralValue::Number(-3.2));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(-4.0)
        );
    }
    #[test]
    fn int_floor_positive() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IntFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "INT").unwrap();
        let n = lit(LiteralValue::Number(3.7));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(3.0)
        );
    }

    // TRUNC
    #[test]
    fn trunc_digits_positive_and_negative() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TruncFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "TRUNC").unwrap();
        let n = lit(LiteralValue::Number(12.3456));
        let d2 = lit(LiteralValue::Int(2));
        let dneg1 = lit(LiteralValue::Int(-1));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&n, &ctx),
                    ArgumentHandle::new(&d2, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(12.34)
        );
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&n, &ctx),
                    ArgumentHandle::new(&dneg1, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(10.0)
        );
    }
    #[test]
    fn trunc_default_zero_digits() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TruncFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "TRUNC").unwrap();
        let n = lit(LiteralValue::Number(-12.999));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(-12.0)
        );
    }

    // ROUND
    #[test]
    fn round_half_away_positive_and_negative() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RoundFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ROUND").unwrap();
        let p = lit(LiteralValue::Number(2.5));
        let n = lit(LiteralValue::Number(-2.5));
        let d0 = lit(LiteralValue::Int(0));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&p, &ctx),
                    ArgumentHandle::new(&d0, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(3.0)
        );
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&n, &ctx),
                    ArgumentHandle::new(&d0, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(-3.0)
        );
    }
    #[test]
    fn round_digits_positive() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RoundFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ROUND").unwrap();
        let n = lit(LiteralValue::Number(1.2345));
        let d = lit(LiteralValue::Int(3));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx), ArgumentHandle::new(&d, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(1.235)
        );
    }

    // ROUNDDOWN
    #[test]
    fn rounddown_truncates() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RoundDownFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ROUNDDOWN").unwrap();
        let n = lit(LiteralValue::Number(1.299));
        let d = lit(LiteralValue::Int(2));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx), ArgumentHandle::new(&d, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(1.29)
        );
    }
    #[test]
    fn rounddown_negative_number() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RoundDownFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ROUNDDOWN").unwrap();
        let n = lit(LiteralValue::Number(-1.299));
        let d = lit(LiteralValue::Int(2));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx), ArgumentHandle::new(&d, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(-1.29)
        );
    }

    // ROUNDUP
    #[test]
    fn roundup_away_from_zero() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RoundUpFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ROUNDUP").unwrap();
        let n = lit(LiteralValue::Number(1.001));
        let d = lit(LiteralValue::Int(2));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx), ArgumentHandle::new(&d, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(1.01)
        );
    }
    #[test]
    fn roundup_negative() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RoundUpFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ROUNDUP").unwrap();
        let n = lit(LiteralValue::Number(-1.001));
        let d = lit(LiteralValue::Int(2));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx), ArgumentHandle::new(&d, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(-1.01)
        );
    }

    // MOD
    #[test]
    fn mod_positive_negative_cases() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ModFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "MOD").unwrap();
        let a = lit(LiteralValue::Int(-3));
        let b = lit(LiteralValue::Int(2));
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Number(1.0));
        let a2 = lit(LiteralValue::Int(3));
        let b2 = lit(LiteralValue::Int(-2));
        let out2 = f
            .dispatch(
                &[
                    ArgumentHandle::new(&a2, &ctx),
                    ArgumentHandle::new(&b2, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out2, LiteralValue::Number(-1.0));
    }
    #[test]
    fn mod_div_by_zero_error() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ModFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "MOD").unwrap();
        let a = lit(LiteralValue::Int(5));
        let zero = lit(LiteralValue::Int(0));
        match f
            .dispatch(
                &[
                    ArgumentHandle::new(&a, &ctx),
                    ArgumentHandle::new(&zero, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            _ => panic!(),
        }
    }

    // SQRT domain
    #[test]
    fn sqrt_basic_and_domain() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SqrtFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "SQRT").unwrap();
        let n = lit(LiteralValue::Number(9.0));
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Number(3.0));
        let neg = lit(LiteralValue::Number(-1.0));
        let out2 = f
            .dispatch(
                &[ArgumentHandle::new(&neg, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap();
        assert!(matches!(out2.into_literal(), LiteralValue::Error(_)));
    }

    #[test]
    fn power_fractional_negative_domain() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(PowerFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "POWER").unwrap();
        let a = lit(LiteralValue::Number(-4.0));
        let half = lit(LiteralValue::Number(0.5));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&a, &ctx),
                    ArgumentHandle::new(&half, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert!(matches!(out.into_literal(), LiteralValue::Error(_))); // complex -> #NUM!
    }

    #[test]
    fn log_variants() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(LogFn))
            .with_function(std::sync::Arc::new(Log10Fn))
            .with_function(std::sync::Arc::new(LnFn));
        let ctx = interp(&wb);
        let logf = ctx.context.get_function("", "LOG").unwrap();
        let log10f = ctx.context.get_function("", "LOG10").unwrap();
        let lnf = ctx.context.get_function("", "LN").unwrap();
        let n = lit(LiteralValue::Number(100.0));
        let base = lit(LiteralValue::Number(10.0));
        assert_eq!(
            logf.dispatch(
                &[
                    ArgumentHandle::new(&n, &ctx),
                    ArgumentHandle::new(&base, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            log10f
                .dispatch(
                    &[ArgumentHandle::new(&n, &ctx)],
                    &ctx.function_context(None)
                )
                .unwrap()
                .into_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            lnf.dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(100.0f64.ln())
        );
    }
    #[test]
    fn ceiling_floor_basic() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(CeilingFn))
            .with_function(std::sync::Arc::new(FloorFn))
            .with_function(std::sync::Arc::new(CeilingMathFn))
            .with_function(std::sync::Arc::new(FloorMathFn));
        let ctx = interp(&wb);
        let c = ctx.context.get_function("", "CEILING").unwrap();
        let f = ctx.context.get_function("", "FLOOR").unwrap();
        let n = lit(LiteralValue::Number(5.1));
        let sig = lit(LiteralValue::Number(2.0));
        assert_eq!(
            c.dispatch(
                &[
                    ArgumentHandle::new(&n, &ctx),
                    ArgumentHandle::new(&sig, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(6.0)
        );
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&n, &ctx),
                    ArgumentHandle::new(&sig, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(4.0)
        );
    }

    #[test]
    fn quotient_basic_and_div_zero() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(QuotientFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "QUOTIENT").unwrap();

        let ten = lit(LiteralValue::Int(10));
        let three = lit(LiteralValue::Int(3));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&ten, &ctx),
                    ArgumentHandle::new(&three, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(3.0)
        );

        let neg_ten = lit(LiteralValue::Int(-10));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&neg_ten, &ctx),
                    ArgumentHandle::new(&three, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(-3.0)
        );

        let zero = lit(LiteralValue::Int(0));
        match f
            .dispatch(
                &[
                    ArgumentHandle::new(&ten, &ctx),
                    ArgumentHandle::new(&zero, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            other => panic!("expected #DIV/0!, got {other:?}"),
        }
    }

    #[test]
    fn even_odd_examples() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(EvenFn))
            .with_function(std::sync::Arc::new(OddFn));
        let ctx = interp(&wb);

        let even = ctx.context.get_function("", "EVEN").unwrap();
        let odd = ctx.context.get_function("", "ODD").unwrap();

        let one_half = lit(LiteralValue::Number(1.5));
        let three = lit(LiteralValue::Int(3));
        let neg_one = lit(LiteralValue::Int(-1));
        let two = lit(LiteralValue::Int(2));
        let zero = lit(LiteralValue::Int(0));

        assert_eq!(
            even.dispatch(
                &[ArgumentHandle::new(&one_half, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            even.dispatch(
                &[ArgumentHandle::new(&three, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(4.0)
        );
        assert_eq!(
            even.dispatch(
                &[ArgumentHandle::new(&neg_one, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(-2.0)
        );
        assert_eq!(
            even.dispatch(
                &[ArgumentHandle::new(&two, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(2.0)
        );

        assert_eq!(
            odd.dispatch(
                &[ArgumentHandle::new(&one_half, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(3.0)
        );
        assert_eq!(
            odd.dispatch(
                &[ArgumentHandle::new(&two, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(3.0)
        );
        assert_eq!(
            odd.dispatch(
                &[ArgumentHandle::new(&neg_one, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(-1.0)
        );
        assert_eq!(
            odd.dispatch(
                &[ArgumentHandle::new(&zero, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(1.0)
        );
    }

    #[test]
    fn sqrtpi_multinomial_and_seriessum_examples() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(SqrtPiFn))
            .with_function(std::sync::Arc::new(MultinomialFn))
            .with_function(std::sync::Arc::new(SeriesSumFn));
        let ctx = interp(&wb);

        let sqrtpi = ctx.context.get_function("", "SQRTPI").unwrap();
        let one = lit(LiteralValue::Int(1));
        match sqrtpi
            .dispatch(
                &[ArgumentHandle::new(&one, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - std::f64::consts::PI.sqrt()).abs() < 1e-12),
            other => panic!("expected numeric SQRTPI, got {other:?}"),
        }

        let multinomial = ctx.context.get_function("", "MULTINOMIAL").unwrap();
        let two = lit(LiteralValue::Int(2));
        let three = lit(LiteralValue::Int(3));
        let four = lit(LiteralValue::Int(4));
        assert_eq!(
            multinomial
                .dispatch(
                    &[
                        ArgumentHandle::new(&two, &ctx),
                        ArgumentHandle::new(&three, &ctx),
                        ArgumentHandle::new(&four, &ctx),
                    ],
                    &ctx.function_context(None),
                )
                .unwrap()
                .into_literal(),
            LiteralValue::Number(1260.0)
        );

        let seriessum = ctx.context.get_function("", "SERIESSUM").unwrap();
        let x = lit(LiteralValue::Int(2));
        let n0 = lit(LiteralValue::Int(0));
        let m1 = lit(LiteralValue::Int(1));
        let coeffs = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(1),
                LiteralValue::Int(2),
                LiteralValue::Int(3),
            ]])),
            None,
        );
        assert_eq!(
            seriessum
                .dispatch(
                    &[
                        ArgumentHandle::new(&x, &ctx),
                        ArgumentHandle::new(&n0, &ctx),
                        ArgumentHandle::new(&m1, &ctx),
                        ArgumentHandle::new(&coeffs, &ctx),
                    ],
                    &ctx.function_context(None),
                )
                .unwrap()
                .into_literal(),
            LiteralValue::Number(17.0)
        );
    }

    #[test]
    fn sumsq_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumsqFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "SUMSQ").unwrap();
        let a = lit(LiteralValue::Int(3));
        let b = lit(LiteralValue::Int(4));
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(25.0)
        );
    }

    #[test]
    fn mround_sign_and_midpoint() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MroundFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "MROUND").unwrap();

        let n = lit(LiteralValue::Number(1.3));
        let m = lit(LiteralValue::Number(0.2));
        match f
            .dispatch(
                &[ArgumentHandle::new(&n, &ctx), ArgumentHandle::new(&m, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 1.4).abs() < 1e-12),
            other => panic!("expected numeric result, got {other:?}"),
        }

        let bad_m = lit(LiteralValue::Number(-2.0));
        let five = lit(LiteralValue::Number(5.0));
        match f
            .dispatch(
                &[
                    ArgumentHandle::new(&five, &ctx),
                    ArgumentHandle::new(&bad_m, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#NUM!"),
            other => panic!("expected #NUM!, got {other:?}"),
        }
    }

    #[test]
    fn roman_and_arabic_examples() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(RomanFn))
            .with_function(std::sync::Arc::new(ArabicFn));
        let ctx = interp(&wb);

        let roman = ctx.context.get_function("", "ROMAN").unwrap();
        let n499 = lit(LiteralValue::Int(499));
        let out = roman
            .dispatch(
                &[ArgumentHandle::new(&n499, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Text("CDXCIX".to_string()));

        let form4 = lit(LiteralValue::Int(4));
        let out_form4 = roman
            .dispatch(
                &[
                    ArgumentHandle::new(&n499, &ctx),
                    ArgumentHandle::new(&form4, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out_form4, LiteralValue::Text("ID".to_string()));

        let arabic = ctx.context.get_function("", "ARABIC").unwrap();
        let roman_text = lit(LiteralValue::Text("CDXCIX".to_string()));
        let out_arabic = arabic
            .dispatch(
                &[ArgumentHandle::new(&roman_text, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out_arabic, LiteralValue::Number(499.0));
    }
}
