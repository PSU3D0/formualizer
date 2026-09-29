use super::super::utils::{
    ARG_NUM_LENIENT_ONE, ARG_NUM_LENIENT_TWO, ARG_RANGE_NUM_LENIENT_ONE, coerce_num,
};
use super::{AggregateArgument, resolve_aggregate_argument};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::function_contract::FunctionDependencyContract;
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
    fn family_kernel(&self) -> Option<crate::function::FamilyKernel> {
        Some(crate::function::FamilyKernel::Abs)
    }
    fn name(&self) -> &'static str {
        "ABS"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn dependency_contract(&self, arity: usize) -> Option<FunctionDependencyContract> {
        FunctionDependencyContract::static_scalar_all_args(arity)
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
        let n = match args[0].value()?.into_literal() {
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
        let out = excel_round_with_mode(n, digits, DecimalRoundingMode::Down);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(out)))
    }
}

#[derive(Debug)]
pub struct RoundFn; // ROUND(number, digits)

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DecimalRoundingMode {
    /// ROUND: half away from zero on the decimal view.
    Nearest,
    /// ROUNDDOWN / TRUNC: toward zero on the decimal view.
    Down,
    /// ROUNDUP: away from zero on the decimal view.
    Up,
}

/// Powers of ten that binary64 represents exactly.
const EXACT_POW10: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

/// The smallest and one-past-largest 15-digit decimal coefficients.
const VIEW_MIN: u64 = 100_000_000_000_000;
const VIEW_END: u64 = 1_000_000_000_000_000;

/// A small stack buffer for `core::fmt` output, so the rare formatting
/// fallbacks below never touch the heap.
struct StackText {
    bytes: [u8; 64],
    len: usize,
}

impl StackText {
    fn new() -> Self {
        Self {
            bytes: [0; 64],
            len: 0,
        }
    }

    fn as_str(&self) -> &str {
        // Only `write_str` fills the buffer, and it copies whole `&str`s.
        std::str::from_utf8(&self.bytes[..self.len]).unwrap_or("")
    }
}

impl std::fmt::Write for StackText {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let end = self.len + s.len();
        if end > self.bytes.len() {
            return Err(std::fmt::Error);
        }
        self.bytes[self.len..end].copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// Excel ROUND first treats a binary64 value as its 15-significant-digit
/// decimal rendering, then rounds that decimal half away from zero.
fn excel_round(number: f64, requested_digits: i32) -> f64 {
    excel_round_with_mode(number, requested_digits, DecimalRoundingMode::Nearest)
}

/// Shared decimal-view rounding for the ROUND family (measured in Excel for
/// Mac 16.105.3 and Excel for the web).
///
/// 1. Reduce the exact binary value to 15 significant decimal digits, to
///    nearest. An exact tie at the 16th digit goes toward zero for `ROUND`
///    and away from zero for `ROUNDUP`, `ROUNDDOWN` and `TRUNC`.
/// 2. Round that decimal at the requested position with the function's
///    carry rule.
///
/// Most inputs never reach the decimal arithmetic: when the scaled value is
/// provably far from the carry boundary, the binary result is the same, see
/// [`round_far_from_boundary`].
fn excel_round_with_mode(number: f64, requested_digits: i32, mode: DecimalRoundingMode) -> f64 {
    if !number.is_finite() || number == 0.0 {
        return number;
    }
    if let Some(rounded) = round_far_from_boundary(number, requested_digits, mode) {
        return rounded;
    }

    let (coefficient, exponent) =
        fifteen_digit_view(number.abs(), mode == DecimalRoundingMode::Nearest);
    let unit_exponent = -i64::from(requested_digits);
    let magnitude = if exponent >= unit_exponent {
        // The requested position is at or below the view's last digit.
        decimal_to_f64(coefficient, exponent)
    } else {
        let discarded = unit_exponent - exponent;
        let (kept, carry) = if discarded > 15 {
            // Everything is discarded and the view is below half a unit.
            (0, mode == DecimalRoundingMode::Up)
        } else {
            let unit = 10_u64.pow(discarded as u32);
            let remainder = coefficient % unit;
            let carry = match mode {
                DecimalRoundingMode::Nearest => remainder * 2 >= unit,
                DecimalRoundingMode::Down => false,
                DecimalRoundingMode::Up => remainder > 0,
            };
            (coefficient / unit, carry)
        };
        let kept = kept + u64::from(carry);
        if kept == 0 {
            0.0
        } else {
            decimal_to_f64(kept, unit_exponent)
        }
    };
    magnitude.copysign(number)
}

/// The binary shortcut. With `digits` in `-22..=22` the power of ten is
/// exact, so `scaled` is `|number| * 10^digits` with one rounding error of at
/// most half an ULP. The 15-digit view moves the value by at most
/// `5e-15 * |number|`. When the scaled value is further than `1e-14 * scaled`
/// from the carry boundary (the half for `Nearest`, the integers otherwise),
/// neither error can cross it, so rounding `scaled` in binary makes the same
/// decision as the decimal path. `scaled < 1e14` keeps the rounding position
/// within the view's 15 digits and the integer result exact; dividing or
/// multiplying it by the exact power of ten is then correctly rounded, the
/// same value the decimal path parses. Integers are handled when the scaling
/// was exact.
#[inline]
fn round_far_from_boundary(number: f64, digits: i32, mode: DecimalRoundingMode) -> Option<f64> {
    if !(-22..=22).contains(&digits) {
        return None;
    }
    let power = EXACT_POW10[digits.unsigned_abs() as usize];
    let magnitude = number.abs();
    let scaled = if digits >= 0 {
        magnitude * power
    } else {
        magnitude / power
    };
    if scaled >= 1e14 {
        return None;
    }
    let whole = scaled.floor();
    let fraction = scaled - whole;
    let margin = scaled * 1e-14;
    let rounded = match mode {
        DecimalRoundingMode::Nearest => {
            if (fraction - 0.5).abs() <= margin {
                return None;
            }
            if fraction > 0.5 { whole + 1.0 } else { whole }
        }
        DecimalRoundingMode::Down | DecimalRoundingMode::Up if fraction == 0.0 => {
            // An integral `scaled` is only safe when it is the exact product:
            // then `number` has at most 14 significant digits, so its view is
            // itself and there is nothing to carry.
            let exact = digits == 0
                || if digits > 0 {
                    magnitude.mul_add(power, -scaled) == 0.0
                } else {
                    scaled.mul_add(power, -magnitude) == 0.0
                };
            if !exact {
                return None;
            }
            whole
        }
        DecimalRoundingMode::Down => {
            if fraction <= margin || 1.0 - fraction <= margin {
                return None;
            }
            whole
        }
        DecimalRoundingMode::Up => {
            if fraction <= margin || 1.0 - fraction <= margin {
                return None;
            }
            whole + 1.0
        }
    };
    let rounded = if digits >= 0 {
        rounded / power
    } else {
        rounded * power
    };
    Some(rounded.copysign(number))
}

/// The 15-significant-digit view of a positive finite `magnitude`, as
/// `(coefficient, exponent)` with `coefficient` in `[10^14, 10^15)` and the
/// view equal to `coefficient * 10^exponent`. `TEXT` renders its digits from
/// this view too.
pub(crate) fn fifteen_digit_view(magnitude: f64, tie_toward_zero: bool) -> (u64, i64) {
    // An exact tie at the 16th digit needs an exact decimal expansion of 16
    // significant digits, which binary64 only has between about 2.4e-7 and
    // 1.8e16. This range covers that with exact integer arithmetic.
    if (1e-7..18_446_744_073_709_551_616.0).contains(&magnitude) {
        let bits = magnitude.to_bits();
        // Normal in this range: magnitude = mantissa * 2^binary_exponent.
        let mantissa = u128::from((bits & ((1 << 52) - 1)) | (1 << 52));
        let binary_exponent = ((bits >> 52) & 0x7ff) as i64 - 1075;
        let mut decimal_exponent = magnitude.log10().floor() as i64;
        loop {
            // scaled = magnitude * 10^(14 - decimal_exponent) = numerator / denominator.
            // The bounds keep both below 2^128: 10^22 * 2^53 and 10^6 * 2^80.
            let shift = 14 - decimal_exponent;
            let mut numerator = mantissa;
            let mut denominator = 1_u128;
            if shift >= 0 {
                numerator *= 10_u128.pow(shift as u32);
            } else {
                denominator = 10_u128.pow((-shift) as u32);
            }
            let (quotient, remainder) = if binary_exponent >= 0 {
                numerator <<= binary_exponent;
                (numerator / denominator, numerator % denominator)
            } else if denominator == 1 {
                let bits = (-binary_exponent) as u32;
                denominator <<= bits;
                (numerator >> bits, numerator & (denominator - 1))
            } else {
                denominator <<= -binary_exponent;
                (numerator / denominator, numerator % denominator)
            };
            // log10 can land one decade off next to a power of ten.
            if quotient >= u128::from(VIEW_END) {
                decimal_exponent += 1;
                continue;
            }
            if quotient < u128::from(VIEW_MIN) {
                decimal_exponent -= 1;
                continue;
            }
            let twice = remainder * 2;
            let round_up = twice > denominator || (twice == denominator && !tie_toward_zero);
            let coefficient = quotient as u64 + u64::from(round_up);
            return if coefficient == VIEW_END {
                (VIEW_MIN, decimal_exponent - 13)
            } else {
                (coefficient, decimal_exponent - 14)
            };
        }
    }

    // Outside that range the correctly rounded `{:.14e}` rendering is the
    // view (no ties to break). Rendered as `d.dddddddddddddde<exp>`.
    use std::fmt::Write as _;
    let mut text = StackText::new();
    let _ = write!(text, "{magnitude:.14e}");
    let (mantissa, exponent) = text.as_str().split_once('e').unwrap_or(("0", "0"));
    let coefficient = mantissa
        .bytes()
        .filter(u8::is_ascii_digit)
        .fold(0_u64, |value, digit| value * 10 + u64::from(digit - b'0'));
    let exponent: i64 = exponent.parse().unwrap_or(0);
    (coefficient, exponent - 14)
}

/// `coefficient * 10^exponent`, correctly rounded, for `coefficient <= 10^15`
/// (exact in binary64).
fn decimal_to_f64(coefficient: u64, exponent: i64) -> f64 {
    let value = coefficient as f64;
    if (0..=22).contains(&exponent) {
        return value * EXACT_POW10[exponent as usize];
    }
    if (-22..0).contains(&exponent) {
        return value / EXACT_POW10[(-exponent) as usize];
    }
    use std::fmt::Write as _;
    let mut text = StackText::new();
    let _ = write!(text, "{coefficient}e{exponent}");
    text.as_str().parse().unwrap_or(f64::NAN)
}
/// Rounds a number to a specified number of digits.
///
/// # Remarks
/// - Positive `digits` rounds to the right of the decimal point.
/// - Negative `digits` rounds to the left of the decimal point.
/// - The input is first reduced to 15 significant decimal digits, matching Excel's
///   numeric precision, then rounded at the requested position.
/// - Halfway cases round away from zero.
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
    fn family_kernel(&self) -> Option<crate::function::FamilyKernel> {
        Some(crate::function::FamilyKernel::Round)
    }
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            round_digits(n, digits),
        )))
    }
}

/// `ROUND`'s arithmetic on coerced operands (shared with the typed lift).
///
/// Rounds Excel's 15-significant-digit decimal view of `n` half away from
/// zero; see [`excel_round`]. Works on decimal digits rather than scaling by
/// a power of ten, so extreme `digits` (including `i32::MIN`/`i32::MAX`)
/// neither overflow nor produce NaN.
#[inline]
pub(crate) fn round_digits(n: f64, digits: i32) -> f64 {
    excel_round(n, digits)
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
        let out = excel_round_with_mode(n, digits, DecimalRoundingMode::Down);
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
        let out = excel_round_with_mode(n, digits, DecimalRoundingMode::Up);
        if n.is_finite() && !out.is_finite() {
            // The away-from-zero carry can leave the representable range
            // (e.g. ROUNDUP(1.23, -400)); fail closed like Excel's overflow
            // errors rather than returning a non-finite number.
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
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
        let result = base.powf(expv);
        // Only guard overflow from finite builtin inputs. Programmatic
        // non-finite values have a separate host policy; do not redefine it.
        if !result.is_finite() && base.is_finite() && expv.is_finite() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
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
/// - Finite inputs that overflow floating-point range return `#NUM!`.
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
///     a: "Yes. Very large finite positive inputs return #NUM! on overflow."
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
        let result = n.exp();
        // Preserve host-provided NaN/infinity policy, but make overflow from
        // finite inputs catchable before IFERROR or result storage sees it.
        if !result.is_finite() && n.is_finite() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
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
        ctx: &dyn FunctionContext<'b>,
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
        match resolve_aggregate_argument(&args[3], ctx)? {
            AggregateArgument::Range(view) => view.for_each_cell(&mut |cell| {
                match cell {
                    LiteralValue::Error(e) => return Err(e.clone()),
                    other => coeffs.push(coerce_num(other)?),
                }
                Ok(())
            })?,
            AggregateArgument::ReferenceError(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            AggregateArgument::Scalar(value) => match value {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coeffs.push(coerce_num(&other)?),
            },
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let date_system = ctx.date_system();
        let mut total = 0.0;
        for arg in args {
            match resolve_aggregate_argument(arg, ctx)? {
                AggregateArgument::Range(view) => view.for_each_cell(&mut |cell| {
                    match cell {
                        LiteralValue::Error(e) => return Err(e.clone()),
                        LiteralValue::Number(n) => total += n * n,
                        LiteralValue::Int(i) => {
                            let n = *i as f64;
                            total += n * n;
                        }
                        LiteralValue::Date(d) => {
                            let n = formualizer_common::date_to_serial_for(date_system, d);
                            total += n * n;
                        }
                        LiteralValue::DateTime(dt) => {
                            let n = formualizer_common::datetime_to_serial_for(date_system, dt);
                            total += n * n;
                        }
                        LiteralValue::Time(t) => {
                            let n = formualizer_common::time_to_fraction(t);
                            total += n * n;
                        }
                        LiteralValue::Duration(d) => {
                            let n = d.num_seconds() as f64 / 86_400.0;
                            total += n * n;
                        }
                        _ => {}
                    }
                    Ok(())
                })?,
                AggregateArgument::ReferenceError(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                AggregateArgument::Scalar(v) => match v {
                    LiteralValue::Error(e) => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                    }
                    other => {
                        let n = coerce_num(&other)?;
                        total += n * n;
                    }
                },
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            total,
        )))
    }
}

/// The smallest binary64 at or above 0.499999999999995: MROUND's quotient
/// fraction rounds up from here (14-decimal-place rounding, then half up).
const MROUND_HALF: f64 = 0.499_999_999_999_995;

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
        // Excel rounds the quotient to 14 decimal places, then half away
        // from zero: a quotient less than 5e-15 below the half rounds up
        // (MROUND(2.5-8*2^-51, 1) = 3, MROUND(2.5-12*2^-51, 1) = 2) and
        // MROUND(12.4999999999995, 5) = 10. Unlike ROUND there is no
        // 15-significant-digit view: MROUND(1234567.5-2^-32, 1) = 1234567.
        // The product with the multiple stays binary, as in Excel:
        // MROUND(1.3, 0.2) = 7*0.2 = 1.4000000000000001.
        let whole = scaled.floor();
        let rounded = if scaled - whole >= MROUND_HALF {
            whole + 1.0
        } else {
            whole
        };
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

/* ─────────────────── BASE / DECIMAL / CEILING.PRECISE / FLOOR.PRECISE / ISO.CEILING ─────────────────── */

#[derive(Debug)]
pub struct BaseFn;
/// Converts a non-negative integer to text in the requested radix.
///
/// # Examples
///
/// ```excel
/// =BASE(31,16)
/// ```
///
/// ```yaml,sandbox
/// title: "Convert decimal to hexadecimal"
/// formula: '=BASE(31,16)'
/// expected: "1F"
/// ```
///
/// ```yaml,docs
/// related:
///   - DECIMAL
/// faq:
///   - q: "What bases are supported?"
///     a: "BASE accepts radix values from 2 through 36."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: BASE
/// Type: BaseFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: BASE(arg1: number@scalar, arg2: number@scalar, arg3...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BaseFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BASE"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static THREE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &THREE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 || args.len() > 3 {
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
        let radix = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?.trunc() as i64,
        };
        let min_len = if args.len() == 3 {
            match args[2].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)?.trunc() as usize,
            }
        } else {
            0
        };
        if !(2..=36).contains(&radix) || number < 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let mut digits = Vec::new();
        let mut n = number as u64;
        if n == 0 {
            digits.push('0');
        } else {
            while n > 0 {
                let d = (n % radix as u64) as u32;
                digits.push(
                    char::from_digit(d, radix as u32)
                        .unwrap()
                        .to_ascii_uppercase(),
                );
                n /= radix as u64;
            }
            digits.reverse();
        }
        while digits.len() < min_len {
            digits.insert(0, '0');
        }
        let text: String = digits.into_iter().collect();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(text)))
    }
}

#[derive(Debug)]
pub struct DecimalFn;
/// Converts text in a given radix back to a decimal number.
///
/// # Examples
///
/// ```excel
/// =DECIMAL("1F",16)
/// ```
///
/// ```yaml,sandbox
/// title: "Convert hexadecimal to decimal"
/// formula: '=DECIMAL("1F",16)'
/// expected: 31
/// ```
///
/// ```yaml,docs
/// related:
///   - BASE
/// faq:
///   - q: "Is DECIMAL case-sensitive for alphabetic digits?"
///     a: "No. Inputs like \"ff\" and \"FF\" both work for hexadecimal conversions."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: DECIMAL
/// Type: DecimalFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: DECIMAL(arg1: any@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for DecimalFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DECIMAL"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::any(), ArgSchema::number_lenient_scalar()]);
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Text(s) => s,
            LiteralValue::Number(n) => format!("{}", n.trunc() as i64),
            LiteralValue::Int(i) => i.to_string(),
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        };
        let radix = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?.trunc() as u32,
        };
        if !(2..=36).contains(&radix) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let trimmed = text.trim();
        match i64::from_str_radix(trimmed, radix) {
            Ok(v) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                v as f64,
            ))),
            Err(_) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            ))),
        }
    }
}

#[derive(Debug)]
pub struct CeilingPreciseFn;
/// Rounds a number up toward positive infinity using absolute significance.
///
/// # Examples
///
/// ```excel
/// =CEILING.PRECISE(-4.3)
/// ```
///
/// ```yaml,sandbox
/// title: "Round a negative number upward"
/// formula: '=CEILING.PRECISE(-4.3)'
/// expected: -4
/// ```
///
/// ```yaml,docs
/// related:
///   - FLOOR.PRECISE
///   - ISO.CEILING
/// faq:
///   - q: "Does the sign of significance matter?"
///     a: "No. CEILING.PRECISE uses the absolute value of significance."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: CEILING.PRECISE
/// Type: CeilingPreciseFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: CEILING.PRECISE(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for CeilingPreciseFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CEILING.PRECISE"
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
        let sig = if args.len() == 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => {
                    let v = coerce_num(&other)?;
                    if v == 0.0 {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
                    }
                    v.abs()
                }
            }
        } else {
            1.0
        };
        let result = (n / sig).ceil() * sig;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

#[derive(Debug)]
pub struct FloorPreciseFn;
/// Rounds a number down toward negative infinity using absolute significance.
///
/// # Examples
///
/// ```excel
/// =FLOOR.PRECISE(-4.3)
/// ```
///
/// ```yaml,sandbox
/// title: "Round a negative number downward"
/// formula: '=FLOOR.PRECISE(-4.3)'
/// expected: -5
/// ```
///
/// ```yaml,docs
/// related:
///   - CEILING.PRECISE
/// faq:
///   - q: "Does FLOOR.PRECISE keep negative significance?"
///     a: "No. Like Excel, this implementation uses the absolute value of significance."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: FLOOR.PRECISE
/// Type: FloorPreciseFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: FLOOR.PRECISE(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FloorPreciseFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FLOOR.PRECISE"
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
        let sig = if args.len() == 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => {
                    let v = coerce_num(&other)?;
                    if v == 0.0 {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
                    }
                    v.abs()
                }
            }
        } else {
            1.0
        };
        let result = (n / sig).floor() * sig;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

#[derive(Debug)]
pub struct IsoCeilingFn;
/// Rounds a number up using ISO ceiling semantics.
///
/// # Examples
///
/// ```excel
/// =ISO.CEILING(-4.3)
/// ```
///
/// ```yaml,sandbox
/// title: "ISO ceiling on a negative value"
/// formula: '=ISO.CEILING(-4.3)'
/// expected: -4
/// ```
///
/// ```yaml,docs
/// related:
///   - CEILING.PRECISE
/// faq:
///   - q: "How does ISO.CEILING differ from legacy CEILING?"
///     a: "ISO.CEILING always uses a positive significance and rounds toward positive infinity."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: ISO.CEILING
/// Type: IsoCeilingFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: ISO.CEILING(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsoCeilingFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISO.CEILING"
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
        let sig = if args.len() == 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => {
                    let v = coerce_num(&other)?;
                    if v == 0.0 {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
                    }
                    v.abs()
                }
            }
        } else {
            1.0
        };
        let result = (n / sig).ceil() * sig;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/* ─────────────────── SUMX2MY2, SUMX2PY2, SUMXMY2 ─────────────────── */

fn collect_nums_from_arg<'a, 'b>(
    arg: &'a crate::traits::ArgumentHandle<'a, 'b>,
    ctx: &dyn FunctionContext<'b>,
) -> Result<Vec<f64>, ExcelError> {
    let mut out = Vec::new();
    match resolve_aggregate_argument(arg, ctx)? {
        AggregateArgument::Range(view) => view.for_each_cell(&mut |cell| {
            match cell {
                LiteralValue::Error(e) => return Err(e.clone()),
                LiteralValue::Number(n) => out.push(*n),
                LiteralValue::Int(i) => out.push(*i as f64),
                LiteralValue::Boolean(b) => out.push(if *b { 1.0 } else { 0.0 }),
                _ => out.push(0.0),
            }
            Ok(())
        })?,
        AggregateArgument::ReferenceError(error) => return Err(error),
        AggregateArgument::Scalar(value) => match value {
            LiteralValue::Error(e) => return Err(e),
            other => out.push(coerce_num(&other)?),
        },
    }
    Ok(out)
}

#[derive(Debug)]
pub struct SumX2MY2Fn;
/// Returns the sum of squares of values in one array minus the squares in another.
///
/// # Examples
///
/// ```excel
/// =SUMX2MY2({2,3},{1,4})
/// ```
///
/// ```yaml,sandbox
/// title: "Pairwise squared difference of squares"
/// formula: '=SUMX2MY2({2,3},{1,4})'
/// expected: -4
/// ```
///
/// ```yaml,docs
/// related:
///   - SUMX2PY2
///   - SUMXMY2
/// faq:
///   - q: "What happens when the arrays have different sizes?"
///     a: "SUMX2MY2 returns #N/A when the two inputs do not contain the same number of values."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: SUMX2MY2
/// Type: SumX2MY2Fn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: SUMX2MY2(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for SumX2MY2Fn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SUMX2MY2"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let xs = collect_nums_from_arg(&args[0], ctx)?;
        let ys = collect_nums_from_arg(&args[1], ctx)?;
        if xs.len() != ys.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        let total: f64 = xs.iter().zip(ys.iter()).map(|(x, y)| x * x - y * y).sum();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            total,
        )))
    }
}

#[derive(Debug)]
pub struct SumX2PY2Fn;
/// Returns the sum of squares of corresponding values from two arrays.
///
/// # Examples
///
/// ```excel
/// =SUMX2PY2({2,3},{1,4})
/// ```
///
/// ```yaml,sandbox
/// title: "Pairwise squared sums"
/// formula: '=SUMX2PY2({2,3},{1,4})'
/// expected: 30
/// ```
///
/// ```yaml,docs
/// related:
///   - SUMX2MY2
///   - SUMXMY2
/// faq:
///   - q: "Does SUMX2PY2 coerce booleans and blanks?"
///     a: "It follows the same collection rules as the engine's paired-array helpers, coercing booleans and treating blanks/text as zero."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: SUMX2PY2
/// Type: SumX2PY2Fn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: SUMX2PY2(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for SumX2PY2Fn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SUMX2PY2"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let xs = collect_nums_from_arg(&args[0], ctx)?;
        let ys = collect_nums_from_arg(&args[1], ctx)?;
        if xs.len() != ys.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        let total: f64 = xs.iter().zip(ys.iter()).map(|(x, y)| x * x + y * y).sum();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            total,
        )))
    }
}

#[derive(Debug)]
pub struct SumXMY2Fn;
/// Returns the sum of squared differences between corresponding values.
///
/// # Examples
///
/// ```excel
/// =SUMXMY2({2,3},{1,4})
/// ```
///
/// ```yaml,sandbox
/// title: "Pairwise squared differences"
/// formula: '=SUMXMY2({2,3},{1,4})'
/// expected: 2
/// ```
///
/// ```yaml,docs
/// related:
///   - SUMX2MY2
///   - SUMX2PY2
/// faq:
///   - q: "What shape must the inputs have?"
///     a: "Both inputs must flatten to the same number of values or the function returns #N/A."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: SUMXMY2
/// Type: SumXMY2Fn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: SUMXMY2(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for SumXMY2Fn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SUMXMY2"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let xs = collect_nums_from_arg(&args[0], ctx)?;
        let ys = collect_nums_from_arg(&args[1], ctx)?;
        if xs.len() != ys.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        let total: f64 = xs.iter().zip(ys.iter()).map(|(x, y)| (x - y).powi(2)).sum();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            total,
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_builtin(Arc::new(AbsFn));
    crate::function_registry::register_builtin(Arc::new(SignFn));
    crate::function_registry::register_builtin(Arc::new(IntFn));
    crate::function_registry::register_builtin(Arc::new(TruncFn));
    crate::function_registry::register_builtin(Arc::new(RoundFn));
    crate::function_registry::register_builtin(Arc::new(RoundDownFn));
    crate::function_registry::register_builtin(Arc::new(RoundUpFn));
    crate::function_registry::register_builtin(Arc::new(ModFn));
    crate::function_registry::register_builtin(Arc::new(CeilingFn));
    crate::function_registry::register_builtin(Arc::new(CeilingMathFn));
    crate::function_registry::register_builtin(Arc::new(CeilingPreciseFn));
    crate::function_registry::register_builtin(Arc::new(IsoCeilingFn));
    crate::function_registry::register_builtin(Arc::new(FloorFn));
    crate::function_registry::register_builtin(Arc::new(FloorMathFn));
    crate::function_registry::register_builtin(Arc::new(FloorPreciseFn));
    crate::function_registry::register_builtin(Arc::new(SqrtFn));
    crate::function_registry::register_builtin(Arc::new(PowerFn));
    crate::function_registry::register_builtin(Arc::new(ExpFn));
    crate::function_registry::register_builtin(Arc::new(LnFn));
    crate::function_registry::register_builtin(Arc::new(LogFn));
    crate::function_registry::register_builtin(Arc::new(Log10Fn));
    crate::function_registry::register_builtin(Arc::new(QuotientFn));
    crate::function_registry::register_builtin(Arc::new(EvenFn));
    crate::function_registry::register_builtin(Arc::new(OddFn));
    crate::function_registry::register_builtin(Arc::new(SqrtPiFn));
    crate::function_registry::register_builtin(Arc::new(MultinomialFn));
    crate::function_registry::register_builtin(Arc::new(SeriesSumFn));
    crate::function_registry::register_builtin(Arc::new(SumsqFn));
    crate::function_registry::register_builtin(Arc::new(MroundFn));
    crate::function_registry::register_builtin(Arc::new(RomanFn));
    crate::function_registry::register_builtin(Arc::new(ArabicFn));
    crate::function_registry::register_builtin(Arc::new(BaseFn));
    crate::function_registry::register_builtin(Arc::new(DecimalFn));
    crate::function_registry::register_builtin(Arc::new(SumX2MY2Fn));
    crate::function_registry::register_builtin(Arc::new(SumX2PY2Fn));
    crate::function_registry::register_builtin(Arc::new(SumXMY2Fn));
}

#[cfg(test)]
mod tests_numeric {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    use proptest::prelude::*;

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    fn evaluate_round(number: f64, digits: i32) -> f64 {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RoundFn));
        let ctx = interp(&wb);
        let function = ctx.context.get_function("", "ROUND").unwrap();
        let number = lit(LiteralValue::Number(number));
        let digits = lit(LiteralValue::Int(digits as i64));
        match function
            .dispatch(
                &[
                    ArgumentHandle::new(&number, &ctx),
                    ArgumentHandle::new(&digits, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(value) => value,
            other => panic!("expected numeric ROUND result, got {other:?}"),
        }
    }

    /// Exact decimal digits of a positive finite binary64, with the decimal
    /// exponent of the last digit: `value = digits * 10^exponent`. Built from
    /// the bit pattern with schoolbook arithmetic on base-10 digit vectors,
    /// so it shares nothing with the implementation's conversions.
    fn exact_decimal(value: f64) -> (Vec<u8>, i64) {
        fn multiply_small(digits: &mut Vec<u8>, factor: u64) {
            // Little-endian decimal digits.
            let mut carry = 0_u64;
            for digit in digits.iter_mut() {
                let product = u64::from(*digit) * factor + carry;
                *digit = (product % 10) as u8;
                carry = product / 10;
            }
            while carry > 0 {
                digits.push((carry % 10) as u8);
                carry /= 10;
            }
        }
        let bits = value.to_bits();
        let biased = ((bits >> 52) & 0x7ff) as i64;
        let fraction = bits & ((1_u64 << 52) - 1);
        let (mantissa, binary_exponent) = if biased == 0 {
            (fraction, -1074)
        } else {
            (fraction | (1 << 52), biased - 1075)
        };
        let mut digits: Vec<u8> = mantissa
            .to_string()
            .bytes()
            .rev()
            .map(|byte| byte - b'0')
            .collect();
        let mut exponent = 0_i64;
        let (base, mut count) = if binary_exponent >= 0 {
            (2_u64, binary_exponent)
        } else {
            // m / 2^k = m * 5^k / 10^k.
            exponent = binary_exponent;
            (5_u64, -binary_exponent)
        };
        while count > 0 {
            let step = count.min(13);
            multiply_small(&mut digits, base.pow(step as u32));
            count -= step;
        }
        while digits.len() > 1 && digits.last() == Some(&0) {
            digits.pop();
        }
        digits.reverse();
        (digits, exponent)
    }

    /// Rounds big-endian `digits` (value `digits * 10^exponent`) to keep
    /// `keep` leading digits (may be zero or negative). Returns the kept
    /// digits and their exponent.
    fn round_digit_vector(
        mut digits: Vec<u8>,
        exponent: i64,
        keep: i64,
        carry_rule: impl Fn(&[u8], &[u8]) -> bool,
    ) -> (Vec<u8>, i64) {
        let len = digits.len() as i64;
        if keep >= len {
            return (digits, exponent);
        }
        let new_exponent = exponent + (len - keep);
        let split = keep.max(0) as usize;
        let tail = digits.split_off(split);
        let mut kept = if keep < 0 { Vec::new() } else { digits };
        let tail = if keep < 0 {
            // Positions above the leading digit are zeros.
            let mut padded = vec![0; (-keep) as usize];
            padded.extend(tail);
            padded
        } else {
            tail
        };
        if carry_rule(&kept, &tail) {
            let mut index = kept.len();
            loop {
                if index == 0 {
                    kept.insert(0, 1);
                    break;
                }
                index -= 1;
                if kept[index] < 9 {
                    kept[index] += 1;
                    break;
                }
                kept[index] = 0;
            }
        }
        (kept, new_exponent)
    }

    fn reference_round(number: f64, requested_digits: i32, mode: DecimalRoundingMode) -> f64 {
        if !number.is_finite() || number == 0.0 {
            return number;
        }
        let (digits, exponent) = exact_decimal(number.abs());
        let first = |tail: &[u8]| tail.first().copied().unwrap_or(0);
        let beyond_first = |tail: &[u8]| tail.iter().skip(1).any(|digit| *digit != 0);
        // 15 significant digits, nearest; exact ties toward zero for ROUND,
        // away from zero for the others.
        let (view, view_exponent) = round_digit_vector(digits, exponent, 15, |_, tail| {
            first(tail) > 5
                || (first(tail) == 5
                    && (beyond_first(tail) || mode != DecimalRoundingMode::Nearest))
        });
        let unit_exponent = -i64::from(requested_digits);
        let keep = view.len() as i64 - (unit_exponent - view_exponent);
        let (kept, kept_exponent) =
            round_digit_vector(view, view_exponent, keep, |_, tail| match mode {
                DecimalRoundingMode::Nearest => first(tail) >= 5,
                DecimalRoundingMode::Down => false,
                DecimalRoundingMode::Up => tail.iter().any(|digit| *digit != 0),
            });
        let text: String = kept.iter().map(|digit| char::from(b'0' + digit)).collect();
        let magnitude = if text.is_empty() || text.bytes().all(|byte| byte == b'0') {
            0.0
        } else {
            format!("{text}e{kept_exponent}").parse::<f64>().unwrap()
        };
        if number < 0.0 { -magnitude } else { magnitude }
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

    #[test]
    fn round_digits_extreme_digits_do_not_overflow() {
        // The decimal-view rounding never scales by 10^digits, so extreme
        // digits neither overflow nor turn a finite input into NaN.
        for n in [0.0, -0.0, 1.5, -2.25, 1e308, f64::MIN_POSITIVE] {
            for digits in [i32::MIN, i32::MIN + 1, -400] {
                let got = round_digits(n, digits);
                assert_eq!(got, 0.0, "ROUND({n}, {digits})");
                assert_eq!(
                    got.is_sign_negative(),
                    n.is_sign_negative(),
                    "ROUND({n}, {digits}) keeps the sign of zero"
                );
            }
            // Huge positive digits leave the 15-digit decimal view unchanged:
            // the identity for these inputs, and 15 significant digits of
            // f64::MIN_POSITIVE.
            let view = if n == f64::MIN_POSITIVE {
                2.2250738585072e-308
            } else {
                n
            };
            for digits in [i32::MAX, 400] {
                assert_eq!(round_digits(n, digits), view, "ROUND({n}, {digits})");
            }
        }
        for n in [f64::INFINITY, f64::NEG_INFINITY] {
            for digits in [i32::MIN, i32::MAX, -400, 400] {
                assert_eq!(round_digits(n, digits), n, "ROUND({n}, {digits})");
            }
        }
        assert!(round_digits(f64::NAN, i32::MIN).is_nan());
        // Through the functions (ROUNDDOWN/ROUNDUP share the arithmetic):
        // i32::MIN digits evaluate without panicking.
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(RoundFn))
            .with_function(std::sync::Arc::new(RoundDownFn))
            .with_function(std::sync::Arc::new(RoundUpFn));
        let ctx = interp(&wb);
        let n = lit(LiteralValue::Number(1.5));
        let d = lit(LiteralValue::Number(f64::from(i32::MIN)));
        for name in ["ROUND", "ROUNDDOWN", "ROUNDUP"] {
            let f = ctx.context.get_function("", name).unwrap();
            let _ = f
                .dispatch(
                    &[ArgumentHandle::new(&n, &ctx), ArgumentHandle::new(&d, &ctx)],
                    &ctx.function_context(None),
                )
                .unwrap()
                .into_literal();
        }
    }

    fn assert_round_symmetry(bits: u64, digits: i32, expected: f64) {
        let input = f64::from_bits(bits);
        assert_eq!(
            evaluate_round(input, digits),
            expected,
            "ROUND({input:.17}, {digits})"
        );
        assert_eq!(
            evaluate_round(-input, digits),
            -expected,
            "ROUND({:.17}, {digits})",
            -input
        );
    }

    #[test]
    fn round_excel_half_rule_one_ulp_below() {
        assert_round_symmetry(0x40B6_2E1F_FFFF_FFFF, 2, 5_678.13);
    }

    #[test]
    fn round_excel_half_rule_two_ulps_below() {
        assert_round_symmetry(0x40B6_2E1F_FFFF_FFFE, 2, 5_678.13);
    }

    #[test]
    fn round_excel_half_rule_three_ulps_below() {
        assert_round_symmetry(0x40B6_2E1F_FFFF_FFFD, 2, 5_678.13);
    }

    #[test]
    fn round_excel_half_rule_controls() {
        let vectors = [
            // 32 ULPs below 5678.125: below the half in the 15-digit view.
            (0x40B6_2E1F_FFFF_FFE0, 2, 5_678.12),
            // 5678.125 exactly: a representable tie, away from zero.
            (0x40B6_2E20_0000_0000, 2, 5_678.13),
            // 2.675 and 1.005 sit just below the half in binary.
            (0x4005_6666_6666_6666, 2, 2.68),
            (0x3FF0_147A_E147_AE14, 2, 1.01),
        ];

        for (bits, digits, expected) in vectors {
            assert_round_symmetry(bits, digits, expected);
        }
    }

    #[test]
    fn round_excel_half_rule_zero_digits() {
        assert_round_symmetry(0x40B0_E17F_FFFF_FFFE, 0, 4_322.0);
    }

    #[test]
    fn round_excel_half_rule_negative_digits() {
        assert_round_symmetry(0x40BA_5DFF_FFFF_FFFE, -2, 6_800.0);
    }

    #[test]
    fn round_excel_half_rule_extreme_contracts() {
        assert_eq!(excel_round(0.0, i32::MIN).to_bits(), 0.0_f64.to_bits());
        assert_eq!(excel_round(-0.0, i32::MAX).to_bits(), (-0.0_f64).to_bits());
        assert!(excel_round(f64::NAN, 2).is_nan());
        assert_eq!(excel_round(f64::INFINITY, 2), f64::INFINITY);
        assert_eq!(excel_round(f64::NEG_INFINITY, 2), f64::NEG_INFINITY);
        assert_eq!(
            excel_round(1.234_567_890_123_456_7, i32::MAX),
            1.234_567_890_123_46
        );
        assert_eq!(
            excel_round(1.234_567_890_123_456_7e300, 2),
            1.234_567_890_123_46e300
        );
        assert_eq!(
            excel_round(f64::from_bits(1), i32::MIN).to_bits(),
            0.0_f64.to_bits()
        );
    }

    #[test]
    fn round_family_view_is_the_exact_value_not_the_shortest_rendering() {
        // 2.675 - 22 ULPs renders as 2.674999999999995 (shortest round trip)
        // but its exact 15-digit view is 2.67499999999999, and Excel rounds
        // the exact view. Likewise 0.285 - 9 ULPs and 7.3645 + 6 ULPs.
        let tie_case = 2.675 - 22.0 * 2f64.powi(-52);
        let below = 0.285 - 9.0 * 2f64.powi(-54);
        let above = 7.3645 + 6.0 * 2f64.powi(-50);
        let nearest = DecimalRoundingMode::Nearest;
        let down = DecimalRoundingMode::Down;
        assert_eq!(excel_round(tie_case, 2), 2.67);
        assert_eq!(excel_round(tie_case, 14), 2.674_999_999_999_99);
        assert_eq!(excel_round(below, 2), 0.28);
        assert_eq!(excel_round_with_mode(below, 3, down), 0.284);
        assert_eq!(
            excel_round_with_mode(below, 15, down),
            0.284_999_999_999_999
        );
        assert_eq!(excel_round_with_mode(above, 14, nearest), 7.3645);
    }

    #[test]
    fn round_family_ties_at_the_sixteenth_digit() {
        // Exact ties in the 15-digit view: ROUND takes them toward zero,
        // ROUNDUP, ROUNDDOWN and TRUNC away from zero (measured).
        for (number, digits, toward_zero, away) in [
            (
                1_234_567_890_123_445.0,
                0,
                1_234_567_890_123_440.0,
                1_234_567_890_123_450.0,
            ),
            (
                1_234_567_890_123_445.0,
                3,
                1_234_567_890_123_440.0,
                1_234_567_890_123_450.0,
            ),
            (
                123_456_789_012_344.5,
                1,
                123_456_789_012_344.0,
                123_456_789_012_345.0,
            ),
            (
                12_345_678_901_234.25,
                2,
                12_345_678_901_234.2,
                12_345_678_901_234.3,
            ),
        ] {
            for sign in [1.0, -1.0] {
                let n = sign * number;
                assert_eq!(
                    excel_round(n, digits),
                    sign * toward_zero,
                    "ROUND({n}, {digits})"
                );
                for mode in [DecimalRoundingMode::Down, DecimalRoundingMode::Up] {
                    assert_eq!(
                        excel_round_with_mode(n, digits, mode),
                        sign * away,
                        "{digits} {n}"
                    );
                }
            }
        }
    }

    #[test]
    fn round_family_large_digits_keep_the_fifteen_digit_view() {
        let two_ulps_above_one = 1.0 + 2.0 * f64::EPSILON;
        for digits in [15, 16, 17, 20, 400] {
            assert_eq!(excel_round(0.1 + 0.2, digits), 0.3, "digits {digits}");
            assert_eq!(
                excel_round(two_ulps_above_one, digits),
                1.0,
                "digits {digits}"
            );
            for mode in [DecimalRoundingMode::Down, DecimalRoundingMode::Up] {
                assert_eq!(excel_round_with_mode(two_ulps_above_one, digits, mode), 1.0);
            }
        }
        // 2^60 has 19 digits; every member returns its 15-digit view.
        let big = 2f64.powi(60);
        for mode in [
            DecimalRoundingMode::Nearest,
            DecimalRoundingMode::Down,
            DecimalRoundingMode::Up,
        ] {
            assert_eq!(excel_round_with_mode(big, 0, mode), 1.152_921_504_606_85e18);
            assert_eq!(
                excel_round_with_mode(big, -2, mode),
                1.152_921_504_606_85e18
            );
        }
        assert_eq!(excel_round(1.23, -400), 0.0);
        assert_eq!(excel_round(1.23, -308), 0.0);
        assert_eq!(excel_round(1.23, 308), 1.23);
        assert_eq!(excel_round(1.23, 400), 1.23);
    }

    /// Money-like values a few ULPs either side of a decimal boundary, where
    /// the binary shortcut must hand over to the decimal path.
    fn near_boundary_value() -> impl Strategy<Value = f64> {
        (0_u64..10_000_000_000, 0_i32..=6, -40_i64..=40).prop_map(|(units, places, ulps)| {
            let value = units as f64 / 10f64.powi(places) + 0.5 / 10f64.powi(places);
            f64::from_bits((value.to_bits() as i64 + ulps) as u64)
        })
    }

    fn all_modes() -> [DecimalRoundingMode; 3] {
        [
            DecimalRoundingMode::Nearest,
            DecimalRoundingMode::Down,
            DecimalRoundingMode::Up,
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 2_000,
            rng_seed: proptest::test_runner::RngSeed::Fixed(0x5EED_0F15),
            .. ProptestConfig::default()
        })]

        #[test]
        fn round_family_matches_exact_decimal_reference(bits in any::<u64>(), digits in -340_i32..=340_i32) {
            let number = f64::from_bits(bits);
            prop_assume!(number.is_finite());
            for mode in all_modes() {
                let actual = excel_round_with_mode(number, digits, mode);
                let expected = reference_round(number, digits, mode);
                prop_assert_eq!(actual.to_bits(), expected.to_bits(), "{:?} {} {:?}", number, digits, mode);
            }
        }

        #[test]
        fn round_family_matches_reference_near_boundaries(number in near_boundary_value(), digits in -3_i32..=8, negative in any::<bool>()) {
            let number = if negative { -number } else { number };
            for mode in all_modes() {
                let actual = excel_round_with_mode(number, digits, mode);
                let expected = reference_round(number, digits, mode);
                prop_assert_eq!(actual.to_bits(), expected.to_bits(), "{:?} {} {:?}", number, digits, mode);
            }
        }
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

    // The ROUND siblings on half-adjacent and just-below-boundary inputs.
    // Excel applies ROUND's 15-significant-digit decimal view to ROUNDUP,
    // ROUNDDOWN and TRUNC, differing only in the carry rule. MROUND rounds its
    // binary quotient at 14 decimal places, then half away from zero.
    // Products such as `1.15 * 100` are written out because their binary64
    // results (`114.99999999999999`) are the point of the case.
    fn eval_two_arg(
        fun: std::sync::Arc<dyn Function>,
        name: &str,
        n: LiteralValue,
        d: LiteralValue,
    ) -> LiteralValue {
        let wb = TestWorkbook::new().with_function(fun);
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", name).unwrap();
        let a = lit(n);
        let b = lit(d);
        f.dispatch(
            &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
            &ctx.function_context(None),
        )
        .unwrap()
        .into_literal()
    }

    #[test]
    fn rounddown_excel_oracle_vectors() {
        let vectors: &[(f64, i64, f64)] = &[
            (1.15 * 100.0, 0, 115.0),
            (2.3 * 100.0, 0, 230.0),
            (0.36 * 100.0, 0, 36.0),
            (4.1 * 2.3, 2, 9.43),
            (3.3 * 3.3, 2, 10.89),
            (-(4.1 * 2.3), 2, -9.43),
            (-(1.15 * 100.0), 0, -115.0),
            (1.23, -400, 0.0), // no 10^400 overflow
        ];
        for (n, d, expected) in vectors {
            assert_eq!(
                eval_two_arg(
                    std::sync::Arc::new(RoundDownFn),
                    "ROUNDDOWN",
                    LiteralValue::Number(*n),
                    LiteralValue::Int(*d),
                ),
                LiteralValue::Number(*expected),
                "ROUNDDOWN({n:?}, {d})"
            );
        }
    }

    #[test]
    fn roundup_excel_oracle_vectors() {
        let vectors: &[(f64, i64, f64)] = &[
            (1.1 * 100.0, 0, 110.0),
            (26.000000000000004, 0, 26.0),
            (1.1 * 1.1, 2, 1.21),
            (1.1, 2, 1.1),
            (-1.1, 2, -1.1),
            (1.23, 400, 1.23), // identity, no overflow
        ];
        for (n, d, expected) in vectors {
            assert_eq!(
                eval_two_arg(
                    std::sync::Arc::new(RoundUpFn),
                    "ROUNDUP",
                    LiteralValue::Number(*n),
                    LiteralValue::Int(*d),
                ),
                LiteralValue::Number(*expected),
                "ROUNDUP({n:?}, {d})"
            );
        }
        // Unmeasured in Excel (would overflow the decimal carry to 1e400):
        // a finite input whose rounded-up magnitude is not representable
        // fails closed as #NUM! rather than returning a non-finite number.
        match eval_two_arg(
            std::sync::Arc::new(RoundUpFn),
            "ROUNDUP",
            LiteralValue::Number(1.23),
            LiteralValue::Int(-400),
        ) {
            LiteralValue::Error(e) => assert_eq!(e, "#NUM!"),
            other => panic!("expected #NUM! for ROUNDUP(1.23,-400), got {other:?}"),
        }
    }

    #[test]
    fn trunc_excel_oracle_vectors() {
        let vectors: &[(f64, i64, f64)] = &[(0.29 * 100.0, 0, 29.0), (0.36 * 100.0, 0, 36.0)];
        for (n, d, expected) in vectors {
            assert_eq!(
                eval_two_arg(
                    std::sync::Arc::new(TruncFn),
                    "TRUNC",
                    LiteralValue::Number(*n),
                    LiteralValue::Int(*d),
                ),
                LiteralValue::Number(*expected),
                "TRUNC({n:?}, {d})"
            );
        }
    }

    #[test]
    fn mround_excel_oracle_vectors() {
        let vectors: &[(f64, f64, f64)] = &[
            (12.4999999999995, 5.0, 10.0), // near-half stays down
            (0.4999999999995, 1.0, 0.0),
            (1.24999999999994, 0.5, 1.0),
            (2.5, 1.0, 3.0), // exact tie away from zero
            (-2.5, -1.0, -3.0),
            // The quotient's fraction rounds up from 0.499999999999995.
            (2.5 - 8.0 * 2f64.powi(-51), 1.0, 3.0),
            (2.5 - 12.0 * 2f64.powi(-51), 1.0, 2.0),
            (1.5 - 22.0 * 2f64.powi(-52), 1.0, 2.0),
            (1.5 - 23.0 * 2f64.powi(-52), 1.0, 1.0),
            (10.5 - 2.0 * 2f64.powi(-49), 1.0, 11.0),
            (10.5 - 3.0 * 2f64.powi(-49), 1.0, 10.0),
            (25.0 - 8.0 * 2f64.powi(-48), 10.0, 30.0),
            (25.0 - 16.0 * 2f64.powi(-48), 10.0, 20.0),
            (-2.5 + 8.0 * 2f64.powi(-51), -1.0, -3.0),
            // No 15-digit view of the quotient.
            (100.5 - 2f64.powi(-46), 1.0, 100.0),
            (1_234_567.5 - 2f64.powi(-32), 1.0, 1_234_567.0),
            (2f64.powi(51) + 0.5, 1.0, 2f64.powi(51) + 1.0),
            (2f64.powi(60), 3.0, 2f64.powi(60)),
            // The product with the multiple is binary, as in Excel.
            (0.7, 0.1, 7.0 * 0.1),
            (1.3, 0.2, 7.0 * 0.2),
            (3.3, 1.1, 3.0 * 1.1),
        ];
        for (n, m, expected) in vectors {
            assert_eq!(
                eval_two_arg(
                    std::sync::Arc::new(MroundFn),
                    "MROUND",
                    LiteralValue::Number(*n),
                    LiteralValue::Number(*m),
                ),
                LiteralValue::Number(*expected),
                "MROUND({n:?}, {m:?})"
            );
        }
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

    // ── Too-few-arguments: must not panic ────────────────────────────────

    #[test]
    fn round_one_arg_returns_error_not_panic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RoundFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ROUND").unwrap();
        let n = lit(LiteralValue::Number(2.5));
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(
            matches!(result, LiteralValue::Error(_)),
            "Expected an error, got {result:?}"
        );
    }

    #[test]
    fn rounddown_one_arg_returns_error_not_panic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RoundDownFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ROUNDDOWN").unwrap();
        let n = lit(LiteralValue::Number(1.9));
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(
            matches!(result, LiteralValue::Error(_)),
            "Expected an error, got {result:?}"
        );
    }

    #[test]
    fn abs_zero_args_returns_error_not_panic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AbsFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ABS").unwrap();
        let result = f
            .dispatch(&[], &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(
            matches!(result, LiteralValue::Error(_)),
            "Expected an error, got {result:?}"
        );
    }

    #[test]
    fn mod_one_arg_returns_error_not_panic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ModFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "MOD").unwrap();
        let n = lit(LiteralValue::Number(10.0));
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(
            matches!(result, LiteralValue::Error(_)),
            "Expected an error, got {result:?}"
        );
    }
}
