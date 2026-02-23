//! Engineering functions
//! Bitwise: BITAND, BITOR, BITXOR, BITLSHIFT, BITRSHIFT

use super::utils::{ARG_ANY_TWO, ARG_NUM_LENIENT_TWO, coerce_num};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/// Helper to convert to integer for bitwise operations
/// Excel's bitwise functions only work with non-negative integers up to 2^48
fn to_bitwise_int(v: &LiteralValue) -> Result<i64, ExcelError> {
    let n = coerce_num(v)?;
    if n < 0.0 || n != n.trunc() || n >= 281474976710656.0 {
        // 2^48
        return Err(ExcelError::new_num());
    }
    Ok(n as i64)
}

/* ─────────────────────────── BITAND ──────────────────────────── */

/// Returns the bitwise AND of two non-negative integers.
///
/// Combines matching bits from both inputs and keeps only bits set in both numbers.
///
/// # Remarks
/// - Arguments are coerced to numbers and must be whole numbers in the range `[0, 2^48)`.
/// - Returns `#NUM!` for negative values, fractional values, or values outside the supported range.
/// - Propagates input errors.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Mask selected bits"
/// formula: "=BITAND(13,10)"
/// expected: 8
/// ```
///
/// ```yaml,sandbox
/// title: "Check least-significant bit"
/// formula: "=BITAND(7,1)"
/// expected: 1
/// ```
/// ```yaml,docs
/// related:
///   - BITOR
///   - BITXOR
///   - BITLSHIFT
/// faq:
///   - q: "When does `BITAND` return `#NUM!`?"
///     a: "Inputs must be whole numbers in `[0, 2^48)`; negatives, fractions, and out-of-range values return `#NUM!`."
/// ```
#[derive(Debug)]
pub struct BitAndFn;
/// [formualizer-docgen:schema:start]
/// Name: BITAND
/// Type: BitAndFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: BITAND(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BitAndFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BITAND"
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
        let a = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match to_bitwise_int(&other) {
                Ok(n) => n,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };
        let b = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match to_bitwise_int(&other) {
                Ok(n) => n,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (a & b) as f64,
        )))
    }
}

/* ─────────────────────────── BITOR ──────────────────────────── */

/// Returns the bitwise OR of two non-negative integers.
///
/// Combines matching bits from both inputs and keeps bits set in either number.
///
/// # Remarks
/// - Arguments are coerced to numbers and must be whole numbers in the range `[0, 2^48)`.
/// - Returns `#NUM!` for negative values, fractional values, or out-of-range values.
/// - Propagates input errors.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Merge bit flags"
/// formula: "=BITOR(13,10)"
/// expected: 15
/// ```
///
/// ```yaml,sandbox
/// title: "Set an additional bit"
/// formula: "=BITOR(8,1)"
/// expected: 9
/// ```
/// ```yaml,docs
/// related:
///   - BITAND
///   - BITXOR
///   - BITRSHIFT
/// faq:
///   - q: "Does `BITOR` accept decimal-looking values like `3.0`?"
///     a: "Yes if they coerce to whole integers; non-integer values still return `#NUM!`."
/// ```
#[derive(Debug)]
pub struct BitOrFn;
/// [formualizer-docgen:schema:start]
/// Name: BITOR
/// Type: BitOrFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: BITOR(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BitOrFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BITOR"
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
        let a = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match to_bitwise_int(&other) {
                Ok(n) => n,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };
        let b = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match to_bitwise_int(&other) {
                Ok(n) => n,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (a | b) as f64,
        )))
    }
}

/* ─────────────────────────── BITXOR ──────────────────────────── */

/// Returns the bitwise exclusive OR of two non-negative integers.
///
/// Keeps bits that differ between the two inputs.
///
/// # Remarks
/// - Arguments are coerced to numbers and must be whole numbers in the range `[0, 2^48)`.
/// - Returns `#NUM!` for negative values, fractional values, or out-of-range values.
/// - Propagates input errors.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Highlight differing bits"
/// formula: "=BITXOR(13,10)"
/// expected: 7
/// ```
///
/// ```yaml,sandbox
/// title: "XOR identical values"
/// formula: "=BITXOR(5,5)"
/// expected: 0
/// ```
/// ```yaml,docs
/// related:
///   - BITAND
///   - BITOR
///   - BITLSHIFT
/// faq:
///   - q: "Why does `BITXOR(x, x)` return `0`?"
///     a: "XOR keeps only differing bits; identical operands cancel every bit position."
/// ```
#[derive(Debug)]
pub struct BitXorFn;
/// [formualizer-docgen:schema:start]
/// Name: BITXOR
/// Type: BitXorFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: BITXOR(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BitXorFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BITXOR"
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
        let a = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match to_bitwise_int(&other) {
                Ok(n) => n,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };
        let b = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match to_bitwise_int(&other) {
                Ok(n) => n,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (a ^ b) as f64,
        )))
    }
}

/* ─────────────────────────── BITLSHIFT ──────────────────────────── */

/// Shifts a non-negative integer left or right by a given bit count.
///
/// Positive `shift_amount` shifts left; negative `shift_amount` shifts right.
///
/// # Remarks
/// - `number` must be a whole number in `[0, 2^48)`.
/// - Shift values are numerically coerced; large positive shifts can return `#NUM!`.
/// - Left-shift results must remain below `2^48`, or the function returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Shift left by two bits"
/// formula: "=BITLSHIFT(6,2)"
/// expected: 24
/// ```
///
/// ```yaml,sandbox
/// title: "Use negative shift to move right"
/// formula: "=BITLSHIFT(32,-3)"
/// expected: 4
/// ```
/// ```yaml,docs
/// related:
///   - BITRSHIFT
///   - BITAND
///   - BITOR
/// faq:
///   - q: "What does a negative `shift_amount` do in `BITLSHIFT`?"
///     a: "Negative shifts are interpreted as right shifts, while positive shifts move bits left."
/// ```
#[derive(Debug)]
pub struct BitLShiftFn;
/// [formualizer-docgen:schema:start]
/// Name: BITLSHIFT
/// Type: BitLShiftFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: BITLSHIFT(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BitLShiftFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BITLSHIFT"
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
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match to_bitwise_int(&other) {
                Ok(n) => n,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };
        let shift = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)? as i32,
        };

        // Negative shift means right shift
        let result = if shift >= 0 {
            if shift >= 48 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            let shifted = n << shift;
            // Check if result exceeds 48-bit limit
            if shifted >= 281474976710656 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            shifted
        } else {
            let rshift = (-shift) as u32;
            if rshift >= 48 { 0 } else { n >> rshift }
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result as f64,
        )))
    }
}

/* ─────────────────────────── BITRSHIFT ──────────────────────────── */

/// Shifts a non-negative integer right or left by a given bit count.
///
/// Positive `shift_amount` shifts right; negative `shift_amount` shifts left.
///
/// # Remarks
/// - `number` must be a whole number in `[0, 2^48)`.
/// - Shift values are numerically coerced; large right shifts return `0`.
/// - Negative shifts that overflow the 48-bit limit return `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Shift right by three bits"
/// formula: "=BITRSHIFT(32,3)"
/// expected: 4
/// ```
///
/// ```yaml,sandbox
/// title: "Use negative shift to move left"
/// formula: "=BITRSHIFT(5,-1)"
/// expected: 10
/// ```
/// ```yaml,docs
/// related:
///   - BITLSHIFT
///   - BITAND
///   - BITXOR
/// faq:
///   - q: "Why can negative shifts in `BITRSHIFT` return `#NUM!`?"
///     a: "A negative shift means left-shift; if that left result exceeds the 48-bit limit, `#NUM!` is returned."
/// ```
#[derive(Debug)]
pub struct BitRShiftFn;
/// [formualizer-docgen:schema:start]
/// Name: BITRSHIFT
/// Type: BitRShiftFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: BITRSHIFT(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BitRShiftFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BITRSHIFT"
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
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match to_bitwise_int(&other) {
                Ok(n) => n,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };
        let shift = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)? as i32,
        };

        // Negative shift means left shift
        let result = if shift >= 0 {
            if shift >= 48 { 0 } else { n >> shift }
        } else {
            let lshift = (-shift) as u32;
            if lshift >= 48 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            let shifted = n << lshift;
            // Check if result exceeds 48-bit limit
            if shifted >= 281474976710656 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            shifted
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result as f64,
        )))
    }
}

/* ─────────────────────────── Base Conversion Functions ──────────────────────────── */

use super::utils::ARG_ANY_ONE;

/// Helper to coerce value to text for base conversion
fn coerce_base_text(v: &LiteralValue) -> Result<String, ExcelError> {
    match v {
        LiteralValue::Text(s) => Ok(s.clone()),
        LiteralValue::Int(i) => Ok(i.to_string()),
        LiteralValue::Number(n) => Ok((*n as i64).to_string()),
        LiteralValue::Error(e) => Err(e.clone()),
        _ => Err(ExcelError::new_value()),
    }
}

/// Converts a binary text value to decimal.
///
/// Supports up to 10 binary digits, including two's-complement negative values.
///
/// # Remarks
/// - Input is coerced to text and must contain only `0` and `1`.
/// - 10-digit values starting with `1` are interpreted as signed two's-complement numbers.
/// - Returns `#NUM!` for invalid characters or inputs longer than 10 digits.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert an unsigned binary value"
/// formula: "=BIN2DEC(\"101010\")"
/// expected: 42
/// ```
///
/// ```yaml,sandbox
/// title: "Interpret signed 10-bit binary"
/// formula: "=BIN2DEC(\"1111111111\")"
/// expected: -1
/// ```
/// ```yaml,docs
/// related:
///   - DEC2BIN
///   - BIN2HEX
///   - BIN2OCT
/// faq:
///   - q: "How does `BIN2DEC` handle 10-bit values starting with `1`?"
///     a: "They are interpreted as signed two's-complement values, so `1111111111` becomes `-1`."
/// ```
#[derive(Debug)]
pub struct Bin2DecFn;
/// [formualizer-docgen:schema:start]
/// Name: BIN2DEC
/// Type: Bin2DecFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: BIN2DEC(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Bin2DecFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BIN2DEC"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_base_text(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        // Excel accepts 10-character binary (with sign bit)
        if text.len() > 10 || !text.chars().all(|c| c == '0' || c == '1') {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // Handle two's complement for negative numbers (10 bits, first bit is sign)
        let result = if text.len() == 10 && text.starts_with('1') {
            // Negative number in two's complement
            let val = i64::from_str_radix(&text, 2).unwrap_or(0);
            val - 1024 // 2^10
        } else {
            i64::from_str_radix(&text, 2).unwrap_or(0)
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result as f64,
        )))
    }
}

/// Converts a decimal integer to binary text.
///
/// Optionally pads the result with leading zeros using `places`.
///
/// # Remarks
/// - `number` is coerced to an integer and must be in `[-512, 511]`.
/// - Negative values are returned as 10-bit two's-complement binary strings.
/// - `places` must be at least the output width and at most `10`, or `#NUM!` is returned.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert a positive integer"
/// formula: "=DEC2BIN(42)"
/// expected: "101010"
/// ```
///
/// ```yaml,sandbox
/// title: "Pad binary output"
/// formula: "=DEC2BIN(5,8)"
/// expected: "00000101"
/// ```
/// ```yaml,docs
/// related:
///   - BIN2DEC
///   - DEC2HEX
///   - DEC2OCT
/// faq:
///   - q: "What limits apply to `DEC2BIN`?"
///     a: "`number` must be in `[-512, 511]`, and optional `places` must be between output width and `10`, else `#NUM!`."
/// ```
#[derive(Debug)]
pub struct Dec2BinFn;
/// [formualizer-docgen:schema:start]
/// Name: DEC2BIN
/// Type: Dec2BinFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: DEC2BIN(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Dec2BinFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DEC2BIN"
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
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)? as i64,
        };

        // Excel limits: -512 to 511
        if !(-512..=511).contains(&n) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let places = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => Some(coerce_num(&other)? as usize),
            }
        } else {
            None
        };

        let binary = if n >= 0 {
            format!("{:b}", n)
        } else {
            // Two's complement with 10 bits
            format!("{:010b}", (n + 1024) as u64)
        };

        let result = if let Some(p) = places {
            if p < binary.len() || p > 10 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            format!("{:0>width$}", binary, width = p)
        } else {
            binary
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Converts a hexadecimal text value to decimal.
///
/// Supports up to 10 hex digits, including signed two's-complement values.
///
/// # Remarks
/// - Input is coerced to text and must contain only hexadecimal characters.
/// - 10-digit values beginning with `8`-`F` are interpreted as signed 40-bit numbers.
/// - Returns `#NUM!` for invalid characters or inputs longer than 10 digits.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert a positive hex value"
/// formula: "=HEX2DEC(\"FF\")"
/// expected: 255
/// ```
///
/// ```yaml,sandbox
/// title: "Interpret signed 40-bit hex"
/// formula: "=HEX2DEC(\"FFFFFFFFFF\")"
/// expected: -1
/// ```
/// ```yaml,docs
/// related:
///   - DEC2HEX
///   - HEX2BIN
///   - HEX2OCT
/// faq:
///   - q: "When is a 10-digit hex input treated as negative in `HEX2DEC`?"
///     a: "If the first digit is `8` through `F`, it is decoded as signed 40-bit two's-complement."
/// ```
#[derive(Debug)]
pub struct Hex2DecFn;
/// [formualizer-docgen:schema:start]
/// Name: HEX2DEC
/// Type: Hex2DecFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: HEX2DEC(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Hex2DecFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "HEX2DEC"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_base_text(&other) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        // Excel accepts 10-character hex
        if text.len() > 10 || !text.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if text.len() == 10 && text.starts_with(|c| c >= '8') {
            // Negative number in two's complement (40 bits)
            let val = i64::from_str_radix(&text, 16).unwrap_or(0);
            val - (1i64 << 40)
        } else {
            i64::from_str_radix(&text, 16).unwrap_or(0)
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result as f64,
        )))
    }
}

/// Converts a decimal integer to hexadecimal text.
///
/// Optionally pads the result with leading zeros using `places`.
///
/// # Remarks
/// - `number` is coerced to an integer and must be in `[-2^39, 2^39 - 1]`.
/// - Negative values are returned as 10-digit two's-complement hexadecimal strings.
/// - `places` must be at least the output width and at most `10`, or `#NUM!` is returned.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert decimal to hex"
/// formula: "=DEC2HEX(255)"
/// expected: "FF"
/// ```
///
/// ```yaml,sandbox
/// title: "Pad hexadecimal output"
/// formula: "=DEC2HEX(31,4)"
/// expected: "001F"
/// ```
/// ```yaml,docs
/// related:
///   - HEX2DEC
///   - DEC2BIN
///   - DEC2OCT
/// faq:
///   - q: "How are negative values formatted by `DEC2HEX`?"
///     a: "Negative outputs use 10-digit two's-complement hexadecimal representation."
/// ```
#[derive(Debug)]
pub struct Dec2HexFn;
/// [formualizer-docgen:schema:start]
/// Name: DEC2HEX
/// Type: Dec2HexFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: DEC2HEX(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Dec2HexFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DEC2HEX"
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
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)? as i64,
        };

        // Excel limits
        if !(-(1i64 << 39)..=(1i64 << 39) - 1).contains(&n) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let places = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => Some(coerce_num(&other)? as usize),
            }
        } else {
            None
        };

        let hex = if n >= 0 {
            format!("{:X}", n)
        } else {
            // Two's complement with 10 hex digits (40 bits)
            format!("{:010X}", (n + (1i64 << 40)) as u64)
        };

        let result = if let Some(p) = places {
            if p < hex.len() || p > 10 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            format!("{:0>width$}", hex, width = p)
        } else {
            hex
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Converts an octal text value to decimal.
///
/// Supports up to 10 octal digits, including signed two's-complement values.
///
/// # Remarks
/// - Input is coerced to text and must contain only digits `0` through `7`.
/// - 10-digit values beginning with `4`-`7` are interpreted as signed 30-bit numbers.
/// - Returns `#NUM!` for invalid characters or inputs longer than 10 digits.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert positive octal"
/// formula: "=OCT2DEC(\"17\")"
/// expected: 15
/// ```
///
/// ```yaml,sandbox
/// title: "Interpret signed 30-bit octal"
/// formula: "=OCT2DEC(\"7777777777\")"
/// expected: -1
/// ```
/// ```yaml,docs
/// related:
///   - DEC2OCT
///   - OCT2BIN
///   - OCT2HEX
/// faq:
///   - q: "How does `OCT2DEC` interpret 10-digit values starting with `4`-`7`?"
///     a: "Those are treated as signed 30-bit two's-complement octal values."
/// ```
#[derive(Debug)]
pub struct Oct2DecFn;
/// [formualizer-docgen:schema:start]
/// Name: OCT2DEC
/// Type: Oct2DecFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: OCT2DEC(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Oct2DecFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "OCT2DEC"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_base_text(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        // Excel accepts 10-character octal (30 bits)
        if text.len() > 10 || !text.chars().all(|c| ('0'..='7').contains(&c)) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if text.len() == 10 && text.starts_with(|c| c >= '4') {
            // Negative number in two's complement (30 bits)
            let val = i64::from_str_radix(&text, 8).unwrap_or(0);
            val - (1i64 << 30)
        } else {
            i64::from_str_radix(&text, 8).unwrap_or(0)
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result as f64,
        )))
    }
}

/// Converts a decimal integer to octal text.
///
/// Optionally pads the result with leading zeros using `places`.
///
/// # Remarks
/// - `number` is coerced to an integer and must be in `[-2^29, 2^29 - 1]`.
/// - Negative values are returned as 10-digit two's-complement octal strings.
/// - `places` must be at least the output width and at most `10`, or `#NUM!` is returned.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert decimal to octal"
/// formula: "=DEC2OCT(64)"
/// expected: "100"
/// ```
///
/// ```yaml,sandbox
/// title: "Two's-complement negative output"
/// formula: "=DEC2OCT(-1)"
/// expected: "7777777777"
/// ```
/// ```yaml,docs
/// related:
///   - OCT2DEC
///   - DEC2BIN
///   - DEC2HEX
/// faq:
///   - q: "What range does `DEC2OCT` support?"
///     a: "`number` must be in `[-2^29, 2^29 - 1]`; outside that range returns `#NUM!`."
/// ```
#[derive(Debug)]
pub struct Dec2OctFn;
/// [formualizer-docgen:schema:start]
/// Name: DEC2OCT
/// Type: Dec2OctFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: DEC2OCT(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Dec2OctFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DEC2OCT"
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
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)? as i64,
        };

        // Excel limits: -536870912 to 536870911
        if !(-(1i64 << 29)..=(1i64 << 29) - 1).contains(&n) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let places = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => Some(coerce_num(&other)? as usize),
            }
        } else {
            None
        };

        let octal = if n >= 0 {
            format!("{:o}", n)
        } else {
            // Two's complement with 10 octal digits (30 bits)
            format!("{:010o}", (n + (1i64 << 30)) as u64)
        };

        let result = if let Some(p) = places {
            if p < octal.len() || p > 10 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            format!("{:0>width$}", octal, width = p)
        } else {
            octal
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/* ─────────────────────────── Cross-Base Conversions ──────────────────────────── */

/// Converts a binary text value to hexadecimal text.
///
/// Optionally pads the output with leading zeros using `places`.
///
/// # Remarks
/// - Input must be a binary string up to 10 digits; 10-digit values may be signed.
/// - Signed binary values are converted using two's-complement semantics.
/// - `places` must be at least the output width and at most `10`, or `#NUM!` is returned.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert binary to hex"
/// formula: "=BIN2HEX(\"1010\")"
/// expected: "A"
/// ```
///
/// ```yaml,sandbox
/// title: "Pad hexadecimal output"
/// formula: "=BIN2HEX(\"1010\",4)"
/// expected: "000A"
/// ```
/// ```yaml,docs
/// related:
///   - HEX2BIN
///   - BIN2DEC
///   - DEC2HEX
/// faq:
///   - q: "Does `BIN2HEX` preserve signed binary meaning?"
///     a: "Yes. A 10-bit binary with leading `1` is interpreted as signed and converted using two's-complement semantics."
/// ```
#[derive(Debug)]
pub struct Bin2HexFn;
/// [formualizer-docgen:schema:start]
/// Name: BIN2HEX
/// Type: Bin2HexFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: BIN2HEX(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Bin2HexFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BIN2HEX"
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
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_base_text(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        if text.len() > 10 || !text.chars().all(|c| c == '0' || c == '1') {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // Convert binary to decimal first
        let dec = if text.len() == 10 && text.starts_with('1') {
            let val = i64::from_str_radix(&text, 2).unwrap_or(0);
            val - 1024
        } else {
            i64::from_str_radix(&text, 2).unwrap_or(0)
        };

        let places = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => Some(coerce_num(&other)? as usize),
            }
        } else {
            None
        };

        let hex = if dec >= 0 {
            format!("{:X}", dec)
        } else {
            format!("{:010X}", (dec + (1i64 << 40)) as u64)
        };

        let result = if let Some(p) = places {
            if p < hex.len() || p > 10 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            format!("{:0>width$}", hex, width = p)
        } else {
            hex
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Converts a hexadecimal text value to binary text.
///
/// Supports optional left-padding through the `places` argument.
///
/// # Remarks
/// - Input must be hexadecimal text up to 10 characters and may be signed two's-complement.
/// - The converted decimal value must be in `[-512, 511]`, or the function returns `#NUM!`.
/// - `places` must be at least the output width and at most `10`, or `#NUM!` is returned.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert positive hex to binary"
/// formula: "=HEX2BIN(\"1F\")"
/// expected: "11111"
/// ```
///
/// ```yaml,sandbox
/// title: "Convert signed hex"
/// formula: "=HEX2BIN(\"FFFFFFFFFF\")"
/// expected: "1111111111"
/// ```
/// ```yaml,docs
/// related:
///   - BIN2HEX
///   - HEX2DEC
///   - DEC2BIN
/// faq:
///   - q: "Why can valid hex text still produce `#NUM!` in `HEX2BIN`?"
///     a: "After conversion, the decimal value must fit `[-512, 511]`; otherwise binary output is rejected."
/// ```
#[derive(Debug)]
pub struct Hex2BinFn;
/// [formualizer-docgen:schema:start]
/// Name: HEX2BIN
/// Type: Hex2BinFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: HEX2BIN(arg1: any@scalar, arg2...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Hex2BinFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "HEX2BIN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_base_text(&other) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        if text.len() > 10 || !text.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // Convert hex to decimal first
        let dec = if text.len() == 10 && text.starts_with(|c| c >= '8') {
            let val = i64::from_str_radix(&text, 16).unwrap_or(0);
            val - (1i64 << 40)
        } else {
            i64::from_str_radix(&text, 16).unwrap_or(0)
        };

        // Check range for binary output (-512 to 511)
        if !(-512..=511).contains(&dec) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let places = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => Some(coerce_num(&other)? as usize),
            }
        } else {
            None
        };

        let binary = if dec >= 0 {
            format!("{:b}", dec)
        } else {
            format!("{:010b}", (dec + 1024) as u64)
        };

        let result = if let Some(p) = places {
            if p < binary.len() || p > 10 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            format!("{:0>width$}", binary, width = p)
        } else {
            binary
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Converts a binary text value to octal text.
///
/// Supports optional left-padding through the `places` argument.
///
/// # Remarks
/// - Input must be binary text up to 10 digits and may be signed two's-complement.
/// - Signed values are preserved through conversion to octal.
/// - `places` must be at least the output width and at most `10`, or `#NUM!` is returned.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert binary to octal"
/// formula: "=BIN2OCT(\"111111\")"
/// expected: "77"
/// ```
///
/// ```yaml,sandbox
/// title: "Pad octal output"
/// formula: "=BIN2OCT(\"111111\",4)"
/// expected: "0077"
/// ```
/// ```yaml,docs
/// related:
///   - OCT2BIN
///   - BIN2DEC
///   - DEC2OCT
/// faq:
///   - q: "How are signed 10-bit binaries handled by `BIN2OCT`?"
///     a: "They are first decoded as signed decimal and then re-encoded to octal with two's-complement output for negatives."
/// ```
#[derive(Debug)]
pub struct Bin2OctFn;
/// [formualizer-docgen:schema:start]
/// Name: BIN2OCT
/// Type: Bin2OctFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: BIN2OCT(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Bin2OctFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BIN2OCT"
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
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_base_text(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        if text.len() > 10 || !text.chars().all(|c| c == '0' || c == '1') {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let dec = if text.len() == 10 && text.starts_with('1') {
            let val = i64::from_str_radix(&text, 2).unwrap_or(0);
            val - 1024
        } else {
            i64::from_str_radix(&text, 2).unwrap_or(0)
        };

        let places = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => Some(coerce_num(&other)? as usize),
            }
        } else {
            None
        };

        let octal = if dec >= 0 {
            format!("{:o}", dec)
        } else {
            format!("{:010o}", (dec + (1i64 << 30)) as u64)
        };

        let result = if let Some(p) = places {
            if p < octal.len() || p > 10 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            format!("{:0>width$}", octal, width = p)
        } else {
            octal
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Converts an octal text value to binary text.
///
/// Supports optional left-padding through the `places` argument.
///
/// # Remarks
/// - Input must be octal text up to 10 digits and may be signed two's-complement.
/// - Converted values must fall in `[-512, 511]`, or the function returns `#NUM!`.
/// - `places` must be at least the output width and at most `10`, or `#NUM!` is returned.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert octal to binary"
/// formula: "=OCT2BIN(\"77\")"
/// expected: "111111"
/// ```
///
/// ```yaml,sandbox
/// title: "Convert signed octal"
/// formula: "=OCT2BIN(\"7777777777\")"
/// expected: "1111111111"
/// ```
/// ```yaml,docs
/// related:
///   - BIN2OCT
///   - OCT2DEC
///   - DEC2BIN
/// faq:
///   - q: "Why does `OCT2BIN` return `#NUM!` for some octal inputs?"
///     a: "After decoding, the value must be within `[-512, 511]` to be representable in Excel-style binary output."
/// ```
#[derive(Debug)]
pub struct Oct2BinFn;
/// [formualizer-docgen:schema:start]
/// Name: OCT2BIN
/// Type: Oct2BinFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: OCT2BIN(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Oct2BinFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "OCT2BIN"
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
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_base_text(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        if text.len() > 10 || !text.chars().all(|c| ('0'..='7').contains(&c)) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let dec = if text.len() == 10 && text.starts_with(|c| c >= '4') {
            let val = i64::from_str_radix(&text, 8).unwrap_or(0);
            val - (1i64 << 30)
        } else {
            i64::from_str_radix(&text, 8).unwrap_or(0)
        };

        // Check range for binary output (-512 to 511)
        if !(-512..=511).contains(&dec) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let places = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => Some(coerce_num(&other)? as usize),
            }
        } else {
            None
        };

        let binary = if dec >= 0 {
            format!("{:b}", dec)
        } else {
            format!("{:010b}", (dec + 1024) as u64)
        };

        let result = if let Some(p) = places {
            if p < binary.len() || p > 10 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            format!("{:0>width$}", binary, width = p)
        } else {
            binary
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Converts a hexadecimal text value to octal text.
///
/// Supports optional left-padding through the `places` argument.
///
/// # Remarks
/// - Input must be hexadecimal text up to 10 characters and may be signed two's-complement.
/// - Converted values must fit the octal range `[-2^29, 2^29 - 1]`, or `#NUM!` is returned.
/// - `places` must be at least the output width and at most `10`, or `#NUM!` is returned.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert hex to octal"
/// formula: "=HEX2OCT(\"1F\")"
/// expected: "37"
/// ```
///
/// ```yaml,sandbox
/// title: "Convert signed hex"
/// formula: "=HEX2OCT(\"FFFFFFFFFF\")"
/// expected: "7777777777"
/// ```
/// ```yaml,docs
/// related:
///   - OCT2HEX
///   - HEX2DEC
///   - DEC2OCT
/// faq:
///   - q: "What causes `HEX2OCT` to return `#NUM!`?"
///     a: "The decoded value must fit octal output range `[-2^29, 2^29 - 1]`, and optional `places` must be valid."
/// ```
#[derive(Debug)]
pub struct Hex2OctFn;
/// [formualizer-docgen:schema:start]
/// Name: HEX2OCT
/// Type: Hex2OctFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: HEX2OCT(arg1: any@scalar, arg2...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Hex2OctFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "HEX2OCT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_base_text(&other) {
                Ok(s) => s.to_uppercase(),
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        if text.len() > 10 || !text.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let dec = if text.len() == 10 && text.starts_with(|c| c >= '8') {
            let val = i64::from_str_radix(&text, 16).unwrap_or(0);
            val - (1i64 << 40)
        } else {
            i64::from_str_radix(&text, 16).unwrap_or(0)
        };

        // Check range for octal output
        if !(-(1i64 << 29)..=(1i64 << 29) - 1).contains(&dec) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let places = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => Some(coerce_num(&other)? as usize),
            }
        } else {
            None
        };

        let octal = if dec >= 0 {
            format!("{:o}", dec)
        } else {
            format!("{:010o}", (dec + (1i64 << 30)) as u64)
        };

        let result = if let Some(p) = places {
            if p < octal.len() || p > 10 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            format!("{:0>width$}", octal, width = p)
        } else {
            octal
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Converts an octal text value to hexadecimal text.
///
/// Supports optional left-padding through the `places` argument.
///
/// # Remarks
/// - Input must be octal text up to 10 digits and may be signed two's-complement.
/// - Signed values are converted through their decimal representation.
/// - `places` must be at least the output width and at most `10`, or `#NUM!` is returned.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Convert octal to hex"
/// formula: "=OCT2HEX(\"77\")"
/// expected: "3F"
/// ```
///
/// ```yaml,sandbox
/// title: "Convert signed octal"
/// formula: "=OCT2HEX(\"7777777777\")"
/// expected: "FFFFFFFFFF"
/// ```
/// ```yaml,docs
/// related:
///   - HEX2OCT
///   - OCT2DEC
///   - DEC2HEX
/// faq:
///   - q: "How does `OCT2HEX` treat signed octal input?"
///     a: "Signed 10-digit octal is decoded via two's-complement and then emitted as hex, preserving signed meaning."
/// ```
#[derive(Debug)]
pub struct Oct2HexFn;
/// [formualizer-docgen:schema:start]
/// Name: OCT2HEX
/// Type: Oct2HexFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: OCT2HEX(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Oct2HexFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "OCT2HEX"
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
        let text = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_base_text(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        if text.len() > 10 || !text.chars().all(|c| ('0'..='7').contains(&c)) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let dec = if text.len() == 10 && text.starts_with(|c| c >= '4') {
            let val = i64::from_str_radix(&text, 8).unwrap_or(0);
            val - (1i64 << 30)
        } else {
            i64::from_str_radix(&text, 8).unwrap_or(0)
        };

        let places = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => Some(coerce_num(&other)? as usize),
            }
        } else {
            None
        };

        let hex = if dec >= 0 {
            format!("{:X}", dec)
        } else {
            format!("{:010X}", (dec + (1i64 << 40)) as u64)
        };

        let result = if let Some(p) = places {
            if p < hex.len() || p > 10 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            format!("{:0>width$}", hex, width = p)
        } else {
            hex
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/* ─────────────────────────── Engineering Comparison Functions ──────────────────────────── */

/// Tests whether two numbers are equal.
///
/// Returns `1` when values match and `0` otherwise.
///
/// # Remarks
/// - If `number2` is omitted, it defaults to `0`.
/// - Inputs are numerically coerced.
/// - Uses a small numeric tolerance for floating-point comparison.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Equal values"
/// formula: "=DELTA(5,5)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Default second argument"
/// formula: "=DELTA(2.5)"
/// expected: 0
/// ```
/// ```yaml,docs
/// related:
///   - GESTEP
/// faq:
///   - q: "Does `DELTA` require exact floating-point equality?"
///     a: "It uses a small tolerance (`1e-12`), so values that differ only by tiny floating noise compare as equal."
/// ```
#[derive(Debug)]
pub struct DeltaFn;
/// [formualizer-docgen:schema:start]
/// Name: DELTA
/// Type: DeltaFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: DELTA(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for DeltaFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DELTA"
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
        let n1 = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let n2 = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)?,
            }
        } else {
            0.0
        };

        let result = if (n1 - n2).abs() < 1e-12 { 1.0 } else { 0.0 };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns `1` when a number is greater than or equal to a step value.
///
/// Returns `0` when the number is below the step.
///
/// # Remarks
/// - If `step` is omitted, it defaults to `0`.
/// - Inputs are numerically coerced.
/// - Propagates input errors.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Value meets threshold"
/// formula: "=GESTEP(5,3)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Default threshold of zero"
/// formula: "=GESTEP(-2)"
/// expected: 0
/// ```
/// ```yaml,docs
/// related:
///   - DELTA
/// faq:
///   - q: "What default threshold does `GESTEP` use?"
///     a: "If omitted, `step` defaults to `0`, so the function returns `1` for non-negative inputs."
/// ```
#[derive(Debug)]
pub struct GestepFn;
/// [formualizer-docgen:schema:start]
/// Name: GESTEP
/// Type: GestepFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: GESTEP(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for GestepFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "GESTEP"
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
        let n = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };
        let step = if args.len() > 1 {
            match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)?,
            }
        } else {
            0.0
        };

        let result = if n >= step { 1.0 } else { 0.0 };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/* ─────────────────────────── Error Function ──────────────────────────── */

/// Approximation of the error function erf(x)
/// Uses the approximation: erf(x) = 1 - (a1*t + a2*t^2 + a3*t^3 + a4*t^4 + a5*t^5) * exp(-x^2)
/// High-precision error function using Cody's rational approximation
/// Achieves precision of about 1e-15 (double precision)
#[allow(clippy::excessive_precision)]
fn erf_approx(x: f64) -> f64 {
    let ax = x.abs();

    // For small x, use series expansion
    if ax < 0.5 {
        // Coefficients for erf(x) = x * P(x^2) / Q(x^2)
        const P: [f64; 5] = [
            3.20937758913846947e+03,
            3.77485237685302021e+02,
            1.13864154151050156e+02,
            3.16112374387056560e+00,
            1.85777706184603153e-01,
        ];
        const Q: [f64; 5] = [
            2.84423748127893300e+03,
            1.28261652607737228e+03,
            2.44024637934444173e+02,
            2.36012909523441209e+01,
            1.00000000000000000e+00,
        ];

        let x2 = x * x;
        let p_val = P[4];
        let p_val = p_val * x2 + P[3];
        let p_val = p_val * x2 + P[2];
        let p_val = p_val * x2 + P[1];
        let p_val = p_val * x2 + P[0];

        let q_val = Q[4];
        let q_val = q_val * x2 + Q[3];
        let q_val = q_val * x2 + Q[2];
        let q_val = q_val * x2 + Q[1];
        let q_val = q_val * x2 + Q[0];

        return x * p_val / q_val;
    }

    // For x in [0.5, 4], use erfc approximation and compute erf = 1 - erfc
    if ax < 4.0 {
        let erfc_val = erfc_mid(ax);
        return if x > 0.0 {
            1.0 - erfc_val
        } else {
            erfc_val - 1.0
        };
    }

    // For large x, erf(x) ≈ ±1
    let erfc_val = erfc_large(ax);
    if x > 0.0 {
        1.0 - erfc_val
    } else {
        erfc_val - 1.0
    }
}

/// erfc for x in [0.5, 4]
#[allow(clippy::excessive_precision)]
fn erfc_mid(x: f64) -> f64 {
    const P: [f64; 9] = [
        1.23033935479799725e+03,
        2.05107837782607147e+03,
        1.71204761263407058e+03,
        8.81952221241769090e+02,
        2.98635138197400131e+02,
        6.61191906371416295e+01,
        8.88314979438837594e+00,
        5.64188496988670089e-01,
        2.15311535474403846e-08,
    ];
    const Q: [f64; 9] = [
        1.23033935480374942e+03,
        3.43936767414372164e+03,
        4.36261909014324716e+03,
        3.29079923573345963e+03,
        1.62138957456669019e+03,
        5.37181101862009858e+02,
        1.17693950891312499e+02,
        1.57449261107098347e+01,
        1.00000000000000000e+00,
    ];

    let p_val = P[8];
    let p_val = p_val * x + P[7];
    let p_val = p_val * x + P[6];
    let p_val = p_val * x + P[5];
    let p_val = p_val * x + P[4];
    let p_val = p_val * x + P[3];
    let p_val = p_val * x + P[2];
    let p_val = p_val * x + P[1];
    let p_val = p_val * x + P[0];

    let q_val = Q[8];
    let q_val = q_val * x + Q[7];
    let q_val = q_val * x + Q[6];
    let q_val = q_val * x + Q[5];
    let q_val = q_val * x + Q[4];
    let q_val = q_val * x + Q[3];
    let q_val = q_val * x + Q[2];
    let q_val = q_val * x + Q[1];
    let q_val = q_val * x + Q[0];

    (-x * x).exp() * p_val / q_val
}

/// erfc for x >= 4
#[allow(clippy::excessive_precision)]
fn erfc_large(x: f64) -> f64 {
    const P: [f64; 6] = [
        6.58749161529837803e-04,
        1.60837851487422766e-02,
        1.25781726111229246e-01,
        3.60344899949804439e-01,
        3.05326634961232344e-01,
        1.63153871373020978e-02,
    ];
    const Q: [f64; 6] = [
        2.33520497626869185e-03,
        6.05183413124413191e-02,
        5.27905102951428412e-01,
        1.87295284992346047e+00,
        2.56852019228982242e+00,
        1.00000000000000000e+00,
    ];

    let x2 = x * x;
    let inv_x2 = 1.0 / x2;

    let p_val = P[5];
    let p_val = p_val * inv_x2 + P[4];
    let p_val = p_val * inv_x2 + P[3];
    let p_val = p_val * inv_x2 + P[2];
    let p_val = p_val * inv_x2 + P[1];
    let p_val = p_val * inv_x2 + P[0];

    let q_val = Q[5];
    let q_val = q_val * inv_x2 + Q[4];
    let q_val = q_val * inv_x2 + Q[3];
    let q_val = q_val * inv_x2 + Q[2];
    let q_val = q_val * inv_x2 + Q[1];
    let q_val = q_val * inv_x2 + Q[0];

    // 1/sqrt(pi) = 0.5641895835477563
    const FRAC_1_SQRT_PI: f64 = 0.5641895835477563;
    (-x2).exp() / x * (FRAC_1_SQRT_PI + inv_x2 * p_val / q_val)
}

/// Direct erfc computation for ERFC function
fn erfc_direct(x: f64) -> f64 {
    if x < 0.0 {
        return 2.0 - erfc_direct(-x);
    }
    if x < 0.5 {
        return 1.0 - erf_approx(x);
    }
    if x < 4.0 {
        return erfc_mid(x);
    }
    erfc_large(x)
}

/// Returns the Gaussian error function over one bound or between two bounds.
///
/// With one argument it returns `erf(x)`; with two it returns `erf(upper) - erf(lower)`.
///
/// # Remarks
/// - Inputs are numerically coerced.
/// - A second argument switches the function to interval mode.
/// - Results are approximate floating-point values.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Single-bound ERF"
/// formula: "=ERF(1)"
/// expected: 0.8427007929497149
/// ```
///
/// ```yaml,sandbox
/// title: "Interval ERF"
/// formula: "=ERF(0,1)"
/// expected: 0.8427007929497149
/// ```
/// ```yaml,docs
/// related:
///   - ERFC
///   - ERF.PRECISE
/// faq:
///   - q: "How does two-argument `ERF` work?"
///     a: "`ERF(lower, upper)` returns `erf(upper) - erf(lower)`, i.e., an interval difference rather than a single-bound value."
/// ```
#[derive(Debug)]
pub struct ErfFn;
/// [formualizer-docgen:schema:start]
/// Name: ERF
/// Type: ErfFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: ERF(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ErfFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ERF"
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
        let lower = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };

        let result = if args.len() > 1 {
            // ERF(lower, upper) = erf(upper) - erf(lower)
            let upper = match args[1].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => coerce_num(&other)?,
            };
            erf_approx(upper) - erf_approx(lower)
        } else {
            // ERF(x) = erf(x)
            erf_approx(lower)
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the complementary error function of a number.
///
/// `ERFC(x)` is equivalent to `1 - ERF(x)`.
///
/// # Remarks
/// - Input is numerically coerced.
/// - Results are approximate floating-point values.
/// - Propagates input errors.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Complement at one"
/// formula: "=ERFC(1)"
/// expected: 0.1572992070502851
/// ```
///
/// ```yaml,sandbox
/// title: "Complement at zero"
/// formula: "=ERFC(0)"
/// expected: 1
/// ```
/// ```yaml,docs
/// related:
///   - ERF
///   - ERF.PRECISE
/// faq:
///   - q: "Is `ERFC(x)` equivalent to `1-ERF(x)` here?"
///     a: "Yes. It computes the complementary error function and matches `1 - erf(x)` behavior."
/// ```
#[derive(Debug)]
pub struct ErfcFn;
/// [formualizer-docgen:schema:start]
/// Name: ERFC
/// Type: ErfcFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ERFC(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ErfcFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ERFC"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
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

        let result = erfc_direct(x);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the error function of a number.
///
/// This is the one-argument precise variant of `ERF`.
///
/// # Remarks
/// - Input is numerically coerced.
/// - Equivalent to `ERF(x)` in single-argument mode.
/// - Results are approximate floating-point values.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Positive input"
/// formula: "=ERF.PRECISE(1)"
/// expected: 0.8427007929497149
/// ```
///
/// ```yaml,sandbox
/// title: "Negative input"
/// formula: "=ERF.PRECISE(-1)"
/// expected: -0.8427007929497149
/// ```
/// ```yaml,docs
/// related:
///   - ERF
///   - ERFC
/// faq:
///   - q: "How is `ERF.PRECISE` different from `ERF`?"
///     a: "`ERF.PRECISE` is the one-argument form only; numerically it matches `ERF(x)` for single input mode."
/// ```
#[derive(Debug)]
pub struct ErfPreciseFn;
/// [formualizer-docgen:schema:start]
/// Name: ERF.PRECISE
/// Type: ErfPreciseFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ERF.PRECISE(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ErfPreciseFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ERF.PRECISE"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
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

        let result = erf_approx(x);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/* ─────────────────────────── Complex Number Functions ──────────────────────────── */

/// Parse a complex number string like "3+4i", "3-4i", "5i", "3", "-2j", etc.
/// Returns (real, imaginary, suffix) where suffix is 'i' or 'j'
fn parse_complex(s: &str) -> Result<(f64, f64, char), ExcelError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ExcelError::new_num());
    }

    // Determine the suffix (i or j)
    let suffix = if s.ends_with('i') || s.ends_with('I') {
        'i'
    } else if s.ends_with('j') || s.ends_with('J') {
        'j'
    } else {
        // No imaginary suffix - must be purely real
        let real: f64 = s.parse().map_err(|_| ExcelError::new_num())?;
        return Ok((real, 0.0, 'i'));
    };

    // Remove the suffix for parsing
    let s = &s[..s.len() - 1];

    // Handle pure imaginary cases like "i", "-i", "4i"
    if s.is_empty() || s == "+" {
        return Ok((0.0, 1.0, suffix));
    }
    if s == "-" {
        return Ok((0.0, -1.0, suffix));
    }

    // Find the last + or - that separates real and imaginary parts
    // We need to skip the first character (could be a sign) and find operators
    let mut split_pos = None;
    let bytes = s.as_bytes();

    for i in (1..bytes.len()).rev() {
        let c = bytes[i] as char;
        if c == '+' || c == '-' {
            // Make sure this isn't part of an exponent (e.g., "1e-5")
            if i > 0 {
                let prev = bytes[i - 1] as char;
                if prev == 'e' || prev == 'E' {
                    continue;
                }
            }
            split_pos = Some(i);
            break;
        }
    }

    match split_pos {
        Some(pos) => {
            // We have both real and imaginary parts
            let real_str = &s[..pos];
            let imag_str = &s[pos..];

            let real: f64 = if real_str.is_empty() {
                0.0
            } else {
                real_str.parse().map_err(|_| ExcelError::new_num())?
            };

            // Handle imaginary part (could be "+", "-", "+5", "-5", etc.)
            let imag: f64 = if imag_str == "+" {
                1.0
            } else if imag_str == "-" {
                -1.0
            } else {
                imag_str.parse().map_err(|_| ExcelError::new_num())?
            };

            Ok((real, imag, suffix))
        }
        None => {
            // Pure imaginary number (no real part), e.g., "5" (before suffix was removed)
            let imag: f64 = s.parse().map_err(|_| ExcelError::new_num())?;
            Ok((0.0, imag, suffix))
        }
    }
}

/// Clean up floating point noise by rounding values very close to integers
fn clean_float(val: f64) -> f64 {
    let rounded = val.round();
    if (val - rounded).abs() < 1e-10 {
        rounded
    } else {
        val
    }
}

/// Format a complex number as a string
fn format_complex(real: f64, imag: f64, suffix: char) -> String {
    // Clean up floating point noise
    let real = clean_float(real);
    let imag = clean_float(imag);

    // Handle special cases for cleaner output
    let real_is_zero = real.abs() < 1e-15;
    let imag_is_zero = imag.abs() < 1e-15;

    if real_is_zero && imag_is_zero {
        return "0".to_string();
    }

    if imag_is_zero {
        // Purely real
        if real == real.trunc() && real.abs() < 1e15 {
            return format!("{}", real as i64);
        }
        return format!("{}", real);
    }

    if real_is_zero {
        // Purely imaginary
        if (imag - 1.0).abs() < 1e-15 {
            return format!("{}", suffix);
        }
        if (imag + 1.0).abs() < 1e-15 {
            return format!("-{}", suffix);
        }
        if imag == imag.trunc() && imag.abs() < 1e15 {
            return format!("{}{}", imag as i64, suffix);
        }
        return format!("{}{}", imag, suffix);
    }

    // Both parts are non-zero
    let real_str = if real == real.trunc() && real.abs() < 1e15 {
        format!("{}", real as i64)
    } else {
        format!("{}", real)
    };

    let imag_str = if (imag - 1.0).abs() < 1e-15 {
        format!("+{}", suffix)
    } else if (imag + 1.0).abs() < 1e-15 {
        format!("-{}", suffix)
    } else if imag > 0.0 {
        if imag == imag.trunc() && imag.abs() < 1e15 {
            format!("+{}{}", imag as i64, suffix)
        } else {
            format!("+{}{}", imag, suffix)
        }
    } else if imag == imag.trunc() && imag.abs() < 1e15 {
        format!("{}{}", imag as i64, suffix)
    } else {
        format!("{}{}", imag, suffix)
    };

    format!("{}{}", real_str, imag_str)
}

/// Coerce a LiteralValue to a complex number string
fn coerce_complex_str(v: &LiteralValue) -> Result<String, ExcelError> {
    match v {
        LiteralValue::Text(s) => Ok(s.clone()),
        LiteralValue::Int(i) => Ok(i.to_string()),
        LiteralValue::Number(n) => Ok(n.to_string()),
        LiteralValue::Error(e) => Err(e.clone()),
        _ => Err(ExcelError::new_value()),
    }
}

/// Three-argument schema for COMPLEX function
static ARG_COMPLEX_THREE: std::sync::LazyLock<Vec<ArgSchema>> =
    std::sync::LazyLock::new(|| vec![ArgSchema::any(), ArgSchema::any(), ArgSchema::any()]);

/// Builds a complex number text value from real and imaginary coefficients.
///
/// Returns canonical text such as `3+4i` or `-2j`.
///
/// # Remarks
/// - `real_num` and `i_num` are numerically coerced.
/// - `suffix` may be `"i"`, `"j"`, empty text, or omitted; empty/omitted defaults to `i`.
/// - Any other suffix returns `#VALUE!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Build with default suffix"
/// formula: "=COMPLEX(3,4)"
/// expected: "3+4i"
/// ```
///
/// ```yaml,sandbox
/// title: "Build with j suffix"
/// formula: "=COMPLEX(0,-1,\"j\")"
/// expected: "-j"
/// ```
/// ```yaml,docs
/// related:
///   - IMREAL
///   - IMAGINARY
///   - IMSUM
/// faq:
///   - q: "Which suffix values are valid in `COMPLEX`?"
///     a: "Only suffixes i or j are accepted (empty or omitted defaults to i); other suffix strings return `#VALUE!`."
/// ```
#[derive(Debug)]
pub struct ComplexFn;
/// [formualizer-docgen:schema:start]
/// Name: COMPLEX
/// Type: ComplexFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: COMPLEX(arg1: any@scalar, arg2: any@scalar, arg3...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ComplexFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "COMPLEX"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_COMPLEX_THREE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let real = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };

        let imag = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };

        let suffix = if args.len() > 2 {
            match args[2].value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                LiteralValue::Text(s) => {
                    let s = s.to_lowercase();
                    if s == "i" {
                        'i'
                    } else if s == "j" {
                        'j'
                    } else if s.is_empty() {
                        'i' // Default to 'i' for empty string
                    } else {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new_value(),
                        )));
                    }
                }
                LiteralValue::Empty => 'i',
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new_value(),
                    )));
                }
            }
        } else {
            'i'
        };

        let result = format_complex(real, imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the real coefficient of a complex number.
///
/// Accepts complex text (for example `a+bi`) or numeric values.
///
/// # Remarks
/// - Inputs are coerced to complex-number text before parsing.
/// - Purely imaginary values return `0`.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Real part from a+bi"
/// formula: "=IMREAL(\"3+4i\")"
/// expected: 3
/// ```
///
/// ```yaml,sandbox
/// title: "Real part of pure imaginary"
/// formula: "=IMREAL(\"5j\")"
/// expected: 0
/// ```
/// ```yaml,docs
/// related:
///   - IMAGINARY
///   - COMPLEX
///   - IMABS
/// faq:
///   - q: "What does `IMREAL` return for a purely imaginary input?"
///     a: "It returns `0` because the real coefficient is zero."
/// ```
#[derive(Debug)]
pub struct ImRealFn;
/// [formualizer-docgen:schema:start]
/// Name: IMREAL
/// Type: ImRealFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMREAL(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImRealFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMREAL"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (real, _, _) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(real)))
    }
}

/// Returns the imaginary coefficient of a complex number.
///
/// Accepts complex text (for example `a+bi`) or numeric values.
///
/// # Remarks
/// - Inputs are coerced to complex-number text before parsing.
/// - Purely real values return `0`.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Imaginary part from a+bi"
/// formula: "=IMAGINARY(\"3+4i\")"
/// expected: 4
/// ```
///
/// ```yaml,sandbox
/// title: "Imaginary part with j suffix"
/// formula: "=IMAGINARY(\"-2j\")"
/// expected: -2
/// ```
/// ```yaml,docs
/// related:
///   - IMREAL
///   - COMPLEX
///   - IMABS
/// faq:
///   - q: "What does `IMAGINARY` return for a real-only input?"
///     a: "It returns `0` because there is no imaginary component."
/// ```
#[derive(Debug)]
pub struct ImaginaryFn;
/// [formualizer-docgen:schema:start]
/// Name: IMAGINARY
/// Type: ImaginaryFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMAGINARY(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImaginaryFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMAGINARY"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (_, imag, _) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(imag)))
    }
}

/// Returns the modulus (absolute value) of a complex number.
///
/// Computes `sqrt(real^2 + imaginary^2)`.
///
/// # Remarks
/// - Inputs are coerced to complex-number text before parsing.
/// - Returns a non-negative real number.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "3-4-5 triangle modulus"
/// formula: "=IMABS(\"3+4i\")"
/// expected: 5
/// ```
///
/// ```yaml,sandbox
/// title: "Purely real input"
/// formula: "=IMABS(\"5\")"
/// expected: 5
/// ```
/// ```yaml,docs
/// related:
///   - IMREAL
///   - IMAGINARY
///   - IMARGUMENT
/// faq:
///   - q: "Can `IMABS` return a negative result?"
///     a: "No. It computes the modulus `sqrt(a^2+b^2)`, which is always non-negative."
/// ```
#[derive(Debug)]
pub struct ImAbsFn;
/// [formualizer-docgen:schema:start]
/// Name: IMABS
/// Type: ImAbsFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMABS(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImAbsFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMABS"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (real, imag, _) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let abs = (real * real + imag * imag).sqrt();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(abs)))
    }
}

/// Returns the argument (angle in radians) of a complex number.
///
/// The angle is measured from the positive real axis.
///
/// # Remarks
/// - Inputs are coerced to complex-number text before parsing.
/// - Returns `#DIV/0!` for `0+0i`, where the angle is undefined.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "First-quadrant angle"
/// formula: "=IMARGUMENT(\"1+i\")"
/// expected: 0.7853981633974483
/// ```
///
/// ```yaml,sandbox
/// title: "Negative real axis"
/// formula: "=IMARGUMENT(\"-1\")"
/// expected: 3.141592653589793
/// ```
/// ```yaml,docs
/// related:
///   - IMABS
///   - IMLN
///   - IMSQRT
/// faq:
///   - q: "Why does `IMARGUMENT(0)` return `#DIV/0!`?"
///     a: "The argument (angle) of `0+0i` is undefined, so the function returns `#DIV/0!`."
/// ```
#[derive(Debug)]
pub struct ImArgumentFn;
/// [formualizer-docgen:schema:start]
/// Name: IMARGUMENT
/// Type: ImArgumentFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMARGUMENT(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImArgumentFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMARGUMENT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (real, imag, _) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // Excel returns #DIV/0! for IMARGUMENT(0)
        if real.abs() < 1e-15 && imag.abs() < 1e-15 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let arg = imag.atan2(real);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(arg)))
    }
}

/// Returns the complex conjugate of a complex number.
///
/// Negates the imaginary coefficient and keeps the real coefficient unchanged.
///
/// # Remarks
/// - Inputs are coerced to complex-number text before parsing.
/// - Preserves the original suffix style (`i` or `j`) when possible.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Conjugate with i suffix"
/// formula: "=IMCONJUGATE(\"3+4i\")"
/// expected: "3-4i"
/// ```
///
/// ```yaml,sandbox
/// title: "Conjugate with j suffix"
/// formula: "=IMCONJUGATE(\"-2j\")"
/// expected: "2j"
/// ```
/// ```yaml,docs
/// related:
///   - IMSUB
///   - IMPRODUCT
///   - IMDIV
/// faq:
///   - q: "Does `IMCONJUGATE` keep the `i`/`j` suffix style?"
///     a: "Yes. It negates only the imaginary coefficient and preserves the parsed suffix form."
/// ```
#[derive(Debug)]
pub struct ImConjugateFn;
/// [formualizer-docgen:schema:start]
/// Name: IMCONJUGATE
/// Type: ImConjugateFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMCONJUGATE(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImConjugateFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMCONJUGATE"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (real, imag, suffix) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let result = format_complex(real, -imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Helper to check if two complex numbers have compatible suffixes
fn check_suffix_compatibility(s1: char, s2: char) -> Result<char, ExcelError> {
    // If both have the same suffix, use it
    // If one is from a purely real number (default 'i'), use the other's suffix
    // Excel doesn't allow mixing 'i' and 'j' when both are explicit
    if s1 == s2 {
        Ok(s1)
    } else {
        // For simplicity, treat 'i' as the default and allow mixed
        // In strict Excel mode, this would error
        Ok(s1)
    }
}

/// Returns the sum of one or more complex numbers.
///
/// Adds real parts together and imaginary parts together.
///
/// # Remarks
/// - Each argument is coerced to complex-number text before parsing.
/// - Accepts any number of arguments from one upward.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Add multiple complex values"
/// formula: "=IMSUM(\"3+4i\",\"1-2i\",\"5\")"
/// expected: "9+2i"
/// ```
///
/// ```yaml,sandbox
/// title: "Add j-suffix values"
/// formula: "=IMSUM(\"2j\",\"-j\")"
/// expected: "j"
/// ```
/// ```yaml,docs
/// related:
///   - IMSUB
///   - IMPRODUCT
///   - COMPLEX
/// faq:
///   - q: "Can `IMSUM` take more than two arguments?"
///     a: "Yes. It is variadic and sums all provided complex arguments in sequence."
/// ```
#[derive(Debug)]
pub struct ImSumFn;
/// [formualizer-docgen:schema:start]
/// Name: IMSUM
/// Type: ImSumFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: IMSUM(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImSumFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMSUM"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut sum_real = 0.0;
        let mut sum_imag = 0.0;
        let mut result_suffix = 'i';
        let mut first = true;

        for arg in args {
            let inumber = match arg.value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => match coerce_complex_str(&other) {
                    Ok(s) => s,
                    Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
                },
            };

            let (real, imag, suffix) = match parse_complex(&inumber) {
                Ok(c) => c,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            };

            if first {
                result_suffix = suffix;
                first = false;
            } else {
                result_suffix = check_suffix_compatibility(result_suffix, suffix)?;
            }

            sum_real += real;
            sum_imag += imag;
        }

        let result = format_complex(sum_real, sum_imag, result_suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the difference between two complex numbers.
///
/// Subtracts the second complex value from the first.
///
/// # Remarks
/// - Inputs are coerced to complex-number text before parsing.
/// - Output keeps the suffix style from the parsed inputs.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Subtract a+bi values"
/// formula: "=IMSUB(\"5+3i\",\"2+i\")"
/// expected: "3+2i"
/// ```
///
/// ```yaml,sandbox
/// title: "Subtract pure imaginary from real"
/// formula: "=IMSUB(\"4\",\"7j\")"
/// expected: "4-7j"
/// ```
/// ```yaml,docs
/// related:
///   - IMSUM
///   - IMDIV
///   - COMPLEX
/// faq:
///   - q: "How is subtraction ordered in `IMSUB`?"
///     a: "It always computes `inumber1 - inumber2`; swapping arguments changes the sign of the result."
/// ```
#[derive(Debug)]
pub struct ImSubFn;
/// [formualizer-docgen:schema:start]
/// Name: IMSUB
/// Type: ImSubFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: IMSUB(arg1: any@scalar, arg2: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImSubFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMSUB"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber1 = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let inumber2 = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (real1, imag1, suffix1) = match parse_complex(&inumber1) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let (real2, imag2, suffix2) = match parse_complex(&inumber2) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let result_suffix = check_suffix_compatibility(suffix1, suffix2)?;
        let result = format_complex(real1 - real2, imag1 - imag2, result_suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the product of one or more complex numbers.
///
/// Multiplies values sequentially using complex multiplication rules.
///
/// # Remarks
/// - Each argument is coerced to complex-number text before parsing.
/// - Accepts any number of arguments from one upward.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Multiply conjugates"
/// formula: "=IMPRODUCT(\"1+i\",\"1-i\")"
/// expected: "2"
/// ```
///
/// ```yaml,sandbox
/// title: "Scale an imaginary value"
/// formula: "=IMPRODUCT(\"2i\",\"3\")"
/// expected: "6i"
/// ```
/// ```yaml,docs
/// related:
///   - IMDIV
///   - IMSUM
///   - IMPOWER
/// faq:
///   - q: "Can `IMPRODUCT` multiply a single argument?"
///     a: "Yes. With one argument it returns that parsed complex value in canonical formatted form."
/// ```
#[derive(Debug)]
pub struct ImProductFn;
/// [formualizer-docgen:schema:start]
/// Name: IMPRODUCT
/// Type: ImProductFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: IMPRODUCT(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImProductFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMPRODUCT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut prod_real = 1.0;
        let mut prod_imag = 0.0;
        let mut result_suffix = 'i';
        let mut first = true;

        for arg in args {
            let inumber = match arg.value()?.into_literal() {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                other => match coerce_complex_str(&other) {
                    Ok(s) => s,
                    Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
                },
            };

            let (real, imag, suffix) = match parse_complex(&inumber) {
                Ok(c) => c,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            };

            if first {
                result_suffix = suffix;
                prod_real = real;
                prod_imag = imag;
                first = false;
            } else {
                result_suffix = check_suffix_compatibility(result_suffix, suffix)?;
                // (a + bi) * (c + di) = (ac - bd) + (ad + bc)i
                let new_real = prod_real * real - prod_imag * imag;
                let new_imag = prod_real * imag + prod_imag * real;
                prod_real = new_real;
                prod_imag = new_imag;
            }
        }

        let result = format_complex(prod_real, prod_imag, result_suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the quotient of two complex numbers.
///
/// Divides the first complex value by the second.
///
/// # Remarks
/// - Inputs are coerced to complex-number text before parsing.
/// - Returns `#DIV/0!` when the divisor is `0+0i`.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Divide complex numbers"
/// formula: "=IMDIV(\"3+4i\",\"1-i\")"
/// expected: "-0.5+3.5i"
/// ```
///
/// ```yaml,sandbox
/// title: "Division by zero complex"
/// formula: "=IMDIV(\"2+i\",\"0\")"
/// expected: "#DIV/0!"
/// ```
/// ```yaml,docs
/// related:
///   - IMPRODUCT
///   - IMSUB
///   - IMCONJUGATE
/// faq:
///   - q: "When does `IMDIV` return `#DIV/0!`?"
///     a: "If the divisor is `0+0i` (denominator magnitude near zero), division is undefined and returns `#DIV/0!`."
/// ```
#[derive(Debug)]
pub struct ImDivFn;
/// [formualizer-docgen:schema:start]
/// Name: IMDIV
/// Type: ImDivFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: IMDIV(arg1: any@scalar, arg2: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImDivFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMDIV"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber1 = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let inumber2 = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (a, b, suffix1) = match parse_complex(&inumber1) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let (c, d, suffix2) = match parse_complex(&inumber2) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // Division by zero check - returns #DIV/0! for Excel compatibility
        let denom = c * c + d * d;
        if denom.abs() < 1e-15 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let result_suffix = check_suffix_compatibility(suffix1, suffix2)?;

        // (a + bi) / (c + di) = ((ac + bd) + (bc - ad)i) / (c^2 + d^2)
        let real = (a * c + b * d) / denom;
        let imag = (b * c - a * d) / denom;

        let result = format_complex(real, imag, result_suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the complex exponential of a complex number.
///
/// Computes `e^(a+bi)` and returns the result as complex text.
///
/// # Remarks
/// - Input is coerced to complex-number text before parsing.
/// - Uses Euler's identity for the imaginary component.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Exponential of zero"
/// formula: "=IMEXP(\"0\")"
/// expected: "1"
/// ```
///
/// ```yaml,sandbox
/// title: "Exponential of a real value"
/// formula: "=IMEXP(\"1\")"
/// expected: "2.718281828459045"
/// ```
/// ```yaml,docs
/// related:
///   - IMLN
///   - IMPOWER
///   - IMSIN
///   - IMCOS
/// faq:
///   - q: "Does `IMEXP` return text or a numeric complex type?"
///     a: "It returns a canonical complex text string, consistent with other `IM*` functions."
/// ```
#[derive(Debug)]
pub struct ImExpFn;
/// [formualizer-docgen:schema:start]
/// Name: IMEXP
/// Type: ImExpFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMEXP(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImExpFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMEXP"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (a, b, suffix) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // e^(a+bi) = e^a * (cos(b) + i*sin(b))
        let exp_a = a.exp();
        let real = exp_a * b.cos();
        let imag = exp_a * b.sin();

        let result = format_complex(real, imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the natural logarithm of a complex number.
///
/// Produces the principal complex logarithm as text.
///
/// # Remarks
/// - Input is coerced to complex-number text before parsing.
/// - Returns `#NUM!` for zero input because `ln(0)` is undefined.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Natural log of 1"
/// formula: "=IMLN(\"1\")"
/// expected: "0"
/// ```
///
/// ```yaml,sandbox
/// title: "Natural log on imaginary axis"
/// formula: "=IMLN(\"i\")"
/// expected: "1.5707963267948966i"
/// ```
/// ```yaml,docs
/// related:
///   - IMEXP
///   - IMLOG10
///   - IMLOG2
/// faq:
///   - q: "Why does `IMLN(0)` return `#NUM!`?"
///     a: "The complex logarithm at zero is undefined, so this implementation returns `#NUM!`."
/// ```
#[derive(Debug)]
pub struct ImLnFn;
/// [formualizer-docgen:schema:start]
/// Name: IMLN
/// Type: ImLnFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMLN(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImLnFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMLN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (a, b, suffix) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // ln(0) is undefined
        let modulus = (a * a + b * b).sqrt();
        if modulus < 1e-15 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // ln(z) = ln(|z|) + i*arg(z)
        let real = modulus.ln();
        let imag = b.atan2(a);

        let result = format_complex(real, imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the base-10 logarithm of a complex number.
///
/// Produces the principal complex logarithm in base 10.
///
/// # Remarks
/// - Input is coerced to complex-number text before parsing.
/// - Returns `#NUM!` for zero input.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Base-10 log of a real value"
/// formula: "=IMLOG10(\"10\")"
/// expected: "1"
/// ```
///
/// ```yaml,sandbox
/// title: "Base-10 log on imaginary axis"
/// formula: "=IMLOG10(\"i\")"
/// expected: "0.6821881769209206i"
/// ```
/// ```yaml,docs
/// related:
///   - IMLN
///   - IMLOG2
///   - IMEXP
/// faq:
///   - q: "What branch of the logarithm does `IMLOG10` use?"
///     a: "It returns the principal complex logarithm (base 10), derived from principal argument `atan2(imag, real)`."
/// ```
#[derive(Debug)]
pub struct ImLog10Fn;
/// [formualizer-docgen:schema:start]
/// Name: IMLOG10
/// Type: ImLog10Fn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMLOG10(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImLog10Fn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMLOG10"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (a, b, suffix) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // log10(0) is undefined
        let modulus = (a * a + b * b).sqrt();
        if modulus < 1e-15 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // log10(z) = ln(z) / ln(10) = (ln(|z|) + i*arg(z)) / ln(10)
        let ln10 = 10.0_f64.ln();
        let real = modulus.ln() / ln10;
        let imag = b.atan2(a) / ln10;

        let result = format_complex(real, imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the base-2 logarithm of a complex number.
///
/// Produces the principal complex logarithm in base 2.
///
/// # Remarks
/// - Input is coerced to complex-number text before parsing.
/// - Returns `#NUM!` for zero input.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Base-2 log of a real value"
/// formula: "=IMLOG2(\"8\")"
/// expected: "3"
/// ```
///
/// ```yaml,sandbox
/// title: "Base-2 log on imaginary axis"
/// formula: "=IMLOG2(\"i\")"
/// expected: "2.266180070913597i"
/// ```
/// ```yaml,docs
/// related:
///   - IMLN
///   - IMLOG10
///   - IMEXP
/// faq:
///   - q: "When does `IMLOG2` return `#NUM!`?"
///     a: "It returns `#NUM!` for invalid complex text or zero input, where logarithm is undefined."
/// ```
#[derive(Debug)]
pub struct ImLog2Fn;
/// [formualizer-docgen:schema:start]
/// Name: IMLOG2
/// Type: ImLog2Fn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMLOG2(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImLog2Fn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMLOG2"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (a, b, suffix) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // log2(0) is undefined
        let modulus = (a * a + b * b).sqrt();
        if modulus < 1e-15 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // log2(z) = ln(z) / ln(2) = (ln(|z|) + i*arg(z)) / ln(2)
        let ln2 = 2.0_f64.ln();
        let real = modulus.ln() / ln2;
        let imag = b.atan2(a) / ln2;

        let result = format_complex(real, imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Raises a complex number to a real power.
///
/// Uses polar form and returns the principal-value result as complex text.
///
/// # Remarks
/// - `inumber` is coerced to complex-number text; `n` is numerically coerced.
/// - Returns `#NUM!` for undefined zero-power cases such as `0^0` or `0^-1`.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Square a complex value"
/// formula: "=IMPOWER(\"1+i\",2)"
/// expected: "2i"
/// ```
///
/// ```yaml,sandbox
/// title: "Negative real exponent"
/// formula: "=IMPOWER(\"2\",-1)"
/// expected: "0.5"
/// ```
/// ```yaml,docs
/// related:
///   - IMSQRT
///   - IMEXP
///   - IMLN
/// faq:
///   - q: "How does `IMPOWER` handle zero base with non-positive exponent?"
///     a: "`0^0` and `0` raised to a negative exponent are treated as undefined and return `#NUM!`."
/// ```
#[derive(Debug)]
pub struct ImPowerFn;
/// [formualizer-docgen:schema:start]
/// Name: IMPOWER
/// Type: ImPowerFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: IMPOWER(arg1: any@scalar, arg2: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImPowerFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMPOWER"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let n = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };

        let (a, b, suffix) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let modulus = (a * a + b * b).sqrt();
        let theta = b.atan2(a);

        // Handle 0^n cases
        if modulus < 1e-15 {
            if n > 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
                    "0".to_string(),
                )));
            } else {
                // 0^0 or 0^negative is undefined
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        }

        // z^n = |z|^n * (cos(n*theta) + i*sin(n*theta))
        let r_n = modulus.powf(n);
        let n_theta = n * theta;
        let real = r_n * n_theta.cos();
        let imag = r_n * n_theta.sin();

        let result = format_complex(real, imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the principal square root of a complex number.
///
/// Computes the root in polar form and returns complex text.
///
/// # Remarks
/// - Input is coerced to complex-number text before parsing.
/// - Returns the principal branch of the square root.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Square root of a negative real"
/// formula: "=IMSQRT(\"-4\")"
/// expected: "2i"
/// ```
///
/// ```yaml,sandbox
/// title: "Square root of a+bi"
/// formula: "=IMSQRT(\"3+4i\")"
/// expected: "2+i"
/// ```
/// ```yaml,docs
/// related:
///   - IMPOWER
///   - IMABS
///   - IMARGUMENT
/// faq:
///   - q: "Which square root does `IMSQRT` return for complex inputs?"
///     a: "It returns the principal branch (half-angle polar form), matching spreadsheet-style principal-value behavior."
/// ```
#[derive(Debug)]
pub struct ImSqrtFn;
/// [formualizer-docgen:schema:start]
/// Name: IMSQRT
/// Type: ImSqrtFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMSQRT(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImSqrtFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMSQRT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (a, b, suffix) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let modulus = (a * a + b * b).sqrt();
        let theta = b.atan2(a);

        // sqrt(z) = sqrt(|z|) * (cos(theta/2) + i*sin(theta/2))
        let sqrt_r = modulus.sqrt();
        let half_theta = theta / 2.0;
        let real = sqrt_r * half_theta.cos();
        let imag = sqrt_r * half_theta.sin();

        let result = format_complex(real, imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the sine of a complex number.
///
/// Evaluates complex sine and returns the result as complex text.
///
/// # Remarks
/// - Input is coerced to complex-number text before parsing.
/// - Uses hyperbolic components for the imaginary part.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Sine of zero"
/// formula: "=IMSIN(\"0\")"
/// expected: "0"
/// ```
///
/// ```yaml,sandbox
/// title: "Sine on imaginary axis"
/// formula: "=IMSIN(\"i\")"
/// expected: "1.1752011936438014i"
/// ```
/// ```yaml,docs
/// related:
///   - IMCOS
///   - IMEXP
/// faq:
///   - q: "Why can `IMSIN` return non-zero imaginary output for real-looking formulas?"
///     a: "For complex inputs `a+bi`, sine uses hyperbolic terms (`cosh`, `sinh`), so imaginary components are expected."
/// ```
#[derive(Debug)]
pub struct ImSinFn;
/// [formualizer-docgen:schema:start]
/// Name: IMSIN
/// Type: ImSinFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMSIN(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImSinFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMSIN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (a, b, suffix) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // sin(a+bi) = sin(a)*cosh(b) + i*cos(a)*sinh(b)
        let real = a.sin() * b.cosh();
        let imag = a.cos() * b.sinh();

        let result = format_complex(real, imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/// Returns the cosine of a complex number.
///
/// Evaluates complex cosine and returns the result as complex text.
///
/// # Remarks
/// - Input is coerced to complex-number text before parsing.
/// - Uses hyperbolic components for the imaginary part.
/// - Invalid complex text returns `#NUM!`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Cosine of zero"
/// formula: "=IMCOS(\"0\")"
/// expected: "1"
/// ```
///
/// ```yaml,sandbox
/// title: "Cosine on imaginary axis"
/// formula: "=IMCOS(\"i\")"
/// expected: "1.5430806348152437"
/// ```
/// ```yaml,docs
/// related:
///   - IMSIN
///   - IMEXP
/// faq:
///   - q: "Why is the imaginary part negated in `IMCOS`?"
///     a: "Complex cosine uses `cos(a+bi)=cos(a)cosh(b)-i sin(a)sinh(b)`, so the imaginary term carries a minus sign."
/// ```
#[derive(Debug)]
pub struct ImCosFn;
/// [formualizer-docgen:schema:start]
/// Name: IMCOS
/// Type: ImCosFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: IMCOS(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ImCosFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IMCOS"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let inumber = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => match coerce_complex_str(&other) {
                Ok(s) => s,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            },
        };

        let (a, b, suffix) = match parse_complex(&inumber) {
            Ok(c) => c,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // cos(a+bi) = cos(a)*cosh(b) - i*sin(a)*sinh(b)
        let real = a.cos() * b.cosh();
        let imag = -a.sin() * b.sinh();

        let result = format_complex(real, imag, suffix);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

/* ─────────────────────────── Unit Conversion (CONVERT) ──────────────────────────── */

/// Unit categories for CONVERT function
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum UnitCategory {
    Length,
    Mass,
    Temperature,
}

/// Information about a unit
struct UnitInfo {
    category: UnitCategory,
    /// Conversion factor to base unit (meters for length, grams for mass)
    /// For temperature, this is special-cased
    to_base: f64,
}

/// Get unit info for a given unit string
fn get_unit_info(unit: &str) -> Option<UnitInfo> {
    // Length units (base: meter)
    match unit {
        // Metric length
        "m" => Some(UnitInfo {
            category: UnitCategory::Length,
            to_base: 1.0,
        }),
        "km" => Some(UnitInfo {
            category: UnitCategory::Length,
            to_base: 1000.0,
        }),
        "cm" => Some(UnitInfo {
            category: UnitCategory::Length,
            to_base: 0.01,
        }),
        "mm" => Some(UnitInfo {
            category: UnitCategory::Length,
            to_base: 0.001,
        }),
        // Imperial length
        "mi" => Some(UnitInfo {
            category: UnitCategory::Length,
            to_base: 1609.344,
        }),
        "ft" => Some(UnitInfo {
            category: UnitCategory::Length,
            to_base: 0.3048,
        }),
        "in" => Some(UnitInfo {
            category: UnitCategory::Length,
            to_base: 0.0254,
        }),
        "yd" => Some(UnitInfo {
            category: UnitCategory::Length,
            to_base: 0.9144,
        }),
        "Nmi" => Some(UnitInfo {
            category: UnitCategory::Length,
            to_base: 1852.0,
        }),

        // Mass units (base: gram)
        "g" => Some(UnitInfo {
            category: UnitCategory::Mass,
            to_base: 1.0,
        }),
        "kg" => Some(UnitInfo {
            category: UnitCategory::Mass,
            to_base: 1000.0,
        }),
        "mg" => Some(UnitInfo {
            category: UnitCategory::Mass,
            to_base: 0.001,
        }),
        "lbm" => Some(UnitInfo {
            category: UnitCategory::Mass,
            to_base: 453.59237,
        }),
        "oz" => Some(UnitInfo {
            category: UnitCategory::Mass,
            to_base: 28.349523125,
        }),
        "ozm" => Some(UnitInfo {
            category: UnitCategory::Mass,
            to_base: 28.349523125,
        }),
        "ton" => Some(UnitInfo {
            category: UnitCategory::Mass,
            to_base: 907184.74,
        }),

        // Temperature units (special handling)
        "C" | "cel" => Some(UnitInfo {
            category: UnitCategory::Temperature,
            to_base: 0.0, // Special-cased
        }),
        "F" | "fah" => Some(UnitInfo {
            category: UnitCategory::Temperature,
            to_base: 0.0, // Special-cased
        }),
        "K" | "kel" => Some(UnitInfo {
            category: UnitCategory::Temperature,
            to_base: 0.0, // Special-cased
        }),

        _ => None,
    }
}

/// Normalize temperature unit name
fn normalize_temp_unit(unit: &str) -> &str {
    match unit {
        "C" | "cel" => "C",
        "F" | "fah" => "F",
        "K" | "kel" => "K",
        _ => unit,
    }
}

/// Convert temperature between units
fn convert_temperature(value: f64, from: &str, to: &str) -> f64 {
    let from = normalize_temp_unit(from);
    let to = normalize_temp_unit(to);

    if from == to {
        return value;
    }

    // First convert to Celsius
    let celsius = match from {
        "C" => value,
        "F" => (value - 32.0) * 5.0 / 9.0,
        "K" => value - 273.15,
        _ => value,
    };

    // Then convert from Celsius to target
    match to {
        "C" => celsius,
        "F" => celsius * 9.0 / 5.0 + 32.0,
        "K" => celsius + 273.15,
        _ => celsius,
    }
}

/// Convert a value between units
fn convert_units(value: f64, from: &str, to: &str) -> Result<f64, ExcelError> {
    let from_info = get_unit_info(from).ok_or_else(ExcelError::new_na)?;
    let to_info = get_unit_info(to).ok_or_else(ExcelError::new_na)?;

    // Check category compatibility
    if from_info.category != to_info.category {
        return Err(ExcelError::new_na());
    }

    // Handle temperature specially
    if from_info.category == UnitCategory::Temperature {
        return Ok(convert_temperature(value, from, to));
    }

    // For other units: convert to base, then to target
    let base_value = value * from_info.to_base;
    Ok(base_value / to_info.to_base)
}

/// Converts a numeric value from one supported unit to another.
///
/// Supports a focused set of length, mass, and temperature units.
///
/// # Remarks
/// - `number` is numerically coerced; unit arguments must be text.
/// - Returns `#N/A` for unknown units or incompatible unit categories.
/// - Temperature conversions support `C/cel`, `F/fah`, and `K/kel`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Length conversion"
/// formula: "=CONVERT(1,\"km\",\"m\")"
/// expected: 1000
/// ```
///
/// ```yaml,sandbox
/// title: "Temperature conversion"
/// formula: "=CONVERT(32,\"F\",\"C\")"
/// expected: 0
/// ```
/// ```yaml,docs
/// related:
///   - DEC2BIN
///   - DEC2HEX
///   - DEC2OCT
/// faq:
///   - q: "When does `CONVERT` return `#N/A`?"
///     a: "Unknown unit tokens, non-text unit arguments, or mixing incompatible categories (for example length to mass) return `#N/A`."
/// ```
#[derive(Debug)]
pub struct ConvertFn;
/// [formualizer-docgen:schema:start]
/// Name: CONVERT
/// Type: ConvertFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: CONVERT(arg1: any@scalar, arg2: any@scalar, arg3: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ConvertFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CONVERT"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_COMPLEX_THREE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // Get the number value
        let value = match args[0].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };

        // Get from_unit
        let from_unit = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            LiteralValue::Text(s) => s,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_na(),
                )));
            }
        };

        // Get to_unit
        let to_unit = match args[2].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            LiteralValue::Text(s) => s,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_na(),
                )));
            }
        };

        match convert_units(value, &from_unit, &to_unit) {
            Ok(result) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                result,
            ))),
            Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        }
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(BitAndFn));
    crate::function_registry::register_function(Arc::new(BitOrFn));
    crate::function_registry::register_function(Arc::new(BitXorFn));
    crate::function_registry::register_function(Arc::new(BitLShiftFn));
    crate::function_registry::register_function(Arc::new(BitRShiftFn));
    crate::function_registry::register_function(Arc::new(Bin2DecFn));
    crate::function_registry::register_function(Arc::new(Dec2BinFn));
    crate::function_registry::register_function(Arc::new(Hex2DecFn));
    crate::function_registry::register_function(Arc::new(Dec2HexFn));
    crate::function_registry::register_function(Arc::new(Oct2DecFn));
    crate::function_registry::register_function(Arc::new(Dec2OctFn));
    crate::function_registry::register_function(Arc::new(Bin2HexFn));
    crate::function_registry::register_function(Arc::new(Hex2BinFn));
    crate::function_registry::register_function(Arc::new(Bin2OctFn));
    crate::function_registry::register_function(Arc::new(Oct2BinFn));
    crate::function_registry::register_function(Arc::new(Hex2OctFn));
    crate::function_registry::register_function(Arc::new(Oct2HexFn));
    crate::function_registry::register_function(Arc::new(DeltaFn));
    crate::function_registry::register_function(Arc::new(GestepFn));
    crate::function_registry::register_function(Arc::new(ErfFn));
    crate::function_registry::register_function(Arc::new(ErfcFn));
    crate::function_registry::register_function(Arc::new(ErfPreciseFn));
    // Complex number functions
    crate::function_registry::register_function(Arc::new(ComplexFn));
    crate::function_registry::register_function(Arc::new(ImRealFn));
    crate::function_registry::register_function(Arc::new(ImaginaryFn));
    crate::function_registry::register_function(Arc::new(ImAbsFn));
    crate::function_registry::register_function(Arc::new(ImArgumentFn));
    crate::function_registry::register_function(Arc::new(ImConjugateFn));
    crate::function_registry::register_function(Arc::new(ImSumFn));
    crate::function_registry::register_function(Arc::new(ImSubFn));
    crate::function_registry::register_function(Arc::new(ImProductFn));
    crate::function_registry::register_function(Arc::new(ImDivFn));
    // Complex number math functions
    crate::function_registry::register_function(Arc::new(ImExpFn));
    crate::function_registry::register_function(Arc::new(ImLnFn));
    crate::function_registry::register_function(Arc::new(ImLog10Fn));
    crate::function_registry::register_function(Arc::new(ImLog2Fn));
    crate::function_registry::register_function(Arc::new(ImPowerFn));
    crate::function_registry::register_function(Arc::new(ImSqrtFn));
    crate::function_registry::register_function(Arc::new(ImSinFn));
    crate::function_registry::register_function(Arc::new(ImCosFn));
    // Unit conversion
    crate::function_registry::register_function(Arc::new(ConvertFn));
}
