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

/// BITAND(number1, number2) - Returns bitwise AND of two numbers
#[derive(Debug)]
pub struct BitAndFn;
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

/// BITOR(number1, number2) - Returns bitwise OR of two numbers
#[derive(Debug)]
pub struct BitOrFn;
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

/// BITXOR(number1, number2) - Returns bitwise XOR of two numbers
#[derive(Debug)]
pub struct BitXorFn;
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

/// BITLSHIFT(number, shift_amount) - Returns number shifted left by shift_amount bits
#[derive(Debug)]
pub struct BitLShiftFn;
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

/// BITRSHIFT(number, shift_amount) - Returns number shifted right by shift_amount bits
#[derive(Debug)]
pub struct BitRShiftFn;
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

/// BIN2DEC(number) - Converts binary number to decimal
#[derive(Debug)]
pub struct Bin2DecFn;
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

/// DEC2BIN(number, [places]) - Converts decimal to binary
#[derive(Debug)]
pub struct Dec2BinFn;
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
        if n < -512 || n > 511 {
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

/// HEX2DEC(number) - Converts hexadecimal to decimal
#[derive(Debug)]
pub struct Hex2DecFn;
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

/// DEC2HEX(number, [places]) - Converts decimal to hexadecimal
#[derive(Debug)]
pub struct Dec2HexFn;
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
        if n < -(1i64 << 39) || n > (1i64 << 39) - 1 {
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

/// OCT2DEC(number) - Converts octal to decimal
#[derive(Debug)]
pub struct Oct2DecFn;
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
        if text.len() > 10 || !text.chars().all(|c| c >= '0' && c <= '7') {
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

/// DEC2OCT(number, [places]) - Converts decimal to octal
#[derive(Debug)]
pub struct Dec2OctFn;
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
        if n < -(1i64 << 29) || n > (1i64 << 29) - 1 {
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

/// BIN2HEX(number, [places]) - Converts binary to hexadecimal
#[derive(Debug)]
pub struct Bin2HexFn;
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

/// HEX2BIN(number, [places]) - Converts hexadecimal to binary
#[derive(Debug)]
pub struct Hex2BinFn;
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
        if dec < -512 || dec > 511 {
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

/// BIN2OCT(number, [places]) - Converts binary to octal
#[derive(Debug)]
pub struct Bin2OctFn;
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

/// OCT2BIN(number, [places]) - Converts octal to binary
#[derive(Debug)]
pub struct Oct2BinFn;
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

        if text.len() > 10 || !text.chars().all(|c| c >= '0' && c <= '7') {
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
        if dec < -512 || dec > 511 {
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

/// HEX2OCT(number, [places]) - Converts hexadecimal to octal
#[derive(Debug)]
pub struct Hex2OctFn;
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
        if dec < -(1i64 << 29) || dec > (1i64 << 29) - 1 {
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

/// OCT2HEX(number, [places]) - Converts octal to hexadecimal
#[derive(Debug)]
pub struct Oct2HexFn;
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

        if text.len() > 10 || !text.chars().all(|c| c >= '0' && c <= '7') {
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

/// DELTA(number1, [number2]) - Tests whether two values are equal
#[derive(Debug)]
pub struct DeltaFn;
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

/// GESTEP(number, [step]) - Tests whether a number is >= step value
#[derive(Debug)]
pub struct GestepFn;
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

/// ERF(lower_limit, [upper_limit]) - Returns the error function integrated between lower_limit and upper_limit
/// If only lower_limit is provided, returns erf(lower_limit)
/// If both are provided, returns erf(upper_limit) - erf(lower_limit)
#[derive(Debug)]
pub struct ErfFn;
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

/// ERFC(x) - Returns the complementary error function = 1 - erf(x)
#[derive(Debug)]
pub struct ErfcFn;
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

/// ERF.PRECISE(x) - Returns the error function (same as ERF with one argument)
#[derive(Debug)]
pub struct ErfPreciseFn;
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

/// COMPLEX(real_num, i_num, [suffix]) - Converts real and imaginary coefficients into a complex number
#[derive(Debug)]
pub struct ComplexFn;
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

/// IMREAL(inumber) - Returns the real coefficient of a complex number
#[derive(Debug)]
pub struct ImRealFn;
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

/// IMAGINARY(inumber) - Returns the imaginary coefficient of a complex number
#[derive(Debug)]
pub struct ImaginaryFn;
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

/// IMABS(inumber) - Returns the absolute value (modulus) of a complex number
#[derive(Debug)]
pub struct ImAbsFn;
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

/// IMARGUMENT(inumber) - Returns the argument theta (angle in radians) of a complex number
#[derive(Debug)]
pub struct ImArgumentFn;
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

/// IMCONJUGATE(inumber) - Returns the complex conjugate of a complex number
#[derive(Debug)]
pub struct ImConjugateFn;
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

/// IMSUM(inumber1, [inumber2], ...) - Returns the sum of complex numbers
#[derive(Debug)]
pub struct ImSumFn;
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

/// IMSUB(inumber1, inumber2) - Returns the difference of two complex numbers
#[derive(Debug)]
pub struct ImSubFn;
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

/// IMPRODUCT(inumber1, [inumber2], ...) - Returns the product of complex numbers
#[derive(Debug)]
pub struct ImProductFn;
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

/// IMDIV(inumber1, inumber2) - Returns the quotient of two complex numbers
#[derive(Debug)]
pub struct ImDivFn;
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

/// IMEXP(inumber) - Returns exponential of a complex number
/// e^(a+bi) = e^a * (cos(b) + i*sin(b))
#[derive(Debug)]
pub struct ImExpFn;
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

/// IMLN(inumber) - Returns the natural logarithm of a complex number
/// ln(a+bi) = ln(|z|) + i*arg(z)
#[derive(Debug)]
pub struct ImLnFn;
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

/// IMLOG10(inumber) - Returns the base-10 logarithm of a complex number
/// log10(z) = ln(z) / ln(10)
#[derive(Debug)]
pub struct ImLog10Fn;
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

/// IMLOG2(inumber) - Returns the base-2 logarithm of a complex number
/// log2(z) = ln(z) / ln(2)
#[derive(Debug)]
pub struct ImLog2Fn;
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

/// IMPOWER(inumber, n) - Returns a complex number raised to a power
/// z^n = |z|^n * (cos(n*theta) + i*sin(n*theta))
#[derive(Debug)]
pub struct ImPowerFn;
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

/// IMSQRT(inumber) - Returns the square root of a complex number
/// sqrt(z) = sqrt(|z|) * (cos(theta/2) + i*sin(theta/2))
#[derive(Debug)]
pub struct ImSqrtFn;
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

/// IMSIN(inumber) - Returns the sine of a complex number
/// sin(a+bi) = sin(a)*cosh(b) + i*cos(a)*sinh(b)
#[derive(Debug)]
pub struct ImSinFn;
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

/// IMCOS(inumber) - Returns the cosine of a complex number
/// cos(a+bi) = cos(a)*cosh(b) - i*sin(a)*sinh(b)
#[derive(Debug)]
pub struct ImCosFn;
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

/// CONVERT(number, from_unit, to_unit) - Converts between measurement units
#[derive(Debug)]
pub struct ConvertFn;
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
