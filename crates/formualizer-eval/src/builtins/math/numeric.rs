use super::super::utils::{ARG_NUM_LENIENT_ONE, ARG_NUM_LENIENT_TWO, coerce_num};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

#[derive(Debug)]
pub struct AbsFn;
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
}
