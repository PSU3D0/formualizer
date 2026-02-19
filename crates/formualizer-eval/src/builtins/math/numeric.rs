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
