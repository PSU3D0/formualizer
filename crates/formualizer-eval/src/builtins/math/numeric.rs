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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let v = args[0].value()?;
        match v.as_ref() {
            LiteralValue::Error(e) => Ok(LiteralValue::Error(e.clone())),
            other => Ok(LiteralValue::Number(coerce_num(other)?.abs())),
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let v = args[0].value()?;
        match v.as_ref() {
            LiteralValue::Error(e) => Ok(LiteralValue::Error(e.clone())),
            other => {
                let n = coerce_num(other)?;
                Ok(LiteralValue::Number(if n > 0.0 {
                    1.0
                } else if n < 0.0 {
                    -1.0
                } else {
                    0.0
                }))
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let v = args[0].value()?;
        match v.as_ref() {
            LiteralValue::Error(e) => Ok(LiteralValue::Error(e.clone())),
            other => Ok(LiteralValue::Number(coerce_num(other)?.floor())),
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.is_empty() || args.len() > 2 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let mut n = match args[0].value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            other => coerce_num(other)?,
        };
        let digits: i32 = if args.len() == 2 {
            match args[1].value()?.as_ref() {
                LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
                other => coerce_num(other)? as i32,
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
        Ok(LiteralValue::Number(n))
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let n = match args[0].value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            other => coerce_num(other)?,
        };
        let digits = match args[1].value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            other => coerce_num(other)? as i32,
        };
        let f = 10f64.powi(digits.abs());
        let out = if digits >= 0 {
            (n * f).round() / f
        } else {
            (n / f).round() * f
        };
        Ok(LiteralValue::Number(out))
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let n = match args[0].value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            other => coerce_num(other)?,
        };
        let digits = match args[1].value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            other => coerce_num(other)? as i32,
        };
        let f = 10f64.powi(digits.abs());
        let out = if digits >= 0 {
            (n * f).trunc() / f
        } else {
            (n / f).trunc() * f
        };
        Ok(LiteralValue::Number(out))
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let n = match args[0].value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            other => coerce_num(other)?,
        };
        let digits = match args[1].value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            other => coerce_num(other)? as i32,
        };
        let f = 10f64.powi(digits.abs());
        let mut scaled = if digits >= 0 { n * f } else { n / f };
        if scaled > 0.0 {
            scaled = scaled.ceil();
        } else {
            scaled = scaled.floor();
        }
        let out = if digits >= 0 { scaled / f } else { scaled * f };
        Ok(LiteralValue::Number(out))
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = match args[0].value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            other => coerce_num(other)?,
        };
        let y = match args[1].value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            other => coerce_num(other)?,
        };
        if y == 0.0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
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
        Ok(LiteralValue::Number(r))
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
}

#[cfg(test)]
mod tests_numeric {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_core::parser::{ASTNode, ASTNodeType};

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
            .unwrap(),
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
            .unwrap(),
            LiteralValue::Number(-1.0)
        );
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&zero, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
            LiteralValue::Number(0.0)
        );
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&pos, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
            .unwrap(),
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
        {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            _ => panic!(),
        }
    }
}
