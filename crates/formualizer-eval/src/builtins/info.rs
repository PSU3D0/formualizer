use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

use super::utils::ARG_ANY_ONE;

/*
Sprint 9 – Info / Error Introspection Functions

Implemented:
  ISNUMBER, ISTEXT, ISLOGICAL, ISBLANK, ISERROR, ISERR, ISNA, ISFORMULA, TYPE,
  NA, N, T

Excel semantic notes (baseline):
  - ISNUMBER returns TRUE for numeric types (Int, Number) and also Date/DateTime/Time/Duration
    because Excel stores these as serial numbers. (If this diverges from desired behavior,
    adjust by removing temporal variants.)
  - ISBLANK is TRUE only for truly empty cells (LiteralValue::Empty), NOT for empty string "".
  - ISERROR matches all error kinds; ISERR excludes #N/A.
  - TYPE codes (Excel): 1 Number, 2 Text, 4 Logical, 16 Error, 64 Array. Blank coerces to 1.
    Date/DateTime/Time/Duration mapped to 1 (numeric) for now.
  - NA() returns the canonical #N/A error.
  - N(value) coercion (Excel): number -> itself; date/time -> serial; TRUE->1, FALSE->0; text->0;
    error -> propagates error; empty -> 0; other (array) -> first element via implicit (TODO) currently returns 0 with TODO flag.
  - T(value): if text -> text; if error -> error; else -> empty text "".
  - ISFORMULA requires formula provenance metadata (not yet tracked). Returns FALSE always (unless
    we detect a formula node later). Marked TODO.

TODO(excel-nuance): Implement implicit intersection for N() over arrays if/when model finalised.
TODO(excel-nuance): Track formula provenance to support ISFORMULA.
*/

#[derive(Debug)]
pub struct IsNumberFn;
impl Function for IsNumberFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISNUMBER"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?;
        let is_num = matches!(
            v.as_ref(),
            LiteralValue::Int(_)
                | LiteralValue::Number(_)
                | LiteralValue::Date(_)
                | LiteralValue::DateTime(_)
                | LiteralValue::Time(_)
                | LiteralValue::Duration(_)
        );
        Ok(LiteralValue::Boolean(is_num))
    }
}

#[derive(Debug)]
pub struct IsTextFn;
impl Function for IsTextFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISTEXT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?;
        Ok(LiteralValue::Boolean(matches!(
            v.as_ref(),
            LiteralValue::Text(_)
        )))
    }
}

#[derive(Debug)]
pub struct IsLogicalFn;
impl Function for IsLogicalFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISLOGICAL"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?;
        Ok(LiteralValue::Boolean(matches!(
            v.as_ref(),
            LiteralValue::Boolean(_)
        )))
    }
}

#[derive(Debug)]
pub struct IsBlankFn;
impl Function for IsBlankFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISBLANK"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?;
        Ok(LiteralValue::Boolean(matches!(
            v.as_ref(),
            LiteralValue::Empty
        )))
    }
}

#[derive(Debug)]
pub struct IsErrorFn; // TRUE for any error (#N/A included)
impl Function for IsErrorFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISERROR"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?;
        Ok(LiteralValue::Boolean(matches!(
            v.as_ref(),
            LiteralValue::Error(_)
        )))
    }
}

#[derive(Debug)]
pub struct IsErrFn; // TRUE for any error except #N/A
impl Function for IsErrFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISERR"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?;
        let is_err = match v.as_ref() {
            LiteralValue::Error(e) => e.kind != ExcelErrorKind::Na,
            _ => false,
        };
        Ok(LiteralValue::Boolean(is_err))
    }
}

#[derive(Debug)]
pub struct IsNaFn; // TRUE only for #N/A
impl Function for IsNaFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISNA"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?;
        let is_na = matches!(v.as_ref(), LiteralValue::Error(e) if e.kind==ExcelErrorKind::Na);
        Ok(LiteralValue::Boolean(is_na))
    }
}

#[derive(Debug)]
pub struct IsFormulaFn; // Requires provenance tracking (not yet) => always FALSE.
impl Function for IsFormulaFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISFORMULA"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        // TODO(excel-nuance): formula provenance once AST metadata is plumbed.
        Ok(LiteralValue::Boolean(false))
    }
}

#[derive(Debug)]
pub struct TypeFn;
impl Function for TypeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TYPE"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?; // Propagate errors directly
        if let LiteralValue::Error(e) = v.as_ref() {
            return Ok(LiteralValue::Error(e.clone()));
        }
        let code = match v.as_ref() {
            LiteralValue::Int(_)
            | LiteralValue::Number(_)
            | LiteralValue::Empty
            | LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_)
            | LiteralValue::Duration(_) => 1,
            LiteralValue::Text(_) => 2,
            LiteralValue::Boolean(_) => 4,
            LiteralValue::Array(_) => 64,
            LiteralValue::Error(_) => unreachable!(),
            LiteralValue::Pending => 1, // treat as blank/zero numeric; may change
        };
        Ok(LiteralValue::Int(code))
    }
}

#[derive(Debug)]
pub struct NaFn; // NA() -> #N/A error
impl Function for NaFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NA"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn eval_scalar<'a, 'b>(
        &self,
        _args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)))
    }
}

#[derive(Debug)]
pub struct NFn; // N(value)
impl Function for NFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "N"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?;
        match v.as_ref() {
            LiteralValue::Int(i) => Ok(LiteralValue::Int(*i)),
            LiteralValue::Number(n) => Ok(LiteralValue::Number(*n)),
            LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_)
            | LiteralValue::Duration(_) => {
                // Convert via serial number helper
                if let Some(serial) = v.as_ref().as_serial_number() {
                    Ok(LiteralValue::Number(serial))
                } else {
                    Ok(LiteralValue::Int(0))
                }
            }
            LiteralValue::Boolean(b) => Ok(LiteralValue::Int(if *b { 1 } else { 0 })),
            LiteralValue::Text(_) => Ok(LiteralValue::Int(0)),
            LiteralValue::Empty => Ok(LiteralValue::Int(0)),
            LiteralValue::Array(_) => {
                // TODO(excel-nuance): implicit intersection; for now return 0
                Ok(LiteralValue::Int(0))
            }
            LiteralValue::Error(e) => Ok(LiteralValue::Error(e.clone())),
            LiteralValue::Pending => Ok(LiteralValue::Int(0)),
        }
    }
}

#[derive(Debug)]
pub struct TFn; // T(value)
impl Function for TFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "T"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 1 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let v = args[0].value()?;
        match v.as_ref() {
            LiteralValue::Text(s) => Ok(LiteralValue::Text(s.clone())),
            LiteralValue::Error(e) => Ok(LiteralValue::Error(e.clone())),
            _ => Ok(LiteralValue::Text(String::new())),
        }
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(IsNumberFn));
    crate::function_registry::register_function(Arc::new(IsTextFn));
    crate::function_registry::register_function(Arc::new(IsLogicalFn));
    crate::function_registry::register_function(Arc::new(IsBlankFn));
    crate::function_registry::register_function(Arc::new(IsErrorFn));
    crate::function_registry::register_function(Arc::new(IsErrFn));
    crate::function_registry::register_function(Arc::new(IsNaFn));
    crate::function_registry::register_function(Arc::new(IsFormulaFn));
    crate::function_registry::register_function(Arc::new(TypeFn));
    crate::function_registry::register_function(Arc::new(NaFn));
    crate::function_registry::register_function(Arc::new(NFn));
    crate::function_registry::register_function(Arc::new(TFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_core::parser::{ASTNode, ASTNodeType};
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn isnumber_numeric_and_date() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IsNumberFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ISNUMBER").unwrap();
        let num = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(3.14)), None);
        let date = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Date(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            )),
            None,
        );
        let txt = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("x".into())), None);
        let args_num = vec![crate::traits::ArgumentHandle::new(&num, &ctx)];
        let args_date = vec![crate::traits::ArgumentHandle::new(&date, &ctx)];
        let args_txt = vec![crate::traits::ArgumentHandle::new(&txt, &ctx)];
        assert_eq!(
            f.dispatch(&args_num, &ctx.function_context(None)).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&args_date, &ctx.function_context(None)).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&args_txt, &ctx.function_context(None)).unwrap(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn istest_and_isblank() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IsTextFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ISTEXT").unwrap();
        let t = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("abc".into())), None);
        let n = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None);
        let args_t = vec![crate::traits::ArgumentHandle::new(&t, &ctx)];
        let args_n = vec![crate::traits::ArgumentHandle::new(&n, &ctx)];
        assert_eq!(
            f.dispatch(&args_t, &ctx.function_context(None)).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&args_n, &ctx.function_context(None)).unwrap(),
            LiteralValue::Boolean(false)
        );

        // ISBLANK
        let wb2 = TestWorkbook::new().with_function(std::sync::Arc::new(IsBlankFn));
        let ctx2 = interp(&wb2);
        let f2 = ctx2.context.get_function("", "ISBLANK").unwrap();
        let blank = ASTNode::new(ASTNodeType::Literal(LiteralValue::Empty), None);
        let blank_args = vec![crate::traits::ArgumentHandle::new(&blank, &ctx2)];
        assert_eq!(
            f2.dispatch(&blank_args, &ctx2.function_context(None))
                .unwrap(),
            LiteralValue::Boolean(true)
        );
    }

    #[test]
    fn iserror_variants() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IsErrorFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ISERROR").unwrap();
        let err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div))),
            None,
        );
        let ok = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let a_err = vec![crate::traits::ArgumentHandle::new(&err, &ctx)];
        let a_ok = vec![crate::traits::ArgumentHandle::new(&ok, &ctx)];
        assert_eq!(
            f.dispatch(&a_err, &ctx.function_context(None)).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&a_ok, &ctx.function_context(None)).unwrap(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn type_codes_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TypeFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "TYPE").unwrap();
        let v_num = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(2.0)), None);
        let v_txt = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("hi".into())), None);
        let v_bool = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(true)), None);
        let v_err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))),
            None,
        );
        let v_arr = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![LiteralValue::Int(1)]])),
            None,
        );
        let a_num = vec![crate::traits::ArgumentHandle::new(&v_num, &ctx)];
        let a_txt = vec![crate::traits::ArgumentHandle::new(&v_txt, &ctx)];
        let a_bool = vec![crate::traits::ArgumentHandle::new(&v_bool, &ctx)];
        let a_err = vec![crate::traits::ArgumentHandle::new(&v_err, &ctx)];
        let a_arr = vec![crate::traits::ArgumentHandle::new(&v_arr, &ctx)];
        assert_eq!(
            f.dispatch(&a_num, &ctx.function_context(None)).unwrap(),
            LiteralValue::Int(1)
        );
        assert_eq!(
            f.dispatch(&a_txt, &ctx.function_context(None)).unwrap(),
            LiteralValue::Int(2)
        );
        assert_eq!(
            f.dispatch(&a_bool, &ctx.function_context(None)).unwrap(),
            LiteralValue::Int(4)
        );
        match f.dispatch(&a_err, &ctx.function_context(None)).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#VALUE!"),
            _ => panic!(),
        }
        assert_eq!(
            f.dispatch(&a_arr, &ctx.function_context(None)).unwrap(),
            LiteralValue::Int(64)
        );
    }

    #[test]
    fn na_and_n_and_t() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(NaFn))
            .with_function(std::sync::Arc::new(NFn))
            .with_function(std::sync::Arc::new(TFn));
        let ctx = wb.interpreter();
        // NA()
        let na_fn = ctx.context.get_function("", "NA").unwrap();
        match na_fn.eval_scalar(&[], &ctx.function_context(None)).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#N/A"),
            _ => panic!(),
        }
        // N()
        let n_fn = ctx.context.get_function("", "N").unwrap();
        let val = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(true)), None);
        let args = vec![crate::traits::ArgumentHandle::new(&val, &ctx)];
        assert_eq!(
            n_fn.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Int(1)
        );
        // T()
        let t_fn = ctx.context.get_function("", "T").unwrap();
        let txt = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("abc".into())), None);
        let args_t = vec![crate::traits::ArgumentHandle::new(&txt, &ctx)];
        assert_eq!(
            t_fn.dispatch(&args_t, &ctx.function_context(None)).unwrap(),
            LiteralValue::Text("abc".into())
        );
    }
}
