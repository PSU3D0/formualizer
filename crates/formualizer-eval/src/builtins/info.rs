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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        let is_num = matches!(
            v,
            LiteralValue::Int(_)
                | LiteralValue::Number(_)
                | LiteralValue::Date(_)
                | LiteralValue::DateTime(_)
                | LiteralValue::Time(_)
                | LiteralValue::Duration(_)
        );
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            is_num,
        )))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            matches!(v, LiteralValue::Text(_)),
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            matches!(v, LiteralValue::Boolean(_)),
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            matches!(v, LiteralValue::Empty),
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            matches!(v, LiteralValue::Error(_)),
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        let is_err = match v {
            LiteralValue::Error(e) => e.kind != ExcelErrorKind::Na,
            _ => false,
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            is_err,
        )))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        let is_na = matches!(v, LiteralValue::Error(e) if e.kind==ExcelErrorKind::Na);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            is_na,
        )))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        // TODO(excel-nuance): formula provenance once AST metadata is plumbed.
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            false,
        )))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal(); // Propagate errors directly
        if let LiteralValue::Error(e) = v {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
        }
        let code = match v {
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(code)))
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
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Na),
        )))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        match v {
            LiteralValue::Int(i) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(i))),
            LiteralValue::Number(n) => {
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(n)))
            }
            LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_)
            | LiteralValue::Duration(_) => {
                // Convert via serial number helper
                if let Some(serial) = v.as_serial_number() {
                    Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                        serial,
                    )))
                } else {
                    Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
                }
            }
            LiteralValue::Boolean(b) => {
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(if b {
                    1
                } else {
                    0
                })))
            }
            LiteralValue::Text(_) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0))),
            LiteralValue::Empty => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0))),
            LiteralValue::Array(_) => {
                // TODO(excel-nuance): implicit intersection; for now return 0
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
            }
            LiteralValue::Error(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            LiteralValue::Pending => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0))),
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        match v {
            LiteralValue::Text(s) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(s))),
            LiteralValue::Error(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            _ => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
                String::new(),
            ))),
        }
    }
}

/// ISEVEN(number) - Returns TRUE if number is even
#[derive(Debug)]
pub struct IsEvenFn;
impl Function for IsEvenFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISEVEN"
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
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        let n = match v {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            LiteralValue::Int(i) => i as f64,
            LiteralValue::Number(n) => n,
            LiteralValue::Boolean(b) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        };
        // Excel truncates to integer first
        let n = n.trunc() as i64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            n % 2 == 0,
        )))
    }
}

/// ISODD(number) - Returns TRUE if number is odd
#[derive(Debug)]
pub struct IsOddFn;
impl Function for IsOddFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISODD"
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
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        let n = match v {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            LiteralValue::Int(i) => i as f64,
            LiteralValue::Number(n) => n,
            LiteralValue::Boolean(b) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        };
        let n = n.trunc() as i64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            n % 2 != 0,
        )))
    }
}

/// ERROR.TYPE(error_val) - Returns a number corresponding to an error type
/// Returns:
///   1 = #NULL!
///   2 = #DIV/0!
///   3 = #VALUE!
///   4 = #REF!
///   5 = #NAME?
///   6 = #NUM!
///   7 = #N/A
///   8 = #GETTING_DATA (not commonly used)
///   #N/A if the value is not an error
///
/// NOTE: Error codes 9-13 are non-standard extensions for internal error types.
#[derive(Debug)]
pub struct ErrorTypeFn;
impl Function for ErrorTypeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ERROR.TYPE"
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
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        match v {
            LiteralValue::Error(e) => {
                let code = match e.kind {
                    ExcelErrorKind::Null => 1,
                    ExcelErrorKind::Div => 2,
                    ExcelErrorKind::Value => 3,
                    ExcelErrorKind::Ref => 4,
                    ExcelErrorKind::Name => 5,
                    ExcelErrorKind::Num => 6,
                    ExcelErrorKind::Na => 7,
                    ExcelErrorKind::Error => 8,
                    // Non-standard extensions (codes 9-13)
                    ExcelErrorKind::NImpl => 9,
                    ExcelErrorKind::Spill => 10,
                    ExcelErrorKind::Calc => 11,
                    ExcelErrorKind::Circ => 12,
                    ExcelErrorKind::Cancelled => 13,
                };
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(code)))
            }
            _ => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            ))),
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
    crate::function_registry::register_function(Arc::new(IsEvenFn));
    crate::function_registry::register_function(Arc::new(IsOddFn));
    crate::function_registry::register_function(Arc::new(ErrorTypeFn));
    crate::function_registry::register_function(Arc::new(TypeFn));
    crate::function_registry::register_function(Arc::new(NaFn));
    crate::function_registry::register_function(Arc::new(NFn));
    crate::function_registry::register_function(Arc::new(TFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn isnumber_numeric_and_date() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IsNumberFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ISNUMBER").unwrap();
        let num = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Number(std::f64::consts::PI)),
            None,
        );
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
            f.dispatch(&args_num, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&args_date, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&args_txt, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
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
            f.dispatch(&args_t, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&args_n, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
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
                .unwrap()
                .into_literal(),
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
            f.dispatch(&a_err, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&a_ok, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
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
            f.dispatch(&a_num, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(1)
        );
        assert_eq!(
            f.dispatch(&a_txt, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(2)
        );
        assert_eq!(
            f.dispatch(&a_bool, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(4)
        );
        match f
            .dispatch(&a_err, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#VALUE!"),
            _ => panic!(),
        }
        assert_eq!(
            f.dispatch(&a_arr, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
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
        match na_fn
            .eval(&[], &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#N/A"),
            _ => panic!(),
        }
        // N()
        let n_fn = ctx.context.get_function("", "N").unwrap();
        let val = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(true)), None);
        let args = vec![crate::traits::ArgumentHandle::new(&val, &ctx)];
        assert_eq!(
            n_fn.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(1)
        );
        // T()
        let t_fn = ctx.context.get_function("", "T").unwrap();
        let txt = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("abc".into())), None);
        let args_t = vec![crate::traits::ArgumentHandle::new(&txt, &ctx)];
        assert_eq!(
            t_fn.dispatch(&args_t, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Text("abc".into())
        );
    }
}
