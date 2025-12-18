use super::super::utils::ARG_ANY_ONE;
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

fn scalar_like_value(arg: &ArgumentHandle<'_, '_>) -> Result<LiteralValue, ExcelError> {
    Ok(match arg.value()? {
        crate::traits::CalcValue::Scalar(v) => v,
        crate::traits::CalcValue::Range(rv) => rv.get_cell(0, 0),
    })
}

#[derive(Debug)]
pub struct LenFn;
impl Function for LenFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LEN"
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
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let v = scalar_like_value(&args[0])?;
        let count = match v {
            LiteralValue::Text(s) => s.chars().count() as i64,
            LiteralValue::Empty => 0,
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => other.to_string().chars().count() as i64,
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(count)))
    }
}

#[derive(Debug)]
pub struct LeftFn;
impl Function for LeftFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LEFT"
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
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() || args.len() > 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let s_val = scalar_like_value(&args[0])?;
        let s = match s_val {
            LiteralValue::Text(t) => t,
            LiteralValue::Empty => String::new(),
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => other.to_string(),
        };
        let n: i64 = if args.len() == 2 {
            number_like(&args[1])?
        } else {
            1
        };
        if n < 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let chars: Vec<char> = s.chars().collect();
        let take = (n as usize).min(chars.len());
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            chars[..take].iter().collect(),
        )))
    }
}

#[derive(Debug)]
pub struct RightFn;
impl Function for RightFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "RIGHT"
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
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() || args.len() > 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let s_val = scalar_like_value(&args[0])?;
        let s = match s_val {
            LiteralValue::Text(t) => t,
            LiteralValue::Empty => String::new(),
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => other.to_string(),
        };
        let n: i64 = if args.len() == 2 {
            number_like(&args[1])?
        } else {
            1
        };
        if n < 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let chars: Vec<char> = s.chars().collect();
        let len = chars.len();
        let start = len.saturating_sub(n as usize);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            chars[start..].iter().collect(),
        )))
    }
}

fn number_like<'a, 'b>(arg: &ArgumentHandle<'a, 'b>) -> Result<i64, ExcelError> {
    let v = scalar_like_value(arg)?;
    Ok(match v {
        LiteralValue::Int(i) => i,
        LiteralValue::Number(f) => f as i64,
        LiteralValue::Empty => 0,
        LiteralValue::Text(t) => t.parse::<i64>().unwrap_or(0),
        LiteralValue::Boolean(b) => {
            if b {
                1
            } else {
                0
            }
        }
        LiteralValue::Error(e) => return Err(e),
        other => other.to_string().parse::<i64>().unwrap_or(0),
    })
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(LenFn));
    crate::function_registry::register_function(Arc::new(LeftFn));
    crate::function_registry::register_function(Arc::new(RightFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }
    #[test]
    fn len_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(LenFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "LEN").unwrap();
        let s = lit(LiteralValue::Text("abc".into()));
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&s, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out.into_literal(), LiteralValue::Int(3));
    }
    #[test]
    fn left_right() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(LeftFn))
            .with_function(std::sync::Arc::new(RightFn));
        let ctx = wb.interpreter();
        let l = ctx.context.get_function("", "LEFT").unwrap();
        let r = ctx.context.get_function("", "RIGHT").unwrap();
        let s = lit(LiteralValue::Text("hello".into()));
        let n = lit(LiteralValue::Int(2));
        assert_eq!(
            l.dispatch(
                &[ArgumentHandle::new(&s, &ctx), ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Text("he".into())
        );
        assert_eq!(
            r.dispatch(
                &[ArgumentHandle::new(&s, &ctx), ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Text("lo".into())
        );
    }
}
