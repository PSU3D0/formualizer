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

// MID(text, start_num, num_chars)
#[derive(Debug)]
pub struct MidFn;
impl Function for MidFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "MID"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let s = to_text(&args[0])?;
        let start = number_like(&args[1])?;
        let count = number_like(&args[2])?;
        if start < 1 || count < 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let chars: Vec<char> = s.chars().collect();
        if (start as usize) > chars.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
                String::new(),
            )));
        }
        let end = ((start - 1) + count) as usize;
        let end = min(end, chars.len());
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            chars[(start as usize - 1)..end].iter().collect(),
        )))
    }
}

// SUBSTITUTE(text, old_text, new_text, [instance_num]) - limited semantics
#[derive(Debug)]
pub struct SubstituteFn;
impl Function for SubstituteFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SUBSTITUTE"
    }
    fn min_args(&self) -> usize {
        3
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
        if args.len() < 3 || args.len() > 4 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let text = to_text(&args[0])?;
        let old = to_text(&args[1])?;
        let new = to_text(&args[2])?;
        if old.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(text)));
        }
        if args.len() == 4 {
            let instance = number_like(&args[3])?;
            if instance <= 0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
            let mut idx = 0;
            let mut count = 0;
            let mut out = String::new();
            while let Some(pos) = text[idx..].find(&old) {
                out.push_str(&text[idx..idx + pos]);
                count += 1;
                if count == instance {
                    out.push_str(&new);
                    out.push_str(&text[idx + pos + old.len()..]);
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(out)));
                } else {
                    out.push_str(&old);
                    idx += pos + old.len();
                }
            }
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(text)))
        } else {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
                text.replace(&old, &new),
            )))
        }
    }
}

// REPLACE(old_text, start_num, num_chars, new_text)
#[derive(Debug)]
pub struct ReplaceFn;
impl Function for ReplaceFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "REPLACE"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 4 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let text = to_text(&args[0])?;
        let start = number_like(&args[1])?;
        let num = number_like(&args[2])?;
        let new = to_text(&args[3])?;
        if start < 1 || num < 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let mut chars: Vec<char> = text.chars().collect();
        let len = chars.len();
        let start_idx = (start as usize).saturating_sub(1);
        if start_idx > len {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(text)));
        }
        let end_idx = (start_idx + num as usize).min(len);
        chars.splice(start_idx..end_idx, new.chars());
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            chars.into_iter().collect(),
        )))
    }
}

fn to_text<'a, 'b>(arg: &ArgumentHandle<'a, 'b>) -> Result<String, ExcelError> {
    let v = scalar_like_value(arg)?;
    Ok(match v {
        LiteralValue::Text(s) => s,
        LiteralValue::Empty => String::new(),
        LiteralValue::Boolean(b) => {
            if b {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        LiteralValue::Number(f) => {
            let mut s = f.to_string();
            if s.ends_with(".0") {
                s.truncate(s.len() - 2);
            }
            s
        }
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Error(e) => return Err(e),
        other => other.to_string(),
    })
}
fn number_like<'a, 'b>(arg: &ArgumentHandle<'a, 'b>) -> Result<i64, ExcelError> {
    let v = scalar_like_value(arg)?;
    Ok(match v {
        LiteralValue::Int(i) => i,
        LiteralValue::Number(f) => f as i64,
        LiteralValue::Text(t) => t.parse::<i64>().unwrap_or(0),
        LiteralValue::Boolean(b) => {
            if b {
                1
            } else {
                0
            }
        }
        LiteralValue::Empty => 0,
        LiteralValue::Error(e) => return Err(e),
        other => other.to_string().parse::<i64>().unwrap_or(0),
    })
}

use std::cmp::min;

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(MidFn));
    crate::function_registry::register_function(Arc::new(SubstituteFn));
    crate::function_registry::register_function(Arc::new(ReplaceFn));
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
    fn mid_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MidFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "MID").unwrap();
        let s = lit(LiteralValue::Text("hello".into()));
        let start = lit(LiteralValue::Int(2));
        let cnt = lit(LiteralValue::Int(3));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&s, &ctx),
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&cnt, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Text("ell".into()));
    }
    #[test]
    fn substitute_all() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SubstituteFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "SUBSTITUTE").unwrap();
        let text = lit(LiteralValue::Text("a_b_a".into()));
        let old = lit(LiteralValue::Text("_".into()));
        let new = lit(LiteralValue::Text("-".into()));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&old, &ctx),
                    ArgumentHandle::new(&new, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Text("a-b-a".into()));
    }
    #[test]
    fn replace_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ReplaceFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "REPLACE").unwrap();
        let text = lit(LiteralValue::Text("hello".into()));
        let start = lit(LiteralValue::Int(2));
        let num = lit(LiteralValue::Int(2));
        let new = lit(LiteralValue::Text("YY".into()));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&num, &ctx),
                    ArgumentHandle::new(&new, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Text("hYYlo".into()));
    }
}
