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

fn to_text<'a, 'b>(a: &ArgumentHandle<'a, 'b>) -> Result<String, ExcelError> {
    let v = scalar_like_value(a)?;
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
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Number(f) => {
            let s = f.to_string();
            if s.ends_with(".0") {
                s[..s.len() - 2].into()
            } else {
                s
            }
        }
        LiteralValue::Error(e) => return Err(e),
        other => other.to_string(),
    })
}

#[derive(Debug)]
pub struct TrimFn;
impl Function for TrimFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TRIM"
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
        let s = to_text(&args[0])?;
        let mut out = String::new();
        let mut prev_space = false;
        for ch in s.chars() {
            if ch.is_whitespace() {
                prev_space = true;
            } else {
                if prev_space && !out.is_empty() {
                    out.push(' ');
                }
                out.push(ch);
                prev_space = false;
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            out.trim().into(),
        )))
    }
}

#[derive(Debug)]
pub struct UpperFn;
impl Function for UpperFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "UPPER"
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            to_text(&args[0])?.to_ascii_uppercase(),
        )))
    }
}
#[derive(Debug)]
pub struct LowerFn;
impl Function for LowerFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LOWER"
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            to_text(&args[0])?.to_ascii_lowercase(),
        )))
    }
}
#[derive(Debug)]
pub struct ProperFn;
impl Function for ProperFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PROPER"
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
        let s = to_text(&args[0])?;
        let mut out = String::new();
        let mut new_word = true;
        for ch in s.chars() {
            if ch.is_alphanumeric() {
                if new_word {
                    for c in ch.to_uppercase() {
                        out.push(c);
                    }
                } else {
                    for c in ch.to_lowercase() {
                        out.push(c);
                    }
                }
                new_word = false;
            } else {
                out.push(ch);
                new_word = true;
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(out)))
    }
}

// CONCAT(text1, text2, ...)
#[derive(Debug)]
pub struct ConcatFn;
impl Function for ConcatFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CONCAT"
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
        let mut out = String::new();
        for a in args {
            out.push_str(&to_text(a)?);
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(out)))
    }
}
// CONCATENATE (alias semantics)
#[derive(Debug)]
pub struct ConcatenateFn;
impl Function for ConcatenateFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CONCATENATE"
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        ConcatFn.eval(args, ctx)
    }
}

// TEXTJOIN(delimiter, ignore_empty, text1, [text2, ...])
#[derive(Debug)]
pub struct TextJoinFn;
impl Function for TextJoinFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TEXTJOIN"
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
        if args.len() < 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        // Get delimiter
        let delimiter = to_text(&args[0])?;

        // Get ignore_empty flag
        let ignore_empty = match scalar_like_value(&args[1])? {
            LiteralValue::Boolean(b) => b,
            LiteralValue::Int(i) => i != 0,
            LiteralValue::Number(f) => f != 0.0,
            LiteralValue::Text(t) => t.to_uppercase() == "TRUE",
            LiteralValue::Empty => false,
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            _ => false,
        };

        // Collect text values
        let mut parts = Vec::new();
        for arg in args.iter().skip(2) {
            match scalar_like_value(arg)? {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                LiteralValue::Empty => {
                    if !ignore_empty {
                        parts.push(String::new());
                    }
                }
                v => {
                    let s = match v {
                        LiteralValue::Text(t) => t,
                        LiteralValue::Boolean(b) => {
                            if b {
                                "TRUE".to_string()
                            } else {
                                "FALSE".to_string()
                            }
                        }
                        LiteralValue::Int(i) => i.to_string(),
                        LiteralValue::Number(f) => f.to_string(),
                        _ => v.to_string(),
                    };
                    if !ignore_empty || !s.is_empty() {
                        parts.push(s);
                    }
                }
            }
        }

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            parts.join(&delimiter),
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(TrimFn));
    crate::function_registry::register_function(Arc::new(UpperFn));
    crate::function_registry::register_function(Arc::new(LowerFn));
    crate::function_registry::register_function(Arc::new(ProperFn));
    crate::function_registry::register_function(Arc::new(ConcatFn));
    crate::function_registry::register_function(Arc::new(ConcatenateFn));
    crate::function_registry::register_function(Arc::new(TextJoinFn));
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
    fn trim_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TrimFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TRIM").unwrap();
        let s = lit(LiteralValue::Text("  a   b  ".into()));
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&s, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Text("a b".into()));
    }
    #[test]
    fn concat_variants() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(ConcatFn))
            .with_function(std::sync::Arc::new(ConcatenateFn));
        let ctx = wb.interpreter();
        let c = ctx.context.get_function("", "CONCAT").unwrap();
        let ce = ctx.context.get_function("", "CONCATENATE").unwrap();
        let a = lit(LiteralValue::Text("a".into()));
        let b = lit(LiteralValue::Text("b".into()));
        assert_eq!(
            c.dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Text("ab".into())
        );
        assert_eq!(
            ce.dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Text("ab".into())
        );
    }

    #[test]
    fn textjoin_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TextJoinFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TEXTJOIN").unwrap();
        let delim = lit(LiteralValue::Text(",".into()));
        let ignore = lit(LiteralValue::Boolean(true));
        let a = lit(LiteralValue::Text("a".into()));
        let b = lit(LiteralValue::Text("b".into()));
        let c = lit(LiteralValue::Empty);
        let d = lit(LiteralValue::Text("d".into()));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&delim, &ctx),
                    ArgumentHandle::new(&ignore, &ctx),
                    ArgumentHandle::new(&a, &ctx),
                    ArgumentHandle::new(&b, &ctx),
                    ArgumentHandle::new(&c, &ctx),
                    ArgumentHandle::new(&d, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Text("a,b,d".into()));
    }

    #[test]
    fn textjoin_no_ignore() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TextJoinFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TEXTJOIN").unwrap();
        let delim = lit(LiteralValue::Text("-".into()));
        let ignore = lit(LiteralValue::Boolean(false));
        let a = lit(LiteralValue::Text("a".into()));
        let b = lit(LiteralValue::Empty);
        let c = lit(LiteralValue::Text("c".into()));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&delim, &ctx),
                    ArgumentHandle::new(&ignore, &ctx),
                    ArgumentHandle::new(&a, &ctx),
                    ArgumentHandle::new(&b, &ctx),
                    ArgumentHandle::new(&c, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Text("a--c".into()));
    }
}
