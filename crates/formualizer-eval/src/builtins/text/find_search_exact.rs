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
        LiteralValue::Number(f) => f.to_string(),
        LiteralValue::Error(e) => return Err(e),
        other => other.to_string(),
    })
}

// FIND(find_text, within_text, [start_num]) - case sensitive
#[derive(Debug)]
pub struct FindFn;
impl Function for FindFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FIND"
    }
    fn min_args(&self) -> usize {
        2
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
        if args.len() < 2 || args.len() > 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let needle = to_text(&args[0])?;
        let hay = to_text(&args[1])?;
        let start = if args.len() == 3 {
            let n = number_like(&args[2])?;
            if n < 1 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
            (n - 1) as usize
        } else {
            0
        };
        if needle.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(1)));
        }
        if start > hay.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        if let Some(pos) = hay[start..].find(&needle) {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                (start + pos + 1) as i64,
            )))
        } else {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )))
        }
    }
}

// SEARCH(find_text, within_text, [start_num]) - case insensitive + simple wildcard * ?
#[derive(Debug)]
pub struct SearchFn;
impl Function for SearchFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SEARCH"
    }
    fn min_args(&self) -> usize {
        2
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
        if args.len() < 2 || args.len() > 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let needle = to_text(&args[0])?.to_ascii_lowercase();
        let hay_raw = to_text(&args[1])?;
        let hay = hay_raw.to_ascii_lowercase();
        let start = if args.len() == 3 {
            let n = number_like(&args[2])?;
            if n < 1 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
            (n - 1) as usize
        } else {
            0
        };
        if needle.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(1)));
        }
        if start > hay.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        // Convert wildcard to regex-like simple pattern
        // We'll implement manual scanning.
        let is_wild = needle.contains('*') || needle.contains('?');
        if !is_wild {
            if let Some(pos) = hay[start..].find(&needle) {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                    (start + pos + 1) as i64,
                )));
            } else {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        }
        // Wildcard scan
        for offset in start..=hay.len() {
            if wildcard_match(&needle, &hay[offset..]) {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                    (offset + 1) as i64,
                )));
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            ExcelError::from_error_string("#VALUE!"),
        )))
    }
}

fn wildcard_match(pat: &str, text: &str) -> bool {
    fn rec(p: &[u8], t: &[u8]) -> bool {
        if p.is_empty() {
            return true;
        }
        match p[0] {
            b'*' => {
                for i in 0..=t.len() {
                    if rec(&p[1..], &t[i..]) {
                        return true;
                    }
                }
                false
            }
            b'?' => {
                if t.is_empty() {
                    false
                } else {
                    rec(&p[1..], &t[1..])
                }
            }
            c => {
                if !t.is_empty() && t[0] == c {
                    rec(&p[1..], &t[1..])
                } else {
                    false
                }
            }
        }
    }
    rec(pat.as_bytes(), text.as_bytes())
}

// EXACT(text1,text2)
#[derive(Debug)]
pub struct ExactFn;
impl Function for ExactFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "EXACT"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let a = to_text(&args[0])?;
        let b = to_text(&args[1])?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            a == b,
        )))
    }
}

fn number_like<'a, 'b>(a: &ArgumentHandle<'a, 'b>) -> Result<i64, ExcelError> {
    let v = scalar_like_value(a)?;
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

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(FindFn));
    crate::function_registry::register_function(Arc::new(SearchFn));
    crate::function_registry::register_function(Arc::new(ExactFn));
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
    fn find_search() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(FindFn))
            .with_function(std::sync::Arc::new(SearchFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "FIND").unwrap();
        let s = ctx.context.get_function("", "SEARCH").unwrap();
        let hay = lit(LiteralValue::Text("Hello World".into()));
        let needle = lit(LiteralValue::Text("World".into()));
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&needle, &ctx),
                    ArgumentHandle::new(&hay, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(7)
        );
        let needle2 = lit(LiteralValue::Text("world".into()));
        assert_eq!(
            s.dispatch(
                &[
                    ArgumentHandle::new(&needle2, &ctx),
                    ArgumentHandle::new(&hay, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(7)
        );
    }
}
