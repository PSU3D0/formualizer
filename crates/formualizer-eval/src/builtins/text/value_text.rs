use super::super::utils::ARG_ANY_ONE;
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

fn to_text<'a, 'b>(a: &ArgumentHandle<'a, 'b>) -> Result<String, ExcelError> {
    let v = a.value()?;
    Ok(match v.as_ref() {
        LiteralValue::Text(s) => s.clone(),
        LiteralValue::Empty => String::new(),
        LiteralValue::Boolean(b) => {
            if *b {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Number(f) => f.to_string(),
        LiteralValue::Error(e) => return Err(e.clone()),
        other => other.to_string(),
    })
}

// VALUE(text) - parse number
#[derive(Debug)]
pub struct ValueFn;
impl Function for ValueFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "VALUE"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        a: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let s = to_text(&a[0])?;
        match s.trim().parse::<f64>() {
            Ok(n) => Ok(LiteralValue::Number(n)),
            Err(_) => Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            ))),
        }
    }
}

// TEXT(value, format_text) - limited formatting (#,0,0.00, percent, yyyy, mm, dd, hh:mm) naive
#[derive(Debug)]
pub struct TextFn;
impl Function for TextFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TEXT"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        a: &'a [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if a.len() != 2 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let val = a[0].value()?;
        if let LiteralValue::Error(e) = val.as_ref() {
            return Ok(LiteralValue::Error(e.clone()));
        }
        let fmt = to_text(&a[1])?;
        let num = match val.as_ref() {
            LiteralValue::Number(f) => *f,
            LiteralValue::Int(i) => *i as f64,
            LiteralValue::Text(t) => t.parse::<f64>().unwrap_or(0.0),
            LiteralValue::Boolean(b) => {
                if *b {
                    1.0
                } else {
                    0.0
                }
            }
            LiteralValue::Empty => 0.0,
            _ => 0.0,
        };
        let out = if fmt.contains('%') {
            format_percent(num)
        } else if fmt.contains("0.00") {
            format!("{:.2}", num)
        } else if fmt.contains("0") {
            if fmt.contains(".00") {
                format!("{:.2}", num)
            } else {
                format_number_basic(num)
            }
        } else {
            // date tokens naive from serial
            if fmt.contains("yyyy") || fmt.contains("dd") || fmt.contains("mm") {
                format_serial_date(num, &fmt)
            } else {
                num.to_string()
            }
        };
        Ok(LiteralValue::Text(out))
    }
}

fn format_percent(n: f64) -> String {
    format!("{:.0}%", n * 100.0)
}
fn format_number_basic(n: f64) -> String {
    if n.fract() == 0.0 {
        format!("{:.0}", n)
    } else {
        n.to_string()
    }
}

// very naive: treat integer part as days since 1899-12-31 ignoring leap bug for now
fn format_serial_date(n: f64, fmt: &str) -> String {
    use chrono::Datelike;
    let days = n.trunc() as i64;
    let base = chrono::NaiveDate::from_ymd_opt(1899, 12, 31).unwrap();
    let date = base
        .checked_add_signed(chrono::TimeDelta::days(days))
        .unwrap_or(base);
    let mut out = fmt.to_string();
    out = out.replace("yyyy", &format!("{:04}", date.year()));
    out = out.replace("mm", &format!("{:02}", date.month()));
    out = out.replace("dd", &format!("{:02}", date.day()));
    if out.contains("hh:mm") {
        let frac = n.fract();
        let total_minutes = (frac * 24.0 * 60.0).round() as i64;
        let hh = (total_minutes / 60) % 24;
        let mm = total_minutes % 60;
        out = out.replace("hh:mm", &format!("{:02}:{:02}", hh, mm));
    }
    out
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(ValueFn));
    crate::function_registry::register_function(Arc::new(TextFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_core::parser::{ASTNode, ASTNodeType};
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }
    #[test]
    fn value_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ValueFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "VALUE").unwrap();
        let s = lit(LiteralValue::Text("12.5".into()));
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&s, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Number(12.5));
    }
    #[test]
    fn text_basic_number() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TextFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TEXT").unwrap();
        let n = lit(LiteralValue::Number(12.34));
        let fmt = lit(LiteralValue::Text("0.00".into()));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&n, &ctx),
                    ArgumentHandle::new(&fmt, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Text("12.34".into()));
    }
}
