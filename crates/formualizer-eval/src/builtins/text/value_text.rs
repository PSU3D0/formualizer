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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let s = to_text(&args[0])?;
        match s.trim().parse::<f64>() {
            Ok(n) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(n))),
            Err(_) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let val = scalar_like_value(&args[0])?;
        if let LiteralValue::Error(e) = val {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
        }
        let fmt = to_text(&args[1])?;
        let num = match val {
            LiteralValue::Number(f) => f,
            LiteralValue::Int(i) => i as f64,
            LiteralValue::Text(t) => t.parse::<f64>().unwrap_or(0.0),
            LiteralValue::Boolean(b) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
            LiteralValue::Empty => 0.0,
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            _ => 0.0,
        };
        let out = if fmt.contains('%') {
            format_percent(num)
        } else if fmt.contains('#') && fmt.contains(',') {
            // Handle formats like #,##0 or #,##0.00
            format_with_thousands(num, &fmt)
        } else if fmt.contains("0.00") {
            format!("{num:.2}")
        } else if fmt.contains("0") {
            if fmt.contains(".00") {
                format!("{num:.2}")
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(out)))
    }
}

fn format_percent(n: f64) -> String {
    format!("{:.0}%", n * 100.0)
}
fn format_number_basic(n: f64) -> String {
    if n.fract() == 0.0 {
        format!("{n:.0}")
    } else {
        n.to_string()
    }
}

fn format_with_thousands(n: f64, fmt: &str) -> String {
    // Determine decimal places from format
    let decimal_places = if fmt.contains(".00") {
        2
    } else if fmt.contains(".0") {
        1
    } else {
        0
    };

    let abs_n = n.abs();
    let formatted = if decimal_places > 0 {
        format!("{:.prec$}", abs_n, prec = decimal_places)
    } else {
        format!("{:.0}", abs_n)
    };

    // Split into integer and decimal parts
    let parts: Vec<&str> = formatted.split('.').collect();
    let int_part = parts[0];
    let dec_part = parts.get(1);

    // Add thousands separators to integer part
    let int_with_commas: String = int_part
        .chars()
        .rev()
        .enumerate()
        .flat_map(|(i, c)| {
            if i > 0 && i % 3 == 0 {
                vec![',', c]
            } else {
                vec![c]
            }
        })
        .collect::<String>()
        .chars()
        .rev()
        .collect();

    // Combine with decimal part
    let result = if let Some(dec) = dec_part {
        format!("{}.{}", int_with_commas, dec)
    } else {
        int_with_commas
    };

    // Handle negative numbers
    if n < 0.0 {
        format!("-{}", result)
    } else {
        result
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
        out = out.replace("hh:mm", &format!("{hh:02}:{mm:02}"));
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
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
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
            .unwrap()
            .into_literal();
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
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Text("12.34".into()));
    }
}
