use super::super::utils::ARG_ANY_ONE;
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

fn scalar_like_value(arg: &ArgumentHandle<'_, '_>) -> Result<LiteralValue, ExcelError> {
    Ok(match arg.value()? {
        crate::traits::CalcValue::Scalar(v) => v,
        crate::traits::CalcValue::Range(rv) => rv.get_cell(0, 0),
        crate::traits::CalcValue::Callable(_) => LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Calc).with_message("LAMBDA value must be invoked"),
        ),
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
/// Converts text that represents a number into a numeric value.
///
/// # Remarks
/// - Parsing uses locale-aware invariant number parsing from the function context.
/// - Non-numeric text returns `#VALUE!`.
/// - Booleans and numbers are first coerced to text, then parsed.
/// - Errors are propagated unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Parse decimal text"
/// formula: '=VALUE("12.5")'
/// expected: 12.5
/// ```
///
/// ```yaml,sandbox
/// title: "Invalid numeric text"
/// formula: '=VALUE("abc")'
/// expected: "#VALUE!"
/// ```
///
/// ```yaml,docs
/// related:
///   - TEXT
///   - N
///   - ISNUMBER
/// faq:
///   - q: "Does VALUE coerce arbitrary text like TRUE/FALSE?"
///     a: "VALUE parses numeric text only; non-numeric strings return #VALUE!."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: VALUE
/// Type: ValueFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: VALUE(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let s = to_text(&args[0])?;
        let Some(n) = ctx.locale().parse_number_invariant(&s) else {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(n)))
    }
}

/// Converts locale-delimited text to a number.
///
/// Parses text using explicit decimal and group separators, independent of the
/// workbook's invariant locale.
///
/// # Remarks
/// - The decimal separator defaults to `.`.
/// - The group separator defaults to `,`.
/// - Percent suffixes are supported and scale the result by 100 per suffix.
///
/// ```yaml,sandbox
/// title: "Parse with explicit separators"
/// formula: '=NUMBERVALUE("1.234,56",",",".")'
/// expected: 1234.56
/// ```
///
/// ```yaml,sandbox
/// title: "Parse percent suffix"
/// formula: '=NUMBERVALUE("12.5%")'
/// expected: 0.125
/// ```
///
/// ```yaml,docs
/// related:
///   - VALUE
///   - TEXT
///   - DOLLAR
/// faq:
///   - q: "Does NUMBERVALUE use the global locale?"
///     a: "No. Decimal and group separators are passed explicitly as arguments."
/// ```
#[derive(Debug)]
pub struct NumberValueFn;

/// [formualizer-docgen:schema:start]
/// Name: NUMBERVALUE
/// Type: NumberValueFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: NUMBERVALUE(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NumberValueFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NUMBERVALUE"
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
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() || args.len() > 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let text = to_text(&args[0])?;
        let decimal_sep = if args.len() >= 2 {
            to_text(&args[1])?
        } else {
            ".".to_string()
        };
        let group_sep = if args.len() >= 3 {
            to_text(&args[2])?
        } else {
            ",".to_string()
        };

        if decimal_sep.is_empty() || decimal_sep == group_sep {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let mut trimmed = text.trim();
        let mut pct_count = 0u32;
        while let Some(prefix) = trimmed.strip_suffix('%') {
            trimmed = prefix.trim_end();
            pct_count += 1;
        }
        if trimmed.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let cleaned = trimmed.replace(&group_sep, "").replace(&decimal_sep, ".");
        if cleaned.matches('.').count() > 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let Ok(mut n) = cleaned.parse::<f64>() else {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        };
        for _ in 0..pct_count {
            n /= 100.0;
        }

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(n)))
    }
}

// TEXT(value, format_text) - limited formatting (#,0,0.00, percent, yyyy, mm, dd, hh:mm) naive
#[derive(Debug)]
pub struct TextFn;
/// Formats a value as text using a format pattern.
///
/// This implementation supports common numeric, percent, grouping, and basic date tokens.
///
/// # Remarks
/// - Requires exactly two arguments: value and format text.
/// - Numeric text is parsed before formatting. Text that is *clearly* non-numeric (no
///   digits) is returned unchanged (e.g. `=TEXT("abc","00")` -> `"abc"`), matching Excel.
///   Digit-bearing text that is not a plain number (dates, currency, fractions, or
///   locale-ambiguous values like `"1.234,56"`) still returns `#VALUE!` for now.
/// - Error inputs are propagated unchanged.
/// - Supported patterns are intentionally limited compared with full Excel formatting.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Fixed decimal formatting"
/// formula: '=TEXT(12.3, "0.00")'
/// expected: "12.30"
/// ```
///
/// ```yaml,sandbox
/// title: "Percent formatting"
/// formula: '=TEXT(0.256, "0%")'
/// expected: "26%"
/// ```
///
/// ```yaml,docs
/// related:
///   - VALUE
///   - FIXED
///   - DOLLAR
/// faq:
///   - q: "How complete is format_text support?"
///     a: "Only a limited subset of Excel-style numeric/date tokens is supported in this implementation."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: TEXT
/// Type: TextFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: TEXT(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
        ctx: &dyn FunctionContext<'b>,
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
            LiteralValue::Text(t) => match ctx.locale().parse_number_invariant(&t) {
                Some(n) => n,
                None => {
                    // Excel returns the text argument unchanged only when it is
                    // *clearly* non-numeric (e.g. =TEXT("abc","00") -> "abc"). Text
                    // that contains digits may be a number, date, currency or
                    // fraction that Excel would coerce and format (e.g. "3-1",
                    // "$5", "1/2", or locale-ambiguous "1.234,56"); handling those
                    // requires a shared TEXT/VALUE coercion that does not exist yet,
                    // so we conservatively keep returning #VALUE! for them rather
                    // than passing them through unformatted.
                    if t.chars().any(|c| c.is_ascii_digit()) {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new_value(),
                        )));
                    }
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(t)));
                }
            },
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
    crate::function_registry::register_function(Arc::new(NumberValueFn));
    crate::function_registry::register_function(Arc::new(TextFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::{ExcelErrorKind, LiteralValue};
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
    fn value_percent_text() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ValueFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "VALUE").unwrap();
        let s = lit(LiteralValue::Text("90%".into()));
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&s, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Number(0.9));
    }

    #[test]
    fn numbervalue_supports_explicit_separators_and_percent() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NumberValueFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "NUMBERVALUE").unwrap();
        let text = lit(LiteralValue::Text(" 1.234,50%% ".into()));
        let dec = lit(LiteralValue::Text(",".into()));
        let grp = lit(LiteralValue::Text(".".into()));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&text, &ctx),
                    ArgumentHandle::new(&dec, &ctx),
                    ArgumentHandle::new(&grp, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Number(0.12345));
    }

    #[test]
    fn numbervalue_rejects_bad_separators_and_multiple_decimals() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NumberValueFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "NUMBERVALUE").unwrap();
        let text = lit(LiteralValue::Text("1.2.3".into()));
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&text, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(matches!(out, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));

        let sep = lit(LiteralValue::Text(".".into()));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&lit(LiteralValue::Text("1.2".into())), &ctx),
                    ArgumentHandle::new(&sep, &ctx),
                    ArgumentHandle::new(&sep, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(matches!(out, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
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

    #[test]
    fn text_clearly_non_numeric_text_passes_through() {
        // Excel returns the text argument unchanged when it is *clearly* not a
        // number (no digits): =TEXT("abc","00") -> "abc" (not #VALUE!). A numeric
        // format does not coerce arbitrary letters.
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TextFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TEXT").unwrap();
        for input in ["abc", "N/A", "hello world"] {
            let v = lit(LiteralValue::Text(input.into()));
            let fmt = lit(LiteralValue::Text("00".into()));
            let out = f
                .dispatch(
                    &[
                        ArgumentHandle::new(&v, &ctx),
                        ArgumentHandle::new(&fmt, &ctx),
                    ],
                    &ctx.function_context(None),
                )
                .unwrap()
                .into_literal();
            assert_eq!(
                out,
                LiteralValue::Text(input.into()),
                "TEXT({input:?},\"00\")"
            );
        }
    }

    #[test]
    fn text_digit_bearing_text_still_errors() {
        // Text that contains digits may be a number/date/currency/fraction that
        // Excel would coerce and format. Until a shared TEXT/VALUE coercion exists
        // we keep returning #VALUE! rather than passing it through unformatted,
        // and we must not change the locale-ambiguous "1.234,56" case.
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TextFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TEXT").unwrap();
        for input in ["3-1", "10-", "1.234,56", "$5", "1/2"] {
            let v = lit(LiteralValue::Text(input.into()));
            let fmt = lit(LiteralValue::Text("00".into()));
            let out = f
                .dispatch(
                    &[
                        ArgumentHandle::new(&v, &ctx),
                        ArgumentHandle::new(&fmt, &ctx),
                    ],
                    &ctx.function_context(None),
                )
                .unwrap()
                .into_literal();
            match out {
                LiteralValue::Error(e) => {
                    assert_eq!(e.to_string(), "#VALUE!", "TEXT({input:?},\"00\")")
                }
                other => panic!("expected #VALUE! for TEXT({input:?},\"00\"), got {other:?}"),
            }
        }
    }
}
