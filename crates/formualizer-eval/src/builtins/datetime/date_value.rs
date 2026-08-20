//! DATEVALUE and TIMEVALUE functions for parsing date/time strings

use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use chrono::NaiveDate;
use formualizer_common::{
    ExcelError, LiteralValue, date_to_serial_for, parse_excel_date_text, parse_excel_time_text,
    time_to_fraction,
};

fn parse_legacy_datevalue_text(input: &str) -> Option<NaiveDate> {
    let text = input.trim();
    let parts: Vec<&str> = text.split('/').collect();
    if parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        let normalized = if parts[0].len() == 4 {
            format!("{}-{}-{}", parts[0], parts[1], parts[2])
        } else {
            format!("{}/{}/{}", parts[1], parts[0], parts[2])
        };
        return parse_excel_date_text(&normalized);
    }

    let (day_and_month, year) = text.rsplit_once(' ')?;
    let (day, month) = day_and_month.split_once(' ')?;
    parse_excel_date_text(&format!("{month} {day}, {year}"))
}
use formualizer_macros::func_caps;

/// Parses a date string and returns its date serial number.
///
/// # Remarks
/// - Accepted formats are a fixed supported subset (for example `YYYY-MM-DD`, `MM/DD/YYYY`, and month-name forms).
/// - Parsing is not locale-driven; ambiguous text may parse differently than Excel locales.
/// - The returned serial uses the workbook's date system (Excel 1900 or Excel 1904).
///
/// # Examples
/// ```yaml,sandbox
/// title: "Parse ISO date"
/// formula: '=DATEVALUE("2024-01-15")'
/// expected: 45306
/// ```
///
/// ```yaml,sandbox
/// title: "Parse month-name date"
/// formula: '=DATEVALUE("Jan 15, 2024")'
/// expected: 45306
/// ```
///
/// ```yaml,docs
/// related:
///   - DATE
///   - TIMEVALUE
///   - VALUE
/// faq:
///   - q: "Why can DATEVALUE disagree with locale-specific Excel parsing?"
///     a: "This implementation uses a fixed set of accepted formats instead of workbook locale settings, so ambiguous text may parse differently."
/// ```
#[derive(Debug)]
pub struct DateValueFn;

/// [formualizer-docgen:schema:start]
/// Name: DATEVALUE
/// Type: DateValueFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: DATEVALUE(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for DateValueFn {
    fn propagate_format(
        &self,
        _result: &crate::traits::CalcValue<'_>,
    ) -> Option<crate::format::FormatId> {
        Some(crate::format::FormatId::DATE)
    }

    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "DATEVALUE"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        // Single text argument; we allow Any scalar then validate as text in impl.
        static ONE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| vec![ArgSchema::any()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let date_text = match args[0].value()?.into_literal() {
            LiteralValue::Text(s) => s,
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => {
                return Err(ExcelError::new_value()
                    .with_message(format!("DATEVALUE expects text, got {other:?}")));
            }
        };

        if let Some(date) =
            parse_excel_date_text(&date_text).or_else(|| parse_legacy_datevalue_text(&date_text))
        {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                date_to_serial_for(system, &date),
            )));
        }

        Err(ExcelError::new_value()
            .with_message("DATEVALUE could not parse date text in supported formats"))
    }
}

/// Parses a time string and returns its fractional-day serial value.
///
/// # Remarks
/// - Supported formats include 24-hour and AM/PM text forms with optional seconds.
/// - Result is a fraction in the range `0.0..1.0` and does not include a date component.
/// - Because only a time fraction is returned, workbook date-system choice does not affect output.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Parse 24-hour time"
/// formula: '=TIMEVALUE("14:30")'
/// expected: 0.6041666667
/// ```
///
/// ```yaml,sandbox
/// title: "Parse 12-hour AM/PM time"
/// formula: '=TIMEVALUE("02:30 PM")'
/// expected: 0.6041666667
/// ```
///
/// ```yaml,docs
/// related:
///   - TIME
///   - DATEVALUE
///   - SECOND
/// faq:
///   - q: "Does TIMEVALUE depend on the 1900 vs 1904 date system?"
///     a: "No. TIMEVALUE returns only a time fraction, so date-system selection does not change the result."
/// ```
#[derive(Debug)]
pub struct TimeValueFn;

/// [formualizer-docgen:schema:start]
/// Name: TIMEVALUE
/// Type: TimeValueFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: TIMEVALUE(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TimeValueFn {
    fn propagate_format(
        &self,
        _result: &crate::traits::CalcValue<'_>,
    ) -> Option<crate::format::FormatId> {
        Some(crate::format::FormatId::TIME)
    }

    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "TIMEVALUE"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| vec![ArgSchema::any()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let time_text = match args[0].value()?.into_literal() {
            LiteralValue::Text(s) => s,
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => {
                return Err(ExcelError::new_value()
                    .with_message(format!("TIMEVALUE expects text, got {other:?}")));
            }
        };

        if let Some(time) = parse_excel_time_text(&time_text) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                time_to_fraction(&time),
            )));
        }

        Err(ExcelError::new_value()
            .with_message("TIMEVALUE could not parse time text in supported formats"))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_builtin(Arc::new(DateValueFn));
    crate::function_registry::register_builtin(Arc::new(TimeValueFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    use std::sync::Arc;

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn test_datevalue_formats() {
        let wb = TestWorkbook::new().with_function(Arc::new(DateValueFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "DATEVALUE").unwrap();

        // Test ISO format
        let date_str = lit(LiteralValue::Text("2024-01-15".into()));
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&date_str, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(matches!(result, LiteralValue::Number(_)));

        // Test US format
        let date_str = lit(LiteralValue::Text("01/15/2024".into()));
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&date_str, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(matches!(result, LiteralValue::Number(_)));
    }

    #[test]
    fn test_timevalue_formats() {
        let wb = TestWorkbook::new().with_function(Arc::new(TimeValueFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TIMEVALUE").unwrap();

        // Test 24-hour format
        let time_str = lit(LiteralValue::Text("14:30:00".into()));
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&time_str, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        match result {
            LiteralValue::Number(n) => {
                // 14:30 = 14.5/24 ≈ 0.604166...
                assert!((n - 0.6041666667).abs() < 1e-9);
            }
            _ => panic!("TIMEVALUE should return a number"),
        }

        // Test 12-hour format
        let time_str = lit(LiteralValue::Text("02:30 PM".into()));
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&time_str, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        match result {
            LiteralValue::Number(n) => {
                assert!((n - 0.6041666667).abs() < 1e-9);
            }
            _ => panic!("TIMEVALUE should return a number"),
        }
    }

    fn eval_temporal_value_formula(
        system: crate::engine::DateSystem,
        formula: &str,
    ) -> LiteralValue {
        use crate::engine::{Engine, EvalConfig};
        use crate::interpreter::Interpreter;
        use formualizer_parse::parser::parse;

        let wb = TestWorkbook::new()
            .with_function(Arc::new(DateValueFn))
            .with_function(Arc::new(TimeValueFn));
        let engine = Engine::new(wb, EvalConfig::default().with_date_system(system));
        let interpreter = Interpreter::new(&engine, "Sheet1");
        interpreter
            .evaluate_ast(&parse(formula).expect("formula should parse"))
            .expect("formula should evaluate")
            .into_literal()
    }

    #[test]
    fn datevalue_and_timevalue_pin_oracle_verified_text_behavior() {
        use crate::engine::DateSystem;
        use formualizer_common::ExcelErrorKind;

        let number_cases = [
            // Two-digit years use the 29/30 window in slash and month-name forms.
            ("=DATEVALUE(\"1/1/03\")", 37_622.0),
            ("=DATEVALUE(\"1-Jan-03\")", 37_622.0),
            // Surrounding and interior whitespace match the LO oracle.
            ("=DATEVALUE(\" 2003-01-01 \")", 37_622.0),
            ("=TIMEVALUE(\" 12:00 \")", 0.5),
            ("=TIMEVALUE(\"12 : 00\")", 0.5),
        ];
        for (formula, expected) in number_cases {
            assert_eq!(
                eval_temporal_value_formula(DateSystem::Excel1900, formula),
                LiteralValue::Number(expected),
                "{formula} (oracle: lo-verified)"
            );
        }

        let wb = TestWorkbook::new().with_function(Arc::new(DateValueFn));
        let ctx = wb.interpreter();
        let function = ctx.context.get_function("", "DATEVALUE").unwrap();
        let input = lit(LiteralValue::Text("1/ 15/2003".into()));
        let error = function
            .dispatch(
                &[ArgumentHandle::new(&input, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap_err();
        assert_eq!(
            error.kind,
            ExcelErrorKind::Value,
            "oracle: lo-verified interior whitespace"
        );
    }

    #[test]
    fn datevalue_retains_preexisting_unambiguous_slash_fallbacks() {
        // oracle: lo-verified divergence. These shipped DATEVALUE-only forms
        // remain accepted for compatibility; arithmetic rejects both forms.
        for (formula, expected) in [
            ("=DATEVALUE(\"15/01/2003\")", 37_636.0),
            ("=DATEVALUE(\"2003/1/1\")", 37_622.0),
            ("=DATEVALUE(\"15/01/29\")", 47_133.0),
            ("=DATEVALUE(\"15/01/30\")", 10_973.0),
            ("=DATEVALUE(\"1 January 29\")", 47_119.0),
            ("=DATEVALUE(\"1 January 30\")", 10_959.0),
        ] {
            assert_eq!(
                eval_temporal_value_formula(crate::engine::DateSystem::Excel1900, formula),
                LiteralValue::Number(expected),
                "{formula}"
            );
        }
    }

    /// DATEVALUE emits a serial, so the workbook date system decides the epoch.
    #[test]
    fn datevalue_follows_workbook_date_system_1900_and_1904() {
        use crate::engine::DateSystem;
        use formualizer_common::date_to_serial_for;

        let parsed = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            assert_eq!(
                eval_temporal_value_formula(system, "=DATEVALUE(\"2024-01-15\")"),
                LiteralValue::Number(date_to_serial_for(system, &parsed)),
                "DATEVALUE under {system:?}"
            );
        }
    }
}
