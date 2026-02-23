//! DATEVALUE and TIMEVALUE functions for parsing date/time strings

use super::serial::{date_to_serial, time_to_fraction};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use chrono::NaiveDate;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/// Parses a date string and returns its date serial number.
///
/// # Remarks
/// - Accepted formats are a fixed supported subset (for example `YYYY-MM-DD`, `MM/DD/YYYY`, and month-name forms).
/// - Parsing is not locale-driven; ambiguous text may parse differently than Excel locales.
/// - Output uses Excel 1900 serial mapping and does not currently switch to workbook `1904` mode.
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
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
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

        // Try common date formats
        // Excel accepts many formats, we'll support a subset
        let formats = [
            "%Y-%m-%d",  // 2024-01-15
            "%m/%d/%Y",  // 01/15/2024
            "%d/%m/%Y",  // 15/01/2024
            "%Y/%m/%d",  // 2024/01/15
            "%B %d, %Y", // January 15, 2024
            "%b %d, %Y", // Jan 15, 2024
            "%d-%b-%Y",  // 15-Jan-2024
            "%d %B %Y",  // 15 January 2024
        ];

        for fmt in &formats {
            if let Ok(date) = NaiveDate::parse_from_str(&date_text, fmt) {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                    date_to_serial(&date),
                )));
            }
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

        // Try common time formats
        let formats = [
            "%H:%M:%S",    // 14:30:00
            "%H:%M",       // 14:30
            "%I:%M:%S %p", // 02:30:00 PM
            "%I:%M %p",    // 02:30 PM
        ];

        for fmt in &formats {
            if let Ok(time) = chrono::NaiveTime::parse_from_str(&time_text, fmt) {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                    time_to_fraction(&time),
                )));
            }
        }

        Err(ExcelError::new_value()
            .with_message("TIMEVALUE could not parse time text in supported formats"))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(DateValueFn));
    crate::function_registry::register_function(Arc::new(TimeValueFn));
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
}
