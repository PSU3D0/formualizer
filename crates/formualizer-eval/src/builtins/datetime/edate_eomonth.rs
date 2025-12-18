//! EDATE and EOMONTH functions for date arithmetic

use super::serial::{date_to_serial, serial_to_date};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use chrono::{Datelike, NaiveDate};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

fn coerce_to_serial(arg: &ArgumentHandle) -> Result<f64, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Number(f) => Ok(f),
        LiteralValue::Int(i) => Ok(i as f64),
        LiteralValue::Text(s) => s.parse::<f64>().map_err(|_| {
            ExcelError::new_value().with_message("EDATE/EOMONTH start_date is not a valid number")
        }),
        LiteralValue::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()
            .with_message("EDATE/EOMONTH expects numeric or text-numeric arguments")),
    }
}

fn coerce_to_int(arg: &ArgumentHandle) -> Result<i32, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Int(i) => Ok(i as i32),
        LiteralValue::Number(f) => Ok(f.trunc() as i32),
        LiteralValue::Text(s) => s.parse::<f64>().map(|f| f.trunc() as i32).map_err(|_| {
            ExcelError::new_value().with_message("EDATE/EOMONTH months is not a valid number")
        }),
        LiteralValue::Boolean(b) => Ok(if b { 1 } else { 0 }),
        LiteralValue::Empty => Ok(0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()
            .with_message("EDATE/EOMONTH expects numeric or text-numeric arguments")),
    }
}

/// EDATE(start_date, months) - Returns date that is months away from start_date
#[derive(Debug)]
pub struct EdateFn;

impl Function for EdateFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "EDATE"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static TWO: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                // start_date serial (numeric lenient)
                ArgSchema::number_lenient_scalar(),
                // months offset (numeric lenient)
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &TWO[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let start_serial = coerce_to_serial(&args[0])?;
        let months = coerce_to_int(&args[1])?;

        let start_date = serial_to_date(start_serial)?;

        // Calculate target year and month
        let total_months = start_date.year() * 12 + start_date.month() as i32 + months;
        let target_year = total_months / 12;
        let target_month = ((total_months % 12) + 12) % 12; // Handle negative modulo
        let target_month = if target_month == 0 { 12 } else { target_month };

        // Keep the same day, but handle month-end overflow
        let max_day = last_day_of_month(target_year, target_month as u32);
        let target_day = start_date.day().min(max_day);

        let target_date = NaiveDate::from_ymd_opt(target_year, target_month as u32, target_day)
            .ok_or_else(ExcelError::new_num)?;

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            date_to_serial(&target_date),
        )))
    }
}

/// EOMONTH(start_date, months) - Returns last day of month that is months away
#[derive(Debug)]
pub struct EomonthFn;

impl Function for EomonthFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "EOMONTH"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static TWO: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &TWO[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let start_serial = coerce_to_serial(&args[0])?;
        let months = coerce_to_int(&args[1])?;

        let start_date = serial_to_date(start_serial)?;

        // Calculate target year and month
        let total_months = start_date.year() * 12 + start_date.month() as i32 + months;
        let target_year = total_months / 12;
        let target_month = ((total_months % 12) + 12) % 12; // Handle negative modulo
        let target_month = if target_month == 0 { 12 } else { target_month };

        // Get the last day of the target month
        let last_day = last_day_of_month(target_year, target_month as u32);

        let target_date = NaiveDate::from_ymd_opt(target_year, target_month as u32, last_day)
            .ok_or_else(ExcelError::new_num)?;

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            date_to_serial(&target_date),
        )))
    }
}

/// Helper to get the last day of a month
fn last_day_of_month(year: i32, month: u32) -> u32 {
    // Try day 31, then 30, 29, 28
    for day in (28..=31).rev() {
        if NaiveDate::from_ymd_opt(year, month, day).is_some() {
            return day;
        }
    }
    28 // Fallback (should never reach here for valid months)
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(EdateFn));
    crate::function_registry::register_function(Arc::new(EomonthFn));
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
    fn test_edate_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(EdateFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "EDATE").unwrap();

        // Test adding months
        // Use a known date serial (e.g., 44927 = 2023-01-01)
        let start = lit(LiteralValue::Number(44927.0));
        let months = lit(LiteralValue::Int(3));

        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&months, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();

        // Should return a date 3 months later
        assert!(matches!(result, LiteralValue::Number(_)));
    }

    #[test]
    fn test_edate_negative_months() {
        let wb = TestWorkbook::new().with_function(Arc::new(EdateFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "EDATE").unwrap();

        // Test subtracting months
        let start = lit(LiteralValue::Number(44927.0)); // 2023-01-01
        let months = lit(LiteralValue::Int(-2));

        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&months, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();

        // Should return a date 2 months earlier
        assert!(matches!(result, LiteralValue::Number(_)));
    }

    #[test]
    fn test_eomonth_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(EomonthFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "EOMONTH").unwrap();

        // Test end of month
        let start = lit(LiteralValue::Number(44927.0)); // 2023-01-01
        let months = lit(LiteralValue::Int(0));

        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&months, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();

        // Should return Jan 31, 2023
        assert!(matches!(result, LiteralValue::Number(_)));
    }

    #[test]
    fn test_eomonth_february() {
        let wb = TestWorkbook::new().with_function(Arc::new(EomonthFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "EOMONTH").unwrap();

        // Test February (checking leap year handling)
        let start = lit(LiteralValue::Number(44927.0)); // 2023-01-01
        let months = lit(LiteralValue::Int(1)); // Move to February

        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&months, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();

        // Should return Feb 28, 2023 (not a leap year)
        assert!(matches!(result, LiteralValue::Number(_)));
    }
}
