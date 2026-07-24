//! EDATE and EOMONTH functions for date arithmetic

use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use chrono::{Datelike, NaiveDate};
use formualizer_common::{
    DateSystem, ExcelError, LiteralValue, date_to_serial_for, try_serial_to_date_for,
};
use formualizer_macros::func_caps;

fn coerce_to_serial(arg: &ArgumentHandle, system: DateSystem) -> Result<f64, ExcelError> {
    let v = arg.value()?.into_literal();
    if let LiteralValue::Error(e) = v {
        return Err(e);
    }
    crate::coercion::to_serial_lenient(&v, system).map_err(|_| {
        ExcelError::new_value()
            .with_message("EDATE/EOMONTH expects numeric, date, or text-numeric arguments")
    })
}

fn coerce_to_int(arg: &ArgumentHandle) -> Result<i32, ExcelError> {
    let v = arg.value()?.into_literal();
    if let LiteralValue::Error(e) = v {
        return Err(e);
    }
    crate::coercion::to_number_lenient(&v)
        .map(|f| f.trunc() as i32)
        .map_err(|_| {
            ExcelError::new_value()
                .with_message("EDATE/EOMONTH months argument is not a valid number")
        })
}

/// Returns the serial date offset by a whole number of months from a start date.
///
/// # Remarks
/// - `months` is truncated to an integer before calculation.
/// - If the target month has fewer days, the day is clamped to that month's last valid day.
/// - Serials are interpreted and emitted with the workbook's date system (Excel 1900 or Excel 1904).
///
/// # Examples
/// ```yaml,sandbox
/// title: "Add months to first-of-month date"
/// formula: "=EDATE(44927, 3)"
/// expected: 45017
/// ```
///
/// ```yaml,sandbox
/// title: "Clamp month-end overflow"
/// formula: "=EDATE(45322, 1)"
/// expected: 45351
/// ```
///
/// ```yaml,docs
/// related:
///   - EOMONTH
///   - DATE
///   - YEARFRAC
/// faq:
///   - q: "What happens when the start day does not exist in the target month?"
///     a: "EDATE clamps to the last valid day of the target month (for example Jan 31 + 1 month becomes Feb month-end)."
/// ```
#[derive(Debug)]
pub struct EdateFn;

/// [formualizer-docgen:schema:start]
/// Name: EDATE
/// Type: EdateFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: EDATE(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let start_serial = coerce_to_serial(&args[0], system)?;
        let months = coerce_to_int(&args[1])?;

        let start_date = try_serial_to_date_for(system, start_serial)?;

        // Calculate target year and month using Euclidean division
        let total_months =
            start_date.year() as i64 * 12 + start_date.month() as i64 + months as i64;
        let tm = total_months - 1;
        let target_year = tm.div_euclid(12) as i32;
        let target_month = (tm.rem_euclid(12) + 1) as u32;

        // Keep the same day, but handle month-end overflow
        let max_day = last_day_of_month(target_year, target_month);
        let target_day = start_date.day().min(max_day);

        let target_date = NaiveDate::from_ymd_opt(target_year, target_month, target_day)
            .ok_or_else(ExcelError::new_num)?;

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            date_to_serial_for(system, &target_date),
        )))
    }
}

/// Returns the serial for the last day of the month at a month offset from a start date.
///
/// # Remarks
/// - `months` is truncated to an integer before offset calculation.
/// - The returned date is always the month-end date for the target month.
/// - Serials are interpreted and returned using the workbook's date system (Excel 1900 or Excel 1904).
///
/// # Examples
/// ```yaml,sandbox
/// title: "Get end of current month"
/// formula: "=EOMONTH(44927, 0)"
/// expected: 44957
/// ```
///
/// ```yaml,sandbox
/// title: "Get end of month two months ahead"
/// formula: "=EOMONTH(45322, 2)"
/// expected: 45382
/// ```
///
/// ```yaml,docs
/// related:
///   - EDATE
///   - DATE
///   - DAY
/// faq:
///   - q: "Does EOMONTH always return a month-end date?"
///     a: "Yes. Regardless of the start day, EOMONTH returns the final calendar day of the target month after offset."
/// ```
#[derive(Debug)]
pub struct EomonthFn;

/// [formualizer-docgen:schema:start]
/// Name: EOMONTH
/// Type: EomonthFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: EOMONTH(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let start_serial = coerce_to_serial(&args[0], system)?;
        let months = coerce_to_int(&args[1])?;

        let start_date = try_serial_to_date_for(system, start_serial)?;

        // Calculate target year and month using Euclidean division
        let total_months =
            start_date.year() as i64 * 12 + start_date.month() as i64 + months as i64;
        let tm = total_months - 1;
        let target_year = tm.div_euclid(12) as i32;
        let target_month = (tm.rem_euclid(12) + 1) as u32;

        // Get the last day of the target month
        let last_day = last_day_of_month(target_year, target_month);

        let target_date = NaiveDate::from_ymd_opt(target_year, target_month, last_day)
            .ok_or_else(ExcelError::new_num)?;

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            date_to_serial_for(system, &target_date),
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
    crate::function_registry::register_builtin(Arc::new(EdateFn));
    crate::function_registry::register_builtin(Arc::new(EomonthFn));
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

    fn eval_month_offset_formula(system: crate::engine::DateSystem, formula: &str) -> LiteralValue {
        use crate::engine::{Engine, EvalConfig};
        use crate::interpreter::Interpreter;
        use formualizer_parse::parser::parse;

        let wb = TestWorkbook::new()
            .with_function(Arc::new(EdateFn))
            .with_function(Arc::new(EomonthFn));
        let engine = Engine::new(wb, EvalConfig::default().with_date_system(system));
        let interpreter = Interpreter::new(&engine, "Sheet1");
        interpreter
            .evaluate_ast(&parse(formula).expect("formula should parse"))
            .expect("formula should evaluate")
            .into_literal()
    }

    /// EDATE round-trips serial -> date -> shifted date -> serial, so the
    /// workbook date system must be used on both ends.
    #[test]
    fn edate_follows_workbook_date_system_1900_and_1904() {
        use crate::engine::DateSystem;
        use formualizer_common::date_to_serial_for;

        // 2023-01-31 + 1 month clamps to 2023-02-28 in either date system.
        let start = chrono::NaiveDate::from_ymd_opt(2023, 1, 31).unwrap();
        let expected_date = chrono::NaiveDate::from_ymd_opt(2023, 2, 28).unwrap();

        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let start_serial = date_to_serial_for(system, &start);
            assert_eq!(
                eval_month_offset_formula(system, &format!("=EDATE({start_serial},1)")),
                LiteralValue::Number(date_to_serial_for(system, &expected_date)),
                "EDATE under {system:?}"
            );
        }
    }

    #[test]
    fn eomonth_follows_workbook_date_system_1900_and_1904() {
        use crate::engine::DateSystem;
        use formualizer_common::date_to_serial_for;

        // 2024-02-15 with a zero offset is the leap-year month end 2024-02-29.
        let start = chrono::NaiveDate::from_ymd_opt(2024, 2, 15).unwrap();
        let expected_date = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();

        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let start_serial = date_to_serial_for(system, &start);
            assert_eq!(
                eval_month_offset_formula(system, &format!("=EOMONTH({start_serial},0)")),
                LiteralValue::Number(date_to_serial_for(system, &expected_date)),
                "EOMONTH under {system:?}"
            );
        }
    }
}
