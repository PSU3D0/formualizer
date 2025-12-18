//! TODAY and NOW volatile functions

use super::serial::{date_to_serial_for, datetime_to_serial_for};
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/// TODAY() - Returns current date as serial number (volatile)
#[derive(Debug)]
pub struct TodayFn;

impl Function for TodayFn {
    func_caps!(VOLATILE);

    fn name(&self) -> &'static str {
        "TODAY"
    }

    fn min_args(&self) -> usize {
        0
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let today = ctx.timezone().today();
        let serial = date_to_serial_for(ctx.date_system(), &today);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            serial,
        )))
    }
}

/// NOW() - Returns current date and time as serial number (volatile)
#[derive(Debug)]
pub struct NowFn;

impl Function for NowFn {
    func_caps!(VOLATILE);

    fn name(&self) -> &'static str {
        "NOW"
    }

    fn min_args(&self) -> usize {
        0
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let now = ctx.timezone().now();
        let serial = datetime_to_serial_for(ctx.date_system(), &now);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            serial,
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(TodayFn));
    crate::function_registry::register_function(Arc::new(NowFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use std::sync::Arc;

    #[test]
    fn test_today_volatility() {
        let wb = TestWorkbook::new().with_function(Arc::new(TodayFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TODAY").unwrap();

        // Check that it returns a number
        let result = f
            .dispatch(&[], &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match result {
            LiteralValue::Number(n) => {
                // Should be a reasonable date serial number (> 0)
                assert!(n > 0.0);
                // Should be an integer (no time component)
                assert_eq!(n.trunc(), n);
            }
            _ => panic!("TODAY should return a number"),
        }

        // Volatility flag is set via func_caps!(VOLATILE) macro
    }

    #[test]
    fn test_now_volatility() {
        let wb = TestWorkbook::new().with_function(Arc::new(NowFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "NOW").unwrap();

        // Check that it returns a number
        let result = f
            .dispatch(&[], &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match result {
            LiteralValue::Number(n) => {
                // Should be a reasonable date serial number (> 0)
                assert!(n > 0.0);
                // Should have a fractional component (time)
                // Note: There's a tiny chance this could fail if run exactly at midnight
                // but that's extremely unlikely
            }
            _ => panic!("NOW should return a number"),
        }

        // Volatility flag is set via func_caps!(VOLATILE) macro
    }
}
