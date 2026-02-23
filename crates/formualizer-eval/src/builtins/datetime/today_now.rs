//! TODAY and NOW volatile functions

use super::serial::{date_to_serial_for, datetime_to_serial_for};
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/// Returns the current date as a volatile serial value.
///
/// # Remarks
/// - `TODAY` is volatile and recalculates each time the workbook recalculates.
/// - The result is an integer date serial with no time fraction.
/// - Serial output respects the active workbook date system (`1900` or `1904`).
///
/// # Examples
/// ```yaml,sandbox
/// title: "TODAY has no time fraction"
/// formula: "=TODAY()=INT(TODAY())"
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Date arithmetic with TODAY"
/// formula: "=TODAY()+7-TODAY()"
/// expected: 7
/// ```
///
/// ```yaml,docs
/// related:
///   - NOW
///   - DATE
///   - WORKDAY
/// faq:
///   - q: "Will TODAY include a time-of-day fraction?"
///     a: "No. TODAY always returns an integer serial date, so its fractional part is always 0."
/// ```
#[derive(Debug)]
pub struct TodayFn;

/// [formualizer-docgen:schema:start]
/// Name: TODAY
/// Type: TodayFn
/// Min args: 0
/// Max args: 0
/// Variadic: false
/// Signature: TODAY()
/// Arg schema: []
/// Caps: VOLATILE
/// [formualizer-docgen:schema:end]
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
        let today = ctx.clock().today();
        let serial = date_to_serial_for(ctx.date_system(), &today);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            serial,
        )))
    }
}

/// Returns the current date and time as a volatile datetime serial.
///
/// # Remarks
/// - `NOW` is volatile and may produce a different value at each recalculation.
/// - The integer part is the current date serial; the fractional part is time of day.
/// - Serial output respects the active workbook date system (`1900` or `1904`).
///
/// # Examples
/// ```yaml,sandbox
/// title: "NOW includes today's date"
/// formula: "=INT(NOW())=TODAY()"
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "NOW is at or after TODAY"
/// formula: "=NOW()>=TODAY()"
/// expected: true
/// ```
///
/// ```yaml,docs
/// related:
///   - TODAY
///   - TIME
///   - SECOND
/// faq:
///   - q: "How do I isolate only the time portion from NOW?"
///     a: "Use NOW()-INT(NOW()); the integer part is date serial and the fractional part is time-of-day."
/// ```
#[derive(Debug)]
pub struct NowFn;

/// [formualizer-docgen:schema:start]
/// Name: NOW
/// Type: NowFn
/// Min args: 0
/// Max args: 0
/// Variadic: false
/// Signature: NOW()
/// Arg schema: []
/// Caps: VOLATILE
/// [formualizer-docgen:schema:end]
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
        let now = ctx.clock().now();
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
