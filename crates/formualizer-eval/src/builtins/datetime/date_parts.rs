//! Date and time component extraction functions

use super::serial::{serial_to_date, serial_to_datetime};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use chrono::{Datelike, Timelike};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

fn coerce_to_serial(arg: &ArgumentHandle) -> Result<f64, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Number(f) => Ok(f),
        LiteralValue::Int(i) => Ok(i as f64),
        LiteralValue::Text(s) => s.parse::<f64>().map_err(|_| {
            ExcelError::new_value().with_message("Date/time serial is not a valid number")
        }),
        LiteralValue::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()
            .with_message("Date/time functions expect numeric or text-numeric serials")),
    }
}

/// YEAR(serial_number) - Extracts year from date
#[derive(Debug)]
pub struct YearFn;

impl Function for YearFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "YEAR"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;
        let date = serial_to_date(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            date.year() as i64,
        )))
    }
}

/// MONTH(serial_number) - Extracts month from date
#[derive(Debug)]
pub struct MonthFn;

impl Function for MonthFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "MONTH"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;
        let date = serial_to_date(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            date.month() as i64,
        )))
    }
}

/// DAY(serial_number) - Extracts day from date
#[derive(Debug)]
pub struct DayFn;

impl Function for DayFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "DAY"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;
        let date = serial_to_date(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            date.day() as i64,
        )))
    }
}

/// HOUR(serial_number) - Extracts hour from time
#[derive(Debug)]
pub struct HourFn;

impl Function for HourFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "HOUR"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;

        // For time values < 1, we just use the fractional part
        let time_fraction = if serial < 1.0 { serial } else { serial.fract() };

        // Convert fraction to hours (24 hours = 1.0)
        let hours = (time_fraction * 24.0) as i64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(hours)))
    }
}

/// MINUTE(serial_number) - Extracts minute from time
#[derive(Debug)]
pub struct MinuteFn;

impl Function for MinuteFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "MINUTE"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;

        // Extract time component
        let datetime = serial_to_datetime(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            datetime.minute() as i64,
        )))
    }
}

/// SECOND(serial_number) - Extracts second from time
#[derive(Debug)]
pub struct SecondFn;

impl Function for SecondFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "SECOND"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static ONE: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;

        // Extract time component
        let datetime = serial_to_datetime(serial)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            datetime.second() as i64,
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(YearFn));
    crate::function_registry::register_function(Arc::new(MonthFn));
    crate::function_registry::register_function(Arc::new(DayFn));
    crate::function_registry::register_function(Arc::new(HourFn));
    crate::function_registry::register_function(Arc::new(MinuteFn));
    crate::function_registry::register_function(Arc::new(SecondFn));
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
    fn test_year_month_day() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(YearFn))
            .with_function(Arc::new(MonthFn))
            .with_function(Arc::new(DayFn));
        let ctx = wb.interpreter();

        // Test with a known date serial number
        // Serial 44927 = 2023-01-01
        let serial = lit(LiteralValue::Number(44927.0));

        let year_fn = ctx.context.get_function("", "YEAR").unwrap();
        let result = year_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(2023));

        let month_fn = ctx.context.get_function("", "MONTH").unwrap();
        let result = month_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(1));

        let day_fn = ctx.context.get_function("", "DAY").unwrap();
        let result = day_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(1));
    }

    #[test]
    fn test_hour_minute_second() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(HourFn))
            .with_function(Arc::new(MinuteFn))
            .with_function(Arc::new(SecondFn));
        let ctx = wb.interpreter();

        // Test with noon (0.5 = 12:00:00)
        let serial = lit(LiteralValue::Number(0.5));

        let hour_fn = ctx.context.get_function("", "HOUR").unwrap();
        let result = hour_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(12));

        let minute_fn = ctx.context.get_function("", "MINUTE").unwrap();
        let result = minute_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(0));

        let second_fn = ctx.context.get_function("", "SECOND").unwrap();
        let result = second_fn
            .dispatch(
                &[ArgumentHandle::new(&serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(0));

        // Test with 15:30:45 = 15.5/24 + 0.75/24/60 = 0.6463541667
        let time_serial = lit(LiteralValue::Number(0.6463541667));

        let hour_result = hour_fn
            .dispatch(
                &[ArgumentHandle::new(&time_serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(hour_result, LiteralValue::Int(15));

        let minute_result = minute_fn
            .dispatch(
                &[ArgumentHandle::new(&time_serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(minute_result, LiteralValue::Int(30));

        let second_result = second_fn
            .dispatch(
                &[ArgumentHandle::new(&time_serial, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(second_result, LiteralValue::Int(45));
    }
}
