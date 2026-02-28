//! WEEKDAY, WEEKNUM, DATEDIF, NETWORKDAYS, WORKDAY functions

use super::serial::{date_to_serial, serial_to_date};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use chrono::{Datelike, NaiveDate, Weekday};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/// Day of year in a standard 365-day (non-leap) year.
/// Feb 29 dates are clamped to Feb 28 (day 59).
fn non_leap_day_of_year(month: u32, day: u32) -> i64 {
    const CUM: [i64; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    const DAYS: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let capped = day.min(DAYS[(month - 1) as usize]);
    CUM[(month - 1) as usize] + capped as i64
}

fn coerce_to_serial(arg: &ArgumentHandle) -> Result<f64, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Number(f) => Ok(f),
        LiteralValue::Int(i) => Ok(i as f64),
        LiteralValue::Date(d) => Ok(date_to_serial(&d)),
        LiteralValue::DateTime(dt) => Ok(date_to_serial(&dt.date())),
        LiteralValue::Text(s) => s
            .parse::<f64>()
            .map_err(|_| ExcelError::new_value().with_message("Not a valid number")),
        LiteralValue::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()),
    }
}

fn coerce_to_int(arg: &ArgumentHandle) -> Result<i64, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Number(f) => Ok(f.trunc() as i64),
        LiteralValue::Int(i) => Ok(i),
        LiteralValue::Boolean(b) => Ok(if b { 1 } else { 0 }),
        LiteralValue::Empty => Ok(0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()),
    }
}

/// WEEKDAY(serial_number, [return_type]) - Returns the day of the week
/// return_type:
///   1 (default): 1 (Sunday) to 7 (Saturday)
///   2: 1 (Monday) to 7 (Sunday)
///   3: 0 (Monday) to 6 (Sunday)
///   11-17: Various configurations
#[derive(Debug)]
pub struct WeekdayFn;
impl Function for WeekdayFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "WEEKDAY"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;
        let serial_int = serial.trunc() as i64;
        if serial_int < 0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }
        let return_type = if args.len() > 1 {
            coerce_to_int(&args[1])?
        } else {
            1
        };

        // Compute weekday directly from serial number (not chrono) to correctly
        // handle Excel's phantom Feb 29. serial % 7: 0=Sat, 1=Sun, 2=Mon, ..., 6=Fri
        let d = (serial_int % 7) as i64;

        // Map return_type to the d-value of its starting day and whether 0-based
        let (start_d, zero_based) = match return_type {
            1 | 17 => (1i64, false), // Sun=1..Sat=7
            2 | 11 => (2, false),    // Mon=1..Sun=7
            3 => (2, true),          // Mon=0..Sun=6
            12 => (3, false),        // Tue=1..Mon=7
            13 => (4, false),        // Wed=1..Tue=7
            14 => (5, false),        // Thu=1..Wed=7
            15 => (6, false),        // Fri=1..Thu=7
            16 => (0, false),        // Sat=1..Fri=7
            _ => {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }
        };

        let result = if zero_based {
            (d - start_d + 7) % 7
        } else {
            (d - start_d + 7) % 7 + 1
        };

        Ok(CalcValue::Scalar(LiteralValue::Int(result)))
    }
}

/// WEEKNUM(serial_number, [return_type]) - Returns the week number of the year
#[derive(Debug)]
pub struct WeeknumFn;
impl Function for WeeknumFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "WEEKNUM"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let serial = coerce_to_serial(&args[0])?;
        let serial_int = serial.trunc() as i64;
        if serial_int < 0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Serial 0 always returns week 0
        if serial_int == 0 {
            return Ok(CalcValue::Scalar(LiteralValue::Int(0)));
        }

        let return_type = if args.len() > 1 {
            coerce_to_int(&args[1])?
        } else {
            1
        };

        if return_type == 21 {
            // ISO week number: computed from serial-based weekday
            // serial % 7: 0=Sat, 1=Sun, 2=Mon, ..., 6=Fri
            let d = (serial_int % 7) as i64;
            // ISO weekday: Mon=1, ..., Sun=7
            let iso_wd = if d < 2 { d + 6 } else { d - 1 };

            // Thursday of this ISO week
            let thu_serial = serial_int - iso_wd + 4;

            if thu_serial < 1 {
                // Falls in last week of previous year (only for first days of 1900)
                return Ok(CalcValue::Scalar(LiteralValue::Int(52)));
            }

            // Get year of the Thursday
            let thu_date = serial_to_date(thu_serial as f64)?;
            let thu_year = thu_date.year();

            // Serial for Jan 1 of that year
            let jan1 = NaiveDate::from_ymd_opt(thu_year, 1, 1).unwrap();
            let jan1_serial = date_to_serial(&jan1) as i64;

            let week = (thu_serial - jan1_serial) / 7 + 1;
            return Ok(CalcValue::Scalar(LiteralValue::Int(week)));
        }

        // Non-ISO week number using serial-based weekday for Jan 1
        // Starting weekday as d-value: 0=Sat, 1=Sun, 2=Mon, ..., 6=Fri
        let week_starts_d: i64 = match return_type {
            1 | 17 => 1, // Sunday
            2 | 11 => 2, // Monday
            12 => 3,     // Tuesday
            13 => 4,     // Wednesday
            14 => 5,     // Thursday
            15 => 6,     // Friday
            16 => 0,     // Saturday
            _ => {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }
        };

        // Get the year for this serial
        let date = serial_to_date(serial)?;
        let year = date.year();

        // Serial for Jan 1 of the year
        let jan1 = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
        let jan1_serial = date_to_serial(&jan1) as i64;

        // Jan 1's weekday (d-value from serial)
        let jan1_d = (jan1_serial % 7) as i64;

        // Offset: how many days from week_starts to Jan 1
        let offset = (jan1_d - week_starts_d + 7) % 7;

        // Day of year (1-based)
        let day_of_year = serial_int - jan1_serial + 1;

        // Week number: Jan 1 is always in week 1
        let week = (day_of_year + offset - 1) / 7 + 1;

        Ok(CalcValue::Scalar(LiteralValue::Int(week)))
    }
}

/// DATEDIF(start_date, end_date, unit) - Calculates the difference between two dates
/// unit: "Y" (years), "M" (months), "D" (days), "MD", "YM", "YD"
#[derive(Debug)]
pub struct DatedifFn;
impl Function for DatedifFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DATEDIF"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::any(),
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let start_serial = coerce_to_serial(&args[0])?;
        let end_serial = coerce_to_serial(&args[1])?;

        let unit = match args[2].value()?.into_literal() {
            LiteralValue::Text(s) => s.to_uppercase(),
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            _ => {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        };

        if start_serial > end_serial {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let start_date = serial_to_date(start_serial)?;
        let end_date = serial_to_date(end_serial)?;

        let result = match unit.as_str() {
            "Y" => {
                // Complete years
                let mut years = end_date.year() - start_date.year();
                if (end_date.month(), end_date.day()) < (start_date.month(), start_date.day()) {
                    years -= 1;
                }
                years as i64
            }
            "M" => {
                // Complete months
                let mut months = (end_date.year() - start_date.year()) * 12
                    + (end_date.month() as i32 - start_date.month() as i32);
                if end_date.day() < start_date.day() {
                    months -= 1;
                }
                months as i64
            }
            "D" => {
                // Days
                (end_date - start_date).num_days()
            }
            "MD" => {
                // Days ignoring months and years
                let mut days = end_date.day() as i64 - start_date.day() as i64;
                if days < 0 {
                    // Get days in the previous month
                    let prev_month = if end_date.month() == 1 {
                        NaiveDate::from_ymd_opt(end_date.year() - 1, 12, 1)
                    } else {
                        NaiveDate::from_ymd_opt(end_date.year(), end_date.month() - 1, 1)
                    }
                    .unwrap();
                    let days_in_prev_month = (NaiveDate::from_ymd_opt(
                        if prev_month.month() == 12 {
                            prev_month.year() + 1
                        } else {
                            prev_month.year()
                        },
                        if prev_month.month() == 12 {
                            1
                        } else {
                            prev_month.month() + 1
                        },
                        1,
                    )
                    .unwrap()
                        - prev_month)
                        .num_days();
                    days += days_in_prev_month;
                }
                days
            }
            "YM" => {
                // Months ignoring years
                let mut months = end_date.month() as i64 - start_date.month() as i64;
                if end_date.day() < start_date.day() {
                    months -= 1;
                }
                if months < 0 {
                    months += 12;
                }
                months
            }
            "YD" => {
                // Days ignoring years: use day-of-year in a non-leap context
                // to match Excel/LibreOffice behavior (consistent 365-day year)
                let start_doy = non_leap_day_of_year(start_date.month(), start_date.day());
                let end_doy = non_leap_day_of_year(end_date.month(), end_date.day());
                if end_doy >= start_doy {
                    end_doy - start_doy
                } else {
                    365 - start_doy + end_doy
                }
            }
            _ => {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }
        };

        Ok(CalcValue::Scalar(LiteralValue::Int(result)))
    }
}

/// Helper: check if a date is a weekend (Saturday or Sunday)
fn is_weekend(date: &NaiveDate) -> bool {
    matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

/// NETWORKDAYS(start_date, end_date, [holidays]) - Returns working days between two dates
///
/// NOTE: The holidays parameter is currently accepted but not implemented.
/// Holiday values passed to this function will be silently ignored.
#[derive(Debug)]
pub struct NetworkdaysFn;
impl Function for NetworkdaysFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NETWORKDAYS"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::any(), // holidays (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let start_serial = coerce_to_serial(&args[0])?;
        let end_serial = coerce_to_serial(&args[1])?;

        let start_date = serial_to_date(start_serial)?;
        let end_date = serial_to_date(end_serial)?;

        // Collect holidays if provided
        // TODO: Implement holiday array support
        let holidays: Vec<NaiveDate> = if args.len() > 2 {
            // For now, skip holiday handling (would need array support)
            vec![]
        } else {
            vec![]
        };

        let (start, end, sign) = if start_date <= end_date {
            (start_date, end_date, 1i64)
        } else {
            (end_date, start_date, -1i64)
        };

        let mut count = 0i64;
        let mut current = start;
        while current <= end {
            if !is_weekend(&current) && !holidays.contains(&current) {
                count += 1;
            }
            current = current.succ_opt().unwrap_or(current);
        }

        Ok(CalcValue::Scalar(LiteralValue::Int(count * sign)))
    }
}

/// WORKDAY(start_date, days, [holidays]) - Returns a date that is a specified number of working days away
///
/// NOTE: The holidays parameter is currently accepted but not implemented.
/// Holiday values passed to this function will be silently ignored.
#[derive(Debug)]
pub struct WorkdayFn;
impl Function for WorkdayFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "WORKDAY"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::any(), // holidays (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let start_serial = coerce_to_serial(&args[0])?;
        let days = coerce_to_int(&args[1])?;

        let start_date = serial_to_date(start_serial)?;

        // Collect holidays if provided
        // TODO: Implement holiday array support
        let holidays: Vec<NaiveDate> = if args.len() > 2 { vec![] } else { vec![] };

        let mut current = start_date;
        let mut remaining = days.abs();
        let direction: i64 = if days >= 0 { 1 } else { -1 };

        while remaining > 0 {
            current = if direction > 0 {
                current.succ_opt().ok_or_else(ExcelError::new_num)?
            } else {
                current.pred_opt().ok_or_else(ExcelError::new_num)?
            };

            if !is_weekend(&current) && !holidays.contains(&current) {
                remaining -= 1;
            }
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(date_to_serial(
            &current,
        ))))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(WeekdayFn));
    crate::function_registry::register_function(Arc::new(WeeknumFn));
    crate::function_registry::register_function(Arc::new(DatedifFn));
    crate::function_registry::register_function(Arc::new(NetworkdaysFn));
    crate::function_registry::register_function(Arc::new(WorkdayFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn weekday_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(WeekdayFn));
        let ctx = interp(&wb);
        // Jan 1, 2024 is a Monday
        // Serial for 2024-01-01: date_to_serial gives us the value
        let serial = date_to_serial(&NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        let n = lit(LiteralValue::Number(serial));
        let f = ctx.context.get_function("", "WEEKDAY").unwrap();
        // Default return_type=1: Monday=2
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(2)
        );
    }

    #[test]
    fn datedif_years() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(DatedifFn));
        let ctx = interp(&wb);
        let start = date_to_serial(&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        let end = date_to_serial(&NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        let s = lit(LiteralValue::Number(start));
        let e = lit(LiteralValue::Number(end));
        let unit = lit(LiteralValue::Text("Y".to_string()));
        let f = ctx.context.get_function("", "DATEDIF").unwrap();
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&s, &ctx),
                    ArgumentHandle::new(&e, &ctx),
                    ArgumentHandle::new(&unit, &ctx)
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(4)
        );
    }
}
