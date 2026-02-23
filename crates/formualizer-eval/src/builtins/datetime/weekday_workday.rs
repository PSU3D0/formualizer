//! WEEKDAY, WEEKNUM, DATEDIF, NETWORKDAYS, WORKDAY functions

use super::serial::{date_to_serial, serial_to_date};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use chrono::{Datelike, NaiveDate, Weekday};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

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

/// Returns the day-of-week index for a date serial with configurable numbering.
///
/// # Remarks
/// - Default `return_type` is `1` (`Sunday=1` through `Saturday=7`).
/// - Supported `return_type` values are `1`, `2`, `3`, `11`-`17`; unsupported values return `#NUM!`.
/// - Input serials are interpreted with Excel 1900 date mapping, including its historical leap-year quirk.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Default numbering (Sunday-first)"
/// formula: "=WEEKDAY(45292)"
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Monday-first numbering"
/// formula: "=WEEKDAY(45292, 2)"
/// expected: 1
/// ```
#[derive(Debug)]
pub struct WeekdayFn;
/// [formualizer-docgen:schema:start]
/// Name: WEEKDAY
/// Type: WeekdayFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: WEEKDAY(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
        let return_type = if args.len() > 1 {
            coerce_to_int(&args[1])?
        } else {
            1
        };

        let date = serial_to_date(serial)?;
        let weekday = date.weekday();

        // Convert chrono weekday (Mon=0..Sun=6) to Excel format
        let result = match return_type {
            1 => match weekday {
                Weekday::Sun => 1,
                Weekday::Mon => 2,
                Weekday::Tue => 3,
                Weekday::Wed => 4,
                Weekday::Thu => 5,
                Weekday::Fri => 6,
                Weekday::Sat => 7,
            },
            2 => match weekday {
                Weekday::Mon => 1,
                Weekday::Tue => 2,
                Weekday::Wed => 3,
                Weekday::Thu => 4,
                Weekday::Fri => 5,
                Weekday::Sat => 6,
                Weekday::Sun => 7,
            },
            3 => match weekday {
                Weekday::Mon => 0,
                Weekday::Tue => 1,
                Weekday::Wed => 2,
                Weekday::Thu => 3,
                Weekday::Fri => 4,
                Weekday::Sat => 5,
                Weekday::Sun => 6,
            },
            11 => match weekday {
                // Mon=1..Sun=7
                Weekday::Mon => 1,
                Weekday::Tue => 2,
                Weekday::Wed => 3,
                Weekday::Thu => 4,
                Weekday::Fri => 5,
                Weekday::Sat => 6,
                Weekday::Sun => 7,
            },
            12 => match weekday {
                // Tue=1..Mon=7
                Weekday::Tue => 1,
                Weekday::Wed => 2,
                Weekday::Thu => 3,
                Weekday::Fri => 4,
                Weekday::Sat => 5,
                Weekday::Sun => 6,
                Weekday::Mon => 7,
            },
            13 => match weekday {
                // Wed=1..Tue=7
                Weekday::Wed => 1,
                Weekday::Thu => 2,
                Weekday::Fri => 3,
                Weekday::Sat => 4,
                Weekday::Sun => 5,
                Weekday::Mon => 6,
                Weekday::Tue => 7,
            },
            14 => match weekday {
                // Thu=1..Wed=7
                Weekday::Thu => 1,
                Weekday::Fri => 2,
                Weekday::Sat => 3,
                Weekday::Sun => 4,
                Weekday::Mon => 5,
                Weekday::Tue => 6,
                Weekday::Wed => 7,
            },
            15 => match weekday {
                // Fri=1..Thu=7
                Weekday::Fri => 1,
                Weekday::Sat => 2,
                Weekday::Sun => 3,
                Weekday::Mon => 4,
                Weekday::Tue => 5,
                Weekday::Wed => 6,
                Weekday::Thu => 7,
            },
            16 => match weekday {
                // Sat=1..Fri=7
                Weekday::Sat => 1,
                Weekday::Sun => 2,
                Weekday::Mon => 3,
                Weekday::Tue => 4,
                Weekday::Wed => 5,
                Weekday::Thu => 6,
                Weekday::Fri => 7,
            },
            17 => match weekday {
                // Sun=1..Sat=7
                Weekday::Sun => 1,
                Weekday::Mon => 2,
                Weekday::Tue => 3,
                Weekday::Wed => 4,
                Weekday::Thu => 5,
                Weekday::Fri => 6,
                Weekday::Sat => 7,
            },
            _ => {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }
        };

        Ok(CalcValue::Scalar(LiteralValue::Int(result)))
    }
}

/// Returns the week number of the year for a date serial.
///
/// # Remarks
/// - Default `return_type` is `1` (week starts on Sunday).
/// - Supported `return_type` values are `1`, `2`, `11`-`17`, and `21` (ISO week numbering).
/// - Unsupported `return_type` values return `#NUM!`.
/// - Input serials are interpreted using Excel 1900 date mapping rather than workbook `1904` interpretation.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Default week numbering"
/// formula: "=WEEKNUM(45292)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "ISO week numbering"
/// formula: "=WEEKNUM(42370, 21)"
/// expected: 53
/// ```
#[derive(Debug)]
pub struct WeeknumFn;
/// [formualizer-docgen:schema:start]
/// Name: WEEKNUM
/// Type: WeeknumFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: WEEKNUM(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
        let return_type = if args.len() > 1 {
            coerce_to_int(&args[1])?
        } else {
            1
        };

        let date = serial_to_date(serial)?;

        // Determine first day of week
        let week_starts = match return_type {
            1 | 17 => Weekday::Sun,
            2 | 11 => Weekday::Mon,
            12 => Weekday::Tue,
            13 => Weekday::Wed,
            14 => Weekday::Thu,
            15 => Weekday::Fri,
            16 => Weekday::Sat,
            21 => {
                // ISO week number (special case)
                return Ok(CalcValue::Scalar(LiteralValue::Int(
                    date.iso_week().week() as i64
                )));
            }
            _ => {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }
        };

        // Calculate week number based on when week starts
        let jan1 = NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap();
        let jan1_weekday = jan1.weekday();

        // Days from week start day to Jan 1
        let days_to_week_start = |wd: Weekday| -> i64 {
            let target = week_starts.num_days_from_sunday() as i64;
            let current = wd.num_days_from_sunday() as i64;
            (current - target + 7) % 7
        };

        let jan1_offset = days_to_week_start(jan1_weekday);
        let day_of_year = date.ordinal() as i64;

        // Week 1 starts on the first occurrence of week_starts day, or Jan 1 if it is that day
        let week_num = if jan1_offset == 0 {
            (day_of_year - 1) / 7 + 1
        } else {
            (day_of_year + jan1_offset - 1) / 7 + 1
        };

        Ok(CalcValue::Scalar(LiteralValue::Int(week_num)))
    }
}

/// Returns the difference between two dates in a requested unit.
///
/// # Remarks
/// - Supported units are `"Y"`, `"M"`, `"D"`, `"MD"`, `"YM"`, and `"YD"`.
/// - If `start_date > end_date`, the function returns `#NUM!`.
/// - Unit matching is case-insensitive.
/// - `"YD"` uses a Feb-29 normalization strategy that can differ slightly from Excel in edge cases.
/// - Input serials are interpreted with Excel 1900 date mapping.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Difference in days"
/// formula: '=DATEDIF(44197, 44378, "D")'
/// expected: 181
/// ```
///
/// ```yaml,sandbox
/// title: "Complete months difference"
/// formula: '=DATEDIF(44197, 44378, "M")'
/// expected: 6
/// ```
#[derive(Debug)]
pub struct DatedifFn;
/// [formualizer-docgen:schema:start]
/// Name: DATEDIF
/// Type: DatedifFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: DATEDIF(arg1: number@scalar, arg2: number@scalar, arg3: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
                // Days ignoring years
                // NOTE: Known edge case - uses .min(28) for Feb 29 handling which may differ from Excel
                let start_in_end_year = NaiveDate::from_ymd_opt(
                    end_date.year(),
                    start_date.month(),
                    start_date.day().min(28), // Handle Feb 29 -> Feb 28
                );
                match start_in_end_year {
                    Some(d) if d <= end_date => (end_date - d).num_days(),
                    _ => {
                        // Start date would be after end date in same year, use previous year
                        let start_prev_year = NaiveDate::from_ymd_opt(
                            end_date.year() - 1,
                            start_date.month(),
                            start_date.day().min(28),
                        )
                        .unwrap();
                        (end_date - start_prev_year).num_days()
                    }
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

/// Returns the number of weekday business days between two dates, inclusive.
///
/// # Remarks
/// - Weekends are fixed to Saturday and Sunday.
/// - If `start_date > end_date`, the result is negative.
/// - The optional `holidays` argument is currently accepted but ignored; holiday exclusions are not yet supported.
/// - Input serials are interpreted with Excel 1900 date mapping.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Count weekdays in a range"
/// formula: "=NETWORKDAYS(45292, 45299)"
/// expected: 6
/// ```
///
/// ```yaml,sandbox
/// title: "Holiday argument currently has no effect"
/// formula: "=NETWORKDAYS(45292, 45299, 45293)"
/// expected: 6
/// ```
#[derive(Debug)]
pub struct NetworkdaysFn;
/// [formualizer-docgen:schema:start]
/// Name: NETWORKDAYS
/// Type: NetworkdaysFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: NETWORKDAYS(arg1: number@scalar, arg2: number@scalar, arg3...: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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

/// Returns the date serial that is a given number of weekdays from a start date.
///
/// # Remarks
/// - Positive `days` moves forward; negative `days` moves backward.
/// - Weekends are fixed to Saturday and Sunday.
/// - The optional `holidays` argument is currently accepted but ignored; holiday exclusions are not yet supported.
/// - Input and output serials use Excel 1900 date mapping.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Move forward by five workdays"
/// formula: "=WORKDAY(45292, 5)"
/// expected: 45299
/// ```
///
/// ```yaml,sandbox
/// title: "Holiday argument currently has no effect"
/// formula: "=WORKDAY(45292, 5, 45293)"
/// expected: 45299
/// ```
#[derive(Debug)]
pub struct WorkdayFn;
/// [formualizer-docgen:schema:start]
/// Name: WORKDAY
/// Type: WorkdayFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: WORKDAY(arg1: number@scalar, arg2: number@scalar, arg3...: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
        // Holidays parameter is currently accepted but ignored.
        let holidays: Vec<NaiveDate> = Vec::new();

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
