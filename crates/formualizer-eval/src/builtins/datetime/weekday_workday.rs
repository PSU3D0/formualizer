//! WEEKDAY, WEEKNUM, DATEDIF, NETWORKDAYS, WORKDAY functions

use super::serial::{date_to_serial, serial_to_date};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use chrono::{Datelike, NaiveDate, Weekday};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;
use std::collections::HashSet;

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
///
/// ```yaml,docs
/// related:
///   - WEEKNUM
///   - ISOWEEKNUM
///   - WORKDAY
/// faq:
///   - q: "Why do I get #NUM! for some return_type values?"
///     a: "WEEKDAY only accepts specific Excel return_type codes (1, 2, 3, 11-17); other codes return #NUM!."
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
        let d = serial_int % 7;

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
///
/// ```yaml,docs
/// related:
///   - WEEKDAY
///   - ISOWEEKNUM
///   - DATE
/// faq:
///   - q: "What is special about return_type 21 in WEEKNUM?"
///     a: "return_type=21 switches to ISO week numbering, matching ISOWEEKNUM behavior."
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

        // Serial 0 ("January 0, 1900") is before the first week of any year
        if serial_int == 0 {
            return Ok(CalcValue::Scalar(LiteralValue::Int(0)));
        }

        if return_type == 21 {
            // ISO week number: computed from serial-based weekday
            // serial % 7: 0=Sat, 1=Sun, 2=Mon, ..., 6=Fri
            let d = serial_int % 7;
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
        let jan1_d = jan1_serial % 7;

        // Offset: how many days from week_starts to Jan 1
        let offset = (jan1_d - week_starts_d + 7) % 7;

        // Day of year (1-based)
        let day_of_year = serial_int - jan1_serial + 1;

        // Week number: Jan 1 is always in week 1
        let week = (day_of_year + offset - 1) / 7 + 1;

        Ok(CalcValue::Scalar(LiteralValue::Int(week)))
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
///
/// ```yaml,docs
/// related:
///   - DAYS
///   - YEARFRAC
///   - DATE
/// faq:
///   - q: "How are unit strings interpreted in DATEDIF?"
///     a: "Unit text is case-insensitive, but only Y, M, D, MD, YM, and YD are supported; other units return #NUM!."
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

/// Coerce a single `LiteralValue` into a date serial for holiday collection.
///
/// Mirrors the coercion rules in `coerce_to_serial` but operates on an
/// already-evaluated literal rather than an `ArgumentHandle`.  Callers are
/// expected to filter out `Empty` values before invoking this function.
fn literal_to_serial(v: &LiteralValue) -> Result<f64, ExcelError> {
    match v {
        LiteralValue::Number(f) => Ok(*f),
        LiteralValue::Int(i) => Ok(*i as f64),
        LiteralValue::Date(d) => Ok(date_to_serial(d)),
        LiteralValue::DateTime(dt) => Ok(date_to_serial(&dt.date())),
        LiteralValue::Text(s) => s
            .parse::<f64>()
            .map_err(|_| ExcelError::new_value().with_message("Not a valid number")),
        LiteralValue::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
        LiteralValue::Error(e) => Err(e.clone()),
        _ => Err(ExcelError::new_value()),
    }
}

/// Collect the optional holidays argument into a `HashSet<NaiveDate>`.
///
/// The argument may be a cell range, an inline array literal, or a single
/// scalar value.  Each element is coerced to a date serial via the same
/// rules as the start/end date arguments.  Errors in individual holiday
/// values propagate immediately (matching Excel behavior).  Empty cells
/// and duplicate dates are silently ignored.
fn collect_holidays(
    args: &[ArgumentHandle<'_, '_>],
    idx: usize,
) -> Result<HashSet<NaiveDate>, ExcelError> {
    if args.len() <= idx {
        return Ok(HashSet::new());
    }

    let arg = &args[idx];
    let mut holidays = HashSet::new();

    for v in arg.lazy_values_owned()? {
        // `lazy_values_owned` may yield a `LiteralValue::Array` when the
        // argument evaluates to an array value (for example, from a
        // pre-evaluated scalar that contains an array). Flatten it so
        // each element is processed individually.
        match v {
            LiteralValue::Empty => continue,
            LiteralValue::Array(rows) => {
                for row in &rows {
                    for cell in row {
                        if matches!(cell, LiteralValue::Empty) {
                            continue;
                        }
                        let serial = literal_to_serial(cell)?;
                        holidays.insert(serial_to_date(serial)?);
                    }
                }
            }
            other => {
                let serial = literal_to_serial(&other)?;
                holidays.insert(serial_to_date(serial)?);
            }
        }
    }

    Ok(holidays)
}

/// Returns the number of weekday business days between two dates, inclusive.
///
/// # Remarks
/// - Weekends are fixed to Saturday and Sunday.
/// - If `start_date > end_date`, the result is negative.
/// - The optional `holidays` argument accepts a range, inline array, or single serial.
///   Dates falling on holidays are excluded from the count in addition to weekends.
/// - Holidays that fall on weekends have no additional effect.
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
/// title: "Single holiday excluded"
/// formula: "=NETWORKDAYS(45292, 45299, 45293)"
/// expected: 5
/// ```
///
/// ```yaml,docs
/// related:
///   - WORKDAY
///   - WEEKDAY
///   - DAYS
/// faq:
///   - q: "Are custom holidays excluded in NETWORKDAYS?"
///     a: "Yes. The third argument accepts a range, inline array, or single date serial. Holiday dates that fall on business days are excluded from the count."
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

        let holidays = collect_holidays(args, 2)?;

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
/// - The optional `holidays` argument accepts a range, inline array, or single serial.
///   Dates falling on holidays are skipped in addition to weekends.
/// - Holidays that fall on weekends have no additional effect.
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
/// title: "Holiday pushes result forward"
/// formula: "=WORKDAY(45292, 5, 45293)"
/// expected: 45300
/// ```
///
/// ```yaml,docs
/// related:
///   - NETWORKDAYS
///   - WEEKDAY
///   - TODAY
/// faq:
///   - q: "Does WORKDAY include the start date when days=0?"
///     a: "Yes. With zero offset, WORKDAY returns the start date serial unchanged; nonzero offsets skip weekend days while stepping."
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
        let holidays = collect_holidays(args, 2)?;

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

    // ── NETWORKDAYS holiday tests ──────────────────────────────────────

    #[test]
    fn networkdays_no_holidays() {
        // Jan 1 (Mon) to Jan 8 (Mon) 2024 → 6 business days
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0)); // 2024-01-01
        let end = lit(LiteralValue::Number(45299.0)); // 2024-01-08
        let f = ctx.context.get_function("", "NETWORKDAYS").unwrap();
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&end, &ctx),
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(6)
        );
    }

    #[test]
    fn networkdays_single_holiday() {
        // Jan 1 to Jan 8, with Jan 2 (Tue) as holiday → 5 business days
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0)); // 2024-01-01
        let end = lit(LiteralValue::Number(45299.0)); // 2024-01-08
        let hol = lit(LiteralValue::Number(45293.0)); // 2024-01-02
        let f = ctx.context.get_function("", "NETWORKDAYS").unwrap();
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&end, &ctx),
                    ArgumentHandle::new(&hol, &ctx),
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(5)
        );
    }

    #[test]
    fn networkdays_array_holidays() {
        // Jan 1 to Jan 8, with Jan 2 (Tue) and Jan 3 (Wed) as holidays → 4
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0));
        let end = lit(LiteralValue::Number(45299.0));
        let hols = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Number(45293.0), // Jan 2
            LiteralValue::Number(45294.0), // Jan 3
        ]]));
        let f = ctx.context.get_function("", "NETWORKDAYS").unwrap();
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&end, &ctx),
                    ArgumentHandle::new(&hols, &ctx),
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(4)
        );
    }

    #[test]
    fn networkdays_holiday_on_weekend_no_effect() {
        // Jan 1 to Jan 8, with Jan 6 (Sat) as holiday → still 6
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0));
        let end = lit(LiteralValue::Number(45299.0));
        let hol = lit(LiteralValue::Number(45297.0)); // 2024-01-06 Saturday
        let f = ctx.context.get_function("", "NETWORKDAYS").unwrap();
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&end, &ctx),
                    ArgumentHandle::new(&hol, &ctx),
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(6)
        );
    }

    #[test]
    fn networkdays_holiday_on_start_date() {
        // Jan 1 to Jan 8, with Jan 1 (Mon) as holiday → 5 (start is inclusive but excluded)
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0));
        let end = lit(LiteralValue::Number(45299.0));
        let hol = lit(LiteralValue::Number(45292.0)); // Jan 1 itself
        let f = ctx.context.get_function("", "NETWORKDAYS").unwrap();
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&end, &ctx),
                    ArgumentHandle::new(&hol, &ctx),
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(5)
        );
    }

    #[test]
    fn networkdays_native_date_holidays() {
        // Holidays passed as LiteralValue::Date (as from an xlsx import)
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0)); // Jan 1
        let end = lit(LiteralValue::Number(45299.0)); // Jan 8
        let hol = lit(LiteralValue::Date(
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        ));
        let f = ctx.context.get_function("", "NETWORKDAYS").unwrap();
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&end, &ctx),
                    ArgumentHandle::new(&hol, &ctx),
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(5)
        );
    }

    #[test]
    fn networkdays_reversed_with_holidays() {
        // end < start → negative, with holiday excluded
        // Jan 8 to Jan 1 with Jan 2 holiday → -5
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45299.0)); // Jan 8
        let end = lit(LiteralValue::Number(45292.0)); // Jan 1
        let hol = lit(LiteralValue::Number(45293.0)); // Jan 2
        let f = ctx.context.get_function("", "NETWORKDAYS").unwrap();
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&end, &ctx),
                    ArgumentHandle::new(&hol, &ctx),
                ],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(-5)
        );
    }

    // ── WORKDAY holiday tests ──────────────────────────────────────────

    #[test]
    fn workday_no_holidays() {
        // 5 workdays from Jan 1 (Mon) → Jan 8 (Mon), serial 45299
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(WorkdayFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0));
        let days = lit(LiteralValue::Number(5.0));
        let f = ctx.context.get_function("", "WORKDAY").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&days, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Number(45299.0));
    }

    #[test]
    fn workday_single_holiday() {
        // 5 workdays from Jan 1, with Jan 2 (Tue) as holiday → Jan 9 (Tue), serial 45300
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(WorkdayFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0));
        let days = lit(LiteralValue::Number(5.0));
        let hol = lit(LiteralValue::Number(45293.0)); // Jan 2
        let f = ctx.context.get_function("", "WORKDAY").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&days, &ctx),
                    ArgumentHandle::new(&hol, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Number(45300.0));
    }

    #[test]
    fn workday_array_holidays() {
        // 5 workdays from Jan 1, with Jan 2 + Jan 3 as holidays → Jan 10 (Wed), serial 45301
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(WorkdayFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0));
        let days = lit(LiteralValue::Number(5.0));
        let hols = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Number(45293.0),
            LiteralValue::Number(45294.0),
        ]]));
        let f = ctx.context.get_function("", "WORKDAY").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&days, &ctx),
                    ArgumentHandle::new(&hols, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Number(45301.0));
    }

    #[test]
    fn workday_negative_with_holiday() {
        // -3 workdays from Jan 8 (Mon), with Jan 4 (Thu) as holiday → Jan 2 (Tue), serial 45293
        // Stepping back: Jan 5(Fri)=1, Jan 4(Thu)=holiday skip, Jan 3(Wed)=2, Jan 2(Tue)=3
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(WorkdayFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45299.0)); // Jan 8
        let days = lit(LiteralValue::Number(-3.0));
        let hol = lit(LiteralValue::Number(45295.0)); // Jan 4
        let f = ctx.context.get_function("", "WORKDAY").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&days, &ctx),
                    ArgumentHandle::new(&hol, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Number(45293.0));
    }

    #[test]
    fn workday_holiday_on_weekend_no_effect() {
        // 5 workdays from Jan 1, with Jan 6 (Sat) as holiday → still Jan 8
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(WorkdayFn));
        let ctx = interp(&wb);
        let start = lit(LiteralValue::Number(45292.0));
        let days = lit(LiteralValue::Number(5.0));
        let hol = lit(LiteralValue::Number(45297.0)); // Jan 6 Saturday
        let f = ctx.context.get_function("", "WORKDAY").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&days, &ctx),
                    ArgumentHandle::new(&hol, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Number(45299.0));
    }
}
