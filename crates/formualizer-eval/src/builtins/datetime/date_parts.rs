//! Date and time component extraction functions

use super::serial::{date_to_serial, datetime_to_serial, serial_to_date, serial_to_datetime};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use chrono::{Datelike, NaiveDate, Timelike};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
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
        LiteralValue::Date(d) => Ok(date_to_serial(&d)),
        LiteralValue::DateTime(dt) => Ok(datetime_to_serial(&dt)),
        LiteralValue::Empty => Ok(0.0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()
            .with_message("Date/time functions expect numeric or text-numeric serials")),
    }
}

fn coerce_to_date(arg: &ArgumentHandle) -> Result<NaiveDate, ExcelError> {
    let serial = coerce_to_serial(arg)?;
    serial_to_date(serial)
}

fn days_in_year(year: i32) -> f64 {
    if NaiveDate::from_ymd_opt(year, 2, 29).is_some() {
        366.0
    } else {
        365.0
    }
}

fn is_last_day_of_month(d: NaiveDate) -> bool {
    d.succ_opt().is_none_or(|next| next.month() != d.month())
}

fn next_month(year: i32, month: u32) -> (i32, u32) {
    if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    }
}

fn days_360_between(start: NaiveDate, end: NaiveDate, european: bool) -> i64 {
    let sy = start.year();
    let sm = start.month();
    let mut sd = start.day();

    let mut ey = end.year();
    let mut em = end.month();
    let mut ed = end.day();

    if european {
        if sd == 31 {
            sd = 30;
        }
        if ed == 31 {
            ed = 30;
        }
    } else {
        if sd == 31 || is_last_day_of_month(start) {
            sd = 30;
        }

        if ed == 31 || is_last_day_of_month(end) {
            if sd < 30 {
                let (ny, nm) = next_month(ey, em);
                ey = ny;
                em = nm;
                ed = 1;
            } else {
                ed = 30;
            }
        }
    }

    360 * i64::from(ey - sy)
        + 30 * i64::from(em as i32 - sm as i32)
        + i64::from(ed as i32 - sd as i32)
}

/// Returns the number of whole days between two date serial values.
///
/// # Remarks
/// - Result is `end_date - start_date`; negative results are allowed.
/// - Fractional serial inputs are truncated to their date portion.
/// - Serials are interpreted with the Excel 1900 mapping (including the 1900 leap-year bug behavior).
/// - This function currently does not switch interpretation based on workbook `1900`/`1904` mode.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Positive day difference"
/// formula: "=DAYS(45366, 45323)"
/// expected: 43
/// ```
///
/// ```yaml,sandbox
/// title: "Negative day difference"
/// formula: "=DAYS(45323, 45366)"
/// expected: -43
/// ```
#[derive(Debug)]
pub struct DaysFn;

/// [formualizer-docgen:schema:start]
/// Name: DAYS
/// Type: DaysFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: DAYS(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for DaysFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "DAYS"
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
        let end = coerce_to_date(&args[0])?;
        let start = coerce_to_date(&args[1])?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (end - start).num_days() as f64,
        )))
    }
}

/// Returns the day count between two dates using a 30/360 convention.
///
/// # Remarks
/// - `method` omitted or `FALSE` uses U.S. (NASD) rules; `TRUE` uses the European 30E/360 method.
/// - Inputs are coerced to dates by truncating serials to integer days.
/// - Serials are interpreted with the Excel 1900 date mapping, not a workbook-specific date system.
///
/// # Examples
/// ```yaml,sandbox
/// title: "U.S. 30/360 method"
/// formula: "=DAYS360(40574, 40602)"
/// expected: 30
/// ```
///
/// ```yaml,sandbox
/// title: "European 30E/360 method"
/// formula: "=DAYS360(40574, 40602, TRUE)"
/// expected: 28
/// ```
#[derive(Debug)]
pub struct Days360Fn;

/// [formualizer-docgen:schema:start]
/// Name: DAYS360
/// Type: Days360Fn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: DAYS360(arg1: number@scalar, arg2: number@scalar, arg3...: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for Days360Fn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "DAYS360"
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
                ArgSchema::any(),
            ]
        });
        &SCHEMA[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let start = coerce_to_date(&args[0])?;
        let end = coerce_to_date(&args[1])?;

        let european = if args.len() >= 3 {
            match args[2].value()?.into_literal() {
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                LiteralValue::Empty => false,
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Value),
                    )));
                }
            }
        } else {
            false
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            days_360_between(start, end, european) as f64,
        )))
    }
}

/// Returns the fraction of a year between two dates for a selected day-count basis.
///
/// # Remarks
/// - Supported `basis` values: `0` (US 30/360), `1` (actual/actual), `2` (actual/360), `3` (actual/365), `4` (European 30/360).
/// - If `start_date > end_date`, the result is negative.
/// - Invalid `basis` values return `#NUM!`.
/// - Serial dates are interpreted with the Excel 1900 mapping rather than workbook `1900`/`1904` context.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Actual/360 convention"
/// formula: "=YEARFRAC(44197, 44378, 2)"
/// expected: 0.5027777778
/// ```
///
/// ```yaml,sandbox
/// title: "Actual/365 convention"
/// formula: "=YEARFRAC(44197, 44378, 3)"
/// expected: 0.4958904110
/// ```
#[derive(Debug)]
pub struct YearFracFn;

/// [formualizer-docgen:schema:start]
/// Name: YEARFRAC
/// Type: YearFracFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: YEARFRAC(arg1: number@scalar, arg2: number@scalar, arg3...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for YearFracFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "YEARFRAC"
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
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let start = coerce_to_date(&args[0])?;
        let end = coerce_to_date(&args[1])?;

        let basis = if args.len() >= 3 {
            match args[2].value()?.into_literal() {
                LiteralValue::Number(n) => n.trunc() as i64,
                LiteralValue::Int(i) => i,
                LiteralValue::Boolean(b) => {
                    if b {
                        1
                    } else {
                        0
                    }
                }
                LiteralValue::Empty => 0,
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Value),
                    )));
                }
            }
        } else {
            0
        };

        if !(0..=4).contains(&basis) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Num),
            )));
        }

        if start == end {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
        }

        let (s, e, sign) = if start <= end {
            (start, end, 1.0)
        } else {
            (end, start, -1.0)
        };

        let actual_days = (e - s).num_days() as f64;
        let frac = match basis {
            0 => days_360_between(s, e, false) as f64 / 360.0,
            1 => {
                if s.year() == e.year() {
                    actual_days / days_in_year(s.year())
                } else {
                    let start_year_end = NaiveDate::from_ymd_opt(s.year() + 1, 1, 1).unwrap();
                    let end_year_start = NaiveDate::from_ymd_opt(e.year(), 1, 1).unwrap();

                    let mut out = (start_year_end - s).num_days() as f64 / days_in_year(s.year());
                    for year in (s.year() + 1)..e.year() {
                        out += 1.0;
                    }
                    out + (e - end_year_start).num_days() as f64 / days_in_year(e.year())
                }
            }
            2 => actual_days / 360.0,
            3 => actual_days / 365.0,
            4 => days_360_between(s, e, true) as f64 / 360.0,
            _ => unreachable!(),
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            sign * frac,
        )))
    }
}

/// Returns the ISO 8601 week number (`1` to `53`) for a date serial.
///
/// # Remarks
/// - Weeks start on Monday and week 1 is the week containing the first Thursday of the year.
/// - Input serials are truncated to whole dates before evaluation.
/// - Serials are read using the Excel 1900 date mapping.
///
/// # Examples
/// ```yaml,sandbox
/// title: "ISO week at year start"
/// formula: "=ISOWEEKNUM(45292)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "ISO week crossing year boundary"
/// formula: "=ISOWEEKNUM(42370)"
/// expected: 53
/// ```
#[derive(Debug)]
pub struct IsoWeekNumFn;

/// [formualizer-docgen:schema:start]
/// Name: ISOWEEKNUM
/// Type: IsoWeekNumFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISOWEEKNUM(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsoWeekNumFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "ISOWEEKNUM"
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
        let d = coerce_to_date(&args[0])?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            d.iso_week().week() as i64,
        )))
    }
}

/// Extracts the calendar year from a date serial.
///
/// # Remarks
/// - Fractional time is ignored; only the integer date portion is used.
/// - Input serials are interpreted with Excel 1900 date semantics.
/// - Results are Gregorian calendar years.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Extract year from date serial"
/// formula: "=YEAR(44927)"
/// expected: 2023
/// ```
///
/// ```yaml,sandbox
/// title: "Extract year from datetime serial"
/// formula: "=YEAR(45351.75)"
/// expected: 2024
/// ```
#[derive(Debug)]
pub struct YearFn;

/// [formualizer-docgen:schema:start]
/// Name: YEAR
/// Type: YearFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: YEAR(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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

/// Extracts the month number (`1` to `12`) from a date serial.
///
/// # Remarks
/// - Fractional time is ignored; only the date portion contributes.
/// - Serials are interpreted with Excel 1900 date semantics.
/// - The result always uses January=`1` through December=`12`.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Extract month from January date"
/// formula: "=MONTH(44927)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Extract month from leap-day serial"
/// formula: "=MONTH(45351)"
/// expected: 2
/// ```
#[derive(Debug)]
pub struct MonthFn;

/// [formualizer-docgen:schema:start]
/// Name: MONTH
/// Type: MonthFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: MONTH(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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

/// Extracts the day-of-month (`1` to `31`) from a date serial.
///
/// # Remarks
/// - Fractional time is ignored; only the integer serial portion is used.
/// - Serials are interpreted with Excel 1900 date semantics.
/// - Output is the day within the month, not day-of-year.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Extract day from first-of-month"
/// formula: "=DAY(44927)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Extract day from leap date"
/// formula: "=DAY(45351)"
/// expected: 29
/// ```
#[derive(Debug)]
pub struct DayFn;

/// [formualizer-docgen:schema:start]
/// Name: DAY
/// Type: DayFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: DAY(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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

/// Extracts the hour component (`0` to `23`) from a time or datetime serial.
///
/// # Remarks
/// - For values `>= 1`, only the fractional time part is used.
/// - For values `< 1`, the value is treated directly as a time fraction.
/// - Date-system choice does not affect hour extraction because only the fractional part is used.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Extract hour from noon"
/// formula: "=HOUR(0.5)"
/// expected: 12
/// ```
///
/// ```yaml,sandbox
/// title: "Extract hour from datetime serial"
/// formula: "=HOUR(45351.75)"
/// expected: 18
/// ```
#[derive(Debug)]
pub struct HourFn;

/// [formualizer-docgen:schema:start]
/// Name: HOUR
/// Type: HourFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: HOUR(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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

/// Extracts the minute component (`0` to `59`) from a time or datetime serial.
///
/// # Remarks
/// - The integer date portion is ignored for minute extraction.
/// - Conversion uses Excel 1900 serial interpretation for the date portion when present.
/// - Time is derived from the fractional serial component.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Extract minute from 15:30:45"
/// formula: "=MINUTE(0.6463541667)"
/// expected: 30
/// ```
///
/// ```yaml,sandbox
/// title: "Extract minute from exact noon"
/// formula: "=MINUTE(0.5)"
/// expected: 0
/// ```
#[derive(Debug)]
pub struct MinuteFn;

/// [formualizer-docgen:schema:start]
/// Name: MINUTE
/// Type: MinuteFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: MINUTE(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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

/// Extracts the second component (`0` to `59`) from a time or datetime serial.
///
/// # Remarks
/// - The integer date portion is ignored for second extraction.
/// - Conversion uses Excel 1900 serial interpretation when resolving datetime values.
/// - Time is computed from the serial fraction and rounded to whole seconds.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Extract second from 15:30:45"
/// formula: "=SECOND(0.6463541667)"
/// expected: 45
/// ```
///
/// ```yaml,sandbox
/// title: "Extract second from exact noon"
/// formula: "=SECOND(0.5)"
/// expected: 0
/// ```
#[derive(Debug)]
pub struct SecondFn;

/// [formualizer-docgen:schema:start]
/// Name: SECOND
/// Type: SecondFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: SECOND(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
    crate::function_registry::register_function(Arc::new(DaysFn));
    crate::function_registry::register_function(Arc::new(Days360Fn));
    crate::function_registry::register_function(Arc::new(YearFracFn));
    crate::function_registry::register_function(Arc::new(IsoWeekNumFn));
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

    #[test]
    fn test_year_accepts_date_and_datetime_literals() {
        let wb = TestWorkbook::new().with_function(Arc::new(YearFn));
        let ctx = wb.interpreter();
        let year_fn = ctx.context.get_function("", "YEAR").unwrap();

        let date = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let date_ast = lit(LiteralValue::Date(date));
        let from_date = year_fn
            .dispatch(
                &[ArgumentHandle::new(&date_ast, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(from_date, LiteralValue::Int(2024));

        let dt = date.and_hms_opt(13, 45, 0).unwrap();
        let dt_ast = lit(LiteralValue::DateTime(dt));
        let from_dt = year_fn
            .dispatch(
                &[ArgumentHandle::new(&dt_ast, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(from_dt, LiteralValue::Int(2024));
    }

    #[test]
    fn test_days_and_days360() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(DaysFn))
            .with_function(Arc::new(Days360Fn));
        let ctx = wb.interpreter();

        let start = chrono::NaiveDate::from_ymd_opt(2021, 2, 1).unwrap();
        let end = chrono::NaiveDate::from_ymd_opt(2021, 3, 15).unwrap();
        let start_ast = lit(LiteralValue::Date(start));
        let end_ast = lit(LiteralValue::Date(end));

        let days_fn = ctx.context.get_function("", "DAYS").unwrap();
        let days = days_fn
            .dispatch(
                &[
                    ArgumentHandle::new(&end_ast, &ctx),
                    ArgumentHandle::new(&start_ast, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(days, LiteralValue::Number(42.0));

        let d360_fn = ctx.context.get_function("", "DAYS360").unwrap();
        let s2 = lit(LiteralValue::Date(
            chrono::NaiveDate::from_ymd_opt(2011, 1, 31).unwrap(),
        ));
        let e2 = lit(LiteralValue::Date(
            chrono::NaiveDate::from_ymd_opt(2011, 2, 28).unwrap(),
        ));
        let us = d360_fn
            .dispatch(
                &[
                    ArgumentHandle::new(&s2, &ctx),
                    ArgumentHandle::new(&e2, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        let eu_flag = lit(LiteralValue::Boolean(true));
        let eu = d360_fn
            .dispatch(
                &[
                    ArgumentHandle::new(&s2, &ctx),
                    ArgumentHandle::new(&e2, &ctx),
                    ArgumentHandle::new(&eu_flag, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(us, LiteralValue::Number(30.0));
        assert_eq!(eu, LiteralValue::Number(28.0));
    }

    #[test]
    fn test_yearfrac_and_isoweeknum() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(YearFracFn))
            .with_function(Arc::new(IsoWeekNumFn));
        let ctx = wb.interpreter();

        let start = lit(LiteralValue::Date(
            chrono::NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        ));
        let end = lit(LiteralValue::Date(
            chrono::NaiveDate::from_ymd_opt(2021, 7, 1).unwrap(),
        ));
        let basis2 = lit(LiteralValue::Int(2));

        let yearfrac_fn = ctx.context.get_function("", "YEARFRAC").unwrap();
        let out = yearfrac_fn
            .dispatch(
                &[
                    ArgumentHandle::new(&start, &ctx),
                    ArgumentHandle::new(&end, &ctx),
                    ArgumentHandle::new(&basis2, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();

        match out {
            LiteralValue::Number(v) => assert!((v - (181.0 / 360.0)).abs() < 1e-12),
            other => panic!("expected numeric YEARFRAC, got {other:?}"),
        }

        let iso_fn = ctx.context.get_function("", "ISOWEEKNUM").unwrap();
        let d = lit(LiteralValue::Date(
            chrono::NaiveDate::from_ymd_opt(2016, 1, 1).unwrap(),
        ));
        let iso = iso_fn
            .dispatch(
                &[ArgumentHandle::new(&d, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(iso, LiteralValue::Int(53));
    }
}
