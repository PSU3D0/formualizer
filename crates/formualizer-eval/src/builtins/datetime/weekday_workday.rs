//! WEEKDAY, WEEKNUM, DATEDIF, NETWORKDAYS, WORKDAY functions

use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use arrow_array::Array;
use chrono::{Datelike, NaiveDate, Weekday};
use formualizer_common::{
    DateSystem, ExcelError, LiteralValue, date_to_serial_for, try_serial_to_date_for,
};
use formualizer_macros::func_caps;

/// Number of days between the Excel 1900 and Excel 1904 epochs.
///
/// `1462 % 7 == 6`, so the two systems disagree on the weekday a given serial
/// denotes; weekday-dependent builtins are therefore not epoch-invariant.
const EXCEL_1904_EPOCH_OFFSET: i64 = 1462;

/// Excel weekday index for a whole-day serial: `0`=Sat, `1`=Sun, `2`=Mon, ... `6`=Fri.
///
/// The index is derived from the serial rather than from the calendar date so
/// that the Excel 1900 system stays aligned with Excel's phantom 1900-02-29.
/// Excel 1904 serials are rebased onto the 1900 epoch first.
fn weekday_index_from_serial(system: DateSystem, serial: i64) -> i64 {
    match system {
        DateSystem::Excel1900 => serial.rem_euclid(7),
        DateSystem::Excel1904 => (serial + EXCEL_1904_EPOCH_OFFSET).rem_euclid(7),
    }
}

/// Day of year in a standard 365-day (non-leap) year.
/// Feb 29 dates are clamped to Feb 28 (day 59).
fn non_leap_day_of_year(month: u32, day: u32) -> i64 {
    const CUM: [i64; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    const DAYS: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let capped = day.min(DAYS[(month - 1) as usize]);
    CUM[(month - 1) as usize] + capped as i64
}

fn coerce_to_serial(arg: &ArgumentHandle, system: DateSystem) -> Result<f64, ExcelError> {
    let v = arg.value()?.into_literal();
    if let LiteralValue::Error(e) = v {
        return Err(e);
    }
    crate::coercion::to_serial_lenient(&v, system).map_err(|_| ExcelError::new_value())
}

fn coerce_to_int(arg: &ArgumentHandle) -> Result<i64, ExcelError> {
    let v = arg.value()?.into_literal();
    if let LiteralValue::Error(e) = v {
        return Err(e);
    }
    crate::coercion::to_number_lenient(&v)
        .map(|f| f.trunc() as i64)
        .map_err(|_| ExcelError::new_value())
}

/// Returns the day-of-week index for a date serial with configurable numbering.
///
/// # Remarks
/// - Default `return_type` is `1` (`Sunday=1` through `Saturday=7`).
/// - Supported `return_type` values are `1`, `2`, `3`, `11`-`17`; unsupported values return `#NUM!`.
/// - Input serials are interpreted with the workbook's date system (Excel 1900 or Excel 1904).
///   Because the epochs are 1462 days apart (`1462 % 7 == 6`), the same serial denotes a different
///   weekday in each system.
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let serial = coerce_to_serial(&args[0], system)?;
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
        // handle Excel's phantom Feb 29: 0=Sat, 1=Sun, 2=Mon, ..., 6=Fri
        let d = weekday_index_from_serial(system, serial_int);

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
/// - Input serials are interpreted using the workbook's date system (Excel 1900 or Excel 1904);
///   week boundaries differ between the systems because their epochs are 1462 days apart.
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let serial = coerce_to_serial(&args[0], system)?;
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

        // Serial 0 in the 1900 system is "January 0, 1900", before the first week
        // of any year. In the 1904 system serial 0 is the real date 1904-01-01.
        if system == DateSystem::Excel1900 && serial_int == 0 {
            return Ok(CalcValue::Scalar(LiteralValue::Int(0)));
        }

        if return_type == 21 {
            // ISO week number: computed from serial-based weekday
            // 0=Sat, 1=Sun, 2=Mon, ..., 6=Fri
            let d = weekday_index_from_serial(system, serial_int);
            // ISO weekday: Mon=1, ..., Sun=7
            let iso_wd = if d < 2 { d + 6 } else { d - 1 };

            // Thursday of this ISO week
            let thu_serial = serial_int - iso_wd + 4;

            // The ISO week of the first days of the epoch year reaches back before
            // the first representable serial, so the answer is fixed per system:
            // 1899-12-31 falls in ISO week 52 of 1899, 1903-12-31 in week 53 of 1903.
            let (first_serial, week_before_epoch) = match system {
                DateSystem::Excel1900 => (1i64, 52i64),
                DateSystem::Excel1904 => (0i64, 53i64),
            };
            if thu_serial < first_serial {
                return Ok(CalcValue::Scalar(LiteralValue::Int(week_before_epoch)));
            }

            // Get year of the Thursday
            let thu_date = try_serial_to_date_for(system, thu_serial as f64)?;
            let thu_year = thu_date.year();

            // Serial for Jan 1 of that year
            let jan1 = NaiveDate::from_ymd_opt(thu_year, 1, 1).unwrap();
            let jan1_serial = date_to_serial_for(system, &jan1) as i64;

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
        let date = try_serial_to_date_for(system, serial)?;
        let year = date.year();

        // Serial for Jan 1 of the year
        let jan1 = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
        let jan1_serial = date_to_serial_for(system, &jan1) as i64;

        // Jan 1's weekday (d-value from serial)
        let jan1_d = weekday_index_from_serial(system, jan1_serial);

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
/// - Both endpoints are interpreted with the workbook's date system (Excel 1900 or Excel 1904).
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let start_serial = coerce_to_serial(&args[0], system)?;
        let end_serial = coerce_to_serial(&args[1], system)?;

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

        let start_date = try_serial_to_date_for(system, start_serial)?;
        let end_date = try_serial_to_date_for(system, end_serial)?;

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

/// Weekend mask: 7 bools indexed by chrono weekday (Mon=0 .. Sun=6).
/// `true` means the day is a non-working (weekend) day.
type WeekendMask = [bool; 7];

/// Default mask: Saturday and Sunday are weekends.
const DEFAULT_WEEKEND_MASK: WeekendMask = [false, false, false, false, false, true, true];

/// Parse the `weekend` argument for `.INTL` functions.
///
/// Accepts either:
/// - A **number** code (1-7 for two-day weekends, 11-17 for single-day weekends).
/// - A **7-character string** of `0`s and `1`s (`1` = weekend) starting from Monday.
///
/// Returns `None` if the value is invalid (caller should return `#NUM!`).
/// The special all-ones string (`"1111111"`) yields `#VALUE!` per Excel.
fn parse_weekend_mask(arg: &ArgumentHandle) -> Result<Option<WeekendMask>, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Number(f) => Ok(weekend_mask_from_code(f.trunc() as i64)),
        LiteralValue::Int(i) => Ok(weekend_mask_from_code(i)),
        LiteralValue::Boolean(b) => {
            // TRUE = 1, FALSE not valid (0 weekend days isn't a valid code)
            if b {
                Ok(weekend_mask_from_code(1))
            } else {
                Ok(None)
            }
        }
        LiteralValue::Text(s) => {
            if s == "1111111" {
                Err(ExcelError::new_value())
            } else {
                Ok(weekend_mask_from_string(&s))
            }
        }
        LiteralValue::Empty => Ok(Some(DEFAULT_WEEKEND_MASK)),
        LiteralValue::Error(e) => Err(e),
        _ => Ok(None),
    }
}

/// Map a numeric weekend code to a 7-element mask.
fn weekend_mask_from_code(code: i64) -> Option<WeekendMask> {
    // Indices: 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun
    match code {
        1 => Some([false, false, false, false, false, true, true]), // Sat, Sun
        2 => Some([true, false, false, false, false, false, true]), // Sun, Mon
        3 => Some([true, true, false, false, false, false, false]), // Mon, Tue
        4 => Some([false, true, true, false, false, false, false]), // Tue, Wed
        5 => Some([false, false, true, true, false, false, false]), // Wed, Thu
        6 => Some([false, false, false, true, true, false, false]), // Thu, Fri
        7 => Some([false, false, false, false, true, true, false]), // Fri, Sat
        11 => Some([false, false, false, false, false, false, true]), // Sun only
        12 => Some([true, false, false, false, false, false, false]), // Mon only
        13 => Some([false, true, false, false, false, false, false]), // Tue only
        14 => Some([false, false, true, false, false, false, false]), // Wed only
        15 => Some([false, false, false, true, false, false, false]), // Thu only
        16 => Some([false, false, false, false, true, false, false]), // Fri only
        17 => Some([false, false, false, false, false, true, false]), // Sat only
        _ => None,
    }
}

/// Parse a 7-character "0"/"1" string into a weekend mask.
/// Characters map to Mon..Sun. "1" = weekend, "0" = workday.
/// Returns `None` if the string is invalid or all-ones (no workdays).
fn weekend_mask_from_string(s: &str) -> Option<WeekendMask> {
    if s.len() != 7 {
        return None;
    }
    let mut mask = [false; 7];
    let mut all_weekend = true;
    for (i, ch) in s.chars().enumerate() {
        match ch {
            '1' => mask[i] = true,
            '0' => {
                mask[i] = false;
                all_weekend = false;
            }
            _ => return None,
        }
    }
    if all_weekend {
        return None; // Excel returns #VALUE! when all 7 days are weekends
    }
    Some(mask)
}

/// Check whether a date falls on a weekend day according to the given mask.
fn is_weekend_masked(date: &NaiveDate, mask: &WeekendMask) -> bool {
    let idx = date.weekday().num_days_from_monday() as usize; // Mon=0..Sun=6
    mask[idx]
}

/// Collect holiday dates from argument(s) starting at `arg_start`.
/// Handles scalars, inline arrays, and range references.
/// Silently skips non-numeric / empty cells (matching Excel behavior).
fn collect_holidays(
    args: &[ArgumentHandle],
    arg_start: usize,
    system: DateSystem,
) -> Vec<NaiveDate> {
    let mut holidays = Vec::new();
    for arg in args.iter().skip(arg_start) {
        match arg.value() {
            Ok(CalcValue::Scalar(lit)) => {
                collect_holidays_from_literal(&lit, &mut holidays, system)
            }
            Ok(CalcValue::Range(rv)) => {
                if let Ok(slices) = rv.numbers_slices().collect::<Result<Vec<_>, _>>() {
                    for (_row_start, _row_len, cols) in slices {
                        for col in cols {
                            let len = col.len();
                            let values = col.values();
                            for i in 0..len {
                                if !col.is_null(i)
                                    && let Ok(d) = try_serial_to_date_for(system, values[i])
                                {
                                    holidays.push(d);
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    holidays.sort_unstable();
    holidays.dedup();
    holidays
}

fn collect_holidays_from_literal(lit: &LiteralValue, out: &mut Vec<NaiveDate>, system: DateSystem) {
    match lit {
        LiteralValue::Array(rows) => {
            for row in rows {
                for cell in row {
                    collect_holidays_from_literal(cell, out, system);
                }
            }
        }
        _ => {
            if let Some(d) = literal_to_date(lit, system) {
                out.push(d);
            }
        }
    }
}

fn literal_to_date(lit: &LiteralValue, system: DateSystem) -> Option<NaiveDate> {
    match lit {
        LiteralValue::Number(f) => try_serial_to_date_for(system, *f).ok(),
        LiteralValue::Int(i) => try_serial_to_date_for(system, *i as f64).ok(),
        LiteralValue::Date(d) => Some(*d),
        LiteralValue::DateTime(dt) => Some(dt.date()),
        _ => None,
    }
}

/// Returns the number of weekday business days between two dates, inclusive.
///
/// # Remarks
/// - Weekends are fixed to Saturday and Sunday.
/// - If `start_date > end_date`, the result is negative.
/// - Optional `holidays` arguments are excluded from the count and may be provided as scalars,
///   inline arrays, or ranges.
/// - Serials are interpreted with the workbook's date system (Excel 1900 or Excel 1904); the
///   weekend days depend on the calendar dates the serials denote, which differ per system.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Count weekdays in a range"
/// formula: "=NETWORKDAYS(45292, 45299)"
/// expected: 6
/// ```
///
/// ```yaml,sandbox
/// title: "Holiday exclusions reduce the count"
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
///   - q: "Can I exclude custom holidays in NETWORKDAYS?"
///     a: "Yes. Additional arguments can provide holiday serials, arrays, or ranges to exclude from the weekday count."
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let start_serial = coerce_to_serial(&args[0], system)?;
        let end_serial = coerce_to_serial(&args[1], system)?;

        let start_date = try_serial_to_date_for(system, start_serial)?;
        let end_date = try_serial_to_date_for(system, end_serial)?;

        let holidays = collect_holidays(args, 2, system);

        let (start, end, sign) = if start_date <= end_date {
            (start_date, end_date, 1i64)
        } else {
            (end_date, start_date, -1i64)
        };

        let mut count = 0i64;
        let mut current = start;
        while current <= end {
            if !is_weekend(&current) && holidays.binary_search(&current).is_err() {
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
/// - Optional `holidays` arguments are excluded and may be provided as scalars, inline arrays,
///   or ranges.
/// - Input and output serials use the workbook's date system (Excel 1900 or Excel 1904).
///
/// # Examples
/// ```yaml,sandbox
/// title: "Move forward by five workdays"
/// formula: "=WORKDAY(45292, 5)"
/// expected: 45299
/// ```
///
/// ```yaml,sandbox
/// title: "Holiday exclusions push the target date out"
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let start_serial = coerce_to_serial(&args[0], system)?;
        let days = coerce_to_int(&args[1])?;

        let start_date = try_serial_to_date_for(system, start_serial)?;

        let holidays = collect_holidays(args, 2, system);

        let mut current = start_date;
        let mut remaining = days.abs();
        let direction: i64 = if days >= 0 { 1 } else { -1 };

        while remaining > 0 {
            current = if direction > 0 {
                current.succ_opt().ok_or_else(ExcelError::new_num)?
            } else {
                current.pred_opt().ok_or_else(ExcelError::new_num)?
            };

            if !is_weekend(&current) && holidays.binary_search(&current).is_err() {
                remaining -= 1;
            }
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(date_to_serial_for(
            system, &current,
        ))))
    }
}

/// Returns the number of working days between two dates with configurable weekends.
///
/// # Remarks
/// - The `weekend` argument can be a number code (1-7, 11-17) or a 7-character string
///   of `0`s and `1`s (Mon-Sun, `1` = weekend day). Default is `1` (Sat/Sun).
/// - The optional `holidays` argument accepts a range or array of date serials to exclude.
/// - If `start_date > end_date`, the result is negative.
/// - A weekend string of all `1`s (no workdays) returns `#VALUE!`.
///
/// # Examples
/// ```excel
/// =NETWORKDAYS.INTL(DATE(2024,1,1), DATE(2024,1,31))
/// ```
///
/// ```yaml,sandbox
/// title: "Default weekends (same as NETWORKDAYS)"
/// formula: "=NETWORKDAYS.INTL(DATE(2024,1,1), DATE(2024,1,31))"
/// expected: 23
/// ```
///
/// ```yaml,sandbox
/// title: "Friday-only weekend"
/// formula: "=NETWORKDAYS.INTL(DATE(2024,1,1), DATE(2024,1,7), 16)"
/// expected: 6
/// ```
///
/// ```yaml,sandbox
/// title: "Custom weekend string (Mon+Fri off)"
/// formula: "=NETWORKDAYS.INTL(DATE(2024,1,1), DATE(2024,1,7), \"1000100\")"
/// expected: 5
/// ```
///
/// ```yaml,docs
/// related:
///   - NETWORKDAYS
///   - WORKDAY.INTL
///   - WEEKDAY
/// faq:
///   - q: "What happens if the weekend string is all 1s?"
///     a: "NETWORKDAYS.INTL returns #VALUE! because there would be no workdays."
/// ```
#[derive(Debug)]
pub struct NetworkdaysIntlFn;
/// [formualizer-docgen:schema:start]
/// Name: NETWORKDAYS.INTL
/// Type: NetworkdaysIntlFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: NETWORKDAYS.INTL(arg1: number@scalar, arg2: number@scalar, arg3: any@scalar, arg4...: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg4{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NetworkdaysIntlFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NETWORKDAYS.INTL"
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
                ArgSchema::number_lenient_scalar(), // start_date
                ArgSchema::number_lenient_scalar(), // end_date
                ArgSchema::any(),                   // weekend (optional)
                ArgSchema::any(),                   // holidays (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let start_serial = coerce_to_serial(&args[0], system)?;
        let end_serial = coerce_to_serial(&args[1], system)?;

        let start_date = try_serial_to_date_for(system, start_serial)?;
        let end_date = try_serial_to_date_for(system, end_serial)?;

        let mask = if args.len() > 2 {
            match parse_weekend_mask(&args[2]) {
                Ok(Some(m)) => m,
                Ok(None) => {
                    return Ok(CalcValue::Scalar(
                        LiteralValue::Error(ExcelError::new_num()),
                    ));
                }
                Err(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            }
        } else {
            DEFAULT_WEEKEND_MASK
        };

        let holidays = collect_holidays(args, 3, system);

        let (start, end, sign) = if start_date <= end_date {
            (start_date, end_date, 1i64)
        } else {
            (end_date, start_date, -1i64)
        };

        let mut count = 0i64;
        let mut current = start;
        while current <= end {
            if !is_weekend_masked(&current, &mask) && holidays.binary_search(&current).is_err() {
                count += 1;
            }
            current = current.succ_opt().unwrap_or(current);
        }

        Ok(CalcValue::Scalar(LiteralValue::Int(count * sign)))
    }
}

/// Returns the date serial that is a given number of workdays from a start date,
/// with configurable weekends.
///
/// # Remarks
/// - Positive `days` moves forward; negative `days` moves backward.
/// - The `weekend` argument can be a number code (1-7, 11-17) or a 7-character string
///   of `0`s and `1`s (Mon-Sun, `1` = weekend day). Default is `1` (Sat/Sun).
/// - The optional `holidays` argument accepts a range or array of date serials to exclude.
/// - A weekend string of all `1`s (no workdays) returns `#VALUE!`.
///
/// # Examples
/// ```excel
/// =WORKDAY.INTL(DATE(2024,1,1), 5)
/// ```
///
/// ```yaml,sandbox
/// title: "Default weekends (same as WORKDAY)"
/// formula: "=WORKDAY.INTL(DATE(2024,1,1), 5)"
/// expected: 45306
/// ```
///
/// ```yaml,sandbox
/// title: "Sunday-only weekend"
/// formula: "=WORKDAY.INTL(DATE(2024,1,1), 5, 11)"
/// expected: 45302
/// ```
///
/// ```yaml,sandbox
/// title: "Custom weekend string"
/// formula: "=WORKDAY.INTL(DATE(2024,1,1), 5, \"0000011\")"
/// expected: 45299
/// ```
///
/// ```yaml,docs
/// related:
///   - WORKDAY
///   - NETWORKDAYS.INTL
///   - WEEKDAY
/// faq:
///   - q: "What is the difference between WORKDAY and WORKDAY.INTL?"
///     a: "WORKDAY.INTL adds a weekend parameter that lets you define which days are non-working, instead of always using Saturday/Sunday."
/// ```
#[derive(Debug)]
pub struct WorkdayIntlFn;
/// [formualizer-docgen:schema:start]
/// Name: WORKDAY.INTL
/// Type: WorkdayIntlFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: WORKDAY.INTL(arg1: number@scalar, arg2: number@scalar, arg3: any@scalar, arg4...: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg4{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for WorkdayIntlFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "WORKDAY.INTL"
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
                ArgSchema::number_lenient_scalar(), // start_date
                ArgSchema::number_lenient_scalar(), // days
                ArgSchema::any(),                   // weekend (optional)
                ArgSchema::any(),                   // holidays (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let system = ctx.date_system();
        let start_serial = coerce_to_serial(&args[0], system)?;
        let days = coerce_to_int(&args[1])?;

        let start_date = try_serial_to_date_for(system, start_serial)?;

        let mask = if args.len() > 2 {
            match parse_weekend_mask(&args[2]) {
                Ok(Some(m)) => m,
                Ok(None) => {
                    return Ok(CalcValue::Scalar(
                        LiteralValue::Error(ExcelError::new_num()),
                    ));
                }
                Err(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            }
        } else {
            DEFAULT_WEEKEND_MASK
        };

        let holidays = collect_holidays(args, 3, system);

        let mut current = start_date;
        let mut remaining = days.abs();
        let direction: i64 = if days >= 0 { 1 } else { -1 };

        while remaining > 0 {
            current = if direction > 0 {
                current.succ_opt().ok_or_else(ExcelError::new_num)?
            } else {
                current.pred_opt().ok_or_else(ExcelError::new_num)?
            };

            if !is_weekend_masked(&current, &mask) && holidays.binary_search(&current).is_err() {
                remaining -= 1;
            }
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(date_to_serial_for(
            system, &current,
        ))))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_builtin(Arc::new(WeekdayFn));
    crate::function_registry::register_builtin(Arc::new(WeeknumFn));
    crate::function_registry::register_builtin(Arc::new(DatedifFn));
    crate::function_registry::register_builtin(Arc::new(NetworkdaysFn));
    crate::function_registry::register_builtin(Arc::new(WorkdayFn));
    crate::function_registry::register_builtin(Arc::new(NetworkdaysIntlFn));
    crate::function_registry::register_builtin(Arc::new(WorkdayIntlFn));
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

    /// Serial for the default (Excel 1900) workbook date system.
    fn date_to_serial(date: &NaiveDate) -> f64 {
        date_to_serial_for(DateSystem::Excel1900, date)
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

    // ── Weekend mask helpers ──

    #[test]
    fn weekend_mask_from_code_default() {
        let m = weekend_mask_from_code(1).unwrap();
        // Sat(5) and Sun(6) should be true
        assert!(!m[0]); // Mon
        assert!(!m[4]); // Fri
        assert!(m[5]); // Sat
        assert!(m[6]); // Sun
    }

    #[test]
    fn weekend_mask_from_code_sunday_only() {
        let m = weekend_mask_from_code(11).unwrap();
        assert!(m[6]); // Sun
        for weekend in m.iter().take(6) {
            assert!(!weekend);
        }
    }

    #[test]
    fn weekend_mask_from_code_invalid() {
        assert!(weekend_mask_from_code(0).is_none());
        assert!(weekend_mask_from_code(8).is_none());
        assert!(weekend_mask_from_code(18).is_none());
    }

    #[test]
    fn weekend_mask_from_string_basic() {
        // Mon+Fri off
        let m = weekend_mask_from_string("1000100").unwrap();
        assert!(m[0]); // Mon
        assert!(!m[1]);
        assert!(m[4]); // Fri
        assert!(!m[5]);
    }

    #[test]
    fn weekend_mask_from_string_all_ones_invalid() {
        assert!(weekend_mask_from_string("1111111").is_none());
    }

    #[test]
    fn weekend_mask_from_string_wrong_length() {
        assert!(weekend_mask_from_string("000011").is_none());
        assert!(weekend_mask_from_string("00001100").is_none());
    }

    #[test]
    fn weekend_mask_from_string_bad_chars() {
        assert!(weekend_mask_from_string("000012X").is_none());
    }

    #[test]
    fn is_weekend_masked_basic() {
        let mask = weekend_mask_from_code(1).unwrap(); // Sat+Sun
        let mon = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(); // Monday
        let sat = NaiveDate::from_ymd_opt(2024, 1, 6).unwrap(); // Saturday
        let sun = NaiveDate::from_ymd_opt(2024, 1, 7).unwrap(); // Sunday
        assert!(!is_weekend_masked(&mon, &mask));
        assert!(is_weekend_masked(&sat, &mask));
        assert!(is_weekend_masked(&sun, &mask));
    }

    // ── NETWORKDAYS.INTL unit tests ──

    #[test]
    fn networkdays_intl_default_matches_networkdays() {
        // Jan 1-31 2024: NETWORKDAYS gives 23
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysIntlFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        )));
        let e = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 31).unwrap(),
        )));
        let f = ctx.context.get_function("", "NETWORKDAYS.INTL").unwrap();
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&s, &ctx), ArgumentHandle::new(&e, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(23));
    }

    #[test]
    fn networkdays_intl_sunday_only_weekend() {
        // Jan 1-7 2024 (Mon-Sun): code 11 = Sun only weekend => 6 workdays
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysIntlFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        )));
        let e = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 7).unwrap(),
        )));
        let wk = lit(LiteralValue::Int(11));
        let f = ctx.context.get_function("", "NETWORKDAYS.INTL").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&s, &ctx),
                    ArgumentHandle::new(&e, &ctx),
                    ArgumentHandle::new(&wk, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(6));
    }

    #[test]
    fn networkdays_intl_string_mask() {
        // Jan 1-7 2024: "0000011" = Sat+Sun off (same as default) => 5 workdays
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysIntlFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        )));
        let e = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 7).unwrap(),
        )));
        let wk = lit(LiteralValue::Text("0000011".to_string()));
        let f = ctx.context.get_function("", "NETWORKDAYS.INTL").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&s, &ctx),
                    ArgumentHandle::new(&e, &ctx),
                    ArgumentHandle::new(&wk, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(5));
    }

    #[test]
    fn networkdays_intl_invalid_code_returns_num_error() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysIntlFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        )));
        let e = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 7).unwrap(),
        )));
        let wk = lit(LiteralValue::Int(99));
        let f = ctx.context.get_function("", "NETWORKDAYS.INTL").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&s, &ctx),
                    ArgumentHandle::new(&e, &ctx),
                    ArgumentHandle::new(&wk, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(matches!(result, LiteralValue::Error(_)));
    }

    #[test]
    fn networkdays_intl_all_weekends_string_returns_value_error() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysIntlFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        )));
        let e = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 7).unwrap(),
        )));
        let wk = lit(LiteralValue::Text("1111111".to_string()));
        let f = ctx.context.get_function("", "NETWORKDAYS.INTL").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&s, &ctx),
                    ArgumentHandle::new(&e, &ctx),
                    ArgumentHandle::new(&wk, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        match result {
            LiteralValue::Error(err) => {
                assert_eq!(err.kind, formualizer_common::ExcelErrorKind::Value)
            }
            other => panic!("expected #VALUE! error, got {other:?}"),
        }
    }

    #[test]
    fn networkdays_collects_inline_array_holidays() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysIntlFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        )));
        let e = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 7).unwrap(),
        )));
        let wk = lit(LiteralValue::Int(1));
        let holidays = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Number(date_to_serial(
                &NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            )),
            LiteralValue::Number(date_to_serial(
                &NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            )),
        ]]));
        let f = ctx.context.get_function("", "NETWORKDAYS.INTL").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&s, &ctx),
                    ArgumentHandle::new(&e, &ctx),
                    ArgumentHandle::new(&wk, &ctx),
                    ArgumentHandle::new(&holidays, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(3));
    }

    // ── WORKDAY.INTL unit tests ──

    #[test]
    fn workday_intl_default_matches_workday() {
        // WORKDAY(2024-01-01, 10) = 45306
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(WorkdayIntlFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        )));
        let d = lit(LiteralValue::Int(10));
        let f = ctx.context.get_function("", "WORKDAY.INTL").unwrap();
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&s, &ctx), ArgumentHandle::new(&d, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        // serial for 2024-01-15 (Mon) = 45306
        assert_eq!(result, LiteralValue::Number(45306.0));
    }

    #[test]
    fn workday_intl_sunday_only() {
        // 2024-01-01 is Monday. code 11 = Sunday only weekend.
        // 5 workdays forward: Tue..Sat all work, so 5 days = Jan 6 (Sat)
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(WorkdayIntlFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        )));
        let d = lit(LiteralValue::Int(5));
        let wk = lit(LiteralValue::Int(11));
        let f = ctx.context.get_function("", "WORKDAY.INTL").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&s, &ctx),
                    ArgumentHandle::new(&d, &ctx),
                    ArgumentHandle::new(&wk, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        let expected = date_to_serial(&NaiveDate::from_ymd_opt(2024, 1, 6).unwrap());
        assert_eq!(result, LiteralValue::Number(expected));
    }

    #[test]
    fn workday_intl_backward() {
        // 2024-01-15 (Mon) minus 5 workdays with default weekends = 2024-01-08 (Mon)
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(WorkdayIntlFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        )));
        let d = lit(LiteralValue::Int(-5));
        let f = ctx.context.get_function("", "WORKDAY.INTL").unwrap();
        let result = f
            .dispatch(
                &[ArgumentHandle::new(&s, &ctx), ArgumentHandle::new(&d, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        let expected = date_to_serial(&NaiveDate::from_ymd_opt(2024, 1, 8).unwrap());
        assert_eq!(result, LiteralValue::Number(expected));
    }

    #[test]
    fn networkdays_collects_sorted_deduped_holidays() {
        // Jan 1-7 2024 (Mon-Sun) default weekends => 5 workdays
        // Holidays: Jan 2, Jan 2 (dup), Jan 3, and Jan 4 out of order => total 2 unique workdays left
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NetworkdaysFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        )));
        let e = lit(LiteralValue::Number(date_to_serial(
            &NaiveDate::from_ymd_opt(2024, 1, 7).unwrap(),
        )));
        let holidays = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Number(date_to_serial(
                &NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            ))], // Out of order
            vec![LiteralValue::Number(date_to_serial(
                &NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            ))],
            vec![LiteralValue::Number(date_to_serial(
                &NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            ))], // Duplicate
            vec![LiteralValue::Number(date_to_serial(
                &NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            ))],
        ]));
        let f = ctx.context.get_function("", "NETWORKDAYS").unwrap();
        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&s, &ctx),
                    ArgumentHandle::new(&e, &ctx),
                    ArgumentHandle::new(&holidays, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(2)); // Mon(1), Fri(5) are the only active workdays
    }

    fn eval_weekday_formula(system: crate::engine::DateSystem, formula: &str) -> LiteralValue {
        use crate::engine::{Engine, EvalConfig};
        use crate::interpreter::Interpreter;
        use formualizer_parse::parser::parse;

        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(WeekdayFn))
            .with_function(std::sync::Arc::new(WeeknumFn))
            .with_function(std::sync::Arc::new(WorkdayFn))
            .with_function(std::sync::Arc::new(NetworkdaysFn));
        let engine = Engine::new(wb, EvalConfig::default().with_date_system(system));
        let interpreter = Interpreter::new(&engine, "Sheet1");
        interpreter
            .evaluate_ast(&parse(formula).expect("formula should parse"))
            .expect("formula should evaluate")
            .into_literal()
    }

    /// The epochs are 1462 days apart and `1462 % 7 == 6`, so a serial denotes a
    /// different weekday in each system. The same calendar date must not.
    #[test]
    fn weekday_follows_workbook_date_system_1900_and_1904() {
        use formualizer_common::date_to_serial_for;

        // 2024-01-15 is a Monday => WEEKDAY default numbering is 2.
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(
            (date_to_serial_for(DateSystem::Excel1900, &date) as i64) % 7,
            (date_to_serial_for(DateSystem::Excel1904, &date) as i64 + 6) % 7,
            "the raw serial parities differ between systems"
        );

        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let serial = date_to_serial_for(system, &date);
            assert_eq!(
                eval_weekday_formula(system, &format!("=WEEKDAY({serial})")),
                LiteralValue::Int(2),
                "WEEKDAY({serial}) under {system:?}"
            );
        }
    }

    #[test]
    fn workday_follows_workbook_date_system_1900_and_1904() {
        use formualizer_common::date_to_serial_for;

        // Deliberately weekday-sensitive: ten workdays is exactly two calendar
        // weeks from *any* weekday, so it cannot distinguish the two epochs.
        // One workday from a Friday skips the weekend (+3 days); from any other
        // weekday it is +1. Misreading the epoch shifts the weekday by six days
        // (1462 % 7 == 6) and therefore changes the answer.
        let friday = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let monday = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();

        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let start_serial = date_to_serial_for(system, &friday);
            assert_eq!(
                eval_weekday_formula(system, &format!("=WORKDAY({start_serial},1)")),
                LiteralValue::Number(date_to_serial_for(system, &monday)),
                "WORKDAY over a weekend under {system:?}"
            );
        }
    }

    #[test]
    fn networkdays_follows_workbook_date_system_1900_and_1904() {
        use formualizer_common::date_to_serial_for;

        // A weekend-only span is weekday-sensitive: it contains zero working
        // days, but under a misread epoch the same serials denote weekdays.
        // (A whole-month span is a poor probe -- several start weekdays yield
        // the same 23.)
        let saturday = NaiveDate::from_ymd_opt(2024, 1, 6).unwrap();
        let sunday = NaiveDate::from_ymd_opt(2024, 1, 7).unwrap();

        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let s = date_to_serial_for(system, &saturday);
            let e = date_to_serial_for(system, &sunday);
            assert_eq!(
                eval_weekday_formula(system, &format!("=NETWORKDAYS({s},{e})")),
                LiteralValue::Int(0),
                "NETWORKDAYS across a weekend under {system:?}"
            );
        }

        // 2024-01-01 .. 2024-01-31 contains 23 Mon-Fri days.
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 31).unwrap();

        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let s = date_to_serial_for(system, &start);
            let e = date_to_serial_for(system, &end);
            assert_eq!(
                eval_weekday_formula(system, &format!("=NETWORKDAYS({s},{e})")),
                LiteralValue::Int(23),
                "NETWORKDAYS under {system:?}"
            );
        }
    }

    /// WEEKNUM's epoch-relative edge cases differ per system (serial 0 is a
    /// display-only date only in the 1900 system), but a real calendar date must
    /// land in the same week under both.
    #[test]
    fn weeknum_follows_workbook_date_system_1900_and_1904() {
        use formualizer_common::date_to_serial_for;

        // 2024-01-01 is a Monday, so 2024-01-15 is in the third Sunday-start week
        // and in ISO week 3 of 2024.
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let serial = date_to_serial_for(system, &date);
            assert_eq!(
                eval_weekday_formula(system, &format!("=WEEKNUM({serial})")),
                LiteralValue::Int(3),
                "WEEKNUM({serial}) under {system:?}"
            );
            assert_eq!(
                eval_weekday_formula(system, &format!("=WEEKNUM({serial},21)")),
                LiteralValue::Int(3),
                "WEEKNUM({serial},21) under {system:?}"
            );
        }

        // A week boundary is far more discriminating than a mid-week date:
        // 2024-01-07 is the Sunday that starts the second Sunday-based week.
        let boundary = NaiveDate::from_ymd_opt(2024, 1, 7).unwrap();
        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let serial = date_to_serial_for(system, &boundary);
            assert_eq!(
                eval_weekday_formula(system, &format!("=WEEKNUM({serial})")),
                LiteralValue::Int(2),
                "WEEKNUM({serial}) on a week boundary under {system:?}"
            );
        }

        // Serial 0 is the display-only "January 0, 1900" in the 1900 system, but the
        // real date 1904-01-01, which falls in ISO week 53 of 1903, in the 1904 system.
        assert_eq!(
            eval_weekday_formula(DateSystem::Excel1900, "=WEEKNUM(0,21)"),
            LiteralValue::Int(0)
        );
        assert_eq!(
            eval_weekday_formula(DateSystem::Excel1904, "=WEEKNUM(0,21)"),
            LiteralValue::Int(53)
        );
    }
}
