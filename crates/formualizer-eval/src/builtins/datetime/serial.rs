//! Compatibility date-serial helpers retained at the released eval path.
//!
//! The checked conversion APIs in `formualizer-common` are the canonical
//! production conversion path. The decode helpers here intentionally keep the
//! component-wise behavior released in 0.7.1, including its handling of
//! negative fractional serials and rounded near-midnight times. This is an
//! eval-local compatibility adapter, not a second canonical conversion API.

use chrono::{Days, NaiveDate, NaiveDateTime, NaiveTime};
use formualizer_common::{DateSystem, ExcelError};

const EXCEL_1900_EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1899, 12, 31).unwrap();
const EXCEL_1904_EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1904, 1, 1).unwrap();
const SECONDS_PER_DAY: f64 = 86_400.0;

fn checked_add_days(date: NaiveDate, days: i64) -> Result<NaiveDate, ExcelError> {
    if days >= 0 {
        date.checked_add_days(Days::new(days as u64))
    } else {
        date.checked_sub_days(Days::new(days.unsigned_abs()))
    }
    .ok_or_else(ExcelError::new_num)
}

fn legacy_time(serial: f64) -> Result<NaiveTime, ExcelError> {
    // This deliberately clamps components instead of carrying 24:00 into the
    // next date: 0.999995... was observable as 23:00:00 in 0.7.1.
    let total_seconds = (serial.fract() * SECONDS_PER_DAY).round() as u32;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    NaiveTime::from_hms_opt(hours.min(23), minutes.min(59), seconds.min(59))
        .ok_or_else(ExcelError::new_num)
}

fn legacy_excel_1900_date(serial: f64) -> Result<NaiveDate, ExcelError> {
    if serial.is_infinite() {
        return Err(ExcelError::new_num());
    }

    // Keep the old truncation-before-validation behavior: -0.25 and NaN both
    // truncate/cast to zero and therefore decode to the epoch date, while
    // negative whole serials remain #NUM.
    let serial_int = serial.trunc();
    if serial_int < 0.0 {
        return Err(ExcelError::new_num());
    }
    let serial_int = serial_int as i64;
    if serial_int == 60 {
        return Ok(NaiveDate::from_ymd_opt(1900, 2, 28).unwrap());
    }
    let offset = if serial_int < 60 {
        serial_int
    } else {
        serial_int - 1
    };
    checked_add_days(EXCEL_1900_EPOCH, offset)
}

/// Convert a serial to a representable Excel-1900 date.
pub fn serial_to_date(serial: f64) -> Result<NaiveDate, ExcelError> {
    legacy_excel_1900_date(serial)
}

/// Convert a date to an Excel-1900 serial.
pub fn date_to_serial(date: &NaiveDate) -> f64 {
    formualizer_common::date_to_serial_for(DateSystem::Excel1900, date)
}

/// Convert a date to a serial in the selected date system.
pub fn date_to_serial_for(system: DateSystem, date: &NaiveDate) -> f64 {
    formualizer_common::date_to_serial_for(system, date)
}

/// Convert a datetime to an Excel-1900 serial.
pub fn datetime_to_serial(datetime: &NaiveDateTime) -> f64 {
    formualizer_common::datetime_to_serial_for(DateSystem::Excel1900, datetime)
}

/// Convert a datetime to a serial in the selected date system.
pub fn datetime_to_serial_for(system: DateSystem, datetime: &NaiveDateTime) -> f64 {
    formualizer_common::datetime_to_serial_for(system, datetime)
}

/// Convert an Excel-1900 serial to a representable datetime.
///
/// This preserves the released component-clamping behavior. Unlike the
/// released implementation, infinities and date arithmetic overflow return
/// `#NUM!` rather than panicking.
pub fn serial_to_datetime(serial: f64) -> Result<NaiveDateTime, ExcelError> {
    let date = legacy_excel_1900_date(serial)?;
    Ok(NaiveDateTime::new(date, legacy_time(serial)?))
}

/// Convert a serial to a representable datetime in the selected date system.
///
/// The Excel-1900 and Excel-1904 branches retain the released eval helper's
/// component-wise decode behavior. Infinities and chrono date overflow are
/// hardened to typed `#NUM!` errors.
pub fn serial_to_datetime_for(
    system: DateSystem,
    serial: f64,
) -> Result<NaiveDateTime, ExcelError> {
    if serial.is_infinite() {
        return Err(ExcelError::new_num());
    }
    match system {
        DateSystem::Excel1900 => serial_to_datetime(serial),
        DateSystem::Excel1904 => {
            if serial.is_nan() {
                return Err(ExcelError::new_num());
            }
            let days = serial.trunc() as i64;
            let date = checked_add_days(EXCEL_1904_EPOCH, days)?;
            Ok(NaiveDateTime::new(date, legacy_time(serial)?))
        }
    }
}

/// Convert a time to a fractional day.
pub fn time_to_fraction(time: &NaiveTime) -> f64 {
    formualizer_common::time_to_fraction(time)
}

/// Create a date using the normalization behavior released in 0.7.1.
pub fn create_date_normalized(year: i32, month: i32, day: i32) -> Result<NaiveDate, ExcelError> {
    let total_months = (year * 12) + month - 1;
    let normalized_year = total_months / 12;
    let normalized_month = (total_months % 12) + 1;
    let first_of_month = NaiveDate::from_ymd_opt(normalized_year, normalized_month as u32, 1)
        .ok_or_else(ExcelError::new_num)?;
    first_of_month
        .checked_add_signed(chrono::TimeDelta::days((day - 1) as i64))
        .ok_or_else(ExcelError::new_num)
}
