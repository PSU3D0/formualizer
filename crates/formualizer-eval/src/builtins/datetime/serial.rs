//! Excel serial date system with 1900 leap year bug compatibility

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use formualizer_common::ExcelError;

use crate::engine::DateSystem;

// Excel's serial date system:
// Serial 1 = 1900-01-01
// Serial 60 = 1900-02-29 (doesn't exist, but Excel thinks it does - leap year bug)
// Serial 61 = 1900-03-01
// Implementation approach:
//   Base date = 1899-12-31 (Excel serial 1 => 1900-01-01 has a one-day diff from base)
//   Phantom day: Excel treats 1900-02-29 as serial 60 (non-existent). For serial->date we:
//     serial < 60:  date = base + serial days
//     serial == 60: date = 1900-02-28 (we do NOT invent an impossible date object)
//     serial > 60:  date = base + (serial - 1) days (skip phantom)
//   For date->serial we compute diff_days = date - base, then:
//     if date >= 1900-03-01 add 1 to account for phantom day
//     else use diff_days directly.
// This matches Excel's mapping: 59 => 1900-02-28, 60 => (displays 29) we surface 28, 61 => 1900-03-01.

const EXCEL_BASE_YEAR: i32 = 1899;
const EXCEL_BASE_MONTH: u32 = 12;
const EXCEL_BASE_DAY: u32 = 31;

/// Convert Excel serial number to date
/// Handles the 1900 leap year bug where Excel incorrectly treats 1900 as a leap year
pub fn serial_to_date(serial: f64) -> Result<NaiveDate, ExcelError> {
    let serial_int = serial.trunc();
    if serial_int < 0.0 {
        return Err(ExcelError::new_num());
    }
    let serial_int = serial_int as i64; // safe now

    // Handle phantom day (serial 60) explicitly
    if serial_int == 60 {
        return Ok(NaiveDate::from_ymd_opt(1900, 2, 28).unwrap());
    }

    let base = NaiveDate::from_ymd_opt(EXCEL_BASE_YEAR, EXCEL_BASE_MONTH, EXCEL_BASE_DAY)
        .ok_or_else(ExcelError::new_num)?;

    // serial < 60: offset = serial
    // serial > 60: offset = serial - 1 (skip phantom day)
    let offset = if serial_int < 60 {
        serial_int
    } else {
        serial_int - 1
    };

    base.checked_add_signed(chrono::TimeDelta::days(offset))
        .ok_or_else(ExcelError::new_num)
}

/// Convert date to Excel serial number
/// Handles the 1900 leap year bug
pub fn date_to_serial(date: &NaiveDate) -> f64 {
    let base = NaiveDate::from_ymd_opt(EXCEL_BASE_YEAR, EXCEL_BASE_MONTH, EXCEL_BASE_DAY).unwrap();
    let diff = (*date - base).num_days(); // 1900-01-01 => 1
    let serial = if *date >= NaiveDate::from_ymd_opt(1900, 3, 1).unwrap() {
        diff + 1 // account for phantom Feb 29
    } else {
        diff
    };
    serial as f64
}

/// Convert Excel serial number to datetime
/// The fractional part represents time of day
pub fn serial_to_datetime(serial: f64) -> Result<NaiveDateTime, ExcelError> {
    let date = serial_to_date(serial)?;
    let time_fraction = serial.fract();

    // Convert fraction to seconds (24 hours * 60 minutes * 60 seconds = 86400 seconds)
    let total_seconds = (time_fraction * 86400.0).round() as u32;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    let time = NaiveTime::from_hms_opt(hours.min(23), minutes.min(59), seconds.min(59))
        .ok_or_else(ExcelError::new_num)?;

    Ok(NaiveDateTime::new(date, time))
}

/// Convert datetime to Excel serial number
pub fn datetime_to_serial(datetime: &NaiveDateTime) -> f64 {
    let date_serial = date_to_serial(&datetime.date());
    let time_fraction = time_to_fraction(&datetime.time());
    date_serial + time_fraction
}

// ───────── Date-system aware variants (1900 vs 1904) ─────────

const EXCEL_1904_EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1904, 1, 1).unwrap();

/// Convert a date to Excel serial according to the provided date system.
pub fn date_to_serial_for(system: DateSystem, date: &NaiveDate) -> f64 {
    match system {
        DateSystem::Excel1900 => date_to_serial(date),
        DateSystem::Excel1904 => (*date - EXCEL_1904_EPOCH).num_days() as f64,
    }
}

/// Convert a datetime to Excel serial according to the provided date system.
pub fn datetime_to_serial_for(system: DateSystem, dt: &NaiveDateTime) -> f64 {
    match system {
        DateSystem::Excel1900 => datetime_to_serial(dt),
        DateSystem::Excel1904 => {
            let days = (dt.date() - EXCEL_1904_EPOCH).num_days() as f64;
            let frac = time_to_fraction(&dt.time());
            days + frac
        }
    }
}

/// Convert a serial to datetime according to the provided date system.
pub fn serial_to_datetime_for(
    system: DateSystem,
    serial: f64,
) -> Result<NaiveDateTime, ExcelError> {
    match system {
        DateSystem::Excel1900 => serial_to_datetime(serial),
        DateSystem::Excel1904 => {
            if serial.is_nan() || serial.is_infinite() {
                return Err(ExcelError::new_num());
            }
            let days = serial.trunc() as i64;
            let date = EXCEL_1904_EPOCH
                .checked_add_signed(chrono::TimeDelta::days(days))
                .ok_or_else(ExcelError::new_num)?;
            let time_fraction = serial.fract();
            let total_seconds = (time_fraction * 86400.0).round() as u32;
            let hours = total_seconds / 3600;
            let minutes = (total_seconds % 3600) / 60;
            let seconds = total_seconds % 60;
            let time = NaiveTime::from_hms_opt(hours.min(23), minutes.min(59), seconds.min(59))
                .ok_or_else(ExcelError::new_num)?;
            Ok(NaiveDateTime::new(date, time))
        }
    }
}

/// Convert time to fractional day (0.0 to 0.999...)
pub fn time_to_fraction(time: &NaiveTime) -> f64 {
    let total_seconds =
        time.hour() as f64 * 3600.0 + time.minute() as f64 * 60.0 + time.second() as f64;
    total_seconds / 86400.0
}

/// Create a date from year, month, day with Excel normalization
/// Excel normalizes out-of-range values (e.g., month 13 becomes next January)
pub fn create_date_normalized(year: i32, month: i32, day: i32) -> Result<NaiveDate, ExcelError> {
    // Normalize month and adjust year
    let total_months = (year * 12) + month - 1;
    let normalized_year = total_months / 12;
    let normalized_month = (total_months % 12) + 1;

    // Create a temporary date with day 1 to handle month boundaries
    let temp_date = NaiveDate::from_ymd_opt(normalized_year, normalized_month as u32, 1)
        .ok_or_else(ExcelError::new_num)?;

    // Add the days (minus 1 because we started at day 1)
    temp_date
        .checked_add_signed(chrono::TimeDelta::days((day - 1) as i64))
        .ok_or_else(ExcelError::new_num)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serial_to_date_basic() {
        // Serial 1 = 1900-01-01
        let date = serial_to_date(1.0).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(1900, 1, 1).unwrap());

        // Serial 2 = 1900-01-02
        let date = serial_to_date(2.0).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(1900, 1, 2).unwrap());
    }

    #[test]
    fn test_leap_year_bug() {
        // Serial 59 = 1900-02-28
        let date = serial_to_date(59.0).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(1900, 2, 28).unwrap());

        // Serial 60 = 1900-02-29 (doesn't exist in reality, but Excel treats it as Feb 28)
        let date = serial_to_date(60.0).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(1900, 2, 28).unwrap());

        // Serial 61 = 1900-03-01
        let date = serial_to_date(61.0).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(1900, 3, 1).unwrap());
    }

    #[test]
    fn test_date_to_serial() {
        // 1900-01-01 = Serial 1
        let date = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
        assert_eq!(date_to_serial(&date), 1.0);

        // 1900-02-28 = Serial 59
        let date = NaiveDate::from_ymd_opt(1900, 2, 28).unwrap();
        assert_eq!(date_to_serial(&date), 59.0);

        // 1900-03-01 = Serial 61 (accounting for leap year bug)
        let date = NaiveDate::from_ymd_opt(1900, 3, 1).unwrap();
        assert_eq!(date_to_serial(&date), 61.0);
    }

    #[test]
    fn test_time_fraction() {
        // Noon = 0.5
        let time = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        assert!((time_to_fraction(&time) - 0.5).abs() < 1e-10);

        // 6 AM = 0.25
        let time = NaiveTime::from_hms_opt(6, 0, 0).unwrap();
        assert!((time_to_fraction(&time) - 0.25).abs() < 1e-10);
    }

    #[test]
    fn test_date_normalization() {
        // Month 13 becomes next January
        let date = create_date_normalized(2024, 13, 5).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(2025, 1, 5).unwrap());

        // Negative month
        let date = create_date_normalized(2024, 0, 15).unwrap();
        assert_eq!(date, NaiveDate::from_ymd_opt(2023, 12, 15).unwrap());
    }
}
