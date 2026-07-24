//! Excel serial date system with 1900 leap year bug compatibility

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
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
    validate_excel_serial(DateSystem::Excel1900, serial)?;
    let serial_int = serial.trunc() as i64;

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

fn normalized_serial_parts(
    system: DateSystem,
    serial: f64,
) -> Result<(i64, NaiveTime), ExcelError> {
    validate_excel_serial(system, serial)?;

    let mut whole_days = serial.trunc() as i64;
    let mut total_seconds = (serial.fract() * 86_400.0).round() as u32;
    if total_seconds == 86_400 {
        whole_days = whole_days.checked_add(1).ok_or_else(ExcelError::new_num)?;
        if whole_days as f64 > max_excel_serial_for(system) {
            return Err(ExcelError::new_num());
        }
        total_seconds = 0;
    }

    let time = NaiveTime::from_num_seconds_from_midnight_opt(total_seconds, 0)
        .ok_or_else(ExcelError::new_num)?;
    Ok((whole_days, time))
}

/// Convert Excel serial number to datetime
/// The fractional part represents time of day
pub fn serial_to_datetime(serial: f64) -> Result<NaiveDateTime, ExcelError> {
    let (whole_days, time) = normalized_serial_parts(DateSystem::Excel1900, serial)?;
    let date = serial_to_date(whole_days as f64)?;
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
const EXCEL_MAX_DATE: NaiveDate = NaiveDate::from_ymd_opt(9999, 12, 31).unwrap();

/// Return the last whole-day serial supported by Excel's calendar.
pub fn max_excel_serial_for(system: DateSystem) -> f64 {
    date_to_serial_for(system, &EXCEL_MAX_DATE)
}

/// Validate a serial before converting it to a calendar value.
///
/// Excel date serials cannot be negative or extend past 9999-12-31. Checking
/// finite values and bounds before any float-to-integer cast also keeps extreme
/// inputs from silently saturating.
pub fn validate_excel_serial(system: DateSystem, serial: f64) -> Result<(), ExcelError> {
    if !serial.is_finite() || serial < 0.0 || serial.trunc() > max_excel_serial_for(system) {
        return Err(ExcelError::new_num());
    }
    Ok(())
}

/// Return date fields using Excel's display semantics.
///
/// The 1900 system has two display-only values that cannot be represented by
/// `chrono::NaiveDate`: serial 0 is 1900-01-00 and serial 60 is the phantom
/// 1900-02-29. All real dates continue through the shared serial converter.
pub fn serial_to_display_date_parts_for(
    system: DateSystem,
    serial: f64,
) -> Result<(i32, u32, u32), ExcelError> {
    validate_excel_serial(system, serial)?;
    let whole_days = serial.trunc();
    if system == DateSystem::Excel1900 {
        if whole_days == 0.0 {
            return Ok((1900, 1, 0));
        }
        if whole_days == 60.0 {
            return Ok((1900, 2, 29));
        }
    }

    let date = serial_to_datetime_for(system, whole_days)?.date();
    Ok((date.year(), date.month(), date.day()))
}

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
            let (whole_days, time) = normalized_serial_parts(system, serial)?;
            let date = EXCEL_1904_EPOCH
                .checked_add_signed(chrono::TimeDelta::days(whole_days))
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
    fn test_serial_to_datetime_rounding_carries_across_1900_phantom_day() {
        let rounds_up = 86_399.6 / 86_400.0;

        assert_eq!(
            serial_to_datetime(59.0 + rounds_up).unwrap(),
            serial_to_datetime(60.0).unwrap()
        );
        assert_eq!(
            serial_to_datetime(60.0 + rounds_up).unwrap(),
            NaiveDate::from_ymd_opt(1900, 3, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        );
        assert_eq!(
            serial_to_datetime(61.0 + rounds_up).unwrap(),
            NaiveDate::from_ymd_opt(1900, 3, 2)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        );
    }

    #[test]
    fn test_serial_to_datetime_rounding_carries_in_1904_system() {
        let rounds_up = 86_399.6 / 86_400.0;

        assert_eq!(
            serial_to_datetime_for(DateSystem::Excel1904, 59.0 + rounds_up).unwrap(),
            NaiveDate::from_ymd_opt(1904, 3, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        );
    }

    #[test]
    fn test_serial_to_datetime_rejects_rounded_carry_past_max_date() {
        let rounds_up = 86_399.6 / 86_400.0;

        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let serial = max_excel_serial_for(system) + rounds_up;
            assert!(serial_to_datetime_for(system, serial).is_err());
        }
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
