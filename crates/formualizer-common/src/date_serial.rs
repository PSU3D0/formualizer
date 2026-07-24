//! Canonical Excel date-serial conversion.
//!
//! Excel workbooks use either the 1900 or 1904 date system. The 1900 system
//! also contains a fictitious 1900-02-29 at serial 60. Since `chrono` cannot
//! represent that date (or Excel's display-only 1900-01-00 at serial 0),
//! calendar conversion and display conversion are intentionally separate.

use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::{DateSystem, ExcelError};

const SECONDS_PER_DAY: f64 = 86_400.0;
const EXCEL_1900_EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1899, 12, 31).unwrap();
const EXCEL_1904_EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1904, 1, 1).unwrap();
const EXCEL_MAX_DATE: NaiveDate = NaiveDate::from_ymd_opt(9999, 12, 31).unwrap();
const EXCEL_1900_PHANTOM_CUTOFF: NaiveDate = NaiveDate::from_ymd_opt(1900, 3, 1).unwrap();
const EXCEL_1900_PHANTOM_PREVIOUS_DATE: NaiveDate = NaiveDate::from_ymd_opt(1900, 2, 28).unwrap();

/// Calendar fields rendered by Excel, including display-only dates that
/// cannot be represented by `chrono::NaiveDate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExcelDateParts {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

/// Convert a date to an Excel serial in the selected date system.
///
/// Dates before the selected epoch produce negative serials. Checked
/// serial-to-calendar conversion rejects those serials because Excel does not
/// treat them as valid calendar values.
pub fn date_to_serial_for(system: DateSystem, date: &NaiveDate) -> f64 {
    match system {
        DateSystem::Excel1900 => {
            let days = (*date - EXCEL_1900_EPOCH).num_days();
            if *date >= EXCEL_1900_PHANTOM_CUTOFF {
                (days + 1) as f64
            } else {
                days as f64
            }
        }
        DateSystem::Excel1904 => (*date - EXCEL_1904_EPOCH).num_days() as f64,
    }
}

/// Convert a datetime to an Excel serial in the selected date system.
///
/// Formualizer's existing temporal representation is second-precision:
/// subsecond nanoseconds are intentionally not encoded.
pub fn datetime_to_serial_for(system: DateSystem, datetime: &NaiveDateTime) -> f64 {
    date_to_serial_for(system, &datetime.date()) + time_to_fraction(&datetime.time())
}

/// Convert a time to its fractional-day representation.
///
/// Subsecond nanoseconds are intentionally ignored for compatibility with the
/// existing Formualizer temporal model.
pub fn time_to_fraction(time: &NaiveTime) -> f64 {
    time.num_seconds_from_midnight() as f64 / SECONDS_PER_DAY
}

/// Return the final whole-day serial supported by Excel's calendar.
pub fn max_excel_serial_for(system: DateSystem) -> f64 {
    date_to_serial_for(system, &EXCEL_MAX_DATE)
}

/// Validate an Excel serial before converting it to a calendar value.
pub fn validate_excel_serial(system: DateSystem, serial: f64) -> Result<(), ExcelError> {
    if !serial.is_finite() || serial < 0.0 || serial.trunc() > max_excel_serial_for(system) {
        return Err(ExcelError::new_num());
    }
    Ok(())
}

fn normalized_serial_parts(
    system: DateSystem,
    serial: f64,
) -> Result<(i64, NaiveTime), ExcelError> {
    validate_excel_serial(system, serial)?;

    let mut whole_days = serial.trunc() as i64;
    let mut total_seconds = (serial.fract() * SECONDS_PER_DAY).round() as u32;
    if total_seconds == SECONDS_PER_DAY as u32 {
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

fn date_for_whole_serial(system: DateSystem, whole_days: i64) -> Result<NaiveDate, ExcelError> {
    match system {
        DateSystem::Excel1900 => {
            if whole_days == 60 {
                return Ok(EXCEL_1900_PHANTOM_PREVIOUS_DATE);
            }
            let offset = if whole_days < 60 {
                whole_days
            } else {
                whole_days - 1
            };
            EXCEL_1900_EPOCH
                .checked_add_signed(chrono::TimeDelta::days(offset))
                .ok_or_else(ExcelError::new_num)
        }
        DateSystem::Excel1904 => EXCEL_1904_EPOCH
            .checked_add_signed(chrono::TimeDelta::days(whole_days))
            .ok_or_else(ExcelError::new_num),
    }
}

/// Convert an Excel serial to a representable `chrono` date.
///
/// In the 1900 system, serial 60 maps to 1900-02-28 because the fictitious
/// 1900-02-29 cannot be represented. Use
/// [`try_serial_to_display_date_parts_for`] when rendering Excel date fields.
pub fn try_serial_to_date_for(system: DateSystem, serial: f64) -> Result<NaiveDate, ExcelError> {
    validate_excel_serial(system, serial)?;
    date_for_whole_serial(system, serial.trunc() as i64)
}

/// Convert an Excel serial to a representable `chrono` datetime.
///
/// Fractional days are rounded to the nearest second. A rounded value of
/// 24:00 carries into the next serial day and is rejected if it exceeds
/// Excel's maximum date. In the 1900 system, carrying into phantom serial 60
/// still aliases to representable 1900-02-28.
pub fn try_serial_to_datetime_for(
    system: DateSystem,
    serial: f64,
) -> Result<NaiveDateTime, ExcelError> {
    let (whole_days, time) = normalized_serial_parts(system, serial)?;
    let date = date_for_whole_serial(system, whole_days)?;
    Ok(NaiveDateTime::new(date, time))
}

/// Return the date fields Excel displays for a serial.
///
/// In the 1900 system this returns `1900-01-00` for serial 0 and the phantom
/// `1900-02-29` for serial 60. Those values are deliberately not exposed as a
/// `chrono::NaiveDate`.
pub fn try_serial_to_display_date_parts_for(
    system: DateSystem,
    serial: f64,
) -> Result<ExcelDateParts, ExcelError> {
    validate_excel_serial(system, serial)?;
    let whole_days = serial.trunc();
    if system == DateSystem::Excel1900 {
        if whole_days == 0.0 {
            return Ok(ExcelDateParts {
                year: 1900,
                month: 1,
                day: 0,
            });
        }
        if whole_days == 60.0 {
            return Ok(ExcelDateParts {
                year: 1900,
                month: 2,
                day: 29,
            });
        }
    }

    let date = try_serial_to_date_for(system, whole_days)?;
    Ok(ExcelDateParts {
        year: date.year(),
        month: date.month(),
        day: date.day(),
    })
}

/// Compatibility wrapper for the historical, implicit Excel-1900 API.
pub fn datetime_to_serial(datetime: &NaiveDateTime) -> f64 {
    datetime_to_serial_for(DateSystem::Excel1900, datetime)
}

fn legacy_serial_to_datetime(serial: f64) -> NaiveDateTime {
    let days = serial.trunc() as i64;
    let fractional_seconds = (serial.fract() * SECONDS_PER_DAY).round() as i64;
    let offset_days = if days == 60 {
        59
    } else if days < 60 {
        days
    } else {
        days - 1
    };
    let date = EXCEL_1900_EPOCH + ChronoDuration::days(offset_days);
    let time = NaiveTime::from_num_seconds_from_midnight_opt(
        fractional_seconds.rem_euclid(SECONDS_PER_DAY as i64) as u32,
        0,
    )
    .expect("legacy fractional-day normalization must produce a valid time");
    date.and_time(time)
}

/// Compatibility wrapper for the historical, implicit Excel-1900 API.
///
/// Valid Excel serials use the canonical checked conversion. Inputs outside
/// Excel's calendar domain retain the legacy common behavior, including
/// finite negative serials that represent pre-epoch datetimes. New code should
/// use [`try_serial_to_datetime_for`] when invalid input must return an error.
pub fn serial_to_datetime(serial: f64) -> NaiveDateTime {
    try_serial_to_datetime_for(DateSystem::Excel1900, serial)
        .unwrap_or_else(|_| legacy_serial_to_datetime(serial))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn datetime(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> NaiveDateTime {
        date(year, month, day).and_hms_opt(hour, minute, 0).unwrap()
    }

    #[test]
    fn excel_1900_representable_and_display_boundaries() {
        let cases = [
            (0.0, date(1899, 12, 31)),
            (1.0, date(1900, 1, 1)),
            (59.0, date(1900, 2, 28)),
            (60.0, date(1900, 2, 28)),
            (61.0, date(1900, 3, 1)),
            (45_306.0, date(2024, 1, 15)),
        ];
        for (serial, expected) in cases {
            assert_eq!(
                try_serial_to_date_for(DateSystem::Excel1900, serial).unwrap(),
                expected,
                "serial {serial}"
            );
        }

        assert_eq!(
            try_serial_to_display_date_parts_for(DateSystem::Excel1900, 0.0).unwrap(),
            ExcelDateParts {
                year: 1900,
                month: 1,
                day: 0,
            }
        );
        assert_eq!(
            try_serial_to_display_date_parts_for(DateSystem::Excel1900, 60.0).unwrap(),
            ExcelDateParts {
                year: 1900,
                month: 2,
                day: 29,
            }
        );
    }

    #[test]
    fn excel_1904_boundaries() {
        let cases = [
            (0.0, date(1904, 1, 1)),
            (1.0, date(1904, 1, 2)),
            (59.0, date(1904, 2, 29)),
            (60.0, date(1904, 3, 1)),
            (61.0, date(1904, 3, 2)),
            (43_844.0, date(2024, 1, 15)),
        ];
        for (serial, expected) in cases {
            assert_eq!(
                try_serial_to_date_for(DateSystem::Excel1904, serial).unwrap(),
                expected,
                "serial {serial}"
            );
        }
    }

    #[test]
    fn date_and_datetime_encode_for_both_systems() {
        assert_eq!(
            date_to_serial_for(DateSystem::Excel1900, &date(1900, 1, 1)),
            1.0
        );
        assert_eq!(
            date_to_serial_for(DateSystem::Excel1900, &date(1900, 2, 28)),
            59.0
        );
        assert_eq!(
            date_to_serial_for(DateSystem::Excel1900, &date(1900, 3, 1)),
            61.0
        );
        assert_eq!(
            date_to_serial_for(DateSystem::Excel1900, &date(1904, 1, 1)),
            1462.0
        );
        assert_eq!(
            date_to_serial_for(DateSystem::Excel1904, &date(1904, 1, 1)),
            0.0
        );
        assert_eq!(
            datetime_to_serial_for(DateSystem::Excel1904, &datetime(2024, 1, 15, 12, 0)),
            43_844.5
        );
    }

    #[test]
    fn fractional_seconds_round_and_carry_across_boundaries() {
        let stays = 86_399.4 / 86_400.0;
        let carries = 86_399.6 / 86_400.0;

        assert_eq!(
            try_serial_to_datetime_for(DateSystem::Excel1900, 59.0 + stays).unwrap(),
            date(1900, 2, 28).and_hms_opt(23, 59, 59).unwrap()
        );
        assert_eq!(
            try_serial_to_datetime_for(DateSystem::Excel1900, 59.0 + carries).unwrap(),
            date(1900, 2, 28).and_hms_opt(0, 0, 0).unwrap()
        );
        assert_eq!(
            try_serial_to_datetime_for(DateSystem::Excel1900, 60.0 + carries).unwrap(),
            date(1900, 3, 1).and_hms_opt(0, 0, 0).unwrap()
        );
        assert_eq!(
            try_serial_to_datetime_for(DateSystem::Excel1904, 59.0 + carries).unwrap(),
            date(1904, 3, 1).and_hms_opt(0, 0, 0).unwrap()
        );
    }

    #[test]
    fn invalid_and_out_of_bounds_serials_are_rejected() {
        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            for serial in [
                -1.0,
                -f64::MIN_POSITIVE,
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::MAX,
            ] {
                assert!(try_serial_to_datetime_for(system, serial).is_err());
                assert!(try_serial_to_date_for(system, serial).is_err());
                assert!(try_serial_to_display_date_parts_for(system, serial).is_err());
            }

            let max = max_excel_serial_for(system);
            assert_eq!(try_serial_to_date_for(system, max).unwrap(), EXCEL_MAX_DATE);
            assert!(try_serial_to_date_for(system, max + 1.0).is_err());
            assert!(try_serial_to_datetime_for(system, max + 86_399.6 / 86_400.0).is_err());
        }
    }

    #[test]
    fn real_dates_round_trip_and_phantom_day_is_documented_non_bijective() {
        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            for expected in [date(1904, 1, 1), date(2024, 1, 15), EXCEL_MAX_DATE] {
                let serial = date_to_serial_for(system, &expected);
                assert_eq!(try_serial_to_date_for(system, serial).unwrap(), expected);
            }
        }

        let phantom = try_serial_to_date_for(DateSystem::Excel1900, 60.0).unwrap();
        assert_eq!(phantom, date(1900, 2, 28));
        assert_eq!(date_to_serial_for(DateSystem::Excel1900, &phantom), 59.0);
    }

    #[test]
    fn compatibility_wrappers_match_excel_1900_and_retain_negative_serials() {
        let expected = datetime(2024, 1, 15, 12, 0);
        assert_eq!(datetime_to_serial(&expected), 45_306.5);
        assert_eq!(serial_to_datetime(45_306.5), expected);
        assert_eq!(
            serial_to_datetime(-1.0),
            date(1899, 12, 30).and_hms_opt(0, 0, 0).unwrap()
        );
        assert_eq!(
            serial_to_datetime(-1.25),
            date(1899, 12, 30).and_hms_opt(18, 0, 0).unwrap()
        );
    }

    #[test]
    fn time_fraction_is_second_precision() {
        let time = NaiveTime::from_hms_nano_opt(12, 0, 0, 999_999_999).unwrap();
        assert_eq!(time_to_fraction(&time), 0.5);
    }
}
