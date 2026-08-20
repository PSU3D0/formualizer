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

/// Parse date text using Formualizer's deterministic en-US spreadsheet convention.
///
/// Numeric slash dates use month/day/year ordering. Two-digit years in slash
/// and English month-name forms use Excel's fixed window: `00..=29` means
/// 2000 through 2029 and `30..=99` means 1930 through 1999. ISO dates require
/// a four-digit year. Parsing has no locale parameter and never consults the
/// host locale.
pub fn parse_excel_date_text(input: &str) -> Option<NaiveDate> {
    let text = input.trim();
    if text.is_empty() {
        return None;
    }

    if let Some(date) = parse_numeric_slash_date(text) {
        return Some(date);
    }

    parse_iso_date(text).or_else(|| parse_month_name_date(text))
}

fn parse_numeric_slash_date(text: &str) -> Option<NaiveDate> {
    let parts: Vec<&str> = text.split('/').collect();
    if parts.len() != 3
        || parts
            .iter()
            .any(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }

    let month = parts[0].parse::<u32>().ok()?;
    let day = parts[1].parse::<u32>().ok()?;
    let year = parse_excel_year(parts[2])?;
    NaiveDate::from_ymd_opt(year, month, day)
}

fn parse_excel_year(text: &str) -> Option<i32> {
    let year = text.parse::<i32>().ok()?;
    match text.len() {
        1 => Some(2000 + year),
        2 if year <= 29 => Some(2000 + year),
        2 => Some(1900 + year),
        4 => Some(year),
        _ => None,
    }
}

fn parse_iso_date(text: &str) -> Option<NaiveDate> {
    let (year, rest) = text.split_once('-')?;
    if year.len() != 4 || !year.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let normalized = format!("{}-{rest}", year.parse::<i32>().ok()?);
    NaiveDate::parse_from_str(&normalized, "%Y-%m-%d").ok()
}

fn parse_month_name_date(text: &str) -> Option<NaiveDate> {
    const FORMATS: &[&str] = &["%B %d, %Y", "%b %d, %Y", "%d-%b-%Y"];
    if let Some(date) = FORMATS.iter().find_map(|format| {
        let separator = if *format == "%d-%b-%Y" { '-' } else { ' ' };
        let (prefix, year_text) = text.rsplit_once(separator)?;
        let year = parse_excel_year(year_text)?;
        let normalized = format!("{prefix}{separator}{year:04}");
        NaiveDate::parse_from_str(&normalized, format).ok()
    }) {
        return Some(date);
    }

    // Month-year only: "Jan 2024", "January 2024" → first day of month.
    // Excel accepts these and returns serial for the 1st of that month.
    parse_month_year_only(text)
}

/// Parse "Jan 2024" or "January 2024" as the first day of that month.
fn parse_month_year_only(text: &str) -> Option<NaiveDate> {
    let (month_text, year_text) = text.rsplit_once(' ')?;
    let year = parse_excel_year(year_text.trim())?;
    // Try abbreviated and full month names
    let month = parse_month_name(month_text.trim())?;
    NaiveDate::from_ymd_opt(year, month, 1)
}

fn parse_month_name(text: &str) -> Option<u32> {
    const MONTHS_FULL: &[&str] = &[
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ];
    const MONTHS_ABBR: &[&str] = &[
        "jan", "feb", "mar", "apr", "may", "jun",
        "jul", "aug", "sep", "oct", "nov", "dec",
    ];
    let lower = text.to_lowercase();
    if let Some(pos) = MONTHS_FULL.iter().position(|&m| m == lower) {
        return Some(pos as u32 + 1);
    }
    if let Some(pos) = MONTHS_ABBR.iter().position(|&m| m == lower) {
        return Some(pos as u32 + 1);
    }
    None
}

/// Parse time text using fixed 24-hour or English AM/PM formats.
///
/// Parsing has no locale parameter, uses English AM/PM markers, and never
/// consults the host locale. ASCII whitespace around separators is ignored.
/// Fractional seconds (e.g. `12:30:45.5`) are truncated to whole seconds.
/// `24:00` and `24:00:00` are accepted as midnight (Excel compatibility).
pub fn parse_excel_time_text(input: &str) -> Option<NaiveTime> {
    let text = input.trim();
    let mut normalized = String::with_capacity(text.len());
    let mut pending_space = false;
    for ch in text.chars() {
        if ch.is_ascii_whitespace() {
            pending_space = true;
        } else {
            if pending_space && ch != ':' && !normalized.ends_with(':') && !normalized.is_empty() {
                normalized.push(' ');
            }
            normalized.push(ch);
            pending_space = false;
        }
    }

    // Handle 24:00 and 24:00:00 as midnight (Excel treats this as end-of-day = 0:00)
    if normalized == "24:00" || normalized == "24:00:00" {
        return Some(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    }

    // Strip fractional seconds: "12:30:45.5" → "12:30:45"
    // Find the seconds decimal point (after the second colon) and truncate
    let normalized = strip_fractional_seconds(&normalized);

    const FORMATS: &[&str] = &["%H:%M:%S", "%H:%M", "%I:%M:%S %p", "%I:%M %p"];
    FORMATS
        .iter()
        .find_map(|format| NaiveTime::parse_from_str(&normalized, format).ok())
}

/// Strip fractional seconds from a time string: "12:30:45.123" → "12:30:45"
fn strip_fractional_seconds(text: &str) -> String {
    // Find pattern: digits followed by '.' followed by digits, where this
    // appears after the second ':' (seconds position) or before a space/AM/PM
    if let Some(dot_pos) = text.find('.') {
        // Verify the dot is in a seconds position (preceded by digits, followed by digits)
        let before_dot = &text[..dot_pos];
        let after_dot = &text[dot_pos + 1..];
        if before_dot.ends_with(|c: char| c.is_ascii_digit()) {
            // Find where the fractional digits end
            let frac_end = after_dot
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(after_dot.len());
            if frac_end > 0 {
                // Reconstruct without the fractional part
                let mut result = before_dot.to_string();
                result.push_str(&after_dot[frac_end..]);
                return result;
            }
        }
    }
    text.to_string()
}

/// Parse an en-US date and time separated by whitespace or an ISO `T`.
///
/// Date and time components use [`parse_excel_date_text`] and
/// [`parse_excel_time_text`]. `T` is accepted only after a four-digit-year ISO
/// date. There is no locale parameter, and parsing is independent of the host
/// locale.
pub fn parse_excel_datetime_text(input: &str) -> Option<NaiveDateTime> {
    let text = input.trim();
    text.char_indices()
        .filter(|(_, ch)| *ch == 'T' || ch.is_ascii_whitespace())
        .find_map(|(index, ch)| {
            let time_start = index + ch.len_utf8();
            let date = if ch == 'T' {
                parse_iso_date(&text[..index])?
            } else {
                parse_excel_date_text(&text[..index])?
            };
            let time = parse_excel_time_text(&text[time_start..])?;
            Some(date.and_time(time))
        })
}

/// Parse spreadsheet date, time, or datetime text and return its serial.
///
/// This is the canonical entry point for text operands that need a temporal
/// serial. Dates use deterministic en-US month/day/year ordering, with no
/// locale parameter. Date-bearing results honor the selected workbook date
/// system; time-only results are fractional days in either system.
pub fn parse_excel_datetime_text_to_serial_for(system: DateSystem, input: &str) -> Option<f64> {
    if let Some(datetime) = parse_excel_datetime_text(input) {
        return Some(datetime_to_serial_for(system, &datetime));
    }
    if let Some(date) = parse_excel_date_text(input) {
        return Some(date_to_serial_for(system, &date));
    }
    parse_excel_time_text(input).map(|time| time_to_fraction(&time))
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

    #[test]
    fn temporal_text_parser_uses_excel_year_window_and_date_system() {
        assert_eq!(
            parse_excel_datetime_text_to_serial_for(DateSystem::Excel1900, "1/1/03"),
            Some(37_622.0)
        );
        assert_eq!(
            parse_excel_datetime_text_to_serial_for(DateSystem::Excel1904, "1/1/03 12:00"),
            Some(36_160.5)
        );

        // oracle: lo-verified for every accepted two-digit-year date shape.
        for (input, expected) in [
            ("1/1/29", date(2029, 1, 1)),
            ("1/1/30", date(1930, 1, 1)),
            ("January 1, 29", date(2029, 1, 1)),
            ("January 1, 30", date(1930, 1, 1)),
            ("Jan 1, 29", date(2029, 1, 1)),
            ("Jan 1, 30", date(1930, 1, 1)),
            ("1-Jan-29", date(2029, 1, 1)),
            ("1-Jan-30", date(1930, 1, 1)),
        ] {
            assert_eq!(parse_excel_date_text(input), Some(expected), "{input}");
        }

        // oracle: lo-verified. A short year is not accepted in ISO year position.
        assert_eq!(parse_excel_date_text("03-01-01"), None);
        assert_eq!(
            parse_excel_datetime_text_to_serial_for(DateSystem::Excel1900, "12:00"),
            Some(0.5)
        );
    }

    #[test]
    fn temporal_text_parser_restricts_slash_order_and_t_separator() {
        // oracle: lo-verified. Arithmetic follows en-US m/d/y, unlike DATEVALUE's
        // separately retained legacy fallbacks.
        assert_eq!(parse_excel_date_text("15/01/2003"), None);
        assert_eq!(parse_excel_date_text("2003/1/1"), None);
        assert_eq!(parse_excel_datetime_text("1/1/03T12:00"), None);
        assert_eq!(
            parse_excel_datetime_text("2003-01-01T12:00"),
            Some(datetime(2003, 1, 1, 12, 0))
        );
    }

    #[test]
    fn temporal_text_parser_rejects_invalid_and_non_dates() {
        for text in ["2/30/03", "abc", "", "13/13/13", "123-456"] {
            assert!(
                parse_excel_datetime_text_to_serial_for(DateSystem::Excel1900, text).is_none(),
                "{text}"
            );
        }
    }

    #[test]
    fn single_digit_year_uses_2000_window() {
        // "1/2/5" → January 2, 2005
        assert_eq!(parse_excel_date_text("1/2/5"), Some(date(2005, 1, 2)));
        // "3/15/9" → March 15, 2009
        assert_eq!(parse_excel_date_text("3/15/9"), Some(date(2009, 3, 15)));
        // "12/31/0" → December 31, 2000
        assert_eq!(parse_excel_date_text("12/31/0"), Some(date(2000, 12, 31)));
    }

    #[test]
    fn time_24_00_parses_as_midnight() {
        assert_eq!(
            parse_excel_time_text("24:00"),
            Some(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        );
        assert_eq!(
            parse_excel_time_text("24:00:00"),
            Some(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        );
    }

    #[test]
    fn fractional_seconds_are_truncated() {
        // "12:30:45.5" → 12:30:45 (fractional part ignored)
        assert_eq!(
            parse_excel_time_text("12:30:45.5"),
            Some(NaiveTime::from_hms_opt(12, 30, 45).unwrap())
        );
        // "08:15:30.999" → 08:15:30
        assert_eq!(
            parse_excel_time_text("08:15:30.999"),
            Some(NaiveTime::from_hms_opt(8, 15, 30).unwrap())
        );
        // "2:05:00.0" → 02:05:00
        assert_eq!(
            parse_excel_time_text("2:05:00.0"),
            Some(NaiveTime::from_hms_opt(2, 5, 0).unwrap())
        );
    }

    #[test]
    fn month_year_only_parses_as_first_of_month() {
        assert_eq!(parse_excel_date_text("Jan 2024"), Some(date(2024, 1, 1)));
        assert_eq!(parse_excel_date_text("February 2024"), Some(date(2024, 2, 1)));
        assert_eq!(parse_excel_date_text("Dec 99"), Some(date(1999, 12, 1)));
        assert_eq!(parse_excel_date_text("Mar 05"), Some(date(2005, 3, 1)));
        assert_eq!(parse_excel_date_text("July 2000"), Some(date(2000, 7, 1)));
    }
}
