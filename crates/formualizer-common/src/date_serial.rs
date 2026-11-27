//! Canonical Excel date-serial conversion.
//!
//! Excel workbooks use either the 1900 or 1904 date system. The 1900 system
//! also contains a fictitious 1900-02-29 at serial 60. Since `chrono` cannot
//! represent that date (or Excel's display-only 1900-01-00 at serial 0),
//! calendar conversion and display conversion are intentionally separate.

use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::{DateSystem, ExcelError};

const SECONDS_PER_DAY: f64 = 86_400.0;
const MILLIS_PER_DAY: f64 = 86_400_000.0;
const MICROS_PER_DAY_U64: u64 = 86_400 * 1_000_000;
const MICROS_PER_DAY: f64 = 86_400_000_000.0;
const NANOS_PER_SECOND: f64 = 1_000_000_000.0;
const NANOS_PER_DAY_U64: u64 = 86_400 * 1_000_000_000;
const NANOS_PER_DAY: f64 = 86_400_000_000_000.0;
/// `i64::MAX as f64` rounds up to `2^63`; strict `< I64_UPPER_BOUND_F64` guarantees
/// casting a finite `f64` to `i64` never saturates at the upper bound.
const I64_UPPER_BOUND_F64: f64 = 9_223_372_036_854_775_808.0;
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

/// Convert a datetime to an Excel serial in the selected date system,
/// preserving sub-second precision via [`time_to_fraction`].
///
/// For dates on Excel's maximum supported calendar day (`9999-12-31`), if
/// adding the sub-second fraction rounds up in IEEE-754 `f64` to the next day
/// (`max_excel_serial_for(system) + 1.0`), the result is clamped to the largest
/// `f64` strictly below that next-day boundary so the encoded serial stays
/// within Excel's valid calendar domain.
pub fn datetime_to_serial_for(system: DateSystem, datetime: &NaiveDateTime) -> f64 {
    let date_serial = date_to_serial_for(system, &datetime.date());
    let serial = date_serial + time_to_fraction(&datetime.time());
    let max_serial = max_excel_serial_for(system);
    let next_day = max_serial + 1.0;
    if date_serial <= max_serial && serial >= next_day {
        next_day.next_down()
    } else {
        serial
    }
}

/// Convert a time to its fractional-day representation, preserving sub-second
/// nanoseconds.
///
/// Whole-second times use exact `seconds / 86_400.0` division for bit-exact
/// compatibility with existing whole-second serials. Sub-second times encode
/// total nanoseconds divided by `86_400_000_000_000.0`.
pub fn time_to_fraction(time: &NaiveTime) -> f64 {
    let secs = time.num_seconds_from_midnight();
    // Clamp leap-second nanoseconds (>= 1e9) so the fraction stays strictly in [0.0, 1.0).
    let nanos = time.nanosecond().min(999_999_999);
    if nanos == 0 {
        secs as f64 / SECONDS_PER_DAY
    } else {
        let total_nanos = u64::from(secs) * 1_000_000_000 + u64::from(nanos);
        total_nanos as f64 / NANOS_PER_DAY
    }
}

/// Convert a fractional day in `[0.0, 1.0)` back to a `NaiveTime` with
/// sub-second nanosecond precision (inverse of [`time_to_fraction`]).
///
/// Returns `None` if `fraction` is non-finite or outside `[0.0, 1.0)`.
pub fn fraction_to_time(fraction: f64) -> Option<NaiveTime> {
    if !fraction.is_finite() || !(0.0..1.0).contains(&fraction) {
        return None;
    }
    let nanos = (fraction * NANOS_PER_DAY).round() as u64;
    let nanos = nanos.min(NANOS_PER_DAY_U64 - 1);
    NaiveTime::from_num_seconds_from_midnight_opt(
        (nanos / 1_000_000_000) as u32,
        (nanos % 1_000_000_000) as u32,
    )
}

/// Convert a `chrono::Duration` to fractional Excel days, preserving sub-second
/// nanoseconds/microseconds for both positive and negative durations.
///
/// Whole-second durations use exact `seconds / 86_400.0` division for bit-exact
/// compatibility. Sub-second durations within `i64` nanosecond range (±292 years)
/// convert via total nanoseconds; larger durations fall back to seconds plus
/// sub-second fractional seconds.
pub fn duration_to_days_fraction(duration: &ChronoDuration) -> f64 {
    let subsec_nanos = duration.subsec_nanos();
    if subsec_nanos == 0 {
        duration.num_seconds() as f64 / SECONDS_PER_DAY
    } else if let Some(total_nanos) = duration.num_nanoseconds() {
        total_nanos as f64 / NANOS_PER_DAY
    } else {
        (duration.num_seconds() as f64 + f64::from(subsec_nanos) / NANOS_PER_SECOND)
            / SECONDS_PER_DAY
    }
}

/// Convert fractional Excel days back to a `chrono::Duration` with sub-second
/// precision (inverse of [`duration_to_days_fraction`]).
///
/// Returns the coarsest precision among whole seconds, milliseconds,
/// microseconds, and nanoseconds whose re-encoding through
/// [`duration_to_days_fraction`] reproduces the exact `f64` `days` value. This
/// prevents ±ULP nanosecond noise on whole-second, millisecond, and microsecond
/// durations beyond ~30 days, allowing exact round-trips up to 10+ years
/// (positive and negative). Falls back to nanosecond precision within `i64`
/// nanoseconds (±292 years), or whole-second precision for larger finite
/// durations. Returns `None` for non-finite or out-of-range inputs.
pub fn days_fraction_to_duration(days: f64) -> Option<ChronoDuration> {
    if !days.is_finite() {
        return None;
    }

    let secs = (days * SECONDS_PER_DAY).round();
    if secs.is_finite()
        && secs >= i64::MIN as f64
        && secs < I64_UPPER_BOUND_F64
        && let Some(candidate) = ChronoDuration::try_seconds(secs as i64)
        && duration_to_days_fraction(&candidate) == days
    {
        return Some(candidate);
    }

    let millis = (days * MILLIS_PER_DAY).round();
    if millis.is_finite()
        && millis >= i64::MIN as f64
        && millis < I64_UPPER_BOUND_F64
        && let Some(candidate) = ChronoDuration::try_milliseconds(millis as i64)
        && duration_to_days_fraction(&candidate) == days
    {
        return Some(candidate);
    }

    let micros = (days * MICROS_PER_DAY).round();
    if micros.is_finite() && micros >= i64::MIN as f64 && micros < I64_UPPER_BOUND_F64 {
        let candidate = ChronoDuration::microseconds(micros as i64);
        if duration_to_days_fraction(&candidate) == days {
            return Some(candidate);
        }
    }

    let nanos = (days * NANOS_PER_DAY).round();
    if nanos.is_finite() && nanos >= i64::MIN as f64 && nanos < I64_UPPER_BOUND_F64 {
        Some(ChronoDuration::nanoseconds(nanos as i64))
    } else if secs.is_finite() && secs >= i64::MIN as f64 && secs < I64_UPPER_BOUND_F64 {
        ChronoDuration::try_seconds(secs as i64)
    } else {
        None
    }
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
///
/// The year token must be four digits. `parse_excel_year` also accepts one- and
/// two-digit years, but admitting them here reads `"Jan 3"` as `2003-01-01`,
/// which collides with a month-plus-day-without-year form. #290 explicitly
/// refuses those (`"Jan-03"`, `"1/03"`) because a wall-clock year fill-in is
/// ambiguous, so the month-year form is held to an unambiguous four-digit year.
fn parse_month_year_only(text: &str) -> Option<NaiveDate> {
    let (month_text, year_text) = text.rsplit_once(' ')?;
    let year_text = year_text.trim();
    if year_text.len() != 4 || !year_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let year = year_text.parse::<i32>().ok()?;
    // Try abbreviated and full month names
    let month = parse_month_name(month_text.trim())?;
    NaiveDate::from_ymd_opt(year, month, 1)
}

fn parse_month_name(text: &str) -> Option<u32> {
    const MONTHS_FULL: &[&str] = &[
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ];
    const MONTHS_ABBR: &[&str] = &[
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
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
///
/// Only a dot that terminates a full `HH:MM:SS` field is treated as fractional
/// seconds. The dot must be preceded by two colons (the hour and minute
/// separators), so `"12:00.5"` — a single colon, i.e. a malformed time — is
/// left untouched and subsequently rejected by the parser as `#VALUE!` rather
/// than silently accepted as `12:00`.
fn strip_fractional_seconds(text: &str) -> String {
    // Find pattern: digits followed by '.' followed by digits, where this
    // appears after the second ':' (seconds position) or before a space/AM/PM
    if let Some(dot_pos) = text.find('.') {
        // Verify the dot is in a seconds position (preceded by digits, followed by digits)
        let before_dot = &text[..dot_pos];
        let after_dot = &text[dot_pos + 1..];
        if before_dot.matches(':').count() >= 2
            && before_dot.ends_with(|c: char| c.is_ascii_digit())
        {
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
/// Fractional days are rounded to the nearest second (matching Excel's
/// `HOUR`/`MINUTE`/`SECOND` semantics). A rounded value of 24:00 carries into
/// the next serial day and is rejected if it exceeds Excel's maximum date. In
/// the 1900 system, carrying into phantom serial 60 still aliases to
/// representable 1900-02-28.
pub fn try_serial_to_datetime_for(
    system: DateSystem,
    serial: f64,
) -> Result<NaiveDateTime, ExcelError> {
    let (whole_days, time) = normalized_serial_parts(system, serial)?;
    let date = date_for_whole_serial(system, whole_days)?;
    Ok(NaiveDateTime::new(date, time))
}

fn datetime_from_serial_micros(
    system: DateSystem,
    mut whole_days: i64,
    mut total_micros: u64,
) -> Result<NaiveDateTime, ExcelError> {
    if total_micros >= MICROS_PER_DAY_U64 {
        whole_days = whole_days.checked_add(1).ok_or_else(ExcelError::new_num)?;
        if whole_days as f64 > max_excel_serial_for(system) {
            return Err(ExcelError::new_num());
        }
        total_micros = 0;
    }

    let secs = (total_micros / 1_000_000) as u32;
    let nanos = ((total_micros % 1_000_000) * 1_000) as u32;
    let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
        .ok_or_else(ExcelError::new_num)?;
    let date = date_for_whole_serial(system, whole_days)?;
    Ok(NaiveDateTime::new(date, time))
}

/// Convert an Excel serial to a representable `chrono` datetime, preserving
/// sub-second precision up to microseconds.
///
/// Decodes at microsecond resolution and returns the coarsest precision among
/// whole seconds, milliseconds, and microseconds whose re-encoding through
/// [`datetime_to_serial_for`] reproduces the exact `f64` serial. Sub-second
/// microsecond precision is exact before ~2079-06-06 (serial 65,536 in the 1900
/// system, where IEEE-754 `f64` fractional-day spacing crosses 1 µs), while
/// whole-second and millisecond datetimes round-trip exactly across all eras
/// through year 9999. A rounded value of 24:00:00.000000 carries into the next
/// serial day and is rejected if it exceeds Excel's maximum date.
pub fn try_serial_to_datetime_subsec_for(
    system: DateSystem,
    serial: f64,
) -> Result<NaiveDateTime, ExcelError> {
    validate_excel_serial(system, serial)?;

    let mut whole_days = serial.trunc() as i64;
    let mut total_micros = (serial.fract() * MICROS_PER_DAY).round() as u64;
    if total_micros >= MICROS_PER_DAY_U64 {
        whole_days = whole_days.checked_add(1).ok_or_else(ExcelError::new_num)?;
        if whole_days as f64 > max_excel_serial_for(system) {
            return Err(ExcelError::new_num());
        }
        total_micros = 0;
    }

    if !total_micros.is_multiple_of(1_000_000) {
        for unit_micros in [1_000_000_u64, 1_000_u64] {
            let rounded_micros = ((total_micros + unit_micros / 2) / unit_micros) * unit_micros;
            if let Ok(candidate) = datetime_from_serial_micros(system, whole_days, rounded_micros)
                && datetime_to_serial_for(system, &candidate) == serial
            {
                return Ok(candidate);
            }
        }
    }

    datetime_from_serial_micros(system, whole_days, total_micros)
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
    fn time_fraction_preserves_subsecond_precision() {
        let half_sec = NaiveTime::from_hms_milli_opt(12, 0, 0, 500).unwrap();
        let frac = time_to_fraction(&half_sec);
        assert!(frac > 0.5);
        assert_eq!(fraction_to_time(frac), Some(half_sec));

        let exact_ns = NaiveTime::from_hms_nano_opt(12, 0, 0, 123_456_789).unwrap();
        assert_eq!(
            fraction_to_time(time_to_fraction(&exact_ns)),
            Some(exact_ns)
        );

        // Whole seconds remain bit-exact.
        let noon = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        assert_eq!(time_to_fraction(&noon), 0.5);

        // Near-end-of-day sub-second stays strictly < 1.0.
        let late = NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999).unwrap();
        let late_frac = time_to_fraction(&late);
        assert!(late_frac < 1.0);
        assert_eq!(fraction_to_time(late_frac), Some(late));
    }

    #[test]
    fn datetime_and_duration_subsecond_roundtrip() {
        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let dt = date(2024, 1, 1).and_hms_milli_opt(12, 0, 0, 500).unwrap();
            let serial = datetime_to_serial_for(system, &dt);
            assert_eq!(
                try_serial_to_datetime_subsec_for(system, serial).unwrap(),
                dt
            );

            let dt_us = date(2024, 1, 1)
                .and_hms_micro_opt(12, 0, 0, 123_456)
                .unwrap();
            let serial_us = datetime_to_serial_for(system, &dt_us);
            assert_eq!(
                try_serial_to_datetime_subsec_for(system, serial_us).unwrap(),
                dt_us
            );
        }

        // Duration 0.123456789 days round-trips identically to nanosecond precision.
        let expected_dur = days_fraction_to_duration(0.123456789).unwrap();
        assert_eq!(
            expected_dur,
            ChronoDuration::nanoseconds(10_666_666_569_600)
        );
        let rt_days = duration_to_days_fraction(&expected_dur);
        assert_eq!(days_fraction_to_duration(rt_days), Some(expected_dur));

        // Negative sub-second duration round-trips cleanly.
        let neg = ChronoDuration::milliseconds(-1_250);
        assert_eq!(
            days_fraction_to_duration(duration_to_days_fraction(&neg)),
            Some(neg)
        );
    }

    #[test]
    fn far_future_whole_second_and_millisecond_datetimes_roundtrip_exactly() {
        let cases = [
            // Whole-second timestamps across post-2079 eras (including review repros)
            date(2080, 1, 1).and_hms_opt(0, 0, 1).unwrap(),
            date(2080, 6, 15).and_hms_opt(13, 45, 59).unwrap(),
            date(2091, 11, 15).and_hms_opt(8, 29, 37).unwrap(),
            date(2164, 7, 17).and_hms_opt(18, 41, 57).unwrap(),
            date(2200, 3, 21).and_hms_opt(7, 11, 19).unwrap(),
            date(5000, 8, 9).and_hms_opt(22, 33, 44).unwrap(),
            date(9999, 12, 31).and_hms_opt(23, 59, 59).unwrap(),
            // Millisecond timestamps across post-2079 eras through year 9999
            date(2080, 1, 1).and_hms_milli_opt(12, 34, 56, 789).unwrap(),
            date(2091, 11, 15)
                .and_hms_milli_opt(8, 29, 37, 250)
                .unwrap(),
            date(2200, 3, 21).and_hms_milli_opt(7, 11, 19, 1).unwrap(),
            date(5000, 8, 9).and_hms_milli_opt(22, 33, 44, 555).unwrap(),
            date(9999, 12, 31)
                .and_hms_milli_opt(23, 59, 59, 999)
                .unwrap(),
        ];

        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            for dt in cases {
                let serial = datetime_to_serial_for(system, &dt);
                assert_eq!(
                    try_serial_to_datetime_subsec_for(system, serial).unwrap(),
                    dt,
                    "system={system:?} dt={dt}"
                );
            }
        }
    }

    #[test]
    fn datetime_max_clamps_within_excel_calendar_domain() {
        let dt_max = date(9999, 12, 31)
            .and_hms_micro_opt(23, 59, 59, 999_999)
            .unwrap();

        for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
            let max_serial = max_excel_serial_for(system);
            let serial = datetime_to_serial_for(system, &dt_max);
            assert!(
                serial < max_serial + 1.0,
                "system={system:?} serial={serial} exceeded max day"
            );
            assert_eq!(serial.trunc(), max_serial, "system={system:?}");

            let decoded = try_serial_to_datetime_subsec_for(system, serial).unwrap();
            assert_eq!(decoded.date(), EXCEL_MAX_DATE, "system={system:?}");

            let parts = try_serial_to_display_date_parts_for(system, serial).unwrap();
            assert_eq!(
                parts,
                ExcelDateParts {
                    year: 9999,
                    month: 12,
                    day: 31,
                },
                "system={system:?}"
            );
        }
    }

    #[test]
    fn long_durations_roundtrip_without_nanosecond_noise() {
        let day_counts = [30_i64, 64, 104, 3_650, 8_192];
        let sub_offsets = [
            ChronoDuration::seconds(1),
            ChronoDuration::seconds(3_723),
            ChronoDuration::milliseconds(1_250),
            ChronoDuration::milliseconds(500),
            ChronoDuration::microseconds(123_456),
            ChronoDuration::microseconds(1),
        ];

        for &days in &day_counts {
            for offset in &sub_offsets {
                let pos = ChronoDuration::days(days) + *offset;
                let neg = -pos;
                for dur in [pos, neg] {
                    let frac = duration_to_days_fraction(&dur);
                    assert_eq!(
                        days_fraction_to_duration(frac),
                        Some(dur),
                        "days={days} offset={offset:?} dur={dur:?}"
                    );
                }
            }
        }
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
        assert_eq!(
            parse_excel_date_text("February 2024"),
            Some(date(2024, 2, 1))
        );
        assert_eq!(parse_excel_date_text("July 2000"), Some(date(2000, 7, 1)));
    }

    #[test]
    fn month_year_only_requires_a_four_digit_year() {
        // A one- or two-digit trailing token is a day, not a year: the
        // month-year form is held to an unambiguous four-digit year so that
        // "Jan 3" is not silently read as 2003-01-01 (#290). Excel and
        // LibreOffice reject the month-plus-day-without-year shape.
        assert_eq!(parse_excel_date_text("Jan 3"), None);
        assert_eq!(parse_excel_date_text("Mar 05"), None);
        assert_eq!(parse_excel_date_text("Dec 99"), None);
    }

    #[test]
    fn malformed_single_colon_time_with_dot_is_rejected() {
        // "12:00.5" has a single colon, so the dot does not terminate an
        // HH:MM:SS field. It must not be silently accepted as 12:00 (#290).
        assert_eq!(parse_excel_time_text("12:00.5"), None);
        // The well-formed HH:MM:SS.f case still truncates.
        assert_eq!(
            parse_excel_time_text("12:00:00.5"),
            Some(NaiveTime::from_hms_opt(12, 0, 0).unwrap())
        );
    }
}
