use formualizer_common::{DateSystem, ExcelError, ExcelErrorKind, LiteralValue};

/// Centralized coercion and error policy utilities (Milestone 7).
/// These functions implement invariant, Excel-compatible coercions and
/// numeric sanitization. They should be used by the interpreter, builtins,
/// and evaluation pipelines (map/fold/window) instead of ad-hoc parsing.
/// Strict numeric coercion.
/// - Accepts Number/Int/Boolean/Empty/Date-like serial-bearing variants
/// - Rejects Text (returns #VALUE!)
pub fn to_number_strict(value: &LiteralValue) -> Result<f64, ExcelError> {
    match value {
        LiteralValue::Number(n) => Ok(*n),
        LiteralValue::Int(i) => Ok(*i as f64),
        LiteralValue::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        // Date/time/duration map to serials
        other if other.as_serial_number().is_some() => Ok(other.as_serial_number().unwrap()),
        LiteralValue::Error(e) => Err(e.clone()),
        _ => Err(ExcelError::new(ExcelErrorKind::Value)
            .with_message("Cannot convert to number (strict)")),
    }
}

/// Lenient numeric coercion.
/// - As strict, but also parses numeric text using ASCII/invariant rules
pub fn to_number_lenient(value: &LiteralValue) -> Result<f64, ExcelError> {
    match value {
        LiteralValue::Text(s) => crate::locale::Locale::invariant()
            .parse_number_invariant(s)
            .ok_or_else(|| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("Cannot convert '{s}' to number"))
            }),
        _ => to_number_strict(value),
    }
}

/// Date or time text as a serial, the way arithmetic, date functions and
/// holiday lists read a text cell, so `YEAR(A1)` and `A1+0` agree. The text is
/// read uppercased, so that the result does not depend on case: the parser
/// takes only an uppercase ISO `T`, while month names and AM/PM are
/// case-insensitive anyway. So `"1999-10-26t00:00"` is a date, as in Google
/// Sheets.
///
/// Where Excel 2609 errors but Google Sheets reads the text, this follows
/// Sheets (#576): ISO text with a `T` separator (`"1999-10-26T00:00"`) is a
/// date, and a date before the epoch (`"1/1/1899"`) is its negative serial,
/// which the date functions then reject with #NUM! (Excel: #VALUE!).
pub(crate) fn date_text_serial(system: DateSystem, text: &str) -> Option<f64> {
    if !text.bytes().any(|b| b.is_ascii_digit()) {
        return None;
    }
    let text = if text.contains('t') {
        std::borrow::Cow::Owned(text.to_ascii_uppercase())
    } else {
        std::borrow::Cow::Borrowed(text)
    };
    formualizer_common::parse_excel_datetime_text_to_serial_for(system, &text)
}

/// Lenient serial coercion for arguments that Excel reads as a date.
///
/// As [`to_number_lenient`], except that date-bearing literals are converted
/// to serials in the workbook's date system, and text that is not a number is
/// parsed as date/time text (`"10/26/1999"`, `"1999-10-27"`), as Excel's date
/// functions do (`YEAR("10/26/1999")` is 1999). The text parse is the one the
/// arithmetic operators use, so `YEAR(A1)` and `A1+0` agree on what a date
/// text cell means.
pub fn to_serial_lenient(value: &LiteralValue, system: DateSystem) -> Result<f64, ExcelError> {
    match value {
        LiteralValue::Date(_)
        | LiteralValue::DateTime(_)
        | LiteralValue::Time(_)
        | LiteralValue::Duration(_) => value.as_serial_number_for(system).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message("Cannot convert to date/time serial")
        }),
        LiteralValue::Text(s) => crate::locale::Locale::invariant()
            .parse_number_invariant(s)
            .or_else(|| date_text_serial(system, s))
            .ok_or_else(|| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("Cannot convert '{s}' to a date serial"))
            }),
        _ => to_number_lenient(value),
    }
}

/// Strict numeric coercion that resolves temporal values in a date system.
///
/// Identical to [`to_number_strict`] except that date-bearing literals are
/// converted to serials using the workbook's date system instead of the
/// implicit Excel-1900 default. Unlike [`to_serial_lenient`] it does **not**
/// parse numeric text, so callers that reject text today keep rejecting it.
///
/// This is the coercion financial builtins want for arguments Excel treats as
/// plain numbers on the sheet: a date cell *is* a number there, so a
/// `Date`/`DateTime`/`Time`/`Duration` literal must become its serial rather
/// than being dropped or rejected.
pub(crate) fn to_serial_strict(
    value: &LiteralValue,
    system: DateSystem,
) -> Result<f64, ExcelError> {
    match value {
        LiteralValue::Date(_)
        | LiteralValue::DateTime(_)
        | LiteralValue::Time(_)
        | LiteralValue::Duration(_) => value.as_serial_number_for(system).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message("Cannot convert to date/time serial")
        }),
        _ => to_number_strict(value),
    }
}

/// Context-aware lenient numeric coercion using locale.
pub fn to_number_lenient_with_locale(
    value: &LiteralValue,
    loc: &crate::locale::Locale,
) -> Result<f64, ExcelError> {
    match value {
        LiteralValue::Text(s) => loc.parse_number_invariant(s).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("Cannot convert '{s}' to number"))
        }),
        _ => to_number_strict(value),
    }
}

/// Numeric coercion for arithmetic operators.
///
/// This deliberately extends only the operator boundary: numeric text is
/// parsed first using the engine's invariant locale, then date/time text is
/// parsed by `formualizer-common` and encoded in the workbook's date system.
/// Aggregate arguments, comparisons, criteria, and functions such as `N`
/// continue to use their existing coercion policies.
pub(crate) fn to_arithmetic_number_with_locale(
    value: &LiteralValue,
    loc: &crate::locale::Locale,
    system: DateSystem,
) -> Result<f64, ExcelError> {
    match value {
        LiteralValue::Text(s) => loc
            .parse_number_invariant(s)
            .or_else(|| date_text_serial(system, s))
            .ok_or_else(|| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("Cannot convert '{s}' to arithmetic operand"))
            }),
        LiteralValue::Date(_)
        | LiteralValue::DateTime(_)
        | LiteralValue::Time(_)
        | LiteralValue::Duration(_) => value.as_serial_number_for(system).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message("Cannot convert date/time to arithmetic operand")
        }),
        _ => to_number_strict(value),
    }
}

/// Logical coercion.
/// - Accepts Boolean
/// - Numbers: nonzero → true, zero → false
/// - Text: "TRUE"/"FALSE" (ASCII case-insensitive)
pub fn to_logical(value: &LiteralValue) -> Result<bool, ExcelError> {
    match value {
        LiteralValue::Boolean(b) => Ok(*b),
        LiteralValue::Number(n) => Ok(*n != 0.0),
        LiteralValue::Int(i) => Ok(*i != 0),
        LiteralValue::Text(s) => match s.to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Cannot convert text to logical")),
        },
        LiteralValue::Empty => Ok(false),
        LiteralValue::Error(e) => Err(e.clone()),
        _ => Err(ExcelError::new(ExcelErrorKind::Value).with_message("Cannot convert to logical")),
    }
}

/// Invariant textification for comparisons/concatenation.
pub fn to_text_invariant(value: &LiteralValue) -> String {
    match value {
        LiteralValue::Text(s) => s.clone(),
        LiteralValue::Number(n) => n.to_string(),
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        LiteralValue::Error(e) => e.to_string(),
        LiteralValue::Empty => "".into(),
        // Dates/times/durations are stored as serial numbers in spreadsheet engines.
        // Use invariant numeric serialization so downstream consumers (e.g., criteria strings
        // like ">="&A1) parse consistently.
        LiteralValue::Date(_)
        | LiteralValue::DateTime(_)
        | LiteralValue::Time(_)
        | LiteralValue::Duration(_) => value.as_serial_number().unwrap_or(0.0).to_string(),
        other => format!("{other:?}"),
    }
}

/// Numeric sanitization: NaN/Inf → #NUM!
pub fn sanitize_numeric(n: f64) -> Result<f64, ExcelError> {
    if n.is_nan() || n.is_infinite() {
        return Err(ExcelError::new_num());
    }
    Ok(n)
}

/// Coerce to Excel serial (date/time/duration) or error.
pub fn to_datetime_serial(value: &LiteralValue) -> Result<f64, ExcelError> {
    match value.as_serial_number() {
        Some(n) => Ok(n),
        None => Err(ExcelError::new(ExcelErrorKind::Value)
            .with_message("Cannot convert to date/time serial")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_lenient_parses_text_and_booleans() {
        assert_eq!(
            to_number_lenient(&LiteralValue::Text(" 42 ".into())).unwrap(),
            42.0
        );
        assert_eq!(
            to_number_lenient(&LiteralValue::Boolean(true)).unwrap(),
            1.0
        );
        assert_eq!(to_number_lenient(&LiteralValue::Empty).unwrap(), 0.0);
    }

    #[test]
    fn number_lenient_parses_percent_text() {
        assert_eq!(
            to_number_lenient(&LiteralValue::Text("90%".into())).unwrap(),
            0.9
        );
        assert_eq!(
            to_number_lenient(&LiteralValue::Text(" 90.5% ".into())).unwrap(),
            0.905
        );
        assert!(to_number_lenient(&LiteralValue::Text("abc%".into())).is_err());
    }

    #[test]
    fn number_strict_rejects_text() {
        assert!(to_number_strict(&LiteralValue::Text("1".into())).is_err());
    }

    #[test]
    fn logical_from_number_and_text() {
        assert!(to_logical(&LiteralValue::Int(5)).unwrap());
        assert!(!to_logical(&LiteralValue::Number(0.0)).unwrap());
        assert!(to_logical(&LiteralValue::Text("TRUE".into())).unwrap());
        assert!(to_logical(&LiteralValue::Text("true".into())).unwrap());
        assert!(to_logical(&LiteralValue::Text(" True ".into())).is_err());
    }

    #[test]
    fn sanitize_numeric_nan_inf() {
        assert!(sanitize_numeric(f64::NAN).is_err());
        assert!(sanitize_numeric(f64::INFINITY).is_err());
        assert_eq!(sanitize_numeric(1.5).unwrap(), 1.5);
    }
}
