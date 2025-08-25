use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

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
        LiteralValue::Text(s) => s.trim().parse::<f64>().map_err(|_| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("Cannot convert '{s}' to number"))
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
