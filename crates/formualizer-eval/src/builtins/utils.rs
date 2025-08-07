use formualizer_common::{ExcelError, LiteralValue};

/// Small epsilon used to detect near-zero denominators in trig/hyperbolic functions.
pub const EPSILON_NEAR_ZERO: f64 = 1e-12;

/// Coerce a `LiteralValue` to `f64` using Excel semantics.
/// - Number/Int map to f64
/// - Boolean maps to 1.0/0.0
/// - Empty maps to 0.0
/// - Others -> `#VALUE!`
pub fn coerce_num(value: &LiteralValue) -> Result<f64, ExcelError> {
    match value {
        LiteralValue::Number(n) => Ok(*n),
        LiteralValue::Int(i) => Ok(*i as f64),
        LiteralValue::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        _ => Err(ExcelError::from_error_string("#VALUE!")
            .with_message(format!("Cannot convert {value:?} to number"))),
    }
}

/// Get a single numeric argument, with count and error checks.
pub fn unary_numeric_arg<'a, 'b>(
    args: &'a [crate::traits::ArgumentHandle<'a, 'b>],
) -> Result<f64, ExcelError> {
    if args.len() != 1 {
        return Err(
            ExcelError::from_error_string("#VALUE!")
                .with_message(format!("Expected 1 argument, got {}", args.len())),
        );
    }
    let v = args[0].value()?;
    match v.as_ref() {
        LiteralValue::Error(e) => Err(e.clone()),
        other => coerce_num(other),
    }
}

/// Get two numeric arguments, with count and error checks.
pub fn binary_numeric_args<'a, 'b>(
    args: &'a [crate::traits::ArgumentHandle<'a, 'b>],
) -> Result<(f64, f64), ExcelError> {
    if args.len() != 2 {
        return Err(
            ExcelError::from_error_string("#VALUE!")
                .with_message(format!("Expected 2 arguments, got {}", args.len())),
        );
    }
    let a = args[0].value()?;
    let b = args[1].value()?;
    let a_num = match a.as_ref() {
        LiteralValue::Error(e) => return Err(e.clone()),
        other => coerce_num(other)?,
    };
    let b_num = match b.as_ref() {
        LiteralValue::Error(e) => return Err(e.clone()),
        other => coerce_num(other)?,
    };
    Ok((a_num, b_num))
}

/// Forward-looking: clamp numeric result to Excel-friendly finite values.
/// Converts NaN to `#NUM!` and +/-Inf to large finite sentinels if desired.
pub fn sanitize_numeric_result(n: f64) -> Result<f64, ExcelError> {
    if n.is_nan() {
        return Err(ExcelError::from_error_string("#NUM!"));
    }
    Ok(n)
}

/// Forward-looking: try converting text that looks like a number (Excel often parses text numbers).
pub fn coerce_text_to_number_maybe(value: &LiteralValue) -> Option<f64> {
    if let LiteralValue::Text(s) = value {
        if let Ok(i) = s.trim().parse::<i64>() {
            return Some(i as f64);
        }
        if let Ok(f) = s.trim().parse::<f64>() {
            return Some(f);
        }
    }
    None
}

/// Forward-looking: common rounding strategy for functions requiring specific rounding.
pub fn round_to_precision(n: f64, digits: i32) -> f64 {
    if digits <= 0 { return n.round(); }
    let factor = 10f64.powi(digits);
    (n * factor).round() / factor
}


