use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use formualizer_common::{ExcelError, LiteralValue};
use std::sync::LazyLock;

/// Small epsilon used to detect near-zero denominators in trig/hyperbolic functions.
pub const EPSILON_NEAR_ZERO: f64 = 1e-12;

/// Coerce a `LiteralValue` to `f64` using Excel semantics.
/// - Number/Int map to f64
/// - Boolean maps to 1.0/0.0
/// - Empty maps to 0.0
/// - Others -> `#VALUE!`
pub fn coerce_num(value: &LiteralValue) -> Result<f64, ExcelError> {
    crate::coercion::to_number_lenient(value)
}

/// Get a single numeric argument, with count and error checks.
pub fn unary_numeric_arg<'a, 'b>(
    args: &'a [crate::traits::ArgumentHandle<'a, 'b>],
) -> Result<f64, ExcelError> {
    if args.len() != 1 {
        return Err(ExcelError::from_error_string("#VALUE!")
            .with_message(format!("Expected 1 argument, got {}", args.len())));
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
        return Err(ExcelError::from_error_string("#VALUE!")
            .with_message(format!("Expected 2 arguments, got {}", args.len())));
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
    crate::coercion::sanitize_numeric(n)
}

/// Forward-looking: try converting text that looks like a number (Excel often parses text numbers).
pub fn coerce_text_to_number_maybe(value: &LiteralValue) -> Option<f64> {
    match value {
        LiteralValue::Text(_) => crate::coercion::to_number_lenient(value).ok(),
        _ => None,
    }
}

/// Forward-looking: common rounding strategy for functions requiring specific rounding.
pub fn round_to_precision(n: f64, digits: i32) -> f64 {
    if digits <= 0 {
        return n.round();
    }
    let factor = 10f64.powi(digits);
    (n * factor).round() / factor
}

// ─────────────────────────────── ArgSchema presets ───────────────────────────────

/// Single scalar argument of any type.
/// Used by many unary or variadic-any functions (e.g., `LEN`, `TYPE`, simple wrappers).
pub static ARG_ANY_ONE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| vec![ArgSchema::any()]);

/// Two scalar arguments of any type.
/// Used by generic binary functions (e.g., comparisons, concatenation variants).
pub static ARG_ANY_TWO: LazyLock<Vec<ArgSchema>> =
    LazyLock::new(|| vec![ArgSchema::any(), ArgSchema::any()]);

/// Single numeric scalar argument, with lenient text-to-number coercion.
/// Ideal for elementwise numeric functions (e.g., `SIN`, `COS`, `ABS`).
pub static ARG_NUM_LENIENT_ONE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
    vec![{
        let s = ArgSchema::number_lenient_scalar();
        s
    }]
});

/// Two numeric scalar arguments, with lenient text-to-number coercion.
/// Suited for binary numeric operations (e.g., `ATAN2`, `POWER`, `LOG(base)`).
pub static ARG_NUM_LENIENT_TWO: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
    vec![
        {
            let s = ArgSchema::number_lenient_scalar();
            s
        },
        {
            let s = ArgSchema::number_lenient_scalar();
            s
        },
    ]
});

/// Single range argument, numeric semantics with lenient text-to-number coercion.
/// Best for reductions over ranges (e.g., `SUM`, `AVERAGE`, `COUNT`-like families).
pub static ARG_RANGE_NUM_LENIENT_ONE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
    vec![{
        let mut s = ArgSchema::number_lenient_scalar();
        s.shape = ShapeKind::Range;
        s.coercion = CoercionPolicy::NumberLenientText;
        s
    }]
});
