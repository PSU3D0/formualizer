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
        return Err(ExcelError::new_value()
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
        return Err(ExcelError::new_value()
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

// ─────────────────────────────── Criteria helpers (shared by *IF* aggregators) ───────────────────────────────

/// Match a value against a parsed `CriteriaPredicate` (see `crate::args::CriteriaPredicate`).
/// Implements Excel-style semantics for equality (case-insensitive text, lenient numeric),
/// inequality comparisons with numeric coercion, wildcard text matching, and type tests.
pub fn criteria_match(pred: &crate::args::CriteriaPredicate, v: &LiteralValue) -> bool {
    use crate::args::CriteriaPredicate as P;
    match pred {
        P::Eq(t) => values_equal_invariant(t, v),
        P::Ne(t) => !values_equal_invariant(t, v),
        P::Gt(n) => value_to_number(v).map(|x| x > *n).unwrap_or(false),
        P::Ge(n) => value_to_number(v).map(|x| x >= *n).unwrap_or(false),
        P::Lt(n) => value_to_number(v).map(|x| x < *n).unwrap_or(false),
        P::Le(n) => value_to_number(v).map(|x| x <= *n).unwrap_or(false),
        P::TextLike {
            pattern,
            case_insensitive,
        } => text_like_match(pattern, *case_insensitive, v),
        P::IsBlank => matches!(v, LiteralValue::Empty),
        P::IsNumber => value_to_number(v).is_ok(),
        P::IsText => matches!(v, LiteralValue::Text(_)),
        P::IsLogical => matches!(v, LiteralValue::Boolean(_)),
    }
}

fn value_to_number(v: &LiteralValue) -> Result<f64, ExcelError> {
    crate::coercion::to_number_lenient(v)
}

fn values_equal_invariant(a: &LiteralValue, b: &LiteralValue) -> bool {
    match (a, b) {
        (LiteralValue::Number(x), LiteralValue::Number(y)) => (x - y).abs() < 1e-12,
        (LiteralValue::Int(x), LiteralValue::Int(y)) => x == y,
        (LiteralValue::Boolean(x), LiteralValue::Boolean(y)) => x == y,
        (LiteralValue::Text(x), LiteralValue::Text(y)) => x.eq_ignore_ascii_case(y),
        // Treat blank and empty text as equal (Excel semantics)
        (LiteralValue::Text(x), LiteralValue::Empty) if x.is_empty() => true,
        (LiteralValue::Empty, LiteralValue::Text(y)) if y.is_empty() => true,
        (LiteralValue::Empty, LiteralValue::Empty) => true,
        (LiteralValue::Number(x), _) => value_to_number(b)
            .map(|y| (x - y).abs() < 1e-12)
            .unwrap_or(false),
        (_, LiteralValue::Number(_)) => values_equal_invariant(b, a),
        _ => false,
    }
}

fn text_like_match(pattern: &str, case_insensitive: bool, v: &LiteralValue) -> bool {
    let s = match v {
        LiteralValue::Text(t) => t.clone(),
        LiteralValue::Number(n) => n.to_string(),
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Boolean(b) => {
            if *b {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        LiteralValue::Empty => String::new(),
        _ => return false,
    };
    let (pat, text) = if case_insensitive {
        (pattern.to_ascii_lowercase(), s.to_ascii_lowercase())
    } else {
        (pattern.to_string(), s)
    };

    // Fast-path for anchored patterns without '?' or escape sequences
    if !pat.contains('?') && !pat.contains("~*") && !pat.contains("~?") {
        // Pattern like "text*" - starts with
        if pat.ends_with('*') && !pat[..pat.len() - 1].contains('*') {
            return text.starts_with(&pat[..pat.len() - 1]);
        }
        // Pattern like "*text" - ends with
        if pat.starts_with('*') && !pat[1..].contains('*') {
            return text.ends_with(&pat[1..]);
        }
        // Pattern like "*text*" - contains
        if pat.starts_with('*') && pat.ends_with('*') && !pat[1..pat.len() - 1].contains('*') {
            return text.contains(&pat[1..pat.len() - 1]);
        }
        // Pattern with no wildcards - exact match
        if !pat.contains('*') {
            return text == pat;
        }
    }

    // Fall back to general wildcard matching for complex patterns
    wildcard_match(&pat, &text)
}

fn wildcard_match(pat: &str, text: &str) -> bool {
    // Simple glob-like matcher for * and ? (non-greedy backtracking).
    fn helper(p: &[u8], t: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            b'*' => {
                for i in 0..=t.len() {
                    if helper(&p[1..], &t[i..]) {
                        return true;
                    }
                }
                false
            }
            b'?' => {
                if t.is_empty() {
                    false
                } else {
                    helper(&p[1..], &t[1..])
                }
            }
            ch => {
                if t.first().copied() == Some(ch) {
                    helper(&p[1..], &t[1..])
                } else {
                    false
                }
            }
        }
    }
    helper(pat.as_bytes(), text.as_bytes())
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
pub static ARG_NUM_LENIENT_ONE: LazyLock<Vec<ArgSchema>> =
    LazyLock::new(|| vec![{ ArgSchema::number_lenient_scalar() }]);

/// Two numeric scalar arguments, with lenient text-to-number coercion.
/// Suited for binary numeric operations (e.g., `ATAN2`, `POWER`, `LOG(base)`).
pub static ARG_NUM_LENIENT_TWO: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
    vec![{ ArgSchema::number_lenient_scalar() }, {
        ArgSchema::number_lenient_scalar()
    }]
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
