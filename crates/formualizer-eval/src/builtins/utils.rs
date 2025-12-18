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
    let v = args[0].value()?.into_literal();
    match v {
        LiteralValue::Error(e) => Err(e),
        other => coerce_num(&other),
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
    let a = args[0].value()?.into_literal();
    let b = args[1].value()?.into_literal();
    let a_num = match a {
        LiteralValue::Error(e) => return Err(e),
        other => coerce_num(&other)?,
    };
    let b_num = match b {
        LiteralValue::Error(e) => return Err(e),
        other => coerce_num(&other)?,
    };
    Ok((a_num, b_num))
}

fn calc_from_literal<'b>(
    v: LiteralValue,
    date_system: crate::engine::DateSystem,
) -> crate::traits::CalcValue<'b> {
    match v {
        LiteralValue::Array(rows) => crate::traits::CalcValue::Range(
            crate::engine::range_view::RangeView::from_owned_rows(rows, date_system),
        ),
        other => crate::traits::CalcValue::Scalar(other),
    }
}

pub fn unary_numeric_elementwise<'a, 'b, F>(
    args: &'a [crate::traits::ArgumentHandle<'a, 'b>],
    ctx: &dyn crate::traits::FunctionContext<'b>,
    mut f: F,
) -> Result<crate::traits::CalcValue<'b>, ExcelError>
where
    F: FnMut(f64) -> Result<LiteralValue, ExcelError>,
{
    if args.len() != 1 {
        return Err(ExcelError::new_value()
            .with_message(format!("Expected 1 argument, got {}", args.len())));
    }

    let shape = if let Ok(rv) = args[0].range_view() {
        rv.dims()
    } else if let Ok(cv) = args[0].value() {
        match cv.into_literal() {
            LiteralValue::Array(arr) => (arr.len(), arr.first().map(|r| r.len()).unwrap_or(0)),
            _ => (1, 1),
        }
    } else {
        (1, 1)
    };

    if shape != (1, 1) {
        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(shape.0);
        if let Ok(view) = args[0].range_view() {
            view.for_each_row(&mut |row| {
                let mut out_row: Vec<LiteralValue> = Vec::with_capacity(row.len());
                for cell in row.iter() {
                    let num_opt = match cell {
                        LiteralValue::Error(e) => return Err(e.clone()),
                        other => {
                            crate::coercion::to_number_lenient_with_locale(other, &ctx.locale())
                                .ok()
                        }
                    };
                    match num_opt {
                        Some(n) => out_row.push(f(n)?),
                        None => out_row.push(LiteralValue::Error(
                            ExcelError::new_value()
                                .with_message("Element is not coercible to number"),
                        )),
                    }
                }
                out.push(out_row);
                Ok(())
            })?;
        } else {
            let v = args[0].value()?.into_literal();
            let LiteralValue::Array(arr) = v else {
                // Defensive: if shape says array but value isn't, treat as scalar.
                let x = unary_numeric_arg(args)?;
                return Ok(calc_from_literal(f(x)?, ctx.date_system()));
            };

            for row in arr {
                let mut out_row: Vec<LiteralValue> = Vec::with_capacity(row.len());
                for cell in row {
                    let num_opt = match &cell {
                        LiteralValue::Error(e) => return Err(e.clone()),
                        other => {
                            crate::coercion::to_number_lenient_with_locale(other, &ctx.locale())
                                .ok()
                        }
                    };
                    match num_opt {
                        Some(n) => out_row.push(f(n)?),
                        None => out_row.push(LiteralValue::Error(
                            ExcelError::new_value()
                                .with_message("Element is not coercible to number"),
                        )),
                    }
                }
                out.push(out_row);
            }
        }

        return Ok(calc_from_literal(
            LiteralValue::Array(out),
            ctx.date_system(),
        ));
    }

    let x = unary_numeric_arg(args)?;
    Ok(calc_from_literal(f(x)?, ctx.date_system()))
}

pub fn binary_numeric_elementwise<'a, 'b, F>(
    args: &'a [crate::traits::ArgumentHandle<'a, 'b>],
    ctx: &dyn crate::traits::FunctionContext<'b>,
    mut f: F,
) -> Result<crate::traits::CalcValue<'b>, ExcelError>
where
    F: FnMut(f64, f64) -> Result<LiteralValue, ExcelError>,
{
    if args.len() != 2 {
        return Err(ExcelError::new_value()
            .with_message(format!("Expected 2 arguments, got {}", args.len())));
    }

    use crate::broadcast::{broadcast_shape, project_index};

    enum Grid<'b> {
        Range(crate::engine::range_view::RangeView<'b>),
        Array(Vec<Vec<LiteralValue>>),
        Scalar(LiteralValue),
    }

    impl<'b> Grid<'b> {
        fn shape(&self) -> (usize, usize) {
            match self {
                Grid::Range(rv) => rv.dims(),
                Grid::Array(arr) => (arr.len(), arr.first().map(|r| r.len()).unwrap_or(0)),
                Grid::Scalar(_) => (1, 1),
            }
        }

        fn get(&self, r: usize, c: usize) -> LiteralValue {
            match self {
                Grid::Range(rv) => rv.get_cell(r, c),
                Grid::Array(arr) => arr
                    .get(r)
                    .and_then(|row| row.get(c))
                    .cloned()
                    .unwrap_or(LiteralValue::Empty),
                Grid::Scalar(v) => v.clone(),
            }
        }
    }

    fn to_grid<'a, 'b>(ah: &crate::traits::ArgumentHandle<'a, 'b>) -> Result<Grid<'b>, ExcelError> {
        if let Ok(rv) = ah.range_view() {
            return Ok(Grid::Range(rv));
        }
        let v = ah.value()?.into_literal();
        Ok(match v {
            LiteralValue::Array(arr) => Grid::Array(arr),
            other => Grid::Scalar(other),
        })
    }

    let g0 = to_grid(&args[0])?;
    let g1 = to_grid(&args[1])?;
    let s0 = g0.shape();
    let s1 = g1.shape();
    let target = broadcast_shape(&[s0, s1])?;

    if target != (1, 1) {
        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(target.0);
        for r in 0..target.0 {
            let mut out_row = Vec::with_capacity(target.1);
            for c in 0..target.1 {
                let (r0, c0) = project_index((r, c), s0);
                let (r1, c1) = project_index((r, c), s1);
                let lv0 = g0.get(r0, c0);
                let lv1 = g1.get(r1, c1);

                let n0 = match &lv0 {
                    LiteralValue::Error(e) => return Err(e.clone()),
                    other => {
                        crate::coercion::to_number_lenient_with_locale(other, &ctx.locale()).ok()
                    }
                };
                let n1 = match &lv1 {
                    LiteralValue::Error(e) => return Err(e.clone()),
                    other => {
                        crate::coercion::to_number_lenient_with_locale(other, &ctx.locale()).ok()
                    }
                };

                let out_cell = match (n0, n1) {
                    (Some(a), Some(b)) => f(a, b)?,
                    _ => LiteralValue::Error(
                        ExcelError::new_value()
                            .with_message("Elements are not coercible to numbers"),
                    ),
                };
                out_row.push(out_cell);
            }
            out.push(out_row);
        }
        return Ok(calc_from_literal(
            LiteralValue::Array(out),
            ctx.date_system(),
        ));
    }

    let (a, b) = binary_numeric_args(args)?;
    Ok(calc_from_literal(f(a, b)?, ctx.date_system()))
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

pub fn collapse_if_scalar(
    rows: Vec<Vec<LiteralValue>>,
    date_system: crate::engine::DateSystem,
) -> crate::traits::CalcValue<'static> {
    if rows.len() == 1 && rows[0].len() == 1 {
        crate::traits::CalcValue::Scalar(rows[0][0].clone())
    } else {
        crate::traits::CalcValue::Range(crate::engine::range_view::RangeView::from_owned_rows(
            rows,
            date_system,
        ))
    }
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
