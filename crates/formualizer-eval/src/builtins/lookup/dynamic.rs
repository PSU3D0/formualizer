//! Dynamic / modern lookup & array helpers: XLOOKUP, FILTER, UNIQUE (initial sprint subset)
//!
//! Notes / Simplifications (documented for future refinement):
//! - XLOOKUP supports: lookup_value, lookup_array, return_array, [if_not_found], [match_mode], [search_mode]
//!   * match_mode: 0 exact (default), -1 exact-or-next-smaller, 1 exact-or-next-larger, 2 wildcard (basic * ?)
//!   * search_mode: 1 forward (default), -1 reverse; (2 / -2 binary not yet implemented -> treated as 1 / -1)
//!   * Wildcard mode (2) currently case-insensitive ASCII only; TODO: full Excel semantics, escape handling (~)
//! - FILTER supports: array, include, [if_empty]; Shapes must be broadcast-compatible by rows (include is 1-D).
//!   * include may be vertical column vector OR same sized 2D; we reduce any non-zero truthy cell to include row.
//!   * if_empty omitted -> #CALC! per Excel when no matches.
//! - UNIQUE supports: array, [by_col], [exactly_once]
//!   * by_col TRUE -> operate column-wise returning unique columns (NYI -> returns #N/IMPL! if TRUE)
//!   * exactly_once TRUE returns only values with count == 1 (supported in row-wise primitive set)
//! - All functions return Array literal values (spills) – engine handles spill placement later.
//!
//! TODO(backlog):
//! - Binary search for XLOOKUP approximate modes; currently linear scan.
//! - Better type coercion parity with Excel (booleans/text vs numbers nuances).
//! - Match unsorted detection for approximate modes (#N/A) and wildcard escaping.
//! - PERFORMANCE: streaming FILTER without full materialization; UNIQUE using smallvec for tiny sets.

use super::lookup_utils::{
    approximate_select_ascending, equals_maybe_wildcard, find_exact_index, guard_sorted_ascending,
};
use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::function::Function; // FnCaps imported via macro
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;
use std::collections::HashMap;

/* ───────────────────────── helpers ───────────────────────── */

fn to_values_flat(
    arg: &ArgumentHandle,
    ctx: &dyn FunctionContext,
) -> Result<Vec<LiteralValue>, ExcelError> {
    if let Ok(r) = arg.as_reference_or_eval() {
        let mut out = Vec::new();
        let sheet = "Sheet1"; // TODO: propagate actual sheet if present in reference
        let rv = ctx.resolve_range_view(&r, sheet)?;
        rv.for_each_cell(&mut |v| {
            out.push(v.clone());
            Ok(())
        })?;
        Ok(out)
    } else {
        Ok(vec![arg.value()?.as_ref().clone()])
    }
}

fn to_rows_2d(
    arg: &ArgumentHandle,
    ctx: &dyn FunctionContext,
) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
    if let Ok(r) = arg.as_reference_or_eval() {
        let mut rows: Vec<Vec<LiteralValue>> = Vec::new();
        let sheet = "Sheet1";
        let rv = ctx.resolve_range_view(&r, sheet)?;
        rv.for_each_row(&mut |row| {
            rows.push(row.to_vec());
            Ok(())
        })?;
        Ok(rows)
    } else {
        match arg.value()?.as_ref() {
            LiteralValue::Array(a) => Ok(a.clone()),
            v => Ok(vec![vec![v.clone()]]),
        }
    }
}

pub fn super_wildcard_match(pattern: &str, text: &str) -> bool {
    // public for shared lookup utils
    // Excel-style wildcards with escape (~): * any seq, ? single char, ~ escapes next (*, ?, ~)
    // Implement non-recursive DP for performance & to support escapes.
    #[derive(Clone, Copy, Debug)]
    enum Token<'a> {
        AnySeq,
        AnyChar,
        Lit(&'a str),
    }
    let mut tokens: Vec<Token> = Vec::new();
    let mut i = 0;
    let bytes = pattern.as_bytes();
    let mut lit_start = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'~' => {
                // escape next if present
                if i + 1 < bytes.len() {
                    // flush pending literal
                    if lit_start < i {
                        tokens.push(Token::Lit(&pattern[lit_start..i]));
                    }
                    tokens.push(Token::Lit(&pattern[i + 1..i + 2]));
                    i += 2;
                    lit_start = i;
                } else {
                    // trailing ~ treated literal
                    i += 1;
                }
            }
            b'*' => {
                if lit_start < i {
                    tokens.push(Token::Lit(&pattern[lit_start..i]));
                }
                tokens.push(Token::AnySeq);
                i += 1;
                lit_start = i;
            }
            b'?' => {
                if lit_start < i {
                    tokens.push(Token::Lit(&pattern[lit_start..i]));
                }
                tokens.push(Token::AnyChar);
                i += 1;
                lit_start = i;
            }
            _ => i += 1,
        }
    }
    if lit_start < bytes.len() {
        tokens.push(Token::Lit(&pattern[lit_start..]));
    }
    // Simplify consecutive AnySeq
    let mut compact: Vec<Token> = Vec::new();
    for t in tokens {
        match t {
            Token::AnySeq => {
                if !matches!(compact.last(), Some(Token::AnySeq)) {
                    compact.push(t);
                }
            }
            _ => compact.push(t),
        }
    }
    // Backtracking matcher
    fn match_tokens<'a>(tokens: &[Token<'a>], text: &str) -> bool {
        fn eq_icase(a: &str, b: &str) -> bool {
            a.eq_ignore_ascii_case(b)
        }
        // Convert Lit tokens into lowercase for quick compare
        let mut ti = 0;
        let tb = tokens;
        // Use manual stack for backtracking when encountering AnySeq
        let mut backtrack: Vec<(usize, usize)> = Vec::new(); // (token_index, text_index after consuming 1 more char by *)
        let text_bytes = text.as_bytes();
        let mut si = 0; // text index
        loop {
            if ti == tb.len() {
                // tokens consumed
                if si == text_bytes.len() {
                    return true;
                }
                // Maybe backtrack
            } else {
                match tb[ti] {
                    Token::AnySeq => {
                        // try to match zero chars first
                        ti += 1;
                        backtrack.push((ti - 1, si + 1));
                        continue;
                    }
                    Token::AnyChar => {
                        if si < text_bytes.len() {
                            ti += 1;
                            si += 1;
                            continue;
                        }
                    }
                    Token::Lit(l) => {
                        let l_len = l.len();
                        if si + l_len <= text_bytes.len() && eq_icase(&text[si..si + l_len], l) {
                            ti += 1;
                            si += l_len;
                            continue;
                        }
                    }
                }
            }
            // failed match; attempt backtrack
            if let Some((tok_star, new_si)) = backtrack.pop() {
                if new_si <= text_bytes.len() {
                    ti = tok_star + 1;
                    si = new_si;
                    continue;
                } else {
                    continue;
                }
            }
            return false;
        }
    }
    match_tokens(&compact, text)
}

fn value_equal(a: &LiteralValue, b: &LiteralValue, wildcard: bool) -> bool {
    match (a, b) {
        (LiteralValue::Int(i1), LiteralValue::Int(i2)) => i1 == i2,
        (LiteralValue::Number(n1), LiteralValue::Number(n2)) => (n1 - n2).abs() < 1e-12,
        (LiteralValue::Int(i), LiteralValue::Number(n))
        | (LiteralValue::Number(n), LiteralValue::Int(i)) => (*i as f64 - *n).abs() < 1e-12,
        (LiteralValue::Text(s1), LiteralValue::Text(s2)) => {
            if wildcard && (s1.contains('*') || s1.contains('?') || s1.contains('~')) {
                super_wildcard_match(s1, s2)
            } else {
                s1.eq_ignore_ascii_case(s2)
            }
        }
        (LiteralValue::Boolean(b1), LiteralValue::Boolean(b2)) => b1 == b2,
        _ => false,
    }
}

/* ───────────────────────── XLOOKUP() ───────────────────────── */

#[derive(Debug)]
pub struct XLookupFn;

impl Function for XLookupFn {
    func_caps!(PURE, LOOKUP);
    fn name(&self) -> &'static str {
        "XLOOKUP"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // lookup_value
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // lookup_array (range)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // return_array (range)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // if_not_found (any optional)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // match_mode (number) default 0
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Int(0)),
                },
                // search_mode (number) default 1
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Int(1)),
                },
            ]
        });
        &SCHEMA
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 3 {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        let lookup_value = args[0].value()?;
        if let LiteralValue::Error(e) = lookup_value.as_ref() {
            return Ok(LiteralValue::Error(e.clone()));
        }
        // Materialize lookup & return arrays with both flat & 2D for orientation logic
        let lookup_rows = to_rows_2d(&args[1], ctx)?; // could be 1 x N or N x 1
        let ret_rows = to_rows_2d(&args[2], ctx)?; // may be multi-column/row slice
        // Derive flat list of candidates (respect row/col orientation). If lookup_rows has 1 row -> horizontal; else if 1 col -> vertical
        let vertical = lookup_rows.len() > 1; // heuristic: multi-row => vertical orientation
        let lookup_vals: Vec<LiteralValue> = if vertical {
            // Each row should have at least 1 col; use first column
            lookup_rows
                .iter()
                .map(|r| r.first().cloned().unwrap_or(LiteralValue::Empty))
                .collect()
        } else {
            // horizontal
            lookup_rows.first().cloned().unwrap_or_default()
        };
        if lookup_vals.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
        }
        let match_mode = if args.len() >= 5 {
            match args[4].value()?.as_ref() {
                LiteralValue::Int(i) => *i,
                LiteralValue::Number(n) => *n as i64,
                _ => 0,
            }
        } else {
            0
        };
        let search_mode = if args.len() >= 6 {
            match args[5].value()?.as_ref() {
                LiteralValue::Int(i) => *i,
                LiteralValue::Number(n) => *n as i64,
                _ => 1,
            }
        } else {
            1
        };
        let wildcard = match_mode == 2;
        let indices: Box<dyn Iterator<Item = usize>> = if search_mode == -1 {
            Box::new((0..lookup_vals.len()).rev())
        } else {
            Box::new(0..lookup_vals.len())
        };
        let mut found: Option<usize> = None;
        if match_mode == 0 || wildcard {
            // respect search direction
            if search_mode == -1 {
                for i in (0..lookup_vals.len()).rev() {
                    if equals_maybe_wildcard(lookup_value.as_ref(), &lookup_vals[i], wildcard) {
                        found = Some(i);
                        break;
                    }
                }
            } else {
                found = find_exact_index(&lookup_vals, lookup_value.as_ref(), wildcard);
            }
        } else if match_mode == -1 || match_mode == 1 {
            if let Err(e) = guard_sorted_ascending(&lookup_vals) {
                return Ok(LiteralValue::Error(e));
            }
            found = approximate_select_ascending(
                &lookup_vals,
                lookup_value.as_ref(),
                match_mode as i32,
            );
        } else {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        if let Some(idx) = found {
            if ret_rows.is_empty() {
                return Ok(LiteralValue::Empty);
            }
            if vertical {
                if idx < ret_rows.len() {
                    let row = &ret_rows[idx];
                    if row.len() == 1 {
                        return Ok(row[0].clone());
                    }
                    return Ok(LiteralValue::Array(vec![row.clone()]));
                }
            } else {
                // horizontal orientation: treat idx as column; build column vector
                let height = ret_rows.len();
                if height == 1 {
                    // single row -> scalar
                    let row = &ret_rows[0];
                    if idx < row.len() {
                        return Ok(row[idx].clone());
                    }
                } else {
                    let mut col: Vec<Vec<LiteralValue>> = Vec::new();
                    for r in &ret_rows {
                        col.push(vec![r.get(idx).cloned().unwrap_or(LiteralValue::Empty)]);
                    }
                    return Ok(LiteralValue::Array(col));
                }
            }
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        if args.len() >= 4 {
            return args[3].value().map(|c| c.into_owned());
        }
        Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)))
    }
}

/* ───────────────────────── FILTER() ───────────────────────── */

#[derive(Debug)]
pub struct FilterFn;
impl Function for FilterFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FILTER"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // array
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // include
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // if_empty optional scalar
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
            ]
        });
        &SCHEMA
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        let rows = to_rows_2d(&args[0], ctx)?;
        if rows.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        let include_rows = to_rows_2d(&args[1], ctx)?;
        let mut result: Vec<Vec<LiteralValue>> = Vec::new();
        let row_count = rows.len();
        for (i, row) in rows.iter().enumerate() {
            let include = if include_rows.len() == row_count {
                include_rows[i].iter().any(|v| v.is_truthy())
            } else if include_rows.len() == 1 {
                include_rows[0].iter().any(|v| v.is_truthy())
            } else {
                return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
            };
            if include {
                result.push(row.clone());
            }
        }
        if result.is_empty() {
            if args.len() >= 3 {
                return args[2].value().map(|c| c.into_owned());
            }
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Calc)));
        }
        if result.len() == 1 && result[0].len() == 1 {
            return Ok(result[0][0].clone());
        }
        Ok(LiteralValue::Array(result))
    }
}

/* ───────────────────────── UNIQUE() ───────────────────────── */

#[derive(Debug)]
pub struct UniqueFn;
impl Function for UniqueFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "UNIQUE"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Logical],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::Logical,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Boolean(false)),
                },
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Logical],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::Logical,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Boolean(false)),
                },
            ]
        });
        &SCHEMA
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let rows = to_rows_2d(&args[0], ctx)?;
        if rows.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        let by_col = if args.len() >= 2 {
            matches!(args[1].value()?.as_ref(), LiteralValue::Boolean(true))
        } else {
            false
        };
        let exactly_once = if args.len() >= 3 {
            matches!(args[2].value()?.as_ref(), LiteralValue::Boolean(true))
        } else {
            false
        };
        if by_col {
            // Transpose columns to vectors and dedup columns
            let width = rows[0].len();
            // Ensure all rows same width
            if rows.iter().any(|r| r.len() != width) {
                return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
            }
            #[derive(Hash, Eq, PartialEq, Clone)]
            struct ColKey(Vec<LiteralValue>);
            let mut order: Vec<ColKey> = Vec::new();
            let mut counts: HashMap<ColKey, usize> = HashMap::new();
            for c in 0..width {
                let key = ColKey(rows.iter().map(|r| r[c].clone()).collect());
                if !counts.contains_key(&key) {
                    order.push(key.clone());
                }
                *counts.entry(key).or_insert(0) += 1;
            }
            let mut out: Vec<Vec<LiteralValue>> = Vec::new();
            for k in order {
                if !exactly_once || counts.get(&k) == Some(&1) {
                    out.push(k.0);
                }
            }
            if out.len() == 1 && out[0].len() == 1 {
                return Ok(out[0][0].clone());
            }
            return Ok(LiteralValue::Array(out));
        }
        // Row-wise uniqueness (entire rows)
        #[derive(Hash, Eq, PartialEq, Clone)]
        struct RowKey(Vec<LiteralValue>);
        let mut order: Vec<RowKey> = Vec::new();
        let mut counts: HashMap<RowKey, usize> = HashMap::new();
        for r in &rows {
            let key = RowKey(r.clone());
            if !counts.contains_key(&key) {
                order.push(key.clone());
            }
            *counts.entry(key).or_insert(0) += 1;
        }
        let mut out: Vec<Vec<LiteralValue>> = Vec::new();
        for k in order {
            if !exactly_once || counts.get(&k) == Some(&1) {
                out.push(k.0);
            }
        }
        if out.len() == 1 && out[0].len() == 1 {
            return Ok(out[0][0].clone());
        }
        Ok(LiteralValue::Array(out))
    }
}

pub fn register_builtins() {
    use crate::function_registry::register_function;
    use std::sync::Arc;
    register_function(Arc::new(XLookupFn));
    register_function(Arc::new(FilterFn));
    register_function(Arc::new(UniqueFn));
    register_function(Arc::new(SequenceFn));
    register_function(Arc::new(TransposeFn));
    register_function(Arc::new(TakeFn));
    register_function(Arc::new(DropFn));
}

/* ───────────────────────── SEQUENCE() ───────────────────────── */

#[derive(Debug)]
pub struct SequenceFn;
impl Function for SequenceFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SEQUENCE"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // rows
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // columns (default 1)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Int(1)),
                },
                // start (default 1)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Int(1)),
                },
                // step (default 1)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Int(1)),
                },
            ]
        });
        &SCHEMA
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        // Extract numbers (allow float but coerce to i64 for dimensions)
        let num = |a: &ArgumentHandle| -> Result<f64, ExcelError> {
            Ok(match a.value()?.as_ref() {
                LiteralValue::Int(i) => *i as f64,
                LiteralValue::Number(n) => *n,
                _other => {
                    return Err(ExcelError::new(ExcelErrorKind::Value));
                }
            })
        };
        let rows_f = num(&args[0])?;
        let rows = rows_f as i64;
        let cols = if args.len() >= 2 {
            num(&args[1])? as i64
        } else {
            1
        };
        let start = if args.len() >= 3 { num(&args[2])? } else { 1.0 };
        let step = if args.len() >= 4 { num(&args[3])? } else { 1.0 };
        if rows <= 0 || cols <= 0 {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        let total = rows.saturating_mul(cols);
        // TODO(perf): guard extremely large allocations (#NUM!).
        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(rows as usize);
        let mut current = start;
        for _r in 0..rows {
            let mut row_vec: Vec<LiteralValue> = Vec::with_capacity(cols as usize);
            for _c in 0..cols {
                // Use Int when value integral & within i64 range
                if (current.fract().abs() < 1e-12) && current.abs() < (i64::MAX as f64) {
                    row_vec.push(LiteralValue::Int(current as i64));
                } else {
                    row_vec.push(LiteralValue::Number(current));
                }
                current += step;
            }
            out.push(row_vec);
        }
        if out.len() == 1 && out[0].len() == 1 {
            return Ok(out[0][0].clone());
        }
        Ok(LiteralValue::Array(out))
    }
}

/* ───────────────────────── TRANSPOSE() ───────────────────────── */

#[derive(Debug)]
pub struct TransposeFn;
impl Function for TransposeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TRANSPOSE"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        false
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![ArgSchema {
                kinds: smallvec::smallvec![ArgKind::Range],
                required: true,
                by_ref: true,
                shape: ShapeKind::Range,
                coercion: CoercionPolicy::None,
                max: None,
                repeating: None,
                default: None,
            }]
        });
        &SCHEMA
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let rows = to_rows_2d(&args[0], ctx)?;
        if rows.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        let width = rows[0].len();
        // Validate rectangular
        if rows.iter().any(|r| r.len() != width) {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        if width == 0 {
            return Ok(LiteralValue::Array(vec![]));
        }
        let mut out: Vec<Vec<LiteralValue>> = vec![Vec::with_capacity(rows.len()); width];
        for row in &rows {
            for (c, cell) in row.iter().enumerate() {
                out[c].push(cell.clone());
            }
        }
        if out.len() == 1 && out[0].len() == 1 {
            return Ok(out[0][0].clone());
        }
        Ok(LiteralValue::Array(out))
    }
}

/* ───────────────────────── TAKE() ───────────────────────── */

#[derive(Debug)]
pub struct TakeFn;
impl Function for TakeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TAKE"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: None,
                },
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: None,
                },
            ]
        });
        &SCHEMA
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let rows = to_rows_2d(&args[0], ctx)?;
        if rows.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        let height = rows.len() as i64;
        let width = rows[0].len() as i64;
        if rows.iter().any(|r| r.len() as i64 != width) {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        let num = |a: &ArgumentHandle| -> Result<i64, ExcelError> {
            Ok(match a.value()?.as_ref() {
                LiteralValue::Int(i) => *i,
                LiteralValue::Number(n) => *n as i64,
                _ => 0,
            })
        };
        let take_rows = num(&args[1])?;
        let take_cols = if args.len() >= 3 {
            Some(num(&args[2])?)
        } else {
            None
        };
        if take_rows.abs() > height {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        let row_start;
        let row_end; // inclusive-exclusive
        if take_rows >= 0 {
            row_start = 0;
            row_end = take_rows as usize;
        } else {
            row_start = (height + take_rows) as usize;
            row_end = height as usize;
        }
        let (col_start, col_end) = if let Some(tc) = take_cols {
            if tc.abs() > width {
                return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
            }
            if tc >= 0 {
                (0, tc as usize)
            } else {
                ((width + tc) as usize, width as usize)
            }
        } else {
            (0, width as usize)
        };
        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(row_end - row_start);
        for row in rows.iter().skip(row_start).take(row_end - row_start) {
            out.push(row[col_start..col_end].to_vec());
        }
        if out.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        if out.len() == 1 && out[0].len() == 1 {
            return Ok(out[0][0].clone());
        }
        Ok(LiteralValue::Array(out))
    }
}

/* ───────────────────────── DROP() ───────────────────────── */

#[derive(Debug)]
pub struct DropFn;
impl Function for DropFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DROP"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: true,
                    by_ref: true,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: None,
                },
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: None,
                    default: None,
                },
            ]
        });
        &SCHEMA
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let rows = to_rows_2d(&args[0], ctx)?;
        if rows.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        let height = rows.len() as i64;
        let width = rows[0].len() as i64;
        if rows.iter().any(|r| r.len() as i64 != width) {
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
        }
        let num = |a: &ArgumentHandle| -> Result<i64, ExcelError> {
            Ok(match a.value()?.as_ref() {
                LiteralValue::Int(i) => *i,
                LiteralValue::Number(n) => *n as i64,
                _ => 0,
            })
        };
        let drop_rows = num(&args[1])?;
        let drop_cols = if args.len() >= 3 {
            Some(num(&args[2])?)
        } else {
            None
        };
        let (row_start, row_end) = if drop_rows >= 0 {
            ((drop_rows as usize).min(height as usize), height as usize)
        } else {
            (0, ((height + drop_rows).max(0) as usize))
        };
        let (col_start, col_end) = if let Some(dc) = drop_cols {
            if dc >= 0 {
                (((dc as usize).min(width as usize)), width as usize)
            } else {
                (0, ((width + dc).max(0) as usize))
            }
        } else {
            (0, width as usize)
        };
        if row_start >= row_end || col_start >= col_end {
            return Ok(LiteralValue::Array(vec![]));
        }
        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(row_end - row_start);
        for row in rows.iter().skip(row_start).take(row_end - row_start) {
            out.push(row[col_start..col_end].to_vec());
        }
        if out.len() == 1 && out[0].len() == 1 {
            return Ok(out[0][0].clone());
        }
        Ok(LiteralValue::Array(out))
    }
}

/* ───────────────────────── tests ───────────────────────── */

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
    use std::sync::Arc;
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn xlookup_basic_exact_and_if_not_found() {
        let wb = TestWorkbook::new().with_function(Arc::new(XLookupFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("a".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("b".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20));
        let ctx = wb.interpreter();
        let lookup_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(1),
                },
            },
            None,
        );
        let return_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "B1:B2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(2),
                    end_row: Some(2),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "XLOOKUP").unwrap();
        let key_b = lit(LiteralValue::Text("b".into()));
        let args = vec![
            ArgumentHandle::new(&key_b, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
        ];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert_eq!(v, LiteralValue::Int(20));
        let key_missing = lit(LiteralValue::Text("z".into()));
        let if_nf = lit(LiteralValue::Text("NF".into()));
        let args_nf = vec![
            ArgumentHandle::new(&key_missing, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
            ArgumentHandle::new(&if_nf, &ctx),
        ];
        let v_nf = f.dispatch(&args_nf, &ctx.function_context(None)).unwrap();
        assert_eq!(v_nf, LiteralValue::Text("NF".into()));
    }

    #[test]
    fn xlookup_match_modes_next_smaller_larger() {
        let wb = TestWorkbook::new().with_function(Arc::new(XLookupFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(3));
        let ctx = wb.interpreter();
        let lookup_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(1),
                },
            },
            None,
        );
        let return_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "B1:B3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(2),
                    end_row: Some(3),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "XLOOKUP").unwrap();
        let needle_25 = lit(LiteralValue::Int(25));
        let mm_next_smaller = lit(LiteralValue::Int(-1));
        let nf_text = lit(LiteralValue::Text("NF".into()));
        let args_smaller = vec![
            ArgumentHandle::new(&needle_25, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
            ArgumentHandle::new(&nf_text, &ctx),
            ArgumentHandle::new(&mm_next_smaller, &ctx),
        ];
        let v_smaller = f
            .dispatch(&args_smaller, &ctx.function_context(None))
            .unwrap();
        assert_eq!(v_smaller, LiteralValue::Int(2));
        let mm_next_larger = lit(LiteralValue::Int(1));
        let nf_text2 = lit(LiteralValue::Text("NF".into()));
        let args_larger = vec![
            ArgumentHandle::new(&needle_25, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
            ArgumentHandle::new(&nf_text2, &ctx),
            ArgumentHandle::new(&mm_next_larger, &ctx),
        ];
        let v_larger = f
            .dispatch(&args_larger, &ctx.function_context(None))
            .unwrap();
        assert_eq!(v_larger, LiteralValue::Int(3));
    }

    #[test]
    fn xlookup_wildcard_and_not_found_default_na() {
        let wb = TestWorkbook::new().with_function(Arc::new(XLookupFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("Alpha".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("Beta".into()))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Text("Gamma".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(100))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(200))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(300));
        let ctx = wb.interpreter();
        let lookup_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(1),
                },
            },
            None,
        );
        let return_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "B1:B3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(2),
                    end_row: Some(3),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "XLOOKUP").unwrap();
        // Wildcard should match Beta (*et*) with match_mode 2
        let pattern = lit(LiteralValue::Text("*et*".into()));
        let match_mode_wild = lit(LiteralValue::Int(2));
        let nf_binding = lit(LiteralValue::Text("NF".into()));
        let args_wild = vec![
            ArgumentHandle::new(&pattern, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
            ArgumentHandle::new(&nf_binding, &ctx),
            ArgumentHandle::new(&match_mode_wild, &ctx),
        ];
        let v_wild = f.dispatch(&args_wild, &ctx.function_context(None)).unwrap();
        assert_eq!(v_wild, LiteralValue::Int(200));
        // Escaped wildcard literal ~* should not match Beta
        let pattern_lit_star = lit(LiteralValue::Text("~*eta".into()));
        let args_lit = vec![
            ArgumentHandle::new(&pattern_lit_star, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
            ArgumentHandle::new(&nf_binding, &ctx),
            ArgumentHandle::new(&match_mode_wild, &ctx),
        ];
        let v_lit = f.dispatch(&args_lit, &ctx.function_context(None)).unwrap();
        match v_lit {
            LiteralValue::Text(s) => assert_eq!(s, "NF"),
            other => panic!("expected NF text got {other:?}"),
        }
        // Not found without if_not_found -> #N/A
        let missing = lit(LiteralValue::Text("Zeta".into()));
        let args_nf = vec![
            ArgumentHandle::new(&missing, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
        ];
        let v_nf = f.dispatch(&args_nf, &ctx.function_context(None)).unwrap();
        match v_nf {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Na),
            other => panic!("expected #N/A got {other:?}"),
        }
    }

    #[test]
    fn xlookup_reverse_search_mode_picks_last() {
        let wb = TestWorkbook::new().with_function(Arc::new(XLookupFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Text("First".into()))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Text("Mid".into()))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Text("Last".into()));
        let ctx = wb.interpreter();
        let lookup_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(1),
                },
            },
            None,
        );
        let return_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "B1:B3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(2),
                    end_row: Some(3),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "XLOOKUP").unwrap();
        let needle_one = lit(LiteralValue::Int(1));
        let search_rev = lit(LiteralValue::Int(-1));
        let nf_binding2 = lit(LiteralValue::Text("NF".into()));
        let match_mode_zero = lit(LiteralValue::Int(0));
        let args_rev = vec![
            ArgumentHandle::new(&needle_one, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
            ArgumentHandle::new(&nf_binding2, &ctx),
            /* match_mode default */ ArgumentHandle::new(&match_mode_zero, &ctx),
            ArgumentHandle::new(&search_rev, &ctx),
        ];
        let v_rev = f.dispatch(&args_rev, &ctx.function_context(None)).unwrap();
        assert_eq!(v_rev, LiteralValue::Text("Last".into()));
    }

    #[test]
    fn filter_basic_and_if_empty() {
        let wb = TestWorkbook::new().with_function(Arc::new(FilterFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "C1", LiteralValue::Boolean(true))
            .with_cell_a1("Sheet1", "C2", LiteralValue::Boolean(false));
        let ctx = wb.interpreter();
        let array_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(2),
                },
            },
            None,
        );
        let include_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "C1:C2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(3),
                    end_row: Some(2),
                    end_col: Some(3),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "FILTER").unwrap();
        let args = vec![
            ArgumentHandle::new(&array_range, &ctx),
            ArgumentHandle::new(&include_range, &ctx),
        ];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a[0], vec![LiteralValue::Int(1), LiteralValue::Int(10)]);
            }
            other => panic!("expected array got {other:?}"),
        }
        // Overwrite C1:C2 to both FALSE to produce empty result
        let wb2 = wb
            .with_cell_a1("Sheet1", "C1", LiteralValue::Boolean(false))
            .with_cell_a1("Sheet1", "C2", LiteralValue::Boolean(false));
        let ctx2 = wb2.interpreter();
        let include_range_false = ASTNode::new(
            ASTNodeType::Reference {
                original: "C1:C2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(3),
                    end_row: Some(2),
                    end_col: Some(3),
                },
            },
            None,
        );
        let f2 = ctx2.context.get_function("", "FILTER").unwrap();
        let empty_text = lit(LiteralValue::Text("EMPTY".into()));
        let args_empty = vec![
            ArgumentHandle::new(&array_range, &ctx2),
            ArgumentHandle::new(&include_range_false, &ctx2),
            ArgumentHandle::new(&empty_text, &ctx2),
        ];
        let v_empty = f2
            .dispatch(&args_empty, &ctx2.function_context(None))
            .unwrap();
        assert_eq!(v_empty, LiteralValue::Text("EMPTY".into()));
        // Now test #CALC! path (remove fallback)
        let args_calc = vec![
            ArgumentHandle::new(&array_range, &ctx2),
            ArgumentHandle::new(&include_range_false, &ctx2),
        ];
        let v_calc = f2
            .dispatch(&args_calc, &ctx2.function_context(None))
            .unwrap();
        match v_calc {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Calc),
            other => panic!("expected #CALC! got {other:?}"),
        }
    }

    #[test]
    fn unique_basic_and_exactly_once() {
        let wb = TestWorkbook::new().with_function(Arc::new(UniqueFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "A4", LiteralValue::Int(3));
        let ctx = wb.interpreter();
        let range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A4".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(4),
                    end_col: Some(1),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "UNIQUE").unwrap();
        let args = vec![ArgumentHandle::new(&range, &ctx)];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 3);
                assert_eq!(a[0][0], LiteralValue::Int(1));
                assert_eq!(a[1][0], LiteralValue::Int(2));
                assert_eq!(a[2][0], LiteralValue::Int(3));
            }
            _ => panic!("expected array"),
        }
        let true_lit = lit(LiteralValue::Boolean(true));
        let false_lit = lit(LiteralValue::Boolean(false));
        let args_once = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&false_lit, &ctx),
            ArgumentHandle::new(&true_lit, &ctx),
        ];
        let v_once = f.dispatch(&args_once, &ctx.function_context(None)).unwrap();
        match v_once {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(a[0][0], LiteralValue::Int(2));
                assert_eq!(a[1][0], LiteralValue::Int(3));
            }
            _ => panic!("expected array"),
        }
        // by_col = TRUE (single column -> same result)
        let true_lit2 = lit(LiteralValue::Boolean(true));
        let args_by_col = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&true_lit2, &ctx),
        ];
        let v_by_col = f
            .dispatch(&args_by_col, &ctx.function_context(None))
            .unwrap();
        match v_by_col {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 1);
            }
            other => panic!("expected array got {other:?}"),
        }

        // Collapse single cell test: shrink range to single cell
        let single = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A1".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(1),
                    end_col: Some(1),
                },
            },
            None,
        );
        let args_single = vec![ArgumentHandle::new(&single, &ctx)];
        let v_single = f
            .dispatch(&args_single, &ctx.function_context(None))
            .unwrap();
        assert_eq!(v_single, LiteralValue::Int(1));
    }

    #[test]
    fn xlookup_unsorted_approx_returns_na() {
        let wb = TestWorkbook::new().with_function(Arc::new(XLookupFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(3));
        let ctx = wb.interpreter();
        let lookup_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(1),
                },
            },
            None,
        );
        let return_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "B1:B3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(2),
                    end_row: Some(3),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "XLOOKUP").unwrap();
        let needle = lit(LiteralValue::Int(25));
        let mm_next_smaller = lit(LiteralValue::Int(-1));
        let nf_binding = lit(LiteralValue::Text("NF".into()));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
            ArgumentHandle::new(&nf_binding, &ctx),
            ArgumentHandle::new(&mm_next_smaller, &ctx),
        ];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        match v {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Na),
            other => panic!("expected #N/A got {other:?}"),
        }
    }

    #[test]
    fn unique_multi_column_row_and_col_modes() {
        let wb = TestWorkbook::new().with_function(Arc::new(UniqueFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Text("x".into()))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Text("x".into()))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Text("y".into()));
        let ctx = wb.interpreter();
        let range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "UNIQUE").unwrap();
        // Row-wise unique => (1,x) & (2,y)
        let args_rows = vec![ArgumentHandle::new(&range, &ctx)];
        let vr = f.dispatch(&args_rows, &ctx.function_context(None)).unwrap();
        match vr {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(
                    a[0],
                    vec![LiteralValue::Int(1), LiteralValue::Text("x".into())]
                );
                assert_eq!(
                    a[1],
                    vec![LiteralValue::Int(2), LiteralValue::Text("y".into())]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
        // Column-wise unique -> columns [1,1,2] and [x,x,y]
        let true_lit = lit(LiteralValue::Boolean(true));
        let args_cols = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&true_lit, &ctx),
        ];
        let vc = f.dispatch(&args_cols, &ctx.function_context(None)).unwrap();
        match vc {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(a[0].len(), 3);
                assert_eq!(a[1].len(), 3);
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn sequence_basic_rows_cols_step() {
        let wb = TestWorkbook::new().with_function(Arc::new(SequenceFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "SEQUENCE").unwrap();
        let rows = lit(LiteralValue::Int(2));
        let cols = lit(LiteralValue::Int(3));
        let start = lit(LiteralValue::Int(5));
        let step = lit(LiteralValue::Int(2));
        let args = vec![
            ArgumentHandle::new(&rows, &ctx),
            ArgumentHandle::new(&cols, &ctx),
            ArgumentHandle::new(&start, &ctx),
            ArgumentHandle::new(&step, &ctx),
        ];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(
                    a[0],
                    vec![
                        LiteralValue::Int(5),
                        LiteralValue::Int(7),
                        LiteralValue::Int(9)
                    ]
                );
                assert_eq!(
                    a[1],
                    vec![
                        LiteralValue::Int(11),
                        LiteralValue::Int(13),
                        LiteralValue::Int(15)
                    ]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn transpose_rectangular_and_single_cell() {
        let wb = TestWorkbook::new().with_function(Arc::new(TransposeFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20));
        let ctx = wb.interpreter();
        use formualizer_parse::parser::{ASTNodeType, ReferenceType};
        let range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B2".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(2),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "TRANSPOSE").unwrap();
        let args = vec![ArgumentHandle::new(&range, &ctx)];
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2); // 2 columns -> rows
                assert_eq!(a[0], vec![LiteralValue::Int(1), LiteralValue::Int(2)]);
                assert_eq!(a[1], vec![LiteralValue::Int(10), LiteralValue::Int(20)]);
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn take_positive_and_negative() {
        let wb = TestWorkbook::new().with_function(Arc::new(TakeFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(3))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(30));
        let ctx = wb.interpreter();
        use formualizer_parse::parser::{ASTNodeType, ReferenceType};
        let range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "TAKE").unwrap();
        // TAKE first 2 rows
        let n2 = lit(LiteralValue::Int(2));
        let args_first = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&n2, &ctx),
        ];
        let v_first = f
            .dispatch(&args_first, &ctx.function_context(None))
            .unwrap();
        match v_first {
            LiteralValue::Array(a) => assert_eq!(a.len(), 2),
            other => panic!("expected array got {other:?}"),
        }
        // TAKE last 1 row (negative)
        let n_neg1 = lit(LiteralValue::Int(-1));
        let args_last = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&n_neg1, &ctx),
        ];
        let v_last = f.dispatch(&args_last, &ctx.function_context(None)).unwrap();
        match v_last {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a[0][0], LiteralValue::Int(3));
            }
            other => panic!("expected array got {other:?}"),
        }
        // TAKE with columns subset
        let one = lit(LiteralValue::Int(1));
        let args_col = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&n2, &ctx),
            ArgumentHandle::new(&one, &ctx),
        ];
        let v_col = f.dispatch(&args_col, &ctx.function_context(None)).unwrap();
        match v_col {
            LiteralValue::Array(a) => {
                assert_eq!(a[0].len(), 1);
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn drop_positive_and_negative() {
        let wb = TestWorkbook::new().with_function(Arc::new(DropFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(3))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(30));
        let ctx = wb.interpreter();
        use formualizer_parse::parser::{ASTNodeType, ReferenceType};
        let range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:B3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(2),
                },
            },
            None,
        );
        let f = ctx.context.get_function("", "DROP").unwrap();
        let one = lit(LiteralValue::Int(1));
        let args_drop_first_row = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&one, &ctx),
        ];
        let v_d1 = f
            .dispatch(&args_drop_first_row, &ctx.function_context(None))
            .unwrap();
        match v_d1 {
            LiteralValue::Array(a) => assert_eq!(a.len(), 2),
            other => panic!("expected array got {other:?}"),
        }
        let neg_one = lit(LiteralValue::Int(-1));
        let args_drop_last_row = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&neg_one, &ctx),
        ];
        let v_d2 = f
            .dispatch(&args_drop_last_row, &ctx.function_context(None))
            .unwrap();
        match v_d2 {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(a[0][0], LiteralValue::Int(1));
            }
            other => panic!("expected array got {other:?}"),
        }
        // Drop columns
        let args_drop_col = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&one, &ctx),
            ArgumentHandle::new(&one, &ctx),
        ];
        let v_dc = f
            .dispatch(&args_drop_col, &ctx.function_context(None))
            .unwrap();
        match v_dc {
            LiteralValue::Array(a) => {
                assert_eq!(a[0].len(), 1);
            }
            other => panic!("expected array got {other:?}"),
        }
    }
}
