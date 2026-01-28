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

use super::super::utils::collapse_if_scalar;
use super::lookup_utils::{cmp_for_lookup, equals_maybe_wildcard, value_to_f64_lenient};
use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::function::Function; // FnCaps imported via macro
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;
use std::collections::HashMap;

/* ───────────────────────── helpers ───────────────────────── */

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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }
        let lookup_value = args[0].value()?.into_literal();
        if let LiteralValue::Error(ref e) = lookup_value {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                e.clone(),
            )));
        }
        let lookup_view = match args[1].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let ret_view = match args[2].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let (lookup_rows, lookup_cols) = lookup_view.dims();
        let (ret_rows, ret_cols) = ret_view.dims();

        // XLOOKUP requires a 1-D lookup array (single row or single column).
        // If the lookup range is completely empty (used-region trimmed to 0),
        // fall back to the return range's used-region length and treat missing lookup
        // cells as Empty.
        let vertical = if lookup_cols == 1 {
            true
        } else if lookup_rows == 1 {
            false
        } else if lookup_rows == 0 && lookup_cols == 0 {
            if ret_cols == 1 {
                true
            } else if ret_rows == 1 {
                false
            } else {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
        } else {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        };

        let lookup_len = {
            let raw = if vertical { lookup_rows } else { lookup_cols };
            if raw == 0 {
                if vertical { ret_rows } else { ret_cols }
            } else {
                raw
            }
        };

        if lookup_len == 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Na),
            )));
        }

        let match_mode = if args.len() >= 5 {
            match args[4].value()?.into_literal() {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
                _ => 0,
            }
        } else {
            0
        };
        let search_mode = if args.len() >= 6 {
            match args[5].value()?.into_literal() {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
                _ => 1,
            }
        } else {
            1
        };

        let wildcard = match_mode == 2;

        let mut found: Option<usize> = None;
        let needle = lookup_value;
        if match_mode == 0 || wildcard {
            if search_mode == 1 && lookup_rows > 0 && lookup_cols > 0 {
                found =
                    super::lookup_utils::find_exact_index_in_view(&lookup_view, &needle, wildcard)?;
            } else if search_mode == -1 {
                for i in (0..lookup_len).rev() {
                    let cand = if vertical {
                        lookup_view.get_cell(i, 0)
                    } else {
                        lookup_view.get_cell(0, i)
                    };
                    if equals_maybe_wildcard(&needle, &cand, wildcard) {
                        found = Some(i);
                        break;
                    }
                }
            } else {
                // Fallback linear scan (also used when the lookup view is empty and
                // we are treating missing cells as Empty).
                for i in 0..lookup_len {
                    let cand = if vertical {
                        lookup_view.get_cell(i, 0)
                    } else {
                        lookup_view.get_cell(0, i)
                    };
                    if equals_maybe_wildcard(&needle, &cand, wildcard) {
                        found = Some(i);
                        break;
                    }
                }
            }
        } else if match_mode == -1 || match_mode == 1 {
            let needle_num = value_to_f64_lenient(&needle);
            let mut best_idx: Option<usize> = None;
            let mut best_val: f64 = if match_mode == -1 {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };

            let mut prev: Option<LiteralValue> = None;
            for i in 0..lookup_len {
                let cand = if vertical {
                    lookup_view.get_cell(i, 0)
                } else {
                    lookup_view.get_cell(0, i)
                };

                if let Some(p) = prev.as_ref() {
                    let sorted_ok = cmp_for_lookup(p, &cand).is_some_and(|o| o <= 0);
                    if !sorted_ok {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Na),
                        )));
                    }
                }
                prev = Some(cand.clone());

                if cmp_for_lookup(&cand, &needle).is_some_and(|o| o == 0) {
                    found = Some(i);
                    break;
                }

                if let (Some(nn), Some(vv)) = (needle_num, value_to_f64_lenient(&cand)) {
                    if match_mode == -1 {
                        if vv <= nn && vv > best_val {
                            best_val = vv;
                            best_idx = Some(i);
                        }
                    } else if vv >= nn && vv < best_val {
                        best_val = vv;
                        best_idx = Some(i);
                    }
                }
            }

            if found.is_none() {
                found = best_idx;
            }
        } else {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        if let Some(idx) = found {
            let (ret_rows, ret_cols) = ret_view.dims();
            if ret_rows == 0 || ret_cols == 0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Empty));
            }

            if vertical {
                if ret_cols == 1 {
                    return Ok(crate::traits::CalcValue::Scalar(ret_view.get_cell(idx, 0)));
                }
                let mut row_out: Vec<LiteralValue> = Vec::with_capacity(ret_cols);
                for c in 0..ret_cols {
                    row_out.push(ret_view.get_cell(idx, c));
                }
                return Ok(crate::traits::CalcValue::Range(
                    crate::engine::range_view::RangeView::from_owned_rows(
                        vec![row_out],
                        _ctx.date_system(),
                    ),
                ));
            }

            // Horizontal orientation: treat idx as column.
            if ret_rows == 1 {
                return Ok(crate::traits::CalcValue::Scalar(ret_view.get_cell(0, idx)));
            }

            let mut col_out: Vec<Vec<LiteralValue>> = Vec::with_capacity(ret_rows);
            for r in 0..ret_rows {
                col_out.push(vec![ret_view.get_cell(r, idx)]);
            }
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(col_out, _ctx.date_system()),
            ));
        }

        if args.len() >= 4 {
            return args[3].value();
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Na),
        )))
    }
}

/* ───────────────────────── XMATCH() ───────────────────────── */

#[derive(Debug)]
pub struct XMatchFn;
impl Function for XMatchFn {
    func_caps!(PURE, LOOKUP);
    fn name(&self) -> &'static str {
        "XMATCH"
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
                // match_mode (number) default 0
                // 0 = exact (default), -1 = exact or next smaller, 1 = exact or next larger, 2 = wildcard
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
                // 1 = first to last (default), -1 = last to first, 2 = binary ascending, -2 = binary descending
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }
        let lookup_value = args[0].value()?.into_literal();
        if let LiteralValue::Error(ref e) = lookup_value {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                e.clone(),
            )));
        }
        let lookup_view = match args[1].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let (lookup_rows, lookup_cols) = lookup_view.dims();

        // XMATCH requires a 1-D lookup array (single row or single column).
        let vertical = if lookup_cols == 1 {
            true
        } else if lookup_rows == 1 {
            false
        } else if lookup_rows == 0 || lookup_cols == 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Na),
            )));
        } else {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        };

        let lookup_len = if vertical { lookup_rows } else { lookup_cols };

        if lookup_len == 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Na),
            )));
        }

        let match_mode = if args.len() >= 3 {
            match args[2].value()?.into_literal() {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
                _ => 0,
            }
        } else {
            0
        };
        let search_mode = if args.len() >= 4 {
            match args[3].value()?.into_literal() {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
                _ => 1,
            }
        } else {
            1
        };

        let wildcard = match_mode == 2;
        let needle = lookup_value;

        let mut found: Option<usize> = None;

        if match_mode == 0 || wildcard {
            // Exact match or wildcard match
            if search_mode == 1 || search_mode == 2 {
                // Forward search (first to last) or binary ascending (treated as forward for exact)
                if lookup_rows > 0 && lookup_cols > 0 {
                    found = super::lookup_utils::find_exact_index_in_view(
                        &lookup_view,
                        &needle,
                        wildcard,
                    )?;
                }
            } else if search_mode == -1 || search_mode == -2 {
                // Reverse search (last to first) or binary descending (treated as reverse for exact)
                for i in (0..lookup_len).rev() {
                    let cand = if vertical {
                        lookup_view.get_cell(i, 0)
                    } else {
                        lookup_view.get_cell(0, i)
                    };
                    if equals_maybe_wildcard(&needle, &cand, wildcard) {
                        found = Some(i);
                        break;
                    }
                }
            } else {
                // Fallback linear scan
                for i in 0..lookup_len {
                    let cand = if vertical {
                        lookup_view.get_cell(i, 0)
                    } else {
                        lookup_view.get_cell(0, i)
                    };
                    if equals_maybe_wildcard(&needle, &cand, wildcard) {
                        found = Some(i);
                        break;
                    }
                }
            }
        } else if match_mode == -1 || match_mode == 1 {
            // Approximate match: -1 = exact or next smaller, 1 = exact or next larger
            let needle_num = value_to_f64_lenient(&needle);
            let mut best_idx: Option<usize> = None;
            let mut best_val: f64 = if match_mode == -1 {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };

            // Determine iteration direction based on search_mode
            let use_reverse = search_mode == -1 || search_mode == -2;
            let indices: Box<dyn Iterator<Item = usize>> = if use_reverse {
                Box::new((0..lookup_len).rev())
            } else {
                Box::new(0..lookup_len)
            };

            // For binary search modes (2, -2), data should be sorted
            // We verify sorting for approximate modes
            if (search_mode == 2 || search_mode == -2) && match_mode != 0 {
                let ascending = search_mode == 2;
                let mut prev: Option<LiteralValue> = None;
                for i in 0..lookup_len {
                    let cand = if vertical {
                        lookup_view.get_cell(i, 0)
                    } else {
                        lookup_view.get_cell(0, i)
                    };
                    if let Some(p) = prev.as_ref() {
                        let sorted_ok = if ascending {
                            cmp_for_lookup(p, &cand).is_some_and(|o| o <= 0)
                        } else {
                            cmp_for_lookup(p, &cand).is_some_and(|o| o >= 0)
                        };
                        if !sorted_ok {
                            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                                ExcelError::new(ExcelErrorKind::Na),
                            )));
                        }
                    }
                    prev = Some(cand);
                }
            }

            for i in indices {
                let cand = if vertical {
                    lookup_view.get_cell(i, 0)
                } else {
                    lookup_view.get_cell(0, i)
                };

                if cmp_for_lookup(&cand, &needle).is_some_and(|o| o == 0) {
                    found = Some(i);
                    break;
                }

                if let (Some(nn), Some(vv)) = (needle_num, value_to_f64_lenient(&cand)) {
                    if match_mode == -1 {
                        // exact or next smaller
                        if vv <= nn && vv > best_val {
                            best_val = vv;
                            best_idx = Some(i);
                        }
                    } else {
                        // match_mode == 1: exact or next larger
                        if vv >= nn && vv < best_val {
                            best_val = vv;
                            best_idx = Some(i);
                        }
                    }
                }
            }

            if found.is_none() {
                found = best_idx;
            }
        } else {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        match found {
            Some(idx) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                (idx + 1) as i64,
            ))),
            None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Na),
            ))),
        }
    }
}

/* ───────────────────────── SORT() ───────────────────────── */

#[derive(Debug)]
pub struct SortFn;
impl Function for SortFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SORT"
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
                // sort_index (default 1)
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
                // sort_order (default 1 = ascending, -1 = descending)
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
                // by_col (default FALSE = sort rows, TRUE = sort columns)
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let view = match args[0].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let (rows, cols) = view.dims();
        if rows == 0 || cols == 0 {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let sort_index = if args.len() >= 2 {
            match args[1].value()?.into_literal() {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
                _ => 1,
            }
        } else {
            1
        };

        let sort_order = if args.len() >= 3 {
            match args[2].value()?.into_literal() {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
                _ => 1,
            }
        } else {
            1
        };

        let by_col = if args.len() >= 4 {
            matches!(args[3].value()?.into_literal(), LiteralValue::Boolean(true))
        } else {
            false
        };

        let ascending = sort_order >= 0;

        if by_col {
            // Sort columns by the specified row
            let sort_row_idx = (sort_index - 1).max(0) as usize;
            if sort_row_idx >= rows {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }

            // Extract columns as vectors
            let mut columns: Vec<(usize, Vec<LiteralValue>)> = Vec::with_capacity(cols);
            for c in 0..cols {
                let mut col_vals: Vec<LiteralValue> = Vec::with_capacity(rows);
                for r in 0..rows {
                    col_vals.push(view.get_cell(r, c));
                }
                columns.push((c, col_vals));
            }

            // Sort columns by the value in sort_row_idx
            columns.sort_by(|a, b| {
                let val_a = &a.1[sort_row_idx];
                let val_b = &b.1[sort_row_idx];
                let cmp = cmp_for_lookup(val_a, val_b).unwrap_or(0);
                if ascending { cmp.cmp(&0) } else { 0.cmp(&cmp) }
            });

            // Reconstruct the array with sorted columns
            let mut out: Vec<Vec<LiteralValue>> = vec![Vec::with_capacity(cols); rows];
            for (_orig_idx, col_vals) in columns {
                for (r, val) in col_vals.into_iter().enumerate() {
                    out[r].push(val);
                }
            }

            Ok(collapse_if_scalar(out, _ctx.date_system()))
        } else {
            // Sort rows by the specified column
            let sort_col_idx = (sort_index - 1).max(0) as usize;
            if sort_col_idx >= cols {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }

            // Extract rows
            let mut row_data: Vec<Vec<LiteralValue>> = Vec::with_capacity(rows);
            for r in 0..rows {
                let mut row_vals: Vec<LiteralValue> = Vec::with_capacity(cols);
                for c in 0..cols {
                    row_vals.push(view.get_cell(r, c));
                }
                row_data.push(row_vals);
            }

            // Sort rows by the value in sort_col_idx
            row_data.sort_by(|a, b| {
                let val_a = &a[sort_col_idx];
                let val_b = &b[sort_col_idx];
                let cmp = cmp_for_lookup(val_a, val_b).unwrap_or(0);
                if ascending { cmp.cmp(&0) } else { 0.cmp(&cmp) }
            });

            Ok(collapse_if_scalar(row_data, _ctx.date_system()))
        }
    }
}

/* ───────────────────────── SORTBY() ───────────────────────── */

#[derive(Debug)]
pub struct SortByFn;
impl Function for SortByFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SORTBY"
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
                // by_array1
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
                // sort_order1 (optional, default 1)
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
                // Additional by_array/sort_order pairs can follow (variadic)
            ]
        });
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        let view = match args[0].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let (rows, cols) = view.dims();
        if rows == 0 || cols == 0 {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        // Parse sort criteria: pairs of (by_array, sort_order)
        // Arguments after array: by_array1, [sort_order1], [by_array2], [sort_order2], ...
        let mut sort_criteria: Vec<(Vec<LiteralValue>, bool)> = Vec::new();
        let mut arg_idx = 1;

        while arg_idx < args.len() {
            // by_array
            let by_view = match args[arg_idx].range_view() {
                Ok(v) => v,
                Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            };
            let (by_rows, by_cols) = by_view.dims();

            // The by_array should be 1-D and match the number of rows in the main array
            let by_values: Vec<LiteralValue> = if by_cols == 1 {
                if by_rows != rows {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Value),
                    )));
                }
                (0..by_rows).map(|r| by_view.get_cell(r, 0)).collect()
            } else if by_rows == 1 {
                if by_cols != rows {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Value),
                    )));
                }
                (0..by_cols).map(|c| by_view.get_cell(0, c)).collect()
            } else {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            };

            arg_idx += 1;

            // sort_order (optional)
            let ascending = if arg_idx < args.len() {
                // TODO(phase6): SORTBY parsing can mis-handle multi-criteria sort_order.
                // Check if next arg is a number (sort_order) or a range (next by_array)
                match args[arg_idx].value() {
                    Ok(v) => {
                        let lit = v.into_literal();
                        match lit {
                            LiteralValue::Int(i) => {
                                arg_idx += 1;
                                i >= 0
                            }
                            LiteralValue::Number(n) => {
                                arg_idx += 1;
                                n >= 0.0
                            }
                            _ => true, // Next arg is likely a range, use default ascending
                        }
                    }
                    Err(_) => true,
                }
            } else {
                true
            };

            sort_criteria.push((by_values, ascending));
        }

        if sort_criteria.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Extract rows with their indices
        let mut indexed_rows: Vec<(usize, Vec<LiteralValue>)> = Vec::with_capacity(rows);
        for r in 0..rows {
            let mut row_vals: Vec<LiteralValue> = Vec::with_capacity(cols);
            for c in 0..cols {
                row_vals.push(view.get_cell(r, c));
            }
            indexed_rows.push((r, row_vals));
        }

        // Sort using all criteria
        indexed_rows.sort_by(|a, b| {
            for (by_values, ascending) in &sort_criteria {
                let val_a = &by_values[a.0];
                let val_b = &by_values[b.0];
                let cmp = cmp_for_lookup(val_a, val_b).unwrap_or(0);
                if cmp != 0 {
                    return if *ascending { cmp.cmp(&0) } else { 0.cmp(&cmp) };
                }
            }
            std::cmp::Ordering::Equal
        });

        // Extract sorted rows
        let out: Vec<Vec<LiteralValue>> = indexed_rows.into_iter().map(|(_, row)| row).collect();

        Ok(collapse_if_scalar(out, _ctx.date_system()))
    }
}

/* ───────────────────────── RANDARRAY() ───────────────────────── */

#[derive(Debug)]
pub struct RandArrayFn;
impl Function for RandArrayFn {
    // Note: RANDARRAY is NOT pure - it returns different values on each evaluation
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::empty()
    }
    fn name(&self) -> &'static str {
        "RANDARRAY"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // rows (default 1)
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
                // min (default 0)
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
                // max (default 1)
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
                // whole_number (default FALSE)
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        use rand::Rng;

        // Extract numbers (allow float but coerce to i64 for dimensions)
        let num = |a: &ArgumentHandle| -> Result<f64, ExcelError> {
            Ok(match a.value()?.into_literal() {
                LiteralValue::Int(i) => i as f64,
                LiteralValue::Number(n) => n,
                LiteralValue::Error(e) => return Err(e),
                _other => {
                    return Err(ExcelError::new(ExcelErrorKind::Value));
                }
            })
        };

        let rows = if !args.is_empty() {
            num(&args[0])? as i64
        } else {
            1
        };
        let cols = if args.len() >= 2 {
            num(&args[1])? as i64
        } else {
            1
        };
        let min_val = if args.len() >= 3 { num(&args[2])? } else { 0.0 };
        let max_val = if args.len() >= 4 { num(&args[3])? } else { 1.0 };
        let whole_number = if args.len() >= 5 {
            matches!(args[4].value()?.into_literal(), LiteralValue::Boolean(true))
        } else {
            false
        };

        // Validate dimensions
        if rows <= 0 || cols <= 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Validate min <= max for whole numbers
        if whole_number && min_val > max_val {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        let mut rng = rand::thread_rng();
        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(rows as usize);

        for _r in 0..rows {
            let mut row_vec: Vec<LiteralValue> = Vec::with_capacity(cols as usize);
            for _c in 0..cols {
                let value = if whole_number {
                    // Generate random integer in range [min, max] inclusive
                    let min_int = min_val.ceil() as i64;
                    let max_int = max_val.floor() as i64;
                    if min_int > max_int {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Value),
                        )));
                    }
                    let rand_int = rng.gen_range(min_int..=max_int);
                    LiteralValue::Int(rand_int)
                } else {
                    // Generate random float in range [min, max)
                    let rand_float = rng.r#gen::<f64>() * (max_val - min_val) + min_val;
                    LiteralValue::Number(rand_float)
                };
                row_vec.push(value);
            }
            out.push(row_vec);
        }

        Ok(collapse_if_scalar(out, _ctx.date_system()))
    }
}

/* ───────────────────────── GROUPBY() ───────────────────────── */

/// Aggregation type for GROUPBY and PIVOTBY
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupAggregation {
    Sum,
    Average,
    Count,
    CountA,
    Max,
    Min,
    Product,
    StDev,
    StDevP,
    Var,
    VarP,
    Median,
}

impl GroupAggregation {
    fn from_literal(val: &LiteralValue) -> Option<Self> {
        match val {
            LiteralValue::Text(s) => Self::from_str(s),
            LiteralValue::Int(n) => Self::from_num(*n as i32),
            LiteralValue::Number(n) => Self::from_num(*n as i32),
            _ => None,
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        let upper = s.to_ascii_uppercase();
        match upper.as_str() {
            "SUM" => Some(Self::Sum),
            "AVERAGE" | "AVG" => Some(Self::Average),
            "COUNT" => Some(Self::Count),
            "COUNTA" => Some(Self::CountA),
            "MAX" => Some(Self::Max),
            "MIN" => Some(Self::Min),
            "PRODUCT" => Some(Self::Product),
            "STDEV" | "STDEV.S" => Some(Self::StDev),
            "STDEVP" | "STDEV.P" => Some(Self::StDevP),
            "VAR" | "VAR.S" => Some(Self::Var),
            "VARP" | "VAR.P" => Some(Self::VarP),
            "MEDIAN" => Some(Self::Median),
            _ => None,
        }
    }

    fn from_num(n: i32) -> Option<Self> {
        // Excel's AGGREGATE function_num mapping (common subset)
        match n {
            1 => Some(Self::Average),
            2 => Some(Self::Count),
            3 => Some(Self::CountA),
            4 => Some(Self::Max),
            5 => Some(Self::Min),
            6 => Some(Self::Product),
            7 => Some(Self::StDev),
            8 => Some(Self::StDevP),
            9 => Some(Self::Sum),
            10 => Some(Self::Var),
            11 => Some(Self::VarP),
            12 => Some(Self::Median),
            _ => None,
        }
    }

    fn apply(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return match self {
                Self::Count | Self::CountA => 0.0,
                Self::Sum | Self::Product => 0.0,
                _ => f64::NAN,
            };
        }

        match self {
            Self::Sum => values.iter().sum(),
            Self::Average => values.iter().sum::<f64>() / values.len() as f64,
            Self::Count | Self::CountA => values.len() as f64,
            Self::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            Self::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
            Self::Product => values.iter().product(),
            Self::StDev => {
                if values.len() < 2 {
                    return f64::NAN;
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                    / (values.len() - 1) as f64;
                variance.sqrt()
            }
            Self::StDevP => {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                variance.sqrt()
            }
            Self::Var => {
                if values.len() < 2 {
                    return f64::NAN;
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64
            }
            Self::VarP => {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64
            }
            Self::Median => {
                let mut sorted = values.to_vec();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mid = sorted.len() / 2;
                if sorted.len() % 2 == 0 {
                    (sorted[mid - 1] + sorted[mid]) / 2.0
                } else {
                    sorted[mid]
                }
            }
        }
    }
}

/// Helper to convert LiteralValue to a group key string
fn literal_to_group_key(v: &LiteralValue) -> String {
    match v {
        LiteralValue::Text(s) => s.clone(),
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Number(n) => format!("{:.10}", n),
        LiteralValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        LiteralValue::Empty => String::new(),
        LiteralValue::Error(e) => format!("#{:?}", e.kind),
        LiteralValue::Array(_) => "[Array]".to_string(),
        LiteralValue::Date(d) => d.to_string(),
        LiteralValue::DateTime(dt) => dt.to_string(),
        LiteralValue::Time(t) => t.to_string(),
        LiteralValue::Duration(d) => format!("{:?}", d),
        LiteralValue::Pending => "[Pending]".to_string(),
    }
}

/// Helper to extract numeric value for aggregation
fn literal_to_num_opt(v: &LiteralValue) -> Option<f64> {
    match v {
        LiteralValue::Number(n) => Some(*n),
        LiteralValue::Int(i) => Some(*i as f64),
        LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

#[derive(Debug)]
pub struct GroupByFn;

impl Function for GroupByFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "GROUPBY"
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
                // row_fields - range to group by
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
                // values - range of values to aggregate
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
                // function - aggregation function (SUM, AVERAGE, etc.)
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
                // field_headers (optional) - 0: no headers, 1: has headers (default), 2: generate headers, 3: has headers + generate
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
                // total_depth (optional) - 0: no totals, 1: grand total, 2: subtotals, etc.
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
                // sort_order (optional) - 0: no sorting, 1: ascending, -1: descending, 2: by value asc, -2: by value desc
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
            ]
        });
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Get row_fields and values ranges
        let row_fields_view = match args[0].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let values_view = match args[1].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // Parse aggregation function
        let agg_val = args[2].value()?.into_literal();
        let aggregation = match GroupAggregation::from_literal(&agg_val) {
            Some(a) => a,
            None => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Invalid aggregation function"),
                )));
            }
        };

        // Parse optional parameters
        let field_headers = if args.len() >= 4 {
            match args[3].value()?.into_literal() {
                LiteralValue::Int(i) => i as i32,
                LiteralValue::Number(n) => n as i32,
                _ => 1,
            }
        } else {
            1
        };

        let total_depth = if args.len() >= 5 {
            match args[4].value()?.into_literal() {
                LiteralValue::Int(i) => i as i32,
                LiteralValue::Number(n) => n as i32,
                _ => 0,
            }
        } else {
            0
        };

        let sort_order = if args.len() >= 6 {
            match args[5].value()?.into_literal() {
                LiteralValue::Int(i) => i as i32,
                LiteralValue::Number(n) => n as i32,
                _ => 0,
            }
        } else {
            0
        };

        let (rf_rows, rf_cols) = row_fields_view.dims();
        let (val_rows, val_cols) = values_view.dims();

        // Determine if we have headers
        let has_headers = field_headers == 1 || field_headers == 3;
        let data_start_row = if has_headers { 1 } else { 0 };

        // Validate that row counts match (accounting for headers)
        if rf_rows != val_rows {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message("Row fields and values must have same number of rows"),
            )));
        }

        if rf_rows <= data_start_row {
            // No data rows
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        // Build groups: key -> list of values for each value column
        // For multi-column row_fields, concatenate keys
        let mut groups: std::collections::HashMap<String, Vec<Vec<f64>>> = HashMap::new();
        let mut group_order: Vec<String> = Vec::new();

        for r in data_start_row..rf_rows {
            // Build composite key from all row_field columns
            let mut key_parts: Vec<String> = Vec::with_capacity(rf_cols);
            for c in 0..rf_cols {
                key_parts.push(literal_to_group_key(&row_fields_view.get_cell(r, c)));
            }
            let key = key_parts.join("\x00"); // Use null separator for composite keys

            // Get values for this row
            let mut row_values: Vec<Option<f64>> = Vec::with_capacity(val_cols);
            for c in 0..val_cols {
                row_values.push(literal_to_num_opt(&values_view.get_cell(r, c)));
            }

            // Add to groups
            if !groups.contains_key(&key) {
                group_order.push(key.clone());
                groups.insert(key.clone(), vec![Vec::new(); val_cols]);
            }

            let group_vals = groups.get_mut(&key).unwrap();
            for (c, val) in row_values.iter().enumerate() {
                if let Some(v) = val {
                    group_vals[c].push(*v);
                }
            }
        }

        // Sort groups if requested
        if sort_order != 0 {
            group_order.sort_by(|a, b| if sort_order > 0 { a.cmp(b) } else { b.cmp(a) });
        }

        // Build output
        let mut output: Vec<Vec<LiteralValue>> = Vec::new();

        // Add header row if requested
        let generate_headers = field_headers == 2 || field_headers == 3;
        if generate_headers {
            let mut header_row: Vec<LiteralValue> = Vec::new();
            // Row field headers
            for c in 0..rf_cols {
                if has_headers {
                    header_row.push(row_fields_view.get_cell(0, c));
                } else {
                    header_row.push(LiteralValue::Text(format!("Field{}", c + 1)));
                }
            }
            // Value headers
            for c in 0..val_cols {
                if has_headers {
                    header_row.push(values_view.get_cell(0, c));
                } else {
                    header_row.push(LiteralValue::Text(format!("Value{}", c + 1)));
                }
            }
            output.push(header_row);
        }

        // Add grouped data rows
        for key in &group_order {
            let mut row: Vec<LiteralValue> = Vec::new();

            // Add row field values (split composite key)
            let key_parts: Vec<&str> = key.split('\x00').collect();
            for part in &key_parts {
                row.push(LiteralValue::Text(part.to_string()));
            }

            // Add aggregated values
            let group_vals = groups.get(key).unwrap();
            for col_vals in group_vals {
                let result = aggregation.apply(col_vals);
                if result.is_nan() {
                    row.push(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
                } else if result.fract() == 0.0 && result.abs() < i64::MAX as f64 {
                    row.push(LiteralValue::Int(result as i64));
                } else {
                    row.push(LiteralValue::Number(result));
                }
            }
            output.push(row);
        }

        // Add grand total if requested
        if total_depth >= 1 {
            let mut total_row: Vec<LiteralValue> = Vec::new();
            // Empty cells for row fields (except first which says "Grand Total")
            total_row.push(LiteralValue::Text("Grand Total".to_string()));
            for _ in 1..rf_cols {
                total_row.push(LiteralValue::Empty);
            }

            // Aggregate all values across all groups
            for c in 0..val_cols {
                let mut all_vals: Vec<f64> = Vec::new();
                for group_vals in groups.values() {
                    all_vals.extend(&group_vals[c]);
                }
                let result = aggregation.apply(&all_vals);
                if result.is_nan() {
                    total_row.push(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
                } else if result.fract() == 0.0 && result.abs() < i64::MAX as f64 {
                    total_row.push(LiteralValue::Int(result as i64));
                } else {
                    total_row.push(LiteralValue::Number(result));
                }
            }
            output.push(total_row);
        }

        if output.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Calc),
            )));
        }

        Ok(collapse_if_scalar(output, _ctx.date_system()))
    }
}

/* ───────────────────────── PIVOTBY() ───────────────────────── */

#[derive(Debug)]
pub struct PivotByFn;

impl Function for PivotByFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PIVOTBY"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // row_fields - range to group rows by
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
                // col_fields - range to group columns by
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
                // values - range of values to aggregate
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
                // function - aggregation function
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
                // field_headers (optional)
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
                // row_total_depth (optional)
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
                // row_sort_order (optional)
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
                // col_total_depth (optional)
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
                // col_sort_order (optional)
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
            ]
        });
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 4 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Get ranges
        let row_fields_view = match args[0].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let col_fields_view = match args[1].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let values_view = match args[2].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // Parse aggregation function
        let agg_val = args[3].value()?.into_literal();
        let aggregation = match GroupAggregation::from_literal(&agg_val) {
            Some(a) => a,
            None => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Invalid aggregation function"),
                )));
            }
        };

        // Parse optional parameters
        let field_headers = if args.len() >= 5 {
            match args[4].value()?.into_literal() {
                LiteralValue::Int(i) => i as i32,
                LiteralValue::Number(n) => n as i32,
                _ => 1,
            }
        } else {
            1
        };

        let row_total_depth = if args.len() >= 6 {
            match args[5].value()?.into_literal() {
                LiteralValue::Int(i) => i as i32,
                LiteralValue::Number(n) => n as i32,
                _ => 0,
            }
        } else {
            0
        };

        let row_sort_order = if args.len() >= 7 {
            match args[6].value()?.into_literal() {
                LiteralValue::Int(i) => i as i32,
                LiteralValue::Number(n) => n as i32,
                _ => 0,
            }
        } else {
            0
        };

        let col_total_depth = if args.len() >= 8 {
            match args[7].value()?.into_literal() {
                LiteralValue::Int(i) => i as i32,
                LiteralValue::Number(n) => n as i32,
                _ => 0,
            }
        } else {
            0
        };

        let col_sort_order = if args.len() >= 9 {
            match args[8].value()?.into_literal() {
                LiteralValue::Int(i) => i as i32,
                LiteralValue::Number(n) => n as i32,
                _ => 0,
            }
        } else {
            0
        };

        let (rf_rows, rf_cols) = row_fields_view.dims();
        let (cf_rows, _cf_cols) = col_fields_view.dims();
        let (val_rows, _val_cols) = values_view.dims();

        // Validate dimensions
        if rf_rows != cf_rows || rf_rows != val_rows {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message("All ranges must have same number of rows"),
            )));
        }

        let has_headers = field_headers == 1 || field_headers == 3;
        let data_start_row = if has_headers { 1 } else { 0 };

        if rf_rows <= data_start_row {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        // Collect unique row and column keys
        let mut row_keys: Vec<String> = Vec::new();
        let mut col_keys: Vec<String> = Vec::new();
        let mut row_key_set: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut col_key_set: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Build pivot data: (row_key, col_key) -> values
        let mut pivot_data: HashMap<(String, String), Vec<f64>> = HashMap::new();

        for r in data_start_row..rf_rows {
            // Build row key
            let mut row_key_parts: Vec<String> = Vec::with_capacity(rf_cols);
            for c in 0..rf_cols {
                row_key_parts.push(literal_to_group_key(&row_fields_view.get_cell(r, c)));
            }
            let row_key = row_key_parts.join("\x00");

            // Build col key (use first column of col_fields for simplicity)
            // TODO(phase6): PIVOTBY only uses first col of col_fields/values.
            let col_key = literal_to_group_key(&col_fields_view.get_cell(r, 0));

            // Get value (use first column of values)
            let val = literal_to_num_opt(&values_view.get_cell(r, 0));

            // Track unique keys in order
            if !row_key_set.contains(&row_key) {
                row_key_set.insert(row_key.clone());
                row_keys.push(row_key.clone());
            }
            if !col_key_set.contains(&col_key) {
                col_key_set.insert(col_key.clone());
                col_keys.push(col_key.clone());
            }

            // Add to pivot data
            let entry = pivot_data
                .entry((row_key, col_key))
                .or_insert_with(Vec::new);
            if let Some(v) = val {
                entry.push(v);
            }
        }

        // Sort keys if requested
        if row_sort_order != 0 {
            row_keys.sort_by(|a, b| {
                if row_sort_order > 0 {
                    a.cmp(b)
                } else {
                    b.cmp(a)
                }
            });
        }
        if col_sort_order != 0 {
            col_keys.sort_by(|a, b| {
                if col_sort_order > 0 {
                    a.cmp(b)
                } else {
                    b.cmp(a)
                }
            });
        }

        // Build output grid
        let mut output: Vec<Vec<LiteralValue>> = Vec::new();

        // Header row: empty cells for row fields + column keys
        let generate_headers = field_headers == 2 || field_headers == 3;
        if generate_headers || has_headers {
            let mut header_row: Vec<LiteralValue> = Vec::new();
            // Empty cells for row field columns
            for _ in 0..rf_cols {
                header_row.push(LiteralValue::Empty);
            }
            // Column headers
            for col_key in &col_keys {
                // Split composite key and use the visible parts
                let parts: Vec<&str> = col_key.split('\x00').collect();
                header_row.push(LiteralValue::Text(parts.join(" ")));
            }
            // Total column header
            if col_total_depth >= 1 {
                header_row.push(LiteralValue::Text("Total".to_string()));
            }
            output.push(header_row);
        }

        // Data rows
        for row_key in &row_keys {
            let mut row: Vec<LiteralValue> = Vec::new();

            // Row field values
            let row_parts: Vec<&str> = row_key.split('\x00').collect();
            for part in &row_parts {
                row.push(LiteralValue::Text(part.to_string()));
            }

            // Values for each column
            let mut row_total_vals: Vec<f64> = Vec::new();
            for col_key in &col_keys {
                let key = (row_key.clone(), col_key.clone());
                let vals = pivot_data.get(&key).map(|v| v.as_slice()).unwrap_or(&[]);
                let result = aggregation.apply(vals);

                // Collect for row total
                row_total_vals.extend(vals);

                if result.is_nan() || vals.is_empty() {
                    row.push(LiteralValue::Empty);
                } else if result.fract() == 0.0 && result.abs() < i64::MAX as f64 {
                    row.push(LiteralValue::Int(result as i64));
                } else {
                    row.push(LiteralValue::Number(result));
                }
            }

            // Row total
            if col_total_depth >= 1 {
                let result = aggregation.apply(&row_total_vals);
                if result.is_nan() {
                    row.push(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
                } else if result.fract() == 0.0 && result.abs() < i64::MAX as f64 {
                    row.push(LiteralValue::Int(result as i64));
                } else {
                    row.push(LiteralValue::Number(result));
                }
            }

            output.push(row);
        }

        // Grand total row
        if row_total_depth >= 1 {
            let mut total_row: Vec<LiteralValue> = Vec::new();
            total_row.push(LiteralValue::Text("Total".to_string()));
            for _ in 1..rf_cols {
                total_row.push(LiteralValue::Empty);
            }

            let mut grand_total_vals: Vec<f64> = Vec::new();
            for col_key in &col_keys {
                let mut col_vals: Vec<f64> = Vec::new();
                for row_key in &row_keys {
                    let key = (row_key.clone(), col_key.clone());
                    if let Some(vals) = pivot_data.get(&key) {
                        col_vals.extend(vals);
                    }
                }
                grand_total_vals.extend(&col_vals);
                let result = aggregation.apply(&col_vals);
                if result.is_nan() {
                    total_row.push(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
                } else if result.fract() == 0.0 && result.abs() < i64::MAX as f64 {
                    total_row.push(LiteralValue::Int(result as i64));
                } else {
                    total_row.push(LiteralValue::Number(result));
                }
            }

            // Grand total of grand totals
            if col_total_depth >= 1 {
                let result = aggregation.apply(&grand_total_vals);
                if result.is_nan() {
                    total_row.push(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
                } else if result.fract() == 0.0 && result.abs() < i64::MAX as f64 {
                    total_row.push(LiteralValue::Int(result as i64));
                } else {
                    total_row.push(LiteralValue::Number(result));
                }
            }

            output.push(total_row);
        }

        if output.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Calc),
            )));
        }

        Ok(collapse_if_scalar(output, _ctx.date_system()))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }
        let array_view = args[0].range_view()?;
        let include_view = args[1].range_view()?;

        let (array_rows, array_cols) = array_view.dims();
        if array_rows == 0 || array_cols == 0 {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let (include_rows, include_cols) = include_view.dims();
        if include_rows != array_rows && include_rows != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        let mut result: Vec<Vec<LiteralValue>> = Vec::new();
        for r in 0..array_rows {
            let include_r = if include_rows == array_rows { r } else { 0 };
            let mut include = false;
            for c in 0..include_cols {
                if include_view.get_cell(include_r, c).is_truthy() {
                    include = true;
                    break;
                }
            }

            if include {
                let mut row_out: Vec<LiteralValue> = Vec::with_capacity(array_cols);
                for c in 0..array_cols {
                    row_out.push(array_view.get_cell(r, c));
                }
                result.push(row_out);
            }
        }

        if result.is_empty() {
            if args.len() >= 3 {
                return args[2].value();
            }
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Calc),
            )));
        }

        Ok(crate::traits::CalcValue::Range(
            crate::engine::range_view::RangeView::from_owned_rows(result, _ctx.date_system()),
        ))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let view = match args[0].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let (rows, cols) = view.dims();
        if rows == 0 || cols == 0 {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let by_col = if args.len() >= 2 {
            matches!(args[1].value()?.into_literal(), LiteralValue::Boolean(true))
        } else {
            false
        };
        let exactly_once = if args.len() >= 3 {
            matches!(args[2].value()?.into_literal(), LiteralValue::Boolean(true))
        } else {
            false
        };

        if by_col {
            #[derive(Hash, Eq, PartialEq, Clone)]
            struct ColKey(Vec<LiteralValue>);

            let mut order: Vec<ColKey> = Vec::new();
            let mut counts: HashMap<ColKey, usize> = HashMap::new();

            for c in 0..cols {
                let mut col_vals: Vec<LiteralValue> = Vec::with_capacity(rows);
                for r in 0..rows {
                    col_vals.push(view.get_cell(r, c));
                }
                let key = ColKey(col_vals);
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
            return Ok(collapse_if_scalar(out, _ctx.date_system()));
        }

        #[derive(Hash, Eq, PartialEq, Clone)]
        struct RowKey(Vec<LiteralValue>);

        let mut order: Vec<RowKey> = Vec::new();
        let mut counts: HashMap<RowKey, usize> = HashMap::new();
        for r in 0..rows {
            let mut row_vals: Vec<LiteralValue> = Vec::with_capacity(cols);
            for c in 0..cols {
                row_vals.push(view.get_cell(r, c));
            }
            let key = RowKey(row_vals);
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
        Ok(collapse_if_scalar(out, _ctx.date_system()))
    }
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // Extract numbers (allow float but coerce to i64 for dimensions)
        let num = |a: &ArgumentHandle| -> Result<f64, ExcelError> {
            Ok(match a.value()?.into_literal() {
                LiteralValue::Int(i) => i as f64,
                LiteralValue::Number(n) => n,
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
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
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

        Ok(collapse_if_scalar(out, _ctx.date_system()))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let view = match args[0].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let (rows, cols) = view.dims();
        if rows == 0 || cols == 0 {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let mut out: Vec<Vec<LiteralValue>> = vec![Vec::with_capacity(rows); cols];
        for (c, col) in out.iter_mut().enumerate().take(cols) {
            for r in 0..rows {
                col.push(view.get_cell(r, c));
            }
        }
        Ok(collapse_if_scalar(out, _ctx.date_system()))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let view = match args[0].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let (rows, cols) = view.dims();
        if rows == 0 || cols == 0 {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let height = rows as i64;
        let width = cols as i64;

        let num = |a: &ArgumentHandle| -> Result<i64, ExcelError> {
            Ok(match a.value()?.into_literal() {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
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
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        let (row_start, row_end) = if take_rows >= 0 {
            (0usize, take_rows as usize)
        } else {
            ((height + take_rows) as usize, height as usize)
        };

        let (col_start, col_end) = if let Some(tc) = take_cols {
            if tc.abs() > width {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
            if tc >= 0 {
                (0usize, tc as usize)
            } else {
                ((width + tc) as usize, width as usize)
            }
        } else {
            (0usize, width as usize)
        };

        if row_start >= row_end || col_start >= col_end {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(row_end - row_start);
        for r in row_start..row_end {
            let mut row_out: Vec<LiteralValue> = Vec::with_capacity(col_end - col_start);
            for c in col_start..col_end {
                row_out.push(view.get_cell(r, c));
            }
            out.push(row_out);
        }

        Ok(collapse_if_scalar(out, _ctx.date_system()))
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let view = match args[0].range_view() {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };
        let (rows, cols) = view.dims();
        if rows == 0 || cols == 0 {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let height = rows as i64;
        let width = cols as i64;

        let num = |a: &ArgumentHandle| -> Result<i64, ExcelError> {
            Ok(match a.value()?.into_literal() {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
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
            (0usize, (height + drop_rows).max(0) as usize)
        };

        let (col_start, col_end) = if let Some(dc) = drop_cols {
            if dc >= 0 {
                ((dc as usize).min(width as usize), width as usize)
            } else {
                (0usize, (width + dc).max(0) as usize)
            }
        } else {
            (0usize, width as usize)
        };

        if row_start >= row_end || col_start >= col_end {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(row_end - row_start);
        for r in row_start..row_end {
            let mut row_out: Vec<LiteralValue> = Vec::with_capacity(col_end - col_start);
            for c in col_start..col_end {
                row_out.push(view.get_cell(r, c));
            }
            out.push(row_out);
        }

        Ok(collapse_if_scalar(out, _ctx.date_system()))
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
    register_function(Arc::new(XMatchFn));
    register_function(Arc::new(SortFn));
    register_function(Arc::new(SortByFn));
    register_function(Arc::new(RandArrayFn));
    register_function(Arc::new(GroupByFn));
    register_function(Arc::new(PivotByFn));
}

/* ───────────────────────── tests ───────────────────────── */

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
    use std::sync::Arc;

    #[test]
    fn test_all_dynamic_functions_registered() {
        // Ensure builtins are registered
        crate::builtins::load_builtins();

        let functions = [
            "XLOOKUP",
            "FILTER",
            "UNIQUE",
            "SEQUENCE",
            "TRANSPOSE",
            "TAKE",
            "DROP",
            "XMATCH",
            "SORT",
            "SORTBY",
            "RANDARRAY",
            "GROUPBY",
            "PIVOTBY",
        ];

        for name in &functions {
            let result = crate::function_registry::get("", name);
            assert!(result.is_some(), "Function {} should be registered", name);
        }
    }

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    fn range(r: &str, sr: u32, sc: u32, er: u32, ec: u32) -> ASTNode {
        ASTNode::new(
            ASTNodeType::Reference {
                original: r.into(),
                reference: ReferenceType::range(None, Some(sr), Some(sc), Some(er), Some(ec)),
            },
            None,
        )
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
        let lookup_range = range("A1:A2", 1, 1, 2, 1);
        let return_range = range("B1:B2", 1, 2, 2, 2);
        let f = ctx.context.get_function("", "XLOOKUP").unwrap();
        let key_b = lit(LiteralValue::Text("b".into()));
        let args = vec![
            ArgumentHandle::new(&key_b, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Number(20.0));
        let key_missing = lit(LiteralValue::Text("z".into()));
        let if_nf = lit(LiteralValue::Text("NF".into()));
        let args_nf = vec![
            ArgumentHandle::new(&key_missing, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
            ArgumentHandle::new(&if_nf, &ctx),
        ];
        let v_nf = f
            .dispatch(&args_nf, &ctx.function_context(None))
            .unwrap()
            .into_literal();
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
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let return_range = range("B1:B3", 1, 2, 3, 2);
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
            .unwrap()
            .into_literal();
        assert_eq!(v_smaller, LiteralValue::Number(2.0));
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
            .unwrap()
            .into_literal();
        assert_eq!(v_larger, LiteralValue::Number(3.0));
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
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let return_range = range("B1:B3", 1, 2, 3, 2);
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
        let v_wild = f
            .dispatch(&args_wild, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v_wild, LiteralValue::Number(200.0));
        // Escaped wildcard literal ~* should not match Beta
        let pattern_lit_star = lit(LiteralValue::Text("~*eta".into()));
        let args_lit = vec![
            ArgumentHandle::new(&pattern_lit_star, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
            ArgumentHandle::new(&nf_binding, &ctx),
            ArgumentHandle::new(&match_mode_wild, &ctx),
        ];
        let v_lit = f
            .dispatch(&args_lit, &ctx.function_context(None))
            .unwrap()
            .into_literal();
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
        let v_nf = f
            .dispatch(&args_nf, &ctx.function_context(None))
            .unwrap()
            .into_literal();
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
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let return_range = range("B1:B3", 1, 2, 3, 2);
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
        let v_rev = f
            .dispatch(&args_rev, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v_rev, LiteralValue::Text("Last".into()));
    }

    #[test]
    fn xlookup_horizontal_returns_column_vector_for_matrix_return() {
        let wb = TestWorkbook::new().with_function(Arc::new(XLookupFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "C1", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "C2", LiteralValue::Int(3))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(4))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(5))
            .with_cell_a1("Sheet1", "C3", LiteralValue::Int(6));
        let ctx = wb.interpreter();
        let lookup_range = range("A1:C1", 1, 1, 1, 3);
        let return_range = range("A2:C3", 2, 1, 3, 3);
        let f = ctx.context.get_function("", "XLOOKUP").unwrap();
        let needle = lit(LiteralValue::Int(20));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(
                    a,
                    vec![
                        vec![LiteralValue::Number(2.0)],
                        vec![LiteralValue::Number(5.0)]
                    ]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn xlookup_vertical_returns_row_vector_for_matrix_return() {
        let wb = TestWorkbook::new().with_function(Arc::new(XLookupFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(101))
            .with_cell_a1("Sheet1", "C1", LiteralValue::Int(102))
            .with_cell_a1("Sheet1", "D1", LiteralValue::Int(103))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(201))
            .with_cell_a1("Sheet1", "C2", LiteralValue::Int(202))
            .with_cell_a1("Sheet1", "D2", LiteralValue::Int(203))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(301))
            .with_cell_a1("Sheet1", "C3", LiteralValue::Int(302))
            .with_cell_a1("Sheet1", "D3", LiteralValue::Int(303));
        let ctx = wb.interpreter();
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let return_range = range("B1:D3", 1, 2, 3, 4);
        let f = ctx.context.get_function("", "XLOOKUP").unwrap();
        let needle = lit(LiteralValue::Int(20));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&return_range, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(
                    a,
                    vec![vec![
                        LiteralValue::Number(201.0),
                        LiteralValue::Number(202.0),
                        LiteralValue::Number(203.0)
                    ]]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
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
        let array_range = range("A1:B2", 1, 1, 2, 2);
        let include_range = range("C1:C2", 1, 3, 2, 3);
        let f = ctx.context.get_function("", "FILTER").unwrap();
        let args = vec![
            ArgumentHandle::new(&array_range, &ctx),
            ArgumentHandle::new(&include_range, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 1);
                assert_eq!(
                    a[0],
                    vec![LiteralValue::Number(1.0), LiteralValue::Number(10.0)]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
        let wb2 = wb
            .with_cell_a1("Sheet1", "C1", LiteralValue::Boolean(false))
            .with_cell_a1("Sheet1", "C2", LiteralValue::Boolean(false));
        let ctx2 = wb2.interpreter();
        let f2 = ctx2.context.get_function("", "FILTER").unwrap();
        let empty_text = lit(LiteralValue::Text("EMPTY".into()));
        let args_empty = vec![
            ArgumentHandle::new(&array_range, &ctx2),
            ArgumentHandle::new(&include_range, &ctx2),
            ArgumentHandle::new(&empty_text, &ctx2),
        ];
        let v_empty = f2
            .dispatch(&args_empty, &ctx2.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v_empty, LiteralValue::Text("EMPTY".into()));
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
        let range = range("A1:A4", 1, 1, 4, 1);
        let f = ctx.context.get_function("", "UNIQUE").unwrap();
        let args = vec![ArgumentHandle::new(&range, &ctx)];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 3);
                assert_eq!(a[0][0], LiteralValue::Number(1.0));
            }
            _ => panic!("expected array"),
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
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(a[0][0], LiteralValue::Number(5.0));
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn transpose_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(TransposeFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20));
        let ctx = wb.interpreter();
        let arr = range("A1:B2", 1, 1, 2, 2);
        let f = ctx.context.get_function("", "TRANSPOSE").unwrap();
        let args = vec![ArgumentHandle::new(&arr, &ctx)];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(
                    a[0],
                    vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn take_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(TakeFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2));
        let ctx = wb.interpreter();
        let arr = range("A1:A2", 1, 1, 2, 1);
        let f = ctx.context.get_function("", "TAKE").unwrap();
        let one = lit(LiteralValue::Int(1));
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&one, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Number(1.0));
    }

    #[test]
    fn drop_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(DropFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2));
        let ctx = wb.interpreter();
        let arr = range("A1:A2", 1, 1, 2, 1);
        let f = ctx.context.get_function("", "DROP").unwrap();
        let one = lit(LiteralValue::Int(1));
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&one, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Number(2.0));
    }

    #[test]
    fn xmatch_exact_match_default() {
        let wb = TestWorkbook::new().with_function(Arc::new(XMatchFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("apple".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("banana".into()))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Text("cherry".into()));
        let ctx = wb.interpreter();
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let f = ctx.context.get_function("", "XMATCH").unwrap();
        let key = lit(LiteralValue::Text("banana".into()));
        let args = vec![
            ArgumentHandle::new(&key, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(2));
    }

    #[test]
    fn xmatch_exact_or_next_smaller() {
        let wb = TestWorkbook::new().with_function(Arc::new(XMatchFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(30));
        let ctx = wb.interpreter();
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let f = ctx.context.get_function("", "XMATCH").unwrap();
        let needle = lit(LiteralValue::Int(25));
        let match_mode = lit(LiteralValue::Int(-1)); // exact or next smaller
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&match_mode, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(2)); // 20 is the largest <= 25
    }

    #[test]
    fn xmatch_exact_or_next_larger() {
        let wb = TestWorkbook::new().with_function(Arc::new(XMatchFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(30));
        let ctx = wb.interpreter();
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let f = ctx.context.get_function("", "XMATCH").unwrap();
        let needle = lit(LiteralValue::Int(25));
        let match_mode = lit(LiteralValue::Int(1)); // exact or next larger
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&match_mode, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(3)); // 30 is the smallest >= 25
    }

    #[test]
    fn xmatch_wildcard() {
        let wb = TestWorkbook::new().with_function(Arc::new(XMatchFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("alpha".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("beta".into()))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Text("gamma".into()));
        let ctx = wb.interpreter();
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let f = ctx.context.get_function("", "XMATCH").unwrap();
        let pattern = lit(LiteralValue::Text("*eta".into()));
        let match_mode = lit(LiteralValue::Int(2)); // wildcard
        let args = vec![
            ArgumentHandle::new(&pattern, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&match_mode, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(2)); // "beta" matches "*eta"
    }

    #[test]
    fn xmatch_reverse_search() {
        let wb = TestWorkbook::new().with_function(Arc::new(XMatchFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(1)); // duplicate
        let ctx = wb.interpreter();
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let f = ctx.context.get_function("", "XMATCH").unwrap();
        let needle = lit(LiteralValue::Int(1));
        let match_mode = lit(LiteralValue::Int(0));
        let search_mode = lit(LiteralValue::Int(-1)); // last to first
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
            ArgumentHandle::new(&match_mode, &ctx),
            ArgumentHandle::new(&search_mode, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(3)); // last occurrence of 1
    }

    #[test]
    fn xmatch_not_found() {
        let wb = TestWorkbook::new().with_function(Arc::new(XMatchFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(3));
        let ctx = wb.interpreter();
        let lookup_range = range("A1:A3", 1, 1, 3, 1);
        let f = ctx.context.get_function("", "XMATCH").unwrap();
        let needle = lit(LiteralValue::Int(5));
        let args = vec![
            ArgumentHandle::new(&needle, &ctx),
            ArgumentHandle::new(&lookup_range, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Na),
            other => panic!("expected #N/A got {other:?}"),
        }
    }

    #[test]
    fn sort_basic_ascending() {
        let wb = TestWorkbook::new().with_function(Arc::new(SortFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(20));
        let ctx = wb.interpreter();
        let arr = range("A1:A3", 1, 1, 3, 1);
        let f = ctx.context.get_function("", "SORT").unwrap();
        let args = vec![ArgumentHandle::new(&arr, &ctx)];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 3);
                assert_eq!(a[0][0], LiteralValue::Number(10.0));
                assert_eq!(a[1][0], LiteralValue::Number(20.0));
                assert_eq!(a[2][0], LiteralValue::Number(30.0));
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn sort_descending() {
        let wb = TestWorkbook::new().with_function(Arc::new(SortFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(20));
        let ctx = wb.interpreter();
        let arr = range("A1:A3", 1, 1, 3, 1);
        let f = ctx.context.get_function("", "SORT").unwrap();
        let sort_index = lit(LiteralValue::Int(1));
        let sort_order = lit(LiteralValue::Int(-1)); // descending
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&sort_index, &ctx),
            ArgumentHandle::new(&sort_order, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 3);
                assert_eq!(a[0][0], LiteralValue::Number(30.0));
                assert_eq!(a[1][0], LiteralValue::Number(20.0));
                assert_eq!(a[2][0], LiteralValue::Number(10.0));
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn sort_by_column() {
        let wb = TestWorkbook::new().with_function(Arc::new(SortFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("Charlie".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(30))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("Alice".into()))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Text("Bob".into()))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(20));
        let ctx = wb.interpreter();
        let arr = range("A1:B3", 1, 1, 3, 2);
        let f = ctx.context.get_function("", "SORT").unwrap();
        let sort_index = lit(LiteralValue::Int(2)); // sort by column B
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&sort_index, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 3);
                // Should be sorted by column B: Alice(10), Bob(20), Charlie(30)
                assert_eq!(a[0][0], LiteralValue::Text("Alice".into()));
                assert_eq!(a[1][0], LiteralValue::Text("Bob".into()));
                assert_eq!(a[2][0], LiteralValue::Text("Charlie".into()));
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn sortby_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(SortByFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("Charlie".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("Alice".into()))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Text("Bob".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(3))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(2));
        let ctx = wb.interpreter();
        let arr = range("A1:A3", 1, 1, 3, 1);
        let by_arr = range("B1:B3", 1, 2, 3, 2);
        let f = ctx.context.get_function("", "SORTBY").unwrap();
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&by_arr, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 3);
                // Should be sorted by B values: Alice(1), Bob(2), Charlie(3)
                assert_eq!(a[0][0], LiteralValue::Text("Alice".into()));
                assert_eq!(a[1][0], LiteralValue::Text("Bob".into()));
                assert_eq!(a[2][0], LiteralValue::Text("Charlie".into()));
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn sortby_descending() {
        let wb = TestWorkbook::new().with_function(Arc::new(SortByFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Text("Charlie".into()))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("Alice".into()))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Text("Bob".into()))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(3))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(2));
        let ctx = wb.interpreter();
        let arr = range("A1:A3", 1, 1, 3, 1);
        let by_arr = range("B1:B3", 1, 2, 3, 2);
        let sort_order = lit(LiteralValue::Int(-1)); // descending
        let f = ctx.context.get_function("", "SORTBY").unwrap();
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&by_arr, &ctx),
            ArgumentHandle::new(&sort_order, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 3);
                // Should be sorted by B values descending: Charlie(3), Bob(2), Alice(1)
                assert_eq!(a[0][0], LiteralValue::Text("Charlie".into()));
                assert_eq!(a[1][0], LiteralValue::Text("Bob".into()));
                assert_eq!(a[2][0], LiteralValue::Text("Alice".into()));
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn randarray_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(RandArrayFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "RANDARRAY").unwrap();

        // Test basic 2x3 array with defaults
        let rows = lit(LiteralValue::Int(2));
        let cols = lit(LiteralValue::Int(3));
        let args = vec![
            ArgumentHandle::new(&rows, &ctx),
            ArgumentHandle::new(&cols, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(a[0].len(), 3);
                // Check all values are between 0 and 1
                for row in &a {
                    for cell in row {
                        match cell {
                            LiteralValue::Number(n) => {
                                assert!(*n >= 0.0 && *n < 1.0, "Value {n} not in [0, 1)");
                            }
                            other => panic!("expected Number got {other:?}"),
                        }
                    }
                }
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn randarray_whole_numbers() {
        let wb = TestWorkbook::new().with_function(Arc::new(RandArrayFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "RANDARRAY").unwrap();

        // Test 3x2 array with whole numbers between 1 and 10
        let rows = lit(LiteralValue::Int(3));
        let cols = lit(LiteralValue::Int(2));
        let min = lit(LiteralValue::Int(1));
        let max = lit(LiteralValue::Int(10));
        let whole = lit(LiteralValue::Boolean(true));
        let args = vec![
            ArgumentHandle::new(&rows, &ctx),
            ArgumentHandle::new(&cols, &ctx),
            ArgumentHandle::new(&min, &ctx),
            ArgumentHandle::new(&max, &ctx),
            ArgumentHandle::new(&whole, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 3);
                assert_eq!(a[0].len(), 2);
                // Check all values are integers between 1 and 10
                for row in &a {
                    for cell in row {
                        let n = match cell {
                            LiteralValue::Int(n) => *n as f64,
                            LiteralValue::Number(n) => *n,
                            other => panic!("expected Int or Number got {other:?}"),
                        };
                        assert!(n >= 1.0 && n <= 10.0, "Value {n} not in [1, 10]");
                        // Verify it's actually a whole number
                        assert!(n.fract() == 0.0, "Value {n} is not a whole number");
                    }
                }
            }
            other => panic!("expected array got {other:?}"),
        }
    }
}
