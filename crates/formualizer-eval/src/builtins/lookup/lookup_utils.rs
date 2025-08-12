//! Shared helpers for lookup-family functions (MATCH, VLOOKUP, HLOOKUP, XLOOKUP)
//! Provides unified coercion, comparison and approximate-mode selection logic.

use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

/// Coerce a value to f64 with Excel-like rules for numeric comparisons:
/// - Number / Int: numeric
/// - Text: parsed if it looks numeric (lenient)
/// - Boolean: TRUE=1, FALSE=0
/// - Empty: treated as 0
pub fn value_to_f64_lenient(v: &LiteralValue) -> Option<f64> {
    match v {
        LiteralValue::Number(n) => Some(*n),
        LiteralValue::Int(i) => Some(*i as f64),
        LiteralValue::Text(s) => s.parse::<f64>().ok(),
        LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Some(0.0),
        _ => None,
    }
}

/// Case-insensitive text equality (no wildcards).
pub fn text_equal_ci(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// Compare two values for ordering using lenient numeric coercion first, fallback to case-insensitive text.
/// Returns Some(ordering) where ordering <0, 0, >0 similar to cmp, or None if incomparable.
pub fn cmp_for_lookup(a: &LiteralValue, b: &LiteralValue) -> Option<i32> {
    if let (Some(x), Some(y)) = (value_to_f64_lenient(a), value_to_f64_lenient(b)) {
        if (x - y).abs() < 1e-12 {
            return Some(0);
        }
        return Some(if x < y { -1 } else { 1 });
    }
    match (a, b) {
        (LiteralValue::Text(x), LiteralValue::Text(y)) => {
            let xl = x.to_ascii_lowercase();
            let yl = y.to_ascii_lowercase();
            Some(match xl.cmp(&yl) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            })
        }
        (LiteralValue::Boolean(x), LiteralValue::Boolean(y)) => {
            let xv = if *x { 1 } else { 0 };
            let yv = if *y { 1 } else { 0 };
            Some(xv.cmp(&yv) as i32)
        }
        _ => None,
    }
}

/// Exact equality leveraging cmp_for_lookup plus wildcard option (pattern side may have * or ?).
pub fn equals_maybe_wildcard(
    pattern: &LiteralValue,
    candidate: &LiteralValue,
    wildcard: bool,
) -> bool {
    match (pattern, candidate) {
        (LiteralValue::Text(p), LiteralValue::Text(c))
            if wildcard && (p.contains('*') || p.contains('?') || p.contains('~')) =>
        {
            wildcard_pattern_match(p, c)
        }
        _ => cmp_for_lookup(pattern, candidate)
            .map(|o| o == 0)
            .unwrap_or(false),
    }
}

/// Detect ascending sort (strict or equal allowed) for slice according to cmp_for_lookup.
pub fn is_sorted_ascending(values: &[LiteralValue]) -> bool {
    values
        .windows(2)
        .all(|w| cmp_for_lookup(&w[0], &w[1]).map_or(false, |c| c <= 0))
}

/// Detect descending sort (strict or equal allowed).
pub fn is_sorted_descending(values: &[LiteralValue]) -> bool {
    values
        .windows(2)
        .all(|w| cmp_for_lookup(&w[0], &w[1]).map_or(false, |c| c >= 0))
}

/// Approximate mode selection (ascending):
/// match_mode 1 -> largest <= needle
/// match_mode -1 -> smallest >= needle (Excel MATCH uses -1 for descending; we adapt for XLOOKUP semantics)
pub fn approximate_select_ascending(
    values: &[LiteralValue],
    needle: &LiteralValue,
    mode: i32,
) -> Option<usize> {
    if values.is_empty() {
        return None;
    }
    let needle_num = value_to_f64_lenient(needle);
    match mode {
        -1 => {
            // exact or next smaller (our XLOOKUP -1 semantics) -> largest <= needle
            let mut best: Option<usize> = None;
            for (i, v) in values.iter().enumerate() {
                if cmp_for_lookup(v, needle).map(|c| c == 0).unwrap_or(false) {
                    return Some(i);
                }
                if let (Some(nn), Some(vv)) = (needle_num, value_to_f64_lenient(v)) {
                    if vv <= nn {
                        if best.map_or(true, |b| {
                            value_to_f64_lenient(&values[b]).unwrap_or(f64::NEG_INFINITY) < vv
                        }) {
                            best = Some(i);
                        }
                    }
                }
            }
            best
        }
        1 => {
            // exact or next larger -> smallest >= needle
            let mut best: Option<usize> = None;
            for (i, v) in values.iter().enumerate() {
                if cmp_for_lookup(v, needle).map(|c| c == 0).unwrap_or(false) {
                    return Some(i);
                }
                if let (Some(nn), Some(vv)) = (needle_num, value_to_f64_lenient(v)) {
                    if vv >= nn {
                        if best.map_or(true, |b| {
                            value_to_f64_lenient(&values[b]).unwrap_or(f64::INFINITY) > vv
                        }) {
                            best = Some(i);
                        }
                    }
                }
            }
            best
        }
        _ => None,
    }
}

/// Validate ascending sort for approximate selection; return #N/A if unsorted.
pub fn guard_sorted_ascending(values: &[LiteralValue]) -> Result<(), ExcelError> {
    if !is_sorted_ascending(values) {
        return Err(ExcelError::new(ExcelErrorKind::Na));
    }
    Ok(())
}

/// Excel-style wildcard pattern matcher with escape (~) supporting *, ? and literal escaping of ~ * ?
pub fn wildcard_pattern_match(pattern: &str, text: &str) -> bool {
    #[derive(Clone, Copy, Debug)]
    enum Token<'a> {
        AnySeq,
        AnyChar,
        Lit(&'a str),
    }
    let mut tokens: Vec<Token> = Vec::new();
    let bytes = pattern.as_bytes();
    let mut i = 0;
    let mut lit_start = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'~' => {
                if i + 1 < bytes.len() {
                    if lit_start < i {
                        tokens.push(Token::Lit(&pattern[lit_start..i]));
                    }
                    tokens.push(Token::Lit(&pattern[i + 1..i + 2]));
                    i += 2;
                    lit_start = i;
                } else {
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
            _ => {
                i += 1;
            }
        }
    }
    if lit_start < bytes.len() {
        tokens.push(Token::Lit(&pattern[lit_start..]));
    }
    // collapse consecutive *
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
    fn match_tokens(tokens: &[Token], text: &str) -> bool {
        let mut ti = 0usize;
        let mut si = 0usize;
        let mut bt: Vec<(usize, usize)> = Vec::new();
        let tb = tokens;
        let b = text.as_bytes();
        loop {
            if ti == tb.len() {
                if si == b.len() {
                    return true;
                }
            } else {
                match tb[ti] {
                    Token::AnySeq => {
                        ti += 1;
                        bt.push((ti - 1, si + 1));
                        continue;
                    }
                    Token::AnyChar => {
                        if si < b.len() {
                            ti += 1;
                            si += 1;
                            continue;
                        }
                    }
                    Token::Lit(l) => {
                        let ll = l.len();
                        if si + ll <= b.len() && text[si..si + ll].eq_ignore_ascii_case(l) {
                            ti += 1;
                            si += ll;
                            continue;
                        }
                    }
                }
            }
            if let Some((star_tok, new_si)) = bt.pop() {
                if new_si <= b.len() {
                    ti = star_tok + 1;
                    si = new_si;
                    continue;
                }
            }
            return false;
        }
    }
    match_tokens(&compact, text)
}

/// Find index of exact (or wildcard) match in values; returns first match (Excel semantics).
pub fn find_exact_index(
    values: &[LiteralValue],
    needle: &LiteralValue,
    wildcard: bool,
) -> Option<usize> {
    for (i, v) in values.iter().enumerate() {
        if equals_maybe_wildcard(needle, v, wildcard) {
            return Some(i);
        }
    }
    None
}
