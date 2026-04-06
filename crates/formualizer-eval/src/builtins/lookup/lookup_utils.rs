//! Shared helpers for lookup-family functions (MATCH, VLOOKUP, HLOOKUP, XLOOKUP)
//! Provides unified coercion, comparison and approximate-mode selection logic.

use crate::engine::range_view::RangeView;
use arrow_array::Array;
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
    a.to_lowercase() == b.to_lowercase()
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
            let xl = x.to_lowercase();
            let yl = y.to_lowercase();
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

enum PreparedTextMatcher {
    Exact { folded_needle: String },
    Wildcard { compiled: CompiledWildcardPattern },
}

pub(crate) struct PreparedLookupMatcher<'a> {
    needle: &'a LiteralValue,
    text: Option<PreparedTextMatcher>,
}

impl<'a> PreparedLookupMatcher<'a> {
    pub(crate) fn new(needle: &'a LiteralValue, wildcard: bool) -> Self {
        let text = match needle {
            LiteralValue::Text(s) => {
                let folded = s.to_lowercase();
                if wildcard && (s.contains('*') || s.contains('?') || s.contains('~')) {
                    Some(PreparedTextMatcher::Wildcard {
                        compiled: CompiledWildcardPattern::from_folded(&folded),
                    })
                } else {
                    Some(PreparedTextMatcher::Exact {
                        folded_needle: folded,
                    })
                }
            }
            _ => None,
        };
        Self { needle, text }
    }

    pub(crate) fn matches(&self, candidate: &LiteralValue) -> bool {
        match (&self.text, candidate) {
            (
                Some(PreparedTextMatcher::Exact { folded_needle }),
                LiteralValue::Text(candidate_text),
            ) => candidate_text.to_lowercase() == *folded_needle,
            (
                Some(PreparedTextMatcher::Wildcard { compiled }),
                LiteralValue::Text(candidate_text),
            ) => {
                let folded_candidate = candidate_text.to_lowercase();
                compiled.matches_folded(&folded_candidate)
            }
            _ => cmp_for_lookup(self.needle, candidate)
                .map(|o| o == 0)
                .unwrap_or(false),
        }
    }
}

/// Exact equality leveraging cmp_for_lookup plus wildcard option (pattern side may have * or ?).
pub fn equals_maybe_wildcard(
    pattern: &LiteralValue,
    candidate: &LiteralValue,
    wildcard: bool,
) -> bool {
    PreparedLookupMatcher::new(pattern, wildcard).matches(candidate)
}

/// Detect ascending sort (strict or equal allowed) for slice according to cmp_for_lookup.
pub fn is_sorted_ascending(values: &[LiteralValue]) -> bool {
    values
        .windows(2)
        .all(|w| cmp_for_lookup(&w[0], &w[1]).is_some_and(|c| c <= 0))
}

/// Detect descending sort (strict or equal allowed).
pub fn is_sorted_descending(values: &[LiteralValue]) -> bool {
    values
        .windows(2)
        .all(|w| cmp_for_lookup(&w[0], &w[1]).is_some_and(|c| c >= 0))
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
                if let (Some(nn), Some(vv)) = (needle_num, value_to_f64_lenient(v))
                    && vv <= nn
                    && best.is_none_or(|b| {
                        value_to_f64_lenient(&values[b]).unwrap_or(f64::NEG_INFINITY) < vv
                    })
                {
                    best = Some(i);
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
                if let (Some(nn), Some(vv)) = (needle_num, value_to_f64_lenient(v))
                    && vv >= nn
                    && best.is_none_or(|b| {
                        value_to_f64_lenient(&values[b]).unwrap_or(f64::INFINITY) > vv
                    })
                {
                    best = Some(i);
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

#[derive(Clone, Debug)]
enum WildcardToken {
    AnySeq,
    AnyChar,
    Lit(Box<[char]>),
}

#[derive(Clone, Debug)]
struct CompiledWildcardPattern {
    tokens: Vec<WildcardToken>,
}

impl CompiledWildcardPattern {
    fn from_folded(pattern: &str) -> Self {
        let mut tokens: Vec<WildcardToken> = Vec::new();
        let mut lit = String::new();
        let mut chars = pattern.chars();
        while let Some(ch) = chars.next() {
            match ch {
                '~' => {
                    if let Some(next) = chars.next() {
                        lit.push(next);
                    } else {
                        lit.push('~');
                    }
                }
                '*' => {
                    if !lit.is_empty() {
                        tokens.push(WildcardToken::Lit(
                            lit.chars().collect::<Vec<_>>().into_boxed_slice(),
                        ));
                        lit.clear();
                    }
                    tokens.push(WildcardToken::AnySeq);
                }
                '?' => {
                    if !lit.is_empty() {
                        tokens.push(WildcardToken::Lit(
                            lit.chars().collect::<Vec<_>>().into_boxed_slice(),
                        ));
                        lit.clear();
                    }
                    tokens.push(WildcardToken::AnyChar);
                }
                _ => lit.push(ch),
            }
        }
        if !lit.is_empty() {
            tokens.push(WildcardToken::Lit(
                lit.chars().collect::<Vec<_>>().into_boxed_slice(),
            ));
        }

        let mut compact: Vec<WildcardToken> = Vec::new();
        for t in tokens {
            match t {
                WildcardToken::AnySeq => {
                    if !matches!(compact.last(), Some(WildcardToken::AnySeq)) {
                        compact.push(t);
                    }
                }
                _ => compact.push(t),
            }
        }

        Self { tokens: compact }
    }

    fn matches_folded(&self, text: &str) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        self.matches_folded_chars(&text_chars)
    }

    fn matches_folded_chars(&self, text: &[char]) -> bool {
        let mut ti = 0usize;
        let mut si = 0usize;
        let mut bt: Vec<(usize, usize)> = Vec::new();
        loop {
            if ti == self.tokens.len() {
                if si == text.len() {
                    return true;
                }
            } else {
                match &self.tokens[ti] {
                    WildcardToken::AnySeq => {
                        ti += 1;
                        bt.push((ti - 1, si + 1));
                        continue;
                    }
                    WildcardToken::AnyChar => {
                        if si < text.len() {
                            ti += 1;
                            si += 1;
                            continue;
                        }
                    }
                    WildcardToken::Lit(lit) => {
                        let ll = lit.len();
                        if si + ll <= text.len() && &text[si..si + ll] == lit.as_ref() {
                            ti += 1;
                            si += ll;
                            continue;
                        }
                    }
                }
            }
            if let Some((star_tok, new_si)) = bt.pop()
                && new_si <= text.len()
            {
                ti = star_tok + 1;
                si = new_si;
                continue;
            }
            return false;
        }
    }
}

/// Excel-style wildcard pattern matcher with escape (~) supporting *, ? and literal escaping of ~ * ?
pub fn wildcard_pattern_match(pattern: &str, text: &str) -> bool {
    let pattern_folded = pattern.to_lowercase();
    let text_folded = text.to_lowercase();
    let compiled = CompiledWildcardPattern::from_folded(&pattern_folded);
    compiled.matches_folded(&text_folded)
}

/// Find index of exact (or wildcard) match in values; returns first match (Excel semantics).
pub fn find_exact_index(
    values: &[LiteralValue],
    needle: &LiteralValue,
    wildcard: bool,
) -> Option<usize> {
    let matcher = PreparedLookupMatcher::new(needle, wildcard);
    for (i, v) in values.iter().enumerate() {
        if matcher.matches(v) {
            return Some(i);
        }
    }
    None
}

/// Find index of exact (or wildcard) match in a 1D RangeView; returns first match (Excel semantics).
/// Supports both single-column (vertical) and single-row (horizontal) views.
pub fn find_exact_index_in_view(
    view: &RangeView<'_>,
    needle: &LiteralValue,
    wildcard: bool,
) -> Result<Option<usize>, ExcelError> {
    let (rows, cols) = view.dims();
    let vertical = if cols == 1 {
        true
    } else if rows == 1 {
        false
    } else {
        // Not a 1D range
        return Ok(None);
    };

    match needle {
        LiteralValue::Number(n) => find_exact_number_in_view(view, *n, vertical),
        LiteralValue::Int(i) => find_exact_number_in_view(view, *i as f64, vertical),
        LiteralValue::Text(s) => find_exact_text_in_view(view, s, wildcard, vertical),
        LiteralValue::Boolean(b) => find_exact_boolean_in_view(view, *b, vertical),
        LiteralValue::Empty => find_exact_empty_in_view(view, vertical),
        LiteralValue::Error(e) => Err(e.clone()),
        _ => Ok(None),
    }
}

fn find_exact_number_in_view(
    view: &RangeView<'_>,
    n: f64,
    vertical: bool,
) -> Result<Option<usize>, ExcelError> {
    if vertical {
        for res in view.numbers_slices() {
            let (row_start, _row_len, cols) = res?;
            if !cols.is_empty() {
                let arr = &cols[0];
                for i in 0..arr.len() {
                    if !arr.is_null(i) && (arr.value(i) - n).abs() < 1e-12 {
                        return Ok(Some(row_start + i));
                    }
                }
            }
        }
    } else {
        // Horizontal: check columns in the first row segment
        for res in view.numbers_slices() {
            let (_row_start, _row_len, cols) = res?;
            for (c, arr) in cols.iter().enumerate() {
                if !arr.is_null(0) && (arr.value(0) - n).abs() < 1e-12 {
                    return Ok(Some(c));
                }
            }
        }
    }

    // Excel-like semantics: Empty cells compare equal to numeric zero.
    if n.abs() < 1e-12
        && let Some(idx) = find_exact_empty_in_view(view, vertical)?
    {
        return Ok(Some(idx));
    }

    Ok(None)
}

fn find_exact_text_in_view(
    view: &RangeView<'_>,
    s: &str,
    wildcard: bool,
    vertical: bool,
) -> Result<Option<usize>, ExcelError> {
    let needle_folded = s.to_lowercase();
    let compiled_wildcard = (wildcard && (s.contains('*') || s.contains('?') || s.contains('~')))
        .then(|| CompiledWildcardPattern::from_folded(&needle_folded));

    if vertical {
        for res in view.lowered_text_slices() {
            let (row_start, _row_len, cols) = res?;
            if !cols.is_empty() {
                let arr = &cols[0];
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        if let Some(pattern) = &compiled_wildcard {
                            if pattern.matches_folded(val) {
                                return Ok(Some(row_start + i));
                            }
                        } else if val == needle_folded {
                            return Ok(Some(row_start + i));
                        }
                    }
                }
            }
        }
    } else {
        for res in view.lowered_text_slices() {
            let (_row_start, _row_len, cols) = res?;
            for (c, arr) in cols.iter().enumerate() {
                if !arr.is_null(0) {
                    let val = arr.value(0);
                    if let Some(pattern) = &compiled_wildcard {
                        if pattern.matches_folded(val) {
                            return Ok(Some(c));
                        }
                    } else if val == needle_folded {
                        return Ok(Some(c));
                    }
                }
            }
        }
    }
    Ok(None)
}

fn find_exact_boolean_in_view(
    view: &RangeView<'_>,
    b: bool,
    vertical: bool,
) -> Result<Option<usize>, ExcelError> {
    if vertical {
        for res in view.booleans_slices() {
            let (row_start, _row_len, cols) = res?;
            if !cols.is_empty() {
                let arr = &cols[0];
                for i in 0..arr.len() {
                    if !arr.is_null(i) && arr.value(i) == b {
                        return Ok(Some(row_start + i));
                    }
                }
            }
        }
    } else {
        for res in view.booleans_slices() {
            let (_row_start, _row_len, cols) = res?;
            for (c, arr) in cols.iter().enumerate() {
                if !arr.is_null(0) && arr.value(0) == b {
                    return Ok(Some(c));
                }
            }
        }
    }
    Ok(None)
}

fn find_exact_empty_in_view(
    view: &RangeView<'_>,
    vertical: bool,
) -> Result<Option<usize>, ExcelError> {
    if vertical {
        for res in view.type_tags_slices() {
            let (row_start, _row_len, cols) = res?;
            if !cols.is_empty() {
                let arr = &cols[0];
                for i in 0..arr.len() {
                    if !arr.is_null(i) && arr.value(i) == crate::arrow_store::TypeTag::Empty as u8 {
                        return Ok(Some(row_start + i));
                    }
                }
            }
        }
    } else {
        for res in view.type_tags_slices() {
            let (_row_start, _row_len, cols) = res?;
            for (c, arr) in cols.iter().enumerate() {
                if !arr.is_null(0) && arr.value(0) == crate::arrow_store::TypeTag::Empty as u8 {
                    return Ok(Some(c));
                }
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, EvalConfig};
    use crate::test_workbook::TestWorkbook;
    use crate::traits::EvaluationContext;
    use formualizer_parse::parser::ReferenceType;
    use std::hint::black_box;
    use std::time::Instant;

    fn arrow_eval_config() -> EvalConfig {
        EvalConfig {
            arrow_storage_enabled: true,
            delta_overlay_enabled: true,
            write_formula_overlay_enabled: true,
            ..Default::default()
        }
    }

    fn build_vertical_text_engine(
        values: &[LiteralValue],
        chunk_rows: usize,
    ) -> Engine<TestWorkbook> {
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 1, chunk_rows);
        for value in values {
            ab.append_row("Sheet1", std::slice::from_ref(value))
                .unwrap();
        }
        ab.finish().unwrap();
        engine
    }

    fn build_horizontal_text_engine(
        values: &[LiteralValue],
        chunk_rows: usize,
    ) -> Engine<TestWorkbook> {
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", values.len(), chunk_rows);
        ab.append_row("Sheet1", values).unwrap();
        ab.finish().unwrap();
        engine
    }

    fn raw_baseline_find_exact_text_in_view(
        view: &RangeView<'_>,
        s: &str,
        wildcard: bool,
        vertical: bool,
    ) -> Result<Option<usize>, ExcelError> {
        let needle_folded = s.to_lowercase();
        let compiled_wildcard = (wildcard
            && (s.contains('*') || s.contains('?') || s.contains('~')))
        .then(|| CompiledWildcardPattern::from_folded(&needle_folded));

        if vertical {
            for res in view.text_slices() {
                let (row_start, _row_len, cols) = res?;
                if !cols.is_empty() {
                    let arr = cols[0]
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            let val = arr.value(i);
                            if let Some(pattern) = &compiled_wildcard {
                                let val_folded = val.to_lowercase();
                                if pattern.matches_folded(&val_folded) {
                                    return Ok(Some(row_start + i));
                                }
                            } else if val.to_lowercase() == needle_folded {
                                return Ok(Some(row_start + i));
                            }
                        }
                    }
                }
            }
        } else {
            for res in view.text_slices() {
                let (_row_start, _row_len, cols) = res?;
                for (c, arr_ref) in cols.iter().enumerate() {
                    let arr = arr_ref
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    if !arr.is_null(0) {
                        let val = arr.value(0);
                        if let Some(pattern) = &compiled_wildcard {
                            let val_folded = val.to_lowercase();
                            if pattern.matches_folded(&val_folded) {
                                return Ok(Some(c));
                            }
                        } else if val.to_lowercase() == needle_folded {
                            return Ok(Some(c));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    #[test]
    fn find_exact_index_in_view_matches_unicode_exact_and_wildcard_across_chunks_and_overlays() {
        let values = vec![LiteralValue::Empty; 8];
        let mut engine = build_vertical_text_engine(&values, 3);
        engine
            .set_cell_value("Sheet1", 2, 1, LiteralValue::Text("ИВАН".into()))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 7, 1, LiteralValue::Text("Иванов".into()))
            .unwrap();

        let range = ReferenceType::range(
            Some("Sheet1".to_string()),
            Some(1),
            Some(1),
            Some(8),
            Some(1),
        );
        let view = engine.resolve_range_view(&range, "Sheet1").unwrap();

        let exact = LiteralValue::Text("иван".into());
        let wildcard = LiteralValue::Text("ив?н*".into());

        assert_eq!(
            find_exact_index_in_view(&view, &exact, false).unwrap(),
            Some(1)
        );
        assert_eq!(
            find_exact_index_in_view(&view, &wildcard, true).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn find_exact_index_in_view_matches_unicode_exact_and_wildcard_horizontally() {
        let values = vec![
            LiteralValue::Text("Петр".into()),
            LiteralValue::Text("ИВАН".into()),
            LiteralValue::Text("Иванов".into()),
            LiteralValue::Text("Анна".into()),
        ];
        let engine = build_horizontal_text_engine(&values, 4);
        let range = ReferenceType::range(
            Some("Sheet1".to_string()),
            Some(1),
            Some(1),
            Some(1),
            Some(4),
        );
        let view = engine.resolve_range_view(&range, "Sheet1").unwrap();

        let exact = LiteralValue::Text("иван".into());
        let wildcard = LiteralValue::Text("ив?н*".into());

        assert_eq!(
            find_exact_index_in_view(&view, &exact, false).unwrap(),
            Some(1)
        );
        assert_eq!(
            find_exact_index_in_view(&view, &wildcard, true).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn wildcard_pattern_match_treats_unicode_scalar_as_single_char() {
        assert!(wildcard_pattern_match("?", "😀"));
        assert!(wildcard_pattern_match("??", "😀x"));
        assert!(!wildcard_pattern_match("?", "😀x"));
    }

    #[test]
    fn find_exact_index_matches_unicode_exact_and_wildcard_in_materialized_vectors() {
        let values = vec![
            LiteralValue::Text("Петр".into()),
            LiteralValue::Text("ИВАН".into()),
            LiteralValue::Text("Иванов".into()),
            LiteralValue::Text("Анна".into()),
        ];
        let exact = LiteralValue::Text("иван".into());
        let wildcard = LiteralValue::Text("ив?н*".into());

        assert_eq!(find_exact_index(&values, &exact, false), Some(1));
        assert_eq!(find_exact_index(&values, &wildcard, true), Some(1));
    }

    #[test]
    #[ignore = "benchmark smoke test"]
    fn benchmark_text_lookup_vector_path_vs_raw_baseline() {
        let total = 50_000usize;
        let mut values = Vec::with_capacity(total);
        for i in 0..total {
            if i + 1 == total {
                values.push(LiteralValue::Text("Иванов".into()));
            } else {
                values.push(LiteralValue::Text(format!("строка-{i}")));
            }
        }

        let exact_needle = LiteralValue::Text("иванов".into());
        let wildcard_needle = LiteralValue::Text("ив?н*".into());
        let iters = 30;

        let start = Instant::now();
        for _ in 0..iters {
            let mut out = None;
            for (i, value) in values.iter().enumerate() {
                if equals_maybe_wildcard(&exact_needle, black_box(value), false) {
                    out = Some(i);
                    break;
                }
            }
            black_box(out);
        }
        let raw_exact = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            black_box(find_exact_index(black_box(&values), &exact_needle, false));
        }
        let opt_exact = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            let mut out = None;
            for (i, value) in values.iter().enumerate() {
                if equals_maybe_wildcard(&wildcard_needle, black_box(value), true) {
                    out = Some(i);
                    break;
                }
            }
            black_box(out);
        }
        let raw_wildcard = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            black_box(find_exact_index(black_box(&values), &wildcard_needle, true));
        }
        let opt_wildcard = start.elapsed();

        println!(
            "vector exact raw={:?} opt={:?} speedup={:.2}x",
            raw_exact,
            opt_exact,
            raw_exact.as_secs_f64() / opt_exact.as_secs_f64()
        );
        println!(
            "vector wildcard raw={:?} opt={:?} speedup={:.2}x",
            raw_wildcard,
            opt_wildcard,
            raw_wildcard.as_secs_f64() / opt_wildcard.as_secs_f64()
        );
    }

    #[test]
    #[ignore = "benchmark smoke test"]
    fn benchmark_text_lookup_view_path_vs_raw_baseline() {
        let total_rows = 50_000u32;
        let chunk_rows = 512usize;
        let mut values = Vec::with_capacity(total_rows as usize);
        for i in 0..total_rows {
            if i + 1 == total_rows {
                values.push(LiteralValue::Text("Иванов".into()));
            } else {
                values.push(LiteralValue::Text(format!("строка-{i}")));
            }
        }

        let engine = build_vertical_text_engine(&values, chunk_rows);
        let range = ReferenceType::range(
            Some("Sheet1".to_string()),
            Some(1),
            Some(1),
            Some(total_rows),
            Some(1),
        );
        let view = engine.resolve_range_view(&range, "Sheet1").unwrap();

        let exact_needle = LiteralValue::Text("иванов".into());
        let wildcard_needle = LiteralValue::Text("ив?н*".into());
        let iters = 20;

        let start = Instant::now();
        for _ in 0..iters {
            black_box(raw_baseline_find_exact_text_in_view(&view, "иванов", false, true).unwrap());
        }
        let raw_exact = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            black_box(find_exact_index_in_view(&view, black_box(&exact_needle), false).unwrap());
        }
        let opt_exact = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            black_box(raw_baseline_find_exact_text_in_view(&view, "ив?н*", true, true).unwrap());
        }
        let raw_wildcard = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            black_box(find_exact_index_in_view(&view, black_box(&wildcard_needle), true).unwrap());
        }
        let opt_wildcard = start.elapsed();

        println!(
            "lookup exact raw={:?} opt={:?} speedup={:.2}x",
            raw_exact,
            opt_exact,
            raw_exact.as_secs_f64() / opt_exact.as_secs_f64()
        );
        println!(
            "lookup wildcard raw={:?} opt={:?} speedup={:.2}x",
            raw_wildcard,
            opt_wildcard,
            raw_wildcard.as_secs_f64() / opt_wildcard.as_secs_f64()
        );
    }
}
