//! Shared helpers for lookup-family functions (MATCH, VLOOKUP, HLOOKUP, XLOOKUP)
//! Provides unified coercion, comparison and approximate-mode selection logic.

use crate::engine::{DateSystem, range_view::RangeView};
use arrow_array::Array;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

/// Coerce a value to f64 with Excel-like rules for numeric comparisons:
/// - Number / Int: numeric
/// - Text: parsed if it looks numeric (lenient)
/// - Boolean: TRUE=1, FALSE=0
/// - Date / DateTime / Time: the Excel serial number. Excel has no date type
///   at the formula level -- a date cell holds a serial carrying a date number
///   format -- so a temporal value must order against plain numerics and
///   against other temporal values exactly as two numbers do.
/// - Empty: treated as 0
pub fn value_to_f64_lenient(v: &LiteralValue, date_system: DateSystem) -> Option<f64> {
    match v {
        LiteralValue::Number(n) => Some(*n),
        LiteralValue::Int(i) => Some(*i as f64),
        LiteralValue::Text(s) => s.parse::<f64>().ok(),
        LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        LiteralValue::Date(_) | LiteralValue::DateTime(_) | LiteralValue::Time(_) => {
            v.as_serial_number_for(date_system)
        }
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
pub fn cmp_for_lookup(a: &LiteralValue, b: &LiteralValue, date_system: DateSystem) -> Option<i32> {
    if let (Some(x), Some(y)) = (
        value_to_f64_lenient(a, date_system),
        value_to_f64_lenient(b, date_system),
    ) {
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
    date_system: DateSystem,
}

impl<'a> PreparedLookupMatcher<'a> {
    pub(crate) fn new(needle: &'a LiteralValue, wildcard: bool, date_system: DateSystem) -> Self {
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
        Self {
            needle,
            text,
            date_system,
        }
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
            // Excel exact lookups never match a text needle against a
            // non-text candidate: "20" does not find the number 20.
            (Some(_), _) => false,
            _ => cmp_for_lookup(self.needle, candidate, self.date_system)
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
    date_system: DateSystem,
) -> bool {
    PreparedLookupMatcher::new(pattern, wildcard, date_system).matches(candidate)
}

/// Whether Excel's approximate search visits `value` when looking for `needle`.
///
/// The legacy approximate lookups (`MATCH` with `match_type` 1/-1,
/// `VLOOKUP`/`HLOOKUP` with `range_lookup` TRUE) consider only entries in the
/// needle's comparable value set. A blank cell, or an incomparable entry such
/// as a text header sitting above a column of numbers, is skipped: it is neither
/// out-of-order data nor a matchable position. Errors are handled separately
/// and propagate rather than being classified as skippable.
pub fn is_searchable_for_approximate(
    value: &LiteralValue,
    needle: &LiteralValue,
    date_system: DateSystem,
) -> bool {
    !matches!(value, LiteralValue::Empty) && cmp_for_lookup(value, needle, date_system).is_some()
}

/// A lookup vector projected onto the entries an approximate search visits.
///
/// Positions are only materialized when something is actually skipped, so the
/// common case — a vector that is entirely in the needle's class — borrows the
/// original slice and allocates nothing. Indices returned by a search over this
/// projection are mapped back with [`SearchedVector::original_position`],
/// because Excel counts the answer from the top of the *original* range.
pub struct SearchedVector<'a> {
    values: &'a [LiteralValue],
    positions: Option<Vec<usize>>,
    date_system: DateSystem,
}

impl<'a> SearchedVector<'a> {
    pub fn new(
        values: &'a [LiteralValue],
        needle: &LiteralValue,
        date_system: DateSystem,
    ) -> Result<Self, ExcelError> {
        let first_skipped = values
            .iter()
            .position(|v| !is_searchable_for_approximate(v, needle, date_system));
        let positions = first_skipped.map(|skip| {
            let mut positions: Vec<usize> = (0..skip).collect();
            positions.extend(
                ((skip + 1)..values.len())
                    .filter(|&i| is_searchable_for_approximate(&values[i], needle, date_system)),
            );
            positions
        });
        Ok(Self {
            values,
            positions,
            date_system,
        })
    }

    pub fn len(&self) -> usize {
        match &self.positions {
            Some(positions) => positions.len(),
            None => self.values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> &'a LiteralValue {
        &self.values[self.original_position(index)]
    }

    /// Map an index in this projection back to its row in the original range.
    pub fn original_position(&self, index: usize) -> usize {
        match &self.positions {
            Some(positions) => positions[index],
            None => index,
        }
    }

    pub fn is_sorted_ascending(&self) -> bool {
        (1..self.len()).all(|i| {
            cmp_for_lookup(self.get(i - 1), self.get(i), self.date_system).is_some_and(|c| c <= 0)
        })
    }

    pub fn is_sorted_descending(&self) -> bool {
        (1..self.len()).all(|i| {
            cmp_for_lookup(self.get(i - 1), self.get(i), self.date_system).is_some_and(|c| c >= 0)
        })
    }
}

/// Detect ascending sort (strict or equal allowed) for slice according to cmp_for_lookup.
pub fn is_sorted_ascending(values: &[LiteralValue], date_system: DateSystem) -> bool {
    values
        .windows(2)
        .all(|w| cmp_for_lookup(&w[0], &w[1], date_system).is_some_and(|c| c <= 0))
}

/// Detect descending sort (strict or equal allowed).
pub fn is_sorted_descending(values: &[LiteralValue], date_system: DateSystem) -> bool {
    values
        .windows(2)
        .all(|w| cmp_for_lookup(&w[0], &w[1], date_system).is_some_and(|c| c >= 0))
}

/// Approximate mode selection (ascending):
/// match_mode 1 -> largest <= needle
/// match_mode -1 -> smallest >= needle (Excel MATCH uses -1 for descending; we adapt for XLOOKUP semantics)
pub fn approximate_select_ascending(
    values: &[LiteralValue],
    needle: &LiteralValue,
    mode: i32,
    date_system: DateSystem,
) -> Option<usize> {
    if values.is_empty() {
        return None;
    }
    let needle_num = value_to_f64_lenient(needle, date_system);
    match mode {
        -1 => {
            // exact or next smaller (our XLOOKUP -1 semantics) -> largest <= needle
            let mut best: Option<usize> = None;
            for (i, v) in values.iter().enumerate() {
                if cmp_for_lookup(v, needle, date_system)
                    .map(|c| c == 0)
                    .unwrap_or(false)
                {
                    return Some(i);
                }
                if let (Some(nn), Some(vv)) = (needle_num, value_to_f64_lenient(v, date_system))
                    && vv <= nn
                    && best.is_none_or(|b| {
                        value_to_f64_lenient(&values[b], date_system).unwrap_or(f64::NEG_INFINITY)
                            < vv
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
                if cmp_for_lookup(v, needle, date_system)
                    .map(|c| c == 0)
                    .unwrap_or(false)
                {
                    return Some(i);
                }
                if let (Some(nn), Some(vv)) = (needle_num, value_to_f64_lenient(v, date_system))
                    && vv >= nn
                    && best.is_none_or(|b| {
                        value_to_f64_lenient(&values[b], date_system).unwrap_or(f64::INFINITY) > vv
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
pub fn guard_sorted_ascending(
    values: &[LiteralValue],
    date_system: DateSystem,
) -> Result<(), ExcelError> {
    if !is_sorted_ascending(values, date_system) {
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
        let mut star_retry: Option<(usize, usize)> = None;
        loop {
            if ti == self.tokens.len() {
                if si == text.len() {
                    return true;
                }
            } else {
                match &self.tokens[ti] {
                    WildcardToken::AnySeq => {
                        ti += 1;
                        star_retry = Some((ti, si));
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
            if let Some((resume_ti, consumed_to)) = star_retry.as_mut()
                && *consumed_to < text.len()
            {
                *consumed_to += 1;
                ti = *resume_ti;
                si = *consumed_to;
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
    date_system: DateSystem,
) -> Option<usize> {
    let matcher = PreparedLookupMatcher::new(needle, wildcard, date_system);
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
    date_system: DateSystem,
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
        // A temporal needle searches the numeric lane by serial: the arrow
        // store keeps dates and times as serials under a temporal type tag,
        // so an exact lookup for a date must not fall through to "no match".
        LiteralValue::Date(_) | LiteralValue::DateTime(_) | LiteralValue::Time(_) => {
            match needle.as_serial_number_for(date_system) {
                Some(serial) => find_exact_number_in_view(view, serial, vertical),
                None => Ok(None),
            }
        }
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

    // The lowered-text lane can carry text renderings of non-text cells
    // (some load paths materialize them), so a lane hit is only a match
    // when the underlying cell value really is text: Excel exact lookups
    // never match a text needle against a number/boolean/date cell.
    if vertical {
        for res in view.lowered_text_slices() {
            let (row_start, _row_len, cols) = res?;
            if !cols.is_empty() {
                let arr = &cols[0];
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let hit = if let Some(pattern) = &compiled_wildcard {
                            pattern.matches_folded(val)
                        } else {
                            val == needle_folded
                        };
                        if hit && matches!(view.get_cell(row_start + i, 0), LiteralValue::Text(_)) {
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
                    let hit = if let Some(pattern) = &compiled_wildcard {
                        pattern.matches_folded(val)
                    } else {
                        val == needle_folded
                    };
                    if hit && matches!(view.get_cell(0, c), LiteralValue::Text(_)) {
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
            find_exact_index_in_view(&view, &exact, false, DateSystem::Excel1900).unwrap(),
            Some(1)
        );
        assert_eq!(
            find_exact_index_in_view(&view, &wildcard, true, DateSystem::Excel1900).unwrap(),
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
            find_exact_index_in_view(&view, &exact, false, DateSystem::Excel1900).unwrap(),
            Some(1)
        );
        assert_eq!(
            find_exact_index_in_view(&view, &wildcard, true, DateSystem::Excel1900).unwrap(),
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
    fn wildcard_pattern_match_retries_stars_without_branching() {
        // oracle: lo-verified
        let cases = [
            ("*", "", true),
            ("*", "bravo", true),
            ("br*", "bravo", true),
            ("*avo", "bravo", true),
            ("a**b***d", "abcbd", true),
            ("a*?d", "abcbd", true),
            ("*b*d", "abcbd", true),
            ("*b*d", "abcbx", false),
            ("~*", "*", true),
            ("~?", "?", true),
            ("~~", "~", true),
            ("a~**~?", "a*middle?", true),
            ("ив?н*", "ИВАНОВИЧ", true),
        ];

        for (pattern, text, expected) in cases {
            assert_eq!(
                wildcard_pattern_match(pattern, text),
                expected,
                "pattern={pattern:?}, text={text:?}"
            );
        }
    }

    #[test]
    fn wildcard_pattern_match_pathological_shape_completes_fast() {
        let text = "a".repeat(250_000);
        let start = Instant::now();
        assert!(!wildcard_pattern_match("*a*a*a*a*a*b", &text));
        assert!(
            start.elapsed() < std::time::Duration::from_secs(2),
            "wildcard retry became pathologically slow: {:?}",
            start.elapsed()
        );
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

        assert_eq!(
            find_exact_index(&values, &exact, false, DateSystem::Excel1900),
            Some(1)
        );
        assert_eq!(
            find_exact_index(&values, &wildcard, true, DateSystem::Excel1900),
            Some(1)
        );
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
                if equals_maybe_wildcard(
                    &exact_needle,
                    black_box(value),
                    false,
                    DateSystem::Excel1900,
                ) {
                    out = Some(i);
                    break;
                }
            }
            black_box(out);
        }
        let raw_exact = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            black_box(find_exact_index(
                black_box(&values),
                &exact_needle,
                false,
                DateSystem::Excel1900,
            ));
        }
        let opt_exact = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            let mut out = None;
            for (i, value) in values.iter().enumerate() {
                if equals_maybe_wildcard(
                    &wildcard_needle,
                    black_box(value),
                    true,
                    DateSystem::Excel1900,
                ) {
                    out = Some(i);
                    break;
                }
            }
            black_box(out);
        }
        let raw_wildcard = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            black_box(find_exact_index(
                black_box(&values),
                &wildcard_needle,
                true,
                DateSystem::Excel1900,
            ));
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
            black_box(
                find_exact_index_in_view(
                    &view,
                    black_box(&exact_needle),
                    false,
                    DateSystem::Excel1900,
                )
                .unwrap(),
            );
        }
        let opt_exact = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            black_box(raw_baseline_find_exact_text_in_view(&view, "ив?н*", true, true).unwrap());
        }
        let raw_wildcard = start.elapsed();

        let start = Instant::now();
        for _ in 0..iters {
            black_box(
                find_exact_index_in_view(
                    &view,
                    black_box(&wildcard_needle),
                    true,
                    DateSystem::Excel1900,
                )
                .unwrap(),
            );
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
