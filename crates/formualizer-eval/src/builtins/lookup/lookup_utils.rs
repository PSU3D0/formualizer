//! Shared helpers for lookup-family functions (MATCH, VLOOKUP, HLOOKUP, XLOOKUP)
//! Provides unified coercion, comparison and approximate-mode selection logic.

use crate::builtins::utils::{GENERATED_ARRAY_MAX_COLS, GENERATED_ARRAY_MAX_ROWS};
use crate::engine::{DateSystem, range_view::RangeView};
use arrow_array::Array;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::ReferenceType;

/// The `(rows, cols)` a cell or range reference declares: an open row bound
/// reaches row 1 or row 1,048,576, an open column bound column 1 or column
/// 16,384. A view is trimmed to the used region along an open axis, so this
/// is the extent of the reference, not of its data. Any other reference (a
/// defined name, a table) has none.
pub(crate) fn reference_extent(reference: &ReferenceType) -> Option<(usize, usize)> {
    let span = |start: Option<u32>, end: Option<u32>, limit: i64| -> usize {
        let start = i64::from(start.unwrap_or(1));
        let end = end.map_or(limit, i64::from);
        (end - start + 1).max(0) as usize
    };
    match reference {
        ReferenceType::Cell { .. } => Some((1, 1)),
        ReferenceType::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            ..
        } => Some((
            span(*start_row, *end_row, GENERATED_ARRAY_MAX_ROWS),
            span(*start_col, *end_col, GENERATED_ARRAY_MAX_COLS),
        )),
        _ => None,
    }
}

/// A lookup whose lookup value is an array (or a multi-cell range): one
/// lookup per element, as in Excel, giving an array of the same shape. An
/// element whose own lookup returns several cells contributes its first
/// cell. A single lookup value never comes here, so the scalar path (and
/// the family memo that reuses its results) is unchanged.
pub(crate) fn lift_lookup_values<'b>(
    ctx: &dyn crate::traits::FunctionContext<'b>,
    needles: Vec<Vec<LiteralValue>>,
    mut one: impl FnMut(LiteralValue) -> Result<crate::traits::CalcValue<'b>, ExcelError>,
) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
    let (rows, cols) = (needles.len(), needles.first().map_or(0, |r| r.len()));
    if let Some(error) = crate::engine::resource_ledger::materialized_cells_exceed(
        rows,
        cols,
        ctx.materialized_array_cells(),
    ) {
        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(error)));
    }
    let mut out = Vec::with_capacity(rows);
    for row in needles {
        let mut out_row = Vec::with_capacity(row.len());
        for needle in row {
            out_row.push(match one(needle) {
                Ok(value) => match value.into_literal() {
                    LiteralValue::Array(found) => found
                        .into_iter()
                        .next()
                        .and_then(|r| r.into_iter().next())
                        .unwrap_or(LiteralValue::Empty),
                    other => other,
                },
                Err(e) => LiteralValue::Error(e),
            });
        }
        out.push(out_row);
    }
    Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(out)))
}

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

/// Precomputed key for SORT / SORTBY ordering.
///
/// Excel's sort order is a total order over value classes: numbers (including
/// dates and times, by serial) < text (case-insensitive) < FALSE < TRUE <
/// errors, with blanks last in both directions. Keys are built once per sorted
/// element, so a comparison never allocates: text is folded with the same
/// `to_lowercase` the lookup comparison uses.
///
/// Ties: two keys are equal when they are in the same class and have the same
/// number (`-0` equals `0`), the same folded text or the same boolean; all
/// errors are equal to each other. Callers sort stably, so tied elements keep
/// their input order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SortKey {
    Number(f64),
    Text(String),
    Boolean(bool),
    Error,
    Blank,
}

impl SortKey {
    pub(crate) fn new(v: &LiteralValue, date_system: DateSystem) -> Self {
        let number = match v {
            LiteralValue::Number(n) => Some(*n),
            LiteralValue::Int(i) => Some(*i as f64),
            LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_)
            | LiteralValue::Duration(_) => v.as_serial_number_for(date_system),
            LiteralValue::Text(s) => return SortKey::Text(s.to_lowercase()),
            LiteralValue::Boolean(b) => return SortKey::Boolean(*b),
            LiteralValue::Empty => return SortKey::Blank,
            LiteralValue::Error(_) | LiteralValue::Array(_) | LiteralValue::Pending => None,
        };
        match number {
            // `+ 0.0` turns -0 into +0, so `total_cmp` treats them as equal.
            Some(n) => SortKey::Number(n + 0.0),
            None => SortKey::Error,
        }
    }

    fn class(&self) -> u8 {
        match self {
            SortKey::Number(_) => 0,
            SortKey::Text(_) => 1,
            SortKey::Boolean(_) => 2,
            SortKey::Error => 3,
            SortKey::Blank => 4,
        }
    }

    /// Ascending order of two keys (blanks last).
    fn cmp_ascending(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (SortKey::Number(a), SortKey::Number(b)) => a.total_cmp(b),
            (SortKey::Text(a), SortKey::Text(b)) => a.cmp(b),
            (SortKey::Boolean(a), SortKey::Boolean(b)) => a.cmp(b),
            _ => self.class().cmp(&other.class()),
        }
    }

    /// Order for a sort in the given direction. Descending reverses the
    /// ascending order, except that blanks stay last, as in Excel.
    pub(crate) fn cmp_directed(&self, other: &Self, ascending: bool) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (SortKey::Blank, SortKey::Blank) => Ordering::Equal,
            (SortKey::Blank, _) => Ordering::Greater,
            (_, SortKey::Blank) => Ordering::Less,
            _ if ascending => self.cmp_ascending(other),
            _ => other.cmp_ascending(self),
        }
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

fn is_numeric_exact_value(value: &LiteralValue) -> bool {
    matches!(
        value,
        LiteralValue::Number(_)
            | LiteralValue::Int(_)
            | LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_)
    )
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
        // Exact lookup eligibility is asymmetric: blank needles still coerce
        // to zero, but a blank range entry can never satisfy a match (#319).
        // Numeric and temporal needles share one candidate class; unlike the
        // lenient comparison helper, they do not admit boolean or text values.
        if matches!(candidate, LiteralValue::Empty) {
            return false;
        }
        if matches!(self.needle, LiteralValue::Empty) || is_numeric_exact_value(self.needle) {
            return is_numeric_exact_value(candidate)
                && cmp_for_lookup(self.needle, candidate, self.date_system) == Some(0);
        }
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
/// needle's comparable value set. A blank cell, error cell, or incomparable
/// entry such as a text header sitting above a column of numbers is skipped: it
/// is neither out-of-order data nor a matchable position.
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
        LiteralValue::Empty => find_exact_number_in_view(view, 0.0, vertical),
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

    // Excel exact-match semantics: blank cells are NOT equal to numeric
    // zero. MATCH(0, {blank,1,2}, 0) returns #N/A, not a position. (#319)

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
    fn find_exact_number_in_view_does_not_match_blank_as_zero() {
        // Regression test for #319: MATCH(0, {blank, 1, 2}, 0) must return
        // None (which surfaces as #N/A), not match the blank cell.
        let values = vec![
            LiteralValue::Empty,
            LiteralValue::Number(1.0),
            LiteralValue::Number(2.0),
        ];
        let engine = build_vertical_text_engine(&values, 8);
        let range = ReferenceType::range(
            Some("Sheet1".to_string()),
            Some(1),
            Some(1),
            Some(3),
            Some(1),
        );
        let view = engine.resolve_range_view(&range, "Sheet1").unwrap();

        // Searching for 0 must NOT match the blank cell at index 0.
        assert_eq!(
            find_exact_index_in_view(
                &view,
                &LiteralValue::Number(0.0),
                false,
                DateSystem::Excel1900
            )
            .unwrap(),
            None
        );
        assert_eq!(
            find_exact_index_in_view(&view, &LiteralValue::Int(0), false, DateSystem::Excel1900)
                .unwrap(),
            None
        );

        // Ratified S6: a blank needle coerces to zero, not a blank candidate.
        assert_eq!(
            find_exact_index_in_view(&view, &LiteralValue::Empty, false, DateSystem::Excel1900)
                .unwrap(),
            None
        );

        // Searching for 1 still works normally.
        assert_eq!(
            find_exact_index_in_view(
                &view,
                &LiteralValue::Number(1.0),
                false,
                DateSystem::Excel1900
            )
            .unwrap(),
            Some(1)
        );
    }

    #[test]
    fn zero_needles_only_find_real_zero_in_both_orientations() {
        let values = vec![
            LiteralValue::Empty,
            LiteralValue::Text(String::new()),
            LiteralValue::Text("0".into()),
            LiteralValue::Boolean(false),
            LiteralValue::Number(-0.0),
            LiteralValue::Number(0.0),
            LiteralValue::Empty,
            LiteralValue::Boolean(false),
            LiteralValue::Text("0".into()),
        ];
        let no_numeric_zero = vec![
            LiteralValue::Empty,
            LiteralValue::Text(String::new()),
            LiteralValue::Text("0".into()),
            LiteralValue::Boolean(false),
            LiteralValue::Text("0".into()),
            LiteralValue::Boolean(false),
        ];
        for vertical in [true, false] {
            let make_rows = |values: &[LiteralValue]| {
                if vertical {
                    values.iter().cloned().map(|value| vec![value]).collect()
                } else {
                    vec![values.to_vec()]
                }
            };
            let view = RangeView::from_owned_rows(make_rows(&values), DateSystem::Excel1900);
            let missing_view =
                RangeView::from_owned_rows(make_rows(&no_numeric_zero), DateSystem::Excel1900);
            for needle in [
                LiteralValue::Number(0.0),
                LiteralValue::Number(-0.0),
                LiteralValue::Empty,
            ] {
                assert_eq!(
                    find_exact_index_in_view(&view, &needle, false, DateSystem::Excel1900).unwrap(),
                    Some(4)
                );
                assert_eq!(
                    find_exact_index_in_view(&missing_view, &needle, false, DateSystem::Excel1900)
                        .unwrap(),
                    None
                );
            }
        }
    }

    #[test]
    fn materialized_zero_needles_only_match_numeric_zero() {
        let values = vec![
            LiteralValue::Empty,
            LiteralValue::Text(String::new()),
            LiteralValue::Text("0".into()),
            LiteralValue::Boolean(false),
            LiteralValue::Number(-0.0),
            LiteralValue::Number(0.0),
            LiteralValue::Boolean(false),
            LiteralValue::Text("0".into()),
        ];
        let no_numeric_zero = vec![
            LiteralValue::Empty,
            LiteralValue::Text(String::new()),
            LiteralValue::Text("0".into()),
            LiteralValue::Boolean(false),
            LiteralValue::Text("0".into()),
            LiteralValue::Boolean(false),
        ];
        for needle in [
            LiteralValue::Number(0.0),
            LiteralValue::Number(-0.0),
            LiteralValue::Empty,
        ] {
            assert_eq!(
                find_exact_index(&values, &needle, false, DateSystem::Excel1900),
                Some(4)
            );
            assert_eq!(
                find_exact_index(&no_numeric_zero, &needle, false, DateSystem::Excel1900),
                None
            );
        }
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

#[cfg(test)]
mod sort_key_tests {
    use super::*;

    /// The comparator SORT and SORTBY used before `SortKey`.
    fn old_cmp(a: &LiteralValue, b: &LiteralValue, ascending: bool) -> std::cmp::Ordering {
        let cmp = cmp_for_lookup(a, b, DateSystem::Excel1900).unwrap_or(0);
        if ascending { cmp.cmp(&0) } else { 0.cmp(&cmp) }
    }

    struct XorShift(u64);
    impl XorShift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// One random single-class array: the old comparator is a consistent
    /// order on it and agrees with Excel's order (no near-ties, no numeric
    /// text, no cross-class pairs).
    fn random_array(rng: &mut XorShift) -> Vec<LiteralValue> {
        let len = rng.below(101) as usize;
        let class = rng.below(3);
        (0..len)
            .map(|_| match class {
                0 => {
                    // A grid of values (spacing >= 0.25) with duplicates, both
                    // zero signs and Int/Number mixes of the same value.
                    let v = (rng.below(41) as f64 - 20.0) * 0.25;
                    match rng.below(4) {
                        0 if v.fract() == 0.0 => LiteralValue::Int(v as i64),
                        1 if v == 0.0 => LiteralValue::Number(-0.0),
                        2 => LiteralValue::Number(v * 1e6),
                        _ => LiteralValue::Number(v),
                    }
                }
                1 => {
                    // Letters that never parse as a number ("nan", "inf").
                    const LETTERS: &[u8] = b"abcdxyzABCDXYZ";
                    let n = 1 + rng.below(3) as usize;
                    let s: String = (0..n)
                        .map(|_| LETTERS[rng.below(LETTERS.len() as u64) as usize] as char)
                        .collect();
                    LiteralValue::Text(s)
                }
                _ => LiteralValue::Boolean(rng.below(2) == 1),
            })
            .collect()
    }

    #[test]
    fn sort_key_matches_old_comparator_where_it_agreed_with_excel() {
        let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
        let mut compared = 0usize;
        let mut long = 0usize;
        for _ in 0..20_000 {
            let values = random_array(&mut rng);
            for ascending in [true, false] {
                let mut old: Vec<usize> = (0..values.len()).collect();
                old.sort_by(|&a, &b| old_cmp(&values[a], &values[b], ascending));
                let keys: Vec<SortKey> = values
                    .iter()
                    .map(|v| SortKey::new(v, DateSystem::Excel1900))
                    .collect();
                let mut new: Vec<usize> = (0..values.len()).collect();
                new.sort_by(|&a, &b| keys[a].cmp_directed(&keys[b], ascending));
                assert_eq!(old, new, "{values:?} ascending={ascending}");
                compared += 1;
                if values.len() >= 20 {
                    long += 1;
                }
            }
        }
        assert_eq!(compared, 40_000);
        assert!(long > 30_000, "{long}");
    }

    #[test]
    fn sort_key_is_excel_order() {
        let ds = DateSystem::Excel1900;
        let ordered = [
            LiteralValue::Number(-1.0),
            LiteralValue::Int(2),
            LiteralValue::Text("10".into()),
            LiteralValue::Text("a".into()),
            LiteralValue::Text("B".into()),
            LiteralValue::Boolean(false),
            LiteralValue::Boolean(true),
            LiteralValue::Error(ExcelError::new_na()),
            LiteralValue::Empty,
        ];
        for (i, a) in ordered.iter().enumerate() {
            for (j, b) in ordered.iter().enumerate() {
                let (ka, kb) = (SortKey::new(a, ds), SortKey::new(b, ds));
                assert_eq!(ka.cmp_directed(&kb, true), i.cmp(&j), "{a:?} {b:?}");
                let desc = if i == ordered.len() - 1 || j == ordered.len() - 1 {
                    i.cmp(&j)
                } else {
                    j.cmp(&i)
                };
                assert_eq!(ka.cmp_directed(&kb, false), desc, "{a:?} {b:?}");
            }
        }
        let ties = [
            (LiteralValue::Number(0.0), LiteralValue::Number(-0.0)),
            (LiteralValue::Int(3), LiteralValue::Number(3.0)),
            (
                LiteralValue::Text("Ab".into()),
                LiteralValue::Text("aB".into()),
            ),
            (
                LiteralValue::Error(ExcelError::new_na()),
                LiteralValue::Error(ExcelError::new_num()),
            ),
        ];
        for (a, b) in ties {
            let (ka, kb) = (SortKey::new(&a, ds), SortKey::new(&b, ds));
            assert_eq!(ka.cmp_directed(&kb, true), std::cmp::Ordering::Equal);
            assert_eq!(ka.cmp_directed(&kb, false), std::cmp::Ordering::Equal);
        }
    }
}

/* ───────────── Excel's approximate-match search (LOOKUP) ───────────── */

/// A value as Excel's approximate-match search compares it. Only keys of the
/// same kind compare, and exactly: a number never matches numeric text, a
/// boolean or a blank cell, and text is never parsed as a number.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LookupKey {
    /// A number, date, time or duration, as its serial number.
    Number(f64),
    /// Text, folded with `to_lowercase` (Excel ignores case).
    Text(String),
    /// A boolean, `FALSE < TRUE`.
    Boolean(bool),
}

impl LookupKey {
    const KINDS: usize = 3;

    /// The key of a lookup value: a blank lookup value is the number 0. An
    /// error, an array or a pending value has none.
    pub(crate) fn for_lookup_value(value: &LiteralValue, date_system: DateSystem) -> Option<Self> {
        match value {
            LiteralValue::Empty => Some(Self::Number(0.0)),
            other => Self::for_cell(other, date_system),
        }
    }

    /// The key of a searched cell. A blank cell, an error, an array or a
    /// pending value has none: the search never compares it.
    pub(crate) fn for_cell(value: &LiteralValue, date_system: DateSystem) -> Option<Self> {
        match value {
            LiteralValue::Boolean(b) => Some(Self::Boolean(*b)),
            LiteralValue::Text(s) => Some(Self::Text(s.to_lowercase())),
            LiteralValue::Empty
            | LiteralValue::Error(_)
            | LiteralValue::Array(_)
            | LiteralValue::Pending => None,
            other => other
                .as_serial_number_for(date_system)
                .filter(|n| !n.is_nan())
                .map(Self::Number),
        }
    }

    fn kind(&self) -> usize {
        match self {
            Self::Number(_) => 0,
            Self::Text(_) => 1,
            Self::Boolean(_) => 2,
        }
    }

    /// `self` against `other`, or `None` for keys of different kinds.
    fn compare(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Number(a), Self::Number(b)) => a.partial_cmp(b),
            (Self::Text(a), Self::Text(b)) => Some(a.cmp(b)),
            (Self::Boolean(a), Self::Boolean(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// A position in the tables of [`SearchCells`]. A searched vector is one row
/// or one column of a sheet, or of an array no larger, so it fits.
fn table_position(i: usize) -> u32 {
    u32::try_from(i).unwrap_or(u32::MAX)
}

/// The cells one approximate-match search runs over, keyed once. The search
/// of [`excel_approximate_search`] steps forward over the cells it cannot
/// compare and scans the run of equal values it hits: both are linear, so
/// the two tables that make them O(1) are built on first need and kept for
/// the next search of the same vector (an array of lookup values).
pub(crate) struct SearchCells {
    keys: Vec<Option<LookupKey>>,
    len: usize,
    /// Per key kind, the first position at or after `i` holding a key of
    /// that kind (`keys.len()` when none).
    next_comparable: [std::cell::OnceCell<Vec<u32>>; LookupKey::KINDS],
    /// The last position of the run of equal keys starting at `i`.
    run_end: std::cell::OnceCell<Vec<u32>>,
}

impl SearchCells {
    /// The cells `values`, followed by blank cells up to `len` (the trimmed
    /// tail of a whole-column reference). `len` is never less than
    /// `values.len()`.
    pub(crate) fn new<'v>(
        values: impl IntoIterator<Item = &'v LiteralValue>,
        len: usize,
        date_system: DateSystem,
    ) -> Self {
        let keys: Vec<_> = values
            .into_iter()
            .map(|v| LookupKey::for_cell(v, date_system))
            .collect();
        Self {
            len: len.max(keys.len()),
            keys,
            next_comparable: Default::default(),
            run_end: Default::default(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The cell at `i` against `needle`, or `None` when it cannot compare.
    fn compare(&self, i: usize, needle: &LookupKey) -> Option<std::cmp::Ordering> {
        self.keys.get(i)?.as_ref()?.compare(needle)
    }

    fn next_comparable(&self, kind: usize) -> &[u32] {
        self.next_comparable[kind].get_or_init(|| {
            let n = self.keys.len();
            let mut next = vec![table_position(n); n + 1];
            for i in (0..n).rev() {
                next[i] = match &self.keys[i] {
                    Some(key) if key.kind() == kind => table_position(i),
                    _ => next[i + 1],
                };
            }
            next
        })
    }

    fn run_end(&self) -> &[u32] {
        self.run_end.get_or_init(|| {
            let n = self.keys.len();
            let mut end = vec![0u32; n];
            for i in (0..n).rev() {
                let continues = i + 1 < n
                    && matches!(
                        (&self.keys[i], &self.keys[i + 1]),
                        (Some(a), Some(b)) if a.compare(b) == Some(std::cmp::Ordering::Equal)
                    );
                end[i] = if continues {
                    end[i + 1]
                } else {
                    table_position(i)
                };
            }
            end
        })
    }

    /// The first position after `mid` and before `hi` whose cell compares
    /// with `needle`, and how it compares.
    fn first_comparable_after(
        &self,
        mid: usize,
        hi: usize,
        needle: &LookupKey,
    ) -> Option<(usize, std::cmp::Ordering)> {
        let start = mid + 1;
        if start >= self.keys.len() {
            return None;
        }
        let next = self.next_comparable(needle.kind())[start] as usize;
        if next >= hi {
            return None;
        }
        Some((next, self.compare(next, needle)?))
    }

    /// The last position before `hi` of the run of cells equal to the cell
    /// at `mid`, which equals the needle.
    fn end_of_equal_run(&self, mid: usize, hi: usize) -> usize {
        (self.run_end()[mid] as usize).min(hi - 1)
    }
}

/// Excel's approximate-match search (largest value <= needle), as LOOKUP,
/// VLOOKUP(..,TRUE) and MATCH(..,1) run it in Excel. No sort check. Cells
/// from `values.len()` up to `len` are blank (the trimmed tail of a
/// whole-column reference). Returns a 0-based position, or None (#N/A).
///
/// Microsoft does not document it; this is Apache POI's reconstruction
/// (`LookupUtils.performBinarySearch`), without its exception for a step
/// forward that reaches the last cell before `hi`, which Excel contradicts.
/// `lo` and `hi` are exclusive bounds, and the midpoint is the lower one. A
/// cell the search cannot compare (blank, error, another type) sends it
/// forward to the first one it can before `hi`; when there is none, the
/// range ends before the midpoint. A cell equal to the needle ends the search
/// at the end of its run of equal cells.
pub(crate) fn excel_approximate_search(cells: &SearchCells, needle: &LookupKey) -> Option<usize> {
    use std::cmp::Ordering;

    let (mut lo, mut hi) = (-1isize, cells.len() as isize);
    loop {
        if hi - lo < 2 {
            return usize::try_from(lo).ok();
        }
        let mut mid = lo + (hi - lo) / 2;
        let ord = match cells.compare(mid as usize, needle) {
            Some(ord) => ord,
            None => match cells.first_comparable_after(mid as usize, hi as usize, needle) {
                None => {
                    hi = mid;
                    continue;
                }
                Some((next, ord)) => {
                    mid = next as isize;
                    ord
                }
            },
        };
        match ord {
            Ordering::Less => lo = mid,
            Ordering::Greater => hi = mid,
            Ordering::Equal => return Some(cells.end_of_equal_run(mid as usize, hi as usize)),
        }
    }
}

/* ───────────── LOOKUP's result read past its end ───────────── */

/// The cells LOOKUP may read from its result reference: from the first cell
/// of `result` up to the length LOOKUP searches in `lookup`, rightward for a
/// one-row result (one cell included), downward for a one-column result,
/// clipped at the sheet edge. None when `result` is long enough or 2-D.
/// Both arguments are cell or range references written in the formula.
///
/// Excel reads a result range shorter than the lookup vector past its end
/// (`=LOOKUP(3,K20:K22,L20:L21)` reads `L22`). The dependency graph knows
/// only the references a formula names, so the reference walkers of
/// `engine::refs` give this footprint as one more reference of the formula,
/// and LOOKUP reads past the end of its result only inside it.
pub(crate) fn lookup_result_footprint(
    lookup: &ReferenceType,
    result: &ReferenceType,
) -> Option<ReferenceType> {
    let (lookup_rows, lookup_cols) = written_extent(lookup)?;
    let (result_rows, result_cols) = written_extent(result)?;
    // The length LOOKUP searches: along the first row of a lookup vector
    // wider than tall, down its first column otherwise.
    let searched = if lookup_cols > lookup_rows {
        lookup_cols
    } else {
        lookup_rows
    };
    let (sheet, row, col, row_abs, col_abs, end_row_abs, end_col_abs) = match result {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => (sheet, *row, *col, *row_abs, *col_abs, *row_abs, *col_abs),
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
            ..
        } => (
            sheet,
            start_row.unwrap_or(1),
            start_col.unwrap_or(1),
            *start_row_abs,
            *start_col_abs,
            *end_row_abs,
            *end_col_abs,
        ),
        _ => return None,
    };
    let reach = |start: u32, limit: i64| -> u32 {
        let end = (i64::from(start) + searched as i64 - 1).min(limit);
        u32::try_from(end).unwrap_or(start)
    };
    let (end_row, end_col) = if result_rows == 1 && result_cols < searched {
        (row, reach(col, GENERATED_ARRAY_MAX_COLS))
    } else if result_cols == 1 && result_rows > 1 && result_rows < searched {
        (reach(row, GENERATED_ARRAY_MAX_ROWS), col)
    } else {
        return None;
    };
    Some(ReferenceType::Range {
        sheet: sheet.clone(),
        start_row: Some(row),
        start_col: Some(col),
        end_row: Some(end_row),
        end_col: Some(end_col),
        start_row_abs: row_abs,
        start_col_abs: col_abs,
        end_row_abs,
        end_col_abs,
    })
}

/// The extent of a cell or range reference written in a formula, or None.
fn written_extent(reference: &ReferenceType) -> Option<(usize, usize)> {
    match reference {
        ReferenceType::Cell { .. } | ReferenceType::Range { .. } => reference_extent(reference),
        _ => None,
    }
}

/// Whether a LOOKUP call with these written lookup and result references
/// must keep its formula out of every path that shares one dependency set
/// across the cells of a family (load-time grouping, the formula plane, the
/// authority's relocatable templates). A footprint cannot always be shifted
/// from the anchor: fill `=LOOKUP(99,K$20:K22,P20)` down one row and
/// `P20:R20` becomes `P21:S21`, longer, not just lower. So the formula is
/// evaluated cell by cell, with its own footprint, when it has a footprint
/// at its own cell, or when an argument has mixed `$` flags along an axis
/// (its length changes from cell to cell). A LOOKUP whose result is long
/// enough and whose ranges are fully absolute or fully relative is shared.
pub(crate) fn lookup_footprint_blocks_sharing(
    lookup: &ReferenceType,
    result: &ReferenceType,
) -> bool {
    let mixed = |reference: &ReferenceType| match reference {
        ReferenceType::Range {
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
            ..
        } => start_row_abs != end_row_abs || start_col_abs != end_col_abs,
        _ => false,
    };
    lookup_result_footprint(lookup, result).is_some() || mixed(lookup) || mixed(result)
}

/// Whether the function the formula calls as `name` reads its result past
/// its end ([`FnCaps::READS_PAST_RESULT`], LOOKUP's), resolved through the
/// global registry, which also resolves Excel prefixes (`_xlfn.`) and
/// aliases. A custom function registered in its place globally does not
/// have the capability. See [`call_reads_past_result`] for the provider.
///
/// [`FnCaps::READS_PAST_RESULT`]: crate::function::FnCaps::READS_PAST_RESULT
pub(crate) fn reads_past_result(name: &str) -> bool {
    crate::function_registry::get("", name).is_some_and(|function| {
        crate::external_callbacks::metadata(
            function.as_ref(),
            crate::function::FnCaps::empty,
            || function.caps(),
        )
        .contains(crate::function::FnCaps::READS_PAST_RESULT)
    })
}

/// Normalized names whose calls get no LOOKUP footprint (the engine's
/// `DependencyGraph::lookup_footprint_suppressed`).
pub(crate) type FootprintSuppressions = rustc_hash::FxHashSet<String>;

/// No suppressed name: global resolution alone, for walkers with no engine.
pub(crate) fn no_footprint_suppressions() -> &'static FootprintSuppressions {
    static EMPTY: std::sync::LazyLock<FootprintSuppressions> =
        std::sync::LazyLock::new(FootprintSuppressions::default);
    &EMPTY
}

/// Whether a call written as `name` has a LOOKUP footprint, and so whether
/// LOOKUP reads past its result's end for it: one predicate for the
/// reference walkers and the interpreter, so that the footprint dependency
/// exists exactly when the builtin reads past the end.
///
/// The walkers cannot reach the workbook's function provider, which
/// evaluation resolves names through, so this asks the global registry
/// ([`reads_past_result`]) and the names the engine found its provider
/// resolving otherwise (`suppressed`, a workbook-local override of LOOKUP).
/// Both differences fail safe: a provider-local alias of LOOKUP (a name the
/// global registry does not know) gets no footprint and reads blank past
/// its end, never a stale value; an override gets no footprint and never
/// runs the builtin. When the answer changes for a name (an override
/// registered or removed, a global alias of LOOKUP registered later), the
/// engine rebuilds the dependencies of the formulas calling it
/// (`DependencyGraph::refresh_lookup_footprint_suppressions`), so formulas
/// ingested earlier follow too. Volatility and dynamic-dependency detection
/// (`is_ast_volatile`, `is_ast_dynamic`) still resolve through the global
/// registry alone.
pub(crate) fn call_reads_past_result(name: &str, suppressed: &FootprintSuppressions) -> bool {
    call_reads_past_result_unless(name, |name| footprint_suppressed(name, suppressed))
}

/// [`call_reads_past_result`] with the suppressed names asked through
/// `suppressed`, as the interpreter does through its context. A spelling
/// with several Excel prefixes (`_xlfn._xlws.LOOKUP`) never reads past its
/// result's end: the engine finds the spellings its provider resolves
/// otherwise among the registry's names and their single-prefix spellings,
/// so it could not tell whether such a spelling is overridden.
pub(crate) fn call_reads_past_result_unless(
    name: &str,
    suppressed: impl FnOnce(&str) -> bool,
) -> bool {
    !crate::function_registry::has_compound_excel_prefix(name)
        && !suppressed(name)
        && reads_past_result(name)
}

/// Whether `suppressed` names the call written as `name`.
pub(crate) fn footprint_suppressed(name: &str, suppressed: &FootprintSuppressions) -> bool {
    !suppressed.is_empty()
        && suppressed.contains(&crate::function_registry::normalized_function_name(name))
}

#[cfg(test)]
mod excel_search_tests {
    use super::*;
    use proptest::prelude::*;
    use std::cmp::Ordering;

    /// The search as the plan writes it, with linear steps and scans: the
    /// reference the indexed search must agree with.
    fn plain_search(cells: &SearchCells, needle: &LookupKey) -> Option<usize> {
        let compare = |i: isize| cells.compare(i as usize, needle);
        let (mut lo, mut hi) = (-1isize, cells.len() as isize);
        loop {
            if hi - lo < 2 {
                return usize::try_from(lo).ok();
            }
            let mut mid = lo + (hi - lo) / 2;
            let ord = match compare(mid) {
                Some(ord) => ord,
                None => match ((mid + 1)..hi).find_map(|i| compare(i).map(|o| (i, o))) {
                    None => {
                        hi = mid;
                        continue;
                    }
                    Some((next, ord)) => {
                        mid = next;
                        ord
                    }
                },
            };
            match ord {
                Ordering::Less => lo = mid,
                Ordering::Greater => hi = mid,
                Ordering::Equal => {
                    let mut end = mid;
                    while end + 1 < hi && compare(end + 1) == Some(Ordering::Equal) {
                        end += 1;
                    }
                    return Some(end as usize);
                }
            }
        }
    }

    fn e() -> LiteralValue {
        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div))
    }
    const B: LiteralValue = LiteralValue::Empty;

    fn n(v: f64) -> LiteralValue {
        LiteralValue::Number(v)
    }
    fn t(s: &str) -> LiteralValue {
        LiteralValue::Text(s.into())
    }
    fn nums(vs: &[f64]) -> Vec<LiteralValue> {
        vs.iter().copied().map(n).collect()
    }
    /// Numbers, `_` for a blank cell and `#` for an error: `"3,1,_,#,3"`.
    fn cells(spec: &str) -> Vec<LiteralValue> {
        spec.split(',')
            .map(|c| match c {
                "_" => B,
                "#" => e(),
                num => n(num.parse().unwrap()),
            })
            .collect()
    }

    /// The 1-based position Excel's search returns, 0 for #N/A.
    fn search(values: &[LiteralValue], len: usize, needle: &LiteralValue) -> usize {
        let cells = SearchCells::new(values, len, DateSystem::Excel1900);
        let key = LookupKey::for_lookup_value(needle, DateSystem::Excel1900).unwrap();
        let found = excel_approximate_search(&cells, &key);
        assert_eq!(found, plain_search(&cells, &key), "indexed vs plain");
        found.map_or(0, |i| i + 1)
    }

    fn search_all(values: &[LiteralValue], needle: &LiteralValue) -> usize {
        search(values, values.len(), needle)
    }

    /// The 17 published Excel results of the plan's "Evidence" section.
    #[test]
    fn published_excel_results() {
        let t5: Vec<f64> = std::iter::repeat_n(33.0, 10)
            .chain([8., 33., 33., 6., 33., 33., 4., 33., 33., 33., 33.])
            .collect();
        let tab6: Vec<f64> = [33., 33., 33., 33., 10., 33., 33., 33., 5., 33., 100.]
            .into_iter()
            .chain(std::iter::repeat_n(33.0, 9))
            .chain([22.])
            .collect();
        let mrexcel: Vec<LiteralValue> = ["w", "x", "y", "z", "zz", "Ω", "ΩΩ"]
            .iter()
            .map(|s| t(s))
            .chain(std::iter::repeat_n(B, 4))
            .chain(["w", "x", "y", "z", "zz", "Ω", "ΩΩ"].iter().map(|s| t(s)))
            .collect();
        let mut z_errors: Vec<LiteralValue> = std::iter::repeat_with(e).take(18).collect();
        z_errors[3] = n(1.0);
        z_errors[14] = n(1.0);

        let cases: Vec<(&str, Vec<LiteralValue>, LiteralValue, usize)> = vec![
            ("issue {1,#DIV/0!,1}", cells("1,#,1"), n(2.), 3),
            ("issue {1,#,#,1,1}", cells("1,#,#,1,1"), n(2.), 5),
            ("issue control", nums(&[1., 1., 1.]), n(2.), 3),
            (
                "POI tab1",
                nums(&[33., 33., 33., 100., 33., 33., 33., 33., 33., 33.]),
                n(33.),
                10,
            ),
            (
                "POI tab2",
                nums(&[33., 33., 33., 33., 100., 33., 33., 33., 33., 33.]),
                n(33.),
                4,
            ),
            (
                "POI tab3",
                nums(&[33., 33., 33., 33., 33., 33., 5., 33., 100., 33.]),
                n(33.),
                6,
            ),
            (
                "POI tab4",
                nums(&[32., 32., 32., 31., 100., 32., 32., 32., 32., 32.]),
                n(33.),
                4,
            ),
            ("POI tab5 (33)", nums(&t5), n(33.), 16),
            ("POI tab5 (10)", nums(&t5), n(10.), 11),
            ("POI tab6", nums(&tab6), n(33.), 8),
            (
                "MS Q&A 7 values",
                nums(&[1., 1., 1., 1., 1., 0., 1.]),
                n(1.),
                5,
            ),
            (
                "MS Q&A 15 values, -1",
                nums(&[1., 1., 1., 1., 1., 0., 1., -1., 1., 1., 1., 1., 1., 0., 1.]),
                n(1.),
                13,
            ),
            (
                "MS Q&A 15 values, 2",
                nums(&[1., 1., 1., 1., 1., 0., 1., 2., 1., 1., 1., 1., 1., 0., 1.]),
                n(1.),
                5,
            ),
            ("MrExcel z", mrexcel.clone(), t("z"), 15),
            ("MrExcel Omega", mrexcel, t("Ω"), 17),
            ("MATCH(9.99E+307,1/(..=\"z\"))", z_errors, n(9.99e307), 15),
            (
                "MAP-994",
                vec![t("MAP-994"), t("MAP-10977"), t("MAP-320")],
                t("MAP-994"),
                3,
            ),
        ];
        for (name, values, needle, excel) in cases {
            assert_eq!(search_all(&values, &needle), excel, "{name}");
        }
    }

    /// The expectations of the five unit tests of the textbook search that
    /// LOOKUP ran before.
    #[test]
    fn sorted_data() {
        let v = nums(&[10., 20., 30.]);
        assert_eq!(search_all(&[], &n(1.)), 0);
        assert_eq!(search_all(&v, &n(5.)), 0);
        assert_eq!(search_all(&v, &n(20.)), 2);
        assert_eq!(search_all(&v, &n(25.)), 2);
        assert_eq!(search_all(&v, &n(100.)), 3);
    }

    /// Excel's step rule (probe cases 8, 9, 17–19 and R1–R4): a step forward
    /// that reaches a larger cell ends the range before that cell, the last
    /// one before `hi` included.
    #[test]
    fn step_rule() {
        assert_eq!(search_all(&cells("3,1,_,3,1"), &n(2.)), 2);
        assert_eq!(search_all(&cells("3,1,_,_,3"), &n(2.)), 2);
        assert_eq!(search_all(&cells("3,1,_,_,_,_,3"), &n(2.)), 0);
        assert_eq!(search_all(&cells("_,2,1,_,#,_,3"), &n(2.)), 3);
        assert_eq!(search_all(&cells("1,2,1,_,_,_,3"), &n(2.)), 3);
        assert_eq!(search_all(&cells("3,2,#,#,3"), &n(2.)), 2);
        assert_eq!(search_all(&cells("5,#,#,#,1"), &n(2.)), 5);
        assert_eq!(search_all(&cells("1,#,#,2,2,5"), &n(2.)), 5);
        assert_eq!(search_all(&cells("9,8,7,6,5"), &n(7.)), 3);
    }

    /// The type table: only keys of the needle's kind compare, exactly.
    #[test]
    fn keys_of_another_kind_never_compare() {
        use LiteralValue::{Boolean, Int};
        let ds = DateSystem::Excel1900;
        let key = |v: &LiteralValue| LookupKey::for_cell(v, ds);
        assert_eq!(key(&Int(2)), Some(LookupKey::Number(2.0)));
        assert_eq!(
            key(&LiteralValue::Date(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
            )),
            Some(LookupKey::Number(45293.0))
        );
        assert_eq!(key(&t("AbC")), Some(LookupKey::Text("abc".into())));
        assert_eq!(key(&t("2")), Some(LookupKey::Text("2".into())));
        assert_eq!(key(&Boolean(true)), Some(LookupKey::Boolean(true)));
        assert_eq!(key(&B), None);
        assert_eq!(key(&e()), None);
        assert_eq!(key(&LiteralValue::Pending), None);
        assert_eq!(key(&LiteralValue::Array(vec![vec![n(1.)]])), None);
        assert_eq!(
            LookupKey::for_lookup_value(&B, ds),
            Some(LookupKey::Number(0.0))
        );
        assert_eq!(LookupKey::for_lookup_value(&e(), ds), None);

        // Probe cases 10–14 and 22–29.
        assert_eq!(search_all(&[n(1.), t("2"), n(3.)], &n(2.)), 1);
        assert_eq!(search_all(&[n(1.), Boolean(true), n(3.)], &n(2.)), 1);
        assert_eq!(search_all(&[Boolean(true)], &n(1.)), 0);
        let mixed = [n(1.), t("a"), n(2.), t("b"), n(3.)];
        assert_eq!(search_all(&mixed, &n(2.)), 3);
        assert_eq!(
            search_all(&[Boolean(false), Boolean(true)], &Boolean(true)),
            2
        );
        assert_eq!(search_all(&[t("A"), t("B"), t("C")], &t("b")), 2);
        assert_eq!(search_all(&[t("A"), t("a")], &t("a")), 2);
        assert_eq!(search_all(&nums(&[-1., 0., 1.]), &B), 2);
        assert_eq!(search_all(&nums(&[1., 2., 3.]), &t("2")), 0);
        assert_eq!(search_all(&[t("1"), t("10"), t("2")], &t("2")), 3);
        assert_eq!(search_all(&nums(&[1., 2., 3.]), &Boolean(true)), 0);
        assert_eq!(search_all(&[n(0.1 + 0.2)], &n(0.3)), 0);
        assert_eq!(search_all(&nums(&[0., 1e-13]), &n(0.)), 1);
        assert_eq!(search_all(&nums(&[-0.0]), &n(0.)), 1);
        assert_eq!(search_all(&[t(""), t("a")], &t("")), 1);
        assert_eq!(search_all(&[t(""), t("a")], &B), 0);
        assert_eq!(search_all(&[B, n(1.)], &n(5.)), 2);
    }

    /// `T1:T6` against `T:T` (probe cases 20a–20c): the blank tail of a
    /// whole column moves the probes.
    #[test]
    fn declared_length_moves_the_probes() {
        let v = nums(&[5., 6., 9., 1., 8., 4.]);
        assert_eq!(search(&v, 6, &n(1.)), 0);
        assert_eq!(search(&v, 100, &n(1.)), 0);
        assert_eq!(search(&v, 1_048_576, &n(1.)), 4);
    }

    fn cell() -> impl Strategy<Value = LiteralValue> {
        prop_oneof![
            (0i64..4).prop_map(|i| LiteralValue::Number(i as f64)),
            (0usize..3).prop_map(|i| LiteralValue::Text(["a", "B", "b"][i].into())),
            any::<bool>().prop_map(LiteralValue::Boolean),
            Just(LiteralValue::Empty),
            Just(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na))),
        ]
    }

    proptest! {
        /// The indexed search equals the plain loop.
        #[test]
        fn indexed_search_equals_the_plain_loop(
            values in proptest::collection::vec(cell(), 0..40),
            tail in 0usize..20,
            needle in cell(),
        ) {
            let cells = SearchCells::new(&values, values.len() + tail, DateSystem::Excel1900);
            if let Some(key) = LookupKey::for_lookup_value(&needle, DateSystem::Excel1900) {
                prop_assert_eq!(
                    excel_approximate_search(&cells, &key),
                    plain_search(&cells, &key)
                );
            }
        }
    }
}

#[cfg(test)]
mod footprint_tests {
    use super::*;
    use formualizer_parse::parser::{ASTNodeType, parse};

    /// The reference written as `text` (`=` excluded).
    fn r(text: &str) -> ReferenceType {
        match parse(format!("={text}")).unwrap().node_type {
            ASTNodeType::Reference { reference, .. } => reference,
            other => panic!("{text} is not a reference: {other:?}"),
        }
    }

    fn footprint(lookup: &str, result: &str) -> Option<String> {
        lookup_result_footprint(&r(lookup), &r(result)).map(|f| f.to_string())
    }

    /// The table of the plan.
    #[test]
    fn footprint_covers_the_whole_result_up_to_the_searched_length() {
        assert_eq!(footprint("K20:K22", "L20:L21").as_deref(), Some("L20:L22"));
        assert_eq!(footprint("K20:K22", "P20").as_deref(), Some("P20:R20"));
        assert_eq!(footprint("A:A", "B1:B10").as_deref(), Some("B1:B1048576"));
        assert_eq!(
            footprint("Keys!K20:K22", "Values!L20:L21").as_deref(),
            Some("Values!L20:L22")
        );
        assert_eq!(footprint("K20:K22", "L20:L22"), None);
        assert_eq!(footprint("K20:K22", "MyName"), None);
        // A row result for a column lookup, a short result not at the top,
        // a row lookup with a one-cell result, a 2-D result, a whole column.
        assert_eq!(footprint("G20:G22", "H24:I24").as_deref(), Some("H24:J24"));
        assert_eq!(footprint("G20:G22", "P21:P22").as_deref(), Some("P21:P23"));
        assert_eq!(footprint("G30:I30", "P30").as_deref(), Some("P30:R30"));
        assert_eq!(footprint("G20:G22", "H20:I21"), None);
        assert_eq!(footprint("A:A", "B:B"), None);
        // A 2-D lookup vector searches its first column when tall.
        assert_eq!(footprint("K26:L28", "M26").as_deref(), Some("M26:O26"));
        assert_eq!(footprint("K20", "L20"), None);
    }

    #[test]
    fn footprint_stops_at_the_sheet_edge() {
        assert_eq!(footprint("A:A", "XFC1").as_deref(), Some("XFC1:XFD1"));
        assert_eq!(footprint("A1:A10", "XFA5").as_deref(), Some("XFA5:XFD5"));
        assert_eq!(
            footprint("A1:A10", "B1048570:B1048571").as_deref(),
            Some("B1048570:B1048576")
        );
    }

    #[test]
    fn footprint_keeps_the_result_flags() {
        let f = lookup_result_footprint(&r("$K$20:$K$22"), &r("$L$20:$L$21")).unwrap();
        assert_eq!(f.to_string(), "$L$20:$L$22");
        let f = lookup_result_footprint(&r("K20:K22"), &r("$P$20")).unwrap();
        assert_eq!(f.to_string(), "$P$20:$R$20");
    }

    #[test]
    fn mixed_bounds_or_a_footprint_block_sharing() {
        let blocks = |l: &str, res: &str| lookup_footprint_blocks_sharing(&r(l), &r(res));
        // Long-enough results, fully relative or fully absolute: shared.
        assert!(!blocks("K20:K22", "L20:L22"));
        assert!(!blocks("$K$20:$K$22", "$L$20:$L$22"));
        assert!(!blocks("$K20:$K22", "L20:L22"));
        assert!(!blocks("A:A", "B:B"));
        // A footprint at the cell itself.
        assert!(blocks("K20:K22", "L20:L21"));
        assert!(blocks("$K$20:$K$22", "$P$20"));
        // Mixed flags along an axis of either argument.
        assert!(blocks("K$20:K22", "L20:L22"));
        assert!(blocks("$K20:K22", "L20:L22"));
        assert!(blocks("K20:K22", "L20:L$22"));
    }
}
