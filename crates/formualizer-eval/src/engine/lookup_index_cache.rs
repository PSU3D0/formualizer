use std::hash::{Hash, Hasher};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use formualizer_common::{ExcelError, LiteralValue, SheetId};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use crate::builtins::lookup::lookup_utils::{
    binary_search_f64_slice, binary_search_text_slice, cmp_f64_lookup, cmp_for_lookup,
    fold_text_boxed, is_searchable_for_approximate, try_parse_f64_fast, value_to_f64_lenient,
};
use crate::engine::{DateSystem, range_view::RangeView};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct LookupIndexKey {
    pub(crate) sheet_id: SheetId,
    pub(crate) start_row: u32,
    pub(crate) start_col: u32,
    pub(crate) end_row: u32,
    pub(crate) end_col: u32,
    pub(crate) axis: LookupAxis,
    pub(crate) snapshot_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum LookupAxis {
    ColumnInView(usize),
    RowInView(usize),
}

#[derive(Debug, Eq, PartialEq)]
pub enum LookupHashKey {
    Number(u64),
    Text(Box<str>),
    Boolean(bool),
    Empty,
}

impl Hash for LookupHashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Number(bits) => {
                0u8.hash(state);
                // Integer-valued f64s have zero low bits. FxHasher preserves
                // those bits, sending whole numeric columns to one bucket.
                // Use the SplitMix64 finalizer to avalanche the bits without
                // changing key equality or numeric normalization.
                let mut mixed = *bits;
                mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                (mixed ^ (mixed >> 31)).hash(state);
            }
            Self::Text(text) => {
                1u8.hash(state);
                text.hash(state);
            }
            Self::Boolean(value) => {
                2u8.hash(state);
                value.hash(state);
            }
            Self::Empty => {
                3u8.hash(state);
            }
        }
    }
}

#[cfg(test)]
impl LookupHashKey {
    fn from_needle(value: &LiteralValue, date_system: DateSystem) -> Option<Self> {
        // Blank needles retain numeric-zero coercion; blank candidates do not.
        if matches!(value, LiteralValue::Empty) {
            Some(Self::Number(0.0f64.to_bits()))
        } else {
            Self::from_literal(value, date_system)
        }
    }

    pub(crate) fn from_literal(value: &LiteralValue, date_system: DateSystem) -> Option<Self> {
        match value {
            LiteralValue::Number(n) => Some(Self::Number(normalize_f64_bits(*n))),
            LiteralValue::Int(i) => Some(Self::Number(normalize_f64_bits(*i as f64))),
            LiteralValue::Text(s) => Some(Self::Text(fold_text_boxed(s))),
            LiteralValue::Boolean(b) => Some(Self::Boolean(*b)),
            LiteralValue::Empty => None,
            // Temporal values are numbers in Excel: key them by their serial so
            // an exact lookup finds them whether the needle or the cell (or
            // both) carry a temporal type rather than a plain numeric.
            LiteralValue::Date(_) | LiteralValue::DateTime(_) | LiteralValue::Time(_) => value
                .as_serial_number_for(date_system)
                .map(|serial| Self::Number(normalize_f64_bits(serial))),
            LiteralValue::Error(_)
            | LiteralValue::Array(_)
            | LiteralValue::Duration(_)
            | LiteralValue::Pending => None,
        }
    }
}

#[inline]
fn normalize_f64_bits(n: f64) -> u64 {
    if n.is_nan() {
        return f64::NAN.to_bits();
    }
    // Fast path for exact integer-valued doubles (|n| <= 2^52): avoids libm round().
    const MAX_EXACT_INT: f64 = 4_503_599_627_370_496.0;
    if n.abs() <= MAX_EXACT_INT {
        let trunc = (n as i64) as f64;
        if trunc == n {
            return if n == 0.0 {
                0.0f64.to_bits()
            } else {
                n.to_bits()
            };
        }
    }
    let rounded = n.round();
    if (n - rounded).abs() < 1e-12 {
        // Exact comparisons identify signed zero; the index must do so too.
        if rounded == 0.0 {
            0.0f64.to_bits()
        } else {
            rounded.to_bits()
        }
    } else {
        n.to_bits()
    }
}

#[derive(Debug, Clone, Default)]
pub struct DuplicateIndices {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) all: SmallVec<[usize; 1]>,
}

/// Compact duplicate index list (16 bytes inline vs 24 bytes for `SmallVec<[usize; 1]>`
/// or 32 bytes for an unboxed `Vec<usize>`). Boxing `Vec<usize>` keeps the 99% unique-key
/// `One` variant compact inside hash buckets.
#[derive(Debug, Clone)]
#[allow(clippy::box_collection)]
enum CompactIndices {
    One(usize),
    Many(Box<Vec<usize>>),
}

impl CompactIndices {
    #[inline]
    fn push(&mut self, idx: usize) {
        match self {
            Self::One(first) => {
                let first = *first;
                *self = Self::Many(Box::new(vec![first, idx]));
            }
            Self::Many(list) => list.push(idx),
        }
    }

    #[inline]
    fn as_slice(&self) -> &[usize] {
        match self {
            Self::One(idx) => std::slice::from_ref(idx),
            Self::Many(list) => list.as_slice(),
        }
    }

    #[inline]
    fn heap_bytes(&self) -> usize {
        match self {
            Self::One(_) => 0,
            Self::Many(list) => {
                std::mem::size_of::<Vec<usize>>()
                    + list.capacity().saturating_mul(std::mem::size_of::<usize>())
            }
        }
    }
}

/// Compact 8-byte hash key for normalized numeric/temporal values with SplitMix64
/// avalanching so integer-valued f64s distribute evenly across FxHashMap buckets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NumericHashKey(u64);

impl Hash for NumericHashKey {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        state.write_u64(mixed ^ (mixed >> 31));
    }
}

/// Exact-match hash index split into a compact numeric map (24 bytes/bucket) and a
/// general map for text/boolean keys, lazily constructed only when exact lookups hit
/// the cached `LookupIndex`.
struct ExactLookupTable {
    numbers: FxHashMap<NumericHashKey, CompactIndices>,
    other: FxHashMap<LookupHashKey, CompactIndices>,
    bytes: usize,
}

impl ExactLookupTable {
    fn build(cell_values: &[LiteralValue], date_system: DateSystem) -> Self {
        let first_is_numeric = cell_values.iter().find_map(|v| match v {
            LiteralValue::Empty => None,
            LiteralValue::Number(_)
            | LiteralValue::Int(_)
            | LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_) => Some(true),
            _ => Some(false),
        });

        let mut numbers: FxHashMap<NumericHashKey, CompactIndices> = match first_is_numeric {
            Some(true) => {
                FxHashMap::with_capacity_and_hasher(cell_values.len(), Default::default())
            }
            _ => FxHashMap::default(),
        };
        let mut other: FxHashMap<LookupHashKey, CompactIndices> = match first_is_numeric {
            Some(false) => {
                FxHashMap::with_capacity_and_hasher(cell_values.len(), Default::default())
            }
            _ => FxHashMap::default(),
        };

        for (idx, value) in cell_values.iter().enumerate() {
            match value {
                LiteralValue::Number(n) => {
                    let key = NumericHashKey(normalize_f64_bits(*n));
                    numbers
                        .entry(key)
                        .and_modify(|entry| entry.push(idx))
                        .or_insert(CompactIndices::One(idx));
                }
                LiteralValue::Int(i) => {
                    let key = NumericHashKey(normalize_f64_bits(*i as f64));
                    numbers
                        .entry(key)
                        .and_modify(|entry| entry.push(idx))
                        .or_insert(CompactIndices::One(idx));
                }
                LiteralValue::Date(_) | LiteralValue::DateTime(_) | LiteralValue::Time(_) => {
                    if let Some(serial) = value.as_serial_number_for(date_system) {
                        let key = NumericHashKey(normalize_f64_bits(serial));
                        numbers
                            .entry(key)
                            .and_modify(|entry| entry.push(idx))
                            .or_insert(CompactIndices::One(idx));
                    }
                }
                LiteralValue::Text(s) => {
                    let key = LookupHashKey::Text(fold_text_boxed(s));
                    other
                        .entry(key)
                        .and_modify(|entry| entry.push(idx))
                        .or_insert(CompactIndices::One(idx));
                }
                LiteralValue::Boolean(b) => {
                    let key = LookupHashKey::Boolean(*b);
                    other
                        .entry(key)
                        .and_modify(|entry| entry.push(idx))
                        .or_insert(CompactIndices::One(idx));
                }
                LiteralValue::Empty
                | LiteralValue::Error(_)
                | LiteralValue::Array(_)
                | LiteralValue::Duration(_)
                | LiteralValue::Pending => {}
            }
        }

        if numbers.len().saturating_mul(2) < numbers.capacity() {
            numbers.shrink_to_fit();
        }
        if other.len().saturating_mul(2) < other.capacity() {
            other.shrink_to_fit();
        }

        let num_buckets = if numbers.capacity() == 0 {
            0
        } else {
            numbers.capacity().saturating_add(1).next_power_of_two()
        };
        let other_buckets = if other.capacity() == 0 {
            0
        } else {
            other.capacity().saturating_add(1).next_power_of_two()
        };
        let mut bytes = num_buckets
            .saturating_mul(std::mem::size_of::<(NumericHashKey, CompactIndices)>() + 1)
            .saturating_add(
                other_buckets
                    .saturating_mul(std::mem::size_of::<(LookupHashKey, CompactIndices)>() + 1),
            );
        for indices in numbers.values() {
            bytes = bytes.saturating_add(indices.heap_bytes());
        }
        for (key, indices) in &other {
            if let LookupHashKey::Text(text) = key {
                bytes = bytes.saturating_add(text.len());
            }
            bytes = bytes.saturating_add(indices.heap_bytes());
        }

        Self {
            numbers,
            other,
            bytes,
        }
    }

    #[inline]
    fn get_indices(&self, needle: &LiteralValue, date_system: DateSystem) -> Option<&[usize]> {
        match needle {
            LiteralValue::Number(n) => self
                .numbers
                .get(&NumericHashKey(normalize_f64_bits(*n)))
                .map(CompactIndices::as_slice),
            LiteralValue::Int(i) => self
                .numbers
                .get(&NumericHashKey(normalize_f64_bits(*i as f64)))
                .map(CompactIndices::as_slice),
            LiteralValue::Empty => self
                .numbers
                .get(&NumericHashKey(0.0f64.to_bits()))
                .map(CompactIndices::as_slice),
            LiteralValue::Date(_) | LiteralValue::DateTime(_) | LiteralValue::Time(_) => {
                let serial = needle.as_serial_number_for(date_system)?;
                self.numbers
                    .get(&NumericHashKey(normalize_f64_bits(serial)))
                    .map(CompactIndices::as_slice)
            }
            LiteralValue::Text(s) => self
                .other
                .get(&LookupHashKey::Text(fold_text_boxed(s)))
                .map(CompactIndices::as_slice),
            LiteralValue::Boolean(b) => self
                .other
                .get(&LookupHashKey::Boolean(*b))
                .map(CompactIndices::as_slice),
            LiteralValue::Error(_)
            | LiteralValue::Array(_)
            | LiteralValue::Duration(_)
            | LiteralValue::Pending => None,
        }
    }
}

/// Shared atomic memory budget for one cache generation. Replaced on `clear()`.
struct CacheBudget {
    bytes_in_use: AtomicUsize,
    max_bytes: usize,
}

impl CacheBudget {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes_in_use: AtomicUsize::new(0),
            max_bytes,
        }
    }

    #[inline]
    fn load(&self) -> usize {
        self.bytes_in_use.load(Ordering::Relaxed)
    }

    fn try_charge(&self, bytes: usize) -> bool {
        let mut current = self.bytes_in_use.load(Ordering::Relaxed);
        loop {
            if bytes > self.max_bytes.saturating_sub(current) {
                return false;
            }
            match self.bytes_in_use.compare_exchange_weak(
                current,
                current + bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}

/// Cached sortedness verdict and compact numeric projection for approximate lookups
/// (`VLOOKUP`/`HLOOKUP` with `range_lookup=TRUE`, `MATCH` with `match_type` 1/-1).
struct ApproxNumericProjection {
    values: Box<[f64]>,
    /// Original indices into the lookup axis, or `None` when every cell was comparable (1:1).
    positions: Option<Box<[usize]>>,
    sorted_asc: bool,
    sorted_desc: bool,
    bytes: usize,
}

impl ApproxNumericProjection {
    fn build(cell_values: &[LiteralValue], date_system: DateSystem) -> Self {
        let mut values = Vec::with_capacity(cell_values.len());
        let mut positions: Option<Vec<usize>> = None;
        let mut sorted_asc = true;
        let mut sorted_desc = true;
        let mut prev: Option<f64> = None;

        for (idx, cell) in cell_values.iter().enumerate() {
            let num = if matches!(cell, LiteralValue::Empty) {
                None
            } else {
                value_to_f64_lenient(cell, date_system)
            };
            if let Some(v) = num {
                if let Some(p) = prev {
                    let c = cmp_f64_lookup(p, v);
                    if c > 0 {
                        sorted_asc = false;
                    }
                    if c < 0 {
                        sorted_desc = false;
                    }
                }
                prev = Some(v);
                if let Some(pos) = positions.as_mut() {
                    pos.push(idx);
                }
                values.push(v);
            } else if positions.is_none() {
                let mut pos = Vec::with_capacity(cell_values.len().saturating_sub(1));
                pos.extend(0..idx);
                positions = Some(pos);
            }
        }

        let values = values.into_boxed_slice();
        let positions = positions.map(Vec::into_boxed_slice);
        let bytes = values
            .len()
            .saturating_mul(std::mem::size_of::<f64>())
            .saturating_add(
                positions
                    .as_ref()
                    .map_or(0, |p| p.len().saturating_mul(std::mem::size_of::<usize>())),
            );
        Self {
            values,
            positions,
            sorted_asc,
            sorted_desc,
            bytes,
        }
    }

    #[inline]
    fn original_position(&self, index: usize) -> usize {
        match &self.positions {
            Some(pos) => pos[index],
            None => index,
        }
    }

    #[inline]
    fn search(&self, needle: f64, mode: i32) -> Option<usize> {
        let is_sorted = match mode {
            1 => self.sorted_asc,
            -1 => self.sorted_desc,
            _ => return None,
        };
        if !is_sorted {
            return None;
        }
        binary_search_f64_slice(&self.values, needle, mode).map(|i| self.original_position(i))
    }
}

/// Cached sortedness verdict and pre-lowercased text projection for approximate lookups
/// with non-numeric text needles.
struct ApproxTextProjection {
    folded: Box<[Box<str>]>,
    /// Original indices into the lookup axis, or `None` when every cell was text (1:1).
    positions: Option<Box<[usize]>>,
    sorted_asc: bool,
    sorted_desc: bool,
    lex_sorted_asc: bool,
    lex_sorted_desc: bool,
    bytes: usize,
}

impl ApproxTextProjection {
    fn build(cell_values: &[LiteralValue]) -> Self {
        let mut folded: Vec<Box<str>> = Vec::new();
        let mut positions: Option<Vec<usize>> = None;
        let mut sorted_asc = true;
        let mut sorted_desc = true;
        let mut lex_sorted_asc = true;
        let mut lex_sorted_desc = true;
        let mut prev_num: Option<f64> = None;
        let mut text_bytes = 0usize;

        for (idx, cell) in cell_values.iter().enumerate() {
            if let LiteralValue::Text(s) = cell {
                let curr_folded = fold_text_boxed(s);
                text_bytes = text_bytes.saturating_add(curr_folded.len());
                let curr_num = try_parse_f64_fast(s);
                if let Some(prev_folded) = folded.last() {
                    let lex_ord = prev_folded.as_ref().cmp(curr_folded.as_ref());
                    if lex_ord == std::cmp::Ordering::Greater {
                        lex_sorted_asc = false;
                    }
                    if lex_ord == std::cmp::Ordering::Less {
                        lex_sorted_desc = false;
                    }
                    let c = match (prev_num, curr_num) {
                        (Some(x), Some(y)) => cmp_f64_lookup(x, y),
                        _ => match lex_ord {
                            std::cmp::Ordering::Less => -1,
                            std::cmp::Ordering::Equal => 0,
                            std::cmp::Ordering::Greater => 1,
                        },
                    };
                    if c > 0 {
                        sorted_asc = false;
                    }
                    if c < 0 {
                        sorted_desc = false;
                    }
                }
                prev_num = curr_num;
                if let Some(pos) = positions.as_mut() {
                    pos.push(idx);
                }
                folded.push(curr_folded);
            } else if positions.is_none() {
                let mut pos = Vec::with_capacity(cell_values.len().saturating_sub(1));
                pos.extend(0..idx);
                positions = Some(pos);
            }
        }

        let folded = folded.into_boxed_slice();
        let positions = positions.map(Vec::into_boxed_slice);
        let bytes = folded
            .len()
            .saturating_mul(std::mem::size_of::<Box<str>>())
            .saturating_add(text_bytes)
            .saturating_add(
                positions
                    .as_ref()
                    .map_or(0, |p| p.len().saturating_mul(std::mem::size_of::<usize>())),
            );
        Self {
            folded,
            positions,
            sorted_asc,
            sorted_desc,
            lex_sorted_asc,
            lex_sorted_desc,
            bytes,
        }
    }

    #[inline]
    fn original_position(&self, index: usize) -> usize {
        match &self.positions {
            Some(pos) => pos[index],
            None => index,
        }
    }

    #[inline]
    fn search(&self, needle_str: &str, mode: i32) -> Option<usize> {
        let (is_sorted, is_lex_sorted) = match mode {
            1 => (self.sorted_asc, self.lex_sorted_asc),
            -1 => (self.sorted_desc, self.lex_sorted_desc),
            _ => return None,
        };
        if !is_sorted {
            return None;
        }
        let needle_folded = fold_text_boxed(needle_str);
        if is_lex_sorted {
            return binary_search_text_slice(&self.folded, &needle_folded, mode)
                .map(|i| self.original_position(i));
        }
        // Mixed numeric-text + non-numeric-text ranges can satisfy numeric
        // sortedness (`"2" < "10"`) while breaking lexicographic order
        // (`"10" < "1a" < "2"`). Match `approximate_search_in_view` linearly.
        let needle_ref = needle_folded.as_ref();
        if mode == 1 {
            self.folded
                .iter()
                .enumerate()
                .rev()
                .find(|(_, item)| item.as_ref() <= needle_ref)
                .map(|(i, _)| self.original_position(i))
        } else {
            let mut best: Option<usize> = None;
            for (i, item) in self.folded.iter().enumerate() {
                match item.as_ref().cmp(needle_ref) {
                    std::cmp::Ordering::Equal => return Some(self.original_position(i)),
                    std::cmp::Ordering::Greater => best = Some(self.original_position(i)),
                    std::cmp::Ordering::Less => {}
                }
            }
            best
        }
    }
}

pub struct LookupIndex {
    pub(crate) len: usize,
    date_system: DateSystem,
    pub(crate) bytes: usize,
    pub(crate) cell_values: Box<[LiteralValue]>,
    budget: Option<Arc<CacheBudget>>,
    exact: OnceLock<Option<ExactLookupTable>>,
    approx_numeric: OnceLock<Option<ApproxNumericProjection>>,
    approx_text: OnceLock<Option<ApproxTextProjection>>,
}

#[cfg(test)]
thread_local! {
    static BUILD_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn take_build_attempts() -> usize {
    BUILD_ATTEMPTS.with(|c| c.replace(0))
}

impl LookupIndex {
    pub(crate) fn build(
        view: &RangeView<'_>,
        axis: LookupAxis,
        date_system: DateSystem,
    ) -> Result<BuildOutcome, ExcelError> {
        #[cfg(test)]
        BUILD_ATTEMPTS.with(|c| c.set(c.get() + 1));
        let (rows, cols) = view.dims();
        let len = match axis {
            LookupAxis::ColumnInView(col) => {
                if col >= cols {
                    return Ok(BuildOutcome::Degenerate);
                }
                rows
            }
            LookupAxis::RowInView(row) => {
                if row >= rows {
                    return Ok(BuildOutcome::Degenerate);
                }
                cols
            }
        };
        if len == 0 {
            return Ok(BuildOutcome::Degenerate);
        }

        let mut cell_values = Vec::with_capacity(len);
        let mut payload_bytes = 0usize;
        let mut has_error = false;

        view.visit_axis_values(axis, &mut |_idx, value| {
            if matches!(value, LiteralValue::Error(_)) {
                has_error = true;
                return ControlFlow::Break(());
            }
            payload_bytes = payload_bytes.saturating_add(literal_payload_bytes(&value));
            cell_values.push(value);
            ControlFlow::Continue(())
        })?;

        if has_error {
            return Ok(BuildOutcome::ErrorInLookupAxis);
        }

        let bytes = cell_values
            .len()
            .saturating_mul(std::mem::size_of::<LiteralValue>())
            .saturating_add(payload_bytes)
            .saturating_add(256);
        Ok(BuildOutcome::Built(Self {
            len,
            date_system,
            bytes,
            cell_values: cell_values.into_boxed_slice(),
            budget: None,
            exact: OnceLock::new(),
            approx_numeric: OnceLock::new(),
            approx_text: OnceLock::new(),
        }))
    }

    #[inline]
    fn try_charge_derived(&self, bytes: usize) -> bool {
        match &self.budget {
            Some(budget) => budget.try_charge(bytes),
            None => true,
        }
    }

    #[inline]
    fn exact_table(&self) -> Option<&ExactLookupTable> {
        self.exact
            .get_or_init(|| {
                let table = ExactLookupTable::build(&self.cell_values, self.date_system);
                if self.try_charge_derived(table.bytes) {
                    Some(table)
                } else {
                    None
                }
            })
            .as_ref()
    }

    pub(crate) fn find_first_exact(&self, needle: &LiteralValue) -> Option<usize> {
        if let Some(table) = self.exact_table() {
            if let Some(indices) = table.get_indices(needle, self.date_system) {
                for &idx in indices {
                    if cmp_for_lookup(needle, &self.cell_values[idx], self.date_system) == Some(0) {
                        return Some(idx);
                    }
                }
            }
            return None;
        }
        // Over-budget fallback when the derived exact hash table could not be admitted.
        let matcher = crate::builtins::lookup::lookup_utils::PreparedLookupMatcher::new(
            needle,
            false,
            self.date_system,
        );
        self.cell_values.iter().position(|v| matcher.matches(v))
    }

    pub(crate) fn find_last_exact(&self, needle: &LiteralValue) -> Option<usize> {
        if let Some(table) = self.exact_table() {
            if let Some(indices) = table.get_indices(needle, self.date_system) {
                for &idx in indices.iter().rev() {
                    if cmp_for_lookup(needle, &self.cell_values[idx], self.date_system) == Some(0) {
                        return Some(idx);
                    }
                }
            }
            return None;
        }
        let matcher = crate::builtins::lookup::lookup_utils::PreparedLookupMatcher::new(
            needle,
            false,
            self.date_system,
        );
        self.cell_values.iter().rposition(|v| matcher.matches(v))
    }

    /// Cached O(log N) approximate lookup (with O(1) amortized #283/#399 sortedness
    /// validation) sharing one projection per comparability class across all calls.
    pub(crate) fn find_approximate(&self, needle: &LiteralValue, mode: i32) -> Option<usize> {
        if (mode != 1 && mode != -1) || self.cell_values.is_empty() {
            return None;
        }
        match needle {
            LiteralValue::Number(_)
            | LiteralValue::Int(_)
            | LiteralValue::Boolean(_)
            | LiteralValue::Empty
            | LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_) => {
                let nn = value_to_f64_lenient(needle, self.date_system)?;
                let proj_opt = self
                    .approx_numeric
                    .get_or_init(|| {
                        let proj =
                            ApproxNumericProjection::build(&self.cell_values, self.date_system);
                        if self.try_charge_derived(proj.bytes) {
                            Some(proj)
                        } else {
                            None
                        }
                    })
                    .as_ref();
                if let Some(proj) = proj_opt {
                    proj.search(nn, mode)
                } else {
                    // Tight-budget fallback: single-pass over cell_values without caching.
                    let mut prev: Option<f64> = None;
                    let mut best: Option<usize> = None;
                    let mut found_exact_desc = false;
                    for (idx, cell) in self.cell_values.iter().enumerate() {
                        if matches!(cell, LiteralValue::Empty) {
                            continue;
                        }
                        let Some(val) = value_to_f64_lenient(cell, self.date_system) else {
                            continue;
                        };
                        if let Some(p) = prev {
                            let c = cmp_f64_lookup(p, val);
                            if (mode == 1 && c > 0) || (mode == -1 && c < 0) {
                                return None;
                            }
                        }
                        prev = Some(val);
                        if mode == 1 {
                            if val <= nn + 1e-12 && cmp_f64_lookup(val, nn) <= 0 {
                                best = Some(idx);
                            }
                        } else if !found_exact_desc {
                            let c = cmp_f64_lookup(val, nn);
                            if c == 0 {
                                best = Some(idx);
                                found_exact_desc = true;
                            } else if c > 0 {
                                best = Some(idx);
                            }
                        }
                    }
                    best
                }
            }
            LiteralValue::Text(s) => {
                if try_parse_f64_fast(s).is_none() {
                    let proj_opt = self
                        .approx_text
                        .get_or_init(|| {
                            let proj = ApproxTextProjection::build(&self.cell_values);
                            if self.try_charge_derived(proj.bytes) {
                                Some(proj)
                            } else {
                                None
                            }
                        })
                        .as_ref();
                    if let Some(proj) = proj_opt {
                        proj.search(s, mode)
                    } else {
                        // Tight-budget fallback: single-pass over cell_values matching
                        // `approximate_search_in_view` without allocating a projection.
                        let needle_folded = fold_text_boxed(s);
                        let mut prev_folded: Option<Box<str>> = None;
                        let mut prev_num: Option<f64> = None;
                        let mut best: Option<usize> = None;
                        let mut found_exact_desc = false;
                        for (idx, cell) in self.cell_values.iter().enumerate() {
                            let LiteralValue::Text(cell_str) = cell else {
                                continue;
                            };
                            let curr_folded = fold_text_boxed(cell_str);
                            let curr_num = try_parse_f64_fast(cell_str);
                            if let Some(pf) = prev_folded.as_ref() {
                                let c = match (prev_num, curr_num) {
                                    (Some(x), Some(y)) => cmp_f64_lookup(x, y),
                                    _ => match pf.as_ref().cmp(curr_folded.as_ref()) {
                                        std::cmp::Ordering::Less => -1,
                                        std::cmp::Ordering::Equal => 0,
                                        std::cmp::Ordering::Greater => 1,
                                    },
                                };
                                if (mode == 1 && c > 0) || (mode == -1 && c < 0) {
                                    return None;
                                }
                            }
                            if mode == 1 {
                                if curr_folded.as_ref() <= needle_folded.as_ref() {
                                    best = Some(idx);
                                }
                            } else if !found_exact_desc {
                                match curr_folded.as_ref().cmp(needle_folded.as_ref()) {
                                    std::cmp::Ordering::Equal => {
                                        best = Some(idx);
                                        found_exact_desc = true;
                                    }
                                    std::cmp::Ordering::Greater => {
                                        best = Some(idx);
                                    }
                                    std::cmp::Ordering::Less => {}
                                }
                            }
                            prev_folded = Some(curr_folded);
                            prev_num = curr_num;
                        }
                        best
                    }
                } else {
                    // Rare numeric-text needle compares with both numbers and text;
                    // match `approximate_search_in_view` in one pass without allocation.
                    let mut prev: Option<&LiteralValue> = None;
                    let mut best: Option<usize> = None;
                    let mut found_exact_desc = false;
                    for (idx, val) in self.cell_values.iter().enumerate() {
                        if !is_searchable_for_approximate(val, needle, self.date_system) {
                            continue;
                        }
                        if let Some(p) = prev {
                            let ok = cmp_for_lookup(p, val, self.date_system)
                                .is_some_and(|c| if mode == 1 { c <= 0 } else { c >= 0 });
                            if !ok {
                                return None;
                            }
                        }
                        if let Some(c) = cmp_for_lookup(val, needle, self.date_system) {
                            if mode == 1 {
                                if c <= 0 {
                                    best = Some(idx);
                                }
                            } else if !found_exact_desc {
                                if c == 0 {
                                    best = Some(idx);
                                    found_exact_desc = true;
                                } else if c > 0 {
                                    best = Some(idx);
                                }
                            }
                        }
                        prev = Some(val);
                    }
                    best
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
fn retained_bytes(
    values: &[LiteralValue],
    entries: &FxHashMap<LookupHashKey, DuplicateIndices>,
) -> usize {
    // HashMap capacity excludes control bytes and vacant buckets. Round up to
    // the backing power-of-two bucket count; allocator bookkeeping is estimated.
    let buckets = if entries.capacity() == 0 {
        0
    } else {
        entries.capacity().saturating_add(1).next_power_of_two()
    };
    let mut bytes = values
        .len()
        .saturating_mul(std::mem::size_of::<LiteralValue>())
        .saturating_add(
            buckets.saturating_mul(std::mem::size_of::<(LookupHashKey, DuplicateIndices)>() + 1),
        )
        .saturating_add(256);
    for value in values {
        bytes = bytes.saturating_add(literal_payload_bytes(value));
    }
    for (key, indices) in entries {
        if let LookupHashKey::Text(text) = key {
            bytes = bytes.saturating_add(text.len());
        }
        if indices.all.spilled() {
            bytes = bytes.saturating_add(
                indices
                    .all
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            );
        }
    }
    bytes
}

fn literal_payload_bytes(value: &LiteralValue) -> usize {
    match value {
        LiteralValue::Text(text) => text.capacity(),
        LiteralValue::Array(rows) => rows.iter().fold(
            rows.capacity()
                .saturating_mul(std::mem::size_of::<Vec<LiteralValue>>()),
            |bytes, row| {
                row.iter().fold(
                    bytes.saturating_add(
                        row.capacity()
                            .saturating_mul(std::mem::size_of::<LiteralValue>()),
                    ),
                    |bytes, value| bytes.saturating_add(literal_payload_bytes(value)),
                )
            },
        ),
        _ => 0,
    }
}

pub(crate) fn estimate_bytes(len: usize, entries: usize) -> usize {
    len.saturating_mul(std::mem::size_of::<LiteralValue>().saturating_add(8))
        .saturating_add(entries.saturating_mul(96))
        .saturating_add(256)
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum BuildOutcome {
    Built(LookupIndex),
    ErrorInLookupAxis,
    Degenerate,
}

const LOOKUP_INDEX_BUILD_THRESHOLD: u32 = 3;
const CAP_REJECTED: u32 = u32::MAX;
const CALL_COUNT_PRUNE_LIMIT: usize = 4096;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LookupIndexCacheReport {
    pub(crate) builds: usize,
    pub(crate) hits: usize,
    pub(crate) misses: usize,
    pub(crate) skipped_volatile: usize,
    pub(crate) skipped_error: usize,
    pub(crate) skipped_tiny: usize,
    pub(crate) skipped_cap: usize,
    pub(crate) skipped_below_threshold: usize,
    pub(crate) bytes_in_cache: usize,
    pub(crate) entries_count: usize,
}

pub struct LookupIndexCache {
    inner: RwLock<FxHashMap<LookupIndexKey, Arc<LookupIndex>>>,
    call_counts: RwLock<FxHashMap<LookupIndexKey, u32>>,
    volatile_keys: RwLock<FxHashMap<LookupIndexKey, ()>>,
    build_threshold: u32,
    budget: Arc<CacheBudget>,
    max_bytes: usize,
    builds: AtomicUsize,
    hits: AtomicUsize,
    misses: AtomicUsize,
    skipped_volatile: AtomicUsize,
    skipped_error: AtomicUsize,
    skipped_tiny: AtomicUsize,
    skipped_cap: AtomicUsize,
    skipped_below_threshold: AtomicUsize,
    /// Program 3: builds in progress. Parallel members of a lookup family
    /// miss the cache together; one builds the index and the others wait
    /// for it instead of each building (and allocating) a copy.
    in_flight: std::sync::Mutex<FxHashMap<LookupIndexKey, Arc<BuildFlight>>>,
    /// Index builds started from `single_flight` (tests).
    #[cfg(test)]
    pub(crate) flights_built: AtomicUsize,
}

/// The outcome of one in-progress build, shared by the callers waiting on it.
pub(crate) type BuildFlight = std::sync::OnceLock<Option<Arc<LookupIndex>>>;

fn volatile_key(mut key: LookupIndexKey) -> LookupIndexKey {
    key.snapshot_id = 0;
    key
}

impl LookupIndexCache {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            inner: RwLock::new(FxHashMap::default()),
            call_counts: RwLock::new(FxHashMap::default()),
            volatile_keys: RwLock::new(FxHashMap::default()),
            build_threshold: LOOKUP_INDEX_BUILD_THRESHOLD,
            budget: Arc::new(CacheBudget::new(max_bytes)),
            max_bytes,
            builds: AtomicUsize::new(0),
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            skipped_volatile: AtomicUsize::new(0),
            skipped_error: AtomicUsize::new(0),
            skipped_tiny: AtomicUsize::new(0),
            skipped_cap: AtomicUsize::new(0),
            skipped_below_threshold: AtomicUsize::new(0),
            in_flight: std::sync::Mutex::new(FxHashMap::default()),
            #[cfg(test)]
            flights_built: AtomicUsize::new(0),
        }
    }

    /// Run `build` once for `key` among concurrent callers: the first
    /// caller builds, the others block until it has finished and share its
    /// outcome.
    pub(crate) fn single_flight(
        &self,
        key: LookupIndexKey,
        build: impl FnOnce() -> Option<Arc<LookupIndex>>,
    ) -> Option<Arc<LookupIndex>> {
        let flight = {
            let Ok(mut guard) = self.in_flight.lock() else {
                return build();
            };
            Arc::clone(guard.entry(key).or_default())
        };
        let mut built = false;
        let outcome = flight
            .get_or_init(|| {
                built = true;
                build()
            })
            .clone();
        // A waiter's shared outcome is a cache hit (as a duplicate build
        // finding the entry in `insert_if_room` was).
        if !built && outcome.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut guard) = self.in_flight.lock()
            && guard.get(&key).is_some_and(|f| Arc::ptr_eq(f, &flight))
        {
            guard.remove(&key);
        }
        outcome
    }

    // Called only at an exclusive Engine mutation boundary, after all evaluation
    // workers have joined. No old-generation builder can race this reclamation.
    pub(crate) fn clear(&mut self) {
        self.inner
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        self.call_counts
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        self.volatile_keys
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        self.budget = Arc::new(CacheBudget::new(self.max_bytes));
        self.in_flight
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }

    pub(crate) fn get(&self, key: &LookupIndexKey) -> Option<Arc<LookupIndex>> {
        let found = self
            .inner
            .read()
            .ok()
            .and_then(|guard| guard.get(key).cloned());
        if found.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        found
    }

    /// A builder's re-check after its `get` missed: an entry published in
    /// between is a hit (no second miss is counted).
    pub(crate) fn recheck(&self, key: &LookupIndexKey) -> Option<Arc<LookupIndex>> {
        let found = self
            .inner
            .read()
            .ok()
            .and_then(|guard| guard.get(key).cloned());
        if found.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        found
    }

    pub(crate) fn should_build(&self, key: LookupIndexKey) -> bool {
        let Ok(mut guard) = self.call_counts.write() else {
            self.skipped_below_threshold.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        if guard.len() > CALL_COUNT_PRUNE_LIMIT {
            guard.retain(|existing_key, _| existing_key.snapshot_id == key.snapshot_id);
        }
        let count = guard.entry(key).or_insert(0);
        if *count == CAP_REJECTED {
            self.skipped_cap.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        *count = count.saturating_add(1).min(CAP_REJECTED - 1);
        if *count <= self.build_threshold {
            self.skipped_below_threshold.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    pub(crate) fn would_exceed_cap(&self, bytes: usize) -> bool {
        self.budget.load().saturating_add(bytes) > self.max_bytes
    }

    pub(crate) fn is_known_volatile(&self, key: &LookupIndexKey) -> bool {
        let volatile_key = volatile_key(*key);
        self.volatile_keys
            .read()
            .map(|guard| guard.contains_key(&volatile_key))
            .unwrap_or(false)
    }

    pub(crate) fn note_volatile_key(&self, key: LookupIndexKey) {
        if let Ok(mut guard) = self.volatile_keys.write() {
            if guard.len() > CALL_COUNT_PRUNE_LIMIT {
                guard.clear();
            }
            guard.insert(volatile_key(key), ());
        }
    }

    pub(crate) fn insert_if_room(
        &self,
        key: LookupIndexKey,
        mut index: LookupIndex,
    ) -> Option<Arc<LookupIndex>> {
        let bytes = index.bytes;
        if let Ok(mut guard) = self.inner.write() {
            if let Some(existing) = guard.get(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(existing.clone());
            }
            // Serialize duplicate detection, admission and accounting. The earlier
            // preflight estimate is only a hint and cannot reserve concurrent space.
            if !self.budget.try_charge(bytes) {
                self.skipped_cap.fetch_add(1, Ordering::Relaxed);
                // Actual payloads (especially text) can exceed the preflight
                // estimate. Do not rebuild and discard them on every later call.
                if let Ok(mut counts) = self.call_counts.write() {
                    counts.insert(key, CAP_REJECTED);
                }
                return None;
            }
            index.budget = Some(Arc::clone(&self.budget));
            let index = Arc::new(index);
            guard.insert(key, index.clone());
            self.builds.fetch_add(1, Ordering::Relaxed);
            Some(index)
        } else {
            None
        }
    }

    pub(crate) fn note_skipped_volatile(&self) {
        self.skipped_volatile.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_skipped_error(&self) {
        self.skipped_error.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_skipped_tiny(&self) {
        self.skipped_tiny.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_skipped_cap(&self) {
        self.skipped_cap.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reset_counters(&self) {
        self.builds.store(0, Ordering::Relaxed);
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.skipped_volatile.store(0, Ordering::Relaxed);
        self.skipped_error.store(0, Ordering::Relaxed);
        self.skipped_tiny.store(0, Ordering::Relaxed);
        self.skipped_cap.store(0, Ordering::Relaxed);
        self.skipped_below_threshold.store(0, Ordering::Relaxed);
    }

    pub(crate) fn report(&self) -> LookupIndexCacheReport {
        let guard = self.inner.read().ok();
        let entries_count = guard.as_ref().map_or(0, |guard| guard.len());
        LookupIndexCacheReport {
            builds: self.builds.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            skipped_volatile: self.skipped_volatile.load(Ordering::Relaxed),
            skipped_error: self.skipped_error.load(Ordering::Relaxed),
            skipped_tiny: self.skipped_tiny.load(Ordering::Relaxed),
            skipped_cap: self.skipped_cap.load(Ordering::Relaxed),
            skipped_below_threshold: self.skipped_below_threshold.load(Ordering::Relaxed),
            bytes_in_cache: self.budget.load(),
            entries_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;

    fn key(col: u32) -> LookupIndexKey {
        LookupIndexKey {
            sheet_id: 0,
            start_row: 0,
            start_col: col,
            end_row: 128,
            end_col: col,
            axis: LookupAxis::ColumnInView(0),
            snapshot_id: 1,
        }
    }
    fn index(bytes: usize) -> LookupIndex {
        LookupIndex {
            len: 0,
            date_system: DateSystem::Excel1900,
            bytes,
            cell_values: Box::new([]),
            budget: None,
            exact: OnceLock::new(),
            approx_numeric: OnceLock::new(),
            approx_text: OnceLock::new(),
        }
    }

    #[test]
    fn numeric_keys_distribute_across_initial_buckets() {
        let buckets: std::collections::HashSet<_> = (1..=4096)
            .map(|i| {
                let mut hasher = rustc_hash::FxHasher::default();
                LookupHashKey::Number((i as f64).to_bits()).hash(&mut hasher);
                hasher.finish() & 8191
            })
            .collect();
        assert!(
            buckets.len() > 2048,
            "only {} initial buckets",
            buckets.len()
        );
    }

    #[test]
    fn exact_index_selects_signed_zero_and_temporal_zero_duplicates() {
        assert_eq!(
            LookupHashKey::from_literal(&LiteralValue::Empty, DateSystem::Excel1900),
            None
        );
        assert_eq!(
            LookupHashKey::from_needle(&LiteralValue::Empty, DateSystem::Excel1900),
            Some(LookupHashKey::Number(0.0f64.to_bits()))
        );
        let midnight = LiteralValue::Time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let values = vec![
            LiteralValue::Empty,
            LiteralValue::Boolean(false),
            LiteralValue::Text("0".into()),
            LiteralValue::Number(-0.0),
            midnight.clone(),
            LiteralValue::Number(0.0),
            LiteralValue::Boolean(false),
            LiteralValue::Text("0".into()),
        ];
        let view = RangeView::from_owned_rows(
            values.into_iter().map(|value| vec![value]).collect(),
            DateSystem::Excel1900,
        );
        let BuildOutcome::Built(index) =
            LookupIndex::build(&view, LookupAxis::ColumnInView(0), DateSystem::Excel1900).unwrap()
        else {
            panic!("expected a lookup index");
        };

        assert_eq!(index.find_first_exact(&LiteralValue::Number(0.0)), Some(3));
        assert_eq!(index.find_last_exact(&LiteralValue::Number(-0.0)), Some(5));
        assert_eq!(index.find_first_exact(&midnight), Some(3));
        assert_eq!(index.find_last_exact(&midnight), Some(5));
        assert_eq!(index.find_first_exact(&LiteralValue::Empty), Some(3));
        assert_eq!(index.find_last_exact(&LiteralValue::Empty), Some(5));

        let temporal_only_view = RangeView::from_owned_rows(
            vec![
                vec![LiteralValue::Empty],
                vec![LiteralValue::Boolean(false)],
                vec![LiteralValue::Text("0".into())],
                vec![midnight],
            ],
            DateSystem::Excel1900,
        );
        let BuildOutcome::Built(temporal_only) = LookupIndex::build(
            &temporal_only_view,
            LookupAxis::ColumnInView(0),
            DateSystem::Excel1900,
        )
        .unwrap() else {
            panic!("expected a temporal lookup index");
        };
        assert_eq!(
            temporal_only.find_first_exact(&LiteralValue::Number(0.0)),
            Some(3)
        );
    }

    #[test]
    fn concurrent_admission_and_duplicate_races_obey_cap() {
        for duplicate in [false, true] {
            let mut cache = LookupIndexCache::new(if duplicate { 1024 } else { 4096 });
            let barrier = std::sync::Barrier::new(16);
            std::thread::scope(|scope| {
                let handles: Vec<_> = (0..16)
                    .map(|i| {
                        let cache = &cache;
                        let barrier = &barrier;
                        scope.spawn(move || {
                            barrier.wait();
                            cache.insert_if_room(key(if duplicate { 0 } else { i }), index(1024))
                        })
                    })
                    .collect();
                let admitted: Vec<_> = handles
                    .into_iter()
                    .filter_map(|h| h.join().unwrap())
                    .collect();
                assert_eq!(admitted.len(), if duplicate { 16 } else { 4 });
                if duplicate {
                    assert!(admitted.iter().all(|item| Arc::ptr_eq(item, &admitted[0])));
                }
            });
            let report = cache.report();
            assert_eq!(report.bytes_in_cache, cache.max_bytes);
            assert_eq!(report.builds, if duplicate { 1 } else { 4 });
            assert_eq!(report.entries_count, report.builds);
            assert_eq!(report.skipped_cap, if duplicate { 0 } else { 12 });
            cache.clear();
            assert_eq!(cache.report().bytes_in_cache, 0);
            assert_eq!(cache.report().entries_count, 0);
            assert!(cache.insert_if_room(key(0), index(1024)).is_some());
        }
    }

    #[test]
    fn retained_text_and_duplicate_heap_payloads_are_charged() {
        let mut entries = FxHashMap::default();
        let mut text = String::with_capacity(4096);
        text.push_str("LONG KEY");
        let values = [LiteralValue::Text(text)];
        let empty_bytes = retained_bytes(&[], &entries);
        assert_eq!(
            retained_bytes(&values, &entries) - empty_bytes,
            std::mem::size_of::<LiteralValue>() + 4096
        );
        let mut dups = DuplicateIndices::default();
        dups.all.extend(0..100);
        let heap_bytes = dups.all.capacity() * std::mem::size_of::<usize>();
        entries.insert(LookupHashKey::Text("long key".into()), dups);
        let charged = retained_bytes(&values, &entries);
        assert!(charged >= empty_bytes + 4096 + 8 + heap_bytes);
        entries
            .get_mut(&LookupHashKey::Text("long key".into()))
            .unwrap()
            .all = SmallVec::new();
        assert_eq!(charged - retained_bytes(&values, &entries), heap_bytes);
        let cache = LookupIndexCache::new(charged - 1);
        assert!(cache.insert_if_room(key(0), index(charged)).is_none());
    }

    #[test]
    fn derived_exact_and_approx_projections_respect_tight_budget_and_stay_accurate() {
        let rows: Vec<Vec<LiteralValue>> = (1..=128)
            .map(|i| vec![LiteralValue::Number(i as f64 * 10.0)])
            .collect();
        let view = RangeView::from_owned_rows(rows, DateSystem::Excel1900);
        let BuildOutcome::Built(built) =
            LookupIndex::build(&view, LookupAxis::ColumnInView(0), DateSystem::Excel1900).unwrap()
        else {
            panic!("expected built index");
        };
        let base_bytes = built.bytes;

        // Budget admits base cell_values, but leaves zero headroom for derived exact/approx tables.
        let cache = LookupIndexCache::new(base_bytes);
        let admitted = cache
            .insert_if_room(key(0), built)
            .expect("base index should fit");
        assert_eq!(cache.report().bytes_in_cache, base_bytes);

        // Fallback paths still return exact answers without exceeding max_bytes.
        assert_eq!(
            admitted.find_first_exact(&LiteralValue::Number(500.0)),
            Some(49)
        );
        assert_eq!(
            admitted.find_last_exact(&LiteralValue::Number(500.0)),
            Some(49)
        );
        assert_eq!(
            admitted.find_approximate(&LiteralValue::Number(505.0), 1),
            Some(49)
        );
        assert_eq!(
            admitted.find_approximate(&LiteralValue::Number(5.0), 1),
            None
        );
        assert!(
            cache.report().bytes_in_cache <= base_bytes,
            "derived projections exceeded budget: {:?}",
            cache.report()
        );
    }

    #[test]
    fn axis_visitors_and_lookup_index_honor_uniform_range_tails() {
        use crate::builtins::lookup::lookup_utils::approximate_search_in_view;

        // 2x2 numeric core extended to 1_048_576 x 4 via RangeTail:
        // col 0: [10, 20, 30, 30, ..., 30] (ascending)
        // col 1: [50, 40, 30, 30, ..., 30] (descending)
        // col 2 (right_tail + corner): [100, 200, 300, ..., 300] (ascending)
        let core_num = RangeView::from_owned_rows(
            vec![
                vec![LiteralValue::Number(10.0), LiteralValue::Number(50.0)],
                vec![LiteralValue::Number(20.0), LiteralValue::Number(40.0)],
            ],
            DateSystem::Excel1900,
        );
        let tailed_num = RangeView::from_core_and_tail(
            1_048_576,
            4,
            2,
            2,
            Arc::new(core_num.sheet().clone()),
            vec![LiteralValue::Number(100.0), LiteralValue::Number(200.0)],
            vec![LiteralValue::Number(30.0), LiteralValue::Number(30.0)],
            LiteralValue::Number(300.0),
            None,
            10_000_000,
        );

        // O(1) tail handling on 1M-row ascending numeric column (last tie is row 1_048_575).
        assert_eq!(
            approximate_search_in_view(
                &tailed_num,
                LookupAxis::ColumnInView(0),
                &LiteralValue::Number(30.0),
                1,
                DateSystem::Excel1900,
            )
            .unwrap(),
            Some(1_048_575)
        );
        assert_eq!(
            approximate_search_in_view(
                &tailed_num,
                LookupAxis::ColumnInView(0),
                &LiteralValue::Number(25.0),
                1,
                DateSystem::Excel1900,
            )
            .unwrap(),
            Some(1)
        );
        // Descending col 1: exact tie on 30 starts at row 2 (first occurrence), while 35 -> row 1.
        assert_eq!(
            approximate_search_in_view(
                &tailed_num,
                LookupAxis::ColumnInView(1),
                &LiteralValue::Number(30.0),
                -1,
                DateSystem::Excel1900,
            )
            .unwrap(),
            Some(2)
        );
        assert_eq!(
            approximate_search_in_view(
                &tailed_num,
                LookupAxis::ColumnInView(1),
                &LiteralValue::Number(35.0),
                -1,
                DateSystem::Excel1900,
            )
            .unwrap(),
            Some(1)
        );
        // Right + corner tail column (col 2): [100, 200, 300, ..., 300].
        assert_eq!(
            approximate_search_in_view(
                &tailed_num,
                LookupAxis::ColumnInView(2),
                &LiteralValue::Number(300.0),
                1,
                DateSystem::Excel1900,
            )
            .unwrap(),
            Some(1_048_575)
        );
        // Row 0 across core + right_tail: [10, 50, 100, 100].
        assert_eq!(
            approximate_search_in_view(
                &tailed_num,
                LookupAxis::RowInView(0),
                &LiteralValue::Number(100.0),
                1,
                DateSystem::Excel1900,
            )
            .unwrap(),
            Some(3)
        );
        // Row 5 (bottom_tail + corner): [30, 30, 300, 300].
        assert_eq!(
            approximate_search_in_view(
                &tailed_num,
                LookupAxis::RowInView(5),
                &LiteralValue::Number(50.0),
                1,
                DateSystem::Excel1900,
            )
            .unwrap(),
            Some(1)
        );

        // Small tailed text view verifying visit_axis_values, LookupIndex::build,
        // exact + approximate search, and visit_axis_approx_text parity.
        let core_text = RangeView::from_owned_rows(
            vec![
                vec![LiteralValue::Text("alpha".into())],
                vec![LiteralValue::Text("beta".into())],
            ],
            DateSystem::Excel1900,
        );
        let tailed_text = RangeView::from_core_and_tail(
            6,
            2,
            2,
            1,
            Arc::new(core_text.sheet().clone()),
            vec![
                LiteralValue::Text("x".into()),
                LiteralValue::Text("y".into()),
            ],
            vec![LiteralValue::Text("gamma".into())],
            LiteralValue::Text("z".into()),
            None,
            10_000_000,
        );

        let BuildOutcome::Built(idx) = LookupIndex::build(
            &tailed_text,
            LookupAxis::ColumnInView(0),
            DateSystem::Excel1900,
        )
        .unwrap() else {
            panic!("expected built index for tailed text view");
        };
        assert_eq!(idx.len, 6);
        assert_eq!(
            idx.find_first_exact(&LiteralValue::Text("GAMMA".into())),
            Some(2)
        );
        assert_eq!(
            idx.find_last_exact(&LiteralValue::Text("GAMMA".into())),
            Some(5)
        );
        assert_eq!(
            idx.find_approximate(&LiteralValue::Text("GAMMA".into()), 1),
            Some(5)
        );
        assert_eq!(
            approximate_search_in_view(
                &tailed_text,
                LookupAxis::ColumnInView(0),
                &LiteralValue::Text("GAMMA".into()),
                1,
                DateSystem::Excel1900,
            )
            .unwrap(),
            Some(5)
        );
        assert_eq!(
            approximate_search_in_view(
                &tailed_text,
                LookupAxis::ColumnInView(1),
                &LiteralValue::Text("Z".into()),
                1,
                DateSystem::Excel1900,
            )
            .unwrap(),
            Some(5)
        );
    }

    #[test]
    fn approx_text_projection_matches_uncached_on_mixed_numeric_text_ranges() {
        use crate::builtins::lookup::lookup_utils::approximate_search_in_view;

        let asc_values = vec![
            LiteralValue::Text("2".into()),
            LiteralValue::Text("3".into()),
            LiteralValue::Text("4".into()),
            LiteralValue::Text("10".into()),
            LiteralValue::Text("z".into()),
        ];
        let asc_view = RangeView::from_owned_rows(
            asc_values.into_iter().map(|v| vec![v]).collect(),
            DateSystem::Excel1900,
        );
        let BuildOutcome::Built(asc_idx) = LookupIndex::build(
            &asc_view,
            LookupAxis::ColumnInView(0),
            DateSystem::Excel1900,
        )
        .unwrap() else {
            panic!("expected built asc index");
        };

        let pure_text = [
            LiteralValue::Text("alpha".into()),
            LiteralValue::Text("beta".into()),
            LiteralValue::Text("gamma".into()),
        ];
        let pure_proj = ApproxTextProjection::build(&pure_text);
        assert!(pure_proj.sorted_asc);
        assert!(pure_proj.lex_sorted_asc);
        assert!(!pure_proj.sorted_desc);
        assert!(!pure_proj.lex_sorted_desc);

        let mixed_proj = ApproxTextProjection::build(&asc_idx.cell_values);
        assert!(mixed_proj.sorted_asc);
        assert!(!mixed_proj.lex_sorted_asc);

        for needle_str in ["1", "1a", "2", "3", "5", "10", "10a", "z", "zz"] {
            let needle = LiteralValue::Text(needle_str.into());
            let expected = approximate_search_in_view(
                &asc_view,
                LookupAxis::ColumnInView(0),
                &needle,
                1,
                DateSystem::Excel1900,
            )
            .unwrap();
            assert_eq!(
                asc_idx.find_approximate(&needle, 1),
                expected,
                "ascending mode=1 mismatch for needle {needle_str:?}"
            );
        }

        let desc_values = vec![
            LiteralValue::Text("z".into()),
            LiteralValue::Text("20".into()),
            LiteralValue::Text("10".into()),
            LiteralValue::Text("4".into()),
            LiteralValue::Text("3".into()),
            LiteralValue::Text("2".into()),
            LiteralValue::Text("0".into()),
        ];
        let desc_view = RangeView::from_owned_rows(
            desc_values.into_iter().map(|v| vec![v]).collect(),
            DateSystem::Excel1900,
        );
        let BuildOutcome::Built(desc_idx) = LookupIndex::build(
            &desc_view,
            LookupAxis::ColumnInView(0),
            DateSystem::Excel1900,
        )
        .unwrap() else {
            panic!("expected built desc index");
        };

        for needle_str in ["zz", "z", "20", "15", "10", "1a", "3", "0", "-1"] {
            let needle = LiteralValue::Text(needle_str.into());
            let expected = approximate_search_in_view(
                &desc_view,
                LookupAxis::ColumnInView(0),
                &needle,
                -1,
                DateSystem::Excel1900,
            )
            .unwrap();
            assert_eq!(
                desc_idx.find_approximate(&needle, -1),
                expected,
                "descending mode=-1 mismatch for needle {needle_str:?}"
            );
        }
    }
}
