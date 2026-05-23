use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use formualizer_common::{ExcelError, LiteralValue, SheetId};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use crate::builtins::lookup::lookup_utils::cmp_for_lookup;
use crate::engine::range_view::RangeView;

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
                bits.hash(state);
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

impl LookupHashKey {
    pub(crate) fn from_literal(value: &LiteralValue) -> Option<Self> {
        match value {
            LiteralValue::Number(n) => Some(Self::Number(normalize_f64_bits(*n))),
            LiteralValue::Int(i) => Some(Self::Number(normalize_f64_bits(*i as f64))),
            LiteralValue::Text(s) => Some(Self::Text(s.to_lowercase().into_boxed_str())),
            LiteralValue::Boolean(b) => Some(Self::Boolean(*b)),
            LiteralValue::Empty => Some(Self::Empty),
            LiteralValue::Error(_)
            | LiteralValue::Array(_)
            | LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_)
            | LiteralValue::Duration(_)
            | LiteralValue::Pending => None,
        }
    }
}

fn normalize_f64_bits(n: f64) -> u64 {
    if n.is_nan() {
        return f64::NAN.to_bits();
    }
    let rounded = n.round();
    if (n - rounded).abs() < 1e-12 {
        rounded.to_bits()
    } else {
        n.to_bits()
    }
}

#[derive(Debug, Clone, Default)]
pub struct DuplicateIndices {
    pub(crate) first: usize,
    pub(crate) last: usize,
    pub(crate) all: SmallVec<[usize; 1]>,
}

pub struct LookupIndex {
    pub(crate) len: usize,
    pub(crate) bytes: usize,
    pub(crate) entries: FxHashMap<LookupHashKey, DuplicateIndices>,
    pub(crate) cell_values: Box<[LiteralValue]>,
    pub(crate) first_empty: Option<usize>,
}

impl LookupIndex {
    pub(crate) fn build(
        view: &RangeView<'_>,
        axis: LookupAxis,
    ) -> Result<BuildOutcome, ExcelError> {
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

        let mut entries: FxHashMap<LookupHashKey, DuplicateIndices> = FxHashMap::default();
        let mut cell_values = Vec::with_capacity(len);
        let mut first_empty = None;
        let mut error_count = 0usize;

        for idx in 0..len {
            let value = match axis {
                LookupAxis::ColumnInView(col) => view.get_cell(idx, col),
                LookupAxis::RowInView(row) => view.get_cell(row, idx),
            };
            if matches!(value, LiteralValue::Error(_)) {
                error_count += 1;
            }
            if matches!(value, LiteralValue::Empty) && first_empty.is_none() {
                first_empty = Some(idx);
            }
            if let Some(key) = LookupHashKey::from_literal(&value) {
                let dups = entries.entry(key).or_insert_with(|| DuplicateIndices {
                    first: idx,
                    last: idx,
                    all: SmallVec::new(),
                });
                if dups.all.is_empty() {
                    dups.first = idx;
                }
                dups.last = idx;
                dups.all.push(idx);
            }
            cell_values.push(value);
        }

        if error_count > 0 {
            return Ok(BuildOutcome::ErrorInLookupAxis);
        }

        let bytes = estimate_bytes(len, entries.len());
        Ok(BuildOutcome::Built(Self {
            len,
            bytes,
            entries,
            cell_values: cell_values.into_boxed_slice(),
            first_empty,
        }))
    }

    pub(crate) fn find_first_exact(&self, needle: &LiteralValue) -> Option<usize> {
        let hash_key = LookupHashKey::from_literal(needle)?;
        if let Some(dups) = self.entries.get(&hash_key) {
            for &idx in &dups.all {
                if cmp_for_lookup(needle, &self.cell_values[idx]) == Some(0) {
                    return Some(idx);
                }
            }
        }
        if let Some(n) = numeric_zero_candidate(needle)
            && n.abs() < 1e-12
        {
            return self.first_empty;
        }
        None
    }

    pub(crate) fn find_last_exact(&self, needle: &LiteralValue) -> Option<usize> {
        let hash_key = LookupHashKey::from_literal(needle)?;
        if let Some(dups) = self.entries.get(&hash_key) {
            for &idx in dups.all.iter().rev() {
                if cmp_for_lookup(needle, &self.cell_values[idx]) == Some(0) {
                    return Some(idx);
                }
            }
        }
        if let Some(n) = numeric_zero_candidate(needle)
            && n.abs() < 1e-12
        {
            return self.first_empty;
        }
        None
    }
}

fn numeric_zero_candidate(needle: &LiteralValue) -> Option<f64> {
    match needle {
        LiteralValue::Number(n) => Some(*n),
        LiteralValue::Int(i) => Some(*i as f64),
        _ => None,
    }
}

pub(crate) fn estimate_bytes(len: usize, entries: usize) -> usize {
    len.saturating_mul(std::mem::size_of::<LiteralValue>().saturating_add(8))
        .saturating_add(entries.saturating_mul(96))
        .saturating_add(256)
}

pub(crate) enum BuildOutcome {
    Built(LookupIndex),
    ErrorInLookupAxis,
    Degenerate,
}

const LOOKUP_INDEX_BUILD_THRESHOLD: u32 = 3;
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
    bytes_in_use: AtomicUsize,
    max_bytes: usize,
    builds: AtomicUsize,
    hits: AtomicUsize,
    misses: AtomicUsize,
    skipped_volatile: AtomicUsize,
    skipped_error: AtomicUsize,
    skipped_tiny: AtomicUsize,
    skipped_cap: AtomicUsize,
    skipped_below_threshold: AtomicUsize,
}

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
            bytes_in_use: AtomicUsize::new(0),
            max_bytes,
            builds: AtomicUsize::new(0),
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            skipped_volatile: AtomicUsize::new(0),
            skipped_error: AtomicUsize::new(0),
            skipped_tiny: AtomicUsize::new(0),
            skipped_cap: AtomicUsize::new(0),
            skipped_below_threshold: AtomicUsize::new(0),
        }
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

    pub(crate) fn should_build(&self, key: LookupIndexKey) -> bool {
        let Ok(mut guard) = self.call_counts.write() else {
            self.skipped_below_threshold.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        if guard.len() > CALL_COUNT_PRUNE_LIMIT {
            guard.retain(|existing_key, _| existing_key.snapshot_id == key.snapshot_id);
        }
        let count = guard.entry(key).or_insert(0);
        *count = count.saturating_add(1);
        if *count <= self.build_threshold {
            self.skipped_below_threshold.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    pub(crate) fn would_exceed_cap(&self, bytes: usize) -> bool {
        self.bytes_in_use
            .load(Ordering::Relaxed)
            .saturating_add(bytes)
            > self.max_bytes
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
        index: LookupIndex,
    ) -> Option<Arc<LookupIndex>> {
        let bytes = index.bytes;
        let current = self.bytes_in_use.load(Ordering::Relaxed);
        if current.saturating_add(bytes) > self.max_bytes {
            self.skipped_cap.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let index = Arc::new(index);
        if let Ok(mut guard) = self.inner.write() {
            if let Some(existing) = guard.get(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(existing.clone());
            }
            guard.insert(key, index.clone());
            self.bytes_in_use.fetch_add(bytes, Ordering::Relaxed);
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
        let entries_count = self
            .inner
            .read()
            .map(|guard| guard.len())
            .unwrap_or_default();
        LookupIndexCacheReport {
            builds: self.builds.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            skipped_volatile: self.skipped_volatile.load(Ordering::Relaxed),
            skipped_error: self.skipped_error.load(Ordering::Relaxed),
            skipped_tiny: self.skipped_tiny.load(Ordering::Relaxed),
            skipped_cap: self.skipped_cap.load(Ordering::Relaxed),
            skipped_below_threshold: self.skipped_below_threshold.load(Ordering::Relaxed),
            bytes_in_cache: self.bytes_in_use.load(Ordering::Relaxed),
            entries_count,
        }
    }
}
