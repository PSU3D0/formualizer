//! Exact verdicts only: matching and aggregate traversal remain in their original paths.
use crate::args::CriteriaPredicate;
use crate::engine::range_view::RangeView;
use arrow_array::{Array, ArrayRef, BooleanArray};
use formualizer_common::LiteralValue;
use rustc_hash::FxHashMap;
use std::sync::{Arc, Mutex};

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_ENTRIES: usize = 256;
const MAX_ROWS: usize = 65_536;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Operand {
    Number(u64),
    Int(i64),
    Text(String),
    Boolean(bool),
    Empty,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PredicateKey {
    kind: u8,
    operand: Operand,
    insensitive: bool,
}

impl PredicateKey {
    fn new(pred: &CriteriaPredicate) -> Option<Self> {
        use CriteriaPredicate::*;
        let literal = |v: &LiteralValue| match v {
            LiteralValue::Number(n) => Some(Operand::Number(n.to_bits())),
            LiteralValue::Int(n) => Some(Operand::Int(*n)),
            LiteralValue::Text(s) => Some(Operand::Text(s.clone())),
            LiteralValue::Boolean(b) => Some(Operand::Boolean(*b)),
            LiteralValue::Empty => Some(Operand::Empty),
            _ => None,
        };
        let (kind, operand, insensitive) = match pred {
            Eq(v) => (0, literal(v)?, false),
            Ne(v) => (1, literal(v)?, false),
            Gt(n) => (2, Operand::Number(n.to_bits()), false),
            Ge(n) => (3, Operand::Number(n.to_bits()), false),
            Lt(n) => (4, Operand::Number(n.to_bits()), false),
            Le(n) => (5, Operand::Number(n.to_bits()), false),
            TextLike {
                pattern,
                case_insensitive,
            } => (6, Operand::Text(pattern.clone()), *case_insensitive),
            NotTextLike {
                pattern,
                case_insensitive,
            } => (7, Operand::Text(pattern.clone()), *case_insensitive),
            TextGt(s) => (8, Operand::Text(s.clone()), false),
            TextGe(s) => (9, Operand::Text(s.clone()), false),
            TextLt(s) => (10, Operand::Text(s.clone()), false),
            TextLe(s) => (11, Operand::Text(s.clone()), false),
            IsBlank => (12, Operand::Empty, false),
            NotBlank => (13, Operand::Empty, false),
            IsNumber => (14, Operand::Empty, false),
            IsText => (15, Operand::Empty, false),
            IsLogical => (16, Operand::Empty, false),
        };
        Some(Self {
            kind,
            operand,
            insensitive,
        })
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct Key {
    predicate: PredicateKey,
    sheet: String,
    date_1904: bool,
    bounds: (usize, usize, usize, usize, usize, usize),
    // Physical chunk origin, both overlay generations, and all base lane identities.
    chunks: Vec<(usize, u64, u64, Vec<usize>)>,
    context: (u64, u64, u64),
}

struct Entry {
    mask: Arc<BooleanArray>,
    // Prevent allocator address reuse after base replacement or sheet recreation.
    _bases: Vec<ArrayRef>,
}

#[derive(Default)]
struct Inner {
    entries: FxHashMap<Key, Entry>,
    bytes: usize,
}

#[derive(Default)]
pub(super) struct CriteriaMaskCache(Mutex<Inner>);

impl CriteriaMaskCache {
    pub(super) fn get_or_build(
        &self,
        view: &RangeView<'_>,
        col: usize,
        pred: &CriteriaPredicate,
        context: (u64, u64, u64),
        build: impl FnOnce() -> Option<Arc<BooleanArray>>,
    ) -> Option<Arc<BooleanArray>> {
        let Some((mut key, bases, bytes)) = Self::key(view, col, pred) else {
            return build();
        };
        key.context = context;
        if let Ok(inner) = self.0.lock()
            && let Some(entry) = inner.entries.get(&key)
        {
            return Some(entry.mask.clone());
        }
        let mask = build()?;
        let bytes = bytes.saturating_add(mask.get_array_memory_size());
        if bytes <= MAX_BYTES
            && let Ok(mut inner) = self.0.lock()
        {
            if inner.bytes.saturating_add(bytes) > MAX_BYTES || inner.entries.len() >= MAX_ENTRIES {
                inner.entries.clear();
                inner.bytes = 0;
            }
            if !inner.entries.contains_key(&key) {
                inner.bytes += bytes;
                inner.entries.insert(
                    key,
                    Entry {
                        mask: mask.clone(),
                        _bases: bases,
                    },
                );
            }
        }
        Some(mask)
    }

    fn key(
        view: &RangeView<'_>,
        col: usize,
        pred: &CriteriaPredicate,
    ) -> Option<(Key, Vec<ArrayRef>, usize)> {
        let (rows, cols) = view.dims();
        let sheet = view.sheet();
        if view.criteria_cache_cancelled()
            || !view.is_sheet_backed()
            || rows == 0
            || rows > MAX_ROWS
            || col >= cols
            || view.end_row() >= sheet.nrows as usize
        {
            return None;
        }
        let column = sheet.columns.get(view.start_col().checked_add(col)?)?;
        let mut bases = Vec::<ArrayRef>::new();
        let mut chunks = Vec::new();
        let mut bytes = 0usize;
        for (index, chunk) in column.chunks.iter().enumerate() {
            let start = *sheet.chunk_starts.get(index)?;
            if start > view.end_row() || start.saturating_add(chunk.len()) <= view.start_row() {
                continue;
            }
            if chunks.len() >= 16 {
                return None;
            }
            let mut lanes: Vec<ArrayRef> = vec![chunk.type_tag.clone()];
            if let Some(a) = &chunk.numbers {
                lanes.push(a.clone());
            }
            if let Some(a) = &chunk.text {
                lanes.push(a.clone());
            }
            if let Some(a) = &chunk.booleans {
                lanes.push(a.clone());
            }
            if let Some(a) = &chunk.errors {
                lanes.push(a.clone());
            }
            let identities: Vec<usize> = lanes
                .iter()
                .map(|a| Arc::as_ptr(a) as *const () as usize)
                .collect();
            for a in &lanes {
                bytes = bytes.saturating_add(a.get_array_memory_size());
            }
            if bytes > MAX_BYTES {
                return None;
            }
            bases.extend(lanes);
            let (user, computed) = chunk.criteria_overlay_epochs();
            chunks.push((start, user, computed, identities));
        }
        // Sparse/unbacked coverage has no stable ordinary chunk identity yet.
        if chunks.is_empty() || !column.sparse_chunks.is_empty() {
            return None;
        }
        let key = Key {
            predicate: PredicateKey::new(pred)?,
            sheet: sheet.name.to_string(),
            date_1904: sheet.date_system == crate::engine::DateSystem::Excel1904,
            bounds: (
                view.start_row(),
                view.end_row(),
                view.start_col(),
                col,
                rows,
                cols,
            ),
            chunks,
            context: (0, 0, 0),
        };
        // Conservatively charge the retained key, Arc slots and lane buffers.
        // Shared buffers are intentionally charged again per entry: an upper
        // bound is preferable to an unbounded identity-interning side table.
        bytes = bytes
            .saturating_add(std::mem::size_of::<(Key, Entry)>())
            .saturating_add(key.sheet.len())
            .saturating_add(bases.capacity() * std::mem::size_of::<ArrayRef>())
            .saturating_add(
                key.chunks.capacity() * std::mem::size_of::<(usize, u64, u64, Vec<usize>)>(),
            );
        for (_, _, _, lanes) in &key.chunks {
            bytes = bytes.saturating_add(lanes.capacity() * std::mem::size_of::<usize>());
        }
        if let Operand::Text(text) = &key.predicate.operand {
            bytes = bytes.saturating_add(text.capacity());
        }
        if bytes > MAX_BYTES {
            return None;
        }
        Some((key, bases, bytes))
    }
}
