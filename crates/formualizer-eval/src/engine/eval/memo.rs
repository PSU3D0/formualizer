//! Program 2 memoized family runs (P2-M4, first cut for criteria aggregates
//! and lookups): members of a run whose template is `SUMIF(S)`,
//! `COUNTIF(S)`, `AVERAGEIF(S)`, `VLOOKUP`, `HLOOKUP` or `MATCH` with every
//! range argument absolute share the same ranges, so two members whose other
//! arguments evaluate to the same values (and formats) get the same result.
//! A run evaluates each distinct argument tuple once, through the walk, and
//! reuses its value and format for the members that repeat it (a report
//! filled from a few dimension keys). Exact by construction: the reused
//! result is the walk's result for identical inputs.

use super::*;
use crate::engine::arena::{AstNodeData, CompactRefType, DataStore};
use crate::format::FormatId;
use crate::function::FamilyKernel;

/// The template's argument nodes that vary per member (everything that is
/// not an absolute reference).
pub(super) struct MemoPlan {
    pub(super) key_args: smallvec::SmallVec<[AstNodeId; 8]>,
}

impl MemoPlan {
    pub(super) fn plan(
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        template: AstNodeId,
    ) -> Option<Self> {
        let AstNodeData::Function { name_id, .. } = ds.get_node(template)? else {
            return None;
        };
        let fun = functions.get_function("", ds.resolve_ast_string(*name_id))?;
        if !matches!(
            fun.family_kernel(),
            Some(FamilyKernel::CriteriaAggregate | FamilyKernel::Lookup)
        ) {
            return None;
        }
        let mut key_args = smallvec::SmallVec::new();
        for &arg in ds.get_args(template)? {
            match ds.get_node(arg)? {
                AstNodeData::Reference { ref_type, .. } => match ref_type {
                    CompactRefType::Cell {
                        row_abs, col_abs, ..
                    } if *row_abs && *col_abs => {}
                    // Each bound absolute or open (a start of 0 or an end
                    // of u32::MAX: whole columns/rows), which relocation
                    // leaves in place.
                    CompactRefType::Range {
                        start_row,
                        start_col,
                        end_row,
                        end_col,
                        start_row_abs,
                        start_col_abs,
                        end_row_abs,
                        end_col_abs,
                        ..
                    } if (*start_row_abs || *start_row == 0)
                        && (*start_col_abs || *start_col == 0)
                        && (*end_row_abs || *end_row == u32::MAX)
                        && (*end_col_abs || *end_col == u32::MAX) => {}
                    // A relative single cell is a value that varies.
                    CompactRefType::Cell { .. } => key_args.push(arg),
                    // A relative range, a name, a table, 3-D or external:
                    // the argument itself varies (or may): no memo.
                    _ => return None,
                },
                _ => key_args.push(arg),
            }
        }
        Some(Self { key_args })
    }
}

/// A hashable argument value (numbers by bits); `None` for values not keyed
/// (errors, arrays, pending), whose member is evaluated on its own.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) enum KeyValue {
    Number(u64),
    Int(i64),
    Text(String),
    Boolean(bool),
    Empty,
    Date(chrono::NaiveDate),
    DateTime(chrono::NaiveDateTime),
    Time(chrono::NaiveTime),
    Duration(i64, i32),
}

impl KeyValue {
    pub(super) fn of(value: LiteralValue) -> Option<Self> {
        Some(match value {
            LiteralValue::Number(n) => KeyValue::Number(n.to_bits()),
            LiteralValue::Int(i) => KeyValue::Int(i),
            LiteralValue::Text(s) => KeyValue::Text(s),
            LiteralValue::Boolean(b) => KeyValue::Boolean(b),
            LiteralValue::Empty => KeyValue::Empty,
            LiteralValue::Date(d) => KeyValue::Date(d),
            LiteralValue::DateTime(d) => KeyValue::DateTime(d),
            LiteralValue::Time(t) => KeyValue::Time(t),
            LiteralValue::Duration(d) => KeyValue::Duration(d.num_seconds(), d.subsec_nanos()),
            _ => return None,
        })
    }
}

pub(super) type MemoKey = smallvec::SmallVec<[(KeyValue, Option<FormatId>); 4]>;

type MemoMap = rustc_hash::FxHashMap<MemoKey, (LiteralValue, Option<FormatId>)>;

/// A run's memo shared by the parallel chunks it is split into (chunks that
/// miss the same key concurrently both evaluate it: same result), with the
/// run-wide counts the give-up rule reads.
#[derive(Default)]
pub(super) struct SharedMemo {
    map: std::sync::Mutex<MemoMap>,
    seen: std::sync::atomic::AtomicUsize,
    /// The run's criteria index (built by the first chunk that needs it).
    pub(super) criteria: std::sync::OnceLock<Option<super::criteria::CriteriaIndex>>,
}

/// Per-run (or per-chunk, over a shared map) memo with its give-up rule:
/// after 64 members, more than half of them bringing a new key stops keying
/// (the key costs one extra evaluation of the varying arguments per
/// member). Distinct keys, not hits, decide: concurrent chunks miss a key
/// together before one of them stores it.
pub(super) struct RunMemo<'s> {
    local: MemoMap,
    shared: Option<&'s SharedMemo>,
    seen: usize,
    off: bool,
}

impl<'s> RunMemo<'s> {
    pub(super) fn new(shared: Option<&'s SharedMemo>) -> Self {
        Self {
            local: MemoMap::default(),
            shared,
            seen: 0,
            off: false,
        }
    }

    /// (members keyed, distinct keys) so far.
    fn counts(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering::Relaxed;
        match self.shared {
            Some(s) => (
                s.seen.load(Relaxed),
                s.map.lock().map(|m| m.len()).unwrap_or(usize::MAX),
            ),
            None => (self.seen, self.local.len()),
        }
    }

    pub(super) fn active(&mut self) -> bool {
        // Checked every 16 members (the shared count takes the lock).
        if !self.off && self.seen.is_multiple_of(16) {
            let (seen, distinct) = self.counts();
            if seen >= 64 && distinct.saturating_mul(2) > seen {
                self.off = true;
                self.local = MemoMap::default();
            }
        }
        !self.off
    }

    pub(super) fn get(&mut self, key: &MemoKey) -> Option<(LiteralValue, Option<FormatId>)> {
        use std::sync::atomic::Ordering::Relaxed;
        match self.shared {
            Some(shared) => {
                self.seen += 1;
                shared.seen.fetch_add(1, Relaxed);
                shared.map.lock().ok()?.get(key).cloned()
            }
            None => {
                self.seen += 1;
                self.local.get(key).cloned()
            }
        }
    }

    pub(super) fn insert(&mut self, key: MemoKey, value: (LiteralValue, Option<FormatId>)) {
        match self.shared {
            Some(shared) => {
                if let Ok(mut map) = shared.map.lock() {
                    map.insert(key, value);
                }
            }
            None => {
                self.local.insert(key, value);
            }
        }
    }
}
