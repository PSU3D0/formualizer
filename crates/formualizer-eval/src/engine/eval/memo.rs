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
            return Self::plan_general(functions, ds, template);
        };
        let fun = functions.get_function("", ds.resolve_ast_string(*name_id))?;
        if !matches!(
            fun.family_kernel(),
            Some(FamilyKernel::CriteriaAggregate | FamilyKernel::Lookup)
        ) {
            return Self::plan_general(functions, ds, template);
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

    /// Program 3: any template whose members differ only in the values of
    /// relative single-cell references (every other reference the same
    /// cells for all members: rows absolute, ranges only as call arguments)
    /// through pure context-free functions, when it holds a lookup or a
    /// criteria aggregate (`INDEX(.., MATCH(C5, ..))`): those references are
    /// the key; equal keys give equal results.
    fn plan_general(
        functions: &dyn crate::traits::FunctionProvider,
        ds: &DataStore,
        template: AstNodeId,
    ) -> Option<Self> {
        fn visit(
            functions: &dyn crate::traits::FunctionProvider,
            ds: &DataStore,
            id: AstNodeId,
            as_arg: bool,
            keys: &mut smallvec::SmallVec<[AstNodeId; 8]>,
            worth: &mut bool,
        ) -> Option<()> {
            match ds.get_node(id)? {
                AstNodeData::Literal(vref) => {
                    if matches!(ds.retrieve_value(*vref), LiteralValue::Array(_)) {
                        return None;
                    }
                }
                AstNodeData::Reference { ref_type, .. } => match ref_type {
                    CompactRefType::Cell {
                        row_abs: true, row, ..
                    } if *row > 0 => {}
                    CompactRefType::Cell { row, col, .. } if *row > 0 && *col > 0 => {
                        if keys.len() >= 8 {
                            return None;
                        }
                        keys.push(id);
                    }
                    CompactRefType::Range {
                        start_row,
                        end_row,
                        start_row_abs,
                        end_row_abs,
                        ..
                    } if as_arg
                        && (*start_row_abs || *start_row == 0)
                        && (*end_row_abs || *end_row == u32::MAX) => {}
                    _ => return None,
                },
                AstNodeData::UnaryOp { expr_id, .. } => {
                    visit(functions, ds, *expr_id, false, keys, worth)?
                }
                AstNodeData::BinaryOp {
                    left_id, right_id, ..
                } => {
                    visit(functions, ds, *left_id, false, keys, worth)?;
                    visit(functions, ds, *right_id, false, keys, worth)?;
                }
                AstNodeData::Function { name_id, .. } => {
                    let name = ds.resolve_ast_string(*name_id);
                    if !super::lift::pure_listed_function(functions, name) {
                        return None;
                    }
                    *worth |= MEMO_WORTH.iter().any(|f| f.eq_ignore_ascii_case(name));
                    for &arg in ds.get_args(id)? {
                        visit(functions, ds, arg, true, keys, worth)?;
                    }
                }
                _ => return None,
            }
            Some(())
        }
        let mut key_args = smallvec::SmallVec::new();
        let mut worth = false;
        visit(functions, ds, template, false, &mut key_args, &mut worth)?;
        (worth && !key_args.is_empty()).then_some(Self { key_args })
    }
}

/// Functions whose calls make a general memo worth its keying.
const MEMO_WORTH: &[&str] = &[
    "INDEX",
    "MATCH",
    "VLOOKUP",
    "HLOOKUP",
    "XLOOKUP",
    "XMATCH",
    "SUMIF",
    "SUMIFS",
    "COUNTIF",
    "COUNTIFS",
    "AVERAGEIF",
    "AVERAGEIFS",
    "SUMPRODUCT",
];

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
    /// The run's invariant call values (computed by the first chunk that
    /// reaches a member with the template's literals).
    pub(super) invariant: std::sync::OnceLock<crate::interpreter::InvariantValues>,
    /// Members of the whole run the chunks were split from.
    pub(super) run_len: u32,
}

/// Per-run (or per-chunk, over a shared map) memo with its give-up rule:
/// after `MEMO_WARMUP` members, more than half of them bringing a new key
/// stops keying (the key costs one extra evaluation of the varying arguments
/// per member). Distinct keys, not hits, decide: concurrent chunks miss a key
/// together before one of them stores it. The warm-up is long enough for a
/// report cycling through a few dozen dimension keys: with 64 members, 60
/// keys cycling down a column turned the memo off in sequential mode (one
/// chunk sees the run's first members), while parallel chunks kept it.
const MEMO_WARMUP: usize = 256;

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
            if seen >= MEMO_WARMUP && distinct.saturating_mul(2) > seen {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn key(i: usize) -> MemoKey {
        smallvec::smallvec![(KeyValue::Number((i as f64).to_bits()), None)]
    }

    /// Drives a memo the way a run does; returns (members evaluated, hits,
    /// whether the memo was still on at the end).
    fn drive(memo: &mut RunMemo<'_>, keys: impl Iterator<Item = usize>) -> (usize, usize, bool) {
        let (mut evaluated, mut hits) = (0, 0);
        for k in keys {
            if !memo.active() {
                evaluated += 1;
                continue;
            }
            match memo.get(&key(k)) {
                Some(_) => hits += 1,
                None => {
                    evaluated += 1;
                    memo.insert(key(k), (LiteralValue::Number(k as f64), None));
                }
            }
        }
        (evaluated, hits, memo.active())
    }

    #[test]
    fn cycling_keys_keep_the_memo_on() {
        // 60 keys cycling down 10k members (sumifs_fact_table's report):
        // the first 64 members bring 60 keys, which turned the memo off.
        for shared in [None, Some(SharedMemo::default())] {
            let mut memo = RunMemo::new(shared.as_ref());
            let (evaluated, hits, on) = drive(&mut memo, (0..10_000).map(|i| i % 60));
            assert!(on);
            assert_eq!(evaluated, 60);
            assert_eq!(hits, 10_000 - 60);
        }
    }

    #[test]
    fn distinct_keys_turn_the_memo_off_after_the_warm_up() {
        for shared in [None, Some(SharedMemo::default())] {
            let mut memo = RunMemo::new(shared.as_ref());
            let (evaluated, hits, on) = drive(&mut memo, 0..10_000);
            assert!(!on);
            assert_eq!((evaluated, hits), (10_000, 0));
            assert!(memo.seen <= MEMO_WARMUP + 16);
        }
    }
}
