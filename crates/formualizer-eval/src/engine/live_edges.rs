//! Live-edge collection for statically-cyclic SCC evaluation (Stage 1 of the
//! runtime-cycle-verdicts work; pre-work for RFC #112).
//!
//! When a statically-cyclic SCC is evaluated member-by-member (Stage 2), we
//! must record which reads *actually occurred* targeting other SCC members
//! ("live edges"). Untaken short-circuit branches (`IF`/`IFS`/`CHOOSE`/
//! `SWITCH`, ...) never execute their reads, so they contribute no live edges
//! for free. After a pass, Stage 2 classifies the live subgraph: acyclic means
//! the cycle was phantom (values stand); cyclic means `#CIRC!` or iterative
//! evaluation.
//!
//! Stage 1 ships only the collection machinery:
//!
//! * [`LiveEdgeCollector`] — a per-SCC set of member cells plus the live edges
//!   observed so far.
//! * [`RecordingContext`] — a delegating [`EvaluationContext`] wrapper around
//!   `&Engine<R>` that records reads as they resolve and forwards everything
//!   else verbatim.
//!
//! # Inertness (binding constraint)
//!
//! Nothing in this module is wired into any production evaluation path. The
//! acyclic/hot evaluation path never constructs a `RecordingContext`; no
//! `Engine` field, flag, or branch was added. The wrapper is only exercised by
//! Stage-2 SCC tasks (future) and by tests, so its cost is strictly zero for
//! ordinary recalculation.
//!
//! # Threading
//!
//! SCC members are evaluated **sequentially on a single thread**; the
//! collector is never contended. Interior mutability is required because the
//! resolver traits take `&self`, and the `Send + Sync` super-bounds on
//! [`crate::traits::ReferenceResolver`] et al. rule out `RefCell`, so we use a
//! `Mutex`. It is uncontended by construction (single-threaded SCC pass), so
//! the lock is a fast path (uncontested futex acquire) and never blocks.
//!
//! # Coordinates
//!
//! The collector API uses the engine's internal convention: 0-based row and
//! column indices, rectangles **inclusive** of both corners (matching
//! [`RangeView::start_row`]/[`RangeView::end_row`] and `CellRef`'s `Coord`).
//! Resolver-level call sites (1-based Excel coordinates) convert before
//! recording.

use std::sync::{Mutex, OnceLock};

use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parser::{ReferenceType, TableReference};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::engine::eval::Engine;
use crate::engine::range_view::RangeView;
use crate::reference::{CellRef, SheetId};
use crate::traits::{
    EvaluationContext, FunctionProvider, NamedRangeResolver, Range, RangeResolver, ReferenceInfo,
    ReferenceResolver, Resolver, SourceResolver, Table, TableResolver,
};

/* ───────────────────────── LiveEdgeCollector ───────────────────────── */

/// Below this member count, linear scanning `members` beats allocating and
/// hashing a `FxHashMap` (critical for workloads with thousands of 2-member
/// SCCs).
const SMALL_SCALAR_SCAN_THRESHOLD: usize = 8;

/// Above this member count, `record_rect` lazily builds a per-sheet dual-axis
/// CSR index (`RectIndex`) so range intersection costs
/// `O(min(cols_hit, rows_hit) * log |SCC| + K)` instead of `O(|SCC|)`.
const RECT_INDEX_THRESHOLD: usize = 16;

/// One SCC member cell in collector-internal form (0-based coordinates).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MemberCell {
    sheet_id: SheetId,
    row: u32,
    col: u32,
}

/// Per-sheet dual-axis sorted CSR index of SCC cell members for sub-linear
/// rectangle intersection.
struct SheetRectIndex {
    sheet_id: SheetId,
    /// Distinct occupied columns in ascending order.
    col_keys: Vec<u32>,
    /// CSR offsets into `col_entries` (`len == col_keys.len() + 1`).
    col_offsets: Vec<u32>,
    /// `(row, member_idx)` grouped by column and sorted ascending by `row`.
    col_entries: Vec<(u32, u32)>,
    /// Distinct occupied rows in ascending order.
    row_keys: Vec<u32>,
    /// CSR offsets into `row_entries` (`len == row_keys.len() + 1`).
    row_offsets: Vec<u32>,
    /// `(col, member_idx)` grouped by row and sorted ascending by `col`.
    row_entries: Vec<(u32, u32)>,
}

impl SheetRectIndex {
    fn for_each_in_rect(&self, sr: u32, sc: u32, er: u32, ec: u32, mut f: impl FnMut(u32)) {
        let c_lo = self.col_keys.partition_point(|&c| c < sc);
        let c_hi = self.col_keys.partition_point(|&c| c <= ec);
        let num_cols = c_hi - c_lo;
        if num_cols == 0 {
            return;
        }
        let r_lo = self.row_keys.partition_point(|&r| r < sr);
        let r_hi = self.row_keys.partition_point(|&r| r <= er);
        let num_rows = r_hi - r_lo;
        if num_rows == 0 {
            return;
        }

        if num_cols <= num_rows {
            for ci in c_lo..c_hi {
                let start_off = self.col_offsets[ci] as usize;
                let end_off = self.col_offsets[ci + 1] as usize;
                let entries = &self.col_entries[start_off..end_off];
                let start = entries.partition_point(|&(r, _)| r < sr);
                for &(r, idx) in &entries[start..] {
                    if r > er {
                        break;
                    }
                    f(idx);
                }
            }
        } else {
            for ri in r_lo..r_hi {
                let start_off = self.row_offsets[ri] as usize;
                let end_off = self.row_offsets[ri + 1] as usize;
                let entries = &self.row_entries[start_off..end_off];
                let start = entries.partition_point(|&(c, _)| c < sc);
                for &(c, idx) in &entries[start..] {
                    if c > ec {
                        break;
                    }
                    f(idx);
                }
            }
        }
    }
}

struct RectIndex {
    /// Sorted ascending by `sheet_id`.
    sheets: Vec<SheetRectIndex>,
}

impl RectIndex {
    fn build(members: &[MemberCell]) -> Self {
        let mut by_sheet_col: Vec<(SheetId, u32, u32, u32)> = members
            .iter()
            .enumerate()
            .map(|(i, m)| (m.sheet_id, m.col, m.row, i as u32))
            .collect();
        by_sheet_col.sort_unstable();

        let mut sheets = Vec::new();
        let mut by_row_buf: Vec<(u32, u32, u32)> = Vec::new();
        let mut start = 0usize;
        while start < by_sheet_col.len() {
            let sid = by_sheet_col[start].0;
            let mut end = start + 1;
            while end < by_sheet_col.len() && by_sheet_col[end].0 == sid {
                end += 1;
            }
            let slice = &by_sheet_col[start..end];

            let mut col_keys = Vec::new();
            let mut col_offsets = Vec::new();
            let mut col_entries = Vec::with_capacity(slice.len());
            for &(_, col, row, idx) in slice {
                if col_keys.last().copied() != Some(col) {
                    col_keys.push(col);
                    col_offsets.push(col_entries.len() as u32);
                }
                col_entries.push((row, idx));
            }
            col_offsets.push(col_entries.len() as u32);

            by_row_buf.clear();
            by_row_buf.reserve(slice.len());
            for &(_, col, row, idx) in slice {
                by_row_buf.push((row, col, idx));
            }
            by_row_buf.sort_unstable();

            let mut row_keys = Vec::new();
            let mut row_offsets = Vec::new();
            let mut row_entries = Vec::with_capacity(slice.len());
            for &(row, col, idx) in &by_row_buf {
                if row_keys.last().copied() != Some(row) {
                    row_keys.push(row);
                    row_offsets.push(row_entries.len() as u32);
                }
                row_entries.push((col, idx));
            }
            row_offsets.push(row_entries.len() as u32);

            sheets.push(SheetRectIndex {
                sheet_id: sid,
                col_keys,
                col_offsets,
                col_entries,
                row_keys,
                row_offsets,
                row_entries,
            });
            start = end;
        }

        Self { sheets }
    }

    #[inline]
    fn sheet(&self, sheet_id: SheetId) -> Option<&SheetRectIndex> {
        if self.sheets.len() == 1 {
            let s = &self.sheets[0];
            return (s.sheet_id == sheet_id).then_some(s);
        }
        self.sheets
            .binary_search_by_key(&sheet_id, |s| s.sheet_id)
            .ok()
            .map(|idx| &self.sheets[idx])
    }
}

#[derive(Default)]
struct CollectorState {
    /// Index (into `members`) of the member currently being evaluated.
    /// `None` until `set_current` is called; reads observed while `None` are
    /// not attributable and are dropped.
    current: Option<u32>,
    /// Live edges as `(from_member_idx, to_member_idx)`. Self-edges `(i, i)`
    /// are recorded (e.g. a member whose range argument includes itself).
    /// May contain duplicates until drained/deduplicated.
    edges: Vec<(u32, u32)>,
    /// Reusable scratch buffer for per-member target deduplication in
    /// `finish_member_targets`.
    scratch_targets: Vec<u32>,
}

/// Records which reads actually occurred targeting SCC members during a
/// sequential member-by-member evaluation pass.
///
/// * Scalar reads are O(1) (hash lookup keyed by `(sheet, row, col)`, or a
///   tiny inline scan for SCCs with `<= 8` members to avoid hash-map setup).
/// * Rectangle reads are recorded **once per resolved rect** and intersected
///   with the membership via a lazy per-sheet dual-axis CSR index in
///   `O(min(cols_hit, rows_hit) * log |SCC| + K)` — never per cell of the rect
///   and never scanning all `|SCC|` members on large SCCs.
/// * Name reads (named-formula SCC members, spec §7.13) are O(1) lookups by
///   the engine-folded name key.
///
/// Member indices are split: cell members occupy `0..cell_count`, name
/// members occupy `cell_count..cell_count + name_count` (matching the spec
/// §7.13 member ordering used by SCC tasks: cells first, then names).
pub struct LiveEdgeCollector {
    /// Iterable membership for small-SCC scans and lazy rect-index construction.
    members: Vec<MemberCell>,
    /// O(1) scalar lookup: (sheet_id, row0, col0) -> member index (populated
    /// only when `members.len() > SMALL_SCALAR_SCAN_THRESHOLD`).
    index: FxHashMap<(SheetId, u32, u32), u32>,
    /// Lazy per-sheet spatial index for `record_rect` on SCCs larger than
    /// `RECT_INDEX_THRESHOLD`.
    rect_index: OnceLock<RectIndex>,
    /// O(1) name lookup: engine-folded name key -> member index (indices
    /// start after the cell members).
    name_index: FxHashMap<String, u32>,
    /// Total member count (cells + names); valid `set_current` range.
    total_members: usize,
    /// See module docs: uncontended Mutex forced by `Send + Sync` bounds on
    /// the resolver traits; SCC passes are single-threaded.
    state: Mutex<CollectorState>,
}

impl LiveEdgeCollector {
    /// Build a collector for the given SCC membership. Member order defines
    /// the indices used in recorded edges.
    pub fn new(members: &[CellRef]) -> Self {
        Self::new_with_names(members, &[])
    }

    /// Build a collector over cell members plus name-vertex members. Cell
    /// members get indices `0..cells.len()`; name member `j` gets index
    /// `cells.len() + j`. `names` must already be folded with the engine's
    /// name-folding rule (see [`Engine::fold_name_key`]).
    pub fn new_with_names(cells: &[CellRef], names: &[String]) -> Self {
        let members: Vec<MemberCell> = cells
            .iter()
            .map(|c| MemberCell {
                sheet_id: c.sheet_id,
                row: c.coord.row(),
                col: c.coord.col(),
            })
            .collect();
        let mut index = FxHashMap::default();
        if members.len() > SMALL_SCALAR_SCAN_THRESHOLD {
            index.reserve(members.len());
            for (i, m) in members.iter().enumerate() {
                index.insert((m.sheet_id, m.row, m.col), i as u32);
            }
        }
        let mut name_index = FxHashMap::default();
        if !names.is_empty() {
            name_index.reserve(names.len());
            for (j, name) in names.iter().enumerate() {
                name_index.insert(name.clone(), (members.len() + j) as u32);
            }
        }
        let total_members = members.len() + names.len();
        Self {
            members,
            index,
            rect_index: OnceLock::new(),
            name_index,
            total_members,
            state: Mutex::new(CollectorState::default()),
        }
    }

    pub fn member_count(&self) -> usize {
        self.total_members
    }

    /// Set the member whose formula is about to be evaluated; subsequent
    /// recorded reads are attributed to it.
    pub fn set_current(&self, member_idx: u32) {
        debug_assert!((member_idx as usize) < self.total_members);
        self.state.lock().unwrap().current = Some(member_idx);
    }

    /// Begin recording for a single member in `evaluate_scc_unit`, clearing
    /// any previously buffered edges.
    pub(crate) fn begin_member(&self, member_idx: u32) {
        debug_assert!((member_idx as usize) < self.total_members);
        let mut st = self.state.lock().unwrap();
        st.current = Some(member_idx);
        st.edges.clear();
    }

    /// Discard edges recorded for the current member (e.g. when the member is
    /// excluded mid-pass after returning an array value).
    pub(crate) fn discard_current_edges(&self) {
        self.state.lock().unwrap().edges.clear();
    }

    /// Finish recording for the current member and compare its sorted,
    /// deduplicated target list against `prev_targets`.
    ///
    /// Returns `true` if `prev_targets` changed (updating it in place), or
    /// `false` when the member's live out-edge set is identical to the previous
    /// pass.
    pub(crate) fn finish_member_targets(&self, prev_targets: &mut Vec<u32>) -> bool {
        let mut st = self.state.lock().unwrap();
        let CollectorState {
            current,
            edges,
            scratch_targets,
        } = &mut *st;
        debug_assert!(
            current.is_some(),
            "finish_member_targets called without active member"
        );
        let cur = current.unwrap_or(0);
        match edges.len() {
            0 => {
                if prev_targets.is_empty() {
                    false
                } else {
                    prev_targets.clear();
                    true
                }
            }
            1 => {
                let (from, to) = edges[0];
                debug_assert_eq!(from, cur);
                edges.clear();
                if prev_targets.len() == 1 && prev_targets[0] == to {
                    false
                } else {
                    prev_targets.clear();
                    prev_targets.push(to);
                    true
                }
            }
            2 => {
                let (f0, a) = edges[0];
                let (f1, b) = edges[1];
                debug_assert_eq!(f0, cur);
                debug_assert_eq!(f1, cur);
                edges.clear();
                if a == b {
                    if prev_targets.len() == 1 && prev_targets[0] == a {
                        false
                    } else {
                        prev_targets.clear();
                        prev_targets.push(a);
                        true
                    }
                } else {
                    let (lo, hi) = if a < b { (a, b) } else { (b, a) };
                    if prev_targets.len() == 2 && prev_targets[0] == lo && prev_targets[1] == hi {
                        false
                    } else {
                        prev_targets.clear();
                        prev_targets.push(lo);
                        prev_targets.push(hi);
                        true
                    }
                }
            }
            _ => {
                scratch_targets.clear();
                scratch_targets.reserve(edges.len());
                for &(from, to) in edges.iter() {
                    debug_assert_eq!(from, cur);
                    scratch_targets.push(to);
                }
                edges.clear();
                scratch_targets.sort_unstable();
                scratch_targets.dedup();
                if scratch_targets.as_slice() == prev_targets.as_slice() {
                    false
                } else {
                    prev_targets.clear();
                    prev_targets.extend_from_slice(scratch_targets);
                    true
                }
            }
        }
    }

    /// Stop attributing reads to any member (used between passes so that
    /// out-of-band reads — snapshots, deltas — never record edges).
    pub fn clear_current(&self) {
        self.state.lock().unwrap().current = None;
    }

    /// Record a scalar read of `(sheet_id, row, col)` (0-based).
    pub fn record_scalar(&self, sheet_id: SheetId, row: u32, col: u32) {
        let to = if self.members.len() <= SMALL_SCALAR_SCAN_THRESHOLD {
            let Some(pos) = self
                .members
                .iter()
                .rposition(|m| m.sheet_id == sheet_id && m.row == row && m.col == col)
            else {
                return;
            };
            pos as u32
        } else {
            let Some(&to) = self.index.get(&(sheet_id, row, col)) else {
                return;
            };
            to
        };
        let mut st = self.state.lock().unwrap();
        if let Some(from) = st.current {
            st.edges.push((from, to));
        }
    }

    /// Record a rectangle read (0-based, inclusive corners). For SCCs larger
    /// than `RECT_INDEX_THRESHOLD`, intersects via a lazy per-sheet dual-axis
    /// CSR index in `O(min(cols_hit, rows_hit) * log |SCC| + K)` instead of
    /// scanning all members.
    pub fn record_rect(&self, sheet_id: SheetId, sr: u32, sc: u32, er: u32, ec: u32) {
        if er < sr || ec < sc || self.members.is_empty() {
            return;
        }
        let mut st = self.state.lock().unwrap();
        let Some(from) = st.current else {
            return;
        };
        if self.members.len() <= RECT_INDEX_THRESHOLD {
            for (i, m) in self.members.iter().enumerate() {
                if m.sheet_id == sheet_id
                    && m.row >= sr
                    && m.row <= er
                    && m.col >= sc
                    && m.col <= ec
                {
                    st.edges.push((from, i as u32));
                }
            }
            return;
        }

        let rect_index = self
            .rect_index
            .get_or_init(|| RectIndex::build(&self.members));
        let Some(sheet_idx) = rect_index.sheet(sheet_id) else {
            return;
        };
        sheet_idx.for_each_in_rect(sr, sc, er, ec, |idx| {
            st.edges.push((from, idx));
        });
    }

    /// Record a read of a named entity by folded name key (e.g. a formula
    /// referencing a named-formula SCC member).
    pub fn record_name(&self, folded_name: &str) {
        if self.name_index.is_empty() {
            return;
        }
        let Some(&to) = self.name_index.get(folded_name) else {
            return;
        };
        let mut st = self.state.lock().unwrap();
        if let Some(from) = st.current {
            st.edges.push((from, to));
        }
    }

    /// Drain the collected edges, leaving the collector empty (current member
    /// attribution is preserved).
    pub fn take_edges(&self) -> FxHashSet<(u32, u32)> {
        self.state.lock().unwrap().edges.drain(..).collect()
    }
}

/* ───────────────────────── ReadSink ───────────────────────── */

/// Where a [`RecordingContext`] sends observed reads (0-based, inclusive).
pub trait ReadSink: Send + Sync {
    fn record_scalar(&self, sheet_id: SheetId, row: u32, col: u32);
    fn record_rect(&self, sheet_id: SheetId, sr: u32, sc: u32, er: u32, ec: u32);
    fn record_name(&self, folded_name: &str);
}

impl ReadSink for LiveEdgeCollector {
    fn record_scalar(&self, sheet_id: SheetId, row: u32, col: u32) {
        LiveEdgeCollector::record_scalar(self, sheet_id, row, col)
    }
    fn record_rect(&self, sheet_id: SheetId, sr: u32, sc: u32, er: u32, ec: u32) {
        LiveEdgeCollector::record_rect(self, sheet_id, sr, sc, er, ec)
    }
    fn record_name(&self, folded_name: &str) {
        LiveEdgeCollector::record_name(self, folded_name)
    }
}

/// Every read of one evaluation, as rectangles `(sheet, r0, c0, r1, c1)`
/// (0-based, inclusive; a cell read is a 1×1 rectangle). Names are not
/// recorded: a name's reads flow through the range view of its definition.
/// Used by the freshness recorder for dynamic readers (design §8.2).
#[derive(Default)]
pub struct ReadLog {
    reads: Mutex<Vec<ReadRect>>,
}

/// `(sheet, r0, c0, r1, c1)`, 0-based inclusive.
pub type ReadRect = (SheetId, u32, u32, u32, u32);

impl ReadLog {
    pub fn take(&self) -> Vec<ReadRect> {
        std::mem::take(&mut self.reads.lock().unwrap())
    }
}

impl ReadSink for ReadLog {
    fn record_scalar(&self, sheet_id: SheetId, row: u32, col: u32) {
        self.reads
            .lock()
            .unwrap()
            .push((sheet_id, row, col, row, col));
    }
    fn record_rect(&self, sheet_id: SheetId, sr: u32, sc: u32, er: u32, ec: u32) {
        self.reads.lock().unwrap().push((sheet_id, sr, sc, er, ec));
    }
    fn record_name(&self, _folded_name: &str) {}
}

/* ───────────────────────── RecordingContext ───────────────────────── */

/// Delegating [`EvaluationContext`] that wraps `&Engine<R>` and records reads
/// into a [`LiveEdgeCollector`].
///
/// Interception points (everything else is pure delegation):
///
/// * `EvaluationContext::resolve_cell_reference_value` — the interpreter's
///   scalar read path (current-sheet aware).
/// * `EvaluationContext::resolve_range_view` — the single choke point for
///   range, named-range, table and dynamic (`INDIRECT`/`OFFSET`) reads. The
///   engine resolves un/partially-bounded references to concrete used-region
///   bounds, and the returned view carries the resolved sheet + rect, so we
///   record exactly that rect once. Views materialised from owned rows (array
///   literals, named literals/formulas) carry the synthetic `"__tmp"` sheet,
///   which has no `SheetId`, so they are skipped automatically.
/// * `ReferenceResolver::resolve_cell_reference` — sheet-qualified scalar
///   reads (e.g. implicit intersection).
/// * `RangeResolver::resolve_range_reference` — legacy boxed-range path; the
///   rect is resolved via the engine's own `resolve_range_view` normalisation
///   so unbounded references record their used-region bounds.
///
/// Not recordable at this layer (Stage 2 follow-ups, noted in tests):
///
/// * `NamedRangeResolver::resolve_named_range_reference` — values-only API
///   with no sheet/region context. The engine-level named-range path flows
///   through `resolve_range_view` (intercepted); only the external-resolver
///   fallback is invisible.
/// * `TableResolver::resolve_table_reference` — returns an opaque `Table`.
///   Engine-registered tables flow through `resolve_range_view` (intercepted).
pub struct RecordingContext<'a, R: EvaluationContext, S: ReadSink = LiveEdgeCollector> {
    engine: &'a Engine<R>,
    collector: &'a S,
}

impl<'a, R: EvaluationContext, S: ReadSink> RecordingContext<'a, R, S> {
    pub fn new(engine: &'a Engine<R>, collector: &'a S) -> Self {
        Self { engine, collector }
    }

    /// Record a read of a named entity, folding the raw reference text with
    /// the engine's name-folding rule so it matches collector name keys.
    fn record_name(&self, raw_name: &str) {
        let key = self.engine.graph.name_lookup_key(raw_name);
        self.collector.record_name(&key);
    }

    /// Record a scalar read given Excel 1-based coordinates.
    fn record_cell_1based(&self, sheet_name: &str, row: u32, col: u32) {
        if row == 0 || col == 0 {
            return;
        }
        if let Some(sid) = self.engine.sheet_id(sheet_name) {
            self.collector.record_scalar(sid, row - 1, col - 1);
        }
    }

    /// Record the resolved rect of a `RangeView`. View bounds are absolute,
    /// 0-based and inclusive. Owned/temporary views (sheet `"__tmp"`) have no
    /// registered `SheetId` and are skipped.
    fn record_view(&self, view: &RangeView<'_>) {
        if view.is_empty() {
            return;
        }
        if let Some(sid) = self.engine.sheet_id(view.sheet_name()) {
            self.collector.record_rect(
                sid,
                view.start_row() as u32,
                view.start_col() as u32,
                view.end_row() as u32,
                view.end_col() as u32,
            );
        }
    }
}

impl<'a, R: EvaluationContext, S: ReadSink> ReferenceResolver for RecordingContext<'a, R, S> {
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, ExcelError> {
        // Unqualified (`None`) references are rejected by the engine itself
        // (no current-sheet context at this trait level), so there is nothing
        // attributable to record in that case.
        if let Some(sheet_name) = sheet {
            self.record_cell_1based(sheet_name, row, col);
        }
        self.engine.resolve_cell_reference(sheet, row, col)
    }
}

impl<'a, R: EvaluationContext, S: ReadSink> RangeResolver for RecordingContext<'a, R, S> {
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        // Resolve the rect through the engine's own bound normalisation
        // (used-region for unbounded axes) rather than duplicating it here.
        if let Some(sheet_name) = sheet {
            let reference = ReferenceType::Range {
                sheet: Some(sheet_name.to_string()),
                start_row: sr,
                start_col: sc,
                end_row: er,
                end_col: ec,
                start_row_abs: true,
                start_col_abs: true,
                end_row_abs: true,
                end_col_abs: true,
            };
            if let Ok(view) = self.engine.resolve_range_view(&reference, sheet_name) {
                self.record_view(&view);
            }
        }
        self.engine.resolve_range_reference(sheet, sr, sc, er, ec)
    }
}

impl<'a, R: EvaluationContext, S: ReadSink> NamedRangeResolver for RecordingContext<'a, R, S> {
    fn resolve_named_range_reference(
        &self,
        name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        // Values-only API without sheet/region context; record the *name*
        // member edge (if the name itself is an SCC member) — region-level
        // reads flow through `resolve_range_view` instead.
        self.record_name(name);
        self.engine.resolve_named_range_reference(name)
    }
}

impl<'a, R: EvaluationContext, S: ReadSink> TableResolver for RecordingContext<'a, R, S> {
    fn resolve_table_reference(&self, tref: &TableReference) -> Result<Box<dyn Table>, ExcelError> {
        // Opaque `Table` without region context; engine-registered tables are
        // intercepted in `resolve_range_view` instead.
        self.engine.resolve_table_reference(tref)
    }
}

impl<'a, R: EvaluationContext, S: ReadSink> SourceResolver for RecordingContext<'a, R, S> {
    fn source_scalar_version(&self, name: &str) -> Option<u64> {
        self.engine.source_scalar_version(name)
    }
    fn resolve_source_scalar(&self, name: &str) -> Result<LiteralValue, ExcelError> {
        self.engine.resolve_source_scalar(name)
    }
    fn source_table_version(&self, name: &str) -> Option<u64> {
        self.engine.source_table_version(name)
    }
    fn resolve_source_table(&self, name: &str) -> Result<Box<dyn Table>, ExcelError> {
        self.engine.resolve_source_table(name)
    }
}

impl<'a, R: EvaluationContext, S: ReadSink> Resolver for RecordingContext<'a, R, S> {}

impl<'a, R: EvaluationContext, S: ReadSink> FunctionProvider for RecordingContext<'a, R, S> {
    fn planning_semantic_revision(&self) -> Option<u64> {
        self.engine.planning_semantic_revision()
    }

    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::traits::Function>> {
        self.engine.get_function(ns, name)
    }

    fn get_function_for_planning(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::traits::Function>> {
        self.engine.get_function_for_planning(ns, name)
    }
}

impl<'a, R: EvaluationContext, S: ReadSink> EvaluationContext for RecordingContext<'a, R, S> {
    fn named_binding_presence(&self, name: &str, current_sheet: &str) -> Option<bool> {
        self.engine.named_binding_presence(name, current_sheet)
    }
    fn operation_diagnostic_collector(
        &self,
    ) -> Option<&crate::operation_diagnostics::OperationDiagnosticCollector> {
        self.engine.operation_diagnostic_collector()
    }

    /* ── intercept-and-record ── */

    fn resolve_range_view<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        // Named reads can target a name *vertex* that is itself an SCC member
        // (a named formula, spec §7.13). Those resolve to owned-row views with
        // no sheet rect, so they must be recorded by name here in addition to
        // the rect recording below (which covers Cell/Range definitions).
        if let ReferenceType::NamedRange(name) = reference {
            self.record_name(name);
        }
        let view = self.engine.resolve_range_view(reference, current_sheet)?;
        self.record_view(&view);
        Ok(view)
    }

    fn resolve_cell_reference_value(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        current_sheet: &str,
    ) -> Result<LiteralValue, ExcelError> {
        self.record_cell_1based(sheet.unwrap_or(current_sheet), row, col);
        self.engine
            .resolve_cell_reference_value(sheet, row, col, current_sheet)
    }

    fn resolve_cell_reference_value_formatted(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        current_sheet: &str,
    ) -> Result<(LiteralValue, Option<crate::format::FormatId>), ExcelError> {
        self.record_cell_1based(sheet.unwrap_or(current_sheet), row, col);
        self.engine
            .resolve_cell_reference_value_formatted(sheet, row, col, current_sheet)
    }

    fn resolve_cell_format(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
        current_sheet: &str,
    ) -> Option<crate::format::FormatId> {
        self.engine
            .resolve_cell_format(sheet, row, col, current_sheet)
    }

    fn format_class(
        &self,
        format: crate::format::FormatId,
    ) -> Option<formualizer_common::numfmt::FormatClass> {
        self.engine.format_class(format)
    }

    fn record_cell_derived_format(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
        format: Option<crate::format::FormatId>,
    ) {
        self.engine
            .record_cell_derived_format(sheet, row, col, format)
    }

    /* ── pure delegation ── */

    fn thread_pool(&self) -> Option<&std::sync::Arc<rayon::ThreadPool>> {
        self.engine.thread_pool()
    }
    fn cancellation_token(&self) -> Option<crate::engine::CancelToken> {
        self.engine.cancellation_token()
    }
    fn chunk_hint(&self) -> Option<usize> {
        self.engine.chunk_hint()
    }
    fn materialized_array_cells(&self) -> u64 {
        self.engine.materialized_array_cells()
    }
    fn locale(&self) -> crate::locale::Locale {
        self.engine.locale()
    }
    fn workbook_sheet_count(&self) -> Option<usize> {
        self.engine.workbook_sheet_count()
    }
    fn sheet_index_by_name(&self, sheet: &str) -> Option<usize> {
        self.engine.sheet_index_by_name(sheet)
    }
    fn current_sheet_index(&self, current_sheet: &str) -> Option<usize> {
        self.engine.current_sheet_index(current_sheet)
    }
    fn workbook_path(&self) -> Option<&str> {
        self.engine.workbook_path()
    }
    fn sheet_name_by_id(&self, sheet: crate::SheetId) -> Option<&str> {
        self.engine.sheet_name_by_id(sheet)
    }
    fn inspect_reference(
        &self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<Option<ReferenceInfo>, ExcelError> {
        self.engine.inspect_reference(reference, current_sheet)
    }
    fn has_formula_at_cell(&self, cell: CellRef) -> Result<bool, ExcelError> {
        self.engine.has_formula_at_cell(cell)
    }
    fn formula_text_at_cell(&self, cell: CellRef) -> Result<Option<String>, ExcelError> {
        self.engine.formula_text_at_cell(cell)
    }
    fn resolve_spill_reference(
        &self,
        anchor: &ReferenceType,
        current_sheet: &str,
    ) -> Result<ReferenceType, ExcelError> {
        // The extent is a read of the anchor: record it even when the anchor
        // has no current spill (the rectangle itself is recorded when the
        // caller reads it through `resolve_range_view`).
        match anchor {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => self.record_cell_1based(sheet.as_deref().unwrap_or(current_sheet), *row, *col),
            ReferenceType::NamedRange(name) => self.record_name(name),
            _ => {}
        }
        self.engine.resolve_spill_reference(anchor, current_sheet)
    }
    fn clock(&self) -> &dyn crate::timezone::ClockProvider {
        self.engine.clock()
    }
    fn timezone(&self) -> &crate::timezone::TimeZoneSpec {
        self.engine.timezone()
    }
    fn volatile_level(&self) -> crate::traits::VolatileLevel {
        self.engine.volatile_level()
    }
    fn workbook_seed(&self) -> u64 {
        self.engine.workbook_seed()
    }
    fn recalc_epoch(&self) -> u64 {
        self.engine.recalc_epoch()
    }
    fn used_rows_for_columns(
        &self,
        sheet: &str,
        start_col: u32,
        end_col: u32,
    ) -> Option<(u32, u32)> {
        self.engine.used_rows_for_columns(sheet, start_col, end_col)
    }
    fn used_cols_for_rows(&self, sheet: &str, start_row: u32, end_row: u32) -> Option<(u32, u32)> {
        self.engine.used_cols_for_rows(sheet, start_row, end_row)
    }
    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        self.engine.sheet_bounds(sheet)
    }
    fn data_snapshot_id(&self) -> u64 {
        self.engine.data_snapshot_id()
    }
    fn backend_caps(&self) -> crate::traits::BackendCaps {
        self.engine.backend_caps()
    }
    fn date_system(&self) -> crate::engine::DateSystem {
        self.engine.date_system()
    }
    fn build_lookup_index(
        &self,
        view: &RangeView<'_>,
        axis: crate::engine::lookup_index_cache::LookupAxis,
    ) -> Option<std::sync::Arc<crate::engine::lookup_index_cache::LookupIndex>> {
        self.engine.build_lookup_index(view, axis)
    }
    fn build_criteria_mask(
        &self,
        view: &RangeView<'_>,
        col_in_view: usize,
        pred: &crate::args::CriteriaPredicate,
    ) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
        self.engine.build_criteria_mask(view, col_in_view, pred)
    }
    fn build_row_visibility_mask(
        &self,
        view: &RangeView<'_>,
        mode: crate::engine::row_visibility::VisibilityMaskMode,
    ) -> Option<std::sync::Arc<arrow_array::BooleanArray>> {
        self.engine.build_row_visibility_mask(view, mode)
    }
    fn nested_subtotal_cells(
        &self,
        view: &RangeView<'_>,
        include_aggregate: bool,
    ) -> Option<Vec<(usize, usize, usize)>> {
        self.engine.nested_subtotal_cells(view, include_aggregate)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::reference::Coord;

    #[test]
    fn record_rect_skips_lazy_rect_index_when_no_current_member() {
        let cells: Vec<CellRef> = (0..(RECT_INDEX_THRESHOLD as u32 + 8))
            .map(|r| CellRef::new(0, Coord::new(r, 0, true, true)))
            .collect();
        let collector = LiveEdgeCollector::new(&cells);
        collector.record_rect(0, 0, 0, 100, 10);
        assert!(
            collector.rect_index.get().is_none(),
            "record_rect must not build RectIndex when current member is None"
        );
        collector.set_current(0);
        collector.record_rect(0, 0, 0, 5, 0);
        assert!(collector.rect_index.get().is_some());
        assert_eq!(collector.take_edges().len(), 6);
    }
}
