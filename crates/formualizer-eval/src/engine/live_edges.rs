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

use std::sync::Mutex;

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

/// One SCC member cell in collector-internal form (0-based coordinates).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MemberCell {
    sheet_id: SheetId,
    row: u32,
    col: u32,
}

#[derive(Default)]
struct CollectorState {
    /// Index (into `members`) of the member currently being evaluated.
    /// `None` until `set_current` is called; reads observed while `None` are
    /// not attributable and are dropped.
    current: Option<u32>,
    /// Live edges as `(from_member_idx, to_member_idx)`. Self-edges `(i, i)`
    /// are recorded (e.g. a member whose range argument includes itself).
    edges: FxHashSet<(u32, u32)>,
}

/// Records which reads actually occurred targeting SCC members during a
/// sequential member-by-member evaluation pass.
///
/// * Scalar reads are O(1) (hash lookup keyed by `(sheet, row, col)`).
/// * Rectangle reads are recorded **once per resolved rect** and intersected
///   with the membership in O(|SCC|) — never per cell of the rect.
/// * Name reads (named-formula SCC members, spec §7.13) are O(1) lookups by
///   the engine-folded name key.
///
/// Member indices are split: cell members occupy `0..cell_count`, name
/// members occupy `cell_count..cell_count + name_count` (matching the spec
/// §7.13 member ordering used by SCC tasks: cells first, then names).
pub struct LiveEdgeCollector {
    /// Iterable membership for rect intersection.
    members: Vec<MemberCell>,
    /// O(1) scalar lookup: (sheet_id, row0, col0) -> member index.
    index: FxHashMap<(SheetId, u32, u32), u32>,
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
        index.reserve(members.len());
        for (i, m) in members.iter().enumerate() {
            index.insert((m.sheet_id, m.row, m.col), i as u32);
        }
        let mut name_index = FxHashMap::default();
        name_index.reserve(names.len());
        for (j, name) in names.iter().enumerate() {
            name_index.insert(name.clone(), (members.len() + j) as u32);
        }
        let total_members = members.len() + names.len();
        Self {
            members,
            index,
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

    /// Stop attributing reads to any member (used between passes so that
    /// out-of-band reads — snapshots, deltas — never record edges).
    pub fn clear_current(&self) {
        self.state.lock().unwrap().current = None;
    }

    /// Record a scalar read of `(sheet_id, row, col)` (0-based).
    pub fn record_scalar(&self, sheet_id: SheetId, row: u32, col: u32) {
        let Some(&to) = self.index.get(&(sheet_id, row, col)) else {
            return;
        };
        let mut st = self.state.lock().unwrap();
        if let Some(from) = st.current {
            st.edges.insert((from, to));
        }
    }

    /// Record a rectangle read (0-based, inclusive corners). Intersection is
    /// O(|SCC|): each member is tested against the rect once; the rect is
    /// never enumerated per cell.
    pub fn record_rect(&self, sheet_id: SheetId, sr: u32, sc: u32, er: u32, ec: u32) {
        let mut st = self.state.lock().unwrap();
        let Some(from) = st.current else {
            return;
        };
        for (i, m) in self.members.iter().enumerate() {
            if m.sheet_id == sheet_id && m.row >= sr && m.row <= er && m.col >= sc && m.col <= ec {
                st.edges.insert((from, i as u32));
            }
        }
    }

    /// Record a read of a named entity by folded name key (e.g. a formula
    /// referencing a named-formula SCC member).
    pub fn record_name(&self, folded_name: &str) {
        let Some(&to) = self.name_index.get(folded_name) else {
            return;
        };
        let mut st = self.state.lock().unwrap();
        if let Some(from) = st.current {
            st.edges.insert((from, to));
        }
    }

    /// Drain the collected edges, leaving the collector empty (current member
    /// attribution is preserved).
    pub fn take_edges(&self) -> FxHashSet<(u32, u32)> {
        std::mem::take(&mut self.state.lock().unwrap().edges)
    }
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
pub struct RecordingContext<'a, R: EvaluationContext> {
    engine: &'a Engine<R>,
    collector: &'a LiveEdgeCollector,
}

impl<'a, R: EvaluationContext> RecordingContext<'a, R> {
    pub fn new(engine: &'a Engine<R>, collector: &'a LiveEdgeCollector) -> Self {
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

impl<'a, R: EvaluationContext> ReferenceResolver for RecordingContext<'a, R> {
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

impl<'a, R: EvaluationContext> RangeResolver for RecordingContext<'a, R> {
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

impl<'a, R: EvaluationContext> NamedRangeResolver for RecordingContext<'a, R> {
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

impl<'a, R: EvaluationContext> TableResolver for RecordingContext<'a, R> {
    fn resolve_table_reference(&self, tref: &TableReference) -> Result<Box<dyn Table>, ExcelError> {
        // Opaque `Table` without region context; engine-registered tables are
        // intercepted in `resolve_range_view` instead.
        self.engine.resolve_table_reference(tref)
    }
}

impl<'a, R: EvaluationContext> SourceResolver for RecordingContext<'a, R> {
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

impl<'a, R: EvaluationContext> Resolver for RecordingContext<'a, R> {}

impl<'a, R: EvaluationContext> FunctionProvider for RecordingContext<'a, R> {
    fn get_function(
        &self,
        ns: &str,
        name: &str,
    ) -> Option<std::sync::Arc<dyn crate::traits::Function>> {
        self.engine.get_function(ns, name)
    }
}

impl<'a, R: EvaluationContext> EvaluationContext for RecordingContext<'a, R> {
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

    /* ── pure delegation ── */

    fn thread_pool(&self) -> Option<&std::sync::Arc<rayon::ThreadPool>> {
        self.engine.thread_pool()
    }
    fn cancellation_token(&self) -> Option<std::sync::Arc<std::sync::atomic::AtomicBool>> {
        self.engine.cancellation_token()
    }
    fn chunk_hint(&self) -> Option<usize> {
        self.engine.chunk_hint()
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
    fn inspect_reference(
        &self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<Option<ReferenceInfo>, ExcelError> {
        self.engine.inspect_reference(reference, current_sheet)
    }
    fn formula_text_at_cell(&self, cell: CellRef) -> Result<Option<String>, ExcelError> {
        self.engine.formula_text_at_cell(cell)
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
}
