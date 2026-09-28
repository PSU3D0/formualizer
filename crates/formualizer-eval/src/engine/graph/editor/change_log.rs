//! Standalone change logging infrastructure for tracking graph mutations
//!
//! This module provides:
//! - ChangeLog: Audit trail of all graph changes
//! - ChangeEvent: Granular representation of individual changes
//! - ChangeLogger: Trait for pluggable logging strategies

use crate::SheetId;
use crate::engine::addr::GridAddr;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::row_visibility::RowVisibilitySource;
use crate::engine::vertex::VertexId;
use crate::reference::CellRef;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::ASTNode;

#[derive(Debug, Clone, PartialEq)]
pub struct SpillSnapshot {
    /// Declared target cells (row-major rectangle) owned by this spill anchor.
    pub target_cells: Vec<CellRef>,
    /// Row-major rectangular values corresponding to the target rectangle.
    pub values: Vec<Vec<LiteralValue>>,
}

/// Per-event metadata attached by the caller.
///
/// This is intentionally lightweight (Strings) to avoid leaking application types
/// into the engine layer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangeEventMeta {
    pub actor_id: Option<String>,
    pub correlation_id: Option<String>,
    pub reason: Option<String>,
}

/// Represents a single change to the dependency graph
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeEvent {
    // Simple events
    SetValue {
        addr: CellRef,
        old_value: Option<LiteralValue>,
        old_formula: Option<ASTNode>,
        new: LiteralValue,
    },
    SetFormula {
        addr: CellRef,
        old_value: Option<LiteralValue>,
        old_formula: Option<ASTNode>,
        new: ASTNode,
    },
    SetRowVisibility {
        sheet_id: SheetId,
        row0: u32,
        source: RowVisibilitySource,
        old_hidden: bool,
        new_hidden: bool,
    },
    /// Vertex creation snapshot (for undo). Minimal for now.
    AddVertex {
        id: VertexId,
        coord: GridAddr,
        sheet_id: SheetId,
        value: Option<LiteralValue>,
        formula: Option<ASTNode>,
        kind: Option<crate::engine::vertex::VertexKind>,
        flags: Option<u8>,
    },
    RemoveVertex {
        id: VertexId,
        // Need to capture more for rollback!
        old_value: Option<LiteralValue>,
        old_formula: Option<ASTNode>,
        old_dependencies: Vec<VertexId>, // outgoing
        old_dependents: Vec<VertexId>,   // incoming
        coord: Option<GridAddr>,
        sheet_id: Option<SheetId>,
        kind: Option<crate::engine::vertex::VertexKind>,
        flags: Option<u8>,
    },

    // Compound operation markers
    CompoundStart {
        description: String, // e.g., "InsertRows(sheet=0, before=5, count=2)"
        depth: usize,
    },
    CompoundEnd {
        depth: usize,
    },

    // Granular events for compound operations
    VertexMoved {
        id: VertexId,
        sheet_id: SheetId,
        old_coord: GridAddr,
        new_coord: GridAddr,
    },
    FormulaAdjusted {
        id: VertexId,
        /// Cell address for replay. May be None for non-cell formula vertices.
        addr: Option<CellRef>,
        old_ast: ASTNode,
        new_ast: ASTNode,
    },
    NamedRangeAdjusted {
        name: String,
        scope: NameScope,
        old_definition: NamedDefinition,
        new_definition: NamedDefinition,
    },
    EdgeAdded {
        from: VertexId,
        to: VertexId,
    },
    EdgeRemoved {
        from: VertexId,
        to: VertexId,
    },

    // Named range operations
    DefineName {
        name: String,
        scope: NameScope,
        definition: NamedDefinition,
    },
    UpdateName {
        name: String,
        scope: NameScope,
        old_definition: NamedDefinition,
        new_definition: NamedDefinition,
    },
    DeleteName {
        name: String,
        scope: NameScope,
        old_definition: Option<NamedDefinition>,
    },

    // Spill region changes (dynamic arrays)
    SpillCommitted {
        anchor: VertexId,
        old: Option<SpillSnapshot>,
        new: SpillSnapshot,
    },
    SpillCleared {
        anchor: VertexId,
        old: SpillSnapshot,
    },
    /// Workbook-level per-cell staged formula delta used to keep deferred edits
    /// undoable.
    ///
    /// Replaces the former `StagedFormulaStateChanged` full before/after snapshot
    /// pair (which made interactive `set_formula` O(N) per edit and O(N^2) in
    /// changelog memory — see #126). Each edit records only the affected cell's
    /// staged text transition, so a sequence of N edits costs O(N) total.
    ///
    /// - `old`: the staged formula text for the cell before the edit, if any.
    /// - `new`: the staged formula text for the cell after the edit, if any.
    ///
    /// Undo restores `old` (re-stage if `Some`, clear if `None`); redo applies
    /// `new` (re-stage if `Some`, clear if `None`).
    StagedFormulaCellChanged {
        sheet: String,
        row: u32,
        col: u32,
        old: Option<String>,
        new: Option<String>,
    },
}

/// The `FormulaAdjusted` events of a run of consecutive family members
/// that a structural edit rewrote as one (Program 2, contract decision
/// 20.5): members `0..len` are vertices `first + i` at rows `row0 + i` of
/// column `col` (after the edit); each member's old and new formula is the
/// first member's relocated down by `i` rows. Logs keep the record and
/// expand it into the per-member events only when those are read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FormulaRunAdjusted {
    pub(crate) first: VertexId,
    pub(crate) len: u32,
    pub(crate) sheet_id: SheetId,
    pub(crate) col: u32,
    pub(crate) row0: u32,
    pub(crate) old_first: ASTNode,
    pub(crate) new_first: ASTNode,
}

impl FormulaRunAdjusted {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len as usize
    }

    /// Member `i`'s event, exactly as the per-cell editor logs it.
    pub(crate) fn event(&self, i: u32) -> ChangeEvent {
        let relocate = |ast: &ASTNode| {
            if i == 0 {
                return ast.clone();
            }
            // Every member between the run's (checked) first and last
            // relocates: references are affine in the member row.
            crate::engine::template::relocate::instantiate_member_ast(ast, i64::from(i), 0)
                .expect("a run member's formula relocates")
        };
        ChangeEvent::FormulaAdjusted {
            id: VertexId(self.first.0 + i),
            addr: Some(CellRef::new(
                self.sheet_id,
                crate::reference::Coord::new(self.row0 + i, self.col, true, true),
            )),
            old_ast: relocate(&self.old_first),
            new_ast: relocate(&self.new_first),
        }
    }

    /// Every member's event, in vertex order.
    pub(crate) fn events(&self) -> impl Iterator<Item = ChangeEvent> + '_ {
        (0..self.len).map(|i| self.event(i))
    }
}

/// A run record held by a log: it stands for `run.len` events placed
/// before retained event `pos`, with consecutive sequence numbers from
/// `seq0`, one group and one meta.
#[derive(Debug)]
struct LazyEntry {
    pos: usize,
    seq0: u64,
    group: Option<u64>,
    meta: ChangeEventMeta,
    run: std::sync::Arc<FormulaRunAdjusted>,
}

/// The retained events of a log. `events`/`metas`/`seqs`/`groups` are the
/// canonical (parallel) vectors; `lazy` holds run records not yet expanded
/// into them, in log order, each positioned before `events[pos]`. Every
/// record's position is at or after the previous record's, and the events
/// before the first record are expanded history.
#[derive(Debug, Default)]
struct Retained {
    events: Vec<ChangeEvent>,
    metas: Vec<ChangeEventMeta>,
    seqs: Vec<u64>,
    groups: Vec<Option<u64>>,
    lazy: Vec<LazyEntry>,
}

impl Retained {
    /// Expand every pending run record in place. Only the events from the
    /// first record's position on are touched (moved, never cloned), so
    /// history expanded by an earlier call costs nothing again. Returns the
    /// number of events written (expanded plus moved), for the work gate.
    fn expand(&mut self) -> usize {
        let Some(first) = self.lazy.first() else {
            return 0;
        };
        let start = first.pos;
        let tail_events = self.events.split_off(start);
        let tail_metas = self.metas.split_off(start);
        let tail_seqs = self.seqs.split_off(start);
        let tail_groups = self.groups.split_off(start);
        let tail_len = tail_events.len();
        let added: usize = self.lazy.iter().map(|e| e.run.len()).sum();
        self.events.reserve(tail_len + added);
        self.metas.reserve(tail_len + added);
        self.seqs.reserve(tail_len + added);
        self.groups.reserve(tail_len + added);
        let mut tail = tail_events
            .into_iter()
            .zip(tail_metas)
            .zip(tail_seqs.into_iter().zip(tail_groups));
        let mut p = start;
        for entry in std::mem::take(&mut self.lazy) {
            while p < entry.pos {
                let ((event, meta), (seq, group)) = tail.next().expect("record position in range");
                self.events.push(event);
                self.metas.push(meta);
                self.seqs.push(seq);
                self.groups.push(group);
                p += 1;
            }
            for (i, event) in entry.run.events().enumerate() {
                self.events.push(event);
                self.metas.push(entry.meta.clone());
                self.seqs.push(entry.seq0 + i as u64);
                self.groups.push(entry.group);
            }
        }
        for ((event, meta), (seq, group)) in tail {
            self.events.push(event);
            self.metas.push(meta);
            self.seqs.push(seq);
            self.groups.push(group);
        }
        tail_len + added
    }
}

/// [`Retained`] behind a cell that lets a shared read expand pending run
/// records in place, once, so readers borrow one contiguous expanded log
/// and every run record is expanded exactly once over the log's life.
///
/// Invariant: `expanded` is initialized exactly when `inner.lazy` is
/// empty. Shared access to `inner` goes only through [`Self::get`], which
/// runs the expansion inside `expanded.get_or_init` and hands out
/// references only after it completes; every other access takes `&mut
/// self`, and a record is added (resetting `expanded`) only through
/// `&mut self` as well.
struct RetainedCell {
    inner: std::cell::UnsafeCell<Retained>,
    expanded: std::sync::OnceLock<()>,
    /// Retained events, pending records included.
    len: usize,
    /// Pending run records (meaningful while `expanded` is uninitialized).
    pending_records: usize,
    /// Events written by expansion (expanded and moved) over the log's
    /// life: the work gate of the changelog scaling test.
    #[cfg(test)]
    work: std::sync::atomic::AtomicUsize,
}

// SAFETY: `inner` is mutated through a shared reference only inside
// `expanded.get_or_init`, which runs the closure at most once per
// initialization, blocks concurrent callers until it finishes and
// publishes its writes (happens-before) to them. No shared reference into
// `inner` can exist while it runs: `get` returns only after initialization,
// and `expanded` is reset only through `&mut self`. This is the same
// contract as `Mutex<Retained>` / `OnceLock<Retained>`, hence the
// `Send + Sync` requirement on the contents (asserted below).
unsafe impl Sync for RetainedCell {}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Retained>();
};

impl Default for RetainedCell {
    fn default() -> Self {
        Self {
            inner: std::cell::UnsafeCell::new(Retained::default()),
            expanded: std::sync::OnceLock::from(()),
            len: 0,
            pending_records: 0,
            #[cfg(test)]
            work: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl std::fmt::Debug for RetainedCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedCell")
            .field("len", &self.len)
            .field("expanded", &self.expanded.get().is_some())
            .finish()
    }
}

impl RetainedCell {
    #[inline]
    fn note_work(&self, _work: usize) {
        #[cfg(test)]
        self.work
            .fetch_add(_work, std::sync::atomic::Ordering::Relaxed);
    }

    /// The expanded log (pending records are expanded first, once).
    fn get(&self) -> &Retained {
        self.expanded.get_or_init(|| {
            // SAFETY: see the `Sync` impl: this closure is the only access
            // to `inner` while it runs.
            let inner = unsafe { &mut *self.inner.get() };
            self.note_work(inner.expand());
        });
        // SAFETY: initialized: no mutation through `&self` until `&mut self`.
        unsafe { &*self.inner.get() }
    }

    /// Expand pending records in place and return the expanded vectors.
    fn settle(&mut self) -> &mut Retained {
        if self.expanded.get().is_none() {
            let work = self.inner.get_mut().expand();
            self.note_work(work);
            self.expanded = std::sync::OnceLock::from(());
            self.pending_records = 0;
        }
        self.inner.get_mut()
    }

    /// Plain events so far (the position of a record appended now).
    fn plain_len(&mut self) -> usize {
        self.inner.get_mut().events.len()
    }

    fn push(&mut self, event: ChangeEvent, meta: ChangeEventMeta, seq: u64, group: Option<u64>) {
        let r = self.inner.get_mut();
        r.events.push(event);
        r.metas.push(meta);
        r.seqs.push(seq);
        r.groups.push(group);
        self.len += 1;
    }

    fn push_run(&mut self, entry: LazyEntry) {
        self.len += entry.run.len();
        self.inner.get_mut().lazy.push(entry);
        self.pending_records += 1;
        self.expanded = std::sync::OnceLock::new();
    }

    fn drain_front(&mut self, n: usize) {
        let r = self.settle();
        r.events.drain(0..n);
        r.metas.drain(0..n);
        r.seqs.drain(0..n);
        r.groups.drain(0..n);
        self.len = r.events.len();
    }

    fn truncate(&mut self, len: usize) {
        let r = self.settle();
        r.events.truncate(len);
        r.metas.truncate(len);
        r.seqs.truncate(len);
        r.groups.truncate(len);
        self.len = r.events.len();
    }

    fn split_off(&mut self, index: usize) -> Vec<ChangeEvent> {
        let r = self.settle();
        let events = r.events.split_off(index);
        let _ = r.metas.split_off(index);
        let _ = r.seqs.split_off(index);
        let _ = r.groups.split_off(index);
        self.len = r.events.len();
        events
    }

    fn clear(&mut self) {
        *self = Self {
            #[cfg(test)]
            work: std::sync::atomic::AtomicUsize::new(
                self.work.load(std::sync::atomic::Ordering::Relaxed),
            ),
            ..Self::default()
        };
    }
}

/// Audit trail for tracking all changes to the dependency graph
#[derive(Debug, Default)]
pub struct ChangeLog {
    enabled: bool,
    /// Optional cap on retained events; when exceeded, oldest events are evicted (FIFO).
    max_changelog_events: Option<usize>,
    /// Track compound operations for atomic rollback
    compound_depth: usize,
    next_seq: u64,
    /// Stack of active group ids for nested compounds
    group_stack: Vec<u64>,
    next_group_id: u64,

    current_meta: ChangeEventMeta,

    /// Retained events with their metadata, monotonic sequence numbers and
    /// optional group (compound) ids. Run records (Program 2) stay
    /// unexpanded until the log is read or indexed by a mutation; each
    /// record is expanded in place exactly once (see [`RetainedCell`]).
    retained: RetainedCell,
}

/// Complete, operation-local mutation capture used by `Engine` correctness paths.
///
/// Unlike `ChangeLog`, this sink is always enabled and never evicts. It is crate-private so
/// audit retention remains a property of `ChangeLog`, not of graph mutation.
#[derive(Debug)]
pub(crate) struct MutationCapture {
    events: Vec<ChangeEvent>,
    /// Run records (Program 2): `(pos, run)` stands for the run's events
    /// placed before `events[pos]`, in order.
    lazy: Vec<(usize, std::sync::Arc<FormulaRunAdjusted>)>,
    compound_depth: usize,
    current_meta: ChangeEventMeta,
}

impl MutationCapture {
    pub(crate) fn new(current_meta: ChangeEventMeta) -> Self {
        Self {
            events: Vec::new(),
            lazy: Vec::new(),
            compound_depth: 0,
            current_meta,
        }
    }

    /// Number of plain (non-run) events: a position marker for
    /// [`Self::events`].
    pub(crate) fn len(&self) -> usize {
        self.events.len()
    }

    /// The plain events; run records are not in this slice (see
    /// [`Self::expanded_events_from`]). Every consumer of a forward edit's
    /// events ignores `FormulaAdjusted` except invalidation, which also
    /// asks [`Self::lazy_len`].
    pub(crate) fn events(&self) -> &[ChangeEvent] {
        &self.events
    }

    /// Number of run records (a position marker for records).
    pub(crate) fn lazy_len(&self) -> usize {
        self.lazy.len()
    }

    /// Record a run's `FormulaAdjusted` events without expanding them.
    pub(crate) fn record_lazy(&mut self, run: FormulaRunAdjusted) {
        if run.len == 0 {
            return;
        }
        self.lazy
            .push((self.events.len(), std::sync::Arc::new(run)));
    }

    /// Every event from plain position `start` (and run record
    /// `lazy_start`) on, run records expanded in place.
    pub(crate) fn expanded_events_from(&self, start: usize, lazy_start: usize) -> Vec<ChangeEvent> {
        let lazy = &self.lazy[lazy_start.min(self.lazy.len())..];
        let mut out = Vec::with_capacity(
            self.events.len().saturating_sub(start)
                + lazy.iter().map(|(_, r)| r.len()).sum::<usize>(),
        );
        let mut k = 0;
        for p in start..=self.events.len() {
            while k < lazy.len() && lazy[k].0 <= p {
                out.extend(lazy[k].1.events());
                k += 1;
            }
            if let Some(e) = self.events.get(p) {
                out.push(e.clone());
            }
        }
        out
    }

    pub(crate) fn close_compounds(&mut self) {
        while self.compound_depth > 0 {
            self.end_compound();
        }
    }

    fn push(&mut self, event: ChangeEvent) {
        self.events.push(event);
    }
}

/// Backward replay helper: the description of the compound whose end
/// marker is event `end` (`event(i)` reads the events in forward order):
/// the start marker that closes it, counting nesting (depth fields are not
/// unique: a redo re-logs a group's markers inside its own compound).
pub(crate) fn compound_start_description<'a>(
    end: usize,
    event: impl Fn(usize) -> &'a ChangeEvent,
) -> Option<&'a str> {
    if !matches!(event(end), ChangeEvent::CompoundEnd { .. }) {
        return None;
    }
    let mut open = 1usize;
    for i in (0..end).rev() {
        match event(i) {
            ChangeEvent::CompoundEnd { .. } => open += 1,
            ChangeEvent::CompoundStart { description, .. } => {
                open -= 1;
                if open == 0 {
                    return Some(description.as_str());
                }
            }
            _ => {}
        }
    }
    None
}

impl ChangeLogger for MutationCapture {
    fn record(&mut self, event: ChangeEvent) {
        self.push(event);
    }

    fn set_enabled(&mut self, _: bool) {}

    fn begin_compound(&mut self, description: String) {
        self.compound_depth += 1;
        self.push(ChangeEvent::CompoundStart {
            description,
            depth: self.compound_depth,
        });
    }

    fn end_compound(&mut self) {
        if self.compound_depth == 0 {
            return;
        }
        self.push(ChangeEvent::CompoundEnd {
            depth: self.compound_depth,
        });
        self.compound_depth -= 1;
    }
}

impl ChangeLog {
    pub fn new() -> Self {
        Self {
            enabled: true,
            max_changelog_events: None,
            compound_depth: 0,
            next_seq: 0,
            group_stack: Vec::new(),
            next_group_id: 1,
            current_meta: ChangeEventMeta::default(),
            retained: RetainedCell::default(),
        }
    }

    pub fn with_max_changelog_events(max: usize) -> Self {
        let mut out = Self::new();
        out.max_changelog_events = Some(max);
        out
    }

    pub fn set_max_changelog_events(&mut self, max: Option<usize>) {
        self.max_changelog_events = max;
        self.enforce_cap();
    }

    fn enforce_cap(&mut self) {
        let Some(max) = self.max_changelog_events else {
            return;
        };
        if max == 0 {
            self.retained.clear();
            return;
        }
        if self.len() <= max {
            return;
        }
        let drop_n = self.len() - max;
        self.retained.drain_front(drop_n);
    }

    fn replay_lazy(
        &mut self,
        run: std::sync::Arc<FormulaRunAdjusted>,
        meta: &ChangeEventMeta,
        retain: bool,
    ) {
        if !self.enabled {
            return;
        }
        let seq0 = self.next_seq;
        self.next_seq += run.len as u64;
        if retain {
            let entry = LazyEntry {
                pos: self.retained.plain_len(),
                seq0,
                group: self.group_stack.last().copied(),
                meta: meta.clone(),
                run,
            };
            self.retained.push_run(entry);
        }
    }

    fn replay_record(&mut self, event: ChangeEvent, meta: &ChangeEventMeta, retain: bool) {
        if !self.enabled {
            return;
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        if retain {
            let group = self.group_stack.last().copied();
            self.retained.push(event, meta.clone(), seq, group);
        }
    }

    fn replay_begin_compound(&mut self, description: String, meta: &ChangeEventMeta, retain: bool) {
        self.compound_depth += 1;
        if self.compound_depth == 1 {
            let gid = self.next_group_id;
            self.next_group_id += 1;
            self.group_stack.push(gid);
        } else if let Some(&gid) = self.group_stack.last() {
            self.group_stack.push(gid);
        }
        self.replay_record(
            ChangeEvent::CompoundStart {
                description,
                depth: self.compound_depth,
            },
            meta,
            retain,
        );
    }

    fn replay_end_compound(&mut self, meta: &ChangeEventMeta, retain: bool) {
        if self.compound_depth == 0 {
            return;
        }
        self.replay_record(
            ChangeEvent::CompoundEnd {
                depth: self.compound_depth,
            },
            meta,
            retain,
        );
        self.compound_depth -= 1;
        self.group_stack.pop();
    }

    fn replay_capture(&mut self, capture: MutationCapture, retain: bool) {
        let mut lazy = capture.lazy.into_iter().peekable();
        for (p, event) in capture.events.into_iter().enumerate() {
            while let Some((_, run)) = lazy.next_if(|(pos, _)| *pos <= p) {
                self.replay_lazy(run, &capture.current_meta, retain);
            }
            match event {
                ChangeEvent::CompoundStart { description, .. } => {
                    self.replay_begin_compound(description, &capture.current_meta, retain);
                }
                ChangeEvent::CompoundEnd { .. } => {
                    self.replay_end_compound(&capture.current_meta, retain);
                }
                event => self.replay_record(event, &capture.current_meta, retain),
            }
        }
        for (_, run) in lazy {
            self.replay_lazy(run, &capture.current_meta, retain);
        }
        if retain {
            self.enforce_cap();
        }
    }

    pub(crate) fn current_meta(&self) -> ChangeEventMeta {
        self.current_meta.clone()
    }

    pub(crate) fn publish_capture(&mut self, capture: MutationCapture) {
        self.replay_capture(capture, true);
    }

    pub(crate) fn discard_capture(&mut self, capture: MutationCapture) {
        self.replay_capture(capture, false);
    }

    pub fn record(&mut self, event: ChangeEvent) {
        if self.enabled {
            let seq = self.next_seq;
            self.next_seq += 1;
            let current_group = self.group_stack.last().copied();
            self.retained
                .push(event, self.current_meta.clone(), seq, current_group);
            self.enforce_cap();
        }
    }

    /// Record an event with explicit metadata (used for replay/redo).
    pub fn record_with_meta(&mut self, event: ChangeEvent, meta: ChangeEventMeta) {
        if self.enabled {
            let seq = self.next_seq;
            self.next_seq += 1;
            let current_group = self.group_stack.last().copied();
            self.retained.push(event, meta, seq, current_group);
            self.enforce_cap();
        }
    }

    /// Begin a compound operation (multiple changes from single action)
    pub fn begin_compound(&mut self, description: String) {
        self.compound_depth += 1;
        if self.compound_depth == 1 {
            // allocate new group id
            let gid = self.next_group_id;
            self.next_group_id += 1;
            self.group_stack.push(gid);
        } else {
            // nested: reuse top id
            if let Some(&gid) = self.group_stack.last() {
                self.group_stack.push(gid);
            }
        }
        if self.enabled {
            self.record(ChangeEvent::CompoundStart {
                description,
                depth: self.compound_depth,
            });
        }
    }

    /// End a compound operation
    pub fn end_compound(&mut self) {
        if self.compound_depth > 0 {
            if self.enabled {
                self.record(ChangeEvent::CompoundEnd {
                    depth: self.compound_depth,
                });
            }
            self.compound_depth -= 1;
            self.group_stack.pop();
        }
    }

    /// Run records not yet expanded in place (tests: laziness).
    #[cfg(test)]
    pub(crate) fn unexpanded_run_records(&self) -> usize {
        if self.retained.expanded.get().is_some() {
            0
        } else {
            self.retained.pending_records
        }
    }

    /// Events written into the retained vectors by run-record expansion
    /// (expanded members plus plain events moved behind them) over the
    /// log's life (tests: the work-scaling gate).
    #[cfg(test)]
    pub(crate) fn expansion_work(&self) -> usize {
        self.retained
            .work
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn events(&self) -> &[ChangeEvent] {
        &self.retained.get().events
    }

    pub fn event_meta(&self, index: usize) -> Option<&ChangeEventMeta> {
        self.retained.get().metas.get(index)
    }

    pub fn set_actor_id(&mut self, actor_id: Option<String>) {
        self.current_meta.actor_id = actor_id;
    }

    pub fn set_correlation_id(&mut self, correlation_id: Option<String>) {
        self.current_meta.correlation_id = correlation_id;
    }

    pub fn set_reason(&mut self, reason: Option<String>) {
        self.current_meta.reason = reason;
    }

    /// Truncate log (and metadata) to len
    pub fn truncate(&mut self, len: usize) {
        self.retained.truncate(len);
    }

    pub fn clear(&mut self) {
        self.retained.clear();
        self.compound_depth = 0;
        self.group_stack.clear();
    }

    pub fn len(&self) -> usize {
        self.retained.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Extract events from index to end
    pub fn take_from(&mut self, index: usize) -> Vec<ChangeEvent> {
        self.retained.split_off(index)
    }

    /// Temporarily disable logging (for rollback operations)
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Get current compound depth (for testing)
    pub fn compound_depth(&self) -> usize {
        self.compound_depth
    }

    /// Return (sequence_number, group_id) metadata for event index
    pub fn meta(&self, index: usize) -> Option<(u64, Option<u64>)> {
        let r = self.retained.get();
        r.seqs.get(index).copied().zip(r.groups.get(index).copied())
    }

    /// Collect indices belonging to the last (innermost) complete group. Fallback: last single event.
    pub fn last_group_indices(&self) -> Vec<usize> {
        let groups = &self.retained.get().groups;
        if let Some(&last_gid) = groups.iter().rev().flatten().next() {
            let idxs: Vec<usize> = groups
                .iter()
                .enumerate()
                .filter_map(|(i, g)| if *g == Some(last_gid) { Some(i) } else { None })
                .collect();
            if !idxs.is_empty() {
                return idxs;
            }
        }
        self.len().checked_sub(1).into_iter().collect()
    }
}

/// Trait for pluggable logging strategies
pub trait ChangeLogger {
    fn record(&mut self, event: ChangeEvent);
    fn set_enabled(&mut self, enabled: bool);
    fn begin_compound(&mut self, description: String);
    fn end_compound(&mut self);
}

impl ChangeLogger for ChangeLog {
    fn record(&mut self, event: ChangeEvent) {
        ChangeLog::record(self, event);
    }

    fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    fn begin_compound(&mut self, description: String) {
        ChangeLog::begin_compound(self, description);
    }

    fn end_compound(&mut self) {
        ChangeLog::end_compound(self);
    }
}

/// Null logger for when change tracking not needed
pub struct NullChangeLogger;

impl ChangeLogger for NullChangeLogger {
    fn record(&mut self, _: ChangeEvent) {}
    fn set_enabled(&mut self, _: bool) {}
    fn begin_compound(&mut self, _: String) {}
    fn end_compound(&mut self) {}
}
