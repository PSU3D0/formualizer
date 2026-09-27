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

/// The log with every run record expanded (built on first read).
#[derive(Debug, Default)]
struct Flat {
    events: Vec<ChangeEvent>,
    metas: Vec<ChangeEventMeta>,
    seqs: Vec<u64>,
    groups: Vec<Option<u64>>,
}

/// Audit trail for tracking all changes to the dependency graph
#[derive(Debug, Default)]
pub struct ChangeLog {
    events: Vec<ChangeEvent>,
    metas: Vec<ChangeEventMeta>,
    enabled: bool,
    /// Optional cap on retained events; when exceeded, oldest events are evicted (FIFO).
    max_changelog_events: Option<usize>,
    /// Track compound operations for atomic rollback
    compound_depth: usize,
    /// Monotonic sequence number per event
    seqs: Vec<u64>,
    /// Optional group id (compound) per event
    groups: Vec<Option<u64>>,
    next_seq: u64,
    /// Stack of active group ids for nested compounds
    group_stack: Vec<u64>,
    next_group_id: u64,

    current_meta: ChangeEventMeta,

    /// Run records (Program 2) not yet expanded into `events`, in log
    /// order; `lazy_events` counts the events they stand for. Readers see
    /// the expanded log (`flat`, built once); a mutation other than an
    /// append expands them in place first (`settle`).
    lazy: Vec<LazyEntry>,
    lazy_events: usize,
    flat: std::sync::OnceLock<Flat>,
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
            events: Vec::new(),
            metas: Vec::new(),
            enabled: true,
            max_changelog_events: None,
            compound_depth: 0,
            seqs: Vec::new(),
            groups: Vec::new(),
            next_seq: 0,
            group_stack: Vec::new(),
            next_group_id: 1,
            current_meta: ChangeEventMeta::default(),
            lazy: Vec::new(),
            lazy_events: 0,
            flat: std::sync::OnceLock::new(),
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
            self.clear_retained();
            return;
        }
        if self.len() <= max {
            return;
        }
        self.settle();
        let drop_n = self.events.len() - max;
        self.events.drain(0..drop_n);
        self.metas.drain(0..drop_n);
        self.seqs.drain(0..drop_n);
        self.groups.drain(0..drop_n);
    }

    fn clear_retained(&mut self) {
        self.events.clear();
        self.metas.clear();
        self.seqs.clear();
        self.groups.clear();
        self.lazy.clear();
        self.lazy_events = 0;
        self.flat = std::sync::OnceLock::new();
    }

    /// Build the expanded log (plain events with every run record expanded
    /// in place).
    fn build_flat(&self) -> Flat {
        let n = self.len();
        let mut f = Flat {
            events: Vec::with_capacity(n),
            metas: Vec::with_capacity(n),
            seqs: Vec::with_capacity(n),
            groups: Vec::with_capacity(n),
        };
        let mut k = 0;
        for p in 0..=self.events.len() {
            while k < self.lazy.len() && self.lazy[k].pos <= p {
                let e = &self.lazy[k];
                for (i, ev) in e.run.events().enumerate() {
                    f.events.push(ev);
                    f.metas.push(e.meta.clone());
                    f.seqs.push(e.seq0 + i as u64);
                    f.groups.push(e.group);
                }
                k += 1;
            }
            if p < self.events.len() {
                f.events.push(self.events[p].clone());
                f.metas.push(self.metas[p].clone());
                f.seqs.push(self.seqs[p]);
                f.groups.push(self.groups[p]);
            }
        }
        f
    }

    /// The expanded log when run records are held, else `None` (read the
    /// plain vectors).
    #[inline]
    fn flat(&self) -> Option<&Flat> {
        if self.lazy.is_empty() {
            None
        } else {
            Some(self.flat.get_or_init(|| self.build_flat()))
        }
    }

    /// Expand every run record in place (before a mutation that indexes
    /// the retained events).
    fn settle(&mut self) {
        if self.lazy.is_empty() {
            return;
        }
        let flat = match self.flat.take() {
            Some(f) => f,
            None => self.build_flat(),
        };
        self.events = flat.events;
        self.metas = flat.metas;
        self.seqs = flat.seqs;
        self.groups = flat.groups;
        self.lazy.clear();
        self.lazy_events = 0;
    }

    /// Before an append: once the expanded log has been read, keep it (the
    /// append goes to the expanded vectors); otherwise records stay lazy.
    #[inline]
    fn before_append(&mut self) {
        if !self.lazy.is_empty() && self.flat.get().is_some() {
            self.settle();
        }
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
            self.before_append();
            let entry = LazyEntry {
                pos: self.events.len(),
                seq0,
                group: self.group_stack.last().copied(),
                meta: meta.clone(),
                run,
            };
            debug_assert!(self.flat.get().is_none());
            self.lazy_events += entry.run.len();
            self.lazy.push(entry);
        }
    }

    fn replay_record(&mut self, event: ChangeEvent, meta: &ChangeEventMeta, retain: bool) {
        if !self.enabled {
            return;
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        if retain {
            self.before_append();
            self.events.push(event);
            self.metas.push(meta.clone());
            self.seqs.push(seq);
            self.groups.push(self.group_stack.last().copied());
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
            self.before_append();
            let seq = self.next_seq;
            self.next_seq += 1;
            let current_group = self.group_stack.last().copied();
            self.events.push(event);
            self.metas.push(self.current_meta.clone());
            self.seqs.push(seq);
            self.groups.push(current_group);
            self.enforce_cap();
        }
    }

    /// Record an event with explicit metadata (used for replay/redo).
    pub fn record_with_meta(&mut self, event: ChangeEvent, meta: ChangeEventMeta) {
        if self.enabled {
            self.before_append();
            let seq = self.next_seq;
            self.next_seq += 1;
            let current_group = self.group_stack.last().copied();
            self.events.push(event);
            self.metas.push(meta);
            self.seqs.push(seq);
            self.groups.push(current_group);
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
        if self.flat.get().is_some() {
            0
        } else {
            self.lazy.len()
        }
    }

    pub fn events(&self) -> &[ChangeEvent] {
        match self.flat() {
            Some(f) => &f.events,
            None => &self.events,
        }
    }

    pub fn event_meta(&self, index: usize) -> Option<&ChangeEventMeta> {
        match self.flat() {
            Some(f) => f.metas.get(index),
            None => self.metas.get(index),
        }
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
        self.settle();
        self.events.truncate(len);
        self.metas.truncate(len);
        self.seqs.truncate(len);
        self.groups.truncate(len);
    }

    pub fn clear(&mut self) {
        self.clear_retained();
        self.compound_depth = 0;
        self.group_stack.clear();
    }

    pub fn len(&self) -> usize {
        self.events.len() + self.lazy_events
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Extract events from index to end
    pub fn take_from(&mut self, index: usize) -> Vec<ChangeEvent> {
        self.settle();
        let events = self.events.split_off(index);
        let _ = self.metas.split_off(index);
        let _ = self.seqs.split_off(index);
        let _ = self.groups.split_off(index);
        events
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
        let (seqs, groups) = match self.flat() {
            Some(f) => (&f.seqs, &f.groups),
            None => (&self.seqs, &self.groups),
        };
        seqs.get(index).copied().zip(groups.get(index).copied())
    }

    /// Collect indices belonging to the last (innermost) complete group. Fallback: last single event.
    pub fn last_group_indices(&self) -> Vec<usize> {
        let groups = match self.flat() {
            Some(f) => &f.groups,
            None => &self.groups,
        };
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
