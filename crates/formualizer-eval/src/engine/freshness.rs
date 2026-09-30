//! Freshness of dynamic reads under `unified_authority` (design §8.2, M1c).
//!
//! Legacy orders a dynamic reader (INDIRECT, OFFSET) by a pre-probe that
//! evaluates it against the current, possibly stale, state, and after the
//! pass re-dirties only readers whose pre-probe changed. A reader whose real
//! target was still dirty therefore publishes a stale value, and its static
//! readers later in the pass clear their dirty bits with stale values too.
//!
//! This module enforces the freshness invariant FR for a recalculation pass:
//! - **Recording.** A dynamic reader is evaluated through a
//!   [`RecordingContext`] that logs every cell and range it resolves.
//! - **Dirty-at-read (FR2).** The reader is *stale* if a read covers a
//!   dirty formula cell other than itself, or the current spill extent of a
//!   dirty anchor (DirtyExtents). D is the engine's dirty flags.
//! - **No stale publication (FR3).** A stale reader's result is dropped at
//!   its commit (`plan_vertex_effects` plans no effects); it stays dirty and
//!   its reads become request-scoped replan hints.
//! - **Commit clears (FR2/FR5).** Any other vertex leaves D when its effects
//!   are planned, so a spill that commits later in the pass and re-dirties
//!   an already committed reader is not undone by an end-of-pass clear.
//! - **Layer barrier (FR4).** After a unit with a stale reader the pass
//!   stops; later units stay dirty and the loop replans with the hints.
//!
//! - **Late extents (FR5).** A spill commit writes its children through
//!   the graph, which re-dirties their readers; because committed vertices
//!   were cleared at their commit, a reader that ran before the spill stays
//!   dirty and the loop replans. Known extents of dirty anchors are ordered
//!   first by plan hints (DirtyExtents), so that is the exception.
//!
//! - **Observed reads (`rdi_dyn`, OR1).** A fresh dynamic reader's reads
//!   become its observed set in the authority host at commit. Plans order
//!   the reader after them (rectangle hints, no pre-probe evaluation), the
//!   demand walk follows them, and the schedule cache keys on their
//!   revision (design §8.3–8.4). The pre-probe remains for readers without
//!   an observed set (first evaluation, after an edit or a rebuild).
//!
//! Every pass of the replan loops is armed under the authority. Commit
//! granularity is legacy's (a cell in sequential layers, a phase group in
//! parallel layers); cycle units evaluate on their own recorded path and
//! are cleared at pass end as before. A self read (`A1 = INDIRECT("A1")`)
//! is not stale: legacy evaluates it to a value, and legacy is the spec
//! (design §8.2 case 2 plans a Δ here; not adopted).

use super::Engine;
use crate::engine::authority::geom::SYMBOL_SHEET;
use crate::engine::live_edges::{ReadLog, ReadRect, RecordingContext};
use crate::engine::scheduler::{Schedule, ScheduleUnit};
use crate::engine::vertex::VertexId;
use crate::interpreter::Interpreter;
use crate::traits::EvaluationContext;
use formualizer_common::{ExcelError, LiteralValue};
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Mutex;

/// Per-request freshness state.
#[derive(Default)]
pub(crate) struct Freshness {
    /// The current pass enforces FR (its schedule has a dynamic reader).
    armed: bool,
    /// Stale readers of the current pass and the dirty cells they read.
    /// Written from evaluation (possibly parallel), read at commit.
    stale: Mutex<FxHashMap<VertexId, Vec<VertexId>>>,
    /// Reads of fresh dynamic evaluations, moved into the authority's
    /// observed set (`rdi_dyn`) at their commit.
    fresh_reads: Mutex<FxHashMap<VertexId, Vec<ReadRect>>>,
    /// A stale reader was dropped in the current unit (FR4).
    barrier: bool,
    /// Scheduled vertices the barrier kept from running.
    skipped: Vec<VertexId>,
    /// Vertices whose effects were planned (committed) in this pass.
    committed: crate::engine::idset::DenseIdSet,
    /// Committed members of a buffered layer whose values are still in the
    /// layer's computed-write buffer: dirty for a dynamic reader's check.
    unflushed: crate::engine::idset::DenseIdSet,
    /// Stale readers dropped in this pass (they stay dirty).
    stale_this_pass: Vec<VertexId>,
    /// Fresh members of a parallel group that also held a stale reader:
    /// dropped with it (the group is one commit unit), no hints of their own.
    group_dropped: FxHashSet<VertexId>,
    /// Request-scoped plan-only hints: reader → cells it read while dirty.
    hints: FxHashMap<VertexId, Vec<VertexId>>,
    /// A spill existed when the pass began (it may have been cleared
    /// since): only then can a spill re-dirty a committed vertex (FR5).
    had_spills: bool,
    /// Stale readers dropped over the request (tests, telemetry).
    stale_total: u64,
    /// Passes stopped at a barrier over the request.
    barrier_stops: u64,
}

impl<R: EvaluationContext> Engine<R> {
    /// Reset the request-scoped state (hints, counters).
    pub(super) fn freshness_begin_request(&mut self) {
        self.freshness = Freshness::default();
    }

    /// Start a pass: every pass under the authority is armed (commit-time
    /// clearing costs what legacy's end-of-pass clear of the same vertices
    /// costs, and it is what lets FR5 re-dirties survive).
    pub(super) fn freshness_begin_pass(&mut self, _schedule: &Schedule) {
        let f = &mut self.freshness;
        f.barrier = false;
        f.skipped.clear();
        f.committed.clear();
        f.stale_this_pass.clear();
        f.group_dropped.clear();
        f.stale.get_mut().unwrap().clear();
        f.fresh_reads.get_mut().unwrap().clear();
        f.armed = true;
        f.had_spills = self.graph.has_spill_anchors();
    }

    /// DirtyExtents (design §8.2, FR2 for static reads): order every dirty
    /// spill anchor among `candidates` before the candidates whose static
    /// images meet its current extent. A first spill or a grown extent is
    /// not known here; FR5 re-dirties those readers at the spill commit.
    pub(super) fn freshness_extent_hints(
        &self,
        candidates: &[VertexId],
        vdeps: &mut FxHashMap<VertexId, Vec<VertexId>>,
    ) {
        use crate::engine::authority::geom::Rect;
        use crate::engine::authority::store::TagFilter;
        let anchors: Vec<(VertexId, u16, Rect)> = candidates
            .iter()
            .filter_map(|&v| {
                let cells = self.graph.spill_cells_for_anchor(v)?;
                let first = cells.first()?;
                let (mut r0, mut c0, mut r1, mut c1) = (u32::MAX, u32::MAX, 0u32, 0u32);
                for c in cells {
                    r0 = r0.min(c.coord.row());
                    c0 = c0.min(c.coord.col());
                    r1 = r1.max(c.coord.row());
                    c1 = c1.max(c.coord.col());
                }
                Some((v, first.sheet_id, Rect::new(r0, c0, r1, c1)))
            })
            .collect();
        if anchors.is_empty() {
            return;
        }
        let Ok(store) = self.graph.authority_plan_store() else {
            return;
        };
        let wanted: FxHashSet<VertexId> = candidates.iter().copied().collect();
        for (anchor, sheet, rect) in anchors {
            let readers = store.direct_grid_dependents(sheet, &rect, TagFilter::All);
            for (s, row, col) in readers.cells() {
                let Some(reader) = self.graph.authority_vertex_of_cell((s, row, col)) else {
                    continue;
                };
                if reader != anchor && wanted.contains(&reader) {
                    let deps = vdeps.entry(reader).or_default();
                    if !deps.contains(&anchor) {
                        deps.push(anchor);
                    }
                }
            }
        }
    }

    /// `(stale readers dropped, barrier stops)` over the current request.
    #[cfg(test)]
    pub(crate) fn freshness_counters_for_test(&self) -> (u64, u64) {
        (self.freshness.stale_total, self.freshness.barrier_stops)
    }

    pub(super) fn freshness_armed(&self) -> bool {
        self.freshness.armed
    }

    /// FR4: after unit `index`, stop the pass if a stale reader was dropped;
    /// the vertices of the remaining units stay dirty.
    pub(super) fn freshness_stop_after_unit(&mut self, schedule: &Schedule, index: usize) -> bool {
        if !self.freshness.armed || !self.freshness.barrier {
            return false;
        }
        for &unit in &schedule.units[index + 1..] {
            match unit {
                ScheduleUnit::Layer(i) => self
                    .freshness
                    .skipped
                    .extend_from_slice(&schedule.layers[i as usize].vertices),
                ScheduleUnit::Cycle(i) => self
                    .freshness
                    .skipped
                    .extend_from_slice(&schedule.cycles[i as usize]),
            }
        }
        self.freshness.barrier_stops += 1;
        true
    }

    /// Commit hook, before planning a vertex's effects: true when the
    /// vertex is a stale reader whose result must not publish (FR3). Its
    /// reads become replan hints and the unit raises the barrier (FR4).
    pub(super) fn freshness_drop_stale(&mut self, vertex: VertexId) -> bool {
        if self.freshness.group_dropped.remove(&vertex) {
            self.freshness
                .fresh_reads
                .get_mut()
                .unwrap()
                .remove(&vertex);
            self.freshness.stale_this_pass.push(vertex);
            self.freshness.barrier = true;
            return true;
        }
        let Some(reads) = self.freshness.stale.get_mut().unwrap().remove(&vertex) else {
            return false;
        };
        self.freshness.barrier = true;
        self.freshness.stale_total += 1;
        self.freshness.stale_this_pass.push(vertex);
        let hints = self.freshness.hints.entry(vertex).or_default();
        hints.extend(reads);
        hints.sort_unstable();
        hints.dedup();
        true
    }

    /// A parallel group is one commit unit (design §8.2): if any member is
    /// stale after the group's evaluation, every member is dropped.
    pub(super) fn freshness_gate_group(&mut self, group: &[VertexId]) {
        if !self.freshness.armed {
            return;
        }
        let stale = self.freshness.stale.get_mut().unwrap();
        if stale.is_empty() || !group.iter().any(|v| stale.contains_key(v)) {
            return;
        }
        for &v in group {
            if !stale.contains_key(&v) {
                self.freshness.group_dropped.insert(v);
            }
        }
    }

    /// Commit hook, after a vertex's effects were planned: it leaves the
    /// dirty set now (FR2), so a later re-dirty (a spill committing over
    /// its reads, FR5) survives the end of the pass.
    /// A group of non-dynamic vertices can commit without per-vertex stale
    /// checks: no reader is stale or dropped this pass.
    pub(super) fn freshness_group_commit_ok(&mut self) -> bool {
        !self.freshness.armed
            || (self.freshness.group_dropped.is_empty()
                && self.freshness.stale.get_mut().unwrap().is_empty())
    }

    /// `freshness_mark_committed` for a group of non-dynamic vertices
    /// (which never have fresh reads to publish).
    pub(super) fn freshness_mark_committed_group(&mut self, vertices: &[VertexId]) {
        if !self.freshness.armed {
            return;
        }
        self.freshness.committed.extend(vertices.iter().copied());
        self.graph.clear_dirty_flags(vertices);
    }

    /// FR5 for a batch of results evaluated before any of them commits (a
    /// parallel phase group, or a run unit on the per-vertex path): every
    /// member read the state from before the batch. When a member may
    /// write or clear a spill, take the members that will commit out of D
    /// now, as a sequential commit order would have. A spill committed by
    /// an earlier member then re-dirties a later member that reads it, and
    /// [`Self::freshness_batch_redirtied`] keeps that flag across the later
    /// member's own commit, so the loop replans it (FORM-192).
    ///
    /// Returns whether the batch is guarded. Only scans the batch; members
    /// are recorded as committed so an aborted pass restores their flags.
    pub(super) fn freshness_begin_batch_commit(
        &mut self,
        results: &[(VertexId, LiteralValue)],
    ) -> bool {
        if !self.freshness.armed || results.len() < 2 {
            return false;
        }
        let has_spills = self.graph.has_spill_anchors();
        if !results.iter().any(|(v, value)| {
            matches!(value, LiteralValue::Array(_))
                || (has_spills && self.graph.is_spill_anchor(*v))
        }) {
            return false;
        }
        let stale = self.freshness.stale.get_mut().unwrap();
        let group_dropped = &self.freshness.group_dropped;
        let members: Vec<VertexId> = results
            .iter()
            .map(|(v, _)| *v)
            .filter(|v| !stale.contains_key(v) && !group_dropped.contains(v))
            .collect();
        self.freshness.committed.extend(members.iter().copied());
        self.graph.clear_dirty_flags(&members);
        true
    }

    /// For a member of a guarded batch, before its commit: whether an
    /// earlier commit of the batch re-dirtied it.
    pub(super) fn freshness_batch_redirtied(&self, guarded: bool, vertex: VertexId) -> bool {
        guarded && self.graph.is_dirty(vertex)
    }

    /// After the commit of a member that [`Self::freshness_batch_redirtied`]
    /// reported: it published a value computed before the spill it reads,
    /// so it stays dirty and the loop replans it.
    pub(super) fn freshness_keep_redirtied(&mut self, redirtied: bool, vertex: VertexId) {
        if redirtied && self.graph.is_live_formula_vertex(vertex) {
            self.graph.set_dirty(vertex, true);
        }
    }

    /// Members committed in a buffered layer, values not written yet.
    pub(super) fn freshness_note_unflushed(&mut self, vertices: &[VertexId]) {
        self.freshness.unflushed.extend(vertices.iter().copied());
    }

    /// The buffered layer's writes are flushed.
    pub(super) fn freshness_flushed(&mut self) {
        self.freshness.unflushed.clear();
    }

    pub(super) fn freshness_mark_committed(&mut self, vertex: VertexId) {
        self.freshness.committed.insert(vertex);
        self.graph.clear_dirty_flags(&[vertex]);
        // OR1: a published dynamic value's reads become its observed set.
        if let Some(reads) = self
            .freshness
            .fresh_reads
            .get_mut()
            .unwrap()
            .remove(&vertex)
        {
            self.graph.authority_host_mut().set_observed(vertex, reads);
        }
    }

    /// End of a pass. Armed: clear what legacy clears except stale readers,
    /// barrier-skipped vertices and committed vertices re-dirtied after
    /// their commit; returns whether the loop must replan: any formula still
    /// dirty (`whole_workbook`), or any of the pass's candidates (targeted). Unarmed: legacy (clear all, re-dirty `changed`).
    pub(super) fn freshness_finish_pass(
        &mut self,
        to_evaluate: &[VertexId],
        changed: &[VertexId],
        whole_workbook: bool,
    ) -> bool {
        if !self.freshness.armed {
            self.graph.clear_dirty_flags(to_evaluate);
            for &v in changed {
                self.graph.set_dirty(v, true);
            }
            return !changed.is_empty();
        }
        self.freshness.armed = false;
        let keep: FxHashSet<VertexId> = self
            .freshness
            .skipped
            .iter()
            .copied()
            .chain(self.freshness.stale_this_pass.iter().copied())
            .collect();
        let clear: Vec<VertexId> = to_evaluate
            .iter()
            .copied()
            .filter(|v| !keep.contains(v) && !self.freshness.committed.contains(v))
            .collect();
        self.graph.clear_dirty_flags(&clear);
        // FR5 re-dirtied committed vertices (a spill committed or cleared
        // over their reads after their commit). Their dependents that ran
        // later in this pass read the stale value and cleared themselves at
        // their own commit: propagate from the re-dirtied set once, so the
        // replan re-evaluates them in order (FORM-192). Only a pass that
        // had or made a spill scans its committed set; the set is empty
        // unless a spill changed under a committed reader.
        if self.freshness.had_spills || self.graph.has_spill_anchors() {
            let redirtied: Vec<VertexId> = self
                .freshness
                .committed
                .iter()
                .filter(|&v| self.graph.is_dirty(v))
                .collect();
            if !redirtied.is_empty() {
                self.graph.mark_dirty_many(&redirtied);
            }
        }
        // The pass's committed set is not read after its end (the next pass
        // starts empty); a bitmap keeps one bit per vertex id.
        self.freshness.committed.clear();
        // `changed` is only non-empty from the test hook that forces replans.
        for &v in changed {
            self.graph.set_dirty(v, true);
        }
        // Stale, unreached and re-dirtied (FR5) vertices are dirty; so is a
        // reader outside this pass whose spill child was just written. A
        // targeted request only answers for its own candidates.
        !changed.is_empty()
            || if whole_workbook {
                self.graph.has_dirty_evaluation_vertices()
            } else {
                to_evaluate.iter().any(|&v| self.graph.is_dirty(v))
            }
    }

    /// A request failed with a pass still armed (a commit preflight, a
    /// deadline, cancellation): legacy clears a pass's dirty flags only at
    /// its end, so a failed pass leaves every scheduled vertex dirty. Put
    /// back the flags that commit-time clearing already took.
    pub(super) fn freshness_abort_pass(&mut self) {
        if !self.freshness.armed {
            return;
        }
        self.freshness.armed = false;
        let committed: Vec<VertexId> = self.freshness.committed.iter().collect();
        self.freshness.committed.clear();
        for v in committed {
            if !self.graph.is_dirty(v) && self.graph.is_live_formula_vertex(v) {
                self.graph.set_dirty(v, true);
            }
        }
    }

    pub(super) fn freshness_has_hints(&self) -> bool {
        !self.freshness.hints.is_empty()
    }

    /// Replan hints recorded this request for `reader`.
    pub(super) fn freshness_hints(&self, reader: VertexId) -> Option<&[VertexId]> {
        self.freshness.hints.get(&reader).map(Vec::as_slice)
    }

    /// Merge the request's replan hints into planner hints for `candidates`.
    pub(super) fn freshness_merge_hints(
        &self,
        candidates: &[VertexId],
        vdeps: &mut FxHashMap<VertexId, Vec<VertexId>>,
    ) {
        if self.freshness.hints.is_empty() {
            return;
        }
        for v in candidates {
            if let Some(hints) = self.freshness.hints.get(v) {
                let deps = vdeps.entry(*v).or_default();
                deps.extend_from_slice(hints);
                deps.sort_unstable();
                deps.dedup();
            }
        }
    }

    /// Evaluate a dynamic reader through the recorder when the pass is
    /// armed, finalizing the result exactly as `evaluate_vertex_immutable`
    /// does. `None`: not recorded, evaluate normally.
    pub(super) fn freshness_evaluate_recorded(
        &self,
        vertex: VertexId,
        sheet_name: &str,
        cell_ref: crate::reference::CellRef,
        view: crate::engine::graph::FormulaView,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        if !self.freshness.armed || !self.graph.is_dynamic(vertex) {
            return None;
        }
        let log = ReadLog::default();
        let result = {
            let ctx = RecordingContext::new(self, &log);
            let interpreter = Interpreter::new_with_cell(&ctx, sheet_name, cell_ref);
            interpreter
                .evaluate_formula_view(view, self.graph.data_store(), self.graph.sheet_reg())
                .map(|cv| {
                    let format = cv.format_id();
                    self.record_derived_format(vertex, format);
                    crate::engine::result_finalization::finalize_published_calc_result(
                        cv,
                        self.config.spill.max_spill_cells,
                    )
                })
        };
        let reads = log.take();
        let dirty = self.freshness_dirty_reads(vertex, &reads);
        if dirty.is_empty() {
            self.freshness
                .fresh_reads
                .lock()
                .unwrap()
                .insert(vertex, reads);
        } else {
            self.freshness.stale.lock().unwrap().insert(vertex, dirty);
        }
        Some(result)
    }

    /// Formula vertices other than `reader` that are dirty (or committed
    /// with their value still buffered) and covered by `reads`, plus dirty
    /// spill anchors whose extent meets a read.
    fn freshness_dirty_reads(
        &self,
        reader: VertexId,
        reads: &[(u16, u32, u32, u32, u32)],
    ) -> Vec<VertexId> {
        let mut out = Vec::new();
        let Ok(store) = self.graph.authority_plan_store() else {
            return out;
        };
        let ids = store.ids();
        for &(sheet, r0, c0, r1, c1) in reads {
            if sheet == SYMBOL_SHEET {
                continue;
            }
            for col in c0..=c1 {
                ids.visit_runs_in(sheet, col, r0, r1, &mut |h| {
                    let run = ids.run(h);
                    let a = run.row_start.max(r0);
                    let b = (run.row_start + run.len - 1).min(r1);
                    for row in a..=b {
                        let id = run.first_id + (row - run.row_start);
                        if let Some(v) = self
                            .graph
                            .authority_vertex_of_formula(id, (sheet, row, col))
                            && v != reader
                            && (self.graph.is_dirty(v) || self.freshness.unflushed.contains(&v))
                        {
                            out.push(v);
                        }
                    }
                });
            }
            for anchor in self.graph.spill_anchors_in_region(sheet, r0, c0, r1, c1) {
                if anchor != reader && self.graph.is_dirty(anchor) {
                    out.push(anchor);
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }
}
