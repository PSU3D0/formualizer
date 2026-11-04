//! The single held-schedule driver for explicit jobs and evaluate_all_until.
use super::*;
use crate::engine::resumable::*;

pub(in crate::engine) struct HeldRecalcSchedule {
    full: Walk,
    priority: Option<Walk>,
    candidates: Vec<VertexId>,
    old_vdeps: FxHashMap<VertexId, Vec<VertexId>>,
    aux_retained_bytes: u64,
}
struct Walk {
    schedule: EvaluationSchedule,
    unit: usize,
    /// Vertex offset within the current layer, always at an atomic run boundary.
    layer_offset: usize,
    completed: usize,
    total: usize,
    retained_bytes: u64,
}
impl Walk {
    fn new(schedule: EvaluationSchedule) -> Self {
        let total = schedule
            .layers
            .iter()
            .map(|l| l.vertices.len())
            .sum::<usize>()
            + schedule.cycles.iter().map(Vec::len).sum::<usize>();
        let retained_bytes = match &schedule {
            EvaluationSchedule::Shared(_) => 0,
            EvaluationSchedule::Owned(s) => {
                (s.units.capacity() * std::mem::size_of::<ScheduleUnit>()
                    + s.layers.capacity() * std::mem::size_of::<crate::engine::scheduler::Layer>()
                    + s.cycles.capacity() * std::mem::size_of::<Vec<VertexId>>()
                    + s.cycles
                        .iter()
                        .map(|c| c.capacity() * std::mem::size_of::<VertexId>())
                        .sum::<usize>()
                    + s.layers
                        .iter()
                        .map(|l| {
                            l.vertices.capacity() * std::mem::size_of::<VertexId>()
                                + l.runs.capacity()
                                    * std::mem::size_of::<crate::engine::scheduler::LayerRun>()
                        })
                        .sum::<usize>()) as u64
            }
        };
        Self {
            schedule,
            unit: 0,
            layer_offset: 0,
            completed: 0,
            total,
            retained_bytes,
        }
    }
    fn done(&self) -> bool {
        self.unit == self.schedule.units.len()
    }
}
impl<R: EvaluationContext> Engine<R> {
    /// O(1) setup. Planning and clock sampling happen in the first step.
    pub fn begin_recalc(&mut self, options: RecalcJobOptions) -> Result<(), RecalcJobError> {
        if self.has_active_recalc_job() || self.resumable.is_some() {
            return Err(Self::job_error(
                "recalculation already pending; finish it before beginning another job",
            ).into());
        }
        self.recalc_options = Some(options);
        Ok(())
    }
    fn job_error(message: &str) -> ExcelError {
        ExcelError::new(ExcelErrorKind::Value).with_message(message)
    }
    /// True only for an explicit job. Implicit Phase 1 continuations are
    /// reported by has_pending_resumable_evaluation.
    pub fn has_active_recalc_job(&self) -> bool {
        self.recalc_options.is_some()
    }

    /// Last request wins; materialization is deferred until the next step.
    pub fn prioritize_recalc(
        &mut self,
        targets: &[crate::engine::EvaluationTarget],
    ) -> Result<(), RecalcJobError> {
        let options = self
            .recalc_options
            .as_mut()
            .ok_or_else(|| Self::job_error("no active recalculation job"))?;
        options.initial_priority = targets.to_vec();
        if let Some(held) = self.resumable.as_mut().and_then(|s| s.held.as_mut()) {
            held.priority = None;
        }
        Ok(())
    }

    /// Preserve installed values and dirtiness; cancellation never advances the epoch.
    pub fn cancel_recalc(&mut self) -> Option<StepReport> {
        self.recalc_options.take()?;
        let progress = self.recalc_progress();
        self.resumable = None;
        self.redirty_for_next_recalc();
        self.freshness_begin_request();
        Some(StepReport {
            phase: StepPhase::Cancelled,
            progress,
            step_computed_vertices: 0,
            step_elapsed: std::time::Duration::ZERO,
            changed: Default::default(),
            longest_unit: None,
            result: None,
        })
    }
    pub fn step_recalc(&mut self, stop: EvaluationStop) -> Result<StepReport, RecalcJobError> {
        if !self.has_active_recalc_job() {
            return Err(Self::job_error("no active recalculation job").into());
        }
        let result = self.drive_recalc_step(stop);
        if result.as_ref().map_or(true, |r| {
            matches!(r.phase, StepPhase::Finished | StepPhase::Cancelled)
        }) {
            self.recalc_options = None;
            if result
                .as_ref()
                .is_ok_and(|r| r.phase == StepPhase::Cancelled)
            {
                self.resumable = None;
                self.redirty_for_next_recalc();
                self.freshness_begin_request();
            }
        }
        result.map_err(Into::into)
    }

    /// Drive an explicit job, invoking the callback between steps.
    pub fn recalc_with(
        &mut self,
        options: RecalcJobOptions,
        _stop_per_step: EvaluationStop,
        _on_step: impl FnMut(&mut Self, &StepReport) -> std::ops::ControlFlow<()>,
    ) -> Result<RecalcOutcome, RecalcJobError> {
        self.begin_recalc(options)?;
        Ok(RecalcOutcome::Paused)
    }

    pub(super) fn drive_recalc_step(
        &mut self,
        stop: EvaluationStop,
    ) -> Result<StepReport, ExcelError> {
        self.resumable_call = true;
        self.active_cancel_flag = stop.cancel.clone();
        let result = self.observe_evaluation_resource_request(
            EvaluationRequestKind::FullCancellable,
            |engine| engine.run_recalc_step(stop),
        );
        self.active_cancel_flag = None;
        self.resumable_call = false;
        if result.is_err() {
            self.resumable = None;
            self.redirty_for_next_recalc();
        }
        result
    }

    fn recalc_progress(&self) -> StepProgress {
        let Some(s) = &self.resumable else {
            return StepProgress::default();
        };
        let held = s.held.as_ref();
        StepProgress {
            pass: s.pass.max(1),
            completed_in_pass: s.pass_completed,
            scheduled_in_pass: s.pass_scheduled,
            cumulative_computed_vertices: s.computed_vertices,
            total_exact: s.pass_exact
                && !self.graph.has_spill_anchors()
                && !self.has_staged_formulas(),
            staged_formulas_remaining: self.staged_formula_count(),
            priority_pending: s.priority_deferred
                || held.is_some_and(|h| h.priority.as_ref().is_some_and(|p| !p.done()))
                || self
                    .recalc_options
                    .as_ref()
                    .is_some_and(|o| !o.initial_priority.is_empty()),
        }
    }

    fn run_recalc_step(&mut self, stop: EvaluationStop) -> Result<StepReport, ExcelError> {
        debug_assert!(!self.graph.deferred_dirty_active());
        let start = crate::instant::FzInstant::now();
        let stop_at = stop.soft_deadline.and_then(|d| start.checked_add(d));
        let explicit = self.has_active_recalc_job();
        let function_epoch = crate::function_registry::semantic_epoch();
        let provider_revision = self.resolver.planning_semantic_revision();
        self.observe_function_semantic_epoch()?;
        self.lookup_index_cache.reset_counters();
        let _source_cache = self.source_cache_session();
        self.validate_deterministic_mode()?;
        self.account_recalc_retained()?;
        let initial = self.resumable.is_none();
        if initial {
            self.begin_evaluation_request();
            let mut state = LogicalRecalculation::default();
            let mut retained: FxHashMap<u64, Vec<VertexId>> = FxHashMap::default();
            for (&vertex, &scc) in &self.retained_scc_members {
                retained.entry(scc).or_default().push(vertex);
            }
            for mut key in retained.into_values() {
                key.sort_unstable();
                state
                    .cycle_seeds
                    .insert(key.clone(), self.capture_original_cycle_seeds(&key));
                state.original_cycles.insert(key);
            }
            state.refresh_seed_retained_bytes();
            self.resumable = Some(state);
        }
        let mutation_revision = self.inspection_mutation_revision();
        let state = self.resumable.as_ref().unwrap();
        let semantics_changed = !initial
            && (state.function_epoch != function_epoch
                || state.provider_revision != provider_revision);
        let mutated = !initial
            && (state.mutation_revision != mutation_revision
                || state.source_mutation_generation != self.source_mutation_generation);
        let invalidated = semantics_changed
            || mutated
            || (!initial
                && (state.topology_epoch != self.topology_epoch
                    || state.authority_revision != self.schedule_cache_authority_revision()));
        if semantics_changed || mutated {
            let vertices: Vec<_> = state
                .installed
                .iter()
                .filter(|&v| {
                    self.graph.is_live_formula_vertex(v)
                        && (semantics_changed
                            || self.graph.is_volatile(v)
                            || self.graph.is_dynamic(v))
                })
                .collect();
            self.graph.mark_dirty_many(&vertices);
        }
        if invalidated {
            // Supported edits and semantic changes publish one of these keys.
            // Stable steps must not scan the resolution snapshot's sheet names.
            self.refresh_resolution_snapshots();
            let state = self.resumable.as_mut().unwrap();
            state.held = None;
            if let Some(options) = &mut self.recalc_options
                && options.initial_priority.is_empty()
            {
                options.initial_priority = state.priority_targets.clone();
            }
        }
        let mut computed = 0;
        let mut cycle_errors = 0;
        let mut units = 0;
        let mut phase = StepPhase::Evaluating;
        let mut longest: Option<SlowUnitReport> = None;
        let collect = self
            .recalc_options
            .as_ref()
            .is_some_and(|o| o.collect_deltas);
        let mut delta = DeltaCollector::new(if collect {
            DeltaMode::Cells
        } else {
            DeltaMode::Off
        });
        let cancelled =
            |stop: &EvaluationStop| stop.cancel.as_ref().is_some_and(|t| t.is_cancelled());
        let expired = || stop_at.is_some_and(|t| crate::instant::FzInstant::now() >= t);
        if explicit && cancelled(&stop) {
            phase = StepPhase::Cancelled;
        }
        if phase != StepPhase::Cancelled
            && self.config.defer_graph_building
            && self.has_staged_formulas()
        {
            let t = crate::instant::FzInstant::now();
            // Graph construction is one atomic unit in v1. A soft token
            // cannot discard it mid-build; hard ledger checks stay armed.
            let saved_cancel = self.active_cancel_flag.take();
            let built = self.build_graph_all();
            self.active_cancel_flag = saved_cancel;
            built?;
            // Parsing staged formulas introduces function names after admission.
            // Refresh dispatch here without restarting the logical clock/collector.
            self.refresh_resolution_snapshots();
            Self::note_slow_unit(
                &mut longest,
                SlowUnitKind::GraphBuild,
                &[],
                t.elapsed(),
                &self.graph,
            );
            self.resumable.as_mut().unwrap().held = None;
            if explicit && cancelled(&stop) {
                phase = StepPhase::Cancelled;
            } else if explicit && expired() {
                phase = StepPhase::BuildingGraph;
            }
        }
        if phase != StepPhase::Cancelled {
            self.require_unified_authority()?;
        }
        if phase == StepPhase::Evaluating {
            loop {
                if self.resumable.as_ref().unwrap().held.is_none() {
                    let candidates: Vec<_> = self
                        .graph
                        .get_evaluation_vertices()
                        .into_iter()
                        .filter(|&v| {
                            initial && self.resumable.as_ref().unwrap().pass == 0
                                || self.graph.is_dirty(v)
                        })
                        .collect();
                    if candidates.is_empty() {
                        phase = StepPhase::Finished;
                        break;
                    }
                    if cancelled(&stop) && self.resumable.as_ref().unwrap().original_cycles_recorded
                    {
                        phase = StepPhase::Cancelled;
                        break;
                    }
                    // Pre-cancelled initial planning still captures original SCC
                    // seeds before edits can dissolve and recreate them (R1).
                    let saved = if cancelled(&stop) {
                        self.active_cancel_flag.take()
                    } else {
                        None
                    };
                    let t = crate::instant::FzInstant::now();
                    let planned = self.create_evaluation_schedule(&candidates);
                    if saved.is_some() {
                        self.active_cancel_flag = saved;
                    }
                    let (schedule, old_vdeps, _) = planned?;
                    self.record_original_cycle_seeds(&schedule);
                    // Conservative: function calls and array/reference expressions
                    // may introduce a first spill even with no current anchor.
                    let exact = self.recalc_total_exact(&candidates);
                    Self::note_slow_unit(
                        &mut longest,
                        SlowUnitKind::ScheduleBuild,
                        &[],
                        t.elapsed(),
                        &self.graph,
                    );
                    let authority_revision = self.schedule_cache_authority_revision();
                    let aux_retained_bytes = (candidates.capacity()
                        * std::mem::size_of::<VertexId>()
                        + old_vdeps.capacity() * std::mem::size_of::<(VertexId, Vec<VertexId>)>()
                        + old_vdeps
                            .values()
                            .map(|v| v.capacity() * std::mem::size_of::<VertexId>())
                            .sum::<usize>()) as u64;
                    let full = Walk::new(schedule);
                    let s = self.resumable.as_mut().unwrap();
                    s.pass += 1;
                    s.pass_completed = 0;
                    s.pass_scheduled = full.total;
                    s.pass_exact = exact;
                    s.authority_revision = authority_revision;
                    s.held = Some(HeldRecalcSchedule {
                        full,
                        priority: None,
                        candidates,
                        old_vdeps,
                        aux_retained_bytes,
                    });
                    self.account_recalc_retained()?;
                    if cancelled(&stop) {
                        phase = StepPhase::Cancelled;
                        break;
                    }
                    if explicit && expired() {
                        phase = StepPhase::Scheduling;
                        break;
                    }
                }
                if cancelled(&stop) {
                    phase = StepPhase::Cancelled;
                    break;
                }
                let mut held = self.resumable.as_mut().unwrap().held.take().unwrap();
                if let Some(options) = self.recalc_options.as_mut()
                    && !options.initial_priority.is_empty()
                {
                    let targets = std::mem::take(&mut options.initial_priority);
                    self.resumable.as_mut().unwrap().priority_targets = targets.clone();
                    let t = crate::instant::FzInstant::now();
                    held.priority = self.build_recalc_priority(&held.full.schedule, &targets)?;
                    self.resumable.as_mut().unwrap().priority_deferred = held.priority.is_none();
                    Self::note_slow_unit(
                        &mut longest,
                        SlowUnitKind::ScheduleBuild,
                        &[],
                        t.elapsed(),
                        &self.graph,
                    );
                    if explicit && expired() {
                        self.resumable.as_mut().unwrap().held = Some(held);
                        phase = StepPhase::Scheduling;
                        break;
                    }
                }
                self.begin_pass(&held.full.schedule);
                let mut completed_cycles = Vec::new();
                let mut barrier = false;
                let mut stopped = false;
                loop {
                    let priority = held.priority.as_ref().is_some_and(|p| !p.done());
                    let walk = if priority {
                        held.priority.as_mut().unwrap()
                    } else {
                        &mut held.full
                    };
                    if walk.done() {
                        break;
                    }
                    let index = walk.unit;
                    match walk.schedule.units[index] {
                        ScheduleUnit::Cycle(i) => {
                            let vertices = walk.schedule.unit_cycle(i);
                            let skip = vertices.iter().all(|v| {
                                self.resumable.as_ref().unwrap().installed.contains(v)
                                    && !self.graph.is_dirty(*v)
                            });
                            let t = crate::instant::FzInstant::now();
                            if !skip {
                                match self.handle_cycle_unit(
                                    vertices,
                                    collect.then_some(&mut delta),
                                    None,
                                    stop.cancel.as_ref().map(|t| &**t.as_flag()),
                                ) {
                                    Ok(n) => {
                                        computed += vertices.len();
                                        cycle_errors += usize::from(n > 0);
                                        completed_cycles.extend_from_slice(vertices);
                                    }
                                    Err(e) if e.kind == ExcelErrorKind::Cancelled => {
                                        self.restore_cancelled_recalc_cycle(vertices);
                                        phase = StepPhase::Cancelled;
                                        stopped = true;
                                        break;
                                    }
                                    Err(e) => return Err(e),
                                }
                                Self::note_slow_unit(
                                    &mut longest,
                                    SlowUnitKind::Cycle,
                                    vertices,
                                    t.elapsed(),
                                    &self.graph,
                                );
                                units += 1;
                            }
                            walk.completed += vertices.len();
                            walk.unit += 1;
                        }
                        ScheduleUnit::Layer(i) => {
                            let layer = walk.schedule.unit_layer(i);
                            let lo = walk.layer_offset;
                            let run_idx = layer.runs.partition_point(|r| (r.start as usize) < lo);
                            let run = layer.runs.get(run_idx).filter(|r| r.start as usize == lo);
                            let hi = run.map_or(lo + 1, |r| (r.start + r.len) as usize);
                            let vertices = &layer.vertices[lo..hi];
                            let skip = vertices.iter().all(|v| {
                                self.resumable.as_ref().unwrap().installed.contains(v)
                                    && !self.graph.is_dirty(*v)
                            });
                            if !skip {
                                let part = layer.sub_layer(lo, hi);
                                self.resource_checkpoint(part.vertices.len() as u64)?;
                                self.resumable
                                    .as_mut()
                                    .unwrap()
                                    .forget_dissolved_new_cycles(&part.vertices);
                                let t = crate::instant::FzInstant::now();
                                // Asking the ordinary evaluator for per-cell deltas
                                // disables its scalar bulk commit. Keep that path
                                // for scalar arithmetic families and conservatively
                                // report the whole installed run instead.
                                let run_delta = collect
                                    && run.is_some()
                                    && !self.graph.has_spill_anchors()
                                    && (self.resumable.as_ref().unwrap().pass_exact
                                        || self
                                            .graph
                                            .get_formula(vertices[0])
                                            .is_some_and(|ast| Self::recalc_scalar_ast(&ast)));
                                // Local run proof is independent of the bounded
                                // whole-pass progress proof on heterogeneous books.
                                match self.evaluate_layer_units_until(
                                    &part,
                                    (collect && !run_delta).then_some(&mut delta),
                                    None,
                                    None,
                                    buffer_layer_writes(&part),
                                    None,
                                ) {
                                    Ok(n) => computed += n,
                                    Err(e) if e.kind == ExcelErrorKind::Cancelled => {
                                        phase = StepPhase::Cancelled;
                                        stopped = true;
                                        break;
                                    }
                                    Err(e) => return Err(e),
                                }
                                if run_delta {
                                    let first = self.graph.get_cell_ref(vertices[0]).unwrap();
                                    let last =
                                        self.graph.get_cell_ref(*vertices.last().unwrap()).unwrap();
                                    delta.record_region(
                                        first.sheet_id,
                                        first.coord.row(),
                                        first.coord.col(),
                                        last.coord.row(),
                                        last.coord.col(),
                                    );
                                    // A reference can expose an array-valued input.
                                    // No old spill existed on admission to this
                                    // optimization; cover any newly installed extent.
                                    if self.graph.has_spill_anchors() {
                                        for &vertex in vertices {
                                            if let Some((a, b)) =
                                                self.graph.spill_extent_for_anchor(vertex)
                                            {
                                                delta.record_region(
                                                    a.sheet_id,
                                                    a.coord.row(),
                                                    a.coord.col(),
                                                    b.coord.row(),
                                                    b.coord.col(),
                                                );
                                            }
                                        }
                                    }
                                }
                                Self::note_slow_unit(
                                    &mut longest,
                                    if run.is_some() {
                                        SlowUnitKind::FamilyRun
                                    } else {
                                        SlowUnitKind::SingleCell
                                    },
                                    vertices,
                                    t.elapsed(),
                                    &self.graph,
                                );
                                units += 1;
                            }
                            walk.completed += hi - lo;
                            walk.layer_offset = hi;
                            if hi == layer.vertices.len() {
                                walk.layer_offset = 0;
                                walk.unit += 1;
                            }
                        }
                    }
                    if self.stop_after_unit(&walk.schedule, index) {
                        barrier = true;
                        break;
                    }
                    if self.resumable_should_stop(units, stop_at) {
                        stopped = true;
                        break;
                    }
                }
                let changed = self.changed_virtual_dep_vertices(&held.candidates, &held.old_vdeps);
                self.resource_checkpoint(0)?;
                let mut installed = std::mem::take(&mut self.resumable.as_mut().unwrap().installed);
                let pending =
                    self.freshness_finish_interrupted_pass(&completed_cycles, &mut installed);
                self.resumable.as_mut().unwrap().installed = installed;
                let (pending, fr5_replan) = pending?;
                barrier |= fr5_replan;
                for v in changed {
                    self.graph.set_dirty(v, true);
                }
                let full_done = held.full.done();
                self.resumable.as_mut().unwrap().pass_completed = held.full.completed;
                self.resumable.as_mut().unwrap().held = Some(held);
                if !pending && !self.graph.has_dirty_evaluation_vertices() {
                    phase = StepPhase::Finished;
                    break;
                }
                if phase == StepPhase::Cancelled {
                    break;
                }
                if barrier || full_done {
                    let state = self.resumable.as_mut().unwrap();
                    if state.replans >= 5 {
                        return Err(self.replan_exhausted_error(5, "dynamic dependency evaluation"));
                    }
                    state.replans += 1;
                    state.held = None;
                    if let Some(options) = &mut self.recalc_options {
                        options.initial_priority = state.priority_targets.clone();
                    }
                }
                if stopped {
                    break;
                }
            }
        }
        self.resource_checkpoint(0)?;
        let authority_revision = self.schedule_cache_authority_revision();
        let mutation_revision = self.inspection_mutation_revision();
        let s = self.resumable.as_mut().unwrap();
        s.function_epoch = function_epoch;
        s.provider_revision = provider_revision;
        s.mutation_revision = mutation_revision;
        s.source_mutation_generation = self.source_mutation_generation;
        s.topology_epoch = self.topology_epoch;
        if s.authority_revision != authority_revision && s.held.is_some() {
            s.held = None;
            if let Some(options) = &mut self.recalc_options
                && options.initial_priority.is_empty()
            {
                options.initial_priority = s.priority_targets.clone();
            }
        }
        s.authority_revision = authority_revision;
        if phase == StepPhase::Finished {
            s.priority_deferred = false;
            s.pass_completed = s.pass_scheduled;
        }
        s.computed_vertices += computed;
        s.cycle_errors += cycle_errors;
        s.elapsed += start.elapsed();
        let progress = self.recalc_progress();
        self.account_recalc_retained()?;
        let changed = Self::finish_step_delta(
            delta,
            self.recalc_options
                .as_ref()
                .map_or(1024, |o| o.max_delta_records),
        );
        let result = if phase == StepPhase::Finished {
            let state = self.resumable.take().unwrap();
            self.redirty_for_next_recalc();
            self.recalc_epoch = self.recalc_epoch.wrapping_add(1);
            Some(EvalResult {
                computed_vertices: state.computed_vertices,
                cycle_errors: state.cycle_errors,
                elapsed: state.elapsed,
            })
        } else {
            None
        };
        Ok(StepReport {
            phase,
            progress,
            step_computed_vertices: computed,
            step_elapsed: start.elapsed(),
            changed,
            longest_unit: longest,
            result,
        })
    }

    fn restore_cancelled_recalc_cycle(&mut self, vertices: &[VertexId]) {
        let mut key = vertices.to_vec();
        key.sort_unstable();
        let seeds = self
            .resumable
            .as_ref()
            .and_then(|s| s.cycle_seeds.get(&key))
            .cloned()
            .unwrap_or_default();
        for (vertex, value) in seeds {
            self.graph.update_vertex_value_ref(vertex, &value);
            self.mirror_vertex_value_to_overlay(vertex, &value);
            self.graph.set_dirty(vertex, true);
        }
    }

    fn account_recalc_retained(&mut self) -> Result<(), ExcelError> {
        let bytes = self.resumable.as_ref().map_or(0, |s| {
            let held = s.held.as_ref().map_or(0, |h| {
                h.full.retained_bytes
                    + h.priority.as_ref().map_or(0, |p| p.retained_bytes)
                    + h.aux_retained_bytes
            });
            held + s.installed.heap_bytes() as u64
                + s.new_cycle_members.heap_bytes() as u64
                + s.seed_retained_bytes
        });
        if let Some(ledger) = self.active_resource_ledger.as_mut() {
            ledger
                .account_recalc_retained(bytes)
                .map_err(crate::engine::ResourceLedgerError::into_excel_error)?;
        }
        Ok(())
    }

    fn recalc_total_exact(&self, candidates: &[VertexId]) -> bool {
        if self.graph.has_spill_anchors() {
            return false;
        }
        let mut scalar_templates = FxHashSet::default();
        for &vertex in candidates {
            if self.graph.is_dynamic(vertex) {
                return false;
            }
            let Some(view) = self.graph.formula_view(vertex) else {
                return false;
            };
            if scalar_templates.contains(&view.template) {
                continue;
            }
            // Exactness is optional, so bound proof work on heterogeneous
            // workbooks rather than clone one AST per formula at admission.
            if scalar_templates.len() >= 1024
                || !self
                    .graph
                    .get_formula(vertex)
                    .is_some_and(|ast| Self::recalc_scalar_ast(&ast))
            {
                return false;
            }
            scalar_templates.insert(view.template);
        }
        true
    }

    fn recalc_scalar_ast(ast: &ASTNode) -> bool {
        match &ast.node_type {
            ASTNodeType::Literal(v) => !matches!(v, LiteralValue::Array(_)),
            ASTNodeType::Reference { reference, .. } => {
                matches!(reference, ReferenceType::Cell { .. })
            }
            ASTNodeType::UnaryOp { expr, .. } => Self::recalc_scalar_ast(expr),
            ASTNodeType::BinaryOp { left, right, .. } => {
                Self::recalc_scalar_ast(left) && Self::recalc_scalar_ast(right)
            }
            _ => false,
        }
    }

    fn note_slow_unit(
        longest: &mut Option<SlowUnitReport>,
        kind: SlowUnitKind,
        vertices: &[VertexId],
        elapsed: std::time::Duration,
        graph: &DependencyGraph,
    ) {
        if longest.as_ref().is_none_or(|r| elapsed > r.elapsed) {
            let representative_cell = vertices
                .first()
                .and_then(|v| graph.get_cell_ref(*v))
                .and_then(|c| {
                    formualizer_common::PackedSheetCell::try_new(
                        c.sheet_id,
                        c.coord.row(),
                        c.coord.col(),
                    )
                });
            *longest = Some(SlowUnitReport {
                kind,
                representative_cell,
                vertex_count: vertices.len(),
                elapsed,
            });
        }
    }
    fn finish_step_delta(delta: DeltaCollector, cap: usize) -> StepDelta {
        let mut delta = delta.finish_target();
        let mut overflow_sheets = Vec::new();
        if delta.records.len() > cap {
            overflow_sheets.extend(delta.records.iter().map(|r| r.sheet_id()));
            overflow_sheets.sort_unstable();
            overflow_sheets.dedup();
            delta.records.clear();
        }
        StepDelta {
            delta,
            overflow_sheets,
        }
    }

    fn build_recalc_priority(
        &mut self,
        full: &crate::engine::scheduler::Schedule,
        targets: &[crate::engine::EvaluationTarget],
    ) -> Result<Option<Walk>, ExcelError> {
        use crate::engine::target_preparation::TargetProducer;
        let roots = self.resolve_target_producers(targets)?;
        let roots: Vec<_> = roots
            .into_iter()
            .filter_map(|r| match r {
                TargetProducer::Legacy(v) | TargetProducer::Symbol(v) => Some(v),
                TargetProducer::ValueOnly(_) => None,
            })
            .collect();
        let (mut vertices, _) = self.demand_subgraph(&roots)?;
        // Expand every touched family run and its precedent cone to a fixed
        // point. Cap priority expansion at 65,536 formulas; exceeding it
        // defers priority wholesale, never splits a run.
        loop {
            let mut selected = crate::engine::idset::DenseIdSet::default();
            selected.extend(vertices.iter().copied());
            let mut expanded = false;
            for layer in &full.layers {
                for run in &layer.runs {
                    let members =
                        &layer.vertices[run.start as usize..(run.start + run.len) as usize];
                    if members.iter().any(|v| selected.contains(v)) {
                        for &v in members {
                            expanded |= selected.insert(v);
                        }
                    }
                }
            }
            if selected.len() > 65_536 {
                return Ok(None);
            }
            if !expanded {
                return Ok(full
                    .restrict(&selected)
                    .map(|(s, _)| Walk::new(EvaluationSchedule::Owned(s))));
            }
            let seeds: Vec<_> = selected.iter().collect();
            let (closure, _) = self.demand_subgraph(&seeds)?;
            vertices = seeds;
            vertices.extend(closure);
        }
    }

    /// Zero-based inclusive viewport. Visits only coordinates inside it,
    /// including staged formulas; no workbook-wide dirty-set scan.
    pub fn has_pending_in_rect(&self, sheet: SheetId, r0: u32, c0: u32, r1: u32, c1: u32) -> bool {
        if r0 > r1 || c0 > c1 {
            return false;
        }
        for row in r0..=r1 {
            for col in c0..=c1 {
                if self.pending_at(sheet, row, col) {
                    return true;
                }
            }
        }
        false
    }
    pub fn pending_cells_in_rect(
        &self,
        sheet: SheetId,
        r0: u32,
        c0: u32,
        r1: u32,
        c1: u32,
    ) -> crate::engine::TargetEvalDelta {
        let mut delta = DeltaCollector::new(DeltaMode::Cells);
        if r0 <= r1 && c0 <= c1 {
            for row in r0..=r1 {
                for col in c0..=c1 {
                    if self.pending_at(sheet, row, col) {
                        delta.record_cell(sheet, row, col);
                    }
                }
            }
        }
        delta.finish_target()
    }
    fn pending_at(&self, sheet: SheetId, row: u32, col: u32) -> bool {
        let cell = self
            .graph
            .make_cell_ref(self.graph.sheet_name(sheet), row + 1, col + 1);
        self.graph
            .get_vertex_id_for_address(&cell)
            .is_some_and(|v| self.graph.is_live_formula_vertex(v) && self.graph.is_dirty(v))
            || self
                .get_staged_formula_text(self.graph.sheet_name(sheet), row + 1, col + 1)
                .is_some()
    }
}
