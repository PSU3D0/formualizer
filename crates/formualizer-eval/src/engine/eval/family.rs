//! Program 2 region-native execution (P2-M1, tier 1): a family node's cells
//! at one schedule layer evaluate as one unit through the node's template
//! (relocated by each member's offset from the template anchor, with the
//! member's literal slot row bound), instead of each cell's own AST.
//!
//! The per-cell path (`evaluate_vertex_immutable`) is the value oracle and
//! the fallback: dynamic templates (OFFSET/INDIRECT, observed reads), and
//! members whose literal row cannot be bound, evaluate per cell. Cycle
//! members never reach here (cycle units are not layers). Results that are
//! arrays go through the same effect planning (spills) as per-cell results.

use super::ComputedWriteBuffer;
use super::*;
use crate::engine::authority::store::Store;
use crate::engine::result_finalization::materialize_published_calc_result;
use crate::engine::scheduler::{Layer, LayerRun};
use crate::engine::template::canonical::LiteralSlotId;
use crate::interpreter::InterpreterParameterBindings;

/// One schedule unit of a layer: a single vertex or a family run.
#[derive(Clone, Copy, Debug)]
pub(super) enum LayerUnit {
    Cell(usize),
    Run(LayerRun),
}

/// The members of `unit`.
pub(super) fn unit_members(layer: &Layer, unit: LayerUnit) -> &[VertexId] {
    match unit {
        LayerUnit::Cell(i) => &layer.vertices[i..=i],
        LayerUnit::Run(run) => &layer.vertices[run.start as usize..(run.start + run.len) as usize],
    }
}

/// The units of `layer` in vertex order.
pub(super) fn layer_units(layer: &Layer) -> impl Iterator<Item = LayerUnit> + '_ {
    let mut i = 0usize;
    let mut runs = layer.runs.iter().peekable();
    std::iter::from_fn(move || {
        if i >= layer.vertices.len() {
            return None;
        }
        if let Some(run) = runs.peek()
            && run.start as usize == i
        {
            let run = **run;
            runs.next();
            i += run.len as usize;
            return Some(LayerUnit::Run(run));
        }
        let unit = LayerUnit::Cell(i);
        i += 1;
        Some(unit)
    })
}

/// Literal binding plan of a template: its literal nodes (pre-order) and,
/// when every literal node id is distinct, the node → slot map.
struct LiteralPlan {
    template_literals: smallvec::SmallVec<[crate::engine::arena::ValueRef; 4]>,
    slots_by_node: Option<FxHashMap<AstNodeId, LiteralSlotId>>,
}

impl LiteralPlan {
    fn new(ds: &crate::engine::arena::DataStore, template: AstNodeId, anchor: (u32, u32)) -> Self {
        let facts =
            crate::engine::authority::template::template_facts(ds, template, anchor.0, anchor.1);
        let nodes = crate::engine::authority::template::template_literal_nodes(ds, template);
        let mut map = FxHashMap::default();
        let mut distinct = nodes.len() == facts.literals.len();
        for (i, &n) in nodes.iter().enumerate() {
            if map.insert(n, LiteralSlotId(i as u16)).is_some() || i > u16::MAX as usize {
                distinct = false;
            }
        }
        Self {
            template_literals: facts.literals,
            slots_by_node: distinct.then_some(map),
        }
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// The value of one unit's vertices, in vertex order.
    pub(super) fn evaluate_unit_immutable(
        &self,
        layer: &Layer,
        unit: LayerUnit,
    ) -> smallvec::SmallVec<[(VertexId, LiteralValue); 1]> {
        self.evaluate_unit_immutable_memo(layer, unit, None)
    }

    /// [`Self::evaluate_unit_immutable`] for a chunk of a run whose memo is
    /// shared with the run's other chunks.
    pub(super) fn evaluate_unit_immutable_memo(
        &self,
        layer: &Layer,
        unit: LayerUnit,
        memo: Option<&super::memo::SharedMemo>,
    ) -> smallvec::SmallVec<[(VertexId, LiteralValue); 1]> {
        match unit {
            LayerUnit::Cell(i) => {
                let v = layer.vertices[i];
                let value = self
                    .evaluate_vertex_immutable(v)
                    .unwrap_or_else(LiteralValue::Error);
                smallvec::smallvec![(v, value)]
            }
            LayerUnit::Run(run) => {
                let members = &layer.vertices[run.start as usize..(run.start + run.len) as usize];
                let values = self.evaluate_run_immutable(run, members, memo);
                members.iter().copied().zip(values).collect()
            }
        }
    }

    /// The two parallel phases of a layer (units that do not / do read a
    /// compressed range), each with its vertices in unit order.
    pub(super) fn parallel_phases(&self, layer: &Layer) -> [(Vec<LayerUnit>, Vec<VertexId>); 2] {
        let mut phases: [(Vec<LayerUnit>, Vec<VertexId>); 2] = Default::default();
        for unit in layer_units(layer) {
            let phase = &mut phases[usize::from(self.unit_reads_compressed_range(layer, unit))];
            match unit {
                LayerUnit::Cell(i) => phase.1.push(layer.vertices[i]),
                LayerUnit::Run(run) => phase.1.extend_from_slice(
                    &layer.vertices[run.start as usize..(run.start + run.len) as usize],
                ),
            }
            phase.0.push(unit);
        }
        phases
    }

    /// Evaluate `units` on the current rayon pool; runs are split into
    /// bounded chunks so one long family does not serialize the layer.
    pub(super) fn evaluate_units_parallel(
        &self,
        layer: &Layer,
        units: &[LayerUnit],
        cancel_flag: Option<&AtomicBool>,
        min_chunk: u32,
    ) -> Result<Vec<(VertexId, LiteralValue)>, ExcelError> {
        use rayon::prelude::*;
        // Enough tasks to balance the pool (a run of expensive members, e.g.
        // SUMIFS over a fact table, must not collapse into a few tasks).
        let total: usize = units
            .iter()
            .map(|u| match u {
                LayerUnit::Cell(_) => 1,
                LayerUnit::Run(run) => run.len as usize,
            })
            .sum();
        let threads = rayon::current_num_threads().max(1);
        // At least `min_chunk` members per task: for cheap members a
        // one-member chunk loses the run (literal plan, lift, memo) for
        // nothing; expensive members pass 1.
        let run_chunk = ((total / (threads * 8)) as u32).clamp(min_chunk.max(1), 256);
        // A split run's chunks share one memo (see `memo.rs`).
        let mut memos: Vec<super::memo::SharedMemo> = Vec::new();
        let mut split: Vec<(LayerUnit, Option<usize>)> = Vec::with_capacity(units.len());
        for &unit in units {
            match unit {
                LayerUnit::Run(run) if run.len > run_chunk => {
                    let mut memo = super::memo::SharedMemo::default();
                    memo.run_len = run.len;
                    memos.push(memo);
                    let memo = Some(memos.len() - 1);
                    let mut k = 0;
                    while k < run.len {
                        let len = run_chunk.min(run.len - k);
                        split.push((
                            LayerUnit::Run(LayerRun {
                                start: run.start + k,
                                len,
                                row0: run.row0 + k,
                                ..run
                            }),
                            memo,
                        ));
                        k += len;
                    }
                }
                other => split.push((other, None)),
            }
        }
        let chunks: Result<Vec<_>, ExcelError> = split
            .par_iter()
            .map(|&(unit, memo)| {
                if cancel_flag.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
                    return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                        "Parallel evaluation cancelled during execution".to_string(),
                    ));
                }
                Ok(self.evaluate_unit_immutable_memo(layer, unit, memo.map(|k| &memos[k])))
            })
            .collect();
        Ok(chunks?.into_iter().flatten().collect())
    }

    /// Whether a unit reads a compressed range (the per-cell flush rule,
    /// applied to the unit before it evaluates).
    pub(super) fn unit_reads_compressed_range(&self, layer: &Layer, unit: LayerUnit) -> bool {
        let first = match unit {
            LayerUnit::Cell(i) => layer.vertices[i],
            LayerUnit::Run(run) => layer.vertices[run.start as usize],
        };
        self.graph.reads_compressed_range(first)
    }

    fn evaluate_members_per_cell(&self, members: &[VertexId]) -> Vec<LiteralValue> {
        members
            .iter()
            .map(|&v| {
                self.evaluate_vertex_immutable(v)
                    .unwrap_or_else(LiteralValue::Error)
            })
            .collect()
    }

    /// Evaluate the members of a family run (tier 1): one template, one
    /// literal plan and one sheet resolution for the run.
    pub(super) fn evaluate_run_immutable(
        &self,
        run: LayerRun,
        members: &[VertexId],
        shared_memo: Option<&super::memo::SharedMemo>,
    ) -> Vec<LiteralValue> {
        if !self.config.family_execution {
            return self.evaluate_members_per_cell(members);
        }
        let Ok(store) = self.graph.authority_plan_store() else {
            return self.evaluate_members_per_cell(members);
        };
        if members.iter().any(|&v| self.graph.is_dynamic(v)) {
            return self.evaluate_members_per_cell(members);
        }
        let (template, anchor) = store.owner_template(run.owner);
        let ds = self.graph.data_store();
        let reg = self.graph.sheet_reg();
        if let Some(values) = self.try_run_kernel(run, members, ds, template, anchor) {
            return values;
        }
        let literals = LiteralPlan::new(ds, template, anchor);
        if let Some(values) =
            self.try_run_lift(run, members, store, ds, template, anchor, &literals)
        {
            return values;
        }
        let sheet_name = self.graph.sheet_name(run.sheet);
        let col_delta = i64::from(run.col) - i64::from(anchor.1);
        let mut out = Vec::with_capacity(members.len());
        let mut bound: Vec<LiteralValue> = Vec::new();
        // P2-M4 memo: members repeating another's varying arguments reuse
        // its result (see `memo.rs`).
        let memo_plan = (self.config.family_kernels && members.len() > 1)
            .then(|| super::memo::MemoPlan::plan(self, ds, template))
            .flatten()
            .filter(|_| !members.iter().any(|&v| self.graph.is_volatile(v)));
        let mut memo = super::memo::RunMemo::new(shared_memo);
        // P2-M4 criteria kernel: SUMIF(S)/COUNTIF(S)/AVERAGEIF(S) over
        // invariant ranges index them once per run (shared by the run's
        // parallel chunks); see `criteria.rs`.
        let criteria_plan = (self.config.family_kernels
            && (members.len() > 1 || shared_memo.is_some()))
        .then(|| super::criteria::CriteriaPlan::plan(self, ds, template))
        .flatten()
        .filter(|_| !members.iter().any(|&v| self.graph.is_volatile(v)));
        let local_index;
        let criteria_index = match &criteria_plan {
            Some(plan) => {
                let run_len = shared_memo.map_or(run.len, |shared| shared.run_len);
                let build = || self.build_criteria_index(plan, ds, sheet_name, run_len);
                match shared_memo {
                    Some(shared) => shared.criteria.get_or_init(build).as_ref(),
                    None => {
                        local_index = build();
                        local_index.as_ref()
                    }
                }
            }
            None => None,
        };
        // Program 3: the template's run-invariant calls (a lookup's
        // invariant column index, a name's MATCH) are computed once, at the
        // first member with the template's literals, and bound for every
        // such member.
        let invariant_nodes = if self.config.family_kernels
            && (members.len() > 1 || shared_memo.is_some())
        {
            let names =
                |name: &str, as_arg: bool| self.lift_name_is_run_constant(run.sheet, name, as_arg);
            super::lift::LiftProgram::invariant_subtrees(self, ds, template, &names)
        } else {
            Vec::new()
        };
        let mut local_invariant: Option<crate::interpreter::InvariantValues> = None;
        for (i, &v) in members.iter().enumerate() {
            let row = run.row0 + i as u32;
            #[cfg(debug_assertions)]
            self.debug_check_member(store, v, template, anchor, (run.sheet, row, run.col));
            let Some(cell_ref) = self.graph.get_cell_ref(v) else {
                out.push(
                    self.evaluate_vertex_immutable(v)
                        .unwrap_or_else(LiteralValue::Error),
                );
                continue;
            };
            let bindings = match Self::member_bindings(
                store,
                ds,
                &literals,
                (run.sheet, row, run.col),
                &mut bound,
            ) {
                Some(b) => b,
                None => {
                    out.push(
                        self.evaluate_vertex_immutable(v)
                            .unwrap_or_else(LiteralValue::Error),
                    );
                    continue;
                }
            };
            let row_delta = i64::from(row) - i64::from(anchor.0);
            let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
            let compute =
                || self.run_invariant_values(&interpreter, &invariant_nodes, row_delta, col_delta);
            let invariant_values: Option<&crate::interpreter::InvariantValues> =
                if bindings || invariant_nodes.is_empty() {
                    None
                } else if let Some(shared) = shared_memo {
                    Some(shared.invariant.get_or_init(compute))
                } else {
                    if local_invariant.is_none() {
                        local_invariant = Some(compute());
                    }
                    local_invariant.as_ref()
                };
            let interpreter = match (bindings, &literals.slots_by_node) {
                (true, Some(map)) => {
                    interpreter.with_parameter_bindings(InterpreterParameterBindings {
                        literal_slots_by_node: map,
                        literal_values: &bound,
                        invariant_values: None,
                    })
                }
                (false, _) if invariant_values.is_some_and(|m| !m.is_empty()) => {
                    #[cfg(test)]
                    self.invariant_bound_members_for_test
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    static NO_SLOTS: std::sync::LazyLock<FxHashMap<AstNodeId, LiteralSlotId>> =
                        std::sync::LazyLock::new(FxHashMap::default);
                    interpreter.with_parameter_bindings(InterpreterParameterBindings {
                        literal_slots_by_node: &NO_SLOTS,
                        literal_values: &[],
                        invariant_values,
                    })
                }
                _ => interpreter,
            };
            #[cfg(test)]
            self.family_members_for_test
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let (Some(plan), Some(index)) = (&criteria_plan, criteria_index)
                && let Some(value) = self.criteria_member(
                    plan,
                    index,
                    &interpreter,
                    ds,
                    sheet_name,
                    row_delta,
                    col_delta,
                )
            {
                // Debug builds: the kernel's result is the walk's.
                #[cfg(debug_assertions)]
                {
                    let walked = interpreter
                        .evaluate_arena_ast_with_offset(template, row_delta, col_delta, ds, reg)
                        .map(|cv| {
                            let format = cv.format_id();
                            (
                                format,
                                materialize_published_calc_result(
                                    cv,
                                    self.config.spill.max_spill_cells,
                                ),
                            )
                        });
                    match walked {
                        Ok((format, walked)) => assert!(
                            same_value(&walked, &value) && format.is_none(),
                            "criteria kernel differs from the walk at {cell_ref:?}: {value:?} vs {walked:?} {format:?}"
                        ),
                        Err(e) => panic!(
                            "criteria kernel result where the walk errs at {cell_ref:?}: {e:?}"
                        ),
                    }
                }
                #[cfg(test)]
                self.criteria_kernel_members_for_test
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.record_derived_format_at(cell_ref, None);
                out.push(crate::engine::result_finalization::finalize_formula_result(
                    value,
                ));
                continue;
            }
            let key = match &memo_plan {
                Some(plan) if memo.active() => plan
                    .key_args
                    .iter()
                    .map(|&arg| {
                        let cv = interpreter
                            .evaluate_arena_ast_with_offset(arg, row_delta, col_delta, ds, reg)
                            .ok()?;
                        let format = cv.format_id();
                        Some((super::memo::KeyValue::of(cv.into_literal())?, format))
                    })
                    .collect::<Option<super::memo::MemoKey>>(),
                _ => None,
            };
            let result = match key.as_ref().and_then(|key| memo.get(key)) {
                Some(hit) => {
                    // Debug builds: a reused result is the walk's result.
                    #[cfg(debug_assertions)]
                    {
                        let walked = interpreter
                            .evaluate_arena_ast_with_offset(template, row_delta, col_delta, ds, reg)
                            .map(|cv| {
                                let format = cv.format_id();
                                (
                                    format,
                                    materialize_published_calc_result(
                                        cv,
                                        self.config.spill.max_spill_cells,
                                    ),
                                )
                            });
                        match walked {
                            Ok((format, value)) => {
                                assert!(
                                    same_value(&value, &hit.0) && format == hit.1,
                                    "memoized result differs from the walk at {cell_ref:?}: {hit:?} vs {value:?} {format:?}"
                                );
                            }
                            Err(e) => {
                                panic!("memoized result where the walk errs at {cell_ref:?}: {e:?}")
                            }
                        }
                    }
                    #[cfg(test)]
                    self.memo_hits_for_test
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(hit)
                }
                None => interpreter
                    .evaluate_arena_ast_with_offset(template, row_delta, col_delta, ds, reg)
                    .map(|cv| {
                        let format = cv.format_id();
                        let value = materialize_published_calc_result(
                            cv,
                            self.config.spill.max_spill_cells,
                        );
                        if let Some(key) = key {
                            memo.insert(key, (value.clone(), format));
                        }
                        (value, format)
                    }),
            };
            let value = result
                .map(|(value, format)| {
                    self.record_derived_format_at(cell_ref, format);
                    crate::engine::result_finalization::finalize_formula_result(value)
                })
                .unwrap_or_else(LiteralValue::Error);
            out.push(value);
        }
        out
    }

    /// P2-M3: the elementwise lift for the whole run, when the template is
    /// operators over cell references and literals and every member has
    /// the template's literals.
    #[allow(clippy::too_many_arguments)]
    fn try_run_lift(
        &self,
        run: LayerRun,
        members: &[VertexId],
        store: &Store,
        ds: &crate::engine::arena::DataStore,
        template: AstNodeId,
        anchor: (u32, u32),
        literals: &LiteralPlan,
    ) -> Option<Vec<LiteralValue>> {
        if !self.config.family_lift || members.len() < 2 {
            return None;
        }
        let names =
            |name: &str, as_arg: bool| self.lift_name_is_run_constant(run.sheet, name, as_arg);
        let program = super::lift::LiftProgram::compile(self, ds, template, &names)?;
        let mut bound = Vec::new();
        let mut cells = Vec::with_capacity(members.len());
        for (i, &v) in members.iter().enumerate() {
            let row = run.row0 + i as u32;
            #[cfg(debug_assertions)]
            self.debug_check_member(store, v, template, anchor, (run.sheet, row, run.col));
            let cell_ref = self.graph.get_cell_ref(v)?;
            if Self::member_bindings(store, ds, literals, (run.sheet, row, run.col), &mut bound)? {
                return None;
            }
            cells.push(cell_ref);
        }
        let lifted = self.evaluate_run_lifted(&program, run, anchor, members.len())?;
        #[cfg(test)]
        {
            self.family_members_for_test
                .fetch_add(members.len() as u64, std::sync::atomic::Ordering::Relaxed);
            self.lifted_members_for_test
                .fetch_add(members.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        let reg = self.graph.sheet_reg();
        let sheet_name = self.graph.sheet_name(run.sheet);
        let col_delta = i64::from(run.col) - i64::from(anchor.1);
        let values: Vec<LiteralValue> = lifted
            .into_iter()
            .zip(&cells)
            .enumerate()
            .map(|(i, (result, &cell))| {
                // Members a builtin could not take on lanes: the walk (the
                // member has the template's literals, so no bindings).
                let result = result.unwrap_or_else(|| {
                    let row_delta = i64::from(run.row0 + i as u32) - i64::from(anchor.0);
                    Interpreter::new_with_cell(self, sheet_name, cell)
                        .evaluate_arena_ast_with_offset(template, row_delta, col_delta, ds, reg)
                        .map(|cv| {
                            let format = cv.format_id();
                            (
                                materialize_published_calc_result(
                                    cv,
                                    self.config.spill.max_spill_cells,
                                ),
                                format,
                            )
                        })
                });
                match result {
                    Ok((value, format)) => {
                        self.record_derived_format_at(cell, format);
                        crate::engine::result_finalization::finalize_formula_result(value)
                    }
                    Err(e) => LiteralValue::Error(e),
                }
            })
            .collect();
        // Debug builds: every lifted member equals the per-cell path, value
        // and recorded format (the oracle records its own format; it must
        // be the one the lift recorded).
        #[cfg(debug_assertions)]
        for ((&v, value), &cell) in members.iter().zip(&values).zip(&cells) {
            let lifted_format = self.derived_formats.get(&cell);
            let oracle = self
                .evaluate_vertex_immutable(v)
                .unwrap_or_else(LiteralValue::Error);
            assert!(
                same_value(&oracle, value),
                "lifted value differs from the per-cell path at {cell:?}: {value:?} vs {oracle:?}"
            );
            let oracle_format = self.derived_formats.get(&cell);
            assert_eq!(
                lifted_format, oracle_format,
                "lifted format differs from the per-cell path at {cell:?}"
            );
        }
        Some(values)
    }

    /// Program 3 chain unit: the members of `run` in row order, each
    /// reading the one above (see `Engine::evaluate_chain_lifted`). `None`
    /// leaves the run to the per-cell path, member by member.
    pub(super) fn try_chain_lift(&self, run: LayerRun, members: &[VertexId]) -> Option<Vec<f64>> {
        if !self.config.family_execution || !self.config.family_lift || members.len() < 2 {
            return None;
        }
        let store = self.graph.authority_plan_store().ok()?;
        if members
            .iter()
            .any(|&v| self.graph.is_dynamic(v) || self.graph.is_volatile(v))
        {
            return None;
        }
        let (template, anchor) = store.owner_template(run.owner);
        let ds = self.graph.data_store();
        let literals = LiteralPlan::new(ds, template, anchor);
        let names =
            |name: &str, as_arg: bool| self.lift_name_is_run_constant(run.sheet, name, as_arg);
        let program = super::lift::LiftProgram::compile(self, ds, template, &names)?;
        // Every member has the template's literal row (a formula cell's
        // authority id is its vertex id).
        #[cfg(debug_assertions)]
        for (i, &v) in members.iter().enumerate() {
            let row = run.row0 + i as u32;
            self.debug_check_member(store, v, template, anchor, (run.sheet, row, run.col));
            debug_assert_eq!(store.ids().id_of((run.sheet, row, run.col)), Some(v.0));
        }
        if !literals.template_literals.is_empty() {
            let mut page = None;
            for &v in members {
                let row = store.slots().get_cached(v.0, &mut page)?;
                if row != literals.template_literals.as_slice()
                    && !crate::engine::graph::authority_host::literal_rows_equal(
                        ds,
                        row,
                        &literals.template_literals,
                    )
                {
                    return None;
                }
            }
        }
        let lifted = self.evaluate_chain_lifted(&program, run, anchor, members.len())?;
        #[cfg(test)]
        self.chained_members_for_test
            .fetch_add(members.len() as u64, std::sync::atomic::Ordering::Relaxed);
        // Every member is a clean finite number (no format).
        for i in 0..lifted.len() as u32 {
            let cell = CellRef::new(run.sheet, Coord::new(run.row0 + i, run.col, true, true));
            self.record_derived_format_at(cell, None);
        }
        Some(lifted)
    }

    /// The values of run-invariant calls `nodes`, evaluated for one member
    /// (the walk's own evaluation); calls whose value is not a scalar are
    /// left out (the walk evaluates them per member).
    fn run_invariant_values(
        &self,
        interpreter: &Interpreter<'_>,
        nodes: &[AstNodeId],
        row_delta: i64,
        col_delta: i64,
    ) -> crate::interpreter::InvariantValues {
        let ds = self.graph.data_store();
        let reg = self.graph.sheet_reg();
        let mut out = crate::interpreter::InvariantValues::default();
        for &node in nodes {
            let value =
                interpreter.evaluate_arena_ast_with_offset(node, row_delta, col_delta, ds, reg);
            let entry = match value {
                Ok(crate::traits::CalcValue::Scalar(v)) => (v, None),
                Ok(crate::traits::CalcValue::AnnotatedScalar(v, f)) => (v, Some(f)),
                _ => continue,
            };
            if matches!(entry.0, LiteralValue::Array(_)) {
                continue;
            }
            out.insert(node, entry);
        }
        out
    }

    /// Whether defined name `name`, read from a run on `sheet`, is one value
    /// for every member (the lift evaluates it once): a cell, a literal or
    /// a non-volatile, non-dynamic formula; a range only as a call's direct
    /// argument (`as_arg`: elsewhere it is intersected with the member).
    fn lift_name_is_run_constant(&self, sheet: SheetId, name: &str, as_arg: bool) -> bool {
        use crate::engine::named_range::NamedDefinition as D;
        let Some(named) = self.graph.resolve_name_entry(name, sheet) else {
            return false;
        };
        match &named.definition {
            D::Cell(_) | D::Literal(_) => true,
            D::Range(_) => as_arg,
            D::Formula { .. } => {
                !self.graph.is_volatile(named.vertex) && !self.graph.is_dynamic(named.vertex)
            }
        }
    }

    /// Tier 3: a range kernel for the whole run, when the template has one.
    fn try_run_kernel(
        &self,
        run: LayerRun,
        members: &[VertexId],
        ds: &crate::engine::arena::DataStore,
        template: AstNodeId,
        anchor: (u32, u32),
    ) -> Option<Vec<LiteralValue>> {
        if !self.config.family_kernels {
            return None;
        }
        let kernel = super::kernels::AggregateKernel::plan(self, ds, template, run.sheet)?;
        let values = kernel.run(self, anchor, run.row0, members.len(), run.col)?;
        for (i, &v) in members.iter().enumerate() {
            let cell = crate::reference::CellRef::new(
                run.sheet,
                Coord::new(run.row0 + i as u32, run.col, true, true),
            );
            self.record_derived_format_at(cell, None);
            #[cfg(debug_assertions)]
            {
                let oracle = self
                    .evaluate_vertex_immutable(v)
                    .unwrap_or_else(LiteralValue::Error);
                assert!(
                    same_value(&oracle, &values[i]),
                    "family kernel differs from the per-cell path at {cell:?}: {:?} vs {:?}",
                    values[i],
                    oracle
                );
            }
            #[cfg(not(debug_assertions))]
            let _ = v;
        }
        #[cfg(test)]
        self.family_members_for_test
            .fetch_add(members.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Some(values)
    }

    /// `Some(false)`: the member's literals are the template's (no binding).
    /// `Some(true)`: bind `bound` (filled here). `None`: evaluate per cell.
    fn member_bindings(
        store: &Store,
        ds: &crate::engine::arena::DataStore,
        literals: &LiteralPlan,
        cell: (u16, u32, u32),
        bound: &mut Vec<LiteralValue>,
    ) -> Option<bool> {
        if literals.template_literals.is_empty() {
            return Some(false);
        }
        let id = store.ids().id_of(cell)?;
        let row = store.slots().get(id)?;
        if row == literals.template_literals.as_slice()
            || crate::engine::graph::authority_host::literal_rows_equal(
                ds,
                row,
                &literals.template_literals,
            )
        {
            return Some(false);
        }
        if literals.slots_by_node.is_none() || row.len() != literals.template_literals.len() {
            return None;
        }
        bound.clear();
        bound.extend(row.iter().map(|&r| ds.retrieve_value(r)));
        Some(true)
    }

    /// Debug builds: the member's own formula is the template relocated to
    /// it with its literal row (the `formula_view` contract).
    #[cfg(debug_assertions)]
    fn debug_check_member(
        &self,
        store: &Store,
        v: VertexId,
        template: AstNodeId,
        anchor: (u32, u32),
        cell: (u16, u32, u32),
    ) {
        use crate::engine::authority::template::template_facts;
        // A compressed member was checked against its template when it
        // was compressed.
        let Some(own) = self.graph.get_formula_id(v) else {
            return;
        };
        let ds = self.graph.data_store();
        let mine = template_facts(ds, own, cell.1, cell.2);
        let tmpl = template_facts(ds, template, anchor.0, anchor.1);
        assert_eq!(
            mine.tokens, tmpl.tokens,
            "family member {cell:?} is not its owner's template relocated"
        );
        let values = |refs: &[crate::engine::arena::ValueRef]| {
            refs.iter()
                .map(|&r| ds.retrieve_value(r))
                .collect::<Vec<_>>()
        };
        if let Some(row) = store.ids().id_of(cell).and_then(|id| store.slots().get(id)) {
            assert_eq!(
                values(row),
                values(&mine.literals),
                "family member {cell:?}: slot row differs from its own literals"
            );
        }
    }
}

#[cfg(test)]
impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// Family members walked with run-invariant calls bound so far.
    pub(crate) fn invariant_bound_members_for_test(&self) -> u64 {
        self.invariant_bound_members_for_test
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Members evaluated through a family template so far.
    pub(crate) fn family_members_for_test(&self) -> u64 {
        self.family_members_for_test
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Family members that reused a memoized result so far.
    pub(crate) fn memo_hits_for_test(&self) -> u64 {
        self.memo_hits_for_test
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Members evaluated through the elementwise lift so far.
    pub(crate) fn lifted_members_for_test(&self) -> u64 {
        self.lifted_members_for_test
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn chained_members_for_test(&self) -> u64 {
        self.chained_members_for_test
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Members evaluated through the criteria kernel so far.
    pub(crate) fn criteria_kernel_members_for_test(&self) -> u64 {
        self.criteria_kernel_members_for_test
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Operand elements the lift read as clean typed-lane numbers so far.
    pub(crate) fn lane_clean_reads_for_test(&self) -> u64 {
        self.lane_clean_reads_for_test
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Value identity for differential checks: numbers by bits (NaN as a
/// class), errors by kind, everything else by equality.
#[cfg(any(test, debug_assertions))]
pub(crate) fn same_value(a: &LiteralValue, b: &LiteralValue) -> bool {
    match (a, b) {
        (LiteralValue::Number(x), LiteralValue::Number(y)) => {
            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
        }
        (LiteralValue::Error(x), LiteralValue::Error(y)) => x.kind == y.kind,
        _ => a == b,
    }
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// Parallel apply: commit the runs of `units` (results in unit order)
    /// as units when no result is an array (arrays must apply first), and
    /// return the results left for the per-vertex path (in order) and the
    /// number committed. Cells are distinct, so the relative order of run
    /// and single-cell overlay writes is not observable.
    pub(super) fn commit_parallel_runs(
        &mut self,
        layer: &Layer,
        units: &[LayerUnit],
        results: Vec<(VertexId, LiteralValue)>,
        delta_active: bool,
        computed_writes: &mut ComputedWriteBuffer,
    ) -> Result<(Vec<(VertexId, LiteralValue)>, usize), ExcelError> {
        if delta_active
            || !units.iter().any(|u| matches!(u, LayerUnit::Run(_)))
            || results
                .iter()
                .any(|(_, v)| matches!(v, LiteralValue::Array(_)))
        {
            return Ok((results, 0));
        }
        let mut rest = Vec::with_capacity(results.len());
        let mut committed = 0usize;
        let mut at = 0usize;
        for &unit in units {
            match unit {
                LayerUnit::Cell(_) => {
                    rest.push(results[at].clone());
                    at += 1;
                }
                LayerUnit::Run(run) => {
                    let n = run.len as usize;
                    let members =
                        &layer.vertices[run.start as usize..(run.start + run.len) as usize];
                    let values = &results[at..at + n];
                    debug_assert!(values.iter().zip(members).all(|(r, &m)| r.0 == m));
                    if self.commit_run_scalars(
                        run,
                        members,
                        values,
                        false,
                        Some(computed_writes),
                    )? {
                        committed += n;
                    } else {
                        rest.extend_from_slice(values);
                    }
                    at += n;
                }
            }
        }
        debug_assert_eq!(at, results.len());
        Ok((rest, committed))
    }

    /// Commit a run's results as one unit, when nothing needs the per-vertex
    /// effect path: every value is a scalar, no delta is collected, no
    /// reader is stale, no spill is pending and no member anchors a spill.
    /// Returns `false` (having done nothing) otherwise. Effects are those of
    /// `plan_scalar_effects` + `apply_write_cell` per member, in order.
    /// [`Self::commit_run_scalars`] for a run whose every result is the
    /// number `values[i]` (the chain lift): no per-member value vector.
    pub(super) fn commit_run_numbers(
        &mut self,
        run: LayerRun,
        members: &[VertexId],
        values: &[f64],
        delta_active: bool,
        computed_writes: Option<&mut ComputedWriteBuffer>,
    ) -> Result<bool, ExcelError> {
        if delta_active
            || !self.blocked_pending_spills.is_empty()
            || !self.freshness_group_commit_ok()
            || (self.graph.has_spill_anchors()
                && members.iter().any(|&v| self.graph.is_spill_anchor(v)))
        {
            return Ok(false);
        }
        self.freshness_mark_committed_group(members);
        for (&v, &x) in members.iter().zip(values) {
            self.graph
                .update_vertex_value_ref(v, &LiteralValue::Number(x));
        }
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
            || self.computed_overlay_mirroring_disabled
        {
            return Ok(true);
        }
        let sheet_name = self.graph.sheet_name(run.sheet).to_string();
        let date_system = self.arrow_sheet_date_system(&sheet_name);
        let overlay =
            |x: f64| Self::literal_to_overlay_value(&LiteralValue::Number(x), date_system);
        match computed_writes {
            Some(buffer) => {
                // The members' formats are clear (`try_chain_lift`).
                let entries: Vec<(OverlayValue, Option<crate::format::FormatId>)> =
                    values.iter().map(|&x| (overlay(x), None)).collect();
                buffer.push_column_run(run.sheet, run.row0, run.col, entries);
                if self.should_flush_computed_write_buffer(buffer) {
                    self.flush_computed_write_buffer(buffer)?;
                }
            }
            None => {
                for (i, &x) in values.iter().enumerate() {
                    let row = run.row0 + i as u32;
                    self.write_computed_overlay_value_0based(&sheet_name, row, run.col, overlay(x));
                    self.write_computed_overlay_format_0based(&sheet_name, row, run.col, None);
                }
            }
        }
        Ok(true)
    }

    pub(super) fn commit_run_scalars(
        &mut self,
        run: LayerRun,
        members: &[VertexId],
        values: &[(VertexId, LiteralValue)],
        delta_active: bool,
        computed_writes: Option<&mut ComputedWriteBuffer>,
    ) -> Result<bool, ExcelError> {
        if delta_active
            || !self.blocked_pending_spills.is_empty()
            || values
                .iter()
                .any(|(_, v)| matches!(v, LiteralValue::Array(_)))
            || !self.freshness_group_commit_ok()
            || (self.graph.has_spill_anchors()
                && members.iter().any(|&v| self.graph.is_spill_anchor(v)))
        {
            return Ok(false);
        }
        self.freshness_mark_committed_group(members);
        for (v, value) in values {
            self.graph.update_vertex_value_ref(*v, value);
        }
        if !(self.config.arrow_storage_enabled
            && self.config.delta_overlay_enabled
            && self.config.write_formula_overlay_enabled)
            || self.computed_overlay_mirroring_disabled
        {
            return Ok(true);
        }
        let sheet_name = self.graph.sheet_name(run.sheet).to_string();
        let date_system = self.arrow_sheet_date_system(&sheet_name);
        match computed_writes {
            Some(buffer) => {
                // One block write (the plan groups it per chunk segment, not
                // per cell); the formats read under one lock.
                let formats = &self.derived_formats;
                let entries: Vec<(OverlayValue, Option<crate::format::FormatId>)> = values
                    .iter()
                    .enumerate()
                    .map(|(i, (_, value))| {
                        let format_id = formats.get(&CellRef::new(
                            run.sheet,
                            Coord::new(run.row0 + i as u32, run.col, true, true),
                        ));
                        (
                            Self::literal_to_overlay_value(value, date_system),
                            format_id,
                        )
                    })
                    .collect();
                buffer.push_column_run(run.sheet, run.row0, run.col, entries);
                if self.should_flush_computed_write_buffer(buffer) {
                    self.flush_computed_write_buffer(buffer)?;
                }
            }
            None => {
                // The format lane follows the values (as on the buffered path).
                let formats: Vec<Option<crate::format::FormatId>> = (0..values.len() as u32)
                    .map(|i| {
                        self.derived_formats.get(&CellRef::new(
                            run.sheet,
                            Coord::new(run.row0 + i, run.col, true, true),
                        ))
                    })
                    .collect();
                for (i, (_, value)) in values.iter().enumerate() {
                    let row = run.row0 + i as u32;
                    let ov = Self::literal_to_overlay_value(value, date_system);
                    self.write_computed_overlay_value_0based(&sheet_name, row, run.col, ov);
                    self.write_computed_overlay_format_0based(
                        &sheet_name,
                        row,
                        run.col,
                        formats[i],
                    );
                }
            }
        }
        Ok(true)
    }
}
