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

use super::*;
use crate::engine::authority::store::Store;
use crate::engine::scheduler::{Layer, LayerRun};
use crate::engine::template::canonical::LiteralSlotId;
use crate::interpreter::InterpreterParameterBindings;

/// One schedule unit of a layer: a single vertex or a family run.
#[derive(Clone, Copy, Debug)]
pub(super) enum LayerUnit {
    Cell(usize),
    Run(LayerRun),
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
                let values = self.evaluate_run_immutable(run, members);
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
    ) -> Result<Vec<(VertexId, LiteralValue)>, ExcelError> {
        use rayon::prelude::*;
        const RUN_CHUNK: u32 = 256;
        let mut split: Vec<LayerUnit> = Vec::with_capacity(units.len());
        for &unit in units {
            match unit {
                LayerUnit::Run(run) if run.len > RUN_CHUNK => {
                    let mut k = 0;
                    while k < run.len {
                        let len = RUN_CHUNK.min(run.len - k);
                        split.push(LayerUnit::Run(LayerRun {
                            start: run.start + k,
                            len,
                            row0: run.row0 + k,
                            ..run
                        }));
                        k += len;
                    }
                }
                other => split.push(other),
            }
        }
        let chunks: Result<Vec<_>, ExcelError> = split
            .par_iter()
            .map(|&unit| {
                if cancel_flag.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
                    return Err(ExcelError::new(ExcelErrorKind::Cancelled).with_message(
                        "Parallel evaluation cancelled during execution".to_string(),
                    ));
                }
                Ok(self.evaluate_unit_immutable(layer, unit))
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
        let literals = LiteralPlan::new(ds, template, anchor);
        let sheet_name = self.graph.sheet_name(run.sheet);
        let col_delta = i64::from(run.col) - i64::from(anchor.1);
        let mut out = Vec::with_capacity(members.len());
        let mut bound: Vec<LiteralValue> = Vec::new();
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
            let interpreter = Interpreter::new_with_cell(self, sheet_name, cell_ref);
            let interpreter = match (bindings, &literals.slots_by_node) {
                (true, Some(map)) => {
                    interpreter.with_parameter_bindings(InterpreterParameterBindings {
                        literal_slots_by_node: map,
                        literal_values: &bound,
                    })
                }
                _ => interpreter,
            };
            #[cfg(test)]
            self.family_members_for_test
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let row_delta = i64::from(row) - i64::from(anchor.0);
            let value = interpreter
                .evaluate_arena_ast_with_offset(template, row_delta, col_delta, ds, reg)
                .map(|cv| {
                    let format = cv.format_id();
                    self.derived_format_results
                        .write()
                        .unwrap()
                        .insert(v, format);
                    self.record_derived_format_at(cell_ref, format);
                    crate::engine::result_finalization::finalize_formula_result(cv.into_literal())
                })
                .unwrap_or_else(LiteralValue::Error);
            out.push(value);
        }
        out
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
        if row == literals.template_literals.as_slice() {
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
    /// Members evaluated through a family template so far.
    pub(crate) fn family_members_for_test(&self) -> u64 {
        self.family_members_for_test
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}
