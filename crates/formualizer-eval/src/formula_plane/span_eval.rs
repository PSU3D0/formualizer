//! Internal scalar FormulaPlane span evaluator for FP6.4.
//!
//! This is direct FormulaPlane substrate only. It evaluates accepted spans with
//! existing scalar interpreter semantics and stages results into
//! `ComputedWriteBuffer`; it does not integrate with normal engine scheduling.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::arrow_store::{OverlayValue, map_error_code};
use crate::engine::arena::{AstNodeData, AstNodeId, CompactRefType, DataStore};
use crate::engine::eval::ComputedWriteBuffer;
use crate::engine::sheet_registry::SheetRegistry;
use crate::interpreter::{Interpreter, InterpreterParameterBindings};
use crate::reference::CellRef;
use crate::traits::EvaluationContext;
use formualizer_common::{ExcelErrorExtra, ExcelErrorKind, LiteralValue};

use super::region_index::{DirtyDomain, RegionKey, RegionPattern};
use super::runtime::{
    FormulaPlane, FormulaSpan, FormulaSpanRef, PlacementCoord, PlacementDomain,
    PlacementDomainIter, SpanBindingSet, TemplateRecord,
};
use super::template_canonical::{AxisRef, CanonicalReference, SheetBinding};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpanEvalTask {
    pub(crate) span: FormulaSpanRef,
    pub(crate) dirty: DirtyDomain,
    pub(crate) plane_epoch: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SpanEvalReport {
    pub(crate) span_eval_task_count: u64,
    pub(crate) span_eval_placement_count: u64,
    pub(crate) skipped_overlay_punchout_count: u64,
    pub(crate) computed_write_buffer_push_count: u64,
    /// Number of placement-time reference-offset evaluations. This used to
    /// count per-placement transient AST clones; the evaluator now walks the
    /// canonical AST in place and applies offsets at reference leaves.
    pub(crate) transient_ast_relocation_count: u64,
    pub(crate) fallback_count: u64,
    pub(crate) memo_eval_count: u64,
    pub(crate) memo_broadcast_count: u64,
    pub(crate) memo_fallback_count: u64,
    pub(crate) sample_only_key_build_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpanEvalError {
    StalePlaneEpoch,
    StaleSpan,
    MissingTemplate,
    UnsupportedDirtyDomain,
    UnsupportedReferenceRelocation,
}

pub(crate) struct SpanComputedWriteSink<'a> {
    buffer: &'a mut ComputedWriteBuffer,
    push_count: u64,
}

impl<'a> SpanComputedWriteSink<'a> {
    pub(crate) fn new(buffer: &'a mut ComputedWriteBuffer) -> Self {
        Self {
            buffer,
            push_count: 0,
        }
    }

    pub(crate) fn push_cell(&mut self, placement: PlacementCoord, value: OverlayValue) {
        self.buffer
            .push_cell(placement.sheet_id, placement.row, placement.col, value);
        self.push_count = self.push_count.saturating_add(1);
    }

    pub(crate) fn push_count(&self) -> u64 {
        self.push_count
    }
}

#[cfg(test)]
thread_local! {
    static RELOCATABLE_VALIDATION_WALK_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static DIRTY_PLACEMENT_VEC_MATERIALIZATION_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_span_eval_test_counters() {
    RELOCATABLE_VALIDATION_WALK_COUNT.set(0);
    DIRTY_PLACEMENT_VEC_MATERIALIZATION_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn relocatable_validation_walk_count() -> usize {
    RELOCATABLE_VALIDATION_WALK_COUNT.get()
}

#[cfg(test)]
pub(crate) fn dirty_placement_vec_materialization_count() -> usize {
    DIRTY_PLACEMENT_VEC_MATERIALIZATION_COUNT.get()
}

const MEMO_SAMPLE_LIMIT: usize = 64;
const MEMO_MAX_UNIQUE_RATIO_NUM: usize = 3;
const MEMO_MAX_UNIQUE_RATIO_DEN: usize = 4;
const MEMO_MAX_ENTRIES_PER_TASK: usize = 16_384;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ParameterKey {
    pub(crate) atoms: Box<[ParameterAtom]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ParameterAtom {
    Int(i64),
    NumberBits(u64),
    Text(Arc<str>),
    Boolean(bool),
    Date(String),
    DateTime(String),
    Time(String),
    Duration(String),
    Empty,
    Pending,
    Error {
        kind: ExcelErrorKind,
        message: Option<Arc<str>>,
        context_row: Option<u32>,
        context_col: Option<u32>,
        origin_row: Option<u32>,
        origin_col: Option<u32>,
        origin_sheet: Option<Arc<str>>,
        extra: ErrorExtraAtom,
    },
    ResidualRowDelta(i64),
    ResidualColDelta(i64),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ErrorExtraAtom {
    None,
    Spill {
        expected_rows: u32,
        expected_cols: u32,
    },
}

struct MemoGroup {
    representative: PlacementCoord,
    placements: Vec<PlacementCoord>,
    binding_id: u32,
}

pub(crate) struct SpanEvaluator<'a> {
    plane: &'a FormulaPlane,
    context: &'a dyn EvaluationContext,
    current_sheet: &'a str,
    data_store: &'a DataStore,
    sheet_registry: &'a SheetRegistry,
}

impl<'a> SpanEvaluator<'a> {
    pub(crate) fn new(
        plane: &'a FormulaPlane,
        context: &'a dyn EvaluationContext,
        current_sheet: &'a str,
        data_store: &'a DataStore,
        sheet_registry: &'a SheetRegistry,
    ) -> Self {
        Self {
            plane,
            context,
            current_sheet,
            data_store,
            sheet_registry,
        }
    }

    pub(crate) fn evaluate_task(
        &self,
        task: &SpanEvalTask,
        sink: &mut SpanComputedWriteSink<'_>,
    ) -> Result<SpanEvalReport, SpanEvalError> {
        if self.plane.epoch().0 != task.plane_epoch {
            return Err(SpanEvalError::StalePlaneEpoch);
        }

        let span = self
            .plane
            .spans
            .get(task.span)
            .ok_or(SpanEvalError::StaleSpan)?;
        let template = self
            .plane
            .templates
            .get(span.template_id)
            .ok_or(SpanEvalError::MissingTemplate)?;
        ensure_template_relocatable(template, self.data_store)?;
        let placements = placements_for_dirty(span, &task.dirty)?;
        let push_count_before = sink.push_count();
        let base_interpreter = Interpreter::new(self.context, self.current_sheet);

        let mut report = SpanEvalReport {
            span_eval_task_count: 1,
            ..SpanEvalReport::default()
        };

        if span.is_constant_result {
            let first_writable_placement = placements
                .iter()
                .find(|placement| self.plane.formula_overlay.find_at(*placement).is_none());
            let Some(first_writable_placement) = first_writable_placement else {
                report.skipped_overlay_punchout_count = report
                    .skipped_overlay_punchout_count
                    .saturating_add(placements.len() as u64);
                report.computed_write_buffer_push_count =
                    sink.push_count().saturating_sub(push_count_before);
                return Ok(report);
            };

            // Constant-result spans have only all-absolute precedents, so
            // placement offsets cannot affect their value. Materialize the
            // template once and evaluate through `evaluate_ast`, which keeps
            // the AST planner enabled for chunked reductions such as SUMIFS.
            report.transient_ast_relocation_count =
                report.transient_ast_relocation_count.saturating_add(1);
            let ast_tree = self
                .data_store
                .retrieve_ast(template.ast_id, self.sheet_registry)
                .ok_or(SpanEvalError::MissingTemplate)?;
            let interpreter = base_interpreter.with_current_cell(CellRef::new_absolute(
                first_writable_placement.sheet_id,
                first_writable_placement.row,
                first_writable_placement.col,
            ));
            let value = match interpreter.evaluate_ast(&ast_tree) {
                Ok(calc) => literal_to_overlay(calc.into_literal()),
                Err(err) => OverlayValue::Error(map_error_code(err.kind)),
            };

            for placement in placements.iter() {
                if self.plane.formula_overlay.find_at(placement).is_some() {
                    report.skipped_overlay_punchout_count =
                        report.skipped_overlay_punchout_count.saturating_add(1);
                    continue;
                }
                sink.push_cell(placement, value.clone());
                report.span_eval_placement_count =
                    report.span_eval_placement_count.saturating_add(1);
            }
            report.computed_write_buffer_push_count =
                sink.push_count().saturating_sub(push_count_before);
            return Ok(report);
        }

        if let Some(binding_set_id) = span.binding_set_id
            && let Some(binding_set) = self.plane.binding_sets.get(binding_set_id)
            && self.should_try_memoization(span, binding_set, &placements, &mut report)
            && let Some(memo_report) = self.evaluate_memoized(
                span,
                template.ast_id,
                template.origin_row,
                template.origin_col,
                binding_set,
                &placements,
                &base_interpreter,
                sink,
                push_count_before,
            )?
        {
            return Ok(memo_report);
        }

        let per_placement_binding_set = span
            .binding_set_id
            .and_then(|binding_set_id| self.plane.binding_sets.get(binding_set_id));
        for placement in placements.iter() {
            if self.plane.formula_overlay.find_at(placement).is_some() {
                report.skipped_overlay_punchout_count =
                    report.skipped_overlay_punchout_count.saturating_add(1);
                continue;
            }

            // Placement coordinates are 0-indexed; template origins are stored
            // in 1-indexed Excel coordinates to match canonicalization anchors.
            let row_delta = i64::from(placement.row) + 1 - i64::from(template.origin_row);
            let col_delta = i64::from(placement.col) + 1 - i64::from(template.origin_col);
            report.transient_ast_relocation_count =
                report.transient_ast_relocation_count.saturating_add(1);
            let interpreter = base_interpreter.with_current_cell(CellRef::new_absolute(
                placement.sheet_id,
                placement.row,
                placement.col,
            ));
            let value = if let Some(binding_set) = per_placement_binding_set {
                let binding_id = binding_id_for_placement(span, binding_set, placement)?;
                let binding = binding_set
                    .unique_literal_bindings
                    .get(binding_id as usize)
                    .ok_or(SpanEvalError::StaleSpan)?;
                let interpreter =
                    interpreter.with_parameter_bindings(InterpreterParameterBindings {
                        literal_slots_by_node: &binding_set
                            .template_slot_map
                            .literal_slots_by_arena_node,
                        literal_values: binding,
                    });
                match interpreter.evaluate_arena_ast_with_offset(
                    template.ast_id,
                    row_delta,
                    col_delta,
                    self.data_store,
                    self.sheet_registry,
                ) {
                    Ok(calc) => literal_to_overlay(calc.into_literal()),
                    Err(err) => OverlayValue::Error(map_error_code(err.kind)),
                }
            } else {
                match interpreter.evaluate_arena_ast_with_offset(
                    template.ast_id,
                    row_delta,
                    col_delta,
                    self.data_store,
                    self.sheet_registry,
                ) {
                    Ok(calc) => literal_to_overlay(calc.into_literal()),
                    Err(err) => OverlayValue::Error(map_error_code(err.kind)),
                }
            };
            sink.push_cell(placement, value);
            report.span_eval_placement_count = report.span_eval_placement_count.saturating_add(1);
        }
        report.computed_write_buffer_push_count =
            sink.push_count().saturating_sub(push_count_before);
        Ok(report)
    }

    fn should_try_memoization(
        &self,
        span: &FormulaSpan,
        binding_set: &SpanBindingSet,
        placements: &PlacementSelection<'_>,
        report: &mut SpanEvalReport,
    ) -> bool {
        if self.reads_formula_plane_result(span) {
            return false;
        }
        let writable: Vec<_> = placements
            .iter()
            .filter(|placement| self.plane.formula_overlay.find_at(*placement).is_none())
            .collect();
        if writable.len() < 2 {
            return false;
        }
        if binding_set.value_ref_slots.is_empty()
            && binding_set.unique_literal_bindings.len() == writable.len()
        {
            return false;
        }
        let sample_len = writable.len().min(MEMO_SAMPLE_LIMIT);
        let mut unique = FxHashMap::<ParameterKey, ()>::default();
        for placement in writable.iter().take(sample_len) {
            let Ok(key) = self.parameter_key(binding_set, *placement, 0, 0) else {
                return false;
            };
            unique.insert(key, ());
            report.sample_only_key_build_count =
                report.sample_only_key_build_count.saturating_add(1);
        }
        let unique_count = unique.len();
        unique_count < sample_len
            && unique_count * MEMO_MAX_UNIQUE_RATIO_DEN <= sample_len * MEMO_MAX_UNIQUE_RATIO_NUM
    }

    fn reads_formula_plane_result(&self, span: &FormulaSpan) -> bool {
        let Some(read_summary_id) = span.read_summary_id else {
            return false;
        };
        let Some(summary) = self.plane.span_read_summaries.get(read_summary_id) else {
            return false;
        };
        self.plane.spans.active_spans().any(|other| {
            other.id != span.id
                && summary.dependencies.iter().any(|dependency| {
                    dependency
                        .read_region
                        .intersects(&RegionPattern::from_domain(other.result_region.domain()))
                })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_memoized(
        &self,
        span: &FormulaSpan,
        ast_id: AstNodeId,
        origin_row: u32,
        origin_col: u32,
        binding_set: &SpanBindingSet,
        placements: &PlacementSelection<'_>,
        base_interpreter: &Interpreter<'a>,
        sink: &mut SpanComputedWriteSink<'_>,
        push_count_before: u64,
    ) -> Result<Option<SpanEvalReport>, SpanEvalError> {
        let writable_count = placements
            .iter()
            .filter(|placement| self.plane.formula_overlay.find_at(*placement).is_none())
            .count();
        let mut groups: FxHashMap<ParameterKey, MemoGroup> = FxHashMap::default();
        let mut skipped_overlay = 0u64;
        for placement in placements.iter() {
            if self.plane.formula_overlay.find_at(placement).is_some() {
                skipped_overlay = skipped_overlay.saturating_add(1);
                continue;
            }
            let row_delta = i64::from(placement.row) + 1 - i64::from(origin_row);
            let col_delta = i64::from(placement.col) + 1 - i64::from(origin_col);
            let key = self.parameter_key(binding_set, placement, row_delta, col_delta)?;
            if !groups.contains_key(&key) && groups.len() >= MEMO_MAX_ENTRIES_PER_TASK {
                return Ok(None);
            }
            let binding_id = binding_id_for_placement(span, binding_set, placement)?;
            groups
                .entry(key)
                .and_modify(|group| group.placements.push(placement))
                .or_insert_with(|| MemoGroup {
                    representative: placement,
                    placements: vec![placement],
                    binding_id,
                });
            if groups.len() * MEMO_MAX_UNIQUE_RATIO_DEN > writable_count * MEMO_MAX_UNIQUE_RATIO_NUM
            {
                return Ok(None);
            }
        }

        let mut report = SpanEvalReport {
            span_eval_task_count: 1,
            skipped_overlay_punchout_count: skipped_overlay,
            ..SpanEvalReport::default()
        };
        for group in groups.values() {
            let placement = group.representative;
            let row_delta = i64::from(placement.row) + 1 - i64::from(origin_row);
            let col_delta = i64::from(placement.col) + 1 - i64::from(origin_col);
            report.transient_ast_relocation_count =
                report.transient_ast_relocation_count.saturating_add(1);
            report.memo_eval_count = report.memo_eval_count.saturating_add(1);
            let binding = binding_set
                .unique_literal_bindings
                .get(group.binding_id as usize)
                .ok_or(SpanEvalError::StaleSpan)?;
            let interpreter = base_interpreter
                .with_current_cell(CellRef::new_absolute(
                    placement.sheet_id,
                    placement.row,
                    placement.col,
                ))
                .with_parameter_bindings(InterpreterParameterBindings {
                    literal_slots_by_node: &binding_set
                        .template_slot_map
                        .literal_slots_by_arena_node,
                    literal_values: binding,
                });
            let value = match interpreter.evaluate_arena_ast_with_offset(
                ast_id,
                row_delta,
                col_delta,
                self.data_store,
                self.sheet_registry,
            ) {
                Ok(calc) => literal_to_overlay(calc.into_literal()),
                Err(err) => OverlayValue::Error(map_error_code(err.kind)),
            };
            for placement in &group.placements {
                sink.push_cell(*placement, value.clone());
                report.span_eval_placement_count =
                    report.span_eval_placement_count.saturating_add(1);
            }
            report.memo_broadcast_count = report
                .memo_broadcast_count
                .saturating_add(group.placements.len().saturating_sub(1) as u64);
        }
        report.computed_write_buffer_push_count =
            sink.push_count().saturating_sub(push_count_before);
        Ok(Some(report))
    }

    fn parameter_key(
        &self,
        binding_set: &SpanBindingSet,
        placement: PlacementCoord,
        row_delta: i64,
        col_delta: i64,
    ) -> Result<ParameterKey, SpanEvalError> {
        let binding_id = binding_id_for_domain(
            &binding_set.placement_literal_binding_ids,
            placement,
            binding_set,
            self.plane,
        )?;
        let binding = binding_set
            .unique_literal_bindings
            .get(binding_id as usize)
            .ok_or(SpanEvalError::StaleSpan)?;
        let mut atoms = Vec::with_capacity(binding.len() + binding_set.value_ref_slots.len() + 2);
        for value in binding.iter() {
            atoms.push(parameter_atom_from_literal(value));
        }
        for slot in binding_set.value_ref_slots.iter() {
            let value = self.resolve_value_ref_slot(&slot.reference_pattern, placement)?;
            atoms.push(parameter_atom_from_literal(&value));
        }
        if residual_has_row(binding_set) {
            atoms.push(ParameterAtom::ResidualRowDelta(row_delta));
        }
        if residual_has_col(binding_set) {
            atoms.push(ParameterAtom::ResidualColDelta(col_delta));
        }
        Ok(ParameterKey {
            atoms: atoms.into_boxed_slice(),
        })
    }

    fn resolve_value_ref_slot(
        &self,
        reference: &CanonicalReference,
        placement: PlacementCoord,
    ) -> Result<LiteralValue, SpanEvalError> {
        let CanonicalReference::Cell { sheet, row, col } = reference else {
            return Err(SpanEvalError::UnsupportedReferenceRelocation);
        };
        let row = instantiate_axis(row, placement.row)?;
        let col = instantiate_axis(col, placement.col)?;
        let sheet_name = match sheet {
            SheetBinding::CurrentSheet => None,
            SheetBinding::ExplicitName { name } => Some(name.as_str()),
        };
        self.context
            .resolve_cell_reference_value(sheet_name, row, col, self.current_sheet)
            .map_err(|_| SpanEvalError::UnsupportedReferenceRelocation)
    }
}

fn binding_id_for_placement(
    span: &FormulaSpan,
    binding_set: &SpanBindingSet,
    placement: PlacementCoord,
) -> Result<u32, SpanEvalError> {
    let ordinal = span
        .domain
        .ordinal_of(placement)
        .ok_or(SpanEvalError::StaleSpan)?;
    binding_set
        .placement_literal_binding_ids
        .get(ordinal)
        .copied()
        .ok_or(SpanEvalError::StaleSpan)
}

fn binding_id_for_domain(
    ids: &[u32],
    placement: PlacementCoord,
    binding_set: &SpanBindingSet,
    plane: &FormulaPlane,
) -> Result<u32, SpanEvalError> {
    let span = plane
        .spans
        .get(binding_set.span_ref)
        .ok_or(SpanEvalError::StaleSpan)?;
    let ordinal = span
        .domain
        .ordinal_of(placement)
        .ok_or(SpanEvalError::StaleSpan)?;
    ids.get(ordinal).copied().ok_or(SpanEvalError::StaleSpan)
}

fn residual_has_row(binding_set: &SpanBindingSet) -> bool {
    binding_set.template_slot_map.residual_relative_row
}

fn residual_has_col(binding_set: &SpanBindingSet) -> bool {
    binding_set.template_slot_map.residual_relative_col
}

fn instantiate_axis(axis: &AxisRef, placement_zero_based_axis: u32) -> Result<u32, SpanEvalError> {
    match axis {
        AxisRef::RelativeToPlacement { offset } => {
            let one_based = i64::from(placement_zero_based_axis) + 1 + *offset;
            u32::try_from(one_based).map_err(|_| SpanEvalError::UnsupportedReferenceRelocation)
        }
        AxisRef::AbsoluteVc { index } => Ok(*index),
        _ => Err(SpanEvalError::UnsupportedReferenceRelocation),
    }
}

fn parameter_atom_from_literal(value: &LiteralValue) -> ParameterAtom {
    match value {
        LiteralValue::Int(value) => ParameterAtom::Int(*value),
        LiteralValue::Number(value) => ParameterAtom::NumberBits(value.to_bits()),
        LiteralValue::Text(value) => ParameterAtom::Text(Arc::from(value.as_str())),
        LiteralValue::Boolean(value) => ParameterAtom::Boolean(*value),
        LiteralValue::Date(value) => ParameterAtom::Date(value.to_string()),
        LiteralValue::DateTime(value) => ParameterAtom::DateTime(value.to_string()),
        LiteralValue::Time(value) => ParameterAtom::Time(value.to_string()),
        LiteralValue::Duration(value) => ParameterAtom::Duration(format!("{value:?}")),
        LiteralValue::Empty => ParameterAtom::Empty,
        LiteralValue::Pending => ParameterAtom::Pending,
        LiteralValue::Error(err) => ParameterAtom::Error {
            kind: err.kind,
            message: err.message.as_deref().map(Arc::from),
            context_row: err.context.as_ref().and_then(|context| context.row),
            context_col: err.context.as_ref().and_then(|context| context.col),
            origin_row: err.context.as_ref().and_then(|context| context.origin_row),
            origin_col: err.context.as_ref().and_then(|context| context.origin_col),
            origin_sheet: err
                .context
                .as_ref()
                .and_then(|context| context.origin_sheet.as_deref().map(Arc::from)),
            extra: match err.extra {
                ExcelErrorExtra::None => ErrorExtraAtom::None,
                ExcelErrorExtra::Spill {
                    expected_rows,
                    expected_cols,
                } => ErrorExtraAtom::Spill {
                    expected_rows,
                    expected_cols,
                },
            },
        },
        LiteralValue::Array(rows) => ParameterAtom::Text(Arc::from(format!("{rows:?}"))),
    }
}

enum PlacementSelection<'a> {
    Whole(&'a PlacementDomain),
    Vec(Vec<PlacementCoord>),
}

impl PlacementSelection<'_> {
    fn iter(&self) -> PlacementSelectionIter<'_> {
        match self {
            Self::Whole(domain) => PlacementSelectionIter::Whole(domain.iter()),
            Self::Vec(coords) => PlacementSelectionIter::Vec(coords.iter().copied()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Whole(domain) => domain.cell_count() as usize,
            Self::Vec(coords) => coords.len(),
        }
    }
}

enum PlacementSelectionIter<'a> {
    Whole(PlacementDomainIter),
    Vec(std::iter::Copied<std::slice::Iter<'a, PlacementCoord>>),
}

impl Iterator for PlacementSelectionIter<'_> {
    type Item = PlacementCoord;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Whole(iter) => iter.next(),
            Self::Vec(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Whole(iter) => iter.size_hint(),
            Self::Vec(iter) => iter.size_hint(),
        }
    }
}

impl ExactSizeIterator for PlacementSelectionIter<'_> {}

fn placements_for_dirty<'a>(
    span: &'a FormulaSpan,
    dirty: &DirtyDomain,
) -> Result<PlacementSelection<'a>, SpanEvalError> {
    match dirty {
        DirtyDomain::WholeSpan(span_ref) => {
            if span_ref.id != span.id || span_ref.generation != span.generation {
                return Err(SpanEvalError::StaleSpan);
            }
            Ok(PlacementSelection::Whole(&span.domain))
        }
        DirtyDomain::Cells(keys) => {
            #[cfg(test)]
            DIRTY_PLACEMENT_VEC_MATERIALIZATION_COUNT
                .set(DIRTY_PLACEMENT_VEC_MATERIALIZATION_COUNT.get() + 1);
            Ok(PlacementSelection::Vec(
                keys.iter()
                    .copied()
                    .map(PlacementCoord::from)
                    .filter(|coord| span.domain.contains(*coord))
                    .collect(),
            ))
        }
        DirtyDomain::Regions(regions) => {
            #[cfg(test)]
            DIRTY_PLACEMENT_VEC_MATERIALIZATION_COUNT
                .set(DIRTY_PLACEMENT_VEC_MATERIALIZATION_COUNT.get() + 1);
            Ok(PlacementSelection::Vec(
                span.domain
                    .iter()
                    .filter(|coord| {
                        let key = RegionKey::from(*coord);
                        regions.iter().any(|region| region.contains_key(key))
                    })
                    .collect(),
            ))
        }
    }
}

impl From<RegionKey> for PlacementCoord {
    fn from(key: RegionKey) -> Self {
        PlacementCoord::new(key.sheet_id, key.row, key.col)
    }
}

fn ensure_template_relocatable(
    template: &TemplateRecord,
    data_store: &DataStore,
) -> Result<(), SpanEvalError> {
    if let Some(validated) = template.relocatable_ast_validated.get() {
        return if *validated {
            Ok(())
        } else {
            Err(SpanEvalError::UnsupportedReferenceRelocation)
        };
    }

    #[cfg(test)]
    RELOCATABLE_VALIDATION_WALK_COUNT.set(RELOCATABLE_VALIDATION_WALK_COUNT.get() + 1);
    match validate_relocatable_arena_ast(template.ast_id, data_store) {
        Ok(()) => {
            let _ = template.relocatable_ast_validated.set(true);
            Ok(())
        }
        Err(err) => {
            let _ = template.relocatable_ast_validated.set(false);
            Err(err)
        }
    }
}

fn validate_relocatable_arena_ast(
    node_id: AstNodeId,
    data_store: &DataStore,
) -> Result<(), SpanEvalError> {
    let node = data_store
        .get_node(node_id)
        .ok_or(SpanEvalError::UnsupportedReferenceRelocation)?;
    match node {
        AstNodeData::Literal(_) => Ok(()),
        AstNodeData::Reference { ref_type, .. } => validate_relocatable_compact_reference(ref_type),
        AstNodeData::UnaryOp { expr_id, .. } => {
            validate_relocatable_arena_ast(*expr_id, data_store)
        }
        AstNodeData::BinaryOp {
            left_id, right_id, ..
        } => {
            validate_relocatable_arena_ast(*left_id, data_store)?;
            validate_relocatable_arena_ast(*right_id, data_store)
        }
        AstNodeData::Function { .. } => {
            let args = data_store
                .get_args(node_id)
                .ok_or(SpanEvalError::UnsupportedReferenceRelocation)?;
            for arg in args {
                validate_relocatable_arena_ast(*arg, data_store)?;
            }
            Ok(())
        }
        AstNodeData::Array { .. } => {
            let (_, _, elements) = data_store
                .get_array_elems(node_id)
                .ok_or(SpanEvalError::UnsupportedReferenceRelocation)?;
            for element in elements {
                validate_relocatable_arena_ast(*element, data_store)?;
            }
            Ok(())
        }
    }
}

fn validate_relocatable_compact_reference(reference: &CompactRefType) -> Result<(), SpanEvalError> {
    match reference {
        CompactRefType::Cell { .. } | CompactRefType::Range { .. } => Ok(()),
        CompactRefType::NamedRange(_)
        | CompactRefType::Table { .. }
        | CompactRefType::Cell3D { .. }
        | CompactRefType::Range3D { .. }
        | CompactRefType::External { .. } => Err(SpanEvalError::UnsupportedReferenceRelocation),
    }
}

fn literal_to_overlay(value: LiteralValue) -> OverlayValue {
    match value {
        LiteralValue::Int(i) => OverlayValue::Number(i as f64),
        LiteralValue::Number(n) => OverlayValue::Number(n),
        LiteralValue::Text(s) => OverlayValue::Text(Arc::from(s)),
        LiteralValue::Boolean(b) => OverlayValue::Boolean(b),
        LiteralValue::Array(mut rows) => rows
            .get_mut(0)
            .and_then(|row| row.get_mut(0))
            .cloned()
            .map(literal_to_overlay)
            .unwrap_or(OverlayValue::Empty),
        LiteralValue::Date(_) | LiteralValue::DateTime(_) | LiteralValue::Time(_) => value
            .as_serial_number()
            .map(OverlayValue::DateTime)
            .unwrap_or(OverlayValue::Empty),
        LiteralValue::Duration(_) => value
            .as_serial_number()
            .map(OverlayValue::Duration)
            .unwrap_or(OverlayValue::Empty),
        LiteralValue::Empty => OverlayValue::Empty,
        LiteralValue::Pending => OverlayValue::Pending,
        LiteralValue::Error(err) => OverlayValue::Error(map_error_code(err.kind)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType, parse};

    use crate::engine::EvalConfig;
    use crate::engine::arena::DataStore;
    use crate::engine::eval::{ComputedWrite, Engine};
    use crate::engine::sheet_registry::SheetRegistry;
    use crate::test_workbook::TestWorkbook;

    use super::super::placement::{FormulaPlacementCandidate, place_candidate_family};
    use super::super::region_index::RegionPattern;
    use super::super::runtime::{
        FormulaOverlayEntryKind, NewFormulaSpan, PlacementDomain, ResultRegion,
    };
    use super::*;

    fn candidate(
        data_store: &mut DataStore,
        sheet_registry: &SheetRegistry,
        sheet_id: u16,
        row: u32,
        col: u32,
        formula: &str,
    ) -> FormulaPlacementCandidate {
        let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
        let ast_id = data_store.store_ast(&ast, sheet_registry);
        FormulaPlacementCandidate::new(sheet_id, row, col, ast_id, Some(Arc::<str>::from(formula)))
    }

    fn column_label(mut zero_based_col: u32) -> String {
        let mut chars = Vec::new();
        loop {
            let rem = zero_based_col % 26;
            chars.push((b'A' + rem as u8) as char);
            zero_based_col /= 26;
            if zero_based_col == 0 {
                break;
            }
            zero_based_col -= 1;
        }
        chars.iter().rev().collect()
    }

    fn whole_span_task(plane: &FormulaPlane, span: FormulaSpanRef) -> SpanEvalTask {
        SpanEvalTask {
            span,
            dirty: DirtyDomain::WholeSpan(span),
            plane_epoch: plane.epoch().0,
        }
    }

    fn cells_task(
        plane: &FormulaPlane,
        span: FormulaSpanRef,
        cells: Vec<RegionKey>,
    ) -> SpanEvalTask {
        SpanEvalTask {
            span,
            dirty: DirtyDomain::Cells(cells),
            plane_epoch: plane.epoch().0,
        }
    }

    fn regions_task(
        plane: &FormulaPlane,
        span: FormulaSpanRef,
        regions: Vec<RegionPattern>,
    ) -> SpanEvalTask {
        SpanEvalTask {
            span,
            dirty: DirtyDomain::Regions(regions),
            plane_epoch: plane.epoch().0,
        }
    }

    fn eval_task(
        plane: &FormulaPlane,
        workbook: &TestWorkbook,
        task: &SpanEvalTask,
        buffer: &mut ComputedWriteBuffer,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> SpanEvalReport {
        let evaluator = SpanEvaluator::new(plane, workbook, "Sheet1", data_store, sheet_registry);
        let mut sink = SpanComputedWriteSink::new(buffer);
        evaluator.evaluate_task(task, &mut sink).unwrap()
    }

    fn arrow_eval_config() -> EvalConfig {
        EvalConfig {
            arrow_storage_enabled: true,
            delta_overlay_enabled: true,
            write_formula_overlay_enabled: true,
            ..Default::default()
        }
    }

    fn span_from_report(
        report: &super::super::placement::FormulaPlacementReport,
    ) -> FormulaSpanRef {
        match report.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        }
    }

    fn cell_values(buffer: &ComputedWriteBuffer) -> Vec<(u32, u32, OverlayValue)> {
        buffer
            .writes()
            .iter()
            .map(|write| match write {
                ComputedWrite::Cell {
                    row0, col0, value, ..
                } => (*row0, *col0, value.clone()),
                ComputedWrite::Rect { .. } => panic!("span evaluator should push cells"),
            })
            .collect()
    }

    fn computed_overlay_stats(
        engine: &Engine<TestWorkbook>,
        sheet: &str,
        col0: usize,
        row0: usize,
    ) -> crate::arrow_store::OverlayDebugStats {
        let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
        let (chunk_idx, _) = asheet.chunk_of_row(row0).expect("row chunk");
        asheet.columns[col0]
            .chunk(chunk_idx)
            .expect("column chunk")
            .computed_overlay
            .debug_stats()
    }

    fn range_number_values(
        engine: &Engine<TestWorkbook>,
        sheet: &str,
        sr: u32,
        sc: u32,
        er: u32,
        ec: u32,
    ) -> Vec<f64> {
        let view = engine.read_range_values(sheet, sr, sc, er, ec);
        let rows = er.saturating_sub(sr).saturating_add(1) as usize;
        let cols = view.slice_numbers(0, rows);
        assert_eq!(cols.len(), ec.saturating_sub(sc).saturating_add(1) as usize);
        let arr = cols[0].as_ref().expect("numeric column");
        (0..arr.len()).map(|idx| arr.value(idx)).collect()
    }

    #[test]
    fn span_eval_row_run_matches_legacy_outputs() {
        let workbook = (1..=100).fold(TestWorkbook::new(), |workbook, row| {
            workbook
                .with_cell("Sheet1", row, 1, LiteralValue::Number(row as f64))
                .with_cell("Sheet1", row, 2, LiteralValue::Number(row as f64 * 10.0))
        });
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        2,
                        &format!("=A{}+B{}", row + 1, row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.span_eval_placement_count, 100);
        assert_eq!(report.computed_write_buffer_push_count, 100);
        assert_eq!(
            cell_values(&buffer),
            (0..100)
                .map(|row| (row, 2, OverlayValue::Number((row + 1) as f64 * 11.0)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn span_eval_reuses_template_anchor_across_discontiguous_components() {
        let workbook = (1..=300).fold(TestWorkbook::new(), |workbook, row| {
            workbook.with_cell("Sheet1", row, 1, LiteralValue::Number(row as f64))
        });
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let first_component = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}*2", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let second_component = place_candidate_family(
            &mut plane,
            (200..300)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}*2", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let first_span = span_from_report(&first_component);
        let second_span = span_from_report(&second_component);
        let first_template = match first_component.results[0] {
            super::super::placement::FormulaPlacementResult::Span { template_id, .. } => {
                template_id
            }
            _ => panic!("expected first component span"),
        };
        let second_template = match second_component.results[0] {
            super::super::placement::FormulaPlacementResult::Span { template_id, .. } => {
                template_id
            }
            _ => panic!("expected second component span"),
        };
        assert_eq!(first_template, second_template);
        assert_eq!(plane.templates.len(), 1);

        let mut buffer = ComputedWriteBuffer::default();
        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, first_span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );
        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, second_span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );

        let expected = (0..100)
            .chain(200..300)
            .map(|row| (row, 1, OverlayValue::Number((row + 1) as f64 * 2.0)))
            .collect::<Vec<_>>();
        assert_eq!(cell_values(&buffer), expected);
    }

    #[test]
    fn span_eval_col_run_matches_legacy_outputs() {
        let workbook = (0..100).fold(TestWorkbook::new(), |workbook, col| {
            workbook
                .with_cell("Sheet1", 1, col + 1, LiteralValue::Number((col + 1) as f64))
                .with_cell(
                    "Sheet1",
                    2,
                    col + 1,
                    LiteralValue::Number((col + 1) as f64 * 10.0),
                )
        });
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|col| {
                    let label = column_label(col);
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        2,
                        col,
                        &format!("={label}1+{label}2"),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );

        assert_eq!(
            cell_values(&buffer),
            (0..100)
                .map(|col| (2, col, OverlayValue::Number((col + 1) as f64 * 11.0)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn span_eval_rect_matches_legacy_outputs() {
        // Use externally-anchored reads so the rect family has no internal
        // dependency: every cell reads column A on its own row, none of which
        // is in the rect, and the relative row keeps the family non-constant.
        let workbook = (2..=11).fold(TestWorkbook::new(), |workbook, row| {
            workbook.with_cell("Sheet1", row, 1, LiteralValue::Number(10.0))
        });
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let mut candidates = Vec::new();
        for row in 1..=10 {
            for col in 1..=10 {
                candidates.push(candidate(
                    &mut data_store,
                    &sheet_registry,
                    0,
                    row,
                    col,
                    &format!("=$A{}+1", row + 1),
                ));
            }
        }
        let placement =
            place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );

        assert_eq!(
            cell_values(&buffer),
            (1..=10)
                .flat_map(|row| (1..=10).map(move |col| (row, col, OverlayValue::Number(11.0))))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn span_eval_preserves_explicit_empty_outputs() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let template_id = plane.intern_template(
            Arc::<str>::from("empty-template"),
            data_store.store_ast(
                &ASTNode::new(ASTNodeType::Literal(LiteralValue::Empty), None),
                &sheet_registry,
            ),
            1,
            1,
            Some(Arc::<str>::from("=")),
        );
        let domain = PlacementDomain::row_run(0, 0, 1, 0);
        let span = plane.insert_span(NewFormulaSpan {
            sheet_id: 0,
            template_id,
            result_region: ResultRegion::scalar_cells(domain.clone()),
            domain,
            intrinsic_mask_id: None,
            read_summary_id: None,
            binding_set_id: None,
            is_constant_result: false,
        });
        let workbook = TestWorkbook::new();
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );

        assert_eq!(
            cell_values(&buffer),
            vec![(0, 0, OverlayValue::Empty), (1, 0, OverlayValue::Empty)]
        );
    }

    #[test]
    fn span_eval_effective_domain_skips_overlay_punchouts() {
        let workbook = (1..=100).fold(TestWorkbook::new(), |workbook, row| {
            workbook.with_cell("Sheet1", row, 1, LiteralValue::Number(row as f64))
        });
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}+1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 1, 1, 1),
            FormulaOverlayEntryKind::ValueOverride,
            Some(span),
        );
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.span_eval_placement_count, 99);
        assert_eq!(report.skipped_overlay_punchout_count, 1);
        assert_eq!(
            cell_values(&buffer),
            (0..100)
                .filter(|row| *row != 1)
                .map(|row| (row, 1, OverlayValue::Number((row + 1) as f64 + 1.0)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn span_eval_writes_through_computed_write_buffer_not_direct_overlay() {
        let workbook = TestWorkbook::new().with_cell("Sheet1", 1, 1, LiteralValue::Number(9.0));
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}+1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = match placement.results[0] {
            super::super::placement::FormulaPlacementResult::Span { span, .. } => span,
            _ => panic!("expected span"),
        };
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.computed_write_buffer_push_count, buffer.len() as u64);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn span_eval_cells_dirty_domain_evaluates_only_matching_cells() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 3, 1, LiteralValue::Number(3.0));
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}+1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let mut buffer = ComputedWriteBuffer::default();
        let task = cells_task(
            &plane,
            span,
            vec![RegionKey::new(0, 1, 1), RegionKey::new(0, 199, 1)],
        );

        let report = eval_task(
            &plane,
            &workbook,
            &task,
            &mut buffer,
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.span_eval_placement_count, 1);
        assert_eq!(report.transient_ast_relocation_count, 1);
        assert_eq!(
            cell_values(&buffer),
            vec![(1, 1, OverlayValue::Number(3.0))]
        );
    }

    #[test]
    fn span_eval_regions_dirty_domain_intersects_span_domain() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 3, 1, LiteralValue::Number(3.0));
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}+1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let mut buffer = ComputedWriteBuffer::default();
        let task = regions_task(&plane, span, vec![RegionPattern::rect(0, 1, 2, 1, 1)]);

        let report = eval_task(
            &plane,
            &workbook,
            &task,
            &mut buffer,
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.span_eval_placement_count, 2);
        assert_eq!(report.transient_ast_relocation_count, 2);
        assert_eq!(
            cell_values(&buffer),
            vec![
                (1, 1, OverlayValue::Number(3.0)),
                (2, 1, OverlayValue::Number(4.0))
            ]
        );
    }

    #[test]
    fn span_eval_stale_span_generation_fails_closed() {
        let workbook = TestWorkbook::new();
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}*0+1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let task = whole_span_task(&plane, span);
        assert!(plane.remove_span(span));
        let stale_generation_task = SpanEvalTask {
            plane_epoch: plane.epoch().0,
            ..task
        };
        let mut buffer = ComputedWriteBuffer::default();
        let evaluator =
            SpanEvaluator::new(&plane, &workbook, "Sheet1", &data_store, &sheet_registry);
        let mut sink = SpanComputedWriteSink::new(&mut buffer);

        let err = evaluator
            .evaluate_task(&stale_generation_task, &mut sink)
            .unwrap_err();

        assert_eq!(err, SpanEvalError::StaleSpan);
        assert!(buffer.is_empty());
    }

    #[test]
    fn span_eval_stale_plane_epoch_fails_closed() {
        let workbook = TestWorkbook::new();
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}*0+1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let task = whole_span_task(&plane, span);
        plane.insert_overlay(
            0,
            PlacementDomain::row_run(0, 0, 0, 1),
            FormulaOverlayEntryKind::ValueOverride,
            Some(span),
        );
        let mut buffer = ComputedWriteBuffer::default();
        let evaluator =
            SpanEvaluator::new(&plane, &workbook, "Sheet1", &data_store, &sheet_registry);
        let mut sink = SpanComputedWriteSink::new(&mut buffer);

        let err = evaluator.evaluate_task(&task, &mut sink).unwrap_err();

        assert_eq!(err, SpanEvalError::StalePlaneEpoch);
        assert!(buffer.is_empty());
    }

    #[test]
    fn span_eval_absolute_refs_match_legacy_engine_outputs() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .with_cell("Sheet1", 3, 1, LiteralValue::Number(4.0))
            .with_cell("Sheet1", 1, 6, LiteralValue::Number(10.0));
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        2,
                        &format!("=A{}*$F$1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let mut formula_plane_engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(formula_plane_engine.graph.sheet_id_mut("Sheet1"), 0);
        let mut buffer = ComputedWriteBuffer::default();
        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );
        formula_plane_engine
            .flush_computed_write_buffer(&mut buffer)
            .unwrap();

        let mut legacy = Engine::new(TestWorkbook::new(), arrow_eval_config());
        legacy
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(4.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(10.0))
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 1, 3, parse("=A1*$F$1").unwrap())
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 2, 3, parse("=A2*$F$1").unwrap())
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 3, 3, parse("=A3*$F$1").unwrap())
            .unwrap();
        legacy.evaluate_all().unwrap();

        for row in 1..=3 {
            assert_eq!(
                formula_plane_engine.get_cell_value("Sheet1", row, 3),
                legacy.get_cell_value("Sheet1", row, 3)
            );
        }
    }

    #[test]
    fn span_eval_div_zero_error_matches_legacy_engine_outputs() {
        let workbook = TestWorkbook::new()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 1, 2, LiteralValue::Number(0.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .with_cell("Sheet1", 2, 2, LiteralValue::Number(0.0));
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        2,
                        &format!("=A{}/B{}", row + 1, row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let mut formula_plane_engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(formula_plane_engine.graph.sheet_id_mut("Sheet1"), 0);
        let mut buffer = ComputedWriteBuffer::default();
        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );
        formula_plane_engine
            .flush_computed_write_buffer(&mut buffer)
            .unwrap();

        let mut legacy = Engine::new(TestWorkbook::new(), arrow_eval_config());
        legacy
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(0.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .unwrap();
        legacy
            .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(0.0))
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 1, 3, parse("=A1/B1").unwrap())
            .unwrap();
        legacy
            .set_cell_formula("Sheet1", 2, 3, parse("=A2/B2").unwrap())
            .unwrap();
        legacy.evaluate_all().unwrap();

        for row in 1..=2 {
            assert_eq!(
                formula_plane_engine.get_cell_value("Sheet1", row, 3),
                legacy.get_cell_value("Sheet1", row, 3)
            );
        }
    }

    #[test]
    fn span_eval_varying_outputs_emit_dense_fragment_and_rangeview_reads_results() {
        let workbook = (1..=100).fold(TestWorkbook::new(), |workbook, row| {
            workbook.with_cell("Sheet1", row, 1, LiteralValue::Number(row as f64))
        });
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        2,
                        &format!("=A{}+1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(engine.graph.sheet_id_mut("Sheet1"), 0);
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );
        engine.flush_computed_write_buffer(&mut buffer).unwrap();

        assert_eq!(report.transient_ast_relocation_count, 100);
        let stats = computed_overlay_stats(&engine, "Sheet1", 2, 0);
        assert_eq!(stats.dense_fragments, 1);
        assert_eq!(stats.run_fragments, 0);
        assert_eq!(
            range_number_values(&engine, "Sheet1", 1, 3, 4, 3),
            vec![2.0, 3.0, 4.0, 5.0]
        );
    }

    #[test]
    fn span_eval_constant_outputs_emit_run_fragment() {
        let workbook = (1..=100).fold(TestWorkbook::new(), |workbook, row| {
            workbook.with_cell("Sheet1", row, 1, LiteralValue::Number(row as f64))
        });
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}*0+7", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(engine.graph.sheet_id_mut("Sheet1"), 0);
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );
        engine.flush_computed_write_buffer(&mut buffer).unwrap();

        let stats = computed_overlay_stats(&engine, "Sheet1", 1, 0);
        assert_eq!(stats.run_fragments, 1);
        assert_eq!(stats.dense_fragments, 0);
        assert_eq!(
            range_number_values(&engine, "Sheet1", 1, 2, 8, 2),
            vec![7.0; 8]
        );
    }

    #[test]
    fn span_eval_sparse_dirty_domain_emits_sparse_fragment() {
        let workbook = (1..=128).fold(TestWorkbook::new(), |workbook, row| {
            workbook.with_cell("Sheet1", row, 1, LiteralValue::Number(row as f64))
        });
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..128)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        1,
                        &format!("=A{}*0+1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(engine.graph.sheet_id_mut("Sheet1"), 0);
        let dirty_cells = (0..128)
            .step_by(2)
            .map(|row| RegionKey::new(0, row, 1))
            .collect();
        let task = cells_task(&plane, span, dirty_cells);
        let mut buffer = ComputedWriteBuffer::default();

        let report = eval_task(
            &plane,
            &workbook,
            &task,
            &mut buffer,
            &data_store,
            &sheet_registry,
        );
        engine.flush_computed_write_buffer(&mut buffer).unwrap();

        assert_eq!(report.span_eval_placement_count, 64);
        let stats = computed_overlay_stats(&engine, "Sheet1", 1, 0);
        assert_eq!(stats.sparse_fragments, 1);
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 2),
            Some(LiteralValue::Number(1.0))
        );
        assert_eq!(engine.get_cell_value("Sheet1", 2, 2), None);
        assert_eq!(
            engine.get_cell_value("Sheet1", 127, 2),
            Some(LiteralValue::Number(1.0))
        );
        assert_eq!(engine.get_cell_value("Sheet1", 128, 2), None);
    }

    #[test]
    fn span_eval_user_overlay_masks_computed_span_result_after_flush() {
        let workbook = (1..=100).fold(TestWorkbook::new(), |workbook, row| {
            workbook.with_cell("Sheet1", row, 1, LiteralValue::Number(row as f64))
        });
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let placement = place_candidate_family(
            &mut plane,
            (0..100)
                .map(|row| {
                    candidate(
                        &mut data_store,
                        &sheet_registry,
                        0,
                        row,
                        2,
                        &format!("=A{}+1", row + 1),
                    )
                })
                .collect(),
            &data_store,
            &sheet_registry,
        );
        let span = span_from_report(&placement);
        let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
        assert_eq!(engine.graph.sheet_id_mut("Sheet1"), 0);
        {
            let mut ingest = engine.begin_bulk_ingest_arrow();
            ingest.add_sheet("Sheet1", 3, 100);
            for _ in 0..100 {
                ingest
                    .append_row(
                        "Sheet1",
                        &[
                            LiteralValue::Empty,
                            LiteralValue::Empty,
                            LiteralValue::Empty,
                        ],
                    )
                    .unwrap();
            }
            ingest.finish().unwrap();
        }
        {
            let asheet = engine.sheet_store_mut().sheet_mut("Sheet1").unwrap();
            let (chunk_idx, offset) = asheet.chunk_of_row(1).unwrap();
            asheet.columns[2].chunks[chunk_idx]
                .overlay
                .set_scalar(offset, OverlayValue::Text("user".into()));
        }
        let mut buffer = ComputedWriteBuffer::default();

        eval_task(
            &plane,
            &workbook,
            &whole_span_task(&plane, span),
            &mut buffer,
            &data_store,
            &sheet_registry,
        );
        engine.flush_computed_write_buffer(&mut buffer).unwrap();

        let stats = computed_overlay_stats(&engine, "Sheet1", 2, 0);
        assert_eq!(stats.dense_fragments, 1);
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 3),
            Some(LiteralValue::Number(2.0))
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", 2, 3),
            Some(LiteralValue::Text("user".into()))
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", 3, 3),
            Some(LiteralValue::Number(4.0))
        );
    }

    #[test]
    fn span_eval_fallback_for_unsupported_template_matches_legacy() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let template_id = plane.intern_template(
            Arc::<str>::from("external-ref"),
            data_store.store_ast(&parse("='[book.xlsx]Sheet1'!A1").unwrap(), &sheet_registry),
            1,
            1,
            Some(Arc::<str>::from("='[book.xlsx]Sheet1'!A1")),
        );
        let domain = PlacementDomain::row_run(0, 0, 1, 0);
        let span = plane.insert_span(NewFormulaSpan {
            sheet_id: 0,
            template_id,
            result_region: ResultRegion::scalar_cells(domain.clone()),
            domain,
            intrinsic_mask_id: None,
            read_summary_id: None,
            binding_set_id: None,
            is_constant_result: false,
        });
        let workbook = TestWorkbook::new();
        let mut buffer = ComputedWriteBuffer::default();
        let evaluator =
            SpanEvaluator::new(&plane, &workbook, "Sheet1", &data_store, &sheet_registry);
        let mut sink = SpanComputedWriteSink::new(&mut buffer);

        let err = evaluator
            .evaluate_task(&whole_span_task(&plane, span), &mut sink)
            .unwrap_err();

        assert_eq!(err, SpanEvalError::UnsupportedReferenceRelocation);
        assert!(buffer.is_empty());
    }
}
