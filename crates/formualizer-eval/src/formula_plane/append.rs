//! Checked, additions-only FormulaPlane append transactions.

use std::sync::Arc;

use formualizer_common::PackedSheetCell;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::SheetId;
use crate::engine::arena::{AstNodeId, DataStore};
use crate::engine::sheet_registry::SheetRegistry;

use super::authority::FormulaAuthority;
use super::ids::FormulaTemplateId;
use super::placement::PreparedAnchorOncePlacement;
use super::producer::{
    DirtyProjectionRule, FormulaProducerId, SpanReadDependency, SpanReadSummary,
};
use super::region_index::{AxisRange, Region, SpanDomainIndex};
use super::runtime::{
    FormulaPlaneEpoch, FormulaSpanId, FormulaSpanRef, LiteralBindingEncoding, NewFormulaSpan,
    PlacementCoord, PlacementDomain, ResultRegion, SpanAstRelocation, SpanBindingSet,
};

#[derive(Debug)]
struct FormulaPlaneAppendPlacement {
    sheet_id: SheetId,
    exact_canonical_key: Arc<str>,
    parameterized_canonical_key: Arc<str>,
    ast_id: AstNodeId,
    origin_row: u32,
    origin_col: u32,
    formula_text: Option<Arc<str>>,
    domain: PlacementDomain,
    result_region: ResultRegion,
    read_summary: SpanReadSummary,
    binding_set: SpanBindingSet,
    is_constant_result: bool,
    resolved_named_refs: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FormulaPlaneAppendWork {
    pub(crate) placements: usize,
    pub(crate) templates_to_append: usize,
    pub(crate) templates_reused: usize,
    pub(crate) span_domains_checked: usize,
    pub(crate) existing_span_candidates_checked: usize,
    pub(crate) overlay_candidates_checked: usize,
    pub(crate) read_dependencies_checked: usize,
    pub(crate) result_index_entries: usize,
    pub(crate) read_index_entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaPlaneAppendReport {
    pub(crate) spans: Vec<FormulaSpanRef>,
    pub(crate) template_ids: Vec<FormulaTemplateId>,
    pub(crate) work: FormulaPlaneAppendWork,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FormulaPlaneAppendError {
    Empty,
    InvalidSheet(SheetId),
    InvalidAst(AstNodeId),
    InvalidTemplateOrigin {
        row: u32,
        col: u32,
    },
    InvalidDomain,
    InvalidResultBinding,
    InvalidReadSummary,
    InvalidBindingSet,
    DuplicateDependency,
    DuplicateDomain,
    BatchOverlap,
    SpanOwnershipConflict,
    OverlayConflict,
    IdentifierExhausted(&'static str),
    Stale,
    #[cfg(test)]
    InjectedFinalCheckFailure,
}

impl std::fmt::Display for FormulaPlaneAppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "empty FormulaPlane append batch"),
            Self::InvalidSheet(id) => write!(f, "invalid FormulaPlane sheet id {id}"),
            Self::InvalidAst(id) => write!(f, "invalid FormulaPlane AST id {id:?}"),
            Self::InvalidTemplateOrigin { row, col } => {
                write!(f, "invalid FormulaPlane template origin {row}:{col}")
            }
            Self::InvalidDomain => write!(f, "invalid FormulaPlane placement domain"),
            Self::InvalidResultBinding => write!(f, "FormulaPlane result/domain binding mismatch"),
            Self::InvalidReadSummary => write!(f, "invalid FormulaPlane read summary"),
            Self::InvalidBindingSet => write!(f, "invalid FormulaPlane binding set"),
            Self::DuplicateDependency => write!(f, "duplicate FormulaPlane read dependency"),
            Self::DuplicateDomain => write!(f, "duplicate FormulaPlane append domain"),
            Self::BatchOverlap => write!(f, "overlapping FormulaPlane append domains"),
            Self::SpanOwnershipConflict => write!(f, "FormulaPlane span ownership conflict"),
            Self::OverlayConflict => write!(f, "FormulaPlane overlay conflict"),
            Self::IdentifierExhausted(kind) => write!(f, "FormulaPlane {kind} ids exhausted"),
            Self::Stale => write!(f, "prepared FormulaPlane append is stale"),
            #[cfg(test)]
            Self::InjectedFinalCheckFailure => {
                write!(f, "injected FormulaPlane append final-check failure")
            }
        }
    }
}

impl std::error::Error for FormulaPlaneAppendError {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExistingTemplateAssumption {
    id: FormulaTemplateId,
    generation: u32,
    version: u32,
    parameterized_canonical_key: Arc<str>,
}

#[derive(Debug)]
struct PreparedTemplateAppend {
    id: FormulaTemplateId,
    exact_canonical_key: Arc<str>,
    parameterized_canonical_key: Arc<str>,
    ast_id: AstNodeId,
    origin_row: u32,
    origin_col: u32,
    formula_text: Option<Arc<str>>,
}

#[derive(Debug)]
struct PreparedSpanAppend {
    template_id: FormulaTemplateId,
    sheet_id: SheetId,
    domain: PlacementDomain,
    result_region: ResultRegion,
    read_summary: SpanReadSummary,
    read_deltas: Vec<SpanReadDependency>,
    binding_set: SpanBindingSet,
    relocation: SpanAstRelocation,
    is_constant_result: bool,
    resolved_named_refs: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct PreparedFormulaPlaneAppend {
    expected_plane_epoch: FormulaPlaneEpoch,
    expected_template_epoch: u64,
    expected_span_epoch: u64,
    expected_summary_epoch: u64,
    expected_binding_epoch: u64,
    expected_overlay_epoch: u64,
    expected_indexes_epoch: u64,
    expected_producer_epoch: u64,
    expected_consumer_epoch: u64,
    expected_domain_epoch: u64,
    expected_overlay_index_epoch: u64,
    expected_template_len: usize,
    expected_span_len: usize,
    expected_summary_len: usize,
    expected_binding_len: usize,
    checked_sheets: Vec<(SheetId, String)>,
    checked_asts: Vec<AstNodeId>,
    existing_templates: Vec<ExistingTemplateAssumption>,
    templates: Vec<PreparedTemplateAppend>,
    spans: Vec<PreparedSpanAppend>,
    work: FormulaPlaneAppendWork,
}

impl PreparedFormulaPlaneAppend {
    pub(crate) fn work(&self) -> &FormulaPlaneAppendWork {
        &self.work
    }
}

fn checked_sheet_name(
    registry: &SheetRegistry,
    id: SheetId,
) -> Result<String, FormulaPlaneAppendError> {
    let name = registry.name(id);
    if name.is_empty() || registry.get_id(name) != Some(id) {
        Err(FormulaPlaneAppendError::InvalidSheet(id))
    } else {
        Ok(name.to_string())
    }
}

fn axis_valid(axis: AxisRange, max: u32) -> bool {
    match axis {
        AxisRange::Point(value) | AxisRange::From(value) | AxisRange::To(value) => value <= max,
        AxisRange::Span(start, end) => start <= end && end <= max,
        AxisRange::All => true,
    }
}

fn region_valid(region: Region) -> bool {
    let (rows, cols) = region.axis_ranges();
    axis_valid(rows, PackedSheetCell::MAX_ROW0) && axis_valid(cols, PackedSheetCell::MAX_COL0)
}

fn domain_valid(domain: &PlacementDomain) -> bool {
    match domain {
        PlacementDomain::RowRun {
            row_start,
            row_end,
            col,
            ..
        } => {
            row_start <= row_end
                && *row_end <= PackedSheetCell::MAX_ROW0
                && *col <= PackedSheetCell::MAX_COL0
        }
        PlacementDomain::ColRun {
            row,
            col_start,
            col_end,
            ..
        } => {
            *row <= PackedSheetCell::MAX_ROW0
                && col_start <= col_end
                && *col_end <= PackedSheetCell::MAX_COL0
        }
        PlacementDomain::Rect {
            row_start,
            row_end,
            col_start,
            col_end,
            ..
        } => {
            row_start <= row_end
                && col_start <= col_end
                && *row_end <= PackedSheetCell::MAX_ROW0
                && *col_end <= PackedSheetCell::MAX_COL0
        }
    }
}

fn checked_reserved_id(
    base: usize,
    offset: usize,
    kind: &'static str,
) -> Result<u32, FormulaPlaneAppendError> {
    let value = base
        .checked_add(offset)
        .ok_or(FormulaPlaneAppendError::IdentifierExhausted(kind))?;
    u32::try_from(value).map_err(|_| FormulaPlaneAppendError::IdentifierExhausted(kind))
}

fn binding_valid(binding: &SpanBindingSet, domain: &PlacementDomain) -> bool {
    if binding.template_origin_row == 0 || binding.template_origin_col == 0 {
        return false;
    }
    let literal_slot_ids: FxHashSet<_> = binding
        .literal_slots
        .iter()
        .map(|slot| slot.slot_id)
        .collect();
    let value_slot_ids: FxHashSet<_> = binding
        .value_ref_slots
        .iter()
        .map(|slot| slot.slot_id)
        .collect();
    let literal_slot_count = binding.literal_slots.len();
    if literal_slot_ids.len() != literal_slot_count
        || value_slot_ids.len() != binding.value_ref_slots.len()
        || binding
            .unique_literal_bindings
            .iter()
            .any(|values| values.len() != literal_slot_count)
    {
        return false;
    }
    match &binding.literal_binding_encoding {
        LiteralBindingEncoding::Broadcast => binding.unique_literal_bindings.len() == 1,
        LiteralBindingEncoding::Dictionary => {
            usize::try_from(domain.cell_count()).ok()
                == Some(binding.placement_literal_binding_ids.len())
                && binding
                    .placement_literal_binding_ids
                    .iter()
                    .all(|id| (*id as usize) < binding.unique_literal_bindings.len())
        }
        LiteralBindingEncoding::AffineByRow { base, steps, .. }
        | LiteralBindingEncoding::AffineByCol { base, steps, .. } => {
            base.len() == literal_slot_count
                && base.len() == steps.len()
                && affine_extremes_valid(binding, domain)
        }
        LiteralBindingEncoding::AffineRect {
            base,
            row_steps,
            col_steps,
            ..
        } => {
            base.len() == literal_slot_count
                && base.len() == row_steps.len()
                && base.len() == col_steps.len()
                && affine_extremes_valid(binding, domain)
        }
    }
}

fn affine_extremes_valid(binding: &SpanBindingSet, domain: &PlacementDomain) -> bool {
    let (sheet_id, row_start, row_end, col_start, col_end) = match domain {
        PlacementDomain::RowRun {
            sheet_id,
            row_start,
            row_end,
            col,
        } => (*sheet_id, *row_start, *row_end, *col, *col),
        PlacementDomain::ColRun {
            sheet_id,
            row,
            col_start,
            col_end,
        } => (*sheet_id, *row, *row, *col_start, *col_end),
        PlacementDomain::Rect {
            sheet_id,
            row_start,
            row_end,
            col_start,
            col_end,
        } => (*sheet_id, *row_start, *row_end, *col_start, *col_end),
    };
    [
        PlacementCoord::new(sheet_id, row_start, col_start),
        PlacementCoord::new(sheet_id, row_start, col_end),
        PlacementCoord::new(sheet_id, row_end, col_start),
        PlacementCoord::new(sheet_id, row_end, col_end),
    ]
    .into_iter()
    .all(|coord| {
        binding
            .literal_bindings_for_placement(domain, coord)
            .is_some()
    })
}

fn read_summary_valid(
    summary: &SpanReadSummary,
    expected: Region,
) -> Result<(), FormulaPlaneAppendError> {
    if summary.result_region != expected || !region_valid(summary.result_region) {
        return Err(FormulaPlaneAppendError::InvalidReadSummary);
    }
    let mut seen = Vec::with_capacity(summary.dependencies.len());
    for dependency in &summary.dependencies {
        if !region_valid(dependency.read_region) {
            return Err(FormulaPlaneAppendError::InvalidReadSummary);
        }
        if seen.contains(dependency) {
            return Err(FormulaPlaneAppendError::DuplicateDependency);
        }
        if dependency.projection != DirtyProjectionRule::WholeResult {
            let projected = dependency
                .projection
                .read_regions_for_result(dependency.read_region.sheet_id(), expected)
                .map_err(|_| FormulaPlaneAppendError::InvalidReadSummary)?;
            if !projected.contains(&dependency.read_region) {
                return Err(FormulaPlaneAppendError::InvalidReadSummary);
            }
        }
        seen.push(dependency.clone());
    }
    Ok(())
}

impl FormulaPlaneAppendPlacement {
    fn from_anchor_once_proof(
        prepared: PreparedAnchorOncePlacement,
    ) -> Result<Self, FormulaPlaneAppendError> {
        let (
            candidate,
            exact_canonical_key,
            parameterized_canonical_key,
            resolved_named_refs,
            domain,
            result_region,
            read_summary,
            binding_set,
            is_constant_result,
        ) = prepared.into_append_proof_parts();
        let origin_row =
            candidate
                .row
                .checked_add(1)
                .ok_or(FormulaPlaneAppendError::InvalidTemplateOrigin {
                    row: candidate.row,
                    col: candidate.col,
                })?;
        let origin_col =
            candidate
                .col
                .checked_add(1)
                .ok_or(FormulaPlaneAppendError::InvalidTemplateOrigin {
                    row: candidate.row,
                    col: candidate.col,
                })?;
        let mut seen_names = FxHashSet::default();
        let resolved_named_refs = resolved_named_refs
            .into_iter()
            .map(|name| name.to_lowercase())
            .filter(|name| seen_names.insert(name.clone()))
            .collect();
        Ok(Self {
            sheet_id: candidate.sheet_id,
            exact_canonical_key,
            parameterized_canonical_key,
            ast_id: candidate.ast_id,
            origin_row,
            origin_col,
            formula_text: candidate.formula_text,
            domain,
            result_region,
            read_summary,
            binding_set,
            is_constant_result,
            resolved_named_refs,
        })
    }
}

impl FormulaAuthority {
    /// Prepare an append exclusively from sealed, one-analysis placement proofs.
    /// Canonical keys and normalized name dependencies are not accepted from callers.
    pub(crate) fn prepare_formula_plane_append(
        &self,
        placements: Vec<PreparedAnchorOncePlacement>,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<PreparedFormulaPlaneAppend, FormulaPlaneAppendError> {
        let placements = placements
            .into_iter()
            .map(FormulaPlaneAppendPlacement::from_anchor_once_proof)
            .collect::<Result<Vec<_>, _>>()?;
        self.prepare_formula_plane_append_inputs(placements, data_store, sheet_registry)
    }

    #[cfg(test)]
    fn prepare_formula_plane_append_for_test(
        &self,
        placements: Vec<FormulaPlaneAppendPlacement>,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<PreparedFormulaPlaneAppend, FormulaPlaneAppendError> {
        self.prepare_formula_plane_append_inputs(placements, data_store, sheet_registry)
    }

    fn prepare_formula_plane_append_inputs(
        &self,
        placements: Vec<FormulaPlaneAppendPlacement>,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<PreparedFormulaPlaneAppend, FormulaPlaneAppendError> {
        if placements.is_empty() {
            return Err(FormulaPlaneAppendError::Empty);
        }
        if self.indexed_plane_epoch != self.plane.epoch().0
            || self.overlays.built_from_overlay_epoch() != self.plane.formula_overlay.epoch()
        {
            return Err(FormulaPlaneAppendError::Stale);
        }

        let mut work = FormulaPlaneAppendWork {
            placements: placements.len(),
            result_index_entries: placements.len(),
            ..FormulaPlaneAppendWork::default()
        };
        let mut checked_sheets = FxHashMap::default();
        let mut checked_asts = FxHashSet::default();
        let mut existing_templates = Vec::new();
        let mut existing_template_ids = FxHashSet::default();
        let mut template_ids = FxHashMap::<Arc<str>, FormulaTemplateId>::default();
        let mut templates = Vec::new();
        let mut local_domains = SpanDomainIndex::default();
        let mut exact_domains = FxHashSet::default();
        let mut spans = Vec::with_capacity(placements.len());

        for (offset, mut placement) in placements.into_iter().enumerate() {
            if placement.sheet_id != placement.domain.sheet_id() || !domain_valid(&placement.domain)
            {
                return Err(FormulaPlaneAppendError::InvalidDomain);
            }
            checked_sheets
                .entry(placement.sheet_id)
                .or_insert(checked_sheet_name(sheet_registry, placement.sheet_id)?);
            if placement.result_region.domain() != &placement.domain {
                return Err(FormulaPlaneAppendError::InvalidResultBinding);
            }
            let origin_row0 = placement.origin_row.checked_sub(1).ok_or(
                FormulaPlaneAppendError::InvalidTemplateOrigin {
                    row: placement.origin_row,
                    col: placement.origin_col,
                },
            )?;
            let origin_col0 = placement.origin_col.checked_sub(1).ok_or(
                FormulaPlaneAppendError::InvalidTemplateOrigin {
                    row: placement.origin_row,
                    col: placement.origin_col,
                },
            )?;
            if PackedSheetCell::try_new(placement.sheet_id, origin_row0, origin_col0).is_none() {
                return Err(FormulaPlaneAppendError::InvalidTemplateOrigin {
                    row: placement.origin_row,
                    col: placement.origin_col,
                });
            }
            if data_store.get_node(placement.ast_id).is_none() {
                return Err(FormulaPlaneAppendError::InvalidAst(placement.ast_id));
            }
            checked_asts.insert(placement.ast_id);

            let result_region = Region::from_domain(&placement.domain);
            read_summary_valid(&placement.read_summary, result_region)?;
            for dependency in &placement.read_summary.dependencies {
                checked_sheets
                    .entry(dependency.read_region.sheet_id())
                    .or_insert(checked_sheet_name(
                        sheet_registry,
                        dependency.read_region.sheet_id(),
                    )?);
            }
            work.read_dependencies_checked = work
                .read_dependencies_checked
                .checked_add(placement.read_summary.dependencies.len())
                .ok_or(FormulaPlaneAppendError::IdentifierExhausted("work counter"))?;
            work.read_index_entries = work.read_dependencies_checked;

            if placement.binding_set.template_ast_id != placement.ast_id
                || placement.binding_set.template_origin_row != placement.origin_row
                || placement.binding_set.template_origin_col != placement.origin_col
                || !binding_valid(&placement.binding_set, &placement.domain)
            {
                return Err(FormulaPlaneAppendError::InvalidBindingSet);
            }

            if !exact_domains.insert(result_region) {
                return Err(FormulaPlaneAppendError::DuplicateDomain);
            }
            work.span_domains_checked += 1;
            let local = local_domains.find_intersections(result_region);
            if !local.matches.is_empty() {
                return Err(FormulaPlaneAppendError::BatchOverlap);
            }
            let existing = self.span_domains.find_intersections(result_region);
            work.existing_span_candidates_checked = work
                .existing_span_candidates_checked
                .checked_add(existing.stats.candidate_count)
                .ok_or(FormulaPlaneAppendError::IdentifierExhausted("work counter"))?;
            if !existing.matches.is_empty() {
                return Err(FormulaPlaneAppendError::SpanOwnershipConflict);
            }
            let overlays = self.overlays.find_intersections(result_region);
            work.overlay_candidates_checked = work
                .overlay_candidates_checked
                .checked_add(overlays.stats.candidate_count)
                .ok_or(FormulaPlaneAppendError::IdentifierExhausted("work counter"))?;
            if !overlays.matches.is_empty() {
                return Err(FormulaPlaneAppendError::OverlayConflict);
            }

            let template_id = if let Some(id) = template_ids
                .get(&placement.parameterized_canonical_key)
                .copied()
            {
                work.templates_reused += 1;
                id
            } else if let Some(id) = self
                .plane
                .templates
                .interned_id(&placement.parameterized_canonical_key)
            {
                let record = self
                    .plane
                    .templates
                    .get(id)
                    .ok_or(FormulaPlaneAppendError::Stale)?;
                if existing_template_ids.insert(id) {
                    existing_templates.push(ExistingTemplateAssumption {
                        id,
                        generation: record.generation,
                        version: record.version,
                        parameterized_canonical_key: Arc::clone(
                            &placement.parameterized_canonical_key,
                        ),
                    });
                }
                template_ids.insert(Arc::clone(&placement.parameterized_canonical_key), id);
                work.templates_reused += 1;
                id
            } else {
                let id = FormulaTemplateId(checked_reserved_id(
                    self.plane.templates.len(),
                    templates.len(),
                    "template",
                )?);
                template_ids.insert(Arc::clone(&placement.parameterized_canonical_key), id);
                templates.push(PreparedTemplateAppend {
                    id,
                    exact_canonical_key: Arc::clone(&placement.exact_canonical_key),
                    parameterized_canonical_key: Arc::clone(&placement.parameterized_canonical_key),
                    ast_id: placement.ast_id,
                    origin_row: placement.origin_row,
                    origin_col: placement.origin_col,
                    formula_text: placement.formula_text.clone(),
                });
                work.templates_to_append += 1;
                id
            };

            let span_ref = FormulaSpanRef {
                id: FormulaSpanId(checked_reserved_id(
                    self.plane.spans.slot_len(),
                    offset,
                    "span",
                )?),
                generation: 0,
                version: 0,
            };
            checked_reserved_id(self.plane.binding_sets.slot_len(), offset, "binding set")?;
            checked_reserved_id(
                self.plane.span_read_summaries.slot_len(),
                offset,
                "read summary",
            )?;
            placement.binding_set.span_ref = span_ref;
            let read_deltas = placement.read_summary.dependencies.clone();
            local_domains.insert_domain(span_ref, placement.domain.clone());
            spans.push(PreparedSpanAppend {
                template_id,
                sheet_id: placement.sheet_id,
                domain: placement.domain,
                result_region: placement.result_region,
                read_summary: placement.read_summary,
                read_deltas,
                binding_set: placement.binding_set,
                relocation: SpanAstRelocation {
                    ast_id: placement.ast_id,
                    anchor_row: placement.origin_row,
                    anchor_col: placement.origin_col,
                },
                is_constant_result: placement.is_constant_result,
                resolved_named_refs: placement.resolved_named_refs,
            });
        }

        Ok(PreparedFormulaPlaneAppend {
            expected_plane_epoch: self.plane.epoch(),
            expected_template_epoch: self.plane.templates.epoch(),
            expected_span_epoch: self.plane.spans.epoch(),
            expected_summary_epoch: self.plane.span_read_summaries.epoch(),
            expected_binding_epoch: self.plane.binding_sets.epoch(),
            expected_overlay_epoch: self.plane.formula_overlay.epoch(),
            expected_indexes_epoch: self.indexes_epoch,
            expected_producer_epoch: self.producer_results.epoch(),
            expected_consumer_epoch: self.consumer_reads.epoch(),
            expected_domain_epoch: self.span_domains.epoch(),
            expected_overlay_index_epoch: self.overlays.epoch(),
            expected_template_len: self.plane.templates.len(),
            expected_span_len: self.plane.spans.slot_len(),
            expected_summary_len: self.plane.span_read_summaries.slot_len(),
            expected_binding_len: self.plane.binding_sets.slot_len(),
            checked_sheets: checked_sheets.into_iter().collect(),
            checked_asts: checked_asts.into_iter().collect(),
            existing_templates,
            templates,
            spans,
            work,
        })
    }

    /// Final pre-mutation seam used by a future graph + plane coordinator.
    pub(crate) fn validate_prepared_formula_plane_append(
        &self,
        prepared: &PreparedFormulaPlaneAppend,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<(), FormulaPlaneAppendError> {
        #[cfg(test)]
        if self.prepared_append_failure_for_test {
            return Err(FormulaPlaneAppendError::InjectedFinalCheckFailure);
        }
        if self.plane.epoch() != prepared.expected_plane_epoch
            || self.plane.templates.epoch() != prepared.expected_template_epoch
            || self.plane.spans.epoch() != prepared.expected_span_epoch
            || self.plane.span_read_summaries.epoch() != prepared.expected_summary_epoch
            || self.plane.binding_sets.epoch() != prepared.expected_binding_epoch
            || self.plane.formula_overlay.epoch() != prepared.expected_overlay_epoch
            || self.indexes_epoch != prepared.expected_indexes_epoch
            || self.producer_results.epoch() != prepared.expected_producer_epoch
            || self.consumer_reads.epoch() != prepared.expected_consumer_epoch
            || self.span_domains.epoch() != prepared.expected_domain_epoch
            || self.overlays.epoch() != prepared.expected_overlay_index_epoch
            || self.plane.templates.len() != prepared.expected_template_len
            || self.plane.spans.slot_len() != prepared.expected_span_len
            || self.plane.span_read_summaries.slot_len() != prepared.expected_summary_len
            || self.plane.binding_sets.slot_len() != prepared.expected_binding_len
            || self.indexed_plane_epoch != self.plane.epoch().0
        {
            return Err(FormulaPlaneAppendError::Stale);
        }
        for (id, name) in &prepared.checked_sheets {
            if sheet_registry.name(*id) != name || sheet_registry.get_id(name) != Some(*id) {
                return Err(FormulaPlaneAppendError::Stale);
            }
        }
        if prepared
            .checked_asts
            .iter()
            .any(|id| data_store.get_node(*id).is_none())
        {
            return Err(FormulaPlaneAppendError::Stale);
        }
        for assumption in &prepared.existing_templates {
            let Some(record) = self.plane.templates.get(assumption.id) else {
                return Err(FormulaPlaneAppendError::Stale);
            };
            if record.generation != assumption.generation
                || record.version != assumption.version
                || record.parameterized_canonical_key != assumption.parameterized_canonical_key
                || self
                    .plane
                    .templates
                    .interned_id(&assumption.parameterized_canonical_key)
                    != Some(assumption.id)
            {
                return Err(FormulaPlaneAppendError::Stale);
            }
        }
        Ok(())
    }

    pub(crate) fn commit_prepared_formula_plane_append(
        &mut self,
        prepared: PreparedFormulaPlaneAppend,
        data_store: &DataStore,
        sheet_registry: &SheetRegistry,
    ) -> Result<FormulaPlaneAppendReport, FormulaPlaneAppendError> {
        self.validate_prepared_formula_plane_append(&prepared, data_store, sheet_registry)?;
        Ok(self.apply_prevalidated_formula_plane_append(prepared))
    }

    /// Infallible after `validate_prepared_formula_plane_append` succeeds and
    /// while the caller retains exclusive access to the checked stores.
    pub(crate) fn apply_prevalidated_formula_plane_append(
        &mut self,
        prepared: PreparedFormulaPlaneAppend,
    ) -> FormulaPlaneAppendReport {
        let PreparedFormulaPlaneAppend {
            templates,
            mut spans,
            work,
            ..
        } = prepared;
        let mut committed_spans = Vec::with_capacity(spans.len());
        let mut template_ids = Vec::with_capacity(spans.len());
        for template in templates {
            let reserved_id = template.id;
            let actual_id = self.plane.intern_template_parameterized(
                template.exact_canonical_key,
                template.parameterized_canonical_key,
                template.ast_id,
                template.origin_row,
                template.origin_col,
                template.formula_text,
            );
            for span in &mut spans {
                if span.template_id == reserved_id {
                    span.template_id = actual_id;
                }
            }
        }

        for span in spans {
            let binding_id = self.plane.insert_binding_set(span.binding_set);
            let summary_id = self.plane.insert_span_read_summary(span.read_summary);
            let span_ref = self.plane.insert_span_with_ast_relocation(
                NewFormulaSpan {
                    sheet_id: span.sheet_id,
                    template_id: span.template_id,
                    domain: span.domain.clone(),
                    result_region: span.result_region,
                    intrinsic_mask_id: None,
                    read_summary_id: Some(summary_id),
                    binding_set_id: Some(binding_id),
                    is_constant_result: span.is_constant_result,
                },
                span.relocation,
            );
            self.plane.set_binding_span_ref(binding_id, span_ref);
            if !span.resolved_named_refs.is_empty() {
                self.plane
                    .register_fresh_span_name_dependents(span_ref, &span.resolved_named_refs);
            }

            let result_region = Region::from_domain(&span.domain);
            let producer = FormulaProducerId::Span(span_ref.id);
            self.span_domains.insert_domain(span_ref, span.domain);
            self.producer_results
                .insert_producer(producer, result_region);
            for dependency in span.read_deltas {
                self.consumer_reads.insert_read(
                    producer,
                    dependency.read_region,
                    result_region,
                    dependency.projection,
                );
            }
            committed_spans.push(span_ref);
            template_ids.push(span.template_id);
        }
        self.indexes_epoch = self.indexes_epoch.saturating_add(1);
        self.indexed_plane_epoch = self.plane.epoch().0;
        FormulaPlaneAppendReport {
            spans: committed_spans,
            template_ids,
            work,
        }
    }
}

#[cfg(test)]
mod tests {
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::parse;

    use super::*;
    use crate::engine::arena::DataStore;
    use crate::formula_plane::producer::{AxisProjection, ProducerDirtyDomain, ProjectionResult};
    use crate::formula_plane::runtime::{
        FormulaResolution, TemplateSlotMap, ValueRefSlotDescriptor,
    };

    fn fixture() -> (
        FormulaAuthority,
        DataStore,
        SheetRegistry,
        SheetId,
        AstNodeId,
    ) {
        let authority = FormulaAuthority::default();
        let mut registry = SheetRegistry::new();
        let sheet = registry.id_for("Sheet1");
        let mut data_store = DataStore::new();
        let ast = data_store.store_ast(&parse("=A1").unwrap(), &registry);
        (authority, data_store, registry, sheet, ast)
    }

    fn projection(col_offset: i64) -> DirtyProjectionRule {
        DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: col_offset },
        }
    }

    fn placement(
        sheet: SheetId,
        ast_id: AstNodeId,
        key: &str,
        domain: PlacementDomain,
        dependencies: Vec<SpanReadDependency>,
    ) -> FormulaPlaneAppendPlacement {
        let result_region = Region::from_domain(&domain);
        let origin = domain.iter().next().unwrap();
        FormulaPlaneAppendPlacement {
            sheet_id: sheet,
            exact_canonical_key: Arc::from(key),
            parameterized_canonical_key: Arc::from(key),
            ast_id,
            origin_row: origin.row + 1,
            origin_col: origin.col + 1,
            formula_text: Some(Arc::from("=A1")),
            domain: domain.clone(),
            result_region: ResultRegion::scalar_cells(domain),
            read_summary: SpanReadSummary {
                result_region,
                dependencies,
            },
            binding_set: SpanBindingSet {
                span_ref: FormulaSpanRef {
                    id: FormulaSpanId(0),
                    generation: 0,
                    version: 0,
                },
                template_ast_id: ast_id,
                template_origin_row: origin.row + 1,
                template_origin_col: origin.col + 1,
                literal_slots: Arc::from([]),
                unique_literal_bindings: vec![Box::default()],
                placement_literal_binding_ids: Box::default(),
                literal_binding_encoding: LiteralBindingEncoding::Broadcast,
                value_ref_slots: Arc::<[ValueRefSlotDescriptor]>::from([]),
                template_slot_map: TemplateSlotMap::default(),
            },
            is_constant_result: false,
            resolved_named_refs: Vec::new(),
        }
    }

    fn state(authority: &FormulaAuthority) -> String {
        format!("{authority:?}")
    }

    #[test]
    fn invalid_duplicate_overlap_and_overlay_conflicts_do_not_mutate() {
        let (mut authority, data_store, registry, sheet, ast) = fixture();

        let invalid_domain = PlacementDomain::row_run(sheet, 9, 8, 1);
        let mut invalid = placement(
            sheet,
            ast,
            "invalid",
            PlacementDomain::row_run(sheet, 8, 9, 1),
            vec![],
        );
        invalid.domain = invalid_domain.clone();
        invalid.result_region = ResultRegion::scalar_cells(invalid_domain);
        let before = state(&authority);
        assert_eq!(
            authority
                .prepare_formula_plane_append_for_test(vec![invalid], &data_store, &registry)
                .unwrap_err(),
            FormulaPlaneAppendError::InvalidDomain
        );
        assert_eq!(state(&authority), before);

        let domain = PlacementDomain::row_run(sheet, 0, 9, 2);
        let before = state(&authority);
        assert_eq!(
            authority
                .prepare_formula_plane_append_for_test(
                    vec![
                        placement(sheet, ast, "dup-a", domain.clone(), vec![]),
                        placement(sheet, ast, "dup-b", domain.clone(), vec![]),
                    ],
                    &data_store,
                    &registry,
                )
                .unwrap_err(),
            FormulaPlaneAppendError::DuplicateDomain
        );
        assert_eq!(state(&authority), before);

        let first = authority
            .prepare_formula_plane_append_for_test(
                vec![placement(sheet, ast, "owned", domain, vec![])],
                &data_store,
                &registry,
            )
            .unwrap();
        authority
            .commit_prepared_formula_plane_append(first, &data_store, &registry)
            .unwrap();
        let before = state(&authority);
        assert_eq!(
            authority
                .prepare_formula_plane_append_for_test(
                    vec![placement(
                        sheet,
                        ast,
                        "overlap",
                        PlacementDomain::rect(sheet, 5, 12, 2, 3),
                        vec![],
                    )],
                    &data_store,
                    &registry,
                )
                .unwrap_err(),
            FormulaPlaneAppendError::SpanOwnershipConflict
        );
        assert_eq!(state(&authority), before);

        authority.plane.insert_overlay(
            sheet,
            PlacementDomain::row_run(sheet, 20, 20, 4),
            super::super::runtime::FormulaOverlayEntryKind::ValueOverride,
            None,
        );
        authority.rebuild_indexes();
        let before = state(&authority);
        assert_eq!(
            authority
                .prepare_formula_plane_append_for_test(
                    vec![placement(
                        sheet,
                        ast,
                        "overlay",
                        PlacementDomain::row_run(sheet, 20, 25, 4),
                        vec![],
                    )],
                    &data_store,
                    &registry,
                )
                .unwrap_err(),
            FormulaPlaneAppendError::OverlayConflict
        );
        assert_eq!(state(&authority), before);
    }

    #[test]
    fn checked_sheet_ast_generation_and_arithmetic_fail_before_mutation() {
        let (mut authority, data_store, registry, sheet, ast) = fixture();
        let domain = PlacementDomain::row_run(sheet, 0, 9, 2);

        let before = state(&authority);
        let mut bad_sheet = placement(
            99,
            ast,
            "sheet",
            PlacementDomain::row_run(99, 0, 9, 2),
            vec![],
        );
        bad_sheet.result_region = ResultRegion::scalar_cells(PlacementDomain::row_run(99, 0, 8, 2));
        assert_eq!(
            authority
                .prepare_formula_plane_append_for_test(vec![bad_sheet], &data_store, &registry)
                .unwrap_err(),
            FormulaPlaneAppendError::InvalidSheet(99)
        );
        let invalid_ast = AstNodeId::from_u32(u32::MAX);
        assert_eq!(
            authority
                .prepare_formula_plane_append_for_test(
                    vec![placement(sheet, invalid_ast, "ast", domain.clone(), vec![])],
                    &data_store,
                    &registry,
                )
                .unwrap_err(),
            FormulaPlaneAppendError::InvalidAst(invalid_ast)
        );
        assert_eq!(state(&authority), before);
        assert_eq!(
            checked_reserved_id(usize::MAX, 1, "span"),
            Err(FormulaPlaneAppendError::IdentifierExhausted("span"))
        );

        let first = authority
            .prepare_formula_plane_append_for_test(
                vec![placement(sheet, ast, "shared", domain, vec![])],
                &data_store,
                &registry,
            )
            .unwrap();
        let first = authority
            .commit_prepared_formula_plane_append(first, &data_store, &registry)
            .unwrap();
        let next = authority
            .prepare_formula_plane_append_for_test(
                vec![placement(
                    sheet,
                    ast,
                    "shared",
                    PlacementDomain::row_run(sheet, 20, 29, 2),
                    vec![],
                )],
                &data_store,
                &registry,
            )
            .unwrap();
        authority
            .plane
            .templates
            .get_mut_for_test(first.template_ids[0])
            .unwrap()
            .generation += 1;
        let before = state(&authority);
        assert_eq!(
            authority.commit_prepared_formula_plane_append(next, &data_store, &registry),
            Err(FormulaPlaneAppendError::Stale)
        );
        assert_eq!(state(&authority), before);

        let key_plan = authority
            .prepare_formula_plane_append_for_test(
                vec![placement(
                    sheet,
                    ast,
                    "shared",
                    PlacementDomain::row_run(sheet, 60, 69, 2),
                    vec![],
                )],
                &data_store,
                &registry,
            )
            .unwrap();
        authority
            .plane
            .templates
            .get_mut_for_test(first.template_ids[0])
            .unwrap()
            .parameterized_canonical_key = Arc::from("forged-after-prepare");
        let before = state(&authority);
        assert_eq!(
            authority.commit_prepared_formula_plane_append(key_plan, &data_store, &registry),
            Err(FormulaPlaneAppendError::Stale)
        );
        assert_eq!(state(&authority), before);

        let mut overflow = placement(
            sheet,
            ast,
            "overflow",
            PlacementDomain::row_run(sheet, 40, 49, 2),
            vec![],
        );
        overflow.binding_set.literal_binding_encoding = LiteralBindingEncoding::AffineByRow {
            origin_row: 0,
            base: Box::new([LiteralValue::Int(i64::MAX)]),
            steps: Box::new([1]),
        };
        overflow.binding_set.unique_literal_bindings.clear();
        let before = state(&authority);
        assert_eq!(
            authority
                .prepare_formula_plane_append_for_test(vec![overflow], &data_store, &registry)
                .unwrap_err(),
            FormulaPlaneAppendError::InvalidBindingSet
        );
        assert_eq!(state(&authority), before);
    }

    #[test]
    fn malformed_literal_binding_shapes_fail_before_mutation() {
        let (authority, data_store, registry, sheet, ast) = fixture();
        let mut malformed = Vec::new();

        let mut broadcast = placement(
            sheet,
            ast,
            "bad-broadcast",
            PlacementDomain::row_run(sheet, 0, 9, 2),
            vec![],
        );
        broadcast.binding_set.unique_literal_bindings = vec![Box::new([LiteralValue::Int(1)])];
        malformed.push(broadcast);

        let mut dictionary = placement(
            sheet,
            ast,
            "bad-dictionary",
            PlacementDomain::row_run(sheet, 20, 29, 2),
            vec![],
        );
        dictionary.binding_set.literal_binding_encoding = LiteralBindingEncoding::Dictionary;
        dictionary.binding_set.unique_literal_bindings = vec![Box::new([LiteralValue::Int(1)])];
        dictionary.binding_set.placement_literal_binding_ids = vec![0; 10].into_boxed_slice();
        malformed.push(dictionary);

        let mut dictionary_id = placement(
            sheet,
            ast,
            "bad-dictionary-id",
            PlacementDomain::row_run(sheet, 30, 39, 2),
            vec![],
        );
        dictionary_id.binding_set.literal_binding_encoding = LiteralBindingEncoding::Dictionary;
        dictionary_id.binding_set.placement_literal_binding_ids = vec![1; 10].into_boxed_slice();
        malformed.push(dictionary_id);

        let mut affine = placement(
            sheet,
            ast,
            "bad-affine",
            PlacementDomain::row_run(sheet, 40, 49, 2),
            vec![],
        );
        affine.binding_set.literal_binding_encoding = LiteralBindingEncoding::AffineByRow {
            origin_row: 40,
            base: Box::new([LiteralValue::Int(1)]),
            steps: Box::new([0]),
        };
        malformed.push(affine);

        for placement in malformed {
            let before = state(&authority);
            assert_eq!(
                authority
                    .prepare_formula_plane_append_for_test(vec![placement], &data_store, &registry,)
                    .unwrap_err(),
                FormulaPlaneAppendError::InvalidBindingSet
            );
            assert_eq!(state(&authority), before);
        }
    }

    #[test]
    fn production_append_consumes_sealed_anchor_once_proof() {
        let (mut authority, data_store, registry, sheet, ast_id) = fixture();
        let ast = parse("=A1").unwrap();
        let candidate = super::super::placement::FormulaPlacementCandidate::new(
            sheet,
            0,
            2,
            ast_id,
            Some(Arc::from("=A1")),
        );
        let analysis =
            super::super::placement::analyze_candidate(&candidate, &ast, &data_store, &registry)
                .unwrap();
        let mut prepared_placement = super::super::placement::prepare_anchor_once_family(
            candidate,
            analysis,
            PlacementDomain::row_run(sheet, 0, 99, 2),
            100,
        )
        .unwrap();
        prepared_placement
            .set_resolved_named_refs_for_test(vec!["Revenue".to_string(), "revenue".to_string()]);

        let prepared = authority
            .prepare_formula_plane_append(vec![prepared_placement], &data_store, &registry)
            .unwrap();
        let report = authority
            .commit_prepared_formula_plane_append(prepared, &data_store, &registry)
            .unwrap();
        assert_eq!(report.spans.len(), 1);
        assert_eq!(
            authority.plane.name_dependent_span_refs("REVENUE"),
            vec![report.spans[0]]
        );
        assert!(matches!(
            authority
                .plane
                .resolve_formula_at(PlacementCoord::new(sheet, 50, 2), None)
                .resolution,
            FormulaResolution::SpanPlacement { .. }
        ));
    }

    #[test]
    fn successful_multi_span_append_resolves_and_indexes_direct_and_range_reads() {
        let (mut authority, data_store, registry, sheet, ast) = fixture();
        let direct_domain = PlacementDomain::row_run(sheet, 100, 199, 2);
        let direct_result = Region::from_domain(&direct_domain);
        let direct_rule = projection(-1);
        let direct_read = direct_rule
            .read_region_for_result(sheet, direct_result)
            .unwrap();

        let range_domain = PlacementDomain::row_run(sheet, 100, 199, 4);
        let range_result = Region::from_domain(&range_domain);
        let range_rule = DirtyProjectionRule::AffineRange {
            row_start: AxisProjection::Relative { offset: 0 },
            row_end: AxisProjection::Relative { offset: 0 },
            col_start: AxisProjection::Relative { offset: -4 },
            col_end: AxisProjection::Relative { offset: -3 },
        };
        let range_read = range_rule
            .read_regions_for_result(sheet, range_result)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let prepared = authority
            .prepare_formula_plane_append_for_test(
                vec![
                    placement(
                        sheet,
                        ast,
                        "direct",
                        direct_domain,
                        vec![SpanReadDependency {
                            read_region: direct_read,
                            projection: direct_rule,
                        }],
                    ),
                    placement(
                        sheet,
                        ast,
                        "range",
                        range_domain,
                        vec![SpanReadDependency {
                            read_region: range_read,
                            projection: range_rule,
                        }],
                    ),
                ],
                &data_store,
                &registry,
            )
            .unwrap();
        let report = authority
            .commit_prepared_formula_plane_append(prepared, &data_store, &registry)
            .unwrap();
        assert_eq!(report.spans.len(), 2);
        assert_eq!(report.work.placements, 2);
        assert_eq!(report.work.read_index_entries, 2);

        let handle = authority
            .plane
            .resolve_formula_at(PlacementCoord::new(sheet, 150, 2), None);
        assert!(matches!(
            handle.resolution,
            FormulaResolution::SpanPlacement { span, .. } if span == report.spans[0]
        ));
        assert_eq!(
            authority
                .producer_results
                .producer_result_region(FormulaProducerId::Span(report.spans[1].id)),
            Some(range_result)
        );

        let direct_dirty = authority
            .consumer_reads
            .query_changed_region(Region::point(sheet, 150, 1));
        assert!(direct_dirty.matches.iter().any(|candidate| {
            candidate.value.consumer == FormulaProducerId::Span(report.spans[0].id)
                && candidate.value.dirty
                    == ProjectionResult::Exact(ProducerDirtyDomain::Cells(vec![
                        super::super::region_index::RegionKey::new(sheet, 150, 2),
                    ]))
        }));
        let range_dirty = authority
            .consumer_reads
            .query_changed_region(Region::point(sheet, 150, 0));
        assert!(range_dirty.matches.iter().any(|candidate| {
            candidate.value.consumer == FormulaProducerId::Span(report.spans[1].id)
                && candidate.value.dirty
                    == ProjectionResult::Exact(ProducerDirtyDomain::Cells(vec![
                        super::super::region_index::RegionKey::new(sheet, 150, 4),
                    ]))
        }));
    }

    #[test]
    fn append_is_incremental_preserves_existing_entries_and_counts_local_work() {
        let (mut authority, data_store, registry, sheet, ast) = fixture();
        let initial = authority
            .prepare_formula_plane_append_for_test(
                vec![placement(
                    sheet,
                    ast,
                    "initial",
                    PlacementDomain::row_run(sheet, 0, 9, 1),
                    vec![],
                )],
                &data_store,
                &registry,
            )
            .unwrap();
        let initial = authority
            .commit_prepared_formula_plane_append(initial, &data_store, &registry)
            .unwrap();
        let existing_ref = initial.spans[0];
        let rebuilds = (
            authority.span_domains.rebuild_count(),
            authority.producer_results.rebuild_count(),
            authority.consumer_reads.rebuild_count(),
            authority.overlays.rebuild_count(),
        );
        let existing_region = authority
            .producer_results
            .producer_result_region(FormulaProducerId::Span(existing_ref.id));

        let next = authority
            .prepare_formula_plane_append_for_test(
                vec![placement(
                    sheet,
                    ast,
                    "next",
                    PlacementDomain::row_run(sheet, 20, 29, 3),
                    vec![],
                )],
                &data_store,
                &registry,
            )
            .unwrap();
        assert_eq!(next.work.placements, 1);
        assert_eq!(next.work.span_domains_checked, 1);
        assert_eq!(next.work.templates_to_append, 1);
        let next = authority
            .commit_prepared_formula_plane_append(next, &data_store, &registry)
            .unwrap();
        assert_eq!(next.work.placements, 1);
        assert_eq!(
            authority.plane.spans.get(existing_ref).unwrap().id,
            existing_ref.id
        );
        assert_eq!(
            authority
                .producer_results
                .producer_result_region(FormulaProducerId::Span(existing_ref.id)),
            existing_region
        );
        assert_eq!(
            (
                authority.span_domains.rebuild_count(),
                authority.producer_results.rebuild_count(),
                authority.consumer_reads.rebuild_count(),
                authority.overlays.rebuild_count(),
            ),
            rebuilds
        );
    }

    #[test]
    fn fresh_common_name_registration_does_not_scan_existing_dependents() {
        let (mut authority, data_store, registry, sheet, ast) = fixture();
        let mut initial = Vec::new();
        for col in 0..64 {
            let mut item = placement(
                sheet,
                ast,
                "common-name-template",
                PlacementDomain::row_run(sheet, 0, 9, col),
                vec![],
            );
            item.resolved_named_refs = vec!["common".to_string()];
            initial.push(item);
        }
        let prepared = authority
            .prepare_formula_plane_append_for_test(initial, &data_store, &registry)
            .unwrap();
        let first = authority
            .commit_prepared_formula_plane_append(prepared, &data_store, &registry)
            .unwrap();
        assert_eq!(authority.plane.fresh_name_registration_work_for_test(), 64);
        assert_eq!(
            authority.plane.name_dependent_span_refs("COMMON"),
            first.spans
        );

        let mut final_item = placement(
            sheet,
            ast,
            "common-name-template",
            PlacementDomain::row_run(sheet, 0, 9, 64),
            vec![],
        );
        final_item.resolved_named_refs = vec!["common".to_string()];
        let work_before = authority.plane.fresh_name_registration_work_for_test();
        let prepared = authority
            .prepare_formula_plane_append_for_test(vec![final_item], &data_store, &registry)
            .unwrap();
        let final_report = authority
            .commit_prepared_formula_plane_append(prepared, &data_store, &registry)
            .unwrap();
        assert_eq!(
            authority.plane.fresh_name_registration_work_for_test() - work_before,
            1,
            "fresh registration work must count new names, not existing bucket entries"
        );
        let all = authority.plane.name_dependent_span_refs("common");
        assert_eq!(all.len(), 65);
        assert!(first.spans.iter().all(|span| all.contains(span)));
        assert!(all.contains(&final_report.spans[0]));
    }

    #[test]
    fn stale_and_injected_final_checks_leave_authority_unchanged() {
        let (mut authority, data_store, registry, sheet, ast) = fixture();
        let prepared = authority
            .prepare_formula_plane_append_for_test(
                vec![placement(
                    sheet,
                    ast,
                    "stale",
                    PlacementDomain::row_run(sheet, 0, 9, 1),
                    vec![],
                )],
                &data_store,
                &registry,
            )
            .unwrap();
        authority
            .plane
            .intern_template(Arc::from("unrelated"), ast, 1, 1, Some(Arc::from("=A1")));
        let before = state(&authority);
        assert_eq!(
            authority.commit_prepared_formula_plane_append(prepared, &data_store, &registry),
            Err(FormulaPlaneAppendError::Stale)
        );
        assert_eq!(state(&authority), before);

        authority.rebuild_indexes();
        let prepared = authority
            .prepare_formula_plane_append_for_test(
                vec![placement(
                    sheet,
                    ast,
                    "fault",
                    PlacementDomain::row_run(sheet, 20, 29, 1),
                    vec![],
                )],
                &data_store,
                &registry,
            )
            .unwrap();
        authority.prepared_append_failure_for_test = true;
        let before = state(&authority);
        assert_eq!(
            authority.commit_prepared_formula_plane_append(prepared, &data_store, &registry),
            Err(FormulaPlaneAppendError::InjectedFinalCheckFailure)
        );
        assert_eq!(state(&authority), before);
    }
}
