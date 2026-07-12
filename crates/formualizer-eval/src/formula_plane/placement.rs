//! Internal FormulaPlane span-placement substrate for FP6.2.
//!
//! This module promotes already-collected candidate formula families into the
//! inert FormulaPlane runtime stores. It does not wire FormulaPlane into graph
//! build, dirty propagation, scheduling, or evaluation.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use rustc_hash::FxHashMap;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

use crate::SheetId;
use crate::engine::arena::{AstNodeData, AstNodeId, DataStore};
use crate::engine::ingest_pipeline::IngestedFormula;
use crate::engine::sheet_registry::SheetRegistry;

use super::dependency_summary::{FormulaClass, summarize_canonical_template};
use super::ids::FormulaTemplateId;
use super::producer::{
    AxisProjection, DirtyProjectionRule, ProjectionFallbackReason, ReadProjection,
    SpanReadDependency, SpanReadSummary,
};
use super::region_index::Region;
use super::runtime::{
    FormulaPlane, FormulaSpanRef, LiteralBindingEncoding, NewFormulaSpan, PlacementCoord,
    PlacementDomain, ResultRegion, SpanBindingSet, TemplateSlotMap, ValueRefSlotDescriptor,
    ValueRefSlotId,
};
use super::template_canonical::{
    AxisRef, CanonicalExpr, CanonicalReference, LiteralSlotDescriptor, SlotContext,
    canonicalize_template, function_argument_slot_context,
};

/// Minimum cell count for a non-constant span to be promoted. Below this
/// threshold the per-span overhead (template intern, scheduler edge,
/// per-task setup) overwhelms the legacy graph path.
///
/// 100 chosen because s025 medium showed 99-cell non-constant spans still
/// 3.3x slower than legacy. Constant-result spans bypass this threshold
/// because broadcast cost is amortized regardless of cell count.
const MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS: u64 = 100;
const MAX_BINDING_SET_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct FormulaPlacementCandidate {
    pub(crate) sheet_id: SheetId,
    pub(crate) row: u32,
    pub(crate) col: u32,
    pub(crate) ast_id: AstNodeId,
    pub(crate) formula_text: Option<Arc<str>>,
}

impl FormulaPlacementCandidate {
    pub(crate) fn new(
        sheet_id: SheetId,
        row: u32,
        col: u32,
        ast_id: AstNodeId,
        formula_text: Option<Arc<str>>,
    ) -> Self {
        Self {
            sheet_id,
            row,
            col,
            ast_id,
            formula_text,
        }
    }

    pub(crate) fn placement(&self) -> PlacementCoord {
        PlacementCoord::new(self.sheet_id, self.row, self.col)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FormulaPlacementResult {
    Legacy {
        placement: PlacementCoord,
        reason: PlacementFallbackReason,
    },
    Span {
        span: FormulaSpanRef,
        template_id: FormulaTemplateId,
        placement: PlacementCoord,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum PlacementFallbackReason {
    EmptyCandidateSet,
    UnsupportedCanonicalTemplate,
    UnsupportedDependencySummary,
    UnsupportedDirtyProjection,
    NonEquivalentTemplate,
    UnsupportedShapeOrGaps,
    SingletonUnique,
    CrossSheetOrSheetMismatch,
    DuplicatePlacement,
    /// A formula contains an explicit sheet reference that could not be resolved
    /// in the current sheet registry. Keep it legacy so the graph evaluator
    /// preserves existing #REF!/missing-sheet diagnostics.
    UnknownSheetBinding,
    /// A formula references a defined name that is undefined in the
    /// placement's scope or whose definition is not a concrete Cell/Range
    /// (Literal/Formula definitions). Legacy evaluation preserves #NAME? and
    /// named-formula semantics.
    UnsupportedNamedReference,
    /// At least one read region intersects the family's own result region.
    /// These families have an internal/self dependency that the current span
    /// runtime cannot evaluate as bounded dirty work and demotes to whole-span
    /// recompute on every change, producing O(N²) edit recalc on chains.
    InternalDependency,
    SmallDomain,
    BindingMemoryCapExceeded,
    /// At least one member of the family participates in a statically-cyclic
    /// SCC. A cycle member must never be span-evaluated (gotcha G8 of the
    /// cycle-architecture track, refs #112): under `CycleDetection::Static` the
    /// cycle stamping would race/overwrite span writes, and under
    /// `CycleDetection::Runtime` SCC members are evaluated by the legacy
    /// `evaluate_scc_unit` path and must stay on the graph. Cross-cell cycles
    /// that pass through a span producer are only observable once the producer
    /// graph exists, so this reason is recorded when the FP mixed scheduler
    /// demotes a cyclic span at schedule-build time rather than at initial
    /// placement.
    CycleMember,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FormulaPlacementReport {
    pub(crate) counters: FormulaPlacementCounters,
    pub(crate) results: Vec<FormulaPlacementResult>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FormulaPlacementCounters {
    pub(crate) formula_cells_seen: u64,
    pub(crate) accepted_span_cells: u64,
    pub(crate) legacy_cells: u64,
    pub(crate) templates_interned: u64,
    pub(crate) spans_created: u64,
    pub(crate) formula_vertices_avoided: u64,
    pub(crate) ast_roots_avoided: u64,
    pub(crate) edge_rows_avoided: u64,
    pub(crate) per_placement_formula_vertices_created: u64,
    pub(crate) per_placement_ast_roots_created: u64,
    pub(crate) per_placement_edge_rows_created: u64,
    pub(crate) fallback_reasons: BTreeMap<PlacementFallbackReason, u64>,
}

pub(crate) fn place_candidate_family(
    plane: &mut FormulaPlane,
    candidates: Vec<FormulaPlacementCandidate>,
    data_store: &DataStore,
    sheet_registry: &SheetRegistry,
) -> FormulaPlacementReport {
    let analyses = match analyze_candidates(&candidates, data_store, sheet_registry) {
        Ok(analyses) => analyses,
        Err(reason) => {
            let mut report = FormulaPlacementReport::default();
            report.counters.formula_cells_seen = candidates.len() as u64;
            mark_all_legacy(&mut report, &candidates, reason);
            return report;
        }
    };
    place_candidate_family_with_analyses(plane, candidates, analyses)
}

pub(crate) fn place_candidate_family_with_analyses(
    plane: &mut FormulaPlane,
    candidates: Vec<FormulaPlacementCandidate>,
    analyses: Vec<CandidateAnalysis>,
) -> FormulaPlacementReport {
    match prepare_family_placement(candidates, analyses) {
        Ok(prepared) => commit_prepared_family(plane, prepared),
        Err(report) => *report,
    }
}

pub(crate) struct PreparedFamilyPlacement {
    candidates: Vec<FormulaPlacementCandidate>,
    analyses: Vec<CandidateAnalysis>,
    domain: PlacementDomain,
    result_region: ResultRegion,
    read_summary: SpanReadSummary,
    binding_set: SpanBindingSet,
    is_constant_result: bool,
}

pub(crate) struct PreparedAnchorOncePlacement {
    candidate: FormulaPlacementCandidate,
    analysis: CandidateAnalysis,
    domain: PlacementDomain,
    result_region: ResultRegion,
    read_summary: SpanReadSummary,
    binding_set: SpanBindingSet,
    is_constant_result: bool,
    pub(crate) member_count: u64,
}

#[derive(Clone)]
pub(crate) struct CandidateAnalysis {
    sheet_id: SheetId,
    row: u32,
    col: u32,
    exact_canonical_hash: u64,
    exact_canonical_key: Arc<str>,
    parameterized_canonical_hash: u64,
    parameterized_canonical_key: Arc<str>,
    literal_slot_descriptors: Arc<[LiteralSlotDescriptor]>,
    literal_bindings: Box<[LiteralValue]>,
    value_ref_slot_descriptors: Arc<[ValueRefSlotDescriptor]>,
    template_slot_map: TemplateSlotMap,
    read_projections: Vec<ReadProjection>,
    read_projections_constant: bool,
    /// Defined names this formula resolved at ingest (raw text). Used to
    /// register accepted spans in the FormulaPlane name-dependents map so
    /// define/update/delete of a name can invalidate them. Empty for
    /// formulas without names (no allocation on the hot path).
    resolved_named_refs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompressedPreparationReport {
    pub(crate) member_count: u64,
    pub(crate) accepted: bool,
    pub(crate) broadcast_binding_bytes: usize,
    pub(crate) fallback_reason: Option<PlacementFallbackReason>,
}

impl CandidateAnalysis {
    pub(crate) fn parameterized_canonical_hash(&self) -> u64 {
        self.parameterized_canonical_hash
    }

    fn placement(&self) -> PlacementCoord {
        PlacementCoord::new(self.sheet_id, self.row, self.col)
    }

    pub(crate) fn from_ingested(
        candidate: &FormulaPlacementCandidate,
        ingested: &IngestedFormula,
    ) -> Result<Self, PlacementFallbackReason> {
        if ingested.labels.rejects != 0 {
            return Err(PlacementFallbackReason::UnsupportedCanonicalTemplate);
        }
        let read_projections =
            ingested
                .read_projections
                .clone()
                .ok_or(match ingested.read_projection_fallback {
                    Some(ProjectionFallbackReason::UnsupportedSheetBinding) => {
                        PlacementFallbackReason::UnknownSheetBinding
                    }
                    Some(ProjectionFallbackReason::NamedReferenceUnsupported) => {
                        PlacementFallbackReason::UnsupportedNamedReference
                    }
                    _ => PlacementFallbackReason::UnsupportedDependencySummary,
                })?;
        let is_constant_result = read_projections
            .iter()
            .all(|read_projection| is_constant_projection(&read_projection.rule));
        Ok(Self {
            sheet_id: candidate.sheet_id,
            row: candidate.row,
            col: candidate.col,
            exact_canonical_hash: ingested.exact_canonical_hash,
            exact_canonical_key: ingested.exact_canonical_key.clone(),
            parameterized_canonical_hash: ingested.parameterized_canonical_hash,
            parameterized_canonical_key: ingested.parameterized_canonical_key.clone(),
            literal_slot_descriptors: ingested.literal_slot_descriptors.clone(),
            literal_bindings: ingested.literal_bindings.clone(),
            value_ref_slot_descriptors: ingested.value_ref_slot_descriptors.clone(),
            template_slot_map: ingested.template_slot_map.clone(),
            read_projections,
            read_projections_constant: is_constant_result,
            resolved_named_refs: ingested.dep_plan.resolved_named_refs.clone(),
        })
    }
}

fn is_constant_projection(rule: &DirtyProjectionRule) -> bool {
    match rule {
        DirtyProjectionRule::AffineCell { row, col } => {
            axis_projection_is_absolute(row) && axis_projection_is_absolute(col)
        }
        DirtyProjectionRule::AffineRange {
            row_start,
            row_end,
            col_start,
            col_end,
        } => {
            axis_projection_is_absolute(row_start)
                && axis_projection_is_absolute(row_end)
                && axis_projection_is_absolute(col_start)
                && axis_projection_is_absolute(col_end)
        }
        DirtyProjectionRule::WholeColumnRange { col_start, col_end } => {
            axis_projection_is_absolute(col_start) && axis_projection_is_absolute(col_end)
        }
        DirtyProjectionRule::WholeResult => false,
    }
}

fn axis_projection_is_absolute(projection: &AxisProjection) -> bool {
    matches!(projection, AxisProjection::Absolute { .. })
}

pub(crate) fn analyze_candidate(
    candidate: &FormulaPlacementCandidate,
    ast: &ASTNode,
    data_store: &DataStore,
    sheet_registry: &SheetRegistry,
) -> Result<CandidateAnalysis, PlacementFallbackReason> {
    let anchor_row = candidate
        .row
        .checked_add(1)
        .ok_or(PlacementFallbackReason::UnsupportedShapeOrGaps)?;
    let anchor_col = candidate
        .col
        .checked_add(1)
        .ok_or(PlacementFallbackReason::UnsupportedShapeOrGaps)?;
    let template = canonicalize_template(ast, anchor_row, anchor_col);
    if !template.labels.is_authority_supported() {
        return Err(PlacementFallbackReason::UnsupportedCanonicalTemplate);
    }
    let summary = summarize_canonical_template(&template);
    if summary.formula_class != FormulaClass::StaticPointwise || !summary.reject_reasons.is_empty()
    {
        return Err(PlacementFallbackReason::UnsupportedDependencySummary);
    }
    let scalar_domain = PlacementDomain::row_run(
        candidate.sheet_id,
        candidate.row,
        candidate.row,
        candidate.col,
    );
    let result_region = ResultRegion::scalar_cells(scalar_domain);
    let mut read_projections = Vec::new();
    for dependency in SpanReadSummary::from_formula_summary(
        candidate.sheet_id,
        &result_region,
        &summary,
        sheet_registry,
    )
    .map_err(|reason| match reason {
        ProjectionFallbackReason::UnsupportedSheetBinding => {
            PlacementFallbackReason::UnknownSheetBinding
        }
        _ => PlacementFallbackReason::UnsupportedDirtyProjection,
    })?
    .dependencies
    {
        let read_projection = ReadProjection {
            target_sheet_id: dependency.read_region.sheet_id(),
            rule: dependency.projection,
        };
        if !read_projections.contains(&read_projection) {
            read_projections.push(read_projection);
        }
    }
    let exact_payload = template.key.payload();
    let mut exact_hasher = std::collections::hash_map::DefaultHasher::new();
    exact_payload.hash(&mut exact_hasher);
    let parameterized_payload = template.parameterized_key.payload();
    let mut parameterized_hasher = std::collections::hash_map::DefaultHasher::new();
    parameterized_payload.hash(&mut parameterized_hasher);
    Ok(CandidateAnalysis {
        sheet_id: candidate.sheet_id,
        row: candidate.row,
        col: candidate.col,
        exact_canonical_hash: exact_hasher.finish(),
        exact_canonical_key: Arc::<str>::from(exact_payload),
        parameterized_canonical_hash: parameterized_hasher.finish(),
        parameterized_canonical_key: Arc::<str>::from(parameterized_payload),
        literal_slot_descriptors: template.literal_slot_descriptors.clone(),
        literal_bindings: template.literal_bindings.clone(),
        value_ref_slot_descriptors: Arc::from(
            value_ref_slot_descriptors(&template.expr).into_boxed_slice(),
        ),
        template_slot_map: build_template_slot_map(candidate.ast_id, data_store, &template.expr),
        read_projections,
        read_projections_constant: summary.is_constant_result(),
        // This registry-less analysis path rejects named references at the
        // dependency-summary gate above, so there are never names to record.
        resolved_named_refs: Vec::new(),
    })
}

fn prepare_domain_semantics(
    domain: &PlacementDomain,
    read_projections: &[ReadProjection],
    is_constant_result: bool,
) -> Result<(ResultRegion, SpanReadSummary), PlacementFallbackReason> {
    if !is_constant_result && domain.cell_count() < MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS {
        return Err(PlacementFallbackReason::SmallDomain);
    }
    let result_region = ResultRegion::scalar_cells(domain.clone());
    let read_summary = span_read_summary_for_domain(Region::from_domain(domain), read_projections)
        .map_err(|_| PlacementFallbackReason::UnsupportedDirtyProjection)?;
    if read_summary.dependencies.iter().any(|dependency| {
        dependency
            .read_region
            .intersects(&read_summary.result_region)
    }) {
        return Err(PlacementFallbackReason::InternalDependency);
    }
    Ok((result_region, read_summary))
}

struct AnchorOnceGateOutput {
    result_region: ResultRegion,
    read_summary: SpanReadSummary,
    is_constant_result: bool,
    binding_bytes: usize,
}

fn run_anchor_once_placement_gates(
    candidate: &FormulaPlacementCandidate,
    analysis: &CandidateAnalysis,
    domain: &PlacementDomain,
    member_count: u64,
) -> Result<AnchorOnceGateOutput, PlacementFallbackReason> {
    if member_count != domain.cell_count() || !domain.contains(candidate.placement()) {
        return Err(PlacementFallbackReason::UnsupportedShapeOrGaps);
    }
    if analysis.sheet_id != domain.sheet_id()
        || analysis.placement() != domain_origin(domain)
        || candidate.placement() != analysis.placement()
    {
        return Err(PlacementFallbackReason::CrossSheetOrSheetMismatch);
    }
    let is_constant_result =
        analysis.read_projections_constant && analysis.value_ref_slot_descriptors.is_empty();
    let (result_region, read_summary) =
        prepare_domain_semantics(domain, &analysis.read_projections, is_constant_result)?;
    let binding_bytes = literal_binding_bytes(&analysis.literal_bindings);
    if binding_bytes > MAX_BINDING_SET_BYTES {
        return Err(PlacementFallbackReason::BindingMemoryCapExceeded);
    }
    Ok(AnchorOnceGateOutput {
        result_region,
        read_summary,
        is_constant_result,
        binding_bytes,
    })
}

pub(crate) fn prepare_anchor_once_family(
    candidate: FormulaPlacementCandidate,
    analysis: CandidateAnalysis,
    domain: PlacementDomain,
    member_count: u64,
) -> Result<PreparedAnchorOncePlacement, PlacementFallbackReason> {
    let gates = run_anchor_once_placement_gates(&candidate, &analysis, &domain, member_count)?;
    let AnchorOnceGateOutput {
        result_region,
        read_summary,
        is_constant_result,
        ..
    } = gates;
    let binding_set = SpanBindingSet {
        span_ref: FormulaSpanRef {
            id: super::runtime::FormulaSpanId(0),
            generation: 0,
            version: 0,
        },
        template_ast_id: candidate.ast_id,
        template_origin_row: candidate.row + 1,
        template_origin_col: candidate.col + 1,
        literal_slots: analysis.literal_slot_descriptors.clone(),
        unique_literal_bindings: vec![analysis.literal_bindings.clone()],
        placement_literal_binding_ids: Box::default(),
        literal_binding_encoding: LiteralBindingEncoding::Broadcast,
        value_ref_slots: analysis.value_ref_slot_descriptors.clone(),
        template_slot_map: analysis.template_slot_map.clone(),
    };
    Ok(PreparedAnchorOncePlacement {
        candidate,
        analysis,
        domain,
        result_region,
        read_summary,
        binding_set,
        is_constant_result,
        member_count,
    })
}

pub(crate) fn commit_prepared_anchor_once_family(
    plane: &mut FormulaPlane,
    prepared: PreparedAnchorOncePlacement,
) -> FormulaPlacementReport {
    let PreparedAnchorOncePlacement {
        candidate,
        analysis,
        domain,
        result_region,
        read_summary,
        binding_set,
        is_constant_result,
        member_count,
    } = prepared;
    let mut report = FormulaPlacementReport::default();
    report.counters.formula_cells_seen = member_count;
    let template_count_before = plane.templates.len();
    let template_id = plane.intern_template_parameterized(
        analysis.exact_canonical_key.clone(),
        analysis.parameterized_canonical_key.clone(),
        candidate.ast_id,
        candidate.row + 1,
        candidate.col + 1,
        candidate.formula_text.clone(),
    );
    if plane.templates.len() > template_count_before {
        report.counters.templates_interned = 1;
    }
    let binding_set_id = plane.insert_binding_set(binding_set);
    let read_summary_id = plane.insert_span_read_summary(read_summary);
    let span = plane.insert_span_with_ast_relocation(
        NewFormulaSpan {
            sheet_id: candidate.sheet_id,
            template_id,
            result_region,
            domain,
            intrinsic_mask_id: None,
            read_summary_id: Some(read_summary_id),
            binding_set_id: Some(binding_set_id),
            is_constant_result,
        },
        super::runtime::SpanAstRelocation {
            ast_id: candidate.ast_id,
            anchor_row: candidate.row + 1,
            anchor_col: candidate.col + 1,
        },
    );
    plane.set_binding_span_ref(binding_set_id, span);
    if !analysis.resolved_named_refs.is_empty() {
        plane.register_span_name_dependents(span, &analysis.resolved_named_refs);
    }
    report.counters.spans_created = 1;
    report.counters.accepted_span_cells = member_count;
    report.counters.formula_vertices_avoided = member_count;
    report.counters.ast_roots_avoided = member_count.saturating_sub(1);
    report.counters.edge_rows_avoided = member_count;
    report
}

pub(crate) fn prepare_compressed_family_shadow(
    candidate: &FormulaPlacementCandidate,
    analysis: &CandidateAnalysis,
    domain: PlacementDomain,
    member_count: u64,
) -> CompressedPreparationReport {
    let fallback = |reason| CompressedPreparationReport {
        member_count,
        accepted: false,
        broadcast_binding_bytes: 0,
        fallback_reason: Some(reason),
    };
    match run_anchor_once_placement_gates(candidate, analysis, &domain, member_count) {
        Ok(gates) => CompressedPreparationReport {
            member_count,
            accepted: true,
            broadcast_binding_bytes: gates.binding_bytes,
            fallback_reason: None,
        },
        Err(reason) => fallback(reason),
    }
}

/// Phase-4 allowlist: literals; finite A1 cells/ranges (including explicit
/// worksheet bindings); unary `+`/`-`;
/// and binary arithmetic `+`, `-`, `*`, `/`. Functions, names, arrays, calls,
/// 3-D/external/table references, whole-axis ranges, and all other operators
/// conservatively replay.
pub(crate) fn validate_anchor_once_syntax(
    ast: &ASTNode,
    anchor_row0: u32,
    anchor_col0: u32,
    domain: &PlacementDomain,
) -> Result<(), &'static str> {
    validate_anchor_once_relocation(ast, anchor_row0, anchor_col0, domain, false)
}

pub(crate) fn validate_anchor_once_shadow_relocation(
    ast: &ASTNode,
    anchor_row0: u32,
    anchor_col0: u32,
    domain: &PlacementDomain,
) -> Result<(), &'static str> {
    validate_anchor_once_relocation(ast, anchor_row0, anchor_col0, domain, true)
}

fn validate_anchor_once_relocation(
    ast: &ASTNode,
    anchor_row0: u32,
    anchor_col0: u32,
    domain: &PlacementDomain,
    allow_safe_functions: bool,
) -> Result<(), &'static str> {
    const MAX_ROW: i64 = 1_048_576;
    const MAX_COL: i64 = 16_384;

    fn axis_safe(
        value: u32,
        absolute: bool,
        anchor0: u32,
        domain_start: u32,
        domain_end: u32,
        maximum: i64,
    ) -> bool {
        let value = i64::from(value);
        if absolute {
            return (1..=maximum).contains(&value);
        }
        let min = value + i64::from(domain_start) - i64::from(anchor0);
        let max = value + i64::from(domain_end) - i64::from(anchor0);
        min >= 1 && max <= maximum
    }

    fn walk(
        node: &ASTNode,
        anchor_row0: u32,
        anchor_col0: u32,
        row_start: u32,
        row_end: u32,
        col_start: u32,
        col_end: u32,
        allow_safe_functions: bool,
    ) -> Result<(), &'static str> {
        match &node.node_type {
            ASTNodeType::Literal(_) => Ok(()),
            ASTNodeType::Reference { reference, .. } => match reference {
                ReferenceType::Cell {
                    sheet: _,
                    row,
                    col,
                    row_abs,
                    col_abs,
                } if axis_safe(*row, *row_abs, anchor_row0, row_start, row_end, MAX_ROW)
                    && axis_safe(*col, *col_abs, anchor_col0, col_start, col_end, MAX_COL) =>
                {
                    Ok(())
                }
                ReferenceType::Range {
                    sheet: _,
                    start_row: Some(start_row),
                    start_col: Some(start_col),
                    end_row: Some(end_row),
                    end_col: Some(end_col),
                    start_row_abs,
                    start_col_abs,
                    end_row_abs,
                    end_col_abs,
                } if axis_safe(
                    *start_row,
                    *start_row_abs,
                    anchor_row0,
                    row_start,
                    row_end,
                    MAX_ROW,
                ) && axis_safe(
                    *end_row,
                    *end_row_abs,
                    anchor_row0,
                    row_start,
                    row_end,
                    MAX_ROW,
                ) && axis_safe(
                    *start_col,
                    *start_col_abs,
                    anchor_col0,
                    col_start,
                    col_end,
                    MAX_COL,
                ) && axis_safe(
                    *end_col,
                    *end_col_abs,
                    anchor_col0,
                    col_start,
                    col_end,
                    MAX_COL,
                ) =>
                {
                    Ok(())
                }
                _ => Err("UnsupportedAnchorReference"),
            },
            ASTNodeType::UnaryOp { op, expr } if matches!(op.as_str(), "+" | "-") => walk(
                expr,
                anchor_row0,
                anchor_col0,
                row_start,
                row_end,
                col_start,
                col_end,
                allow_safe_functions,
            ),
            ASTNodeType::BinaryOp { op, left, right }
                if matches!(op.as_str(), "+" | "-" | "*" | "/") =>
            {
                walk(
                    left,
                    anchor_row0,
                    anchor_col0,
                    row_start,
                    row_end,
                    col_start,
                    col_end,
                    allow_safe_functions,
                )?;
                walk(
                    right,
                    anchor_row0,
                    anchor_col0,
                    row_start,
                    row_end,
                    col_start,
                    col_end,
                    allow_safe_functions,
                )
            }
            ASTNodeType::Function { name, args } if allow_safe_functions => {
                let function = super::template_canonical::resolve_canonical_function(name, args.len());
                let contract = function.contract.ok_or("AnchorFunctionUnresolvedOrInvalid")?;
                if contract.dependency
                    != crate::function_contract::FunctionDependencySemantics::RecursiveSyntacticArgs
                    || contract.environment
                        != crate::function_contract::FunctionEnvironmentSemantics::None
                    || contract.context != crate::function_contract::FunctionContextDependence::None
                    || contract.result.may_return_reference()
                    || contract.result.may_spill()
                    || function.semantic_flags & crate::function::FnCaps::VOLATILE.bits() != 0
                {
                    return Err("AnchorFunctionSemanticsUnsupported");
                }
                for arg in args {
                    walk(
                        arg,
                        anchor_row0,
                        anchor_col0,
                        row_start,
                        row_end,
                        col_start,
                        col_end,
                        allow_safe_functions,
                    )?;
                }
                Ok(())
            }
            _ => Err("UnsupportedAnchorSyntax"),
        }
    }

    let (row_start, row_end, col_start, col_end) = match domain {
        PlacementDomain::RowRun {
            row_start,
            row_end,
            col,
            ..
        } => (*row_start, *row_end, *col, *col),
        PlacementDomain::ColRun {
            row,
            col_start,
            col_end,
            ..
        } => (*row, *row, *col_start, *col_end),
        PlacementDomain::Rect {
            row_start,
            row_end,
            col_start,
            col_end,
            ..
        } => (*row_start, *row_end, *col_start, *col_end),
    };
    walk(
        ast,
        anchor_row0,
        anchor_col0,
        row_start,
        row_end,
        col_start,
        col_end,
        allow_safe_functions,
    )
}

fn analyze_candidates(
    candidates: &[FormulaPlacementCandidate],
    data_store: &DataStore,
    sheet_registry: &SheetRegistry,
) -> Result<Vec<CandidateAnalysis>, PlacementFallbackReason> {
    candidates
        .iter()
        .map(|candidate| {
            let ast = data_store
                .retrieve_ast(candidate.ast_id, sheet_registry)
                .ok_or(PlacementFallbackReason::UnsupportedCanonicalTemplate)?;
            analyze_candidate(candidate, &ast, data_store, sheet_registry)
        })
        .collect()
}

pub(crate) fn prepare_family_placement(
    candidates: Vec<FormulaPlacementCandidate>,
    analyses: Vec<CandidateAnalysis>,
) -> Result<PreparedFamilyPlacement, Box<FormulaPlacementReport>> {
    let mut report = FormulaPlacementReport::default();
    report.counters.formula_cells_seen = candidates.len() as u64;

    if candidates.is_empty() {
        return Err(Box::new(report));
    }

    debug_assert_eq!(candidates.len(), analyses.len());

    let first = &analyses[0];
    let sheet_id = first.sheet_id;
    if analyses
        .iter()
        .any(|analysis| analysis.sheet_id != sheet_id)
    {
        mark_all_legacy(
            &mut report,
            &candidates,
            PlacementFallbackReason::CrossSheetOrSheetMismatch,
        );
        return Err(Box::new(report));
    }

    if analyses.iter().any(|analysis| {
        analysis.parameterized_canonical_hash != first.parameterized_canonical_hash
            || analysis.parameterized_canonical_key != first.parameterized_canonical_key
    }) {
        mark_all_legacy(
            &mut report,
            &candidates,
            PlacementFallbackReason::NonEquivalentTemplate,
        );
        return Err(Box::new(report));
    }

    let domain = match detect_domain(&analyses) {
        Ok(domain) => domain,
        Err(reason) => {
            mark_all_legacy(&mut report, &candidates, reason);
            return Err(Box::new(report));
        }
    };

    let is_constant_result = first.read_projections_constant
        && first.value_ref_slot_descriptors.is_empty()
        && analyses
            .iter()
            .all(|analysis| analysis.literal_bindings.as_ref() == first.literal_bindings.as_ref());

    let origin = domain_origin(&domain);
    let origin_analysis = analyses
        .iter()
        .find(|analysis| analysis.placement() == origin)
        .ok_or(PlacementFallbackReason::UnsupportedShapeOrGaps);
    let origin_analysis = match origin_analysis {
        Ok(origin_analysis) => origin_analysis,
        Err(reason) => {
            mark_all_legacy(&mut report, &candidates, reason);
            return Err(Box::new(report));
        }
    };

    let (result_region, read_summary) = match prepare_domain_semantics(
        &domain,
        &origin_analysis.read_projections,
        is_constant_result,
    ) {
        Ok(semantics) => semantics,
        Err(reason) => {
            mark_all_legacy(&mut report, &candidates, reason);
            return Err(Box::new(report));
        }
    };

    let Some(origin_candidate) = candidates
        .iter()
        .find(|candidate| candidate.placement() == origin)
    else {
        mark_all_legacy(
            &mut report,
            &candidates,
            PlacementFallbackReason::UnsupportedShapeOrGaps,
        );
        return Err(Box::new(report));
    };

    let binding_set = match build_span_binding_set(
        FormulaSpanRef {
            id: super::runtime::FormulaSpanId(0),
            generation: 0,
            version: 0,
        },
        &domain,
        &candidates,
        &analyses,
    ) {
        Ok(binding_set) => binding_set,
        Err(reason) => {
            mark_all_legacy(&mut report, &candidates, reason);
            return Err(Box::new(report));
        }
    };
    Ok(PreparedFamilyPlacement {
        candidates,
        analyses,
        domain,
        result_region,
        read_summary,
        binding_set,
        is_constant_result,
    })
}

pub(crate) fn commit_prepared_family(
    plane: &mut FormulaPlane,
    prepared: PreparedFamilyPlacement,
) -> FormulaPlacementReport {
    let PreparedFamilyPlacement {
        candidates,
        analyses,
        domain,
        result_region,
        read_summary,
        binding_set,
        is_constant_result,
    } = prepared;
    let first = &analyses[0];
    let sheet_id = first.sheet_id;
    let origin = domain_origin(&domain);
    let origin_analysis = analyses
        .iter()
        .find(|analysis| analysis.placement() == origin)
        .expect("prepared placement has an origin analysis");
    let origin_candidate = candidates
        .iter()
        .find(|candidate| candidate.placement() == origin)
        .expect("prepared placement has an origin candidate");
    let mut report = FormulaPlacementReport::default();
    report.counters.formula_cells_seen = candidates.len() as u64;

    let template_count_before = plane.templates.len();
    let template_id = plane.intern_template_parameterized(
        first.exact_canonical_key.clone(),
        first.parameterized_canonical_key.clone(),
        origin_candidate.ast_id,
        origin_candidate.row + 1,
        origin_candidate.col + 1,
        origin_candidate.formula_text.clone(),
    );
    if plane.templates.len() > template_count_before {
        report.counters.templates_interned = 1;
    }
    let binding_set_id = plane.insert_binding_set(binding_set);

    let read_summary_id = plane.insert_span_read_summary(read_summary);
    let span = plane.insert_span_with_ast_relocation(
        NewFormulaSpan {
            sheet_id,
            template_id,
            result_region,
            domain,
            intrinsic_mask_id: None,
            read_summary_id: Some(read_summary_id),
            binding_set_id: Some(binding_set_id),
            is_constant_result,
        },
        super::runtime::SpanAstRelocation {
            ast_id: origin_candidate.ast_id,
            anchor_row: origin_candidate.row + 1,
            anchor_col: origin_candidate.col + 1,
        },
    );
    plane.set_binding_span_ref(binding_set_id, span);
    if !origin_analysis.resolved_named_refs.is_empty() {
        plane.register_span_name_dependents(span, &origin_analysis.resolved_named_refs);
    }

    report.counters.spans_created = 1;
    report.counters.accepted_span_cells = candidates.len() as u64;
    report.counters.formula_vertices_avoided = report.counters.accepted_span_cells;
    report.counters.ast_roots_avoided = report.counters.accepted_span_cells.saturating_sub(1);
    report.counters.edge_rows_avoided = report.counters.accepted_span_cells;
    report.results = candidates
        .iter()
        .map(|candidate| FormulaPlacementResult::Span {
            span,
            template_id,
            placement: candidate.placement(),
        })
        .collect();
    report
}

pub(crate) fn value_ref_slot_descriptors(expr: &CanonicalExpr) -> Vec<ValueRefSlotDescriptor> {
    fn walk(expr: &CanonicalExpr, out: &mut Vec<ValueRefSlotDescriptor>, preorder: &mut u32) {
        match expr {
            CanonicalExpr::Literal(_) => {}
            CanonicalExpr::Reference { context, reference } => {
                let slot_context = match context {
                    super::template_canonical::CanonicalReferenceContext::Value => {
                        SlotContext::Value
                    }
                    super::template_canonical::CanonicalReferenceContext::Reference => {
                        SlotContext::Reference
                    }
                    super::template_canonical::CanonicalReferenceContext::CallArgument {
                        ..
                    } => SlotContext::CallArgument,
                    super::template_canonical::CanonicalReferenceContext::FunctionArgument {
                        function,
                        arg_index,
                    } => function_argument_slot_context(function, *arg_index),
                };
                if matches!(
                    slot_context,
                    SlotContext::Value | SlotContext::CriteriaExpressionArg
                ) && finite_relative_cell(reference)
                {
                    out.push(ValueRefSlotDescriptor {
                        slot_id: ValueRefSlotId(u16::try_from(out.len()).unwrap_or(u16::MAX)),
                        preorder_index: *preorder,
                        context: slot_context,
                        reference_pattern: reference.clone(),
                    });
                }
                *preorder = preorder.saturating_add(1);
            }
            CanonicalExpr::Unary { expr, .. } => walk(expr, out, preorder),
            CanonicalExpr::Binary { left, right, .. } => {
                walk(left, out, preorder);
                walk(right, out, preorder);
            }
            CanonicalExpr::Function { args, .. } | CanonicalExpr::CallUnsupported { args, .. } => {
                if let CanonicalExpr::CallUnsupported { callee, .. } = expr {
                    walk(callee, out, preorder);
                }
                for arg in args {
                    walk(arg, out, preorder);
                }
            }
            CanonicalExpr::ArrayUnsupported { rows } => {
                for row in rows {
                    for expr in row {
                        walk(expr, out, preorder);
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    let mut preorder = 0u32;
    walk(expr, &mut out, &mut preorder);
    out
}

fn finite_relative_cell(reference: &CanonicalReference) -> bool {
    matches!(
        reference,
        CanonicalReference::Cell {
            row: AxisRef::RelativeToPlacement { .. } | AxisRef::AbsoluteVc { .. },
            col: AxisRef::RelativeToPlacement { .. } | AxisRef::AbsoluteVc { .. },
            ..
        }
    ) && match reference {
        CanonicalReference::Cell { row, col, .. } => {
            matches!(row, AxisRef::RelativeToPlacement { .. })
                || matches!(col, AxisRef::RelativeToPlacement { .. })
        }
        _ => false,
    }
}

fn build_span_binding_set(
    span_ref: FormulaSpanRef,
    domain: &PlacementDomain,
    candidates: &[FormulaPlacementCandidate],
    analyses: &[CandidateAnalysis],
) -> Result<SpanBindingSet, PlacementFallbackReason> {
    let first = &analyses[0];
    for analysis in analyses {
        if analysis.literal_slot_descriptors.as_ref() != first.literal_slot_descriptors.as_ref()
            || analysis.value_ref_slot_descriptors.as_ref()
                != first.value_ref_slot_descriptors.as_ref()
        {
            return Err(PlacementFallbackReason::NonEquivalentTemplate);
        }
    }

    let mut by_placement = BTreeMap::new();
    for (candidate, analysis) in candidates.iter().zip(analyses) {
        by_placement.insert(
            (candidate.sheet_id, candidate.row, candidate.col),
            analysis.literal_bindings.clone(),
        );
    }

    let mut unique_literal_bindings: Vec<Box<[LiteralValue]>> = Vec::new();
    let mut unique_keys: FxHashMap<String, u32> = FxHashMap::default();
    let mut placement_ids = Vec::with_capacity(domain.cell_count() as usize);
    for placement in domain.iter() {
        let Some(binding) = by_placement.get(&(placement.sheet_id, placement.row, placement.col))
        else {
            return Err(PlacementFallbackReason::UnsupportedShapeOrGaps);
        };
        let key = literal_binding_key(binding);
        let id = if let Some(id) = unique_keys.get(&key).copied() {
            id
        } else {
            let id = u32::try_from(unique_literal_bindings.len())
                .map_err(|_| PlacementFallbackReason::BindingMemoryCapExceeded)?;
            unique_keys.insert(key, id);
            unique_literal_bindings.push(binding.clone());
            id
        };
        placement_ids.push(id);
    }

    let dictionary_bytes: usize = unique_literal_bindings
        .iter()
        .map(|binding| literal_binding_bytes(binding))
        .sum();
    let literal_binding_encoding = detect_affine_literal_encoding(domain, &by_placement)
        .unwrap_or(LiteralBindingEncoding::Dictionary);
    let encoded_bytes = match &literal_binding_encoding {
        LiteralBindingEncoding::Broadcast => dictionary_bytes,
        LiteralBindingEncoding::Dictionary => dictionary_bytes,
        LiteralBindingEncoding::AffineByRow { base, steps, .. }
        | LiteralBindingEncoding::AffineByCol { base, steps, .. } => {
            literal_binding_bytes(base) + std::mem::size_of_val(steps.as_ref())
        }
        LiteralBindingEncoding::AffineRect {
            base,
            row_steps,
            col_steps,
            ..
        } => {
            literal_binding_bytes(base)
                + std::mem::size_of_val(row_steps.as_ref())
                + std::mem::size_of_val(col_steps.as_ref())
        }
    };
    if encoded_bytes > MAX_BINDING_SET_BYTES {
        return Err(PlacementFallbackReason::BindingMemoryCapExceeded);
    }

    let (unique_literal_bindings, placement_literal_binding_ids) = match &literal_binding_encoding {
        LiteralBindingEncoding::Broadcast => (unique_literal_bindings, Box::<[u32]>::default()),
        LiteralBindingEncoding::Dictionary => {
            (unique_literal_bindings, placement_ids.into_boxed_slice())
        }
        LiteralBindingEncoding::AffineByRow { base, .. }
        | LiteralBindingEncoding::AffineByCol { base, .. }
        | LiteralBindingEncoding::AffineRect { base, .. } => {
            (vec![base.clone()], Box::<[u32]>::default())
        }
    };

    let origin = domain_origin(domain);
    let origin_index = candidates
        .iter()
        .position(|candidate| candidate.placement() == origin)
        .ok_or(PlacementFallbackReason::UnsupportedShapeOrGaps)?;
    let origin_candidate = &candidates[origin_index];
    let origin_analysis = &analyses[origin_index];

    Ok(SpanBindingSet {
        span_ref,
        template_ast_id: origin_candidate.ast_id,
        template_origin_row: origin_candidate.row + 1,
        template_origin_col: origin_candidate.col + 1,
        literal_slots: first.literal_slot_descriptors.clone(),
        unique_literal_bindings,
        placement_literal_binding_ids,
        literal_binding_encoding,
        value_ref_slots: first.value_ref_slot_descriptors.clone(),
        template_slot_map: origin_analysis.template_slot_map.clone(),
    })
}

fn detect_affine_literal_encoding(
    domain: &PlacementDomain,
    by_placement: &BTreeMap<(SheetId, u32, u32), Box<[LiteralValue]>>,
) -> Option<LiteralBindingEncoding> {
    let origin = domain_origin(domain);
    let base = by_placement
        .get(&(origin.sheet_id, origin.row, origin.col))?
        .clone();
    if base.is_empty() || !base.iter().all(is_affine_numeric_literal) {
        return None;
    }

    match domain {
        PlacementDomain::RowRun { .. } => {
            let steps = detect_affine_steps(domain, by_placement, &base, |placement, origin| {
                i64::from(placement.row) - i64::from(origin.row)
            })?;
            Some(LiteralBindingEncoding::AffineByRow {
                origin_row: origin.row,
                base,
                steps: steps.into_boxed_slice(),
            })
        }
        PlacementDomain::ColRun { .. } => {
            let steps = detect_affine_steps(domain, by_placement, &base, |placement, origin| {
                i64::from(placement.col) - i64::from(origin.col)
            })?;
            Some(LiteralBindingEncoding::AffineByCol {
                origin_col: origin.col,
                base,
                steps: steps.into_boxed_slice(),
            })
        }
        PlacementDomain::Rect { .. } => detect_affine_rect_encoding(domain, by_placement, base),
    }
}

fn is_affine_numeric_literal(value: &LiteralValue) -> bool {
    match value {
        LiteralValue::Int(_) => true,
        LiteralValue::Number(value) => f64_to_exact_i64(*value).is_some(),
        _ => false,
    }
}

fn f64_to_exact_i64(value: f64) -> Option<i64> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    if value < i64::MIN as f64 || value > i64::MAX as f64 {
        return None;
    }
    let int = value as i64;
    ((int as f64) == value).then_some(int)
}

fn literal_as_exact_i64(value: &LiteralValue) -> Option<i64> {
    match value {
        LiteralValue::Int(value) => Some(*value),
        LiteralValue::Number(value) => f64_to_exact_i64(*value),
        _ => None,
    }
}

fn exact_i64_to_like(value: i64, like: &LiteralValue) -> Option<LiteralValue> {
    match like {
        LiteralValue::Int(_) => Some(LiteralValue::Int(value)),
        LiteralValue::Number(_) => {
            let value_f64 = value as f64;
            ((value_f64 as i64) == value).then_some(LiteralValue::Number(value_f64))
        }
        _ => None,
    }
}

fn affine_step_between(
    base_value: &LiteralValue,
    value: &LiteralValue,
    delta: i64,
) -> Option<Option<i64>> {
    let base = literal_as_exact_i64(base_value)?;
    let value = literal_as_exact_i64(value)?;
    let diff = value.checked_sub(base)?;
    if delta == 0 {
        return (diff == 0).then_some(None);
    }
    if diff % delta != 0 {
        return None;
    }
    Some(Some(diff / delta))
}

fn detect_affine_steps(
    domain: &PlacementDomain,
    by_placement: &BTreeMap<(SheetId, u32, u32), Box<[LiteralValue]>>,
    base: &[LiteralValue],
    axis_delta: impl Fn(PlacementCoord, PlacementCoord) -> i64,
) -> Option<Vec<i64>> {
    let origin = domain_origin(domain);
    let mut steps: Vec<Option<i64>> = vec![None; base.len()];
    for placement in domain.iter() {
        let delta = axis_delta(placement, origin);
        let binding = by_placement.get(&(placement.sheet_id, placement.row, placement.col))?;
        if binding.len() != base.len() {
            return None;
        }
        for (slot, (base_value, value)) in base.iter().zip(binding.iter()).enumerate() {
            let step = affine_step_between(base_value, value, delta)?;
            if let Some(step) = step {
                match &steps[slot] {
                    Some(existing) if existing != &step => return None,
                    Some(_) => {}
                    None => steps[slot] = Some(step),
                }
            }
        }
    }
    Some(steps.into_iter().map(|step| step.unwrap_or(0)).collect())
}

fn detect_affine_rect_encoding(
    domain: &PlacementDomain,
    by_placement: &BTreeMap<(SheetId, u32, u32), Box<[LiteralValue]>>,
    base: Box<[LiteralValue]>,
) -> Option<LiteralBindingEncoding> {
    let PlacementDomain::Rect {
        row_start,
        col_start,
        col_end,
        ..
    } = domain
    else {
        return None;
    };
    let origin = domain_origin(domain);
    let right = if *col_start < *col_end {
        by_placement.get(&(origin.sheet_id, origin.row, origin.col + 1))?
    } else {
        &base
    };
    let down = by_placement
        .get(&(origin.sheet_id, row_start.saturating_add(1), origin.col))
        .unwrap_or(&base);
    if right.len() != base.len() || down.len() != base.len() {
        return None;
    }
    let mut row_steps = Vec::with_capacity(base.len());
    let mut col_steps = Vec::with_capacity(base.len());
    for ((base_value, down_value), right_value) in base.iter().zip(down.iter()).zip(right.iter()) {
        row_steps.push(affine_step_between(base_value, down_value, 1)??);
        col_steps.push(affine_step_between(base_value, right_value, 1)??);
    }

    for placement in domain.iter() {
        let binding = by_placement.get(&(placement.sheet_id, placement.row, placement.col))?;
        if binding.len() != base.len() {
            return None;
        }
        let row_delta = i64::from(placement.row) - i64::from(origin.row);
        let col_delta = i64::from(placement.col) - i64::from(origin.col);
        for (((base_value, row_step), col_step), value) in base
            .iter()
            .zip(row_steps.iter())
            .zip(col_steps.iter())
            .zip(binding.iter())
        {
            let expected_i64 = literal_as_exact_i64(base_value)?
                .checked_add(row_step.checked_mul(row_delta)?)?
                .checked_add(col_step.checked_mul(col_delta)?)?;
            let expected = exact_i64_to_like(expected_i64, base_value)?;
            if &expected != value {
                return None;
            }
        }
    }

    Some(LiteralBindingEncoding::AffineRect {
        origin_row: origin.row,
        origin_col: origin.col,
        base,
        row_steps: row_steps.into_boxed_slice(),
        col_steps: col_steps.into_boxed_slice(),
    })
}

pub(crate) fn split_candidate_affine_literal_runs(
    candidates: Vec<FormulaPlacementCandidate>,
    analyses: Vec<CandidateAnalysis>,
) -> Vec<(Vec<FormulaPlacementCandidate>, Vec<CandidateAnalysis>)> {
    if candidates.len() < MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS as usize {
        return vec![(candidates, analyses)];
    }
    debug_assert_eq!(candidates.len(), analyses.len());

    // Fast path: identical literal bindings across the family mean every
    // adjacent step is zero, so the family never splits. Skip the per-pair
    // step computation (which allocates a Vec per window) entirely.
    if analyses
        .windows(2)
        .all(|pair| pair[0].literal_bindings == pair[1].literal_bindings)
    {
        return vec![(candidates, analyses)];
    }

    let is_row_run = candidates.windows(2).all(|w| {
        w[0].sheet_id == w[1].sheet_id && w[0].col == w[1].col && w[0].row + 1 == w[1].row
    });
    let is_col_run = candidates.windows(2).all(|w| {
        w[0].sheet_id == w[1].sheet_id && w[0].row == w[1].row && w[0].col + 1 == w[1].col
    });
    if !is_row_run && !is_col_run {
        return vec![(candidates, analyses)];
    }
    if analyses.iter().any(|analysis| {
        analysis.literal_bindings.is_empty()
            || analysis
                .literal_bindings
                .iter()
                .any(|value| !is_affine_numeric_literal(value))
    }) {
        return vec![(candidates, analyses)];
    }

    let mut edge_steps = Vec::with_capacity(analyses.len().saturating_sub(1));
    for pair in analyses.windows(2) {
        let Some(step) = literal_binding_step(
            pair[0].literal_bindings.as_ref(),
            pair[1].literal_bindings.as_ref(),
        ) else {
            return vec![(candidates, analyses)];
        };
        edge_steps.push(step);
    }

    if edge_steps.windows(2).all(|w| w[0] == w[1]) {
        return vec![(candidates, analyses)];
    }

    let min_cells = MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS as usize;
    let mut kept = Vec::<(usize, usize)>::new();
    let mut edge_start = 0usize;
    for idx in 1..edge_steps.len() {
        if edge_steps[idx] != edge_steps[edge_start] {
            let start = edge_start;
            let end = idx;
            if end + 1 - start >= min_cells {
                kept.push((start, end));
            }
            edge_start = idx;
        }
    }
    if !edge_steps.is_empty() {
        let start = edge_start;
        let end = edge_steps.len();
        if end + 1 - start >= min_cells {
            kept.push((start, end));
        }
    }

    if kept.is_empty() {
        return vec![(candidates, analyses)];
    }

    let mut result = Vec::new();
    let mut cursor = 0usize;
    for (mut start, end) in kept {
        if start < cursor {
            start = cursor;
        }
        if start > cursor {
            push_component_slice(&candidates, &analyses, cursor, start, &mut result);
        }
        if end + 1 > start {
            push_component_slice(&candidates, &analyses, start, end + 1, &mut result);
        }
        cursor = end + 1;
    }
    if cursor < candidates.len() {
        push_component_slice(
            &candidates,
            &analyses,
            cursor,
            candidates.len(),
            &mut result,
        );
    }
    result
}

fn push_component_slice(
    candidates: &[FormulaPlacementCandidate],
    analyses: &[CandidateAnalysis],
    start: usize,
    end: usize,
    out: &mut Vec<(Vec<FormulaPlacementCandidate>, Vec<CandidateAnalysis>)>,
) {
    if start >= end {
        return;
    }
    out.push((
        candidates[start..end].to_vec(),
        analyses[start..end].to_vec(),
    ));
}

fn literal_binding_step(left: &[LiteralValue], right: &[LiteralValue]) -> Option<Vec<i64>> {
    if left.len() != right.len() {
        return None;
    }
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| literal_as_exact_i64(right)?.checked_sub(literal_as_exact_i64(left)?))
        .collect()
}

pub(crate) fn build_template_slot_map(
    origin_ast_id: AstNodeId,
    data_store: &DataStore,
    expr: &CanonicalExpr,
) -> TemplateSlotMap {
    fn walk(
        node_id: AstNodeId,
        data_store: &DataStore,
        next: &mut u16,
        out: &mut FxHashMap<AstNodeId, super::template_canonical::LiteralSlotId>,
    ) {
        let Some(node) = data_store.get_node(node_id) else {
            return;
        };
        match node {
            AstNodeData::Literal(vref) => {
                let value = data_store.retrieve_value(*vref);
                if !matches!(value, LiteralValue::Array(_)) {
                    out.insert(node_id, super::template_canonical::LiteralSlotId(*next));
                    *next = next.saturating_add(1);
                }
            }
            AstNodeData::Reference { .. } => {}
            AstNodeData::UnaryOp { expr_id, .. } => walk(*expr_id, data_store, next, out),
            AstNodeData::BinaryOp {
                left_id, right_id, ..
            } => {
                walk(*left_id, data_store, next, out);
                walk(*right_id, data_store, next, out);
            }
            AstNodeData::Function { .. } => {
                if let Some(args) = data_store.get_args(node_id) {
                    for arg in args {
                        walk(*arg, data_store, next, out);
                    }
                }
            }
            AstNodeData::Array { .. } => {}
        }
    }
    let mut map = FxHashMap::default();
    let mut next = 0u16;
    walk(origin_ast_id, data_store, &mut next, &mut map);
    let (residual_relative_row, residual_relative_col) = residual_relative_axes(expr);
    TemplateSlotMap {
        literal_slots_by_arena_node: map,
        residual_relative_row,
        residual_relative_col,
    }
}

fn residual_relative_axes(expr: &CanonicalExpr) -> (bool, bool) {
    fn reference_axes(reference: &CanonicalReference) -> (bool, bool) {
        match reference {
            CanonicalReference::Cell { row, col, .. } => (
                matches!(row, AxisRef::RelativeToPlacement { .. }),
                matches!(col, AxisRef::RelativeToPlacement { .. }),
            ),
            CanonicalReference::Range {
                start_row,
                end_row,
                start_col,
                end_col,
                ..
            } => (
                matches!(start_row, AxisRef::RelativeToPlacement { .. })
                    || matches!(end_row, AxisRef::RelativeToPlacement { .. }),
                matches!(start_col, AxisRef::RelativeToPlacement { .. })
                    || matches!(end_col, AxisRef::RelativeToPlacement { .. }),
            ),
            // Named references are placement-invariant (all-absolute after
            // resolution), so they contribute no residual relative axes.
            CanonicalReference::Named { .. } => (false, false),
            CanonicalReference::Unsupported { .. } => (false, false),
        }
    }

    fn walk(expr: &CanonicalExpr, row: &mut bool, col: &mut bool) {
        match expr {
            CanonicalExpr::Literal(_) => {}
            CanonicalExpr::Reference { context, reference } => {
                let slot_context = match context {
                    super::template_canonical::CanonicalReferenceContext::Value => {
                        SlotContext::Value
                    }
                    super::template_canonical::CanonicalReferenceContext::Reference => {
                        SlotContext::Reference
                    }
                    super::template_canonical::CanonicalReferenceContext::CallArgument {
                        ..
                    } => SlotContext::CallArgument,
                    super::template_canonical::CanonicalReferenceContext::FunctionArgument {
                        function,
                        arg_index,
                    } => function_argument_slot_context(function, *arg_index),
                };
                let captured_as_value_slot = matches!(
                    slot_context,
                    SlotContext::Value | SlotContext::CriteriaExpressionArg
                ) && finite_relative_cell(reference);
                if !captured_as_value_slot {
                    let (has_row, has_col) = reference_axes(reference);
                    *row |= has_row;
                    *col |= has_col;
                }
            }
            CanonicalExpr::Unary { expr, .. } => walk(expr, row, col),
            CanonicalExpr::Binary { left, right, .. } => {
                walk(left, row, col);
                walk(right, row, col);
            }
            CanonicalExpr::Function { args, .. } => {
                for arg in args {
                    walk(arg, row, col);
                }
            }
            CanonicalExpr::CallUnsupported { callee, args } => {
                walk(callee, row, col);
                for arg in args {
                    walk(arg, row, col);
                }
            }
            CanonicalExpr::ArrayUnsupported { rows } => {
                for cells in rows {
                    for cell in cells {
                        walk(cell, row, col);
                    }
                }
            }
        }
    }

    let mut row = false;
    let mut col = false;
    walk(expr, &mut row, &mut col);
    (row, col)
}

fn literal_binding_key(binding: &[LiteralValue]) -> String {
    let mut out = String::new();
    for value in binding {
        match value {
            LiteralValue::Int(v) => out.push_str(&format!("i:{v};")),
            LiteralValue::Number(v) => out.push_str(&format!("n:{:016x};", v.to_bits())),
            LiteralValue::Text(v) => out.push_str(&format!("t:{}:{v};", v.len())),
            LiteralValue::Boolean(v) => out.push_str(if *v { "b:1;" } else { "b:0;" }),
            LiteralValue::Error(v) => out.push_str(&format!("e:{v:?};")),
            LiteralValue::Array(_) => out.push_str("array;"),
            LiteralValue::Date(v) => out.push_str(&format!("d:{v};")),
            LiteralValue::DateTime(v) => out.push_str(&format!("dt:{v};")),
            LiteralValue::Time(v) => out.push_str(&format!("tm:{v};")),
            LiteralValue::Duration(v) => out.push_str(&format!("du:{v:?};")),
            LiteralValue::Empty => out.push_str("empty;"),
            LiteralValue::Pending => out.push_str("pending;"),
        }
    }
    out
}

fn literal_binding_bytes(binding: &[LiteralValue]) -> usize {
    binding
        .iter()
        .map(|value| match value {
            LiteralValue::Text(v) => v.len(),
            LiteralValue::Error(v) => format!("{v:?}").len(),
            _ => std::mem::size_of::<LiteralValue>(),
        })
        .sum()
}

fn span_read_summary_for_domain(
    result_region: Region,
    projections: &[ReadProjection],
) -> Result<SpanReadSummary, crate::formula_plane::producer::ProjectionFallbackReason> {
    let mut dependencies = Vec::new();
    for &read_projection in projections {
        let projection = read_projection.rule;
        for read_region in
            projection.read_regions_for_result(read_projection.target_sheet_id, result_region)?
        {
            let dependency = SpanReadDependency {
                read_region,
                projection,
            };
            if !dependencies.contains(&dependency) {
                dependencies.push(dependency);
            }
        }
    }
    Ok(SpanReadSummary {
        result_region,
        dependencies,
    })
}

fn detect_domain(
    analyses: &[CandidateAnalysis],
) -> Result<PlacementDomain, PlacementFallbackReason> {
    if analyses.len() < 2 {
        return Err(PlacementFallbackReason::SingletonUnique);
    }

    let sheet_id = analyses[0].sheet_id;
    let mut coords = Vec::with_capacity(analyses.len());
    let mut unique = BTreeSet::new();
    for analysis in analyses {
        let coord = analysis.placement();
        if !unique.insert((coord.row, coord.col)) {
            return Err(PlacementFallbackReason::DuplicatePlacement);
        }
        coords.push(coord);
    }

    if coords.iter().any(|coord| coord.sheet_id != sheet_id) {
        return Err(PlacementFallbackReason::CrossSheetOrSheetMismatch);
    }

    let same_col = coords.iter().all(|coord| coord.col == coords[0].col);
    if same_col {
        let mut rows: Vec<_> = coords.iter().map(|coord| coord.row).collect();
        rows.sort_unstable();
        if is_contiguous(&rows) {
            return Ok(PlacementDomain::row_run(
                sheet_id,
                rows[0],
                *rows.last().expect("non-empty rows"),
                coords[0].col,
            ));
        }
        return Err(PlacementFallbackReason::UnsupportedShapeOrGaps);
    }

    let same_row = coords.iter().all(|coord| coord.row == coords[0].row);
    if same_row {
        let mut cols: Vec<_> = coords.iter().map(|coord| coord.col).collect();
        cols.sort_unstable();
        if is_contiguous(&cols) {
            return Ok(PlacementDomain::col_run(
                sheet_id,
                coords[0].row,
                cols[0],
                *cols.last().expect("non-empty cols"),
            ));
        }
        return Err(PlacementFallbackReason::UnsupportedShapeOrGaps);
    }

    let rows: BTreeSet<_> = coords.iter().map(|coord| coord.row).collect();
    let cols: BTreeSet<_> = coords.iter().map(|coord| coord.col).collect();
    let rows_vec: Vec<_> = rows.iter().copied().collect();
    let cols_vec: Vec<_> = cols.iter().copied().collect();
    if !is_contiguous(&rows_vec) || !is_contiguous(&cols_vec) {
        return Err(PlacementFallbackReason::UnsupportedShapeOrGaps);
    }
    if rows.len() * cols.len() != coords.len() {
        return Err(PlacementFallbackReason::UnsupportedShapeOrGaps);
    }

    Ok(PlacementDomain::rect(
        sheet_id,
        rows_vec[0],
        *rows_vec.last().expect("non-empty rows"),
        cols_vec[0],
        *cols_vec.last().expect("non-empty cols"),
    ))
}

fn domain_origin(domain: &PlacementDomain) -> PlacementCoord {
    match *domain {
        PlacementDomain::RowRun {
            sheet_id,
            row_start,
            col,
            ..
        } => PlacementCoord::new(sheet_id, row_start, col),
        PlacementDomain::ColRun {
            sheet_id,
            row,
            col_start,
            ..
        } => PlacementCoord::new(sheet_id, row, col_start),
        PlacementDomain::Rect {
            sheet_id,
            row_start,
            col_start,
            ..
        } => PlacementCoord::new(sheet_id, row_start, col_start),
    }
}

fn is_contiguous(values: &[u32]) -> bool {
    values
        .windows(2)
        .all(|window| window[0].saturating_add(1) == window[1])
}

fn mark_all_legacy(
    report: &mut FormulaPlacementReport,
    candidates: &[FormulaPlacementCandidate],
    reason: PlacementFallbackReason,
) {
    report.counters.legacy_cells = candidates.len() as u64;
    report
        .counters
        .fallback_reasons
        .insert(reason, candidates.len() as u64);
    report.results = candidates
        .iter()
        .map(|candidate| FormulaPlacementResult::Legacy {
            placement: candidate.placement(),
            reason,
        })
        .collect();
}

#[cfg(test)]
mod tests {
    use formualizer_parse::parser::parse;

    use super::super::runtime::FormulaResolution;
    use super::*;
    use crate::engine::arena::{CanonicalLabels, DataStore};
    use crate::engine::ingest_pipeline::DependencyPlanRow;
    use crate::engine::sheet_registry::SheetRegistry;
    use crate::reference::CellRef;

    fn candidate(
        data_store: &mut DataStore,
        sheet_registry: &SheetRegistry,
        sheet_id: SheetId,
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

    fn row_run_candidates(
        data_store: &mut DataStore,
        sheet_registry: &SheetRegistry,
        sheet_id: SheetId,
        cells: u32,
    ) -> Vec<FormulaPlacementCandidate> {
        (0..cells)
            .map(|row| {
                candidate(
                    data_store,
                    sheet_registry,
                    sheet_id,
                    row,
                    2,
                    &format!("=A{}+1", row + 1),
                )
            })
            .collect()
    }

    fn col_run_candidates(
        data_store: &mut DataStore,
        sheet_registry: &SheetRegistry,
        sheet_id: SheetId,
        cells: u32,
    ) -> Vec<FormulaPlacementCandidate> {
        (0..cells)
            .map(|col| {
                candidate(
                    data_store,
                    sheet_registry,
                    sheet_id,
                    2,
                    col,
                    &format!("={}1+1", column_label(col)),
                )
            })
            .collect()
    }

    fn assert_all_legacy(
        report: &FormulaPlacementReport,
        cells: u64,
        reason: PlacementFallbackReason,
    ) {
        assert_eq!(report.counters.formula_cells_seen, cells);
        assert_eq!(report.counters.accepted_span_cells, 0);
        assert_eq!(report.counters.legacy_cells, cells);
        assert_eq!(report.counters.spans_created, 0);
        assert_eq!(report.counters.templates_interned, 0);
        assert_eq!(report.counters.formula_vertices_avoided, 0);
        assert_eq!(report.counters.ast_roots_avoided, 0);
        assert_eq!(report.counters.edge_rows_avoided, 0);
        assert_eq!(report.counters.per_placement_formula_vertices_created, 0);
        assert_eq!(report.counters.per_placement_ast_roots_created, 0);
        assert_eq!(report.counters.per_placement_edge_rows_created, 0);
        assert_eq!(
            report.counters.fallback_reasons,
            BTreeMap::from([(reason, cells)])
        );
        assert_eq!(report.results.len(), cells as usize);
        assert!(report.results.iter().all(|result| matches!(
            result,
            FormulaPlacementResult::Legacy {
                reason: result_reason,
                ..
            } if *result_reason == reason
        )));
    }

    #[test]
    fn compressed_anchor_syntax_allowlist_checks_domains_and_xlsx_boundaries() {
        let vertical = PlacementDomain::row_run(0, 0, 99, 1);
        assert!(validate_anchor_once_syntax(&parse("=A1+2*3").unwrap(), 0, 1, &vertical).is_ok());
        assert!(
            validate_anchor_once_syntax(&parse("=A1048576").unwrap(), 0, 1, &vertical).is_err()
        );

        let horizontal = PlacementDomain::col_run(0, 0, 0, 99);
        assert!(validate_anchor_once_syntax(&parse("=XFD1").unwrap(), 0, 0, &horizontal).is_err());
        assert!(
            validate_anchor_once_syntax(&parse("=Sheet2!A1").unwrap(), 0, 0, &vertical).is_ok()
        );
        for unsupported in ["=SUM(A1:A2)", "=NamedValue", "=A1^2", "=A:A"] {
            assert!(
                validate_anchor_once_syntax(&parse(unsupported).unwrap(), 0, 0, &vertical).is_err(),
                "{unsupported}"
            );
        }

        let rect = PlacementDomain::rect(0, 3, 12, 4, 15);
        assert!(
            validate_anchor_once_syntax(&parse("=$A$1+B4+C4:D5").unwrap(), 3, 4, &rect).is_ok()
        );
    }

    #[test]
    fn compressed_preparation_uses_one_analysis_and_constant_size_report() {
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let candidate = candidate(&mut data_store, &sheet_registry, 0, 0, 1, "=$A$1+7");
        let ast = data_store
            .retrieve_ast(candidate.ast_id, &sheet_registry)
            .unwrap();
        let analysis = analyze_candidate(&candidate, &ast, &data_store, &sheet_registry).unwrap();
        for (domain, count) in [
            (PlacementDomain::row_run(0, 0, 999_999, 1), 1_000_000),
            (PlacementDomain::col_run(0, 0, 1, 100), 100),
            (PlacementDomain::rect(0, 0, 9, 1, 10), 100),
        ] {
            let report = prepare_compressed_family_shadow(&candidate, &analysis, domain, count);
            assert!(report.accepted, "{report:?}");
            assert_eq!(report.member_count, count);
            assert!(report.broadcast_binding_bytes < 128);
        }
        assert!(std::mem::size_of::<CompressedPreparationReport>() < 64);
    }

    #[test]
    fn compressed_shadow_and_direct_anchor_once_have_exhaustive_gate_parity() {
        fn assert_parity(
            candidate: &FormulaPlacementCandidate,
            analysis: &CandidateAnalysis,
            domain: PlacementDomain,
            count: u64,
        ) {
            let shadow =
                prepare_compressed_family_shadow(candidate, analysis, domain.clone(), count);
            let direct =
                prepare_anchor_once_family(candidate.clone(), analysis.clone(), domain, count);
            assert_eq!(shadow.accepted, direct.is_ok());
            assert_eq!(shadow.fallback_reason, direct.err());
        }

        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let anchor = candidate(&mut data_store, &sheet_registry, 0, 0, 1, "=$A$1+7");
        let ast = data_store
            .retrieve_ast(anchor.ast_id, &sheet_registry)
            .unwrap();
        let analysis = analyze_candidate(&anchor, &ast, &data_store, &sheet_registry).unwrap();
        let accepted = PlacementDomain::row_run(0, 0, 99, 1);
        assert_parity(&anchor, &analysis, accepted.clone(), 100);
        assert_parity(&anchor, &analysis, accepted.clone(), 99);
        assert_parity(
            &anchor,
            &analysis,
            PlacementDomain::row_run(1, 0, 99, 1),
            100,
        );
        assert_parity(
            &anchor,
            &analysis,
            PlacementDomain::row_run(0, 1, 100, 1),
            100,
        );

        let relative = candidate(&mut data_store, &sheet_registry, 0, 0, 1, "=A1+1");
        let ast = data_store
            .retrieve_ast(relative.ast_id, &sheet_registry)
            .unwrap();
        let relative_analysis =
            analyze_candidate(&relative, &ast, &data_store, &sheet_registry).unwrap();
        assert_parity(
            &relative,
            &relative_analysis,
            PlacementDomain::row_run(0, 0, 98, 1),
            99,
        );

        let chain = candidate(&mut data_store, &sheet_registry, 0, 1, 1, "=B1+1");
        let ast = data_store
            .retrieve_ast(chain.ast_id, &sheet_registry)
            .unwrap();
        let chain_analysis = analyze_candidate(&chain, &ast, &data_store, &sheet_registry).unwrap();
        assert_parity(
            &chain,
            &chain_analysis,
            PlacementDomain::row_run(0, 1, 100, 1),
            100,
        );

        let mut oversized = analysis.clone();
        oversized.literal_bindings =
            vec![LiteralValue::Text("x".repeat(MAX_BINDING_SET_BYTES + 1))].into_boxed_slice();
        assert_parity(&anchor, &oversized, accepted, 100);
    }

    #[test]
    fn compressed_preparation_preserves_small_domain_and_internal_dependency_gates() {
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let small = candidate(&mut data_store, &sheet_registry, 0, 0, 1, "=A1+1");
        let ast = data_store
            .retrieve_ast(small.ast_id, &sheet_registry)
            .unwrap();
        let analysis = analyze_candidate(&small, &ast, &data_store, &sheet_registry).unwrap();
        let report = prepare_compressed_family_shadow(
            &small,
            &analysis,
            PlacementDomain::row_run(0, 0, 98, 1),
            99,
        );
        assert_eq!(
            report.fallback_reason,
            Some(PlacementFallbackReason::SmallDomain)
        );

        let chain = candidate(&mut data_store, &sheet_registry, 0, 1, 1, "=B1+1");
        let ast = data_store
            .retrieve_ast(chain.ast_id, &sheet_registry)
            .unwrap();
        let analysis = analyze_candidate(&chain, &ast, &data_store, &sheet_registry).unwrap();
        let report = prepare_compressed_family_shadow(
            &chain,
            &analysis,
            PlacementDomain::row_run(0, 1, 100, 1),
            100,
        );
        assert_eq!(
            report.fallback_reason,
            Some(PlacementFallbackReason::InternalDependency)
        );
    }

    #[test]
    fn row_run_same_template_promotes_to_span() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let candidates = row_run_candidates(&mut data_store, &sheet_registry, 0, 100);
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.formula_cells_seen, 100);
        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.legacy_cells, 0);
        assert_eq!(report.counters.spans_created, 1);
        assert_eq!(report.counters.templates_interned, 1);
        assert!(matches!(
            report.results[0],
            FormulaPlacementResult::Span { .. }
        ));
        assert!(matches!(
            plane
                .resolve_formula_at(PlacementCoord::new(0, 1, 2), None)
                .resolution,
            FormulaResolution::SpanPlacement { .. }
        ));
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        let span_record = plane.spans.get(span).expect("span record");
        let read_summary_id = span_record
            .read_summary_id
            .expect("accepted span retains read summary");
        let read_summary = plane
            .span_read_summaries
            .get(read_summary_id)
            .expect("read summary");
        assert_eq!(read_summary.dependencies.len(), 1);
        assert_eq!(
            read_summary.dependencies[0].read_region,
            super::super::region_index::Region::col_interval(0, 0, 0, 99)
        );
    }

    #[test]
    fn col_run_same_template_promotes_to_span() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let candidates = col_run_candidates(&mut data_store, &sheet_registry, 0, 100);
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.spans_created, 1);
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        assert!(matches!(
            plane.spans.get(span).unwrap().domain,
            PlacementDomain::ColRun { .. }
        ));
    }

    #[test]
    fn rect_same_template_promotes_to_span() {
        // 10x10 rect of `=<col_left><row>+1` cells. Each cell reads its
        // immediate left neighbor; the read region for cols C:K lands inside
        // the family domain. This is an internal-dependency family that the FP
        // runtime would have to demote to whole-span recompute on every change,
        // so placement falls back to legacy.
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
                    &format!("={}{}+1", column_label(col - 1), row + 1),
                ));
            }
        }
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_all_legacy(&report, 100, PlacementFallbackReason::InternalDependency);
    }

    #[test]
    fn rect_anchored_external_reads_promotes_to_span() {
        // 10x10 rect of `=$A<row>+1` cells. Reads use a relative row but an
        // absolute column outside the rect, so there is no internal dependency
        // and the result varies by placement.
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
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.spans_created, 1);
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        assert!(matches!(
            plane.spans.get(span).unwrap().domain,
            PlacementDomain::Rect { .. }
        ));
    }

    #[test]
    fn placement_promotes_constant_result_family_and_marks_span() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 0, "=$Z$1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 0, "=$Z$1+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.counters.accepted_span_cells, 2);
        assert_eq!(report.counters.spans_created, 1);
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        assert!(plane.spans.get(span).unwrap().is_constant_result);
    }

    #[test]
    fn placement_from_ingested_marks_constant_result_family() {
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let candidate = candidate(&mut data_store, &sheet_registry, 0, 0, 0, "=$Z$1+1");
        let ingested = IngestedFormula {
            ast_id: candidate.ast_id,
            placement: CellRef::new_absolute(0, 1, 1),
            canonical_hash: 0x1234,
            exact_canonical_hash: 0x1234,
            exact_canonical_key: Arc::<str>::from("exact"),
            parameterized_canonical_hash: 0x1234,
            parameterized_canonical_key: Arc::<str>::from("param"),
            literal_slot_descriptors: Arc::from(Vec::new().into_boxed_slice()),
            literal_bindings: Vec::new().into_boxed_slice(),
            value_ref_slot_descriptors: Arc::from(Vec::new().into_boxed_slice()),
            template_slot_map: TemplateSlotMap::default(),
            labels: CanonicalLabels::default(),
            dep_plan: DependencyPlanRow::default(),
            read_summary: None,
            read_projections: Some(vec![ReadProjection {
                target_sheet_id: 0,
                rule: DirtyProjectionRule::AffineCell {
                    row: AxisProjection::Absolute { index: 0 },
                    col: AxisProjection::Absolute { index: 0 },
                },
            }]),
            read_projection_fallback: None,
            formula_text: None,
        };

        let analysis = CandidateAnalysis::from_ingested(&candidate, &ingested).unwrap();

        assert!(analysis.read_projections_constant);
    }

    #[test]
    fn placement_from_ingested_marks_absolute_whole_column_projection_constant() {
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let candidate = candidate(&mut data_store, &sheet_registry, 0, 0, 1, "=SUM($A:$A)");
        let ingested = IngestedFormula {
            ast_id: candidate.ast_id,
            placement: CellRef::new_absolute(0, 1, 2),
            canonical_hash: 0x5678,
            exact_canonical_hash: 0x5678,
            exact_canonical_key: Arc::<str>::from("exact"),
            parameterized_canonical_hash: 0x5678,
            parameterized_canonical_key: Arc::<str>::from("param"),
            literal_slot_descriptors: Arc::from(Vec::new().into_boxed_slice()),
            literal_bindings: Vec::new().into_boxed_slice(),
            value_ref_slot_descriptors: Arc::from(Vec::new().into_boxed_slice()),
            template_slot_map: TemplateSlotMap::default(),
            labels: CanonicalLabels::default(),
            dep_plan: DependencyPlanRow::default(),
            read_summary: None,
            read_projections: Some(vec![ReadProjection {
                target_sheet_id: 0,
                rule: DirtyProjectionRule::WholeColumnRange {
                    col_start: AxisProjection::Absolute { index: 0 },
                    col_end: AxisProjection::Absolute { index: 0 },
                },
            }]),
            read_projection_fallback: None,
            formula_text: None,
        };

        let analysis = CandidateAnalysis::from_ingested(&candidate, &ingested).unwrap();

        assert!(analysis.read_projections_constant);
    }

    #[test]
    fn placement_promotes_absolute_whole_column_sum_as_constant_span() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let mut sheet_registry = SheetRegistry::new();
        let sheet_id = sheet_registry.id_for("Sheet1");
        let candidates = (0..100)
            .map(|row| {
                candidate(
                    &mut data_store,
                    &sheet_registry,
                    sheet_id,
                    row,
                    1,
                    "=SUM($A:$A)",
                )
            })
            .collect();
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.spans_created, 1);
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        let span_record = plane.spans.get(span).expect("span record");
        assert!(span_record.is_constant_result);
        let read_summary = plane
            .span_read_summaries
            .get(span_record.read_summary_id.expect("read summary id"))
            .expect("read summary");
        assert_eq!(read_summary.dependencies.len(), 1);
        assert_eq!(
            read_summary.dependencies[0].read_region,
            super::super::region_index::Region::whole_col(sheet_id, 0)
        );
    }

    #[test]
    fn placement_promotes_whole_column_sum_with_relative_cell_as_non_constant_span() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let mut sheet_registry = SheetRegistry::new();
        let sheet_id = sheet_registry.id_for("Sheet1");
        let candidates = (0..100)
            .map(|row| {
                candidate(
                    &mut data_store,
                    &sheet_registry,
                    sheet_id,
                    row,
                    2,
                    &format!("=SUM($A:$A)-A{}", row + 1),
                )
            })
            .collect();
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.spans_created, 1);
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        let span_record = plane.spans.get(span).expect("span record");
        assert!(!span_record.is_constant_result);
        let read_summary = plane
            .span_read_summaries
            .get(span_record.read_summary_id.expect("read summary id"))
            .expect("read summary");
        assert_eq!(read_summary.dependencies.len(), 2);
        let read_regions = read_summary
            .dependencies
            .iter()
            .map(|dependency| dependency.read_region)
            .collect::<Vec<_>>();
        assert!(read_regions.contains(&super::super::region_index::Region::whole_col(sheet_id, 0)));
        assert!(
            read_regions.contains(&super::super::region_index::Region::col_interval(
                sheet_id, 0, 0, 99
            ))
        );
    }

    #[test]
    fn placement_promotes_sumifs_whole_column_ranges_as_constant_span() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let mut sheet_registry = SheetRegistry::new();
        let sheet_id = sheet_registry.id_for("Sheet1");
        let candidates = (0..100)
            .map(|row| {
                candidate(
                    &mut data_store,
                    &sheet_registry,
                    sheet_id,
                    row,
                    2,
                    "=SUMIFS($B:$B,$A:$A,\"Type1\")",
                )
            })
            .collect();
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.spans_created, 1);
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        let span_record = plane.spans.get(span).expect("span record");
        assert!(span_record.is_constant_result);
        let read_summary = plane
            .span_read_summaries
            .get(span_record.read_summary_id.expect("read summary id"))
            .expect("read summary");
        let read_regions = read_summary
            .dependencies
            .iter()
            .map(|dependency| dependency.read_region)
            .collect::<Vec<_>>();
        assert_eq!(read_regions.len(), 2);
        assert!(read_regions.contains(&super::super::region_index::Region::whole_col(sheet_id, 0)));
        assert!(read_regions.contains(&super::super::region_index::Region::whole_col(sheet_id, 1)));
    }

    #[test]
    fn placement_promotes_cross_sheet_absolute_whole_column_sum() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let mut sheet_registry = SheetRegistry::new();
        let sheet1_id = sheet_registry.id_for("Sheet1");
        let data_id = sheet_registry.id_for("DataA");
        let candidates = (0..100)
            .map(|row| {
                candidate(
                    &mut data_store,
                    &sheet_registry,
                    sheet1_id,
                    row,
                    1,
                    "=SUM(DataA!$A:$A)",
                )
            })
            .collect();
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.spans_created, 1);
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        let span_record = plane.spans.get(span).expect("span record");
        let read_summary = plane
            .span_read_summaries
            .get(span_record.read_summary_id.expect("read summary id"))
            .expect("read summary");
        assert_eq!(read_summary.dependencies.len(), 1);
        assert_eq!(
            read_summary.dependencies[0].read_region,
            super::super::region_index::Region::whole_col(data_id, 0)
        );
    }

    #[test]
    fn unique_formulas_remain_legacy() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![candidate(
                &mut data_store,
                &sheet_registry,
                0,
                0,
                0,
                "=A1+1",
            )],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(&report, 1, PlacementFallbackReason::SingletonUnique);
    }

    #[test]
    fn placement_rejects_without_supported_dependency_summary() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=A1:A2"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=A2:A3"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(
            &report,
            2,
            PlacementFallbackReason::UnsupportedDependencySummary,
        );
    }

    #[test]
    fn placement_does_not_promote_open_top_level_or_whole_row_ranges() {
        for (formula, reason) in [
            (
                "=SUM($A$1:$A)",
                PlacementFallbackReason::UnsupportedCanonicalTemplate,
            ),
            (
                "=$A:$A",
                PlacementFallbackReason::UnsupportedDependencySummary,
            ),
            (
                "=SUM($1:$1)",
                PlacementFallbackReason::UnsupportedDirtyProjection,
            ),
        ] {
            let mut plane = FormulaPlane::default();
            let mut data_store = DataStore::new();
            let sheet_registry = SheetRegistry::new();
            let report = place_candidate_family(
                &mut plane,
                vec![
                    candidate(&mut data_store, &sheet_registry, 0, 0, 2, formula),
                    candidate(&mut data_store, &sheet_registry, 0, 1, 2, formula),
                ],
                &data_store,
                &sheet_registry,
            );

            assert_all_legacy(&report, 2, reason);
        }
    }

    #[test]
    fn unsupported_dynamic_formula_remains_legacy_with_reason() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 0, "=RAND()"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 0, "=RAND()"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(
            &report,
            2,
            PlacementFallbackReason::UnsupportedCanonicalTemplate,
        );
    }

    #[test]
    fn gapped_row_run_remains_legacy() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 2, "=A3+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(&report, 2, PlacementFallbackReason::UnsupportedShapeOrGaps);
    }

    #[test]
    fn gapped_col_run_remains_legacy() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 2, 0, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 2, "=C1+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(&report, 2, PlacementFallbackReason::UnsupportedShapeOrGaps);
    }

    #[test]
    fn rect_with_missing_cell_remains_legacy() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 1, 1, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=B1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 1, "=A2+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(&report, 3, PlacementFallbackReason::UnsupportedShapeOrGaps);
    }

    #[test]
    fn duplicate_placement_remains_legacy_with_reason() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=A1+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(&report, 2, PlacementFallbackReason::DuplicatePlacement);
    }

    #[test]
    fn unknown_explicit_sheet_binding_remains_legacy_with_reason() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=Sheet2!A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=Sheet2!A2+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(&report, 2, PlacementFallbackReason::UnknownSheetBinding);
    }

    #[test]
    fn explicit_sheet_binding_with_known_sheet_promotes() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let mut sheet_registry = SheetRegistry::new();
        let sheet1_id = sheet_registry.id_for("Sheet1");
        let sheet2_id = sheet_registry.id_for("Sheet2");
        let candidates = (0..100)
            .map(|row| {
                candidate(
                    &mut data_store,
                    &sheet_registry,
                    sheet1_id,
                    row,
                    2,
                    &format!("=Sheet2!A{}+1", row + 1),
                )
            })
            .collect();
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.spans_created, 1);
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        let span_record = plane.spans.get(span).expect("span record");
        let read_summary = plane
            .span_read_summaries
            .get(span_record.read_summary_id.expect("read summary id"))
            .expect("read summary");
        assert_eq!(read_summary.dependencies.len(), 1);
        assert_eq!(
            read_summary.dependencies[0].read_region,
            super::super::region_index::Region::col_interval(sheet2_id, 0, 0, 99)
        );
    }

    #[test]
    fn mixed_sheet_candidates_remain_legacy_with_reason() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 1, 1, 2, "=A2+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(
            &report,
            2,
            PlacementFallbackReason::CrossSheetOrSheetMismatch,
        );
    }

    #[test]
    fn placement_promotes_supported_mixed_anchor_family() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let candidates = (0..100)
            .map(|row| {
                candidate(
                    &mut data_store,
                    &sheet_registry,
                    0,
                    row,
                    2,
                    &format!("=$A{}+B$1", row + 1),
                )
            })
            .collect();
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.formula_cells_seen, 100);
        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.legacy_cells, 0);
        assert_eq!(report.counters.spans_created, 1);
        assert_eq!(report.counters.templates_interned, 1);
        assert!(report.counters.fallback_reasons.is_empty());
        assert!(
            report
                .results
                .iter()
                .all(|result| matches!(result, FormulaPlacementResult::Span { .. }))
        );
        let FormulaPlacementResult::Span { span, .. } = report.results[0] else {
            panic!("expected span result");
        };
        let span_record = plane.spans.get(span).expect("span record");
        let read_summary = plane
            .span_read_summaries
            .get(span_record.read_summary_id.expect("read summary id"))
            .expect("read summary");
        let mut read_regions = read_summary
            .dependencies
            .iter()
            .map(|dependency| dependency.read_region)
            .collect::<Vec<_>>();
        read_regions.sort_by_key(|region| format!("{region:?}"));
        assert_eq!(read_regions.len(), 2);
        assert!(
            read_regions.contains(&super::super::region_index::Region::col_interval(
                0, 0, 0, 99
            ))
        );
        assert!(read_regions.contains(&super::super::region_index::Region::point(0, 0, 1)));
    }

    #[test]
    fn non_equivalent_formula_never_promotes() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=A1+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(&report, 2, PlacementFallbackReason::NonEquivalentTemplate);
    }

    #[test]
    fn accepted_row_run_avoids_per_placement_vertices_ast_and_edges() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let candidates = row_run_candidates(&mut data_store, &sheet_registry, 0, 100);
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        assert_eq!(report.counters.accepted_span_cells, 100);
        assert_eq!(report.counters.per_placement_formula_vertices_created, 0);
        assert_eq!(report.counters.per_placement_ast_roots_created, 0);
        assert_eq!(report.counters.per_placement_edge_rows_created, 0);
        assert_eq!(report.counters.formula_vertices_avoided, 100);
        assert_eq!(report.counters.ast_roots_avoided, 99);
        assert_eq!(report.counters.edge_rows_avoided, 100);
    }

    #[test]
    fn span_virtual_formula_matches_legacy_formula_text_or_ast_relocation() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let candidates = row_run_candidates(&mut data_store, &sheet_registry, 0, 100);
        let report = place_candidate_family(&mut plane, candidates, &data_store, &sheet_registry);

        let FormulaPlacementResult::Span {
            span, template_id, ..
        } = report.results[1]
        else {
            panic!("expected span result");
        };
        let handle = plane.resolve_formula_at(PlacementCoord::new(0, 1, 2), None);

        assert_eq!(
            handle.resolution,
            FormulaResolution::SpanPlacement {
                span,
                template_id,
                placement: PlacementCoord::new(0, 1, 2),
            }
        );
        assert_eq!(
            plane
                .templates
                .get(template_id)
                .unwrap()
                .formula_text
                .as_deref(),
            Some("=A1+1")
        );
        assert_eq!(report.counters.per_placement_formula_vertices_created, 0);
    }
}
