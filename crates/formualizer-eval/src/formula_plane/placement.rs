//! Internal FormulaPlane span-placement substrate for FP6.2.
//!
//! This module promotes already-collected candidate formula families into the
//! inert FormulaPlane runtime stores. It does not wire FormulaPlane into graph
//! build, dirty propagation, scheduling, or evaluation.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use formualizer_parse::parser::ASTNode;

use crate::SheetId;
use crate::engine::arena::{AstNodeId, DataStore};
use crate::engine::ingest_pipeline::IngestedFormula;
use crate::engine::sheet_registry::SheetRegistry;

use super::dependency_summary::{FormulaClass, summarize_canonical_template};
use super::ids::FormulaTemplateId;
use super::producer::{
    ProjectionFallbackReason, ReadProjection, SpanReadDependency, SpanReadSummary,
};
use super::region_index::RegionPattern;
use super::runtime::{
    FormulaPlane, FormulaSpanRef, NewFormulaSpan, PlacementCoord, PlacementDomain, ResultRegion,
};
use super::template_canonical::canonicalize_template;

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
    /// At least one read region intersects the family's own result region.
    /// These families have an internal/self dependency that the current span
    /// runtime cannot evaluate as bounded dirty work and demotes to whole-span
    /// recompute on every change, producing O(N²) edit recalc on chains.
    InternalDependency,
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
    place_analyzed_family(plane, &candidates, &analyses)
}

pub(crate) fn place_candidate_family_with_analyses(
    plane: &mut FormulaPlane,
    candidates: Vec<FormulaPlacementCandidate>,
    analyses: Vec<CandidateAnalysis>,
) -> FormulaPlacementReport {
    place_analyzed_family(plane, &candidates, &analyses)
}

pub(crate) struct CandidateAnalysis {
    sheet_id: SheetId,
    row: u32,
    col: u32,
    canonical_hash: u64,
    canonical_key: Arc<str>,
    read_projections: Vec<ReadProjection>,
}

impl CandidateAnalysis {
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
        let read_projections = ingested.read_projections.clone().ok_or_else(|| {
            if ingested.read_projection_fallback
                == Some(ProjectionFallbackReason::UnsupportedSheetBinding)
            {
                PlacementFallbackReason::UnknownSheetBinding
            } else {
                PlacementFallbackReason::UnsupportedDependencySummary
            }
        })?;
        Ok(Self {
            sheet_id: candidate.sheet_id,
            row: candidate.row,
            col: candidate.col,
            canonical_hash: ingested.canonical_hash,
            canonical_key: Arc::<str>::from(format!("fp8:{:016x}", ingested.canonical_hash)),
            read_projections,
        })
    }
}

pub(crate) fn analyze_candidate(
    candidate: &FormulaPlacementCandidate,
    ast: &ASTNode,
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
    let payload = template.key.payload();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    payload.hash(&mut hasher);
    Ok(CandidateAnalysis {
        sheet_id: candidate.sheet_id,
        row: candidate.row,
        col: candidate.col,
        canonical_hash: hasher.finish(),
        canonical_key: Arc::<str>::from(payload),
        read_projections,
    })
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
            analyze_candidate(candidate, &ast, sheet_registry)
        })
        .collect()
}

fn place_analyzed_family(
    plane: &mut FormulaPlane,
    candidates: &[FormulaPlacementCandidate],
    analyses: &[CandidateAnalysis],
) -> FormulaPlacementReport {
    let mut report = FormulaPlacementReport::default();
    report.counters.formula_cells_seen = candidates.len() as u64;

    if candidates.is_empty() {
        return report;
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
            candidates,
            PlacementFallbackReason::CrossSheetOrSheetMismatch,
        );
        return report;
    }

    if analyses
        .iter()
        .any(|analysis| analysis.canonical_hash != first.canonical_hash)
    {
        mark_all_legacy(
            &mut report,
            candidates,
            PlacementFallbackReason::NonEquivalentTemplate,
        );
        return report;
    }

    let domain = match detect_domain(analyses) {
        Ok(domain) => domain,
        Err(reason) => {
            mark_all_legacy(&mut report, candidates, reason);
            return report;
        }
    };

    let origin = domain_origin(&domain);
    let origin_analysis = analyses
        .iter()
        .find(|analysis| analysis.placement() == origin)
        .ok_or(PlacementFallbackReason::UnsupportedShapeOrGaps);
    let origin_analysis = match origin_analysis {
        Ok(origin_analysis) => origin_analysis,
        Err(reason) => {
            mark_all_legacy(&mut report, candidates, reason);
            return report;
        }
    };

    let result_region = ResultRegion::scalar_cells(domain.clone());
    let result_region_pattern = RegionPattern::from_domain(&domain);
    let read_summary = match span_read_summary_for_domain(
        result_region_pattern,
        &origin_analysis.read_projections,
    ) {
        Ok(summary) => summary,
        Err(_reason) => {
            mark_all_legacy(
                &mut report,
                candidates,
                PlacementFallbackReason::UnsupportedDirtyProjection,
            );
            return report;
        }
    };

    // Reject internal-dependency families. If any precedent read region
    // intersects the family's own result region, the span has self/internal
    // dependencies (e.g. chains where B[r] reads B[r-1]). Bounded dirty
    // projection cannot represent the cell-by-cell sequencing these need, so
    // the runtime would always demote to whole-span recompute and produce
    // O(N²) edit recalc behavior. Defer to legacy graph scheduling instead.
    if read_summary
        .dependencies
        .iter()
        .any(|dep| dep.read_region.intersects(&read_summary.result_region))
    {
        mark_all_legacy(
            &mut report,
            candidates,
            PlacementFallbackReason::InternalDependency,
        );
        return report;
    }

    let Some(origin_candidate) = candidates
        .iter()
        .find(|candidate| candidate.placement() == origin)
    else {
        mark_all_legacy(
            &mut report,
            candidates,
            PlacementFallbackReason::UnsupportedShapeOrGaps,
        );
        return report;
    };

    let template_count_before = plane.templates.len();
    let template_id = plane.intern_template(
        first.canonical_key.clone(),
        origin_candidate.ast_id,
        origin_candidate.row + 1,
        origin_candidate.col + 1,
        origin_candidate.formula_text.clone(),
    );
    if plane.templates.len() > template_count_before {
        report.counters.templates_interned = 1;
    }

    let read_summary_id = plane.insert_span_read_summary(read_summary);
    let span = plane.insert_span(NewFormulaSpan {
        sheet_id,
        template_id,
        result_region,
        domain,
        intrinsic_mask_id: None,
        read_summary_id: Some(read_summary_id),
    });

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

fn span_read_summary_for_domain(
    result_region: RegionPattern,
    projections: &[ReadProjection],
) -> Result<SpanReadSummary, crate::formula_plane::producer::ProjectionFallbackReason> {
    let mut dependencies = Vec::new();
    for &read_projection in projections {
        let projection = read_projection.rule;
        let read_region =
            projection.read_region_for_result(read_projection.target_sheet_id, result_region)?;
        let dependency = SpanReadDependency {
            read_region,
            projection,
        };
        if !dependencies.contains(&dependency) {
            dependencies.push(dependency);
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
    use crate::engine::arena::DataStore;
    use crate::engine::sheet_registry::SheetRegistry;

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
    fn row_run_same_template_promotes_to_span() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=A2+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 2, "=A3+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.counters.formula_cells_seen, 3);
        assert_eq!(report.counters.accepted_span_cells, 3);
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
            super::super::region_index::RegionPattern::col_interval(0, 0, 0, 2)
        );
    }

    #[test]
    fn col_run_same_template_promotes_to_span() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 2, 0, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 1, "=B1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 2, "=C1+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.counters.accepted_span_cells, 3);
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
        // 2x2 rect of `=<col_left><row>+1` cells. Each cell reads its
        // immediate left neighbor; the read region for col=2 cells lands on
        // the col=1 cells which are also in the family domain. This is an
        // internal-dependency family that the FP runtime would have to demote
        // to whole-span recompute on every change, so placement falls back to
        // legacy.
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 1, 1, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=B1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 1, "=A2+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 2, "=B2+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_all_legacy(&report, 4, PlacementFallbackReason::InternalDependency);
    }

    #[test]
    fn rect_anchored_external_reads_promotes_to_span() {
        // 2x2 rect of `=$A$1+1` cells. Reads are anchored to a single cell
        // outside the rect, so no internal dependency.
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 1, 1, "=$A$1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=$A$1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 1, "=$A$1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 2, "=$A$1+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.counters.accepted_span_cells, 4);
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
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=SUM(A1:A2)"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=SUM(A2:A3)"),
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
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(
                    &mut data_store,
                    &sheet_registry,
                    sheet1_id,
                    0,
                    2,
                    "=Sheet2!A1+1",
                ),
                candidate(
                    &mut data_store,
                    &sheet_registry,
                    sheet1_id,
                    1,
                    2,
                    "=Sheet2!A2+1",
                ),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.counters.accepted_span_cells, 2);
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
            super::super::region_index::RegionPattern::col_interval(sheet2_id, 0, 0, 1)
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
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=$A1+B$1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=$A2+B$1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 2, "=$A3+B$1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.counters.formula_cells_seen, 3);
        assert_eq!(report.counters.accepted_span_cells, 3);
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
            read_regions.contains(&super::super::region_index::RegionPattern::col_interval(
                0, 0, 0, 2
            ))
        );
        assert!(read_regions.contains(&super::super::region_index::RegionPattern::point(0, 0, 1)));
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
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=A2+1"),
                candidate(&mut data_store, &sheet_registry, 0, 2, 2, "=A3+1"),
            ],
            &data_store,
            &sheet_registry,
        );

        assert_eq!(report.counters.accepted_span_cells, 3);
        assert_eq!(report.counters.per_placement_formula_vertices_created, 0);
        assert_eq!(report.counters.per_placement_ast_roots_created, 0);
        assert_eq!(report.counters.per_placement_edge_rows_created, 0);
        assert_eq!(report.counters.formula_vertices_avoided, 3);
        assert_eq!(report.counters.ast_roots_avoided, 2);
        assert_eq!(report.counters.edge_rows_avoided, 3);
    }

    #[test]
    fn span_virtual_formula_matches_legacy_formula_text_or_ast_relocation() {
        let mut plane = FormulaPlane::default();
        let mut data_store = DataStore::new();
        let sheet_registry = SheetRegistry::new();
        let report = place_candidate_family(
            &mut plane,
            vec![
                candidate(&mut data_store, &sheet_registry, 0, 0, 2, "=A1+1"),
                candidate(&mut data_store, &sheet_registry, 0, 1, 2, "=A2+1"),
            ],
            &data_store,
            &sheet_registry,
        );

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
