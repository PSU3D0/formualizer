use std::sync::Arc;

use super::arena::AstNodeId;
use super::formula_ingest::{FormulaIngestRecord, FormulaIngestReport};
use super::formula_source::{
    DeferredReplayFormula, FormulaReplayDisposition, PartitionLegacyMember,
    PartitionLegacyMemberKind, PartitionedSourceFormulaFamily, SourceCoord, SourceFamilyId,
};
use super::graph::DependencyGraph;
use super::graph::prepared_legacy_graph::{PreparedLegacyGraphError, PreparedLegacyGraphPlan};
use super::ingest_pipeline::{DependencyPlanRow, FormulaAstInput, IngestedFormula};
use super::named_range::{NameScope, NamedDefinition};
use super::vertex::VertexId;
use crate::SheetId;
use crate::formula_plane::append::{
    FormulaPlaneAppendError, FormulaPlaneAppendReport, FormulaPlaneAppendWork,
    PreparedFormulaPlaneAppend,
};
use crate::formula_plane::placement::PreparedAnchorOncePlacement;
use crate::formula_plane::region_index::Region;
use crate::formula_plane::runtime::PlacementDomain;
use crate::reference::{CellRef, Coord};
use crate::traits::FunctionProvider;
use formualizer_parse::parser::ASTNode;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FragmentedNameAssumption {
    name: String,
    sheet_id: SheetId,
    scope: NameScope,
    definition: NamedDefinition,
    vertex: VertexId,
}

pub(crate) struct PreparedPartitionedSourceFamily {
    pub(crate) engine_token: Arc<()>,
    pub(crate) sheet_id: SheetId,
    pub(crate) sheet_name: Arc<str>,
    pub(crate) source: PartitionedSourceFormulaFamily,
    pub(crate) placements: Vec<PreparedAnchorOncePlacement>,
    pub(crate) function_semantic_epoch: u64,
    pub(crate) function_provider_revision: Option<u64>,
    pub(crate) function_semantics_used: bool,
    pub(crate) name_assumptions: Vec<FragmentedNameAssumption>,
    pub(crate) direct_cells: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedFragmentedLegacyFormula {
    source_id: SourceFamilyId,
    coord: SourceCoord,
    kind: PartitionLegacyMemberKind,
    formula_text: Arc<str>,
    ast_id: AstNodeId,
    plan: DependencyPlanRow,
}

impl PreparedFragmentedLegacyFormula {
    fn formula_text_matches_ast(formula_text: &str, stored_ast: &ASTNode) -> bool {
        let formula = if formula_text.starts_with('=') {
            formula_text.to_string()
        } else {
            format!("={formula_text}")
        };
        formualizer_parse::parser::parse(&formula).is_ok_and(|parsed| {
            formualizer_parse::pretty::canonical_formula(&parsed)
                == formualizer_parse::pretty::canonical_formula(stored_ast)
        })
    }

    fn exact_record_provenance_matches(
        source_id: SourceFamilyId,
        expected: PartitionLegacyMember,
        replay: &DeferredReplayFormula,
        record: &FormulaIngestRecord,
    ) -> bool {
        let Some(replay_coord) = replay
            .row
            .checked_sub(1)
            .zip(replay.col.checked_sub(1))
            .map(|(row, col)| SourceCoord { row, col })
        else {
            return false;
        };
        let expected_owner = match expected.kind {
            PartitionLegacyMemberKind::SharedFamilyMember => {
                replay.family == Some(source_id)
                    && replay
                        .partition_owner
                        .is_none_or(|owner| owner == source_id)
            }
            PartitionLegacyMemberKind::OrdinaryException => {
                replay.family.is_none() && replay.partition_owner == Some(source_id)
            }
        };
        let Some(record_text) = record.formula_text.as_deref() else {
            return false;
        };
        let normalized_replay = replay.text.strip_prefix('=').unwrap_or(&replay.text);
        let normalized_record = record_text.strip_prefix('=').unwrap_or(record_text);
        replay_coord == expected.coord
            && record.row == replay.row
            && record.col == replay.col
            && expected_owner
            && normalized_replay == normalized_record
    }

    fn from_verified_exact_replay_analysis(
        source_id: SourceFamilyId,
        expected: PartitionLegacyMember,
        replay: DeferredReplayFormula,
        record: FormulaIngestRecord,
        ingested: IngestedFormula,
    ) -> Result<Self, FragmentedTransactionPrepareError> {
        if !Self::exact_record_provenance_matches(source_id, expected, &replay, &record) {
            return Err(FragmentedTransactionPrepareError::LegacyProvenance);
        }
        let record_text = record.formula_text.as_deref().unwrap_or_default();
        if record.ast_id != ingested.ast_id
            || ingested.placement.coord.row() != expected.coord.row
            || ingested.placement.coord.col() != expected.coord.col
            || ingested.formula_text.as_deref() != Some(record_text)
        {
            return Err(FragmentedTransactionPrepareError::LegacyProvenance);
        }
        Ok(Self {
            source_id,
            coord: expected.coord,
            kind: expected.kind,
            formula_text: Arc::from(record_text),
            ast_id: record.ast_id,
            plan: ingested.dep_plan,
        })
    }

    #[cfg(test)]
    pub(crate) fn exact_formula_text_for_test(&self) -> &str {
        &self.formula_text
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FragmentedTransactionWork {
    pub(crate) fragments_checked: usize,
    pub(crate) legacy_members_checked: usize,
    pub(crate) legacy_formulas_staged: usize,
    pub(crate) names_checked: usize,
    pub(crate) cross_fragment_regions_checked: usize,
    pub(crate) direct_cells: u64,
    pub(crate) holes_observed: u64,
    pub(crate) plane: FormulaPlaneAppendWork,
}

#[derive(Debug)]
pub(crate) struct PreparedFragmentedSourceTransaction {
    engine_token: Arc<()>,
    source_id: SourceFamilyId,
    source: PartitionedSourceFormulaFamily,
    expected_disposition: FormulaReplayDisposition,
    function_semantic_epoch: u64,
    function_provider_revision: Option<u64>,
    function_semantics_used: bool,
    name_assumptions: Vec<FragmentedNameAssumption>,
    legacy_graph: PreparedLegacyGraphPlan,
    formula_plane: PreparedFormulaPlaneAppend,
    report_delta: FormulaIngestReport,
    work: FragmentedTransactionWork,
}

impl PreparedFragmentedSourceTransaction {
    #[cfg(test)]
    pub(crate) fn semantic_revisions_for_test(&self) -> (u64, Option<u64>) {
        (
            self.function_semantic_epoch,
            self.function_provider_revision,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FragmentedTransactionPrepareError {
    EngineIdentity,
    SourceIdentity,
    SheetIdentity,
    DispositionOwnership,
    NameBinding,
    CrossFragmentDependency,
    LegacyProvenance,
    LegacyOwnership,
    InvalidCoordinate(SourceCoord),
    LegacyGraph(PreparedLegacyGraphError),
    FormulaPlane(FormulaPlaneAppendError),
}

impl FragmentedTransactionPrepareError {
    pub(crate) fn selects_whole_family_replay(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FragmentedCommitFault {
    None,
    DispositionCheck,
    SemanticRevisionCheck,
    LegacyGraphFinalCheck,
    FormulaPlaneFinalCheck,
    BeforeFirstMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FragmentedReplayReason {
    Injected(FragmentedCommitFault),
    EngineIdentityChanged,
    DispositionChanged,
    FunctionProviderRevisionChanged,
    FunctionSemanticEpochChanged,
    NameBindingChanged,
    LegacyGraphStale(PreparedLegacyGraphError),
    FormulaPlaneStale(FormulaPlaneAppendError),
}

#[derive(Debug)]
pub(crate) struct FragmentedCommitSuccess {
    pub(crate) source_id: SourceFamilyId,
    pub(crate) graph_formulas: usize,
    pub(crate) plane: FormulaPlaneAppendReport,
    pub(crate) report_delta: FormulaIngestReport,
    pub(crate) work: FragmentedTransactionWork,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum FragmentedCommitDecision {
    Committed(FragmentedCommitSuccess),
    ReplayWholeFamily {
        source_id: SourceFamilyId,
        reason: FragmentedReplayReason,
    },
}

fn placement_domain(
    sheet_id: SheetId,
    transport: super::formula_source::PlacementDomainTransport,
) -> PlacementDomain {
    match transport {
        super::formula_source::PlacementDomainTransport::RowRun {
            row_start,
            row_end,
            col,
        } => PlacementDomain::row_run(sheet_id, row_start, row_end, col),
        super::formula_source::PlacementDomainTransport::ColRun {
            row,
            col_start,
            col_end,
        } => PlacementDomain::col_run(sheet_id, row, col_start, col_end),
        super::formula_source::PlacementDomainTransport::Rect(rect) => PlacementDomain::rect(
            sheet_id,
            rect.start.row,
            rect.end.row,
            rect.start.col,
            rect.end.col,
        ),
    }
}

impl DependencyGraph {
    pub(crate) fn analyze_fragmented_exact_replay_record(
        &mut self,
        function_provider: &dyn FunctionProvider,
        sheet_id: SheetId,
        source_id: SourceFamilyId,
        expected: PartitionLegacyMember,
        replay: DeferredReplayFormula,
    ) -> Result<PreparedFragmentedLegacyFormula, FragmentedTransactionPrepareError> {
        if self.sheet_reg().name(sheet_id).is_empty() {
            return Err(FragmentedTransactionPrepareError::SheetIdentity);
        }
        if replay.row == 0 || replay.col == 0 {
            return Err(FragmentedTransactionPrepareError::LegacyProvenance);
        }
        let formula = if replay.text.starts_with('=') {
            replay.text.clone()
        } else {
            format!("={}", replay.text)
        };
        let ast = formualizer_parse::parser::parse(&formula)
            .map_err(|_| FragmentedTransactionPrepareError::LegacyProvenance)?;
        let ast_id = self.store_ast(&ast);
        let placement = CellRef::new(
            sheet_id,
            Coord::from_excel(replay.row, replay.col, true, true),
        );
        let ingested = self
            .ingest_pipeline(function_provider)
            .enable_function_semantics()
            .ingest_formula(
                FormulaAstInput::RawArena(ast_id),
                placement,
                Some(Arc::from(formula.clone())),
            )
            .map_err(|_| FragmentedTransactionPrepareError::LegacyProvenance)?;
        let record = FormulaIngestRecord::new(
            replay.row,
            replay.col,
            ingested.ast_id,
            Some(Arc::from(formula)),
        );
        let stored_ast = self
            .data_store()
            .retrieve_ast(record.ast_id, self.sheet_reg())
            .ok_or(FragmentedTransactionPrepareError::LegacyProvenance)?;
        if !PreparedFragmentedLegacyFormula::formula_text_matches_ast(
            record.formula_text.as_deref().unwrap_or_default(),
            &stored_ast,
        ) {
            return Err(FragmentedTransactionPrepareError::LegacyProvenance);
        }
        PreparedFragmentedLegacyFormula::from_verified_exact_replay_analysis(
            source_id, expected, replay, record, ingested,
        )
    }

    pub(crate) fn capture_fragmented_name_assumptions(
        &self,
        sheet_id: SheetId,
        names: &[String],
    ) -> Result<Vec<FragmentedNameAssumption>, FragmentedTransactionPrepareError> {
        let mut names = names.to_vec();
        names.sort();
        names.dedup();
        names
            .into_iter()
            .map(|name| {
                let entry = self
                    .resolve_name_entry(&name, sheet_id)
                    .ok_or(FragmentedTransactionPrepareError::NameBinding)?;
                Ok(FragmentedNameAssumption {
                    name,
                    sheet_id,
                    scope: entry.scope,
                    definition: entry.definition.clone(),
                    vertex: entry.vertex,
                })
            })
            .collect()
    }

    fn fragmented_name_assumptions_match(&self, assumptions: &[FragmentedNameAssumption]) -> bool {
        assumptions.iter().all(|assumption| {
            self.resolve_name_entry(&assumption.name, assumption.sheet_id)
                .is_some_and(|entry| {
                    entry.scope == assumption.scope
                        && entry.definition == assumption.definition
                        && entry.vertex == assumption.vertex
                })
        })
    }

    pub(crate) fn prepare_fragmented_source_transaction(
        &self,
        engine_token: &Arc<()>,
        source: &PartitionedSourceFormulaFamily,
        disposition: &FormulaReplayDisposition,
        prepared: PreparedPartitionedSourceFamily,
        mut legacy_formulas: Vec<PreparedFragmentedLegacyFormula>,
    ) -> Result<PreparedFragmentedSourceTransaction, FragmentedTransactionPrepareError> {
        if !Arc::ptr_eq(engine_token, &prepared.engine_token) {
            return Err(FragmentedTransactionPrepareError::EngineIdentity);
        }
        if &prepared.source != source || !source.reconciles_compact_geometry() {
            return Err(FragmentedTransactionPrepareError::SourceIdentity);
        }
        if self.sheet_reg().name(prepared.sheet_id) != prepared.sheet_name.as_ref()
            || self.sheet_reg().get_id(&prepared.sheet_name) != Some(prepared.sheet_id)
        {
            return Err(FragmentedTransactionPrepareError::SheetIdentity);
        }
        if !disposition.owns_partition_exactly(source) {
            return Err(FragmentedTransactionPrepareError::DispositionOwnership);
        }
        if !self.fragmented_name_assumptions_match(&prepared.name_assumptions) {
            return Err(FragmentedTransactionPrepareError::NameBinding);
        }
        let template_ast = prepared
            .placements
            .first()
            .map(|placement| placement.ownership_proof().3);
        let exact_placements =
            prepared.placements.len() == source.fragments.len()
                && prepared.placements.iter().zip(&source.fragments).all(
                    |(placement, fragment)| {
                        let (sheet, origin_row, origin_col, ast_id, domain, member_count) =
                            placement.ownership_proof();
                        let expected_domain = placement_domain(prepared.sheet_id, *fragment);
                        sheet == prepared.sheet_id
                            && origin_row == source.template_origin0.row
                            && origin_col == source.template_origin0.col
                            && Some(ast_id) == template_ast
                            && domain == &expected_domain
                            && member_count == expected_domain.cell_count()
                    },
                );
        let expected_direct_cells = source
            .fragments
            .iter()
            .map(|fragment| {
                let rect = fragment.rect();
                (u64::from(rect.end.row - rect.start.row) + 1)
                    * (u64::from(rect.end.col - rect.start.col) + 1)
            })
            .sum::<u64>();
        if !exact_placements || prepared.direct_cells != expected_direct_cells {
            return Err(FragmentedTransactionPrepareError::SourceIdentity);
        }
        let fragment_regions: Vec<_> = prepared
            .placements
            .iter()
            .map(|placement| Region::from_domain(placement.fragment_dependency_proof().0))
            .collect();
        let mut cross_fragment_regions_checked = 0usize;
        for (fragment_index, placement) in prepared.placements.iter().enumerate() {
            let (_, read_summary) = placement.fragment_dependency_proof();
            for dependency in &read_summary.dependencies {
                for (candidate_index, candidate) in fragment_regions.iter().enumerate() {
                    if candidate_index == fragment_index {
                        continue;
                    }
                    cross_fragment_regions_checked = cross_fragment_regions_checked
                        .checked_add(1)
                        .ok_or(FragmentedTransactionPrepareError::CrossFragmentDependency)?;
                    if dependency.read_region.intersects(candidate) {
                        return Err(FragmentedTransactionPrepareError::CrossFragmentDependency);
                    }
                }
            }
        }

        legacy_formulas.sort_by_key(|formula| formula.coord);
        let expected = source.legacy_members.as_slice();
        if legacy_formulas.len() != expected.len()
            || legacy_formulas
                .iter()
                .zip(expected)
                .any(|(formula, member)| {
                    formula.source_id != source.source_id
                        || formula.coord != member.coord
                        || formula.kind != member.kind
                })
        {
            return Err(FragmentedTransactionPrepareError::LegacyOwnership);
        }
        let planned = legacy_formulas
            .into_iter()
            .map(|formula| {
                let row = formula.coord.row.checked_add(1).ok_or(
                    FragmentedTransactionPrepareError::InvalidCoordinate(formula.coord),
                )?;
                let col = formula.coord.col.checked_add(1).ok_or(
                    FragmentedTransactionPrepareError::InvalidCoordinate(formula.coord),
                )?;
                Ok((row, col, formula.ast_id, formula.plan))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let legacy_graph = self
            .prepare_legacy_graph_plan(prepared.sheet_id, planned)
            .map_err(FragmentedTransactionPrepareError::LegacyGraph)?;
        let formula_plane = self
            .formula_authority()
            .prepare_formula_plane_append(prepared.placements, self.data_store(), self.sheet_reg())
            .map_err(FragmentedTransactionPrepareError::FormulaPlane)?;

        let shared_legacy_cells = source.legacy_members.shared_member_count() as u64;
        let ordinary_cells = source.legacy_members.ordinary_exception_count() as u64;
        let mut report_delta = FormulaIngestReport::with_mode(super::FormulaPlaneMode::Shadow);
        report_delta.formula_cells_seen =
            source.surviving_member_count.saturating_add(ordinary_cells);
        report_delta.graph_formula_cells_materialized = expected.len() as u64;
        report_delta.shadow_candidate_cells = source.surviving_member_count;
        report_delta.shadow_accepted_span_cells = prepared.direct_cells;
        report_delta.shadow_fallback_cells = shared_legacy_cells;
        report_delta.graph_formula_vertices_avoided_shadow = prepared.direct_cells;
        report_delta.ast_roots_avoided_shadow = prepared
            .direct_cells
            .saturating_sub(source.fragments.len() as u64);
        report_delta.edge_rows_avoided_shadow = prepared.direct_cells;
        report_delta.source_anchor_parses = 1;
        report_delta.source_anchor_asts = 1;
        report_delta.source_anchor_analyses = 1;
        report_delta.source_partitioned_families_seen = 1;
        report_delta.source_partitioned_families_prepared = 1;
        report_delta.source_partition_fragments_prepared = source.fragments.len() as u64;
        report_delta.source_partition_span_cells_prepared = prepared.direct_cells;
        report_delta.source_partition_fallback_cells = shared_legacy_cells;
        report_delta.source_partition_holes = source.reconciliation.holes;
        report_delta.source_partition_surviving_cells = source.surviving_member_count;
        report_delta.source_partition_ordinary_exceptions =
            source.reconciliation.ordinary_exceptions;
        report_delta.source_partition_analyses_reused =
            (source.fragments.len() as u64).saturating_sub(1);
        report_delta.source_partition_function_semantics =
            u64::from(prepared.function_semantics_used);
        let work = FragmentedTransactionWork {
            fragments_checked: source.fragments.len(),
            legacy_members_checked: expected.len(),
            legacy_formulas_staged: expected.len(),
            names_checked: prepared.name_assumptions.len(),
            cross_fragment_regions_checked,
            direct_cells: prepared.direct_cells,
            holes_observed: source.reconciliation.holes,
            plane: formula_plane.work().clone(),
        };
        Ok(PreparedFragmentedSourceTransaction {
            engine_token: prepared.engine_token,
            source_id: source.source_id,
            source: source.clone(),
            expected_disposition: disposition.clone(),
            function_semantic_epoch: prepared.function_semantic_epoch,
            function_provider_revision: prepared.function_provider_revision,
            function_semantics_used: prepared.function_semantics_used,
            name_assumptions: prepared.name_assumptions,
            legacy_graph,
            formula_plane,
            report_delta,
            work,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_fragmented_source_transaction(
        &mut self,
        prepared: PreparedFragmentedSourceTransaction,
        engine_token: &Arc<()>,
        disposition: &FormulaReplayDisposition,
        function_semantic_epoch: u64,
        function_provider_revision: Option<u64>,
        final_provider_revision: impl FnOnce() -> Option<u64>,
        fault: FragmentedCommitFault,
    ) -> FragmentedCommitDecision {
        let replay = |reason| FragmentedCommitDecision::ReplayWholeFamily {
            source_id: prepared.source_id,
            reason,
        };
        if fault == FragmentedCommitFault::DispositionCheck {
            return replay(FragmentedReplayReason::Injected(fault));
        }
        if !Arc::ptr_eq(engine_token, &prepared.engine_token) {
            return replay(FragmentedReplayReason::EngineIdentityChanged);
        }
        if disposition != &prepared.expected_disposition
            || !disposition.owns_partition_exactly(&prepared.source)
        {
            return replay(FragmentedReplayReason::DispositionChanged);
        }

        if fault == FragmentedCommitFault::SemanticRevisionCheck {
            return replay(FragmentedReplayReason::Injected(fault));
        }
        if prepared.function_semantics_used
            && prepared.function_provider_revision != function_provider_revision
        {
            return replay(FragmentedReplayReason::FunctionProviderRevisionChanged);
        }
        if prepared.function_semantics_used
            && prepared.function_semantic_epoch != function_semantic_epoch
        {
            return replay(FragmentedReplayReason::FunctionSemanticEpochChanged);
        }
        if !self.fragmented_name_assumptions_match(&prepared.name_assumptions) {
            return replay(FragmentedReplayReason::NameBindingChanged);
        }

        if fault == FragmentedCommitFault::LegacyGraphFinalCheck {
            return replay(FragmentedReplayReason::Injected(fault));
        }
        if let Err(error) = self.validate_prepared_legacy_graph_plan(&prepared.legacy_graph) {
            return replay(FragmentedReplayReason::LegacyGraphStale(error));
        }

        if fault == FragmentedCommitFault::FormulaPlaneFinalCheck {
            return replay(FragmentedReplayReason::Injected(fault));
        }
        if let Err(error) = self
            .formula_authority()
            .validate_prepared_formula_plane_append(
                &prepared.formula_plane,
                self.data_store(),
                self.sheet_reg(),
            )
        {
            return replay(FragmentedReplayReason::FormulaPlaneStale(error));
        }

        if !self.fragmented_name_assumptions_match(&prepared.name_assumptions) {
            return replay(FragmentedReplayReason::NameBindingChanged);
        }
        if prepared.function_semantics_used
            && prepared.function_provider_revision != final_provider_revision()
        {
            return replay(FragmentedReplayReason::FunctionProviderRevisionChanged);
        }

        if fault == FragmentedCommitFault::BeforeFirstMutation {
            return replay(FragmentedReplayReason::Injected(fault));
        }

        let graph_formulas = self.apply_prevalidated_legacy_graph_plan(prepared.legacy_graph);
        let plane = self
            .formula_authority_mut()
            .apply_prevalidated_formula_plane_append(prepared.formula_plane);
        FragmentedCommitDecision::Committed(FragmentedCommitSuccess {
            source_id: prepared.source_id,
            graph_formulas,
            plane,
            report_delta: prepared.report_delta,
            work: prepared.work,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_replay_formula_text_must_match_the_stored_ast() {
        let stored = formualizer_parse::parser::parse("=A3+1").unwrap();
        assert!(PreparedFragmentedLegacyFormula::formula_text_matches_ast(
            "=A3+1", &stored
        ));
        assert!(!PreparedFragmentedLegacyFormula::formula_text_matches_ast(
            "=B3+1", &stored
        ));
    }

    #[test]
    fn ordinary_exact_replay_provenance_rejects_a_separate_formula_record() {
        let source_id = SourceFamilyId {
            sheet_instance: 7,
            source_index: 9,
        };
        let expected = PartitionLegacyMember {
            coord: SourceCoord { row: 3, col: 2 },
            kind: PartitionLegacyMemberKind::OrdinaryException,
        };
        let replay = DeferredReplayFormula {
            row: 4,
            col: 3,
            text: "=$A$1+5".to_string(),
            family: None,
            partition_owner: Some(source_id),
        };
        let replacement =
            FormulaIngestRecord::new(4, 3, AstNodeId::from_u32(1), Some(Arc::from("=$A$1+99")));
        assert!(
            !PreparedFragmentedLegacyFormula::exact_record_provenance_matches(
                source_id,
                expected,
                &replay,
                &replacement,
            )
        );
    }
}
