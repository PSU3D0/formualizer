use formualizer_eval::engine::{
    CycleTelemetry, EngineBaselineStats, EvalDelta, EvalDeltaRecord, EvalResult,
    EvaluationRequestKind, EvaluationRequestOutcome, EvaluationRequestPhaseTimings,
    EvaluationResourceBaselineStats, EvaluationResourceLedgerRequestStats,
    EvaluationResourceRequestStats, FormulaPlaneRouteEvent, FormulaPlaneTopologyRequestStats,
    OpaqueReason, PreparationOutcome, PreparationRevision, PrepareScope, PreparedTargetGraphReport,
    TableMetadata, TargetEvalDelta, VirtualDepTelemetry,
};
use formualizer_eval::function_registry::{
    RegistrationError, ResolvedFunction, SemanticConformanceIssue, SemanticContractResolution,
};

fn read_target_reports(
    preparation: &PreparedTargetGraphReport,
    revision: &PreparationRevision,
    delta: &TargetEvalDelta,
    record: &EvalDeltaRecord,
) {
    let _ = (
        preparation.request_id,
        preparation.requested_targets,
        revision.graph,
        delta.version,
        record.sheet_id(),
    );
}

fn read_engine_reports(
    result: &EvalResult,
    table: &TableMetadata,
    baseline: &EngineBaselineStats,
    virtual_deps: &VirtualDepTelemetry,
    cycles: &CycleTelemetry,
    delta: &EvalDelta,
) {
    let _ = (
        result.computed_vertices,
        table.name.as_str(),
        baseline.graph_vertex_count,
        virtual_deps.candidate_vertices_total,
        cycles.static_sccs,
        delta.changed_cells.len(),
    );
}

fn read_resource_reports(
    route: &FormulaPlaneRouteEvent,
    topology: &FormulaPlaneTopologyRequestStats,
    ledger: &EvaluationResourceLedgerRequestStats,
    phases: &EvaluationRequestPhaseTimings,
    request: &EvaluationResourceRequestStats,
    baseline: &EvaluationResourceBaselineStats,
) {
    let _ = (
        route.island_id,
        topology.candidates_observed,
        ledger.retained_peak,
        phases.total_ns,
        request.request_id,
        baseline.requests_started,
    );
}

fn match_future_enums(
    scope: PrepareScope,
    reason: OpaqueReason,
    outcome: PreparationOutcome,
    record: EvalDeltaRecord,
    issue: SemanticConformanceIssue,
    registration: RegistrationError,
    request_kind: EvaluationRequestKind,
    request_outcome: EvaluationRequestOutcome,
) {
    let _ = match scope {
        PrepareScope::Exact => 0,
        _ => 1,
    };
    let _ = match reason {
        OpaqueReason::DynamicReference => 0,
        _ => 1,
    };
    let _ = match outcome {
        PreparationOutcome::Prepared => 0,
        _ => 1,
    };
    let _ = match record {
        EvalDeltaRecord::Run { .. } | EvalDeltaRecord::Region { .. } => 0,
        _ => 1,
    };
    let _ = match issue {
        SemanticConformanceIssue::CapabilityPanicked => 0,
        _ => 1,
    };
    let _ = match registration {
        RegistrationError::NameMetadataPanicked => 0,
        _ => 1,
    };
    let _ = match request_kind {
        EvaluationRequestKind::Vertex => 0,
        _ => 1,
    };
    let _ = match request_outcome {
        EvaluationRequestOutcome::InProgress => 0,
        _ => 1,
    };
}

fn read_semantic_reports(resolution: &SemanticContractResolution, function: &ResolvedFunction) {
    let _ = (
        resolution.generation,
        resolution.trusted_builtin,
        function.namespace.as_str(),
        function.canonical_name.as_str(),
    );
}

#[test]
fn downstream_report_readers_and_wildcard_matches_compile() {
    let _ = read_target_reports
        as fn(&PreparedTargetGraphReport, &PreparationRevision, &TargetEvalDelta, &EvalDeltaRecord);
    let _ = read_engine_reports
        as fn(
            &EvalResult,
            &TableMetadata,
            &EngineBaselineStats,
            &VirtualDepTelemetry,
            &CycleTelemetry,
            &EvalDelta,
        );
    let _ = read_resource_reports
        as fn(
            &FormulaPlaneRouteEvent,
            &FormulaPlaneTopologyRequestStats,
            &EvaluationResourceLedgerRequestStats,
            &EvaluationRequestPhaseTimings,
            &EvaluationResourceRequestStats,
            &EvaluationResourceBaselineStats,
        );
    let _ = match_future_enums
        as fn(
            PrepareScope,
            OpaqueReason,
            PreparationOutcome,
            EvalDeltaRecord,
            SemanticConformanceIssue,
            RegistrationError,
            EvaluationRequestKind,
            EvaluationRequestOutcome,
        );
    let _ = read_semantic_reports as fn(&SemanticContractResolution, &ResolvedFunction);
}
