use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use formualizer_common::LiteralValue;
use formualizer_parse::ExcelErrorKind;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, EvaluationRequestKind, EvaluationRequestOutcome, EvaluationResourceClass,
    EvaluationResourceReason, FormulaDirtyLeaseOutcome, FormulaIngestBatch, FormulaIngestRecord,
    FormulaPlaneMode, FormulaPlaneTopologyCacheOutcome, FormulaPlaneTopologyStrategy,
};
use crate::test_workbook::TestWorkbook;

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn build_mode_engine(mode: FormulaPlaneMode, cache_bytes: Option<usize>) -> Engine<TestWorkbook> {
    let mut config = EvalConfig::default().with_formula_plane_mode(mode);
    if let Some(cache_bytes) = cache_bytes {
        config.max_formula_plane_cache_bytes = cache_bytes;
    }
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}*2")));
    }
    formulas.push(record(&mut engine, 1, 3, "=B100+1"));
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    engine
}

#[test]
fn resource_taxonomy_is_stable_and_observational() {
    assert_eq!(
        EvaluationResourceReason::FormulaPlaneTopologyCandidates.class(),
        EvaluationResourceClass::Optimization
    );
    assert_eq!(
        EvaluationResourceReason::FormulaPlaneTopologyRetainedBytes.class(),
        EvaluationResourceClass::RetainedMemory
    );
    assert_eq!(
        EvaluationResourceReason::FormulaPlaneMaterializationCells.class(),
        EvaluationResourceClass::Admission
    );
    assert_eq!(
        EvaluationResourceReason::FormulaPlaneTopologyEdges.as_str(),
        "formula_plane_topology_edges"
    );
}

#[test]
fn request_ids_accumulate_and_reset_without_reuse() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1+1").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    let first = *engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(first.request_id, 1);
    assert_eq!(first.kind, EvaluationRequestKind::Full);
    assert_eq!(first.outcome, EvaluationRequestOutcome::Success);
    assert_eq!(
        first.topology.strategy,
        FormulaPlaneTopologyStrategy::Legacy
    );

    engine.evaluate_cell("Sheet1", 1, 1).unwrap();
    let second = *engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(second.request_id, 2);
    assert_eq!(second.kind, EvaluationRequestKind::Cell);
    assert_eq!(second.outcome, EvaluationRequestOutcome::Success);

    let total = engine.evaluation_resource_baseline_stats();
    assert_eq!(total.last_request_id, 2);
    assert_eq!(total.requests_started, 2);
    assert_eq!(total.requests_succeeded, 2);
    assert_eq!(total.requests_cancelled, 0);
    assert_eq!(total.requests_errored, 0);

    engine.reset_evaluation_resource_telemetry();
    assert_eq!(
        engine.evaluation_resource_baseline_stats(),
        Default::default()
    );
    assert!(engine.last_evaluation_resource_request_stats().is_none());

    engine.evaluate_all().unwrap();
    let third = *engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(third.request_id, 3, "reset must not reuse request IDs");
    let total = engine.evaluation_resource_baseline_stats();
    assert_eq!(total.requests_started, 1);
    assert_eq!(total.requests_succeeded, 1);
}

#[test]
fn cancellation_and_error_outcomes_accumulate_deterministically() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1+1").unwrap())
        .unwrap();

    let cancel = Arc::new(AtomicBool::new(true));
    let error = engine.evaluate_all_cancellable(cancel).unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    let cancelled = engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(cancelled.outcome, EvaluationRequestOutcome::Cancelled);

    let error = engine.evaluate_cell("Sheet1", 0, 1).unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Ref);
    let failed = engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(failed.outcome, EvaluationRequestOutcome::Error);

    let total = engine.evaluation_resource_baseline_stats();
    assert_eq!(total.requests_started, 2);
    assert_eq!(total.requests_cancelled, 1);
    assert_eq!(total.requests_errored, 1);
}

#[test]
fn deferred_preparation_records_selected_and_restored_staging() {
    let config = EvalConfig {
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::default(), config);
    engine.graph.add_sheet("Other").unwrap();
    engine.stage_formula_text("Sheet1", 1, 1, "=1+1".to_string());
    engine.stage_formula_text("Other", 1, 1, "=2+2".to_string());

    engine.evaluate_cell("Sheet1", 1, 1).unwrap();
    let request = engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(request.staged_selected, 2);
    assert_eq!(request.staged_retained, 0);

    let mut failed = Engine::new(
        TestWorkbook::default(),
        EvalConfig {
            defer_graph_building: true,
            ..EvalConfig::default()
        },
    );
    failed.graph.add_sheet("Other").unwrap();
    failed.stage_formula_text("Sheet1", 1, 1, "=1+".to_string());
    failed.stage_formula_text("Other", 1, 1, "=2+2".to_string());
    assert!(failed.evaluate_all().is_err());
    let request = failed.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(request.outcome, EvaluationRequestOutcome::Error);
    assert_eq!(request.staged_selected, 2);
    assert_eq!(request.staged_retained, 2);
    assert_eq!(failed.staged_formula_count(), 2);
}

#[test]
fn topology_build_hit_and_overflow_materialization_are_exactly_observed() {
    let mut cached = build_mode_engine(FormulaPlaneMode::AuthoritativeExperimental, None);
    assert_eq!(cached.baseline_stats().formula_plane_active_span_count, 1);
    cached.evaluate_all().unwrap();
    let built = cached.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        built.topology.cache_outcome,
        FormulaPlaneTopologyCacheOutcome::Built
    );
    assert_eq!(
        built.topology.strategy,
        FormulaPlaneTopologyStrategy::CompiledAndCached
    );
    assert!(built.topology.producers_observed >= 2);
    assert!(built.topology.retained_bytes_observed > 0);
    assert_eq!(built.topology.cache_build_events, 1);
    assert_eq!(built.topology.cache_hit_events, 0);
    assert_eq!(built.topology.cache_skip_events, 0);
    assert_eq!(built.fallback_materialized_cells, 0);
    assert_eq!(built.dirty_lease, FormulaDirtyLeaseOutcome::Acknowledged);

    cached
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(9.0))
        .unwrap();
    cached.evaluate_all().unwrap();
    let hit = cached.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        hit.topology.cache_outcome,
        FormulaPlaneTopologyCacheOutcome::Hit
    );
    assert_eq!(hit.topology.strategy, FormulaPlaneTopologyStrategy::Cached);
    assert_eq!(hit.topology.cache_hit_events, 1);
    assert_eq!(hit.topology.cache_build_events, 0);
    assert_eq!(hit.topology.cache_skip_events, 0);
    assert_eq!(hit.topology.producers_observed, 0);
    assert_eq!(hit.topology.candidates_observed, 0);
    assert_eq!(hit.topology.edges_observed, 0);
    let totals = cached.evaluation_resource_baseline_stats();
    assert_eq!(totals.topology_cache_builds, 1);
    assert_eq!(totals.topology_cache_hits, 1);

    let mut overflow = build_mode_engine(FormulaPlaneMode::AuthoritativeExperimental, Some(0));
    overflow.evaluate_all().unwrap();
    let request = overflow.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        request.topology.cache_outcome,
        FormulaPlaneTopologyCacheOutcome::SkippedOverflow
    );
    assert_eq!(
        request.topology.overflow_reason,
        Some(EvaluationResourceReason::FormulaPlaneTopologyRetainedBytes)
    );
    assert_eq!(request.topology.cache_build_events, 1);
    assert_eq!(request.topology.cache_skip_events, 1);
    assert!(request.topology.byte_cap_hits > 0);
    assert!(request.topology.retained_bytes_observed > 0);
    assert_eq!(request.fallback_materialized_cells, 100);
    assert_eq!(overflow.baseline_stats().formula_plane_active_span_count, 0);
    let totals = overflow.evaluation_resource_baseline_stats();
    assert_eq!(totals.topology_cache_skips, 1);
    assert_eq!(totals.topology_byte_cap_hits, 1);
    assert_eq!(totals.fallback_materialized_cells_total, 100);

    let mut candidate = build_mode_engine(FormulaPlaneMode::AuthoritativeExperimental, None);
    candidate.config.max_formula_plane_cache_candidates = 0;
    candidate.evaluate_all().unwrap();
    let request = candidate.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        request.topology.overflow_reason,
        Some(EvaluationResourceReason::FormulaPlaneTopologyCandidates)
    );
    assert!(request.topology.candidate_cap_hits > 0);
    assert!(request.topology.candidates_observed > 0);

    let mut edge = build_mode_engine(FormulaPlaneMode::AuthoritativeExperimental, None);
    edge.config.max_formula_plane_cache_edges = 0;
    edge.evaluate_all().unwrap();
    let request = edge.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        request.topology.overflow_reason,
        Some(EvaluationResourceReason::FormulaPlaneTopologyEdges)
    );
    assert!(request.topology.edge_cap_hits > 0);
    assert!(request.topology.edges_observed > 0);
}

#[test]
fn off_shadow_and_authoritative_values_remain_equal() {
    let run = |mode| {
        let mut engine = build_mode_engine(mode, None);
        engine.evaluate_all().unwrap();
        (
            engine.get_cell_value("Sheet1", 100, 2),
            engine.get_cell_value("Sheet1", 1, 3),
        )
    };

    let off = run(FormulaPlaneMode::Off);
    let shadow = run(FormulaPlaneMode::Shadow);
    let authoritative = run(FormulaPlaneMode::AuthoritativeExperimental);
    assert_eq!(off, shadow);
    assert_eq!(off, authoritative);
    assert_eq!(off.0, Some(LiteralValue::Number(200.0)));
    assert_eq!(off.1, Some(LiteralValue::Number(201.0)));
}
