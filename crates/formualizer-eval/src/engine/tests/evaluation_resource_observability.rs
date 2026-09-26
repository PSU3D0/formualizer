use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use formualizer_common::LiteralValue;
use formualizer_parse::ExcelErrorKind;
use formualizer_parse::parser::parse;

use crate::engine::{
    DiskScratchPolicy, Engine, EvalConfig, EvaluationBudgets, EvaluationRequestKind,
    EvaluationRequestOutcome, EvaluationResourceClass, EvaluationResourceReason,
    FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode, FormulaPlaneTopologyStrategy,
    ScratchResourceBudget,
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
    let error = engine
        .evaluate_all_cancellable(crate::engine::CancelToken::from_flag(cancel))
        .unwrap_err();
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
    assert_eq!(request.staged_selected, 1);
    assert_eq!(request.staged_retained, 1);

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
fn candidate_overflow_retains_topology_for_next_request_hit() {
    let mut config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    config.max_formula_plane_cache_candidates = 1;
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}*2")));
        formulas.push(record(&mut engine, row, 3, &format!("=B{row}+1")));
        formulas.push(record(&mut engine, row, 4, &format!("=C{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();

    engine.evaluate_all().unwrap();
    let overflow = engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 100, 4),
        Some(LiteralValue::Number(202.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(9.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    let hit = engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(20.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 100, 4),
        Some(LiteralValue::Number(202.0))
    );
}

/// The value assertion of `perpetual_cache_skip_preserves_values_streak_and_no_disk_policy`
/// under each scratch policy (its topology-strategy and cache-skip
/// counters are span-internal).
#[test]
fn perpetual_cache_skip_preserves_values_streak_and_no_disk_policy_values() {
    for (policy, scratch_limit) in [
        (DiskScratchPolicy::NativeTemporary, 20_000),
        (DiskScratchPolicy::MemoryOnly, 30_000),
        (DiskScratchPolicy::MemoryOnly, 19_000),
    ] {
        let mut engine = build_mode_engine(FormulaPlaneMode::AuthoritativeExperimental, None);
        engine.config.max_formula_plane_cache_candidates = 0;
        engine.set_evaluation_budgets_for_test(EvaluationBudgets {
            scratch: ScratchResourceBudget {
                total_bytes: Some(scratch_limit),
                schedule_discovery_bytes: Some(scratch_limit),
                disk_scratch_policy: Some(policy),
                ..ScratchResourceBudget::default()
            },
            ..EvaluationBudgets::default()
        });
        for request in 1..=3_u64 {
            if request > 1 {
                engine
                    .set_cell_value("Sheet1", request as u32, 1, LiteralValue::Number(10.0))
                    .unwrap();
            }
            engine.evaluate_all().unwrap();
            let stats = engine.last_evaluation_resource_request_stats().unwrap();
            assert_eq!(stats.ledger.scratch_current, 0);
        }
        assert_eq!(
            engine.get_cell_value("Sheet1", 100, 2),
            Some(LiteralValue::Number(200.0))
        );
    }
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
