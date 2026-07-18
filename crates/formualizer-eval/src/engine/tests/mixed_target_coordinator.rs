use crate::engine::target_preparation::TargetProducer;
use crate::engine::{
    Engine, EvalConfig, EvalDeltaRecord, EvaluationBudgets, EvaluationTarget, FormulaIngestBatch,
    FormulaIngestRecord, FormulaPlaneMode, OptimizationResourceBudget,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, ExcelErrorExtra, LiteralValue, ResourceExhaustionReason};
use formualizer_parse::parser::parse;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::active_span_gate_audit::build_engine_with_active_spans;

fn cell(sheet: &str, row: u32, col: u32) -> EvaluationTarget {
    EvaluationTarget::Cell {
        sheet: sheet.to_string(),
        row,
        col,
    }
}

fn assert_replan_exhaustion(error: &ExcelError) {
    let ExcelErrorExtra::Resource { detail } = &error.extra else {
        panic!("expected typed resource exhaustion, got {error:?}");
    };
    assert_eq!(detail.reason, ResourceExhaustionReason::WorkUnits);
    assert_eq!(detail.limit, 5);
    assert_eq!(detail.observed, 6);
}

fn formula_record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: String,
) -> FormulaIngestRecord {
    let ast = parse(&formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

struct MidSpanCancelFn {
    name: &'static str,
    calls: Arc<AtomicUsize>,
    trip_at: Arc<AtomicUsize>,
    cancel: Arc<AtomicBool>,
}

impl crate::function::Function for MidSpanCancelFn {
    fn name(&self) -> &'static str {
        self.name
    }

    fn semantic_contract(
        &self,
        _arity: usize,
    ) -> Option<crate::function_contract::FunctionSemanticContract> {
        Some(crate::function_contract::FunctionSemanticContract::trusted_builtin_default(None))
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel).saturating_add(1);
        if call >= self.trip_at.load(Ordering::Acquire) {
            self.cancel.store(true, Ordering::Release);
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
    }
}

fn mid_span_cancellation_engine(
    name: &'static str,
    enable_parallel: bool,
) -> (
    Engine<TestWorkbook>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicBool>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let trip_at = Arc::new(AtomicUsize::new(usize::MAX));
    let cancel = Arc::new(AtomicBool::new(false));
    crate::function_registry::register_function(Arc::new(MidSpanCancelFn {
        name,
        calls: calls.clone(),
        trip_at: trip_at.clone(),
        cancel: cancel.clone(),
    }));
    let config = EvalConfig {
        enable_parallel,
        max_threads: enable_parallel.then_some(4),
        ..EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
    };
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut formulas = Vec::new();
    for row in 1..=600_u32 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(formula_record(
            &mut engine,
            row,
            2,
            format!("=A{row}+{name}()"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    (engine, calls, trip_at, cancel)
}

fn assert_mid_span_cancellation_is_transactional(name: &'static str, enable_parallel: bool) {
    let (mut engine, calls, trip_at, cancel) = mid_span_cancellation_engine(name, enable_parallel);
    let old_first = engine.get_cell_value("Sheet1", 1, 2);
    let old_middle = engine.get_cell_value("Sheet1", 300, 2);
    for row in 1..=600_u32 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number((row + 1_000) as f64))
            .unwrap();
    }
    let pending = engine.graph.pending_formula_dirty_event_count();
    assert!(pending > 0);
    calls.store(0, Ordering::Release);
    trip_at.store(100, Ordering::Release);
    cancel.store(false, Ordering::Release);

    let error = engine
        .evaluate_all_cancellable(cancel.clone())
        .expect_err("mid-span cancellation must abort the request");
    assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Cancelled);
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), old_first);
    assert_eq!(engine.get_cell_value("Sheet1", 300, 2), old_middle);
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), pending);

    trip_at.store(usize::MAX, Ordering::Release);
    cancel.store(false, Ordering::Release);
    engine.evaluate_all_cancellable(cancel).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(1_001.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 300, 2),
        Some(LiteralValue::Number(1_300.0))
    );
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
}

fn independent_span_engine() -> Engine<TestWorkbook> {
    let config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut left = Vec::new();
    let mut right = Vec::new();
    for row in 1..=200 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 3, LiteralValue::Number(row as f64))
            .unwrap();
        left.push(formula_record(&mut engine, row, 2, format!("=A{row}*2")));
        right.push(formula_record(&mut engine, row, 4, format!("=C{row}*3")));
    }
    engine
        .ingest_formula_batches(vec![
            FormulaIngestBatch::new("Sheet1", left),
            FormulaIngestBatch::new("Sheet1", right),
        ])
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine
}

#[test]
fn target_roots_distinguish_span_legacy_and_value_only_cells() {
    let mut engine = build_engine_with_active_spans();
    let roots = engine
        .resolve_target_producers(&[cell("Sheet1", 100, 2), cell("Sheet1", 100, 1)])
        .unwrap();
    assert!(
        roots
            .iter()
            .any(|root| matches!(root, TargetProducer::Span { .. }))
    );
    assert!(
        roots
            .iter()
            .any(|root| matches!(root, TargetProducer::ValueOnly(_)))
    );

    engine
        .set_cell_formula("Sheet1", 100, 3, parse("=B100+1").unwrap())
        .unwrap();
    let roots = engine
        .resolve_target_producers(&[cell("Sheet1", 100, 3)])
        .unwrap();
    assert!(
        roots
            .iter()
            .any(|root| matches!(root, TargetProducer::Legacy(_)))
    );
}

#[test]
fn large_range_target_root_dedup_uses_one_hash_probe_per_candidate() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    let rows = 4_096_u32;
    for row in 1..=rows {
        engine
            .set_cell_formula("Sheet1", row, 2, parse("=1").unwrap())
            .unwrap();
    }
    let range = EvaluationTarget::Range(
        formualizer_common::RangeAddress::new("Sheet1", 1, 2, rows, 2).unwrap(),
    );
    Engine::<TestWorkbook>::reset_target_root_dedup_probes_for_test();
    let roots = engine
        .resolve_target_producers(&[range.clone(), range])
        .unwrap();
    let probes = Engine::<TestWorkbook>::target_root_dedup_probes_for_test();

    assert_eq!(roots.len(), rows as usize);
    assert_eq!(probes, rows as usize * 2);
}

#[test]
fn mixed_span_legacy_scc_target_evaluation_demotes_and_stamps_cycle() {
    let config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut span = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        span.push(formula_record(
            &mut engine,
            row,
            2,
            format!("=A{row}+$C$50"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", span)])
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 50, 3, parse("=B50").unwrap())
        .unwrap();

    engine.evaluate_targets(&[cell("Sheet1", 50, 2)]).unwrap();
    assert!(matches!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Error(ref error))
            if error.kind == formualizer_common::ExcelErrorKind::Circ
    ));
    assert!(matches!(
        engine.get_cell_value("Sheet1", 50, 3),
        Some(LiteralValue::Error(ref error))
            if error.kind == formualizer_common::ExcelErrorKind::Circ
    ));
}

#[test]
fn spill_child_target_resolves_to_anchor_producer() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    let roots = engine
        .resolve_target_producers(&[cell("Sheet1", 2, 1)])
        .unwrap();
    assert_eq!(roots.len(), 1);
    let TargetProducer::Legacy(root) = roots[0] else {
        panic!("spill child must route to its legacy anchor");
    };
    assert_eq!(engine.graph.get_cell_ref(root).unwrap().coord.row(), 0);
}

#[test]
fn sparse_and_warm_target_requests_skip_mixed_topology_construction() {
    let config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut sparse = Engine::new(TestWorkbook::default(), config);
    sparse
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(7.0))
        .unwrap();
    sparse.evaluate_cell("Sheet1", 1, 1).unwrap();
    let sparse_request = sparse.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        sparse_request.topology.strategy,
        crate::engine::FormulaPlaneTopologyStrategy::SkippedNoActiveSpans
    );
    assert_eq!(
        sparse
            .baseline_stats()
            .formula_plane_mixed_topology_cache_builds,
        0
    );

    let mut warm = independent_span_engine();
    let builds = warm
        .baseline_stats()
        .formula_plane_mixed_topology_cache_builds;
    warm.evaluate_cell("Sheet1", 100, 2).unwrap();
    let warm_request = warm.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        warm_request.topology.strategy,
        crate::engine::FormulaPlaneTopologyStrategy::SkippedNoDirtyWork
    );
    assert_eq!(
        warm.baseline_stats()
            .formula_plane_mixed_topology_cache_builds,
        builds
    );
}

#[test]
fn target_evaluation_leaves_unrelated_dirty_span_branch_pending() {
    let mut engine = independent_span_engine();
    engine
        .set_cell_value("Sheet1", 100, 1, LiteralValue::Number(500.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 100, 3, LiteralValue::Number(700.0))
        .unwrap();
    let unrelated_before = engine.get_cell_value("Sheet1", 100, 4);
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 2);

    assert_eq!(
        engine.evaluate_cell("Sheet1", 100, 2).unwrap(),
        Some(LiteralValue::Number(1000.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 100, 4), unrelated_before);
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 1);

    assert_eq!(
        engine.evaluate_cell("Sheet1", 100, 4).unwrap(),
        Some(LiteralValue::Number(2100.0))
    );
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
}

#[test]
fn target_cache_overflow_selects_exact_strategy_without_demotion() {
    let mut engine = independent_span_engine();
    engine
        .set_cell_formula("Sheet1", 1, 5, parse("=B25+1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.set_evaluation_budgets_for_test(EvaluationBudgets {
        optimization: OptimizationResourceBudget {
            mixed_cache_candidates: Some(0),
            ..OptimizationResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    });
    engine
        .set_cell_value("Sheet1", 25, 1, LiteralValue::Number(111.0))
        .unwrap();
    assert_eq!(
        engine.evaluate_cell("Sheet1", 1, 5).unwrap(),
        Some(LiteralValue::Number(223.0))
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    let strategy = engine
        .last_evaluation_resource_request_stats()
        .unwrap()
        .topology
        .strategy;
    assert!(
        matches!(
            strategy,
            crate::engine::FormulaPlaneTopologyStrategy::ExactPagedIndexed
                | crate::engine::FormulaPlaneTopologyStrategy::ExactInMemoryRuns
                | crate::engine::FormulaPlaneTopologyStrategy::ExactNativeScratch
                | crate::engine::FormulaPlaneTopologyStrategy::ExactRepeatedPasses
        ),
        "strategy={strategy:?}"
    );
}

#[test]
fn capacity_fallback_acknowledges_full_selected_legacy_sublease_without_growth() {
    let mut engine = independent_span_engine();
    engine
        .set_cell_value("Sheet1", 50, 1, LiteralValue::Number(300.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 100, 3, LiteralValue::Number(400.0))
        .unwrap();
    engine.force_non_cycle_schedule_fallback_for_test();

    assert_eq!(
        engine.evaluate_cell("Sheet1", 50, 2).unwrap(),
        Some(LiteralValue::Number(600.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 100, 4),
        Some(LiteralValue::Number(300.0))
    );
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 1);
    assert_eq!(
        engine.evaluate_cell("Sheet1", 100, 4).unwrap(),
        Some(LiteralValue::Number(1200.0))
    );
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
}

#[test]
fn cancellation_acknowledges_no_dirty_sublease_and_retry_converges() {
    let mut engine = independent_span_engine();
    engine
        .set_cell_value("Sheet1", 50, 1, LiteralValue::Number(123.0))
        .unwrap();
    let pending = engine.graph.pending_formula_dirty_event_count();
    let cancelled = Arc::new(AtomicBool::new(true));
    assert!(
        engine
            .evaluate_cells_cancellable(&[("Sheet1", 50, 2)], cancelled)
            .is_err()
    );
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), pending);
    assert_eq!(
        engine.evaluate_cell("Sheet1", 50, 2).unwrap(),
        Some(LiteralValue::Number(246.0))
    );
}

#[test]
fn sequential_mid_span_cancellation_has_no_partial_publication_or_dirty_ack() {
    assert_mid_span_cancellation_is_transactional("__MID_SPAN_CANCEL_SEQUENTIAL__", false);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn parallel_mid_span_cancellation_has_no_partial_publication_or_dirty_ack() {
    assert_mid_span_cancellation_is_transactional("__MID_SPAN_CANCEL_PARALLEL__", true);
}

#[test]
fn mixed_target_delta_contains_real_span_changes_and_compatibility_cells() {
    let mut engine = independent_span_engine();
    engine
        .set_cell_value("Sheet1", 75, 1, LiteralValue::Number(321.0))
        .unwrap();
    let (values, target_delta) = engine
        .evaluate_cells_with_target_delta(&[("Sheet1", 75, 2)])
        .unwrap();
    assert_eq!(values, vec![Some(LiteralValue::Number(642.0))]);
    assert_eq!(target_delta.records.len(), 1);
    assert!(matches!(
        target_delta.records[0],
        EvalDeltaRecord::Run { .. }
    ));

    engine
        .set_cell_value("Sheet1", 76, 1, LiteralValue::Number(400.0))
        .unwrap();
    let (_, compatibility) = engine
        .evaluate_cells_with_delta(&[("Sheet1", 76, 2)])
        .unwrap();
    assert_eq!(compatibility.changed_cells.len(), 1);
}

#[test]
fn full_mixed_delta_uses_same_span_collector_substrate() {
    let mut engine = independent_span_engine();
    engine
        .set_cell_value("Sheet1", 33, 1, LiteralValue::Number(515.0))
        .unwrap();
    let (_, delta) = engine.evaluate_all_with_target_delta().unwrap();
    assert!(delta.records.iter().any(|record| {
        let (start_row, start_col, end_row, end_col) = record.bounds();
        start_row <= 32 && end_row >= 32 && start_col <= 1 && end_col >= 1
    }));
}

#[test]
fn target_delta_collects_spill_anchor_and_children_as_a_run() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3)").unwrap())
        .unwrap();
    let (_, delta) = engine
        .evaluate_cells_with_target_delta(&[("Sheet1", 1, 1)])
        .unwrap();
    assert_eq!(delta.records.len(), 1);
    assert!(matches!(
        delta.records[0],
        EvalDeltaRecord::Run {
            start_row: 0,
            end_row: 2,
            start_col: 0,
            end_col: 0,
            ..
        }
    ));
}

#[test]
fn targeted_two_now_epoch_does_not_recalculate_out_of_demand_volatile() {
    crate::builtins::datetime::register_builtins();
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=NOW()").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=NOW()").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    let unrelated = engine.get_cell_value("Sheet1", 1, 2);
    engine.evaluate_cell("Sheet1", 1, 1).unwrap();
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), unrelated);
    let unrelated_vertex = *engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 0, 1))
        .unwrap();
    assert!(engine.graph.is_dirty(unrelated_vertex));
}

#[test]
fn authoritative_dynamic_reference_replans_under_one_request_ledger() {
    let config = EvalConfig::default()
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
        .with_virtual_dep_telemetry(true);
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut span = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 3, LiteralValue::Number(row as f64))
            .unwrap();
        span.push(formula_record(&mut engine, row, 2, format!("=C{row}*2")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", span)])
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 5, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=E1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=INDIRECT(\"B\"&A1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.reset_evaluation_resource_telemetry();

    engine
        .set_cell_value("Sheet1", 50, 3, LiteralValue::Number(900.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 5, LiteralValue::Number(50.0))
        .unwrap();
    assert_eq!(
        engine.evaluate_cell("Sheet1", 1, 4).unwrap(),
        Some(LiteralValue::Number(1800.0))
    );
    let request = engine.last_evaluation_resource_request_stats().unwrap();
    assert!(request.request_id >= 1);
    assert_eq!(request.target_requested, 1);
    assert!((1..=5).contains(&request.runtime_replan_rounds));
    assert!(request.workbook_exact_attempts <= 1);
    assert_eq!(
        request.topology.cache_outcome,
        crate::engine::FormulaPlaneTopologyCacheOutcome::SkippedDynamicLegacy
    );
    assert!(request.topology.cache_skip_streak >= 1);
    assert_eq!(
        engine.evaluation_resource_baseline_stats().requests_started,
        1,
        "preparation and mixed evaluation must share one outer request ledger"
    );
}

#[test]
fn cancellable_a1_routing_preserves_quoted_bang_and_apostrophe_sheet_names() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    for sheet in ["Bang! Sheet", "O'Brien"] {
        engine
            .set_cell_value(sheet, 1, 1, LiteralValue::Number(4.0))
            .unwrap();
        engine
            .set_cell_formula(sheet, 1, 2, parse("=A1+1").unwrap())
            .unwrap();
    }
    let targets = ["'Bang! Sheet'!B1", "'O''Brien'!B1"];
    engine
        .evaluate_until_cancellable(&targets, Arc::new(AtomicBool::new(false)))
        .unwrap();
    assert_eq!(
        engine.get_cell_value("Bang! Sheet", 1, 2),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        engine.get_cell_value("O'Brien", 1, 2),
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn legacy_cell_routes_preserve_unknown_sheet_interning_and_empty_outputs() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    assert!(engine.sheet_id("Unknown Legacy Target").is_none());
    let values = engine
        .evaluate_cells(&[("Unknown Legacy Target", 1, 1)])
        .unwrap();
    assert_eq!(values, vec![None]);
    assert!(engine.sheet_id("Unknown Legacy Target").is_some());

    let value = engine
        .evaluate_cell("Another Unknown Legacy Target", 1, 1)
        .unwrap();
    assert_eq!(value, None);
    assert!(engine.sheet_id("Another Unknown Legacy Target").is_some());

    engine
        .evaluate_until(&[("Until Unknown Legacy Target", 1, 1)])
        .unwrap();
    assert!(engine.sheet_id("Until Unknown Legacy Target").is_some());
    engine
        .evaluate_until_cancellable(
            &["'Cancellable Unknown Legacy Target'!A1"],
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    assert!(
        engine
            .sheet_id("Cancellable Unknown Legacy Target")
            .is_some()
    );
}

#[test]
fn legacy_and_mixed_max_five_replans_share_typed_terminal_error_and_remain_dirty() {
    let mut legacy = Engine::new(TestWorkbook::default(), EvalConfig::default());
    legacy
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    legacy
        .set_cell_formula("Sheet1", 1, 2, parse("=A1+1").unwrap())
        .unwrap();
    legacy.force_virtual_dep_changes_for_test(6);
    let legacy_error = legacy.evaluate_cell("Sheet1", 1, 2).unwrap_err();
    assert_replan_exhaustion(&legacy_error);
    let legacy_target = *legacy
        .graph
        .get_vertex_id_for_address(&legacy.graph.make_cell_ref("Sheet1", 0, 1))
        .unwrap();
    assert!(legacy.graph.is_dirty(legacy_target));
    legacy.force_virtual_dep_changes_for_test(0);
    assert_eq!(
        legacy.evaluate_cell("Sheet1", 1, 2).unwrap(),
        Some(LiteralValue::Number(2.0))
    );

    let mut mixed = independent_span_engine();
    mixed
        .set_cell_formula("Sheet1", 1, 5, parse("=B50+1").unwrap())
        .unwrap();
    mixed.evaluate_all().unwrap();
    mixed
        .set_cell_value("Sheet1", 50, 1, LiteralValue::Number(123.0))
        .unwrap();
    let pending_before = mixed.graph.pending_formula_dirty_event_count();
    let mixed_target = mixed
        .resolve_target_producers(&[cell("Sheet1", 1, 5)])
        .unwrap()
        .into_iter()
        .find_map(|root| match root {
            TargetProducer::Legacy(vertex) => Some(vertex),
            _ => None,
        })
        .expect("mixed target must retain its legacy producer");
    mixed.graph.set_dirty(mixed_target, true);
    mixed.force_virtual_dep_changes_for_test(7);
    let mixed_error = mixed.evaluate_cell("Sheet1", 1, 5).unwrap_err();
    assert_replan_exhaustion(&mixed_error);
    assert!(mixed.graph.is_dirty(mixed_target));
    assert_eq!(
        mixed.graph.pending_formula_dirty_event_count(),
        pending_before,
        "terminal incompleteness must not acknowledge FormulaPlane dirty work"
    );
    mixed.force_virtual_dep_changes_for_test(0);
    assert_eq!(
        mixed.evaluate_cell("Sheet1", 1, 5).unwrap(),
        Some(LiteralValue::Number(247.0))
    );
    assert_eq!(mixed.graph.pending_formula_dirty_event_count(), 0);
}

#[test]
fn mixed_commit_window_deadline_has_no_partial_publication_and_retry_converges() {
    let mut engine = independent_span_engine();
    let before = engine.get_cell_value("Sheet1", 50, 2);
    engine
        .set_cell_value("Sheet1", 50, 1, LiteralValue::Number(123.0))
        .unwrap();
    let pending = engine.graph.pending_formula_dirty_event_count();
    engine.fail_evaluation_commit_preflight_once_for_test();

    let error = engine.evaluate_cell("Sheet1", 50, 2).unwrap_err();
    let ExcelErrorExtra::Resource { detail } = &error.extra else {
        panic!("expected typed deadline error, got {error:?}");
    };
    assert_eq!(detail.reason, ResourceExhaustionReason::Deadline);
    assert_eq!(engine.get_cell_value("Sheet1", 50, 2), before);
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), pending);

    assert_eq!(
        engine.evaluate_cell("Sheet1", 50, 2).unwrap(),
        Some(LiteralValue::Number(246.0))
    );
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
}

#[test]
fn routed_evaluation_retries_semantic_stale_before_preparation_mutation() {
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig {
            defer_graph_building: true,
            ..Default::default()
        },
    );
    engine.stage_formula_text("Sheet1", 1, 1, "=SUM(40,2)".into());
    engine.inject_target_semantic_stale_once_for_test();

    assert_eq!(
        engine.evaluate_cell("Sheet1", 1, 1).unwrap(),
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(engine.staged_formula_count(), 0);
}

#[test]
fn routed_evaluation_retries_parallel_provider_stale_before_mutation() {
    use std::sync::atomic::Ordering;

    let workbook = TestWorkbook::default();
    let revision = workbook.planning_revision_handle();
    let mut engine = Engine::new(
        workbook,
        EvalConfig {
            defer_graph_building: true,
            ..Default::default()
        },
    );
    engine.stage_formula_text("Sheet1", 1, 1, "=SUM(40,2)".into());
    let bump = revision.clone();
    engine.set_before_target_planning_snapshot_hook_for_test(move || {
        std::thread::spawn(move || {
            bump.fetch_add(1, Ordering::AcqRel);
        })
        .join()
        .unwrap();
    });

    assert_eq!(
        engine.evaluate_cell("Sheet1", 1, 1).unwrap(),
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(engine.staged_formula_count(), 0);
}
