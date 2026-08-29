use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::engine::graph::editor::change_log::ChangeEvent;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{
    CancelToken, ChangeLog, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord,
    FormulaPlaneMode, VertexId,
};
use crate::function::Function;
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{
    ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue, ResourceExhaustionReason,
};
use formualizer_parse::parser::parse;

const TARGET_ROW: u32 = 100;
const TARGET_COL: u32 = 2;
const EDITED_INPUT: f64 = 500.0;
const EXPECTED_TARGET: f64 = EDITED_INPUT * 2.0;

#[derive(Debug)]
struct MidEvaluationCanceller {
    calls: Arc<AtomicUsize>,
}

impl Function for MidEvaluationCanceller {
    fn name(&self) -> &'static str {
        "MID_EVALUATION_CANCELLER"
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ctx.cancellation_token()
            .expect("cancellable evaluation must expose its token")
            .cancel();
        Ok(CalcValue::Scalar(LiteralValue::Int(1)))
    }
}

fn formula_record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn build_never_evaluated_engine_in_mode(
    workbook: TestWorkbook,
    mode: FormulaPlaneMode,
) -> Engine<TestWorkbook> {
    let cfg = EvalConfig::default().with_formula_plane_mode(mode);
    let mut engine = Engine::new(workbook, cfg);
    let mut formulas = Vec::new();
    for row in 1..=200 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(formula_record(
            &mut engine,
            row,
            TARGET_COL,
            &format!("=A{row}*2"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    engine
}

fn build_never_evaluated_engine_with_active_spans_in(
    workbook: TestWorkbook,
) -> Engine<TestWorkbook> {
    build_never_evaluated_engine_in_mode(workbook, FormulaPlaneMode::AuthoritativeExperimental)
}

fn build_never_evaluated_engine_with_active_spans() -> Engine<TestWorkbook> {
    build_never_evaluated_engine_with_active_spans_in(TestWorkbook::default())
}

fn build_engine_with_active_spans_in(workbook: TestWorkbook) -> Engine<TestWorkbook> {
    let mut engine = build_never_evaluated_engine_with_active_spans_in(workbook);
    engine.evaluate_all().unwrap();
    engine
        .set_cell_value("Sheet1", TARGET_ROW, 1, LiteralValue::Number(EDITED_INPUT))
        .unwrap();
    engine
}

pub(super) fn build_engine_with_active_spans() -> Engine<TestWorkbook> {
    build_engine_with_active_spans_in(TestWorkbook::default())
}

fn switch_to_off_with_spans(engine: &mut Engine<TestWorkbook>) {
    assert_active_spans(engine);
    engine.config.formula_plane_mode = FormulaPlaneMode::Off;
}

fn assert_active_spans(engine: &Engine<TestWorkbook>) {
    assert!(engine.graph.formula_authority().active_span_count() > 0);
}

fn assert_target_fresh(engine: &Engine<TestWorkbook>) {
    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        Some(LiteralValue::Number(EXPECTED_TARGET))
    );
}

fn switch_never_evaluated_engine_to_off() -> Engine<TestWorkbook> {
    let mut engine = build_never_evaluated_engine_with_active_spans();
    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        None
    );
    switch_to_off_with_spans(&mut engine);
    engine
}

fn assert_never_evaluated_target_computed(engine: &Engine<TestWorkbook>) {
    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        Some(LiteralValue::Number((TARGET_ROW * 2) as f64))
    );
}

fn define_target_name(engine: &mut Engine<TestWorkbook>) -> VertexId {
    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    engine
        .define_name(
            "TargetValue",
            NamedDefinition::Cell(CellRef::new(
                sheet_id,
                Coord::from_excel(TARGET_ROW, TARGET_COL, true, true),
            )),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .graph
        .resolve_name_entry("TargetValue", sheet_id)
        .expect("workbook name")
        .vertex
}

#[test]
fn off_evaluate_all_demotes_and_computes_never_evaluated_spans_once() {
    let mut engine = switch_never_evaluated_engine_to_off();

    let first = engine.evaluate_all().unwrap();

    assert_eq!(first.computed_vertices, 200);
    assert_never_evaluated_target_computed(&engine);
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);

    let second = engine.evaluate_all().unwrap();
    assert_eq!(second.computed_vertices, 0);
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);
}

#[test]
fn off_transition_deadline_failure_is_precommit_and_retry_clears_retired_dirty_prefix() {
    let mut engine = switch_never_evaluated_engine_to_off();
    let span_refs = engine.graph.formula_authority().active_span_refs();
    let authority_epochs = {
        let authority = engine.graph.formula_authority();
        (
            authority.plane.epoch(),
            authority.indexes_epoch(),
            authority.indexed_plane_epoch(),
        )
    };
    let stats = engine.baseline_stats();
    let pending_dirty = engine
        .graph
        .pending_formula_dirty_regions()
        .collect::<Vec<_>>();
    let pending_event_count = engine.graph.pending_formula_dirty_event_count();
    let evaluation_vertices = engine.graph.get_evaluation_vertices();
    let topology_epoch = engine.topology_epoch_for_test();
    let graph_revision = engine.graph_topology_revision_for_test();
    engine.fail_evaluation_commit_preflight_once_for_test();

    let error = engine.evaluate_all().unwrap_err();

    let ExcelErrorExtra::Resource { detail } = &error.extra else {
        panic!("expected typed deadline failure, got {error:?}");
    };
    assert_eq!(detail.reason, ResourceExhaustionReason::Deadline);
    assert_eq!(
        engine.graph.formula_authority().active_span_refs(),
        span_refs
    );
    let authority = engine.graph.formula_authority();
    assert_eq!(
        (
            authority.plane.epoch(),
            authority.indexes_epoch(),
            authority.indexed_plane_epoch(),
        ),
        authority_epochs
    );
    let after = engine.baseline_stats();
    assert_eq!(after.graph_vertex_count, stats.graph_vertex_count);
    assert_eq!(
        after.graph_formula_vertex_count,
        stats.graph_formula_vertex_count
    );
    assert_eq!(after.graph_edge_count, stats.graph_edge_count);
    assert_eq!(
        engine
            .graph
            .pending_formula_dirty_regions()
            .collect::<Vec<_>>(),
        pending_dirty
    );
    assert_eq!(
        engine.graph.pending_formula_dirty_event_count(),
        pending_event_count
    );
    assert_eq!(engine.graph.get_evaluation_vertices(), evaluation_vertices);
    assert_eq!(engine.topology_epoch_for_test(), topology_epoch);
    assert_eq!(engine.graph_topology_revision_for_test(), graph_revision);
    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        None
    );

    let retry = engine.evaluate_all().unwrap();

    assert_eq!(retry.computed_vertices, 200);
    assert_never_evaluated_target_computed(&engine);
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
    assert!(
        engine
            .graph
            .pending_formula_dirty_regions()
            .next()
            .is_none(),
        "successful demotion must acknowledge the retired FormulaPlane prefix"
    );
}

#[test]
fn off_evaluate_all_with_delta_reports_never_evaluated_span_cells() {
    let mut engine = switch_never_evaluated_engine_to_off();

    let (result, delta) = engine.evaluate_all_with_delta().unwrap();

    assert_eq!(result.computed_vertices, 200);
    assert_eq!(delta.changed_cells.len(), 200);
    assert!(delta.changed_cells.iter().any(|cell| {
        let (_, row, col) = cell.to_excel_1based();
        (row, col) == (TARGET_ROW, TARGET_COL)
    }));
    assert_never_evaluated_target_computed(&engine);
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);
}

#[test]
fn off_evaluate_all_cancellable_preserves_precancel_and_completes_normally() {
    let mut cancelled = switch_never_evaluated_engine_to_off();
    let token = CancelToken::new();
    token.cancel();

    let error = cancelled.evaluate_all_cancellable(token).unwrap_err();

    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert_active_spans(&cancelled);
    assert_eq!(
        cancelled.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        None
    );

    let mut completed = switch_never_evaluated_engine_to_off();
    let result = completed
        .evaluate_all_cancellable(CancelToken::new())
        .unwrap();
    assert_eq!(result.computed_vertices, 200);
    assert_never_evaluated_target_computed(&completed);
    assert_eq!(completed.graph.formula_authority().active_span_count(), 0);
}

#[test]
fn off_evaluate_all_logged_computes_never_evaluated_spans_and_keeps_log_shape() {
    let mut engine = switch_never_evaluated_engine_to_off();
    let mut log = ChangeLog::new();

    let result = engine.evaluate_all_logged(&mut log).unwrap();

    assert_eq!(result.computed_vertices, 200);
    assert_never_evaluated_target_computed(&engine);
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);
    assert_eq!(log.events().len(), 2);
    assert!(matches!(log.events()[0], ChangeEvent::CompoundStart { .. }));
    assert!(matches!(log.events()[1], ChangeEvent::CompoundEnd { .. }));
}

#[test]
fn off_edit_after_initial_demotion_recalculates_legacy_formula() {
    let mut engine = switch_never_evaluated_engine_to_off();
    engine.evaluate_all().unwrap();

    engine
        .set_cell_value("Sheet1", TARGET_ROW, 1, LiteralValue::Number(7.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        Some(LiteralValue::Number(14.0))
    );
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);
}

#[test]
fn authoritative_off_authoritative_toggle_keeps_demoted_formulas_correct() {
    let mut engine = switch_never_evaluated_engine_to_off();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);

    engine.config.formula_plane_mode = FormulaPlaneMode::AuthoritativeExperimental;
    engine
        .set_cell_value("Sheet1", TARGET_ROW, 1, LiteralValue::Number(9.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        Some(LiteralValue::Number(18.0))
    );
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);
}

#[test]
fn off_demotion_prepare_and_final_validation_failures_preserve_edit_name_and_retry() {
    use crate::engine::eval::FormulaSpanDemotionFault;

    for fault in [
        FormulaSpanDemotionFault::AstPreparation,
        FormulaSpanDemotionFault::FinalAuthorityValidation,
    ] {
        let mut engine = build_never_evaluated_engine_with_active_spans();
        engine
            .set_cell_value("Sheet1", TARGET_ROW, 1, LiteralValue::Number(7.0))
            .unwrap();
        let name_vertex = define_target_name(&mut engine);
        switch_to_off_with_spans(&mut engine);

        let refs = engine.graph.formula_authority().active_span_refs();
        let authority_epochs = {
            let authority = engine.graph.formula_authority();
            (
                authority.plane.epoch(),
                authority.indexes_epoch(),
                authority.indexed_plane_epoch(),
            )
        };
        let stats = engine.baseline_stats();
        let pending_dirty = engine
            .graph
            .pending_formula_dirty_regions()
            .collect::<Vec<_>>();
        let pending_event_count = engine.graph.pending_formula_dirty_event_count();
        let evaluation_vertices = engine.graph.get_evaluation_vertices();
        let topology_epoch = engine.topology_epoch_for_test();
        let graph_revision = engine.graph_topology_revision_for_test();
        let name_definition = engine
            .graph
            .resolve_name_entry("TargetValue", engine.sheet_id("Sheet1").unwrap())
            .unwrap()
            .definition
            .clone();
        engine.set_formula_span_demotion_fault_for_test(fault);

        let error = engine.evaluate_all().unwrap_err();

        assert_eq!(error.kind, ExcelErrorKind::NImpl, "fault {fault:?}");
        assert_eq!(engine.graph.formula_authority().active_span_refs(), refs);
        let authority = engine.graph.formula_authority();
        assert_eq!(
            (
                authority.plane.epoch(),
                authority.indexes_epoch(),
                authority.indexed_plane_epoch(),
            ),
            authority_epochs
        );
        let after = engine.baseline_stats();
        assert_eq!(after.graph_vertex_count, stats.graph_vertex_count);
        assert_eq!(
            after.graph_formula_vertex_count,
            stats.graph_formula_vertex_count
        );
        assert_eq!(after.graph_edge_count, stats.graph_edge_count);
        assert_eq!(
            engine
                .graph
                .pending_formula_dirty_regions()
                .collect::<Vec<_>>(),
            pending_dirty
        );
        assert_eq!(
            engine.graph.pending_formula_dirty_event_count(),
            pending_event_count
        );
        assert_eq!(engine.graph.get_evaluation_vertices(), evaluation_vertices);
        assert_eq!(engine.topology_epoch_for_test(), topology_epoch);
        assert_eq!(engine.graph_topology_revision_for_test(), graph_revision);
        assert_eq!(
            engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
            None
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", TARGET_ROW, 1),
            Some(LiteralValue::Number(7.0))
        );
        assert_eq!(
            engine
                .graph
                .resolve_name_entry("TargetValue", engine.sheet_id("Sheet1").unwrap())
                .unwrap()
                .definition,
            name_definition
        );

        let retry = engine.evaluate_all().unwrap();

        assert_eq!(retry.computed_vertices, 201, "fault {fault:?}");
        assert_eq!(
            engine.evaluate_vertex(name_vertex).unwrap(),
            LiteralValue::Number(14.0)
        );
        assert_eq!(engine.graph.formula_authority().active_span_count(), 0);
        assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
    }
}

#[test]
fn off_targeted_evaluation_demotes_before_resolving_never_evaluated_span() {
    let mut engine = switch_never_evaluated_engine_to_off();

    let value = engine
        .evaluate_cell("Sheet1", TARGET_ROW, TARGET_COL)
        .unwrap();

    assert_eq!(value, Some(LiteralValue::Number((TARGET_ROW * 2) as f64)));
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);
}

#[test]
fn off_recalc_plan_falls_back_to_demoted_legacy_graph() {
    let mut engine = switch_never_evaluated_engine_to_off();
    let plan = engine.build_recalc_plan().unwrap();

    let result = engine.evaluate_recalc_plan(&plan).unwrap();

    assert_eq!(result.computed_vertices, 200);
    assert_never_evaluated_target_computed(&engine);
    assert_eq!(engine.graph.formula_authority().active_span_count(), 0);
}

#[test]
fn evaluate_all_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    engine.evaluate_all().unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn authoritative_evaluate_all_with_delta_keeps_coordinator_delta_behavior() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    let (_, delta) = engine.evaluate_all_with_delta().unwrap();

    assert!(delta.changed_cells.iter().any(|cell| {
        let (_, row, col) = cell.to_excel_1based();
        (row, col) == (TARGET_ROW, TARGET_COL)
    }));
    assert_target_fresh(&engine);
}

#[test]
fn off_evaluate_all_with_delta_uses_legacy_collector_with_retained_spans() {
    let mut engine = build_engine_with_active_spans();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=A100+1").unwrap())
        .unwrap();
    switch_to_off_with_spans(&mut engine);

    let (_, delta) = engine.evaluate_all_with_delta().unwrap();

    assert!(!delta.changed_cells.is_empty());
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(EDITED_INPUT + 1.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        Some(LiteralValue::Number(EXPECTED_TARGET))
    );
}

fn cancellation_engine() -> (Engine<TestWorkbook>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::default().with_function(Arc::new(MidEvaluationCanceller {
        calls: Arc::clone(&calls),
    }));
    let mut engine = build_engine_with_active_spans_in(workbook);
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            3,
            parse("=MID_EVALUATION_CANCELLER()").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=C1+1").unwrap())
        .unwrap();
    (engine, calls)
}

#[test]
fn authoritative_evaluate_all_cancellable_keeps_late_cancellation_behavior() {
    let (mut engine, calls) = cancellation_engine();
    let token = CancelToken::new();
    assert_active_spans(&engine);

    let error = engine.evaluate_all_cancellable(token.clone()).unwrap_err();

    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert_eq!(
        error.message.as_deref(),
        Some("Evaluation cancelled during legacy island")
    );
    assert!(token.is_cancelled());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        Some(LiteralValue::Number(TARGET_ROW as f64 * 2.0))
    );
}

#[test]
fn off_evaluate_all_cancellable_observes_mid_evaluation_cancel_with_retained_spans() {
    let (mut engine, calls) = cancellation_engine();
    switch_to_off_with_spans(&mut engine);
    let token = CancelToken::new();

    let error = engine.evaluate_all_cancellable(token).unwrap_err();

    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert_eq!(
        error.message.as_deref(),
        Some("Parallel evaluation cancelled during execution")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn authoritative_evaluate_all_logged_keeps_coordinator_logging_behavior() {
    let mut engine = build_engine_with_active_spans();
    let mut log = ChangeLog::new();
    assert_active_spans(&engine);
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=SEQUENCE(2,1)").unwrap())
        .unwrap();

    engine.evaluate_all_logged(&mut log).unwrap();

    assert!(log.events().is_empty());
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(2.0))
    );
    assert_target_fresh(&engine);
}

#[test]
fn off_evaluate_all_logged_writes_legacy_changelog_with_retained_spans() {
    let mut engine = build_engine_with_active_spans();
    let mut log = ChangeLog::new();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=SEQUENCE(2,1)").unwrap())
        .unwrap();
    switch_to_off_with_spans(&mut engine);

    engine.evaluate_all_logged(&mut log).unwrap();

    assert!(
        log.events()
            .iter()
            .any(|event| matches!(event, ChangeEvent::SpillCommitted { .. }))
    );
    assert!(
        log.events()
            .iter()
            .any(|event| matches!(event, ChangeEvent::CompoundStart { .. }))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        Some(LiteralValue::Number(EXPECTED_TARGET))
    );
}

#[test]
fn evaluate_cell_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    let value = engine
        .evaluate_cell("Sheet1", TARGET_ROW, TARGET_COL)
        .unwrap();

    assert_eq!(value, Some(LiteralValue::Number(EXPECTED_TARGET)));
    assert_target_fresh(&engine);
}

#[test]
fn evaluate_cells_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    let values = engine
        .evaluate_cells(&[("Sheet1", TARGET_ROW, TARGET_COL)])
        .unwrap();

    assert_eq!(values, vec![Some(LiteralValue::Number(EXPECTED_TARGET))]);
    assert_target_fresh(&engine);
}

#[test]
fn evaluate_cells_cancellable_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    let values = engine
        .evaluate_cells_cancellable(
            &[("Sheet1", TARGET_ROW, TARGET_COL)],
            crate::engine::CancelToken::new(),
        )
        .unwrap();

    assert_eq!(values, vec![Some(LiteralValue::Number(EXPECTED_TARGET))]);
    assert_target_fresh(&engine);
}

#[test]
fn evaluate_cells_with_delta_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    let (values, _) = engine
        .evaluate_cells_with_delta(&[("Sheet1", TARGET_ROW, TARGET_COL)])
        .unwrap();

    assert_eq!(values, vec![Some(LiteralValue::Number(EXPECTED_TARGET))]);
    assert_target_fresh(&engine);
}

#[test]
fn evaluate_until_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    engine
        .evaluate_until(&[("Sheet1", TARGET_ROW, TARGET_COL)])
        .unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn evaluate_until_cancellable_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    engine
        .evaluate_until_cancellable(&["Sheet1!B100"], crate::engine::CancelToken::new())
        .unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn authoritative_evaluate_recalc_plan_keeps_coordinator_behavior() {
    let mut engine = build_engine_with_active_spans();
    let plan = engine.build_recalc_plan().unwrap();
    assert_active_spans(&engine);

    engine.evaluate_recalc_plan(&plan).unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn off_evaluate_recalc_plan_honors_legacy_plan_with_retained_spans() {
    let mut engine = build_engine_with_active_spans();
    switch_to_off_with_spans(&mut engine);
    let plan = engine.build_recalc_plan().unwrap();

    let result = engine.evaluate_recalc_plan(&plan).unwrap();

    assert_eq!(result.computed_vertices, 200);
    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        Some(LiteralValue::Number(EXPECTED_TARGET))
    );
}

#[test]
fn off_direct_name_vertex_matches_authoritative_and_legacy_controls() {
    let mut authoritative = build_never_evaluated_engine_with_active_spans();
    let authoritative_name = define_target_name(&mut authoritative);
    assert_active_spans(&authoritative);
    assert_eq!(
        authoritative.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        None
    );

    let mut legacy =
        build_never_evaluated_engine_in_mode(TestWorkbook::default(), FormulaPlaneMode::Off);
    let legacy_name = define_target_name(&mut legacy);
    assert_eq!(legacy.graph.formula_authority().active_span_count(), 0);

    let mut subject = build_never_evaluated_engine_with_active_spans();
    let subject_name = define_target_name(&mut subject);
    switch_to_off_with_spans(&mut subject);

    let authoritative_value = authoritative.evaluate_vertex(authoritative_name).unwrap();
    let legacy_value = legacy.evaluate_vertex(legacy_name).unwrap();
    let subject_value = subject.evaluate_vertex(subject_name).unwrap();

    assert_eq!(authoritative_value, LiteralValue::Number(200.0));
    assert_eq!(legacy_value, authoritative_value);
    assert_eq!(subject_value, authoritative_value);
    assert_eq!(subject.graph.formula_authority().active_span_count(), 0);

    let mut edited = build_never_evaluated_engine_with_active_spans();
    edited
        .set_cell_value("Sheet1", TARGET_ROW, 1, LiteralValue::Number(7.0))
        .unwrap();
    let edited_name = define_target_name(&mut edited);
    switch_to_off_with_spans(&mut edited);

    assert_eq!(
        edited.evaluate_vertex(edited_name).unwrap(),
        LiteralValue::Number(14.0)
    );
    assert_eq!(edited.graph.formula_authority().active_span_count(), 0);
}

#[test]
fn evaluate_vertex_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    let input_vertex = *engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", TARGET_ROW, 1))
        .expect("input vertex");
    assert_active_spans(&engine);

    let value = engine.evaluate_vertex(input_vertex).unwrap();

    assert_eq!(value, LiteralValue::Number(EDITED_INPUT));
    assert_target_fresh(&engine);
}
