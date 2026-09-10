use super::*;
use crate::test_workbook::TestWorkbook;

fn engine() -> Engine<TestWorkbook> {
    static BUILTINS: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    BUILTINS.get_or_init(crate::builtins::load_builtins);
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("S").unwrap();
    engine
        .set_cell_formula("S", 1, 1, formualizer_parse::parse("=SEQUENCE(2)").unwrap())
        .unwrap();
    engine
}

fn anchor(engine: &Engine<TestWorkbook>) -> VertexId {
    *engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("S", 1, 1))
        .unwrap()
}

fn spill(engine: &mut Engine<TestWorkbook>) {
    let value = engine.evaluate_cell("S", 1, 1);
    assert!(
        matches!(value, Ok(Some(LiteralValue::Error(ref e))) | Err(ref e) if e.kind == ExcelErrorKind::Spill)
    );
}

#[test]
fn pending_spill_final_commit_rechecks_before_publication_and_releases_reservation() {
    let mut engine = engine();
    let vertex = anchor(&engine);
    let effects = engine
        .plan_vertex_effects(
            vertex,
            LiteralValue::Array(vec![
                vec![LiteralValue::Number(1.0)],
                vec![LiteralValue::Number(2.0)],
            ]),
            None,
        )
        .unwrap();
    assert_eq!(engine.spill_mgr.active_locks.len(), 1);
    engine.stage_formula_text("S", 2, 1, "NOSHEET!A1".into());
    let before = engine.get_cell_value("S", 1, 1);
    let mut delta = DeltaCollector::new(DeltaMode::Cells);
    assert_eq!(
        engine
            .apply_effect(&effects[0], Some(&mut delta), None)
            .unwrap_err()
            .kind,
        ExcelErrorKind::Spill
    );
    assert!(delta.finish_target().is_empty());
    assert_eq!(engine.get_cell_value("S", 1, 1), before);
    assert_eq!(engine.get_cell_value("S", 2, 1), None);
    assert!(engine.spill_mgr.active_locks.is_empty());
    assert_eq!(engine.graph.spill_registry_counts(), (0, 0));
    assert_eq!(engine.blocked_pending_spills.len(), 1);
    assert_eq!(
        engine.get_staged_formula_text("S", 2, 1).as_deref(),
        Some("NOSHEET!A1")
    );
}

#[test]
fn pending_spill_retry_is_bounded_survives_materialization_and_retires() {
    let mut engine = engine();
    engine.stage_formula_text("S", 2, 1, "99".into());
    let vertex = anchor(&engine);
    for _ in 0..8 {
        engine.graph.mark_vertex_dirty(vertex);
        spill(&mut engine);
        assert_eq!(engine.blocked_pending_spills.len(), 1);
        assert_eq!(engine.blocked_pending_spills.capacity(), 1);
    }
    engine.build_graph_all().unwrap();
    engine.graph.mark_vertex_dirty(vertex);
    spill(&mut engine);
    assert_eq!(engine.blocked_pending_spills.len(), 1);
    engine
        .set_cell_value("S", 2, 1, LiteralValue::Empty)
        .unwrap();
    assert!(engine.graph.is_dirty(vertex));
    let result = engine.evaluate_cell("S", 1, 1);
    assert_eq!(result.unwrap(), Some(LiteralValue::Number(1.0)));
    assert_eq!(
        engine.get_cell_value("S", 2, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert!(engine.blocked_pending_spills.is_empty());

    engine.stage_formula_text("S", 2, 1, "99".into());
    engine.graph.mark_vertex_dirty(vertex);
    spill(&mut engine);
    engine
        .set_cell_value("S", 1, 1, LiteralValue::Number(42.0))
        .unwrap();
    engine.evaluate_cell("S", 1, 1).unwrap();
    assert!(engine.blocked_pending_spills.is_empty());
}

#[test]
fn pending_spill_retry_replaces_attempted_shape_after_materialization() {
    let mut engine = engine();
    engine.stage_formula_text("S", 2, 1, "99".into());
    spill(&mut engine);
    engine.build_graph_all().unwrap();
    engine
        .set_cell_formula("S", 1, 1, formualizer_parse::parse("=SEQUENCE(3)").unwrap())
        .unwrap();
    engine
        .set_cell_value("S", 3, 1, LiteralValue::Number(7.0))
        .unwrap();
    spill(&mut engine);
    assert_eq!(engine.blocked_pending_spills.len(), 1);
    assert_eq!(
        engine.blocked_pending_spills[0].2.rows.query_bounds(),
        (0, 2)
    );
    engine
        .set_cell_value("S", 2, 1, LiteralValue::Empty)
        .unwrap();
    spill(&mut engine);
    engine
        .set_cell_value("S", 3, 1, LiteralValue::Empty)
        .unwrap();
    assert_eq!(
        engine.evaluate_cell("S", 1, 1).unwrap(),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.get_cell_value("S", 3, 1),
        Some(LiteralValue::Number(3.0))
    );
    assert!(engine.blocked_pending_spills.is_empty());
}

#[test]
fn pending_spill_retry_ignores_unrelated_edits_and_prunes_removed_sheets() {
    let mut engine = engine();
    engine.stage_formula_text("S", 2, 1, "99".into());
    spill(&mut engine);
    let vertex = anchor(&engine);
    assert!(!engine.graph.is_dirty(vertex));
    engine
        .set_cell_value("S", 20, 20, LiteralValue::Number(7.0))
        .unwrap();
    assert!(!engine.graph.is_dirty(vertex));
    let before = engine.blocked_pending_spills.clone();
    assert!(
        engine
            .set_cell_formula("S", 2, 1, formualizer_parse::parse("=NOSHEET!A1").unwrap())
            .is_err()
    );
    assert_eq!(engine.blocked_pending_spills, before);
    let sheet = engine.sheet_id("S").unwrap();
    engine.remove_sheet(sheet).unwrap();
    engine.add_sheet("Other").unwrap();
    engine.evaluate_cell("Other", 1, 1).unwrap();
    assert!(engine.blocked_pending_spills.is_empty());
}

#[test]
fn pending_spill_cancel_and_retained_admission_leave_no_failure_publication() {
    let mut engine = engine();
    engine.stage_formula_text("S", 2, 1, "NOSHEET!A1".into());
    let cancel = crate::engine::CancelToken::new();
    cancel.cancel();
    assert_eq!(
        engine
            .evaluate_cells_cancellable(&[("S", 1, 1)], cancel)
            .unwrap_err()
            .kind,
        ExcelErrorKind::Cancelled
    );
    assert!(engine.blocked_pending_spills.is_empty());
    let before = engine.get_cell_value("S", 1, 1);
    let mut budgets = crate::engine::EvaluationBudgets::default();
    budgets.retained.total_bytes = Some(0);
    engine.set_evaluation_resource_budgets(budgets);
    let error = engine.evaluate_cell("S", 1, 1).unwrap_err();
    assert!(
        matches!(error.extra, formualizer_common::ExcelErrorExtra::Resource { ref detail } if detail.reason == formualizer_common::ResourceExhaustionReason::RetainedMemory)
    );
    assert!(engine.blocked_pending_spills.is_empty());
    assert!(engine.spill_mgr.active_locks.is_empty());
    assert_eq!(engine.get_cell_value("S", 1, 1), before);
    assert_eq!(
        engine.get_staged_formula_text("S", 2, 1).as_deref(),
        Some("NOSHEET!A1")
    );
    engine.set_evaluation_resource_budgets(Default::default());
    spill(&mut engine);
    engine.clear_staged_formula_text("S", 2, 1);
    assert_eq!(
        engine.evaluate_cell("S", 1, 1).unwrap(),
        Some(LiteralValue::Number(1.0))
    );
}
