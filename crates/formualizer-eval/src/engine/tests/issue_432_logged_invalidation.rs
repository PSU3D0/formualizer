use crate::engine::addr::GridAddr;
use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{
    ActionJournal, ArrowUndoBatch, ChangeEvent, ChangeLog, EditorError, Engine, EvalConfig,
    FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode, GraphUndoBatch, RowVisibilitySource,
};
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorExtra, LiteralValue, PlanStaleReason};
use formualizer_parse::parser::parse;
use std::sync::Arc;

type TestEngine = Engine<TestWorkbook>;

#[derive(Clone, Copy)]
enum ReplayOperation {
    AtomicCommit,
    UndoAction,
    RedoAction,
}

fn config() -> EvalConfig {
    EvalConfig {
        formula_plane_mode: FormulaPlaneMode::Off,
        enable_parallel: false,
        enable_virtual_dep_telemetry: true,
        ..EvalConfig::default()
    }
}

fn setup(reversed: bool) -> TestEngine {
    let mut engine = Engine::new(TestWorkbook::new(), config());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    let formulas = if reversed {
        ["=C1+1", "=A1+10"]
    } else {
        ["=A1+1", "=B1+1"]
    };
    for (col, formula) in (2..=3).zip(formulas) {
        engine
            .set_cell_formula("Sheet1", 1, col, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    engine
}

fn setup_empty_precedent() -> TestEngine {
    let mut engine = Engine::new(TestWorkbook::new(), config());
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1+1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=B1+1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

fn assert_graph_plan_stale(engine: &mut TestEngine, plan: &crate::engine::RecalcPlan) {
    let error = engine.evaluate_recalc_plan(plan).unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::PlanStale {
            reason: PlanStaleReason::Graph
        }
    ));
}

fn reverse_with_action_logger(engine: &mut TestEngine, log: &mut ChangeLog) {
    engine
        .action_with_logger(log, "reverse dependencies", |action| {
            action.set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())?;
            action.set_cell_formula("Sheet1", 1, 3, parse("=A1+10").unwrap())
        })
        .unwrap();
}

fn prepare_action_replay(operation: ReplayOperation) -> TestEngine {
    let mut engine = setup(false);
    let (_, journal) = engine
        .action_atomic_journal("reverse dependencies", |action| {
            action.set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())?;
            action.set_cell_formula("Sheet1", 1, 3, parse("=A1+10").unwrap())
        })
        .unwrap();
    let mut undo = UndoEngine::new();
    undo.push_action(journal);
    if matches!(operation, ReplayOperation::AtomicCommit) {
        return engine;
    }

    engine.mark_topology_edited();
    engine.evaluate_all().unwrap();
    engine.undo_action(&mut undo).unwrap();
    if matches!(operation, ReplayOperation::UndoAction) {
        return engine;
    }

    engine.mark_topology_edited();
    engine.evaluate_all().unwrap();
    engine.redo_action(&mut undo).unwrap();
    engine
}

fn assert_b1_c1(engine: &mut TestEngine, expected: (f64, f64)) {
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(expected.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(expected.1))
    );
}

fn cell(engine: &TestEngine, row: u32, col: u32) -> CellRef {
    CellRef::new(
        engine.sheet_id("Sheet1").unwrap(),
        Coord::from_excel(row, col, true, true),
    )
}

fn reverse_with_direct_logger(engine: &mut TestEngine, log: &mut ChangeLog) {
    let b1 = cell(engine, 1, 2);
    let c1 = cell(engine, 1, 3);
    engine
        .edit_with_logger(log, |editor| {
            editor.set_cell_formula(b1, parse("=C1+1").unwrap());
            editor.set_cell_formula(c1, parse("=A1+10").unwrap());
        })
        .unwrap();
}

#[test]
fn action_with_logger_formula_edits_invalidate_warm_schedule() {
    let mut engine = setup(false);
    reverse_with_action_logger(&mut engine, &mut ChangeLog::new());
    assert_b1_c1(&mut engine, (12.0, 11.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 0);
}

#[test]
fn atomic_commit_formula_edits_invalidate_warm_schedule() {
    let mut engine = prepare_action_replay(ReplayOperation::AtomicCommit);
    assert_b1_c1(&mut engine, (12.0, 11.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 0);
}

#[test]
fn undo_action_formula_edits_invalidate_independently_warmed_schedule() {
    let mut engine = prepare_action_replay(ReplayOperation::UndoAction);
    assert_b1_c1(&mut engine, (2.0, 3.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 0);
}

#[test]
fn redo_action_formula_edits_invalidate_independently_warmed_schedule() {
    let mut engine = prepare_action_replay(ReplayOperation::RedoAction);
    assert_b1_c1(&mut engine, (12.0, 11.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 0);
}

#[test]
fn direct_edit_with_logger_formula_edits_invalidate_warm_schedule() {
    let mut engine = setup(false);
    reverse_with_direct_logger(&mut engine, &mut ChangeLog::new());
    assert_b1_c1(&mut engine, (12.0, 11.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 0);
}

#[test]
fn changelog_undo_formula_edits_invalidate_independently_warmed_schedule() {
    let mut engine = setup(false);
    let mut log = ChangeLog::new();
    log.begin_compound("reverse dependencies".to_string());
    reverse_with_direct_logger(&mut engine, &mut log);
    log.end_compound();
    engine.mark_topology_edited();
    engine.evaluate_all().unwrap();

    engine
        .undo_logged(&mut UndoEngine::new(), &mut log)
        .unwrap();
    assert_b1_c1(&mut engine, (2.0, 3.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 0);
}

#[test]
fn changelog_redo_formula_edits_invalidate_independently_warmed_schedule() {
    let mut engine = setup(false);
    let mut log = ChangeLog::new();
    log.begin_compound("reverse dependencies".to_string());
    reverse_with_direct_logger(&mut engine, &mut log);
    log.end_compound();
    engine.mark_topology_edited();
    engine.evaluate_all().unwrap();
    let mut undo = UndoEngine::new();
    engine.undo_logged(&mut undo, &mut log).unwrap();
    engine.mark_topology_edited();
    engine.evaluate_all().unwrap();

    engine.redo_logged(&mut undo, &mut log).unwrap();
    assert_b1_c1(&mut engine, (12.0, 11.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 0);
}

#[test]
fn logged_formula_value_transitions_are_topology_changes() {
    let mut engine = setup(false);
    let b1 = cell(&engine, 1, 2);
    let mut log = ChangeLog::new();
    let before_formula_to_value = engine.topology_epoch_for_test();
    engine
        .edit_with_logger(&mut log, |editor| {
            editor.set_cell_value(b1, LiteralValue::Int(7));
        })
        .unwrap();
    assert!(engine.topology_epoch_for_test() > before_formula_to_value);

    let before_value_to_formula = engine.topology_epoch_for_test();
    engine
        .edit_with_logger(&mut log, |editor| {
            editor.set_cell_formula(b1, parse("=A1+20").unwrap());
        })
        .unwrap();
    assert!(engine.topology_epoch_for_test() > before_value_to_formula);
    assert_b1_c1(&mut engine, (21.0, 22.0));
}

#[test]
fn failed_action_rollback_invalidates_retained_plan_conservatively() {
    let mut engine = setup(false);
    let plan = engine.build_recalc_plan().unwrap();
    let mut log = ChangeLog::new();
    let result = engine.action_with_logger(
        &mut log,
        "failed reversal",
        |action| -> Result<(), EditorError> {
            action.set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())?;
            action.set_cell_formula("Sheet1", 1, 3, parse("=A1+10").unwrap())?;
            Err(EditorError::TransactionFailed {
                reason: "intentional".to_string(),
            })
        },
    );
    assert!(result.is_err());
    assert!(log.is_empty());
    let error = engine.evaluate_recalc_plan(&plan).unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::PlanStale {
            reason: PlanStaleReason::Graph
        }
    ));
    assert_b1_c1(&mut engine, (2.0, 3.0));
}

#[test]
fn undo_logged_empty_precedent_removal_invalidates_retained_plan() {
    let mut engine = setup_empty_precedent();
    let mut log = ChangeLog::new();
    engine
        .action_with_logger(&mut log, "fill empty precedent", |action| {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        })
        .unwrap();
    engine.evaluate_all().unwrap();
    let plan = engine.build_recalc_plan().unwrap();
    let topology_before = engine.topology_epoch_for_test();

    engine
        .undo_logged(&mut UndoEngine::new(), &mut log)
        .unwrap();
    assert_eq!(
        engine.topology_epoch_for_test(),
        topology_before.wrapping_add(1)
    );
    assert_graph_plan_stale(&mut engine, &plan);
}

#[test]
fn undo_action_empty_precedent_removal_invalidates_retained_plan() {
    let mut engine = setup_empty_precedent();
    let (_, journal) = engine
        .action_atomic_journal("fill empty precedent", |action| {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        })
        .unwrap();
    let mut undo = UndoEngine::new();
    undo.push_action(journal);
    engine.evaluate_all().unwrap();
    let plan = engine.build_recalc_plan().unwrap();
    let topology_before = engine.topology_epoch_for_test();

    engine.undo_action(&mut undo).unwrap();
    assert_eq!(
        engine.topology_epoch_for_test(),
        topology_before.wrapping_add(1)
    );
    assert_graph_plan_stale(&mut engine, &plan);
}

#[test]
fn redo_action_after_empty_precedent_removal_invalidates_retained_plan() {
    let mut engine = setup_empty_precedent();
    let (_, journal) = engine
        .action_atomic_journal("fill empty precedent", |action| {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        })
        .unwrap();
    let mut undo = UndoEngine::new();
    undo.push_action(journal);
    engine.undo_action(&mut undo).unwrap();
    let plan = engine.build_recalc_plan().unwrap();
    let topology_before = engine.topology_epoch_for_test();

    engine.redo_action(&mut undo).unwrap();
    assert_eq!(
        engine.topology_epoch_for_test(),
        topology_before.wrapping_add(1)
    );
    assert_graph_plan_stale(&mut engine, &plan);
}

#[test]
fn failed_empty_precedent_write_rollback_invalidates_retained_plan() {
    let mut engine = setup_empty_precedent();
    let plan = engine.build_recalc_plan().unwrap();
    let topology_before = engine.topology_epoch_for_test();
    let result = engine.action_with_logger(
        &mut ChangeLog::new(),
        "failed empty precedent write",
        |action| -> Result<(), EditorError> {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))?;
            Err(EditorError::TransactionFailed {
                reason: "intentional".to_string(),
            })
        },
    );

    assert!(result.is_err());
    assert_eq!(
        engine.topology_epoch_for_test(),
        topology_before.wrapping_add(1)
    );
    assert_graph_plan_stale(&mut engine, &plan);
}

#[test]
fn nonempty_value_undo_redo_reuses_warm_schedule() {
    let mut engine = setup(false);
    let (_, journal) = engine
        .action_atomic_journal("value only", |action| {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(5))
        })
        .unwrap();
    let mut undo = UndoEngine::new();
    undo.push_action(journal);
    let topology_before = engine.topology_epoch_for_test();

    assert_b1_c1(&mut engine, (6.0, 7.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
    engine.undo_action(&mut undo).unwrap();
    assert_eq!(engine.topology_epoch_for_test(), topology_before);
    assert_b1_c1(&mut engine, (2.0, 3.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
    engine.redo_action(&mut undo).unwrap();
    assert_eq!(engine.topology_epoch_for_test(), topology_before);
    assert_b1_c1(&mut engine, (6.0, 7.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
}

#[test]
fn retained_plan_rejects_direct_logged_topology_edit() {
    let mut engine = setup(false);
    let plan = engine.build_recalc_plan().unwrap();
    reverse_with_direct_logger(&mut engine, &mut ChangeLog::new());
    let error = engine.evaluate_recalc_plan(&plan).unwrap_err();
    assert!(matches!(
        error.extra,
        ExcelErrorExtra::PlanStale {
            reason: PlanStaleReason::Graph
        }
    ));
}

#[test]
fn logged_existing_value_edit_keeps_warm_schedule() {
    let mut engine = setup(false);
    let a1 = cell(&engine, 1, 1);
    let topology_before = engine.topology_epoch_for_test();
    engine
        .edit_with_logger(&mut ChangeLog::new(), |editor| {
            editor.set_cell_value(a1, LiteralValue::Int(5));
        })
        .unwrap();
    assert_eq!(engine.topology_epoch_for_test(), topology_before);
    assert_b1_c1(&mut engine, (6.0, 7.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
}

#[test]
fn action_logged_existing_value_edit_keeps_warm_schedule() {
    let mut engine = setup(false);
    let topology_before = engine.topology_epoch_for_test();
    engine
        .action_with_logger(&mut ChangeLog::new(), "value only", |action| {
            action.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(8))
        })
        .unwrap();
    assert_eq!(engine.topology_epoch_for_test(), topology_before);
    assert_b1_c1(&mut engine, (9.0, 10.0));
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
}

#[test]
fn logged_visibility_edit_keeps_warm_topology() {
    let mut engine = Engine::new(TestWorkbook::new(), config());
    for row in 2..=5 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Int((row - 1) as i64))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUBTOTAL(109,A2:A5)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    let topology_before = engine.topology_epoch_for_test();

    engine
        .action_with_logger(&mut ChangeLog::new(), "hide row", |action| {
            action.set_row_hidden("Sheet1", 2, true, RowVisibilitySource::Manual)
        })
        .unwrap();
    assert_eq!(engine.topology_epoch_for_test(), topology_before);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(9.0))
    );
    assert_eq!(engine.last_virtual_dep_telemetry().schedule_cache_hits, 1);
}

#[test]
fn compound_markers_without_edits_do_not_invalidate() {
    let mut engine = setup(false);
    let topology_before = engine.topology_epoch_for_test();
    engine
        .action_with_logger(&mut ChangeLog::new(), "no edits", |_action| Ok(()))
        .unwrap();
    assert_eq!(engine.topology_epoch_for_test(), topology_before);
}

#[test]
fn retained_plan_is_stale_before_partial_inverse_error_returns() {
    let mut engine = setup(false);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
        .unwrap();
    engine.evaluate_all().unwrap();
    let plan = engine.build_recalc_plan().unwrap();
    let a1 = cell(&engine, 1, 1);
    let b1 = cell(&engine, 1, 2);
    let a1_vertex = engine.graph.get_vertex_for_cell(&a1).unwrap();
    let b1_vertex = engine.graph.get_vertex_for_cell(&b1).unwrap();
    let journal = ActionJournal {
        name: "partial inverse failure".to_string(),
        graph: GraphUndoBatch {
            // Undo runs in reverse: move B1 to B2, then fail on EdgeAdded's
            // intentionally unsupported inverse.
            events: vec![
                ChangeEvent::EdgeAdded {
                    from: b1_vertex,
                    to: a1_vertex,
                },
                ChangeEvent::VertexMoved {
                    id: b1_vertex,
                    sheet_id: a1.sheet_id,
                    old_coord: GridAddr::new(1, 1),
                    new_coord: GridAddr::new(0, 1),
                },
            ],
        },
        arrow: ArrowUndoBatch::default(),
        affected_cells: 0,
    };
    let mut undo = UndoEngine::new();
    undo.push_action(journal);

    assert!(engine.undo_action(&mut undo).is_err());
    assert_graph_plan_stale(&mut engine, &plan);
}

#[test]
fn arrow_only_structural_journal_advances_topology_epoch_once() {
    let mut engine = setup(false);
    let mut arrow = ArrowUndoBatch::default();
    arrow.record_insert_rows(engine.sheet_id("Sheet1").unwrap(), 0, 1);
    let journal = ActionJournal {
        name: "arrow-only insert".to_string(),
        graph: GraphUndoBatch::default(),
        arrow,
        affected_cells: 1,
    };
    let mut undo = UndoEngine::new();
    undo.push_action(journal);
    let topology_before = engine.topology_epoch_for_test();

    engine.undo_action(&mut undo).unwrap();
    assert_eq!(
        engine.topology_epoch_for_test(),
        topology_before.wrapping_add(1)
    );
}

#[test]
fn formula_plane_demotion_and_later_logged_edit_bump_topology_once() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
    );
    let mut formulas = Vec::new();
    for row in 1..=200 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let formula = format!("=A{row}*2");
        let ast_id = engine.intern_formula_ast(&parse(&formula).unwrap());
        formulas.push(FormulaIngestRecord::new(
            row,
            2,
            ast_id,
            Some(Arc::<str>::from(formula)),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    let topology_before = engine.topology_epoch_for_test();

    engine
        .action_atomic("demote then edit", |action| {
            action.set_cell_formula("Sheet1", 8, 2, parse("=A8*3").unwrap())?;
            action.set_cell_formula("Sheet1", 1, 3, parse("=A1+1").unwrap())
        })
        .unwrap();
    assert_eq!(
        engine.topology_epoch_for_test(),
        topology_before.wrapping_add(1)
    );
}

#[test]
fn direct_logged_lookup_axis_value_edit_advances_snapshot() {
    let mut engine = Engine::new(TestWorkbook::new(), config());
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Int(row as i64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 4, LiteralValue::Int(row as i64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 5, LiteralValue::Int((row * 10) as i64))
            .unwrap();
        engine
            .set_cell_formula(
                "Sheet1",
                row,
                2,
                parse(format!("=VLOOKUP(A{row}, $D$1:$E$100, 2, FALSE)")).unwrap(),
            )
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    assert!(engine.last_lookup_index_cache_report().hits > 0);

    let d50 = cell(&engine, 50, 4);
    engine
        .edit_with_logger(&mut ChangeLog::new(), |editor| {
            editor.set_cell_value(d50, LiteralValue::Int(500));
        })
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(matches!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Error(error)) if error.kind == formualizer_common::ExcelErrorKind::Na
    ));
}
