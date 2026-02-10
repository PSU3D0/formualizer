//! Ticket 617: Engine-level transaction boundary tests (Arrow-truth + undo/redo).

use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{ChangeLog, EditorError, Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::LiteralValue;
use formualizer_parse::parser::parse;

use super::common::arrow_eval_config;

#[test]
fn engine_action_with_logger_commit_then_undo_redo_restores_end_states() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();

    // Pre-action state.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 1), None);
    assert!(engine.get_cell("Sheet1", 1, 2).is_none());

    // Commit an action containing multiple edits.
    engine
        .action_with_logger(&mut log, "seed", |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))?;
            tx.set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())?;
            Ok(())
        })
        .unwrap();

    engine.evaluate_all().unwrap();

    // Post-action state.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );
    let b1 = engine.get_cell("Sheet1", 1, 2).expect("expected B1 cell");
    assert!(b1.0.is_some(), "expected B1 to have a formula");

    // Undo once: must restore the pre-action state (atomic boundary is the action).
    engine.undo_logged(&mut undo, &mut log).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.get_cell_value("Sheet1", 1, 1), None);
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), None);
    match engine.get_cell("Sheet1", 1, 2) {
        None => {}
        Some((ast, _v)) => assert!(ast.is_none(), "expected B1 formula to be cleared"),
    }
    assert_eq!(log.len(), 0, "undo should truncate the action group");

    // Redo once: must restore the post-action state.
    engine.redo_logged(&mut undo, &mut log).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );
    let b1_after = engine
        .get_cell("Sheet1", 1, 2)
        .expect("expected B1 cell after redo");
    assert!(
        b1_after.0.is_some(),
        "expected B1 to have a formula after redo"
    );
}

#[test]
fn engine_action_with_logger_rollback_truncates_log_and_allows_future_actions() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    let mut log = ChangeLog::new();

    // Seed the log so we can assert rollback truncates back to an arbitrary start len.
    engine
        .action_with_logger(&mut log, "seed", |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))?;
            Ok(())
        })
        .unwrap();
    let start_len = log.len();
    assert!(start_len > 0);

    // Failed action must rollback both graph and Arrow-truth, and truncate the log.
    let err = engine
        .action_with_logger(&mut log, "boom", |tx| -> Result<(), EditorError> {
            tx.set_cell_value("Sheet1", 2, 1, LiteralValue::Number(99.0))?;
            tx.set_cell_formula("Sheet1", 2, 2, parse("=A2+1").unwrap())?;
            Err(EditorError::TransactionFailed {
                reason: "intentional failure".to_string(),
            })
        })
        .unwrap_err();
    match err {
        EditorError::TransactionFailed { reason } => {
            assert!(reason.contains("intentional failure"), "reason={reason}");
        }
        other => panic!("expected TransactionFailed, got {other:?}"),
    }

    assert_eq!(log.len(), start_len, "rollback must truncate to start_len");
    assert_eq!(log.compound_depth(), 0, "compound stack must not leak");
    assert_eq!(engine.get_cell_value("Sheet1", 2, 1), None);
    match engine.get_cell("Sheet1", 2, 2) {
        None => {}
        Some((ast, _v)) => assert!(ast.is_none(), "expected B2 formula to be cleared"),
    }

    // A subsequent successful action must still work.
    engine
        .action_with_logger(&mut log, "future", |tx| {
            tx.set_cell_formula("Sheet1", 1, 2, parse("=A1+1").unwrap())?;
            Ok(())
        })
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    let b1 = engine.get_cell("Sheet1", 1, 2).expect("expected B1 cell");
    assert!(b1.0.is_some(), "expected B1 to have a formula");
}

#[test]
fn transaction_context_is_structure_only() {
    // The graph no longer caches cell/formula values in canonical mode.
    // TransactionContext rollback guarantees structural restoration (formulas/vertex mapping),
    // but callers must not treat it as a value rollback mechanism.
    use crate::CellRef;
    use crate::engine::DependencyGraph;
    use crate::engine::graph::editor::transaction_context::TransactionContext;
    use crate::reference::Coord;

    fn cell_ref(sheet_id: u16, row: u32, col: u32) -> CellRef {
        CellRef::new(sheet_id, Coord::from_excel(row, col, true, true))
    }

    let mut graph = DependencyGraph::new_with_config(arrow_eval_config());
    let a1 = cell_ref(0, 1, 1);

    // Create a formula in a transaction, then roll back.
    {
        let mut ctx = TransactionContext::new(&mut graph);
        ctx.begin().unwrap();
        {
            let mut editor = ctx.editor();
            editor.set_cell_formula(a1, parse("=1+1").unwrap());
        }
        ctx.rollback().unwrap();
    }

    // Rollback must remove the formula structurally.
    match graph.get_vertex_id_for_address(&a1) {
        None => {}
        Some(id) => {
            assert!(
                graph.get_formula(*id).is_none(),
                "expected A1 formula to be cleared"
            );
        }
    }
}
