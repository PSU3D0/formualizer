//! Ticket 618: action_atomic journal (no ChangeLog dependency for correctness).

use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{EditorError, Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use formualizer_parse::LiteralValue;

use super::common::arrow_eval_config;

#[test]
fn action_atomic_rollback_restores_arrow_truth_without_logger() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());

    let err = engine
        .action_atomic("boom".to_string(), |tx| -> Result<(), EditorError> {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))?;
            tx.set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())?;
            Err(EditorError::TransactionFailed {
                reason: "intentional failure".to_string(),
            })
        })
        .unwrap_err();

    match err {
        EditorError::TransactionFailed { reason } => assert!(reason.contains("intentional")),
        other => panic!("expected TransactionFailed, got {other:?}"),
    }

    assert_eq!(engine.get_cell_value("Sheet1", 1, 1), None);
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), None);
    match engine.get_cell("Sheet1", 1, 2) {
        None => {}
        Some((ast, _v)) => assert!(ast.is_none(), "expected B1 formula to be cleared"),
    }
}

#[test]
fn action_atomic_commit_then_undo_redo_restores_states() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    let mut undo = UndoEngine::new();

    let (_v, journal) = engine
        .action_atomic_journal("seed".to_string(), |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))?;
            tx.set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())?;
            Ok(())
        })
        .unwrap();
    undo.push_action(journal);

    engine.evaluate_all().unwrap();
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

    engine.undo_action(&mut undo).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.get_cell_value("Sheet1", 1, 1), None);
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), None);
    match engine.get_cell("Sheet1", 1, 2) {
        None => {}
        Some((ast, _v)) => assert!(ast.is_none(), "expected B1 formula to be cleared"),
    }

    engine.redo_action(&mut undo).unwrap();
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
fn action_atomic_rejects_delete_rows_cols() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());

    let err = engine
        .action_atomic("nope".to_string(), |tx| {
            let _ = tx.delete_rows("Sheet1", 1, 1)?;
            Ok(())
        })
        .unwrap_err();

    match err {
        EditorError::TransactionUnsupported { reason } => {
            assert!(reason.contains("delete_rows"), "reason={reason}");
        }
        other => panic!("expected TransactionUnsupported, got {other:?}"),
    }

    let err2 = engine
        .action_atomic("nope2".to_string(), |tx| {
            let _ = tx.delete_columns("Sheet1", 1, 1)?;
            Ok(())
        })
        .unwrap_err();

    match err2 {
        EditorError::TransactionUnsupported { reason } => {
            assert!(reason.contains("delete_columns"), "reason={reason}");
        }
        other => panic!("expected TransactionUnsupported, got {other:?}"),
    }
}

#[test]
fn action_atomic_spill_clear_undo_restores_spill_rect() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    let mut undo = UndoEngine::new();

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(4.0))
    );

    // Commit an action that overwrites the spill anchor; this should clear the spill.
    let (_v, journal) = engine
        .action_atomic_journal("clear_spill".to_string(), |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(9.0))?;
            Ok(())
        })
        .unwrap();
    undo.push_action(journal);

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(9.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 2, 2), None);

    // Undo must restore the spill rectangle values without requiring a recalculation pass.
    engine.undo_action(&mut undo).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(4.0))
    );
    let a1 = engine.get_cell("Sheet1", 1, 1).expect("expected A1");
    assert!(a1.0.is_some(), "expected A1 formula restored");

    // Redo should clear the spill again.
    engine.redo_action(&mut undo).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(9.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 2, 2), None);
}
