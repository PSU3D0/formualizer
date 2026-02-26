use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{ChangeLog, EditorError, Engine, RowVisibilitySource};
use crate::test_workbook::TestWorkbook;

use super::common::arrow_eval_config;

#[test]
fn action_with_logger_rollback_restores_row_visibility() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    let mut log = ChangeLog::new();

    engine
        .set_row_hidden("Sheet1", 2, true, RowVisibilitySource::Manual)
        .unwrap();

    let err = engine
        .action_with_logger(
            &mut log,
            "row-visibility-rollback",
            |tx| -> Result<(), EditorError> {
                tx.set_row_hidden("Sheet1", 2, false, RowVisibilitySource::Manual)?;
                tx.set_row_hidden("Sheet1", 3, true, RowVisibilitySource::Filter)?;
                Err(EditorError::TransactionFailed {
                    reason: "intentional".to_string(),
                })
            },
        )
        .unwrap_err();

    match err {
        EditorError::TransactionFailed { reason } => assert!(reason.contains("intentional")),
        other => panic!("expected TransactionFailed, got {other:?}"),
    }

    assert_eq!(log.len(), 0, "rollback should truncate action events");
    assert_eq!(
        engine.is_row_hidden("Sheet1", 2, Some(RowVisibilitySource::Manual)),
        Some(true)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 3, Some(RowVisibilitySource::Filter)),
        Some(false)
    );
}

#[test]
fn undo_logged_and_redo_logged_restore_row_visibility() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();

    engine
        .action_with_logger(&mut log, "row-visibility", |tx| {
            tx.set_row_hidden("Sheet1", 2, true, RowVisibilitySource::Manual)?;
            tx.set_row_hidden("Sheet1", 3, true, RowVisibilitySource::Filter)?;
            Ok(())
        })
        .unwrap();

    assert!(
        log.events()
            .iter()
            .any(|e| matches!(e, crate::engine::ChangeEvent::SetRowVisibility { .. }))
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 2, Some(RowVisibilitySource::Manual)),
        Some(true)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 3, Some(RowVisibilitySource::Filter)),
        Some(true)
    );

    engine.undo_logged(&mut undo, &mut log).unwrap();
    assert_eq!(
        engine.is_row_hidden("Sheet1", 2, Some(RowVisibilitySource::Manual)),
        Some(false)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 3, Some(RowVisibilitySource::Filter)),
        Some(false)
    );

    engine.redo_logged(&mut undo, &mut log).unwrap();
    assert_eq!(
        engine.is_row_hidden("Sheet1", 2, Some(RowVisibilitySource::Manual)),
        Some(true)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 3, Some(RowVisibilitySource::Filter)),
        Some(true)
    );
}

#[test]
fn undo_action_and_redo_action_restore_row_visibility() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    let mut undo = UndoEngine::new();

    let (_v, journal) = engine
        .action_atomic_journal("row-visibility".to_string(), |tx| {
            tx.set_row_hidden("Sheet1", 5, true, RowVisibilitySource::Manual)?;
            tx.set_rows_hidden("Sheet1", 7, 8, true, RowVisibilitySource::Filter)?;
            Ok(())
        })
        .unwrap();
    undo.push_action(journal);

    assert_eq!(
        engine.is_row_hidden("Sheet1", 5, Some(RowVisibilitySource::Manual)),
        Some(true)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 7, Some(RowVisibilitySource::Filter)),
        Some(true)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 8, Some(RowVisibilitySource::Filter)),
        Some(true)
    );

    engine.undo_action(&mut undo).unwrap();
    assert_eq!(
        engine.is_row_hidden("Sheet1", 5, Some(RowVisibilitySource::Manual)),
        Some(false)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 7, Some(RowVisibilitySource::Filter)),
        Some(false)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 8, Some(RowVisibilitySource::Filter)),
        Some(false)
    );

    engine.redo_action(&mut undo).unwrap();
    assert_eq!(
        engine.is_row_hidden("Sheet1", 5, Some(RowVisibilitySource::Manual)),
        Some(true)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 7, Some(RowVisibilitySource::Filter)),
        Some(true)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 8, Some(RowVisibilitySource::Filter)),
        Some(true)
    );
}
