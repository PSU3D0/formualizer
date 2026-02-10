//! Ticket 618: ChangeLog is an observability sink.

use crate::engine::{ChangeLog, EditorError, Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use formualizer_parse::LiteralValue;

use super::common::arrow_eval_config;

#[test]
fn action_with_logger_emits_events_on_commit_and_truncates_on_rollback() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());
    let mut log = ChangeLog::new();

    let start_len = log.len();
    engine
        .action_with_logger(&mut log, "seed", |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))?;
            Ok(())
        })
        .unwrap();
    assert!(log.len() > start_len);
    assert_eq!(log.compound_depth(), 0);

    let mid_len = log.len();
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
        EditorError::TransactionFailed { reason } => assert!(reason.contains("intentional")),
        other => panic!("expected TransactionFailed, got {other:?}"),
    }

    assert_eq!(log.len(), mid_len, "rollback must truncate to start_len");
    assert_eq!(log.compound_depth(), 0, "compound stack must not leak");
    assert_eq!(engine.get_cell_value("Sheet1", 2, 1), None);
    match engine.get_cell("Sheet1", 2, 2) {
        None => {}
        Some((ast, _v)) => assert!(ast.is_none(), "expected B2 formula to be cleared"),
    }
}
