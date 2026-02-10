use crate::engine::{ChangeLog, EditorError, Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use formualizer_parse::LiteralValue;

use super::common::arrow_eval_config;

#[test]
fn engine_action_with_logger_rollback_restores_values_and_formulas() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut log = ChangeLog::new();

    let err = engine
        .action_with_logger(&mut log, "rollback", |tx| -> Result<(), EditorError> {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))?;
            tx.set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())?;
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

    // Values (Arrow-truth) are reverted.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 1), None);
    assert_eq!(engine.get_cell_value("Sheet1", 1, 2), None);

    // Formula is reverted.
    let b1 = engine.get_cell("Sheet1", 1, 2);
    match b1 {
        None => {}
        Some((ast, _v)) => assert!(ast.is_none(), "expected B1 formula to be cleared"),
    }
}

#[test]
fn engine_action_with_logger_rollback_truncates_changelog() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut log = ChangeLog::new();

    assert_eq!(log.len(), 0);

    let _ = engine.action_with_logger(&mut log, "rollback", |tx| -> Result<(), EditorError> {
        tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))?;
        Err(EditorError::TransactionFailed {
            reason: "intentional failure".to_string(),
        })
    });

    assert_eq!(
        log.len(),
        0,
        "action events should be truncated on rollback"
    );
}
