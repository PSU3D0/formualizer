//! Ticket 614: Engine Action API (commit-only transaction surface)

use crate::engine::{EditorError, Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::LiteralValue;
use formualizer_parse::parser::parse;

use super::common::arrow_eval_config;

#[test]
fn engine_action_simple_edit_evaluates_arrow_truth() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .action("seed", |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))?;
            tx.set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())?;
            Ok(())
        })
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );
}

#[test]
fn engine_action_nested_is_rejected() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let err = engine
        .action("outer", |tx| {
            tx.action("inner", |_inner| Ok(()))?;
            Ok(())
        })
        .unwrap_err();

    match err {
        EditorError::TransactionFailed { reason } => {
            assert!(reason.contains("Nested Engine::action"), "reason={reason}");
        }
        other => panic!("expected TransactionFailed, got {other:?}"),
    }
}
