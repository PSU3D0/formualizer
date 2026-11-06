//! FORM-000328: structural edits move Arrow cells while the committed spill
//! registry retains its old coordinates. Kept separate from FORM-000319.
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
#[ignore = "FORM-000328: structural edits must move spill ownership with user values"]
fn form_000328_insert_above_spill_preserves_shifted_user_value() {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            family_execution: false,
            ..Default::default()
        },
    );
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.))
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 2, parse("=SEQUENCE(A1)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    e.set_cell_value("Sheet1", 4, 2, LiteralValue::Number(99.))
        .unwrap();
    e.insert_rows("Sheet1", 1, 1).unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(99.))
    );
}
