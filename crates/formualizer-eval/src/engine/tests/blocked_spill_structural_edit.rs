//! A blocked spill registers a waiter on its blocker. A structural edit wakes
//! the waiters of the region it moves, which for an insert or delete is
//! unbounded on one axis; computing that region's area overflowed u32 and the
//! probe then built coordinates past the grid.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn blocked_spill() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=1+1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(matches!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Error(_))
    ));
    engine
}

#[test]
fn structural_edits_with_a_blocked_spill_wake_waiters() {
    for op in 0..4 {
        let mut engine = blocked_spill();
        match op {
            0 => drop(engine.insert_rows("Sheet1", 5, 1).unwrap()),
            1 => drop(engine.delete_rows("Sheet1", 5, 1).unwrap()),
            2 => drop(engine.insert_columns("Sheet1", 5, 1).unwrap()),
            _ => drop(engine.delete_columns("Sheet1", 5, 1).unwrap()),
        }
        engine.evaluate_all().unwrap();
    }
}

#[test]
fn removing_the_blocker_after_an_insert_lets_the_spill_through() {
    let mut engine = blocked_spill();
    engine.insert_rows("Sheet1", 10, 2).unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Empty)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(3.0))
    );
}
