use crate::engine::{EvalConfig, eval::Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn spill_basic_and_block() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Put array formula in A1 that spills 2x2
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    let res = engine.evaluate_all().unwrap();
    assert!(res.computed_vertices >= 1);

    // Anchor shows top-left value
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    // Spilled cells
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(4.0))
    );

    // Change shape: from 2x2 to 1x3 and ensure old cells are cleared/resized correctly
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={7,8,9}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(8.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(9.0))
    );
    // Prior second row should be cleared to Empty now
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Empty)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Empty)
    );

    // Now block the spill by placing a value at A2 and change formula to 2x1 to overlap
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Text("X".into()))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={10;20}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    // Anchor should be #SPILL!
    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => {
            assert_eq!(e, "#SPILL!");
            // Optional: check extra payload if present
            if let formualizer_common::ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            } = &e.extra
            {
                assert_eq!((*expected_rows, *expected_cols), (2, 1));
            }
        }
        v => panic!("expected #SPILL!, got {v:?}"),
    }
}
