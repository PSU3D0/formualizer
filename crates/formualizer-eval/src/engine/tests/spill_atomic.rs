use crate::engine::EvalConfig;
use crate::engine::eval::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_parse::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn spill_commit_is_atomic_under_fault() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Seed with a 2x2 spill
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Next, attempt a larger spill; we will inject a fault via internal API.
    // We can’t call the faulting API from Engine; we’ll use graph directly.
    let anchor_vertex = engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 1, 1))
        .copied()
        .expect("anchor vertex");

    // Prepare new 2x3 values and targets
    let rows = vec![
        vec![
            LiteralValue::Number(7.0),
            LiteralValue::Number(8.0),
            LiteralValue::Number(9.0),
        ],
        vec![
            LiteralValue::Number(10.0),
            LiteralValue::Number(11.0),
            LiteralValue::Number(12.0),
        ],
    ];
    let mut targets = Vec::new();
    for r in 0..rows.len() as u32 {
        for c in 0..rows[0].len() as u32 {
            targets.push(engine.graph.make_cell_ref("Sheet1", 1 + r, 1 + c));
        }
    }

    // Plan should pass
    engine
        .graph
        .plan_spill_region(anchor_vertex, &targets)
        .unwrap();

    // Inject a fault after a few operations, ensure rollback occurs
    let res = engine.graph.commit_spill_region_atomic_with_fault(
        anchor_vertex,
        targets.clone(),
        rows.clone(),
        Some(2),
    );
    assert!(res.is_err());

    // Original 2x2 content should remain intact
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
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
}

#[test]
fn spill_resize_shrink_is_atomic() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Seed with a 2x2 spill [[1,2],[3,4]]
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Now shrink to 1x1 {9}
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={9}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Anchor shows 9 and previously spilled cells are cleared
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(9.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Empty)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Empty)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Empty)
    );
}

#[test]
fn spill_resize_shrink_with_fault_rolls_back() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Seed with a 2x2 spill [[1,2],[3,4]]
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Prepare a shrink to 1x1 at A1 via direct graph call with injected fault
    let anchor_vertex = engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 1, 1))
        .copied()
        .expect("anchor vertex");

    let rows = vec![vec![LiteralValue::Number(9.0)]];
    let targets = vec![engine.graph.make_cell_ref("Sheet1", 1, 1)];

    engine
        .graph
        .plan_spill_region(anchor_vertex, &targets)
        .unwrap();
    let res =
        engine
            .graph
            .commit_spill_region_atomic_with_fault(anchor_vertex, targets, rows, Some(0));
    assert!(res.is_err());

    // Original 2x2 content must remain
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
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
}

#[test]
fn spill_resize_grow_is_atomic() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Seed with a 2x2 spill [[1,2],[3,4]]
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Grow to 2x3 [[10,20,30],[40,50,60]]
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={10,20,30;40,50,60}").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Assert new rectangle is present and consistent
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(30.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(40.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(50.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(60.0))
    );
}
