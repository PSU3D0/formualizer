use crate::engine::{EvalConfig, eval::Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::Parser;

#[test]
fn spill_exceeds_sheet_bounds() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);

    // Anchor at last allowed column (zero-based max 16383), spilling 1x2 should exceed bounds
    engine
        .set_cell_value("Sheet1", 1, 16383, LiteralValue::Int(0))
        .unwrap();
    // Array that would require col 16384 (out of bounds)
    engine
        .set_cell_formula("Sheet1", 1, 16383, Parser::from("={1,2}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 16383) {
        Some(LiteralValue::Error(e)) => {
            assert_eq!(e, "#SPILL!");
            if let formualizer_common::ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            } = &e.extra
            {
                assert_eq!((*expected_rows, *expected_cols), (1, 2));
            }
        }
        v => panic!("expected #SPILL!, got {v:?}"),
    }
}

#[test]
fn spill_exceeds_sheet_bounds_rows() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);

    // Anchor at last allowed row (zero-based max 1_048_575), spilling 2 rows should exceed bounds
    engine
        .set_cell_value("Sheet1", 1_048_575, 1, LiteralValue::Int(0))
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1_048_575,
            1,
            Parser::from("={1;2}").parse().unwrap(),
        )
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1_048_575, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e, "#SPILL!"),
        v => panic!("expected #SPILL!, got {v:?}"),
    }
}

#[test]
fn spill_values_update_dependents() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);

    // A1 spills 2x2
    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("={1,2;3,4}").parse().unwrap())
        .unwrap();
    // C1 reads B2 (spilled bottom-right of 2x2)
    engine
        .set_cell_formula("Sheet1", 1, 3, Parser::from("=B2").parse().unwrap())
        .unwrap();
    // Two-pass: first pass materializes spill cells; second pass updates dependents
    let _ = engine.evaluate_all().unwrap();
    // Demand-driven compute of C1 after spill is materialized
    let _ = engine.evaluate_until(&["C1"]).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(4.0))
    );

    // Change anchor to {5,6;7,8}, B2 becomes 8; C1 should update to 8
    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("={5,6;7,8}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    let _ = engine.evaluate_until(&["C1"]).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(8.0))
    );
}

#[test]
fn scalar_after_array_clears_spill() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);

    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("={1,2;3,4}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Switch to scalar
    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("=42").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(42.0))
    );
    // Previously spilled cells cleared
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
fn empty_cells_do_not_block_spill() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);

    // Pre-fill B1 with Empty explicitly
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Empty)
        .unwrap();
    // A1 spills into A1:B1
    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("={10,20}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );
}

#[test]
fn non_empty_values_block_spill() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);

    // Pre-fill B1 with a non-empty value
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(99.0))
        .unwrap();
    // A1 tries to spill 1x2 into A1:B1; B1 contains a value → #SPILL!
    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("={10,20}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e, "#SPILL!"),
        v => panic!("expected #SPILL!, got {v:?}"),
    }
}

#[test]
fn overlapping_spills_conflict() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);

    // A1 and A2 both try to spill 2x2 overlapping on A2:B3
    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("={1,2;3,4}").parse().unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, Parser::from("={5,6;7,8}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    let a1 = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    let a2 = engine.get_cell_value("Sheet1", 2, 1).unwrap();
    let is_spill = |v: &LiteralValue| matches!(v, LiteralValue::Error(e) if e.kind == formualizer_common::ExcelErrorKind::Spill);
    assert!(
        is_spill(&a1) || is_spill(&a2),
        "expected at least one anchor to be #SPILL!, got A1={a1:?}, A2={a2:?}"
    );
}

#[test]
fn formula_cells_block_spill() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);

    // Put a scalar formula in B1
    engine
        .set_cell_formula("Sheet1", 1, 2, Parser::from("=42").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    // A1 tries to spill 1x2 into A1:B1; B1 is occupied by a formula → #SPILL!
    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("={1,2}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e, "#SPILL!"),
        v => panic!("expected #SPILL!, got {v:?}"),
    }
}

#[test]
fn overlapping_spills_firstwins_is_deterministic_sequential() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);

    // Evaluate A1 first, then A2; A2 should conflict and show #SPILL! (FirstWins)
    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("={1,2;3,4}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    engine
        .set_cell_formula("Sheet1", 2, 1, Parser::from("={5,6;7,8}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    let a1 = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    let a2 = engine.get_cell_value("Sheet1", 2, 1).unwrap();
    match a2 {
        LiteralValue::Error(e) => assert_eq!(e, "#SPILL!"),
        v => panic!("expected #SPILL! at A2, got {v:?} (A1={a1:?})"),
    }
}

#[test]
fn spills_on_different_sheets_do_not_conflict() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(wb, cfg);
    // Add Sheet2
    engine.graph.add_sheet("Sheet2").unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 1, Parser::from("={1,2}").parse().unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet2", 1, 1, Parser::from("={3,4}").parse().unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet2", 1, 1),
        Some(LiteralValue::Number(3.0))
    );
}
