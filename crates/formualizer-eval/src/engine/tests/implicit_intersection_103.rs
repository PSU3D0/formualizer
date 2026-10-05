use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn serial_eval_config() -> EvalConfig {
    EvalConfig {
        enable_parallel: false,
        ..Default::default()
    }
}

#[test]
fn implicit_intersection_column_vector_selects_by_row() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(42.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 5, 2, parse("=@A1:A10").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn implicit_intersection_row_vector_selects_by_column() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(7.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 3, 3, parse("=@A1:E1").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(7.0))
    );
}

#[test]
fn implicit_intersection_2d_selects_by_row_and_col_cross_sheet() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 3, LiteralValue::Number(123.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet2", 5, 3, parse("=@Sheet1!A1:E10").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet2", 5, 3),
        Some(LiteralValue::Number(123.0))
    );
}

#[test]
fn implicit_intersection_out_of_bounds_is_value_error() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(42.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 20, 2, parse("=@A1:A10").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 20, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.to_string(), "#VALUE!"),
        other => panic!("expected #VALUE!, got {other:?}"),
    }
}

#[test]
fn implicit_intersection_suppresses_spill_from_array_function() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=@SEQUENCE(2,2)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(1.0))
    );

    // No spill should occur.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 5), None);
    assert_eq!(engine.get_cell_value("Sheet1", 2, 4), None);
}

#[test]
fn implicit_intersection_against_spilled_values_requires_at_for_scalar() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    // A1 spills a 3x1 vector: A1:A3 = 1,2,3
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();

    // B2 uses @ to pick the intersecting element (A2)
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=@A1:A3").unwrap())
        .unwrap();

    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(2.0))
    );

    // B2 should be scalar (no spill).
    assert_eq!(engine.get_cell_value("Sheet1", 3, 2), None);
}

#[test]
fn implicit_intersection_whole_row_uses_the_formula_column() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    engine
        .set_cell_value("Sheet1", 21, 1, LiteralValue::Text("id".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 21, 7, LiteralValue::Text("Y".into()))
        .unwrap();
    // G2 reads G21; H2 reads H21, past the row's last used cell (blank).
    engine
        .set_cell_formula("Sheet1", 2, 7, parse("=@21:21").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 8, parse("=@21:21=\"Y\"").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 7),
        Some(LiteralValue::Text("Y".into()))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 8),
        Some(LiteralValue::Boolean(false))
    );
}

#[test]
fn implicit_intersection_whole_column_uses_the_formula_row() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    engine
        .set_cell_value("Sheet2", 4, 1, LiteralValue::Number(9.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 4, 3, parse("=@Sheet2!A:A*2").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 3),
        Some(LiteralValue::Number(18.0))
    );
}

#[test]
fn implicit_intersection_in_a_relative_formula_family_uses_each_placement() {
    // One relative shape down a column: each placement intersects its own
    // window, including rows past the first placement's window.
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    for r in 1..=40 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    for r in 5..30 {
        engine
            .set_cell_formula(
                "Sheet1",
                r,
                2,
                parse(format!("=@A{}:A{}", r - 1, r + 5)).unwrap(),
            )
            .unwrap();
    }
    let _ = engine.evaluate_all().unwrap();
    for r in 5..30 {
        assert_eq!(
            engine.get_cell_value("Sheet1", r, 2),
            Some(LiteralValue::Number(r as f64)),
            "B{r}"
        );
    }
}
