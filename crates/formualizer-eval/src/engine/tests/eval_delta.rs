use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use rustc_hash::FxHashSet;

fn serial_eval_config() -> EvalConfig {
    EvalConfig {
        enable_parallel: false,
        ..Default::default()
    }
}

#[test]
fn scalar_delta_initial_and_noop() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1").unwrap())
        .unwrap();
    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    let coords: FxHashSet<(u32, u32)> = delta
        .changed_cells
        .iter()
        .map(|c| {
            let (_sid, r, col) = c.to_excel_1based();
            (r, col)
        })
        .collect();
    assert_eq!(coords, FxHashSet::from_iter([(1, 1)]));

    let (res2, delta2) = engine.evaluate_all_with_delta().unwrap();
    assert_eq!(res2.computed_vertices, 0);
    assert!(delta2.changed_cells.is_empty());
}

#[test]
fn spill_delta_includes_clears() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={1,2;3,4}").unwrap())
        .unwrap();
    let (_res, delta1) = engine.evaluate_all_with_delta().unwrap();
    let coords1: FxHashSet<(u32, u32)> = delta1
        .changed_cells
        .iter()
        .map(|c| {
            let (_sid, r, col) = c.to_excel_1based();
            (r, col)
        })
        .collect();
    assert_eq!(
        coords1,
        FxHashSet::from_iter([(1, 1), (1, 2), (2, 1), (2, 2)])
    );

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("={9}").unwrap())
        .unwrap();
    let (_res, delta2) = engine.evaluate_all_with_delta().unwrap();
    let coords2: FxHashSet<(u32, u32)> = delta2
        .changed_cells
        .iter()
        .map(|c| {
            let (_sid, r, col) = c.to_excel_1based();
            (r, col)
        })
        .collect();
    assert_eq!(
        coords2,
        FxHashSet::from_iter([(1, 1), (1, 2), (2, 1), (2, 2)])
    );

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
fn parallel_delta_is_deterministic_for_scalars() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=2").unwrap())
        .unwrap();

    let (_res, delta) = engine.evaluate_all_with_delta().unwrap();
    let coords: FxHashSet<(u32, u32)> = delta
        .changed_cells
        .iter()
        .map(|c| {
            let (_sid, r, col) = c.to_excel_1based();
            (r, col)
        })
        .collect();
    assert_eq!(coords, FxHashSet::from_iter([(1, 1), (1, 2)]));
}
