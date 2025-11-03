use super::common::{create_binary_op_ast, create_cell_ref_ast};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, LiteralValue};

fn make_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

#[test]
fn recalc_plan_matches_evaluate_all() -> Result<(), ExcelError> {
    let mut engine = make_engine();

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))?;
    engine.set_cell_value("Sheet1", 1, 2, LiteralValue::Int(5))?;

    let sum_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 1, 1),
        create_cell_ref_ast(None, 1, 2),
        "+",
    );
    engine.set_cell_formula("Sheet1", 2, 1, sum_ast)?;

    let double_ast = create_binary_op_ast(
        create_cell_ref_ast(None, 2, 1),
        create_cell_ref_ast(None, 2, 1),
        "+",
    );
    engine.set_cell_formula("Sheet1", 3, 1, double_ast)?;

    engine.evaluate_all()?;
    let plan = engine.build_recalc_plan()?;

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(20))?;
    let plan_result = engine.evaluate_recalc_plan(&plan)?;

    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(50.0))
    );
    assert_eq!(plan_result.computed_vertices, 2);

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(30))?;
    let all_result = engine.evaluate_all()?;
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(70.0))
    );
    assert_eq!(all_result.computed_vertices, 2);

    Ok(())
}

#[test]
fn recalc_plan_no_dirty_is_noop() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(5))?;
    engine.evaluate_all()?;
    let plan = engine.build_recalc_plan()?;
    let result = engine.evaluate_recalc_plan(&plan)?;
    assert_eq!(result.computed_vertices, 0);
    assert_eq!(result.cycle_errors, 0);
    Ok(())
}

#[test]
fn recalc_plan_reused_for_multiple_runs() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))?;

    let chain_ast = |row: u32| {
        create_binary_op_ast(
            create_cell_ref_ast(None, row - 1, 1),
            create_cell_ref_ast(None, row - 1, 1),
            "+",
        )
    };

    engine.set_cell_formula("Sheet1", 2, 1, chain_ast(2))?;
    engine.set_cell_formula("Sheet1", 3, 1, chain_ast(3))?;
    engine.set_cell_formula("Sheet1", 4, 1, chain_ast(4))?;

    engine.evaluate_all()?;
    let plan = engine.build_recalc_plan()?;

    for value in [2, 3, 4] {
        engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(value))?;
        let result = engine.evaluate_recalc_plan(&plan)?;
        assert_eq!(result.computed_vertices, 3);
        let expected = LiteralValue::Number((value * 8) as f64);
        assert_eq!(engine.get_cell_value("Sheet1", 4, 1), Some(expected));
    }

    Ok(())
}
