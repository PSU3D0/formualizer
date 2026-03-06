use super::common::{create_binary_op_ast, create_cell_ref_ast};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, LiteralValue};

fn telemetry_config() -> EvalConfig {
    EvalConfig {
        enable_virtual_dep_telemetry: true,
        ..EvalConfig::default()
    }
}

fn make_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), telemetry_config())
}

fn chain_ast(row: u32) -> formualizer_parse::ASTNode {
    create_binary_op_ast(
        create_cell_ref_ast(None, row - 1, 1),
        create_cell_ref_ast(None, row - 1, 1),
        "+",
    )
}

#[test]
fn schedule_cache_hits_on_repeated_value_only_chain_recalc() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))?;
    engine.set_cell_formula("Sheet1", 2, 1, chain_ast(2))?;
    engine.set_cell_formula("Sheet1", 3, 1, chain_ast(3))?;
    engine.set_cell_formula("Sheet1", 4, 1, chain_ast(4))?;

    engine.evaluate_all()?;

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))?;
    engine.evaluate_all()?;

    let telemetry = engine.last_virtual_dep_telemetry().clone();
    assert_eq!(telemetry.schedule_cache_hits, 1);
    assert_eq!(telemetry.schedule_cache_misses, 0);
    assert_eq!(telemetry.reused_schedule_vertices_total, 3);
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(16.0))
    );
    Ok(())
}

#[test]
fn schedule_cache_invalidates_after_formula_edit() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))?;
    engine.set_cell_formula("Sheet1", 2, 1, chain_ast(2))?;
    engine.set_cell_formula("Sheet1", 3, 1, chain_ast(3))?;

    engine.evaluate_all()?;

    let replacement = create_binary_op_ast(
        create_cell_ref_ast(None, 2, 1),
        create_cell_ref_ast(None, 1, 1),
        "+",
    );
    engine.set_cell_formula("Sheet1", 3, 1, replacement)?;
    engine.evaluate_all()?;

    let telemetry = engine.last_virtual_dep_telemetry().clone();
    assert_eq!(telemetry.schedule_cache_hits, 0);
    assert_eq!(telemetry.schedule_cache_misses, 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(3.0))
    );
    Ok(())
}
