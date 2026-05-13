use std::sync::Arc;

use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{
    EditorError, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn authoritative_engine() -> Engine<TestWorkbook> {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    Engine::new(TestWorkbook::default(), cfg)
}

fn formula_record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn build_single_span_engine(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(formula_record(&mut engine, row, 2, &format!("=A{row}*2")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    engine
}

fn build_two_span_engine(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(formula_record(&mut engine, row, 2, &format!("=A{row}*2")));
        formulas.push(formula_record(&mut engine, row, 3, &format!("=B{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine.evaluate_all().unwrap();
    engine
}

fn assert_number_cell(engine: &Engine<TestWorkbook>, row: u32, col: u32, expected: f64) {
    match engine.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Number(actual)) => assert_eq!(actual, expected),
        Some(LiteralValue::Int(actual)) => assert_eq!(actual as f64, expected),
        other => panic!("expected Sheet1!R{row}C{col} to be {expected}, got {other:?}"),
    }
}

fn edit_first_fifty_values(
    tx: &mut crate::engine::EngineAction<'_, TestWorkbook>,
) -> Result<(), EditorError> {
    for row in 1..=50 {
        tx.set_cell_value("Sheet1", row, 1, LiteralValue::Number((row * 10) as f64))?;
    }
    Ok(())
}

#[test]
fn action_atomic_value_edits_use_dirty_closure_not_whole_all() {
    let mut engine = build_single_span_engine(1_000);
    let epoch = engine.formula_plane_indexes_epoch();

    engine
        .action_atomic_journal("bulk values".to_string(), edit_first_fifty_values)
        .unwrap();
    assert_eq!(engine.formula_plane_indexes_epoch(), epoch);

    engine.evaluate_all().unwrap();
    assert_eq!(engine.formula_plane_indexes_epoch(), epoch);
    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.span_eval_placement_count, 50, "{report:?}");
}

#[test]
fn undo_redo_of_value_bulk_uses_dirty_closure_not_whole_all() {
    let mut engine = build_single_span_engine(1_000);
    let mut undo = UndoEngine::new();

    let (_ret, journal) = engine
        .action_atomic_journal("bulk values".to_string(), edit_first_fifty_values)
        .unwrap();
    undo.push_action(journal);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        50
    );

    engine.undo_action(&mut undo).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        50
    );

    engine.redo_action(&mut undo).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine
            .last_formula_plane_span_eval_report()
            .unwrap()
            .span_eval_placement_count,
        50
    );
}

#[test]
fn per_cell_formula_write_demotion_dirties_only_true_closure() {
    let mut engine = build_two_span_engine(200);

    engine
        .action_atomic_journal("demote precise".to_string(), |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1000.0))?;
            tx.set_cell_formula("Sheet1", 100, 2, parse("=A100*5").unwrap())?;
            tx.set_cell_value("Sheet1", 200, 1, LiteralValue::Number(2000.0))?;
            Ok(())
        })
        .unwrap();

    let result = engine.evaluate_all().unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(result.computed_vertices, 6);
    assert_number_cell(&engine, 1, 2, 2000.0);
    assert_number_cell(&engine, 1, 3, 2001.0);
    assert_number_cell(&engine, 100, 2, 500.0);
    assert_number_cell(&engine, 100, 3, 501.0);
    assert_number_cell(&engine, 200, 2, 4000.0);
    assert_number_cell(&engine, 200, 3, 4001.0);
}

#[test]
fn per_cell_formula_write_demotion_correct_after_undo() {
    let mut engine = build_two_span_engine(200);
    let mut undo = UndoEngine::new();

    let (_ret, journal) = engine
        .action_atomic_journal("demote precise".to_string(), |tx| {
            tx.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1000.0))?;
            tx.set_cell_formula("Sheet1", 100, 2, parse("=A100*5").unwrap())?;
            tx.set_cell_value("Sheet1", 200, 1, LiteralValue::Number(2000.0))?;
            Ok(())
        })
        .unwrap();
    undo.push_action(journal);
    engine.evaluate_all().unwrap();

    engine.undo_action(&mut undo).unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices <= 6,
        "computed_vertices={}",
        result.computed_vertices
    );
    assert_number_cell(&engine, 1, 1, 1.0);
    assert_number_cell(&engine, 1, 2, 2.0);
    assert_number_cell(&engine, 1, 3, 3.0);
    assert_number_cell(&engine, 100, 2, 200.0);
    assert_number_cell(&engine, 100, 3, 201.0);
    assert_number_cell(&engine, 200, 1, 200.0);
    assert_number_cell(&engine, 200, 2, 400.0);
    assert_number_cell(&engine, 200, 3, 401.0);
}
