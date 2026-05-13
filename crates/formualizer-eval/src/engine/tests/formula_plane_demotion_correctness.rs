use std::sync::Arc;

use crate::engine::{
    ChangeLog, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
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

fn build_single_span_column() -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=200 {
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

fn assert_number_cell(
    engine: &Engine<TestWorkbook>,
    sheet: &str,
    row: u32,
    col: u32,
    expected: f64,
) {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(actual)) => assert_eq!(actual, expected),
        Some(LiteralValue::Int(actual)) => assert_eq!(actual as f64, expected),
        other => panic!("expected {sheet}!R{row}C{col} to be {expected}, got {other:?}"),
    }
}

#[test]
fn set_formula_inside_active_span_demotes_and_evaluates_correctly() {
    let mut engine = build_single_span_column();

    engine
        .set_cell_formula("Sheet1", 100, 2, parse("=A100*5").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_number_cell(&engine, "Sheet1", 100, 2, 500.0);
    assert!(engine.baseline_stats().formula_plane_active_span_count <= 1);
}

#[test]
fn set_formula_inside_active_span_via_action_demotes_and_evaluates_correctly() {
    let mut engine = build_single_span_column();
    let mut log = ChangeLog::new();

    engine
        .action_with_logger(&mut log, "test", |action| {
            action.set_cell_formula("Sheet1", 100, 2, parse("=A100*5").unwrap())
        })
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_number_cell(&engine, "Sheet1", 100, 2, 500.0);
    assert!(engine.baseline_stats().formula_plane_active_span_count <= 1);
}

#[test]
fn set_value_inside_active_span_demotes_and_uses_user_value() {
    let mut engine = build_single_span_column();

    engine
        .set_cell_value("Sheet1", 100, 2, LiteralValue::Number(999.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_number_cell(&engine, "Sheet1", 100, 2, 999.0);
    assert!(engine.baseline_stats().formula_plane_active_span_count <= 1);
}

#[test]
fn bulk_set_formulas_inside_active_span_demotes_once() {
    let mut engine = build_single_span_column();

    engine
        .bulk_set_formulas(
            "Sheet1",
            vec![
                (50, 2, parse("=A50*5").unwrap()),
                (100, 2, parse("=A100*6").unwrap()),
                (150, 2, parse("=A150*7").unwrap()),
            ],
        )
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_number_cell(&engine, "Sheet1", 50, 2, 250.0);
    assert_number_cell(&engine, "Sheet1", 100, 2, 600.0);
    assert_number_cell(&engine, "Sheet1", 150, 2, 1050.0);
}

#[test]
fn sheet_remove_then_add_with_cross_sheet_formulas_recomputes_correctly() {
    let mut engine = authoritative_engine();
    let aux_id = engine.add_sheet("Aux").unwrap();
    let mut formulas = Vec::new();

    for row in 1..=200 {
        engine
            .set_cell_value("Aux", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(formula_record(
            &mut engine,
            row,
            1,
            &format!("=IFERROR(Aux!A{row}*2, -1)"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    assert_number_cell(&engine, "Sheet1", 50, 1, 100.0);

    engine.remove_sheet(aux_id).unwrap();
    engine.evaluate_all().unwrap();
    assert_number_cell(&engine, "Sheet1", 50, 1, -1.0);

    engine.add_sheet("Aux").unwrap();
    for row in 1..=200 {
        engine
            .set_cell_value("Aux", row, 1, LiteralValue::Number((row + 10) as f64))
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    assert_number_cell(&engine, "Sheet1", 50, 1, 120.0);
}

#[test]
fn sheet_add_with_no_orphans_does_not_demote_unrelated_spans() {
    let mut engine = build_single_span_column();
    assert!(engine.baseline_stats().formula_plane_active_span_count >= 1);

    engine.add_sheet("Newcomer").unwrap();
    engine.evaluate_all().unwrap();

    assert_number_cell(&engine, "Sheet1", 50, 2, 100.0);
    assert_number_cell(&engine, "Sheet1", 100, 2, 200.0);
    assert_number_cell(&engine, "Sheet1", 150, 2, 300.0);
}
