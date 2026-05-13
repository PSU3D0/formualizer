use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::formula_plane::span_eval::{
    dirty_placement_vec_materialization_count, relocatable_validation_walk_count,
    reset_span_eval_test_counters,
};
use crate::test_workbook::TestWorkbook;

fn authoritative_engine() -> Engine<TestWorkbook> {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    Engine::new(TestWorkbook::default(), cfg)
}

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn ingest(engine: &mut Engine<TestWorkbook>, formulas: Vec<FormulaIngestRecord>) {
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
}

#[test]
fn formula_plane_evaluate_all_handles_many_same_sheet_spans() {
    let mut engine = authoritative_engine();
    let rows = 10u32;
    let span_count = 100u32;
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();

    for col in 2..(2 + span_count) {
        let mut formulas = Vec::new();
        for row in 1..=rows {
            formulas.push(record(&mut engine, row, col, &format!("=$A$1+{col}")));
        }
        ingest(&mut engine, formulas);
    }

    assert_eq!(
        engine.baseline_stats().formula_plane_active_span_count,
        span_count as usize
    );
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 101),
        Some(LiteralValue::Number(102.0))
    );
}

#[test]
fn formula_plane_relocatable_validation_is_cached_per_template() {
    let mut engine = authoritative_engine();
    let rows = 128u32;
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    ingest(&mut engine, formulas);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    reset_span_eval_test_counters();
    engine.evaluate_all().unwrap();
    assert_eq!(relocatable_validation_walk_count(), 1);

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(50.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(relocatable_validation_walk_count(), 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(51.0))
    );
}

#[test]
fn formula_plane_whole_span_dirty_does_not_materialize_dirty_placement_vec() {
    let mut engine = authoritative_engine();
    let rows = 256u32;
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}*2")));
    }
    ingest(&mut engine, formulas);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    reset_span_eval_test_counters();
    engine.evaluate_all().unwrap();

    assert_eq!(dirty_placement_vec_materialization_count(), 0);
    assert_eq!(
        engine.get_cell_value("Sheet1", 256, 2),
        Some(LiteralValue::Number(512.0))
    );
}
