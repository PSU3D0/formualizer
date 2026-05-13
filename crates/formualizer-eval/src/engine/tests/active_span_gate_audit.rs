use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::engine::{
    ChangeLog, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const TARGET_ROW: u32 = 100;
const TARGET_COL: u32 = 2;
const EDITED_INPUT: f64 = 500.0;
const EXPECTED_TARGET: f64 = EDITED_INPUT * 2.0;

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

pub(super) fn build_engine_with_active_spans() -> Engine<TestWorkbook> {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=200 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(formula_record(
            &mut engine,
            row,
            TARGET_COL,
            &format!("=A{row}*2"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .set_cell_value("Sheet1", TARGET_ROW, 1, LiteralValue::Number(EDITED_INPUT))
        .unwrap();
    engine
}

fn assert_active_spans(engine: &Engine<TestWorkbook>) {
    assert!(engine.graph.formula_authority().active_span_count() > 0);
}

fn assert_target_fresh(engine: &Engine<TestWorkbook>) {
    assert_eq!(
        engine.get_cell_value("Sheet1", TARGET_ROW, TARGET_COL),
        Some(LiteralValue::Number(EXPECTED_TARGET))
    );
}

#[test]
fn evaluate_all_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    engine.evaluate_all().unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn evaluate_all_with_delta_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    engine.evaluate_all_with_delta().unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn evaluate_all_cancellable_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    engine
        .evaluate_all_cancellable(Arc::new(AtomicBool::new(false)))
        .unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn evaluate_all_logged_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    let mut log = ChangeLog::new();
    assert_active_spans(&engine);

    engine.evaluate_all_logged(&mut log).unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn evaluate_cell_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    let value = engine
        .evaluate_cell("Sheet1", TARGET_ROW, TARGET_COL)
        .unwrap();

    assert_eq!(value, Some(LiteralValue::Number(EXPECTED_TARGET)));
    assert_target_fresh(&engine);
}

#[test]
fn evaluate_cells_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    let values = engine
        .evaluate_cells(&[("Sheet1", TARGET_ROW, TARGET_COL)])
        .unwrap();

    assert_eq!(values, vec![Some(LiteralValue::Number(EXPECTED_TARGET))]);
    assert_target_fresh(&engine);
}

#[test]
fn evaluate_cells_cancellable_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    let values = engine
        .evaluate_cells_cancellable(
            &[("Sheet1", TARGET_ROW, TARGET_COL)],
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();

    assert_eq!(values, vec![Some(LiteralValue::Number(EXPECTED_TARGET))]);
    assert_target_fresh(&engine);
}

#[test]
fn evaluate_cells_with_delta_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    let (values, _) = engine
        .evaluate_cells_with_delta(&[("Sheet1", TARGET_ROW, TARGET_COL)])
        .unwrap();

    assert_eq!(values, vec![Some(LiteralValue::Number(EXPECTED_TARGET))]);
    assert_target_fresh(&engine);
}

#[test]
fn evaluate_until_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    engine
        .evaluate_until(&[("Sheet1", TARGET_ROW, TARGET_COL)])
        .unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn evaluate_until_cancellable_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    assert_active_spans(&engine);

    engine
        .evaluate_until_cancellable(&["Sheet1!B100"], Arc::new(AtomicBool::new(false)))
        .unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn evaluate_recalc_plan_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    let plan = engine.build_recalc_plan().unwrap();
    assert_active_spans(&engine);

    engine.evaluate_recalc_plan(&plan).unwrap();

    assert_target_fresh(&engine);
}

#[test]
fn evaluate_vertex_flushes_active_spans() {
    let mut engine = build_engine_with_active_spans();
    let input_vertex = *engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", TARGET_ROW, 1))
        .expect("input vertex");
    assert_active_spans(&engine);

    let value = engine.evaluate_vertex(input_vertex).unwrap();

    assert_eq!(value, LiteralValue::Number(EDITED_INPUT));
    assert_target_fresh(&engine);
}
