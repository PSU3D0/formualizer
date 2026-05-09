use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;

const SHEET: &str = "Sheet1";

fn auth_engine(enable_parallel: bool) -> Engine<TestWorkbook> {
    let config = EvalConfig::default()
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
        .with_parallel(enable_parallel);
    Engine::new(TestWorkbook::default(), config)
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
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .expect("ingest formulas");
}

fn numeric_value(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> f64 {
    match engine
        .get_cell_value(SHEET, row, col)
        .unwrap_or_else(|| panic!("missing {SHEET}!R{row}C{col}"))
    {
        LiteralValue::Int(value) => value as f64,
        LiteralValue::Number(value) => value,
        value => panic!("expected numeric {SHEET}!R{row}C{col}, got {value:?}"),
    }
}

fn build_a_plus_one_family(rows: u32, enable_parallel: bool) -> Engine<TestWorkbook> {
    let mut engine = auth_engine(enable_parallel);
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    ingest(&mut engine, formulas);
    engine
}

#[test]
fn parallel_per_placement_produces_identical_results_to_sequential() {
    let mut sequential = build_a_plus_one_family(1_000, false);
    sequential.evaluate_all().unwrap();

    let mut parallel = build_a_plus_one_family(1_000, true);
    parallel.evaluate_all().unwrap();

    for row in 1..=1_000 {
        assert_eq!(
            parallel.get_cell_value(SHEET, row, 2),
            sequential.get_cell_value(SHEET, row, 2),
            "mismatch at row {row}"
        );
    }
}

#[test]
fn parallel_below_threshold_uses_sequential_path() {
    let mut engine = build_a_plus_one_family(100, true);
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.parallel_per_placement_invocations, 0, "{report:?}");
    assert_eq!(report.sequential_per_placement_invocations, 1, "{report:?}");
    assert_eq!(report.span_eval_placement_count, 100, "{report:?}");
}

#[test]
fn parallel_above_threshold_uses_parallel_path() {
    let mut engine = build_a_plus_one_family(1_000, true);
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.parallel_per_placement_invocations, 1, "{report:?}");
    assert_eq!(report.sequential_per_placement_invocations, 0, "{report:?}");
    assert_eq!(report.span_eval_placement_count, 1_000, "{report:?}");
}

#[test]
fn parallel_disabled_via_config_uses_sequential() {
    let mut engine = build_a_plus_one_family(1_000, false);
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.parallel_per_placement_invocations, 0, "{report:?}");
    assert_eq!(report.sequential_per_placement_invocations, 1, "{report:?}");
    assert_eq!(report.span_eval_placement_count, 1_000, "{report:?}");
}

#[test]
fn parallel_with_lookup_cache_no_corruption() {
    let rows = 10_000;
    let mut engine = auth_engine(true);
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value(SHEET, row, 4, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value(SHEET, row, 5, LiteralValue::Number(row as f64 * 10.0 + 5.0))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            3,
            &format!("=VLOOKUP(A{row}, $D$1:$E${rows}, 2, FALSE)"),
        ));
    }
    ingest(&mut engine, formulas);
    engine.evaluate_all().unwrap();

    for row in 1..=rows {
        assert_eq!(numeric_value(&engine, row, 3), row as f64 * 10.0 + 5.0);
    }
    let span_report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(
        span_report.parallel_per_placement_invocations, 1,
        "{span_report:?}"
    );
    let cache_report = engine.last_lookup_index_cache_report();
    assert!(cache_report.builds > 0, "{cache_report:?}");
    assert!(cache_report.hits > 0, "{cache_report:?}");
}

#[test]
fn parallel_per_placement_with_per_placement_bindings() {
    let rows = 1_000;
    let mut engine = auth_engine(true);
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64 * 100.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+{row}")));
    }
    ingest(&mut engine, formulas);
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.parallel_per_placement_invocations, 1, "{report:?}");
    for row in 1..=rows {
        assert_eq!(numeric_value(&engine, row, 2), row as f64 * 101.0);
    }
}

#[test]
fn parallel_memoized_groups_correctly_broadcast() {
    let rows = 1_000;
    let groups = 100u32;
    let mut engine = auth_engine(true);
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        let key = if row <= 64 { row % 32 } else { row % groups };
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(key as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    ingest(&mut engine, formulas);
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.parallel_memoized_invocations, 1, "{report:?}");
    assert_eq!(report.sequential_memoized_invocations, 0, "{report:?}");
    assert_eq!(report.memo_eval_count, groups as u64, "{report:?}");
    assert_eq!(
        report.memo_broadcast_count,
        rows as u64 - groups as u64,
        "{report:?}"
    );
    for row in 1..=rows {
        let key = if row <= 64 { row % 32 } else { row % groups };
        assert_eq!(numeric_value(&engine, row, 2), key as f64 + 1.0);
    }
}

#[test]
fn parallel_short_circuit_correctness_under_parallelism() {
    let rows = 1_000;
    let mut engine = auth_engine(true);
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=IF(A{row}>0, 1, 1/0)"),
        ));
    }
    ingest(&mut engine, formulas);
    engine.evaluate_all().unwrap();

    let report = engine.last_formula_plane_span_eval_report().unwrap();
    assert_eq!(report.parallel_per_placement_invocations, 1, "{report:?}");
    for row in 1..=rows {
        assert_eq!(numeric_value(&engine, row, 2), 1.0);
    }
}
