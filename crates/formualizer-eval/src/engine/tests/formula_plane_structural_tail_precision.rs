use std::sync::Arc;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

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
    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn active_span_count(engine: &Engine<TestWorkbook>) -> usize {
    engine.baseline_stats().formula_plane_active_span_count
}

fn assert_number(engine: &Engine<TestWorkbook>, row: u32, col: u32, expected: f64) {
    assert_eq!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Number(expected))
    );
}

fn build_column_family(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity((rows * 5) as usize);
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        for col in 7..=11 {
            engine
                .set_cell_value(
                    "Sheet1",
                    row,
                    col,
                    LiteralValue::Number((row * 100 + col) as f64),
                )
                .unwrap();
        }
        for (idx, col) in (2..=6).enumerate() {
            let addend = idx + 1;
            formulas.push(record(&mut engine, row, col, &format!("=A{row}+{addend}")));
        }
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(active_span_count(&engine), 5);
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn column_delete_outside_span_region_with_dirty_closure_no_recompute() {
    let mut engine = build_column_family(1000);
    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 123, col, 123.0 + f64::from(col - 1));
    }

    engine.delete_columns("Sheet1", 7, 1).unwrap();

    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 123, col, 123.0 + f64::from(col - 1));
    }
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 0, "result={result:?}");
    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 987, col, 987.0 + f64::from(col - 1));
    }
}

#[test]
fn column_insert_outside_span_region_with_dirty_closure_no_recompute() {
    let mut engine = build_column_family(1000);
    assert_eq!(active_span_count(&engine), 5);

    engine.insert_columns("Sheet1", 7, 1).unwrap();

    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 321, col, 321.0 + f64::from(col - 1));
    }
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 0, "result={result:?}");
    assert_eq!(active_span_count(&engine), 5);
    for col in 2..=6 {
        assert_number(&engine, 654, col, 654.0 + f64::from(col - 1));
    }
}
