use super::common::arrow_eval_config;
use crate::engine::{Engine, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use std::sync::Arc;

fn off_axis_cache_config() -> crate::engine::EvalConfig {
    let mut cfg = arrow_eval_config();
    cfg.formula_plane_mode = FormulaPlaneMode::Off;
    cfg.enable_parallel = false;
    cfg
}

fn ingest_repeated_formula(
    engine: &mut Engine<TestWorkbook>,
    sheet: &str,
    formula: &str,
    cells: impl IntoIterator<Item = (u32, u32)>,
) {
    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    let source = Arc::<str>::from(formula);
    let records = cells
        .into_iter()
        .map(|(row, col)| FormulaIngestRecord::new(row, col, ast_id, Some(source.clone())))
        .collect();
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(sheet, records)])
        .unwrap();
}

#[test]
fn used_row_bounds_cache_parity_and_edit_invalidation() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg.clone());

    let sheet = "S";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 2, 4);
        for _ in 0..10 {
            ab.append_row(sheet, &[LiteralValue::Empty, LiteralValue::Empty])
                .unwrap();
        }
        ab.finish().unwrap();
    }
    // Bounds should be None initially for empty columns
    assert_eq!(engine.used_rows_for_columns(sheet, 1, 1), None);

    // Set a value at row 5 in col 1
    engine
        .set_cell_value(sheet, 5, 1, LiteralValue::Int(1))
        .unwrap();
    let b1 = engine.used_rows_for_columns(sheet, 1, 1).unwrap();
    assert_eq!(b1, (5, 5));

    // Second call hits cache; same result
    let b2 = engine.used_rows_for_columns(sheet, 1, 1).unwrap();
    assert_eq!(b2, (5, 5));

    // Edit extends used region to row 8; should invalidate via snapshot and update
    engine
        .set_cell_value(sheet, 8, 1, LiteralValue::Int(2))
        .unwrap();
    let b3 = engine.used_rows_for_columns(sheet, 1, 1).unwrap();
    assert_eq!(b3, (5, 8));
}

#[test]
fn used_row_bounds_cache_compaction_invalidation() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg.clone());
    let sheet = "S2";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 8);
        for _ in 0..64 {
            ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        }
        ab.finish().unwrap();
    }
    // Write two values in same chunk to trigger compaction (heuristic mirrors overlay test)
    engine
        .set_cell_value(sheet, 1, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value(sheet, 2, 1, LiteralValue::Int(2))
        .unwrap();
    // After compaction, bounds should be (1,2)
    let b = engine.used_rows_for_columns(sheet, 1, 1).unwrap();
    assert_eq!(b, (1, 2));
}

#[test]
fn used_row_bounds_snapshot_change_midpass() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg.clone());
    let sheet = "S3";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 4);
        for _ in 0..8 {
            ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        }
        ab.finish().unwrap();
    }
    // First bounds None
    assert_eq!(engine.used_rows_for_columns(sheet, 1, 1), None);
    // Compute once (cached as None is represented by no entry)
    let _ = engine.used_rows_for_columns(sheet, 1, 1);
    // Change snapshot by edit and then re-check
    engine
        .set_cell_value(sheet, 7, 1, LiteralValue::Int(1))
        .unwrap();
    assert_eq!(engine.used_rows_for_columns(sheet, 1, 1), Some((7, 7)));
}

#[test]
fn used_rows_for_columns_caches_final_result_across_repeated_calls() {
    let cfg = off_axis_cache_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 2, 1024);
        for row in 1..=10_000 {
            ab.append_row(
                "Sheet1",
                &[LiteralValue::Int(row as i64), LiteralValue::Empty],
            )
            .unwrap();
        }
        ab.finish().unwrap();
    }
    ingest_repeated_formula(
        &mut engine,
        "Sheet1",
        "=1+2",
        (1..=10_000).map(|row| (row, 2)),
    );

    assert_eq!(
        engine.used_rows_for_columns("Sheet1", 1, 1),
        Some((1, 10_000))
    );
    assert_eq!(
        engine.used_rows_for_columns("Sheet1", 1, 1),
        Some((1, 10_000))
    );

    let (row_hits, row_misses, _, _) = engine.used_axis_bounds_cache_stats();
    assert_eq!(row_misses, 1);
    assert_eq!(row_hits, 1);
}

#[test]
fn used_rows_for_columns_caches_none_for_empty_column() {
    let cfg = off_axis_cache_config();
    let engine = Engine::new(TestWorkbook::new(), cfg);

    assert_eq!(engine.used_rows_for_columns("Sheet1", 3, 3), None);
    assert_eq!(engine.used_rows_for_columns("Sheet1", 3, 3), None);

    let (row_hits, row_misses, _, _) = engine.used_axis_bounds_cache_stats();
    assert_eq!(row_misses, 1);
    assert_eq!(row_hits, 1);
}

#[test]
fn used_rows_for_columns_invalidates_on_data_edit() {
    let cfg = off_axis_cache_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    for row in 1..=5 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Int(row as i64))
            .unwrap();
    }

    assert_eq!(engine.used_rows_for_columns("Sheet1", 1, 1), Some((1, 5)));
    engine
        .set_cell_value("Sheet1", 8, 1, LiteralValue::Int(8))
        .unwrap();
    assert_eq!(engine.used_rows_for_columns("Sheet1", 1, 1), Some((1, 8)));
    assert_eq!(engine.used_rows_for_columns("Sheet1", 1, 1), Some((1, 8)));

    let (row_hits, row_misses, _, _) = engine.used_axis_bounds_cache_stats();
    assert_eq!(row_misses, 2);
    assert_eq!(row_hits, 1);
}

#[test]
fn used_rows_for_columns_includes_formula_rows_in_union() {
    let cfg = off_axis_cache_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    for row in 1..=5 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Int(row as i64))
            .unwrap();
    }
    ingest_repeated_formula(&mut engine, "Sheet1", "=1+2", [(10, 1)]);

    assert_eq!(engine.used_rows_for_columns("Sheet1", 1, 1), Some((1, 10)));
    assert_eq!(engine.used_rows_for_columns("Sheet1", 1, 1), Some((1, 10)));

    let (row_hits, row_misses, _, _) = engine.used_axis_bounds_cache_stats();
    assert_eq!(row_misses, 1);
    assert_eq!(row_hits, 1);
}

#[test]
fn used_cols_for_rows_caches_final_result() {
    let cfg = off_axis_cache_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    let sheet = "CacheCols";

    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 10_000, 1024);
        let row: Vec<_> = (1..=10_000)
            .map(|col| LiteralValue::Int(col as i64))
            .collect();
        ab.append_row(sheet, &row).unwrap();
        ab.finish().unwrap();
    }
    ingest_repeated_formula(&mut engine, sheet, "=1+2", (1..=10_000).map(|col| (2, col)));

    assert_eq!(engine.used_cols_for_rows(sheet, 1, 1), Some((1, 10_000)));
    assert_eq!(engine.used_cols_for_rows(sheet, 1, 1), Some((1, 10_000)));

    let (_, _, col_hits, col_misses) = engine.used_axis_bounds_cache_stats();
    assert_eq!(col_misses, 1);
    assert_eq!(col_hits, 1);
}

#[test]
fn used_cols_for_rows_invalidates_on_data_edit() {
    let cfg = off_axis_cache_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    for col in 1..=5 {
        engine
            .set_cell_value("Sheet1", 1, col, LiteralValue::Int(col as i64))
            .unwrap();
    }

    assert_eq!(engine.used_cols_for_rows("Sheet1", 1, 1), Some((1, 5)));
    engine
        .set_cell_value("Sheet1", 1, 8, LiteralValue::Int(8))
        .unwrap();
    assert_eq!(engine.used_cols_for_rows("Sheet1", 1, 1), Some((1, 8)));
    assert_eq!(engine.used_cols_for_rows("Sheet1", 1, 1), Some((1, 8)));

    let (_, _, col_hits, col_misses) = engine.used_axis_bounds_cache_stats();
    assert_eq!(col_misses, 2);
    assert_eq!(col_hits, 1);
}

#[test]
fn evaluate_whole_column_sum_uses_cached_bounds() {
    let cfg = off_axis_cache_config();
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Int(row as i64))
            .unwrap();
    }
    ingest_repeated_formula(
        &mut engine,
        "Sheet1",
        "=SUM($A:$A)",
        (1..=100).map(|row| (row, 2)),
    );

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(5050.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 100, 2),
        Some(LiteralValue::Number(5050.0))
    );

    let (row_hits_after_first_eval, row_misses_after_first_eval, _, _) =
        engine.used_axis_bounds_cache_stats();
    assert!(row_hits_after_first_eval > 0);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(101))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(5150.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 100, 2),
        Some(LiteralValue::Number(5150.0))
    );

    let (_, row_misses_after_edit, _, _) = engine.used_axis_bounds_cache_stats();
    assert!(row_misses_after_edit > row_misses_after_first_eval);
}
