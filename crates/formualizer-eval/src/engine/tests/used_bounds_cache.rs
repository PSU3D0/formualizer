use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::LiteralValue;

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
