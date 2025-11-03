use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::engine::arrow_ingest::ArrowBulkUpdateBuilder;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;

#[test]
fn bulk_update_sparse_and_dense_across_chunks() {
    // Engine with Arrow storage enabled
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());

    // Build Arrow sheet with 1 column, 400 rows, chunk_rows=200 → 2 chunks
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 1, 200);
        for _ in 0..400 {
            ab.append_row("S", &[LiteralValue::Empty]).unwrap();
        }
        let _ = ab.finish().unwrap();
    }

    // Bulk updates: sparse in chunk 0 (2 rows), dense in chunk 1 (10 rows)
    {
        let mut ub: ArrowBulkUpdateBuilder<'_, _> = engine.begin_bulk_update_arrow();
        // Sparse updates in chunk 0 (below 2% threshold => overlay path)
        ub.update_cell("S", 1, 1, LiteralValue::Number(1.0));
        ub.update_cell("S", 3, 1, LiteralValue::Number(3.0));

        // Dense contiguous updates in chunk 2 (rows 210..219)
        for r in 210..220 {
            ub.update_cell("S", r, 1, LiteralValue::Number(r as f64));
        }
        let total = ub.finish().unwrap();
        assert_eq!(total, 12);
    }

    // Validate overlay len for chunk 0 is 2; chunk 1 is rebuilt (overlay cleared)
    let sheet = engine.sheet_store().sheet("S").unwrap();
    assert_eq!(sheet.columns[0].chunks.len(), 2);
    let ch0 = &sheet.columns[0].chunks[0];
    let ch1 = &sheet.columns[0].chunks[1];
    assert_eq!(ch0.overlay.len(), 2);
    assert_eq!(ch1.overlay.len(), 0);

    // Validate values via ArrowRangeView
    let av = sheet.range_view(0, 0, (sheet.nrows - 1) as usize, 0);
    assert_eq!(av.get_cell(0, 0), LiteralValue::Number(1.0)); // row1 (0-based) updated to 1.0
    assert_eq!(av.get_cell(2, 0), LiteralValue::Number(3.0)); // row3 → 3.0
    // Dense region in chunk 1
    for (i, r) in (210..220).enumerate() {
        assert_eq!(
            av.get_cell((r - 1) as usize, 0),
            LiteralValue::Number(r as f64)
        );
    }
}

#[test]
fn bulk_update_contiguous_range_triggers_rebuild() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());

    // Build Arrow sheet with 1 column, 200 rows (single chunk)
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 1, 200);
        for _ in 0..200 {
            ab.append_row("S", &[LiteralValue::Empty]).unwrap();
        }
        let _ = ab.finish().unwrap();
    }

    // Contiguous range of 10 updates (>2% of 200 => rebuild)
    {
        let mut ub = engine.begin_bulk_update_arrow();
        for r in 50..60 {
            ub.update_cell("S", r, 1, LiteralValue::Number((r * 2) as f64));
        }
        ub.finish().unwrap();
    }

    let sheet = engine.sheet_store().sheet("S").unwrap();
    assert_eq!(sheet.columns[0].chunks.len(), 1);
    let ch = &sheet.columns[0].chunks[0];
    // Overlay should be cleared after rebuild
    assert_eq!(ch.overlay.len(), 0);

    // Values present in base lane
    let av = sheet.range_view(0, 0, (sheet.nrows - 1) as usize, 0);
    for r in 50..60 {
        assert_eq!(
            av.get_cell((r - 1) as usize, 0),
            LiteralValue::Number((r * 2) as f64)
        );
    }
}

#[test]
fn bulk_update_noncontiguous_dense_triggers_rebuild_varied_chunk2() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());

    // Build Arrow sheet with 1 column, 128 rows, chunk_rows=64 → two chunks
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 1, 64);
        for _ in 0..128 {
            ab.append_row("S", &[LiteralValue::Empty]).unwrap();
        }
        let _ = ab.finish().unwrap();
    }
    // Threshold len/50 = 1; two updates in the same chunk should trigger rebuild even if non-contiguous
    {
        let mut ub = engine.begin_bulk_update_arrow();
        // Two updates in first chunk at non-adjacent rows
        ub.update_cell("S", 10, 1, LiteralValue::Number(10.0));
        ub.update_cell("S", 60, 1, LiteralValue::Number(60.0));
        ub.finish().unwrap();
    }
    let sheet = engine.sheet_store().sheet("S").unwrap();
    let ch0 = &sheet.columns[0].chunks[0];
    assert_eq!(
        ch0.overlay.len(),
        0,
        "dense non-contiguous should rebuild chunk 0"
    );
    let av = sheet.range_view(0, 0, 127, 0);
    assert_eq!(av.get_cell(9, 0), LiteralValue::Number(10.0));
    assert_eq!(av.get_cell(59, 0), LiteralValue::Number(60.0));
}

#[test]
fn bulk_update_noncontiguous_dense_triggers_rebuild_varied_chunk() {
    let mut engine = Engine::new(TestWorkbook::default(), arrow_eval_config());

    // Build Arrow sheet with 1 column, 128 rows, chunk_rows=64 → two chunks
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 1, 64);
        for _ in 0..128 {
            ab.append_row("S", &[LiteralValue::Empty]).unwrap();
        }
        let _ = ab.finish().unwrap();
    }
    // Threshold len/50 = 1; two updates in the same chunk should trigger rebuild even if non-contiguous
    {
        let mut ub = engine.begin_bulk_update_arrow();
        // Two updates in first chunk at non-adjacent rows
        ub.update_cell("S", 10, 1, LiteralValue::Number(10.0));
        ub.update_cell("S", 60, 1, LiteralValue::Number(60.0));
        ub.finish().unwrap();
    }
    let sheet = engine.sheet_store().sheet("S").unwrap();
    let ch0 = &sheet.columns[0].chunks[0];
    assert_eq!(
        ch0.overlay.len(),
        0,
        "dense non-contiguous should rebuild chunk 0"
    );
    let av = sheet.range_view(0, 0, 127, 0);
    assert_eq!(av.get_cell(9, 0), LiteralValue::Number(10.0));
    assert_eq!(av.get_cell(59, 0), LiteralValue::Number(60.0));
}
