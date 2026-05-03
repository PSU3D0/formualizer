use super::common::arrow_eval_config;
use crate::arrow_store::{OverlayFragment, OverlayValue};
use crate::engine::eval::{
    ComputedWrite, ComputedWriteBuffer, ComputedWriteChunkPlanShape, Engine,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn computed_write_buffer_records_sequence_order() {
    let mut buffer = ComputedWriteBuffer::default();

    buffer.push_cell(0, 0, 0, OverlayValue::Number(1.0));
    buffer.push_cell(0, 0, 1, OverlayValue::Text("x".into()));
    buffer.push_rect(
        0,
        1,
        0,
        vec![vec![OverlayValue::Boolean(true), OverlayValue::Empty]],
    );

    assert_eq!(buffer.len(), 3);
    let seqs: Vec<u64> = buffer.writes().iter().map(ComputedWrite::seq).collect();
    assert_eq!(seqs, vec![0, 1, 2]);
    assert!(buffer.estimated_bytes() > 0);
}

#[test]
fn computed_write_coalescing_plan_groups_sorts_and_applies_last_write_wins() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 2, 2);
        for _ in 0..4 {
            ab.append_row(sheet, &[LiteralValue::Empty, LiteralValue::Empty])
                .unwrap();
        }
        ab.finish().unwrap();
    }
    let sheet_id = engine.sheet_id(sheet).unwrap();
    let mut buffer = ComputedWriteBuffer::default();

    buffer.push_cell(sheet_id, 3, 0, OverlayValue::Number(30.0));
    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Number(10.0));
    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Number(11.0));
    buffer.push_rect(
        sheet_id,
        1,
        0,
        vec![
            vec![OverlayValue::Number(20.0), OverlayValue::Text("a".into())],
            vec![OverlayValue::Number(30.0), OverlayValue::Text("b".into())],
        ],
    );

    let plan = engine.debug_plan_computed_write_coalescing(&buffer);
    assert!(!plan.is_empty());
    assert_eq!(plan.input_cells, 7);
    assert_eq!(plan.coalesced_cells, 6);
    assert_eq!(plan.overwritten_cells, 1);
    assert_eq!(
        buffer.len(),
        4,
        "debug planning must not consume the buffer"
    );

    let c0_ch0 = plan
        .chunks
        .iter()
        .find(|chunk| chunk.col0 == 0 && chunk.chunk_idx == 0)
        .expect("column 0 chunk 0");
    assert_eq!(c0_ch0.chunk_start_row0, 0);
    assert_eq!(
        c0_ch0.shape,
        ComputedWriteChunkPlanShape::DenseRange { start: 0, len: 2 }
    );
    assert_eq!(c0_ch0.entries.len(), 2);
    assert_eq!(c0_ch0.entries[0].row_in_chunk, 0);
    assert_eq!(c0_ch0.entries[0].seq, 2);
    assert_eq!(c0_ch0.entries[0].value, OverlayValue::Number(11.0));
    assert_eq!(c0_ch0.entries[1].row_in_chunk, 1);
    assert_eq!(c0_ch0.entries[1].seq, 3);
    assert_eq!(c0_ch0.entries[1].value, OverlayValue::Number(20.0));

    let c0_ch1 = plan
        .chunks
        .iter()
        .find(|chunk| chunk.col0 == 0 && chunk.chunk_idx == 1)
        .expect("column 0 chunk 1");
    assert_eq!(c0_ch1.chunk_start_row0, 2);
    assert_eq!(
        c0_ch1.shape,
        ComputedWriteChunkPlanShape::RunRange {
            start: 0,
            len: 2,
            runs: 1,
        }
    );
    assert_eq!(c0_ch1.entries[0].row_in_chunk, 0);
    assert_eq!(c0_ch1.entries[1].row_in_chunk, 1);

    let c1_ch0 = plan
        .chunks
        .iter()
        .find(|chunk| chunk.col0 == 1 && chunk.chunk_idx == 0)
        .expect("column 1 chunk 0");
    assert_eq!(c1_ch0.entries[0].row_in_chunk, 1);
    assert_eq!(c1_ch0.shape, ComputedWriteChunkPlanShape::Point);

    let c1_ch1 = plan
        .chunks
        .iter()
        .find(|chunk| chunk.col0 == 1 && chunk.chunk_idx == 1)
        .expect("column 1 chunk 1");
    assert_eq!(c1_ch1.entries[0].row_in_chunk, 0);
    assert_eq!(c1_ch1.shape, ComputedWriteChunkPlanShape::Point);
}

#[test]
fn computed_write_coalescing_plan_preserves_sparse_gaps_and_empty_values() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();

    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Empty);
    buffer.push_cell(sheet_id, 2, 0, OverlayValue::Empty);

    let plan = engine.debug_plan_computed_write_coalescing(&buffer);
    assert_eq!(plan.input_cells, 2);
    assert_eq!(plan.coalesced_cells, 2);
    assert_eq!(plan.overwritten_cells, 0);
    assert_eq!(plan.chunks.len(), 1);

    let chunk = &plan.chunks[0];
    assert_eq!(chunk.col0, 0);
    assert_eq!(chunk.chunk_idx, 0);
    assert_eq!(chunk.entries[0].row_in_chunk, 0);
    assert_eq!(chunk.entries[0].value, OverlayValue::Empty);
    assert_eq!(chunk.entries[1].row_in_chunk, 2);
    assert_eq!(chunk.entries[1].value, OverlayValue::Empty);
    assert_eq!(
        chunk.shape,
        ComputedWriteChunkPlanShape::SparseOffsets {
            entries: 2,
            span_len: 3,
        }
    );
}

#[test]
fn coalesced_flush_lww_matches_legacy_point_flush() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();

    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Number(7.0));
    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Number(9.0));

    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    assert!(buffer.is_empty());
    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, in_off) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.points, 1);
    assert_eq!(stats.sparse_fragments, 0);
    assert_eq!(stats.dense_fragments, 0);
    assert_eq!(stats.run_fragments, 0);
    assert_eq!(in_off, 0);
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Number(9.0));
}

#[test]
fn coalesced_flush_rect_expansion_matches_legacy() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();

    buffer.push_rect(
        sheet_id,
        1,
        2,
        vec![
            vec![OverlayValue::Number(1.0), OverlayValue::Number(2.0)],
            vec![OverlayValue::Text("a".into()), OverlayValue::Empty],
        ],
    );

    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    assert_eq!(asheet.get_cell_value(1, 2), LiteralValue::Number(1.0));
    assert_eq!(asheet.get_cell_value(1, 3), LiteralValue::Number(2.0));
    assert_eq!(asheet.get_cell_value(2, 2), LiteralValue::Text("a".into()));
    assert_eq!(asheet.get_cell_value(2, 3), LiteralValue::Empty);
}

#[test]
fn coalesced_flush_empty_masks_base() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 8);
        ab.append_row(sheet, &[LiteralValue::Number(42.0)]).unwrap();
        ab.finish().unwrap();
    }
    let sheet_id = engine.sheet_id(sheet).unwrap();
    let mut buffer = ComputedWriteBuffer::default();

    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Empty);
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Empty);
}

#[test]
fn coalesced_flush_sparse_gaps_do_not_fill_base() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 8);
        ab.append_row(sheet, &[LiteralValue::Number(1.0)]).unwrap();
        ab.append_row(sheet, &[LiteralValue::Number(2.0)]).unwrap();
        ab.append_row(sheet, &[LiteralValue::Number(3.0)]).unwrap();
        ab.finish().unwrap();
    }
    let sheet_id = engine.sheet_id(sheet).unwrap();
    let mut buffer = ComputedWriteBuffer::default();

    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Number(10.0));
    buffer.push_cell(sheet_id, 2, 0, OverlayValue::Empty);
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Number(10.0));
    assert_eq!(asheet.get_cell_value(1, 0), LiteralValue::Number(2.0));
    assert_eq!(asheet.get_cell_value(2, 0), LiteralValue::Empty);
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.points, 2);
    assert_eq!(stats.covered_len, 2);
}

#[test]
fn coalesced_flush_cap_zero_still_compacts_safely() {
    let mut cfg = arrow_eval_config();
    cfg.max_overlay_memory_bytes = Some(0);
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();

    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Number(7.0));
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    assert!(buffer.is_empty());
    assert_eq!(engine.overlay_memory_usage(), 0);
    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Number(7.0));
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.covered_len, 0);
}

#[test]
fn computed_flush_sequential_scalar_layer_flushes_before_return() {
    let mut cfg = arrow_eval_config();
    cfg.enable_parallel = false;
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1+2").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    let asheet = engine.sheet_store().sheet("Sheet1").expect("arrow sheet");
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Number(3.0));
}

#[test]
fn computed_flush_parallel_scalar_group_flushes_before_return() {
    let mut cfg = arrow_eval_config();
    cfg.enable_parallel = true;
    cfg.max_threads = Some(4);
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    for row in 1..=32 {
        engine
            .set_cell_formula("Sheet1", row, 1, parse("=ROW()").unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    let asheet = engine.sheet_store().sheet("Sheet1").expect("arrow sheet");
    for row0 in 0..32 {
        assert_eq!(
            asheet.get_cell_value(row0, 0),
            LiteralValue::Number((row0 + 1) as f64),
            "row {}",
            row0 + 1
        );
    }
}

#[test]
fn user_edit_removes_same_cell_computed_fragment_before_compaction() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";

    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 8);
        ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        ab.finish().unwrap();
    }

    {
        let asheet = engine.sheet_store_mut().sheet_mut(sheet).unwrap();
        let (ch_i, off) = asheet.chunk_of_row(0).unwrap();
        asheet.columns[0].chunks[ch_i]
            .computed_overlay
            .apply_fragment(
                OverlayFragment::run_range(0, vec![OverlayValue::Number(42.0)]).unwrap(),
            );
        assert_eq!(
            asheet.columns[0].chunks[ch_i]
                .computed_overlay
                .get_scalar(off)
                .unwrap()
                .to_literal(),
            LiteralValue::Number(42.0)
        );
    }

    assert!(engine.debug_recompute_computed_overlay_bytes() > 0);

    engine
        .set_cell_value(sheet, 1, 1, LiteralValue::Number(9.0))
        .unwrap();

    let asheet = engine.sheet_store().sheet(sheet).unwrap();
    let (ch_i, off) = asheet.chunk_of_row(0).unwrap();
    assert!(
        asheet.columns[0].chunks[ch_i]
            .computed_overlay
            .get_scalar(off)
            .is_none(),
        "user edit should remove same-cell computed fragment before user overlay compaction"
    );
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Number(9.0));
    assert_eq!(engine.overlay_memory_usage(), 0);
}
