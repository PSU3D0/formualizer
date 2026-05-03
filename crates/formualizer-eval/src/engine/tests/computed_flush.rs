use super::common::arrow_eval_config;
use crate::arrow_store::{
    ArrowSheet, OverlayDebugStats, OverlayFragment, OverlaySelectStats, OverlayValue,
    reset_overlay_select_stats, snapshot_overlay_select_stats,
};
use crate::engine::eval::{
    ComputedWrite, ComputedWriteBuffer, ComputedWriteChunkPlanShape, Engine,
};
use crate::test_workbook::TestWorkbook;
use arrow_array::Array;
use formualizer_common::LiteralValue;
use formualizer_parse::ASTNode;
use formualizer_parse::parser::parse;

#[derive(Debug, Clone, Copy)]
enum Phase5ComputedFlushProbeFixture {
    ConstantCopied,
    RowVarying,
    AlternatingMostlyVaried,
    SparseEveryOther,
    CapZeroConstant,
}

impl Phase5ComputedFlushProbeFixture {
    fn name(self) -> &'static str {
        match self {
            Phase5ComputedFlushProbeFixture::ConstantCopied => "constant_copied",
            Phase5ComputedFlushProbeFixture::RowVarying => "row_varying",
            Phase5ComputedFlushProbeFixture::AlternatingMostlyVaried => "alternating_mostly_varied",
            Phase5ComputedFlushProbeFixture::SparseEveryOther => "sparse_every_other",
            Phase5ComputedFlushProbeFixture::CapZeroConstant => "cap_zero_constant",
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct Phase5ComputedFlushProbeOp {
    ms: f64,
    segments: usize,
    arrays: usize,
    rows_scanned: usize,
    checksum: f64,
    non_null: usize,
}

#[derive(Debug, serde::Serialize)]
struct Phase5ComputedFlushProbeOverlayStats {
    points: usize,
    sparse_fragments: usize,
    dense_fragments: usize,
    run_fragments: usize,
    covered_len: usize,
}

#[derive(Debug, serde::Serialize)]
struct Phase5ComputedFlushProbeRow {
    fixture: &'static str,
    rows: usize,
    formulas: usize,
    chunk_rows: usize,
    eval_ms: f64,
    overlay_memory_usage: usize,
    computed_overlay_estimated_bytes: usize,
    overlay_stats: Phase5ComputedFlushProbeOverlayStats,
    numbers: Phase5ComputedFlushProbeOp,
    type_tags: Phase5ComputedFlushProbeOp,
    lowered_text: Phase5ComputedFlushProbeOp,
    select_stats: OverlaySelectStats,
}

fn phase5_probe_overlay_stats(sheet: &ArrowSheet, col_idx: usize) -> OverlayDebugStats {
    let mut total = OverlayDebugStats::default();
    let Some(column) = sheet.columns.get(col_idx) else {
        return total;
    };
    for chunk in &column.chunks {
        let stats = chunk.computed_overlay.debug_stats();
        total.points += stats.points;
        total.sparse_fragments += stats.sparse_fragments;
        total.dense_fragments += stats.dense_fragments;
        total.run_fragments += stats.run_fragments;
        total.covered_len += stats.covered_len;
    }
    for chunk in column.sparse_chunks.values() {
        let stats = chunk.computed_overlay.debug_stats();
        total.points += stats.points;
        total.sparse_fragments += stats.sparse_fragments;
        total.dense_fragments += stats.dense_fragments;
        total.run_fragments += stats.run_fragments;
        total.covered_len += stats.covered_len;
    }
    total
}

fn phase5_probe_computed_overlay_estimated_bytes(sheet: &ArrowSheet, col_idx: usize) -> usize {
    let Some(column) = sheet.columns.get(col_idx) else {
        return 0;
    };
    column
        .chunks
        .iter()
        .map(|chunk| chunk.computed_overlay.estimated_bytes())
        .chain(
            column
                .sparse_chunks
                .values()
                .map(|chunk| chunk.computed_overlay.estimated_bytes()),
        )
        .fold(0usize, usize::saturating_add)
}

fn phase5_seed_base_rows(
    engine: &mut Engine<TestWorkbook>,
    sheet: &str,
    rows: usize,
    chunk_rows: usize,
) {
    let mut ab = engine.begin_bulk_ingest_arrow();
    ab.add_sheet(sheet, 1, chunk_rows.max(1));
    for _ in 0..rows.max(1) {
        ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
    }
    ab.finish().unwrap();
}

fn phase5_set_formula_rows(
    engine: &mut Engine<TestWorkbook>,
    sheet: &str,
    rows: usize,
    fixture: Phase5ComputedFlushProbeFixture,
) -> usize {
    let one = parse("=1").unwrap();
    let two = parse("=2").unwrap();
    let row_fn = parse("=ROW()").unwrap();
    let mut formulas = 0usize;

    let mut set_formula = |row0: usize, ast: &ASTNode| {
        engine
            .set_cell_formula(sheet, row0 as u32 + 1, 1, ast.clone())
            .unwrap();
        formulas += 1;
    };

    match fixture {
        Phase5ComputedFlushProbeFixture::ConstantCopied
        | Phase5ComputedFlushProbeFixture::CapZeroConstant => {
            for row0 in 0..rows {
                set_formula(row0, &one);
            }
        }
        Phase5ComputedFlushProbeFixture::RowVarying => {
            for row0 in 0..rows {
                set_formula(row0, &row_fn);
            }
        }
        Phase5ComputedFlushProbeFixture::AlternatingMostlyVaried => {
            for row0 in 0..rows {
                let ast = if row0 % 2 == 0 { &one } else { &two };
                set_formula(row0, ast);
            }
        }
        Phase5ComputedFlushProbeFixture::SparseEveryOther => {
            for row0 in (0..rows).step_by(2) {
                set_formula(row0, &row_fn);
            }
        }
    }

    formulas
}

fn phase5_measure_numbers(sheet: &ArrowSheet, rows: usize) -> Phase5ComputedFlushProbeOp {
    let view = sheet.range_view(0, 0, rows.saturating_sub(1), 0);
    let start = std::time::Instant::now();
    let mut segments = 0usize;
    let mut arrays = 0usize;
    let mut rows_scanned = 0usize;
    let mut checksum = 0.0;
    let mut non_null = 0usize;
    for segment in view.numbers_slices() {
        let (_row_start, row_len, cols) = segment.unwrap();
        segments += 1;
        rows_scanned += row_len;
        for array in cols {
            arrays += 1;
            for idx in 0..array.len() {
                if array.is_valid(idx) {
                    checksum += array.value(idx);
                    non_null += 1;
                }
            }
        }
    }
    Phase5ComputedFlushProbeOp {
        ms: start.elapsed().as_secs_f64() * 1000.0,
        segments,
        arrays,
        rows_scanned,
        checksum,
        non_null,
    }
}

fn phase5_measure_type_tags(sheet: &ArrowSheet, rows: usize) -> Phase5ComputedFlushProbeOp {
    let view = sheet.range_view(0, 0, rows.saturating_sub(1), 0);
    let start = std::time::Instant::now();
    let mut segments = 0usize;
    let mut arrays = 0usize;
    let mut rows_scanned = 0usize;
    let mut checksum = 0.0;
    let mut non_null = 0usize;
    for segment in view.type_tags_slices() {
        let (_row_start, row_len, cols) = segment.unwrap();
        segments += 1;
        rows_scanned += row_len;
        for array in cols {
            arrays += 1;
            for idx in 0..array.len() {
                if array.is_valid(idx) {
                    checksum += array.value(idx) as f64;
                    non_null += 1;
                }
            }
        }
    }
    Phase5ComputedFlushProbeOp {
        ms: start.elapsed().as_secs_f64() * 1000.0,
        segments,
        arrays,
        rows_scanned,
        checksum,
        non_null,
    }
}

fn phase5_measure_lowered_text(sheet: &ArrowSheet, rows: usize) -> Phase5ComputedFlushProbeOp {
    let view = sheet.range_view(0, 0, rows.saturating_sub(1), 0);
    let start = std::time::Instant::now();
    let mut segments = 0usize;
    let mut arrays = 0usize;
    let mut rows_scanned = 0usize;
    let mut checksum = 0.0;
    let mut non_null = 0usize;
    for segment in view.lowered_text_slices() {
        let (_row_start, row_len, cols) = segment.unwrap();
        segments += 1;
        rows_scanned += row_len;
        for array in cols {
            arrays += 1;
            for idx in 0..array.len() {
                if array.is_valid(idx) {
                    checksum += array.value(idx).len() as f64;
                    non_null += 1;
                }
            }
        }
    }
    Phase5ComputedFlushProbeOp {
        ms: start.elapsed().as_secs_f64() * 1000.0,
        segments,
        arrays,
        rows_scanned,
        checksum,
        non_null,
    }
}

fn run_phase5_computed_flush_probe_fixture(
    rows: usize,
    chunk_rows: usize,
    fixture: Phase5ComputedFlushProbeFixture,
) -> Phase5ComputedFlushProbeRow {
    let mut cfg = arrow_eval_config();
    if matches!(fixture, Phase5ComputedFlushProbeFixture::CapZeroConstant) {
        cfg.max_overlay_memory_bytes = Some(0);
    }
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    let sheet = "Sheet1";
    phase5_seed_base_rows(&mut engine, sheet, rows, chunk_rows);
    let formulas = phase5_set_formula_rows(&mut engine, sheet, rows, fixture);

    let eval_start = std::time::Instant::now();
    engine.evaluate_all().unwrap();
    let eval_ms = eval_start.elapsed().as_secs_f64() * 1000.0;

    let overlay_memory_usage = engine.overlay_memory_usage();
    let sheet_ref = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let raw_stats = phase5_probe_overlay_stats(sheet_ref, 0);
    let overlay_stats = Phase5ComputedFlushProbeOverlayStats {
        points: raw_stats.points,
        sparse_fragments: raw_stats.sparse_fragments,
        dense_fragments: raw_stats.dense_fragments,
        run_fragments: raw_stats.run_fragments,
        covered_len: raw_stats.covered_len,
    };
    let computed_overlay_estimated_bytes =
        phase5_probe_computed_overlay_estimated_bytes(sheet_ref, 0);

    reset_overlay_select_stats();
    let numbers = phase5_measure_numbers(sheet_ref, rows);
    let type_tags = phase5_measure_type_tags(sheet_ref, rows);
    let lowered_text = phase5_measure_lowered_text(sheet_ref, rows);
    let select_stats = snapshot_overlay_select_stats();

    Phase5ComputedFlushProbeRow {
        fixture: fixture.name(),
        rows,
        formulas,
        chunk_rows,
        eval_ms,
        overlay_memory_usage,
        computed_overlay_estimated_bytes,
        overlay_stats,
        numbers,
        type_tags,
        lowered_text,
        select_stats,
    }
}

#[test]
#[ignore = "manual Phase 5 computed flush observability probe; run with --ignored --nocapture"]
fn phase5_computed_flush_coalescing_observability_probe() {
    let rows = std::env::var("FORMUALIZER_COMPUTED_FLUSH_PROBE_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100_000)
        .max(1);
    let chunk_rows = std::env::var("FORMUALIZER_COMPUTED_FLUSH_PROBE_CHUNK_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(32 * 1024)
        .max(1);

    for fixture in [
        Phase5ComputedFlushProbeFixture::ConstantCopied,
        Phase5ComputedFlushProbeFixture::RowVarying,
        Phase5ComputedFlushProbeFixture::AlternatingMostlyVaried,
        Phase5ComputedFlushProbeFixture::SparseEveryOther,
        Phase5ComputedFlushProbeFixture::CapZeroConstant,
    ] {
        let row = run_phase5_computed_flush_probe_fixture(rows, chunk_rows, fixture);
        println!("{}", serde_json::to_string(&row).unwrap());
    }
}

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
    assert_eq!(stats.sparse_fragments, 0);
    assert_eq!(stats.covered_len, 2);
}

#[test]
fn coalesced_flush_sparse_offsets_creates_sparse_fragment() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 256);
        for row0 in 0..128 {
            ab.append_row(sheet, &[LiteralValue::Number((row0 + 1) as f64)])
                .unwrap();
        }
        ab.finish().unwrap();
    }
    let sheet_id = engine.sheet_id(sheet).unwrap();
    let mut buffer = ComputedWriteBuffer::default();

    for row0 in (0..128).step_by(2) {
        buffer.push_cell(
            sheet_id,
            row0,
            0,
            OverlayValue::Number(1000.0 + row0 as f64),
        );
    }
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.points, 0);
    assert_eq!(stats.sparse_fragments, 1);
    assert_eq!(stats.dense_fragments, 0);
    assert_eq!(stats.run_fragments, 0);
    assert_eq!(stats.covered_len, 64);
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Number(1000.0));
    assert_eq!(asheet.get_cell_value(1, 0), LiteralValue::Number(2.0));
    assert_eq!(asheet.get_cell_value(126, 0), LiteralValue::Number(1126.0));
    assert_eq!(asheet.get_cell_value(127, 0), LiteralValue::Number(128.0));
}

#[test]
fn coalesced_flush_sparse_empty_masks_base_only_at_offsets() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 256);
        for row0 in 0..128 {
            ab.append_row(sheet, &[LiteralValue::Number((row0 + 1) as f64)])
                .unwrap();
        }
        ab.finish().unwrap();
    }
    let sheet_id = engine.sheet_id(sheet).unwrap();
    let mut buffer = ComputedWriteBuffer::default();

    for row0 in (0..128).step_by(2) {
        buffer.push_cell(sheet_id, row0, 0, OverlayValue::Empty);
    }
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.sparse_fragments, 1);
    assert_eq!(stats.covered_len, 64);
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Empty);
    assert_eq!(asheet.get_cell_value(1, 0), LiteralValue::Number(2.0));
    assert_eq!(asheet.get_cell_value(126, 0), LiteralValue::Empty);
    assert_eq!(asheet.get_cell_value(127, 0), LiteralValue::Number(128.0));
}

#[test]
fn coalesced_flush_sparse_user_overlay_precedence() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 256);
        for _ in 0..128 {
            ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        }
        ab.finish().unwrap();
    }
    {
        let asheet = engine.sheet_store_mut().sheet_mut(sheet).unwrap();
        let (ch_i, off) = asheet.chunk_of_row(10).unwrap();
        asheet.columns[0].chunks[ch_i]
            .overlay
            .set_scalar(off, OverlayValue::Text("user".into()));
    }
    let sheet_id = engine.sheet_id(sheet).unwrap();
    let mut buffer = ComputedWriteBuffer::default();

    for row0 in (0..128).step_by(2) {
        buffer.push_cell(
            sheet_id,
            row0,
            0,
            OverlayValue::Number(1000.0 + row0 as f64),
        );
    }
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.sparse_fragments, 1);
    assert_eq!(asheet.get_cell_value(8, 0), LiteralValue::Number(1008.0));
    assert_eq!(
        asheet.get_cell_value(10, 0),
        LiteralValue::Text("user".into())
    );
    assert_eq!(asheet.get_cell_value(11, 0), LiteralValue::Empty);
}

#[test]
fn coalesced_flush_sparse_cap_zero_compacts_fragment_safely() {
    let mut cfg = arrow_eval_config();
    cfg.max_overlay_memory_bytes = Some(0);
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();

    for row0 in (0..128).step_by(2) {
        buffer.push_cell(
            sheet_id,
            row0,
            0,
            OverlayValue::Number(1000.0 + row0 as f64),
        );
    }
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    assert!(buffer.is_empty());
    assert_eq!(engine.overlay_memory_usage(), 0);
    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.covered_len, 0);
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Number(1000.0));
    assert_eq!(asheet.get_cell_value(1, 0), LiteralValue::Empty);
    assert_eq!(asheet.get_cell_value(126, 0), LiteralValue::Number(1126.0));
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
fn cap_zero_batches_computed_writes_before_compaction() {
    let mut cfg = arrow_eval_config();
    cfg.max_overlay_memory_bytes = Some(0);
    cfg.enable_parallel = false;
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    let sheet = "Sheet1";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 8);
        for _ in 0..32 {
            ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        }
        ab.finish().unwrap();
    }

    let formula = parse("=1").unwrap();
    for row in 1..=32 {
        engine
            .set_cell_formula(sheet, row, 1, formula.clone())
            .unwrap();
    }

    engine.evaluate_all().unwrap();

    assert_eq!(engine.overlay_memory_usage(), 0);
    assert!(
        engine.debug_overlay_compactions() <= 4,
        "cap=0 should compact per coalesced chunk, not per formula; compactions={}",
        engine.debug_overlay_compactions()
    );
    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let stats = asheet.columns[0]
        .chunk(0)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.covered_len, 0);
    for row0 in 0..32 {
        assert_eq!(asheet.get_cell_value(row0, 0), LiteralValue::Number(1.0));
    }
}

#[test]
fn coalesced_flush_dense_range_creates_dense_fragment() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();

    for row0 in 0..4 {
        buffer.push_cell(sheet_id, row0, 0, OverlayValue::Number((row0 + 1) as f64));
    }
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.points, 0);
    assert_eq!(stats.dense_fragments, 1);
    assert_eq!(stats.run_fragments, 0);
    assert_eq!(stats.sparse_fragments, 0);
    assert_eq!(stats.covered_len, 4);
    for row0 in 0..4 {
        assert_eq!(
            asheet.get_cell_value(row0 as usize, 0),
            LiteralValue::Number((row0 + 1) as f64)
        );
    }
}

#[test]
fn coalesced_flush_constant_range_creates_run_fragment() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();

    for row0 in 0..8 {
        buffer.push_cell(sheet_id, row0, 0, OverlayValue::Number(7.0));
    }
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.points, 0);
    assert_eq!(stats.dense_fragments, 0);
    assert_eq!(stats.run_fragments, 1);
    assert_eq!(stats.covered_len, 8);
    for row0 in 0..8 {
        assert_eq!(asheet.get_cell_value(row0, 0), LiteralValue::Number(7.0));
    }
}

#[test]
fn coalesced_flush_mostly_varied_range_prefers_dense_not_run() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();
    let values = [1.0, 2.0, 2.0, 3.0, 4.0, 5.0];

    for (row0, value) in values.iter().copied().enumerate() {
        buffer.push_cell(sheet_id, row0 as u32, 0, OverlayValue::Number(value));
    }
    let plan = engine.debug_plan_computed_write_coalescing(&buffer);
    assert_eq!(
        plan.chunks[0].shape,
        ComputedWriteChunkPlanShape::RunRange {
            start: 0,
            len: 6,
            runs: 5,
        }
    );

    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.points, 0);
    assert_eq!(stats.dense_fragments, 1);
    assert_eq!(stats.run_fragments, 0);
    assert_eq!(stats.covered_len, values.len());
}

#[test]
fn coalesced_flush_user_overlay_still_masks_computed_fragment() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 8);
        for _ in 0..3 {
            ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        }
        ab.finish().unwrap();
    }
    {
        let asheet = engine.sheet_store_mut().sheet_mut(sheet).unwrap();
        let (ch_i, off) = asheet.chunk_of_row(1).unwrap();
        asheet.columns[0].chunks[ch_i]
            .overlay
            .set_scalar(off, OverlayValue::Text("user".into()));
    }
    let sheet_id = engine.sheet_id(sheet).unwrap();
    let mut buffer = ComputedWriteBuffer::default();
    for row0 in 0..3 {
        buffer.push_cell(sheet_id, row0, 0, OverlayValue::Number(10.0 + row0 as f64));
    }

    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Number(10.0));
    assert_eq!(
        asheet.get_cell_value(1, 0),
        LiteralValue::Text("user".into())
    );
    assert_eq!(asheet.get_cell_value(2, 0), LiteralValue::Number(12.0));
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.dense_fragments, 1);
}

#[test]
fn coalesced_flush_empty_run_masks_base() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 8);
        for row0 in 0..4 {
            ab.append_row(sheet, &[LiteralValue::Number((row0 + 1) as f64)])
                .unwrap();
        }
        ab.finish().unwrap();
    }
    let sheet_id = engine.sheet_id(sheet).unwrap();
    let mut buffer = ComputedWriteBuffer::default();
    for row0 in 0..4 {
        buffer.push_cell(sheet_id, row0, 0, OverlayValue::Empty);
    }

    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.run_fragments, 1);
    assert_eq!(stats.covered_len, 4);
    for row0 in 0..4 {
        assert_eq!(asheet.get_cell_value(row0, 0), LiteralValue::Empty);
    }
}

#[test]
fn coalesced_flush_cap_zero_compacts_fragment_safely() {
    let mut cfg = arrow_eval_config();
    cfg.max_overlay_memory_bytes = Some(0);
    let mut engine = Engine::new(TestWorkbook::new(), cfg);
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();

    for row0 in 0..4 {
        buffer.push_cell(sheet_id, row0, 0, OverlayValue::Number(7.0));
    }
    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    assert!(buffer.is_empty());
    assert_eq!(engine.overlay_memory_usage(), 0);
    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    let (ch_i, _) = asheet.chunk_of_row(0).unwrap();
    let stats = asheet.columns[0]
        .chunk(ch_i)
        .unwrap()
        .computed_overlay
        .debug_stats();
    assert_eq!(stats.covered_len, 0);
    for row0 in 0..4 {
        assert_eq!(asheet.get_cell_value(row0, 0), LiteralValue::Number(7.0));
    }
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
