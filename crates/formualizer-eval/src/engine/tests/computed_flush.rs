use super::common::arrow_eval_config;
use crate::arrow_store::OverlayValue;
use crate::engine::eval::{ComputedWrite, ComputedWriteBuffer, Engine};
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
fn computed_write_buffer_flush_to_map_matches_immediate_cell_writes() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());
    let sheet = "Sheet1";
    let sheet_id = engine.graph.sheet_id_mut(sheet);
    let mut buffer = ComputedWriteBuffer::default();

    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Number(7.0));
    buffer.push_cell(sheet_id, 0, 0, OverlayValue::Number(9.0));

    engine.flush_computed_write_buffer(&mut buffer).unwrap();

    assert!(buffer.is_empty());
    let asheet = engine.sheet_store().sheet(sheet).expect("arrow sheet");
    assert_eq!(asheet.get_cell_value(0, 0), LiteralValue::Number(9.0));
}

#[test]
fn computed_write_buffer_rect_expands_row_major_correctly() {
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
