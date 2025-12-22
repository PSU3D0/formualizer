use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use arrow_array::Array as _;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::ReferenceType;

#[test]
fn criteria_mask_text_oob_column_uses_physical_rows() {
    let mut cfg = arrow_eval_config();
    cfg.max_open_ended_rows = 1_048_576;
    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    // Ensure the sheet has a physical row extent (nrows=1000) but only 2 columns (A/B).
    engine
        .set_cell_value("Sheet1", 1000, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1000, 2, LiteralValue::Text("Yes".into()))
        .unwrap();

    // Column C is out-of-bounds in the Arrow sheet but resolves as an open-ended whole-column view.
    let c_whole_col = ReferenceType::Range {
        sheet: Some("Sheet1".to_string()),
        start_row: None,
        start_col: Some(3),
        end_row: None,
        end_col: Some(3),
        start_row_abs: false,
        start_col_abs: false,
        end_row_abs: false,
        end_col_abs: false,
    };
    let view = engine.resolve_range_view(&c_whole_col, "Sheet1").unwrap();

    let pred = crate::args::parse_criteria(&LiteralValue::Text("".into())).unwrap();
    let mask = engine
        .build_criteria_mask(&view, 0, &pred)
        .expect("expected cached criteria mask");

    // The mask should be bounded by the sheet's physical row extent, not the open-ended cap.
    assert_eq!(mask.len(), 1000);
    assert_eq!(mask.null_count(), 0);
    for i in 0..mask.len() {
        assert!(mask.value(i));
    }
}

#[test]
fn criteria_mask_text_oob_column_empty_sheet_is_zero_len() {
    let mut cfg = arrow_eval_config();
    cfg.max_open_ended_rows = 1_048_576;
    let engine = Engine::new(TestWorkbook::new(), cfg);

    // Sheet exists in Arrow store but has nrows=0. An open-ended whole-column reference should not
    // force allocating a 1M-row lowered-text array just to build a criteria mask.
    let c_whole_col =
        ReferenceType::range(Some("Sheet1".to_string()), None, Some(3), None, Some(3));
    let view = engine.resolve_range_view(&c_whole_col, "Sheet1").unwrap();

    let pred = crate::args::parse_criteria(&LiteralValue::Text("".into())).unwrap();
    let mask = engine
        .build_criteria_mask(&view, 0, &pred)
        .expect("expected cached criteria mask");

    assert_eq!(mask.len(), 0);
}
