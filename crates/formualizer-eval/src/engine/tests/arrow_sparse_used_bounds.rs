use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use crate::traits::EvaluationContext;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::ReferenceType;

#[test]
fn used_rows_for_columns_sees_sparse_overlay_for_whole_column_refs() {
    let mut cfg = arrow_eval_config();
    cfg.max_open_ended_rows = 10;

    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    let sheet = "S";
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, 1, 64);
        for _ in 0..64 {
            ab.append_row(sheet, &[LiteralValue::Empty]).unwrap();
        }
        ab.finish().unwrap();
    }

    let far_row: u32 = 1000;
    engine
        .set_cell_value(sheet, far_row, 1, LiteralValue::Int(1))
        .unwrap();

    // A:A
    let r = ReferenceType::range(Some(sheet.to_string()), None, Some(1), None, Some(1));

    let view = engine.resolve_range_view(&r, sheet).unwrap();
    let av = view;

    // Whole-column refs are anchored at row 1, but the end row should expand to the last used row
    // (even if the open-ended cap is smaller).
    assert_eq!(av.start_row(), 0);
    assert_eq!(av.end_row(), (far_row - 1) as usize);
    assert_eq!(
        av.get_cell((far_row - 1) as usize, 0),
        LiteralValue::Number(1.0)
    );
}

#[test]
fn used_cols_for_rows_sees_sparse_overlay_for_whole_row_refs() {
    let mut cfg = arrow_eval_config();
    cfg.max_open_ended_cols = 10;

    let mut engine = Engine::new(TestWorkbook::new(), cfg);

    let sheet = "S";
    let ncols: usize = 25;
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet(sheet, ncols, 64);
        let row = vec![LiteralValue::Empty; ncols];
        for _ in 0..64 {
            ab.append_row(sheet, &row).unwrap();
        }
        ab.finish().unwrap();
    }

    let far_row: u32 = 1000;
    let far_col: u32 = 20;
    engine
        .set_cell_value(sheet, far_row, far_col, LiteralValue::Int(1))
        .unwrap();

    // far_row:far_row
    let r = ReferenceType::range(
        Some(sheet.to_string()),
        Some(far_row),
        None,
        Some(far_row),
        None,
    );

    let view = engine.resolve_range_view(&r, sheet).unwrap();
    let av = view;

    // Whole-row refs are anchored at column A, but the end column should expand to the last used
    // column (even if the open-ended cap is smaller).
    assert_eq!(av.start_col(), 0);
    assert_eq!(av.end_col(), (far_col - 1) as usize);
    assert_eq!(
        av.get_cell(0, (far_col - 1) as usize),
        LiteralValue::Number(1.0)
    );
}
