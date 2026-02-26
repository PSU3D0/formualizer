use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig, RowVisibilitySource};
use formualizer_workbook::{SpreadsheetReader, UmyaAdapter};

#[test]
fn umya_hidden_rows_ingest_as_manual_visibility() {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sheet.get_cell_mut((1, 1)).set_value_number(123.0);
        sheet.get_row_dimension_mut(&2).set_hidden(true);
        sheet.get_row_dimension_mut(&5).set_hidden(true);
    });

    let mut adapter = UmyaAdapter::open_path(&path).expect("open xlsx");
    let sheet = adapter.read_sheet("Sheet1").expect("read sheet");
    assert_eq!(sheet.row_hidden_manual, vec![2, 5]);
    assert!(sheet.row_hidden_filter.is_empty());

    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    adapter
        .stream_into_engine(&mut engine)
        .expect("stream into engine");

    assert_eq!(
        engine.is_row_hidden("Sheet1", 2, Some(RowVisibilitySource::Manual)),
        Some(true)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 5, Some(RowVisibilitySource::Manual)),
        Some(true)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 2, Some(RowVisibilitySource::Filter)),
        Some(false)
    );
}
