use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig, RowVisibilitySource};
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader};

#[test]
fn calamine_row_visibility_explicit_fallback_is_empty() {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sheet.get_cell_mut((1, 1)).set_value_number(1.0);
        sheet.get_row_dimension_mut(&3).set_hidden(true);
        sheet.get_row_dimension_mut(&4).set_hidden(true);
    });

    let mut adapter = CalamineAdapter::open_path(&path).expect("open xlsx");
    let sheet = adapter.read_sheet("Sheet1").expect("read sheet");

    assert!(sheet.row_hidden_manual.is_empty());
    assert!(sheet.row_hidden_filter.is_empty());

    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    adapter
        .stream_into_engine(&mut engine)
        .expect("stream into engine");

    assert_eq!(
        engine.is_row_hidden("Sheet1", 3, Some(RowVisibilitySource::Manual)),
        Some(false)
    );
    assert_eq!(
        engine.is_row_hidden("Sheet1", 3, Some(RowVisibilitySource::Filter)),
        Some(false)
    );
}
