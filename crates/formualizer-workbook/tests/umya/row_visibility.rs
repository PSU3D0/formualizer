use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig, RowVisibilitySource};
use formualizer_workbook::{
    LiteralValue, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};

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

#[test]
fn umya_hidden_rows_affect_subtotal_end_to_end() {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sheet.get_cell_mut((1, 2)).set_value_number(10.0); // A2
        sheet.get_cell_mut((1, 3)).set_value_number(20.0); // A3 (hidden)
        sheet.get_cell_mut((1, 4)).set_value_number(30.0); // A4
        sheet.get_cell_mut((1, 5)).set_value_number(100.0); // A5
        sheet.get_cell_mut((2, 1)).set_formula("=SUBTOTAL(109,A2:A5)"); // B1
        sheet.get_row_dimension_mut(&3).set_hidden(true);
    });

    let backend = UmyaAdapter::open_path(&path).expect("open xlsx");
    let mut wb = Workbook::from_reader(backend, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
        .expect("load workbook");

    let value = wb.evaluate_cell("Sheet1", 1, 2).expect("evaluate B1");
    match value {
        LiteralValue::Number(n) => assert!((n - 140.0).abs() < 1e-9, "{n}"),
        LiteralValue::Int(i) => assert_eq!(i, 140),
        other => panic!("expected numeric subtotal, got {other:?}"),
    }
}
