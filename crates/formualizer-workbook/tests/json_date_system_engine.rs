use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_workbook::{CellData, JsonAdapter, SpreadsheetWriter};

#[test]
fn json_date_system_propagates_and_affects_date_serials() {
    // Build a small JSON workbook with a formula DATE(1904,1,1)
    let mut adapter = JsonAdapter::new();
    let sheet = "S";
    adapter.create_sheet(sheet).unwrap();
    // Put formula at A1
    adapter
        .write_cell(sheet, 1, 1, CellData::from_formula("=DATE(1904,1,1)"))
        .unwrap();
    // Set dimensions to at least 1x1
    adapter.set_dimensions(sheet, Some((1, 1)));

    // Engine default (1900)
    let mut eng_1900 = Engine::new(TestWorkbook::new(), EvalConfig::default());
    adapter.stream_into_engine(&mut eng_1900).unwrap();
    let v1 = eng_1900.evaluate_cell(sheet, 1, 1).unwrap().unwrap();

    // Now mark workbook as 1904 and stream into a fresh engine
    let mut adapter_1904 = adapter;
    adapter_1904.set_date_system_1904(sheet, true);
    let cfg = EvalConfig::default();
    let mut eng_1904 = Engine::new(TestWorkbook::new(), cfg);
    adapter_1904.stream_into_engine(&mut eng_1904).unwrap();
    let v2 = eng_1904.evaluate_cell(sheet, 1, 1).unwrap().unwrap();

    match (v1, v2) {
        (
            formualizer_common::LiteralValue::Number(a),
            formualizer_common::LiteralValue::Number(b),
        ) => {
            assert!((a - 1462.0).abs() < 1e-9, "expected 1462, got {a}");
            assert!(b.abs() < 1e-9, "expected 0, got {b}");
        }
        other => panic!("unexpected results: {other:?}"),
    }
}
