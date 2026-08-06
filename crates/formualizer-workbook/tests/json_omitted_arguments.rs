use formualizer_common::LiteralValue;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_workbook::traits::SaveDestination;
use formualizer_workbook::{CellData, JsonAdapter, SpreadsheetReader, SpreadsheetWriter};

#[test]
fn json_round_trip_preserves_omitted_argument_formula_and_value() {
    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Sheet1").unwrap();
    adapter
        .write_cell("Sheet1", 1, 1, CellData::from_formula("=IFERROR(1/0,)"))
        .unwrap();
    adapter.set_dimensions("Sheet1", Some((1, 1)));

    let bytes = adapter
        .save_to(SaveDestination::Bytes)
        .unwrap()
        .expect("bytes destination");
    let serialized = String::from_utf8(bytes.clone()).unwrap();
    assert!(serialized.contains("=IFERROR(1/0,)"));
    assert!(!serialized.to_ascii_lowercase().contains("omitted"));

    let mut reopened = JsonAdapter::open_bytes(bytes).unwrap();
    let cell = reopened.read_cell("Sheet1", 1, 1).unwrap().unwrap();
    assert_eq!(cell.formula.as_deref(), Some("=IFERROR(1/0,)"));

    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    reopened.stream_into_engine(&mut engine).unwrap();
    assert_eq!(
        engine.evaluate_cell("Sheet1", 1, 1).unwrap(),
        Some(LiteralValue::Number(0.0))
    );
}
