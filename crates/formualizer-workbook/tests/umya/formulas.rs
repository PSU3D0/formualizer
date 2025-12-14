// Integration test for Umya backend; run with `--features umya`.

use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{LiteralValue, SpreadsheetReader, UmyaAdapter};

#[test]
fn umya_extracts_formulas_and_normalizes_equals() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(10); // A1
        sh.get_cell_mut((2, 1)).set_formula("A1+5"); // B1 no '='
        sh.get_cell_mut((1, 2)).set_formula("=A1*2"); // A2
        sh.get_cell_mut((2, 2)).set_value_number(3); // B2
    });
    let mut backend = UmyaAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::FastBatch);
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 2) {
        // B1
        Some(LiteralValue::Number(n)) => assert!((n - 15.0).abs() < 1e-9),
        other => panic!("Unexpected B1: {:?}", other),
    }
    match engine.get_cell_value("Sheet1", 2, 1) {
        // A2
        Some(LiteralValue::Number(n)) => assert!((n - 20.0).abs() < 1e-9),
        other => panic!("Unexpected A2: {:?}", other),
    }
}
