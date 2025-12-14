// Integration test for Umya backend; run with `--features umya`.

use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig, SheetIndexMode};
use formualizer_workbook::{
    CellData, LiteralValue, SpreadsheetReader, SpreadsheetWriter, UmyaAdapter,
};

#[test]
fn umya_full_roundtrip_formula_update() {
    // 1. Build initial workbook with A1=10, B1 =A1*2
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("roundtrip.xlsx");
    let mut book = umya_spreadsheet::new_file();
    let ws = book.get_sheet_by_name_mut("Sheet1").unwrap();
    ws.get_cell_mut((1, 1)).set_value_number(10); // A1
    ws.get_cell_mut((2, 1)).set_formula("A1*2"); // B1 (stored without '=')
    umya_spreadsheet::writer::xlsx::write(&book, &path).unwrap();

    // 2. Load via UmyaAdapter and engine evaluate
    let mut backend = UmyaAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    engine.set_sheet_index_mode(SheetIndexMode::FastBatch);
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 2) {
        // row=1 col=2 (B1)
        Some(LiteralValue::Number(n)) => assert!((n - 20.0).abs() < 1e-9),
        other => panic!("Expected 20 got {:?}", other),
    }

    // 3. Update formula B1 -> =A1*3 using writer and commit to disk
    // Need mutable backend again; reopen to get mutable; simpler reopen.
    let mut backend2 = UmyaAdapter::open_path(&path).unwrap();
    backend2
        .write_cell("Sheet1", 1, 2, CellData::from_formula("=A1*3"))
        .unwrap();
    backend2.save().unwrap();

    // 4. Reopen and assert formula string changed then evaluate to 30
    let mut backend3 = UmyaAdapter::open_path(&path).unwrap();
    let ctx2 = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine2: Engine<_> = Engine::new(ctx2, EvalConfig::default());
    engine2.set_sheet_index_mode(SheetIndexMode::FastBatch);
    backend3.stream_into_engine(&mut engine2).unwrap();
    // Demand-driven: evaluate just B1 (Sheet1!B1)
    engine2.evaluate_until(&[("Sheet1", 1, 2)]).unwrap();
    match engine2.get_cell_value("Sheet1", 1, 2) {
        // row=1 col=2
        Some(LiteralValue::Number(n)) => assert!((n - 30.0).abs() < 1e-9),
        other => panic!("Expected on-demand 30 got {:?}", other),
    }

    // Sanity: modifying A1 and re-demand B1 updates result
    engine2
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine2.evaluate_until(&[("Sheet1", 1, 2)]).unwrap();
    match engine2.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Number(n)) => assert!((n - 60.0).abs() < 1e-9),
        other => panic!("Expected updated on-demand 60 got {:?}", other),
    }

    // Evaluate multiple cells (A1 and B1) using new convenience API
    let multi = engine2
        .evaluate_cells(&[("Sheet1", 1, 1), ("Sheet1", 1, 2)])
        .unwrap();
    assert_eq!(multi.len(), 2);
    assert!(matches!(multi[0], Some(LiteralValue::Number(n)) if (n-20.0).abs()<1e-9));
    assert!(matches!(multi[1], Some(LiteralValue::Number(n)) if (n-60.0).abs()<1e-9));
}
