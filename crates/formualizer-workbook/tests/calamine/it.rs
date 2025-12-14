// Integration test for Calamine backend; run with `--features calamine,umya`.

use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{CalamineAdapter, LiteralValue, SpreadsheetReader};

#[test]
fn calamine_reads_values_and_formulas_from_generated_xlsx() {
    // Build a workbook in a temp dir using umya-spreadsheet
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        // Remember: umya coordinate tuple is (col,row)
        sh.get_cell_mut((1, 1)).set_value_number(41); // A1
        sh.get_cell_mut((2, 1)).set_formula("A1+1"); // B1
        sh.get_cell_mut((1, 2)).set_value_number(3.5); // A2
        sh.get_cell_mut((2, 2)).set_formula("A2*2"); // B2
    });

    // Load via Calamine adapter
    let mut backend = CalamineAdapter::open_path(&path).expect("open xlsx");

    // Sanity: sheet names exist
    let sheets = backend.sheet_names().unwrap();
    assert!(!sheets.is_empty());
    let sheet_name = sheets[0].clone();

    // Sanity: adapter sees A1 and B1
    let sheet = backend.read_sheet(&sheet_name).expect("read sheet");
    assert!(
        sheet.cells.contains_key(&(1, 1)),
        "A1 missing from adapter cells"
    );
    assert!(
        sheet.cells.contains_key(&(1, 2)),
        "B1 missing from adapter cells"
    );

    // Build engine
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());

    // Load into engine
    backend
        .stream_into_engine(&mut engine)
        .expect("load into engine");

    assert!(
        engine.sheet_id(&sheet_name).is_some(),
        "Engine did not register sheet {sheet_name}"
    );

    // Debug: check what's in the graph
    let _cell_value = engine.get_cell_value(&sheet_name, 1, 1);

    // Check raw values: allow either Int or Number or Text("41") depending on writer/reader typing
    match engine.get_cell_value(&sheet_name, 1, 1) {
        Some(LiteralValue::Int(41)) => {}
        Some(LiteralValue::Number(n)) if (n - 41.0).abs() < 1e-9 => {}
        Some(LiteralValue::Text(ref s)) if s == "41" => {}
        other => panic!("Unexpected A1 value: {other:?}"),
    }
    // B1 has formula; before evaluation, its value is not computed; evaluate all
    let _eval_result = engine.evaluate_all().expect("eval");

    // B1 should be 42
    match engine.get_cell_value(&sheet_name, 1, 2) {
        Some(LiteralValue::Number(n)) => assert!((n - 42.0).abs() < 1e-9),
        other => panic!("Unexpected B1 value: {other:?}"),
    }

    // B2 should be 7.0
    match engine.get_cell_value(&sheet_name, 2, 2) {
        Some(LiteralValue::Number(n)) => assert!((n - 7.0).abs() < 1e-9),
        other => panic!("Unexpected B2 value: {other:?}"),
    }
}
