// Regression test for issue #162: INDEX over unbounded whole-column/whole-row
// ranges returned #REF! while Excel returns the correct value.
//
// The fixture is Excel-authored: Sheet1!A1:A5 hold the repro formulas and each
// cached Excel value is 42. The Data sheet holds the looked-up values.
use formualizer_common::LiteralValue;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader};

#[test]
fn issue162_index_unbounded_ranges_match_excel_cached_values() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/issue162-failure.xlsx"
    );
    let mut backend = CalamineAdapter::open_path(path).expect("open issue162 fixture");
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    // A1: =INDEX(B:B,2,1)
    // A2: =INDEX(2:2,1,2)
    // A3: =INDEX(Data!B:B,2,1)
    // A4: =INDEX(Data!1:2,2,2)
    // A5: =INDEX(Data!$A:$C, MATCH("row",Data!$A:$A,0), MATCH("col",Data!$1:$1,0))
    for row in 1u32..=5 {
        match engine.get_cell_value("Sheet1", row, 1) {
            Some(LiteralValue::Number(n)) => {
                assert!(
                    (n - 42.0).abs() < 1e-9,
                    "Sheet1!A{row}: expected 42, got {n}"
                )
            }
            Some(LiteralValue::Int(i)) => assert_eq!(i, 42, "Sheet1!A{row}"),
            other => panic!("Sheet1!A{row}: expected 42, got {other:?}"),
        }
    }
}
