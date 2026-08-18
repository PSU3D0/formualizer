use crate::common::build_workbook;
use formualizer_common::LiteralValue;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader};

#[test]
fn calamine_date_arithmetic_returns_number() {
    // C107 = 2024-10-18 (serial 45583)
    // C108 = 1
    // C109 = C107 + (ROUND(C108,0) * 14) => serial 45597
    //
    // Excel returns a plain number from date arithmetic; display formatting
    // (not the value type) is what makes it look like a date. (#312)
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();

        // C107
        sh.get_cell_mut((3, 107)).set_value_number(45583.0);
        let _ = sh
            .get_style_mut("C107")
            .get_number_format_mut()
            .set_format_code(umya_spreadsheet::NumberingFormat::FORMAT_DATE_XLSX14);

        // C108
        sh.get_cell_mut((3, 108)).set_value_number(1.0);

        // C109
        sh.get_cell_mut((3, 109))
            .set_formula("=C107+(ROUND(C108,0)*14)");
    });

    let mut backend = CalamineAdapter::open_path(&path).expect("open via calamine");
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend
        .stream_into_engine(&mut engine)
        .expect("stream into engine");
    engine.evaluate_all().expect("evaluate");

    match engine.get_cell_value("Sheet1", 109, 3) {
        Some(LiteralValue::Number(n)) => {
            // 45583 + 14 = 45597
            assert!(
                (n - 45597.0).abs() < 1e-9,
                "expected serial 45597, got {n}"
            );
        }
        other => panic!("expected Number(45597) at Sheet1!C109, got {other:?}"),
    }
}
