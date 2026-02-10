// Integration test for Calamine backend date serial interpretation.
// Run with: `cargo test -p formualizer-workbook --features calamine --test calamine`

use crate::common::build_workbook;
use chrono::NaiveDate;
use formualizer_common::LiteralValue;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader};

#[test]
fn calamine_excel_1900_date_serial_roundtrips_without_off_by_one() {
    // Excel 1900 date system serial for 2023-03-01 is 44986.
    let serial = 44986.0;

    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();

        // Target cell: H15
        sh.get_cell_mut((8, 15)).set_value_number(serial);

        // Apply a built-in date number format so calamine yields Data::DateTime.
        let _ = sh
            .get_style_mut("H15")
            .get_number_format_mut()
            .set_format_code(umya_spreadsheet::NumberingFormat::FORMAT_DATE_XLSX14);
    });

    let mut backend = CalamineAdapter::open_path(&path).expect("open via calamine");
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend
        .stream_into_engine(&mut engine)
        .expect("stream into engine");
    engine.evaluate_all().expect("evaluate");

    match engine.get_cell_value("Sheet1", 15, 8) {
        Some(LiteralValue::Date(d)) => {
            assert_eq!(d, NaiveDate::from_ymd_opt(2023, 3, 1).unwrap());
        }
        other => panic!("Expected date at Sheet1!H15, got {other:?}"),
    }
}
