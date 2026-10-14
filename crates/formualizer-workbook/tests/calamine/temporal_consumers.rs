use crate::common::build_workbook;
use chrono::{Duration, NaiveDate, NaiveTime};
use formualizer_common::LiteralValue;
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

#[test]
fn loaded_time_and_duration_egress_preserves_elapsed_days() {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for (row, value, format) in [
            (1, 25.5 / 24.0, "[h]:mm"),
            (2, -0.25, "[h]:mm"),
            (3, 0.5, "hh:mm"),
        ] {
            sheet.get_cell_mut((1, row)).set_value_number(value);
            sheet
                .get_style_mut((1, row))
                .get_number_format_mut()
                .set_format_code(format);
        }
    });
    let workbook = Workbook::from_reader(
        CalamineAdapter::open_path(&path).unwrap(),
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    for (row, value) in [(1, Duration::minutes(1530)), (2, Duration::hours(-6))] {
        assert_eq!(
            workbook.get_value("Sheet1", row, 1),
            Some(LiteralValue::Duration(value)),
            "row {row}"
        );
    }
    assert_eq!(
        workbook.get_value("Sheet1", 3, 1),
        Some(LiteralValue::Time(
            NaiveTime::from_hms_opt(12, 0, 0).unwrap()
        ))
    );
}

#[test]
fn loaded_date_formats_do_not_materialize_during_calculation() {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for row in 1..=2 {
            sheet.get_cell_mut((1, row)).set_value_number(36907.0);
            sheet
                .get_style_mut((1, row))
                .get_number_format_mut()
                .set_format_code(umya_spreadsheet::NumberingFormat::FORMAT_DATE_XLSX14);
            sheet.get_cell_mut((2, row)).set_value_number(10.0);
        }
        let formulas = [
            "TEXT(A1,\"mm/dd/yyyy\")",
            "COUNTIF(A1:A2,A1)",
            "SUMIF(A1:A2,A1,B1:B2)",
            "MATCH(A1,A1:A2,0)",
            "VLOOKUP(A1,A1:B2,2,FALSE)",
            "SUM(A1:A2)",
            "DATEVALUE(TEXT(A1,\"mm/dd/yyyy\"))",
            "A1>DATE(2001,1,15)",
            "A1+0.5",
            "D9*1",
        ];
        for (i, formula) in formulas.iter().enumerate() {
            sheet.get_cell_mut((4, i as u32 + 1)).set_formula(*formula);
        }
    });
    let adapter = CalamineAdapter::open_path(&path).unwrap();
    let mut workbook = Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    workbook.evaluate_all().unwrap();
    let date = NaiveDate::from_ymd_opt(2001, 1, 16).unwrap();
    let expected = [
        LiteralValue::Text("01/16/2001".into()),
        LiteralValue::Number(2.0),
        LiteralValue::Number(20.0),
        LiteralValue::Number(1.0),
        LiteralValue::Number(10.0),
        LiteralValue::Number(73814.0),
        LiteralValue::Date(date),
        LiteralValue::Boolean(true),
        LiteralValue::DateTime(date.and_hms_opt(12, 0, 0).unwrap()),
        LiteralValue::Number(36907.5),
    ];
    for (i, expected) in expected.into_iter().enumerate() {
        assert_eq!(
            workbook.get_value("Sheet1", i as u32 + 1, 4),
            Some(expected)
        );
    }
    assert_eq!(
        workbook.get_value("Sheet1", 1, 1),
        Some(LiteralValue::Date(date))
    );
}
