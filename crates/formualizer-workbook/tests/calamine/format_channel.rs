use super::common::build_workbook;
use chrono::NaiveDate;
use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

fn loaded_workbook(path: &std::path::Path) -> Workbook {
    Workbook::from_reader(
        CalamineAdapter::open_path(path).expect("open workbook"),
        LoadStrategy::EagerAll,
        WorkbookConfig::ephemeral(),
    )
    .expect("load workbook")
}

#[test]
fn derived_format_is_keyed_to_its_off_origin_cell() {
    let mut workbook = Workbook::new_with_config(WorkbookConfig::ephemeral());
    workbook.add_sheet("Sheet1").ok();
    workbook
        .set_formula("Sheet1", 2, 2, "=DATE(2024,12,1)")
        .unwrap();
    workbook.set_formula("Sheet1", 3, 3, "=1+1").unwrap();
    workbook
        .set_formula("Sheet1", 5, 7, "=DATE(2024,12,1)")
        .unwrap();
    workbook.evaluate_all().unwrap();

    let date = LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).unwrap());
    assert_eq!(workbook.get_value("Sheet1", 2, 2), Some(date.clone()));
    assert_eq!(
        workbook.get_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(workbook.get_value("Sheet1", 5, 7), Some(date));
}

#[test]
fn overwrites_clear_explicit_and_derived_temporal_formats() {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sheet.get_cell_mut((1, 1)).set_value_number(45_583.0);
        sheet
            .get_style_mut("A1")
            .get_number_format_mut()
            .set_format_code(umya_spreadsheet::NumberingFormat::FORMAT_DATE_XLSX14);
    });
    let mut workbook = loaded_workbook(&path);
    workbook.evaluate_all().unwrap();
    assert!(matches!(
        workbook.get_value("Sheet1", 1, 1),
        Some(LiteralValue::Date(_))
    ));

    workbook
        .set_value("Sheet1", 1, 1, LiteralValue::Number(7.0))
        .unwrap();
    workbook.evaluate_all().unwrap();
    assert_eq!(
        workbook.get_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(7.0))
    );

    workbook
        .set_formula("Sheet1", 4, 4, "=DATE(2024,12,1)")
        .unwrap();
    workbook.evaluate_all().unwrap();
    workbook.set_formula("Sheet1", 4, 4, "=1+1").unwrap();
    workbook.evaluate_all().unwrap();
    assert_eq!(
        workbook.get_value("Sheet1", 4, 4),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn scalar_and_range_egress_agree_for_loaded_and_derived_dates() {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sheet.get_cell_mut((6, 10)).set_value_number(45_583.0);
        sheet
            .get_style_mut("F10")
            .get_number_format_mut()
            .set_format_code(umya_spreadsheet::NumberingFormat::FORMAT_DATE_XLSX14);
        sheet.get_cell_mut((7, 10)).set_formula("=F10+1");
    });
    let mut workbook = loaded_workbook(&path);
    workbook.evaluate_all().unwrap();

    let address = RangeAddress::new("Sheet1", 10, 6, 10, 7).unwrap();
    let range = workbook.read_range(&address);
    assert_eq!(
        range,
        vec![vec![
            workbook.get_value("Sheet1", 10, 6).unwrap(),
            workbook.get_value("Sheet1", 10, 7).unwrap(),
        ]]
    );
    assert!(
        range[0]
            .iter()
            .all(|value| matches!(value, LiteralValue::Date(_)))
    );
}
