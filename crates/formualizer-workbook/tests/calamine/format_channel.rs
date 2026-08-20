use super::common::build_workbook;
use chrono::NaiveDate;
use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig, traits::CellData,
};
use std::collections::BTreeMap;

fn loaded_workbook_with_config(path: &std::path::Path, config: WorkbookConfig) -> Workbook {
    Workbook::from_reader(
        CalamineAdapter::open_path(path).expect("open workbook"),
        LoadStrategy::EagerAll,
        config,
    )
    .expect("load workbook")
}

fn loaded_workbook(path: &std::path::Path) -> Workbook {
    loaded_workbook_with_config(path, WorkbookConfig::ephemeral())
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
    for (mode, config) in [
        ("ephemeral", WorkbookConfig::ephemeral()),
        ("interactive", WorkbookConfig::interactive()),
    ] {
        let mut workbook = loaded_workbook_with_config(&path, config);
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
            Some(LiteralValue::Number(7.0)),
            "loaded-date overwrite in {mode} mode"
        );

        workbook
            .set_formula("Sheet1", 4, 4, "=DATE(2024,12,1)")
            .unwrap();
        workbook.evaluate_all().unwrap();
        workbook.set_formula("Sheet1", 4, 4, "=1+1").unwrap();
        workbook.evaluate_all().unwrap();
        assert_eq!(
            workbook.get_value("Sheet1", 4, 4),
            Some(LiteralValue::Number(2.0)),
            "formula overwrite in {mode} mode"
        );
    }
}

#[test]
fn value_write_paths_clear_derived_formats_in_both_modes() {
    for (mode, config) in [
        ("ephemeral", WorkbookConfig::ephemeral()),
        ("interactive", WorkbookConfig::interactive()),
    ] {
        let mut workbook = Workbook::new_with_config(config);
        workbook.add_sheet("Sheet1").ok();
        for (row, col) in [(4, 4), (5, 5), (6, 6)] {
            workbook
                .set_formula("Sheet1", row, col, "=DATE(2024,12,1)")
                .unwrap();
        }
        workbook.evaluate_all().unwrap();

        workbook
            .set_value("Sheet1", 4, 4, LiteralValue::Number(5.0))
            .unwrap();
        workbook
            .set_values("Sheet1", 5, 5, &[vec![LiteralValue::Number(9.0)]])
            .unwrap();
        let mut cells = BTreeMap::new();
        cells.insert((6, 6), CellData::from_value(LiteralValue::Number(11.0)));
        workbook.write_range("Sheet1", (6, 6), cells).unwrap();
        workbook.evaluate_all().unwrap();

        assert_eq!(
            workbook.get_value("Sheet1", 4, 4),
            Some(LiteralValue::Number(5.0)),
            "set_value in {mode} mode"
        );
        assert_eq!(
            workbook.get_value("Sheet1", 5, 5),
            Some(LiteralValue::Number(9.0)),
            "set_values in {mode} mode"
        );
        assert_eq!(
            workbook.get_value("Sheet1", 6, 6),
            Some(LiteralValue::Number(11.0)),
            "write_range in {mode} mode"
        );
    }
}

#[test]
fn logged_value_over_derived_formula_undo_rederives_date_format() {
    let mut workbook = Workbook::new_with_config(WorkbookConfig::interactive());
    workbook.add_sheet("Sheet1").ok();
    workbook
        .set_formula("Sheet1", 4, 4, "=DATE(2024,12,1)")
        .unwrap();
    workbook.evaluate_all().unwrap();
    assert!(matches!(
        workbook.get_value("Sheet1", 4, 4),
        Some(LiteralValue::Date(_))
    ));

    workbook
        .set_value("Sheet1", 4, 4, LiteralValue::Number(5.0))
        .unwrap();
    workbook.evaluate_all().unwrap();
    assert_eq!(
        workbook.get_value("Sheet1", 4, 4),
        Some(LiteralValue::Number(5.0))
    );

    workbook.undo().unwrap();
    workbook.evaluate_all().unwrap();
    assert!(matches!(
        workbook.get_value("Sheet1", 4, 4),
        Some(LiteralValue::Date(_))
    ));
}

#[test]
fn structural_edits_purge_derived_formats_on_both_axes() {
    let mut rows = Workbook::new_with_config(WorkbookConfig::ephemeral());
    rows.add_sheet("Sheet1").ok();
    rows.set_formula("Sheet1", 1, 1, "=DATE(2024,12,1)")
        .unwrap();
    rows.set_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();
    rows.evaluate_all().unwrap();
    rows.engine_mut().delete_rows("Sheet1", 1, 1).unwrap();
    rows.evaluate_all().unwrap();
    assert_eq!(
        rows.get_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(5.0))
    );

    let mut columns = Workbook::new_with_config(WorkbookConfig::ephemeral());
    columns.add_sheet("Sheet1").ok();
    columns
        .set_formula("Sheet1", 3, 3, "=DATE(2024,12,1)")
        .unwrap();
    columns
        .set_value("Sheet1", 3, 4, LiteralValue::Number(5.0))
        .unwrap();
    columns.evaluate_all().unwrap();
    columns.engine_mut().delete_columns("Sheet1", 3, 1).unwrap();
    columns.evaluate_all().unwrap();
    assert_eq!(
        columns.get_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn inserts_and_sheet_delete_keep_derived_format_store_clean() {
    let mut rows = Workbook::new_with_config(WorkbookConfig::ephemeral());
    rows.add_sheet("Sheet1").ok();
    rows.set_formula("Sheet1", 1, 1, "=DATE(2024,12,1)")
        .unwrap();
    rows.evaluate_all().unwrap();
    rows.engine_mut().insert_rows("Sheet1", 1, 1).unwrap();
    rows.evaluate_all().unwrap();
    assert_eq!(rows.get_value("Sheet1", 1, 1), None);
    assert!(matches!(
        rows.get_value("Sheet1", 2, 1),
        Some(LiteralValue::Date(_))
    ));

    let mut columns = Workbook::new_with_config(WorkbookConfig::ephemeral());
    columns.add_sheet("Sheet1").ok();
    columns
        .set_formula("Sheet1", 3, 3, "=DATE(2024,12,1)")
        .unwrap();
    columns.evaluate_all().unwrap();
    columns.engine_mut().insert_columns("Sheet1", 3, 1).unwrap();
    columns.evaluate_all().unwrap();
    assert_eq!(columns.get_value("Sheet1", 3, 3), None);
    assert!(matches!(
        columns.get_value("Sheet1", 3, 4),
        Some(LiteralValue::Date(_))
    ));

    columns.add_sheet("Other").unwrap();
    columns.delete_sheet("Sheet1").unwrap();
    columns.add_sheet("Sheet1").unwrap();
    columns.set_formula("Sheet1", 3, 3, "=1+1").unwrap();
    columns.evaluate_all().unwrap();
    assert_eq!(
        columns.get_value("Sheet1", 3, 3),
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
