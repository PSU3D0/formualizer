use chrono::{Duration, NaiveDate, NaiveTime};
use formualizer_eval::engine::DateSystem;
use formualizer_workbook::{
    CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

fn roundtrip_and_assert_numeric_arithmetic(
    value: LiteralValue,
    expected_serial: f64,
    config: WorkbookConfig,
) {
    let mut workbook = Workbook::new_with_config(config.clone());
    workbook.add_sheet("S").unwrap();
    workbook.set_value("S", 1, 1, value).unwrap();
    workbook.set_formula("S", 1, 2, "=A1+1").unwrap();
    workbook.set_formula("S", 1, 3, "=A1*1").unwrap();
    workbook.set_formula("S", 1, 4, "=ISNUMBER(A1)").unwrap();

    let bytes = workbook.to_xlsx_bytes().unwrap();
    let adapter = CalamineAdapter::open_bytes(bytes).unwrap();
    let mut reloaded = Workbook::from_reader(adapter, LoadStrategy::EagerAll, config).unwrap();

    let reloaded_value = reloaded.get_value("S", 1, 1);
    assert!(
        matches!(reloaded_value, Some(LiteralValue::Number(n)) if (n - expected_serial).abs() < 1e-9),
        "temporal value must reload as serial {expected_serial}, got {reloaded_value:?}"
    );
    assert_eq!(
        reloaded.evaluate_cell("S", 1, 2).unwrap(),
        LiteralValue::Number(expected_serial + 1.0)
    );
    assert_eq!(
        reloaded.evaluate_cell("S", 1, 3).unwrap(),
        LiteralValue::Number(expected_serial)
    );
    assert_eq!(
        reloaded.evaluate_cell("S", 1, 4).unwrap(),
        LiteralValue::Boolean(true)
    );
}

#[test]
fn date_saved_via_to_xlsx_bytes_reloads_as_number_not_text() {
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();
    roundtrip_and_assert_numeric_arithmetic(
        LiteralValue::Date(date),
        45_627.0,
        WorkbookConfig::ephemeral(),
    );
}

#[test]
fn datetime_saved_via_to_xlsx_bytes_reloads_as_number_not_text() {
    let datetime = NaiveDate::from_ymd_opt(2024, 12, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    roundtrip_and_assert_numeric_arithmetic(
        LiteralValue::DateTime(datetime),
        45_627.5,
        WorkbookConfig::ephemeral(),
    );
}

#[test]
fn time_saved_via_to_xlsx_bytes_reloads_as_number_not_text() {
    let time = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    roundtrip_and_assert_numeric_arithmetic(
        LiteralValue::Time(time),
        0.5,
        WorkbookConfig::ephemeral(),
    );
}

#[test]
fn duration_saved_via_to_xlsx_bytes_reloads_as_number_not_text() {
    roundtrip_and_assert_numeric_arithmetic(
        LiteralValue::Duration(Duration::hours(36)),
        1.5,
        WorkbookConfig::ephemeral(),
    );
}

#[test]
fn date_saved_via_to_xlsx_bytes_uses_excel_1904_serial() {
    let mut config = WorkbookConfig::ephemeral();
    config.eval.date_system = DateSystem::Excel1904;
    let date = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();
    roundtrip_and_assert_numeric_arithmetic(LiteralValue::Date(date), 44_165.0, config);
}
