use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use formualizer_common::{DateSystem, ExcelErrorKind, LiteralValue};
use formualizer_eval::builtins::datetime::{
    create_date_normalized, date_to_serial, date_to_serial_for, datetime_to_serial,
    datetime_to_serial_for, serial_to_date, serial_to_datetime, serial_to_datetime_for,
    time_to_fraction,
};

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

fn datetime(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> NaiveDateTime {
    date(year, month, day)
        .and_hms_opt(hour, minute, second)
        .unwrap()
}

fn assert_num<T>(result: Result<T, formualizer_common::ExcelError>) {
    match result {
        Ok(_) => panic!("expected #NUM!"),
        Err(error) => assert_eq!(error.kind, ExcelErrorKind::Num),
    }
}

#[test]
fn released_decode_wrappers_keep_hardcoded_excel_1900_behavior() {
    assert_eq!(serial_to_date(0.0).unwrap(), date(1899, 12, 31));
    assert_eq!(serial_to_date(59.0).unwrap(), date(1900, 2, 28));
    assert_eq!(serial_to_date(60.0).unwrap(), date(1900, 2, 28));
    assert_eq!(serial_to_date(61.0).unwrap(), date(1900, 3, 1));
    assert_eq!(serial_to_date(-0.25).unwrap(), date(1899, 12, 31));
    assert_eq!(serial_to_date(-0.0).unwrap(), date(1899, 12, 31));
    assert_eq!(serial_to_date(f64::NAN).unwrap(), date(1899, 12, 31));

    assert_eq!(
        serial_to_datetime(0.0).unwrap(),
        datetime(1899, 12, 31, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime(-0.25).unwrap(),
        datetime(1899, 12, 31, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime(-0.0).unwrap(),
        datetime(1899, 12, 31, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime(f64::NAN).unwrap(),
        datetime(1899, 12, 31, 0, 0, 0)
    );
    for serial in [-1.0, -2.0, -1.25] {
        assert_num(serial_to_date(serial));
        assert_num(serial_to_datetime(serial));
    }

    let near_midnight = 86_399.4 / 86_400.0;
    let rounded_to_midnight = 86_399.6 / 86_400.0;
    assert_eq!(
        serial_to_datetime(59.0 + near_midnight).unwrap(),
        datetime(1900, 2, 28, 23, 59, 59)
    );
    assert_eq!(
        serial_to_datetime(59.0 + rounded_to_midnight).unwrap(),
        datetime(1900, 2, 28, 23, 0, 0)
    );
    assert_eq!(
        serial_to_datetime(60.0 + rounded_to_midnight).unwrap(),
        datetime(1900, 2, 28, 23, 0, 0)
    );
    assert_eq!(
        serial_to_datetime(61.0 + near_midnight).unwrap(),
        datetime(1900, 3, 1, 23, 59, 59)
    );
    assert_eq!(
        serial_to_datetime(61.0 + rounded_to_midnight).unwrap(),
        datetime(1900, 3, 1, 23, 0, 0)
    );
}

#[test]
fn released_decode_wrappers_keep_hardcoded_excel_1904_behavior() {
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, -1.25).unwrap(),
        datetime(1903, 12, 31, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, -1.0).unwrap(),
        datetime(1903, 12, 31, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, -0.25).unwrap(),
        datetime(1904, 1, 1, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, -0.0).unwrap(),
        datetime(1904, 1, 1, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, 0.0).unwrap(),
        datetime(1904, 1, 1, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, 59.0).unwrap(),
        datetime(1904, 2, 29, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, 60.0).unwrap(),
        datetime(1904, 3, 1, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, 61.0).unwrap(),
        datetime(1904, 3, 2, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, 59.0 + 86_399.4 / 86_400.0).unwrap(),
        datetime(1904, 2, 29, 23, 59, 59)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, 59.0 + 86_399.6 / 86_400.0).unwrap(),
        datetime(1904, 2, 29, 23, 0, 0)
    );
    assert_num(serial_to_datetime_for(DateSystem::Excel1904, f64::NAN));
}

#[test]
fn decode_wrappers_support_chrono_year_9999_and_10000_and_harden_overflow() {
    assert_eq!(
        serial_to_datetime(2_958_465.0).unwrap(),
        datetime(9999, 12, 31, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime(2_958_466.0).unwrap(),
        datetime(10000, 1, 1, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, 2_957_003.0).unwrap(),
        datetime(9999, 12, 31, 0, 0, 0)
    );
    assert_eq!(
        serial_to_datetime_for(DateSystem::Excel1904, 2_957_004.0).unwrap(),
        datetime(10000, 1, 1, 0, 0, 0)
    );

    for serial in [f64::INFINITY, f64::NEG_INFINITY, f64::MAX, f64::MIN] {
        assert_num(serial_to_date(serial));
        assert_num(serial_to_datetime(serial));
        assert_num(serial_to_datetime_for(DateSystem::Excel1900, serial));
        assert_num(serial_to_datetime_for(DateSystem::Excel1904, serial));
    }
}

#[test]
fn released_encode_helpers_and_normalization_remain_public() {
    let _: fn(f64) -> Result<NaiveDate, formualizer_common::ExcelError> = serial_to_date;
    let _: fn(&NaiveDate) -> f64 = date_to_serial;
    let _: fn(f64) -> Result<NaiveDateTime, formualizer_common::ExcelError> = serial_to_datetime;
    let _: fn(&NaiveDateTime) -> f64 = datetime_to_serial;
    let _: fn(DateSystem, &NaiveDate) -> f64 = date_to_serial_for;
    let _: fn(DateSystem, &NaiveDateTime) -> f64 = datetime_to_serial_for;
    let _: fn(DateSystem, f64) -> Result<NaiveDateTime, formualizer_common::ExcelError> =
        serial_to_datetime_for;
    let _: fn(&NaiveTime) -> f64 = time_to_fraction;
    let _: fn(i32, i32, i32) -> Result<NaiveDate, formualizer_common::ExcelError> =
        create_date_normalized;

    let noon_1900 = datetime(1900, 3, 1, 12, 0, 0);
    let noon_1904 = datetime(1904, 1, 1, 12, 0, 0);
    assert_eq!(date_to_serial(&date(1900, 3, 1)), 61.0);
    assert_eq!(datetime_to_serial(&noon_1900), 61.5);
    assert_eq!(
        date_to_serial_for(DateSystem::Excel1904, &date(1904, 1, 1)),
        0.0
    );
    assert_eq!(
        datetime_to_serial_for(DateSystem::Excel1904, &noon_1904),
        0.5
    );
    assert_eq!(
        time_to_fraction(&NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
        0.5
    );
    assert_eq!(
        create_date_normalized(2024, 13, 5).unwrap(),
        date(2025, 1, 5)
    );
    assert_eq!(
        create_date_normalized(2024, 0, 15).unwrap(),
        date(2023, 12, 15)
    );
    assert_num(create_date_normalized(0, 0, 1));
}

#[test]
fn sparse_public_constructors_keep_temporals_as_serials_internally() {
    use formualizer_eval::arrow_store::{ArrowSheet, OverlayValue};
    use formualizer_eval::format::FormatId;
    let mut excel_1900 = ArrowSheet::new_sparse("1900", 1, 1, 16);
    excel_1900.set_sparse_overlay_value(0, 0, OverlayValue::DateTime(1462.5));
    excel_1900.set_sparse_overlay_format(0, 0, Some(FormatId::DATETIME));
    assert_eq!(
        excel_1900.get_cell_value(0, 0),
        LiteralValue::Number(1462.5)
    );
    assert_eq!(excel_1900.format_id(0, 0), Some(FormatId::DATETIME));

    let mut excel_1904 =
        ArrowSheet::new_sparse_with_date_system("1904", 1, 1, 16, DateSystem::Excel1904);
    excel_1904.set_sparse_overlay_value(0, 0, OverlayValue::DateTime(0.5));
    excel_1904.set_sparse_overlay_format(0, 0, Some(FormatId::DATETIME));
    assert_eq!(excel_1904.get_cell_value(0, 0), LiteralValue::Number(0.5));
    assert_eq!(excel_1904.format_id(0, 0), Some(FormatId::DATETIME));
}
