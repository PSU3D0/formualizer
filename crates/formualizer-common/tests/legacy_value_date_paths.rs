use chrono::{NaiveDate, NaiveDateTime};

fn datetime(year: i32, month: u32, day: u32, hour: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_opt(hour, 0, 0)
        .unwrap()
}

#[test]
fn legacy_value_date_paths_forward_to_excel_1900_compatibility_api() {
    let modern = datetime(2024, 1, 15, 12);
    assert_eq!(
        formualizer_common::value::datetime_to_serial(&modern),
        45_306.5
    );
    assert_eq!(
        formualizer_common::value::datetime_to_serial(&modern),
        formualizer_common::datetime_to_serial(&modern)
    );
    assert_eq!(
        formualizer_common::value::serial_to_datetime(45_306.5),
        modern
    );
    assert_eq!(
        formualizer_common::value::serial_to_datetime(45_306.5),
        formualizer_common::serial_to_datetime(45_306.5)
    );
}

#[test]
fn legacy_value_serial_60_keeps_historical_representable_alias() {
    let represented_phantom = datetime(1900, 2, 28, 0);
    assert_eq!(
        formualizer_common::value::serial_to_datetime(60.0),
        represented_phantom
    );
    assert_eq!(
        formualizer_common::value::datetime_to_serial(&represented_phantom),
        59.0
    );
}

#[test]
fn legacy_value_path_uses_canonical_end_of_day_carry() {
    let rounds_to_next_day = 1.0 + 86_399.6 / 86_400.0;
    assert_eq!(
        formualizer_common::value::serial_to_datetime(rounds_to_next_day),
        datetime(1900, 1, 2, 0)
    );
}
