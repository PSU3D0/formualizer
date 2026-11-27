//! Regression tests for v0.16.0 Step 3: sub-second temporal precision across
//! `Time`, `DateTime`, and `Duration` over `set_value`, `set_values`,
//! `set_columns`, and `set_cells` (with changelog both off and on).

use chrono::{Duration, NaiveDate, NaiveTime};
use formualizer_common::{
    DateSystem, LiteralValue, datetime_to_serial_for, days_fraction_to_duration,
    duration_to_days_fraction, time_to_fraction,
};
use formualizer_workbook::{
    BulkCellTag, CellBatch, ColumnData, FormatId, Workbook, WorkbookConfig,
};

fn build_workbook(date_system: DateSystem, enable_changelog: bool) -> Workbook {
    let mut cfg = WorkbookConfig::interactive().with_changelog(enable_changelog);
    cfg.eval.date_system = date_system;
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("S").unwrap();
    wb
}

#[test]
fn subsecond_time_datetime_and_duration_roundtrip_across_all_write_paths() {
    let time_val = NaiveTime::from_hms_milli_opt(12, 0, 0, 500).unwrap();
    let datetime_val = NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_milli_opt(12, 0, 0, 500)
        .unwrap();
    let dur_days = 0.123456789_f64;
    let dur_val = days_fraction_to_duration(dur_days).unwrap();
    assert_eq!(dur_val, Duration::nanoseconds(10_666_666_569_600));
    assert_eq!(duration_to_days_fraction(&dur_val), dur_days);

    let expected_time = Some(LiteralValue::Time(time_val));
    let expected_dt = Some(LiteralValue::DateTime(datetime_val));
    let expected_dur = Some(LiteralValue::Duration(dur_val));

    for date_system in [DateSystem::Excel1900, DateSystem::Excel1904] {
        let time_serial = time_to_fraction(&time_val);
        let dt_serial = datetime_to_serial_for(date_system, &datetime_val);

        for changelog in [false, true] {
            let label = format!("date_system={date_system:?}, changelog={changelog}");

            // 1. set_value
            let mut wb = build_workbook(date_system, changelog);
            wb.set_value("S", 1, 1, LiteralValue::Time(time_val))
                .unwrap();
            wb.set_value("S", 2, 1, LiteralValue::DateTime(datetime_val))
                .unwrap();
            wb.set_value("S", 3, 1, LiteralValue::Duration(dur_val))
                .unwrap();
            assert_eq!(
                wb.get_value("S", 1, 1),
                expected_time,
                "set_value time ({label})"
            );
            assert_eq!(
                wb.get_value("S", 2, 1),
                expected_dt,
                "set_value datetime ({label})"
            );
            assert_eq!(
                wb.get_value("S", 3, 1),
                expected_dur,
                "set_value duration ({label})"
            );

            // 2. set_values
            let mut wb = build_workbook(date_system, changelog);
            wb.set_values(
                "S",
                1,
                1,
                &[
                    vec![LiteralValue::Time(time_val)],
                    vec![LiteralValue::DateTime(datetime_val)],
                    vec![LiteralValue::Duration(dur_val)],
                ],
            )
            .unwrap();
            assert_eq!(
                wb.get_value("S", 1, 1),
                expected_time,
                "set_values time ({label})"
            );
            assert_eq!(
                wb.get_value("S", 2, 1),
                expected_dt,
                "set_values datetime ({label})"
            );
            assert_eq!(
                wb.get_value("S", 3, 1),
                expected_dur,
                "set_values duration ({label})"
            );

            // 3. set_columns (Serial columns for Time, DateTime, Duration)
            let mut wb = build_workbook(date_system, changelog);
            let time_col = [time_serial];
            let dt_col = [dt_serial];
            let dur_col = [dur_days];
            wb.set_columns(
                "S",
                1,
                1,
                &[
                    ColumnData::Serial {
                        values: &time_col,
                        format: FormatId::TIME,
                        validity: None,
                    },
                    ColumnData::Serial {
                        values: &dt_col,
                        format: FormatId::DATETIME,
                        validity: None,
                    },
                    ColumnData::Serial {
                        values: &dur_col,
                        format: FormatId::DURATION,
                        validity: None,
                    },
                ],
            )
            .unwrap();
            assert_eq!(
                wb.get_value("S", 1, 1),
                expected_time,
                "set_columns time ({label})"
            );
            assert_eq!(
                wb.get_value("S", 1, 2),
                expected_dt,
                "set_columns datetime ({label})"
            );
            assert_eq!(
                wb.get_value("S", 1, 3),
                expected_dur,
                "set_columns duration ({label})"
            );

            // When changelog is enabled, undo/redo must also preserve sub-second precision.
            if changelog {
                wb.undo().unwrap();
                assert_eq!(wb.get_value("S", 1, 1), None, "undo clears time ({label})");
                assert_eq!(
                    wb.get_value("S", 1, 2),
                    None,
                    "undo clears datetime ({label})"
                );
                assert_eq!(
                    wb.get_value("S", 1, 3),
                    None,
                    "undo clears duration ({label})"
                );
                wb.redo().unwrap();
                assert_eq!(
                    wb.get_value("S", 1, 1),
                    expected_time,
                    "redo time ({label})"
                );
                assert_eq!(
                    wb.get_value("S", 1, 2),
                    expected_dt,
                    "redo datetime ({label})"
                );
                assert_eq!(
                    wb.get_value("S", 1, 3),
                    expected_dur,
                    "redo duration ({label})"
                );
            }

            // 4. set_cells (explicit wire DateTime, Duration, and Time tags)
            let mut wb = build_workbook(date_system, changelog);
            let batch = CellBatch {
                rows: &[1, 2, 3],
                cols: &[1, 1, 1],
                tags: &[
                    BulkCellTag::DateTime as u8,
                    BulkCellTag::Duration as u8,
                    BulkCellTag::Time as u8,
                ],
                numbers: &[dt_serial, dur_days, time_serial],
                text_bytes: &[],
                text_offsets: &[],
            };
            wb.set_cells("S", &batch).unwrap();
            assert_eq!(
                wb.get_value("S", 1, 1),
                expected_dt,
                "set_cells datetime ({label})"
            );
            assert_eq!(
                wb.get_value("S", 2, 1),
                expected_dur,
                "set_cells duration ({label})"
            );
            assert_eq!(
                wb.get_value("S", 3, 1),
                expected_time,
                "set_cells time ({label})"
            );

            // Also verify fractional-only serial under BulkCellTag::DateTime matches
            // identically between fast & logged paths.
            let mut wb_time_fast = build_workbook(date_system, false);
            let mut wb_time_logged = build_workbook(date_system, true);
            let time_batch = CellBatch {
                rows: &[1],
                cols: &[1],
                tags: &[BulkCellTag::DateTime as u8],
                numbers: &[time_serial],
                text_bytes: &[],
                text_offsets: &[],
            };
            wb_time_fast.set_cells("S", &time_batch).unwrap();
            wb_time_logged.set_cells("S", &time_batch).unwrap();
            assert_eq!(
                wb_time_fast.get_value("S", 1, 1),
                wb_time_logged.get_value("S", 1, 1),
                "set_cells fractional-day parity ({label})"
            );
        }
    }
}

#[test]
fn subsecond_negative_and_microsecond_temporals_roundtrip() {
    let dt_us = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_micro_opt(8, 30, 45, 123_456)
        .unwrap();
    let time_ns = NaiveTime::from_hms_nano_opt(23, 59, 59, 987_654_321).unwrap();
    let neg_dur = Duration::milliseconds(-1_250);

    for changelog in [false, true] {
        let mut wb = build_workbook(DateSystem::Excel1900, changelog);
        wb.set_values(
            "S",
            1,
            1,
            &[
                vec![LiteralValue::DateTime(dt_us)],
                vec![LiteralValue::Time(time_ns)],
                vec![LiteralValue::Duration(neg_dur)],
            ],
        )
        .unwrap();
        assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::DateTime(dt_us)));
        assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Time(time_ns)));
        assert_eq!(
            wb.get_value("S", 3, 1),
            Some(LiteralValue::Duration(neg_dur))
        );
    }
}

#[test]
fn datetime_max_stays_datetime_and_year_evaluates_to_9999() {
    let dt_max = NaiveDate::from_ymd_opt(9999, 12, 31)
        .unwrap()
        .and_hms_micro_opt(23, 59, 59, 999_999)
        .unwrap();

    for date_system in [DateSystem::Excel1900, DateSystem::Excel1904] {
        for changelog in [false, true] {
            let mut wb = build_workbook(date_system, changelog);
            wb.set_value("S", 1, 1, LiteralValue::DateTime(dt_max))
                .unwrap();
            wb.set_formula("S", 1, 2, "=YEAR(A1)").unwrap();

            match wb.get_value("S", 1, 1) {
                Some(LiteralValue::DateTime(got)) => {
                    assert_eq!(
                        got.date(),
                        NaiveDate::from_ymd_opt(9999, 12, 31).unwrap(),
                        "date_system={date_system:?} changelog={changelog}"
                    );
                }
                other => panic!(
                    "expected DateTime for datetime.max (date_system={date_system:?}, changelog={changelog}), got {other:?}"
                ),
            }

            let year = wb.evaluate_cell("S", 1, 2).unwrap();
            assert_eq!(
                year,
                LiteralValue::Number(9999.0),
                "YEAR(datetime.max) failed for date_system={date_system:?} changelog={changelog}"
            );
        }
    }
}
