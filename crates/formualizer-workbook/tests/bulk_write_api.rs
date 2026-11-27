//! Integration tests for `Workbook` bulk write API (`v0.16.0` Phase 1):
//! `set_columns`, `set_cells`, `set_values` -> `WriteRangeReport`, Q2 changelog
//! policy, `WorksheetHandle` forwarding, limits/external-sheet guards, and
//! opt-in `arrow` feature (`ColumnData::try_from_arrow`, `set_record_batches`).

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_workbook::{
    BulkCellTag, CellBatch, ColumnData, FormatId, PackedColumnBlock, Workbook, WorkbookConfig,
    WorkbookLoadLimits, WriteRangeReport,
};
use std::sync::{Arc, RwLock};

#[test]
fn set_values_set_columns_set_cells_return_exact_write_range_reports() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    wb.add_sheet("S").unwrap();

    // Empty writes return zeroed report.
    assert_eq!(
        wb.set_values("S", 1, 1, &[]).unwrap(),
        WriteRangeReport::default()
    );
    assert_eq!(
        wb.set_values("S", 1, 1, &[vec![], vec![]]).unwrap(),
        WriteRangeReport::default()
    );
    assert_eq!(
        wb.set_columns("S", 1, 1, &[]).unwrap(),
        WriteRangeReport::default()
    );
    assert_eq!(
        wb.set_columns(
            "S",
            1,
            1,
            &[ColumnData::Float64 {
                values: &[],
                validity: None
            }]
        )
        .unwrap(),
        WriteRangeReport::default()
    );
    let empty_batch = CellBatch {
        rows: &[],
        cols: &[],
        tags: &[],
        numbers: &[],
        text_bytes: &[],
        text_offsets: &[],
    };
    assert_eq!(
        wb.set_cells("S", &empty_batch).unwrap(),
        WriteRangeReport::default()
    );

    // Dense set_values with trailing empty row.
    let rep_vals = wb
        .set_values(
            "S",
            2,
            3,
            &[
                vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
                vec![LiteralValue::Number(3.0)],
                vec![],
            ],
        )
        .unwrap();
    assert_eq!(
        rep_vals,
        WriteRangeReport {
            start_row: 2,
            start_col: 3,
            end_row: 3,
            end_col: 4,
            history_cleared: false,
        }
    );

    // Dense set_columns with mixed column types.
    let nums = [10.0, 20.0, 30.0];
    let ints = [1i64, 2, 3];
    let bools = [true, false, true];
    let text_bytes = b"foobarbaz";
    let text_offsets = [0u32, 3, 6, 9];
    let dates = [45292.0, 45293.0, 45294.0];
    let cols = [
        ColumnData::Float64 {
            values: &nums,
            validity: None,
        },
        ColumnData::Int64 {
            values: &ints,
            validity: None,
        },
        ColumnData::Boolean {
            values: &bools,
            validity: None,
        },
        ColumnData::Utf8 {
            bytes: text_bytes,
            offsets: &text_offsets,
            validity: None,
        },
        ColumnData::Serial {
            values: &dates,
            format: FormatId::DATE,
            validity: None,
        },
        ColumnData::Null { len: 3 },
    ];
    let rep_cols = wb.set_columns("S", 5, 1, &cols).unwrap();
    assert_eq!(
        rep_cols,
        WriteRangeReport {
            start_row: 5,
            start_col: 1,
            end_row: 7,
            end_col: 6,
            history_cleared: false,
        }
    );
    assert_eq!(wb.get_value("S", 5, 1), Some(LiteralValue::Number(10.0)));
    assert_eq!(wb.get_value("S", 6, 2), Some(LiteralValue::Number(2.0)));
    assert_eq!(wb.get_value("S", 7, 3), Some(LiteralValue::Boolean(true)));
    assert_eq!(
        wb.get_value("S", 6, 4),
        Some(LiteralValue::Text("bar".to_string()))
    );
    assert!(matches!(
        wb.get_value("S", 5, 5),
        Some(LiteralValue::Date(_))
    ));
    assert!(matches!(
        wb.get_value("S", 5, 6),
        None | Some(LiteralValue::Empty)
    ));

    // Sparse set_cells with duplicate coordinates (last-write-wins).
    let sparse_bytes = b"winner";
    let sparse_offsets = [0u32, 6];
    let batch = CellBatch {
        rows: &[10, 10, 12],
        cols: &[2, 2, 4],
        tags: &[
            BulkCellTag::Number as u8,
            BulkCellTag::Text as u8,
            BulkCellTag::Number as u8,
        ],
        numbers: &[1.0, 0.0, -0.0],
        text_bytes: sparse_bytes,
        text_offsets: &sparse_offsets,
    };
    let rep_cells = wb.set_cells("S", &batch).unwrap();
    assert_eq!(
        rep_cells,
        WriteRangeReport {
            start_row: 10,
            start_col: 2,
            end_row: 12,
            end_col: 4,
            history_cleared: false,
        }
    );
    assert_eq!(
        wb.get_value("S", 10, 2),
        Some(LiteralValue::Text("winner".to_string()))
    );
    match wb.get_value("S", 12, 4) {
        Some(LiteralValue::Number(n)) => assert_eq!(n.to_bits(), (-0.0f64).to_bits()),
        other => panic!("expected -0.0 preserved bit-for-bit, got {other:?}"),
    }
}

#[test]
fn q2_changelog_policy_small_undoable_and_oversized_clears_history() {
    // Cap = 10 events.
    let mut wb = Workbook::new_with_config(
        WorkbookConfig::interactive()
            .with_changelog(true)
            .with_history_limit(Some(10)),
    );
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
    assert!(wb.can_undo());

    // Small set_columns (4 cells + 2 compound markers = 6 <= 10): logged & undoable!
    let small_vals = [10.0, 20.0, 30.0, 40.0];
    let rep_small = wb
        .set_columns(
            "S",
            1,
            1,
            &[ColumnData::Float64 {
                values: &small_vals,
                validity: None,
            }],
        )
        .unwrap();
    assert!(!rep_small.history_cleared);
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(10.0)));
    assert_eq!(wb.get_value("S", 4, 1), Some(LiteralValue::Number(40.0)));

    // Single undo reverts the entire set_columns compound!
    wb.undo().unwrap();
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(1.0)));
    assert!(matches!(
        wb.get_value("S", 4, 1),
        None | Some(LiteralValue::Empty)
    ));
    wb.redo().unwrap();
    assert_eq!(wb.get_value("S", 4, 1), Some(LiteralValue::Number(40.0)));

    // Small set_cells (3 cells + 2 = 5 <= 10): logged & undoable!
    let small_batch = CellBatch {
        rows: &[1, 2, 3],
        cols: &[2, 2, 2],
        tags: &[BulkCellTag::Number as u8; 3],
        numbers: &[7.0, 8.0, 9.0],
        text_bytes: &[],
        text_offsets: &[],
    };
    let rep_small_cells = wb.set_cells("S", &small_batch).unwrap();
    assert!(!rep_small_cells.history_cleared);
    assert_eq!(wb.get_value("S", 2, 2), Some(LiteralValue::Number(8.0)));
    wb.undo().unwrap();
    assert!(matches!(
        wb.get_value("S", 2, 2),
        None | Some(LiteralValue::Empty)
    ));

    // B2: Serial column with temporal formats (DATE, DATETIME, TIME, DURATION) must
    // survive undo + redo on the logged path without losing its temporal type.
    let rep_serial = wb
        .set_columns(
            "S",
            1,
            3,
            &[
                ColumnData::Serial {
                    values: &[45_292.0], // 2024-01-01
                    format: FormatId::DATE,
                    validity: None,
                },
                ColumnData::Serial {
                    values: &[45_292.5], // 2024-01-01 12:00:00
                    format: FormatId::DATETIME,
                    validity: None,
                },
                ColumnData::Serial {
                    values: &[0.5], // 12:00:00
                    format: FormatId::TIME,
                    validity: None,
                },
                ColumnData::Serial {
                    values: &[1.5], // 36h
                    format: FormatId::DURATION,
                    validity: None,
                },
            ],
        )
        .unwrap();
    assert!(!rep_serial.history_cleared);
    let expected_date = Some(LiteralValue::Date(
        chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
    ));
    let expected_dt = Some(LiteralValue::DateTime(
        chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    ));
    let expected_time = Some(LiteralValue::Time(
        chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
    ));
    let expected_dur = Some(LiteralValue::Duration(chrono::Duration::hours(36)));
    assert_eq!(wb.get_value("S", 1, 3), expected_date);
    assert_eq!(wb.get_value("S", 1, 4), expected_dt);
    assert_eq!(wb.get_value("S", 1, 5), expected_time);
    assert_eq!(wb.get_value("S", 1, 6), expected_dur);

    wb.undo().unwrap();
    assert!(matches!(
        wb.get_value("S", 1, 3),
        None | Some(LiteralValue::Empty)
    ));
    assert!(matches!(
        wb.get_value("S", 1, 4),
        None | Some(LiteralValue::Empty)
    ));

    wb.redo().unwrap();
    assert_eq!(
        wb.get_value("S", 1, 3),
        expected_date,
        "Serial DATE must survive redo as Date"
    );
    assert_eq!(
        wb.get_value("S", 1, 4),
        expected_dt,
        "Serial DATETIME must survive redo as DateTime"
    );
    assert_eq!(
        wb.get_value("S", 1, 5),
        expected_time,
        "Serial TIME must survive redo as Time"
    );
    assert_eq!(
        wb.get_value("S", 1, 6),
        expected_dur,
        "Serial DURATION must survive redo as Duration"
    );

    // Oversized set_columns (20 cells > cap 10): takes fast path, clears history!
    let big_vals: Vec<f64> = (1..=20).map(|i| i as f64).collect();
    let rep_big = wb
        .set_columns(
            "S",
            1,
            1,
            &[ColumnData::Float64 {
                values: &big_vals,
                validity: None,
            }],
        )
        .unwrap();
    assert!(rep_big.history_cleared);
    assert!(!wb.can_undo());
    assert!(!wb.can_redo());
    assert_eq!(wb.get_value("S", 20, 1), Some(LiteralValue::Number(20.0)));

    // Oversized set_values (15 cells > cap 10) after a new logged edit: clears history!
    wb.set_value("S", 1, 3, LiteralValue::Number(5.0)).unwrap();
    assert!(wb.can_undo());
    let big_rows = vec![vec![LiteralValue::Number(99.0)]; 15];
    let rep_big_vals = wb.set_values("S", 1, 4, &big_rows).unwrap();
    assert!(rep_big_vals.history_cleared);
    assert!(!wb.can_undo());
    assert_eq!(wb.get_value("S", 15, 4), Some(LiteralValue::Number(99.0)));
}

#[test]
fn s1_set_values_logged_single_compound_and_history_limit_boundary() {
    // (a) set_values of 8 cells with changelog on undoes ALL 8 in ONE undo().
    let mut wb = Workbook::new_with_config(
        WorkbookConfig::interactive()
            .with_changelog(true)
            .with_history_limit(Some(10)),
    );
    wb.add_sheet("S").unwrap();
    let rows8: Vec<Vec<LiteralValue>> = (1..=8)
        .map(|i| vec![LiteralValue::Number(i as f64)])
        .collect();
    let rep8 = wb.set_values("S", 1, 1, &rows8).unwrap();
    assert!(!rep8.history_cleared);
    assert!(wb.can_undo());
    for r in 1..=8u32 {
        assert_eq!(
            wb.get_value("S", r, 1),
            Some(LiteralValue::Number(r as f64))
        );
    }
    wb.undo().unwrap();
    for r in 1..=8u32 {
        assert!(
            matches!(wb.get_value("S", r, 1), None | Some(LiteralValue::Empty)),
            "row {r} must be reverted by single undo()"
        );
    }
    wb.redo().unwrap();
    for r in 1..=8u32 {
        assert_eq!(
            wb.get_value("S", r, 1),
            Some(LiteralValue::Number(r as f64))
        );
    }

    // (b) begin_action(); set_values(n); end_action() with history_limit = 10:
    //     history_cleared == false MUST imply can_undo() == true; n = 9 and n = 10
    //     cannot fit with compound markers and must cleanly clear history.
    for n in [8usize, 9, 10] {
        let mut wb = Workbook::new_with_config(
            WorkbookConfig::interactive()
                .with_changelog(true)
                .with_history_limit(Some(10)),
        );
        wb.add_sheet("S").unwrap();
        let rows: Vec<Vec<LiteralValue>> = (1..=n)
            .map(|i| vec![LiteralValue::Number(i as f64)])
            .collect();

        wb.begin_action("outer");
        let rep = wb.set_values("S", 1, 1, &rows).unwrap();
        wb.end_action();

        if n == 8 {
            assert!(!rep.history_cleared, "n=8 should fit in limit=10");
            assert!(wb.can_undo(), "n=8 must remain undoable");
            wb.undo().unwrap();
            for r in 1..=8u32 {
                assert!(matches!(
                    wb.get_value("S", r, 1),
                    None | Some(LiteralValue::Empty)
                ));
            }
        } else {
            assert!(
                rep.history_cleared || wb.can_undo(),
                "n={n}: history_cleared=false must never lie when compound start would be evicted"
            );
            assert!(
                rep.history_cleared,
                "n={n} with compound markers exceeds limit=10 and must clear history"
            );
            assert!(!wb.can_undo());
        }
    }

    // Also verify prior events inside an enclosing begin_action() are counted across
    // set_values, set_columns, and set_cells.
    {
        let mut wb = Workbook::new_with_config(
            WorkbookConfig::interactive()
                .with_changelog(true)
                .with_history_limit(Some(10)),
        );
        wb.add_sheet("S").unwrap();
        wb.begin_action("outer-with-prior");
        wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap(); // 2 events in open group
        // 2 existing + 7 new + 1 End = 10 <= 10 -> fits!
        let vals7 = [2.0; 7];
        let rep7 = wb
            .set_columns(
                "S",
                2,
                1,
                &[ColumnData::Float64 {
                    values: &vals7,
                    validity: None,
                }],
            )
            .unwrap();
        wb.end_action();
        assert!(!rep7.history_cleared);
        assert!(wb.can_undo());
        wb.undo().unwrap();
        assert!(matches!(
            wb.get_value("S", 1, 1),
            None | Some(LiteralValue::Empty)
        ));
        assert!(matches!(
            wb.get_value("S", 8, 1),
            None | Some(LiteralValue::Empty)
        ));

        // 2 existing + 8 new + 1 End = 11 > 10 -> clears history cleanly!
        wb.begin_action("outer-with-prior-overflow");
        wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
        let rows_c = [2u32, 3, 4, 5, 6, 7, 8, 9];
        let cols_c = [1u32; 8];
        let tags_c = [BulkCellTag::Number as u8; 8];
        let nums_c = [9.0; 8];
        let batch8 = CellBatch {
            rows: &rows_c,
            cols: &cols_c,
            tags: &tags_c,
            numbers: &nums_c,
            text_bytes: &[],
            text_offsets: &[],
        };
        let rep8 = wb.set_cells("S", &batch8).unwrap();
        wb.end_action();
        assert!(rep8.history_cleared);
        assert!(!wb.can_undo());
    }
}

#[test]
fn q2_changelog_disabled_reports_history_cleared_only_when_clearing_action_journal() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    wb.add_sheet("S").unwrap();

    // No prior history -> history_cleared = false.
    let rep1 = wb
        .set_columns(
            "S",
            1,
            1,
            &[ColumnData::Float64 {
                values: &[1.0, 2.0],
                validity: None,
            }],
        )
        .unwrap();
    assert!(!rep1.history_cleared);

    // Record an explicit action journal -> can_undo = true.
    wb.action("edit", |tx| {
        tx.set_value("S", 1, 2, LiteralValue::Number(42.0))
    })
    .unwrap();
    assert!(wb.can_undo());

    // Subsequent unjournaled bulk write clears the action journal -> history_cleared = true!
    let rep2 = wb
        .set_columns(
            "S",
            1,
            1,
            &[ColumnData::Float64 {
                values: &[3.0, 4.0],
                validity: None,
            }],
        )
        .unwrap();
    assert!(rep2.history_cleared);
    assert!(!wb.can_undo());
}

#[test]
fn worksheet_handle_forwards_set_columns_set_cells_and_set_values() {
    let wb = Arc::new(RwLock::new(Workbook::new_with_config(
        WorkbookConfig::interactive().with_changelog(false),
    )));
    wb.write().unwrap().add_sheet("S").unwrap();
    let handle = formualizer_workbook::WorksheetHandle::new(wb.clone(), "S");

    let rep = handle
        .set_columns(
            1,
            1,
            &[ColumnData::Float64 {
                values: &[1.5, 2.5],
                validity: None,
            }],
        )
        .unwrap();
    assert_eq!(rep.end_row, 2);

    let batch = CellBatch {
        rows: &[3],
        cols: &[2],
        tags: &[BulkCellTag::Boolean as u8],
        numbers: &[1.0],
        text_bytes: &[],
        text_offsets: &[],
    };
    let rep2 = handle.set_cells(&batch).unwrap();
    assert_eq!(rep2.start_row, 3);
    assert_eq!(handle.get_value(3, 2), Some(LiteralValue::Boolean(true)));
}

#[test]
fn external_sheet_and_write_limits_rejected_on_set_columns_and_set_cells() {
    let limits = WorkbookLoadLimits {
        max_sheet_rows: 10,
        max_sheet_cols: 5,
        max_sheet_logical_cells: 12,
        ..WorkbookLoadLimits::default()
    };
    let mut wb = Workbook::new_with_config(
        WorkbookConfig::interactive()
            .with_changelog(false)
            .with_ingest_limits(limits),
    );
    wb.add_sheet("S").unwrap();

    // Cached external sheet rejected with #REF!.
    let err = wb
        .set_columns(
            "[1]Ext",
            1,
            1,
            &[ColumnData::Float64 {
                values: &[1.0],
                validity: None,
            }],
        )
        .unwrap_err();
    assert!(err.to_string().contains("read-only"));

    let batch = CellBatch {
        rows: &[1],
        cols: &[1],
        tags: &[BulkCellTag::Number as u8],
        numbers: &[1.0],
        text_bytes: &[],
        text_offsets: &[],
    };
    assert!(wb.set_cells("[1]Ext", &batch).is_err());

    // Limits enforced before allocation.
    assert!(
        wb.set_columns(
            "S",
            11,
            1,
            &[ColumnData::Float64 {
                values: &[1.0],
                validity: None,
            }]
        )
        .is_err()
    );
    assert!(
        wb.set_columns(
            "S",
            1,
            1,
            &[
                ColumnData::Float64 {
                    values: &[1.0; 7],
                    validity: None,
                },
                ColumnData::Float64 {
                    values: &[2.0; 7],
                    validity: None,
                },
            ]
        )
        .is_err()
    );
    assert_eq!(wb.sheet_dimensions("S"), Some((0, 0)));
}

#[test]
fn set_columns_sumifs_keeps_dense_chunks_for_criteria_mask_cache() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    wb.add_sheet("Data").unwrap();
    wb.add_sheet("Summary").unwrap();

    let n = 40_000usize;
    let mut text_bytes = Vec::with_capacity(n * 2);
    let mut text_offsets = Vec::with_capacity(n + 1);
    text_offsets.push(0u32);
    let mut amounts = Vec::with_capacity(n);
    for i in 0..n {
        let key = format!("k{}", i % 10);
        text_bytes.extend_from_slice(key.as_bytes());
        text_offsets.push(text_bytes.len() as u32);
        amounts.push((i % 100) as f64);
    }

    let cols = [
        ColumnData::Utf8 {
            bytes: &text_bytes,
            offsets: &text_offsets,
            validity: None,
        },
        ColumnData::Float64 {
            values: &amounts,
            validity: None,
        },
    ];
    wb.set_columns("Data", 1, 1, &cols).unwrap();
    wb.set_formula(
        "Summary",
        1,
        1,
        r#"=SUMIFS(Data!$B$1:$B$40000,Data!$A$1:$A$40000,"k3")"#,
    )
    .unwrap();
    wb.set_formula("Summary", 2, 1, "=SUM(Data!B:B)").unwrap();
    wb.evaluate_all().unwrap();

    let expected_sumifs: f64 = (0..n)
        .filter(|i| i % 10 == 3)
        .map(|i| (i % 100) as f64)
        .sum();
    let expected_total: f64 = amounts.iter().sum();
    assert_eq!(
        wb.get_value("Summary", 1, 1),
        Some(LiteralValue::Number(expected_sumifs))
    );
    assert_eq!(
        wb.get_value("Summary", 2, 1),
        Some(LiteralValue::Number(expected_total))
    );

    let asheet = wb.engine().sheet_store().sheet("Data").unwrap();
    assert!(asheet.columns[0].sparse_chunks.is_empty());
    assert!(asheet.columns[1].sparse_chunks.is_empty());
    assert_eq!(asheet.columns[0].chunks.len(), 2);
    assert_eq!(asheet.columns[1].chunks.len(), 2);
}

#[test]
fn packed_mixed_column_preserves_all_wire_tags() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    wb.add_sheet("S").unwrap();

    let text_bytes = b"hello";
    let text_offsets = [0u32, 5];
    let tags = [
        BulkCellTag::Empty as u8,
        BulkCellTag::Number as u8,
        BulkCellTag::Boolean as u8,
        BulkCellTag::Text as u8,
        BulkCellTag::Error as u8,
        BulkCellTag::DateTime as u8,
        BulkCellTag::Duration as u8,
    ];
    let numbers = [0.0, 42.5, 1.0, 0.0, 5.0, 45292.0, 0.5];
    let block = PackedColumnBlock {
        tags: &tags,
        numbers: &numbers,
        text_bytes,
        text_offsets: &text_offsets,
    };
    wb.set_columns("S", 1, 1, &[ColumnData::Mixed(block)])
        .unwrap();

    assert!(matches!(
        wb.get_value("S", 1, 1),
        None | Some(LiteralValue::Empty)
    ));
    assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Number(42.5)));
    assert_eq!(wb.get_value("S", 3, 1), Some(LiteralValue::Boolean(true)));
    assert_eq!(
        wb.get_value("S", 4, 1),
        Some(LiteralValue::Text("hello".to_string()))
    );
    assert!(matches!(
        wb.get_value("S", 5, 1),
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Div
    ));
    assert!(matches!(
        wb.get_value("S", 6, 1),
        Some(LiteralValue::Date(_))
    ));
    assert!(matches!(
        wb.get_value("S", 7, 1),
        Some(LiteralValue::Duration(_))
    ));
}

#[cfg(feature = "arrow")]
mod arrow_feature_tests {
    use super::*;
    use arrow_array::types::Int32Type;
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray, Float64Array,
        Int64Array, NullArray, RecordBatch, StringArray, TimestampSecondArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use chrono::NaiveDate;
    use std::sync::Arc;

    #[test]
    fn try_from_arrow_and_set_record_batches_streaming_with_headers() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Float64, true),
            Field::new("count", DataType::Int64, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("day", DataType::Date32, true),
            Field::new(
                "ts_tokyo",
                DataType::Timestamp(TimeUnit::Second, Some("+09:00".into())),
                true,
            ),
            Field::new(
                "cat",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("dec", DataType::Decimal128(10, 2), true),
            Field::new("empty_col", DataType::Null, true),
        ]));

        let make_batch = |offset: i64| -> RecordBatch {
            let dict: DictionaryArray<Int32Type> =
                vec![Some("A"), Some("B"), None].into_iter().collect();
            let dec = Decimal128Array::from(vec![Some(12345), None, Some(-50)])
                .with_precision_and_scale(10, 2)
                .unwrap();
            let ts = TimestampSecondArray::from(vec![
                Some(1_704_067_200), // 2024-01-01 00:00:00 UTC -> 09:00:00 +09:00
                None,
                Some(1_704_067_200 + 3600),
            ])
            .with_timezone("+09:00");
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Float64Array::from(vec![
                        Some(1.5 + offset as f64),
                        None,
                        Some(f64::NAN),
                    ])),
                    Arc::new(Int64Array::from(vec![Some(10 + offset), Some(20), None])),
                    Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                    Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])),
                    Arc::new(Date32Array::from(vec![Some(19_723), None, Some(19_724)])), // 2024-01-01, 2024-01-02
                    Arc::new(ts),
                    Arc::new(dict),
                    Arc::new(dec),
                    Arc::new(NullArray::new(3)),
                ],
            )
            .unwrap()
        };

        let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
        wb.add_sheet("Arrow").unwrap();

        let batches: Vec<Result<RecordBatch, &'static str>> =
            vec![Ok(make_batch(0)), Ok(make_batch(100))];
        let rep = wb.set_record_batches("Arrow", 1, 1, batches, true).unwrap();
        assert_eq!(
            rep,
            WriteRangeReport {
                start_row: 1,
                start_col: 1,
                end_row: 7, // 1 header + 6 data rows
                end_col: 9,
                history_cleared: false,
            }
        );

        // Header row at row 1.
        assert_eq!(
            wb.get_value("Arrow", 1, 1),
            Some(LiteralValue::Text("amount".to_string()))
        );
        assert_eq!(
            wb.get_value("Arrow", 1, 9),
            Some(LiteralValue::Text("empty_col".to_string()))
        );

        // Batch 1 data at rows 2..=4.
        assert_eq!(wb.get_value("Arrow", 2, 1), Some(LiteralValue::Number(1.5)));
        assert!(matches!(
            wb.get_value("Arrow", 3, 1),
            None | Some(LiteralValue::Empty)
        ));
        assert!(matches!(
            wb.get_value("Arrow", 4, 1),
            Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Num
        ));
        assert_eq!(
            wb.get_value("Arrow", 2, 5),
            Some(LiteralValue::Date(
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
            ))
        );
        // +09:00 wall clock -> 2024-01-01 09:00:00
        assert_eq!(
            wb.get_value("Arrow", 2, 6),
            Some(LiteralValue::DateTime(
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap()
            ))
        );
        assert_eq!(
            wb.get_value("Arrow", 2, 7),
            Some(LiteralValue::Text("A".to_string()))
        );
        assert_eq!(
            wb.get_value("Arrow", 2, 8),
            Some(LiteralValue::Number(123.45))
        );

        // Batch 2 data at rows 5..=7.
        assert_eq!(
            wb.get_value("Arrow", 5, 1),
            Some(LiteralValue::Number(101.5))
        );
        assert_eq!(
            wb.get_value("Arrow", 5, 2),
            Some(LiteralValue::Number(110.0))
        );
    }

    #[test]
    fn arrow_preflight_rejects_int64_overflow_iana_tz_and_binary_naming_column() {
        let bad_i64 = Int64Array::from(vec![Some((1i64 << 53) + 1)]);
        let err = ColumnData::try_from_arrow_with_options(
            &bad_i64,
            formualizer_eval::engine::DateSystem::Excel1900,
            Some("user_id"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("user_id"));

        let bad_u64 = UInt64Array::from(vec![Some((1u64 << 53) + 1)]);
        let err = ColumnData::try_from_arrow_with_options(
            &bad_u64,
            formualizer_eval::engine::DateSystem::Excel1900,
            Some("big_u64"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("big_u64"));

        let iana_ts =
            TimestampSecondArray::from(vec![Some(1_704_067_200)]).with_timezone("America/New_York");
        let err = ColumnData::try_from_arrow_with_options(
            &iana_ts,
            formualizer_eval::engine::DateSystem::Excel1900,
            Some("event_time"),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("event_time") && msg.contains("America/New_York"));

        let bin = BinaryArray::from(vec![Some(b"abc".as_slice())]);
        let err = ColumnData::try_from_arrow_with_options(
            &bin,
            formualizer_eval::engine::DateSystem::Excel1900,
            Some("payload"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("payload"));
    }

    #[test]
    fn arrow_null_column_allocates_zero_chunks_without_headers() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("nulls", DataType::Null, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![1.0; 1_000])),
                Arc::new(NullArray::new(1_000)),
            ],
        )
        .unwrap();
        let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
        wb.add_sheet("S").unwrap();
        let batches: Vec<Result<RecordBatch, &'static str>> = vec![Ok(batch)];
        wb.set_record_batches("S", 1, 1, batches, false).unwrap();

        let asheet = wb.engine().sheet_store().sheet("S").unwrap();
        assert_eq!(asheet.columns[1].total_chunk_count(), 0);
    }

    #[test]
    fn arrow_invalid_utf8_reports_field_name() {
        use arrow_array::Array;

        let bin = BinaryArray::from(vec![&[0xffu8, 0xfe][..], &b"ok"[..]]);
        let data = unsafe {
            bin.into_data()
                .into_builder()
                .data_type(DataType::Utf8)
                .build_unchecked()
        };
        let bad_arr = StringArray::from(data);

        let err = ColumnData::try_from_arrow_with_options(
            &bad_arr,
            formualizer_eval::engine::DateSystem::Excel1900,
            Some("bad_text"),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("bad_text") && msg.contains("UTF-8"),
            "expected Arrow field name and UTF-8 diagnostic, got: {msg}"
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "bad_text",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(bad_arr)]).unwrap();
        for changelog in [false, true] {
            let mut wb =
                Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(changelog));
            wb.add_sheet("S").unwrap();
            let batches: Vec<Result<RecordBatch, &'static str>> = vec![Ok(batch.clone())];
            let err = wb
                .set_record_batches("S", 1, 1, batches, false)
                .unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("bad_text") && !msg.contains("Column 0"),
                "changelog={changelog}: expected field name 'bad_text' instead of 'Column 0', got: {msg}"
            );
        }
    }
}
