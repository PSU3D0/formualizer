#![cfg(feature = "json")]

//! Regression tests for #624: `Workbook` JSON loading must preserve full IEEE-754
//! `f64` precision (`serde_json`'s `float_roundtrip` feature).

use formualizer_common::LiteralValue;
use formualizer_workbook::{
    JsonAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

#[test]
fn json_loader_preserves_exact_f64_precision_issue_624() {
    let expected: f64 = 12.540000000000001;
    let json = r#"{
        "version": 1,
        "sheets": {
            "S": {
                "cells": [
                    { "row": 1, "col": 1, "value": { "type": "Number", "value": 12.540000000000001 } },
                    { "row": 1, "col": 2, "formula": "=57+A1" }
                ]
            }
        }
    }"#;

    let adapter = JsonAdapter::open_bytes(json.as_bytes().to_vec()).unwrap();
    let mut wb = Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    wb.evaluate_all().unwrap();

    match wb.get_value("S", 1, 1) {
        Some(LiteralValue::Number(n)) => {
            assert_eq!(
                n.to_bits(),
                expected.to_bits(),
                "expected exact f64 {expected:?} (bits {:016x}), got {n:?} (bits {:016x})",
                expected.to_bits(),
                n.to_bits()
            );
        }
        other => panic!("expected Number({expected:?}), got {other:?}"),
    }

    let expected_sum = 57.0_f64 + expected;
    match wb.get_value("S", 1, 2) {
        Some(LiteralValue::Number(n)) => {
            assert_eq!(
                n.to_bits(),
                expected_sum.to_bits(),
                "expected exact sum {expected_sum:?}, got {n:?}"
            );
        }
        other => panic!("expected Number({expected_sum:?}), got {other:?}"),
    }
}

#[test]
fn json_loader_roundtrips_17_digit_edge_floats() {
    let cases: &[(u32, &str, f64)] = &[
        (1, "12.540000000000001", 12.540000000000001),
        (2, "-12.540000000000001", -12.540000000000001),
        (3, "0.1", 0.1),
        (4, "0.30000000000000004", 0.1_f64 + 0.2_f64),
        (5, "1.7976931348623157e308", f64::MAX),
        (6, "2.2250738585072014e-308", f64::MIN_POSITIVE),
    ];
    let cells: Vec<String> = cases
        .iter()
        .map(|(row, raw, _)| {
            format!(
                r#"{{ "row": {row}, "col": 1, "value": {{ "type": "Number", "value": {raw} }} }}"#
            )
        })
        .collect();
    let json = format!(
        r#"{{ "version": 1, "sheets": {{ "S": {{ "cells": [{}] }} }} }}"#,
        cells.join(",")
    );

    let adapter = JsonAdapter::open_bytes(json.into_bytes()).unwrap();
    let wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
        .unwrap();

    for &(row, raw, expected) in cases {
        match wb.get_value("S", row, 1) {
            Some(LiteralValue::Number(n)) => {
                assert_eq!(
                    n.to_bits(),
                    expected.to_bits(),
                    "row {row} ({raw}): expected bits {:016x}, got {:016x}",
                    expected.to_bits(),
                    n.to_bits()
                );
            }
            other => panic!("row {row} ({raw}): expected Number, got {other:?}"),
        }
    }
}
