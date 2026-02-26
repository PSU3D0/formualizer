#[cfg(feature = "json")]
use formualizer_workbook::{JsonAdapter, SpreadsheetReader, SpreadsheetWriter};

#[cfg(feature = "json")]
#[test]
fn json_row_visibility_roundtrip_preserves_metadata() {
    let bytes = br#"{
        "version": 1,
        "sheets": {
            "S": {
                "cells": [
                    { "row": 1, "col": 1, "value": { "type": "Number", "value": 1.0 } }
                ],
                "row_hidden_manual": [2, 4],
                "row_hidden_filter": [7]
            }
        }
    }"#
    .to_vec();

    let mut adapter = JsonAdapter::open_bytes(bytes).expect("open json workbook");
    let first_read = adapter.read_sheet("S").expect("read sheet");
    assert_eq!(first_read.row_hidden_manual, vec![2, 4]);
    assert_eq!(first_read.row_hidden_filter, vec![7]);

    let saved = adapter.save_to_bytes().expect("save json workbook");
    let mut reopened = JsonAdapter::open_bytes(saved).expect("reopen json workbook");
    let second_read = reopened.read_sheet("S").expect("read sheet after reopen");

    assert_eq!(second_read.row_hidden_manual, vec![2, 4]);
    assert_eq!(second_read.row_hidden_filter, vec![7]);
}

#[cfg(feature = "json")]
#[test]
fn json_row_visibility_defaults_for_legacy_payloads() {
    let bytes = br#"{
        "version": 1,
        "sheets": {
            "S": {
                "cells": [
                    { "row": 1, "col": 1, "value": { "type": "Number", "value": 1.0 } }
                ]
            }
        }
    }"#
    .to_vec();

    let mut adapter = JsonAdapter::open_bytes(bytes).expect("open legacy json workbook");
    let sheet = adapter.read_sheet("S").expect("read sheet");

    assert!(sheet.row_hidden_manual.is_empty());
    assert!(sheet.row_hidden_filter.is_empty());
}
