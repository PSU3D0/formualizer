//! Verifies the examples in `docs/json-workbook-format.md` (issue #212).
//!
//! The reporter reconstructed this schema by feeding wrong JSON to `fromJson`
//! and following serde errors. The documentation is only useful if it is
//! correct, so the worked example and the documented defaults are asserted here.
#![cfg(feature = "json")]

use formualizer_common::LiteralValue;
use formualizer_workbook::backends::JsonAdapter;
use formualizer_workbook::traits::SpreadsheetReader;
use formualizer_workbook::{LoadStrategy, Workbook, WorkbookConfig};

fn load(json: &str) -> Result<Workbook, String> {
    let adapter = <JsonAdapter as SpreadsheetReader>::open_bytes(json.as_bytes().to_vec())
        .map_err(|e| e.to_string())?;
    Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .map_err(|e| e.to_string())
}

/// The worked example from the docs, verbatim.
const WORKED_EXAMPLE: &str = r#"{
  "sheets": {
    "S": {
      "cells": [
        { "row": 1, "col": 1, "value": { "type": "Text", "value": "Nama" } },
        { "row": 1, "col": 2, "value": { "type": "Text", "value": "Nilai" } },
        { "row": 2, "col": 1, "value": { "type": "Text", "value": "Ani" } },
        { "row": 2, "col": 2, "value": { "type": "Number", "value": 10 } },
        { "row": 3, "col": 1, "value": { "type": "Text", "value": "Budi" } },
        { "row": 3, "col": 2, "value": { "type": "Number", "value": 20 } },
        { "row": 4, "col": 1, "value": { "type": "Text", "value": "Cici" } },
        { "row": 4, "col": 2, "value": { "type": "Number", "value": 30 } }
      ],
      "tables": [{
        "name": "Table1",
        "range": [1, 1, 4, 2],
        "headers": ["Nama", "Nilai"],
        "totals_row": false
      }]
    }
  }
}"#;

#[test]
fn the_documented_worked_example_loads_and_evaluates() {
    let mut wb = load(WORKED_EXAMPLE).expect("documented example must load");
    wb.set_formula("S", 10, 4, "=SUM(Table1[Nilai])").unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("S", 10, 4), Some(LiteralValue::Number(60.0)));
}

#[test]
fn header_row_defaults_to_true_and_totals_row_is_required() {
    // `header_row` omitted -> true, as documented.
    let wb = load(WORKED_EXAMPLE).unwrap();
    let table = wb.table_metadata("Table1").expect("table");
    assert!(table.header_row, "header_row should default to true");
    assert!(!table.totals_row);

    // `totals_row` omitted -> load fails, as documented.
    let without_totals = WORKED_EXAMPLE.replace(",\n        \"totals_row\": false", "");
    let Err(error) = load(&without_totals) else {
        panic!("totals_row is required")
    };
    assert!(
        error.contains("totals_row"),
        "error should name the missing field, got: {error}"
    );
}

#[test]
fn a_tables_key_outside_a_sheet_is_ignored() {
    // Documented trap: the key is accepted at the top level and does nothing.
    let misplaced = r#"{
      "sheets": { "S": { "cells": [] } },
      "tables": [{ "name": "Table1", "range": [1,1,4,2], "headers": ["a"], "totals_row": false }]
    }"#;
    let wb = load(misplaced).expect("unknown top-level keys are ignored");
    assert!(
        wb.tables().is_empty(),
        "a top-level `tables` key must not define anything"
    );
}

#[test]
fn cells_must_be_a_sequence_and_values_must_be_tagged() {
    let cells_as_map = r#"{ "sheets": { "S": { "cells": {} } } }"#;
    let Err(error) = load(cells_as_map) else {
        panic!("cells must be a sequence")
    };
    assert!(error.contains("sequence"), "got: {error}");

    let untagged_value =
        r#"{ "sheets": { "S": { "cells": [{ "row": 1, "col": 1, "value": 5 }] } } }"#;
    assert!(
        load(untagged_value).is_err(),
        "a bare scalar value must be rejected"
    );
}

#[test]
fn documented_cell_value_tags_are_accepted() {
    let json = r##"{ "sheets": { "S": { "cells": [
        { "row": 1, "col": 1, "value": { "type": "Int", "value": 42 } },
        { "row": 2, "col": 1, "value": { "type": "Number", "value": 3.5 } },
        { "row": 3, "col": 1, "value": { "type": "Text", "value": "x" } },
        { "row": 4, "col": 1, "value": { "type": "Boolean", "value": true } },
        { "row": 5, "col": 1, "value": { "type": "Empty" } },
        { "row": 6, "col": 1, "value": { "type": "Date", "value": "2026-07-25" } },
        { "row": 7, "col": 1, "value": { "type": "DateTime", "value": "2026-07-25T09:30:00" } },
        { "row": 8, "col": 1, "value": { "type": "Time", "value": "09:30:00" } },
        { "row": 9, "col": 1, "value": { "type": "Duration", "value": 3600 } },
        { "row": 10, "col": 1, "value": { "type": "Error", "value": "#DIV/0!" } }
    ] } } }"##;
    let wb = load(json).expect("every documented tag must be accepted");
    // `Int` is accepted on input but normalised to a number by the ingest path,
    // which the format documentation notes.
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(42.0)));
    assert_eq!(
        wb.get_value("S", 3, 1),
        Some(LiteralValue::Text("x".into()))
    );
    assert_eq!(wb.get_value("S", 4, 1), Some(LiteralValue::Boolean(true)));
}
