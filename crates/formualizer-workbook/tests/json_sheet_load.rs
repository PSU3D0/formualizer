//! Regression tests for issue #332 and #626 on the json backend. A freshly
//! constructed engine is seeded with a default sheet, and the loader used to
//! append the document's sheets alongside it, leaving a phantom `Sheet1` and
//! shifting every `SHEET()` result by one.
//!
//! Sheet order in the JSON document is preserved on load and save so `SHEET()`
//! reflects input order rather than alphabetical order.
#![cfg(feature = "json")]

mod common;

use common::sheet_load::{assert_sheet_layout, sheet_names};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_workbook::{JsonAdapter, SpreadsheetReader};

fn doc(sheets: &[(&str, f64)]) -> Vec<u8> {
    let body: Vec<String> = sheets
        .iter()
        .map(|(name, v)| {
            format!(
                r#""{name}":{{"cells":[{{"row":1,"col":1,"value":{{"type":"Number","value":{v}}}}}]}}"#
            )
        })
        .collect();
    format!(r#"{{"version":1,"sheets":{{{}}}}}"#, body.join(",")).into_bytes()
}

fn load(bytes: Vec<u8>) -> Engine<TestWorkbook> {
    let mut backend = JsonAdapter::open_bytes(bytes).unwrap();
    let mut engine: Engine<_> = Engine::new(TestWorkbook::new(), EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine
}

#[test]
fn json_load_folds_default_sheet_into_first_file_sheet() {
    let mut engine = load(doc(&[("Data", 1.0), ("Extra", 2.0)]));

    assert_eq!(
        engine.sheet_id("Sheet1"),
        None,
        "phantom Sheet1 must not exist"
    );
    assert_sheet_layout(&mut engine, &["Data", "Extra"]);
    assert_eq!(
        engine.get_cell_value("Data", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.get_cell_value("Extra", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn json_load_file_containing_sheet1_has_no_duplicate() {
    // Document order: "Data" then "Sheet1".
    let mut engine = load(doc(&[("Data", 1.0), ("Sheet1", 2.0)]));

    assert_eq!(sheet_names(&engine).len(), 2, "no duplicate sheet");
    assert_sheet_layout(&mut engine, &["Data", "Sheet1"]);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0)),
        "the document's Sheet1 keeps its own contents"
    );
}

#[test]
fn json_load_single_renamed_sheet() {
    let mut engine = load(doc(&[("Report", 7.0)]));

    assert_eq!(engine.sheet_id("Sheet1"), None);
    assert_sheet_layout(&mut engine, &["Report"]);
}

#[test]
fn json_load_into_non_fresh_engine_preserves_user_data() {
    let mut backend = JsonAdapter::open_bytes(doc(&[("Data", 1.0), ("Extra", 2.0)])).unwrap();
    let mut engine: Engine<_> = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("USER-DATA".into()))
        .unwrap();
    // E5 is never written by the document, so its survival is proof that the
    // user's sheet was not handed to the file.
    engine
        .set_cell_value("Sheet1", 5, 5, LiteralValue::Number(999.0))
        .unwrap();

    backend.stream_into_engine(&mut engine).unwrap();

    assert_eq!(sheet_names(&engine), vec!["Sheet1", "Data", "Extra"]);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("USER-DATA".into())),
        "user data must survive the load"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 5),
        Some(LiteralValue::Number(999.0)),
        "a cell the file never writes must survive the load"
    );
    assert_eq!(
        engine.get_cell_value("Data", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
}

#[test]
fn json_load_preserves_sheet_insertion_order_for_sheet_fn() {
    let raw = r#"{
        "version": 1,
        "sheets": {
            "Zeta":   { "cells": [{ "row": 1, "col": 1, "formula": "=SHEET()" }] },
            "Alpha":  { "cells": [{ "row": 1, "col": 1, "formula": "=SHEET()" }] },
            "Middle": { "cells": [{ "row": 1, "col": 1, "formula": "=SHEET()" }] }
        }
    }"#;

    let adapter = JsonAdapter::open_bytes(raw.as_bytes().to_vec()).unwrap();
    assert_eq!(
        adapter.sheet_names().unwrap(),
        vec!["Zeta", "Alpha", "Middle"]
    );

    // Round-tripping JSON serialization preserves the exact input sheet order.
    let serialized = adapter.to_json_string().unwrap();
    let zeta_pos = serialized.find("\"Zeta\"").unwrap();
    let alpha_pos = serialized.find("\"Alpha\"").unwrap();
    let middle_pos = serialized.find("\"Middle\"").unwrap();
    assert!(
        zeta_pos < alpha_pos && alpha_pos < middle_pos,
        "expected Zeta < Alpha < Middle in serialized JSON: {serialized}"
    );

    let mut wb = formualizer_workbook::Workbook::from_reader(
        adapter,
        formualizer_workbook::LoadStrategy::EagerAll,
        formualizer_workbook::WorkbookConfig::interactive(),
    )
    .unwrap();
    assert_eq!(wb.sheet_names(), vec!["Zeta", "Alpha", "Middle"]);
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("Zeta", 1, 1), Some(LiteralValue::Number(1.0)));
    assert_eq!(wb.get_value("Alpha", 1, 1), Some(LiteralValue::Number(2.0)));
    assert_eq!(
        wb.get_value("Middle", 1, 1),
        Some(LiteralValue::Number(3.0))
    );
}
