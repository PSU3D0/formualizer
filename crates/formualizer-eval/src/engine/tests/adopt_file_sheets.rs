//! Unit tests for [`Engine::adopt_file_sheets`], the single seam every
//! `EngineLoadStream` backend uses to register a file's sheets (issue #332).

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;

fn fresh_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

fn store_names<R: crate::traits::EvaluationContext>(engine: &Engine<R>) -> Vec<String> {
    engine
        .sheet_store()
        .sheets
        .iter()
        .map(|s| s.name.as_ref().to_string())
        .collect()
}

#[test]
fn adopt_folds_default_sheet_into_first_file_sheet() {
    let mut engine = fresh_engine();
    assert_eq!(engine.default_sheet_name(), "Sheet1");
    let default_id = engine.default_sheet_id();

    let ids = engine.adopt_file_sheets(["Data", "Extra"]).unwrap();

    assert_eq!(store_names(&engine), vec!["Data", "Extra"]);
    assert_eq!(ids[0], default_id, "first file sheet reuses the default id");
    assert_eq!(engine.sheet_id("Data"), Some(0));
    assert_eq!(engine.sheet_id("Extra"), Some(1));
    assert_eq!(engine.sheet_id("Sheet1"), None, "no phantom default sheet");
}

#[test]
fn adopt_keeps_file_order_when_file_contains_the_default_name() {
    let mut engine = fresh_engine();
    engine.adopt_file_sheets(["Data", "Sheet1"]).unwrap();

    // The file's order wins: Data is first even though the engine was seeded
    // with a sheet named Sheet1.
    assert_eq!(store_names(&engine), vec!["Data", "Sheet1"]);
    assert_eq!(engine.sheet_id("Data"), Some(0));
    assert_eq!(engine.sheet_id("Sheet1"), Some(1));
    assert_eq!(engine.sheet_store().sheets.len(), 2, "no duplicate sheet");
}

#[test]
fn adopt_is_a_noop_when_first_file_sheet_is_the_default_name() {
    let mut engine = fresh_engine();
    let default_id = engine.default_sheet_id();
    let ids = engine.adopt_file_sheets(["Sheet1", "Extra"]).unwrap();

    assert_eq!(store_names(&engine), vec!["Sheet1", "Extra"]);
    assert_eq!(ids, vec![default_id, 1]);
}

#[test]
fn adopt_fixes_casing_of_the_first_sheet() {
    let mut engine = fresh_engine();
    engine.adopt_file_sheets(["SHEET1"]).unwrap();
    assert_eq!(
        store_names(&engine),
        vec!["SHEET1"],
        "the file's casing is authoritative"
    );
    assert_eq!(engine.sheet_store().sheets.len(), 1);
}

#[test]
fn adopt_rejects_duplicate_sheet_names() {
    let mut engine = fresh_engine();
    let err = engine
        .adopt_file_sheets(["Data", "Extra", "Data"])
        .expect_err("duplicate sheet names must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("Duplicate sheet name"), "unexpected: {msg}");
    // Nothing was registered: the check runs before any mutation.
    assert_eq!(store_names(&engine), vec!["Sheet1"]);
}

#[test]
fn adopt_rejects_duplicate_sheet_names_differing_only_by_case() {
    let mut engine = fresh_engine();
    let err = engine
        .adopt_file_sheets(["Data", "DATA"])
        .expect_err("sheet names are case-insensitive in Excel");
    assert!(err.to_string().contains("Duplicate sheet name"));
}

#[test]
fn adopt_preserves_user_data_on_a_non_fresh_engine() {
    let mut engine = fresh_engine();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("USER-DATA".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 5, 5, LiteralValue::Number(999.0))
        .unwrap();

    engine.adopt_file_sheets(["Data", "Extra"]).unwrap();

    // The default sheet is NOT renamed out from under the caller.
    assert_eq!(store_names(&engine), vec!["Sheet1", "Data", "Extra"]);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("USER-DATA".into()))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 5),
        Some(LiteralValue::Number(999.0))
    );
}

#[test]
fn adopt_does_not_fold_when_the_default_sheet_holds_a_named_range() {
    let mut engine = fresh_engine();
    engine
        .define_name(
            "MyName",
            NamedDefinition::Literal(LiteralValue::Number(1.0)),
            NameScope::Workbook,
        )
        .unwrap();
    engine.adopt_file_sheets(["Data"]).unwrap();
    assert_eq!(store_names(&engine), vec!["Sheet1", "Data"]);
}

#[test]
fn adopt_of_no_sheets_leaves_the_default_sheet_alone() {
    let mut engine = fresh_engine();
    let ids = engine.adopt_file_sheets(std::iter::empty()).unwrap();
    assert!(ids.is_empty());
    assert_eq!(store_names(&engine), vec!["Sheet1"]);
}
