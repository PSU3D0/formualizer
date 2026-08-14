//! Regression tests for issue #332: a freshly constructed engine is seeded with
//! a default sheet, and the loader used to append the file's sheets alongside
//! it, leaving a phantom `Sheet1` and shifting every `SHEET()` result by one.

use crate::common::build_workbook;
use crate::common::sheet_load::{assert_sheet_layout, sheet_names};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader};
use std::path::PathBuf;

/// A workbook whose sheets are `Data` (first) then `Extra`, with no sheet
/// named `Sheet1` anywhere.
fn data_extra_fixture() -> PathBuf {
    build_workbook(|book| {
        book.get_sheet_by_name_mut("Sheet1")
            .unwrap()
            .set_name("Data");
        book.new_sheet("Extra").unwrap();
        book.get_sheet_by_name_mut("Data")
            .unwrap()
            .get_cell_mut((1u32, 1u32))
            .set_value_number(1.0);
        book.get_sheet_by_name_mut("Extra")
            .unwrap()
            .get_cell_mut((1u32, 1u32))
            .set_value_number(2.0);
    })
}

fn load(path: &PathBuf) -> Engine<TestWorkbook> {
    let mut backend = CalamineAdapter::open_path(path).unwrap();
    let mut engine: Engine<_> = Engine::new(TestWorkbook::new(), EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine
}

#[test]
fn calamine_load_folds_default_sheet_into_first_file_sheet() {
    let path = data_extra_fixture();
    let mut engine = load(&path);

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
fn calamine_load_file_containing_sheet1_has_no_duplicate() {
    let path = build_workbook(|book| {
        book.get_sheet_by_name_mut("Sheet1")
            .unwrap()
            .set_name("Data");
        book.new_sheet("Sheet1").unwrap();
        book.get_sheet_by_name_mut("Data")
            .unwrap()
            .get_cell_mut((1u32, 1u32))
            .set_value_number(1.0);
        book.get_sheet_by_name_mut("Sheet1")
            .unwrap()
            .get_cell_mut((1u32, 1u32))
            .set_value_number(2.0);
    });
    let mut engine = load(&path);

    assert_eq!(sheet_names(&engine).len(), 2, "no duplicate sheet");
    assert_sheet_layout(&mut engine, &["Data", "Sheet1"]);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0)),
        "the file's Sheet1 keeps its own contents"
    );
}

#[test]
fn calamine_load_single_renamed_sheet() {
    let path = build_workbook(|book| {
        book.get_sheet_by_name_mut("Sheet1")
            .unwrap()
            .set_name("Report");
        book.get_sheet_by_name_mut("Report")
            .unwrap()
            .get_cell_mut((1u32, 1u32))
            .set_value_number(7.0);
    });
    let mut engine = load(&path);

    assert_eq!(engine.sheet_id("Sheet1"), None);
    assert_sheet_layout(&mut engine, &["Report"]);
}

/// The fold must never take a populated default sheet away from the caller:
/// `stream_into_engine` takes an engine supplied by the caller, so it may
/// already hold user data.
#[test]
fn calamine_load_into_non_fresh_engine_preserves_user_data() {
    let path = data_extra_fixture();
    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let mut engine: Engine<_> = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Text("USER-DATA".into()))
        .unwrap();
    // E5 is never written by the fixture, so its survival is proof that the
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
