//! Issue #332 on the csv backend. CSV was never observed to grow a phantom
//! sheet, but only because `CsvAdapter`'s sheet name defaults to `Sheet1` and
//! so always collided with the engine's default sheet. The name is settable
//! through `SpreadsheetWriter::rename_sheet`, and once it differs from the
//! default the old unguarded loop produced the phantom here too. These tests
//! pin the behaviour to the shared seam rather than to the default name.
#![cfg(feature = "csv")]

mod common;

use common::sheet_load::{assert_sheet_layout, sheet_names};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_workbook::{CsvAdapter, SpreadsheetReader, SpreadsheetWriter};

fn adapter() -> CsvAdapter {
    CsvAdapter::open_bytes(b"1,2\n3,4\n".to_vec()).unwrap()
}

#[test]
fn csv_load_with_default_sheet_name_has_no_phantom() {
    let mut backend = adapter();
    let mut engine: Engine<_> = Engine::new(TestWorkbook::new(), EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();

    assert_sheet_layout(&mut engine, &["Sheet1"]);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
}

#[test]
fn csv_load_with_renamed_sheet_has_no_phantom() {
    let mut backend = adapter();
    backend.rename_sheet("Sheet1", "Data").unwrap();
    let mut engine: Engine<_> = Engine::new(TestWorkbook::new(), EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();

    assert_eq!(
        engine.sheet_id("Sheet1"),
        None,
        "phantom Sheet1 must not exist"
    );
    assert_eq!(sheet_names(&engine), vec!["Data"]);
    assert_sheet_layout(&mut engine, &["Data"]);
    assert_eq!(
        engine.get_cell_value("Data", 2, 2),
        Some(LiteralValue::Number(4.0))
    );
}

#[test]
fn csv_load_into_non_fresh_engine_preserves_user_data() {
    let mut backend = adapter();
    backend.rename_sheet("Sheet1", "Data").unwrap();
    let mut engine: Engine<_> = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 5, 5, LiteralValue::Number(999.0))
        .unwrap();

    backend.stream_into_engine(&mut engine).unwrap();

    assert_eq!(sheet_names(&engine), vec!["Sheet1", "Data"]);
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 5),
        Some(LiteralValue::Number(999.0)),
        "a cell the file never writes must survive the load"
    );
}
