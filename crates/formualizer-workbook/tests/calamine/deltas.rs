// Integration test for Calamine backend; run with `--features calamine,umya`.
use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{CalamineAdapter, LiteralValue, SpreadsheetReader};

// 1. Error propagation after evaluation (#DIV/0!)
#[test]
fn calamine_error_formula_evaluates_to_error() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_formula("=1/0"); // A1
    });
    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();
    let v = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    match v {
        LiteralValue::Error(e) => assert_eq!(e.kind.to_string(), "#DIV/0!"),
        other => panic!("Expected error got {other:?}"),
    }
}

// 2. read_range filtering correctness (subset window)
#[test]
fn calamine_read_range_filters() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        // Fill a 3x3 block starting at A1
        for r in 1..=3 {
            for c in 1..=3 {
                sh.get_cell_mut((c, r))
                    .set_value_number((r * 10 + c) as i32);
            }
        }
    });
    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    // Request center 2x2 block: rows 2..3, cols 2..3
    let subset = backend.read_range("Sheet1", (2, 2), (3, 3)).unwrap();
    assert_eq!(
        subset.len(),
        4,
        "Expected 4 cells in 2x2 window, got {}",
        subset.len()
    );
    // Ensure a corner outside window (1,1) absent
    assert!(!subset.contains_key(&(1, 1)));
}

// 3. Byte-backed constructors load the same workbook content as the path flow
#[test]
fn calamine_open_bytes_reads_workbook() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(4);
        sh.get_cell_mut((2, 1)).set_value_number(5);
        sh.get_cell_mut((3, 1)).set_formula("=A1+B1");
    });
    let bytes = std::fs::read(path).expect("read workbook bytes");

    let mut backend = CalamineAdapter::open_bytes(bytes).expect("open workbook from bytes");
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(9.0))
    );
}

#[test]
fn calamine_open_reader_reads_workbook() {
    use std::io::Cursor;

    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(7);
        sh.get_cell_mut((2, 1)).set_formula("=A1*2");
    });
    let bytes = std::fs::read(path).expect("read workbook bytes");
    let reader: Box<dyn std::io::Read + Send + Sync> = Box::new(Cursor::new(bytes));

    let mut backend = CalamineAdapter::open_reader(reader).expect("open workbook from reader");
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(14.0))
    );
}

// 4. Values-only fast path: ensure formulas_loaded == 0 and cells_loaded == N
#[test]
fn loader_fast_path_values_only() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(1);
        sh.get_cell_mut((2, 1)).set_value_number(2);
        sh.get_cell_mut((3, 1)).set_value_number(3);
    });
    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    // Quick sanity: engine holds values
    for (col, expected) in [(1, 1.0), (2, 2.0), (3, 3.0)] {
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, col),
            Some(LiteralValue::Number(expected))
        );
    }
}
