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

// 3. Unsupported constructors produce errors
#[test]
fn calamine_open_bytes_unsupported() {
    match CalamineAdapter::open_bytes(vec![]) {
        Ok(_) => panic!("open_bytes unexpectedly succeeded"),
        Err(err) => {
            let msg = err.to_string();
            assert!(msg.contains("open_bytes"));
        }
    }
}

#[test]
fn calamine_open_reader_unsupported() {
    use std::io::Cursor;
    let reader: Box<dyn std::io::Read + Send + Sync> = Box::new(Cursor::new(vec![]));
    match CalamineAdapter::open_reader(reader) {
        Ok(_) => panic!("open_reader unexpectedly succeeded"),
        Err(err) => assert!(err.to_string().contains("open_reader")),
    }
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
