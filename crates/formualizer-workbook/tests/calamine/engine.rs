// Engine-level tests using CalamineAdapter::stream_into_engine; run with `--features calamine,umya`.
use crate::common::build_workbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_parse::parser::parse;
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader};

#[test]
fn stream_single_sheet_alignment_and_eval() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        // Values starting at D5 (offset row/col) to exercise padding
        sh.get_cell_mut((4, 5)).set_value_number(99.0); // D5
        sh.get_cell_mut((5, 5)).set_formula("=D5+1"); // E5
        sh.get_cell_mut((6, 5)).set_value_bool(true); // F5
        sh.get_cell_mut((4, 7)).set_value("x"); // D7
        sh.get_cell_mut((5, 7)).set_formula("=D7"); // E7
        sh.get_cell_mut((7, 5)).set_formula("=F5"); // G5 mirrors boolean
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    // Non-formula base cells via get_cell_value (Arrow-backed)
    match engine.get_cell_value("Sheet1", 5, 4) {
        Some(LiteralValue::Number(n)) => {
            assert!((n - 99.0).abs() < 1e-9, "D5 expected 99, got {n}")
        }
        other => panic!("Unexpected D5: {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 5, 6) {
        Some(LiteralValue::Boolean(b)) => assert!(b, "Expected TRUE at F5"),
        other => panic!("Unexpected F5: {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 7, 4) {
        Some(LiteralValue::Text(s)) => assert_eq!(s, "x"),
        other => panic!("Unexpected D7: {other:?}"),
    }

    // E5 = D5 + 1 => 100
    match engine.get_cell_value("Sheet1", 5, 5) {
        Some(LiteralValue::Number(n)) => {
            assert!((n - 100.0).abs() < 1e-9, "E5 expected 100, got {n}")
        }
        other => panic!("Unexpected E5: {other:?}"),
    }
    // get_cell parity for E5 (formula)
    let (ast_e5, val_e5) = engine.get_cell("Sheet1", 5, 5).expect("cell E5 present");
    assert!(ast_e5.is_some(), "E5 should have a formula AST");
    assert_eq!(val_e5, engine.get_cell_value("Sheet1", 5, 5));
    // E7 = D7 => "x"
    match engine.get_cell_value("Sheet1", 7, 5) {
        Some(LiteralValue::Text(s)) => assert_eq!(s, "x"),
        other => panic!("Unexpected E7: {other:?}"),
    }
    // get_cell parity for E7 (formula)
    let (ast_e7, val_e7) = engine.get_cell("Sheet1", 7, 5).expect("cell E7 present");
    assert!(ast_e7.is_some(), "E7 should have a formula AST");
    assert_eq!(val_e7, engine.get_cell_value("Sheet1", 7, 5));
    // G5 = F5 => true
    match engine.get_cell_value("Sheet1", 5, 7) {
        Some(LiteralValue::Boolean(b)) => assert!(b, "Expected TRUE at G5"),
        other => panic!("Unexpected G5: {other:?}"),
    }
    // get_cell parity for G5 (formula)
    let (ast_g5, val_g5) = engine.get_cell("Sheet1", 5, 7).expect("cell G5 present");
    assert!(ast_g5.is_some(), "G5 should have a formula AST");
    assert_eq!(val_g5, engine.get_cell_value("Sheet1", 5, 7));

    // get_cell parity for non-formula referenced bases
    let (ast_d5, val_d5) = engine.get_cell("Sheet1", 5, 4).expect("cell D5 present");
    assert!(ast_d5.is_none(), "D5 should be a value cell");
    assert_eq!(val_d5, engine.get_cell_value("Sheet1", 5, 4));
    let (ast_d7, val_d7) = engine.get_cell("Sheet1", 7, 4).expect("cell D7 present");
    assert!(ast_d7.is_none(), "D7 should be a value cell");
    assert_eq!(val_d7, engine.get_cell_value("Sheet1", 7, 4));
    let (ast_f5, val_f5) = engine.get_cell("Sheet1", 5, 6).expect("cell F5 present");
    assert!(ast_f5.is_none(), "F5 should be a value cell");
    assert_eq!(val_f5, engine.get_cell_value("Sheet1", 5, 6));
}

#[test]
fn stream_multi_sheet_cross_ref() {
    let path = build_workbook(|book| {
        // Create second sheet and populate a value at D5
        let _ = book.new_sheet("Data");
        let data = book.get_sheet_by_name_mut("Data").unwrap();
        data.get_cell_mut((4, 5)).set_value_number(21.0); // D5 on Data
        // On Sheet1 reference Data!D5
        let s1 = book.get_sheet_by_name_mut("Sheet1").unwrap();
        s1.get_cell_mut((1, 1)).set_formula("=Data!D5*2"); // A1
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Number(n)) => assert!((n - 42.0).abs() < 1e-9, "Expected 42 got {n}"),
        other => panic!("Unexpected A1: {other:?}"),
    }
    // get_cell parity for A1 (formula)
    let (ast_a1, val_a1) = engine.get_cell("Sheet1", 1, 1).expect("A1 present");
    assert!(ast_a1.is_some(), "A1 should have a formula AST");
    assert_eq!(val_a1, engine.get_cell_value("Sheet1", 1, 1));
    // Also check base cell from other sheet via get_cell_value
    match engine.get_cell_value("Data", 5, 4) {
        Some(LiteralValue::Number(n)) => {
            assert!((n - 21.0).abs() < 1e-9, "Data!D5 expected 21, got {n}")
        }
        other => panic!("Unexpected Data!D5: {other:?}"),
    }
}

#[test]
fn stream_mixed_types_in_column() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        // Mix types in column D starting at row 5
        sh.get_cell_mut((4, 5)).set_value_number(1.0); // D5 number
        sh.get_cell_mut((4, 6)).set_value_bool(true); // D6 boolean
        sh.get_cell_mut((4, 7)).set_value("abc"); // D7 text
        sh.get_cell_mut((4, 8)).set_formula("=1/0"); // D8 error formula

        // Mirror via formulas in column E
        sh.get_cell_mut((5, 5)).set_formula("=D5");
        sh.get_cell_mut((5, 6)).set_formula("=D6");
        sh.get_cell_mut((5, 7)).set_formula("=D7");
        sh.get_cell_mut((5, 8)).set_formula("=D8");
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();

    engine.evaluate_all().unwrap();

    // Debug: verify D8 computed error; E8 should mirror it.
    match engine.get_cell_value("Sheet1", 8, 4) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Div),
        other => panic!("D8 expected #DIV/0!, got {other:?}"),
    }

    // Non-formula base cells via get_cell_value
    match engine.get_cell_value("Sheet1", 5, 4) {
        Some(LiteralValue::Number(n)) => assert!((n - 1.0).abs() < 1e-9),
        other => panic!("D5 expected 1.0, got {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 6, 4) {
        Some(LiteralValue::Boolean(b)) => assert!(b),
        other => panic!("D6 expected TRUE, got {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 7, 4) {
        Some(LiteralValue::Text(s)) => assert_eq!(s, "abc"),
        other => panic!("D7 expected 'abc', got {other:?}"),
    }

    match engine.get_cell_value("Sheet1", 5, 5) {
        Some(LiteralValue::Number(n)) => assert!((n - 1.0).abs() < 1e-9),
        other => panic!("E5 expected 1.0, got {other:?}"),
    }
    // get_cell parity for E5 (formula)
    let (ast_e5, val_e5) = engine.get_cell("Sheet1", 5, 5).expect("E5 present");
    assert!(ast_e5.is_some());
    assert_eq!(val_e5, engine.get_cell_value("Sheet1", 5, 5));
    match engine.get_cell_value("Sheet1", 6, 5) {
        Some(LiteralValue::Boolean(b)) => assert!(b),
        other => panic!("E6 expected TRUE, got {other:?}"),
    }
    // get_cell parity for E6 (formula)
    let (ast_e6, val_e6) = engine.get_cell("Sheet1", 6, 5).expect("E6 present");
    assert!(ast_e6.is_some());
    assert_eq!(val_e6, engine.get_cell_value("Sheet1", 6, 5));
    match engine.get_cell_value("Sheet1", 7, 5) {
        Some(LiteralValue::Text(s)) => assert_eq!(s, "abc"),
        other => panic!("E7 expected 'abc', got {other:?}"),
    }
    // get_cell parity for E7 (formula)
    let (ast_e7, val_e7) = engine.get_cell("Sheet1", 7, 5).expect("E7 present");
    assert!(ast_e7.is_some());
    assert_eq!(val_e7, engine.get_cell_value("Sheet1", 7, 5));
    match engine.get_cell_value("Sheet1", 8, 5) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Div),
        other => panic!("E8 expected #DIV/0!, got {other:?}"),
    }
    // get_cell parity for E8 (formula)
    let (ast_e8, val_e8) = engine.get_cell("Sheet1", 8, 5).expect("E8 present");
    assert!(ast_e8.is_some());
    assert_eq!(val_e8, engine.get_cell_value("Sheet1", 8, 5));

    // get_cell parity for non-formula referenced bases D5/D6/D7
    let (ast_d5, val_d5) = engine.get_cell("Sheet1", 5, 4).expect("D5 present");
    assert!(ast_d5.is_none());
    assert_eq!(val_d5, engine.get_cell_value("Sheet1", 5, 4));
    let (ast_d6, val_d6) = engine.get_cell("Sheet1", 6, 4).expect("D6 present");
    assert!(ast_d6.is_none());
    assert_eq!(val_d6, engine.get_cell_value("Sheet1", 6, 4));
    let (ast_d7, val_d7) = engine.get_cell("Sheet1", 7, 4).expect("D7 present");
    assert!(ast_d7.is_none());
    assert_eq!(val_d7, engine.get_cell_value("Sheet1", 7, 4));
}

#[test]
fn stream_edit_propagation_chain() {
    // Build a simple dependent chain: A1 (value) <- B1 (formula) <- C1 (formula)
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(10.0); // A1 = 10
        sh.get_cell_mut((2, 1)).set_formula("=A1"); // B1 = A1
        sh.get_cell_mut((3, 1)).set_formula("=B1"); // C1 = B1
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();

    // Initial evaluation brings all three up-to-date
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 2) {
        // B1
        Some(formualizer_workbook::LiteralValue::Number(n)) => assert!((n - 10.0).abs() < 1e-9),
        other => panic!("Unexpected B1 initial: {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 1, 3) {
        // C1
        Some(formualizer_workbook::LiteralValue::Number(n)) => assert!((n - 10.0).abs() < 1e-9),
        other => panic!("Unexpected C1 initial: {other:?}"),
    }

    // Edit A1's formula via engine; B1 and C1 should reflect the change
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=20").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(formualizer_workbook::LiteralValue::Number(n)) => assert!((n - 20.0).abs() < 1e-9),
        other => panic!("Unexpected B1 after A1 edit: {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 1, 3) {
        Some(formualizer_workbook::LiteralValue::Number(n)) => assert!((n - 20.0).abs() < 1e-9),
        other => panic!("Unexpected C1 after A1 edit: {other:?}"),
    }

    // Edit B1's formula; only C1 should change accordingly
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1*3").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(formualizer_workbook::LiteralValue::Number(n)) => assert!((n - 60.0).abs() < 1e-9),
        other => panic!("Unexpected B1 after B1 edit: {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 1, 3) {
        Some(formualizer_workbook::LiteralValue::Number(n)) => assert!((n - 60.0).abs() < 1e-9),
        other => panic!("Unexpected C1 after B1 edit: {other:?}"),
    }
}

#[test]
fn stream_sumifs_cross_sheet_multi_predicate() {
    // Build a two-sheet workbook; Data holds rows; Sheet1 computes SUMIFS across sheets
    let path = build_workbook(|book| {
        let _ = book.new_sheet("Data");
        let data = book.get_sheet_by_name_mut("Data").unwrap();
        // Use rows starting at 5 to exercise offsets
        // A: category (text), B: flag (int), C: amount (number)
        data.get_cell_mut((1, 5)).set_value("A");
        data.get_cell_mut((2, 5)).set_value_number(1);
        data.get_cell_mut((3, 5)).set_value_number(10.0);

        data.get_cell_mut((1, 6)).set_value("B");
        data.get_cell_mut((2, 6)).set_value_number(1);
        data.get_cell_mut((3, 6)).set_value_number(20.0);

        data.get_cell_mut((1, 7)).set_value("A");
        data.get_cell_mut((2, 7)).set_value_number(0);
        data.get_cell_mut((3, 7)).set_value_number(30.0);

        data.get_cell_mut((1, 8)).set_value("A");
        data.get_cell_mut((2, 8)).set_value_number(1);
        data.get_cell_mut((3, 8)).set_value_number(40.0);

        // On Sheet1 compute =SUMIFS(Data!C:C, Data!A:A, "A", Data!B:B, 1)
        let s1 = book.get_sheet_by_name_mut("Sheet1").unwrap();
        s1.get_cell_mut((1, 1))
            .set_formula("=SUMIFS(Data!C:C, Data!A:A, \"A\", Data!B:B, 1)");
    });

    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend.stream_into_engine(&mut engine).unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Number(n)) => assert!((n - 50.0).abs() < 1e-9, "Expected 50, got {n}"),
        other => panic!("Unexpected SUMIFS result: {other:?}"),
    }
    // get_cell parity for A1 (formula)
    let (ast_a1, val_a1) = engine.get_cell("Sheet1", 1, 1).expect("A1 present");
    assert!(ast_a1.is_some());
    assert_eq!(val_a1, engine.get_cell_value("Sheet1", 1, 1));
    // Non-formula base cells from Data via get_cell_value
    match engine.get_cell_value("Data", 5, 1) {
        Some(LiteralValue::Text(s)) => assert_eq!(s, "A"),
        other => panic!("Unexpected Data!A5: {other:?}"),
    }
    match engine.get_cell_value("Data", 5, 3) {
        Some(LiteralValue::Number(n)) => assert!((n - 10.0).abs() < 1e-9),
        other => panic!("Unexpected Data!C5: {other:?}"),
    }
}
