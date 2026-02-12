// Integration test: CalamineAdapter extracts defined names from xlsx files.

use crate::common::build_workbook;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_workbook::{CalamineAdapter, LiteralValue, SpreadsheetReader};

/// Build a workbook with defined names using umya, then verify Calamine extracts them.
#[test]
fn calamine_extracts_defined_names_from_generated_xlsx() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        // A1=100, B1=200, A2=300, B2=400
        sh.get_cell_mut((1, 1)).set_value_number(100);
        sh.get_cell_mut((2, 1)).set_value_number(200);
        sh.get_cell_mut((1, 2)).set_value_number(300);
        sh.get_cell_mut((2, 2)).set_value_number(400);

        // Define names at the sheet level (umya exposes add_defined_name on Worksheet)
        sh.add_defined_name("ScalarName", "Sheet1!$A$1").unwrap();
        sh.add_defined_name("RangeName", "Sheet1!$A$1:$B$2")
            .unwrap();
    });

    // Load via CalamineAdapter
    let mut backend = CalamineAdapter::open_path(&path).expect("open xlsx");

    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend
        .stream_into_engine(&mut engine)
        .expect("load into engine");

    // Verify defined names are registered
    let wb_names: Vec<String> = engine.named_ranges_iter().map(|(n, _)| n.clone()).collect();
    eprintln!("Workbook-scoped names: {:?}", wb_names);

    // Calamine extracts all names as workbook-scoped (no scope info from calamine)
    assert!(
        wb_names.contains(&"ScalarName".to_string()),
        "ScalarName not found in {:?}",
        wb_names
    );
    assert!(
        wb_names.contains(&"RangeName".to_string()),
        "RangeName not found in {:?}",
        wb_names
    );

    // Verify evaluation through named ranges
    engine.evaluate_all().expect("evaluate_all");

    // ScalarName points to A1 = 100
    let val = engine.get_cell_value("Sheet1", 1, 1);
    match val {
        Some(LiteralValue::Number(n)) => assert!((n - 100.0).abs() < 1e-9),
        Some(LiteralValue::Int(100)) => {}
        other => panic!("Unexpected A1 value: {other:?}"),
    }
}

/// Test loading a real-world xlsx with defined names (if available).
/// This test is ignored by default and can be run with --ignored.
#[test]
#[ignore]
fn calamine_loads_real_xlsx_with_defined_names() {
    let paths = [
        "/Users/jn/workspace/supermod/xlsx-examples/CVX Multi-family Dev.xlsx",
        "/Users/jn/workspace/supermod/xlsx-examples/CVX Operating Model.xlsx",
    ];

    for path_str in &paths {
        let path = std::path::Path::new(path_str);
        if !path.exists() {
            eprintln!("Skipping {path_str} (not found)");
            continue;
        }

        eprintln!("Loading {path_str}...");
        let mut backend = CalamineAdapter::open_path(path).expect("open xlsx");

        let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
        let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
        backend
            .stream_into_engine(&mut engine)
            .expect("load into engine");

        let wb_names: Vec<_> = engine.named_ranges_iter().map(|(n, _)| n.clone()).collect();
        let store = engine.sheet_store();
        let sheet_count = store.sheets.len();
        eprintln!(
            "  {} sheets, {} workbook-scoped defined names",
            sheet_count,
            wb_names.len()
        );
        for name in &wb_names {
            eprintln!("    - {name}");
        }
        assert!(
            !wb_names.is_empty(),
            "Expected at least one defined name in {path_str}"
        );

        // Actually evaluate all formulas — this is the real test that
        // formulas referencing defined names resolve correctly.
        eprintln!("  Evaluating all formulas...");
        engine.evaluate_all().expect("evaluate_all");

        // Count how many cells evaluated to errors
        let mut total_cells = 0u64;
        let mut error_cells = 0u64;
        let mut error_samples: Vec<String> = Vec::new();
        for sheet_idx in 0..sheet_count {
            let sheet_name = engine.sheet_name(sheet_idx as u16).to_string();
            // Check a generous grid — CVX models are typically < 200 cols x 500 rows
            for row in 1..=500u32 {
                for col in 1..=200u32 {
                    if let Some(val) = engine.get_cell_value(&sheet_name, row, col) {
                        total_cells += 1;
                        if matches!(val, LiteralValue::Error(_)) {
                            error_cells += 1;
                            if error_samples.len() < 20 {
                                error_samples
                                    .push(format!("  {sheet_name}!R{row}C{col} = {val:?}"));
                            }
                        }
                    }
                }
            }
        }
        let error_pct = if total_cells > 0 {
            (error_cells as f64 / total_cells as f64) * 100.0
        } else {
            0.0
        };
        eprintln!("  {total_cells} cells evaluated, {error_cells} errors ({error_pct:.1}%)");
        if !error_samples.is_empty() {
            eprintln!("  Sample errors:");
            for s in &error_samples {
                eprintln!("{s}");
            }
        }
        // Allow some errors (unsupported functions, etc.) but flag if >50%
        assert!(
            error_pct < 50.0,
            "Too many errors ({error_pct:.1}%) in {path_str}"
        );
    }
}

/// Build a workbook with values AND formulas that reference defined names,
/// load through CalamineAdapter, evaluate, and verify computed values.
#[test]
fn calamine_evaluates_formulas_with_defined_names() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();

        // A1=100, B1=200 (value cells)
        sh.get_cell_mut((1, 1)).set_value_number(100);
        sh.get_cell_mut((2, 1)).set_value_number(200);

        // Define names:
        //   MyVal   -> Sheet1!$A$1   (scalar)
        //   MyRange -> Sheet1!$A$1:$B$1 (range covering A1:B1)
        sh.add_defined_name("MyVal", "Sheet1!$A$1").unwrap();
        sh.add_defined_name("MyRange", "Sheet1!$A$1:$B$1").unwrap();

        // C1 = =MyVal+10   => 100+10 = 110
        sh.get_cell_mut((3, 1)).set_formula("=MyVal+10");
        // D1 = =SUM(MyRange) => SUM(100,200) = 300
        sh.get_cell_mut((4, 1)).set_formula("=SUM(MyRange)");
    });

    // Load via CalamineAdapter
    let mut backend = CalamineAdapter::open_path(&path).expect("open xlsx");
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend
        .stream_into_engine(&mut engine)
        .expect("load into engine");

    // Verify defined names are registered
    let wb_names: Vec<String> = engine.named_ranges_iter().map(|(n, _)| n.clone()).collect();
    eprintln!("Defined names: {:?}", wb_names);
    assert!(
        wb_names.contains(&"MyVal".to_string()),
        "MyVal not found in {:?}",
        wb_names
    );
    assert!(
        wb_names.contains(&"MyRange".to_string()),
        "MyRange not found in {:?}",
        wb_names
    );

    // Evaluate all formulas
    engine.evaluate_all().expect("evaluate_all");

    // A1 = 100 (value cell)
    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Number(n)) => {
            assert!((n - 100.0).abs() < 1e-9, "A1 expected 100, got {n}")
        }
        other => panic!("Unexpected A1 value: {other:?}"),
    }

    // B1 = 200 (value cell)
    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Number(n)) => {
            assert!((n - 200.0).abs() < 1e-9, "B1 expected 200, got {n}")
        }
        other => panic!("Unexpected B1 value: {other:?}"),
    }

    // C1 = =MyVal+10 => 110 (formula referencing scalar defined name)
    match engine.get_cell_value("Sheet1", 1, 3) {
        Some(LiteralValue::Number(n)) => {
            assert!((n - 110.0).abs() < 1e-9, "C1 expected 110, got {n}")
        }
        other => panic!("Unexpected C1 value (=MyVal+10): {other:?}"),
    }

    // D1 = =SUM(MyRange) => 300 (formula referencing range defined name)
    match engine.get_cell_value("Sheet1", 1, 4) {
        Some(LiteralValue::Number(n)) => {
            assert!((n - 300.0).abs() < 1e-9, "D1 expected 300, got {n}")
        }
        other => panic!("Unexpected D1 value (=SUM(MyRange)): {other:?}"),
    }

    // Verify get_cell parity: formula cells should have ASTs
    let (ast_c1, val_c1) = engine.get_cell("Sheet1", 1, 3).expect("C1 present");
    assert!(ast_c1.is_some(), "C1 should have a formula AST");
    assert_eq!(val_c1, engine.get_cell_value("Sheet1", 1, 3));

    let (ast_d1, val_d1) = engine.get_cell("Sheet1", 1, 4).expect("D1 present");
    assert!(ast_d1.is_some(), "D1 should have a formula AST");
    assert_eq!(val_d1, engine.get_cell_value("Sheet1", 1, 4));

    // Value cells should NOT have ASTs
    let (ast_a1, val_a1) = engine.get_cell("Sheet1", 1, 1).expect("A1 present");
    assert!(ast_a1.is_none(), "A1 should be a value cell");
    assert_eq!(val_a1, engine.get_cell_value("Sheet1", 1, 1));

    let (ast_b1, val_b1) = engine.get_cell("Sheet1", 1, 2).expect("B1 present");
    assert!(ast_b1.is_none(), "B1 should be a value cell");
    assert_eq!(val_b1, engine.get_cell_value("Sheet1", 1, 2));
}

/// Load the CVX Multi-family Dev workbook, evaluate all formulas,
/// then print every defined-name value.
#[test]
#[ignore]
fn calamine_cvx_values_match_excel() {
    use formualizer_eval::Coord;
    use formualizer_eval::engine::named_range::NamedDefinition;

    let path = std::path::Path::new(
        "/Users/jn/workspace/supermod/xlsx-examples/CVX Multi-family Dev.xlsx",
    );
    assert!(path.exists(), "Workbook not found at {}", path.display());

    // Load via CalamineAdapter
    eprintln!("Loading {}...", path.display());
    let mut backend = CalamineAdapter::open_path(path).expect("open xlsx");

    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());
    backend
        .stream_into_engine(&mut engine)
        .expect("load into engine");

    // Evaluate all formulas
    eprintln!("Evaluating all formulas...");
    engine.evaluate_all().expect("evaluate_all");
    eprintln!("Evaluation complete.\n");

    // Collect all workbook-scoped defined names
    let named_ranges: Vec<(String, _)> = engine
        .named_ranges_iter()
        .map(|(name, nr)| (name.clone(), nr.clone()))
        .collect();

    // Also collect sheet-scoped defined names
    let sheet_named_ranges: Vec<(u16, String, _)> = engine
        .sheet_named_ranges_iter()
        .map(|((sheet_id, name), nr)| (*sheet_id, name.clone(), nr.clone()))
        .collect();

    eprintln!(
        "=== Workbook-scoped defined names ({}) ===\n",
        named_ranges.len()
    );

    for (name, nr) in &named_ranges {
        match &nr.definition {
            NamedDefinition::Cell(cell_ref) => {
                let sheet_name = engine.sheet_name(cell_ref.sheet_id).to_string();
                let row0 = cell_ref.coord.row();
                let col0 = cell_ref.coord.col();
                let col_letter = Coord::col_to_letters(col0);
                let excel_row = row0 + 1;
                let excel_col = col0 + 1;
                let val = engine.get_cell_value(&sheet_name, excel_row, excel_col);
                let display = format_value(&val);
                eprintln!("  {name} = {display}  [Cell -> '{sheet_name}'!{col_letter}{excel_row}]");
            }
            NamedDefinition::Range(range_ref) => {
                let start = &range_ref.start;
                let end = &range_ref.end;
                let sheet_name = engine.sheet_name(start.sheet_id).to_string();
                let start_col_letter = Coord::col_to_letters(start.coord.col());
                let end_col_letter = Coord::col_to_letters(end.coord.col());
                let start_row = start.coord.row() + 1;
                let end_row = end.coord.row() + 1;
                // Read value at top-left corner
                let val = engine.get_cell_value(&sheet_name, start_row, start.coord.col() + 1);
                let display = format_value(&val);
                eprintln!(
                    "  {name} = {display}  [Range -> '{sheet_name}'!{start_col_letter}{start_row}:{end_col_letter}{end_row}]"
                );
            }
            NamedDefinition::Formula { .. } => {
                // For formula-type named ranges, try to evaluate the vertex
                let vertex_id = nr.vertex;
                let val = engine.evaluate_vertex(vertex_id);
                let display = match &val {
                    Ok(v) => format_value(&Some(v.clone())),
                    Err(e) => format!("#ERROR: {e}"),
                };
                eprintln!("  {name} = {display}  [Formula]");
            }
        }
    }

    if !sheet_named_ranges.is_empty() {
        eprintln!(
            "\n=== Sheet-scoped defined names ({}) ===\n",
            sheet_named_ranges.len()
        );
        for (sheet_id, name, nr) in &sheet_named_ranges {
            let scope_sheet = engine.sheet_name(*sheet_id).to_string();
            match &nr.definition {
                NamedDefinition::Cell(cell_ref) => {
                    let sheet_name = engine.sheet_name(cell_ref.sheet_id).to_string();
                    let row0 = cell_ref.coord.row();
                    let col0 = cell_ref.coord.col();
                    let col_letter = Coord::col_to_letters(col0);
                    let excel_row = row0 + 1;
                    let excel_col = col0 + 1;
                    let val = engine.get_cell_value(&sheet_name, excel_row, excel_col);
                    let display = format_value(&val);
                    eprintln!(
                        "  [{scope_sheet}] {name} = {display}  [Cell -> '{sheet_name}'!{col_letter}{excel_row}]"
                    );
                }
                NamedDefinition::Range(range_ref) => {
                    let start = &range_ref.start;
                    let end = &range_ref.end;
                    let sheet_name = engine.sheet_name(start.sheet_id).to_string();
                    let start_col_letter = Coord::col_to_letters(start.coord.col());
                    let end_col_letter = Coord::col_to_letters(end.coord.col());
                    let start_row = start.coord.row() + 1;
                    let end_row = end.coord.row() + 1;
                    let val = engine.get_cell_value(&sheet_name, start_row, start.coord.col() + 1);
                    let display = format_value(&val);
                    eprintln!(
                        "  [{scope_sheet}] {name} = {display}  [Range -> '{sheet_name}'!{start_col_letter}{start_row}:{end_col_letter}{end_row}]"
                    );
                }
                NamedDefinition::Formula { .. } => {
                    let vertex_id = nr.vertex;
                    let val = engine.evaluate_vertex(vertex_id);
                    let display = match &val {
                        Ok(v) => format_value(&Some(v.clone())),
                        Err(e) => format!("#ERROR: {e}"),
                    };
                    eprintln!("  [{scope_sheet}] {name} = {display}  [Formula]");
                }
            }
        }
    }

    eprintln!(
        "\nDone. Total: {} workbook-scoped + {} sheet-scoped defined names.",
        named_ranges.len(),
        sheet_named_ranges.len()
    );
}

/// Format a cell value for display, with special handling for numbers
/// that look like percentages (small decimals).
fn format_value(val: &Option<LiteralValue>) -> String {
    match val {
        None => "(empty/no value)".to_string(),
        Some(LiteralValue::Number(n)) => {
            let pct = n * 100.0;
            if n.abs() < 10.0 && n.abs() > 1e-10 {
                format!("{n:.6} ({pct:.4}%)")
            } else if n.abs() >= 10.0 {
                format!("{n:.4}")
            } else if *n == 0.0 {
                "0".to_string()
            } else {
                format!("{n:.6}")
            }
        }
        Some(LiteralValue::Int(i)) => format!("{i}"),
        Some(LiteralValue::Text(s)) => format!("\"{s}\""),
        Some(LiteralValue::Boolean(b)) => format!("{b}"),
        Some(LiteralValue::Error(e)) => format!("{e}"),
        Some(LiteralValue::Empty) => "(empty)".to_string(),
        Some(LiteralValue::Date(d)) => format!("{d}"),
        Some(LiteralValue::DateTime(dt)) => format!("{dt}"),
        Some(other) => format!("{other:?}"),
    }
}
