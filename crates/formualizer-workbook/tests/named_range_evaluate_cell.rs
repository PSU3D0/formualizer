/// Regression tests for named range dependency resolution in evaluate_cell.
///
/// Bug: `evaluate_cell` (demand-driven evaluation) did not traverse
/// `NamedScalar` / `NamedArray` vertices in `build_demand_subgraph`,
/// so formula cells referenced by a named range were never evaluated
/// before the dependent formula read them — yielding 0 instead of
/// the correct result.
use formualizer_common::LiteralValue;
use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
use formualizer_eval::reference::{CellRef, Coord, RangeRef};
use formualizer_workbook::Workbook;

/// A formula referencing a named *range* (multiple cells with formulas)
/// must evaluate those cells before using them.
///
/// Layout:
///   A1 = 10  (plain value)
///   B1 = =A1*1  (formula → 10)
///   B2 = =A1*2  (formula → 20)
///   B3 = =A1*3  (formula → 30)
///   Named range "values" → B1:B3
///   C1 = =SUM(values)  → should be 60
#[test]
fn evaluate_cell_resolves_named_range_over_formula_cells() {
    let mut wb = Workbook::new();
    wb.add_sheet("Sheet1").unwrap();

    let sheet_id = wb.engine_mut().sheet_id_mut("Sheet1");

    // A1 = 10 (plain value)
    wb.engine_mut()
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    // B1 = =A1*1, B2 = =A1*2, B3 = =A1*3
    for (row, factor) in [(1, 1), (2, 2), (3, 3)] {
        let formula = format!("=A1*{factor}");
        let ast = formualizer_parse::parser::parse(&formula).expect("parse");
        wb.engine_mut()
            .set_cell_formula("Sheet1", row, 2, ast)
            .unwrap();
    }

    // Define named range "values" → B1:B3
    let start = CellRef::new(sheet_id, Coord::new(0, 1, true, true));
    let end = CellRef::new(sheet_id, Coord::new(2, 1, true, true));
    wb.engine_mut()
        .define_name(
            "values",
            NamedDefinition::Range(RangeRef::new(start, end)),
            NameScope::Workbook,
        )
        .unwrap();

    // C1 = =SUM(values)
    let sum_ast = formualizer_parse::parser::parse("=SUM(values)").expect("parse SUM");
    wb.engine_mut()
        .set_cell_formula("Sheet1", 1, 3, sum_ast)
        .unwrap();

    // Demand-driven evaluation of C1 only — should recursively evaluate B1:B3
    let result = wb.evaluate_cell("Sheet1", 1, 3).expect("evaluate_cell");
    assert!(
        matches!(result, LiteralValue::Number(n) if (n - 60.0).abs() < 1e-9),
        "expected SUM(values) = 60, got {result:?}"
    );
}

/// Same scenario but with evaluate_all — this already works, confirming
/// the bug is specific to the demand-driven (evaluate_cell) path.
#[test]
fn evaluate_all_resolves_named_range_over_formula_cells() {
    let mut wb = Workbook::new();
    wb.add_sheet("Sheet1").unwrap();

    let sheet_id = wb.engine_mut().sheet_id_mut("Sheet1");

    wb.engine_mut()
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    for (row, factor) in [(1, 1), (2, 2), (3, 3)] {
        let formula = format!("=A1*{factor}");
        let ast = formualizer_parse::parser::parse(&formula).expect("parse");
        wb.engine_mut()
            .set_cell_formula("Sheet1", row, 2, ast)
            .unwrap();
    }

    let start = CellRef::new(sheet_id, Coord::new(0, 1, true, true));
    let end = CellRef::new(sheet_id, Coord::new(2, 1, true, true));
    wb.engine_mut()
        .define_name(
            "values",
            NamedDefinition::Range(RangeRef::new(start, end)),
            NameScope::Workbook,
        )
        .unwrap();

    let sum_ast = formualizer_parse::parser::parse("=SUM(values)").expect("parse SUM");
    wb.engine_mut()
        .set_cell_formula("Sheet1", 1, 3, sum_ast)
        .unwrap();

    wb.evaluate_all().expect("evaluate_all");
    let result = wb
        .engine()
        .get_cell_value("Sheet1", 1, 3)
        .expect("get value");
    assert!(
        matches!(result, LiteralValue::Number(n) if (n - 60.0).abs() < 1e-9),
        "expected SUM(values) = 60 via evaluate_all, got {result:?}"
    );
}

/// Mirrors the exact loading path used by `from_path()` / calamine backend
/// with `defer_graph_building = true`:
///   1. Values are set (arrow storage)
///   2. Formulas are STAGED (not immediately built into graph)
///   3. Named ranges are defined (which creates NamedArray vertices + cell vertices)
///   4. `evaluate_cell` triggers `build_graph_for_sheets` → processes staged formulas
///
/// This verifies that named-range dependencies are properly resolved when
/// formulas go through the deferred graph building path.
#[test]
fn evaluate_cell_deferred_graph_with_named_range() {
    use formualizer_workbook::WorkbookConfig;

    let config = WorkbookConfig::interactive(); // defer_graph_building = true
    let mut wb = Workbook::new_with_config(config);
    wb.add_sheet("Sheet1").unwrap();

    let sheet_id = wb.engine_mut().sheet_id_mut("Sheet1");

    // A1 = 10 (plain value)
    wb.engine_mut()
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    // Stage formulas (mimicking calamine's defer_graph_building path)
    // B1 = =A1*1, B2 = =A1*2, B3 = =A1*3
    for (row, factor) in [(1u32, 1), (2, 2), (3, 3)] {
        let formula = format!("A1*{factor}");
        wb.engine_mut()
            .stage_formula_text("Sheet1", row, 2, formula);
    }

    // C1 = =SUM(values)  — references named range defined below
    wb.engine_mut()
        .stage_formula_text("Sheet1", 1, 3, "SUM(values)".to_string());

    // Define named range "values" → B1:B3 (0-based coords)
    let start = CellRef::new(sheet_id, Coord::new(0, 1, true, true));
    let end = CellRef::new(sheet_id, Coord::new(2, 1, true, true));
    wb.engine_mut()
        .define_name(
            "values",
            NamedDefinition::Range(RangeRef::new(start, end)),
            NameScope::Workbook,
        )
        .unwrap();

    // Demand-driven evaluation of C1 — triggers build_graph_for_sheets
    let result = wb.evaluate_cell("Sheet1", 1, 3).expect("evaluate_cell");
    assert!(
        matches!(result, LiteralValue::Number(n) if (n - 60.0).abs() < 1e-9),
        "expected SUM(values) = 60 via deferred graph + evaluate_cell, got {result:?}"
    );
}
