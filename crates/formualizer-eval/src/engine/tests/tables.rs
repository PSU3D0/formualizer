use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use formualizer_common::LiteralValue;

#[test]
fn structured_ref_table_column_tracks_cell_edits_via_table_vertex() {
    let ctx = crate::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());

    engine.add_sheet("Sheet1").unwrap();

    // Table region A1:B3 (header + 2 data rows)
    // Headers: Region, Amount
    // Make the non-selected column numeric so we catch over-wide selection bugs.
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(7.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 2, LiteralValue::Number(20.0))
        .unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let start = CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true));
    let end = CellRef::new(sheet_id, Coord::from_excel(3, 2, true, true));
    let range = RangeRef::new(start, end);
    engine
        .define_table(
            "Sales",
            range,
            true,
            vec!["Region".into(), "Amount".into()],
            false,
        )
        .unwrap();

    let ast = formualizer_parse::parser::parse("=SUM(Sales[Amount])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 4, ast).unwrap();

    let v = engine
        .evaluate_cell("Sheet1", 1, 4)
        .unwrap()
        .expect("computed value");
    assert_eq!(v, LiteralValue::Number(30.0));

    // Edit a precedent cell inside the table and ensure the table-dependent formula is dirtied.
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(100.0))
        .unwrap();
    let v2 = engine
        .evaluate_cell("Sheet1", 1, 4)
        .unwrap()
        .expect("computed value");
    assert_eq!(v2, LiteralValue::Number(120.0));
}

#[test]
fn structured_ref_this_row_column_rewrites_to_concrete_cell() {
    let ctx = crate::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());

    engine.add_sheet("Sheet1").unwrap();

    // Table region A1:C3 (header + 2 data rows)
    // Headers: Region, Amount, Double
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Text("N".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Text("S".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 2, LiteralValue::Number(20.0))
        .unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let start = CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true));
    let end = CellRef::new(sheet_id, Coord::from_excel(3, 3, true, true));
    let range = RangeRef::new(start, end);
    engine
        .define_table(
            "Sales",
            range,
            true,
            vec!["Region".into(), "Amount".into(), "Double".into()],
            false,
        )
        .unwrap();

    // Formula inside the table: Double = [@Amount] * 2
    let f2 = formualizer_parse::parser::parse("=[@Amount]*2").unwrap();
    engine.set_cell_formula("Sheet1", 2, 3, f2).unwrap();
    let f3 = formualizer_parse::parser::parse("=[@[Amount]]*2").unwrap();
    engine.set_cell_formula("Sheet1", 3, 3, f3).unwrap();

    let v2 = engine
        .evaluate_cell("Sheet1", 2, 3)
        .unwrap()
        .expect("computed value");
    assert_eq!(v2, LiteralValue::Number(20.0));
    let v3 = engine
        .evaluate_cell("Sheet1", 3, 3)
        .unwrap()
        .expect("computed value");
    assert_eq!(v3, LiteralValue::Number(40.0));

    // Editing Amount should dirty and recompute the this-row dependents.
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(100.0))
        .unwrap();
    let v2b = engine
        .evaluate_cell("Sheet1", 2, 3)
        .unwrap()
        .expect("computed value");
    assert_eq!(v2b, LiteralValue::Number(200.0));
}

#[test]
fn structured_ref_bracket_table_shorthand_selects_data_body() {
    let ctx = crate::test_workbook::TestWorkbook::new();
    let mut engine: Engine<_> = Engine::new(ctx, EvalConfig::default());

    engine.add_sheet("Sheet1").unwrap();

    // Table region A1:B3 (header + 2 data rows)
    // Headers: Region, Amount
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(100.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(7.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 2, LiteralValue::Number(20.0))
        .unwrap();

    let sheet_id = engine.sheet_id("Sheet1").unwrap();
    let start = CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true));
    let end = CellRef::new(sheet_id, Coord::from_excel(3, 2, true, true));
    let range = RangeRef::new(start, end);
    engine
        .define_table(
            "Sales",
            range,
            true,
            vec!["Region".into(), "Amount".into()],
            false,
        )
        .unwrap();

    let ast = formualizer_parse::parser::parse("=SUM([Sales])").unwrap();
    engine.set_cell_formula("Sheet1", 1, 4, ast).unwrap();

    // Header cell (B1=100) should not be included; only data body should contribute.
    let v = engine
        .evaluate_cell("Sheet1", 1, 4)
        .unwrap()
        .expect("computed value");
    assert_eq!(v, LiteralValue::Number(42.0));
}
