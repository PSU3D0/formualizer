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
    let end = CellRef::new(sheet_id, Coord::from_excel(3, 2, true, true));
    let range = RangeRef::new(start, end);
    engine
        .define_table(
            "Sales",
            range,
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
