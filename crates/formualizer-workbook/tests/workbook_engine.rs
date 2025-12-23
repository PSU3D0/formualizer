use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_workbook::{LoadStrategy, Workbook, WorkbookConfig};

#[test]
fn values_roundtrip_and_range() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(10)).unwrap();
    wb.set_value("S", 2, 1, LiteralValue::Number(2.5)).unwrap();

    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Int(10)));
    assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Number(2.5)));

    let ra = RangeAddress::new("S", 1, 1, 2, 1).unwrap();
    let vals = wb.read_range(&ra);
    assert_eq!(vals.len(), 2);
    // read_range reads from Arrow which stores Int as Number
    assert_eq!(vals[0][0], LiteralValue::Number(10.0));
    assert_eq!(vals[1][0], LiteralValue::Number(2.5));
}

#[test]
fn deferred_formula_evaluation() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(7)).unwrap();
    // Stage a formula without '='; deferred graph building should handle it
    wb.set_formula("S", 1, 2, "A1*3").unwrap();

    // Not evaluated yet; no value stored for a staged formula
    assert_eq!(wb.get_value("S", 1, 2), Some(LiteralValue::Empty));

    // Demand-driven eval builds graph for sheet S and computes
    let v = wb.evaluate_cell("S", 1, 2).unwrap();
    assert_eq!(v, LiteralValue::Number(21.0));
}

#[cfg(feature = "json")]
#[test]
fn load_from_json_reader() {
    use formualizer_workbook::backends::JsonAdapter;
    use formualizer_workbook::traits::SpreadsheetWriter;

    let mut json = JsonAdapter::new();
    json.create_sheet("S").unwrap();
    json.write_cell(
        "S",
        1,
        1,
        formualizer_workbook::traits::CellData::from_value(LiteralValue::Int(4)),
    )
    .unwrap();
    json.write_cell(
        "S",
        1,
        2,
        formualizer_workbook::traits::CellData::from_formula("A1*5"),
    )
    .unwrap();

    let cfg = WorkbookConfig::interactive();
    let mut wb = Workbook::from_reader(json, LoadStrategy::EagerAll, cfg).unwrap();
    // Deferred mode: formula not evaluated yet (no stored value)
    assert_eq!(wb.get_value("S", 1, 2), Some(LiteralValue::Empty));
    // Evaluate
    let v = wb.evaluate_cell("S", 1, 2).unwrap();
    assert_eq!(v, LiteralValue::Number(20.0));
}

#[test]
fn value_edit_triggers_recompute_in_deferred_mode() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(3)).unwrap();
    wb.set_formula("S", 1, 2, "A1*2").unwrap();

    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Int(3)));
    assert_eq!(wb.get_formula("S", 1, 2), Some("A1*2".to_string()));

    let v = wb.evaluate_cell("S", 1, 2).unwrap();
    assert_eq!(v, LiteralValue::Number(6.0));

    // Edit precedent A1 and ensure recompute happens on next evaluate
    wb.set_value("S", 1, 1, LiteralValue::Int(10)).unwrap();
    let v2 = wb.evaluate_cell("S", 1, 2).unwrap();
    assert_eq!(v2, LiteralValue::Number(20.0));
}

#[test]
fn staged_formula_edit_recomputes_on_demand() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(5)).unwrap();
    wb.set_formula("S", 1, 2, "A1*2").unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 2).unwrap(),
        LiteralValue::Number(10.0)
    );

    // Change formula text; with deferred mode this is staged and will rebuild on evaluate
    wb.set_formula("S", 1, 2, "A1*3").unwrap();
    let v2 = wb.evaluate_cell("S", 1, 2).unwrap();
    assert_eq!(v2, LiteralValue::Number(15.0));
}

#[test]
fn bulk_write_mixed_values_and_formulas() {
    use formualizer_workbook::traits::CellData;
    use std::collections::BTreeMap;

    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    let mut cells: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
    cells.insert((1, 1), CellData::from_value(LiteralValue::Int(2)));
    cells.insert((1, 2), CellData::from_formula("A1+8"));
    wb.write_range("S", (1, 1), cells).unwrap();

    assert_eq!(
        wb.evaluate_cell("S", 1, 2).unwrap(),
        LiteralValue::Number(10.0)
    );

    // Overwrite both via bulk
    let mut cells2: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
    cells2.insert((1, 1), CellData::from_value(LiteralValue::Int(4)));
    cells2.insert((1, 2), CellData::from_formula("A1+6"));
    wb.write_range("S", (1, 1), cells2).unwrap();

    assert_eq!(
        wb.evaluate_cell("S", 1, 2).unwrap(),
        LiteralValue::Number(10.0)
    );
}

#[test]
fn set_values_batch_and_undo() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb.set_changelog_enabled(true);

    let rows = vec![
        vec![LiteralValue::Int(1), LiteralValue::Int(2)],
        vec![LiteralValue::Int(3), LiteralValue::Int(4)],
    ];
    wb.begin_action("seed");
    wb.set_values("S", 1, 1, &rows).unwrap();
    wb.end_action();

    let ra = RangeAddress::new("S", 1, 1, 2, 2).unwrap();
    let vals = wb.read_range(&ra);
    // read_range reads from Arrow which stores Int as Number
    assert_eq!(vals[0][0], LiteralValue::Number(1.0));
    assert_eq!(vals[1][1], LiteralValue::Number(4.0));

    wb.undo().unwrap();
    let vals2 = wb.read_range(&ra);
    assert_eq!(vals2[0][0], LiteralValue::Empty);
    assert_eq!(vals2[1][1], LiteralValue::Empty);
    wb.redo().unwrap();
    let vals3 = wb.read_range(&ra);
    assert_eq!(vals3[0][0], LiteralValue::Number(1.0));
    assert_eq!(vals3[1][1], LiteralValue::Number(4.0));
}

#[test]
fn set_formulas_batch_deferred_then_eval() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb.set_values(
        "S",
        1,
        1,
        &[vec![LiteralValue::Int(5), LiteralValue::Int(6)]],
    )
    .unwrap();
    let forms = vec![vec!["A1*2".to_string(), "B1+1".to_string()]];
    wb.set_formulas("S", 2, 1, &forms).unwrap();
    // No values yet for staged formulas
    assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Empty));
    // Evaluate both
    let out = wb.evaluate_cells(&[("S", 2, 1), ("S", 2, 2)]).unwrap();
    assert_eq!(out[0], LiteralValue::Number(10.0));
    assert_eq!(out[1], LiteralValue::Number(7.0));
}

#[test]
fn changelog_undo_redo_values() {
    // Use default deferred mode
    let mut wb = Workbook::new();
    wb.set_changelog_enabled(true);
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(1)).unwrap();
    wb.set_value("S", 1, 2, LiteralValue::Int(2)).unwrap();
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Int(1)));

    wb.begin_action("edit A1");
    wb.set_value("S", 1, 1, LiteralValue::Int(5)).unwrap();
    wb.end_action();
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Int(5)));

    wb.undo().unwrap();
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Int(1)));
    wb.redo().unwrap();
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Int(5)));
}

#[test]
fn changelog_with_formulas_non_deferred() {
    // Disable deferral so formula graph exists → logs capture formula edits
    let mut cfg = WorkbookConfig::ephemeral();
    cfg.eval.defer_graph_building = false;
    let mut wb = Workbook::new_with_config(cfg);
    wb.set_changelog_enabled(true);
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(2)).unwrap();
    wb.set_formula("S", 1, 2, "A1*3").unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 2).unwrap(),
        LiteralValue::Number(6.0)
    );

    wb.begin_action("change A1");
    wb.set_value("S", 1, 1, LiteralValue::Int(4)).unwrap();
    wb.end_action();
    assert_eq!(
        wb.evaluate_cell("S", 1, 2).unwrap(),
        LiteralValue::Number(12.0)
    );

    wb.undo().unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 2).unwrap(),
        LiteralValue::Number(6.0)
    );
    wb.redo().unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 2).unwrap(),
        LiteralValue::Number(12.0)
    );
}
