use crate::common::build_workbook;
use formualizer_workbook::{
    LiteralValue, SpreadsheetReader, UmyaAdapter, recalculate_file, recalculate_file_with_limit,
};

#[test]
fn recalculate_file_in_place_writes_cached_values_and_preserves_formulas() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(1); // A1
        sh.get_cell_mut((1, 2)).set_value_number(2); // A2
        sh.get_cell_mut((2, 1))
            .set_formula("=_xlfn._xlws.SUM(A1:A2)"); // B1
        sh.get_cell_mut((2, 2)).set_formula("=1/0"); // B2
    });

    let summary = recalculate_file_with_limit(&path, None, 10).expect("recalculate in place");

    assert_eq!(summary.status.as_str(), "errors_found");
    assert_eq!(summary.evaluated, 2);
    assert_eq!(summary.errors, 1);
    assert_eq!(summary.sheets["Sheet1"].evaluated, 2);
    assert_eq!(summary.sheets["Sheet1"].errors, 1);
    assert_eq!(summary.error_summary["#DIV/0!"].count, 1);
    assert_eq!(
        summary.error_summary["#DIV/0!"].locations,
        vec!["Sheet1!B2"]
    );

    let mut adapter = UmyaAdapter::open_path(&path).expect("open output");
    let sheet = adapter.read_sheet("Sheet1").expect("read sheet");

    let b1 = sheet.cells.get(&(1, 2)).expect("B1");
    assert_eq!(b1.formula.as_deref(), Some("=_xlfn._xlws.SUM(A1:A2)"));
    assert_eq!(b1.value, Some(LiteralValue::Text("3".to_string())));

    let b2 = sheet.cells.get(&(2, 2)).expect("B2");
    assert_eq!(b2.formula.as_deref(), Some("=1/0"));
    assert!(
        matches!(
            b2.value,
            Some(LiteralValue::Error(ref e)) if e.kind.to_string() == "#DIV/0!"
        ) || b2.value == Some(LiteralValue::Text("#DIV/0!".to_string()))
    );
}

#[test]
fn recalculate_file_output_path_does_not_mutate_input() {
    let input = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(4); // A1
        sh.get_cell_mut((1, 2)).set_value_number(6); // A2
        sh.get_cell_mut((2, 1)).set_formula("=A1+A2"); // B1
    });

    let out_dir = tempfile::tempdir().expect("tempdir");
    let output = out_dir.path().join("recalc_output.xlsx");

    let summary = recalculate_file(&input, Some(&output)).expect("recalculate to output path");
    assert_eq!(summary.status.as_str(), "success");
    assert_eq!(summary.evaluated, 1);
    assert_eq!(summary.errors, 0);

    let mut in_adapter = UmyaAdapter::open_path(&input).expect("open input");
    let in_sheet = in_adapter.read_sheet("Sheet1").expect("read input");
    assert_eq!(
        in_sheet.cells.get(&(1, 2)).and_then(|c| c.value.clone()),
        None,
        "input workbook should remain unchanged when output path is provided"
    );

    let mut out_adapter = UmyaAdapter::open_path(&output).expect("open output");
    let out_sheet = out_adapter.read_sheet("Sheet1").expect("read output");
    assert_eq!(
        out_sheet.cells.get(&(1, 2)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Text("10".to_string()))
    );
}
