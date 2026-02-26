use crate::common::build_workbook;
use formualizer_workbook::{
    LiteralValue, SpreadsheetReader, UmyaAdapter, recalculate_file, recalculate_file_with_limit,
};
use std::fs::File;
use std::io::Read;

fn assert_number_or_text_number(value: Option<LiteralValue>, expected: f64) {
    match value {
        Some(LiteralValue::Number(n)) => assert_eq!(n, expected),
        Some(LiteralValue::Text(s)) => {
            let parsed = s.parse::<f64>().expect("numeric text");
            assert_eq!(parsed, expected);
        }
        other => panic!("expected numeric cache {expected}, got {other:?}"),
    }
}

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
    assert_number_or_text_number(b1.value.clone(), 3.0);

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
    assert_number_or_text_number(
        out_sheet.cells.get(&(1, 2)).and_then(|c| c.value.clone()),
        10.0,
    );
}

#[test]
fn recalculate_file_no_formula_cache_changes_copies_input_to_output() {
    let input = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_formula("=\"ok\""); // A1
    });

    // First run seeds cached values.
    recalculate_file(&input, None).expect("seed in-place caches");
    let input_bytes = std::fs::read(&input).expect("read seeded input bytes");

    let out_dir = tempfile::tempdir().expect("tempdir");
    let output = out_dir.path().join("recalc_no_changes.xlsx");

    let summary = recalculate_file(&input, Some(&output)).expect("recalculate to output path");
    assert_eq!(summary.status.as_str(), "success");
    assert_eq!(summary.evaluated, 1);
    assert_eq!(summary.errors, 0);

    let output_bytes = std::fs::read(&output).expect("read copied output bytes");
    assert_eq!(input_bytes, output_bytes);
}

fn sheet1_cell_fragment(path: &std::path::Path, cell_ref: &str) -> String {
    let file = File::open(path).expect("open xlsx");
    let mut zip = zip::ZipArchive::new(file).expect("zip archive");
    let mut sheet_xml = String::new();
    zip.by_name("xl/worksheets/sheet1.xml")
        .expect("sheet1.xml")
        .read_to_string(&mut sheet_xml)
        .expect("read sheet xml");

    let marker = format!("<c r=\"{cell_ref}\"");
    let start = sheet_xml.find(&marker).expect("cell marker present");
    let rest = &sheet_xml[start..];
    let end_rel = rest.find("</c>").expect("cell closing tag");
    rest[..end_rel + 4].to_string()
}

#[test]
fn recalculate_numeric_formula_cache_rewrites_string_typed_cache_to_numeric_type() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(1); // A1
        sh.get_cell_mut((1, 2)).set_value_number(2); // A2
        // Seed a stale string-typed cache on a numeric formula result.
        sh.get_cell_mut((2, 1))
            .set_formula("=A1+A2")
            .set_formula_result_string("3"); // B1
    });

    let before = sheet1_cell_fragment(&path, "B1");
    assert!(
        before.contains("t=\"str\""),
        "expected precondition with string-typed formula cache, got: {before}"
    );

    recalculate_file(&path, None).expect("recalculate in place");

    let after = sheet1_cell_fragment(&path, "B1");
    assert!(
        !after.contains("t=\"str\""),
        "formula cache should not remain string-typed after numeric recalc, got: {after}"
    );
    assert!(
        after.contains("<f>A1+A2</f>") || after.contains("<f>=A1+A2</f>"),
        "formula text must be preserved: {after}"
    );
    assert!(
        after.contains("<v>3</v>") || after.contains("<v>3.0</v>"),
        "numeric cached value must be written, got: {after}"
    );
}
