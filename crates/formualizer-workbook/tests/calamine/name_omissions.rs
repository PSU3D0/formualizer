use crate::common::build_workbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_workbook::traits::LoadStrategy;
use formualizer_workbook::workbook::WorkbookConfig;
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader, Workbook};

fn load(
    definitions: &[(&str, &str)],
    formula: &str,
) -> Result<Workbook, formualizer_workbook::IoError> {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sheet.get_cell_mut((1, 1)).set_formula(formula);
        for (name, definition) in definitions {
            sheet.add_defined_name(*name, *definition).unwrap();
        }
    });
    let adapter = CalamineAdapter::open_path(path).unwrap();
    Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
}

#[test]
fn unused_unevaluable_names_are_diagnosed_and_omitted() {
    for definitions in [
        vec![("Unused", "Missing")],
        vec![("Unused", "[0]!Macro")],
        vec![("Unused", "{1,2}")],
        vec![("Unused", "OFFSET(#REF!,0,0,2,1)")],
        vec![("Unused", "Other"), ("Other", "Unused")],
    ] {
        let mut workbook = load(&definitions, "1+1").unwrap();
        workbook.evaluate_all().unwrap();
        assert_eq!(
            workbook.get_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(2.0))
        );
        let diagnostics = workbook.name_import_diagnostics();
        assert_eq!(diagnostics.len(), definitions.len(), "{definitions:?}");
        assert!(diagnostics.iter().any(|d| d.name == "Unused"));
        assert!(diagnostics.iter().all(|d| !d.message.is_empty()));
    }
}

#[test]
fn referenced_omitted_name_is_not_blank() {
    for definition in ["Missing", "[0]!Macro", "OFFSET(#REF!,0,0,2,1)", "Unused"] {
        let mut workbook = load(&[("Unused", definition)], "Unused").unwrap();
        workbook.evaluate_all().unwrap();
        assert!(
            matches!(workbook.get_value("Sheet1", 1, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Name)
        );
    }
}

#[test]
fn omitted_local_name_keeps_shadowing_a_global_name_after_edits() {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sheet.get_cell_mut((1, 1)).set_formula("1+1");
        sheet.add_defined_name("Rate", "7").unwrap();
        sheet.add_defined_name("Rate", "Missing").unwrap();
        sheet
            .get_defined_names_mut()
            .last_mut()
            .unwrap()
            .set_local_sheet_id(0);
        book.new_sheet("OtherSheet")
            .unwrap()
            .get_cell_mut((1, 1))
            .set_formula("Rate");
    });
    let adapter = CalamineAdapter::open_path(path).unwrap();
    let mut workbook = Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    assert_eq!(workbook.name_import_diagnostics().len(), 1);
    assert_eq!(
        workbook.name_import_diagnostics()[0].scope_sheet.as_deref(),
        Some("Sheet1")
    );
    let sheet_id = workbook.engine().sheet_id("Sheet1").unwrap();
    let entries = workbook.engine().named_ranges_snapshot_for_sheet(sheet_id);
    assert!(entries.iter().any(|entry| entry.name == "Rate" && matches!(&entry.definition,
        formualizer_eval::engine::named_range::NamedDefinition::Literal(LiteralValue::Error(e))
        if e.kind == ExcelErrorKind::Name && e.message.as_deref() == Some("Defined name `Rate` could not be evaluated"))));
    workbook.set_formula("Sheet1", 2, 1, "=Rate").unwrap();
    workbook
        .set_formula("Sheet1", 3, 1, "=INDIRECT(\"Rate\")")
        .unwrap();
    workbook.evaluate_all().unwrap();
    assert_eq!(
        workbook.get_value("OtherSheet", 1, 1),
        Some(LiteralValue::Number(7.0))
    );
    for row in [2, 3] {
        let value = workbook.get_value("Sheet1", row, 1);
        assert!(
            matches!(&value, Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Name),
            "row {row}: {value:?}"
        );
    }
}

#[test]
fn later_formula_reading_omitted_name_is_not_blank() {
    let mut workbook = load(&[("Unused", "Missing")], "1+1").unwrap();
    workbook.set_formula("Sheet1", 2, 1, "=Unused").unwrap();
    workbook.evaluate_all().unwrap();
    assert!(
        matches!(workbook.get_value("Sheet1", 2, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Name)
    );
}
