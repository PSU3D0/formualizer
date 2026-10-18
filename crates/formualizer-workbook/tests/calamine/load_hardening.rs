//! Workbook files that used to panic or abort the Calamine loader: built-in
//! defined names past the grid, one cell at the far corner of the grid, and
//! non-finite numbers in `<v>`.

use super::external_links::source_xlsx::*;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

fn load(bytes: Vec<u8>) -> Workbook {
    Workbook::from_reader(
        CalamineAdapter::open_bytes(bytes).unwrap(),
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap()
}

fn recalc(bytes: &[u8]) -> Result<(), String> {
    formualizer_workbook::recalculate_xlsx_bytes(bytes, Default::default())
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn error_kind(value: Option<LiteralValue>) -> ExcelErrorKind {
    match value {
        Some(LiteralValue::Error(e)) => e.kind,
        other => panic!("expected an error, got {other:?}"),
    }
}

#[test]
fn built_in_names_past_the_grid_are_left_undefined() {
    for name in [
        r#"<definedName name="_xlnm.Print_Area" localSheetId="0">Sheet1!$A$1:$B$1048577</definedName>"#,
        r#"<definedName name="_xlnm._FilterDatabase" localSheetId="0" hidden="1">Sheet1!$A$1:$B$1048577</definedName>"#,
        r#"<definedName name="_xlnm.Print_Titles" localSheetId="0">Sheet1!$1:$1048577</definedName>"#,
        r#"<definedName name="_xlnm.Print_Area" localSheetId="0">Sheet1!$A$1:$XFE$2</definedName>"#,
    ] {
        let bytes = book(
            &[Ws::new(
                "Sheet1",
                vec![num("A1", 1.0), formula("B1", "A1*2")],
            )],
            name,
        );
        let mut wb = load(bytes.clone());
        wb.evaluate_all().unwrap();
        assert_eq!(
            wb.get_value("Sheet1", 1, 2),
            Some(LiteralValue::Number(2.0))
        );
        recalc(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

#[test]
fn a_built_in_name_past_the_grid_reads_like_an_ordinary_one() {
    let read = |name: &str| {
        let defined = format!(
            r#"<definedName name="{name}" localSheetId="0">Sheet1!$A$1:$B$1048577</definedName>"#
        );
        let bytes = book(
            &[Ws::new(
                "Sheet1",
                vec![num("A1", 1.0), formula("C1", &format!("SUM({name})"))],
            )],
            &defined,
        );
        let mut wb = load(bytes.clone());
        wb.evaluate_all().unwrap();
        (
            error_kind(wb.get_value("Sheet1", 1, 3)),
            recalc(&bytes).is_err(),
        )
    };
    let ordinary = read("Big");
    assert_eq!(ordinary, (ExcelErrorKind::Name, true));
    assert_eq!(read("_xlnm.Print_Area"), ordinary);
}

fn far_corner_book() -> Vec<u8> {
    book(
        &[
            Ws::new("Sheet1", vec![num("A1", 1.0), num("XFD1048576", 2.0)])
                .pre(r#"<dimension ref="A1:XFD1048576"/>"#),
        ],
        "",
    )
}

#[test]
fn far_corner_cell_reads_sparsely() {
    let mut adapter = CalamineAdapter::open_bytes(far_corner_book()).unwrap();
    assert_eq!(adapter.sheet_bounds("Sheet1"), Some((1_048_576, 16_384)));
    let sheet = adapter.read_sheet("Sheet1").unwrap();
    assert_eq!(sheet.dimensions, Some((1_048_576, 16_384)));
    assert_eq!(sheet.cells.len(), 2);
    assert_eq!(
        sheet
            .cells
            .get(&(1_048_576, 16_384))
            .and_then(|c| c.value.clone()),
        Some(LiteralValue::Number(2.0))
    );
}

/// A workbook with an external link part, so the loader scans formula cells
/// for external references, and one formula at the far corner of the grid.
fn linked_far_formula_book() -> Vec<u8> {
    let bytes = book(
        &[Ws::new(
            "Sheet1",
            vec![formula("A1", "[1]Data!A1"), formula("XFD1048576", "1+1")],
        )],
        "",
    );
    let mut parts = unpack(&bytes);
    parts.insert(
        "xl/externalLinks/externalLink1.xml".into(),
        format!(
            "<externalLink xmlns=\"{MAIN}\"><externalBook xmlns:r=\"{OFFICE}\" r:id=\"rId1\"><sheetNames><sheetName val=\"Data\"/></sheetNames><sheetDataSet><sheetData sheetId=\"0\"><row r=\"1\"><cell r=\"A1\"><v>7</v></cell></row></sheetData></sheetDataSet></externalBook></externalLink>"
        ),
    );
    parts.insert(
        "xl/externalLinks/_rels/externalLink1.xml.rels".into(),
        format!(
            "<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/externalLinkPath\" Target=\"file:///missing.xlsx\" TargetMode=\"External\"/></Relationships>"
        ),
    );
    let parts = edit(
        parts,
        "xl/workbook.xml",
        "</sheets>",
        "</sheets><externalReferences><externalReference r:id=\"rIdLink\"/></externalReferences>",
    );
    let parts = edit(
        parts,
        WB_RELS,
        "</Relationships>",
        &format!(
            "<Relationship Id=\"rIdLink\" Type=\"{OFFICE}/externalLink\" Target=\"externalLinks/externalLink1.xml\"/></Relationships>"
        ),
    );
    let parts = edit(
        parts,
        TYPES,
        "</Types>",
        "<Override PartName=\"/xl/externalLinks/externalLink1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.externalLink+xml\"/></Types>",
    );
    pack(&parts)
}

#[test]
fn external_link_scan_reads_formula_cells_sparsely() {
    let mut wb = load(linked_far_formula_book());
    wb.evaluate_all().unwrap();
    assert_eq!(
        wb.get_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(
        wb.get_value("Sheet1", 1_048_576, 16_384),
        Some(LiteralValue::Number(2.0))
    );
}

fn non_finite_book() -> Vec<u8> {
    let cell = |r: &str, v: &str| (r.to_owned(), format!("<c r=\"{r}\"><v>{v}</v></c>"));
    book(
        &[Ws::new(
            "Sheet1",
            vec![
                num("A1", 1.0),
                cell("A2", "NaN"),
                num("A3", 3.0),
                cell("A4", "inf"),
                cell("A5", "-Infinity"),
                formula("B1", "MEDIAN(A1:A3)"),
                formula("B2", "ISERROR(A2)"),
                formula("B3", "SMALL(A1:A3,1)"),
                formula("B4", "SUM(A1,A3)"),
            ],
        )],
        "",
    )
}

#[test]
fn non_finite_numbers_load_as_num_errors() {
    let mut wb = load(non_finite_book());
    for row in [2, 4, 5] {
        assert_eq!(
            error_kind(wb.get_value("Sheet1", row, 1)),
            ExcelErrorKind::Num
        );
    }
    wb.evaluate_all().unwrap();
    assert_eq!(
        error_kind(wb.get_value("Sheet1", 1, 2)),
        ExcelErrorKind::Num
    );
    assert_eq!(
        wb.get_value("Sheet1", 2, 2),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(
        error_kind(wb.get_value("Sheet1", 3, 2)),
        ExcelErrorKind::Num
    );
    assert_eq!(
        wb.get_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(4.0))
    );

    let mut adapter = CalamineAdapter::open_bytes(non_finite_book()).unwrap();
    let sheet = adapter.read_sheet("Sheet1").unwrap();
    for row in [2, 4, 5] {
        assert!(matches!(
            sheet.cells.get(&(row, 1)).and_then(|c| c.value.clone()),
            Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Num
        ));
    }
}
