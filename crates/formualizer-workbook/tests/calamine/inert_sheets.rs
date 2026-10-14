use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_workbook::traits::LoadStrategy;
use formualizer_workbook::workbook::WorkbookConfig;
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader, Workbook};
use std::io::{Cursor, Write};

fn fixture(kind: &str, formula: &str) -> Vec<u8> {
    let main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    let office = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    let rid = if kind == "module" { "" } else { "rId3" };
    let mut parts = vec![
        ("[Content_Types].xml".to_owned(), "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"xml\" ContentType=\"application/xml\"/><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/></Types>".to_owned()),
        ("_rels/.rels".into(), format!("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"{office}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml".into(), format!("<workbook xmlns=\"{main}\" xmlns:r=\"{office}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/><sheet name=\"Chart1\" sheetId=\"3\" r:id=\"{rid}\"/><sheet name=\"After\" sheetId=\"2\" r:id=\"rId2\"/></sheets><definedNames><definedName name=\"Rate\" localSheetId=\"2\">7</definedName><definedName name=\"Omitted\" localSheetId=\"1\">9</definedName></definedNames></workbook>")),
        ("xl/_rels/workbook.xml.rels".into(), format!("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"{office}/worksheet\" Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"rId2\" Type=\"{office}/worksheet\" Target=\"worksheets/sheet2.xml\"/>{}</Relationships>", if kind == "module" { String::new() } else { format!("<Relationship Id=\"rId3\" Type=\"{office}/{kind}\" Target=\"{kind}s/sheet3.xml\"/>") })),
        ("xl/worksheets/sheet1.xml".into(), format!("<worksheet xmlns=\"{main}\"><sheetData><row r=\"1\"><c r=\"A1\"><f>{formula}</f></c></row></sheetData></worksheet>")),
        ("xl/worksheets/sheet2.xml".into(), format!("<worksheet xmlns=\"{main}\"><sheetData><row r=\"1\"><c r=\"A1\"><f>Rate</f></c></row></sheetData></worksheet>")),
    ];
    if kind != "module" {
        parts.push((
            format!("xl/{kind}s/sheet3.xml"),
            format!("<{kind} xmlns=\"{main}\"/>"),
        ));
    }
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, body) in parts {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(body.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

fn rewrite_workbook(bytes: Vec<u8>, rewrite: impl FnOnce(String) -> String) -> Vec<u8> {
    use std::io::Read;
    let mut input = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut xml = String::new();
    input
        .by_name("xl/workbook.xml")
        .unwrap()
        .read_to_string(&mut xml)
        .unwrap();
    let xml = rewrite(xml);
    let mut output = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for i in 0..input.len() {
        let entry = input.by_index(i).unwrap();
        if entry.name() == "xl/workbook.xml" {
            output
                .start_file(entry.name(), zip::write::SimpleFileOptions::default())
                .unwrap();
            output.write_all(xml.as_bytes()).unwrap();
        } else {
            output.raw_copy_file(entry).unwrap();
        }
    }
    output.finish().unwrap().into_inner()
}

#[test]
fn module_first_paired_tag_and_relationship_namespace_alias() {
    let bytes = rewrite_workbook(fixture("module", "1+1"), |xml| {
        let entry = "<sheet name=\"Chart1\" sheetId=\"3\" r:id=\"\"/>";
        xml.replace(entry, "")
            .replace(
                "<sheets>",
                &format!("<sheets>{}", entry.replace("/>", "></sheet>")),
            )
            .replace("xmlns:r=", "xmlns:rel=")
            .replace("r:id=", "rel:id=")
            .replace(
                "name=\"Omitted\" localSheetId=\"1\"",
                "name=\"Omitted\" localSheetId=\"0\"",
            )
    });
    let mut book = Workbook::from_reader(
        CalamineAdapter::open_bytes(bytes).unwrap(),
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    book.evaluate_all().unwrap();
    assert_eq!(book.sheet_names(), ["Sheet1", "After"]);
    assert_eq!(
        book.get_value("After", 1, 1),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(book.sheet_import_diagnostics().len(), 1);
    assert_eq!(book.name_import_diagnostics()[0].local_sheet_id, Some(0));
}

#[test]
fn inert_sheets_are_absent_and_local_name_indices_keep_original_order() {
    for kind in ["chartsheet", "dialogsheet", "module"] {
        let bytes = fixture(kind, "1+1");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inert.xlsx");
        std::fs::write(&path, &bytes).unwrap();
        let mut workbook = Workbook::from_reader(
            CalamineAdapter::open_path(&path).unwrap(),
            LoadStrategy::EagerAll,
            WorkbookConfig::interactive(),
        )
        .unwrap();
        workbook.evaluate_all().unwrap();
        assert_eq!(workbook.sheet_names(), ["Sheet1", "After"]);
        assert_eq!(
            workbook.get_value("After", 1, 1),
            Some(LiteralValue::Number(7.0))
        );
        let diagnostic = &workbook.sheet_import_diagnostics()[0];
        assert_eq!((&*diagnostic.name, &*diagnostic.kind), ("Chart1", kind));
        assert_eq!(workbook.sheet_import_diagnostics().len(), 1);
        let diagnostic = workbook
            .name_import_diagnostics()
            .iter()
            .find(|d| d.name == "Omitted")
            .unwrap();
        assert_eq!(diagnostic.local_sheet_id, Some(1));
        assert_eq!(diagnostic.scope_sheet.as_deref(), Some("Chart1"));
        assert!(diagnostic.message.contains("non-worksheet"));
        // No reserved inert-sheet identity remains in the mutable engine.
        workbook.add_sheet("Chart1").unwrap();
        assert!(workbook.sheet_names().contains(&"Chart1".to_owned()));
    }
}

#[test]
fn inert_references_return_ref_errors_on_load_and_after_edits() {
    for kind in ["chartsheet", "dialogsheet", "module"] {
        for (xml, formula) in [
            ("Chart1!A1", "=Chart1!A1"),
            ("SUM(Chart1!A1:B3)", "=SUM(Chart1!A1:B3)"),
            (
                "INDIRECT(&quot;Chart1!A1&quot;)",
                "=INDIRECT(\"Chart1!A1\")",
            ),
        ] {
            let adapter = CalamineAdapter::open_bytes(fixture(kind, xml)).unwrap();
            let mut book = Workbook::from_reader(
                adapter,
                LoadStrategy::EagerAll,
                WorkbookConfig::interactive(),
            )
            .unwrap();
            book.evaluate_all().unwrap();
            assert!(
                matches!(book.get_value("Sheet1", 1, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Ref)
            );
            book.set_formula("Sheet1", 2, 1, formula).unwrap();
            book.evaluate_all().unwrap();
            assert!(
                matches!(book.get_value("Sheet1", 2, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Ref)
            );
            assert_eq!(book.sheet_names(), ["Sheet1", "After"]);
        }
    }
}
