#![cfg(feature = "calamine")]

use formualizer_workbook::{
    CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};
use std::io::{Cursor, Write};

// Deliberately no cached <v> values: source formulas are the only occupancy.
fn fixture(blocker: &str) -> Vec<u8> {
    let main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    let rels = "http://schemas.openxmlformats.org/package/2006/relationships";
    let office = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    let parts = [
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".into()),
        ("_rels/.rels", format!("<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{main}\" xmlns:r=\"{office}\"><sheets><sheet name=\"S\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>")),
        ("xl/_rels/workbook.xml.rels", format!("<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>")),
        ("xl/worksheets/sheet1.xml", format!("<worksheet xmlns=\"{main}\"><sheetData><row r=\"1\"><c r=\"A1\"><f>SEQUENCE(2)</f></c><c r=\"D1\"><f>7</f></c></row><row r=\"2\"><c r=\"A2\"><f>{blocker}</f></c></row></sheetData></worksheet>")),
    ];
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, body) in parts {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(body.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

fn load(blocker: &str) -> Workbook {
    Workbook::from_reader(
        CalamineAdapter::open_bytes(fixture(blocker)).unwrap(),
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap()
}

fn assert_spill(value: LiteralValue) {
    assert!(
        matches!(value, LiteralValue::Error(e) if e.kind == formualizer_common::ExcelErrorKind::Spill)
    );
}

#[test]
fn uncached_xlsx_pending_spill_blocks_without_preparing_source_and_retries() {
    for blocker in ["99", "NOSHEET!A1"] {
        for replacement in [None, Some("101")] {
            let mut wb = load(blocker);
            let source = wb.get_formula("S", 2, 1).unwrap();
            for _ in 0..3 {
                assert_spill(wb.evaluate_cell("S", 1, 1).unwrap());
                assert_eq!(wb.get_formula("S", 2, 1).as_ref(), Some(&source));
                assert!(!matches!(
                    wb.get_value("S", 2, 1),
                    Some(LiteralValue::Number(2.0))
                ));
                assert_eq!(wb.engine().staged_formula_count(), 2);
            }
            assert_eq!(
                wb.evaluate_cell("S", 1, 4).unwrap(),
                LiteralValue::Number(7.0)
            );
            if blocker == "NOSHEET!A1" {
                assert!(wb.evaluate_all().is_err());
                assert_eq!(wb.get_formula("S", 2, 1).as_ref(), Some(&source));
            }
            if let Some(formula) = replacement {
                wb.set_formula("S", 2, 1, formula).unwrap();
                assert_spill(wb.evaluate_cell("S", 1, 1).unwrap());
                assert_eq!(
                    wb.evaluate_cell("S", 2, 1).unwrap(),
                    LiteralValue::Number(101.0)
                );
            }
            wb.set_value("S", 2, 1, LiteralValue::Empty).unwrap();
            assert_eq!(
                wb.evaluate_cell("S", 1, 1).unwrap(),
                LiteralValue::Number(1.0)
            );
            // Existing user-overlay precedence keeps an explicit Empty edit
            // visible over spill output (also reproduced on published 0.9.2).
            // Engine-only tests assert the projected member is actually 2.
            assert_eq!(wb.get_value("S", 2, 1), None);
            wb.evaluate_all().unwrap();
            assert!(wb.get_formula("S", 2, 1).is_none());
            assert_eq!(wb.get_value("S", 2, 1), None);
        }
    }
}

#[test]
fn uncached_xlsx_full_evaluation_keeps_ordinary_formula_blocker() {
    let mut wb = load("99");
    // The legacy full-evaluation path can return the graph spill error rather
    // than publish it as an anchor value. Preserve that existing classification.
    if let Err(error) = wb.evaluate_all() {
        assert!(
            matches!(error, formualizer_workbook::IoError::Engine(e) if e.kind == formualizer_common::ExcelErrorKind::Spill)
        );
    } else {
        assert_spill(wb.get_value("S", 1, 1).unwrap());
    }
    assert_eq!(
        wb.evaluate_cell("S", 2, 1).unwrap(),
        LiteralValue::Number(99.0)
    );
    assert!(wb.get_formula("S", 2, 1).is_some());
}
