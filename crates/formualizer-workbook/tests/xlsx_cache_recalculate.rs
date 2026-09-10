#![cfg(feature = "xlsx-recalc")]
use formualizer_workbook::{XlsxRecalculateOptions, recalculate_xlsx_bytes};
use std::io::{Cursor, Write};
use zip::{ZipArchive, ZipWriter};

fn fixture_with_tag(_formula: &str, cache: &str, tag: &str) -> Vec<u8> {
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    let o = zip::write::SimpleFileOptions::default();
    for (name, body) in [
        (
            "[Content_Types].xml",
            "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"/>",
        ),
        (
            "_rels/.rels",
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>",
        ),
        (
            "xl/workbook.xml",
            "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>",
        ),
        (
            "xl/_rels/workbook.xml.rels",
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>",
        ),
        (
            "xl/worksheets/sheet1.xml",
            &format!(
                "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData><row r=\"1\"><c r=\"A1\">{tag}<v>{cache}</v></c></row></sheetData></worksheet>"
            ),
        ),
        ("custom/opaque.bin", "do not touch"),
    ] {
        z.start_file(name, o).unwrap();
        z.write_all(body.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}
fn fixture(formula: &str, cache: &str) -> Vec<u8> {
    fixture_with_tag(formula, cache, &format!("<f>{formula}</f>"))
}
fn member(bytes: &[u8], name: &str) -> String {
    let mut z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut s = String::new();
    std::io::Read::read_to_string(&mut z.by_name(name).unwrap(), &mut s).unwrap();
    s
}

#[test]
fn updates_stale_cache_and_raw_copies_opaque_member() {
    let input = fixture("1+1", "99");
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(out.summary.evaluated, 1);
    assert!(member(&out.bytes, "xl/worksheets/sheet1.xml").contains("<v>2</v>"));
    assert_eq!(member(&out.bytes, "custom/opaque.bin"), "do not touch");
}
#[test]
fn already_correct_cache_is_exact_noop() {
    let input = fixture("1+1", "2");
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(out.bytes, input);
}
#[test]
fn rejects_array_formula_and_returns_no_output() {
    let input = fixture_with_tag("A1", "1", "<f t=\"array\">A1</f>");
    assert!(recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).is_err());
}
#[test]
fn no_formula_is_exact_noop() {
    let input = fixture_with_tag("", "2", "");
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(out.bytes, input);
}
