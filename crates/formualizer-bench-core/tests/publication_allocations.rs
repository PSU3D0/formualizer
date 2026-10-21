//! End-to-end scalar-publication allocation budget. Uses the benchmark
//! crate's existing optional dhat allocator; no allocator implementation here.
#![cfg(feature = "publication_allocations")]

use std::io::{Cursor, Write};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn fixture() -> Vec<u8> {
    let main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    let office = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    let rels = "http://schemas.openxmlformats.org/package/2006/relationships";
    let mut rows = String::new();
    for row in 1..=400 {
        rows.push_str(&format!("<row r=\"{row}\"><c r=\"A{row}\"><v>1</v></c><c r=\"B{row}\"><f>A{row}+1</f><v>99</v></c></row>"));
    }
    let parts = [
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_owned()),
        ("_rels/.rels", format!("<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{main}\" xmlns:r=\"{office}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>")),
        ("xl/_rels/workbook.xml.rels", format!("<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>")),
        ("xl/worksheets/sheet1.xml", format!("<worksheet xmlns=\"{main}\"><dimension ref=\"A1:B400\"/><sheetData>{rows}</sheetData></worksheet>")),
    ];
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, text) in parts {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(text.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

#[test]
fn scalar_publication_allocation_budget() {
    let bytes = fixture();
    // Exclude one-time registry/lexer initialization from the measured run.
    formualizer_workbook::recalculate_xlsx_bytes(&bytes, Default::default()).unwrap();
    let _profiler = dhat::Profiler::builder().testing().build();
    let before = dhat::HeapStats::get();
    let out = formualizer_workbook::recalculate_xlsx_bytes(&bytes, Default::default()).unwrap();
    let after = dhat::HeapStats::get();
    let blocks = after.total_blocks.saturating_sub(before.total_blocks);
    eprintln!("scalar_publication_allocations={blocks} formulas=400");
    assert_eq!(out.summary.evaluated, 400);
    assert!(
        blocks <= 33_300,
        "scalar publication allocation budget: {blocks}"
    );
}
