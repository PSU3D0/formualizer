//! The umya reader panics on malformed parts (it unwraps quick-xml errors).
//! A load reports that as an error instead of unwinding into the caller.

use crate::common::build_workbook;
use formualizer_workbook::{SpreadsheetReader, UmyaAdapter};
use std::io::{Read, Write};

/// The fixture with `[Content_Types].xml` cut short (an unclosed tag).
fn truncated_content_types() -> Vec<u8> {
    let path = build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sheet.get_cell_mut((1, 1)).set_value_number(1.0);
    });
    let mut source = zip::ZipArchive::new(std::fs::File::open(path).unwrap()).unwrap();
    let mut out = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for i in 0..source.len() {
        let mut file = source.by_index(i).unwrap();
        let name = file.name().to_string();
        let mut body = Vec::new();
        file.read_to_end(&mut body).unwrap();
        if name == "[Content_Types].xml" {
            body.truncate(200);
        }
        out.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        out.write_all(&body).unwrap();
    }
    out.finish().unwrap().into_inner()
}

#[test]
fn a_reader_panic_is_a_load_error() {
    let error = match UmyaAdapter::open_bytes(truncated_content_types()) {
        Ok(_) => panic!("a truncated [Content_Types].xml must not load"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("the workbook reader panicked"), "{error}");
}
