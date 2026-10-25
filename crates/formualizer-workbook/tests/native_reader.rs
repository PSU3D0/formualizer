#![cfg(feature = "xlsx-recalc")]
use formualizer_workbook::{
    CalamineAdapter, CancelToken, IoError, LiteralValue, LoadStrategy, NativeXlsxAdapter,
    SpreadsheetReader, Workbook, WorkbookConfig, XlsxPathSource,
};
use std::io::{Cursor, Write};

fn fixture(kind: &str) -> Vec<u8> {
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let sheet = format!(
        "<worksheet xmlns='http://schemas.openxmlformats.org/spreadsheetml/2006/main'><sheetData><row r='1'><c r='A1' t='{kind}'><v>21</v></c><c r='B1'><f>A1*2</f><v>42</v></c></row></sheetData></worksheet>"
    );
    for (path, data) in [
        (
            "_rels/.rels",
            "<Relationships xmlns='http://schemas.openxmlformats.org/package/2006/relationships'><Relationship Id='r1' Type='http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument' Target='xl/workbook.xml'/></Relationships>",
        ),
        (
            "xl/workbook.xml",
            "<workbook xmlns='http://schemas.openxmlformats.org/spreadsheetml/2006/main' xmlns:r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'><sheets><sheet name='Probe' sheetId='1' r:id='r1'/></sheets></workbook>",
        ),
        (
            "xl/_rels/workbook.xml.rels",
            "<Relationships xmlns='http://schemas.openxmlformats.org/package/2006/relationships'><Relationship Id='r1' Type='http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet' Target='worksheets/sheet1.xml'/></Relationships>",
        ),
        ("xl/worksheets/sheet1.xml", &sheet),
    ] {
        zip.start_file(path, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(data.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}
fn assert_loaded(adapter: NativeXlsxAdapter) {
    let mut wb =
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .unwrap();
    assert_eq!(
        wb.evaluate_cell("Probe", 1, 2).unwrap(),
        LiteralValue::Number(42.0)
    );
}
#[test]
fn native_and_legacy_name_share_path_bytes_and_reader_ingress() {
    let bytes = fixture("n");
    assert_loaded(NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap());
    assert_loaded(CalamineAdapter::open_bytes(bytes.clone()).unwrap());
    let mut prefixed = b"prefix".to_vec();
    prefixed.extend(&bytes);
    let mut reader = Cursor::new(prefixed);
    reader.set_position(6);
    assert_loaded(NativeXlsxAdapter::open_reader(Box::new(reader)).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.xlsx");
    std::fs::write(&path, &bytes).unwrap();
    for policy in [XlsxPathSource::SharedFile, XlsxPathSource::DirectMmap] {
        let adapter = NativeXlsxAdapter::open_path_with_source(&path, policy).unwrap();
        assert_loaded(adapter);
    }
    let adapter = NativeXlsxAdapter::open_path(&path).unwrap();
    std::fs::rename(&path, dir.path().join("retained.xlsx")).unwrap();
    assert_loaded(adapter);
}
#[test]
fn cancellation_is_checked_before_admission_and_during_load() {
    let token = CancelToken::new();
    token.cancel();
    let error = NativeXlsxAdapter::open_bytes_cancellable(b"not a zip".to_vec(), token)
        .err()
        .unwrap();
    assert!(matches!(error, IoError::Engine(error)
        if error.kind == formualizer_common::ExcelErrorKind::Cancelled));
    let token = CancelToken::new();
    let adapter = NativeXlsxAdapter::open_bytes_cancellable(fixture("n"), token.clone()).unwrap();
    token.cancel();
    let error = Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
        .err()
        .unwrap();
    assert!(matches!(error, IoError::Engine(error)
        if error.kind == formualizer_common::ExcelErrorKind::Cancelled));
}
#[cfg(any(unix, windows))]
#[test]
fn direct_mmap_failure_is_an_ordinary_error_without_shared_file_fallback() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let error = NativeXlsxAdapter::open_path_with_source(file.path(), XlsxPathSource::DirectMmap)
        .err()
        .unwrap();
    assert!(matches!(error, IoError::Io(ref error)
        if error.kind() == std::io::ErrorKind::InvalidInput));
    assert!(
        error
            .to_string()
            .contains("cannot memory-map an empty XLSX file")
    );
}

#[test]
fn malformed_cell_types_use_native_error_payload_and_preserve_display() {
    let mut adapter = NativeXlsxAdapter::open_bytes(fixture("unknown")).unwrap();
    let error = adapter.read_sheet("Probe").unwrap_err();
    assert!(matches!(&error, IoError::XlsxRead(_)));
    assert_eq!(
        error.to_string(),
        "Calamine error: Xlsx error: Unknown cell 't' attribute: \"unknown\""
    );
}
