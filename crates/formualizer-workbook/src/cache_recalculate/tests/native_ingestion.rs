use super::super::*;

fn current_cse() -> Vec<u8> {
    super::native_names::fixture_with_sheet(
        br#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Probe" sheetId="1" r:id="r1"/></sheets></workbook>"#,
        br#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1"><f t="array" ref="A1:B1">SEQUENCE(1,2)</f><v>1</v></c><c r="B1"><v>2</v></c></row></sheetData></worksheet>"#,
    )
}
#[test]
fn formula_reader_errors_keep_source_order_against_later_literal_or_row_errors() {
    let workbook = br#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Probe" sheetId="1" r:id="r1"/></sheets></workbook>"#;
    for body in [
        "<row r='1'><c r='A1'><f t='shared' si='&#48;' ref='A1:A1'>1</f><v>1</v></c><c r='B1' t='&#110;'><v>2</v></c></row>",
        "<row r='1'><c r='A1' t='&#110;'><f>1</f><v>1</v></c><c r='B1' t='&#98;'><v>1</v></c></row>",
        "<row r='1'><c r='A1'><f t='shared' si='&#48;' ref='A1:A1'>1</f><v>1</v></c></row><row r='+2'/>",
    ] {
        let xml = format!(
            "<worksheet xmlns='{}'><sheetData>{body}</sheetData></worksheet>",
            xml::MAIN
        );
        let bytes = super::native_names::fixture_with_sheet(workbook, xml.as_bytes());
        let options = XlsxRecalculateOptions::default();
        let admitted = admit_source(&bytes, &options).unwrap();
        let old = legacy_ingest_source(&bytes, admitted, &options, &mut PhaseClock::from_env())
            .err()
            .unwrap();
        let native = recalculate_xlsx_bytes(&bytes, options).err().unwrap();
        assert_eq!(format!("{native:?}"), format!("{old:?}"), "{body}");
    }
}

#[test]
fn output_budget_is_enforced_at_publication_not_a_nonexistent_ingestion_zip() {
    let bytes = current_cse();
    let unchanged = recalculate_xlsx_bytes(&bytes, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(unchanged.cache_cells_changed, 0);
    assert_eq!(unchanged.bytes, bytes);
    let mut options = XlsxRecalculateOptions::default();
    options.limits.max_output_bytes = 0;
    // Positive old-path control: precisely the intermediate-view artifact that
    // this packet deliberately retires, never a blanket budget relaxation.
    let admission = admit_source(&bytes, &options).unwrap();
    let old = legacy_ingest_source(&bytes, admission, &options, &mut PhaseClock::from_env())
        .err()
        .unwrap();
    assert!(
        matches!(old, IoError::Backend { backend, message } if backend == "zip" && message == "i/o error: XLSX output byte limit")
    );
    let error = recalculate_xlsx_bytes(&bytes, options.clone())
        .err()
        .unwrap();
    assert!(
        matches!(error, IoError::Unsupported { feature, context } if feature == "output byte limit" && context == "XLSX package")
    );
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("current-cse.xlsx");
    let output = directory.path().join("result.xlsx");
    std::fs::write(&input, &bytes).unwrap();
    assert!(
        matches!(recalculate_xlsx_file(&input, Some(&output), options.clone()), Err(IoError::Unsupported { feature, context }) if feature == "output byte limit" && context == "XLSX package")
    );
    assert!(!output.exists());
    assert_eq!(std::fs::read(&input).unwrap(), bytes);
    assert!(recalculate_xlsx_file(&input, None, options).is_err());
    assert_eq!(std::fs::read(&input).unwrap(), bytes);
}
