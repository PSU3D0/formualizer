use super::super::*;
use crate::cache_recalculate::native_names::NativeNames;
use crate::traits::SpreadsheetReader;
use std::io::Write;

fn assert_name_parity(bytes: &[u8], workbook: &[u8], sheets: &[package::Sheet]) {
    let mut old = CalamineAdapter::open_bytes(bytes.to_vec()).unwrap();
    let mut native = NativeNames::new(
        workbook,
        sheets,
        None,
        native_names::NameImportPolicy::STRICT_REFERENCED,
    );
    let inert = sheets.iter().any(|sheet| sheet.inert);
    assert_eq!(
        format!("{:?}", old.validate_calculation_names(&[], inert)),
        format!("{:?}", native.validate_calculation_names(&[], inert))
    );
    assert_eq!(
        old.unimported_document_names(),
        native.unimported_document_names()
    );
    assert_eq!(
        old.has_new_unimported_names(),
        native.has_new_unimported_names()
    );
    assert_eq!(
        format!("{:?}", old.calculation_name_asts()),
        format!("{:?}", native.calculation_name_asts())
    );
    let mut old_engine = Engine::new(WBResolver::default(), EvalConfig::default());
    let old_import = old.stream_into_engine(&mut old_engine);
    let mut native_engine = Engine::new(WBResolver::default(), EvalConfig::default());
    native_engine
        .adopt_file_sheets(sheets.iter().map(|sheet| sheet.name.as_str()))
        .unwrap();
    let native_import = native.register(&mut native_engine);
    assert_eq!(
        format!("{:?}", old_import.map(|_| ())),
        format!("{:?}", native_import)
    );
    assert_eq!(
        old_engine.sheet_id("Missing").is_some(),
        native_engine.sheet_id("Missing").is_some()
    );
}

fn fixture(workbook: &[u8]) -> Vec<u8> {
    fixture_with_sheet(workbook, br#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1"><v>7</v></c><c r="B1"><f>Num+1</f><v>43</v></c></row></sheetData></worksheet>"#)
}
pub(super) fn fixture_with_sheet(workbook: &[u8], worksheet: &[u8]) -> Vec<u8> {
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, data) in [
        ("[Content_Types].xml", br#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#.as_slice()),
        ("_rels/.rels", br#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="r1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#.as_slice()),
        ("xl/_rels/workbook.xml.rels", br#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="r1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#.as_slice()),
        ("xl/workbook.xml", workbook),
        ("xl/worksheets/sheet1.xml", worksheet),
    ] {
        zip.start_file(name, zip::write::SimpleFileOptions::default()).unwrap();
        zip.write_all(data).unwrap();
    }
    zip.finish().unwrap().into_inner()
}
#[test]
fn native_name_cycles_scopes_unicode_and_legacy_range_match_adapter() {
    let workbook = br#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Probe" sheetId="1" r:id="r1"/></sheets><definedNames><definedName name="Num">42</definedName><definedName name="Scoped" localSheetId="0">Probe!$A$1</definedName><definedName name="Forward">Num+1</definedName><definedName name="CycleA">CycleB</definedName><definedName name="CycleB">CycleA</definedName><definedName name="_xlnm.Print_Area">Missing!$A$1</definedName><definedName name="Str&#97;sse">2</definedName><definedName name="N&#38;N">3</definedName></definedNames></workbook>"#;
    let sheets = vec![package::Sheet {
        name: "Probe".into(),
        part: "xl/worksheets/sheet1.xml".into(),
        inert: false,
        kind: "worksheet",
        sheet_id: 1,
        tables: BTreeMap::new(),
    }];
    assert_name_parity(&fixture(workbook), workbook, &sheets);
}

#[test]
fn empty_names_preserve_scoped_scan_and_unscoped_fallback() {
    for scope in ["", " localSheetId=\"0\""] {
        for empty in ["/>", "></definedName>"] {
            for referenced in [false, true] {
                for mixed in [
                    "",
                    "<definedName name=\"Num\">42</definedName>",
                    "<definedName name=\"Y\"/>",
                ] {
                    let workbook = format!(
                        "<workbook xmlns='{}' xmlns:r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'><sheets><sheet name='Probe' sheetId='1' r:id='r1'/></sheets><definedNames><definedName name=\"X\"{scope}{empty}{mixed}</definedNames></workbook>",
                        xml::MAIN
                    );
                    let formula = if referenced { "X+1" } else { "A1+1" };
                    let worksheet = format!(
                        "<worksheet xmlns='{}'><sheetData><row r='1'><c r='A1'><v>7</v></c><c r='B1'><f>{formula}</f><v>0</v></c></row></sheetData></worksheet>",
                        xml::MAIN
                    );
                    let bytes = fixture_with_sheet(workbook.as_bytes(), worksheet.as_bytes());
                    let options = XlsxRecalculateOptions::default();
                    let admitted = admit_source(&bytes, &options).unwrap();
                    assert_name_parity(&bytes, workbook.as_bytes(), &admitted.sheets);
                    let old = legacy_ingest_source(
                        &bytes,
                        admitted,
                        &options,
                        &mut PhaseClock::from_env(),
                    )
                    .err();
                    let native = recalculate_xlsx_bytes(&bytes, options).err();
                    assert_eq!(
                        format!("{native:?}"),
                        format!("{old:?}"),
                        "{scope} {empty} {referenced} {mixed}"
                    );
                    if referenced && mixed.is_empty() {
                        assert!(
                            matches!(native, Some(IoError::Unsupported { feature, context }) if feature == "unsupported or cyclic calculation name" && context == "X")
                        );
                        let directory = tempfile::tempdir().unwrap();
                        let input = directory.path().join("input.xlsx");
                        let output = directory.path().join("output.xlsx");
                        std::fs::write(&input, &bytes).unwrap();
                        assert!(
                            recalculate_xlsx_file(
                                &input,
                                Some(&output),
                                XlsxRecalculateOptions::default()
                            )
                            .is_err()
                        );
                        assert!(!output.exists());
                        assert_eq!(std::fs::read(input).unwrap(), bytes);
                    }
                }
            }
        }
    }
}

#[test]
#[ignore = "needs FORMUALIZER_SCAN_DIFF_FILES"]
fn listed_native_name_metadata_matches_strict_adapter() {
    let files =
        std::fs::read_to_string(std::env::var("FORMUALIZER_SCAN_DIFF_FILES").unwrap()).unwrap();
    let mut packages = 0;
    for path in files.lines().filter(|line| !line.trim().is_empty()) {
        let bytes = std::fs::read(path).unwrap();
        let options = XlsxRecalculateOptions::default();
        let mut admission = admit_source(&bytes, &options).unwrap();
        let workbook = package::read_part(
            &mut admission.archive,
            "xl/workbook.xml",
            options.limits.max_worksheet_bytes,
        )
        .unwrap();
        let mut old = CalamineAdapter::open_bytes(bytes.clone()).unwrap();
        let mut native = NativeNames::new(
            &workbook,
            &admission.sheets,
            None,
            native_names::NameImportPolicy::STRICT_REFERENCED,
        );
        if let Some(external) = admission.external {
            let values = std::sync::Arc::new(external.values);
            old.skip_external_names(external.skipped_names.clone());
            old.admit_external_references(values.clone());
            native.admit_external(values, external.skipped_names);
        }
        let inert = admission.sheets.iter().any(|sheet| sheet.inert);
        assert_eq!(
            format!("{:?}", old.validate_calculation_names(&[], inert)),
            format!("{:?}", native.validate_calculation_names(&[], inert)),
            "{path}"
        );
        assert_eq!(
            old.unimported_document_names(),
            native.unimported_document_names(),
            "{path}"
        );
        assert_eq!(
            old.has_new_unimported_names(),
            native.has_new_unimported_names(),
            "{path}"
        );
        assert_eq!(
            format!("{:?}", old.calculation_name_asts()),
            format!("{:?}", native.calculation_name_asts()),
            "{path}"
        );
        packages += 1;
    }
    eprintln!(
        "native calculation-name metadata: {packages} packages, zero strict-name/AST/omission divergences"
    );
}
