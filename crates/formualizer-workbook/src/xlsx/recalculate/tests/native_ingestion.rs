use super::super::*;

fn current_cse() -> Vec<u8> {
    super::names::fixture_with_sheet(
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
        let bytes = super::names::fixture_with_sheet(workbook, xml.as_bytes());
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
fn noncanonical_display_markup_preserves_published_cache_bytes() {
    use std::io::{Read, Write};
    for (body, expected) in [
        ("<t>a<t>b</t>c</t>", "ab"),
        ("<rPh><rPh/><t>a</t></rPh><t>b</t>", "ab"),
        ("<t>a<p:t>b</p:t>c</t>", "abc"),
        ("<p:t>a<p:t>b</p:t>c</p:t>", "ab"),
        ("<t>a<t/>c</t>", "a"),
        ("<t>a<p:t/>c</t>", "ac"),
        ("<rPh><rPh><t>omit</t></rPh><t>a</t></rPh><t>b</t>", "ab"),
        ("<t>a<rPh>b</rPh>c</t><t>d</t>", "abcd"),
        ("<t>a<si>b</si>c</t>", "abc"),
    ] {
        for shared in [true, false] {
            let workbook = format!(
                "<workbook xmlns='{}' xmlns:r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'><sheets><sheet name='Probe' sheetId='1' r:id='r1'/></sheets></workbook>",
                xml::MAIN
            );
            let a = if shared {
                "<c r='A1' t='s'><v>0</v></c>".to_owned()
            } else {
                format!("<c r='A1' t='inlineStr'><is>{body}</is></c>")
            };
            let worksheet = format!(
                "<worksheet xmlns='{}' xmlns:p='{}'><sheetData><row r='1'>{a}<c r='B1'><f>A1</f><v>0</v></c></row></sheetData></worksheet>",
                xml::MAIN,
                xml::MAIN
            );
            let fixture =
                super::names::fixture_with_sheet(workbook.as_bytes(), worksheet.as_bytes());
            let bytes = if shared {
                let mut source = zip::ZipArchive::new(Cursor::new(fixture)).unwrap();
                let mut target = zip::ZipWriter::new(Cursor::new(Vec::new()));
                for i in 0..source.len() {
                    let mut file = source.by_index(i).unwrap();
                    let mut data = String::new();
                    file.read_to_string(&mut data).unwrap();
                    if file.name() == "[Content_Types].xml" {
                        data = data.replace("</Types>", "<Override PartName='/xl/sharedStrings.xml' ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml'/></Types>");
                    } else if file.name() == "xl/_rels/workbook.xml.rels" {
                        data = data.replace("</Relationships>", "<Relationship Id='sst' Type='http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings' Target='sharedStrings.xml'/></Relationships>");
                    }
                    target
                        .start_file(file.name(), zip::write::SimpleFileOptions::default())
                        .unwrap();
                    target.write_all(data.as_bytes()).unwrap();
                }
                target
                    .start_file(
                        "xl/sharedStrings.xml",
                        zip::write::SimpleFileOptions::default(),
                    )
                    .unwrap();
                write!(
                    target,
                    "<sst xmlns='{}' xmlns:p='{}'><si>{body}</si></sst>",
                    xml::MAIN,
                    xml::MAIN
                )
                .unwrap();
                target.finish().unwrap().into_inner()
            } else {
                fixture
            };
            let options = XlsxRecalculateOptions::default();
            let admitted = admit_source(&bytes, &options).unwrap();
            let count = admitted.formula_count;
            let mut clock = PhaseClock::from_env();
            let mut old = legacy_ingest_source(&bytes, admitted, &options, &mut clock).unwrap();
            evaluate(&mut old.engine, &options).unwrap();
            let old = publish(&bytes, old, count, &options, &mut clock).unwrap();
            let native = recalculate_xlsx_bytes(&bytes, options).unwrap();
            let mut archive = zip::ZipArchive::new(Cursor::new(&old.bytes)).unwrap();
            let mut sheet = String::new();
            archive
                .by_name("xl/worksheets/sheet1.xml")
                .unwrap()
                .read_to_string(&mut sheet)
                .unwrap();
            assert!(
                sheet.contains(&format!("<v>{expected}</v>")),
                "{body}: {sheet}"
            );
            assert_eq!(native.bytes, old.bytes, "shared={shared}, {body}");
        }
    }
}

#[test]
fn native_reader_counter_has_a_legacy_positive_control() {
    let bytes = current_cse();
    let options = XlsxRecalculateOptions::default();
    crate::backends::calamine::take_reader_io_calls();
    recalculate_xlsx_bytes(&bytes, options.clone()).unwrap();
    assert_eq!(crate::backends::calamine::take_reader_io_calls(), [0; 5]);
    let admitted = admit_source(&bytes, &options).unwrap();
    legacy_ingest_source(&bytes, admitted, &options, &mut PhaseClock::from_env()).unwrap();
    let positive = crate::backends::calamine::take_reader_io_calls();
    assert!(positive.iter().all(|count| *count > 0), "{positive:?}");
}

#[test]
#[ignore = "needs FORMUALIZER_SCAN_DIFF_FILES"]
fn listed_native_pipeline_counters() {
    super::scan_differential::exclusive(|| {
        let paths =
            std::fs::read_to_string(std::env::var("FORMUALIZER_SCAN_DIFF_FILES").unwrap()).unwrap();
        let options = XlsxRecalculateOptions::default();
        let (mut files, mut walks, mut indexed, mut shared, mut errors) = (0, 0, 0, 0, 0);
        for path in paths.lines().filter(|line| !line.trim().is_empty()) {
            let bytes = std::fs::read(path).unwrap();
            package::take_part_inflates();
            sheet::MACHINE_WALKS.with(|n| n.set(0));
            sheet::INDEXED_SCANS.with(|n| n.set(0));
            let admitted = admit_source(&bytes, &options).unwrap();
            let actual_walks = sheet::MACHINE_WALKS.with(|n| n.get());
            let actual_indexed = sheet::INDEXED_SCANS.with(|n| n.get());
            let inflates = package::take_part_inflates();
            let (mut expected_walks, mut expected_indexed) = (0, 0);
            for (source, plan) in admitted.sheets.iter().zip(&admitted.plans) {
                if !source.inert {
                    assert_eq!(inflates[&source.part], 1, "{path}: {}", source.part);
                }
                let mode = if source.tables.is_empty() {
                    sheet::ScanRetention::Plain
                } else {
                    sheet::ScanRetention::TablePlain
                };
                expected_walks += 1;
                expected_indexed += usize::from(mode == sheet::ScanRetention::Indexed);
                if matches!(
                    sheet::scan_uncounted(&plan.data, &options, mode, &mut 0, &mut 0).unwrap(),
                    sheet::Scanned::NeedsIndex
                ) {
                    expected_walks += 1;
                    expected_indexed += 1;
                }
            }
            assert_eq!(
                (actual_walks, actual_indexed),
                (expected_walks, expected_indexed),
                "{path}"
            );
            drop(admitted);
            crate::backends::calamine::take_reader_io_calls();
            formulas::take_utility_calls();
            let _ = recalculate_xlsx_bytes(&bytes, options.clone());
            assert_eq!(
                crate::backends::calamine::take_reader_io_calls(),
                [0; 5],
                "{path}"
            );
            let utilities = formulas::take_utility_calls();
            files += 1;
            walks += actual_walks;
            indexed += actual_indexed;
            shared += utilities.0;
            errors += utilities.1;
        }
        assert!(files > 0);
        eprintln!(
            "native counters: {files} packages; {walks} admission walks; {indexed} indexed (baseline equal); each worksheet inflated once; reader/open/metadata/worksheet/cell/formula surfaces zero; shared-text utilities={shared}; error-token utilities={errors}"
        );
    });
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
