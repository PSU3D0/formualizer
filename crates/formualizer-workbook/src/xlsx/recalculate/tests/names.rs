use super::super::*;
use std::io::Write;

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
                    let native = recalculate_xlsx_bytes(&bytes, options).err();
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
