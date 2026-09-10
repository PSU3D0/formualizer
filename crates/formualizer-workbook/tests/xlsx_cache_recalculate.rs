#![cfg(feature = "xlsx-recalc")]
use calamine::{Data, Reader, Xlsx};
use formualizer_workbook::{XlsxRecalculateOptions, recalculate_xlsx_bytes};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
};
use zip::{ZipArchive, ZipWriter};

const SHEET: &str = "xl/worksheets/sheet1.xml";
const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
fn parts(rows: &str) -> BTreeMap<String, String> {
    [
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Default Extension=\"bin\" ContentType=\"application/octet-stream\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_owned()),
        ("_rels/.rels",format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml",format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>")),
        ("xl/_rels/workbook.xml.rels",format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>")),
        (SHEET,format!("<worksheet xmlns=\"{MAIN}\"><sheetData>{rows}</sheetData></worksheet>")),
        ("custom/opaque.bin","do not touch".to_owned()),
    ].into_iter().map(|(k,v)|(k.to_owned(),v)).collect()
}
fn pack(parts: &BTreeMap<String, String>) -> Vec<u8> {
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    z.set_comment("archive-comment");
    let options = zip::write::SimpleFileOptions::default()
        .unix_permissions(0o640)
        .last_modified_time(zip::DateTime::from_date_and_time(2020, 1, 2, 3, 4, 6).unwrap());
    for (name, body) in parts {
        z.start_file(name, options).unwrap();
        z.write_all(body.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}
fn single(formula: &str, cache: &str) -> BTreeMap<String, String> {
    parts(&format!(
        "<row r=\"1\"><c r=\"A1\"><f>{formula}</f>{cache}</c></row>"
    ))
}
fn fixture(formula: &str, cache: &str) -> Vec<u8> {
    pack(&single(formula, &format!("<v>{cache}</v>")))
}
fn member(bytes: &[u8], name: &str) -> String {
    let mut z = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut s = String::new();
    z.by_name(name).unwrap().read_to_string(&mut s).unwrap();
    s
}
fn data(bytes: &[u8], row: u32) -> Data {
    let mut x = Xlsx::new(Cursor::new(bytes)).unwrap();
    x.worksheet_range("Sheet1")
        .unwrap()
        .get_value((row, 0))
        .cloned()
        .unwrap_or(Data::Empty)
}
fn reject(parts: &BTreeMap<String, String>) {
    assert!(recalculate_xlsx_bytes(&pack(parts), XlsxRecalculateOptions::default()).is_err());
}
#[test]
fn stale_cache_and_untouched_members_and_metadata() {
    let input = fixture("1+1", "99");
    let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(
        (
            out.formula_cells,
            out.cache_cells_changed,
            out.worksheet_parts_changed
        ),
        (1, 1, 1)
    );
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    let mut before = ZipArchive::new(Cursor::new(&input)).unwrap();
    let mut after = ZipArchive::new(Cursor::new(&out.bytes)).unwrap();
    assert_eq!(before.comment(), after.comment());
    assert_eq!(before.len(), after.len());
    for i in 0..before.len() {
        let a = before.by_index(i).unwrap();
        let b = after.by_index(i).unwrap();
        assert_eq!(a.name(), b.name());
        assert_eq!(a.last_modified(), b.last_modified());
        assert_eq!(a.unix_mode(), b.unix_mode());
        assert_eq!(a.compression(), b.compression());
        if a.name() != SHEET {
            assert_eq!(
                &input[a.data_start() as usize..(a.data_start() + a.compressed_size()) as usize],
                &out.bytes
                    [b.data_start() as usize..(b.data_start() + b.compressed_size()) as usize]
            );
        }
    }
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, XlsxRecalculateOptions::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}
#[test]
fn exact_noops() {
    for input in [
        fixture("1+1", "2"),
        pack(&parts("<row r=\"1\"><c r=\"A1\"><v>2</v></c></row>")),
        pack(&parts("")),
    ] {
        let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
        assert_eq!(out.bytes, input);
        assert_eq!(out.cache_cells_changed, 0);
    }
}
#[test]
fn missing_cache_and_typed_cache_repairs() {
    for (formula, expected, fragment) in [
        ("1+1", Data::Float(2.0), "<v>2</v>"),
        ("TRUE()", Data::Bool(true), "t=\"b\""),
        (
            "&quot;hi &amp; &lt; 💡&quot;",
            Data::String("hi & < 💡".into()),
            "<v>hi &amp; &lt; 💡</v>",
        ),
        ("&quot;&quot;", Data::String(String::new()), "<v></v>"),
        ("1/0", Data::Error(calamine::CellErrorType::Div0), "t=\"e\""),
    ] {
        let input = pack(&single(formula, ""));
        let out = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), expected, "{formula}");
        assert!(member(&out.bytes, SHEET).contains(fragment));
        assert_eq!(
            recalculate_xlsx_bytes(&out.bytes, XlsxRecalculateOptions::default())
                .unwrap()
                .bytes,
            out.bytes
        );
    }
}
#[test]
fn source_epoch_is_authoritative() {
    for (date1904, expected) in [("0", 1463.0), ("1", 1.0), ("true", 1.0)] {
        let mut p = single("DATE(1904,1,2)", "<v>99</v>");
        let wb = p.get_mut("xl/workbook.xml").unwrap();
        *wb = wb.replace(
            "<sheets>",
            &format!("<workbookPr date1904=\"{date1904}\"/><sheets>"),
        );
        let out = recalculate_xlsx_bytes(&pack(&p), XlsxRecalculateOptions::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), Data::Float(expected));
        assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    }
}
#[test]
fn shared_formula_text_is_untouched() {
    let p = parts(
        "<row r=\"1\"><c r=\"A1\"><f t=\"shared\" si=\"0\" ref=\"A1:A3\">ROW()</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><f t=\"shared\" si=\"0\"/><v>99</v></c></row><row r=\"3\"><c r=\"A3\"><f t=\"shared\" si=\"0\"/><v>99</v></c></row>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(out.formula_cells, 3);
    for r in 0..3 {
        assert_eq!(data(&out.bytes, r), Data::Float(f64::from(r + 1)));
    }
    let xml = member(&out.bytes, SHEET);
    assert!(xml.contains("<f t=\"shared\" si=\"0\" ref=\"A1:A3\">ROW()</f>"));
    assert_eq!(xml.matches("<f t=\"shared\" si=\"0\"/>").count(), 2);
}
#[test]
fn prefixes_quotes_and_unknown_children_survive() {
    let mut p = single("1+1", "<v>99</v>");
    p.insert(SHEET.into(),format!("<x:worksheet xmlns:x='{MAIN}' xmlns:u='urn:opaque'><x:sheetData><x:row r='1'><x:c r='A1' t='str' u:note='a&amp;&#13;'><x:f>1+1</x:f><x:v>old</x:v></x:c></x:row></x:sheetData><u:payload token='keep'/></x:worksheet>"));
    let out = recalculate_xlsx_bytes(&pack(&p), XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(
        member(&out.bytes, SHEET),
        p[SHEET]
            .replace("t='str'", "")
            .replace(">old</x:v>", ">2</x:v>")
    );
}
#[test]
fn malformed_and_unsupported_worksheet_matrix() {
    let original = single("1+1", "<v>99</v>");
    for xml in [
        original[SHEET].replace("<f>", "<f t=\"array\">"),
        original[SHEET].replace("<f>", "<f t=\"dataTable\">"),
        original[SHEET].replace("<f>", "<f t=\"shared\" si=\"8\">"),
        original[SHEET].replace("r=\"A1\"", "r=\"A0\""),
        original[SHEET].replace("r=\"A1\"", "r=\"XFE1\""),
        original[SHEET].replace("r=\"A1\"", "r=\"A2\""),
        original[SHEET].replace("r=\"A1\"", "r=\"$A$1\""),
        original[SHEET].replace("r=\"A1\"", "r=\"A1\" cm=\"1\""),
        original[SHEET].replace("<f>", "<f xmlns=\"urn:foreign\">"),
        original[SHEET].replace("<v>99</v>", "<v>99</v><v>1</v>"),
        original[SHEET].replace("<v>99</v>", "<v>&unknown;</v>"),
        original[SHEET].replace("<v>99</v>", "<v>&#0;</v>"),
        original[SHEET].replace("</row>", "</c>"),
        format!("<!DOCTYPE worksheet [<!ENTITY e 'x'>]>{}", original[SHEET]),
        original[SHEET].replace(
            "<sheetData>",
            "<dimension ref=\"A1:XFD1048576\"/><sheetData>",
        ),
        original[SHEET].replace(
            "<sheetData>",
            "<dimension ref=\"A1\"/><dimension ref=\"A1\"/><sheetData>",
        ),
        original[SHEET].replace("<sheetData>", "<dimension ref=\"B1\"/><sheetData>"),
    ] {
        let mut p = original.clone();
        p.insert(SHEET.into(), xml);
        reject(&p);
    }
}
#[test]
fn package_mapping_and_signature_rejections() {
    let original = single("1+1", "<v>99</v>");
    for (name, old, new) in [
        (
            "_rels/.rels",
            "Target=\"xl/workbook.xml\"",
            "Target=\"elsewhere.xml\"",
        ),
        (
            "xl/_rels/workbook.xml.rels",
            "Target=\"worksheets/sheet1.xml\"",
            "Target=\"../../../escape.xml\"",
        ),
        (
            "xl/_rels/workbook.xml.rels",
            "/worksheet\"",
            "/chartsheet\"",
        ),
        ("xl/workbook.xml", MAIN, "urn:foreign"),
        (
            "[Content_Types].xml",
            "spreadsheetml.worksheet+xml",
            "spreadsheetml.styles+xml",
        ),
    ] {
        let mut p = original.clone();
        let s = p.get_mut(name).unwrap();
        *s = s.replace(old, new);
        reject(&p);
    }
    let mut p = original;
    p.insert("_xmlsignatures/sig1.xml".into(), "<Signature/>".into());
    reject(&p);
}
#[test]
fn limits_and_precancellation() {
    let input = fixture("1+1", "99");
    for which in 0..7 {
        let mut o = XlsxRecalculateOptions::default();
        match which {
            0 => o.limits.max_input_bytes = 1,
            1 => o.limits.max_entries = 1,
            2 => o.limits.max_expanded_bytes = 1,
            3 => o.limits.max_worksheet_bytes = 1,
            4 => o.limits.max_formula_cells = 0,
            5 => o.limits.max_xml_depth = 1,
            _ => o.limits.max_cells = 0,
        };
        assert!(recalculate_xlsx_bytes(&input, o).is_err(), "limit {which}");
    }
    let cancel = formualizer_eval::engine::CancelToken::new();
    cancel.cancel();
    let o = XlsxRecalculateOptions {
        cancel: Some(cancel),
        ..Default::default()
    };
    assert!(recalculate_xlsx_bytes(&input, o).is_err());
}
#[test]
fn unsupported_spill_does_not_return_a_partial_package() {
    assert!(
        recalculate_xlsx_bytes(
            &fixture("SEQUENCE(2)", "99"),
            XlsxRecalculateOptions::default()
        )
        .is_err()
    );
}
#[test]
fn error_locations_are_bounded() {
    let o = XlsxRecalculateOptions {
        error_location_limit: 0,
        ..Default::default()
    };
    let out = recalculate_xlsx_bytes(&fixture("1/0", "99"), o).unwrap();
    assert_eq!(out.summary.errors, 1);
    let error = &out.summary.error_summary["#DIV/0!"];
    assert_eq!(error.locations.len(), 0);
    assert_eq!(error.locations_truncated, 1);
}
#[test]
fn typed_text_controls_fail_instead_of_silent_corruption() {
    for formula in ["CHAR(1)", "&quot;_x0041_&quot;"] {
        assert!(
            recalculate_xlsx_bytes(&fixture(formula, "99"), XlsxRecalculateOptions::default())
                .is_err()
        );
    }
}
#[test]
fn modern_scalar_errors_are_cached_and_can_be_recalculated_again() {
    for (formula, token) in [
        ("SEQUENCE(2)", "#SPILL!"),
        ("FILTER(A2:A2,FALSE)", "#CALC!"),
    ] {
        let p = parts(&format!(
            "<row r=\"1\"><c r=\"A1\"><f>{formula}</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><v>7</v></c></row>"
        ));
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert_eq!(out.summary.errors, 1);
        assert!(member(&out.bytes, SHEET).contains(&format!("<v>{token}</v>")));
        let again = recalculate_xlsx_bytes(&out.bytes, Default::default()).unwrap();
        assert_eq!(again.bytes, out.bytes);
        assert_eq!(again.summary.errors, 1);
    }
}
#[test]
fn defined_names_are_evaluated_without_metadata_rewrite() {
    let mut p = parts(
        "<row r=\"1\"><c r=\"A1\"><f>Answer+1</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><v>7</v></c></row>",
    );
    let wb = p.get_mut("xl/workbook.xml").unwrap();
    *wb=wb.replace("</workbook>","<definedNames><definedName name=\"Answer\">Sheet1!$A$2</definedName></definedNames></workbook>");
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(8.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    let wb = p.get_mut("xl/workbook.xml").unwrap();
    *wb = wb.replace(
        "</definedNames>",
        "<definedName name=\"answer\">Sheet1!$A$1</definedName></definedNames>",
    );
    reject(&p);
}
#[test]
fn nonportable_literal_errors_and_tables_are_explicitly_rejected() {
    let p = parts(
        "<row r=\"1\"><c r=\"A1\" t=\"e\"><v>#SPILL!</v></c><c r=\"B1\"><f>IFERROR(A1,0)</f><v>99</v></c></row>",
    );
    let error = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap_err();
    assert!(
        matches!(error,formualizer_workbook::IoError::Unsupported{feature,..} if feature.contains("literal error"))
    );
    let mut p = single("1+1", "<v>99</v>");
    let s = p.get_mut(SHEET).unwrap();
    *s = s.replace("</worksheet>", "<tableParts count=\"0\"/></worksheet>");
    let error = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap_err();
    assert!(
        matches!(error,formualizer_workbook::IoError::Unsupported{feature,..} if feature.contains("table metadata"))
    );
}
#[test]
fn engine_specific_errors_are_unsupported_results_not_invented_excel_tokens() {
    let error = recalculate_xlsx_bytes(&fixture("AGGREGATE(12,0,1)", "99"), Default::default())
        .unwrap_err();
    assert!(
        matches!(&error,formualizer_workbook::IoError::Unsupported{feature,context}
            if feature=="formula result is not current"
                || (feature.contains("no approved XLSX cache encoding") && context=="#N/IMPL!")),
        "unexpected error: {error:?}"
    );
}
#[test]
fn arbitrary_stale_error_cache_is_not_evaluator_authority() {
    let mut p = single("1+1", "<v>#FUTURE_ERROR!</v>");
    let s = p.get_mut(SHEET).unwrap();
    *s = s.replace("r=\"A1\"", "r=\"A1\" t=\"e\"");
    let output = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&output.bytes, 0), Data::Float(2.0));
}
#[test]
fn cached_text_empty_element_is_an_exact_noop() {
    let mut p = single("&quot;&quot;", "<v/>");
    let xml = p.get_mut(SHEET).unwrap();
    *xml = xml.replace("r=\"A1\"", "r=\"A1\" t=\"str\"");
    let input = pack(&p);
    let out = recalculate_xlsx_bytes(&input, Default::default()).unwrap();
    assert_eq!(out.bytes, input);
    assert_eq!(data(&out.bytes, 0), Data::String(String::new()));
}
#[test]
fn one_cell_dynamic_result_does_not_require_geometry_writeback() {
    let out = recalculate_xlsx_bytes(&fixture("SEQUENCE(1)", "99"), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(1.0));
}
#[test]
fn scalar_ingestion_cannot_silently_drop_xml_text() {
    for payload in ["1&#50;", "1<!--split-->2", "<![CDATA[12]]>"] {
        let p = parts(&format!(
            "<row r=\"1\"><c r=\"A1\"><v>{payload}</v></c><c r=\"B1\"><f>A1+1</f><v>99</v></c></row>"
        ));
        reject(&p);
    }
    let input = pack(&single("12", "<v>1&#50;</v>"));
    let out = recalculate_xlsx_bytes(&input, Default::default()).unwrap();
    assert_eq!(out.bytes, input);
    let p = single("1<![CDATA[+1]]>", "<v>99</v>");
    reject(&p);
}
#[test]
fn serial_egress_preserves_phantom_day_and_fractional_dates() {
    for value in ["60", "60.125", "-0.125"] {
        let mut p = single(value, "<v>99</v>");
        let worksheet = p.get_mut(SHEET).unwrap();
        *worksheet = worksheet.replace("r=\"A1\"", "r=\"A1\" s=\"0\"");
        p.insert("xl/styles.xml".into(),format!("<styleSheet xmlns=\"{MAIN}\"><cellXfs count=\"1\"><xf numFmtId=\"14\"/></cellXfs></styleSheet>"));
        let rel = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
        *rel=rel.replace("</Relationships>",&format!("<Relationship Id=\"style\" Type=\"{OFFICE}/styles\" Target=\"styles.xml\"/></Relationships>"));
        let ct = p.get_mut("[Content_Types].xml").unwrap();
        *ct=ct.replace("</Types>","<Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/></Types>");
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert!(member(&out.bytes, SHEET).contains(&format!("<v>{value}</v>")));
        assert_eq!(member(&out.bytes, "xl/styles.xml"), p["xl/styles.xml"]);
    }
}
fn h16(b: &[u8], i: usize) -> usize {
    u16::from_le_bytes(b[i..i + 2].try_into().unwrap()) as usize
}
fn h32(b: &[u8], i: usize) -> usize {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as usize
}
fn directory(bytes: &[u8]) -> (Vec<usize>, usize) {
    let archive = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut at = archive.central_directory_start() as usize;
    let mut result = Vec::new();
    for _ in 0..archive.len() {
        result.push(at);
        at += 46 + h16(bytes, at + 28) + h16(bytes, at + 30) + h16(bytes, at + 32);
    }
    (result, at)
}
#[test]
fn zip_metadata_is_not_normalized_even_for_changed_members() {
    let mut input = fixture("1+1", "99");
    let (headers, _) = directory(&input);
    for &at in &headers {
        input[at + 4] = 20;
        input[at + 36] = 1;
        input[at + 38] |= 0x20;
    }
    let output = recalculate_xlsx_bytes(&input, Default::default())
        .unwrap()
        .bytes;
    let (after, _) = directory(&output);
    for (&a, &b) in headers.iter().zip(&after) {
        let length = 46 + h16(&input, a + 28) + h16(&input, a + 30) + h16(&input, a + 32);
        for i in 0..length {
            if !(16..28).contains(&i) && !(42..46).contains(&i) {
                assert_eq!(input[a + i], output[b + i], "central field {i}");
            }
        }
        let la = h32(&input, a + 42);
        let lb = h32(&output, b + 42);
        let local_length = 30 + h16(&input, la + 26) + h16(&input, la + 28);
        for i in 0..local_length {
            if !(14..26).contains(&i) {
                assert_eq!(input[la + i], output[lb + i], "local field {i}");
            }
        }
    }
}
#[test]
fn multiple_changed_members_relocate_growing_and_shrinking_payloads() {
    let old = (0..2048u32)
        .map(|n| format!("{:08x}", n.wrapping_mul(2_654_435_761)))
        .collect::<String>();
    let mut p = single("1+1", &format!("<v>{old}</v>"));
    let sheet = p.get_mut(SHEET).unwrap();
    *sheet = sheet.replace("r=\"A1\"", "r=\"A1\" t=\"str\"");
    let wb = p.get_mut("xl/workbook.xml").unwrap();
    *wb = wb.replace(
        "</sheets>",
        "<sheet name=\"Sheet2\" sheetId=\"2\" r:id=\"rId2\"/></sheets>",
    );
    let rel = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
    *rel=rel.replace("</Relationships>",&format!("<Relationship Id=\"rId2\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet2.xml\"/></Relationships>"));
    let ct = p.get_mut("[Content_Types].xml").unwrap();
    *ct=ct.replace("</Types>","<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>");
    p.insert("xl/worksheets/sheet2.xml".into(),format!("<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\" t=\"str\"><f>REPT(&quot;x&quot;,32767)</f><v>q</v></c></row></sheetData></worksheet>"));
    p.insert("zz/opaque.bin".into(), "opaque tail".into());
    let input = pack(&p);
    let out = recalculate_xlsx_bytes(&input, Default::default()).unwrap();
    assert_eq!(out.worksheet_parts_changed, 2);
    let mut before = ZipArchive::new(Cursor::new(&input)).unwrap();
    let mut after = ZipArchive::new(Cursor::new(&out.bytes)).unwrap();
    assert!(
        before.by_name(SHEET).unwrap().compressed_size()
            > after.by_name(SHEET).unwrap().compressed_size()
    );
    assert!(
        before
            .by_name("xl/worksheets/sheet2.xml")
            .unwrap()
            .compressed_size()
            < after
                .by_name("xl/worksheets/sheet2.xml")
                .unwrap()
                .compressed_size()
    );
    let a = before.by_name("zz/opaque.bin").unwrap();
    let b = after.by_name("zz/opaque.bin").unwrap();
    assert_eq!(
        &input[a.data_start() as usize..(a.data_start() + a.compressed_size()) as usize],
        &out.bytes[b.data_start() as usize..(b.data_start() + b.compressed_size()) as usize]
    );
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, Default::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}
#[test]
fn duplicate_zip_names_are_not_hidden_by_archive_index() {
    let mut input = fixture("1+1", "99");
    let (headers, footer) = directory(&input);
    let a = headers[0];
    let len = 46 + h16(&input, a + 28) + h16(&input, a + 30) + h16(&input, a + 32);
    let copy = input[a..a + len].to_vec();
    input.splice(footer..footer, copy);
    let footer = footer + len;
    for offset in [8, 10] {
        input[footer + offset..footer + offset + 2]
            .copy_from_slice(&((headers.len() + 1) as u16).to_le_bytes());
    }
    let size = h32(&input, footer + 12) + len;
    input[footer + 12..footer + 16].copy_from_slice(&(size as u32).to_le_bytes());
    assert!(recalculate_xlsx_bytes(&input, Default::default()).is_err());
}
#[test]
fn data_descriptors_and_inconsistent_headers_are_rejected() {
    let original = fixture("1+1", "99");
    let (headers, _) = directory(&original);
    let a = headers[0];
    let local = h32(&original, a + 42);
    let mut mismatch = original.clone();
    mismatch[local + 14] ^= 1;
    assert!(recalculate_xlsx_bytes(&mismatch, Default::default()).is_err());
    let mut descriptor = original;
    descriptor[a + 8] |= 8;
    descriptor[local + 6] |= 8;
    assert!(recalculate_xlsx_bytes(&descriptor, Default::default()).is_err());
}
#[test]
fn actual_expansion_and_output_limits_are_enforced() {
    let mut p = single("1+1", "<v>99</v>");
    p.insert("custom/opaque.bin".into(), "x".repeat(1 << 20));
    let input = pack(&p);
    let mut o = XlsxRecalculateOptions::default();
    o.limits.max_expanded_bytes = 1 << 16;
    assert!(recalculate_xlsx_bytes(&input, o).is_err());
    for cache in ["2", "99"] {
        let mut o = XlsxRecalculateOptions::default();
        o.limits.max_output_bytes = 1;
        assert!(recalculate_xlsx_bytes(&fixture("1+1", cache), o).is_err());
    }
    let mut p = single("1+1", "<v>99</v>");
    let xml = p.get_mut(SHEET).unwrap();
    *xml = xml.replace("<sheetData>", "<dimension ref=\"A1:XFD1\"/><sheetData>");
    reject(&p);
}
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn atomic_native_output_and_permissions() {
    use formualizer_workbook::recalculate_xlsx_file;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.xlsx");
    let output = dir.path().join("out.xlsx");
    std::fs::write(&input, b"bad input").unwrap();
    std::fs::write(&output, b"existing output").unwrap();
    assert!(recalculate_xlsx_file(&input, Some(&output), Default::default()).is_err());
    assert_eq!(std::fs::read(&output).unwrap(), b"existing output");
    let source = fixture("1+1", "99");
    std::fs::write(&input, &source).unwrap();
    let out = recalculate_xlsx_file(&input, Some(&output), Default::default()).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), out.bytes);
    assert_eq!(std::fs::read(&input).unwrap(), source);
    recalculate_xlsx_file(&input, None, Default::default()).unwrap();
    assert_eq!(data(&std::fs::read(&input).unwrap(), 0), Data::Float(2.0));
}
