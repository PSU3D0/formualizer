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
fn defined_constant_is_calculated_not_published_as_name_error() {
    let mut p = single("Rate*2", "<v>99</v>");
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace(
        "</workbook>",
        "<definedNames><definedName name=\"Rate\">0.07</definedName></definedNames></workbook>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(0.14));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    assert_eq!(out.summary.errors, 0);
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, Default::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}
fn with_names(mut p: BTreeMap<String, String>, names: &str) -> BTreeMap<String, String> {
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace(
        "</workbook>",
        &format!("<definedNames>{names}</definedNames></workbook>"),
    );
    p
}

#[test]
fn grounded_formula_names_and_transitive_dependencies() {
    let p = with_names(
        parts(
            "<row r=\"1\"><c r=\"A1\"><v>3</v></c></row><row r=\"2\"><c r=\"A2\"><f>Answer</f><v>99</v></c></row>",
        ),
        "<definedName name=\"Answer\">DoubleBase</definedName><definedName name=\"DoubleBase\">SUM(Sheet1!$A$1,Sheet1!$A$1)</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 1), Data::Float(6.0));
    assert_eq!(data(&out.bytes, 0), Data::Float(3.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, Default::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}

#[test]
fn current_workbook_index_zero_resolves_names_and_keeps_formula_text() {
    // Excel stores `[0]!Name` for a workbook-scoped name qualified with the
    // current workbook (external links are numbered from 1).
    let p = with_names(
        parts(
            "<row r=\"1\"><c r=\"A1\"><v>7</v></c><c r=\"B1\"><f>INDEX([0]!MPRR,2,1)</f></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>8</v></c><c r=\"B2\"><f>SUM([0]!MPRR)+[0]Sheet1!A1</f></c></row>",
        ),
        "<definedName name=\"MPRR\">Sheet1!$A$1:$A$2</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0).to_string(), "7");
    let mut x = Xlsx::new(Cursor::new(&out.bytes)).unwrap();
    let range = x.worksheet_range("Sheet1").unwrap();
    assert_eq!(range.get_value((0, 1)), Some(&Data::Float(8.0)));
    assert_eq!(range.get_value((1, 1)), Some(&Data::Float(22.0)));
    let sheet = member(&out.bytes, SHEET);
    assert!(sheet.contains("<f>INDEX([0]!MPRR,2,1)</f>"), "{sheet}");
    assert!(
        sheet.contains("<f>SUM([0]!MPRR)+[0]Sheet1!A1</f>"),
        "{sheet}"
    );
}

#[test]
fn original_cross_sheet_formula_name_and_reference_control() {
    for (definition, formula) in [("Base!$A$1*2", "DoubleBase"), ("Base!$A$1", "DoubleBase*2")] {
        let mut p = with_names(
            single(formula, "<v>99</v>"),
            &format!("<definedName name=\"DoubleBase\">{definition}</definedName>"),
        );
        let workbook = p.get_mut("xl/workbook.xml").unwrap();
        *workbook = workbook.replace(
            "</sheets>",
            "<sheet name=\"Base\" sheetId=\"2\" r:id=\"rId2\"/></sheets>",
        );
        let relationships = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
        *relationships = relationships.replace("</Relationships>", &format!("<Relationship Id=\"rId2\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet2.xml\"/></Relationships>"));
        let types = p.get_mut("[Content_Types].xml").unwrap();
        *types = types.replace("</Types>", "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>");
        p.insert("xl/worksheets/sheet2.xml".into(), format!("<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\"><v>3</v></c></row></sheetData></worksheet>"));
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), Data::Float(6.0));
        assert_eq!(
            member(&out.bytes, "xl/worksheets/sheet2.xml"),
            p["xl/worksheets/sheet2.xml"]
        );
        assert_eq!(out.summary.errors, 0);
    }
}

#[test]
fn calculation_name_literal_types_and_legitimate_error() {
    for (definition, expected) in [
        ("TRUE", Data::Bool(true)),
        ("FALSE", Data::Bool(false)),
        ("&quot;a &amp; b&quot;", Data::String("a & b".into())),
        ("#N/A", Data::Error(calamine::CellErrorType::NA)),
        ("#DIV/0!", Data::Error(calamine::CellErrorType::Div0)),
        ("#VALUE!", Data::Error(calamine::CellErrorType::Value)),
        ("#REF!", Data::Error(calamine::CellErrorType::Ref)),
        ("Sheet1!#REF!", Data::Error(calamine::CellErrorType::Ref)),
        ("#NUM!", Data::Error(calamine::CellErrorType::Num)),
        ("#NULL!", Data::Error(calamine::CellErrorType::Null)),
        ("#NAME?", Data::Error(calamine::CellErrorType::Name)),
        ("-0.07", Data::Float(-0.07)),
    ] {
        let p = with_names(
            single("ValueName", "<v>99</v>"),
            &format!("<definedName name=\"ValueName\">{definition}</definedName>"),
        );
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), expected, "{definition}");
        assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    }
}

#[test]
fn calculation_name_local_shadowing_and_local_formula_base() {
    let p = with_names(
        parts(
            "<row r=\"1\"><c r=\"A1\"><v>3</v></c></row><row r=\"2\"><c r=\"A2\"><f>ResultName</f><v>99</v></c></row>",
        ),
        "<definedName name=\"Rate\">10</definedName><definedName name=\"Rate\" localSheetId=\"0\">2</definedName><definedName name=\"ResultName\" localSheetId=\"0\">$A$1*Rate</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 1), Data::Float(6.0));
}

#[test]
fn unsupported_and_cyclic_calculation_names_refuse_publication() {
    for names in [
        "<definedName name=\"ResultName\">Sheet1!$A$1,Sheet1!$B$1</definedName>",
        "<definedName name=\"ResultName\">Sheet1!A1</definedName>",
        "<definedName name=\"ResultName\">$A$1*2</definedName>",
        "<definedName name=\"ResultName\">Sheet1!A1*2</definedName>",
        "<definedName name=\"ResultName\">INDIRECT(&quot;A1&quot;)</definedName>",
        "<definedName name=\"ResultName\">ROW()</definedName>",
        "<definedName name=\"ResultName\">OFFSET(#REF!,0,0,2,1)</definedName>",
        "<definedName name=\"ResultName\">OFFSET(Sheet1!$A$1,0,0,2,1)</definedName>",
        "<definedName name=\"ResultName\">[0]!X</definedName>",
        "<definedName name=\"ResultName\">{1,2}</definedName>",
        "<definedName name=\"ResultName\">[other.xlsx]Sheet1!$A$1</definedName>",
        "<definedName name=\"ResultName\">ResultName</definedName>",
        "<definedName name=\"ResultName\">OtherName</definedName><definedName name=\"OtherName\">ResultName</definedName>",
    ] {
        let p = with_names(single("ResultName", "<v>99</v>"), names);
        let input = pack(&p);
        assert!(
            matches!(
                recalculate_xlsx_bytes(&input, Default::default()),
                Err(formualizer_workbook::IoError::Unsupported { .. })
            ),
            "{names}"
        );
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.xlsx");
        let destination = dir.path().join("output.xlsx");
        std::fs::write(&source, &input).unwrap();
        std::fs::write(&destination, b"keep original destination").unwrap();
        assert!(
            formualizer_workbook::recalculate_xlsx_file(
                &source,
                Some(&destination),
                Default::default()
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&source).unwrap(), input);
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"keep original destination"
        );
    }
}

fn inert_fixture(kind: &str, first: bool, formula: &str) -> BTreeMap<String, String> {
    let mut p = parts(&format!(
        "<row r=\"1\"><c r=\"A1\"><f>{formula}</f><v>99</v></c></row>"
    ));
    let inert = if kind == "empty" {
        "<sheet name=\"Inert\" sheetId=\"3\" state=\"veryHidden\" r:id=\"\"/>".to_owned()
    } else {
        "<sheet name=\"Inert\" sheetId=\"3\" r:id=\"rId3\"/>".to_owned()
    };
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    if first {
        *workbook = workbook.replace("<sheets>", &format!("<sheets>{inert}"));
    } else {
        *workbook = workbook.replace("</sheets>", &format!("{inert}</sheets>"));
    }
    *workbook = workbook.replace(
        "</sheets>",
        "<sheet name=\"After\" sheetId=\"2\" r:id=\"rId2\"/></sheets>",
    );
    if kind == "empty" && !first {
        *workbook = workbook
            .replace(&inert, "")
            .replace("</sheets>", &format!("{inert}</sheets>"));
    }
    let scope = if kind == "empty" && !first { 1 } else { 2 };
    *workbook = workbook.replace("</workbook>", &format!("<definedNames><definedName name=\"Rate\" localSheetId=\"{scope}\">7</definedName></definedNames></workbook>"));
    let rels = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
    *rels = rels.replace("</Relationships>", &format!("<Relationship Id=\"rId2\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet2.xml\"/></Relationships>"));
    let types = p.get_mut("[Content_Types].xml").unwrap();
    *types = types.replace("</Types>", "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>");
    p.insert("xl/worksheets/sheet2.xml".into(), format!("<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\"><f>Rate</f><v>99</v></c></row></sheetData></worksheet>"));
    if kind != "empty" {
        let rels = p.get_mut("xl/_rels/workbook.xml.rels").unwrap();
        *rels = rels.replace("</Relationships>", &format!("<Relationship Id=\"rId3\" Type=\"{OFFICE}/{kind}\" Target=\"{kind}s/sheet3.xml\"/></Relationships>"));
        let types = p.get_mut("[Content_Types].xml").unwrap();
        *types = types.replace("</Types>", &format!("<Override PartName=\"/xl/{kind}s/sheet3.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.{kind}+xml\"/></Types>"));
        p.insert(
            format!("xl/{kind}s/sheet3.xml"),
            format!("<{kind} xmlns=\"{MAIN}\"/>"),
        );
        p.insert(format!("xl/{kind}s/_rels/sheet3.xml.rels"), format!("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"drawing\" Type=\"{OFFICE}/drawing\" Target=\"../drawings/drawing1.xml\"/></Relationships>"));
        p.insert("xl/drawings/drawing1.xml".into(), "<drawing/>".into());
        p.insert("xl/charts/chart1.xml".into(), "<chart/>".into());
    }
    p
}

#[test]
fn inert_sheets_keep_scope_order_and_original_parts() {
    let mut failures = Vec::new();
    for (kind, first) in [
        ("chartsheet", false),
        ("dialogsheet", false),
        ("empty", false),
        ("empty", true),
    ] {
        let p = inert_fixture(kind, first, "1+1");
        let out = match recalculate_xlsx_bytes(&pack(&p), Default::default()) {
            Ok(out) => out,
            Err(error) => {
                failures.push(format!("{kind}, first={first}: {error}"));
                continue;
            }
        };
        let expected = if first {
            vec!["Inert", "Sheet1", "After"]
        } else {
            vec!["Sheet1", "Inert", "After"]
        };
        if kind != "empty" {
            let mut x = Xlsx::new(Cursor::new(&out.bytes)).unwrap();
            assert_eq!(x.sheet_names(), expected);
            assert_eq!(
                x.worksheet_range("After").unwrap().get_value((0, 0)),
                Some(&Data::Float(7.0))
            );
        } else {
            assert!(member(&out.bytes, "xl/worksheets/sheet2.xml").contains("<v>7</v>"));
        }
        let zip = ZipArchive::new(Cursor::new(&out.bytes)).unwrap();
        assert_eq!(zip.len(), p.len());
        assert!(!zip.file_names().any(|n| n.contains("__inert_sheet")));
        for (name, original) in &p {
            if !name.starts_with("xl/worksheets/") {
                assert_eq!(member(&out.bytes, name), *original);
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn inert_sheet_direct_and_three_dimensional_references_refuse() {
    for formula in ["Inert!A1", "SUM(Sheet1:After!A1)"] {
        let p = inert_fixture("chartsheet", false, formula);
        match recalculate_xlsx_bytes(&pack(&p), Default::default()) {
            Err(formualizer_workbook::IoError::Unsupported { feature, .. }) => {
                assert_eq!(feature, "reference to a non-worksheet sheet")
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}

#[test]
fn inert_sheet_indirect_references_are_guarded() {
    for formula in [
        "INDIRECT(&quot;Inert!A1&quot;)",
        "INDIRECT(&quot;Inert!R1C1&quot;,FALSE)",
        "INDIRECT(&quot;Sheet1:After!A1&quot;)",
        "_XLFN._XLWS.INDIRECT(&quot;Inert!A1&quot;)",
    ] {
        let p = inert_fixture("chartsheet", false, formula);
        assert!(
            matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "reference to a non-worksheet sheet")
        );
    }
    let p = inert_fixture(
        "chartsheet",
        false,
        "INDIRECT(&quot;After!&quot;&amp;&quot;A1&quot;)",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "nonliteral INDIRECT in a workbook with non-worksheet sheets")
    );
    let p = inert_fixture("chartsheet", false, "INDIRECT(&quot;After!A1&quot;)");
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(7.0));
}

#[test]
fn inert_sheet_positions_are_visible_to_metadata_functions() {
    for (formula, expected) in [
        ("SHEET(&quot;After&quot;)", 3.0),
        ("SHEETS()", 3.0),
        ("SHEET()", 1.0),
    ] {
        let p = inert_fixture("chartsheet", false, formula);
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        assert_eq!(data(&out.bytes, 0), Data::Float(expected));
    }
}

#[test]
fn unused_local_name_does_not_shadow_other_sheet_scope() {
    let mut p = inert_fixture("chartsheet", false, "1+1");
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace(
        "</definedNames>",
        "<definedName name=\"Rate\" localSheetId=\"0\">{1,2}</definedName></definedNames>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert!(member(&out.bytes, "xl/worksheets/sheet2.xml").contains("<v>7</v>"));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
}

#[test]
fn other_nonworksheet_sheet_types_remain_refused() {
    for kind in ["macrosheet", "intlmacrosheet"] {
        let p = inert_fixture(kind, false, "1+1");
        assert!(
            matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "non-worksheet sheet")
        );
    }
}

#[test]
fn unused_unevaluable_names_are_preserved() {
    let mut failures = Vec::new();
    for names in [
        "<definedName name=\"Unused\">{1,2}</definedName>",
        "<definedName name=\"Unused\">OFFSET(Sheet1!$A$1,0,0,2,1)</definedName>",
        "<definedName name=\"Unused\">OFFSET(#REF!,0,0,2,1)</definedName>",
        "<definedName name=\"Unused\">[0]!X</definedName>",
        "<definedName name=\"Unused\">Other</definedName><definedName name=\"Other\">Unused</definedName>",
        "<definedName name=\"Unused\">Other</definedName><definedName name=\"Other\">OFFSET(Unused,0,0)</definedName>",
        "<definedName name=\"Unused\">'#10'!$A$4:$AJ$23</definedName>",
    ] {
        let p = with_names(single("1+1", "<v>99</v>"), names);
        match recalculate_xlsx_bytes(&pack(&p), Default::default()) {
            Ok(out) => {
                assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
                assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
            }
            Err(error) => failures.push(format!("{names}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn unevaluable_name_readers_follow_engine_unicode_case_folding() {
    let p = with_names(
        single("äBAD", "<v>99</v>"),
        "<definedName name=\"Äbad\">{1,2}</definedName>",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
    );
}

#[test]
fn unregistrable_ordinary_names_are_kept_raw_only_when_unused() {
    let names = "<definedName name=\"a10intld\\\">7</definedName>";
    let p = with_names(single("1+1", "<v>99</v>"), names);
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
}

#[test]
fn unregistrable_ordinary_name_readers_refuse() {
    let names = "<definedName name=\"a10intld\\\">7</definedName>";
    for formula in ["a10intld\\", "INDIRECT(&quot;a10intld\\&quot;)"] {
        let p = with_names(single(formula, "<v>99</v>"), names);
        assert!(
            matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
        );
    }
    let p = with_names(
        single("1+1", "<v>99</v>"),
        "<definedName name=\"a10intld\\\">1+1</definedName><definedName name=\"Reader\">a10intld\\+1</definedName>",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
    );
}

#[test]
fn newly_admitted_workbooks_do_not_expose_masked_legacy_name_errors() {
    let legacy = "<definedName name=\"_xlnm.Auto_Open_hook\">[1]!Register.DClick</definedName>";
    let p = with_names(
        single("1+1", "<v>99</v>"),
        &format!("{legacy}<definedName name=\"Unused\">{{1,2}}</definedName>"),
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
    );
    let p = with_names(single("1+1", "<v>99</v>"), legacy);
    assert!(matches!(
        recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Calamine(_))
    ));
    let mut p = inert_fixture("chartsheet", false, "1+1");
    let workbook = p.get_mut("xl/workbook.xml").unwrap();
    *workbook = workbook.replace("</definedNames>", &format!("{legacy}</definedNames>"));
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "unsupported or cyclic calculation name")
    );
}

#[test]
fn document_name_cycles_and_raw_readers_keep_legacy_refusals() {
    for names in [
        "<definedName name=\"_xlnm.Print_Area\">_xlnm.Print_Area</definedName>",
        "<definedName name=\"_xlnm.Print_Area\">Sheet1!$A$1,Sheet1!$B$1</definedName><definedName name=\"Unused\">OFFSET(_xlnm.Print_Area,0,0)</definedName>",
    ] {
        let p = with_names(single("1+1", "<v>99</v>"), names);
        assert!(matches!(
            recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { .. })
        ));
    }
}

#[test]
fn malformed_document_range_keeps_legacy_phantom_sheet_import() {
    let p = with_names(
        single("SHEETS()", "<v>99</v>"),
        "<definedName name=\"_xlnm.Print_Area\">Missing!$A$1</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
}

#[test]
fn generic_calamine_load_keeps_unused_cycle_refusal() {
    use formualizer_eval::engine::ingest::EngineLoadStream;
    use formualizer_workbook::SpreadsheetReader;
    let p = with_names(
        single("1+1", "<v>99</v>"),
        "<definedName name=\"Unused\">Other</definedName><definedName name=\"Other\">Unused</definedName>",
    );
    let mut adapter = formualizer_workbook::CalamineAdapter::open_bytes(pack(&p)).unwrap();
    let mut engine = formualizer_eval::engine::Engine::new(
        formualizer_workbook::workbook::WBResolver::default(),
        Default::default(),
    );
    let error = adapter.stream_into_engine(&mut engine).unwrap_err();
    assert!(
        error.to_string().contains("cyclic calculation name"),
        "{error}"
    );
}

#[test]
fn literal_indirect_reads_unevaluable_names() {
    let p = with_names(
        single("INDIRECT(&quot;BadName&quot;)", "<v>99</v>"),
        "<definedName name=\"FirstBad\">{1,2}</definedName><definedName name=\"BadName\">OFFSET(Sheet1!$A$2,0,0)</definedName>",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, context }) if feature == "unsupported or cyclic calculation name" && context == "BadName")
    );
}

#[test]
fn nonliteral_indirect_is_guarded_only_for_newly_omitted_names() {
    let formula = "INDIRECT(&quot;A&quot;&amp;&quot;2&quot;)";
    let p = with_names(
        single(formula, "<v>99</v>"),
        "<definedName name=\"Unused\">{1,2}</definedName>",
    );
    assert!(
        matches!(recalculate_xlsx_bytes(&pack(&p), Default::default()),
        Err(formualizer_workbook::IoError::Unsupported { feature, .. }) if feature == "nonliteral INDIRECT in a workbook with unevaluable names")
    );
    let p = with_names(
        single(formula, "<v>99</v>"),
        "<definedName name=\"_xlnm.Print_Area\">Sheet1!$A$1,Sheet1!$B$1</definedName>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
}

#[test]
fn evaluated_name_cannot_read_an_unevaluable_name() {
    let p = with_names(
        single("1+1", "<v>99</v>"),
        "<definedName name=\"Unused\">OFFSET(#REF!,0,0,2,1)</definedName><definedName name=\"Reader\">Unused+1</definedName>",
    );
    assert!(recalculate_xlsx_bytes(&pack(&p), Default::default()).is_err());
}

#[test]
fn print_filter_metadata_is_preserved_but_referenced_unsupported_name_refuses() {
    let names = "<definedName name=\"_xlnm.Print_Area\" localSheetId=\"0\">Sheet1!$A$1,Sheet1!$B$1</definedName><definedName name=\"_xlnm._FilterDatabase\" localSheetId=\"0\" hidden=\"1\">Sheet1!$A$1:$B$2</definedName>";
    let p = with_names(single("1+1", "<v>99</v>"), names);
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
    assert_eq!(member(&out.bytes, "xl/workbook.xml"), p["xl/workbook.xml"]);
    // Names are case-insensitive: any spelling of the reference refuses.
    for formula in ["SUM(_xlnm.Print_Area)", "SUM(_XLNM.PRINT_AREA)"] {
        let p = with_names(single(formula, "<v>99</v>"), names);
        assert!(matches!(
            recalculate_xlsx_bytes(&pack(&p), Default::default()),
            Err(formualizer_workbook::IoError::Unsupported { .. })
        ));
    }
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
    // Multi-cell spills are published since FORM211; one crossing a merge
    // is still refused as a whole.
    let mut p = single("SEQUENCE(2)", "<v>99</v>");
    let sheet = p.get_mut(SHEET).unwrap();
    *sheet = sheet.replace(
        "</sheetData>",
        "</sheetData><mergeCells count=\"1\"><mergeCell ref=\"A2:B2\"/></mergeCells>",
    );
    reject(&p);
    let out = recalculate_xlsx_bytes(&fixture("SEQUENCE(2)", "99"), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 1), Data::Float(2.0));
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
fn defined_name_range_endpoints_are_refused_not_written() {
    // Excel computes the bounding range (14 / 6 / 9 here); the evaluator does
    // not resolve name endpoints yet, so the package must be refused rather
    // than rewritten with a cached error.
    for formula in ["SUM(A1:Total)", "SUM(Start:A3)", "SUM(Start:Total)"] {
        let mut p = parts(&format!(
            "<row r=\"1\"><c r=\"A1\"><v>2</v></c><c r=\"B1\"><f>{formula}</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><v>3</v></c></row><row r=\"3\"><c r=\"A3\"><v>4</v></c></row>"
        ));
        let wb = p.get_mut("xl/workbook.xml").unwrap();
        *wb = wb.replace(
            "</workbook>",
            "<definedNames><definedName name=\"Start\">Sheet1!$A$2</definedName><definedName name=\"Total\">Sheet1!$A$3</definedName></definedNames></workbook>",
        );
        let Err(error) = recalculate_xlsx_bytes(&pack(&p), Default::default()) else {
            panic!("{formula}: recalculation wrote the package");
        };
        assert!(
            matches!(&error, formualizer_workbook::IoError::Unsupported { feature, context }
                if feature.contains("no approved XLSX cache encoding") && context == "#N/IMPL!"),
            "{formula}: unexpected error: {error:?}"
        );
    }
}
#[test]
fn nonportable_literal_errors_are_refused_and_empty_table_parts_are_inert() {
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
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(data(&out.bytes, 0), Data::Float(2.0));
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
fn missing_data_descriptors_and_inconsistent_headers_are_rejected() {
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
#[test]
fn unparseable_stored_formula_is_a_refusal_naming_the_cell() {
    use formualizer_eval::engine::FormulaParsePolicy;
    use formualizer_workbook::IoError;
    // A second, valid formula keeps both eager and deferred ingestion busy.
    let mut p = parts(
        "<row r=\"1\"><c r=\"A1\"><v>1</v></c><c r=\"B1\"><f>A1+1</f><v>9</v></c><c r=\"C1\"><f>SUM((A1</f><v>9</v></c></row>",
    );
    let input = pack(&p);
    for defer in [false, true] {
        let mut o = XlsxRecalculateOptions::default();
        o.eval_config.defer_graph_building = defer;
        match recalculate_xlsx_bytes(&input, o) {
            Err(IoError::Unsupported { feature, context }) => {
                assert_eq!(feature, "unparseable formula");
                assert!(context.starts_with("Sheet1!C1: "), "{context}");
                assert!(context.contains("parenthesis"), "{context}");
            }
            other => panic!("defer={defer}: expected a refusal, got {other:?}"),
        }
    }
    // A caller's explicit non-strict policy keeps its meaning: the coerced
    // #ERROR! result has no XLSX cache encoding.
    let mut o = XlsxRecalculateOptions::default();
    o.eval_config.formula_parse_policy = FormulaParsePolicy::CoerceToError;
    match recalculate_xlsx_bytes(&input, o) {
        Err(IoError::Unsupported { feature, context }) => {
            assert!(
                feature.contains("no approved XLSX cache encoding"),
                "{feature}"
            );
            assert_eq!(context, "#ERROR!");
        }
        other => panic!("expected the coerced-error refusal, got {other:?}"),
    }
    // Valid formulas are unaffected.
    let sheet = p.get_mut(SHEET).unwrap();
    *sheet = sheet.replace("SUM((A1", "SUM((A1))");
    assert!(recalculate_xlsx_bytes(&pack(&p), Default::default()).is_ok());
}
