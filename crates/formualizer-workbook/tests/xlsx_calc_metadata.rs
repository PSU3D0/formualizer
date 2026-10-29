#![cfg(feature = "xlsx-recalc")]
use formualizer_common::DateSystem;
use formualizer_eval::engine::{CycleConfig, EvalConfig};
use formualizer_workbook::{
    SourceCalculationPolicy, XlsxRecalculateOptions, recalculate_xlsx_bytes,
};
use std::io::{Cursor, Write};

const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";

fn resolve(xml: &str) -> Result<(), formualizer_workbook::IoError> {
    SourceCalculationPolicy::default()
        .resolve(
            &EvalConfig::default(),
            xml.as_bytes(),
            DateSystem::Excel1900,
        )
        .map(|_| ())
}

#[test]
fn standalone_source_requires_main_namespace_and_direct_unique_properties() {
    for xml in [
        "<workbook><calcPr/></workbook>".to_owned(),
        format!("<workbook xmlns=\"{MAIN}\"><calcPr/><calcPr/></workbook>"),
        format!("<workbook xmlns=\"{MAIN}\"><ext><calcPr/></ext></workbook>"),
        format!("<workbook xmlns=\"{MAIN}\" xmlns:f=\"urn:foreign\"><f:calcPr/></workbook>"),
        format!(
            "<workbook xmlns=\"{MAIN}\" xmlns:f=\"urn:foreign\"><calcPr f:iterate=\"1\"/></workbook>"
        ),
        format!("<!DOCTYPE workbook><workbook xmlns=\"{MAIN}\"/>"),
    ] {
        assert!(resolve(&xml).is_err(), "accepted {xml}");
    }
}

#[test]
fn prefixed_main_source_decodes_entities_and_keeps_origins() {
    let xml = format!(
        "<s:workbook xmlns:s=\"{MAIN}\"><s:calcPr iterate=\"true\" iterateCount=\"&#50;3\" iterateDelta=\"0.02\"/></s:workbook>"
    );
    let (config, report) = SourceCalculationPolicy::default()
        .resolve(
            &EvalConfig::default(),
            xml.as_bytes(),
            DateSystem::Excel1904,
        )
        .unwrap();
    assert_eq!(config.cycle, CycleConfig::iterate(23, 0.02));
    assert_eq!(config.date_system, DateSystem::Excel1904);
    assert!(report.warnings.is_empty());
}

#[test]
fn invalid_precision_is_forwarded_as_unverified_warning() {
    let xml = format!("<workbook xmlns=\"{MAIN}\"><calcPr fullPrecision=\"maybe\"/></workbook>");
    let (_, report) = SourceCalculationPolicy::default()
        .resolve(
            &EvalConfig::default(),
            xml.as_bytes(),
            DateSystem::Excel1900,
        )
        .unwrap();
    assert_eq!(report.warnings.len(), 1);
    assert_eq!(report.warnings[0].field, "fullPrecision");
    assert!(report.warnings[0].message.contains("unverified"));
}

fn package(properties: &str) -> Vec<u8> {
    let office = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    let rels = "http://schemas.openxmlformats.org/package/2006/relationships";
    let entries = [
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_owned()),
        ("_rels/.rels", format!("<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{office}\"><workbookPr date1904=\"1\"/><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets>{properties}</workbook>")),
        ("xl/_rels/workbook.xml.rels", format!("<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>")),
        ("xl/worksheets/sheet1.xml", format!("<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\"><c r=\"A1\"><f>1+1</f><v>99</v></c></row></sheetData></worksheet>")),
    ];
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, body) in entries {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(body.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

#[test]
fn namespace_declarations_named_like_properties_match_package_resolution() {
    for declarations in [
        r#"xmlns:iterate="urn:unused""#,
        r#"xmlns:fullPrecision="urn:unused""#,
        r#"xmlns:iterate="urn:unused" xmlns:fullPrecision="urn:also-unused""#,
    ] {
        let properties = format!(r#"<calcPr {declarations} iterate="0" fullPrecision="1"/>"#);
        let output = recalculate_xlsx_bytes(
            &package(&properties),
            XlsxRecalculateOptions {
                calculation: Some(SourceCalculationPolicy::default()),
                ..Default::default()
            },
        )
        .expect("package accepts namespace declarations");
        let (_, standalone) = SourceCalculationPolicy::default()
            .resolve(
                &EvalConfig::default(),
                format!("<workbook xmlns=\"{MAIN}\">{properties}</workbook>").as_bytes(),
                DateSystem::Excel1904,
            )
            .expect("standalone accepts the same namespace declarations");
        let actual = output.calculation.unwrap();
        assert_eq!(actual.cycle, standalone.cycle);
        assert_eq!(actual.iteration_origin, standalone.iteration_origin);
        assert_eq!(actual.date_system, standalone.date_system);
        assert_eq!(actual.warnings, standalone.warnings);
        assert!(standalone.warnings.is_empty());
    }
}

#[test]
fn malformed_unused_namespace_references_refuse_package_and_standalone() {
    for properties in [
        r#"<calcPr xmlns:helper="&bogus;" iterate="0"/>"#,
        r#"<calcPr xmlns:fullPrecision="&#0;" iterate="0"/>"#,
    ] {
        assert!(
            recalculate_xlsx_bytes(&package(properties), XlsxRecalculateOptions::default())
                .is_err(),
            "package accepted {properties}"
        );
        assert!(
            resolve(&format!(
                "<workbook xmlns=\"{MAIN}\">{properties}</workbook>"
            ))
            .is_err(),
            "standalone accepted {properties}"
        );
    }
}

#[test]
fn real_qualified_properties_and_duplicate_attributes_still_refuse_both_routes() {
    for properties in [
        r#"<calcPr xmlns:f="urn:foreign" f:iterate="1"/>"#,
        r#"<calcPr xmlns:f="urn:foreign" f:fullPrecision="1"/>"#,
        r#"<calcPr iterate="0" iterate="1"/>"#,
        r#"<calcPr fullPrecision="1" fullPrecision="0"/>"#,
        r#"<calcPr xmlns:iterate="urn:one" xmlns:iterate="urn:two"/>"#,
    ] {
        assert!(
            recalculate_xlsx_bytes(&package(properties), XlsxRecalculateOptions::default())
                .is_err(),
            "package accepted {properties}"
        );
        assert!(
            resolve(&format!(
                "<workbook xmlns=\"{MAIN}\">{properties}</workbook>"
            ))
            .is_err(),
            "standalone accepted {properties}"
        );
    }
}

#[test]
fn package_and_standalone_resolution_agree_on_invalid_fields() {
    let properties =
        "<calcPr iterate=\"bad\" iterateCount=\"0\" iterateDelta=\"NaN\" fullPrecision=\"maybe\"/>";
    let input = package(properties);
    let output = recalculate_xlsx_bytes(
        &input,
        XlsxRecalculateOptions {
            calculation: Some(SourceCalculationPolicy::default()),
            ..Default::default()
        },
    )
    .unwrap();
    let (_, standalone) = SourceCalculationPolicy::default()
        .resolve(
            &EvalConfig::default(),
            format!("<workbook xmlns=\"{MAIN}\">{properties}</workbook>").as_bytes(),
            DateSystem::Excel1904,
        )
        .unwrap();
    let actual = output.calculation.unwrap();
    assert_eq!(actual.warnings, standalone.warnings);
    assert_eq!(actual.warnings.len(), 4);
    assert_eq!(actual.date_system, DateSystem::Excel1904);
}

#[test]
fn package_rejects_ambiguous_calculation_metadata() {
    for properties in [
        "<calcPr/><calcPr/>",
        "<extLst><calcPr/></extLst>",
        "<f:calcPr xmlns:f=\"urn:foreign\"/>",
        "<calcPr xmlns:f=\"urn:foreign\" f:iterateCount=\"5\"/>",
    ] {
        assert!(
            recalculate_xlsx_bytes(&package(properties), XlsxRecalculateOptions::default())
                .is_err(),
            "accepted {properties}"
        );
    }
}
