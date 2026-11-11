#![cfg(feature = "xlsx-recalc")]
use formualizer_workbook::{
    LiteralValue, LoadStrategy, NativeXlsxAdapter, SpreadsheetReader, Workbook, WorkbookConfig,
};
use std::io::{Cursor, Write};

const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";

fn package(rows: &str, chain: bool) -> Vec<u8> {
    let metadata = format!(
        "<metadata xmlns=\"{MAIN}\" xmlns:xda=\"http://schemas.microsoft.com/office/spreadsheetml/2017/dynamicarray\"><metadataTypes count=\"1\"><metadataType name=\"XLDAPR\" minSupportedVersion=\"120000\" cellMeta=\"1\"/></metadataTypes><futureMetadata name=\"XLDAPR\" count=\"1\"><bk><extLst><ext uri=\"{{bdbb8cdc-fa1e-496e-a857-3c3f30c029c3}}\"><xda:dynamicArrayProperties fDynamic=\"1\" fCollapsed=\"0\"/></ext></extLst></bk></futureMetadata><cellMetadata count=\"1\"><bk><rc t=\"1\" v=\"0\"/></bk></cellMetadata></metadata>"
    );
    let chain_rel = if chain {
        format!("<Relationship Id=\"chain\" Type=\"{OFFICE}/calcChain\" Target=\"calcChain.xml\"/>")
    } else {
        String::new()
    };
    let mut parts = vec![
        ("[Content_Types].xml", "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/><Override PartName=\"/xl/metadata.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml\"/></Types>".to_owned()),
        ("_rels/.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"book\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Probe\" sheetId=\"1\" r:id=\"sheet\"/></sheets></workbook>")),
        ("xl/_rels/workbook.xml.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"sheet\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"meta\" Type=\"{OFFICE}/sheetMetadata\" Target=\"metadata.xml\"/>{chain_rel}</Relationships>")),
        ("xl/metadata.xml", metadata),
        ("xl/styles.xml", format!("<styleSheet xmlns=\"{MAIN}\"><fonts count=\"1\"><font/></fonts><fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills><borders count=\"1\"><border/></borders><cellStyleXfs count=\"1\"><xf/></cellStyleXfs><cellXfs count=\"1\"><xf xfId=\"0\"/></cellXfs></styleSheet>")),
        ("xl/worksheets/sheet1.xml", format!("<worksheet xmlns=\"{MAIN}\"><sheetData>{rows}</sheetData></worksheet>")),
    ];
    if chain {
        parts.push((
            "xl/calcChain.xml",
            format!("<calcChain xmlns=\"{MAIN}\"><c r=\"C1\" i=\"1\"/><c r=\"C2\"/></calcChain>"),
        ));
    }
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, body) in parts {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(body.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

fn load(bytes: Vec<u8>, backend: usize, deferred: bool) -> Workbook {
    let mut config = WorkbookConfig::ephemeral();
    config.eval.defer_graph_building = deferred;
    match backend {
        0 => Workbook::from_reader(
            NativeXlsxAdapter::open_bytes(bytes).unwrap(),
            LoadStrategy::EagerAll,
            config,
        )
        .unwrap(),
        #[cfg(feature = "umya3")]
        1 => Workbook::from_reader(
            formualizer_workbook::Umya3Adapter::open_bytes(bytes).unwrap(),
            LoadStrategy::EagerAll,
            config,
        )
        .unwrap(),
        #[cfg(feature = "umya")]
        2 => Workbook::from_reader(
            formualizer_workbook::UmyaAdapter::open_bytes(bytes).unwrap(),
            LoadStrategy::EagerAll,
            config,
        )
        .unwrap(),
        _ => unreachable!(),
    }
}
fn backends() -> Vec<usize> {
    let mut out = vec![0];
    if cfg!(feature = "umya3") {
        out.push(1);
    }
    if cfg!(feature = "umya") {
        out.push(2);
    }
    out
}
fn number(value: f64) -> LiteralValue {
    LiteralValue::Number(value)
}

#[test]
fn legacy_match_ranges_and_scalar_controls_with_or_without_calc_chain() {
    for backend in backends() {
        for deferred in [false, true] {
            for chain in [false, true] {
                for missing in [false, true] {
                    for scalar in [false, true] {
                        let a1 = if missing { 9 } else { 10 };
                        let f1 = if scalar {
                            "MATCH(A1,B1:B2,0)"
                        } else {
                            "MATCH(A1:A2,B1:B2,0)"
                        };
                        let f2 = if scalar { "MATCH(A2,B1:B2,0)" } else { f1 };
                        let bytes = package(
                            &format!(
                                "<row r=\"1\"><c r=\"A1\"><v>{a1}</v></c><c r=\"B1\"><v>10</v></c><c r=\"C1\"><f>{f1}</f></c></row><row r=\"2\"><c r=\"A2\"><v>20</v></c><c r=\"B2\"><v>20</v></c><c r=\"C2\"><f>{f2}</f></c></row>"
                            ),
                            chain,
                        );
                        let mut source = NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap();
                        assert_eq!(
                            source.read_sheet("Probe").unwrap().cells[&(1, 3)]
                                .formula
                                .as_deref(),
                            Some(format!("={f1}").as_str())
                        );
                        let mut wb = load(bytes, backend, deferred);
                        wb.evaluate_all().unwrap();
                        let first = wb.get_value("Probe", 1, 3).unwrap();
                        if missing {
                            assert!(
                                matches!(first, LiteralValue::Error(e) if e.kind == formualizer_common::ExcelErrorKind::Na)
                            );
                        } else {
                            assert_eq!(first, number(1.0));
                        }
                        assert_eq!(
                            wb.get_value("Probe", 2, 3),
                            Some(number(2.0)),
                            "backend={backend}, deferred={deferred}, chain={chain}, missing={missing}, scalar={scalar}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn arrays_keep_dynamic_growth_and_static_cse_extent() {
    for backend in backends() {
        for deferred in [false, true] {
            for dynamic in [false, true] {
                let cm = if dynamic { " cm=\"1\"" } else { "" };
                // A scalar prior extent must grow only for an explicit dynamic array.
                let rows = format!(
                    "<row r=\"1\"><c r=\"A1\"><v>10</v></c><c r=\"C1\"{cm}><f t=\"array\" ref=\"C1\">A1:A2</f><v>999</v></c></row><row r=\"2\"><c r=\"A2\"><v>20</v></c></row>"
                );
                let mut wb = load(package(&rows, false), backend, deferred);
                wb.evaluate_all().unwrap();
                assert_eq!(wb.get_value("Probe", 1, 3), Some(number(10.0)));
                assert_eq!(
                    wb.get_value("Probe", 2, 3).unwrap_or(LiteralValue::Empty),
                    if dynamic {
                        number(20.0)
                    } else {
                        LiteralValue::Empty
                    }
                );
            }
        }
    }
}

#[test]
fn saved_array_children_are_outputs_and_do_not_block_recalculation() {
    for backend in backends() {
        for dynamic in [false, true] {
            let cm = if dynamic { " cm=\"1\"" } else { "" };
            let rows = format!(
                "<row r=\"1\"><c r=\"A1\"><v>10</v></c><c r=\"C1\"{cm}><f t=\"array\" ref=\"C1:C2\">A1:A2</f><v>999</v></c></row><row r=\"2\"><c r=\"A2\"><v>20</v></c><c r=\"C2\"><v>888</v></c></row>"
            );
            let mut wb = load(package(&rows, false), backend, false);
            wb.evaluate_all().unwrap();
            assert_eq!(wb.get_value("Probe", 1, 3), Some(number(10.0)));
            assert_eq!(wb.get_value("Probe", 2, 3), Some(number(20.0)));
        }
    }
}

#[test]
fn shared_formulas_intersect_without_a_calc_chain() {
    for backend in backends() {
        for deferred in [false, true] {
            let rows = "<row r=\"1\"><c r=\"A1\"><v>10</v></c><c r=\"B1\"><v>10</v></c><c r=\"C1\"><f t=\"shared\" si=\"0\" ref=\"C1:C2\">MATCH($A$1:$A$2,$B$1:$B$2,0)</f></c></row><row r=\"2\"><c r=\"A2\"><v>20</v></c><c r=\"B2\"><v>20</v></c><c r=\"C2\"><f t=\"shared\" si=\"0\"/></c></row>";
            let mut wb = load(package(rows, false), backend, deferred);
            wb.evaluate_all().unwrap();
            assert_eq!(wb.get_value("Probe", 1, 3), Some(number(1.0)));
            assert_eq!(wb.get_value("Probe", 2, 3), Some(number(2.0)));
        }
    }
}

#[test]
fn scalar_dynamic_anchor_retains_spill_identity() {
    for backend in backends() {
        for deferred in [false, true] {
            let rows = "<row r=\"1\"><c r=\"A1\"><v>10</v></c><c r=\"C1\" cm=\"1\"><f t=\"array\" ref=\"C1\">A1</f><v>999</v></c></row><row r=\"5\"><c r=\"C5\"><f>SUM(C1#)</f></c></row>";
            let mut wb = load(package(rows, false), backend, deferred);
            wb.evaluate_all().unwrap();
            assert_eq!(wb.get_value("Probe", 5, 3), Some(number(10.0)));
        }
    }
}

#[test]
fn new_workbook_formulas_keep_dynamic_semantics() {
    let mut wb = Workbook::new();
    wb.set_value("Sheet1", 1, 1, number(10.0)).unwrap();
    wb.set_value("Sheet1", 2, 1, number(20.0)).unwrap();
    wb.set_formula("Sheet1", 1, 3, "=A1:A2").unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("Sheet1", 2, 3), Some(number(20.0)));
}
