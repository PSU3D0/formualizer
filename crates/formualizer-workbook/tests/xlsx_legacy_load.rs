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
    load_config(bytes, backend, config)
}

fn load_config(bytes: Vec<u8>, backend: usize, config: WorkbookConfig) -> Workbook {
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
fn volatile_array_children_saved_as_empty_formulas_are_outputs() {
    // Excel saves the other cells of a volatile array formula as `<f ca="1"/>`.
    for backend in backends() {
        for deferred in [false, true] {
            let rows = "<row r=\"1\"><c r=\"A1\"><v>10</v></c></row><row r=\"2\"><c r=\"A2\"><v>20</v></c></row><row r=\"3\"><c r=\"A3\"><v>30</v></c></row><row r=\"4\"><c r=\"A4\"><f t=\"array\" aca=\"1\" ref=\"A4:C4\" ca=\"1\">-TRANSPOSE(OFFSET(A1,0,0,3,1))</f><v>999</v></c><c r=\"B4\"><f ca=\"1\"/><v>999</v></c><c r=\"C4\"><f ca=\"1\"/><v>999</v></c></row>";
            let mut wb = load(package(rows, false), backend, deferred);
            wb.evaluate_all().unwrap();
            assert_eq!(wb.get_value("Probe", 4, 1), Some(number(-10.0)));
            assert_eq!(wb.get_value("Probe", 4, 2), Some(number(-20.0)));
            assert_eq!(wb.get_value("Probe", 4, 3), Some(number(-30.0)));
        }
    }
}

#[test]
fn empty_ordinary_formulas_keep_their_cached_value_under_every_parse_policy() {
    use formualizer_eval::engine::FormulaParsePolicy;
    for policy in [
        FormulaParsePolicy::Strict,
        FormulaParsePolicy::CoerceToError,
        FormulaParsePolicy::KeepCachedValue,
        FormulaParsePolicy::AsText,
    ] {
        for formula in ["<f/>", "<f> </f>", "<f t=\"normal\"/>"] {
            let rows = format!("<row r=\"1\"><c r=\"A1\">{formula}<v>7</v></c></row>");
            let mut config = WorkbookConfig::ephemeral();
            config.eval.formula_parse_policy = policy;
            let mut wb = load_config(package(&rows, false), 0, config);
            wb.evaluate_all().unwrap();
            assert_eq!(
                wb.get_value("Probe", 1, 1),
                Some(number(7.0)),
                "{policy:?} {formula}"
            );
            assert!(
                wb.engine().formula_parse_diagnostics().is_empty(),
                "{policy:?} {formula}"
            );
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
fn malformed_arrays_keep_the_existing_cached_fallback_policy() {
    use formualizer_eval::engine::FormulaParsePolicy;
    for backend in backends() {
        for deferred in [false, true] {
            for dynamic in [false, true] {
                let cm = if dynamic { " cm=\"1\"" } else { "" };
                let array = format!(
                    "<row r=\"1\"><c r=\"C1\"{cm}><f t=\"array\" ref=\"C1:C2\">SUM(</f><v>999</v></c><c r=\"E1\"><v>42</v></c></row><row r=\"2\"><c r=\"C2\"><v>888</v></c></row>"
                );
                let mut config = WorkbookConfig::ephemeral();
                config.eval.defer_graph_building = deferred;
                config.eval.formula_parse_policy = FormulaParsePolicy::KeepCachedValue;
                if backend == 0 {
                    config =
                        config.with_ingest_limits(formualizer_eval::engine::WorkbookLoadLimits {
                            max_sheet_logical_cells: 3,
                            ..Default::default()
                        });
                }
                let bytes = package(&array, false);
                // Source reads must still expose the saved cache and formula.
                let mut source = NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap();
                assert_eq!(
                    source.read_sheet("Probe").unwrap().cells[&(1, 3)].value,
                    Some(number(999.0))
                );
                let mut wb = load_config(bytes, backend, config);
                wb.evaluate_all().unwrap();
                // No formula vertex means no ownership or cache masking.
                assert_eq!(wb.get_value("Probe", 1, 3), Some(number(999.0)));
                assert_eq!(wb.get_value("Probe", 2, 3), Some(number(888.0)));
                assert_eq!(wb.get_value("Probe", 1, 5), Some(number(42.0)));
            }
        }
    }
}

#[test]
fn empty_array_formula_retains_cached_anchor_without_ownership() {
    for backend in backends().into_iter().filter(|b| *b != 0) {
        for deferred in [false, true] {
            let rows = "<row r=\"1\"><c r=\"C1\"><f t=\"array\" ref=\"C1:C2\"/><v>999</v></c><c r=\"E1\"><v>42</v></c></row><row r=\"2\"><c r=\"C2\"><v>888</v></c></row>";
            let mut wb = load(package(rows, false), backend, deferred);
            wb.evaluate_all().unwrap();
            assert_eq!(wb.get_value("Probe", 1, 3), Some(number(999.0)));
            assert_eq!(wb.get_value("Probe", 2, 3), Some(number(888.0)));
            assert_eq!(wb.get_value("Probe", 1, 5), Some(number(42.0)));
        }
    }
}

#[test]
fn out_of_order_saved_array_children_are_regenerated() {
    for backend in backends() {
        for deferred in [false, true] {
            for dynamic in [false, true] {
                let cm = if dynamic { " cm=\"1\"" } else { "" };
                let rows = format!(
                    "<row r=\"2\"><c r=\"A2\"><v>20</v></c><c r=\"C2\"><v>888</v></c></row><row r=\"1\"><c r=\"A1\"><v>10</v></c><c r=\"C1\"{cm}><f t=\"array\" ref=\"C1:C2\">A1:A2</f><v>999</v></c></row>"
                );
                let mut wb = load(package(&rows, false), backend, deferred);
                wb.evaluate_all().unwrap();
                assert_eq!(wb.get_value("Probe", 1, 3), Some(number(10.0)));
                assert_eq!(
                    wb.get_value("Probe", 2, 3),
                    Some(number(20.0)),
                    "backend={backend}, deferred={deferred}, dynamic={dynamic}, A2={:?}",
                    wb.get_value("Probe", 2, 1)
                );
            }
        }
    }
}

#[test]
fn over_cap_cse_is_a_local_error_without_allocating_the_extent() {
    for backend in backends() {
        for deferred in [false, true] {
            let rows = "<row r=\"1\"><c r=\"A1\"><v>10</v></c><c r=\"C1\"><f t=\"array\" ref=\"C1:XFD1048576\">A1</f><v>999</v></c><c r=\"B1\"><f>1+2</f></c></row>";
            let mut config = WorkbookConfig::ephemeral();
            config.eval.defer_graph_building = deferred;
            config.eval.spill.max_spill_cells = 2;
            let mut wb = load_config(package(rows, false), backend, config);
            wb.evaluate_all().unwrap();
            assert!(matches!(
                wb.get_value("Probe", 1, 3),
                Some(LiteralValue::Error(e)) if e.kind == formualizer_common::ExcelErrorKind::Spill
            ));
            assert_eq!(wb.get_value("Probe", 1, 2), Some(number(3.0)));
            assert_eq!(wb.get_value("Probe", 1_048_576, 16_384), None);
            assert_eq!(wb.engine().config.spill.max_spill_cells, 2);
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

fn package_without_metadata(rows: &str) -> Vec<u8> {
    let parts = vec![
        (
            "[Content_Types].xml",
            "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>".to_owned(),
        ),
        (
            "_rels/.rels",
            format!(
                "<Relationships xmlns=\"{RELS}\"><Relationship Id=\"book\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>"
            ),
        ),
        (
            "xl/workbook.xml",
            format!(
                "<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Probe\" sheetId=\"1\" r:id=\"sheet\"/></sheets></workbook>"
            ),
        ),
        (
            "xl/_rels/workbook.xml.rels",
            format!(
                "<Relationships xmlns=\"{RELS}\"><Relationship Id=\"sheet\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/></Relationships>"
            ),
        ),
        (
            "xl/styles.xml",
            format!(
                "<styleSheet xmlns=\"{MAIN}\"><fonts count=\"1\"><font/></fonts><fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills><borders count=\"1\"><border/></borders><cellStyleXfs count=\"1\"><xf/></cellStyleXfs><cellXfs count=\"1\"><xf xfId=\"0\"/></cellXfs></styleSheet>"
            ),
        ),
        (
            "xl/worksheets/sheet1.xml",
            format!("<worksheet xmlns=\"{MAIN}\"><sheetData>{rows}</sheetData></worksheet>"),
        ),
    ];
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, body) in parts {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(body.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

#[test]
fn issue_597_legacy_cse_horizontal_with_cached_follower_and_downstream() {
    // Exact minimal repro from issue #597:
    // A1=10, A2=20, C1={=TRANSPOSE(A1:A2)} ref="C1:D1" cached=10, D1 cached=20, C2=C1+D1 cached=30.
    let rows = concat!(
        "<row r=\"1\">",
        "<c r=\"A1\"><v>10</v></c>",
        "<c r=\"C1\"><f t=\"array\" ref=\"C1:D1\">TRANSPOSE(A1:A2)</f><v>10</v></c>",
        "<c r=\"D1\"><v>20</v></c>",
        "</row>",
        "<row r=\"2\">",
        "<c r=\"A2\"><v>20</v></c>",
        "<c r=\"C2\"><f>C1+D1</f><v>30</v></c>",
        "</row>",
    );
    for backend in backends() {
        for with_metadata in [false, true] {
            for interactive in [false, true] {
                for deferred in [false, true] {
                    let bytes = if with_metadata {
                        package(rows, false)
                    } else {
                        package_without_metadata(rows)
                    };
                    let mut config = if interactive {
                        WorkbookConfig::interactive()
                    } else {
                        WorkbookConfig::ephemeral()
                    };
                    config.eval.defer_graph_building = deferred;

                    // Full evaluate_all path
                    let mut wb = load_config(bytes.clone(), backend, config.clone());
                    wb.evaluate_all().unwrap();
                    assert_eq!(
                        wb.get_value("Probe", 1, 3),
                        Some(number(10.0)),
                        "C1 backend={backend} meta={with_metadata} interactive={interactive} deferred={deferred}"
                    );
                    assert_eq!(
                        wb.get_value("Probe", 1, 4),
                        Some(number(20.0)),
                        "D1 backend={backend} meta={with_metadata} interactive={interactive} deferred={deferred}"
                    );
                    assert_eq!(
                        wb.get_value("Probe", 2, 3),
                        Some(number(30.0)),
                        "C2 backend={backend} meta={with_metadata} interactive={interactive} deferred={deferred}"
                    );

                    // Demand-driven evaluate_cell on downstream C2 (without evaluate_all)
                    let mut wb_demand = load_config(bytes.clone(), backend, config.clone());
                    let c2 = wb_demand.evaluate_cell("Probe", 2, 3).unwrap();
                    assert_eq!(
                        c2,
                        number(30.0),
                        "demand C2 backend={backend} meta={with_metadata} interactive={interactive} deferred={deferred}"
                    );
                    assert_eq!(wb_demand.get_value("Probe", 1, 3), Some(number(10.0)));
                    assert_eq!(wb_demand.get_value("Probe", 1, 4), Some(number(20.0)));

                    // Mutating A2 updates D1 and C2
                    wb.set_value("Probe", 2, 1, number(50.0)).unwrap();
                    wb.evaluate_all().unwrap();
                    assert_eq!(wb.get_value("Probe", 1, 3), Some(number(10.0)));
                    assert_eq!(wb.get_value("Probe", 1, 4), Some(number(50.0)));
                    assert_eq!(wb.get_value("Probe", 2, 3), Some(number(60.0)));
                }
            }
        }
    }
}

#[test]
fn issue_597_demand_evaluate_follower_only_consumer_and_follower_cell() {
    let rows = concat!(
        "<row r=\"1\">",
        "<c r=\"A1\"><v>10</v></c>",
        "<c r=\"C1\"><f t=\"array\" ref=\"C1:D1\">TRANSPOSE(A1:A2)</f><v>10</v></c>",
        "<c r=\"D1\"><v>20</v></c>",
        "</row>",
        "<row r=\"2\">",
        "<c r=\"A2\"><v>20</v></c>",
        "<c r=\"E2\"><f>D1*2</f><v>40</v></c>",
        "</row>",
    );
    for backend in backends() {
        for interactive in [false, true] {
            let bytes = package_without_metadata(rows);
            let config = if interactive {
                WorkbookConfig::interactive()
            } else {
                WorkbookConfig::ephemeral()
            };
            let mut wb = load_config(bytes.clone(), backend, config.clone());
            let e2 = wb.evaluate_cell("Probe", 2, 5).unwrap();
            assert_eq!(
                e2,
                number(40.0),
                "demand E2 (=D1*2) backend={backend} interactive={interactive}"
            );
            let mut wb2 = load_config(bytes, backend, config);
            let d1 = wb2.evaluate_cell("Probe", 1, 4).unwrap();
            assert_eq!(
                d1,
                number(20.0),
                "demand D1 (follower) backend={backend} interactive={interactive}"
            );
        }
    }
}

#[test]
fn issue_597_cse_with_cm_zero_and_empty_array_follower_tags() {
    // Test cm="0" (OOXML default "no metadata") and empty `<f t="array"/>` / `<f ca="1"/>` followers.
    let cases = [
        (
            "cm_zero",
            concat!(
                "<row r=\"1\">",
                "<c r=\"A1\"><v>10</v></c>",
                "<c r=\"C1\" cm=\"0\"><f t=\"array\" ref=\"C1:D1\">TRANSPOSE(A1:A2)</f><v>10</v></c>",
                "<c r=\"D1\"><v>20</v></c>",
                "</row>",
                "<row r=\"2\"><c r=\"A2\"><v>20</v></c><c r=\"C2\"><f>C1+D1</f><v>30</v></c></row>",
            ),
        ),
        (
            "empty_array_follower",
            concat!(
                "<row r=\"1\">",
                "<c r=\"A1\"><v>10</v></c>",
                "<c r=\"C1\"><f t=\"array\" ref=\"C1:D1\">TRANSPOSE(A1:A2)</f><v>10</v></c>",
                "<c r=\"D1\"><f t=\"array\"/><v>20</v></c>",
                "</row>",
                "<row r=\"2\"><c r=\"A2\"><v>20</v></c><c r=\"C2\"><f>C1+D1</f><v>30</v></c></row>",
            ),
        ),
        (
            "empty_array_ca_follower",
            concat!(
                "<row r=\"1\">",
                "<c r=\"A1\"><v>10</v></c>",
                "<c r=\"C1\"><f t=\"array\" ref=\"C1:D1\" ca=\"1\">TRANSPOSE(A1:A2)</f><v>10</v></c>",
                "<c r=\"D1\"><f t=\"array\" ca=\"1\"/><v>20</v></c>",
                "</row>",
                "<row r=\"2\"><c r=\"A2\"><v>20</v></c><c r=\"C2\"><f>C1+D1</f><v>30</v></c></row>",
            ),
        ),
    ];
    for (label, rows) in cases {
        for backend in backends() {
            for interactive in [false, true] {
                let bytes = package_without_metadata(rows);
                let config = if interactive {
                    WorkbookConfig::interactive()
                } else {
                    WorkbookConfig::ephemeral()
                };
                let mut wb = load_config(bytes, backend, config);
                wb.evaluate_all().unwrap();
                assert_eq!(
                    wb.get_value("Probe", 1, 3),
                    Some(number(10.0)),
                    "C1 case={label} backend={backend} interactive={interactive}"
                );
                assert_eq!(
                    wb.get_value("Probe", 1, 4),
                    Some(number(20.0)),
                    "D1 case={label} backend={backend} interactive={interactive}"
                );
                assert_eq!(
                    wb.get_value("Probe", 2, 3),
                    Some(number(30.0)),
                    "C2 case={label} backend={backend} interactive={interactive}"
                );
            }
        }
    }
}

#[test]
fn issue_597_cse_2d_broadcast_pad_clip_and_external_blocker() {
    // 2x2 CSE with scalar broadcast, 1x3 CSE padding with #N/A, 1x2 CSE clipping larger output without touching L1,
    // single-cell CSE without ref staying 1x1 without colliding with neighbor,
    // real formula inside CSE ref blocking with #SPILL!, and non-CSE dynamic spill blocked by user value -> #SPILL!.
    let rows = concat!(
        "<row r=\"1\">",
        "<c r=\"A1\"><v>10</v></c>",
        // 2x2 scalar broadcast: C1:D2 -> all 10
        "<c r=\"C1\"><f t=\"array\" ref=\"C1:D2\">A1</f><v>99</v></c><c r=\"D1\"><v>99</v></c>",
        // 1x3 smaller result: F1:H1 -> 10, 20, #N/A
        "<c r=\"F1\"><f t=\"array\" ref=\"F1:H1\">TRANSPOSE(A1:A2)</f><v>99</v></c><c r=\"G1\"><v>99</v></c><c r=\"H1\"><v>99</v></c>",
        // 1x2 larger result clipped to ref: J1:K1 -> 10, 20; L1=777 stays untouched!
        "<c r=\"J1\"><f t=\"array\" ref=\"J1:K1\">TRANSPOSE(A1:A3)</f><v>99</v></c><c r=\"K1\"><v>99</v></c><c r=\"L1\"><v>777</v></c>",
        // Single-cell CSE without `ref` attribute: N1 -> 10; O1=888 stays untouched!
        "<c r=\"N1\"><f t=\"array\">TRANSPOSE(A1:A2)</f><v>99</v></c><c r=\"O1\"><v>888</v></c>",
        "</row>",
        "<row r=\"2\">",
        "<c r=\"A2\"><v>20</v></c>",
        "<c r=\"C2\"><v>99</v></c><c r=\"D2\"><v>99</v></c>",
        // CSE with a real formula inside its ref -> #SPILL!, real formula in G2 survives
        "<c r=\"F2\"><f t=\"array\" ref=\"F2:G2\">TRANSPOSE(A1:A2)</f><v>10</v></c><c r=\"G2\"><f>1+1</f><v>2</v></c>",
        "</row>",
        "<row r=\"3\">",
        "<c r=\"A3\"><v>30</v></c>",
        // Non-CSE dynamic formula blocked by user value in D3 -> #SPILL!
        "<c r=\"C3\"><f>SEQUENCE(1,2)</f><v>1</v></c><c r=\"D3\"><v>20</v></c>",
        // Dynamic array (cm="1") growing past its saved ref into external user value H3 -> #SPILL!
        "<c r=\"F3\" cm=\"1\"><f t=\"array\" ref=\"F3:G3\">SEQUENCE(1,3)</f><v>1</v></c><c r=\"G3\"><v>2</v></c><c r=\"H3\"><v>999</v></c>",
        "</row>",
    );
    for backend in backends() {
        for interactive in [false, true] {
            let bytes = package(rows, false);
            let config = if interactive {
                WorkbookConfig::interactive()
            } else {
                WorkbookConfig::ephemeral()
            };
            let mut wb = load_config(bytes, backend, config);
            wb.evaluate_all().unwrap();
            for r in 1..=2 {
                for c in 3..=4 {
                    assert_eq!(
                        wb.get_value("Probe", r, c),
                        Some(number(10.0)),
                        "2x2 ({r},{c}) backend={backend} interactive={interactive}"
                    );
                }
            }
            assert_eq!(wb.get_value("Probe", 1, 6), Some(number(10.0)));
            assert_eq!(wb.get_value("Probe", 1, 7), Some(number(20.0)));
            assert!(matches!(
                wb.get_value("Probe", 1, 8),
                Some(LiteralValue::Error(e)) if e.kind == formualizer_common::ExcelErrorKind::Na
            ));
            assert_eq!(wb.get_value("Probe", 1, 10), Some(number(10.0)));
            assert_eq!(wb.get_value("Probe", 1, 11), Some(number(20.0)));
            assert_eq!(wb.get_value("Probe", 1, 12), Some(number(777.0)));
            assert_eq!(
                wb.get_value("Probe", 1, 14),
                Some(number(10.0)),
                "single-cell CSE without ref N1 backend={backend} interactive={interactive}"
            );
            assert_eq!(wb.get_value("Probe", 1, 15), Some(number(888.0)));
            assert!(
                matches!(
                    wb.get_value("Probe", 2, 6),
                    Some(LiteralValue::Error(e)) if e.kind == formualizer_common::ExcelErrorKind::Spill
                ),
                "real formula in CSE ref F2 backend={backend} interactive={interactive} got={:?}",
                wb.get_value("Probe", 2, 6)
            );
            assert_eq!(wb.get_value("Probe", 2, 7), Some(number(2.0)));
            assert!(
                matches!(
                    wb.get_value("Probe", 3, 3),
                    Some(LiteralValue::Error(e)) if e.kind == formualizer_common::ExcelErrorKind::Spill
                ),
                "non-CSE blocker C3 backend={backend} interactive={interactive} got={:?}",
                wb.get_value("Probe", 3, 3)
            );
            assert_eq!(wb.get_value("Probe", 3, 4), Some(number(20.0)));
            assert!(
                matches!(
                    wb.get_value("Probe", 3, 6),
                    Some(LiteralValue::Error(e)) if e.kind == formualizer_common::ExcelErrorKind::Spill
                ),
                "dynamic array external blocker F3 backend={backend} interactive={interactive} got={:?}",
                wb.get_value("Probe", 3, 6)
            );
            assert_eq!(wb.get_value("Probe", 3, 8), Some(number(999.0)));
        }
    }
}

#[test]
fn issue_597_cse_sparse_sheet_over_cap_preservation_and_reader_cache() {
    // 1. Reader surface still exposes saved cached values of anchor and follower.
    let rows = concat!(
        "<row r=\"1\">",
        "<c r=\"A1\"><v>10</v></c>",
        "<c r=\"C1\"><f t=\"array\" ref=\"C1:D1\">TRANSPOSE(A1:A2)</f><v>10</v></c>",
        "<c r=\"D1\"><v>20</v></c>",
        "</row>",
        "<row r=\"2\">",
        "<c r=\"A2\"><v>20</v></c>",
        "<c r=\"C2\"><f>C1+D1</f><v>30</v></c>",
        "</row>",
    );
    let bytes = package_without_metadata(rows);
    let mut source = NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap();
    let sheet = source.read_sheet("Probe").unwrap();
    assert_eq!(sheet.cells[&(1, 3)].value, Some(number(10.0)));
    assert_eq!(sheet.cells[&(1, 4)].value, Some(number(20.0)));

    // 2. Sparse ArrowSheet ingestion still clears CSE cached followers cleanly.
    let mut sparse_cfg = WorkbookConfig::ephemeral();
    sparse_cfg = sparse_cfg.with_ingest_limits(formualizer_eval::engine::WorkbookLoadLimits {
        max_sheet_logical_cells: 10,
        ..Default::default()
    });
    let sparse_rows = concat!(
        "<row r=\"1\">",
        "<c r=\"A1\"><v>10</v></c>",
        "<c r=\"C1\"><f t=\"array\" ref=\"C1:D1\">TRANSPOSE(A1:A2)</f><v>10</v></c>",
        "<c r=\"D1\"><v>20</v></c>",
        "</row>",
        "<row r=\"2\"><c r=\"A2\"><v>20</v></c><c r=\"C2\"><f>C1+D1</f><v>30</v></c></row>",
        "<row r=\"100\"><c r=\"Z100\"><v>1</v></c></row>",
    );
    let mut wb_sparse = load_config(package_without_metadata(sparse_rows), 0, sparse_cfg);
    wb_sparse.evaluate_all().unwrap();
    assert_eq!(wb_sparse.get_value("Probe", 1, 3), Some(number(10.0)));
    assert_eq!(wb_sparse.get_value("Probe", 1, 4), Some(number(20.0)));
    assert_eq!(wb_sparse.get_value("Probe", 2, 3), Some(number(30.0)));

    // 2b. Dense sheet with >= 50 rows (where clearing cached children leaves an uncompacted
    // Empty delta-overlay entry) must still expose recalculated CSE and dynamic spill children.
    for backend in backends() {
        let dense_100_rows = concat!(
            "<row r=\"1\">",
            "<c r=\"A1\"><v>10</v></c>",
            "<c r=\"C1\"><f t=\"array\" ref=\"C1:D1\">TRANSPOSE(A1:A2)</f><v>10</v></c>",
            "<c r=\"D1\"><v>20</v></c>",
            "<c r=\"F1\" cm=\"1\"><f t=\"array\" ref=\"F1:G1\">TRANSPOSE(A1:A2)</f><v>10</v></c>",
            "<c r=\"G1\"><v>20</v></c>",
            "</row>",
            "<row r=\"2\">",
            "<c r=\"A2\"><v>20</v></c>",
            "<c r=\"C2\"><f>C1+D1</f><v>30</v></c>",
            "<c r=\"F2\"><f>F1+G1</f><v>30</v></c>",
            "</row>",
            "<row r=\"100\"><c r=\"A100\"><v>1</v></c></row>",
        );
        let mut wb_dense = load(package(dense_100_rows, false), backend, false);
        wb_dense.evaluate_all().unwrap();
        assert_eq!(
            wb_dense.get_value("Probe", 1, 3),
            Some(number(10.0)),
            "dense100 CSE C1 backend={backend}"
        );
        assert_eq!(
            wb_dense.get_value("Probe", 1, 4),
            Some(number(20.0)),
            "dense100 CSE D1 backend={backend}"
        );
        assert_eq!(
            wb_dense.get_value("Probe", 2, 3),
            Some(number(30.0)),
            "dense100 CSE C2 backend={backend}"
        );
        assert_eq!(
            wb_dense.get_value("Probe", 1, 6),
            Some(number(10.0)),
            "dense100 dynamic F1 backend={backend}"
        );
        assert_eq!(
            wb_dense.get_value("Probe", 1, 7),
            Some(number(20.0)),
            "dense100 dynamic G1 backend={backend}"
        );
        assert_eq!(
            wb_dense.get_value("Probe", 2, 6),
            Some(number(30.0)),
            "dense100 dynamic F2 backend={backend}"
        );
    }

    // 3. Over-cap CSE fails locally as #SPILL! without wiping populated cells inside its extent.
    let over_cap_rows = "<row r=\"1\"><c r=\"A1\"><v>10</v></c><c r=\"C1\"><f t=\"array\" ref=\"C1:F1\">A1</f><v>999</v></c><c r=\"D1\"><v>42</v></c></row>";
    let mut cap_cfg = WorkbookConfig::ephemeral();
    cap_cfg.eval.spill.max_spill_cells = 2;
    let mut wb_cap = load_config(package_without_metadata(over_cap_rows), 0, cap_cfg);
    wb_cap.evaluate_all().unwrap();
    assert!(matches!(
        wb_cap.get_value("Probe", 1, 3),
        Some(LiteralValue::Error(e)) if e.kind == formualizer_common::ExcelErrorKind::Spill
    ));
    assert_eq!(wb_cap.get_value("Probe", 1, 4), Some(number(42.0)));

    // 4. Empty `<f t="array"/>` without `ref` keeps its cached value under every parse policy.
    use formualizer_eval::engine::FormulaParsePolicy;
    for policy in [
        FormulaParsePolicy::Strict,
        FormulaParsePolicy::CoerceToError,
        FormulaParsePolicy::KeepCachedValue,
        FormulaParsePolicy::AsText,
    ] {
        let mut cfg = WorkbookConfig::ephemeral();
        cfg.eval.formula_parse_policy = policy;
        let mut wb = load_config(
            package_without_metadata("<row r=\"1\"><c r=\"A1\"><f t=\"array\"/><v>7</v></c></row>"),
            0,
            cfg,
        );
        wb.evaluate_all().unwrap();
        assert_eq!(wb.get_value("Probe", 1, 1), Some(number(7.0)), "{policy:?}");
        assert!(wb.engine().formula_parse_diagnostics().is_empty());
    }
}
