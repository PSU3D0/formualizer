use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::FormulaPlaneMode;
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};
#[path = "../support/source_xlsx.rs"]
pub(super) mod source_xlsx;
use source_xlsx::*;

#[test]
fn linked_workbook_sheet_writes_are_rejected_without_creating_a_sheet() {
    let mut workbook = Workbook::new();
    for sheet in ["[1]Data", "[12]My Sheet", "[0]Data"] {
        assert!(
            workbook
                .set_value(sheet, 1, 1, LiteralValue::Number(7.0))
                .is_err()
        );
        assert!(workbook.engine().sheet_id(sheet).is_none());
    }
    for sheet in ["Data", "[x]Data", "[]Data", "prefix[1]Data"] {
        workbook
            .set_value(sheet, 1, 1, LiteralValue::Number(7.0))
            .unwrap();
        assert!(workbook.engine().sheet_id(sheet).is_some());
    }
}

fn linked_book(formulas: &[&str], failed: bool, names: &str) -> Vec<u8> {
    let cells = formulas
        .iter()
        .enumerate()
        .map(|(i, f)| formula(&format!("A{}", i + 1), f))
        .collect();
    let bytes = book(&[Ws::new("Sheet1", cells)], names);
    let mut parts = unpack(&bytes);
    // Filename deliberately differs from the workbook's [1] index.
    parts.insert("xl/externalLinks/externalLink7.xml".into(), format!(
        "<externalLink xmlns=\"{MAIN}\"><externalBook xmlns:r=\"{OFFICE}\" r:id=\"rId1\"><sheetNames><sheetName val=\"Data\"/></sheetNames><sheetDataSet><sheetData sheetId=\"0\" refreshError=\"{}\"><row r=\"1\"><cell r=\"A1\"><v>7</v></cell><cell r=\"B1\" t=\"str\"><v>text</v></cell><cell r=\"C1\" t=\"b\"><v>1</v></cell></row><row r=\"2\"><cell r=\"A2\"><v>11</v></cell></row></sheetData></sheetDataSet></externalBook></externalLink>", u8::from(failed)));
    parts.insert("xl/externalLinks/_rels/externalLink7.xml.rels".into(), format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/externalLinkPath\" Target=\"file:///missing.xlsx\" TargetMode=\"External\"/></Relationships>"));
    let parts = edit(
        parts,
        "xl/workbook.xml",
        "</sheets>",
        "</sheets><externalReferences><externalReference r:id=\"rIdLink\"/></externalReferences>",
    );
    let parts = edit(
        parts,
        WB_RELS,
        "</Relationships>",
        &format!(
            "<Relationship Id=\"rIdLink\" Type=\"{OFFICE}/externalLink\" Target=\"externalLinks/externalLink7.xml\"/></Relationships>"
        ),
    );
    let parts = edit(
        parts,
        TYPES,
        "</Types>",
        "<Override PartName=\"/xl/externalLinks/externalLink7.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.externalLink+xml\"/></Types>",
    );
    pack(&parts)
}

fn load(bytes: Vec<u8>, config: WorkbookConfig) -> Workbook {
    Workbook::from_reader(
        CalamineAdapter::open_bytes(bytes).unwrap(),
        LoadStrategy::EagerAll,
        config,
    )
    .unwrap()
}

fn check_mode(config: WorkbookConfig) {
    let mut supported = load(
        linked_book(
            &["[1]Data!A1", "SUM([1]Data!A1:A2)", "INDEX([1]Data!A1:A2,2)"],
            false,
            "",
        ),
        config.clone(),
    );
    assert_eq!(
        supported.engine().config.defer_graph_building,
        config.eval.defer_graph_building
    );
    // Engine construction treats the historical FormulaPlane modes as Off.
    assert_eq!(
        supported.engine().config.formula_plane_mode,
        FormulaPlaneMode::Off
    );
    supported.evaluate_all().unwrap();
    for (row, number) in [(1, 7.0), (2, 18.0), (3, 11.0)] {
        assert_eq!(
            supported.get_value("Sheet1", row, 1),
            Some(LiteralValue::Number(number))
        );
    }
    let formulas = [
        "[1]Data!A1",
        "SUM([1]Data!A1:A2)",
        "INDEX([1]Data!A1:A2,2)",
        "ROW([1]Data!A1)",
        "SUM([1]Data!A1:A2)+1",
    ];
    let mut workbook = load(linked_book(&formulas, false, ""), config);
    workbook.evaluate_all().unwrap();
    for (row, number) in [(1, 7.0), (2, 18.0), (3, 11.0), (5, 19.0)] {
        assert_eq!(
            workbook.get_value("Sheet1", row, 1),
            Some(LiteralValue::Number(number))
        );
    }
    assert!(
        matches!(workbook.get_value("Sheet1", 4, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Ref)
    );
    assert_eq!(
        workbook.get_formula("Sheet1", 4, 1).as_deref(),
        Some("=ROW([1]Data!A1)")
    );
    assert_eq!(workbook.cached_external_link_indices(), &[1]);
    workbook.set_formula("Sheet1", 4, 1, "=3").unwrap();
    assert_eq!(workbook.get_formula("Sheet1", 4, 1).as_deref(), Some("=3"));
    workbook.evaluate_all().unwrap();
    assert_eq!(
        workbook.get_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(3.0))
    );
}

#[cfg(feature = "xlsx-recalc")]
#[test]
fn native_cached_links_match_supported_refused_and_named_values() {
    use formualizer_workbook::backends::NativeXlsxAdapter;
    let formulas = [
        "[1]Data!A1",
        "SUM([1]Data!A1:A2)",
        "ROW([1]Data!A1)",
        "Linked+1",
    ];
    let bytes = linked_book(
        &formulas,
        false,
        "<definedName name=\"Linked\">[1]Data!A1</definedName>",
    );
    for config in [
        WorkbookConfig::ephemeral(),
        WorkbookConfig::interactive(),
        WorkbookConfig::ephemeral()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
    ] {
        let (mut baseline, baseline_stats) = Workbook::from_reader_with_adapter_stats(
            CalamineAdapter::open_bytes(bytes.clone()).unwrap(),
            LoadStrategy::EagerAll,
            config.clone(),
        )
        .unwrap();
        let (mut native, native_stats) = Workbook::from_reader_with_adapter_stats(
            NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap(),
            LoadStrategy::EagerAll,
            config,
        )
        .unwrap();
        assert_eq!(baseline_stats, native_stats);
        assert_eq!(
            baseline.name_import_diagnostics(),
            native.name_import_diagnostics()
        );
        assert_eq!(
            baseline.cached_external_link_indices(),
            native.cached_external_link_indices()
        );
        for row in 1..=4 {
            assert_eq!(
                baseline.get_formula("Sheet1", row, 1),
                native.get_formula("Sheet1", row, 1)
            );
            assert_eq!(
                baseline.evaluate_cell("Sheet1", row, 1).unwrap(),
                native.evaluate_cell("Sheet1", row, 1).unwrap()
            );
        }
    }
}

#[cfg(feature = "xlsx-recalc")]
#[test]
fn native_formula_handoff_stats_include_unresolved_shared_records() {
    use formualizer_workbook::backends::NativeXlsxAdapter;
    let bytes = book(
        &[Ws::new(
            "Sheet1",
            vec![(
                "A1".into(),
                "<c r=\"A1\"><f t=\"shared\" si=\"9\"/></c>".into(),
            )],
        )],
        "",
    );
    for config in [WorkbookConfig::ephemeral(), WorkbookConfig::interactive()] {
        let (_, baseline) = Workbook::from_reader_with_adapter_stats(
            CalamineAdapter::open_bytes(bytes.clone()).unwrap(),
            LoadStrategy::EagerAll,
            config.clone(),
        )
        .unwrap();
        let (_, native) = Workbook::from_reader_with_adapter_stats(
            NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap(),
            LoadStrategy::EagerAll,
            config,
        )
        .unwrap();
        assert_eq!(
            baseline.as_ref().unwrap().formula_cells_handed_to_engine,
            Some(1)
        );
        assert_eq!(baseline, native);
    }
}

#[cfg(feature = "xlsx-recalc")]
#[test]
fn native_scalar_text_and_formula_attribute_refusals_match() {
    use formualizer_workbook::backends::NativeXlsxAdapter;
    for cell in [
        r#"<c r="$A$1"><v>1</v></c>"#,
        r#"<c r="A1"><f t="shared" si="bad"/></c>"#,
        r#"<c r="A1"><f si="&#48;" t="shared" ref="A1:A2">1</f></c>"#,
    ] {
        let bytes = book(&[Ws::new("Sheet1", vec![("A1".into(), cell.into())])], "");
        let baseline = Workbook::from_reader(
            CalamineAdapter::open_bytes(bytes.clone()).unwrap(),
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        )
        .err()
        .expect("reader must refuse");
        let native = Workbook::from_reader(
            NativeXlsxAdapter::open_bytes(bytes).unwrap(),
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        )
        .err()
        .expect("reader must refuse");
        assert_eq!(format!("{baseline:?}"), format!("{native:?}"));
    }
    let bytes = book(
        &[Ws::new(
            "Sheet1",
            vec![(
                "A1".into(),
                r#"<c r="A1" t="str"><v>a<![CDATA[b]]>c</v></c>"#.into(),
            )],
        )],
        "",
    );
    let baseline = load(bytes.clone(), WorkbookConfig::ephemeral());
    let native = Workbook::from_reader(
        NativeXlsxAdapter::open_bytes(bytes).unwrap(),
        LoadStrategy::EagerAll,
        WorkbookConfig::ephemeral(),
    )
    .unwrap();
    assert_eq!(
        baseline.get_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("ac".into()))
    );
    assert_eq!(
        baseline.get_value("Sheet1", 1, 1),
        native.get_value("Sheet1", 1, 1)
    );
}

#[cfg(feature = "xlsx-recalc")]
#[test]
fn native_compatible_row_addresses_match() {
    use formualizer_workbook::backends::NativeXlsxAdapter;
    for row in ["a1", "A01", "A0001", "a01", "&#65;1"] {
        let bytes = book(&[Ws::new("Sheet1", vec![formula("A1", "1")])], "");
        let parts = edit(
            unpack(&bytes),
            "xl/worksheets/sheet1.xml",
            "<row r=\"1\">",
            &format!("<row r=\"{row}\">"),
        );
        let bytes = pack(&parts);
        let baseline = Workbook::from_reader(
            CalamineAdapter::open_bytes(bytes.clone()).unwrap(),
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        );
        let native = Workbook::from_reader(
            NativeXlsxAdapter::open_bytes(bytes).unwrap(),
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        );
        match (baseline, native) {
            (Ok(mut baseline), Ok(mut native)) => {
                baseline.evaluate_all().unwrap();
                native.evaluate_all().unwrap();
                assert_eq!(
                    baseline.get_value("Sheet1", 1, 1),
                    native.get_value("Sheet1", 1, 1),
                    "{row}"
                );
            }
            (Err(baseline), Err(native)) => {
                assert_eq!(format!("{baseline:?}"), format!("{native:?}"), "{row}")
            }
            _ => panic!("different admission for row {row}"),
        }
    }
}

#[cfg(feature = "xlsx-recalc")]
#[test]
fn native_shared_follower_before_anchor_matches() {
    use formualizer_workbook::backends::NativeXlsxAdapter;
    let bytes = book(
        &[Ws::new(
            "Sheet1",
            vec![
                (
                    "A1".into(),
                    "<c r=\"A1\"><f t=\"shared\" si=\"0\"/></c>".into(),
                ),
                (
                    "B1".into(),
                    "<c r=\"B1\"><f t=\"shared\" si=\"0\" ref=\"A1:B1\">1</f></c>".into(),
                ),
            ],
        )],
        "",
    );
    for config in [WorkbookConfig::ephemeral(), WorkbookConfig::interactive()] {
        let (mut baseline, baseline_stats) = Workbook::from_reader_with_adapter_stats(
            CalamineAdapter::open_bytes(bytes.clone()).unwrap(),
            LoadStrategy::EagerAll,
            config.clone(),
        )
        .unwrap();
        let (mut native, native_stats) = Workbook::from_reader_with_adapter_stats(
            NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap(),
            LoadStrategy::EagerAll,
            config,
        )
        .unwrap();
        baseline.evaluate_all().unwrap();
        native.evaluate_all().unwrap();
        for col in [1, 2] {
            assert_eq!(
                baseline.get_formula("Sheet1", 1, col),
                native.get_formula("Sheet1", 1, col)
            );
            assert_eq!(
                baseline.get_value("Sheet1", 1, col),
                native.get_value("Sheet1", 1, col)
            );
        }
        assert_eq!(baseline_stats, native_stats);
    }
}

#[cfg(feature = "xlsx-recalc")]
#[test]
fn native_shared_string_cdata_matches_display_text() {
    use formualizer_workbook::backends::NativeXlsxAdapter;
    let bytes = book(
        &[Ws::new(
            "Sheet1",
            vec![
                ("A1".into(), "<c r=\"A1\" t=\"s\"><v>0</v></c>".into()),
                formula("B1", "A1"),
            ],
        )],
        "",
    );
    let mut parts = unpack(&bytes);
    parts.insert(
        "xl/sharedStrings.xml".into(),
        format!("<sst xmlns=\"{MAIN}\"><si><t>a<![CDATA[b]]>c</t></si></sst>"),
    );
    let parts = edit(
        parts,
        WB_RELS,
        "</Relationships>",
        &format!(
            "<Relationship Id=\"rIdSst\" Type=\"{OFFICE}/sharedStrings\" Target=\"sharedStrings.xml\"/></Relationships>"
        ),
    );
    let parts = edit(
        parts,
        TYPES,
        "</Types>",
        "<Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/></Types>",
    );
    let bytes = pack(&parts);
    let mut baseline = load(bytes.clone(), WorkbookConfig::ephemeral());
    let mut native = Workbook::from_reader(
        NativeXlsxAdapter::open_bytes(bytes).unwrap(),
        LoadStrategy::EagerAll,
        WorkbookConfig::ephemeral(),
    )
    .unwrap();
    baseline.evaluate_all().unwrap();
    native.evaluate_all().unwrap();
    for col in [1, 2] {
        assert_eq!(
            baseline.get_value("Sheet1", 1, col),
            Some(LiteralValue::Text("abc".into()))
        );
        assert_eq!(
            baseline.get_value("Sheet1", 1, col),
            native.get_value("Sheet1", 1, col)
        );
    }
}

#[cfg(feature = "xlsx-recalc")]
#[test]
fn native_reader_preserves_source_order_and_separate_refusals() {
    use formualizer_workbook::backends::NativeXlsxAdapter;
    for cells in [
        r#"<c r="A1"><f>1</f><v>1</v></c><c r="A1"><v>2</v></c>"#,
        r#"<c r="A1"><v>2</v></c><c r="A1"><f>1</f><v>1</v></c>"#,
        r#"<c r="A1"><f t="shared" si="bad"/><v>7</v></c>"#,
        "<c r=\"A1\" t=\"str\"><f>1</f><v>a\u{85}b\u{2028}c</v></c>",
        r#"<c r="A1"><f t="shared" si="bad"/><v>7</v></c><c r="B1" t="s"><v>999</v></c>"#,
        r#"<c r="A1" t="s"><v>999</v></c><c r="B1"><v>2</wrong>"#,
        r#"<c r="A1" t="s"><v>999</v></c><c r="B1" t="n"><v>bad</v></c>"#,
        r#"<c r="A1" t="s"><v>999</v></c><c r="B1" t="e"><v>bogus</v></c>"#,
        r#"<c r="A1" t="s"><v>999</v></c><c r="B"><v>1</v></c>"#,
        r#"<c r="A1"><f t="shared" si="bad"/><v>7</v></c><c r="B1"><v>2</wrong>"#,
    ] {
        let bytes = book(&[Ws::new("Sheet1", vec![])], "");
        let mut parts = unpack(&bytes);
        parts.insert(SHEET.into(), format!("<worksheet xmlns=\"{MAIN}\"><sheetData><row r=\"1\">{cells}</row></sheetData></worksheet>"));
        let bytes = pack(&parts);
        let mut baseline = CalamineAdapter::open_bytes(bytes.clone()).unwrap();
        let mut native = NativeXlsxAdapter::open_bytes(bytes.clone()).unwrap();
        for _ in 0..2 {
            assert_eq!(
                baseline.sheet_bounds("Sheet1"),
                native.sheet_bounds("Sheet1"),
                "{cells}"
            );
            let baseline = baseline.read_sheet("Sheet1");
            let native = native.read_sheet("Sheet1");
            assert_eq!(format!("{baseline:?}"), format!("{native:?}"), "{cells}");
        }
        let baseline = Workbook::from_reader_with_adapter_stats(
            CalamineAdapter::open_bytes(bytes.clone()).unwrap(),
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        );
        let native = Workbook::from_reader_with_adapter_stats(
            NativeXlsxAdapter::open_bytes(bytes).unwrap(),
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        );
        match (baseline, native) {
            (Ok((baseline, baseline_stats)), Ok((native, native_stats))) => {
                assert_eq!(baseline_stats, native_stats, "{cells}");
                assert_eq!(
                    baseline.get_value("Sheet1", 1, 1),
                    native.get_value("Sheet1", 1, 1),
                    "{cells}"
                );
                assert_eq!(
                    baseline.get_formula("Sheet1", 1, 1),
                    native.get_formula("Sheet1", 1, 1)
                );
            }
            (Err(baseline), Err(native)) => {
                assert_eq!(format!("{baseline:?}"), format!("{native:?}"))
            }
            _ => panic!("different stream admission for {cells}"),
        }
    }
}

#[cfg(feature = "xlsx-recalc")]
#[test]
fn native_generic_shared_origin_is_separate_from_stream_origin() {
    use formualizer_workbook::backends::NativeXlsxAdapter;
    let bytes = book(
        &[Ws::new(
            "Sheet1",
            vec![
                ("A1".into(), "<c r=\"A1\"><v>3</v></c>".into()),
                (
                    "B1".into(),
                    "<c r=\"B1\"><f t=\"shared\" si=\"0\" ref=\"A1:C1\">A1+1</f></c>".into(),
                ),
                (
                    "C1".into(),
                    "<c r=\"C1\"><f t=\"shared\" si=\"0\"/></c>".into(),
                ),
            ],
        )],
        "",
    );
    let mut baseline = CalamineAdapter::open_bytes(bytes.clone()).unwrap();
    let mut native = NativeXlsxAdapter::open_bytes(bytes).unwrap();
    let a = baseline.read_sheet("Sheet1").unwrap();
    let b = native.read_sheet("Sheet1").unwrap();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
    assert_eq!(a.cells[&(1, 2)].formula.as_deref(), Some("=A1+1"));
    assert_eq!(a.cells[&(1, 3)].formula.as_deref(), Some("=C1+1"));
    let mut a = Workbook::from_reader(
        baseline,
        LoadStrategy::EagerAll,
        WorkbookConfig::ephemeral(),
    )
    .unwrap();
    let mut b =
        Workbook::from_reader(native, LoadStrategy::EagerAll, WorkbookConfig::ephemeral()).unwrap();
    a.evaluate_all().unwrap();
    b.evaluate_all().unwrap();
    assert_eq!(a.get_value("Sheet1", 1, 3), Some(LiteralValue::Number(5.0)));
    assert_eq!(a.get_formula("Sheet1", 1, 3), b.get_formula("Sheet1", 1, 3));
    assert_eq!(a.get_value("Sheet1", 1, 3), b.get_value("Sheet1", 1, 3));
}

#[cfg(feature = "xlsx-recalc")]
#[test]
fn native_ordinary_structure_uses_local_names_and_first_sheet_data() {
    use formualizer_workbook::backends::NativeXlsxAdapter;
    for body in [
        r#"<sheetData><row r="1"><x:c xmlns:x="urn:foreign" r="A1"><x:v>5</x:v></x:c></row></sheetData>"#,
        r#"<x:sheetData xmlns:x="urn:foreign"><x:row r="1"><x:c r="A1"><x:v>5</x:v></x:c></x:row></x:sheetData><sheetData><row r="2"><c r="A2"><v>7</v></c></row></sheetData>"#,
        r#"<sheetData><row r="1"><c r="A1"><v>5</v></c></row></sheetData><sheetData><row r="2"><c r="A2"><v>7</v></c><c r="C2"><f>1</f></c></row></sheetData>"#,
    ] {
        let bytes = book(&[Ws::new("Sheet1", vec![])], "");
        let mut parts = unpack(&bytes);
        parts.insert(
            SHEET.into(),
            format!("<worksheet xmlns=\"{MAIN}\">{body}</worksheet>"),
        );
        let bytes = pack(&parts);
        let mut baseline = CalamineAdapter::open_bytes(bytes.clone()).unwrap();
        let mut native = NativeXlsxAdapter::open_bytes(bytes).unwrap();
        assert_eq!(
            format!("{:?}", baseline.read_sheet("Sheet1").unwrap()),
            format!("{:?}", native.read_sheet("Sheet1").unwrap())
        );
        let (a, a_stats) = Workbook::from_reader_with_adapter_stats(
            baseline,
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        )
        .unwrap();
        let (b, b_stats) = Workbook::from_reader_with_adapter_stats(
            native,
            LoadStrategy::EagerAll,
            WorkbookConfig::ephemeral(),
        )
        .unwrap();
        assert_eq!(a.get_value("Sheet1", 1, 1), Some(LiteralValue::Number(5.0)));
        assert_eq!(a.get_value("Sheet1", 1, 1), b.get_value("Sheet1", 1, 1));
        assert_eq!(a.get_value("Sheet1", 2, 1), b.get_value("Sheet1", 2, 1));
        assert_eq!(a.get_formula("Sheet1", 2, 3), b.get_formula("Sheet1", 2, 3));
        assert_eq!(a.sheet_dimensions("Sheet1"), b.sheet_dimensions("Sheet1"));
        assert_eq!(a_stats, b_stats);
    }
}

#[test]
fn cached_links_eager() {
    check_mode(WorkbookConfig::ephemeral());
}
#[test]
fn cached_links_direct_compressed() {
    check_mode(
        WorkbookConfig::ephemeral()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
    );
}
#[test]
fn cached_links_deferred() {
    check_mode(WorkbookConfig::interactive());
}

#[test]
fn cached_links_values_names_and_missing_cells() {
    let mut workbook = load(
        linked_book(
            &[
                "[1]Data!B1",
                "[1]Data!C1",
                "ISBLANK([1]Data!D1)",
                "Scalar",
                "SUM(Tbl)",
            ],
            false,
            "<definedName name=\"Scalar\">[1]Data!$A$1</definedName><definedName name=\"Tbl\">[1]Data!$A$1:$A$2</definedName>",
        ),
        WorkbookConfig::ephemeral(),
    );
    workbook.evaluate_all().unwrap();
    assert_eq!(
        workbook.get_value("Sheet1", 1, 1),
        Some(LiteralValue::Text("text".into()))
    );
    assert_eq!(
        workbook.get_value("Sheet1", 2, 1),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(
        workbook.get_value("Sheet1", 3, 1),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(
        workbook.get_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(
        workbook.get_value("Sheet1", 5, 1),
        Some(LiteralValue::Number(18.0))
    );
}

#[test]
fn cached_links_failed_refresh_and_refusals_are_cell_errors() {
    let formulas = [
        "[1]Data!D1",
        "SUM([1]Data!A1:A3)",
        "OFFSET([1]Data!A1,1,0)",
        "[1]!Total",
        "SUM([1]Data!A:A)",
        "Bad",
        "[2]Data!A1",
        "[1]Missing!A1",
    ];
    let mut workbook = load(
        linked_book(
            &formulas,
            true,
            "<definedName name=\"Bad\">OFFSET([1]Data!A1,1,0)</definedName>",
        ),
        WorkbookConfig::interactive(),
    );
    workbook.evaluate_all().unwrap();
    for row in 1..=formulas.len() as u32 {
        assert!(
            matches!(workbook.get_value("Sheet1", row, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Ref),
            "row {row}: {:?}",
            workbook.get_value("Sheet1", row, 1)
        );
        assert_eq!(
            workbook.get_formula("Sheet1", row, 1),
            Some(format!("={}", formulas[row as usize - 1]))
        );
    }
}

#[test]
fn cached_links_invalid_metadata_is_a_ref_error() {
    let bytes = linked_book(&["[1]Data!A1"], false, "");
    let parts = edit(
        unpack(&bytes),
        TYPES,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.externalLink+xml",
        "application/xml",
    );
    let mut workbook = load(pack(&parts), WorkbookConfig::ephemeral());
    workbook.evaluate_all().unwrap();
    assert!(
        matches!(workbook.get_value("Sheet1", 1, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Ref)
    );
}

#[test]
fn cached_links_missing_parts_are_ref_errors() {
    let bytes = linked_book(&["[1]Data!A1"], false, "");
    let mut parts = unpack(&bytes);
    parts.retain(|part, _| !part.starts_with("xl/externalLinks/"));
    let mut workbook = load(pack(&parts), WorkbookConfig::ephemeral());
    workbook.evaluate_all().unwrap();
    assert!(
        matches!(workbook.get_value("Sheet1", 1, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Ref)
    );
}

#[test]
fn cached_links_unservable_parts_are_ref_errors() {
    let bytes = linked_book(&["[1]Data!A1"], false, "");
    for part in [
        format!("<externalLink xmlns=\"{MAIN}\"><ddeLink/></externalLink>"),
        format!("<externalLink xmlns=\"{MAIN}\"><oleLink/></externalLink>"),
        format!(
            "<externalLink xmlns=\"{MAIN}\"><externalBook xmlns:r=\"{OFFICE}\" r:id=\"rId1\"><sheetNames><sheetName val=\"Data\"/></sheetNames></externalBook></externalLink>"
        ),
        "<malformed".to_string(),
    ] {
        let mut parts = unpack(&bytes);
        parts.insert("xl/externalLinks/externalLink7.xml".into(), part);
        let mut workbook = load(pack(&parts), WorkbookConfig::ephemeral());
        workbook.evaluate_all().unwrap();
        assert!(
            matches!(workbook.get_value("Sheet1", 1, 1), Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Ref)
        );
    }
}

#[cfg(feature = "umya")]
#[test]
fn umya_external_links_remain_unresolved() {
    let mut parts = unpack(&linked_book(&["[1]Data!A1"], false, ""));
    let mut complete = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&umya_spreadsheet::new_file(), &mut complete)
        .unwrap();
    parts.insert(
        "xl/styles.xml".into(),
        unpack(&complete)["xl/styles.xml"].clone(),
    );
    let adapter = formualizer_workbook::UmyaAdapter::open_bytes(pack(&parts)).unwrap();
    let result =
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral());
    match result {
        Err(error) => assert!(
            error.to_string().contains("Undefined name: [1]Data!A1"),
            "{error}"
        ),
        Ok(_) => panic!("umya unexpectedly loaded the external formula"),
    }
}
