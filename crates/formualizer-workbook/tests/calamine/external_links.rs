use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::FormulaPlaneMode;
use formualizer_workbook::{
    CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};
#[path = "../support/source_xlsx.rs"]
mod source_xlsx;
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
