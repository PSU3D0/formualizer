#![cfg(feature = "xlsx-recalc")]
mod support {
    pub mod source_xlsx;
}
use formualizer_workbook::recalculate_xlsx_bytes;
use support::source_xlsx::*;
const TABLE: &str = "xl/tables/table1.xml";
fn fixture(formula: &str) -> Parts {
    let mut p = without_metadata(package(
        "A1:D4",
        &format!(
            "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Qty</t></is></c><c r=\"D1\"><f>{formula}</f><v>99</v></c></row><row r=\"2\"><c r=\"A2\"><v>2</v></c></row><row r=\"3\"><c r=\"A3\"><v>3</v></c></row>"
        ),
        "<tableParts count=\"1\"><tablePart r:id=\"rId3\"/></tableParts>",
    ));
    p = edit(
        p,
        "xl/worksheets/_rels/sheet1.xml.rels",
        "</Relationships>",
        &format!(
            "<Relationship Id=\"rId3\" Type=\"{OFFICE}/table\" Target=\"../tables/table1.xml\"/></Relationships>"
        ),
    );
    p = edit(
        p,
        TYPES,
        "</Types>",
        "<Override PartName=\"/xl/tables/table1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml\"/></Types>",
    );
    p.insert(TABLE.into(), format!("<table xmlns=\"{MAIN}\" id=\"1\" name=\"Table1\" displayName=\"Table1\" ref=\"A1:A3\" totalsRowShown=\"0\"><autoFilter ref=\"A1:A3\"/><tableColumns count=\"1\"><tableColumn id=\"1\" name=\"Qty\"/></tableColumns><tableStyleInfo name=\"TableStyleMedium9\" showFirstColumn=\"0\" showLastColumn=\"0\" showRowStripes=\"1\" showColumnStripes=\"0\"/></table>"));
    p
}
#[test]
fn column_reference_preserves_table_and_reruns() {
    let p = fixture("SUM(Table1[Qty])");
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(
        parse_sheet(&sheet_xml(&out.bytes)).cell("D1").v.as_deref(),
        Some("5")
    );
    let mut got = unpack(&out.bytes);
    got.remove(SHEET);
    let mut expected = p;
    expected.remove(SHEET);
    assert_eq!(got, expected);
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, Default::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}
#[test]
fn unused_table_is_admitted() {
    let out = recalculate_xlsx_bytes(&pack(&fixture("1+2")), Default::default()).unwrap();
    assert_eq!(
        parse_sheet(&sheet_xml(&out.bytes)).cell("D1").v.as_deref(),
        Some("3")
    );
}
#[test]
fn managed_column_missing_formula_is_refused() {
    let p = edit(
        fixture("1+2"),
        TABLE,
        "<tableColumn id=\"1\" name=\"Qty\"/>",
        "<tableColumn id=\"1\" name=\"Qty\"><calculatedColumnFormula>1+2</calculatedColumnFormula></tableColumn>",
    );
    let error = recalculate_xlsx_bytes(&pack(&p), Default::default())
        .unwrap_err()
        .to_string();
    assert!(error.contains("write the formula into each row"), "{error}");
}
#[test]
fn blank_table_cells_obstruct_spills_and_dependents() {
    let p = edit(
        fixture("SUM(B1)"),
        SHEET,
        "</is></c>",
        "</is></c><c r=\"B1\"><f>SEQUENCE(3,1)</f><v>99</v></c>",
    );
    let p = edit(
        p,
        TABLE,
        "ref=\"A1:A3\"",
        "ref=\"B2:B4\" totalsRowCount=\"1\" headerRowCount=\"0\"",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    let sheet = parse_sheet(&sheet_xml(&out.bytes));
    assert_eq!(sheet.cell("B1").v.as_deref(), Some("#SPILL!"));
    assert_eq!(sheet.cell("D1").v.as_deref(), Some("#SPILL!"));
}

#[test]
fn selectors_and_case_folding() {
    for (formula, expected) in [
        ("SUM(Table1[#Data])", "5"),
        ("SUM(Table1[#All])", "5"),
        ("INDEX(Table1[#Headers],1,1)", "Qty"),
        ("SUM(tAbLe1[qTY])", "5"),
    ] {
        let out = recalculate_xlsx_bytes(&pack(&fixture(formula)), Default::default()).unwrap();
        assert_eq!(
            parse_sheet(&sheet_xml(&out.bytes)).cell("D1").v.as_deref(),
            Some(expected),
            "{formula}"
        );
    }
}
#[test]
fn managed_shared_column_and_totals_with_stored_this_row_spelling() {
    let mut p = fixture("SUM(Table1[Amount])+SUM(Table1[#Totals])");
    p.insert(SHEET.into(), worksheet("A1:D4", concat!(
        "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Qty</t></is></c><c r=\"B1\" t=\"inlineStr\"><is><t>Amount</t></is></c><c r=\"D1\"><f>SUM(Table1[Amount])+SUM(Table1[#Totals])</f><v>99</v></c></row>",
        "<row r=\"2\"><c r=\"A2\"><v>2</v></c><c r=\"B2\"><f t=\"shared\" si=\"0\" ref=\"B2:B3\">Table1[[#This Row],[Qty]]*10</f><v>99</v></c></row>",
        "<row r=\"3\"><c r=\"A3\"><v>3</v></c><c r=\"B3\"><f t=\"shared\" si=\"0\"/><v>99</v></c></row>",
        "<row r=\"4\"><c r=\"A4\"><f>SUBTOTAL(109,Table1[Qty])</f><v>99</v></c><c r=\"B4\"><f>SUM(Table1[Amount])</f><v>99</v></c></row>"
    ), "<tableParts count=\"1\"><tablePart r:id=\"rId3\"/></tableParts>"));
    p.insert(TABLE.into(), format!("<table xmlns=\"{MAIN}\" id=\"1\" name=\"Table1\" displayName=\"Table1\" ref=\"A1:B4\" totalsRowCount=\"1\"><tableColumns count=\"2\"><tableColumn id=\"1\" name=\"Qty\" totalsRowFunction=\"sum\"/><tableColumn id=\"2\" name=\"Amount\"><calculatedColumnFormula>Table1[[#This Row],[Qty]]*10</calculatedColumnFormula><totalsRowFormula>SUM(Table1[Amount])</totalsRowFormula></tableColumn></tableColumns></table>"));
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    let sheet = parse_sheet(&sheet_xml(&out.bytes));
    for (address, expected) in [
        ("B2", "20"),
        ("B3", "30"),
        ("A4", "5"),
        ("B4", "50"),
        ("D1", "105"),
    ] {
        assert_eq!(
            sheet.cell(address).v.as_deref(),
            Some(expected),
            "{address}"
        );
    }
    assert_eq!(unpack(&out.bytes).get(TABLE), p.get(TABLE));
    let missing = edit(p, SHEET, "<f>SUBTOTAL(109,Table1[Qty])</f>", "");
    let err = recalculate_xlsx_bytes(&pack(&missing), Default::default())
        .unwrap_err()
        .to_string();
    assert!(err.contains("column Qty"), "{err}");
}
#[test]
fn malformed_table_metadata_and_relationships_are_refused() {
    let relations = "xl/worksheets/_rels/sheet1.xml.rels";
    for (part, from, to) in [
        (TABLE, "headerRowCount=\"0\"", "headerRowCount=\"2\""),
        (TABLE, "totalsRowShown=\"0\"", "headerRowCount=\"2\""),
        (TABLE, "totalsRowShown=\"0\"", "totalsRowCount=\"2\""),
        (TABLE, "name=\"Qty\"", "name=\"Other\""),
        (TABLE, "count=\"1\"", "count=\"2\""),
        (TABLE, "id=\"1\" name=\"Qty\"", "id=\"0\" name=\"Qty\""),
        (TABLE, "ref=\"A1:A3\"", "ref=\"A1:XFD1048576\""),
        (TABLE, "totalsRowShown=\"0\"", "connectionId=\"1\""),
        (
            relations,
            "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/table\"",
            "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/queryTable\"",
        ),
        (
            relations,
            "Target=\"../tables/table1.xml\"",
            "Target=\"https://example.invalid/table.xml\" TargetMode=\"External\"",
        ),
        (
            relations,
            "Target=\"../tables/table1.xml\"",
            "Target=\"../tables/missing.xml\"",
        ),
        (SHEET, "r:id=\"rId3\"", "r:id=\"rId99\""),
        (SHEET, "<tablePart r:id=\"rId3\"/>", ""),
        (
            TYPES,
            "spreadsheetml.table+xml",
            "spreadsheetml.worksheet+xml",
        ),
    ] {
        let p = fixture("1+2");
        if !p[part].contains(from) {
            continue;
        }
        let err = recalculate_xlsx_bytes(&pack(&edit(p, part, from, to)), Default::default())
            .unwrap_err();
        assert!(
            matches!(err, formualizer_workbook::IoError::Unsupported { .. }),
            "{part}: {err}"
        );
    }
}
#[test]
fn collisions_merges_arrays_limits_and_cancellation() {
    let p = fixture("1+2");
    let merged = edit(
        p.clone(),
        SHEET,
        "<tableParts",
        "<mergeCells count=\"1\"><mergeCell ref=\"A1:B1\"/></mergeCells><tableParts",
    );
    assert!(
        recalculate_xlsx_bytes(&pack(&merged), Default::default())
            .unwrap_err()
            .to_string()
            .contains("merged")
    );
    let cse = edit(
        p.clone(),
        SHEET,
        "<v>2</v>",
        "<f t=\"array\" ref=\"A2:A3\">SEQUENCE(2)</f><v>2</v>",
    );
    assert!(
        recalculate_xlsx_bytes(&pack(&cse), Default::default())
            .unwrap_err()
            .to_string()
            .contains("array footprint")
    );
    let collision = edit(
        p.clone(),
        "xl/workbook.xml",
        "</workbook>",
        "<definedNames><definedName name=\"tAbLe1\">Sheet1!$A$1</definedName></definedNames></workbook>",
    );
    assert!(
        recalculate_xlsx_bytes(&pack(&collision), Default::default())
            .unwrap_err()
            .to_string()
            .contains("collision")
    );
    let mut options = formualizer_workbook::XlsxRecalculateOptions::default();
    options.limits.max_cells = 2;
    assert!(recalculate_xlsx_bytes(&pack(&p), options).is_err());
    let token = formualizer_eval::engine::CancelToken::new();
    token.cancel();
    let options = formualizer_workbook::XlsxRecalculateOptions {
        cancel: Some(token),
        ..Default::default()
    };
    assert!(recalculate_xlsx_bytes(&pack(&p), options).is_err());
}
#[test]
fn array_results_inside_table_include_one_by_one() {
    for n in [1, 2] {
        let p = edit(
            fixture("SUM(B2)"),
            TABLE,
            "ref=\"A1:A3\"",
            "ref=\"B2:B4\" headerRowCount=\"0\"",
        );
        let p = edit(
            p,
            SHEET,
            "<v>2</v></c>",
            &format!("<v>2</v></c><c r=\"B2\"><f>SEQUENCE({n})</f><v>99</v></c>"),
        );
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        let sheet = parse_sheet(&sheet_xml(&out.bytes));
        let expected = if n == 1 { "1" } else { "#SPILL!" };
        assert_eq!(sheet.cell("B2").v.as_deref(), Some(expected), "size {n}");
        assert_eq!(sheet.cell("D1").v.as_deref(), Some(expected));
    }
}

fn native_value(
    formula: &str,
    sheet_name: &str,
    row: u32,
    col: u32,
) -> formualizer_common::LiteralValue {
    use formualizer_common::LiteralValue as V;
    use formualizer_eval::{
        engine::Engine,
        reference::{CellRef, Coord, RangeRef},
    };
    let mut engine = Engine::new(
        formualizer_workbook::workbook::WBResolver::default(),
        Default::default(),
    );
    let id = engine.add_sheet("Sheet1").unwrap();
    engine.add_sheet("Other").unwrap();
    for (row, col, value) in [
        (1, 1, V::Text("Qty".into())),
        (1, 2, V::Text("Price".into())),
        (2, 1, V::Number(2.0)),
        (2, 2, V::Number(3.0)),
        (3, 1, V::Number(4.0)),
        (3, 2, V::Number(5.0)),
        (4, 1, V::Number(6.0)),
        (4, 2, V::Number(8.0)),
    ] {
        engine.set_cell_value("Sheet1", row, col, value).unwrap();
    }
    engine
        .define_table(
            "Table1",
            RangeRef::new(
                CellRef::new(id, Coord::from_excel(1, 1, true, true)),
                CellRef::new(id, Coord::from_excel(4, 2, true, true)),
            ),
            true,
            vec!["Qty".into(), "Price".into()],
            true,
        )
        .unwrap();
    engine
        .set_cell_formula(
            sheet_name,
            row,
            col,
            formualizer_parse::parser::parse(format!("={}", formula.trim_start_matches('=')))
                .unwrap(),
        )
        .unwrap();
    engine.evaluate_cell(sheet_name, row, col).unwrap().unwrap()
}
fn two_column_fixture(formula: &str, row: u32, col: &str, another_sheet: bool) -> Parts {
    let mut p = fixture("0");
    let rows = (1..=4).map(|r| {
        let data = match r {
            1 => "<c r=\"A1\" t=\"inlineStr\"><is><t>Qty</t></is></c><c r=\"B1\" t=\"inlineStr\"><is><t>Price</t></is></c>".to_owned(),
            _ => format!("<c r=\"A{r}\"><v>{}</v></c><c r=\"B{r}\"><v>{}</v></c>",r*2-2,r*2-1+u32::from(r==4)),
        };
        let output = if r==row && !another_sheet { format!("<c r=\"{col}{r}\"><f>{}</f><v>99</v></c>", quick_xml::escape::escape(formula.trim_start_matches('='))) } else { String::new() };
        format!("<row r=\"{r}\">{data}{output}</row>")
    }).collect::<String>();
    p.insert(
        SHEET.into(),
        worksheet(
            "A1:D4",
            &rows,
            "<tableParts count=\"1\"><tablePart r:id=\"rId3\"/></tableParts>",
        ),
    );
    p.insert(TABLE.into(), format!("<table xmlns=\"{MAIN}\" id=\"1\" name=\"Table1\" displayName=\"Table1\" ref=\"A1:B4\" totalsRowCount=\"1\"><tableColumns count=\"2\"><tableColumn id=\"1\" name=\"Qty\"/><tableColumn id=\"2\" name=\"Price\"/></tableColumns></table>"));
    if another_sheet {
        p = edit(
            p,
            "xl/workbook.xml",
            "</sheets>",
            "<sheet name=\"Other\" sheetId=\"2\" r:id=\"rId9\"/></sheets>",
        );
        p = edit(
            p,
            WB_RELS,
            "</Relationships>",
            &format!(
                "<Relationship Id=\"rId9\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet2.xml\"/></Relationships>"
            ),
        );
        p = edit(
            p,
            TYPES,
            "</Types>",
            "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/></Types>",
        );
        p.insert(
            "xl/worksheets/sheet2.xml".into(),
            worksheet(
                "A1:D4",
                &format!(
                    "<row r=\"{row}\"><c r=\"{col}{row}\"><f>{}</f><v>99</v></c></row>",
                    quick_xml::escape::escape(formula.trim_start_matches('='))
                ),
                "",
            ),
        );
    }
    p
}
#[test]
fn lowering_matches_native_selector_values_at_external_placements() {
    use formualizer_common::LiteralValue;
    for formula in [
        "SUM(Table1[Qty])",
        "SUM(Table1[[Qty]:[Price]])",
        "SUM(Table1[#Data])",
        "SUM(Table1[#All])",
        "SUM(Table1[#Headers])",
        "SUM(Table1[#Totals])",
        "SUM(Table1[[#Data],[Qty]])",
        "SUM(Table1[[#Headers],[Price]])",
        "SUM(Table1[[#Totals],[Qty]:[Price]])",
        "SUM(Table1[[#All],[Qty]:[Price]])",
        "TEXTJOIN(\",\",FALSE,Table1[#Headers])",
    ] {
        for (sheet_name, row) in [("Sheet1", 1), ("Sheet1", 2), ("Sheet1", 4), ("Other", 2)] {
            let native = native_value(formula, sheet_name, row, 4);
            let p = two_column_fixture(formula, row, "D", sheet_name == "Other");
            let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
            let output_part = if sheet_name == "Other" {
                "xl/worksheets/sheet2.xml"
            } else {
                SHEET
            };
            let got = parse_sheet(&unpack(&out.bytes)[output_part]);
            let text = match native {
                LiteralValue::Text(s) => s,
                LiteralValue::Number(n) => n.to_string(),
                LiteralValue::Int(n) => n.to_string(),
                formualizer_common::LiteralValue::Error(e)
                    if e.kind == formualizer_common::ExcelErrorKind::NImpl =>
                {
                    match formula {
                        "SUM(Table1[[#Data],[Qty]])" => "6",
                        "SUM(Table1[[#Headers],[Price]])" => "0",
                        "SUM(Table1[[#Totals],[Qty]:[Price]])" => "14",
                        "SUM(Table1[[#All],[Qty]:[Price]])" => "28",
                        _ => panic!("no independent oracle for {formula}"),
                    }
                    .to_owned()
                }
                other => panic!("native unsupported {formula}: {other:?}"),
            };
            assert_eq!(
                got.cell(&format!("D{row}")).v.as_deref(),
                Some(text.as_str()),
                "{formula} {sheet_name} row {row}"
            );
        }
    }
}
#[test]
fn transient_lowering_preserves_nonreference_tokens_and_output_formulas() {
    let formula =
        "_xlfn.ROUND(SUM(Table1[Qty]),0)+IF(\"Table1[Qty]\"=\"Table1[Qty]\",'Sheet1'!$A$2,0)";
    let p = fixture(formula);
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    let xml = sheet_xml(&out.bytes);
    assert!(xml.contains(&format!("<f>{formula}</f>")), "{xml}");
    assert_eq!(parse_sheet(&xml).cell("D1").v.as_deref(), Some("7"));
}
#[test]
fn unlowerable_contexts_names_and_indirect_are_refused() {
    for formula in [
        "SUM(Table1[Unknown])",
        "SUM(Missing[Qty])",
        "Table1[[#This Row],[Qty]]",
        "SUM([@Qty])",
        "INDIRECT(\"Table1[Qty]\")",
        "INDIRECT(\"Table1\"&\"[Qty]\")",
    ] {
        let p = fixture(formula);
        let p = edit(
            p,
            SHEET,
            &format!("<f>{formula}</f>"),
            &format!("<f>{}</f>", quick_xml::escape::escape(formula)),
        );
        let err = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap_err();
        assert!(
            matches!(err, formualizer_workbook::IoError::Unsupported { .. }),
            "{formula}: {err}"
        );
    }
    let p = edit(
        fixture("1+2"),
        "xl/workbook.xml",
        "</workbook>",
        "<definedNames><definedName name=\"Column\">Table1[Qty]</definedName></definedNames></workbook>",
    );
    assert!(
        recalculate_xlsx_bytes(&pack(&p), Default::default())
            .unwrap_err()
            .to_string()
            .contains("defined-name")
    );
}
#[test]
fn escaped_column_names_are_lowered_without_changing_source_formula() {
    for (name, spelling) in [
        ("a[b]", "a'[b']"),
        ("a'b", "a''b"),
        ("#Qty", "'#Qty"),
        ("São Paulo", "São Paulo"),
    ] {
        let formula = format!("SUM(Table1[{spelling}])");
        let p = edit(
            fixture(&formula),
            TABLE,
            "name=\"Qty\"",
            &format!("name=\"{name}\""),
        );
        let p = edit(p, SHEET, "<t>Qty</t>", &format!("<t>{name}</t>"));
        let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
        let xml = sheet_xml(&out.bytes);
        assert!(xml.contains(&format!("<f>{formula}</f>")));
        assert_eq!(
            parse_sheet(&xml).cell("D1").v.as_deref(),
            Some("5"),
            "{spelling}"
        );
    }
}

#[test]
fn existing_dynamic_growth_into_table_preserves_anchor_identity() {
    let mut p = producer();
    p = edit(
        p,
        SHEET,
        "</worksheet>",
        "<tableParts count=\"1\"><tablePart r:id=\"rId3\"/></tableParts></worksheet>",
    );
    p = edit(
        p,
        "xl/worksheets/_rels/sheet1.xml.rels",
        "</Relationships>",
        &format!(
            "<Relationship Id=\"rId3\" Type=\"{OFFICE}/table\" Target=\"../tables/table1.xml\"/></Relationships>"
        ),
    );
    p = edit(
        p,
        TYPES,
        "</Types>",
        "<Override PartName=\"/xl/tables/table1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml\"/></Types>",
    );
    p.insert(TABLE.into(),format!("<table xmlns=\"{MAIN}\" id=\"1\" name=\"Table1\" displayName=\"Table1\" ref=\"C5:C7\" headerRowCount=\"0\"><tableColumns count=\"1\"><tableColumn id=\"1\" name=\"Qty\"/></tableColumns></table>"));
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    let sheet = parse_sheet(&sheet_xml(&out.bytes));
    for address in ["C2", "C9", "C10"] {
        assert_eq!(
            sheet.cell(address).v.as_deref(),
            Some(if address == "C2" { "#SPILL!" } else { "#REF!" }),
            "{address}"
        );
    }
    assert_eq!(
        sheet.cell("C2").attrs.get("cm").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        sheet
            .cell("C2")
            .f
            .as_ref()
            .unwrap()
            .get("t")
            .map(String::as_str),
        Some("array")
    );
    assert_eq!(unpack(&out.bytes)[TABLE], p[TABLE]);
    assert_eq!(
        recalculate_xlsx_bytes(&out.bytes, Default::default())
            .unwrap()
            .bytes,
        out.bytes
    );
}

#[test]
fn this_row_body_placements_use_native_or_independent_oracles() {
    // Row 4 is the totals row: #This Row there is refused (see
    // `this_row_in_the_totals_row_is_refused`).
    for row in [2, 3, 4] {
        for spelling in [
            "[@Qty]",
            "[@[Qty]]",
            "Table1[[#This Row],[Qty]]",
            "Table1[@Qty]",
        ] {
            let formula = format!("{spelling}*10");
            let expected = if spelling.starts_with('[') {
                match native_value(&formula, "Sheet1", row, 2) {
                    formualizer_common::LiteralValue::Number(n) => n.to_string(),
                    other => panic!("{other:?}"),
                }
            } else {
                ((row * 2 - 2) * 10).to_string()
            };
            let p = two_column_fixture("0", 1, "D", false);
            let p = edit(
                p,
                SHEET,
                &format!(
                    "<c r=\"B{row}\"><v>{}</v></c>",
                    row * 2 - 1 + u32::from(row == 4)
                ),
                &format!("<c r=\"B{row}\"><f>{formula}</f><v>99</v></c>"),
            );
            if row == 4 {
                unsupported(&pack(&p), &format!("{formula} in the totals row"));
                continue;
            }
            let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
            assert_eq!(
                parse_sheet(&sheet_xml(&out.bytes))
                    .cell(&format!("B{row}"))
                    .v
                    .as_deref(),
                Some(expected.as_str()),
                "{formula} row {row}"
            );
        }
    }
}
#[test]
fn contiguous_combined_rows_have_independent_value_oracles() {
    for (formula, expected) in [
        ("SUM(Table1[[#Headers],[#Data]])", "14"),
        ("SUM(Table1[[#Data],[#Totals]])", "28"),
        ("SUM(Table1[[#Headers],[#Data],[#Totals]])", "28"),
    ] {
        let out = recalculate_xlsx_bytes(
            &pack(&two_column_fixture(formula, 1, "D", false)),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            parse_sheet(&sheet_xml(&out.bytes)).cell("D1").v.as_deref(),
            Some(expected),
            "{formula}"
        );
    }
    let err = recalculate_xlsx_bytes(
        &pack(&two_column_fixture(
            "SUM(Table1[[#Headers],[#Totals]])",
            1,
            "D",
            false,
        )),
        Default::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("nonrectangular"), "{err}");
}
#[test]
fn duplicate_orphan_and_overlapping_tables_are_refused() {
    let rels = "xl/worksheets/_rels/sheet1.xml.rels";
    let p = fixture("1+2");
    let duplicate = edit(
        p.clone(),
        rels,
        "</Relationships>",
        &format!(
            "<Relationship Id=\"rId4\" Type=\"{OFFICE}/table\" Target=\"../tables/table1.xml\"/></Relationships>"
        ),
    );
    assert!(recalculate_xlsx_bytes(&pack(&duplicate), Default::default()).is_err());
    let unreferenced = edit(
        p.clone(),
        SHEET,
        "<tableParts count=\"1\"><tablePart r:id=\"rId3\"/></tableParts>",
        "",
    );
    assert!(recalculate_xlsx_bytes(&pack(&unreferenced), Default::default()).is_err());
    let orphan = edit(
        p.clone(),
        rels,
        &format!(
            "<Relationship Id=\"rId3\" Type=\"{OFFICE}/table\" Target=\"../tables/table1.xml\"/>"
        ),
        "",
    );
    assert!(recalculate_xlsx_bytes(&pack(&orphan), Default::default()).is_err());
    for table_name in ["Table1", "Table2"] {
        let mut p = edit(
            p.clone(),
            SHEET,
            "count=\"1\"><tablePart r:id=\"rId3\"/>",
            "count=\"2\"><tablePart r:id=\"rId3\"/><tablePart r:id=\"rId4\"/>",
        );
        p = edit(
            p,
            rels,
            "</Relationships>",
            &format!(
                "<Relationship Id=\"rId4\" Type=\"{OFFICE}/table\" Target=\"../tables/table2.xml\"/></Relationships>"
            ),
        );
        p = edit(
            p,
            TYPES,
            "</Types>",
            "<Override PartName=\"/xl/tables/table2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml\"/></Types>",
        );
        p.insert(
            "xl/tables/table2.xml".into(),
            p[TABLE].replace("Table1", table_name),
        );
        assert!(recalculate_xlsx_bytes(&pack(&p), Default::default()).is_err());
    }
}
#[test]
fn table_content_type_default_extension_is_supported() {
    let mut p = fixture("SUM(Table1[Qty])");
    let table = p.remove(TABLE).unwrap();
    p.insert("xl/tables/table1.tbl".into(), table);
    p = edit(
        p,
        "xl/worksheets/_rels/sheet1.xml.rels",
        "table1.xml",
        "table1.tbl",
    );
    p = edit(
        p,
        TYPES,
        "<Override PartName=\"/xl/tables/table1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml\"/>",
        "<Default Extension=\"tbl\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml\"/>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(
        parse_sheet(&sheet_xml(&out.bytes)).cell("D1").v.as_deref(),
        Some("5")
    );
}

#[test]
fn active_table_filter_reducers_refuse_without_stored_hidden_rows() {
    let p = edit(
        two_column_fixture("SUBTOTAL(109,Table1[Qty])", 1, "D", false),
        "xl/tables/table1.xml",
        "<tableColumns",
        "<autoFilter ref=\"A1:B3\"><filterColumn colId=\"0\"><filters><filter val=\"2\"/></filters></filterColumn></autoFilter><tableColumns",
    );
    let e = recalculate_xlsx_bytes(&pack(&p), Default::default())
        .unwrap_err()
        .to_string();
    assert!(e.contains("hidden") || e.contains("filter"), "{e}");
}

#[test]
fn empty_data_body_and_zero_header_count_are_admitted() {
    let p = edit(
        fixture("SUM(A2:A3)"),
        "xl/tables/table1.xml",
        "ref=\"A1:A3\"",
        "ref=\"A1:A1\"",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(
        parse_sheet(&sheet_xml(&out.bytes)).cell("D1").v.as_deref(),
        Some("5")
    );
    let p = edit(
        fixture("SUM(Table1[Qty])"),
        "xl/tables/table1.xml",
        "ref=\"A1:A3\"",
        "headerRowCount=\"0\" ref=\"A1:A3\"",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    assert_eq!(
        parse_sheet(&sheet_xml(&out.bytes)).cell("D1").v.as_deref(),
        Some("5")
    );
}
#[test]
fn declared_table_count_and_huge_area_are_bounded() {
    let p = edit(
        fixture("1+2"),
        SHEET,
        "<tableParts count=\"1\"",
        "<tableParts count=\"999999999\"",
    );
    let e = recalculate_xlsx_bytes(&pack(&p), Default::default())
        .unwrap_err()
        .to_string();
    assert!(e.contains("limit"), "{e}");
    let p = edit(
        fixture("1+2"),
        "xl/tables/table1.xml",
        "ref=\"A1:A3\"",
        "ref=\"A1:XFD1048576\"",
    );
    let e = recalculate_xlsx_bytes(&pack(&p), Default::default())
        .unwrap_err()
        .to_string();
    assert!(e.contains("limit"), "{e}");
}

#[test]
fn calculated_column_exceptions_use_cells_not_table_level_formulas() {
    let p = edit(
        fixture("SUM(Table1[Qty])"),
        TABLE,
        "<tableColumn id=\"1\" name=\"Qty\"/>",
        "<tableColumn id=\"1\" name=\"Qty\"><calculatedColumnFormula>99</calculatedColumnFormula></tableColumn>",
    );
    let p = edit(
        p,
        SHEET,
        "<c r=\"A2\"><v>2</v></c>",
        "<c r=\"A2\"><f>1+1</f><v>2</v></c>",
    );
    let p = edit(
        p,
        SHEET,
        "<c r=\"A3\"><v>3</v></c>",
        "<c r=\"A3\"><f>3+1</f><v>3</v></c>",
    );
    let out = recalculate_xlsx_bytes(&pack(&p), Default::default()).unwrap();
    let sheet = parse_sheet(&sheet_xml(&out.bytes));
    assert_eq!(sheet.cell("A2").v.as_deref(), Some("2"));
    assert_eq!(sheet.cell("A3").v.as_deref(), Some("4"));
    assert_eq!(sheet.cell("D1").v.as_deref(), Some("6"));
    assert_eq!(unpack(&out.bytes)[TABLE], p[TABLE]);
}

#[test]
fn formula_free_tables_still_validate_headers_and_rerun_without_changes() {
    let p = edit(
        fixture("1+2"),
        SHEET,
        "<c r=\"D1\"><f>1+2</f><v>99</v></c>",
        "",
    );
    let bytes = pack(&p);
    let out = recalculate_xlsx_bytes(&bytes, Default::default()).unwrap();
    assert_eq!(out.formula_cells, 0);
    assert_eq!(out.bytes, bytes);
    let p = edit(p, SHEET, "<t>Qty</t>", "<t>Wrong</t>");
    let e = recalculate_xlsx_bytes(&pack(&p), Default::default())
        .unwrap_err()
        .to_string();
    assert!(e.contains("header"), "{e}");
}

#[test]
fn multicolumn_shared_this_row_keeps_column_identity_for_every_follower() {
    let mut rows = "<row r=\"1\">".to_owned();
    for name in ["A", "B", "C", "D"] {
        rows.push_str(&format!(
            "<c r=\"{name}1\" t=\"inlineStr\"><is><t>{name}</t></is></c>"
        ));
    }
    rows.push_str("</row>");
    for row in 2..=5 {
        let master = if row == 2 {
            "<f t=\"shared\" si=\"0\" ref=\"C2:D5\">[@A]</f>"
        } else {
            "<f t=\"shared\" si=\"0\"/>"
        };
        rows.push_str(&format!("<row r=\"{row}\"><c r=\"A{row}\"><v>{}</v></c><c r=\"B{row}\"><v>999</v></c><c r=\"C{row}\">{master}<v>99</v></c><c r=\"D{row}\"><f t=\"shared\" si=\"0\"/><v>99</v></c></row>",(row-1)*10));
    }
    let mut p = fixture("1+2");
    let xml = &p[SHEET];
    let start = xml.find("<sheetData>").unwrap() + "<sheetData>".len();
    let end = xml.find("</sheetData>").unwrap();
    let mut xml = xml.clone();
    xml.replace_range(start..end, &rows);
    p.insert(SHEET.into(), xml.replace("A1:D4", "A1:D5"));
    p.insert(TABLE.into(),format!("<table xmlns=\"{MAIN}\" id=\"1\" name=\"Table1\" displayName=\"Table1\" ref=\"A1:D5\"><tableColumns count=\"4\"><tableColumn id=\"1\" name=\"A\"/><tableColumn id=\"2\" name=\"B\"/><tableColumn id=\"3\" name=\"C\"/><tableColumn id=\"4\" name=\"D\"/></tableColumns></table>"));
    // Compare both a vertical shared master and a horizontal+vertical one
    // against native plain-A1 references over independently seeded literals.
    for vertical_only in [false, true] {
        let source = if vertical_only {
            edit(p.clone(), SHEET, "ref=\"C2:D5\"", "ref=\"C2:C5\"")
        } else {
            p.clone()
        };
        let source = if vertical_only {
            let mut source = source;
            let mut xml = source[SHEET].clone();
            for row in 2..=5 {
                xml = xml.replace(
                    &format!("<c r=\"D{row}\"><f t=\"shared\" si=\"0\"/><v>99</v></c>"),
                    &format!("<c r=\"D{row}\"><v>999</v></c>"),
                );
            }
            source.insert(SHEET.into(), xml);
            source
        } else {
            source
        };
        let mut native = formualizer_eval::engine::Engine::new(
            formualizer_workbook::workbook::WBResolver::default(),
            Default::default(),
        );
        for row in 2..=5 {
            native
                .set_cell_value(
                    "Sheet1",
                    row,
                    1,
                    formualizer_common::LiteralValue::Number(f64::from((row - 1) * 10)),
                )
                .unwrap();
            for col in [3, 4] {
                native
                    .set_cell_formula(
                        "Sheet1",
                        row,
                        col,
                        formualizer_parse::parser::parse(format!("=$A{row}")).unwrap(),
                    )
                    .unwrap();
            }
        }
        native.evaluate_all().unwrap();
        let out = recalculate_xlsx_bytes(&pack(&source), Default::default()).unwrap();
        let sheet = parse_sheet(&sheet_xml(&out.bytes));
        for row in 2..=5 {
            for col in if vertical_only {
                &["C"][..]
            } else {
                &["C", "D"][..]
            } {
                assert_eq!(
                    native.get_cell_value("Sheet1", row, if *col == "C" { 3 } else { 4 }),
                    Some(formualizer_common::LiteralValue::Number(f64::from(
                        (row - 1) * 10
                    )))
                );
                assert_eq!(
                    sheet
                        .cell(&format!("{col}{row}"))
                        .v
                        .as_ref()
                        .unwrap()
                        .parse::<u32>()
                        .unwrap(),
                    (row - 1) * 10
                );
            }
        }
        assert_eq!(unpack(&out.bytes)[TABLE], p[TABLE]);
        assert_eq!(
            recalculate_xlsx_bytes(&out.bytes, Default::default())
                .unwrap()
                .bytes,
            out.bytes
        );
    }
}

// ---- Shared-formula sheet qualifiers, bare table names, INDIRECT text ----

fn unsupported(bytes: &[u8], context: &str) -> String {
    match recalculate_xlsx_bytes(bytes, Default::default()) {
        Err(e @ formualizer_workbook::IoError::Unsupported { .. }) => e.to_string(),
        Err(other) => panic!("{context}: expected Unsupported, got {other:?}"),
        Ok(_) => panic!("{context}: expected Unsupported, got a published package"),
    }
}
/// `Qty` 2/4/6, `Price` 3/5/7 and a blank `Amount` column in `Table1`
/// (A1:C4, or A1:C5 with an empty totals row).
fn priced(sheet: &str, totals: bool, extra: Vec<(String, String)>) -> Ws {
    let mut cells = vec![
        text("A1", "Qty"),
        text("B1", "Price"),
        text("C1", "Amount"),
        num("A2", 2.0),
        num("B2", 3.0),
        num("A3", 4.0),
        num("B3", 5.0),
        num("A4", 6.0),
        num("B4", 7.0),
    ];
    cells.extend(extra);
    Ws::new(sheet, cells).table(table_xml(
        1,
        "Table1",
        if totals { "A1:C5" } else { "A1:C4" },
        &["Qty", "Price", "Amount"],
        totals,
    ))
}
/// `Table` (Qty 1/2/3) on `table_sheet`, a decoy sheet, and `Calc` with the
/// shared formula `formula` over C2:C4 (A2:A4 = 10/20/30).
fn shared_calc(table_sheet: &str, decoy: &str, formula: &str) -> Vec<u8> {
    let table = Ws::new(
        table_sheet,
        vec![
            text("A1", "Qty"),
            num("A2", 1.0),
            num("A3", 2.0),
            num("A4", 3.0),
        ],
    )
    .table(table_xml(1, "Sales", "A1:A4", &["Qty"], false));
    let decoy = Ws::new(
        decoy,
        vec![num("A2", 100.0), num("A3", 100.0), num("A4", 100.0)],
    );
    let calc = Ws::new(
        "Calc",
        vec![
            num("A2", 10.0),
            num("A3", 20.0),
            num("A4", 30.0),
            formula_with("C2", formula, " t=\"shared\" si=\"0\" ref=\"C2:C4\""),
            follower("C3", 0),
            follower("C4", 0),
        ],
    );
    book(&[table, decoy, calc], "")
}
fn calc_values(bytes: &[u8]) -> Vec<Option<String>> {
    ["C2", "C3", "C4"]
        .iter()
        .map(|c| value_at(bytes, 3, c))
        .collect()
}
#[test]
fn shared_cross_sheet_lowering_refuses_qualifiers_calamine_would_shift() {
    for (sheet, decoy) in [("FY2024", "FY2025"), ("Q1", "Q2"), ("x\"y", "Other")] {
        let e = unsupported(
            &shared_calc(sheet, decoy, "A2*SUM(Sales[Qty])"),
            &format!("table on {sheet}"),
        );
        assert!(e.contains("shared formula"), "{sheet}: {e}");
    }
    // Ordinary sheet names keep the exact cross-sheet lowering.
    for sheet in ["Data", "Sheet 1", "Summary", "Sheet2", "It's"] {
        let out = recalculate_xlsx_bytes(
            &shared_calc(sheet, "Other", "A2*SUM(Sales[Qty])"),
            Default::default(),
        )
        .unwrap_or_else(|e| panic!("{sheet}: {e}"));
        assert_eq!(
            calc_values(&out.bytes),
            [Some("60".into()), Some("120".into()), Some("180".into())],
            "{sheet}"
        );
    }
}
#[test]
fn plain_shared_formulas_refuse_shiftable_or_quote_bearing_sheet_qualifiers() {
    for (sheet, formula) in [
        ("Q1", "SUM('Q1'!$A$2:$A$4)+A2"),
        ("FY2024", "'FY2024'!A2+A2"),
        ("x\"y", "'x\"y'!$A$2+A2"),
        ("ABC1", "ABC1!$A$2+A2"),
    ] {
        let e = unsupported(&shared_calc(sheet, "Q2", formula), formula);
        assert!(e.contains("shared formula"), "{formula}: {e}");
    }
    for (sheet, formula, expected) in [
        ("Sheet2", "Sheet2!A2*2", ["2", "4", "6"]),
        ("Sheet 1", "'Sheet 1'!A2+A2", ["11", "22", "33"]),
        ("Data", "'Data'!$A$2+A2", ["11", "21", "31"]),
        ("Summary", "SUM('Summary'!$A$2:$A$4)+A2", ["16", "26", "36"]),
        ("Q1", "IF(\"'Q1'!\"=\"\",0,A2)", ["10", "20", "30"]),
    ] {
        let out = recalculate_xlsx_bytes(&shared_calc(sheet, "Other", formula), Default::default())
            .unwrap_or_else(|e| panic!("{formula}: {e}"));
        assert_eq!(
            calc_values(&out.bytes),
            expected.map(|v| Some(v.to_owned())),
            "{formula}"
        );
    }
}
#[test]
fn bare_table_names_lower_to_the_data_body() {
    for (formula, expected) in [
        ("SUM(Table1)", "27"),
        ("ROWS(Table1)", "3"),
        ("COLUMNS(Table1)", "3"),
        ("SUM(Table1)*1", "27"),
        ("VLOOKUP(4,Table1,2,FALSE)", "5"),
        ("SUM(table1)+LEN(\"Table1\")", "33"),
        ("Table10+Table1x", "5"),
    ] {
        let p = book(
            &[priced(
                "Sheet1",
                false,
                vec![formula_with("E2", formula, "")],
            )],
            "<definedName name=\"Table10\">Sheet1!$A$2</definedName><definedName name=\"Table1x\">Sheet1!$B$2</definedName>",
        );
        let out = recalculate_xlsx_bytes(&p, Default::default())
            .unwrap_or_else(|e| panic!("{formula}: {e}"));
        assert_eq!(
            value_at(&out.bytes, 1, "E2").as_deref(),
            Some(expected),
            "{formula}"
        );
        let xml = unpack(&out.bytes).remove(SHEET).unwrap();
        assert!(
            xml.contains(&format!("<f>{}</f>", quick_xml::escape::escape(formula))),
            "{xml}"
        );
    }
    // Another sheet, and a shared formula over a bare name.
    let p = book(
        &[
            priced("Sheet1", false, vec![]),
            Ws::new(
                "S2",
                vec![
                    formula("A1", "SUM(Table1)"),
                    formula_with(
                        "B1",
                        "COUNT(Table1)+A1",
                        " t=\"shared\" si=\"0\" ref=\"B1:B2\"",
                    ),
                    follower("B2", 0),
                ],
            ),
        ],
        "",
    );
    let out = recalculate_xlsx_bytes(&p, Default::default()).unwrap();
    assert_eq!(value_at(&out.bytes, 2, "A1").as_deref(), Some("27"));
    assert_eq!(value_at(&out.bytes, 2, "B1").as_deref(), Some("33"));
    assert_eq!(value_at(&out.bytes, 2, "B2").as_deref(), Some("6"));
}
#[test]
fn unprovable_bare_table_names_are_refused() {
    let p = book(
        &[priced("Sheet1", false, vec![formula("E2", "SUM(T)")])],
        "<definedName name=\"T\">Table1</definedName>",
    );
    unsupported(&p, "defined name over a bare table name");
    for formula in ["SUM(Sheet1!Table1)", "LET(x,Table1,SUM(x))"] {
        let p = book(
            &[priced(
                "Sheet1",
                false,
                vec![formula_with("E2", formula, "")],
            )],
            "",
        );
        unsupported(&p, formula);
    }
}
#[test]
fn indirect_in_table_workbooks_requires_literal_table_free_text() {
    for (e1, formula) in [
        ("Table1", "COUNTA(INDIRECT(E1))"),
        ("Table1[Qty]:Table1[Price]", "COUNTA(INDIRECT(E1))"),
        ("Table1[#All]", "COUNTA(INDIRECT(E1))"),
        ("A2:A4", "SUM(INDIRECT(E1))"),
        ("A2", "SUM(INDIRECT(\"Tab\"&\"le1\"))"),
        ("A2", "SUM(INDIRECT(\"table1\"))"),
        ("A2", "SUM(INDIRECT(\"A2\"&E1))"),
    ] {
        let p = book(
            &[priced(
                "Sheet1",
                false,
                vec![text("E1", e1), formula_with("E2", formula, "")],
            )],
            "",
        );
        let e = unsupported(&p, &format!("{formula} with E1={e1}"));
        assert!(e.contains("INDIRECT"), "{e}");
    }
    for (formula, expected) in [
        ("SUM(INDIRECT(\"A2:A4\"))", "12"),
        ("SUM(INDIRECT(\"A\"&\"2:A\"&4))", "12"),
        ("IF(\"Table1\"=\"x\",0,SUM(INDIRECT(\"B2:B4\")))", "15"),
    ] {
        let p = book(
            &[priced(
                "Sheet1",
                false,
                vec![formula_with("E2", formula, "")],
            )],
            "",
        );
        let out = recalculate_xlsx_bytes(&p, Default::default())
            .unwrap_or_else(|e| panic!("{formula}: {e}"));
        assert_eq!(
            value_at(&out.bytes, 1, "E2").as_deref(),
            Some(expected),
            "{formula}"
        );
    }
    // Workbooks without tables keep cell-sourced INDIRECT.
    let p = book(
        &[Ws::new(
            "Sheet1",
            vec![
                num("A2", 5.0),
                text("E1", "A2"),
                formula("E2", "INDIRECT(E1)*2"),
            ],
        )],
        "",
    );
    let out = recalculate_xlsx_bytes(&p, Default::default()).unwrap();
    assert_eq!(value_at(&out.bytes, 1, "E2").as_deref(), Some("10"));
}
#[test]
fn this_row_in_the_totals_row_is_refused() {
    for formula in [
        "[@Price]+1",
        "Table1[@Price]+1",
        "Table1[[#This Row],[Price]]",
    ] {
        let p = book(
            &[priced(
                "Sheet1",
                true,
                vec![formula_with("A5", formula, "")],
            )],
            "",
        );
        unsupported(&p, formula);
    }
    // A shared this-row master whose extent reaches the totals row.
    let p = book(
        &[priced(
            "Sheet1",
            true,
            vec![
                formula_with(
                    "C2",
                    "[@Qty]*[@Price]",
                    " t=\"shared\" si=\"0\" ref=\"C2:C5\"",
                ),
                follower("C3", 0),
                follower("C4", 0),
                follower("C5", 0),
            ],
        )],
        "",
    );
    unsupported(&p, "shared this-row into the totals row");
}
#[test]
fn table_past_the_last_stored_cell_without_dimension_is_admitted() {
    let p = book(
        &[Ws::new(
            "Sheet1",
            vec![
                text("A1", "Qty"),
                num("A2", 1.0),
                num("A3", 2.0),
                formula("C1", "SUM(Table1[Qty])+ROWS(Table1[Qty])"),
            ],
        )
        .table(table_xml(1, "Table1", "A1:A6", &["Qty"], false))],
        "",
    );
    let out = recalculate_xlsx_bytes(&p, Default::default()).unwrap();
    assert_eq!(value_at(&out.bytes, 1, "C1").as_deref(), Some("8"));
}
