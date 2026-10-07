#![cfg(feature = "xlsx-recalc")]
//! Legacy implicit intersection for formulas Excel calculated: a formula
//! listed in `xl/calcChain.xml` and not marked as a dynamic array keeps its
//! pre-dynamic-array meaning. Without that evidence evaluation is unchanged.
mod support {
    pub mod source_xlsx;
}
use formualizer_workbook::{XlsxRecalculateOptions, recalculate_xlsx_bytes};
use support::source_xlsx::*;

const CHAIN_REL: &str = "<Relationship Id=\"rIdChain\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain\" Target=\"calcChain.xml\"/>";
const CHAIN_TYPE: &str = "<Override PartName=\"/xl/calcChain.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.calcChain+xml\"/>";

/// Add a calc chain listing `cells` as `(sheetId, A1)`.
fn with_chain(bytes: &[u8], cells: &[(u32, &str)]) -> Vec<u8> {
    let body: String = cells
        .iter()
        .map(|(i, r)| format!("<c r=\"{r}\" i=\"{i}\"/>"))
        .collect();
    with_chain_xml(
        bytes,
        &format!("<calcChain xmlns=\"{MAIN}\">{body}</calcChain>"),
    )
}
fn with_chain_xml(bytes: &[u8], xml: &str) -> Vec<u8> {
    let mut parts = unpack(bytes);
    parts.insert("xl/calcChain.xml".into(), xml.to_owned());
    let parts = edit(
        parts,
        WB_RELS,
        "</Relationships>",
        &format!("{CHAIN_REL}</Relationships>"),
    );
    let parts = edit(parts, TYPES, "</Types>", &format!("{CHAIN_TYPE}</Types>"));
    pack(&parts)
}
fn recalc(bytes: &[u8]) -> Vec<u8> {
    let out = recalculate_xlsx_bytes(bytes, XlsxRecalculateOptions::default()).unwrap();
    // A second run over our own output is a byte no-op.
    let again = recalculate_xlsx_bytes(&out.bytes, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(again.bytes, out.bytes, "second recalc changed the output");
    assert_eq!(again.cache_cells_changed, 0);
    out.bytes
}
fn formula_text(bytes: &[u8], index: usize, cell: &str) -> String {
    let xml = unpack(bytes)
        .remove(&format!("xl/worksheets/sheet{index}.xml"))
        .unwrap();
    let start = xml.find(&format!("<c r=\"{cell}\"")).unwrap();
    let rest = &xml[start..];
    rest[..rest.find("</c>").unwrap()].to_owned()
}

/// The Enron lookup shape: a shared family whose lookup value is a whole
/// column range; each row looks up its own `B` value.
fn lookup_book() -> Vec<u8> {
    let mut cells = vec![
        num("C11", 1.0),
        text("D11", "Mon"),
        num("C12", 2.0),
        text("D12", "Tue"),
        num("C13", 3.0),
        text("D13", "Wed"),
    ];
    for (r, v) in [(4, 2.0), (5, 3.0), (6, 1.0), (7, 9.0)] {
        cells.push(num(&format!("B{r}"), v));
    }
    cells.push(formula_with(
        "E4",
        "VLOOKUP($B$4:$B$7,$C$11:$D$13,2,FALSE)",
        " t=\"shared\" ref=\"E4:E9\" si=\"0\"",
    ));
    for r in 5..=9 {
        cells.push(follower(&format!("E{r}"), 0));
    }
    book(&[Ws::new("Sheet1", cells)], "")
}
const LOOKUP_CELLS: [&str; 6] = ["E4", "E5", "E6", "E7", "E8", "E9"];

#[test]
fn calculated_shared_lookup_intersects_its_own_row() {
    let chain: Vec<_> = LOOKUP_CELLS.iter().map(|c| (1, *c)).collect();
    let out = recalc(&with_chain(&lookup_book(), &chain));
    for (cell, expected) in [
        ("E4", "Tue"),
        ("E5", "Wed"),
        ("E6", "Mon"),
        ("E7", "#N/A"),
        // Rows 8-9 are outside $B$4:$B$7.
        ("E8", "#VALUE!"),
        ("E9", "#VALUE!"),
    ] {
        assert_eq!(value_at(&out, 1, cell).as_deref(), Some(expected), "{cell}");
    }
    // The stored formula text is untouched.
    assert!(formula_text(&out, 1, "E4").contains(
        "<f t=\"shared\" ref=\"E4:E9\" si=\"0\">VLOOKUP($B$4:$B$7,$C$11:$D$13,2,FALSE)</f>"
    ));
}

#[test]
fn without_calc_chain_evaluation_is_unchanged() {
    // An openpyxl/XlsxWriter-style workbook: no calc chain, dynamic semantics.
    let bytes = lookup_book();
    let out = recalc(&bytes);
    assert_ne!(value_at(&out, 1, "E4").as_deref(), Some("Tue"));
    // A family member missing from the chain keeps the whole family dynamic.
    let partial = with_chain(
        &bytes,
        &[(1, "E4"), (1, "E5"), (1, "E6"), (1, "E7"), (1, "E8")],
    );
    let partial_out = recalc(&partial);
    assert_eq!(value_at(&partial_out, 1, "E4"), value_at(&out, 1, "E4"));
}

#[test]
fn whole_row_comparison_and_reference_result() {
    // Hub Tracking G2: E2=21:21 compares with G21; the TRUE branch returns
    // E22:E23, which does not meet row 2.
    let cells = vec![
        text("E2", "Y"),
        text("A21", "id"),
        text("G21", "Y"),
        text("E22", "Y"),
        text("E23", "N"),
        formula("G2", "IF(E2=21:21,E$22:E$23,\" \")"),
        formula("H2", "IF(E2=21:21,\"same\",\"other\")"),
        formula("G22", "IF(E2=21:21,E$22:E$23,\" \")"),
        text("G3", "occupied"),
    ];
    let bytes = book(&[Ws::new("Sheet1", cells)], "");
    let out = recalc(&with_chain(&bytes, &[(1, "G2"), (1, "H2"), (1, "G22")]));
    assert_eq!(value_at(&out, 1, "G2").as_deref(), Some("#VALUE!"));
    // H21 is blank, so "Y" = blank is FALSE.
    assert_eq!(value_at(&out, 1, "H2").as_deref(), Some("other"));
    // In row 22 the condition still reads G21, and E22:E23 meets row 22.
    assert_eq!(value_at(&out, 1, "G22").as_deref(), Some("Y"));
}

#[test]
fn other_sheet_ranges_and_names_intersect_at_the_formula_cell() {
    let summary = vec![
        formula("K135", "'Study 1b'!E134:E157"),
        formula("K133", "'Study 1b'!E134:E157"),
        formula("C6", "+'Hot List'!B6:B11"),
        formula("D6", "Price*2"),
        formula("F1", "Annual!F1:G1"),
        formula("BH84", "AF84+BD84:BD85"),
        num("AF84", 1.0),
        num("BD84", 41.0),
        num("BD85", 1000.0),
    ];
    let study = vec![num("E134", 1.0), num("E135", 2.0), num("E157", 3.0)];
    let hot = vec![text("B6", "Liquids"), text("B7", "Gas")];
    let annual = vec![text("F1", "Modified"), text("G1", "Other")];
    let prices = vec![num("A5", 10.0), num("A6", 20.0), num("A7", 30.0)];
    let bytes = book(
        &[
            Ws::new("Summary", summary),
            Ws::new("Study 1b", study),
            Ws::new("Hot List", hot),
            Ws::new("Annual", annual),
            Ws::new("Prices", prices),
        ],
        "<definedName name=\"Price\">Prices!$A$5:$A$7</definedName>",
    );
    let chain: Vec<_> = ["K135", "K133", "C6", "D6", "F1", "BH84"]
        .iter()
        .map(|c| (1, *c))
        .collect();
    let out = recalc(&with_chain(&bytes, &chain));
    let sheet = parse_sheet(&unpack(&out).remove("xl/worksheets/sheet1.xml").unwrap());
    assert_eq!(sheet.cell("K135").v.as_deref(), Some("2"));
    assert_eq!(sheet.cell("K133").v.as_deref(), Some("#VALUE!"));
    assert_eq!(sheet.cell("C6").v.as_deref(), Some("Liquids"));
    assert_eq!(sheet.cell("D6").v.as_deref(), Some("40"));
    assert_eq!(sheet.cell("F1").v.as_deref(), Some("Modified"));
    assert_eq!(sheet.cell("BH84").v.as_deref(), Some("42"));
}

#[test]
fn mixed_workbook_keeps_dynamic_formulas_dynamic() {
    // B1 was calculated by Excel (legacy); C1 was added later by an agent
    // (not in the chain) and spills as a dynamic array.
    let cells = vec![
        num("A1", 1.0),
        num("A2", 2.0),
        num("A3", 3.0),
        formula("B2", "A1:A3*10"),
        formula("C1", "A1:A3*10"),
        formula("D1", "SUM(A1:A3)"),
    ];
    let bytes = with_chain(
        &book(&[Ws::new("Sheet1", cells)], ""),
        &[(1, "B2"), (1, "D1")],
    );
    let out = recalc(&bytes);
    let sheet = parse_sheet(&unpack(&out).remove(SHEET).unwrap());
    assert_eq!(sheet.cell("B2").v.as_deref(), Some("20"));
    assert!(!sheet.cell("B2").attrs.contains_key("cm"));
    assert_eq!(sheet.cell("C1").v.as_deref(), Some("10"));
    assert_eq!(sheet.cell("C3").v.as_deref(), Some("30"));
    assert!(sheet.cell("C1").attrs.contains_key("cm"));
    assert_eq!(sheet.cell("D1").v.as_deref(), Some("6"));
    // The calc chain is preserved byte for byte.
    assert_eq!(
        unpack(&out).get("xl/calcChain.xml"),
        unpack(&bytes).get("xl/calcChain.xml")
    );
}

#[test]
fn formulas_without_value_position_ranges_are_byte_identical() {
    let cells = vec![
        num("A1", 1.0),
        num("A2", 2.0),
        formula("B1", "SUM(A1:A2)"),
        formula("B2", "VLOOKUP(A1,A1:A2,1,FALSE)"),
        formula("B3", "SUMPRODUCT(A1:A2*2)"),
        formula("B4", "A1+A2"),
        formula("B5", "COUNTIF(A1:A2,\">0\")"),
    ];
    let plain = book(&[Ws::new("Sheet1", cells)], "");
    let chained = with_chain(
        &plain,
        &[(1, "B1"), (1, "B2"), (1, "B3"), (1, "B4"), (1, "B5")],
    );
    let a = recalc(&plain);
    let b = recalc(&chained);
    assert_eq!(unpack(&a).get(SHEET), unpack(&b).get(SHEET));
}

#[test]
fn array_formulas_are_never_intersected() {
    let cells = vec![
        num("A1", 1.0),
        num("A2", 2.0),
        num("A3", 3.0),
        formula_with("B1", "SUM(A1:A3*2)", " t=\"array\" ref=\"B1\""),
        formula("C2", "SUM(A1:A3*2)"),
    ];
    let bytes = with_chain(
        &book(&[Ws::new("Sheet1", cells)], ""),
        &[(1, "B1"), (1, "C2")],
    );
    let out = recalc(&bytes);
    assert_eq!(value_at(&out, 1, "B1").as_deref(), Some("12"));
    // Not entered as an array: legacy SUM(A1:A3*2) in row 2 is A2*2.
    assert_eq!(value_at(&out, 1, "C2").as_deref(), Some("4"));
}

#[test]
fn malformed_or_foreign_calc_chain_is_no_evidence() {
    let cells = vec![num("A1", 1.0), num("A2", 2.0), formula("B2", "A1:A2*10")];
    let bytes = book(&[Ws::new("Sheet1", cells)], "");
    let dynamic = recalc(&bytes);
    for xml in [
        "<calcChain xmlns=\"urn:other\"><c r=\"B2\" i=\"1\"/></calcChain>".to_owned(),
        format!("<calcChain xmlns=\"{MAIN}\"><c r=\"B2\" i=\"7\"/></calcChain>"),
        format!("<calcChain xmlns=\"{MAIN}\"><c r=\"B2\"/></calcChain>"),
        format!("<calcChain xmlns=\"{MAIN}\"><c r=\"not a cell\" i=\"1\"/></calcChain>"),
        "<calcChain".to_owned(),
    ] {
        let out = recalc(&with_chain_xml(&bytes, &xml));
        assert_eq!(
            unpack(&out).get(SHEET),
            unpack(&dynamic).get(SHEET),
            "{xml}"
        );
    }
    // Omitted `i` repeats the previous sheet.
    let out = recalc(&with_chain_xml(
        &bytes,
        &format!("<calcChain xmlns=\"{MAIN}\"><c r=\"A9\" i=\"1\"/><c r=\"B2\"/></calcChain>"),
    ));
    assert_eq!(value_at(&out, 1, "B2").as_deref(), Some("20"));
}

#[test]
fn intersected_cell_that_is_a_formula_is_read_after_it_is_calculated() {
    let cells = vec![
        num("A1", 1.0),
        num("A2", 2.0),
        formula("B1", "A1*100"),
        formula("B2", "A2*100"),
        formula("C2", "B1:B2+1"),
    ];
    let bytes = with_chain(
        &book(&[Ws::new("Sheet1", cells)], ""),
        &[(1, "C2"), (1, "B1"), (1, "B2")],
    );
    let out = recalc(&bytes);
    assert_eq!(value_at(&out, 1, "C2").as_deref(), Some("201"));
}
