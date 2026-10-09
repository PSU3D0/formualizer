#![cfg(feature = "xlsx-recalc")]
//! External workbook links: recalculation reads the values Excel cached in
//! each `xl/externalLinks/externalLinkN.xml` part, never refreshes them and
//! keeps every link part byte for byte. References it cannot map exactly to
//! cached cells refuse the workbook.
mod support {
    pub mod source_xlsx;
}
use formualizer_workbook::{
    ExternalLinkPolicy, IoError, XlsxRecalculateOptions, recalculate_xlsx_bytes,
};
use support::source_xlsx::*;

const LINK_REL: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/externalLink";
const LINK_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.externalLink+xml";
const LINK_PATH_REL: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/externalLinkPath";
const CHAIN_REL: &str = "<Relationship Id=\"rIdChain\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain\" Target=\"calcChain.xml\"/>";
const CHAIN_TYPE: &str = "<Override PartName=\"/xl/calcChain.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.calcChain+xml\"/>";

/// One cached cell: `(A1, t, v)`; `t` is `""` for a number.
type Cached<'a> = (&'a str, &'a str, &'a str);

/// An `externalBook` link listing `sheets`, with one `sheetData` per
/// `(sheetId, refreshError, cells)` entry; `None` omits the `sheetDataSet`.
fn book_link(sheets: &[&str], data: Option<&[(u32, bool, &[Cached<'_>])]>) -> String {
    let names: String = sheets
        .iter()
        .map(|s| format!("<sheetName val=\"{}\"/>", quick_xml::escape::escape(*s)))
        .collect();
    let data = data.map_or(String::new(), |sheets| {
        let body: String = sheets
            .iter()
            .map(|(id, refresh_error, cells)| {
                let mut rows: std::collections::BTreeMap<u32, String> = Default::default();
                for (r, t, v) in cells.iter() {
                    let row: u32 = r
                        .trim_start_matches(|c: char| c.is_ascii_alphabetic())
                        .parse()
                        .unwrap();
                    let t = if t.is_empty() {
                        String::new()
                    } else {
                        format!(" t=\"{t}\"")
                    };
                    rows.entry(row).or_default().push_str(&format!(
                        "<cell r=\"{r}\"{t}><v>{}</v></cell>",
                        quick_xml::escape::escape(*v)
                    ));
                }
                let rows: String = rows
                    .iter()
                    .map(|(r, cells)| format!("<row r=\"{r}\">{cells}</row>"))
                    .collect();
                let error = if *refresh_error {
                    " refreshError=\"1\""
                } else {
                    ""
                };
                format!("<sheetData sheetId=\"{id}\"{error}>{rows}</sheetData>")
            })
            .collect();
        format!("<sheetDataSet>{body}</sheetDataSet>")
    });
    format!(
        "<externalLink xmlns=\"{MAIN}\"><externalBook xmlns:r=\"{OFFICE}\" r:id=\"rId1\"><sheetNames>{names}</sheetNames>{data}</externalBook></externalLink>"
    )
}

/// Attach link parts (`[1]`, `[2]`, ... in order) to a workbook.
fn with_links(bytes: &[u8], links: &[String]) -> Vec<u8> {
    let mut parts = unpack(bytes);
    let mut refs = String::new();
    let mut rels = String::new();
    let mut types = String::new();
    for (i, xml) in links.iter().enumerate() {
        let n = i + 1;
        parts.insert(format!("xl/externalLinks/externalLink{n}.xml"), xml.clone());
        parts.insert(
            format!("xl/externalLinks/_rels/externalLink{n}.xml.rels"),
            format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{LINK_PATH_REL}\" Target=\"file:///C:/Data/Source{n}.xlsx\" TargetMode=\"External\"/></Relationships>"),
        );
        refs.push_str(&format!("<externalReference r:id=\"rIdLink{n}\"/>"));
        rels.push_str(&format!(
            "<Relationship Id=\"rIdLink{n}\" Type=\"{LINK_REL}\" Target=\"externalLinks/externalLink{n}.xml\"/>"
        ));
        types.push_str(&format!(
            "<Override PartName=\"/xl/externalLinks/externalLink{n}.xml\" ContentType=\"{LINK_TYPE}\"/>"
        ));
    }
    let parts = edit(
        parts,
        "xl/workbook.xml",
        "</sheets>",
        &format!("</sheets><externalReferences>{refs}</externalReferences>"),
    );
    let parts = edit(
        parts,
        WB_RELS,
        "</Relationships>",
        &format!("{rels}</Relationships>"),
    );
    let parts = edit(parts, TYPES, "</Types>", &format!("{types}</Types>"));
    pack(&parts)
}

/// List every formula cell of sheet 1 in a calc chain (Excel calculated them).
fn with_chain(bytes: &[u8], cells: &[&str]) -> Vec<u8> {
    let body: String = cells
        .iter()
        .map(|r| format!("<c r=\"{r}\" i=\"1\"/>"))
        .collect();
    let mut parts = unpack(bytes);
    parts.insert(
        "xl/calcChain.xml".into(),
        format!("<calcChain xmlns=\"{MAIN}\">{body}</calcChain>"),
    );
    let parts = edit(
        parts,
        WB_RELS,
        "</Relationships>",
        &format!("{CHAIN_REL}</Relationships>"),
    );
    let parts = edit(parts, TYPES, "</Types>", &format!("{CHAIN_TYPE}</Types>"));
    pack(&parts)
}

/// The standard link: `Data` (cached) and `My Sheet`.
fn data_link() -> String {
    book_link(
        &["Data", "My Sheet", "Failed"],
        Some(&[
            (
                0,
                false,
                &[
                    ("A1", "str", "a"),
                    ("B1", "", "10"),
                    ("A2", "str", "b"),
                    ("B2", "", "21"),
                    ("A3", "str", "c"),
                    ("B3", "", "32.5"),
                    ("C1", "b", "1"),
                    ("C2", "e", "#N/A"),
                    ("C3", "", "45000"),
                ],
            ),
            (1, false, &[("A1", "", "7")]),
            (2, true, &[("A1", "", "3")]),
        ]),
    )
}

fn workbook(cells: Vec<(String, String)>, names: &str) -> Vec<u8> {
    with_links(&book(&[Ws::new("Sheet1", cells)], names), &[data_link()])
}

fn recalc(bytes: &[u8]) -> formualizer_workbook::XlsxRecalculateResult {
    let out = recalculate_xlsx_bytes(bytes, XlsxRecalculateOptions::default()).unwrap();
    // A second run over our own output is a byte no-op.
    let again = recalculate_xlsx_bytes(&out.bytes, XlsxRecalculateOptions::default()).unwrap();
    assert_eq!(again.bytes, out.bytes, "second recalc changed the output");
    out
}

fn refusal(bytes: &[u8]) -> (String, String) {
    match recalculate_xlsx_bytes(bytes, XlsxRecalculateOptions::default()) {
        Err(IoError::Unsupported { feature, context }) => (feature, context),
        Err(other) => panic!("expected a refusal, got {other:?}"),
        Ok(_) => panic!("expected a refusal, got a recalculation"),
    }
}

fn refuses(bytes: &[u8], feature: &str) -> String {
    let (got, context) = refusal(bytes);
    assert_eq!(got, feature, "context: {context}");
    context
}

fn cell_type(bytes: &[u8], cell: &str) -> Option<String> {
    let xml = unpack(bytes).remove(SHEET).unwrap();
    parse_sheet(&xml).cell(cell).attrs.get("t").cloned()
}

#[test]
fn scalar_references_read_cached_values() {
    let out = recalc(&workbook(
        vec![
            formula("A1", "[1]Data!B2*2"),
            formula("A2", "[1]Data!$A$2"),
            formula("A3", "'[1]My Sheet'!A1+1"),
            formula("A4", "[1]Data!C1"),
            formula("A5", "[1]Data!C2"),
            formula("A6", "[1]Data!C3"),
            formula("A7", "[1]data!B1"),
        ],
        "",
    ));
    let bytes = &out.bytes;
    assert_eq!(value_at(bytes, 1, "A1").as_deref(), Some("42"));
    assert_eq!(value_at(bytes, 1, "A2").as_deref(), Some("b"));
    assert_eq!(cell_type(bytes, "A2").as_deref(), Some("str"));
    assert_eq!(value_at(bytes, 1, "A3").as_deref(), Some("8"));
    assert_eq!(value_at(bytes, 1, "A4").as_deref(), Some("1"));
    assert_eq!(cell_type(bytes, "A4").as_deref(), Some("b"));
    assert_eq!(value_at(bytes, 1, "A5").as_deref(), Some("#N/A"));
    assert_eq!(cell_type(bytes, "A5").as_deref(), Some("e"));
    // A date stays its serial number.
    assert_eq!(value_at(bytes, 1, "A6").as_deref(), Some("45000"));
    assert_eq!(cell_type(bytes, "A6"), None);
    // Sheet names match case-insensitively, as in Excel.
    assert_eq!(value_at(bytes, 1, "A7").as_deref(), Some("10"));
    assert_eq!(out.external_links_used, 1);
    assert_eq!(out.summary.errors, 1);
}

#[test]
fn ranges_read_the_cached_rectangle() {
    let out = recalc(&workbook(
        vec![
            formula("A1", "SUM([1]Data!B1:B3)"),
            formula("A2", "VLOOKUP(\"b\",[1]Data!$A$1:$B$3,2,FALSE)"),
            formula("A3", "INDEX('[1]Data'!$A$1:$C$3,3,2)"),
            formula("A4", "COUNTA([1]Data!A1:C3)"),
            formula("A5", "MATCH(32.5,[1]Data!B1:B3,0)"),
        ],
        "",
    ));
    let bytes = &out.bytes;
    assert_eq!(value_at(bytes, 1, "A1").as_deref(), Some("63.5"));
    assert_eq!(value_at(bytes, 1, "A2").as_deref(), Some("21"));
    assert_eq!(value_at(bytes, 1, "A3").as_deref(), Some("32.5"));
    assert_eq!(value_at(bytes, 1, "A4").as_deref(), Some("9"));
    assert_eq!(value_at(bytes, 1, "A5").as_deref(), Some("3"));
}

#[test]
fn shared_formula_followers_read_their_own_cells() {
    let mut cells = vec![formula_with(
        "D1",
        "[1]Data!B1*10",
        " t=\"shared\" ref=\"D1:D3\" si=\"0\"",
    )];
    cells.push(follower("D2", 0));
    cells.push(follower("D3", 0));
    let out = recalc(&workbook(cells, ""));
    for (cell, expected) in [("D1", "100"), ("D2", "210"), ("D3", "325")] {
        assert_eq!(value_at(&out.bytes, 1, cell).as_deref(), Some(expected));
    }
}

#[test]
fn defined_names_read_cached_values() {
    let out = recalc(&workbook(
        vec![
            formula("A1", "Rate*2"),
            formula("A2", "VLOOKUP(\"c\",Tbl,2,FALSE)"),
            formula("A3", "SUM(Tbl)"),
        ],
        "<definedName name=\"Rate\">[1]Data!$B$2</definedName><definedName name=\"Tbl\">[1]Data!$A$1:$B$3</definedName>",
    ));
    assert_eq!(value_at(&out.bytes, 1, "A1").as_deref(), Some("42"));
    assert_eq!(value_at(&out.bytes, 1, "A2").as_deref(), Some("32.5"));
    assert_eq!(value_at(&out.bytes, 1, "A3").as_deref(), Some("63.5"));
    assert_eq!(out.external_links_used, 1);
}

#[test]
fn unused_names_of_uncached_links_stay_out_of_the_calculation() {
    let bytes = with_links(
        &book(
            &[Ws::new("Sheet1", vec![formula("A1", "[1]Data!B1+1")])],
            "<definedName name=\"Old\">[2]Gone!$A$1</definedName><definedName name=\"Old2\">'[2]Gone'!$A$1:$B$2</definedName>",
        ),
        &[data_link(), book_link(&["Gone"], None)],
    );
    let out = recalc(&bytes);
    assert_eq!(value_at(&out.bytes, 1, "A1").as_deref(), Some("11"));
    // Only the first link was read.
    assert_eq!(out.external_links_used, 1);
}

#[test]
fn missing_cells_are_blank_and_failed_refresh_cells_are_ref_errors() {
    let out = recalc(&workbook(
        vec![
            formula("A1", "[1]Data!D9"),
            formula("A2", "[1]Data!D9+1"),
            formula("A3", "SUM([1]Data!B1:B9)"),
            formula("A4", "COUNTA([1]Data!B1:B9)"),
            formula("A5", "COUNTBLANK([1]Data!B1:B9)"),
            formula("A6", "[1]Failed!B5"),
            formula("A7", "[1]Failed!A1"),
            formula("A8", "SUM([1]Failed!A1:A1)"),
            formula("A9", "ISBLANK([1]Data!D9)"),
        ],
        "",
    ));
    let bytes = &out.bytes;
    assert_eq!(value_at(bytes, 1, "A1").as_deref(), Some("0"));
    assert_eq!(value_at(bytes, 1, "A2").as_deref(), Some("1"));
    assert_eq!(value_at(bytes, 1, "A3").as_deref(), Some("63.5"));
    assert_eq!(value_at(bytes, 1, "A4").as_deref(), Some("3"));
    assert_eq!(value_at(bytes, 1, "A5").as_deref(), Some("6"));
    assert_eq!(value_at(bytes, 1, "A6").as_deref(), Some("#REF!"));
    assert_eq!(value_at(bytes, 1, "A7").as_deref(), Some("3"));
    assert_eq!(value_at(bytes, 1, "A8").as_deref(), Some("3"));
    assert_eq!(value_at(bytes, 1, "A9").as_deref(), Some("1"));
}

#[test]
fn calculated_external_range_intersects_the_formula_row() {
    // Excel calculated these (calc chain): a range in a value position
    // reduces to the cell in the formula's row or column.
    let cells = vec![
        formula("E2", "[1]Data!$B$1:$B$3*2"),
        formula("E3", "[1]Data!$B$1:$B$3"),
        formula("E5", "[1]Data!$B$1:$B$3"),
        formula("B6", "[1]Data!$A$1:$C$1"),
    ];
    let bytes = with_chain(&workbook(cells, ""), &["E2", "E3", "E5", "B6"]);
    let out = recalc(&bytes);
    assert_eq!(value_at(&out.bytes, 1, "E2").as_deref(), Some("42"));
    assert_eq!(value_at(&out.bytes, 1, "E3").as_deref(), Some("32.5"));
    assert_eq!(value_at(&out.bytes, 1, "E5").as_deref(), Some("#VALUE!"));
    assert_eq!(value_at(&out.bytes, 1, "B6").as_deref(), Some("10"));
    assert!(!unpack(&out.bytes).contains_key("xl/metadata.xml"));
}

#[test]
fn link_parts_relationships_and_content_types_are_preserved() {
    let bytes = workbook(vec![formula("A1", "[1]Data!B2")], "");
    let out = recalc(&bytes);
    assert_eq!(out.worksheet_parts_changed, 1);
    let (before, after) = (unpack(&bytes), unpack(&out.bytes));
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    for (name, part) in &before {
        if name != SHEET {
            assert_eq!(&after[name], part, "{name} changed");
        }
    }
    // Raw ZIP entries of the untouched members are identical, too.
    let raw = |bytes: &[u8], name: &str| {
        let mut z = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
        let f = z.by_name(name).unwrap();
        (f.crc32(), f.compressed_size(), f.size())
    };
    for name in [
        "xl/externalLinks/externalLink1.xml",
        "xl/externalLinks/_rels/externalLink1.xml.rels",
        "[Content_Types].xml",
        WB_RELS,
        "xl/workbook.xml",
    ] {
        assert_eq!(raw(&bytes, name), raw(&out.bytes, name), "{name}");
    }
}

#[test]
fn workbook_without_external_reads_reports_no_links_used() {
    let out = recalc(&workbook(vec![formula("A1", "1+1")], ""));
    assert_eq!(value_at(&out.bytes, 1, "A1").as_deref(), Some("2"));
    assert_eq!(out.external_links_used, 0);
}

#[test]
fn dde_and_ole_links_are_refused() {
    for (element, kind) in [("ddeLink", "DDE"), ("oleLink", "OLE")] {
        let link = format!(
            "<externalLink xmlns=\"{MAIN}\"><{element} xmlns:r=\"{OFFICE}\" ddeService=\"x\" ddeTopic=\"y\"/></externalLink>"
        );
        let bytes = with_links(
            &book(&[Ws::new("Sheet1", vec![formula("A1", "1")])], ""),
            &[link],
        );
        refuses(&bytes, &format!("{kind} external link"));
    }
}

#[test]
fn references_without_cached_values_are_refused() {
    let no_cache = with_links(
        &book(&[Ws::new("Sheet1", vec![formula("A1", "[1]Data!A1")])], ""),
        &[book_link(&["Data"], None)],
    );
    let context = refuses(&no_cache, "external link without cached values");
    assert_eq!(context, "Sheet1!A1: [1]Data!A1");
    // A used name reading an uncached link refuses as well.
    let used_name = with_links(
        &book(
            &[Ws::new("Sheet1", vec![formula("A1", "Old+1")])],
            "<definedName name=\"Old\">[1]Data!$A$1</definedName>",
        ),
        &[book_link(&["Data"], None)],
    );
    let context = refuses(&used_name, "external link without cached values");
    assert_eq!(context, "defined name Old: [1]Data!$A$1");
    let unlisted = workbook(vec![formula("A1", "[1]Other!A1")], "");
    refuses(&unlisted, "external sheet not listed in the link cache");
    let no_sheet_data = with_links(
        &book(
            &[Ws::new("Sheet1", vec![formula("A1", "[1]Second!A1")])],
            "",
        ),
        &[book_link(
            &["First", "Second"],
            Some(&[(0, false, &[("A1", "", "1")])]),
        )],
    );
    refuses(&no_sheet_data, "external sheet without cached values");
    let undeclared = workbook(vec![formula("A1", "[2]Data!A1")], "");
    refuses(&undeclared, "external reference to an undeclared link");
}

#[test]
fn unmappable_ranges_and_values_are_refused() {
    let whole_column = workbook(vec![formula("A1", "SUM([1]Data!B:B)")], "");
    refuses(
        &whole_column,
        "whole-row or whole-column external reference",
    );
    let failed = workbook(vec![formula("A1", "SUM([1]Failed!A1:A2)")], "");
    refuses(
        &failed,
        "external range over uncached cells of a sheet whose link refresh failed",
    );
    let shared_string = with_links(
        &book(&[Ws::new("Sheet1", vec![formula("A1", "[1]Data!A1")])], ""),
        &[book_link(
            &["Data"],
            Some(&[(0, false, &[("A1", "s", "0")])]),
        )],
    );
    refuses(&shared_string, "unsupported external cached value");
}

#[test]
fn reference_shapes_over_external_references_are_refused() {
    for (formula_text, feature) in [
        ("INDIRECT(B1)", "INDIRECT that may reach a linked workbook"),
        (
            "INDIRECT(\"[1]Data!A1\")",
            "INDIRECT that may reach a linked workbook",
        ),
        (
            "SUM(OFFSET([1]Data!A1,1,1))",
            "OFFSET over an external reference",
        ),
        ("SUM(OFFSET(Ext,1,1))", "OFFSET over an external reference"),
        ("ROW([1]Data!B2)", "ROW over an external reference"),
        ("[1]!Total", "defined name of a linked workbook"),
        ("'[1]Data'!Total", "defined name of a linked workbook"),
        (
            "SUM([1]Data!A1:INDEX([1]Data!B1:B3,2))",
            "reference operator over an external reference",
        ),
        (
            "SUMIF([1]Data!A1:A3,\"a\",[1]Data!B1)",
            "SUMIF resizing an external sum range",
        ),
    ] {
        let bytes = workbook(
            vec![text("B1", "Sheet1!B2"), formula("A1", formula_text)],
            "<definedName name=\"Ext\">[1]Data!$A$1</definedName>",
        );
        refuses(&bytes, feature);
    }
    // A literal INDIRECT that cannot name another workbook is admitted.
    let out = recalc(&workbook(
        vec![num("B2", 5.0), formula("A1", "INDIRECT(\"B2\")+[1]Data!B1")],
        "",
    ));
    assert_eq!(value_at(&out.bytes, 1, "A1").as_deref(), Some("15"));
}

#[test]
fn rich_value_data_keeps_its_own_refusal() {
    let mut parts = unpack(&book(&[Ws::new("Sheet1", vec![formula("A1", "1")])], ""));
    parts.insert(
        "xl/richData/rdrichvalue.xml".into(),
        format!("<rvData xmlns=\"{MAIN}\"/>"),
    );
    refuses(&pack(&parts), "rich value data");
}

#[test]
fn unreferenced_link_parts_and_relationships_are_refused() {
    let bytes = workbook(vec![formula("A1", "[1]Data!B1")], "");
    // A link part the workbook does not reference.
    let mut parts = unpack(&bytes);
    parts.insert(
        "xl/externalLinks/externalLink9.xml".into(),
        book_link(&["Data"], None),
    );
    let parts = edit(
        parts,
        TYPES,
        "</Types>",
        &format!(
            "<Override PartName=\"/xl/externalLinks/externalLink9.xml\" ContentType=\"{LINK_TYPE}\"/></Types>"
        ),
    );
    refuses(&pack(&parts), "unreferenced external link part");
    // A link relationship without an `externalReference`.
    let parts = edit(
        unpack(&bytes),
        "xl/workbook.xml",
        "<externalReference r:id=\"rIdLink1\"/>",
        "",
    );
    refuses(&pack(&parts), "unreferenced external link relationship");
}

#[test]
fn names_that_compute_arrays_or_intersect_external_ranges_are_refused() {
    let array = workbook(
        vec![formula("A1", "SUM(Doubled)")],
        "<definedName name=\"Doubled\">[1]Data!$B$1:$B$3*2</definedName>",
    );
    refuses(
        &array,
        "defined name computing an array from an external range",
    );
    // A calculated formula reading a range name in a value position.
    let intersect = with_chain(
        &workbook(
            vec![formula("E2", "Col*2")],
            "<definedName name=\"Col\">[1]Data!$B$1:$B$3</definedName>",
        ),
        &["E2"],
    );
    refuses(
        &intersect,
        "implicit intersection of a defined name holding an external range",
    );
    // The same name as a lookup table is read as a reference.
    let out = recalc(&with_chain(
        &workbook(
            vec![formula("E2", "SUM(Col)*2")],
            "<definedName name=\"Col\">[1]Data!$B$1:$B$3</definedName>",
        ),
        &["E2"],
    ));
    assert_eq!(value_at(&out.bytes, 1, "E2").as_deref(), Some("127"));
}

#[test]
fn external_range_area_is_bounded() {
    let bytes = workbook(vec![formula("A1", "SUM([1]Data!A1:J100)")], "");
    let mut options = XlsxRecalculateOptions::default();
    options.limits.max_cells = 200;
    match recalculate_xlsx_bytes(&bytes, options) {
        Err(IoError::Unsupported { feature, .. }) => {
            assert_eq!(feature, "external range cell limit")
        }
        other => panic!("expected the area bound, got {other:?}"),
    }
}

fn with_policy(policy: ExternalLinkPolicy) -> XlsxRecalculateOptions {
    XlsxRecalculateOptions {
        external_links: policy,
        ..Default::default()
    }
}

#[test]
fn default_policy_reads_cached_link_values() {
    assert_eq!(ExternalLinkPolicy::default(), ExternalLinkPolicy::Cached);
    assert_eq!(
        XlsxRecalculateOptions::default().external_links,
        ExternalLinkPolicy::Cached
    );
    let bytes = workbook(vec![formula("A1", "[1]Data!B2*2")], "");
    let default = recalculate_xlsx_bytes(&bytes, XlsxRecalculateOptions::default()).unwrap();
    let cached = recalculate_xlsx_bytes(&bytes, with_policy(ExternalLinkPolicy::Cached)).unwrap();
    assert_eq!(cached.bytes, default.bytes);
    assert_eq!(cached.external_links_used, 1);
    assert_eq!(value_at(&cached.bytes, 1, "A1").as_deref(), Some("42"));
}

#[test]
fn refuse_policy_refuses_when_a_formula_reads_a_link_value() {
    let refused =
        |bytes: &[u8]| match recalculate_xlsx_bytes(bytes, with_policy(ExternalLinkPolicy::Refuse))
        {
            Err(IoError::Unsupported { feature, context }) => {
                assert_eq!(feature, "external link values");
                context
            }
            other => panic!("expected a refusal, got {other:?}"),
        };
    let context = refused(&workbook(
        vec![formula("A1", "1+1"), formula("A2", "[1]Data!B2*2")],
        "",
    ));
    assert_eq!(
        context,
        "Sheet1!A2: [1]Data!B2 (recalculating would use the values cached in the workbook for 1 external link; links are never refreshed)"
    );
    // Through a defined name a formula uses, counting every link read.
    let two_links = with_links(
        &book(
            &[Ws::new("Sheet1", vec![formula("A1", "Rate+[2]Data!B1")])],
            "<definedName name=\"Rate\">[1]Data!$B$2</definedName>",
        ),
        &[data_link(), data_link()],
    );
    let context = refused(&two_links);
    assert!(
        context.ends_with("for 2 external links; links are never refreshed)"),
        "{context}"
    );
}

#[test]
fn refuse_policy_recalculates_when_nothing_reads_a_link() {
    for bytes in [
        workbook(vec![formula("A1", "1+1")], ""),
        // Names of an uncached link that no formula uses.
        with_links(
            &book(
                &[Ws::new("Sheet1", vec![formula("A1", "1+1")])],
                "<definedName name=\"Old\">[1]Gone!$A$1</definedName>",
            ),
            &[book_link(&["Gone"], None)],
        ),
    ] {
        let refuse =
            recalculate_xlsx_bytes(&bytes, with_policy(ExternalLinkPolicy::Refuse)).unwrap();
        let cached =
            recalculate_xlsx_bytes(&bytes, with_policy(ExternalLinkPolicy::Cached)).unwrap();
        assert_eq!(refuse.external_links_used, 0);
        assert_eq!(refuse.bytes, cached.bytes);
        assert_eq!(value_at(&refuse.bytes, 1, "A1").as_deref(), Some("2"));
    }
}

#[test]
fn refuse_policy_keeps_other_link_refusals() {
    // A read of a usable link before an unservable one: the unservable
    // reference is still the reason, under either policy.
    let bytes = with_links(
        &book(
            &[Ws::new(
                "Sheet1",
                vec![formula("A1", "[1]Data!B1"), formula("A2", "[2]Data!A1")],
            )],
            "",
        ),
        &[data_link(), book_link(&["Data"], None)],
    );
    for policy in [ExternalLinkPolicy::Cached, ExternalLinkPolicy::Refuse] {
        match recalculate_xlsx_bytes(&bytes, with_policy(policy)) {
            Err(IoError::Unsupported { feature, context }) => {
                assert_eq!(feature, "external link without cached values");
                assert_eq!(context, "Sheet1!A2: [2]Data!A1");
            }
            other => panic!("{policy:?}: expected a refusal, got {other:?}"),
        }
    }
}
