//! FORM211-C/D: validated output projection and worksheet geometry through
//! the private switch. Every output is reopened with independent parsers:
//! Calamine for cached data and a test-local quick-xml reader for raw cell,
//! row and attribute assertions (Calamine 0.36 cannot read a `#SPILL!`
//! cache, so blocked outputs are checked through the XML reader).
use super::super::{
    SpillSupport, XlsxRecalculateOptions, XlsxRecalculateResult, admit_source, apply_patches,
    evaluate, geometry::BindingRequest, ingest_source, plan_spill_publication, publish,
    recalculate_xlsx_bytes, recalculate_xlsx_bytes_with,
};
use super::dynamic_admission::{
    ON, SHEET, TWO_ANCHOR_ROWS, TYPES, WB_RELS, edit, pack, package, producer, producer_wide,
    refused,
};
use crate::IoError;
use calamine::{Data, Reader, Xlsx};
use formualizer_common::ExcelErrorKind;
use formualizer_eval::engine::{CancelToken, FormulaParsePolicy};
use quick_xml::events::Event;
use std::collections::{BTreeMap, HashSet};
use std::io::{Cursor, Read};

type Parts = BTreeMap<String, String>;

fn run(bytes: &[u8]) -> XlsxRecalculateResult {
    recalculate_xlsx_bytes_with(bytes, Default::default(), ON).expect("published")
}
fn unpack(bytes: &[u8]) -> Parts {
    let mut z = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut out = Parts::new();
    for i in 0..z.len() {
        let mut f = z.by_index(i).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        out.insert(f.name().to_owned(), s);
    }
    out
}
fn sheet_xml(bytes: &[u8]) -> String {
    unpack(bytes).remove(SHEET).expect("worksheet")
}
/// Producer package with `B1` set to `n`.
fn sized(p: Parts, from: u32, n: u32) -> Parts {
    edit(
        p,
        SHEET,
        &format!("<c r=\"B1\"><v>{from}</v></c>"),
        &format!("<c r=\"B1\"><v>{n}</v></c>"),
    )
}

/// One independently parsed cell.
#[derive(Debug, Default, Clone, PartialEq)]
struct XCell {
    attrs: BTreeMap<String, String>,
    f: Option<BTreeMap<String, String>>,
    formula: String,
    v: Option<String>,
    inline: bool,
}
#[derive(Debug, Default)]
struct XSheet {
    dimension: Option<String>,
    /// `(r, attributes, cell refs in order)`.
    rows: Vec<(u32, BTreeMap<String, String>, Vec<String>)>,
    cells: BTreeMap<String, XCell>,
}
impl XSheet {
    fn cell(&self, r: &str) -> &XCell {
        self.cells.get(r).unwrap_or_else(|| panic!("no cell {r}"))
    }
    fn row_numbers(&self) -> Vec<u32> {
        self.rows.iter().map(|r| r.0).collect()
    }
    fn row(&self, n: u32) -> &(u32, BTreeMap<String, String>, Vec<String>) {
        self.rows.iter().find(|r| r.0 == n).expect("row")
    }
}
fn attributes(e: &quick_xml::events::BytesStart<'_>) -> BTreeMap<String, String> {
    e.attributes()
        .map(|a| {
            let a = a.unwrap();
            (
                String::from_utf8(a.key.local_name().as_ref().to_vec()).unwrap(),
                String::from_utf8(a.value.to_vec()).unwrap(),
            )
        })
        .collect()
}
fn parse_sheet(xml: &str) -> XSheet {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut out = XSheet::default();
    let mut cell: Option<(String, XCell)> = None;
    let mut text: Option<&'static str> = None;
    loop {
        let event = reader.read_event().unwrap();
        let empty = matches!(event, Event::Empty(_));
        match event {
            Event::Start(e) | Event::Empty(e) => {
                let attrs = attributes(&e);
                match e.local_name().as_ref() {
                    b"dimension" => out.dimension = attrs.get("ref").cloned(),
                    b"row" => out
                        .rows
                        .push((attrs["r"].parse().unwrap(), attrs, Vec::new())),
                    b"c" => {
                        let r = attrs["r"].clone();
                        out.rows.last_mut().unwrap().2.push(r.clone());
                        let c = XCell {
                            attrs,
                            ..Default::default()
                        };
                        if empty {
                            out.cells.insert(r, c);
                        } else {
                            cell = Some((r, c));
                        }
                    }
                    b"f" => {
                        cell.as_mut().unwrap().1.f = Some(attrs);
                        text = (!empty).then_some("f");
                    }
                    b"v" => {
                        cell.as_mut().unwrap().1.v = Some(String::new());
                        text = (!empty).then_some("v");
                    }
                    b"is" => cell.as_mut().unwrap().1.inline = true,
                    _ => {}
                }
            }
            Event::Text(t) => {
                let t = String::from_utf8(t.to_vec()).unwrap();
                if let Some((_, c)) = cell.as_mut() {
                    match text {
                        Some("f") => c.formula.push_str(&t),
                        Some("v") => c.v.as_mut().unwrap().push_str(&t),
                        _ => {}
                    }
                }
            }
            Event::End(e) => match e.local_name().as_ref() {
                b"c" => {
                    let (r, c) = cell.take().unwrap();
                    out.cells.insert(r, c);
                }
                b"f" | b"v" => text = None,
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
    }
    out
}
fn data(bytes: &[u8], cell: &str) -> Data {
    let (r, c, _, _) = formualizer_common::coord::parse_a1_1based(cell).unwrap();
    let mut x = Xlsx::new(Cursor::new(bytes)).unwrap();
    x.worksheet_range_at(0)
        .unwrap()
        .unwrap()
        .get_value((r - 1, c - 1))
        .cloned()
        .unwrap_or(Data::Empty)
}
#[track_caller]
fn assert_numbers(bytes: &[u8], expected: &[(&str, f64)]) {
    let x = parse_sheet(&sheet_xml(bytes));
    for (cell, n) in expected {
        assert_eq!(data(bytes, cell), Data::Float(*n), "calamine {cell}");
        let c = x.cell(cell);
        assert_eq!(c.v.as_deref(), Some(n.to_string().as_str()), "xml {cell}");
        assert!(c.attrs.get("t").is_none_or(|t| t == "n"), "xml {cell} type");
    }
}
/// An obsolete generated cell is a shell: no cache payload or type, other
/// attributes (style) kept.
#[track_caller]
fn assert_cleared(x: &XSheet, cell: &str, style: Option<&str>) {
    let c = x.cell(cell);
    assert_eq!(c.v, None, "{cell}");
    assert!(!c.inline && c.f.is_none(), "{cell}");
    assert_eq!(c.attrs.get("t"), None, "{cell}");
    assert_eq!(c.attrs.get("s").map(String::as_str), style, "{cell}");
}
#[track_caller]
fn assert_anchor(x: &XSheet, cell: &str, reference: &str, cm: Option<&str>) {
    let c = x.cell(cell);
    let f = c.f.as_ref().expect("anchor formula");
    assert_eq!(f.get("t").map(String::as_str), Some("array"), "{cell}");
    assert_eq!(f.get("ref").map(String::as_str), Some(reference), "{cell}");
    assert_eq!(c.attrs.get("cm").map(String::as_str), cm, "{cell}");
}
#[track_caller]
fn assert_error(x: &XSheet, cell: &str, token: &str) {
    let c = x.cell(cell);
    assert_eq!(c.attrs.get("t").map(String::as_str), Some("e"), "{cell}");
    assert_eq!(c.v.as_deref(), Some(token), "{cell}");
}
/// Every part except the worksheet is byte-identical to the source.
#[track_caller]
fn assert_other_parts_unchanged(source: &[u8], out: &[u8]) {
    let mut a = unpack(source);
    let mut b = unpack(out);
    a.remove(SHEET);
    b.remove(SHEET);
    assert_eq!(a, b);
}
/// A repeated recalc of a successful output is a byte-identical no-op.
#[track_caller]
fn assert_rerun_is_noop(out: &XlsxRecalculateResult) {
    let again = run(&out.bytes);
    assert_eq!(again.bytes, out.bytes, "repeated recalc changed bytes");
    assert_eq!(again.cache_cells_changed, 0);
    assert_eq!(again.worksheet_parts_changed, 0);
    assert_eq!(again.formula_cells, out.formula_cells);
}
#[track_caller]
fn assert_counts(out: &XlsxRecalculateResult, formulas: usize, changed: usize) {
    assert_eq!(out.formula_cells, formulas, "formula_cells");
    assert_eq!(out.summary.evaluated, formulas, "summary.evaluated");
    assert_eq!(out.cache_cells_changed, changed, "cache_cells_changed");
    assert_eq!(out.worksheet_parts_changed, 1, "worksheet_parts_changed");
}

#[test]
fn existing_spill_grows_3_to_5_with_rows_inserted_in_order() {
    let source = pack(&producer());
    let out = run(&source);
    assert_numbers(
        &out.bytes,
        &[
            ("C2", 1.0),
            ("C3", 2.0),
            ("C4", 3.0),
            ("C5", 4.0),
            ("C6", 5.0),
            ("C9", 15.0),
            ("C10", 15.0),
        ],
    );
    let xml = sheet_xml(&out.bytes);
    let x = parse_sheet(&xml);
    assert_anchor(&x, "C2", "C2:C6", Some("1"));
    assert_anchor(&x, "C10", "C10", Some("1"));
    assert_eq!(x.row_numbers(), vec![1, 2, 3, 4, 5, 6, 9, 10, 11]);
    assert!(
        xml.contains("<c r=\"C4\" s=\"1\"><v>3</v></c></row><row r=\"5\"><c r=\"C5\"><v>4</v></c></row><row r=\"6\"><c r=\"C6\"><v>5</v></c></row><row r=\"9\""),
        "{xml}"
    );
    assert_eq!(x.dimension.as_deref(), Some("A1:C11"), "still valid");
    // C5 and C6 inserted, C9 and C10 replaced; the anchor's own 1 is kept.
    assert_counts(&out, 3, 4);
    assert_other_parts_unchanged(&source, &out.bytes);
    assert_rerun_is_noop(&out);
}

fn shrink_rows(b1: u32) -> String {
    format!(
        "<row r=\"1\" spans=\"1:3\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\"><v>{b1}</v></c></row>{}",
        concat!(
            "<row r=\"2\" spans=\"1:3\"><c r=\"C2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C2:C6\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
            "<row r=\"3\" spans=\"1:3\"><c r=\"C3\" s=\"1\"><v>2</v></c></row>",
            "<row r=\"4\" spans=\"1:3\"><c r=\"C4\" s=\"1\"><v>3</v></c></row>",
            "<row r=\"5\" spans=\"1:3\"><c r=\"C5\" s=\"1\"><v>4</v></c></row>",
            "<row r=\"6\" spans=\"1:3\"><c r=\"C6\" s=\"1\"><v>5</v></c></row>",
            "<row r=\"9\" spans=\"1:3\"><c r=\"C9\"><f>SUM(C2#)</f><v>99</v></c></row>",
            "<row r=\"10\" spans=\"1:3\"><c r=\"C10\" cm=\"1\"><f t=\"array\" ref=\"C10\">SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c></row>",
            "<row r=\"11\" spans=\"1:3\"><c r=\"A11\" t=\"s\"><v>1</v></c></row>",
        )
    )
}
/// The shrink source with a comment on C4.
fn shrink(b1: u32) -> Parts {
    edit(
        package("A1:C11", &shrink_rows(b1), ""),
        "xl/comments1.xml",
        "<commentList/>",
        "<commentList><comment ref=\"C4\" authorId=\"0\"><text><t>keep me</t></text></comment></commentList>",
    )
}

#[test]
fn existing_spill_shrinks_5_to_2_keeping_styled_and_commented_shells() {
    let source = pack(&shrink(2));
    let out = run(&source);
    assert_numbers(
        &out.bytes,
        &[("C2", 1.0), ("C3", 2.0), ("C9", 3.0), ("C10", 3.0)],
    );
    let xml = sheet_xml(&out.bytes);
    let x = parse_sheet(&xml);
    assert_anchor(&x, "C2", "C2:C3", Some("1"));
    for cell in ["C4", "C5", "C6"] {
        assert_cleared(&x, cell, Some("1"));
        assert_eq!(data(&out.bytes, cell), Data::Empty, "{cell}");
    }
    assert!(xml.contains("<c r=\"C4\" s=\"1\"></c>"), "{xml}");
    assert_eq!(x.row_numbers(), vec![1, 2, 3, 4, 5, 6, 9, 10, 11]);
    // Comment, VML and relationships are untouched.
    assert_other_parts_unchanged(&source, &out.bytes);
    assert!(unpack(&out.bytes)["xl/comments1.xml"].contains("ref=\"C4\""));
    // Three cleared children plus C9 and C10.
    assert_counts(&out, 3, 5);
    assert_rerun_is_noop(&out);
}

#[test]
fn existing_spill_stable_3_to_3_only_refreshes_dependents() {
    let source = pack(&sized(producer(), 5, 3));
    let out = run(&source);
    assert_numbers(
        &out.bytes,
        &[
            ("C2", 1.0),
            ("C3", 2.0),
            ("C4", 3.0),
            ("C9", 6.0),
            ("C10", 6.0),
        ],
    );
    let x = parse_sheet(&sheet_xml(&out.bytes));
    assert_anchor(&x, "C2", "C2:C4", Some("1"));
    assert_eq!(x.row_numbers(), vec![1, 2, 3, 4, 9, 10, 11]);
    assert_counts(&out, 3, 2);
    assert_rerun_is_noop(&out);
}

#[test]
fn existing_spill_collapses_3_to_1() {
    let source = pack(&sized(producer(), 5, 1));
    let out = run(&source);
    assert_numbers(&out.bytes, &[("C2", 1.0), ("C9", 1.0), ("C10", 1.0)]);
    let x = parse_sheet(&sheet_xml(&out.bytes));
    // ref is the anchor cell; the binding is kept.
    assert_anchor(&x, "C2", "C2", Some("1"));
    assert_cleared(&x, "C3", Some("1"));
    assert_cleared(&x, "C4", Some("1"));
    assert_counts(&out, 3, 4);
    assert_other_parts_unchanged(&source, &out.bytes);
    assert_rerun_is_noop(&out);
}

/// `producer()` with a genuine input at C6, outside the prior footprint.
fn obstructed(b1: u32) -> Parts {
    sized(
        edit(
            producer(),
            SHEET,
            "<row r=\"9\"",
            "<row r=\"6\" spans=\"1:3\"><c r=\"C6\"><v>77</v></c></row><row r=\"9\"",
        ),
        5,
        b1,
    )
}

#[test]
fn blocked_spill_publishes_a_typed_spill_error_and_no_children() {
    let source = pack(&obstructed(5));
    let out = run(&source);
    let x = parse_sheet(&sheet_xml(&out.bytes));
    assert_error(&x, "C2", "#SPILL!");
    // A cold reopen must not reclaim the stale cells: ref is the anchor.
    assert_anchor(&x, "C2", "C2", Some("1"));
    assert_cleared(&x, "C3", Some("1"));
    assert_cleared(&x, "C4", Some("1"));
    assert_eq!(x.cell("C6").v.as_deref(), Some("77"), "genuine input kept");
    // Readers of a blocked anchor's spill are #REF! (engine semantics).
    assert_error(&x, "C9", "#REF!");
    assert_error(&x, "C10", "#REF!");
    assert_eq!(out.summary.errors, 3);
    assert_eq!(out.summary.error_summary["#SPILL!"].count, 1);
    assert_eq!(out.summary.error_summary["#REF!"].count, 2);
    // C2, C3, C4 (cleared), C9, C10.
    assert_counts(&out, 3, 5);
    assert_rerun_is_noop(&out);
}

#[test]
fn erroring_anchor_publishes_its_error_and_no_children() {
    let source = pack(&sized(producer(), 5, 0));
    let out = run(&source);
    let x = parse_sheet(&sheet_xml(&out.bytes));
    let c2 = x.cell("C2");
    assert_eq!(c2.attrs.get("t").map(String::as_str), Some("e"));
    let token = c2.v.clone().unwrap();
    assert_ne!(token, "#SPILL!");
    assert_anchor(&x, "C2", "C2", Some("1"));
    assert_cleared(&x, "C3", Some("1"));
    assert_cleared(&x, "C4", Some("1"));
    assert_counts(&out, 3, 5);
    assert_rerun_is_noop(&out);
}

#[test]
fn re_expansion_after_a_published_blocked_spill() {
    let blocked = run(&pack(&obstructed(5))).bytes;
    // Change the size input by a source-XML literal patch of the output.
    let second = edit(
        unpack(&blocked),
        SHEET,
        "<c r=\"B1\"><v>5</v></c>",
        "<c r=\"B1\"><v>3</v></c>",
    );
    let source = pack(&second);
    let out = run(&source);
    assert_numbers(
        &out.bytes,
        &[
            ("C2", 1.0),
            ("C3", 2.0),
            ("C4", 3.0),
            ("C6", 77.0),
            ("C9", 6.0),
            ("C10", 6.0),
        ],
    );
    let xml = sheet_xml(&out.bytes);
    let x = parse_sheet(&xml);
    assert_anchor(&x, "C2", "C2:C4", Some("1"));
    // The cleared shells receive the new member caches; style is kept.
    assert!(xml.contains("<c r=\"C3\" s=\"1\"><v>2</v></c>"), "{xml}");
    assert_eq!(x.cell("C4").attrs.get("s").map(String::as_str), Some("1"));
    // C2, C3, C4, C9, C10.
    assert_counts(&out, 3, 5);
    assert_rerun_is_noop(&out);
}

#[test]
fn two_anchors_sharing_metadata_grow_together() {
    let source = pack(&package("A1:F11", TWO_ANCHOR_ROWS, ""));
    let out = run(&source);
    let mut expected = Vec::new();
    for col in ["C", "F"] {
        for (i, row) in (2..=5).enumerate() {
            expected.push((format!("{col}{row}"), i as f64 + 1.0));
        }
    }
    expected.push(("C9".into(), 10.0));
    expected.push(("C10".into(), 10.0));
    let expected: Vec<_> = expected.iter().map(|(c, n)| (c.as_str(), *n)).collect();
    assert_numbers(&out.bytes, &expected);
    let xml = sheet_xml(&out.bytes);
    let x = parse_sheet(&xml);
    assert_anchor(&x, "C2", "C2:C5", Some("1"));
    assert_anchor(&x, "F2", "F2:F5", Some("1"));
    assert!(
        xml.contains(
            "<row r=\"5\"><c r=\"C5\"><v>4</v></c><c r=\"F5\"><v>4</v></c></row><row r=\"9\""
        ),
        "{xml}"
    );
    // C5, F5, C9, C10.
    assert_counts(&out, 4, 4);
    assert_rerun_is_noop(&out);
}

#[test]
fn sparse_extent_fills_shells_inserts_cells_in_order_and_widens_spans() {
    // `SEQUENCE($B$1,2)` over a prior footprint C2:D5 with a styled shell
    // D3, stale C4, a self-closing row 5 and missing C3/D2/D4/C5/D5.
    let rows = concat!(
        "<row r=\"1\"><c r=\"B1\"><v>4</v></c></row>",
        "<row r=\"2\"><c r=\"C2\" cm=\"1\"><f t=\"array\" ref=\"C2:D5\">_xlfn.SEQUENCE($B$1,2)</f><v>1</v></c></row>",
        "<row r=\"3\"><c r=\"D3\" s=\"1\"/></row>",
        "<row r=\"4\" spans=\"3:3\"><c r=\"C4\"><v>5</v></c></row>",
        "<row r=\"5\"/>",
        "<row r=\"6\"><c r=\"D6\"><v>9</v></c></row>",
        "<row r=\"7\"><c r=\"B7\"><f>SUM(C2#)</f></c></row>",
    );
    let tail = "<mergeCells count=\"1\"><mergeCell ref=\"E7:F8\"/></mergeCells>";
    let source = pack(&package("B1:F8", rows, tail));
    let out = run(&source);
    let xml = sheet_xml(&out.bytes);
    let expected = concat!(
        "<sheetData><row r=\"1\"><c r=\"B1\"><v>4</v></c></row>",
        "<row r=\"2\"><c r=\"C2\" cm=\"1\"><f t=\"array\" ref=\"C2:D5\">_xlfn.SEQUENCE($B$1,2)</f><v>1</v></c><c r=\"D2\"><v>2</v></c></row>",
        "<row r=\"3\"><c r=\"C3\"><v>3</v></c><c r=\"D3\" s=\"1\"><v>4</v></c></row>",
        "<row r=\"4\" spans=\"3:4\"><c r=\"C4\"><v>5</v></c><c r=\"D4\"><v>6</v></c></row>",
        "<row r=\"5\"><c r=\"C5\"><v>7</v></c><c r=\"D5\"><v>8</v></c></row>",
        "<row r=\"6\"><c r=\"D6\"><v>9</v></c></row>",
        "<row r=\"7\"><c r=\"B7\"><f>SUM(C2#)</f><v>36</v></c></row></sheetData>",
    );
    assert!(xml.contains(expected), "{xml}");
    assert_numbers(
        &out.bytes,
        &[
            ("C2", 1.0),
            ("D2", 2.0),
            ("C3", 3.0),
            ("D3", 4.0),
            ("C4", 5.0),
            ("D4", 6.0),
            ("C5", 7.0),
            ("D5", 8.0),
            ("D6", 9.0),
            ("B7", 36.0),
        ],
    );
    // D2, C3, D3, D4, C5, D5 and B7; C4 already held 5.
    assert_counts(&out, 2, 7);
    assert_rerun_is_noop(&out);
}

/// Anchor at C2 with readers in column D, so a long spill stays unblocked.
fn column_d_readers(b1: u32) -> Parts {
    let rows = format!(
        "<row r=\"1\" spans=\"1:4\"><c r=\"B1\"><v>{b1}</v></c></row>{}",
        concat!(
            "<row r=\"2\" spans=\"1:4\"><c r=\"C2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
            "<row r=\"3\" spans=\"1:4\"><c r=\"C3\" s=\"1\"><v>2</v></c></row>",
            "<row r=\"4\" spans=\"1:4\"><c r=\"C4\" s=\"1\"><v>3</v></c></row>",
            "<row r=\"9\" spans=\"1:4\"><c r=\"D9\"><f>SUM(C2#)</f><v>0</v></c></row>",
            "<row r=\"10\" spans=\"1:4\"><c r=\"D10\"><f>SUM(_xlfn.ANCHORARRAY(C2))</f><v>0</v></c></row>",
            "<row r=\"11\" spans=\"1:4\"><c r=\"A11\" t=\"s\"><v>1</v></c></row>",
        )
    );
    package("A1:D11", &rows, "")
}

#[test]
fn dimension_grows_and_rows_are_inserted_before_between_and_after() {
    let source = pack(&column_d_readers(12));
    let out = run(&source);
    let mut expected: Vec<(String, f64)> = (2..=13)
        .map(|r| (format!("C{r}"), f64::from(r - 1)))
        .collect();
    expected.push(("D9".into(), 78.0));
    expected.push(("D10".into(), 78.0));
    let expected: Vec<_> = expected.iter().map(|(c, n)| (c.as_str(), *n)).collect();
    assert_numbers(&out.bytes, &expected);
    let xml = sheet_xml(&out.bytes);
    let x = parse_sheet(&xml);
    assert_eq!(x.dimension.as_deref(), Some("A1:D13"));
    assert_anchor(&x, "C2", "C2:C13", Some("1"));
    assert_eq!(x.row_numbers(), (1..=13).collect::<Vec<_>>());
    assert_eq!(x.row(9).2, vec!["C9", "D9"]);
    assert_eq!(x.row(11).2, vec!["A11", "C11"]);
    // Existing spans already cover column C; inserted rows carry none.
    assert_eq!(x.row(9).1.get("spans").map(String::as_str), Some("1:4"));
    assert_eq!(x.row(12).1.get("spans"), None);
    assert!(xml.ends_with("</row><row r=\"12\"><c r=\"C12\"><v>11</v></c></row><row r=\"13\"><c r=\"C13\"><v>12</v></c></row></sheetData><pageMargins left=\"0.7\" right=\"0.7\" top=\"0.75\" bottom=\"0.75\" header=\"0.3\" footer=\"0.3\"/><legacyDrawing r:id=\"rId1\"/></worksheet>"), "{xml}");
    // C5..C13 inserted (9), D9 and D10.
    assert_counts(&out, 3, 11);
    assert_rerun_is_noop(&out);
}

#[test]
fn spill_across_a_merge_is_refused_and_a_clear_one_publishes() {
    let merged = |b1| {
        sized(
            edit(
                producer_wide(),
                SHEET,
                "</sheetData>",
                "</sheetData><mergeCells count=\"1\"><mergeCell ref=\"C5:D5\"/></mergeCells>",
            ),
            5,
            b1,
        )
    };
    refused(
        recalculate_xlsx_bytes_with(&pack(&merged(5)), Default::default(), ON),
        "merged cell range",
    );
    let out = run(&pack(&merged(3)));
    assert_numbers(&out.bytes, &[("C4", 3.0), ("C9", 6.0)]);
    assert_rerun_is_noop(&out);
}

#[test]
fn spill_over_an_unowned_empty_payload_is_refused_not_overwritten() {
    // `<v></v>` outside the prior footprint reads as empty, so the engine
    // spills over it; the writer still refuses to replace a source payload
    // that ownership does not cover.
    let p = edit(
        producer(),
        SHEET,
        "<row r=\"9\"",
        "<row r=\"6\"><c r=\"C6\" s=\"1\"><v></v></c></row><row r=\"9\"",
    );
    refused(
        recalculate_xlsx_bytes_with(&pack(&p), Default::default(), ON),
        "dynamic spill over an unowned source value",
    );
    // A styled empty shell there is not an obstruction and is filled.
    let p = edit(
        producer(),
        SHEET,
        "<row r=\"9\"",
        "<row r=\"6\"><c r=\"C6\" s=\"1\"/></row><row r=\"9\"",
    );
    let out = run(&pack(&p));
    assert!(sheet_xml(&out.bytes).contains("<c r=\"C6\" s=\"1\"><v>5</v></c>"));
    assert_rerun_is_noop(&out);
}

#[test]
fn shared_family_member_multi_cell_spill_is_refused() {
    let p = edit(
        producer_wide(),
        SHEET,
        "<row r=\"9\"",
        "<row r=\"6\"><c r=\"E6\"><f t=\"shared\" ref=\"E6:F6\" si=\"0\">_xlfn.SEQUENCE(2)</f><v>0</v></c><c r=\"F6\"><f t=\"shared\" si=\"0\"/><v>0</v></c></row><row r=\"9\"",
    );
    let p = sized(p, 5, 3);
    refused(
        recalculate_xlsx_bytes_with(&pack(&p), Default::default(), ON),
        "shared formula family member",
    );
}

#[test]
fn generated_cell_budget_is_refused_before_members_are_materialized() {
    let source = pack(&column_d_readers(100));
    let mut options = XlsxRecalculateOptions::default();
    options.limits.max_cells = 60;
    refused(
        recalculate_xlsx_bytes_with(&source, options.clone(), ON),
        "generated spill cell limit",
    );
    // The same workbook within the limit publishes.
    options.limits.max_cells = 500;
    let out = recalculate_xlsx_bytes_with(&source, options, ON).unwrap();
    assert_numbers(&out.bytes, &[("C101", 100.0), ("D9", 5050.0)]);
}

#[test]
fn cancellation_after_evaluation_publishes_nothing() {
    let source = pack(&producer());
    let token = CancelToken::new();
    let options = XlsxRecalculateOptions {
        cancel: Some(token.clone()),
        ..Default::default()
    };
    let admission = admit_source(&source, &options, ON).unwrap();
    let count = admission.formula_count;
    let mut ingested = ingest_source(&source, admission, &options).unwrap();
    evaluate(&mut ingested.engine, &options).unwrap();
    token.cancel();
    match publish(&source, ingested, count, &options, ON) {
        Err(IoError::Engine(e)) => assert_eq!(e.kind, ExcelErrorKind::Cancelled),
        Err(other) => panic!("expected cancellation, got {other:?}"),
        Ok(_) => panic!("published after cancellation"),
    }
}

/// The parent `new-vertical` shape: an ordinary formula that spills.
fn new_vertical() -> Parts {
    let rows = concat!(
        "<row r=\"1\" spans=\"1:3\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\"><v>3</v></c></row>",
        "<row r=\"2\" spans=\"1:3\"><c r=\"C2\" s=\"1\"><f>_xlfn.SEQUENCE($B$1)</f><v>99</v></c></row>",
        "<row r=\"9\" spans=\"1:3\"><c r=\"C9\"><f>SUM(C2#)</f><v>99</v></c></row>",
        "<row r=\"10\" spans=\"1:3\"><c r=\"C10\"><f>SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c></row>",
        "<row r=\"11\" spans=\"1:3\"><c r=\"A11\" t=\"s\"><v>1</v></c></row>",
    );
    let p = package("A1:C11", rows, "");
    let mut p = edit(
        p,
        WB_RELS,
        "<Relationship Id=\"rId5\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata\" Target=\"metadata.xml\"/>",
        "",
    );
    p.remove("xl/metadata.xml");
    edit(
        p,
        TYPES,
        "<Override PartName=\"/xl/metadata.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml\"/>",
        "",
    )
}

#[test]
fn new_ordinary_spill_plan_binds_the_anchor() {
    let source = pack(&new_vertical());
    let options = XlsxRecalculateOptions::default();
    let admission = admit_source(&source, &options, ON).unwrap();
    let mut ingested = ingest_source(&source, admission, &options).unwrap();
    evaluate(&mut ingested.engine, &options).unwrap();
    let coerced: HashSet<_> = ingested
        .engine
        .formula_parse_diagnostics()
        .iter()
        .filter(|d| d.policy == FormulaParsePolicy::CoerceToError)
        .map(|d| (d.sheet.clone(), d.row, d.col))
        .collect();
    let mut requests = Vec::new();
    let planned = plan_spill_publication(
        &ingested.engine,
        &ingested.sheets,
        &ingested.plans,
        &coerced,
        &options,
        &mut |sheet, request| {
            requests.push((sheet.to_owned(), request));
            Ok(7)
        },
    )
    .unwrap();
    assert_eq!(
        requests,
        vec![(
            "Sheet1".to_owned(),
            BindingRequest {
                row: 2,
                col: 3,
                multi_cell: true
            }
        )]
    );
    // C2, C3, C4, C9, C10; the anchor is one formula.
    assert_eq!((planned.changed, planned.summary.evaluated), (5, 3));
    let patches = planned.patches.into_iter().next().unwrap();
    let xml = apply_patches(&ingested.plans[0].data, patches, usize::MAX).unwrap();
    let xml = String::from_utf8(xml).unwrap();
    // t="array"/ref set, the resolver's cm inserted, rows inserted in order.
    let rows = concat!(
        "<row r=\"2\" spans=\"1:3\"><c r=\"C2\" s=\"1\" cm=\"7\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
        "<row r=\"3\"><c r=\"C3\"><v>2</v></c></row><row r=\"4\"><c r=\"C4\"><v>3</v></c></row>",
        "<row r=\"9\" spans=\"1:3\"><c r=\"C9\"><f>SUM(C2#)</f><v>6</v></c></row>",
        "<row r=\"10\" spans=\"1:3\"><c r=\"C10\"><f>SUM(_xlfn.ANCHORARRAY(C2))</f><v>6</v></c></row>",
    );
    assert!(xml.contains(rows), "{xml}");
}

#[test]
fn new_ordinary_spill_adds_metadata_relationship_and_content_type() {
    let source = pack(&new_vertical());
    let out = run(&source);
    let before = unpack(&source);
    let after = unpack(&out.bytes);
    assert!(!before.contains_key("xl/metadata.xml"));
    let metadata = &after["xl/metadata.xml"];
    assert!(
        metadata
            .contains("<cellMetadata count=\"1\"><bk><rc t=\"1\" v=\"0\"/></bk></cellMetadata>")
    );
    assert_eq!(
        after[WB_RELS],
        before[WB_RELS].replace(
            "</Relationships>",
            "<Relationship Id=\"rId5\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata\" Target=\"metadata.xml\"/></Relationships>"
        )
    );
    assert_eq!(
        after[TYPES],
        before[TYPES].replace(
            "</Types>",
            "<Override PartName=\"/xl/metadata.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml\"/></Types>"
        )
    );
    let x = parse_sheet(&sheet_xml(&out.bytes));
    assert_anchor(&x, "C2", "C2:C4", Some("1"));
    assert_numbers(
        &out.bytes,
        &[
            ("C2", 1.0),
            ("C3", 2.0),
            ("C4", 3.0),
            ("C9", 6.0),
            ("C10", 6.0),
        ],
    );
    // Only the worksheet, metadata, relationships and content types differ.
    let mut rest = after.clone();
    for name in [SHEET, WB_RELS, TYPES, "xl/metadata.xml"] {
        rest.remove(name);
    }
    let mut old = before.clone();
    for name in [SHEET, WB_RELS, TYPES] {
        old.remove(name);
    }
    assert_eq!(rest, old);
    assert_counts(&out, 3, 5);
    // The anchor now keeps its binding: a repeated recalc edits nothing.
    assert_rerun_is_noop(&out);
    // The public path still refuses until admission is enabled.
    refused(
        recalculate_xlsx_bytes(&source, Default::default()),
        "materialized multi-cell dynamic spill",
    );
}

#[test]
fn ordinary_scalar_formulas_on_the_enabled_path_match_the_public_writer() {
    let p = new_vertical();
    let p = edit(p, SHEET, "_xlfn.SEQUENCE($B$1)", "$B$1*2");
    let p = edit(p, SHEET, "SUM(C2#)", "C2+1");
    let p = edit(p, SHEET, "SUM(_xlfn.ANCHORARRAY(C2))", "C9+1");
    let bytes = pack(&p);
    let public = recalculate_xlsx_bytes(&bytes, Default::default()).unwrap();
    let private = recalculate_xlsx_bytes_with(&bytes, Default::default(), ON).unwrap();
    assert_eq!(private.bytes, public.bytes);
    assert_eq!(private.cache_cells_changed, public.cache_cells_changed);
    assert_eq!(private.summary.evaluated, public.summary.evaluated);
    assert_numbers(&private.bytes, &[("C2", 6.0), ("C9", 7.0), ("C10", 8.0)]);
}

#[test]
fn disabled_switch_still_refuses_every_spill_case() {
    for p in [
        producer(),
        sized(producer(), 5, 1),
        obstructed(5),
        new_vertical(),
    ] {
        let bytes = pack(&p);
        assert!(
            recalculate_xlsx_bytes_with(&bytes, Default::default(), SpillSupport::default())
                .is_err()
        );
        assert!(recalculate_xlsx_bytes(&bytes, Default::default()).is_err());
    }
}
