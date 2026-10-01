//! Public source-preserving dynamic-array recalculation (FORM211): every
//! case goes through `recalculate_xlsx_bytes` / `recalculate_xlsx_file`.
//! Outputs are reopened with independent readers: Calamine for cached data
//! (it cannot read `#SPILL!` caches, so blocked outputs are checked through
//! XML only), quick-xml for raw cells/rows/attributes and a namespace-aware
//! reader for the dynamic-array metadata chain.
#![cfg(feature = "xlsx-recalc")]
mod support {
    pub mod source_xlsx;
}
use calamine::Data;
use formualizer_eval::engine::CancelToken;
use formualizer_workbook::{
    IoError, XlsxRecalculateOptions, XlsxRecalculateResult, recalculate_xlsx_bytes,
};
use support::source_xlsx::*;

fn run(bytes: &[u8]) -> XlsxRecalculateResult {
    recalculate_xlsx_bytes(bytes, Default::default()).expect("published")
}
#[track_caller]
fn refused<T>(result: Result<T, IoError>, needle: &str) {
    match result {
        Ok(_) => panic!("published; expected refusal containing {needle:?}"),
        Err(IoError::Unsupported { feature, context }) => assert!(
            feature.contains(needle) || context.contains(needle),
            "expected {needle:?}, got {feature:?} / {context:?}"
        ),
        Err(other) => panic!("expected Unsupported({needle:?}), got {other:?}"),
    }
}
/// Producer package with `B1` changed from `from` to `n`.
fn sized(p: Parts, from: u32, n: u32) -> Parts {
    edit(
        p,
        SHEET,
        &format!("<c r=\"B1\"><v>{from}</v></c>"),
        &format!("<c r=\"B1\"><v>{n}</v></c>"),
    )
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
const FIRST: Chain = Chain {
    cm: 1,
    metadata_type: 1,
    future_block: 0,
    collapsed: false,
    relationship: String::new(),
};
#[track_caller]
fn assert_chain(bytes: &[u8], cell: &str, expected: (u32, u32), relationship: &str) {
    let chain = metadata_chain(bytes, cell);
    assert_eq!(
        chain,
        Chain {
            cm: expected.0,
            future_block: expected.1,
            relationship: relationship.to_owned(),
            ..FIRST
        },
        "{cell}"
    );
}

#[test]
fn new_vertical_spill_gets_a_complete_metadata_chain() {
    let source = pack(&new_vertical());
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
    let xml = sheet_xml(&out.bytes);
    let x = parse_sheet(&xml);
    assert_anchor(&x, "C2", "C2:C4", Some("1"));
    assert_eq!(x.cell("C2").formula, "_xlfn.SEQUENCE($B$1)");
    assert!(xml.contains("<c r=\"C2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C2:C4\">"));
    assert_eq!(x.row_numbers(), vec![1, 2, 3, 4, 9, 10, 11]);
    // cm -> cellMetadata -> futureMetadata XLDAPR -> relationship + type.
    // The source relationships use rId1, rId3, rId4: the new ID is rId5.
    assert_chain(&out.bytes, "C2", (1, 0), "rId5");
    // Only the worksheet, metadata, relationships and content types change;
    // the archive comment is kept.
    let (before, after) = (unpack(&source), unpack(&out.bytes));
    let mut rest = after.clone();
    for name in [SHEET, WB_RELS, TYPES, METADATA] {
        rest.remove(name);
    }
    let mut old = before.clone();
    for name in [SHEET, WB_RELS, TYPES] {
        old.remove(name);
    }
    assert_eq!(rest, old);
    assert_eq!(
        after[WB_RELS],
        before[WB_RELS].replace(
            "</Relationships>",
            &format!("<Relationship Id=\"rId5\" Type=\"{SHEET_METADATA_REL}\" Target=\"metadata.xml\"/></Relationships>")
        )
    );
    assert_eq!(
        after[TYPES],
        before[TYPES].replace("</Types>", &format!("{METADATA_OVERRIDE}</Types>"))
    );
    let archive = zip::ZipArchive::new(std::io::Cursor::new(&out.bytes)).unwrap();
    assert_eq!(archive.comment(), ARCHIVE_COMMENT.as_bytes());
    // C2, C3 and C4 written or inserted, C9 and C10 replaced. Worksheets
    // only are counted; the metadata part is not.
    assert_counts(&out, 3, 5);
    assert_rerun_is_noop(&out);
}

#[test]
fn a_second_anchor_added_later_reuses_the_binding_without_renumbering() {
    let first = run(&pack(&new_vertical())).bytes;
    // Author a second ordinary spilling formula in the published output.
    let p = edit(
        unpack(&first),
        SHEET,
        "<c r=\"C2\" s=\"1\" cm=\"1\">",
        "<c r=\"A2\"><f>_xlfn.SEQUENCE(2)</f></c><c r=\"C2\" s=\"1\" cm=\"1\">",
    );
    let source = pack(&p);
    let out = run(&source);
    let x = parse_sheet(&sheet_xml(&out.bytes));
    assert_anchor(&x, "A2", "A2:A3", Some("1"));
    assert_anchor(&x, "C2", "C2:C4", Some("1"));
    assert_numbers(&out.bytes, &[("A2", 1.0), ("A3", 2.0), ("C4", 3.0)]);
    assert_chain(&out.bytes, "A2", (1, 0), "rId5");
    assert_chain(&out.bytes, "C2", (1, 0), "rId5");
    // Metadata, relationships and content types are untouched.
    assert_other_parts_unchanged(&source, &out.bytes);
    assert_counts(&out, 4, 2);
    assert_rerun_is_noop(&out);
}

#[test]
fn a_collapsed_record_is_not_toggled_and_a_new_record_is_appended() {
    // Both anchors share cm 1, a collapsed (1x1) record. C2 now spills.
    let p = edit(
        sized(producer(), 5, 3),
        SHEET,
        "<f t=\"array\" ref=\"C2:C4\">",
        "<f t=\"array\" ref=\"C2\">",
    );
    let p = edit(p, SHEET, "<c r=\"C3\" s=\"1\"><v>2</v></c>", "");
    let p = edit(p, SHEET, "<c r=\"C4\" s=\"1\"><v>3</v></c>", "");
    let mut p = p;
    p.insert(METADATA.into(), metadata_part(&[true]));
    let source = pack(&p);
    let out = run(&source);
    assert_numbers(
        &out.bytes,
        &[("C2", 1.0), ("C3", 2.0), ("C4", 3.0), ("C9", 6.0)],
    );
    let x = parse_sheet(&sheet_xml(&out.bytes));
    // C2 is rebound to the appended record; C10 keeps the shared record.
    assert_anchor(&x, "C2", "C2:C4", Some("2"));
    assert_anchor(&x, "C10", "C10", Some("1"));
    assert_chain(&out.bytes, "C2", (2, 1), "rId5");
    let c10 = metadata_chain(&out.bytes, "C10");
    assert_eq!((c10.cm, c10.future_block, c10.collapsed), (1, 0, true));
    // The edited part keeps every source byte up to its section ends.
    let (before, after) = (unpack(&source), unpack(&out.bytes));
    let old = &before[METADATA];
    let new = &after[METADATA];
    let cut = old.find("</futureMetadata>").unwrap();
    assert_eq!(
        old[..cut].replace("count=\"1\"", "count=\"2\""),
        new[..cut].replace("count=\"1\"", "count=\"2\"")
    );
    assert_eq!(before[WB_RELS], after[WB_RELS]);
    assert_eq!(before[TYPES], after[TYPES]);
    assert_rerun_is_noop(&out);
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
    assert_chain(&out.bytes, "C2", (1, 0), "rId5");
    assert_eq!(x.row_numbers(), vec![1, 2, 3, 4, 5, 6, 9, 10, 11]);
    assert!(
        xml.contains("<c r=\"C4\" s=\"1\"><v>3</v></c></row><row r=\"5\"><c r=\"C5\"><v>4</v></c></row><row r=\"6\"><c r=\"C6\"><v>5</v></c></row><row r=\"9\""),
        "{xml}"
    );
    assert_eq!(x.dimension.as_deref(), Some("A1:C11"), "still valid");
    assert_counts(&out, 3, 4);
    assert_other_parts_unchanged(&source, &out.bytes);
    assert_rerun_is_noop(&out);
}

fn shrink(b1: u32) -> Parts {
    let rows = format!(
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
    );
    edit(
        package("A1:C11", &rows, ""),
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
    assert_other_parts_unchanged(&source, &out.bytes);
    assert!(unpack(&out.bytes)["xl/comments1.xml"].contains("ref=\"C4\""));
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
    assert_other_parts_unchanged(&source, &out.bytes);
    assert_rerun_is_noop(&out);
}

#[test]
fn existing_spill_collapses_3_to_1_keeping_its_binding() {
    let source = pack(&sized(producer(), 5, 1));
    let out = run(&source);
    assert_numbers(&out.bytes, &[("C2", 1.0), ("C9", 1.0), ("C10", 1.0)]);
    let x = parse_sheet(&sheet_xml(&out.bytes));
    assert_anchor(&x, "C2", "C2", Some("1"));
    assert_cleared(&x, "C3", Some("1"));
    assert_cleared(&x, "C4", Some("1"));
    assert_counts(&out, 3, 4);
    // The shared record is not toggled to collapsed.
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
    assert_anchor(&x, "C2", "C2", Some("1"));
    assert_cleared(&x, "C3", Some("1"));
    assert_cleared(&x, "C4", Some("1"));
    assert_eq!(x.cell("C6").v.as_deref(), Some("77"), "genuine input kept");
    assert_error(&x, "C9", "#REF!");
    assert_error(&x, "C10", "#REF!");
    assert_eq!(out.summary.errors, 3);
    assert_eq!(out.summary.error_summary["#SPILL!"].count, 1);
    assert_eq!(out.summary.error_summary["#REF!"].count, 2);
    assert_counts(&out, 3, 5);
    assert_other_parts_unchanged(&source, &out.bytes);
    assert_rerun_is_noop(&out);
}

#[test]
fn erroring_anchor_publishes_its_error_and_no_children() {
    let source = pack(&sized(producer(), 5, 0));
    let out = run(&source);
    let x = parse_sheet(&sheet_xml(&out.bytes));
    let c2 = x.cell("C2");
    assert_eq!(c2.attrs.get("t").map(String::as_str), Some("e"));
    assert_ne!(c2.v.as_deref(), Some("#SPILL!"));
    assert_anchor(&x, "C2", "C2", Some("1"));
    assert_cleared(&x, "C3", Some("1"));
    assert_cleared(&x, "C4", Some("1"));
    assert_counts(&out, 3, 5);
    assert_rerun_is_noop(&out);
}

#[test]
fn re_expansion_after_a_published_blocked_spill() {
    let blocked = run(&pack(&obstructed(5))).bytes;
    let second = edit(
        unpack(&blocked),
        SHEET,
        "<c r=\"B1\"><v>5</v></c>",
        "<c r=\"B1\"><v>3</v></c>",
    );
    let out = run(&pack(&second));
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
    assert!(xml.contains("<c r=\"C3\" s=\"1\"><v>2</v></c>"), "{xml}");
    assert_counts(&out, 3, 5);
    assert_rerun_is_noop(&out);
}

#[test]
fn two_anchors_sharing_metadata_grow_together() {
    let source = pack(&two_anchors());
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
    assert_counts(&out, 4, 4);
    assert_other_parts_unchanged(&source, &out.bytes);
    assert_rerun_is_noop(&out);
}

#[test]
fn sparse_extent_fills_shells_inserts_cells_in_order_and_widens_spans() {
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
    assert_numbers(&out.bytes, &[("D5", 8.0), ("D6", 9.0), ("B7", 36.0)]);
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
    let x = parse_sheet(&sheet_xml(&out.bytes));
    assert_eq!(x.dimension.as_deref(), Some("A1:D13"));
    assert_anchor(&x, "C2", "C2:C13", Some("1"));
    assert_eq!(x.row_numbers(), (1..=13).collect::<Vec<_>>());
    assert_eq!(x.row(9).2, vec!["C9", "D9"]);
    assert_eq!(x.row(11).2, vec!["A11", "C11"]);
    assert_eq!(x.row(9).1.get("spans").map(String::as_str), Some("1:4"));
    assert_eq!(x.row(12).1.get("spans"), None);
    assert_counts(&out, 3, 11);
    assert_rerun_is_noop(&out);
}

/// A worksheet with custom root namespace declarations and `sheetData`.
fn namespaced(root_ns: &str, sheet_data: &str) -> Parts {
    let mut p = package("A1:B2", "", "");
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<worksheet xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\" {root_ns}><dimension ref=\"A1:B2\"/>{sheet_data}<pageMargins left=\"0.7\" right=\"0.7\" top=\"0.75\" bottom=\"0.75\" header=\"0.3\" footer=\"0.3\"/><legacyDrawing r:id=\"rId1\"/></worksheet>"
    );
    p.insert(SHEET.to_owned(), xml);
    p
}
/// Every listed cell and its `v` are in the main namespace after a public
/// recalc, the output re-admits cold and a rerun is byte-identical.
#[track_caller]
fn assert_main_namespace_cells(source: &[u8], cells: &[(&str, f64)]) -> String {
    let out = run(source);
    let xml = sheet_xml(&out.bytes);
    let resolved = ns_cells(&xml);
    for (cell, n) in cells {
        let c = &resolved[*cell];
        assert_eq!(c.ns, MAIN, "{cell} element namespace: {xml}");
        assert!(
            c.children.iter().all(|(ns, _)| ns == MAIN)
                && c.children.last().is_some_and(|(_, local)| local == "v"),
            "{cell} children {:?}: {xml}",
            c.children
        );
        assert_eq!(data(&out.bytes, cell), Data::Float(*n), "calamine {cell}");
    }
    assert_rerun_is_noop(&out);
    xml
}

#[test]
fn cells_inserted_into_a_row_that_rebinds_the_default_namespace_stay_in_main() {
    // Row 2 rebinds the default namespace and uses `x` (bound to main at
    // the root) for itself; the prior footprint A1:B2 is spilled 2x2.
    let p = namespaced(
        &format!("xmlns:x=\"{MAIN}\""),
        concat!(
            "<sheetData>",
            "<row r=\"1\"><c r=\"A1\" cm=\"1\"><f t=\"array\" ref=\"A1:B2\">_xlfn.SEQUENCE(2,2)</f><v>1</v></c></row>",
            "<x:row xmlns=\"urn:other\" r=\"2\"><x:c r=\"B2\" s=\"1\"/></x:row>",
            "</sheetData>",
        ),
    );
    let xml = assert_main_namespace_cells(
        &pack(&p),
        &[("A1", 1.0), ("B1", 2.0), ("A2", 3.0), ("B2", 4.0)],
    );
    // Inserted into row 2 with the row's own prefix; the self-closing
    // shell B2 is expanded with its own prefix.
    assert!(
        xml.contains("<x:row xmlns=\"urn:other\" r=\"2\"><x:c r=\"A2\"><x:v>3</x:v></x:c><x:c r=\"B2\" s=\"1\"><x:v>4</x:v></x:c></x:row>"),
        "{xml}"
    );
}

#[test]
fn cells_inserted_into_a_row_that_rebinds_the_sheet_data_prefix_stay_in_main() {
    // `x:sheetData`; row 2 rebinds `x` to a foreign namespace and is itself
    // spelled with `y` (main). Row 3 is missing and is inserted at the
    // sheetData scope, where `x` is main.
    let p = namespaced(
        &format!("xmlns:x=\"{MAIN}\" xmlns:y=\"{MAIN}\""),
        concat!(
            "<x:sheetData>",
            "<x:row r=\"1\"><x:c r=\"A1\" cm=\"1\"><x:f t=\"array\" ref=\"A1:A2\">_xlfn.SEQUENCE(3)</x:f><x:v>1</x:v></x:c></x:row>",
            "<y:row xmlns:x=\"urn:other\" r=\"2\"><y:c r=\"B2\"><y:v>9</y:v></y:c></y:row>",
            "</x:sheetData>",
        ),
    );
    let xml = assert_main_namespace_cells(&pack(&p), &[("A1", 1.0), ("A2", 2.0), ("A3", 3.0)]);
    assert!(
        xml.contains("<y:row xmlns:x=\"urn:other\" r=\"2\"><y:c r=\"A2\"><y:v>2</y:v></y:c><y:c r=\"B2\"><y:v>9</y:v></y:c></y:row><x:row r=\"3\"><x:c r=\"A3\"><x:v>3</x:v></x:c></x:row></x:sheetData>"),
        "{xml}"
    );
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
        recalculate_xlsx_bytes(&pack(&merged(5)), Default::default()),
        "merged cell range",
    );
    let out = run(&pack(&merged(3)));
    assert_numbers(&out.bytes, &[("C4", 3.0), ("C9", 6.0)]);
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
    refused(
        recalculate_xlsx_bytes(&pack(&sized(p, 5, 3)), Default::default()),
        "shared formula family member",
    );
}

#[test]
fn generated_cell_budget_is_refused_before_publication() {
    let source = pack(&column_d_readers(100));
    let mut options = XlsxRecalculateOptions::default();
    options.limits.max_cells = 60;
    refused(
        recalculate_xlsx_bytes(&source, options.clone()),
        "generated spill cell limit",
    );
    options.limits.max_cells = 500;
    let out = recalculate_xlsx_bytes(&source, options).unwrap();
    assert_numbers(&out.bytes, &[("C101", 100.0), ("D9", 5050.0)]);
}

#[test]
fn output_and_expanded_budgets_cover_the_added_metadata_part() {
    let source = pack(&new_vertical());
    let full = run(&source);
    let mut options = XlsxRecalculateOptions::default();
    options.limits.max_output_bytes = full.bytes.len() - 1;
    assert!(recalculate_xlsx_bytes(&source, options).is_err());
    let expanded: usize = unpack(&full.bytes).values().map(String::len).sum();
    let mut options = XlsxRecalculateOptions::default();
    options.limits.max_expanded_bytes = expanded - 1;
    refused(
        recalculate_xlsx_bytes(&source, options),
        "expanded output byte limit",
    );
    let mut options = XlsxRecalculateOptions::default();
    options.limits.max_entries = unpack(&source).len();
    refused(
        recalculate_xlsx_bytes(&source, options),
        "ZIP entry count limit",
    );
}

#[test]
fn cancellation_publishes_nothing() {
    let token = CancelToken::new();
    token.cancel();
    let options = XlsxRecalculateOptions {
        cancel: Some(token),
        ..Default::default()
    };
    for p in [producer(), new_vertical()] {
        match recalculate_xlsx_bytes(&pack(&p), options.clone()) {
            Err(IoError::Engine(e)) => {
                assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Cancelled)
            }
            other => panic!("expected cancellation, got {other:?}"),
        }
    }
}

#[test]
fn unsupported_metadata_and_structures_stay_refused() {
    // Legacy CSE: an array formula without dynamic metadata.
    refused(
        recalculate_xlsx_bytes(
            &pack(&edit(
                producer(),
                SHEET,
                " cm=\"1\"><f t=\"array\" ref=\"C2:C4\">",
                "><f t=\"array\" ref=\"C2:C4\">",
            )),
            Default::default(),
        ),
        "legacy CSE",
    );
    // Data tables.
    refused(
        recalculate_xlsx_bytes(
            &pack(&edit(
                producer(),
                SHEET,
                "<f>SUM(C2#)</f>",
                "<f t=\"dataTable\" ref=\"C9\" r1=\"B1\"/>",
            )),
            Default::default(),
        ),
        "data-table formula",
    );
    // Rich value metadata on a cell, and unknown metadata records.
    refused(
        recalculate_xlsx_bytes(
            &pack(&edit(
                producer(),
                SHEET,
                "<c r=\"B1\">",
                "<c r=\"B1\" vm=\"1\">",
            )),
            Default::default(),
        ),
        "rich value metadata (vm)",
    );
    refused(
        recalculate_xlsx_bytes(
            &pack(&edit(
                producer(),
                METADATA,
                "</cellMetadata>",
                "</cellMetadata><valueMetadata count=\"1\"><bk><rc t=\"1\" v=\"0\"/></bk></valueMetadata>",
            )),
            Default::default(),
        ),
        "unsupported sheet metadata element",
    );
    let mut rich = producer();
    rich.insert("xl/richData/rdrichvalue.xml".into(), "<rv/>".into());
    refused(
        recalculate_xlsx_bytes(&pack(&rich), Default::default()),
        "external links or rich",
    );
    // External links.
    let mut linked = producer();
    linked.insert("xl/externalLinks/externalLink1.xml".into(), "<x/>".into());
    refused(
        recalculate_xlsx_bytes(&pack(&linked), Default::default()),
        "external links",
    );
    // Tables.
    refused(
        recalculate_xlsx_bytes(
            &pack(&edit(
                producer(),
                SHEET,
                "<legacyDrawing",
                "<tableParts count=\"1\"><tablePart r:id=\"rId9\"/></tableParts><legacyDrawing",
            )),
            Default::default(),
        ),
        "table metadata",
    );
    // A dangling binding.
    refused(
        recalculate_xlsx_bytes(
            &pack(&edit(producer(), SHEET, "cm=\"1\"", "cm=\"2\"")),
            Default::default(),
        ),
        "dangling",
    );
}

#[test]
fn non_default_spill_policy_is_refused_only_where_spills_are_involved() {
    let mut preempt = XlsxRecalculateOptions::default();
    preempt.eval_config.spill.conflict_policy =
        formualizer_eval::engine::SpillConflictPolicy::Preempt;
    refused(
        recalculate_xlsx_bytes(&pack(&producer()), preempt.clone()),
        "spill conflict policy",
    );
    refused(
        recalculate_xlsx_bytes(&pack(&new_vertical()), preempt.clone()),
        "spill conflict policy",
    );
    // A scalar workbook keeps the existing public behavior.
    let scalar = edit(new_vertical(), SHEET, "_xlfn.SEQUENCE($B$1)", "$B$1*2");
    let scalar = edit(scalar, SHEET, "SUM(C2#)", "C2+1");
    let scalar = edit(scalar, SHEET, "SUM(_xlfn.ANCHORARRAY(C2))", "C9+1");
    let out = recalculate_xlsx_bytes(&pack(&scalar), preempt).unwrap();
    assert_numbers(&out.bytes, &[("C2", 6.0), ("C9", 7.0), ("C10", 8.0)]);
}

#[cfg(not(target_arch = "wasm32"))]
mod file_api {
    use super::*;
    use formualizer_workbook::recalculate_xlsx_file;

    #[test]
    fn new_spill_is_published_atomically_and_failures_leave_the_destination() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.xlsx");
        let output = dir.path().join("out.xlsx");
        let source = pack(&new_vertical());
        std::fs::write(&input, &source).unwrap();
        std::fs::write(&output, b"existing output").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o640)).unwrap();
        }
        // A refused spill (merge) and a cancelled run publish nothing.
        let merged = pack(&edit(
            new_vertical(),
            SHEET,
            "</sheetData>",
            "</sheetData><mergeCells count=\"1\"><mergeCell ref=\"C3:D3\"/></mergeCells>",
        ));
        let bad = dir.path().join("bad.xlsx");
        std::fs::write(&bad, &merged).unwrap();
        refused(
            recalculate_xlsx_file(&bad, Some(&output), Default::default()),
            "merged cell range",
        );
        let token = CancelToken::new();
        token.cancel();
        let cancelled = XlsxRecalculateOptions {
            cancel: Some(token),
            ..Default::default()
        };
        assert!(recalculate_xlsx_file(&input, Some(&output), cancelled).is_err());
        assert_eq!(std::fs::read(&output).unwrap(), b"existing output");
        // Success publishes the same bytes as the bytes API, keeping the
        // destination's permissions; the input is untouched.
        let out = recalculate_xlsx_file(&input, Some(&output), Default::default()).unwrap();
        let written = std::fs::read(&output).unwrap();
        assert_eq!(written, out.bytes);
        assert_eq!(written, run(&source).bytes);
        assert_chain(&written, "C2", (1, 0), "rId5");
        assert_eq!(std::fs::read(&input).unwrap(), source);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&output).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o640);
        }
        // In place: published once, then an exact no-op.
        recalculate_xlsx_file(&input, None, Default::default()).unwrap();
        let first = std::fs::read(&input).unwrap();
        assert_eq!(first, out.bytes);
        let again = recalculate_xlsx_file(&input, None, Default::default()).unwrap();
        assert_eq!(again.cache_cells_changed, 0);
        assert_eq!(std::fs::read(&input).unwrap(), first);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_destination_is_refused_for_spills() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.xlsx");
        let target = dir.path().join("target.xlsx");
        let link = dir.path().join("link.xlsx");
        std::fs::write(&input, pack(&producer())).unwrap();
        std::fs::write(&target, b"target").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        refused(
            recalculate_xlsx_file(&input, Some(&link), Default::default()),
            "symlink destination",
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"target");
    }
}

// Deliberately duplicated in the WASM test: fixed timestamps and stored inputs
// make native/WASM input drift detectable independently of compression.
fn facade_spill_fixture(grow: bool) -> Vec<u8> {
    let main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    let office = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    let rels = "http://schemas.openxmlformats.org/package/2006/relationships";
    let metadata_rel = if grow {
        format!(
            "<Relationship Id=\"rId2\" Type=\"{office}/sheetMetadata\" Target=\"metadata.xml\"/>"
        )
    } else {
        String::new()
    };
    let metadata_type = if grow {
        "<Override PartName=\"/xl/metadata.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml\"/>"
    } else {
        ""
    };
    let anchor = if grow { " cm=\"1\"" } else { "" };
    let array = if grow {
        " t=\"array\" ref=\"C2:C4\""
    } else {
        ""
    };
    let children = if grow {
        "<row r=\"3\"><c r=\"C3\"><v>2</v></c></row><row r=\"4\"><c r=\"C4\"><v>3</v></c></row>"
    } else {
        ""
    };
    let mut parts = vec![
        (
            "[Content_Types].xml",
            format!(
                "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>{metadata_type}</Types>"
            ),
        ),
        (
            "_rels/.rels",
            format!(
                "<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>"
            ),
        ),
        (
            "xl/workbook.xml",
            format!(
                "<workbook xmlns=\"{main}\" xmlns:r=\"{office}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>"
            ),
        ),
        (
            "xl/_rels/workbook.xml.rels",
            format!(
                "<Relationships xmlns=\"{rels}\"><Relationship Id=\"rId1\" Type=\"{office}/worksheet\" Target=\"worksheets/sheet1.xml\"/>{metadata_rel}</Relationships>"
            ),
        ),
        (
            "xl/worksheets/sheet1.xml",
            format!(
                "<worksheet xmlns=\"{main}\"><dimension ref=\"B1:C10\"/><sheetData><row r=\"1\"><c r=\"B1\"><v>{}</v></c></row><row r=\"2\"><c r=\"C2\"{anchor}><f{array}>_xlfn.SEQUENCE($B$1)</f><v>99</v></c></row>{children}<row r=\"9\"><c r=\"C9\"><f>SUM(C2#)</f><v>99</v></c></row><row r=\"10\"><c r=\"C10\"><f>SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c></row></sheetData></worksheet>",
                if grow { 5 } else { 3 }
            ),
        ),
    ];
    if grow {
        parts.push(("xl/metadata.xml", format!("<metadata xmlns=\"{main}\" xmlns:xda=\"http://schemas.microsoft.com/office/spreadsheetml/2017/dynamicarray\"><metadataTypes count=\"1\"><metadataType name=\"XLDAPR\" minSupportedVersion=\"120000\" cellMeta=\"1\"/></metadataTypes><futureMetadata name=\"XLDAPR\" count=\"1\"><bk><extLst><ext uri=\"{{bdbb8cdc-fa1e-496e-a857-3c3f30c029c3}}\"><xda:dynamicArrayProperties fDynamic=\"1\" fCollapsed=\"0\"/></ext></extLst></bk></futureMetadata><cellMetadata count=\"1\"><bk><rc t=\"1\" v=\"0\"/></bk></cellMetadata></metadata>")));
    }
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .last_modified_time(zip::DateTime::from_date_and_time(2020, 1, 2, 3, 4, 6).unwrap());
    for (name, body) in parts {
        zip.start_file(name, options).unwrap();
        std::io::Write::write_all(&mut zip, body.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

fn facade_digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

// Shared length/FNV-1a-64 constants pin actual native output, not an Excel oracle.
#[test]
fn native_facade_spill_digests() {
    for (grow, input_len, input_hash, output_len, output_hash) in [
        (false, 2120, 0x294121765944cf03, 3004, 0xe616d27aa207a8ea),
        (true, 3140, 0x3ce6d5315a538805, 3217, 0x2ab4247034ff1dcf),
    ] {
        let input = facade_spill_fixture(grow);
        assert_eq!(
            (input.len(), facade_digest(&input)),
            (input_len, input_hash)
        );
        let result = recalculate_xlsx_bytes(&input, Default::default()).unwrap();
        assert_eq!(
            (result.bytes.len(), facade_digest(&result.bytes)),
            (output_len, output_hash)
        );
        assert_eq!(result.formula_cells, 3);
        assert_eq!(result.cache_cells_changed, 5);
        assert_eq!(result.worksheet_parts_changed, 1);
        let again = recalculate_xlsx_bytes(&result.bytes, Default::default()).unwrap();
        assert_eq!(again.bytes, result.bytes);
        assert_eq!(again.cache_cells_changed, 0);
    }
}
