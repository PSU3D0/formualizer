//! FORM211-A: namespace-validated XLDAPR admission and prior-footprint
//! ownership, independent of the evaluator.
use super::super::{
    SourceAdmission, XlsxRecalculateOptions, admit_source, recalculate_xlsx_bytes,
    sheet::SourceRect,
};
use crate::IoError;
use formualizer_eval::engine::{SpillBoundsPolicy, SpillConflictPolicy};
use std::collections::BTreeMap;
use std::io::{Cursor, Write};

pub(super) const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
pub(super) const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
pub(super) const OFFICE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const DYNAMIC: &str = "http://schemas.microsoft.com/office/spreadsheetml/2017/dynamicarray";
const XLDAPR_URI: &str = "{bdbb8cdc-fa1e-496e-a857-3c3f30c029c3}";
pub(super) const SHEET: &str = "xl/worksheets/sheet1.xml";
const METADATA: &str = "xl/metadata.xml";
pub(super) const WB_RELS: &str = "xl/_rels/workbook.xml.rels";
pub(super) const TYPES: &str = "[Content_Types].xml";
const METADATA_REL: &str = "<Relationship Id=\"rId5\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata\" Target=\"metadata.xml\"/>";
const METADATA_TYPE: &str = "<Override PartName=\"/xl/metadata.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml\"/>";

/// Shape copied from the original XlsxWriter `existing-grow` fixture.
pub(super) const PRODUCER_ROWS: &str = concat!(
    "<row r=\"1\" spans=\"1:3\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\"><v>5</v></c></row>",
    "<row r=\"2\" spans=\"1:3\"><c r=\"C2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
    "<row r=\"3\" spans=\"1:3\"><c r=\"C3\" s=\"1\"><v>2</v></c></row>",
    "<row r=\"4\" spans=\"1:3\"><c r=\"C4\" s=\"1\"><v>3</v></c></row>",
    "<row r=\"9\" spans=\"1:3\"><c r=\"C9\"><f>SUM(C2#)</f><v>99</v></c></row>",
    "<row r=\"10\" spans=\"1:3\"><c r=\"C10\" cm=\"1\"><f t=\"array\" ref=\"C10\">SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c></row>",
    "<row r=\"11\" spans=\"1:3\"><c r=\"A11\" t=\"s\"><v>1</v></c></row>",
);
/// Shape copied from the original `two-anchors-shared-metadata` fixture.
pub(super) const TWO_ANCHOR_ROWS: &str = concat!(
    "<row r=\"1\" spans=\"1:6\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\"><v>4</v></c></row>",
    "<row r=\"2\" spans=\"1:6\"><c r=\"C2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c><c r=\"F2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"F2:F4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
    "<row r=\"3\" spans=\"1:6\"><c r=\"C3\" s=\"1\"><v>2</v></c><c r=\"F3\" s=\"1\"><v>2</v></c></row>",
    "<row r=\"4\" spans=\"1:6\"><c r=\"C4\" s=\"1\"><v>3</v></c><c r=\"F4\" s=\"1\"><v>3</v></c></row>",
    "<row r=\"9\" spans=\"1:6\"><c r=\"C9\"><f>SUM(C2#)</f><v>99</v></c></row>",
    "<row r=\"10\" spans=\"1:6\"><c r=\"C10\" cm=\"1\"><f t=\"array\" ref=\"C10\">SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c></row>",
    "<row r=\"11\" spans=\"1:6\"><c r=\"A11\" t=\"s\"><v>1</v></c></row>",
);
const C2_ANCHOR: &str = "<c r=\"C2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c>";

fn producer_metadata() -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<metadata xmlns=\"{MAIN}\" xmlns:xda=\"{DYNAMIC}\"><metadataTypes count=\"1\"><metadataType name=\"XLDAPR\" minSupportedVersion=\"120000\" copy=\"1\" pasteAll=\"1\" pasteValues=\"1\" merge=\"1\" splitFirst=\"1\" rowColShift=\"1\" clearFormats=\"1\" clearComments=\"1\" assign=\"1\" coerce=\"1\" cellMeta=\"1\"/></metadataTypes><futureMetadata name=\"XLDAPR\" count=\"1\"><bk><extLst><ext uri=\"{XLDAPR_URI}\"><xda:dynamicArrayProperties fDynamic=\"1\" fCollapsed=\"0\"/></ext></extLst></bk></futureMetadata><cellMetadata count=\"1\"><bk><rc t=\"1\" v=\"0\"/></bk></cellMetadata></metadata>"
    )
}
fn worksheet(dimension: &str, rows: &str, tail: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<worksheet xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><dimension ref=\"{dimension}\"/><sheetViews><sheetView tabSelected=\"1\" workbookViewId=\"0\"/></sheetViews><sheetFormatPr defaultRowHeight=\"15\"/><sheetData>{rows}</sheetData>{tail}<pageMargins left=\"0.7\" right=\"0.7\" top=\"0.75\" bottom=\"0.75\" header=\"0.3\" footer=\"0.3\"/><legacyDrawing r:id=\"rId1\"/></worksheet>"
    )
}
/// Producer-shaped package: explicit sheetMetadata relationship/content type,
/// shared strings, comments/VML sheet relationships and opaque members.
pub(super) fn package(dimension: &str, rows: &str, tail: &str) -> BTreeMap<String, String> {
    [
        (TYPES, format!("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Default Extension=\"vml\" ContentType=\"application/vnd.openxmlformats-officedocument.vmlDrawing\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/><Override PartName=\"/xl/comments1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.comments+xml\"/><Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/>{METADATA_TYPE}</Types>")),
        ("_rels/.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>")),
        (WB_RELS, format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"rId4\" Type=\"{OFFICE}/sharedStrings\" Target=\"sharedStrings.xml\"/>{METADATA_REL}</Relationships>")),
        ("xl/sharedStrings.xml", format!("<sst xmlns=\"{MAIN}\" count=\"2\" uniqueCount=\"2\"><si><t>n</t></si><si><t>tail</t></si></sst>")),
        (SHEET, worksheet(dimension, rows, tail)),
        ("xl/worksheets/_rels/sheet1.xml.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/vmlDrawing\" Target=\"../drawings/vmlDrawing1.vml\"/><Relationship Id=\"rId2\" Type=\"{OFFICE}/comments\" Target=\"../comments1.xml\"/></Relationships>")),
        ("xl/comments1.xml", format!("<comments xmlns=\"{MAIN}\"><authors><author>a</author></authors><commentList/></comments>")),
        ("xl/drawings/vmlDrawing1.vml", "<xml/>".to_owned()),
        (METADATA, producer_metadata()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}
pub(super) fn producer() -> BTreeMap<String, String> {
    package("A1:C11", PRODUCER_ROWS, "")
}
pub(super) fn producer_wide() -> BTreeMap<String, String> {
    package("A1:H11", PRODUCER_ROWS, "")
}
pub(super) fn pack(parts: &BTreeMap<String, String>) -> Vec<u8> {
    let mut z = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .last_modified_time(zip::DateTime::from_date_and_time(2020, 1, 2, 3, 4, 6).unwrap());
    for (name, body) in parts {
        z.start_file(name.as_str(), options).unwrap();
        z.write_all(body.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}
/// Replace an exact fragment; a mutation that matches nothing is a test bug.
pub(super) fn edit(
    mut p: BTreeMap<String, String>,
    name: &str,
    old: &str,
    new: &str,
) -> BTreeMap<String, String> {
    let part = p.get_mut(name).unwrap_or_else(|| panic!("missing {name}"));
    assert!(part.contains(old), "{name} lacks {old:?}");
    *part = part.replacen(old, new, 1);
    p
}
fn with_admission<T>(
    p: &BTreeMap<String, String>,
    options: &XlsxRecalculateOptions,
    check: impl FnOnce(SourceAdmission<'_>) -> T,
) -> Result<T, IoError> {
    let bytes = pack(p);
    admit_source(&bytes, options).map(check)
}
pub(super) fn refused<T>(result: Result<T, IoError>, needle: &str) {
    match result {
        Ok(_) => panic!("admitted; expected refusal containing {needle:?}"),
        Err(IoError::Unsupported { feature, context }) => assert!(
            feature.contains(needle) || context.contains(needle),
            "expected {needle:?}, got {feature:?} / {context:?}"
        ),
        Err(other) => panic!("expected Unsupported({needle:?}), got {other:?}"),
    }
}
fn rejects(p: &BTreeMap<String, String>, needle: &str) {
    refused(with_admission(p, &Default::default(), |_| ()), needle);
}
fn rect(first_row: u32, first_col: u32, last_row: u32, last_col: u32) -> SourceRect {
    SourceRect {
        first_row,
        first_col,
        last_row,
        last_col,
    }
}
/// (anchor, prior footprint) pairs and (child, owner) pairs for the only sheet.
type Owned = (Vec<((u32, u32), SourceRect)>, Vec<((u32, u32), (u32, u32))>);
fn owned(p: &BTreeMap<String, String>) -> Owned {
    with_admission(p, &Default::default(), |a| {
        assert_eq!(a.plans.len(), 1);
        let own = &a.plans[0].ownership;
        (
            own.anchors.iter().map(|(k, v)| (*k, v.footprint)).collect(),
            own.children.iter().map(|(k, v)| (*k, *v)).collect(),
        )
    })
    .expect("admitted")
}

#[test]
fn source_rect_geometry_is_one_based_inclusive() {
    let r = rect(2, 3, 4, 3);
    assert!(r.contains(2, 3) && r.contains(4, 3) && !r.contains(5, 3) && !r.contains(3, 4));
    assert!(r.intersects(rect(4, 1, 9, 3)) && !r.intersects(rect(5, 3, 5, 3)));
    assert_eq!(r.cell_count(), Some(3));
    assert_eq!(rect(10, 3, 10, 3).cell_count(), Some(1));
    assert_eq!(rect(2, 3, 1, 3).cell_count(), None);
}

#[test]
fn producer_shaped_anchor_ownership_and_source_index() {
    let p = producer();
    let bytes = pack(&p);
    let admission = admit_source(&bytes, &Default::default()).expect("admitted");
    let meta = admission.metadata.as_ref().expect("sheet metadata");
    assert_eq!(meta.part, METADATA);
    let plan = &admission.plans[0];
    let xml = &plan.data;
    let text = |r: &std::ops::Range<usize>| std::str::from_utf8(&xml[r.clone()]).unwrap();
    let own = &plan.ownership;
    assert_eq!(
        own.anchors
            .iter()
            .map(|(k, a)| (*k, a.footprint))
            .collect::<Vec<_>>(),
        vec![((2, 3), rect(2, 3, 4, 3)), ((10, 3), rect(10, 3, 10, 3))]
    );
    assert_eq!(
        own.children
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<Vec<_>>(),
        vec![((3, 3), (2, 3)), ((4, 3), (2, 3))]
    );
    for anchor in own.anchors.values() {
        // cm one-based -> cellMetadata bk; rc t one-based type; rc v zero-based block.
        assert_eq!(
            (
                anchor.binding.unwrap().cell_metadata,
                anchor.binding.unwrap().metadata_type,
                anchor.binding.unwrap().future_block
            ),
            (1, 1, 0)
        );
        assert!(!anchor.binding.unwrap().collapsed);
        let cell = &plan.cells[anchor.formula];
        assert_eq!((cell.row, cell.col), (anchor.row, anchor.col));
        assert_eq!(text(cell.cm_span().unwrap()), "cm=\"1\"");
        assert_eq!(
            text(cell.formula_kind_span.as_ref().unwrap()),
            "t=\"array\""
        );
        assert!(text(&cell.formula_open).starts_with("<f t=\"array\""));
    }
    let c2 = &plan.cells[own.anchors[&(2, 3)].formula];
    assert_eq!(text(c2.array_ref_span().unwrap()), "ref=\"C2:C4\"");
    assert_eq!(c2.formula_text, "_xlfn.SEQUENCE($B$1)");
    let index = plan.index.as_ref().expect("indexed anchor sheet");
    let (dimension, dimension_span) = index.dimension.clone().unwrap();
    assert_eq!(dimension, rect(1, 1, 11, 3));
    assert_eq!(text(&dimension_span), "ref=\"A1:C11\"");
    assert_eq!(
        index.rows.iter().map(|r| r.row).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 9, 10, 11]
    );
    let row2 = &index.rows[1];
    assert_eq!(text(row2.spans_attr.as_ref().unwrap()), "spans=\"1:3\"");
    assert!(text(&row2.span).starts_with("<row r=\"2\"") && text(&row2.span).ends_with("</row>"));
    assert_eq!(index.cells[row2.cells.clone()].len(), 1);
    assert_eq!(
        index
            .cells
            .iter()
            .map(|c| (c.row, c.col))
            .collect::<Vec<_>>(),
        vec![
            (1, 1),
            (1, 2),
            (2, 3),
            (3, 3),
            (4, 3),
            (9, 3),
            (10, 3),
            (11, 1)
        ]
    );
    let c3 = index.cell_at(3, 3).unwrap();
    assert_eq!(text(&c3.span), "<c r=\"C3\" s=\"1\"><v>2</v></c>");
    assert_eq!(text(c3.value.as_ref().unwrap()), "<v>2</v>");
    assert!(c3.formula.is_none() && c3.cm.is_none());
    assert_eq!(text(&index.cell_at(2, 3).unwrap().span), C2_ANCHOR);
    assert!(index.merges.is_empty());
    assert!(text(&index.sheet_data).starts_with("<sheetData>"));
    // Anchors remain source formulas; generated children are not formulas.
    assert_eq!(admission.formula_count, 3);
}

#[test]
fn two_anchors_share_one_metadata_record() {
    let (anchors, children) = owned(&package("A1:F11", TWO_ANCHOR_ROWS, ""));
    assert_eq!(
        anchors,
        vec![
            ((2, 3), rect(2, 3, 4, 3)),
            ((2, 6), rect(2, 6, 4, 6)),
            ((10, 3), rect(10, 3, 10, 3)),
        ]
    );
    assert_eq!(
        children,
        vec![
            ((3, 3), (2, 3)),
            ((3, 6), (2, 6)),
            ((4, 3), (2, 3)),
            ((4, 6), (2, 6)),
        ]
    );
}

#[test]
fn one_by_one_declared_anchor_has_one_cell_footprint_and_no_children() {
    let rows = "<row r=\"10\"><c r=\"C10\" cm=\"1\"><f t=\"array\" ref=\"C10\">SUM(1)</f><v>1</v></c><c r=\"D10\"><v>7</v></c></row>";
    let (anchors, children) = owned(&package("C10:D10", rows, ""));
    assert_eq!(anchors, vec![((10, 3), rect(10, 3, 10, 3))]);
    assert!(children.is_empty());
}

#[test]
fn stacked_anchors_reusing_a_column_keep_distinct_ownership() {
    let rows = concat!(
        "<row r=\"1\"><c r=\"B1\"><v>2</v></c></row>",
        "<row r=\"2\"><c r=\"A2\" cm=\"1\"><f t=\"array\" ref=\"A2:A9\">_xlfn.SEQUENCE(8)</f><v>1</v></c><c r=\"C2\" cm=\"1\"><f t=\"array\" ref=\"C2:C3\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
        "<row r=\"3\"><c r=\"C3\"><v>2</v></c></row>",
        "<row r=\"5\"><c r=\"C5\" cm=\"1\"><f t=\"array\" ref=\"C5:C6\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
        "<row r=\"6\"><c r=\"C6\"><v>2</v></c></row>",
        "<row r=\"9\"><c r=\"A9\"><v>8</v></c><c r=\"C9\"><v>0</v></c></row>",
    );
    let (anchors, children) = owned(&package("A1:C9", rows, ""));
    assert_eq!(
        anchors,
        vec![
            ((2, 1), rect(2, 1, 9, 1)),
            ((2, 3), rect(2, 3, 3, 3)),
            ((5, 3), rect(5, 3, 6, 3)),
        ]
    );
    assert_eq!(
        children,
        vec![((3, 3), (2, 3)), ((6, 3), (5, 3)), ((9, 1), (2, 1))]
    );
}

#[test]
fn sparse_extent_keeps_missing_children_missing_and_records_merges() {
    let rows = concat!(
        "<row r=\"1\"><c r=\"B1\"><v>4</v></c></row>",
        "<row r=\"2\"><c r=\"C2\" cm=\"1\"><f t=\"array\" ref=\"C2:D5\">_xlfn.SEQUENCE($B$1,2)</f><v>1</v></c></row>",
        "<row r=\"3\"><c r=\"D3\" s=\"1\"/></row>",
        "<row r=\"4\" spans=\"3:3\"><c r=\"C4\"><v>5</v></c></row>",
        "<row r=\"5\"/>",
        "<row r=\"6\"><c r=\"D6\"><v>9</v></c></row>",
    );
    let tail = "<mergeCells count=\"1\"><mergeCell ref=\"E7:F8\"/></mergeCells>";
    let p = package("B1:F8", rows, tail);
    let (anchors, children) = owned(&p);
    assert_eq!(anchors, vec![((2, 3), rect(2, 3, 5, 4))]);
    // D3 is a styled shell, C4 a stale cache; C3/D4/C5/D5 are not serialized
    // and are not manufactured. D6 is outside the footprint.
    assert_eq!(children, vec![((3, 4), (2, 3)), ((4, 3), (2, 3))]);
    with_admission(&p, &Default::default(), |a| {
        let index = a.plans[0].index.as_ref().expect("indexed anchor sheet");
        assert_eq!(index.merges, vec![rect(7, 5, 8, 6)]);
        assert_eq!(index.cells.len(), 5);
        assert!(index.cell_at(3, 4).unwrap().empty);
        let row5 = index.rows.iter().find(|r| r.row == 5).unwrap();
        assert!(row5.empty && row5.cells.is_empty());
    })
    .unwrap();
}

#[test]
fn owned_children_are_classified_before_literal_readability_refusal() {
    // A stale owned child with an unreadable literal payload is generated
    // cache, not an unsupported input; the same payload outside is refused.
    let p = edit(
        producer(),
        SHEET,
        "<c r=\"C3\" s=\"1\"><v>2</v></c>",
        "<c r=\"C3\" s=\"1\" t=\"b\"><v>2</v></c>",
    );
    assert_eq!(owned(&p).1, vec![((3, 3), (2, 3)), ((4, 3), (2, 3))]);
    let outside = edit(
        producer(),
        SHEET,
        "<c r=\"B1\"><v>5</v></c>",
        "<c r=\"B1\" t=\"b\"><v>2</v></c>",
    );
    rejects(&outside, "literal scalar payload");
}

#[test]
fn non_default_spill_policy_is_rejected_before_evaluation_with_anchors() {
    let bytes = pack(&producer());
    let mut preempt = XlsxRecalculateOptions::default();
    preempt.eval_config.spill.conflict_policy = SpillConflictPolicy::Preempt;
    refused(
        recalculate_xlsx_bytes(&bytes, preempt.clone()),
        "spill conflict policy",
    );
    let mut truncate = XlsxRecalculateOptions::default();
    truncate.eval_config.spill.bounds_policy = SpillBoundsPolicy::Truncate;
    refused(
        recalculate_xlsx_bytes(&bytes, truncate),
        "spill bounds policy",
    );
    // An ordinary scalar workbook keeps accepting the (unused) policy.
    let rows = "<row r=\"1\"><c r=\"A1\"><f>1+1</f><v>9</v></c></row>";
    let mut p = edit(package("A1", rows, ""), WB_RELS, METADATA_REL, "");
    p.remove(METADATA);
    let p = edit(p, TYPES, METADATA_TYPE, "");
    let out = recalculate_xlsx_bytes(&pack(&p), preempt).unwrap();
    assert_eq!(out.cache_cells_changed, 1);
}

#[test]
fn dangling_cell_metadata_is_rejected() {
    rejects(&edit(producer(), SHEET, "cm=\"1\"", "cm=\"2\""), "dangling");
    rejects(&edit(producer(), SHEET, "cm=\"1\"", "cm=\"0\""), "dangling");
    rejects(&edit(producer(), SHEET, "cm=\"1\"", "cm=\"x\""), "dangling");
    // No metadata part at all: every cm is dangling.
    let mut p = edit(producer(), WB_RELS, METADATA_REL, "");
    p.remove(METADATA);
    rejects(&edit(p, TYPES, METADATA_TYPE, ""), "dangling");
}

#[test]
fn value_metadata_is_rejected() {
    rejects(
        &edit(
            producer(),
            SHEET,
            "<c r=\"C3\" s=\"1\">",
            "<c r=\"C3\" s=\"1\" vm=\"1\">",
        ),
        "vm",
    );
    rejects(
        &edit(
            producer(),
            SHEET,
            "<c r=\"C2\" s=\"1\" cm=\"1\">",
            "<c r=\"C2\" s=\"1\" cm=\"1\" vm=\"1\">",
        ),
        "vm",
    );
    let valued = edit(
        producer(),
        METADATA,
        "</cellMetadata>",
        "</cellMetadata><valueMetadata count=\"1\"><bk><rc t=\"1\" v=\"0\"/></bk></valueMetadata>",
    );
    rejects(&valued, "unsupported sheet metadata");
}

#[test]
fn wrong_namespace_lookalikes_are_rejected() {
    let foreign_props = edit(
        producer(),
        METADATA,
        &format!("xmlns:xda=\"{DYNAMIC}\""),
        "xmlns:xda=\"http://schemas.microsoft.com/office/spreadsheetml/2017/dynamicarrays\"",
    );
    rejects(&foreign_props, "lookalike");
    rejects(
        &edit(
            producer(),
            METADATA,
            XLDAPR_URI,
            "{bdbb8cdc-fa1e-496e-a857-3c3f30c029c4}",
        ),
        "extension URI",
    );
    rejects(
        &edit(
            producer(),
            METADATA,
            &format!("<metadata xmlns=\"{MAIN}\""),
            "<metadata xmlns=\"urn:foreign\"",
        ),
        "namespace",
    );
    let foreign_cells = edit(
        edit(
            producer(),
            METADATA,
            "<cellMetadata count=\"1\">",
            "<x:cellMetadata xmlns:x=\"urn:foreign\" count=\"1\">",
        ),
        METADATA,
        "</cellMetadata>",
        "</x:cellMetadata>",
    );
    rejects(&foreign_cells, "lookalike");
    let foreign_rc = edit(
        producer(),
        METADATA,
        "<rc t=\"1\" v=\"0\"/>",
        "<x:rc xmlns:x=\"urn:foreign\" t=\"1\" v=\"0\"/>",
    );
    rejects(&foreign_rc, "lookalike");
    // Strict-namespace relationship: metadata.xml is then unrelated.
    let strict = edit(
        producer(),
        WB_RELS,
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata",
        "http://purl.oclc.org/ooxml/officeDocument/relationships/sheetMetadata",
    );
    rejects(&strict, "unrelated sheet metadata part");
    let unknown_type = edit(
        edit(
            producer(),
            METADATA,
            "<metadataTypes count=\"1\">",
            "<metadataTypes count=\"2\">",
        ),
        METADATA,
        "</metadataTypes>",
        "<metadataType name=\"XLRICHVALUE\"/></metadataTypes>",
    );
    rejects(&unknown_type, "unsupported metadata type");
    rejects(
        &edit(producer(), METADATA, "fDynamic=\"1\"", "fDynamic=\"0\""),
        "non-dynamic",
    );
    rejects(
        &edit(producer(), METADATA, "fCollapsed=\"0\"", "fCollapsed=\"1\""),
        "collapsed",
    );
    rejects(
        &edit(
            producer(),
            METADATA,
            "<cellMetadata count=\"1\">",
            "<cellMetadata count=\"2\">",
        ),
        "count",
    );
}

#[test]
fn out_of_range_metadata_type_and_block_indexes_are_rejected() {
    for t in ["0", "2", "-1", "x"] {
        let rc = format!("<rc t=\"{t}\" v=\"0\"/>");
        rejects(
            &edit(producer(), METADATA, "<rc t=\"1\" v=\"0\"/>", &rc),
            "metadata type index",
        );
    }
    for v in ["1", "-1"] {
        let rc = format!("<rc t=\"1\" v=\"{v}\"/>");
        rejects(
            &edit(producer(), METADATA, "<rc t=\"1\" v=\"0\"/>", &rc),
            "future metadata block index",
        );
    }
}

#[test]
fn legacy_cse_is_admitted_but_data_tables_are_rejected() {
    let bytes = pack(&edit(
        producer(),
        SHEET,
        "<c r=\"C2\" s=\"1\" cm=\"1\">",
        "<c r=\"C2\" s=\"1\">",
    ));
    let admission = admit_source(&bytes, &XlsxRecalculateOptions::default()).unwrap();
    assert!(
        admission.plans[0].ownership.anchors[&(2, 3)]
            .binding
            .is_none()
    );
    let table = edit(
        producer(),
        SHEET,
        C2_ANCHOR,
        "<c r=\"C2\"><f t=\"dataTable\" ref=\"C2:C4\" dt2D=\"0\" dtr=\"0\" r1=\"B1\"/><v>1</v></c>",
    );
    rejects(&table, "data-table");
    let marked_table = edit(
        producer(),
        SHEET,
        C2_ANCHOR,
        "<c r=\"C2\" cm=\"1\"><f t=\"dataTable\" ref=\"C2:C4\" dt2D=\"0\" dtr=\"0\" r1=\"B1\"/><v>1</v></c>",
    );
    rejects(&marked_table, "data-table");
    rejects(
        &edit(
            producer(),
            SHEET,
            "<c r=\"C3\" s=\"1\">",
            "<c r=\"C3\" s=\"1\" cm=\"1\">",
        ),
        "non-dynamic-array cell",
    );
    rejects(
        &edit(
            producer(),
            SHEET,
            "<f t=\"array\" ref=\"C2:C4\">",
            "<f t=\"array\">",
        ),
        "missing dynamic array extent",
    );
    rejects(
        &edit(
            producer(),
            SHEET,
            "<f>SUM(C2#)</f>",
            "<f ref=\"C9\">SUM(C2#)</f>",
        ),
        "non-shared formula extent",
    );
}

#[test]
fn metadata_part_relationship_and_content_type_must_agree() {
    let duplicate_rel = edit(
        producer(),
        WB_RELS,
        METADATA_REL,
        &format!("{METADATA_REL}{}", METADATA_REL.replace("rId5", "rId6")),
    );
    rejects(&duplicate_rel, "duplicate sheet metadata relationship");
    let mut missing_part = producer();
    missing_part.remove(METADATA);
    rejects(&missing_part, "missing sheet metadata part");
    rejects(
        &edit(producer(), WB_RELS, METADATA_REL, ""),
        "unrelated sheet metadata part",
    );
    let mut duplicate_part = edit(
        producer(),
        TYPES,
        METADATA_TYPE,
        &format!(
            "{METADATA_TYPE}{}",
            METADATA_TYPE.replace("metadata.xml", "metadata2.xml")
        ),
    );
    duplicate_part.insert("xl/metadata2.xml".into(), producer_metadata());
    rejects(&duplicate_part, "unrelated sheet metadata part");
    let second_rel = edit(
        duplicate_part.clone(),
        WB_RELS,
        METADATA_REL,
        &format!(
            "{METADATA_REL}{}",
            METADATA_REL
                .replace("rId5", "rId6")
                .replace("metadata.xml", "metadata2.xml")
        ),
    );
    rejects(&second_rel, "duplicate sheet metadata relationship");
    let mut moved = edit(
        producer(),
        WB_RELS,
        "Target=\"metadata.xml\"",
        "Target=\"meta/other.xml\"",
    );
    moved.remove(METADATA);
    rejects(&moved, "missing sheet metadata part");
    let external = edit(
        producer(),
        WB_RELS,
        "Target=\"metadata.xml\"/>",
        "Target=\"https://example.invalid/m.xml\" TargetMode=\"External\"/>",
    );
    rejects(&external, "external sheet metadata relationship");
    // Content-type disagreement: default application/xml, wrong override,
    // or a sheetMetadata default.
    rejects(
        &edit(producer(), TYPES, METADATA_TYPE, ""),
        "content-type disagreement",
    );
    rejects(
        &edit(
            producer(),
            TYPES,
            "sheetMetadata+xml\"/>",
            "sheetMetadatx+xml\"/>",
        ),
        "content-type disagreement",
    );
    rejects(
        &edit(
            producer(),
            TYPES,
            "ContentType=\"application/xml\"",
            "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml\"",
        ),
        "content-type disagreement",
    );
    // Rich data parts remain unsupported.
    let mut rich = producer();
    rich.insert("xl/richData/rdrichvalue.xml".into(), "<rv/>".into());
    rejects(&rich, "rich value data");
}

#[test]
fn overlapping_extents_are_rejected() {
    // B3's extent crosses C2's claimed child C3 without either anchor lying
    // inside the other footprint.
    let crossing = edit(
        producer(),
        SHEET,
        "<row r=\"3\" spans=\"1:3\"><c r=\"C3\" s=\"1\"><v>2</v></c></row>",
        "<row r=\"3\" spans=\"1:4\"><c r=\"B3\" cm=\"1\"><f t=\"array\" ref=\"B3:D3\">_xlfn.SEQUENCE(1,3)</f><v>1</v></c><c r=\"C3\" s=\"1\"><v>2</v></c></row>",
    );
    rejects(&crossing, "overlapping");
    let nested = edit(
        producer(),
        SHEET,
        "<c r=\"C3\" s=\"1\"><v>2</v></c>",
        "<c r=\"C3\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C3\">1</f><v>1</v></c>",
    );
    rejects(&nested, "overlapping");
    // D1:D3 claims only unserialized cells of C2:D4; neither anchor is inside
    // the other footprint.
    let sparse = edit(
        edit(producer_wide(), SHEET, "ref=\"C2:C4\"", "ref=\"C2:D4\""),
        SHEET,
        "<c r=\"B1\"><v>5</v></c>",
        "<c r=\"B1\"><v>5</v></c><c r=\"D1\" cm=\"1\"><f t=\"array\" ref=\"D1:D3\">_xlfn.SEQUENCE(3)</f><v>1</v></c>",
    );
    rejects(&sparse, "overlapping");
}

#[test]
fn separately_authored_formula_inside_a_child_footprint_is_rejected() {
    let authored = edit(
        producer(),
        SHEET,
        "<c r=\"C3\" s=\"1\"><v>2</v></c>",
        "<c r=\"C3\" s=\"1\"><f>1+1</f><v>2</v></c>",
    );
    rejects(&authored, "formula inside a dynamic array child footprint");
    let shared_child = edit(
        edit(
            producer(),
            SHEET,
            "<c r=\"C3\" s=\"1\"><v>2</v></c>",
            "<c r=\"C3\" s=\"1\"><f t=\"shared\" ref=\"C3:C4\" si=\"0\">1+1</f><v>2</v></c>",
        ),
        SHEET,
        "<c r=\"C4\" s=\"1\"><v>3</v></c>",
        "<c r=\"C4\" s=\"1\"><f t=\"shared\" si=\"0\"/><v>3</v></c>",
    );
    rejects(
        &shared_child,
        "formula inside a dynamic array child footprint",
    );
}

#[test]
fn source_shared_family_member_cannot_be_a_dynamic_anchor() {
    let shared_anchor = edit(
        producer(),
        SHEET,
        "<f t=\"array\" ref=\"C2:C4\">",
        "<f t=\"shared\" ref=\"C2:C4\" si=\"0\">",
    );
    rejects(
        &shared_anchor,
        "shared formula family member claimed as dynamic anchor",
    );
    let descendant = edit(
        edit(
            producer_wide(),
            SHEET,
            "<c r=\"B1\"><v>5</v></c>",
            "<c r=\"B1\"><v>5</v></c><c r=\"E1\"><f t=\"shared\" ref=\"E1:E2\" si=\"0\">B1*2</f><v>10</v></c>",
        ),
        SHEET,
        C2_ANCHOR,
        &format!("{C2_ANCHOR}<c r=\"E2\" cm=\"1\"><f t=\"shared\" si=\"0\"/><v>10</v></c>"),
    );
    rejects(
        &descendant,
        "shared formula family member claimed as dynamic anchor",
    );
}

#[test]
fn anchor_geometry_and_extent_limits_are_enforced() {
    rejects(
        &edit(producer(), SHEET, "ref=\"C2:C4\"", "ref=\"B1:C4\""),
        "top-left",
    );
    rejects(
        &edit(producer(), SHEET, "ref=\"C2:C4\"", "ref=\"C2:C1\""),
        "reversed",
    );
    rejects(
        &edit(producer(), SHEET, "ref=\"C2:C4\"", "ref=\"C2:C1048577\""),
        "coordinate",
    );
    rejects(
        &edit(producer(), SHEET, "ref=\"C2:C4\"", "ref=\"C2:IW4\""),
        "dynamic array extent width limit",
    );
    // 42-cell prior footprint versus a 33-cell dimension and 8 serialized cells.
    let tall = edit(producer(), SHEET, "ref=\"C2:C4\"", "ref=\"C2:H8\"");
    let mut small = XlsxRecalculateOptions::default();
    small.limits.max_cells = 40;
    refused(
        with_admission(&tall, &small, |_| ()),
        "dynamic array extent cell limit",
    );
    // Same package under default limits is admitted: the refusal is the limit.
    assert_eq!(owned(&tall).0[0].1, rect(2, 3, 8, 8));
    // Merge rectangles are validated rather than ignored.
    rejects(
        &package(
            "A1:C11",
            PRODUCER_ROWS,
            "<mergeCells count=\"1\"><mergeCell ref=\"E7:D8\"/></mergeCells>",
        ),
        "reversed",
    );
    rejects(
        &package(
            "A1:C11",
            PRODUCER_ROWS,
            "<x:mergeCells xmlns:x=\"urn:foreign\"><x:mergeCell ref=\"E7:F8\"/></x:mergeCells>",
        ),
        "lookalike",
    );
}
