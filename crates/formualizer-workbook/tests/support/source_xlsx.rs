//! Original source-XML builders and independent readers for the public
//! source-preserving spill tests. Packages are built from literal XML (no
//! spreadsheet library round trip), and outputs are inspected with quick-xml
//! (namespace-aware for the metadata chain), the `zip` crate and Calamine.
#![allow(dead_code)]
use calamine::{Data, Reader, Xlsx};
use quick_xml::NsReader;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::ResolveResult;
use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};

pub type Parts = BTreeMap<String, String>;

pub const MAIN: &str = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
pub const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
pub const OFFICE: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
pub const CONTENT_TYPES: &str = "http://schemas.openxmlformats.org/package/2006/content-types";
pub const DYNAMIC: &str = "http://schemas.microsoft.com/office/spreadsheetml/2017/dynamicarray";
pub const XLDAPR_URI: &str = "{bdbb8cdc-fa1e-496e-a857-3c3f30c029c3}";
pub const SHEET_METADATA_REL: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata";
pub const SHEET_METADATA_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml";
pub const SHEET: &str = "xl/worksheets/sheet1.xml";
pub const METADATA: &str = "xl/metadata.xml";
pub const WB_RELS: &str = "xl/_rels/workbook.xml.rels";
pub const TYPES: &str = "[Content_Types].xml";
pub const METADATA_REL: &str = "<Relationship Id=\"rId5\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata\" Target=\"metadata.xml\"/>";
pub const METADATA_OVERRIDE: &str = "<Override PartName=\"/xl/metadata.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml\"/>";
pub const ARCHIVE_COMMENT: &str = "source archive comment";

/// Shape copied from the original XlsxWriter `existing-grow` fixture:
/// `C2 = SEQUENCE($B$1)` over a prior C2:C4 footprint, readers `C9 =
/// SUM(C2#)` and `C10 = SUM(ANCHORARRAY(C2))` (a 1x1 declared anchor).
pub const PRODUCER_ROWS: &str = concat!(
    "<row r=\"1\" spans=\"1:3\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\"><v>5</v></c></row>",
    "<row r=\"2\" spans=\"1:3\"><c r=\"C2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
    "<row r=\"3\" spans=\"1:3\"><c r=\"C3\" s=\"1\"><v>2</v></c></row>",
    "<row r=\"4\" spans=\"1:3\"><c r=\"C4\" s=\"1\"><v>3</v></c></row>",
    "<row r=\"9\" spans=\"1:3\"><c r=\"C9\"><f>SUM(C2#)</f><v>99</v></c></row>",
    "<row r=\"10\" spans=\"1:3\"><c r=\"C10\" cm=\"1\"><f t=\"array\" ref=\"C10\">SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c></row>",
    "<row r=\"11\" spans=\"1:3\"><c r=\"A11\" t=\"s\"><v>1</v></c></row>",
);
/// Shape copied from the original `two-anchors-shared-metadata` fixture.
pub const TWO_ANCHOR_ROWS: &str = concat!(
    "<row r=\"1\" spans=\"1:6\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\"><v>4</v></c></row>",
    "<row r=\"2\" spans=\"1:6\"><c r=\"C2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"C2:C4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c><c r=\"F2\" s=\"1\" cm=\"1\"><f t=\"array\" ref=\"F2:F4\">_xlfn.SEQUENCE($B$1)</f><v>1</v></c></row>",
    "<row r=\"3\" spans=\"1:6\"><c r=\"C3\" s=\"1\"><v>2</v></c><c r=\"F3\" s=\"1\"><v>2</v></c></row>",
    "<row r=\"4\" spans=\"1:6\"><c r=\"C4\" s=\"1\"><v>3</v></c><c r=\"F4\" s=\"1\"><v>3</v></c></row>",
    "<row r=\"9\" spans=\"1:6\"><c r=\"C9\"><f>SUM(C2#)</f><v>99</v></c></row>",
    "<row r=\"10\" spans=\"1:6\"><c r=\"C10\" cm=\"1\"><f t=\"array\" ref=\"C10\">SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c></row>",
    "<row r=\"11\" spans=\"1:6\"><c r=\"A11\" t=\"s\"><v>1</v></c></row>",
);

/// One XLDAPR future block per entry (`fCollapsed` as given) and one cell
/// block per future block, in order.
pub fn metadata_part(collapsed: &[bool]) -> String {
    let future: String = collapsed
        .iter()
        .map(|c| format!("<bk><extLst><ext uri=\"{XLDAPR_URI}\"><xda:dynamicArrayProperties fDynamic=\"1\" fCollapsed=\"{}\"/></ext></extLst></bk>", u8::from(*c)))
        .collect();
    let cells: String = (0..collapsed.len())
        .map(|v| format!("<bk><rc t=\"1\" v=\"{v}\"/></bk>"))
        .collect();
    let n = collapsed.len();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<metadata xmlns=\"{MAIN}\" xmlns:xda=\"{DYNAMIC}\"><metadataTypes count=\"1\"><metadataType name=\"XLDAPR\" minSupportedVersion=\"120000\" copy=\"1\" pasteAll=\"1\" pasteValues=\"1\" merge=\"1\" splitFirst=\"1\" rowColShift=\"1\" clearFormats=\"1\" clearComments=\"1\" assign=\"1\" coerce=\"1\" cellMeta=\"1\"/></metadataTypes><futureMetadata name=\"XLDAPR\" count=\"{n}\">{future}</futureMetadata><cellMetadata count=\"{n}\">{cells}</cellMetadata></metadata>"
    )
}
pub fn worksheet(dimension: &str, rows: &str, tail: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<worksheet xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><dimension ref=\"{dimension}\"/><sheetViews><sheetView tabSelected=\"1\" workbookViewId=\"0\"/></sheetViews><sheetFormatPr defaultRowHeight=\"15\"/><sheetData>{rows}</sheetData>{tail}<pageMargins left=\"0.7\" right=\"0.7\" top=\"0.75\" bottom=\"0.75\" header=\"0.3\" footer=\"0.3\"/><legacyDrawing r:id=\"rId1\"/></worksheet>"
    )
}
/// Producer-shaped package: sheetMetadata relationship/content type,
/// shared strings, styles, comments/VML sheet relationships and an opaque
/// member.
pub fn package(dimension: &str, rows: &str, tail: &str) -> Parts {
    [
        (TYPES, format!("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Types xmlns=\"{CONTENT_TYPES}\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Default Extension=\"vml\" ContentType=\"application/vnd.openxmlformats-officedocument.vmlDrawing\"/><Default Extension=\"bin\" ContentType=\"application/octet-stream\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/><Override PartName=\"/xl/comments1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.comments+xml\"/><Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/><Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>{METADATA_OVERRIDE}</Types>")),
        ("_rels/.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>")),
        ("xl/workbook.xml", format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>")),
        (WB_RELS, format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"rId3\" Type=\"{OFFICE}/styles\" Target=\"styles.xml\"/><Relationship Id=\"rId4\" Type=\"{OFFICE}/sharedStrings\" Target=\"sharedStrings.xml\"/>{METADATA_REL}</Relationships>")),
        ("xl/sharedStrings.xml", format!("<sst xmlns=\"{MAIN}\" count=\"2\" uniqueCount=\"2\"><si><t>n</t></si><si><t>tail</t></si></sst>")),
        ("xl/styles.xml", format!("<styleSheet xmlns=\"{MAIN}\"><fonts count=\"1\"><font/></fonts><fills count=\"1\"><fill/></fills><borders count=\"1\"><border/></borders><cellXfs count=\"2\"><xf numFmtId=\"0\"/><xf numFmtId=\"0\" applyFont=\"1\"/></cellXfs></styleSheet>")),
        (SHEET, worksheet(dimension, rows, tail)),
        ("xl/worksheets/_rels/sheet1.xml.rels", format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/vmlDrawing\" Target=\"../drawings/vmlDrawing1.vml\"/><Relationship Id=\"rId2\" Type=\"{OFFICE}/comments\" Target=\"../comments1.xml\"/></Relationships>")),
        ("xl/comments1.xml", format!("<comments xmlns=\"{MAIN}\"><authors><author>a</author></authors><commentList/></comments>")),
        ("xl/drawings/vmlDrawing1.vml", "<xml/>".to_owned()),
        ("custom/opaque.bin", "opaque payload, stored".to_owned()),
        (METADATA, metadata_part(&[false])),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}
pub fn producer() -> Parts {
    package("A1:C11", PRODUCER_ROWS, "")
}
pub fn producer_wide() -> Parts {
    package("A1:H11", PRODUCER_ROWS, "")
}
pub fn two_anchors() -> Parts {
    package("A1:F11", TWO_ANCHOR_ROWS, "")
}
/// Remove the metadata part, its relationship and its content type.
pub fn without_metadata(p: Parts) -> Parts {
    let mut p = edit(p, WB_RELS, METADATA_REL, "");
    p.remove(METADATA);
    edit(p, TYPES, METADATA_OVERRIDE, "")
}
/// The parent `new-vertical` shape: an ordinary formula `C2 =
/// SEQUENCE($B$1)` that spills, in a package without dynamic metadata.
pub fn new_vertical() -> Parts {
    let rows = concat!(
        "<row r=\"1\" spans=\"1:3\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\"><v>3</v></c></row>",
        "<row r=\"2\" spans=\"1:3\"><c r=\"C2\" s=\"1\"><f>_xlfn.SEQUENCE($B$1)</f><v>99</v></c></row>",
        "<row r=\"9\" spans=\"1:3\"><c r=\"C9\"><f>SUM(C2#)</f><v>99</v></c></row>",
        "<row r=\"10\" spans=\"1:3\"><c r=\"C10\"><f>SUM(_xlfn.ANCHORARRAY(C2))</f><v>99</v></c></row>",
        "<row r=\"11\" spans=\"1:3\"><c r=\"A11\" t=\"s\"><v>1</v></c></row>",
    );
    without_metadata(package("A1:C11", rows, ""))
}
/// Deflated members except the opaque binary (stored), plus an archive
/// comment, with a fixed timestamp.
pub fn pack(parts: &Parts) -> Vec<u8> {
    let mut z = zip::ZipWriter::new(Cursor::new(Vec::new()));
    z.set_comment(ARCHIVE_COMMENT);
    let deflated = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .last_modified_time(zip::DateTime::from_date_and_time(2020, 1, 2, 3, 4, 6).unwrap());
    for (name, body) in parts {
        let options = if name.ends_with(".bin") {
            deflated.compression_method(zip::CompressionMethod::Stored)
        } else {
            deflated
        };
        z.start_file(name.as_str(), options).unwrap();
        z.write_all(body.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}
/// Replace an exact fragment; a mutation that matches nothing is a test bug.
pub fn edit(mut p: Parts, name: &str, old: &str, new: &str) -> Parts {
    let part = p.get_mut(name).unwrap_or_else(|| panic!("missing {name}"));
    assert!(part.contains(old), "{name} lacks {old:?}");
    *part = part.replacen(old, new, 1);
    p
}
/// Every member, read through the `zip` crate (CRC-checked).
pub fn unpack(bytes: &[u8]) -> Parts {
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
pub fn sheet_xml(bytes: &[u8]) -> String {
    unpack(bytes).remove(SHEET).expect("worksheet")
}

/// One independently parsed cell.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct XCell {
    pub attrs: BTreeMap<String, String>,
    pub f: Option<BTreeMap<String, String>>,
    pub formula: String,
    pub v: Option<String>,
    pub inline: bool,
}
#[derive(Debug, Default)]
pub struct XSheet {
    pub dimension: Option<String>,
    /// `(r, attributes, cell refs in order)`.
    pub rows: Vec<(u32, BTreeMap<String, String>, Vec<String>)>,
    pub cells: BTreeMap<String, XCell>,
}
impl XSheet {
    pub fn cell(&self, r: &str) -> &XCell {
        self.cells.get(r).unwrap_or_else(|| panic!("no cell {r}"))
    }
    pub fn row_numbers(&self) -> Vec<u32> {
        self.rows.iter().map(|r| r.0).collect()
    }
    pub fn row(&self, n: u32) -> &(u32, BTreeMap<String, String>, Vec<String>) {
        self.rows.iter().find(|r| r.0 == n).expect("row")
    }
}
fn attributes(e: &BytesStart<'_>) -> BTreeMap<String, String> {
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
pub fn parse_sheet(xml: &str) -> XSheet {
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
/// Cached data through Calamine (which cannot read `#SPILL!` caches).
pub fn data(bytes: &[u8], cell: &str) -> Data {
    let (r, c, _, _) = formualizer_common::coord::parse_a1_1based(cell).unwrap();
    let mut x = Xlsx::new(Cursor::new(bytes)).unwrap();
    x.worksheet_range_at(0)
        .unwrap()
        .unwrap()
        .get_value((r - 1, c - 1))
        .cloned()
        .unwrap_or(Data::Empty)
}

/// A namespace-resolved element: `(namespace, local name, attributes by
/// (namespace, local name))`, plus its depth.
#[derive(Debug, Clone)]
struct NsElement {
    depth: usize,
    ns: String,
    local: String,
    attrs: BTreeMap<(String, String), String>,
}
impl NsElement {
    fn attr(&self, local: &str) -> Option<&str> {
        self.attrs
            .get(&(String::new(), local.to_owned()))
            .map(String::as_str)
    }
    fn is(&self, ns: &str, local: &str) -> bool {
        self.ns == ns && self.local == local
    }
}
fn namespace(r: ResolveResult<'_>) -> String {
    match r {
        ResolveResult::Bound(ns) => String::from_utf8(ns.as_ref().to_vec()).unwrap(),
        ResolveResult::Unbound => String::new(),
        ResolveResult::Unknown(p) => panic!("unbound prefix {p:?}"),
    }
}
/// Every start/empty element in document order, namespace-resolved.
fn ns_elements(xml: &str) -> Vec<NsElement> {
    let mut reader = NsReader::from_str(xml);
    let mut depth = 0;
    let mut out = Vec::new();
    loop {
        let event = reader.read_event().unwrap();
        match &event {
            Event::Start(e) | Event::Empty(e) => {
                let (ns, local) = reader.resolver().resolve_element(e.name());
                let mut attrs = BTreeMap::new();
                for a in e.attributes() {
                    let a = a.unwrap();
                    let key = a.key.as_ref();
                    if key == b"xmlns" || key.starts_with(b"xmlns:") {
                        continue;
                    }
                    let (ans, alocal) = reader.resolver().resolve_attribute(a.key);
                    attrs.insert(
                        (
                            namespace(ans),
                            String::from_utf8(alocal.as_ref().to_vec()).unwrap(),
                        ),
                        a.decode_and_unescape_value(reader.decoder())
                            .unwrap()
                            .into_owned(),
                    );
                }
                out.push(NsElement {
                    depth,
                    ns: namespace(ns),
                    local: String::from_utf8(local.as_ref().to_vec()).unwrap(),
                    attrs,
                });
                if matches!(event, Event::Start(_)) {
                    depth += 1;
                }
            }
            Event::End(_) => depth -= 1,
            Event::Eof => break,
            _ => {}
        }
    }
    out
}

/// A worksheet cell resolved with namespace scoping: the expanded namespace
/// of the `c` element and of each direct child (`(namespace, local)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NsCell {
    pub ns: String,
    pub children: Vec<(String, String)>,
}
/// Every element named `c` (in any namespace) carrying `r`, keyed by `r`.
pub fn ns_cells(xml: &str) -> BTreeMap<String, NsCell> {
    let elements = ns_elements(xml);
    let mut out = BTreeMap::new();
    for (i, e) in elements.iter().enumerate() {
        let (true, Some(r)) = (e.local == "c", e.attr("r")) else {
            continue;
        };
        let children = elements[i + 1..]
            .iter()
            .take_while(|x| x.depth > e.depth)
            .filter(|x| x.depth == e.depth + 1)
            .map(|x| (x.ns.clone(), x.local.clone()))
            .collect();
        out.insert(
            r.to_owned(),
            NsCell {
                ns: e.ns.clone(),
                children,
            },
        );
    }
    out
}

/// The resolved dynamic-array metadata chain of one cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    /// One-based `c/@cm`.
    pub cm: u32,
    /// One-based `rc/@t` (metadata type).
    pub metadata_type: u32,
    /// Zero-based `rc/@v` (XLDAPR future block).
    pub future_block: u32,
    pub collapsed: bool,
    /// The workbook relationship ID of the sheet metadata part.
    pub relationship: String,
}

/// Resolve `cell`'s chain with namespace-aware parsing: worksheet `c/@cm`
/// (one-based) -> `cellMetadata/bk` -> `rc/@t` (one-based) naming the
/// `XLDAPR` metadata type -> `rc/@v` (zero-based) into the XLDAPR
/// `futureMetadata` blocks -> exactly one `ext` with the XLDAPR URI holding
/// a 2017 dynamic-array `dynamicArrayProperties` with `fDynamic="1"`. The
/// part must be the single sheetMetadata relationship target of the
/// workbook and carry the sheetMetadata content-type override.
pub fn metadata_chain(bytes: &[u8], cell: &str) -> Chain {
    let parts = unpack(bytes);
    // Worksheet cell.
    let sheet = ns_elements(&parts[SHEET]);
    let c = sheet
        .iter()
        .find(|e| e.is(MAIN, "c") && e.attr("r") == Some(cell))
        .unwrap_or_else(|| panic!("no cell {cell}"));
    let cm: u32 = c.attr("cm").expect("cm").parse().unwrap();
    // Workbook relationship.
    let rels: Vec<_> = ns_elements(&parts[WB_RELS])
        .into_iter()
        .filter(|e| e.is(RELS, "Relationship") && e.attr("Type") == Some(SHEET_METADATA_REL))
        .collect();
    assert_eq!(rels.len(), 1, "one sheetMetadata relationship");
    let target = rels[0].attr("Target").unwrap();
    let part = target
        .strip_prefix('/')
        .map(str::to_owned)
        .unwrap_or_else(|| format!("xl/{target}"));
    assert_eq!(part, METADATA);
    let ids: Vec<_> = ns_elements(&parts[WB_RELS])
        .iter()
        .filter_map(|e| e.attr("Id").map(str::to_owned))
        .collect();
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), ids.len(), "unique relationship IDs");
    // Content type.
    let overrides: Vec<_> = ns_elements(&parts[TYPES])
        .into_iter()
        .filter(|e| {
            e.is(CONTENT_TYPES, "Override")
                && e.attr("PartName")
                    .is_some_and(|p| p.eq_ignore_ascii_case("/xl/metadata.xml"))
        })
        .collect();
    assert_eq!(overrides.len(), 1, "one metadata content-type override");
    assert_eq!(overrides[0].attr("ContentType"), Some(SHEET_METADATA_TYPE));
    // Metadata part.
    let m = ns_elements(&parts[&part]);
    assert!(m[0].is(MAIN, "metadata"), "metadata root");
    let types: Vec<_> = m
        .iter()
        .filter(|e| e.is(MAIN, "metadataType"))
        .map(|e| e.attr("name").unwrap().to_owned())
        .collect();
    // futureMetadata name="XLDAPR" blocks: each bk's properties.
    let mut future = Vec::new();
    let mut in_future = None;
    let mut cells = Vec::new();
    let mut in_cells = None;
    for (i, e) in m.iter().enumerate() {
        if in_future.is_some_and(|d| e.depth <= d) {
            in_future = None;
        }
        if in_cells.is_some_and(|d| e.depth <= d) {
            in_cells = None;
        }
        if e.is(MAIN, "futureMetadata") && e.attr("name") == Some("XLDAPR") {
            in_future = Some(e.depth);
        }
        if e.is(MAIN, "cellMetadata") {
            in_cells = Some(e.depth);
        }
        if let Some(d) = in_future
            && e.is(MAIN, "bk")
            && e.depth == d + 1
        {
            let block: Vec<_> = m[i + 1..]
                .iter()
                .take_while(|x| x.depth > e.depth)
                .collect();
            let exts: Vec<_> = block.iter().filter(|x| x.is(MAIN, "ext")).collect();
            assert_eq!(exts.len(), 1, "one extension per future block");
            assert_eq!(exts[0].attr("uri"), Some(XLDAPR_URI));
            let props: Vec<_> = block
                .iter()
                .filter(|x| x.is(DYNAMIC, "dynamicArrayProperties"))
                .collect();
            assert_eq!(props.len(), 1, "one dynamicArrayProperties");
            assert_eq!(props[0].attr("fDynamic"), Some("1"));
            future.push(props[0].attr("fCollapsed") == Some("1"));
        }
        if let Some(d) = in_cells
            && e.is(MAIN, "rc")
            && e.depth == d + 2
        {
            cells.push((
                e.attr("t").unwrap().parse::<u32>().unwrap(),
                e.attr("v").unwrap().parse::<u32>().unwrap(),
            ));
        }
    }
    for (section, actual) in [
        ("futureMetadata", future.len()),
        ("cellMetadata", cells.len()),
    ] {
        let declared = m.iter().find(|e| e.is(MAIN, section)).unwrap();
        if let Some(count) = declared.attr("count") {
            assert_eq!(count.parse::<usize>().unwrap(), actual, "{section} count");
        }
    }
    let (t, v) = cells[cm as usize - 1];
    assert_eq!(types[t as usize - 1], "XLDAPR");
    Chain {
        cm,
        metadata_type: t,
        future_block: v,
        collapsed: future[v as usize],
        relationship: rels[0].attr("Id").unwrap().to_owned(),
    }
}

/// One worksheet for [`book`]: `(A1 reference, <c> element)` cells in any
/// order, per-row attribute text, and XML placed before `sheetData`.
#[derive(Default)]
pub struct Ws {
    pub name: String,
    pub cells: Vec<(String, String)>,
    pub row_attrs: Vec<(u32, String)>,
    pub pre: String,
    pub tables: Vec<String>,
}
impl Ws {
    pub fn new(name: &str, cells: Vec<(String, String)>) -> Self {
        Self {
            name: name.to_owned(),
            cells,
            ..Default::default()
        }
    }
    pub fn table(mut self, xml: String) -> Self {
        self.tables.push(xml);
        self
    }
    pub fn row_attr(mut self, row: u32, attrs: &str) -> Self {
        self.row_attrs.push((row, attrs.to_owned()));
        self
    }
    pub fn pre(mut self, xml: &str) -> Self {
        self.pre = xml.to_owned();
        self
    }
}
pub fn num(r: &str, v: f64) -> (String, String) {
    (r.to_owned(), format!("<c r=\"{r}\"><v>{v}</v></c>"))
}
pub fn text(r: &str, t: &str) -> (String, String) {
    (
        r.to_owned(),
        format!(
            "<c r=\"{r}\" t=\"inlineStr\"><is><t>{}</t></is></c>",
            quick_xml::escape::escape(t)
        ),
    )
}
pub fn formula(r: &str, f: &str) -> (String, String) {
    formula_with(r, f, "")
}
pub fn formula_with(r: &str, f: &str, attrs: &str) -> (String, String) {
    (
        r.to_owned(),
        format!(
            "<c r=\"{r}\"><f{attrs}>{}</f><v>99</v></c>",
            quick_xml::escape::escape(f)
        ),
    )
}
/// A shared-formula follower of `si`.
pub fn follower(r: &str, si: u32) -> (String, String) {
    (
        r.to_owned(),
        format!("<c r=\"{r}\"><f t=\"shared\" si=\"{si}\"/><v>99</v></c>"),
    )
}
pub fn table_xml(id: u32, name: &str, rect: &str, columns: &[&str], totals: bool) -> String {
    let cols: String = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "<tableColumn id=\"{}\" name=\"{}\"/>",
                i + 1,
                quick_xml::escape::escape(*c)
            )
        })
        .collect();
    format!(
        "<table xmlns=\"{MAIN}\" id=\"{id}\" name=\"{name}\" displayName=\"{name}\" ref=\"{rect}\" totalsRowCount=\"{}\"><tableColumns count=\"{}\">{cols}</tableColumns></table>",
        u8::from(totals),
        columns.len()
    )
}
fn a1(r: &str) -> (u32, u32) {
    let letters: String = r.chars().take_while(|c| c.is_ascii_alphabetic()).collect();
    let col = letters
        .bytes()
        .fold(0u32, |n, b| n * 26 + u32::from(b - b'A' + 1));
    (r[letters.len()..].parse().unwrap(), col)
}
/// A minimal multi-sheet package (no styles/shared strings) with optional
/// `<definedName>` elements.
pub fn book(sheets: &[Ws], defined_names: &str) -> Vec<u8> {
    let mut parts = Parts::new();
    let mut types = String::new();
    let mut wb_sheets = String::new();
    let mut wb_rels = String::new();
    let mut table_no = 0;
    for (i, ws) in sheets.iter().enumerate() {
        let i = i + 1;
        let mut rows: BTreeMap<u32, Vec<(u32, &str)>> = BTreeMap::new();
        for (r, xml) in &ws.cells {
            let (row, col) = a1(r);
            rows.entry(row).or_default().push((col, xml));
        }
        for (row, _) in &ws.row_attrs {
            rows.entry(*row).or_default();
        }
        let data: String = rows
            .iter_mut()
            .map(|(row, cells)| {
                cells.sort_by_key(|(c, _)| *c);
                let attrs = ws
                    .row_attrs
                    .iter()
                    .find(|(r, _)| r == row)
                    .map(|(_, a)| format!(" {a}"))
                    .unwrap_or_default();
                let body: String = cells.iter().map(|(_, x)| *x).collect();
                format!("<row r=\"{row}\"{attrs}>{body}</row>")
            })
            .collect();
        let mut rels = String::new();
        let mut parts_xml = String::new();
        for (j, t) in ws.tables.iter().enumerate() {
            table_no += 1;
            parts.insert(format!("xl/tables/table{table_no}.xml"), t.clone());
            types.push_str(&format!("<Override PartName=\"/xl/tables/table{table_no}.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml\"/>"));
            rels.push_str(&format!("<Relationship Id=\"rId{}\" Type=\"{OFFICE}/table\" Target=\"../tables/table{table_no}.xml\"/>", j + 1));
            parts_xml.push_str(&format!("<tablePart r:id=\"rId{}\"/>", j + 1));
        }
        if !ws.tables.is_empty() {
            parts.insert(
                format!("xl/worksheets/_rels/sheet{i}.xml.rels"),
                format!("<Relationships xmlns=\"{RELS}\">{rels}</Relationships>"),
            );
            parts_xml = format!(
                "<tableParts count=\"{}\">{parts_xml}</tableParts>",
                ws.tables.len()
            );
        }
        parts.insert(
            format!("xl/worksheets/sheet{i}.xml"),
            format!(
                "<worksheet xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\">{}<sheetData>{data}</sheetData>{parts_xml}</worksheet>",
                ws.pre
            ),
        );
        types.push_str(&format!("<Override PartName=\"/xl/worksheets/sheet{i}.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"));
        wb_rels.push_str(&format!("<Relationship Id=\"rId{i}\" Type=\"{OFFICE}/worksheet\" Target=\"worksheets/sheet{i}.xml\"/>"));
        wb_sheets.push_str(&format!(
            "<sheet name=\"{}\" sheetId=\"{i}\" r:id=\"rId{i}\"/>",
            quick_xml::escape::escape(ws.name.as_str())
        ));
    }
    let names = if defined_names.is_empty() {
        String::new()
    } else {
        format!("<definedNames>{defined_names}</definedNames>")
    };
    parts.insert(TYPES.into(), format!("<Types xmlns=\"{CONTENT_TYPES}\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>{types}</Types>"));
    parts.insert("_rels/.rels".into(), format!("<Relationships xmlns=\"{RELS}\"><Relationship Id=\"rId1\" Type=\"{OFFICE}/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>"));
    parts.insert("xl/workbook.xml".into(), format!("<workbook xmlns=\"{MAIN}\" xmlns:r=\"{OFFICE}\"><sheets>{wb_sheets}</sheets>{names}</workbook>"));
    parts.insert(
        WB_RELS.into(),
        format!("<Relationships xmlns=\"{RELS}\">{wb_rels}</Relationships>"),
    );
    pack(&parts)
}
/// Cached `<v>` text of `cell` on worksheet `index` (1-based).
pub fn value_at(bytes: &[u8], index: usize, cell: &str) -> Option<String> {
    let xml = unpack(bytes)
        .remove(&format!("xl/worksheets/sheet{index}.xml"))
        .expect("worksheet");
    parse_sheet(&xml).cell(cell).v.clone()
}
