//! Namespace-validated XLDAPR dynamic-array metadata and prior-footprint
//! ownership. Parse-only: nothing here edits source bytes.
//!
//! Index bases (ECMA-376 Part 1 §18.9 plus the MS-XLSX XLDAPR extension):
//! * `c/@cm` is ONE-based into `cellMetadata/bk`;
//! * `rc/@t` is ONE-based into `metadataTypes/metadataType`;
//! * `rc/@v` is ZERO-based into the `bk` list of the `futureMetadata` block
//!   whose `name` equals the selected metadata type name.
use super::sheet::{Cell, SourceIndex, SourceRect};
use super::{IoError, XlsxRecalculateOptions, checkpoint, package, unsupported, xml};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

pub(super) const SHEET_METADATA_RELATIONSHIP: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sheetMetadata";
pub(super) const SHEET_METADATA_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheetMetadata+xml";
pub(super) const DYNAMIC_ARRAY_NS: &str =
    "http://schemas.microsoft.com/office/spreadsheetml/2017/dynamicarray";
pub(super) const XLDAPR_URI: &str = "{bdbb8cdc-fa1e-496e-a857-3c3f30c029c3}";
const XLDAPR: &str = "XLDAPR";
const CONTEXT: &str = "sheet metadata";

/// A resolved `cm` binding. Several anchors may share one binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DynamicBinding {
    /// One-based `c/@cm` value (cellMetadata block).
    pub cell_metadata: u32,
    /// One-based `rc/@t` value (metadataType).
    pub metadata_type: u32,
    /// Zero-based `rc/@v` value (XLDAPR futureMetadata block).
    pub future_block: u32,
    /// `dynamicArrayProperties/@fCollapsed` of that shared block.
    pub collapsed: bool,
}
/// The validated metadata part. Every cellMetadata block resolves to an
/// XLDAPR block with `fDynamic="1"`; anything else was refused at parse.
#[derive(Debug)]
pub(super) struct DynamicMetadata {
    /// Package part name, relationship-resolved from the workbook.
    #[allow(dead_code)] // edited by the metadata packet
    pub part: String,
    blocks: Vec<DynamicBinding>,
}
impl DynamicMetadata {
    pub fn resolve(&self, cm: u32) -> Result<DynamicBinding, IoError> {
        cm.checked_sub(1)
            .and_then(|i| self.blocks.get(i as usize))
            .copied()
            .ok_or_else(|| {
                unsupported(
                    "dangling or invalid dynamic cell metadata index (cm)",
                    "worksheet",
                )
            })
    }
}
/// A source-declared dynamic-array anchor and its prior footprint.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // consumed by the ingestion/geometry packets
pub(super) struct DynamicAnchor {
    pub row: u32,
    pub col: u32,
    /// Index into the sheet scan's formula cells.
    pub formula: usize,
    /// Declared `f/@ref`; the anchor is its top-left cell.
    pub footprint: SourceRect,
    pub binding: DynamicBinding,
}
/// Disjoint anchor -> prior footprint map for one worksheet, plus the mask
/// set of proven generated child cells (serialized cells only, anchor
/// excluded). Keys are one-based `(row, col)`.
#[derive(Debug, Default)]
pub(super) struct SheetOwnership {
    pub anchors: BTreeMap<(u32, u32), DynamicAnchor>,
    /// Child cell -> owning anchor.
    pub children: BTreeMap<(u32, u32), (u32, u32)>,
}
#[allow(dead_code)]
impl SheetOwnership {
    pub fn owner_of(&self, row: u32, col: u32) -> Option<(u32, u32)> {
        self.children.get(&(row, col)).copied()
    }
}

fn flag(value: Option<&str>) -> Result<bool, IoError> {
    match value {
        None | Some("0" | "false") => Ok(false),
        Some("1" | "true") => Ok(true),
        Some(_) => Err(unsupported("invalid dynamic array property", CONTEXT)),
    }
}
fn count(node: &xml::Node) -> Result<Option<usize>, IoError> {
    node.value("count")
        .map(|v| {
            v.parse::<usize>()
                .map_err(|_| unsupported("invalid sheet metadata count", CONTEXT))
        })
        .transpose()
}
#[derive(Default)]
struct FutureBlock {
    ext: usize,
    extensions: usize,
    properties: Option<(bool, bool)>,
}
#[derive(Default)]
struct Parsed {
    types: Vec<String>,
    types_section: Option<Option<usize>>,
    future: Vec<FutureBlock>,
    future_section: Option<Option<usize>>,
    cells: Vec<Vec<(String, String)>>,
    cells_section: Option<Option<usize>>,
}
/// Parse and fully validate the relationship-resolved metadata part.
pub(super) fn parse(
    archive: &mut package::Archive<'_>,
    part: &str,
    options: &XlsxRecalculateOptions,
) -> Result<DynamicMetadata, IoError> {
    let data = package::read_part(archive, part, options.limits.max_worksheet_bytes)?;
    parse_bytes(&data, part, options)
}
fn parse_bytes(
    data: &[u8],
    part: &str,
    options: &XlsxRecalculateOptions,
) -> Result<DynamicMetadata, IoError> {
    const MAIN_NAMES: [&str; 12] = [
        "metadata",
        "metadataTypes",
        "metadataType",
        "metadataStrings",
        "mdxMetadata",
        "futureMetadata",
        "cellMetadata",
        "valueMetadata",
        "bk",
        "rc",
        "extLst",
        "ext",
    ];
    let limit = options.limits.max_cells;
    let mut p = Parsed::default();
    xml::walk(data, options, |path, node| {
        let names: Vec<&str> = path.iter().map(|e| e.local.as_str()).collect();
        match &node.kind {
            xml::Kind::Open { attributes, .. } => {
                let e = path.last().expect("open XML element");
                if path.len() == 1 && !xml::path_is(path, xml::MAIN, &["metadata"]) {
                    return Err(unsupported("sheet metadata root/namespace", CONTEXT));
                }
                if MAIN_NAMES.contains(&e.local.as_str()) && e.ns != xml::MAIN {
                    return Err(unsupported("foreign sheet metadata lookalike", &e.local));
                }
                if e.local == "dynamicArrayProperties" && e.ns != DYNAMIC_ARRAY_NS {
                    return Err(unsupported(
                        "foreign dynamic array metadata lookalike",
                        &e.local,
                    ));
                }
                match names.as_slice() {
                    ["metadata"] => {}
                    ["metadata", "metadataTypes"] => {
                        if p.types_section.replace(count(&node)?).is_some() {
                            return Err(unsupported("duplicate metadataTypes", CONTEXT));
                        }
                    }
                    ["metadata", "metadataTypes", "metadataType"] => {
                        // Only one XLDAPR type is interpreted; any other type
                        // (rich values, MDX, …) has no supported semantics.
                        if node.required("name")? != XLDAPR || !p.types.is_empty() {
                            return Err(unsupported("unsupported metadata type", CONTEXT));
                        }
                        p.types.push(XLDAPR.to_owned());
                    }
                    ["metadata", "futureMetadata"] => {
                        if node.required("name")? != XLDAPR {
                            return Err(unsupported("unsupported future metadata", CONTEXT));
                        }
                        if p.future_section.replace(count(&node)?).is_some() {
                            return Err(unsupported("duplicate XLDAPR future metadata", CONTEXT));
                        }
                    }
                    ["metadata", "futureMetadata", "bk"] => {
                        if p.future.len() >= limit {
                            return Err(unsupported("sheet metadata record limit", CONTEXT));
                        }
                        p.future.push(FutureBlock::default());
                    }
                    ["metadata", "futureMetadata", "bk", "extLst"] => {
                        let block = p.future.last_mut().expect("open bk");
                        block.ext += 1;
                        if block.ext != 1 {
                            return Err(unsupported("duplicate future metadata extLst", CONTEXT));
                        }
                    }
                    ["metadata", "futureMetadata", "bk", "extLst", "ext"] => {
                        if node.value("uri") != Some(XLDAPR_URI) {
                            return Err(unsupported(
                                "unknown sheet metadata extension URI",
                                CONTEXT,
                            ));
                        }
                        let block = p.future.last_mut().expect("open bk");
                        block.extensions += 1;
                        if block.extensions != 1 {
                            return Err(unsupported("duplicate XLDAPR extension", CONTEXT));
                        }
                    }
                    [
                        "metadata",
                        "futureMetadata",
                        "bk",
                        "extLst",
                        "ext",
                        "dynamicArrayProperties",
                    ] => {
                        if attributes.iter().any(|a| {
                            !a.ns.is_empty()
                                || !matches!(a.local.as_str(), "fDynamic" | "fCollapsed")
                        }) {
                            return Err(unsupported("unknown dynamic array property", CONTEXT));
                        }
                        let props = (
                            flag(node.value("fDynamic"))?,
                            flag(node.value("fCollapsed"))?,
                        );
                        if p.future
                            .last_mut()
                            .expect("open bk")
                            .properties
                            .replace(props)
                            .is_some()
                        {
                            return Err(unsupported("duplicate dynamic array properties", CONTEXT));
                        }
                    }
                    ["metadata", "cellMetadata"] => {
                        if p.cells_section.replace(count(&node)?).is_some() {
                            return Err(unsupported("duplicate cellMetadata", CONTEXT));
                        }
                    }
                    ["metadata", "cellMetadata", "bk"] => {
                        if p.cells.len() >= limit {
                            return Err(unsupported("sheet metadata record limit", CONTEXT));
                        }
                        p.cells.push(Vec::new());
                    }
                    ["metadata", "cellMetadata", "bk", "rc"] => {
                        let record = (
                            node.required("t")?.to_owned(),
                            node.required("v")?.to_owned(),
                        );
                        let block = p.cells.last_mut().expect("open bk");
                        block.push(record);
                        if block.len() != 1 {
                            // One cell binding per block: XLDAPR only.
                            return Err(unsupported("multi-record cell metadata block", CONTEXT));
                        }
                    }
                    _ => {
                        return Err(unsupported("unsupported sheet metadata element", &e.local));
                    }
                }
            }
            xml::Kind::Text(text) => {
                if !text.trim().is_empty() {
                    return Err(unsupported("unexpected sheet metadata text", CONTEXT));
                }
            }
            xml::Kind::Close => {}
        }
        Ok(())
    })?;
    for (section, actual) in [
        (p.types_section, p.types.len()),
        (p.future_section, p.future.len()),
        (p.cells_section, p.cells.len()),
    ] {
        if section.flatten().is_some_and(|n| n != actual) {
            return Err(unsupported("sheet metadata count mismatch", CONTEXT));
        }
    }
    let mut properties = Vec::with_capacity(p.future.len());
    for block in &p.future {
        let (dynamic, collapsed) = block
            .properties
            .ok_or_else(|| unsupported("XLDAPR block without dynamic array properties", CONTEXT))?;
        if !dynamic {
            return Err(unsupported("non-dynamic XLDAPR block", CONTEXT));
        }
        properties.push(collapsed);
    }
    let mut blocks = Vec::with_capacity(p.cells.len());
    for (i, block) in p.cells.iter().enumerate() {
        let (t, v) = block
            .first()
            .ok_or_else(|| unsupported("empty cell metadata block", CONTEXT))?;
        let metadata_type = t
            .parse::<u32>()
            .ok()
            .filter(|t| (1..=p.types.len()).contains(&(*t as usize)))
            .ok_or_else(|| unsupported("out-of-range metadata type index", CONTEXT))?;
        // The only admitted type is XLDAPR, so `t` selects the XLDAPR block list.
        let future_block = v
            .parse::<u32>()
            .ok()
            .filter(|v| (*v as usize) < properties.len())
            .ok_or_else(|| unsupported("out-of-range future metadata block index", CONTEXT))?;
        blocks.push(DynamicBinding {
            cell_metadata: u32::try_from(i + 1)
                .map_err(|_| unsupported("sheet metadata record limit", CONTEXT))?,
            metadata_type,
            future_block,
            collapsed: properties[future_block as usize],
        });
    }
    Ok(DynamicMetadata {
        part: part.to_owned(),
        blocks,
    })
}

/// Build the disjoint prior-footprint ownership of one worksheet and apply
/// the literal-cache refusals that the scan deferred until classification.
pub(super) fn own_sheet(
    index: &SourceIndex,
    cells: &[Cell],
    metadata: Option<&DynamicMetadata>,
    options: &XlsxRecalculateOptions,
) -> Result<SheetOwnership, IoError> {
    let mut own = SheetOwnership::default();
    for (i, cell) in cells.iter().enumerate() {
        if i & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        let Some(cm) = cell.cm else {
            continue;
        };
        // The scan admitted `cm` only on top-left array anchors with a ref.
        let footprint = cell
            .array_ref
            .ok_or_else(|| unsupported("missing dynamic array extent", "worksheet"))?;
        let binding = metadata
            .ok_or_else(|| {
                unsupported(
                    "dangling or invalid dynamic cell metadata index (cm)",
                    "worksheet",
                )
            })?
            .resolve(cm)?;
        if binding.collapsed && footprint.cell_count() != Some(1) {
            return Err(unsupported(
                "collapsed dynamic array block with a multi-cell extent",
                "worksheet",
            ));
        }
        own.anchors.insert(
            (cell.row, cell.col),
            DynamicAnchor {
                row: cell.row,
                col: cell.col,
                formula: i,
                footprint,
                binding,
            },
        );
    }
    // Row sweep over anchors (already ordered by top row): active column
    // intervals are pairwise disjoint, so only the nearest-left interval can
    // intersect a new one.
    let mut active: BTreeMap<u32, (u32, (u32, u32))> = BTreeMap::new();
    let mut expiry = BinaryHeap::new();
    for (n, (key, anchor)) in own.anchors.iter().enumerate() {
        if n & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        let rect = anchor.footprint;
        while let Some(Reverse((last_row, first_col, owner))) = expiry.peek().copied() {
            if last_row >= rect.first_row {
                break;
            }
            expiry.pop();
            if active.get(&first_col).is_some_and(|(_, k)| *k == owner) {
                active.remove(&first_col);
            }
        }
        if active
            .range(..=rect.last_col)
            .next_back()
            .is_some_and(|(_, (last_col, _))| *last_col >= rect.first_col)
        {
            return Err(unsupported(
                "overlapping dynamic array extents",
                "worksheet",
            ));
        }
        active.insert(rect.first_col, (rect.last_col, *key));
        expiry.push(Reverse((rect.last_row, rect.first_col, *key)));
    }
    // One row-major pass over serialized cells with the same sweep.
    let mut active: BTreeMap<u32, (u32, u32)> = BTreeMap::new();
    let mut expiry = BinaryHeap::new();
    let mut pending = own.anchors.iter().peekable();
    for (n, cell) in index.cells.iter().enumerate() {
        if n & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        // Expire before activating so a reused first column is never removed
        // on behalf of an earlier anchor; skip anchors with no remaining rows.
        while let Some(Reverse((last_row, first_col, owner))) = expiry.peek().copied() {
            if last_row >= cell.row {
                break;
            }
            expiry.pop();
            if active.get(&first_col) == Some(&owner) {
                active.remove(&first_col);
            }
        }
        while let Some((key, anchor)) = pending.peek() {
            if anchor.footprint.first_row > cell.row {
                break;
            }
            let rect = anchor.footprint;
            if rect.last_row >= cell.row {
                active.insert(rect.first_col, **key);
                expiry.push(Reverse((rect.last_row, rect.first_col, **key)));
            }
            pending.next();
        }
        let owner = active
            .range(..=cell.col)
            .next_back()
            .map(|(_, key)| *key)
            .filter(|key| own.anchors[key].footprint.contains(cell.row, cell.col));
        match owner {
            Some(key) if key == (cell.row, cell.col) => {}
            Some(key) => {
                if cell.formula.is_some() {
                    return Err(unsupported(
                        "formula inside a dynamic array child footprint",
                        "worksheet",
                    ));
                }
                own.children.insert((cell.row, cell.col), key);
            }
            None => {
                if let Some(refusal) = cell.literal_refusal {
                    return Err(unsupported(refusal, "worksheet"));
                }
            }
        }
    }
    Ok(own)
}
