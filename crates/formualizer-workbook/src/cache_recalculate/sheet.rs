//! Formula locations/cache spans without a rich cell graph. On request
//! (worksheets with dynamic-array anchors or new spills only), the same
//! bounded pass also builds a source index of rows, cells, dimensions and
//! merges for dynamic-array ownership and geometry.
use super::{IoError, XlsxRecalculateOptions, unsupported, xml};
use formualizer_common::coord::parse_a1_1based;
use std::{collections::HashMap, ops::Range};

/// Inclusive source rectangle in one-based worksheet coordinates (row 1 is
/// the first row, column 1 is `A`), matching `Cell::row`/`Cell::col`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SourceRect {
    pub first_row: u32,
    pub first_col: u32,
    pub last_row: u32,
    pub last_col: u32,
}
impl SourceRect {
    pub fn contains(self, row: u32, col: u32) -> bool {
        row >= self.first_row
            && row <= self.last_row
            && col >= self.first_col
            && col <= self.last_col
    }
    pub fn intersects(self, other: Self) -> bool {
        self.first_row <= other.last_row
            && other.first_row <= self.last_row
            && self.first_col <= other.last_col
            && other.first_col <= self.last_col
    }
    pub fn cell_count(self) -> Option<u64> {
        let rows = u64::from(self.last_row.checked_sub(self.first_row)?) + 1;
        let cols = u64::from(self.last_col.checked_sub(self.first_col)?) + 1;
        rows.checked_mul(cols)
    }
    /// Parse an in-grid, non-reversed, relative A1 cell or range reference.
    pub fn parse(value: &str) -> Result<Self, IoError> {
        let (first_row, first_col, last_row, last_col) = rect(value)?;
        Ok(Self {
            first_row,
            first_col,
            last_row,
            last_col,
        })
    }
}

#[derive(Debug)]
pub(super) struct ValueNode {
    pub span: Range<usize>,
    pub open_end: usize,
    pub close_start: usize,
    pub empty: bool,
    pub qualified: String,
    pub text: String,
}
#[derive(Debug)]
pub(super) struct Cell {
    pub row: u32,
    pub col: u32,
    pub address: String,
    pub span: Range<usize>,
    pub open_end: usize,
    pub qualified: String,
    pub kind: Option<String>,
    pub kind_span: Option<Range<usize>>,
    pub formula_end: usize,
    pub formula_text: String,
    pub value: Option<ValueNode>,
    pub inline: Option<Range<usize>>,
    /// `normal`, `shared` or `array`.
    pub formula_kind: &'static str,
    pub shared_id: Option<u32>,
    shared_range: Option<(u32, u32, u32, u32)>,
    has_formula: bool,
    /// `<f …>` start tag (or whole empty element) span.
    pub formula_open: Range<usize>,
    /// `t="…"` attribute span on the formula start tag.
    pub formula_kind_span: Option<Range<usize>>,
    /// `cm` and array-extent attributes, boxed: present only on the rare
    /// cells that carry them.
    dynamic: Option<Box<DynamicAttributes>>,
}
#[derive(Debug, Default)]
struct DynamicAttributes {
    /// One-based cellMetadata index from `c/@cm`.
    cm: Option<u32>,
    /// `cm="…"` attribute span on the cell start tag.
    cm_span: Option<Range<usize>>,
    /// Declared array extent from `f/@ref` for `t="array"` formulas.
    array_ref: Option<SourceRect>,
    /// `ref="…"` attribute span on an array formula start tag.
    array_ref_span: Option<Range<usize>>,
}
impl Cell {
    pub fn shared_rect(&self) -> Option<SourceRect> {
        self.shared_range
            .map(|(first_row, first_col, last_row, last_col)| SourceRect {
                first_row,
                first_col,
                last_row,
                last_col,
            })
    }
    /// One-based cellMetadata index from `c/@cm`.
    pub fn cm(&self) -> Option<u32> {
        self.dynamic.as_ref().and_then(|d| d.cm)
    }
    /// `cm="…"` attribute span on the cell start tag.
    pub fn cm_span(&self) -> Option<&Range<usize>> {
        self.dynamic.as_ref().and_then(|d| d.cm_span.as_ref())
    }
    /// Declared array extent from `f/@ref` for `t="array"` formulas.
    pub fn array_ref(&self) -> Option<SourceRect> {
        self.dynamic.as_ref().and_then(|d| d.array_ref)
    }
    /// `ref="…"` attribute span on an array formula start tag.
    pub fn array_ref_span(&self) -> Option<&Range<usize>> {
        self.dynamic
            .as_ref()
            .and_then(|d| d.array_ref_span.as_ref())
    }
}
/// One serialized worksheet row.
#[derive(Debug)]
pub(super) struct RowEntry {
    pub row: u32,
    /// Start tag through end tag, or the whole self-closing element.
    pub span: Range<usize>,
    pub open_end: usize,
    pub empty: bool,
    pub qualified: String,
    /// Optional `spans="…"` attribute span, retained verbatim.
    pub spans_attr: Option<Range<usize>>,
    /// Index range into `SourceIndex::cells`.
    pub cells: Range<usize>,
}
/// One serialized cell, formula or not.
#[derive(Debug)]
pub(super) struct IndexedCell {
    pub row: u32,
    pub col: u32,
    /// Start tag through end tag, or the whole self-closing element.
    pub span: Range<usize>,
    pub open_end: usize,
    pub empty: bool,
    pub kind: Option<String>,
    pub kind_span: Option<Range<usize>>,
    #[allow(dead_code)] // `cm` outside anchors is refused at scan time
    pub cm: Option<u32>,
    /// Whole `<v>` element span.
    pub value: Option<Range<usize>>,
    /// Whole `<is>` element span.
    pub inline: Option<Range<usize>>,
    /// Index into the scan's formula cells.
    pub formula: Option<usize>,
    /// Literal-cache refusal deferred until ownership is classified: a proven
    /// generated child is masked, any other such literal is still refused.
    pub(super) literal_refusal: Option<&'static str>,
}
/// Bounded source geometry of one worksheet.
#[derive(Debug)]
pub(super) struct SourceIndex {
    /// Declared dimension and the span of its `ref="…"` attribute.
    pub dimension: Option<(SourceRect, Range<usize>)>,
    /// `<sheetData>` start tag through end tag (or self-closing element).
    pub sheet_data: Range<usize>,
    pub sheet_data_open_end: usize,
    pub sheet_data_empty: bool,
    pub rows: Vec<RowEntry>,
    /// Row-major, strictly increasing coordinates.
    pub cells: Vec<IndexedCell>,
    pub merges: Vec<SourceRect>,
}
impl SourceIndex {
    pub fn cell_at(&self, row: u32, col: u32) -> Option<&IndexedCell> {
        self.cells
            .binary_search_by_key(&(row, col), |c| (c.row, c.col))
            .ok()
            .map(|i| &self.cells[i])
    }
}
pub(super) struct Scan {
    pub cells: Vec<Cell>,
    pub table_ids: Vec<String>,
    pub table_merges: Vec<SourceRect>,
    /// Sorted rows hidden by `hidden`, a zero height or a collapsed outline.
    pub hidden_rows: Vec<u32>,
    /// `sheetFormatPr/@zeroHeight`: unlisted rows are hidden by default.
    pub rows_hidden_by_default: bool,
    /// Whether the worksheet declares a `<dimension>`.
    pub has_dimension: bool,
    pub active_filters: Vec<SourceRect>,
    /// Built only when requested.
    pub index: Option<SourceIndex>,
    /// Serialized `<c>` elements.
    pub serialized: usize,
    /// Logical `(max_row, max_col)` counted toward the workbook area bound:
    /// serialized cells, declared array extents and the dimension.
    pub bounds: (u32, u32),
}
/// Calamine's fast scalar reader consumes just one raw ASCII text event.
/// Literal cells must satisfy that assumption; formula caches may instead be
/// cleared in the transient ingestion view because they are not authority.
pub(super) fn readable_scalar_cache(cell: &Cell, bytes: &[u8]) -> bool {
    let Some(v) = &cell.value else {
        return true;
    };
    if !matches!(cell.kind.as_deref(), None | Some("n" | "s" | "b" | "e")) {
        return true;
    }
    if v.empty || v.text.is_empty() {
        return matches!(cell.kind.as_deref(), None | Some("n"));
    }
    if bytes[v.open_end..v.close_start] != *v.text.as_bytes() {
        return false;
    }
    match cell.kind.as_deref() {
        None | Some("n") => v.text.parse::<f64>().is_ok_and(f64::is_finite),
        Some("s") => v.text.parse::<u32>().is_ok(),
        Some("b") => matches!(v.text.as_str(), "0" | "1"),
        Some("e") => v.text.parse::<calamine::CellErrorType>().is_ok(),
        _ => true,
    }
}
fn coord(value: &str) -> Result<(u32, u32), IoError> {
    let (r, c, ra, ca) =
        parse_a1_1based(value).map_err(|_| unsupported("invalid A1 coordinate", "worksheet"))?;
    if ra || ca || r == 0 || r > 1_048_576 || c == 0 || c > 16_384 {
        return Err(unsupported(
            "out-of-grid or absolute cell coordinate",
            "worksheet",
        ));
    }
    Ok((r, c))
}
fn rect(value: &str) -> Result<(u32, u32, u32, u32), IoError> {
    let (a, b) = value.split_once(':').unwrap_or((value, value));
    let (r1, c1) = coord(a)?;
    let (r2, c2) = coord(b)?;
    if r1 > r2 || c1 > c2 {
        return Err(unsupported("reversed shared formula range", "worksheet"));
    }
    Ok((r1, c1, r2, c2))
}
fn integer(value: &str) -> Result<u32, IoError> {
    value
        .parse()
        .map_err(|_| unsupported("invalid integer XML attribute", "worksheet"))
}
fn literal_refusal(cell: &Cell, bytes: &[u8]) -> Option<&'static str> {
    if cell.has_formula {
        return None;
    }
    if cell.kind.as_deref() == Some("e")
        && cell
            .value
            .as_ref()
            .is_none_or(|v| v.text.parse::<calamine::CellErrorType>().is_err())
    {
        return Some("literal error value unsupported by Calamine");
    }
    if !readable_scalar_cache(cell, bytes) {
        return Some("literal scalar payload is not supported by Calamine");
    }
    None
}
/// Dynamic-array cell rules. Ownership through the
/// metadata chain is resolved later; this validates the source encoding.
fn dynamic_anchor_extent(cell: &Cell) -> Result<Option<SourceRect>, IoError> {
    if cell.cm().is_none() && cell.formula_kind != "array" {
        return Ok(None);
    }
    if cell.formula_kind == "shared" {
        return Err(unsupported(
            "shared formula family member claimed as dynamic anchor",
            "worksheet",
        ));
    }
    if cell.formula_kind != "array" {
        return Err(unsupported(
            "cell metadata on a non-dynamic-array cell",
            "worksheet",
        ));
    }
    let extent = cell
        .array_ref()
        .ok_or_else(|| unsupported("missing dynamic array extent", "worksheet"))?;
    if (extent.first_row, extent.first_col) != (cell.row, cell.col) {
        return Err(unsupported(
            "dynamic array anchor is not the top-left of its extent",
            "worksheet",
        ));
    }
    if cell.formula_text.trim().is_empty() {
        return Err(unsupported("empty dynamic array formula", "worksheet"));
    }
    Ok(Some(extent))
}
/// How [`scan`] treats dynamic-array cell metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    /// No source index. A `cm` attribute stops the scan with
    /// [`Scanned::NeedsIndex`]: until the first anchor no cell can be a
    /// generated child (children follow their top-left anchor in document
    /// order), so literal refusals before it are final.
    Plain,
    /// Scalar table admission: retain merge rectangles, not a per-cell index.
    TablePlain,
    /// Build the source index and defer literal refusals to ownership.
    Indexed,
}
// One scan result crosses this boundary per sheet; retain the unboxed
// scalar hot path instead of adding a heap allocation to every worksheet.
#[allow(clippy::large_enum_variant)]
pub(super) enum Scanned {
    Done(Scan),
    /// A `Plain` scan met dynamic cell metadata; rescan `Indexed`.
    NeedsIndex,
}
const NEEDS_INDEX: &str = "dynamic cell metadata needs a source index";
#[cfg(test)]
thread_local! {
    /// Indexed scans on this thread (tests prove scalar workbooks build none).
    pub(super) static INDEXED_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
pub(super) fn scan(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: Mode,
    observed: &mut usize,
    logical_cells: &mut u64,
) -> Result<Scanned, IoError> {
    #[cfg(test)]
    if mode == Mode::Indexed {
        INDEXED_SCANS.with(|n| n.set(n.get() + 1));
    }
    let mut needs_index = false;
    match scan_inner(
        bytes,
        options,
        mode,
        observed,
        logical_cells,
        &mut needs_index,
    ) {
        Err(_) if needs_index => Ok(Scanned::NeedsIndex),
        result => result.map(Scanned::Done),
    }
}
fn scan_inner(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: Mode,
    observed: &mut usize,
    logical_cells: &mut u64,
    needs_index: &mut bool,
) -> Result<Scan, IoError> {
    let mut hidden_rows = Vec::new();
    let mut rows_hidden_by_default = false;
    let mut outline_rows = Vec::new();
    let mut outline_collapsed = false;
    let mut active_filters = Vec::new();
    let mut filter_seen = false;
    let mut filter_ref: Option<SourceRect> = None;
    let mut filter_active = false;
    let mut table_merges = Vec::new();
    let mut table_ids = Vec::new();
    let mut table_count = None;
    let mut serialized = 0usize;
    let mut cells = Vec::new();
    let mut current: Option<Cell> = None;
    let mut row = 0;
    let mut column = 0;
    let mut sheet_data_count = 0;
    let mut dimension = None;
    let mut max_row = 0u32;
    let mut max_col = 0u32;
    let mut index = (mode == Mode::Indexed).then(|| SourceIndex {
        dimension: None,
        sheet_data: 0..0,
        sheet_data_open_end: 0,
        sheet_data_empty: false,
        rows: Vec::new(),
        cells: Vec::new(),
        merges: Vec::new(),
    });
    let mut extent_cells = 0u64;
    xml::walk(bytes, options, |path, node| {
        let Some(element) = path.last() else {
            return Ok(());
        };
        let is_cell = xml::path_is(path, xml::MAIN, &["worksheet", "sheetData", "row", "c"]);
        let direct = path.len() == 5
            && xml::path_is(
                &path[..4],
                xml::MAIN,
                &["worksheet", "sheetData", "row", "c"],
            );
        match &node.kind {
            xml::Kind::Open { empty, .. } => {
                if xml::path_is(path, xml::MAIN, &["worksheet", "sheetFormatPr"])
                    && matches!(node.value("zeroHeight"), Some("1" | "true"))
                {
                    rows_hidden_by_default = true;
                }
                if xml::path_is(path, xml::MAIN, &["worksheet", "autoFilter"]) {
                    if filter_seen {
                        return Err(unsupported("duplicate worksheet autoFilter", "worksheet"));
                    }
                    filter_seen = true;
                    filter_ref = Some(SourceRect::parse(node.required("ref")?)?);
                }
                if path.len() == 4
                    && path[1].ns == xml::MAIN
                    && path[1].local == "autoFilter"
                    && path[2].local == "filterColumn"
                {
                    filter_active = true;
                }
                if path.len() == 1 && !xml::path_is(path, xml::MAIN, &["worksheet"]) {
                    return Err(unsupported("worksheet root/namespace", "worksheet"));
                }
                let structural = [
                    "worksheet",
                    "sheetData",
                    "row",
                    "c",
                    "f",
                    "v",
                    "is",
                    "t",
                    "r",
                    "rPr",
                    "dimension",
                ];
                if (structural.contains(&element.local.as_str())
                    || matches!(element.local.as_str(), "mergeCells" | "mergeCell"))
                    && element.ns != xml::MAIN
                {
                    return Err(unsupported("foreign worksheet lookalike", &element.local));
                }
                if matches!(element.local.as_str(), "f" | "v" | "is") && !direct {
                    return Err(unsupported("misplaced cell payload", "worksheet"));
                }
                if matches!(element.local.as_str(), "tableParts" | "tablePart") {
                    let valid = if element.local == "tableParts" {
                        xml::path_is(path, xml::MAIN, &["worksheet", "tableParts"])
                    } else {
                        xml::path_is(path, xml::MAIN, &["worksheet", "tableParts", "tablePart"])
                    };
                    if !valid {
                        return Err(unsupported("misplaced/foreign table metadata", "worksheet"));
                    }
                    if element.local == "tableParts" {
                        if table_count.is_some() {
                            return Err(unsupported("duplicate tableParts", "worksheet"));
                        }
                        let count = node
                            .required("count")?
                            .parse::<usize>()
                            .map_err(|_| unsupported("invalid tableParts count", "worksheet"))?;
                        if count > options.limits.max_entries.min(options.limits.max_cells) {
                            return Err(unsupported("table count limit", "worksheet"));
                        }
                        table_count = Some(count);
                    } else {
                        if table_ids.len() >= table_count.unwrap_or(0) {
                            return Err(unsupported("tablePart count disagreement", "worksheet"));
                        }
                        table_ids.push(
                            node.attribute(xml::OFFICE, "id")
                                .ok_or_else(|| {
                                    unsupported("missing table relationship ID", "worksheet")
                                })?
                                .value
                                .clone(),
                        );
                    }
                }
                if element.local == "dimension" {
                    if !xml::path_is(path, xml::MAIN, &["worksheet", "dimension"])
                        || dimension.is_some()
                        || sheet_data_count != 0
                    {
                        return Err(unsupported("duplicate/misplaced dimension", "worksheet"));
                    }
                    let range = rect(node.required("ref")?)?;
                    let area = u64::from(range.2) * u64::from(range.3);
                    if range.3 > options.limits.max_columns {
                        return Err(unsupported("worksheet width limit", "worksheet"));
                    }
                    if area > options.limits.max_cells as u64 {
                        return Err(unsupported("worksheet dimension cell limit", "worksheet"));
                    }
                    dimension = Some(range);
                    if let Some(index) = index.as_mut() {
                        let span = node
                            .attribute("", "ref")
                            .expect("required ref")
                            .span
                            .clone();
                        index.dimension = Some((
                            SourceRect {
                                first_row: range.0,
                                first_col: range.1,
                                last_row: range.2,
                                last_col: range.3,
                            },
                            span,
                        ));
                    }
                }
                if element.local == "sheetData" {
                    if !xml::path_is(path, xml::MAIN, &["worksheet", "sheetData"]) {
                        return Err(unsupported("misplaced sheetData", "worksheet"));
                    }
                    sheet_data_count += 1;
                    if sheet_data_count != 1 {
                        return Err(unsupported("duplicate sheetData", "worksheet"));
                    }
                    if let Some(index) = index.as_mut() {
                        index.sheet_data = node.span.clone();
                        index.sheet_data_open_end = node.span.end;
                        index.sheet_data_empty = *empty;
                    }
                }
                if element.local == "mergeCell" {
                    if !xml::path_is(path, xml::MAIN, &["worksheet", "mergeCells", "mergeCell"]) {
                        return Err(unsupported("misplaced merge cell", "worksheet"));
                    }
                    let merge = SourceRect::parse(node.required("ref")?)?;
                    if mode == Mode::TablePlain {
                        if table_merges.len() >= options.limits.max_cells {
                            return Err(unsupported("merge count limit", "worksheet"));
                        }
                        table_merges.push(merge);
                    }
                    if let Some(index) = index.as_mut() {
                        if index.merges.len() >= options.limits.max_cells {
                            return Err(unsupported("merge count limit", "worksheet"));
                        }
                        index.merges.push(merge);
                    }
                }
                if element.local == "row" {
                    if !xml::path_is(path, xml::MAIN, &["worksheet", "sheetData", "row"]) {
                        return Err(unsupported("misplaced row", "worksheet"));
                    }
                    let next_row = integer(node.required("r")?)?;
                    if next_row <= row || next_row > 1_048_576 {
                        return Err(unsupported(
                            "invalid/non-increasing worksheet row",
                            "worksheet",
                        ));
                    }
                    // Excel shows a zero-height row as hidden even without
                    // the `hidden` flag.
                    let zero_height = node
                        .value("ht")
                        .is_some_and(|h| h.trim().parse::<f64>().map_or(true, |h| h <= 0.0));
                    if matches!(node.value("hidden"), Some("1" | "true")) || zero_height {
                        if hidden_rows.len() >= options.limits.max_cells {
                            return Err(unsupported("hidden row count limit", "worksheet"));
                        }
                        hidden_rows.push(next_row);
                    } else if !matches!(node.value("hidden"), None | Some("0" | "false")) {
                        return Err(unsupported("invalid row hidden flag", "worksheet"));
                    }
                    if node.value("outlineLevel").is_some_and(|l| l.trim() != "0") {
                        if outline_rows.len() >= options.limits.max_cells {
                            return Err(unsupported("outline row count limit", "worksheet"));
                        }
                        outline_rows.push(next_row);
                    }
                    if !matches!(node.value("collapsed"), None | Some("0" | "false")) {
                        outline_collapsed = true;
                    }
                    row = next_row;
                    column = 0;
                    if let Some(index) = index.as_mut() {
                        let at = index.cells.len();
                        index.rows.push(RowEntry {
                            row,
                            span: node.span.clone(),
                            open_end: node.span.end,
                            empty: *empty,
                            qualified: element.qualified.clone(),
                            spans_attr: node.attribute("", "spans").map(|a| a.span.clone()),
                            cells: at..at,
                        });
                    }
                }
                if element.local == "c" {
                    if !is_cell || current.is_some() {
                        return Err(unsupported("misplaced/nested cell", "worksheet"));
                    }
                    let address = node.required("r")?;
                    let (r, c) = coord(address)?;
                    if c > options.limits.max_columns {
                        return Err(unsupported("worksheet width limit", "worksheet"));
                    }
                    max_row = max_row.max(r);
                    max_col = max_col.max(c);
                    if row != r || c <= column {
                        return Err(unsupported(
                            "duplicate/non-increasing cell coordinate",
                            "worksheet",
                        ));
                    }
                    column = c;
                    if let Some((r1, c1, r2, c2)) = dimension
                        && (!(r1..=r2).contains(&r) || !(c1..=c2).contains(&c))
                    {
                        return Err(unsupported("cell outside worksheet dimension", "worksheet"));
                    }
                    *observed = observed
                        .checked_add(1)
                        .ok_or_else(|| unsupported("cell count overflow", "worksheet"))?;
                    if *observed > options.limits.max_cells {
                        return Err(unsupported("serialized cell count limit", "worksheet"));
                    }
                    if node.value("vm").is_some() {
                        return Err(unsupported("rich value metadata (vm)", "worksheet"));
                    }
                    let cm = node
                        .value("cm")
                        .map(|value| {
                            value.parse::<u32>().ok().filter(|n| *n > 0).ok_or_else(|| {
                                unsupported(
                                    "dangling or invalid dynamic cell metadata index (cm)",
                                    "worksheet",
                                )
                            })
                        })
                        .transpose()?;
                    if cm.is_some() && index.is_none() {
                        *needs_index = true;
                        return Err(unsupported(NEEDS_INDEX, "worksheet"));
                    }
                    serialized += 1;
                    let kind = node.value("t").map(str::to_owned);
                    if !matches!(
                        kind.as_deref(),
                        None | Some("n" | "b" | "e" | "str" | "s" | "inlineStr" | "d")
                    ) {
                        return Err(unsupported("unknown cell type", "worksheet"));
                    }
                    if !*empty {
                        current = Some(Cell {
                            row: r,
                            col: c,
                            address: address.to_owned(),
                            span: node.span.clone(),
                            open_end: node.span.end,
                            qualified: element.qualified.clone(),
                            kind,
                            kind_span: node.attribute("", "t").map(|a| a.span.clone()),
                            formula_end: 0,
                            formula_text: String::new(),
                            value: None,
                            inline: None,
                            formula_kind: "",
                            shared_id: None,
                            shared_range: None,
                            has_formula: false,
                            formula_open: 0..0,
                            formula_kind_span: None,
                            dynamic: cm.map(|cm| {
                                Box::new(DynamicAttributes {
                                    cm: Some(cm),
                                    cm_span: node.attribute("", "cm").map(|a| a.span.clone()),
                                    ..Default::default()
                                })
                            }),
                        });
                    } else if let Some(index) = index.as_mut() {
                        if cm.is_some() {
                            return Err(unsupported(
                                "cell metadata on a non-dynamic-array cell",
                                "worksheet",
                            ));
                        }
                        index.cells.push(IndexedCell {
                            row: r,
                            col: c,
                            span: node.span.clone(),
                            open_end: node.span.end,
                            empty: true,
                            kind_span: node.attribute("", "t").map(|a| a.span.clone()),
                            kind,
                            cm: None,
                            value: None,
                            inline: None,
                            formula: None,
                            literal_refusal: None,
                        });
                        index.rows.last_mut().expect("open row").cells.end = index.cells.len();
                    }
                }
                if direct {
                    let cell = current
                        .as_mut()
                        .ok_or_else(|| unsupported("payload without cell", "worksheet"))?;
                    match element.local.as_str() {
                        "f" => {
                            if cell.has_formula || cell.value.is_some() || cell.inline.is_some() {
                                return Err(unsupported(
                                    "duplicate/misordered formula",
                                    "worksheet",
                                ));
                            }
                            cell.has_formula = true;
                            cell.formula_kind = match node.value("t").unwrap_or("normal") {
                                "normal" => "normal",
                                "shared" => "shared",
                                "array" => "array",
                                "dataTable" => {
                                    return Err(unsupported("data-table formula", "worksheet"));
                                }
                                _ => return Err(unsupported("unknown formula kind", "worksheet")),
                            };
                            cell.formula_open = node.span.clone();
                            cell.formula_kind_span =
                                node.attribute("", "t").map(|a| a.span.clone());
                            if cell.formula_kind == "array" {
                                if index.is_none() {
                                    *needs_index = true;
                                    return Err(unsupported(NEEDS_INDEX, "worksheet"));
                                }
                                if let Some(extent) = node.attribute("", "ref") {
                                    let rect = SourceRect::parse(&extent.value)?;
                                    // Validate declared geometry before any
                                    // ownership index or child enumeration.
                                    if rect.last_col > options.limits.max_columns {
                                        return Err(unsupported(
                                            "dynamic array extent width limit",
                                            "worksheet",
                                        ));
                                    }
                                    extent_cells = rect
                                        .cell_count()
                                        .and_then(|n| extent_cells.checked_add(n))
                                        .filter(|n| *n <= options.limits.max_cells as u64)
                                        .ok_or_else(|| {
                                            unsupported(
                                                "dynamic array extent cell limit",
                                                "worksheet",
                                            )
                                        })?;
                                    max_row = max_row.max(rect.last_row);
                                    max_col = max_col.max(rect.last_col);
                                    let dynamic = cell.dynamic.get_or_insert_default();
                                    dynamic.array_ref = Some(rect);
                                    dynamic.array_ref_span = Some(extent.span.clone());
                                }
                            } else if node.value("ref").is_some() && cell.formula_kind != "shared" {
                                return Err(unsupported("non-shared formula extent", "worksheet"));
                            }
                            if cell.formula_kind == "shared" {
                                cell.shared_id = Some(integer(node.required("si")?)?);
                                cell.shared_range = node.value("ref").map(rect).transpose()?;
                            }
                            if *empty {
                                cell.formula_end = node.span.end;
                            }
                        }
                        "v" => {
                            if cell.value.is_some() || cell.inline.is_some() {
                                return Err(unsupported(
                                    "duplicate/ambiguous cell cache",
                                    "worksheet",
                                ));
                            }
                            cell.value = Some(ValueNode {
                                span: node.span.clone(),
                                open_end: node.span.end,
                                close_start: node.span.end,
                                empty: *empty,
                                qualified: element.qualified.clone(),
                                text: String::new(),
                            });
                        }
                        "is" => {
                            if cell.inline.is_some() || cell.value.is_some() {
                                return Err(unsupported(
                                    "duplicate/ambiguous inline cache",
                                    "worksheet",
                                ));
                            }
                            cell.inline = Some(node.span.clone());
                        }
                        _ => {}
                    }
                }
                if path.len() > 5 && matches!(path[4].local.as_str(), "f" | "v") {
                    return Err(unsupported("nested formula/cache content", "worksheet"));
                }
            }
            xml::Kind::Text(text) if direct => {
                if let Some(cell) = current.as_mut() {
                    if element.local == "f" {
                        cell.formula_text.push_str(text);
                    }
                    if element.local == "v" {
                        cell.value.as_mut().expect("opened v").text.push_str(text);
                    }
                }
            }
            xml::Kind::Close => {
                if xml::path_is(path, xml::MAIN, &["worksheet", "autoFilter"]) && filter_active {
                    let mut rect = filter_ref.take().expect("validated autoFilter");
                    rect.first_row += 1;
                    if rect.first_row <= rect.last_row {
                        active_filters.push(rect);
                    }
                }
                if direct {
                    let cell = current
                        .as_mut()
                        .ok_or_else(|| unsupported("unbalanced cell payload", "worksheet"))?;
                    match element.local.as_str() {
                        "f" => cell.formula_end = node.span.end,
                        "v" => {
                            let v = cell.value.as_mut().expect("opened v");
                            v.close_start = node.span.start;
                            v.span.end = node.span.end;
                        }
                        "is" => cell.inline.as_mut().expect("opened is").end = node.span.end,
                        _ => {}
                    }
                }
                if let Some(index) = index.as_mut() {
                    if xml::path_is(path, xml::MAIN, &["worksheet", "sheetData", "row"]) {
                        let entry = index.rows.last_mut().expect("open row");
                        entry.span.end = node.span.end;
                    }
                    if xml::path_is(path, xml::MAIN, &["worksheet", "sheetData"]) {
                        index.sheet_data.end = node.span.end;
                    }
                }
                if is_cell {
                    let mut cell = current
                        .take()
                        .ok_or_else(|| unsupported("unbalanced cell", "worksheet"))?;
                    cell.span.end = node.span.end;
                    let refusal = literal_refusal(&cell, bytes);
                    dynamic_anchor_extent(&cell)?;
                    if let Some(index) = index.as_mut() {
                        // Ownership decides later whether a refused literal
                        // is a masked generated child or a refused input.
                        index.cells.push(IndexedCell {
                            row: cell.row,
                            col: cell.col,
                            span: cell.span.clone(),
                            open_end: cell.open_end,
                            empty: false,
                            kind: cell.kind.clone(),
                            kind_span: cell.kind_span.clone(),
                            cm: cell.cm(),
                            value: cell.value.as_ref().map(|v| v.span.clone()),
                            inline: cell.inline.clone(),
                            formula: cell.has_formula.then_some(cells.len()),
                            literal_refusal: refusal,
                        });
                        index.rows.last_mut().expect("open row").cells.end = index.cells.len();
                    } else if let Some(refusal) = refusal {
                        return Err(unsupported(refusal, "worksheet"));
                    }
                    if cell.has_formula {
                        if cell.formula_kind == "normal" && cell.formula_text.trim().is_empty() {
                            return Err(unsupported("empty ordinary formula", "worksheet"));
                        }
                        if cell.formula_end == 0 {
                            return Err(unsupported("missing formula boundary", "worksheet"));
                        }
                        cells.push(cell);
                        if cells.len() > options.limits.max_formula_cells {
                            return Err(unsupported("formula cell count limit", "worksheet"));
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    })?;
    if sheet_data_count != 1 {
        return Err(unsupported("missing sheetData", "worksheet"));
    }
    if let Some((_, _, r, c)) = dimension {
        max_row = max_row.max(r);
        max_col = max_col.max(c);
    }
    *logical_cells = logical_cells
        .checked_add(u64::from(max_row) * u64::from(max_col))
        .ok_or_else(|| unsupported("logical area overflow", "workbook"))?;
    if *logical_cells > options.limits.max_cells as u64 {
        return Err(unsupported("workbook logical cell limit", "workbook"));
    }
    let mut anchors = HashMap::new();
    for cell in &cells {
        if let Some(id) = cell.shared_id {
            if !cell.formula_text.trim().is_empty() {
                let range = cell
                    .shared_range
                    .ok_or_else(|| unsupported("unbounded shared formula anchor", "worksheet"))?;
                if (cell.row, cell.col) != (range.0, range.1) {
                    return Err(unsupported(
                        "non-top-left shared formula anchor",
                        "worksheet",
                    ));
                }
                let area = u64::from(range.2 - range.0 + 1) * u64::from(range.3 - range.1 + 1);
                if area > options.limits.max_cells as u64 || anchors.insert(id, range).is_some() {
                    return Err(unsupported(
                        "duplicate/oversized shared formula anchor",
                        "worksheet",
                    ));
                }
                if area > 1 {
                    super::shared_qualifiers::validate(&cell.formula_text, &cell.address)?;
                }
            } else if cell.shared_range.is_some() {
                return Err(unsupported("shared descendant declares range", "worksheet"));
            }
        }
    }
    for cell in &cells {
        if let Some(id) = cell.shared_id {
            let &(r1, c1, r2, c2) = anchors
                .get(&id)
                .ok_or_else(|| unsupported("orphan shared formula descendant", "worksheet"))?;
            if !(r1..=r2).contains(&cell.row) || !(c1..=c2).contains(&cell.col) {
                return Err(unsupported(
                    "shared formula outside declared range",
                    "worksheet",
                ));
            }
        }
    }
    if table_count.is_some_and(|n| n != table_ids.len()) {
        return Err(unsupported("tablePart count disagreement", "worksheet"));
    }
    // A collapsed outline may hide grouped rows that carry no `hidden` flag:
    // treat every grouped row as hidden.
    if outline_collapsed && !outline_rows.is_empty() {
        hidden_rows.extend(outline_rows);
        hidden_rows.sort_unstable();
        hidden_rows.dedup();
    }
    Ok(Scan {
        active_filters,
        hidden_rows,
        rows_hidden_by_default,
        has_dimension: dimension.is_some(),
        table_merges,
        table_ids,
        cells,
        index,
        serialized,
        bounds: (max_row, max_col),
    })
}
