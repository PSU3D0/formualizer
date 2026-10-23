//! Formula locations/cache spans without a rich cell graph. On request
//! (worksheets with dynamic-array anchors or new spills only), the same
//! bounded pass also builds a source index of rows, cells, dimensions and
//! merges for dynamic-array ownership and geometry.
use crate::xlsx::recalculate::{IoError, XlsxRecalculateOptions, unsupported, xml};
use formualizer_common::coord::parse_a1_1based;
use std::{
    borrow::Cow,
    collections::HashMap,
    ops::{Deref, Range},
};

mod cursor;
pub(in crate::xlsx) mod events;
#[cfg(test)]
pub(in crate::xlsx) use crate::xlsx::recalculate::tests::oracles::worksheet_legacy as reference;
#[cfg(any(test, feature = "xlsx-recalc"))]
pub(in crate::xlsx) mod scanner;
mod shared;

/// Inclusive source rectangle in one-based worksheet coordinates (row 1 is
/// the first row, column 1 is `A`), matching `FormulaMark::row`/`FormulaMark::col`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceRect {
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

/// Validated cell type, without a per-cell name-interning lookup.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CellKind {
    Number,
    Boolean,
    Error,
    Text,
    Shared,
    Inline,
    Date,
}
impl CellKind {
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "n" => Self::Number,
            "b" => Self::Boolean,
            "e" => Self::Error,
            "str" => Self::Text,
            "s" => Self::Shared,
            "inlineStr" => Self::Inline,
            "d" => Self::Date,
            _ => return None,
        })
    }
}
impl Deref for CellKind {
    type Target = str;
    fn deref(&self) -> &str {
        match self {
            Self::Number => "n",
            Self::Boolean => "b",
            Self::Error => "e",
            Self::Text => "str",
            Self::Shared => "s",
            Self::Inline => "inlineStr",
            Self::Date => "d",
        }
    }
}

#[derive(Debug)]
pub(crate) struct ValueNode {
    pub span: Range<usize>,
    pub open_end: usize,
    pub close_start: usize,
    pub empty: bool,
    prefix_len: usize,
    /// Only entity/EOL-normalised or multi-event payloads need owned text.
    decoded: Option<Box<str>>,
    /// Numeric cache decoded once, including trimming used by Cache::matches.
    /// NaN is the non-number/unparseable sentinel (never display-equal).
    pub number: f64,
    readable: bool,
}
impl ValueNode {
    pub fn text<'a>(&'a self, bytes: &'a [u8]) -> Cow<'a, str> {
        self.decoded.as_deref().map_or_else(
            || String::from_utf8_lossy(&bytes[self.open_end..self.close_start]),
            Cow::Borrowed,
        )
    }
    pub fn qualified(&self, bytes: &[u8]) -> String {
        format!("{}v", prefix(bytes, self.span.start, self.prefix_len))
    }
}
/// Prefix length is unrestricted: long accepted XML prefixes must stay valid.
fn prefix(bytes: &[u8], start: usize, len: usize) -> Cow<'_, str> {
    String::from_utf8_lossy(&bytes[start + 1..start + 1 + len])
}
#[derive(Debug)]
pub(crate) struct FormulaMark {
    pub row: u32,
    pub col: u32,
    #[allow(dead_code)] // consumers use the narrower spans below
    pub span: Range<usize>,
    pub open_end: usize,
    prefix_len: usize,
    pub kind: Option<CellKind>,
    pub kind_span: Option<Range<usize>>,
    pub formula_end: usize,
    pub formula_text: String,
    pub value: Option<ValueNode>,
    pub inline: Option<Range<usize>>,
    /// `normal`, `shared` or `array`.
    pub formula_kind: &'static str,
    pub shared_id: Option<u32>,
    pub(in crate::xlsx) shared_range: Option<(u32, u32, u32, u32)>,
    #[allow(dead_code)] // true for every retained cell
    pub(in crate::xlsx) has_formula: bool,
    /// `<f …>` start tag (or whole empty element) span.
    pub formula_open: Range<usize>,
    /// `t="…"` attribute span on the formula start tag.
    pub formula_kind_span: Option<Range<usize>>,
    /// `cm` and array-extent attributes, boxed: present only on the rare
    /// cells that carry them.
    pub(in crate::xlsx) dynamic: Option<Box<DynamicAttributes>>,
}
#[derive(Debug, Default)]
pub(in crate::xlsx) struct DynamicAttributes {
    /// Preserve accepted noncanonical A1 spellings in cold diagnostics.
    original_address: Option<Box<str>>,
    /// One-based cellMetadata index from `c/@cm`.
    pub(in crate::xlsx) cm: Option<u32>,
    /// `cm="…"` attribute span on the cell start tag.
    pub(in crate::xlsx) cm_span: Option<Range<usize>>,
    /// Declared array extent from `f/@ref` for `t="array"` formulas.
    pub(in crate::xlsx) array_ref: Option<SourceRect>,
    /// `ref="…"` attribute span on an array formula start tag.
    pub(in crate::xlsx) array_ref_span: Option<Range<usize>>,
}
impl FormulaMark {
    pub fn address(&self) -> Cow<'_, str> {
        if let Some(address) = self
            .dynamic
            .as_ref()
            .and_then(|d| d.original_address.as_deref())
        {
            return Cow::Borrowed(address);
        }
        // Coordinates were already checked against the worksheet grid.
        let mut col = self.col;
        let mut letters = [0u8; 7]; // enough even for u32::MAX
        let mut at = letters.len();
        while col > 0 {
            col -= 1;
            at -= 1;
            letters[at] = b'A' + (col % 26) as u8;
            col /= 26;
        }
        Cow::Owned(format!(
            "{}{row}",
            String::from_utf8_lossy(&letters[at..]),
            row = self.row
        ))
    }
    pub fn prefix<'a>(&self, bytes: &'a [u8]) -> Cow<'a, str> {
        prefix(bytes, self.span.start, self.prefix_len)
    }
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
pub(in crate::xlsx) struct RowEntry {
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
pub(in crate::xlsx) struct IndexedCell {
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
    pub(in crate::xlsx) literal_refusal: Option<&'static str>,
}
/// Bounded source geometry of one worksheet.
#[derive(Debug)]
pub(in crate::xlsx) struct SourceIndex {
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
#[cfg_attr(test, derive(Debug))]
pub(in crate::xlsx) struct Scan {
    pub cells: Vec<FormulaMark>,
    pub table_ids: Vec<String>,
    pub table_merges: Vec<SourceRect>,
    /// Sorted rows hidden by `hidden`, a zero height or a collapsed outline.
    pub hidden_rows: Vec<u32>,
    /// `sheetFormatPr/@zeroHeight`: unlisted rows are hidden by default.
    pub rows_hidden_by_default: bool,
    /// Whether the worksheet declares a `<dimension>`.
    pub has_dimension: bool,
    /// Original attribute value bytes, excluding quotes; no normalization.
    pub raw_dimension_span: Option<Range<usize>>,
    /// Earliest raw row-r token incompatible with the pinned reader. Advisory
    /// provenance only: normalized admission and its refusal order are intact.
    pub raw_row_refusal_span: Option<Range<usize>>,
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
pub(in crate::xlsx) fn readable_scalar_cache(cell: &FormulaMark, _bytes: &[u8]) -> bool {
    cell.value.as_ref().is_none_or(|v| v.readable)
}
/// The `<v>` facts [`readable_scalar_cache`] decides on.
struct CacheText<'v> {
    empty: bool,
    text: &'v str,
    open_end: usize,
    close_start: usize,
}
fn scalar_cache_readable(kind: Option<&str>, value: Option<CacheText<'_>>, bytes: &[u8]) -> bool {
    let Some(v) = value else {
        return true;
    };
    if !matches!(kind, None | Some("n" | "s" | "b" | "e")) {
        return true;
    }
    if v.empty || v.text.is_empty() {
        return matches!(kind, None | Some("n"));
    }
    if bytes[v.open_end..v.close_start] != *v.text.as_bytes() {
        return false;
    }
    match kind {
        None | Some("n") => v.text.parse::<f64>().is_ok_and(f64::is_finite),
        Some("s") => v.text.parse::<u32>().is_ok(),
        Some("b") => matches!(v.text, "0" | "1"),
        Some("e") => crate::xlsx::ingest::utilities::parse_error_token(v.text).is_ok(),
        _ => true,
    }
}
/// `[A-Z]{1,3}[1-9][0-9]{0,6}` within the grid, which
/// `parse_a1_1based` reads as the same relative row and column.
pub(in crate::xlsx) use crate::xlsx::xml::plain_coord;
fn coord(value: &str) -> Result<(u32, u32), IoError> {
    if let Some(coord) = plain_coord(value) {
        return Ok(coord);
    }
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
/// A `<v>` cache while its cell is open.
struct OpenValue<'a> {
    span: Range<usize>,
    open_end: usize,
    close_start: usize,
    empty: bool,
    qualified: &'a str,
    text: Cow<'a, str>,
}
/// A `<c>` element while it is open. It borrows the worksheet; only formula
/// cells are kept, as an owned [`FormulaMark`].
struct OpenCell<'a> {
    row: u32,
    col: u32,
    span: Range<usize>,
    open_end: usize,
    qualified: &'a str,
    kind: Option<CellKind>,
    kind_span: Option<Range<usize>>,
    formula_end: usize,
    formula_text: Cow<'a, str>,
    value: Option<OpenValue<'a>>,
    inline: Option<Range<usize>>,
    formula_kind: &'static str,
    shared_id: Option<u32>,
    shared_range: Option<(u32, u32, u32, u32)>,
    has_formula: bool,
    formula_open: Range<usize>,
    formula_kind_span: Option<Range<usize>>,
    dynamic: Option<Box<DynamicAttributes>>,
}
impl<'a> OpenCell<'a> {
    #[cfg(test)]
    fn cm(&self) -> Option<u32> {
        self.dynamic.as_ref().and_then(|d| d.cm)
    }
    #[cfg(test)]
    fn literal_refusal(&self, bytes: &[u8]) -> Option<&'static str> {
        if self.has_formula {
            return None;
        }
        if self.kind.as_deref() == Some("e")
            && self
                .value
                .as_ref()
                .is_none_or(|v| crate::xlsx::ingest::utilities::parse_error_token(&v.text).is_err())
        {
            return Some("literal error value unsupported by Calamine");
        }
        let value = self.value.as_ref().map(|v| CacheText {
            empty: v.empty,
            text: &v.text,
            open_end: v.open_end,
            close_start: v.close_start,
        });
        if !scalar_cache_readable(self.kind.as_deref(), value, bytes) {
            return Some("literal scalar payload is not supported by Calamine");
        }
        None
    }
    /// Dynamic-array cell rules. Ownership through the
    /// metadata chain is resolved later; this validates the source encoding.
    #[cfg(test)]
    fn dynamic_anchor_extent(&self) -> Result<Option<SourceRect>, IoError> {
        if self.cm().is_none() && self.formula_kind != "array" {
            return Ok(None);
        }
        if self.formula_kind == "shared" {
            return Err(unsupported(
                "shared formula family member claimed as dynamic anchor",
                "worksheet",
            ));
        }
        if self.formula_kind != "array" {
            return Err(unsupported(
                "cell metadata on a non-dynamic-array cell",
                "worksheet",
            ));
        }
        let extent = self
            .dynamic
            .as_ref()
            .and_then(|d| d.array_ref)
            .ok_or_else(|| unsupported("missing dynamic array extent", "worksheet"))?;
        if (extent.first_row, extent.first_col) != (self.row, self.col) {
            return Err(unsupported(
                "dynamic array anchor is not the top-left of its extent",
                "worksheet",
            ));
        }
        if self.formula_text.trim().is_empty() {
            return Err(unsupported("empty dynamic array formula", "worksheet"));
        }
        Ok(Some(extent))
    }
    fn into_cell(self, bytes: &[u8]) -> FormulaMark {
        let kind = self.kind.as_deref();
        let value = self.value.map(|v| {
            // Matching trims numeric caches; Calamine's proof deliberately
            // does not. Keep those separate decisions with one numeric parse.
            let numeric = matches!(kind, None | Some("n"));
            let number = if numeric {
                v.text.trim().parse::<f64>().unwrap_or(f64::NAN)
            } else {
                f64::NAN
            };
            let readable = if numeric {
                v.empty
                    || v.text.is_empty()
                    || (bytes[v.open_end..v.close_start] == *v.text.as_bytes()
                        && v.text.trim() == v.text
                        && number.is_finite())
            } else {
                scalar_cache_readable(
                    kind,
                    Some(CacheText {
                        empty: v.empty,
                        text: &v.text,
                        open_end: v.open_end,
                        close_start: v.close_start,
                    }),
                    bytes,
                )
            };
            let decoded = (bytes[v.open_end..v.close_start] != *v.text.as_bytes())
                .then(|| v.text.into_owned().into_boxed_str());
            ValueNode {
                span: v.span,
                open_end: v.open_end,
                close_start: v.close_start,
                empty: v.empty,
                prefix_len: v.qualified.find(':').map_or(0, |i| i + 1),
                decoded,
                number,
                readable,
            }
        });
        FormulaMark {
            row: self.row,
            col: self.col,
            span: self.span,
            open_end: self.open_end,
            prefix_len: self.qualified.find(':').map_or(0, |i| i + 1),
            kind: self.kind,
            kind_span: self.kind_span,
            formula_end: self.formula_end,
            formula_text: self.formula_text.into_owned(),
            value,
            inline: self.inline,
            formula_kind: self.formula_kind,
            shared_id: self.shared_id,
            shared_range: self.shared_range,
            has_formula: self.has_formula,
            formula_open: self.formula_open,
            formula_kind_span: self.formula_kind_span,
            dynamic: self.dynamic,
        }
    }
}
/// Append one text event, borrowing when it is the only one.
fn append<'a>(to: &mut Cow<'a, str>, text: Cow<'a, str>) {
    if to.is_empty() {
        *to = text;
    } else {
        to.to_mut().push_str(&text);
    }
}
/// How [`scan`] treats dynamic-array cell metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::xlsx) enum ScanRetention {
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
#[cfg_attr(test, derive(Debug))]
pub(in crate::xlsx) enum Scanned {
    Done(Scan),
    /// A `Plain` scan met dynamic cell metadata; rescan `Indexed`.
    NeedsIndex,
}
pub(in crate::xlsx) const NEEDS_INDEX: &str = "dynamic cell metadata needs a source index";
#[cfg(test)]
thread_local! {
    /// Indexed scans on this thread (tests prove scalar workbooks build none).
    pub(in crate::xlsx) static INDEXED_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(in crate::xlsx) static MACHINE_WALKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
pub(in crate::xlsx) fn scan(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanRetention,
    observed: &mut usize,
    logical_cells: &mut u64,
) -> Result<Scanned, IoError> {
    #[cfg(test)]
    {
        MACHINE_WALKS.with(|n| n.set(n.get() + 1));
        if mode == ScanRetention::Indexed {
            INDEXED_SCANS.with(|n| n.set(n.get() + 1));
        }
        crate::xlsx::recalculate::tests::oracles::shadow_scan(
            bytes,
            options,
            mode,
            *observed,
            *logical_cells,
        );
    }
    scanner::scan_emitting(bytes, options, mode, observed, logical_cells, |_| Ok(()))
}
/// Native admission's same counter/shadow seam, with ephemeral value emission.
pub(in crate::xlsx) fn scan_emitting(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanRetention,
    observed: &mut usize,
    logical_cells: &mut u64,
    emit: impl FnMut(scanner::emission::Literal<'_>) -> Result<(), IoError>,
) -> Result<Scanned, IoError> {
    #[cfg(test)]
    {
        MACHINE_WALKS.with(|n| n.set(n.get() + 1));
        if mode == ScanRetention::Indexed {
            INDEXED_SCANS.with(|n| n.set(n.get() + 1));
        }
        crate::xlsx::recalculate::tests::oracles::shadow_scan(
            bytes,
            options,
            mode,
            *observed,
            *logical_cells,
        );
    }
    scanner::scan_emitting(bytes, options, mode, observed, logical_cells, emit)
}
/// [`scan`] without the test-only indexed-scan count.
#[cfg(test)]
pub(in crate::xlsx) fn scan_uncounted(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanRetention,
    observed: &mut usize,
    logical_cells: &mut u64,
) -> Result<Scanned, IoError> {
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
const MC: &str = "http://schemas.openxmlformats.org/markup-compatibility/2006";
const XDR: &str = "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing";
const XM: &str = "http://schemas.microsoft.com/office/excel/2006/main";
/// Excel extension markup that reuses a structural local name but is inert:
/// - `xdr:row` in the `from`/`to` anchor of a form control or OLE object,
///   which Excel 2010+ writes inside `mc:AlternateContent`;
/// - `xm:f` in an x14 data validation, conditional format or sparkline under
///   `worksheet/extLst/ext`.
///
/// Calamine 0.36 reads worksheet cells only between `sheetData` and its first
/// closing tag (before it, only `dimension` and `sheetData`), and elsewhere in
/// the part only `mergeCells`/`mergeCell`/`hyperlinks`; the source index
/// reads main-namespace elements by path. Neither reads these positions.
/// Anything under `sheetData` stays refused: there Calamine's cell reader
/// matches `row` by local name and treats any element inside a cell as its
/// payload.
fn inert_extension(path: &[xml::Element<'_>]) -> bool {
    let (element, ancestors) = path.split_last().expect("open XML element");
    if ancestors
        .iter()
        .any(|a| a.ns == xml::MAIN && a.local == "sheetData")
    {
        return false;
    }
    match (&*element.ns, element.local) {
        (XDR, "row") => {
            let n = ancestors.len();
            n >= 3
                && ancestors[n - 1].ns == xml::MAIN
                && matches!(ancestors[n - 1].local, "from" | "to")
                && ancestors[n - 2].ns == xml::MAIN
                && ancestors[n - 2].local == "anchor"
                && ancestors
                    .iter()
                    .any(|a| a.ns == MC && a.local == "AlternateContent")
        }
        (XM, "f") => {
            ancestors.len() >= 3
                && xml::path_is(&ancestors[..3], xml::MAIN, &["worksheet", "extLst", "ext"])
        }
        _ => false,
    }
}
/// Local names the scanner matches on; every other name is `Other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Local {
    Worksheet,
    SheetData,
    Row,
    C,
    F,
    V,
    Is,
    T,
    R,
    RPr,
    Dimension,
    SheetFormatPr,
    AutoFilter,
    FilterColumn,
    MergeCells,
    MergeCell,
    TableParts,
    TablePart,
    Other,
}
/// An open element classified once: its local name and whether it is in the
/// main SpreadsheetML namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tag {
    main: bool,
    local: Local,
}
impl Tag {
    fn of(element: &xml::Element<'_>) -> Self {
        let local = match element.local.as_bytes() {
            b"c" => Local::C,
            b"v" => Local::V,
            b"f" => Local::F,
            b"row" => Local::Row,
            b"is" => Local::Is,
            b"t" => Local::T,
            b"r" => Local::R,
            [b'r', b'P', b'r'] => Local::RPr,
            b"worksheet" => Local::Worksheet,
            b"sheetData" => Local::SheetData,
            b"dimension" => Local::Dimension,
            b"sheetFormatPr" => Local::SheetFormatPr,
            b"autoFilter" => Local::AutoFilter,
            b"filterColumn" => Local::FilterColumn,
            b"mergeCells" => Local::MergeCells,
            b"mergeCell" => Local::MergeCell,
            b"tableParts" => Local::TableParts,
            b"tablePart" => Local::TablePart,
            _ => Local::Other,
        };
        Tag {
            main: element.ns.is_main(),
            local,
        }
    }
}
/// [`xml::path_is`] for the main namespace over classified tags.
#[cfg(test)]
fn main_path(tags: &[Tag], names: &[Local]) -> bool {
    tags.len() == names.len() && tags.iter().zip(names).all(|(t, n)| t.main && t.local == *n)
}
#[cfg(test)]
const CELL_PATH: [Local; 4] = [Local::Worksheet, Local::SheetData, Local::Row, Local::C];
#[cfg(test)]
fn scan_inner(
    bytes: &[u8],
    options: &XlsxRecalculateOptions,
    mode: ScanRetention,
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
    let mut current: Option<OpenCell<'_>> = None;
    let mut row = 0;
    let mut column = 0;
    let mut sheet_data_count = 0;
    let mut dimension = None;
    let mut max_row = 0u32;
    let mut max_col = 0u32;
    let mut index = (mode == ScanRetention::Indexed).then(|| SourceIndex {
        dimension: None,
        sheet_data: 0..0,
        sheet_data_open_end: 0,
        sheet_data_empty: false,
        rows: Vec::new(),
        cells: Vec::new(),
        merges: Vec::new(),
    });
    let mut extent_cells = 0u64;
    // Tags of the open elements, parallel to the walker's path.
    let mut tags: Vec<Tag> = Vec::new();
    xml::walk(bytes, options, |path, node| {
        let Some(element) = path.last() else {
            return Ok(());
        };
        if let xml::Kind::Text(text) = node.kind {
            // Text only matters as the content of a direct `f`/`v` child.
            if tags.len() == 5
                && main_path(&tags[..4], &CELL_PATH)
                && let Some(cell) = current.as_mut()
            {
                match tags[4].local {
                    Local::F => append(&mut cell.formula_text, text),
                    Local::V => append(&mut cell.value.as_mut().expect("opened v").text, text),
                    _ => {}
                }
            }
            return Ok(());
        }
        if matches!(node.kind, xml::Kind::Open { .. }) {
            tags.push(Tag::of(element));
        }
        let closes = matches!(
            node.kind,
            xml::Kind::Open { empty: true, .. } | xml::Kind::Close
        );
        let tags_now = tags.as_slice();
        let result = (|| -> Result<(), IoError> {
            let tags = tags_now;
            let tag = *tags.last().expect("open element tag");
            let is_cell = main_path(tags, &CELL_PATH);
            let direct = tags.len() == 5 && main_path(&tags[..4], &CELL_PATH);
            match &node.kind {
                xml::Kind::Open { empty, .. } => {
                    if main_path(tags, &[Local::Worksheet, Local::SheetFormatPr])
                        && matches!(node.value("zeroHeight"), Some("1" | "true"))
                    {
                        rows_hidden_by_default = true;
                    }
                    if main_path(tags, &[Local::Worksheet, Local::AutoFilter]) {
                        if filter_seen {
                            return Err(unsupported("duplicate worksheet autoFilter", "worksheet"));
                        }
                        filter_seen = true;
                        filter_ref = Some(SourceRect::parse(node.required("ref")?)?);
                    }
                    if tags.len() == 4
                        && tags[1].main
                        && tags[1].local == Local::AutoFilter
                        && tags[2].local == Local::FilterColumn
                    {
                        filter_active = true;
                    }
                    if tags.len() == 1 && !main_path(tags, &[Local::Worksheet]) {
                        return Err(unsupported("worksheet root/namespace", "worksheet"));
                    }
                    // Structural names (and merge metadata) outside the main
                    // namespace.
                    if !tag.main
                        && matches!(
                            tag.local,
                            Local::Worksheet
                                | Local::SheetData
                                | Local::Row
                                | Local::C
                                | Local::F
                                | Local::V
                                | Local::Is
                                | Local::T
                                | Local::R
                                | Local::RPr
                                | Local::Dimension
                                | Local::MergeCells
                                | Local::MergeCell
                        )
                    {
                        if inert_extension(path) {
                            return Ok(());
                        }
                        return Err(unsupported("foreign worksheet lookalike", element.local));
                    }
                    if matches!(tag.local, Local::F | Local::V | Local::Is) && !direct {
                        return Err(unsupported("misplaced cell payload", "worksheet"));
                    }
                    if matches!(tag.local, Local::TableParts | Local::TablePart) {
                        let valid = if tag.local == Local::TableParts {
                            main_path(tags, &[Local::Worksheet, Local::TableParts])
                        } else {
                            main_path(
                                tags,
                                &[Local::Worksheet, Local::TableParts, Local::TablePart],
                            )
                        };
                        if !valid {
                            return Err(unsupported(
                                "misplaced/foreign table metadata",
                                "worksheet",
                            ));
                        }
                        if tag.local == Local::TableParts {
                            if table_count.is_some() {
                                return Err(unsupported("duplicate tableParts", "worksheet"));
                            }
                            let count = node.required("count")?.parse::<usize>().map_err(|_| {
                                unsupported("invalid tableParts count", "worksheet")
                            })?;
                            if count > options.limits.max_entries.min(options.limits.max_cells) {
                                return Err(unsupported("table count limit", "worksheet"));
                            }
                            table_count = Some(count);
                        } else {
                            if table_ids.len() >= table_count.unwrap_or(0) {
                                return Err(unsupported(
                                    "tablePart count disagreement",
                                    "worksheet",
                                ));
                            }
                            table_ids.push(
                                node.attribute(xml::OFFICE, "id")
                                    .ok_or_else(|| {
                                        unsupported("missing table relationship ID", "worksheet")
                                    })?
                                    .value
                                    .clone()
                                    .into_owned(),
                            );
                        }
                    }
                    if tag.local == Local::Dimension {
                        if !main_path(tags, &[Local::Worksheet, Local::Dimension])
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
                    if tag.local == Local::SheetData {
                        if !main_path(tags, &[Local::Worksheet, Local::SheetData]) {
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
                    if tag.local == Local::MergeCell {
                        if !main_path(
                            tags,
                            &[Local::Worksheet, Local::MergeCells, Local::MergeCell],
                        ) {
                            return Err(unsupported("misplaced merge cell", "worksheet"));
                        }
                        let merge = SourceRect::parse(node.required("ref")?)?;
                        if mode == ScanRetention::TablePlain {
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
                    if tag.local == Local::Row {
                        if !main_path(tags, &[Local::Worksheet, Local::SheetData, Local::Row]) {
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
                                qualified: element.qualified.to_owned(),
                                spans_attr: node.attribute("", "spans").map(|a| a.span.clone()),
                                cells: at..at,
                            });
                        }
                    }
                    if tag.local == Local::C {
                        if !is_cell || current.is_some() {
                            return Err(unsupported("misplaced/nested cell", "worksheet"));
                        }
                        let address = &node.required_attribute("r")?.value;
                        let canonical = plain_coord(address);
                        let (r, c) = canonical.map_or_else(|| coord(address), Ok)?;
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
                            return Err(unsupported(
                                "cell outside worksheet dimension",
                                "worksheet",
                            ));
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
                        let kind_attribute = node.attribute("", "t");
                        let kind = kind_attribute.map(|a| &a.value);
                        if !matches!(
                            kind.map(|k| &**k),
                            None | Some("n" | "b" | "e" | "str" | "s" | "inlineStr" | "d")
                        ) {
                            return Err(unsupported("unknown cell type", "worksheet"));
                        }
                        let kind_span = kind_attribute.map(|a| a.span.clone());
                        if !*empty {
                            current = Some(OpenCell {
                                row: r,
                                col: c,
                                span: node.span.clone(),
                                open_end: node.span.end,
                                qualified: element.qualified,
                                kind: kind.and_then(|k| CellKind::parse(k)),
                                kind_span,
                                formula_end: 0,
                                formula_text: Cow::Borrowed(""),
                                value: None,
                                inline: None,
                                formula_kind: "",
                                shared_id: None,
                                shared_range: None,
                                has_formula: false,
                                formula_open: 0..0,
                                formula_kind_span: None,
                                dynamic: (cm.is_some() || canonical.is_none()).then(|| {
                                    Box::new(DynamicAttributes {
                                        original_address: canonical
                                            .is_none()
                                            .then(|| address.to_string().into_boxed_str()),
                                        cm,
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
                                kind_span,
                                kind: kind.map(|k| k.to_string()),
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
                        match tag.local {
                            Local::F => {
                                if cell.has_formula || cell.value.is_some() || cell.inline.is_some()
                                {
                                    return Err(unsupported(
                                        "duplicate/misordered formula",
                                        "worksheet",
                                    ));
                                }
                                cell.has_formula = true;
                                let kind_attribute = node.attribute("", "t");
                                cell.formula_kind = match kind_attribute
                                    .map_or("normal", |a| &*a.value)
                                {
                                    "normal" => "normal",
                                    "shared" => "shared",
                                    "array" => "array",
                                    "dataTable" => {
                                        return Err(unsupported("data-table formula", "worksheet"));
                                    }
                                    _ => {
                                        return Err(unsupported(
                                            "unknown formula kind",
                                            "worksheet",
                                        ));
                                    }
                                };
                                cell.formula_open = node.span.clone();
                                cell.formula_kind_span = kind_attribute.map(|a| a.span.clone());
                                let extent = node.attribute("", "ref");
                                if cell.formula_kind == "array" {
                                    if index.is_none() {
                                        *needs_index = true;
                                        return Err(unsupported(NEEDS_INDEX, "worksheet"));
                                    }
                                    if let Some(extent) = extent {
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
                                } else if extent.is_some() && cell.formula_kind != "shared" {
                                    return Err(unsupported(
                                        "non-shared formula extent",
                                        "worksheet",
                                    ));
                                }
                                if cell.formula_kind == "shared" {
                                    cell.shared_id = Some(integer(node.required("si")?)?);
                                    cell.shared_range =
                                        extent.map(|a| rect(&a.value)).transpose()?;
                                }
                                if *empty {
                                    cell.formula_end = node.span.end;
                                }
                            }
                            Local::V => {
                                if cell.value.is_some() || cell.inline.is_some() {
                                    return Err(unsupported(
                                        "duplicate/ambiguous cell cache",
                                        "worksheet",
                                    ));
                                }
                                cell.value = Some(OpenValue {
                                    span: node.span.clone(),
                                    open_end: node.span.end,
                                    close_start: node.span.end,
                                    empty: *empty,
                                    qualified: element.qualified,
                                    text: Cow::Borrowed(""),
                                });
                            }
                            Local::Is => {
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
                    if tags.len() > 5 && matches!(tags[4].local, Local::F | Local::V) {
                        return Err(unsupported("nested formula/cache content", "worksheet"));
                    }
                }
                xml::Kind::Text(_) => {}
                xml::Kind::Close => {
                    if main_path(tags, &[Local::Worksheet, Local::AutoFilter]) && filter_active {
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
                        match tag.local {
                            Local::F => cell.formula_end = node.span.end,
                            Local::V => {
                                let v = cell.value.as_mut().expect("opened v");
                                v.close_start = node.span.start;
                                v.span.end = node.span.end;
                            }
                            Local::Is => {
                                cell.inline.as_mut().expect("opened is").end = node.span.end
                            }
                            _ => {}
                        }
                    }
                    if let Some(index) = index.as_mut() {
                        if main_path(tags, &[Local::Worksheet, Local::SheetData, Local::Row]) {
                            let entry = index.rows.last_mut().expect("open row");
                            entry.span.end = node.span.end;
                        }
                        if main_path(tags, &[Local::Worksheet, Local::SheetData]) {
                            index.sheet_data.end = node.span.end;
                        }
                    }
                    if is_cell {
                        let cell = current
                            .as_mut()
                            .ok_or_else(|| unsupported("unbalanced cell", "worksheet"))?;
                        cell.span.end = node.span.end;
                        let refusal = cell.literal_refusal(bytes);
                        cell.dynamic_anchor_extent()?;
                        if let Some(index) = index.as_mut() {
                            // Ownership decides later whether a refused literal
                            // is a masked generated child or a refused input.
                            index.cells.push(IndexedCell {
                                row: cell.row,
                                col: cell.col,
                                span: cell.span.clone(),
                                open_end: cell.open_end,
                                empty: false,
                                kind: cell.kind.as_deref().map(str::to_owned),
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
                        if !cell.has_formula {
                            current = None;
                        } else {
                            if cell.formula_kind == "normal" && cell.formula_text.trim().is_empty()
                            {
                                return Err(unsupported("empty ordinary formula", "worksheet"));
                            }
                            if cell.formula_end == 0 {
                                return Err(unsupported("missing formula boundary", "worksheet"));
                            }
                            cells.push(current.take().expect("open cell").into_cell(bytes));
                            if cells.len() > options.limits.max_formula_cells {
                                return Err(unsupported("formula cell count limit", "worksheet"));
                            }
                        }
                    }
                }
            }
            Ok(())
        })();
        if closes {
            tags.pop();
        }
        result
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
                    crate::xlsx::recalculate::shared_qualifiers::validate(
                        &cell.formula_text,
                        &cell.address(),
                    )?;
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
        raw_dimension_span: None,
        raw_row_refusal_span: None,
        table_merges,
        table_ids,
        cells,
        index,
        serialized,
        bounds: (max_row, max_col),
    })
}
#[cfg(test)]
mod coord_tests {
    use super::{parse_a1_1based, plain_coord};

    /// The fast path agrees with the A1 parser wherever it answers.
    #[test]
    fn plain_coordinates_match_the_parser() {
        let mut inputs = vec![
            "A1",
            "Z9",
            "AA10",
            "XFD1048576",
            "XFE1",
            "XFD1048577",
            "A0",
            "A01",
            "a1",
            "$A1",
            "A$1",
            "AAAA1",
            "A12345678",
            "A",
            "1",
            "",
            "ZZZ1",
            "B2:C3",
            "A1 ",
            "É1",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        let mut state = 0x2545_f491_4f6c_dd1du64;
        for _ in 0..200_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let letters = (state % 4) as usize;
            let digits = ((state >> 8) % 9) as usize;
            let mut s = String::new();
            for i in 0..letters {
                s.push((b'A' + ((state >> (16 + 5 * i)) % 26) as u8) as char);
            }
            for i in 0..digits {
                s.push((b'0' + ((state >> (32 + 3 * i)) % 10) as u8) as char);
            }
            inputs.push(s);
        }
        for input in inputs {
            assert_eq!(
                super::scanner::plain_coord(&input),
                plain_coord(&input),
                "{input}"
            );
            if let Some((row, col)) = plain_coord(&input) {
                assert_eq!(
                    parse_a1_1based(&input).ok(),
                    Some((row, col, false, false)),
                    "{input}"
                );
            }
        }
    }
}
