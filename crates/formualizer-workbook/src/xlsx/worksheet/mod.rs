//! Formula locations/cache spans without a rich cell graph. On request
//! (worksheets with dynamic-array anchors or new spills only), the same
//! bounded pass also builds a source index of rows, cells, dimensions and
//! merges for dynamic-array ownership and geometry.
use crate::xlsx::recalculate::{IoError, XlsxRecalculateOptions, unsupported, xml};
use formualizer_common::coord::parse_a1_1based;
use std::{borrow::Cow, collections::HashMap, ops::Range};

pub(in crate::xlsx) mod compatibility;
mod cursor;
pub(in crate::xlsx) mod events;
#[cfg(test)]
pub(in crate::xlsx) use crate::xlsx::recalculate::tests::oracles::worksheet_legacy as reference;
#[cfg(any(test, feature = "xlsx-recalc"))]
pub(in crate::xlsx) mod scanner;
mod shared;

pub(in crate::xlsx) mod marks;
pub(crate) use marks::{CellKind, FormulaMark, SourceRect, ValueNode};
pub(in crate::xlsx) use marks::{DynamicAttributes, IndexedCell, RowEntry, Scan, SourceIndex};

/// Retained proof that the pinned reader can decode this scalar payload.
/// Literal inputs need the proof; non-authoritative formula caches may instead
/// be treated as absent without rewriting the source bytes.
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
            compatibility: None,
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
pub(in crate::xlsx) enum ScanStrategy {
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
/// Retention requirements do not select an index/rescan strategy or admission policy.
#[derive(Debug, Clone, Copy)]
pub(in crate::xlsx) struct ScanRequest {
    pub retention: crate::xlsx::options::ScanRetention,
    pub strategy: ScanStrategy,
}
impl ScanRequest {
    pub fn recalculate(strategy: ScanStrategy) -> Self {
        Self {
            retention: crate::xlsx::options::ScanRetention {
                values: true,
                formula_events: false,
                marks: true,
                geometry: strategy != ScanStrategy::Plain,
            },
            strategy,
        }
    }
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
    mode: ScanStrategy,
    observed: &mut usize,
    logical_cells: &mut u64,
) -> Result<Scanned, IoError> {
    #[cfg(test)]
    {
        MACHINE_WALKS.with(|n| n.set(n.get() + 1));
        if mode == ScanStrategy::Indexed {
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
    mode: ScanStrategy,
    observed: &mut usize,
    logical_cells: &mut u64,
    emit: impl FnMut(scanner::emission::Literal<'_>) -> Result<(), IoError>,
) -> Result<Scanned, IoError> {
    #[cfg(test)]
    {
        MACHINE_WALKS.with(|n| n.set(n.get() + 1));
        if mode == ScanStrategy::Indexed {
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
#[cfg(test)]
#[path = "../recalculate/tests/oracles/worksheet_intermediate.rs"]
pub(in crate::xlsx) mod oracle_loop;
#[cfg(test)]
pub(in crate::xlsx) use oracle_loop::scan_uncounted;

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
