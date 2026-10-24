//! Retained formula marks, scalar cache proofs and optional source geometry.
//! All spans identify immutable authoritative bytes; evaluation text is separate.
use super::{IoError, rect};
use std::borrow::Cow;
use std::ops::{Deref, Range};

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
    Unsupported,
}
impl CellKind {
    pub(super) fn parse(value: &str) -> Option<Self> {
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
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug)]
pub(crate) struct ValueNode {
    pub span: Range<usize>,
    pub open_end: usize,
    pub close_start: usize,
    pub empty: bool,
    pub(super) prefix_len: usize,
    /// Only entity/EOL-normalised or multi-event payloads need owned text.
    pub(super) decoded: Option<Box<str>>,
    /// Numeric cache decoded once, including trimming used by Cache::matches.
    /// NaN is the non-number/unparseable sentinel (never display-equal).
    pub number: f64,
    pub(super) readable: bool,
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
    pub(in crate::xlsx) compatibility: Option<Box<super::compatibility::Evidence>>,
    #[allow(dead_code)] // consumers use the narrower spans below
    pub span: Range<usize>,
    pub open_end: usize,
    pub(super) prefix_len: usize,
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
    pub(super) original_address: Option<Box<str>>,
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
    pub(in crate::xlsx) fn source_event(&self, sequence: u64) -> super::events::Formula<'_> {
        use super::events::{Formula, FormulaEnvelope, FormulaKind};
        let kind = match self.formula_kind {
            "normal" => FormulaKind::Ordinary {
                text: &self.formula_text,
            },
            "shared" => match self.shared_rect() {
                Some(declared_range) => FormulaKind::SharedAnchor {
                    shared_index: self.shared_id,
                    declared_range,
                    text: &self.formula_text,
                },
                None => FormulaKind::SharedDescendant {
                    shared_index: self.shared_id,
                },
            },
            kind => FormulaKind::Unsupported {
                kind,
                text: &self.formula_text,
            },
        };
        Formula {
            row: self.row,
            col: self.col,
            sequence,
            offset: self.span.start,
            kind,
            envelope: FormulaEnvelope {
                cell: self.span.clone(),
                formula: self.formula_open.start..self.formula_end,
                raw_coordinate: None,
                raw_cell_type: None,
                raw_formula_type: None,
                raw_shared_index: None,
                raw_declared_range: None,
                raw_cache: self.value.as_ref().map(|v| v.open_end..v.close_start),
            },
        }
    }

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
