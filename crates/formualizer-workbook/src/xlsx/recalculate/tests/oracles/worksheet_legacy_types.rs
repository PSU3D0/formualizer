//! Frozen owned records for the pre-compact scanner oracle. Test-only: changing
//! production storage must not also change the reference's scalar decisions.
use crate::xlsx::worksheet::{SourceIndex, SourceRect};
use std::{ops::Range, rc::Rc};

pub(crate) struct ValueNode {
    pub span: Range<usize>,
    pub open_end: usize,
    pub close_start: usize,
    pub empty: bool,
    pub qualified: Rc<str>,
    pub text: String,
}
pub(crate) struct Cell {
    pub row: u32,
    pub col: u32,
    pub address: String,
    pub span: Range<usize>,
    pub open_end: usize,
    pub qualified: Rc<str>,
    pub kind: Option<Rc<str>>,
    pub kind_span: Option<Range<usize>>,
    pub formula_end: usize,
    pub formula_text: String,
    pub value: Option<ValueNode>,
    pub inline: Option<Range<usize>>,
    pub formula_kind: &'static str,
    pub shared_id: Option<u32>,
    pub shared_range: Option<(u32, u32, u32, u32)>,
    pub has_formula: bool,
    pub formula_open: Range<usize>,
    pub formula_kind_span: Option<Range<usize>>,
    pub dynamic: Option<Box<DynamicAttributes>>,
}
#[derive(Default)]
pub(crate) struct DynamicAttributes {
    pub cm: Option<u32>,
    pub cm_span: Option<Range<usize>>,
    pub array_ref: Option<SourceRect>,
    pub array_ref_span: Option<Range<usize>>,
}
impl Cell {
    pub fn cm(&self) -> Option<u32> {
        self.dynamic.as_ref().and_then(|d| d.cm)
    }
    pub fn array_ref(&self) -> Option<SourceRect> {
        self.dynamic.as_ref().and_then(|d| d.array_ref)
    }
}
pub(crate) struct Scan {
    pub cells: Vec<Cell>,
    pub table_ids: Vec<String>,
    pub table_merges: Vec<SourceRect>,
    pub hidden_rows: Vec<u32>,
    pub rows_hidden_by_default: bool,
    pub has_dimension: bool,
    pub active_filters: Vec<SourceRect>,
    pub index: Option<SourceIndex>,
    pub serialized: usize,
    pub bounds: (u32, u32),
}
pub(crate) enum Scanned {
    Done(Box<Scan>),
    NeedsIndex,
}
// Explicitly read every field in the comparison output, including provenance
// fields only observed through this oracle (without dead-code lint exceptions).
macro_rules! record_debug {
    ($ty:ident: $($field:ident),+ $(,)?) => {
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($ty))
                    $(.field(stringify!($field), &self.$field))+
                    .finish()
            }
        }
    };
}
record_debug!(ValueNode: span, open_end, close_start, empty, qualified, text);
record_debug!(Cell: row, col, address, span, open_end, qualified, kind, kind_span,
    formula_end, formula_text, value, inline, formula_kind, shared_id, shared_range,
    has_formula, formula_open, formula_kind_span, dynamic);
record_debug!(DynamicAttributes: cm, cm_span, array_ref, array_ref_span);
record_debug!(Scan: cells, table_ids, table_merges, hidden_rows, rows_hidden_by_default,
    has_dimension, active_filters, index, serialized, bounds);
impl std::fmt::Debug for Scanned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Done(scan) => f.debug_tuple("Done").field(scan).finish(),
            Self::NeedsIndex => f.write_str("NeedsIndex"),
        }
    }
}
