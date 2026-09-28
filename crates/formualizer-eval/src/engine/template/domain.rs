//! Internal FormulaPlane runtime store vocabulary for FP6.1.

use rustc_hash::FxHashMap;

use crate::SheetId;
use crate::engine::arena::AstNodeId;

use super::canonical::{CanonicalReference, LiteralSlotId, SlotContext};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PlacementDomain {
    RowRun {
        sheet_id: SheetId,
        row_start: u32,
        row_end: u32,
        col: u32,
    },
    ColRun {
        sheet_id: SheetId,
        row: u32,
        col_start: u32,
        col_end: u32,
    },
    Rect {
        sheet_id: SheetId,
        row_start: u32,
        row_end: u32,
        col_start: u32,
        col_end: u32,
    },
}

impl PlacementDomain {
    pub(crate) fn row_run(sheet_id: SheetId, row_start: u32, row_end: u32, col: u32) -> Self {
        Self::RowRun {
            sheet_id,
            row_start,
            row_end,
            col,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResultRegion {
    domain: PlacementDomain,
}

impl ResultRegion {
    pub(crate) fn scalar_cells(domain: PlacementDomain) -> Self {
        Self { domain }
    }

    pub(crate) fn domain(&self) -> &PlacementDomain {
        &self.domain
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ValueRefSlotId(pub(crate) u16);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ValueRefSlotDescriptor {
    pub(crate) slot_id: ValueRefSlotId,
    pub(crate) preorder_index: u32,
    pub(crate) context: SlotContext,
    pub(crate) reference_pattern: CanonicalReference,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TemplateSlotMap {
    pub(crate) literal_slots_by_arena_node: FxHashMap<AstNodeId, LiteralSlotId>,
    pub(crate) residual_relative_row: bool,
    pub(crate) residual_relative_col: bool,
}
