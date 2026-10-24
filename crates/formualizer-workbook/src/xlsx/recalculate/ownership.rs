//! Proven prior spill footprints and generated-child ownership.
//! Classification only observes immutable source marks and deferred cache refusals.
use super::sheet::{FormulaMark, SourceIndex, SourceRect};
use super::{IoError, XlsxRecalculateOptions, checkpoint, unsupported};
use crate::xlsx::metadata::dynamic::{DynamicBinding, DynamicMetadata};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

/// A source-declared dynamic-array anchor and its prior footprint.
#[derive(Debug, Clone, Copy)]
pub(in crate::xlsx) struct DynamicAnchor {
    #[allow(dead_code)] // the map key is authoritative; kept for diagnostics
    pub row: u32,
    #[allow(dead_code)] // the map key is authoritative; kept for diagnostics
    pub col: u32,
    /// Index into the sheet scan's formula cells.
    pub formula: usize,
    /// Declared `f/@ref`; the anchor is its top-left cell.
    pub footprint: SourceRect,
    /// None denotes a legacy fixed-extent CSE formula.
    pub binding: Option<DynamicBinding>,
}
/// Disjoint anchor -> prior footprint map for one worksheet, plus the mask
/// set of proven generated child cells (serialized cells only, anchor
/// excluded). Keys are one-based `(row, col)`.
#[derive(Debug, Default)]
pub(in crate::xlsx) struct SheetOwnership {
    pub anchors: BTreeMap<(u32, u32), DynamicAnchor>,
    /// Child cell -> owning anchor.
    pub children: BTreeMap<(u32, u32), (u32, u32)>,
}
impl SheetOwnership {
    pub fn owner_of(&self, row: u32, col: u32) -> Option<(u32, u32)> {
        self.children.get(&(row, col)).copied()
    }
}

/// Build the disjoint prior-footprint ownership of one worksheet and apply
/// the literal-cache refusals that the scan deferred until classification.
pub(in crate::xlsx) fn own_sheet(
    index: &SourceIndex,
    cells: &[FormulaMark],
    metadata: Option<&DynamicMetadata>,
    options: &XlsxRecalculateOptions,
) -> Result<SheetOwnership, IoError> {
    let mut own = SheetOwnership::default();
    for (i, cell) in cells.iter().enumerate() {
        if i & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        if cell.cm().is_none() && cell.formula_kind != "array" {
            continue;
        }
        // The scan admitted `cm` only on top-left array anchors with a ref.
        let footprint = cell
            .array_ref()
            .ok_or_else(|| unsupported("missing dynamic array extent", "worksheet"))?;
        let binding = cell
            .cm()
            .map(|cm| {
                metadata
                    .ok_or_else(|| {
                        unsupported(
                            "dangling or invalid dynamic cell metadata index (cm)",
                            "worksheet",
                        )
                    })?
                    .resolve(cm)
            })
            .transpose()?;
        if binding.is_none()
            && footprint
                .cell_count()
                .is_none_or(|n| n > u64::from(options.eval_config.spill.max_spill_cells))
        {
            return Err(unsupported(
                "fixed CSE array extent exceeds spill cap",
                "worksheet",
            ));
        }
        if binding.is_some_and(|b| b.collapsed) && footprint.cell_count() != Some(1) {
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
