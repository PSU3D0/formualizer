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
    /// Serialized literal cache indices owned by unsupported data tables.
    pub unavailable_children: Vec<usize>,
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
    own.unavailable_children = data_table_children(index, cells, &own, options)?;
    Ok(own)
}

/// Sweep disjoint rectangles without expanding their logical area.
pub(super) fn disjoint_rects(
    mut rects: Vec<SourceRect>,
    options: &XlsxRecalculateOptions,
) -> Result<bool, IoError> {
    rects.sort_unstable_by_key(|r| (r.first_row, r.first_col));
    let mut active: BTreeMap<u32, (u32, usize)> = BTreeMap::new();
    let mut expiry = BinaryHeap::new();
    for (id, r) in rects.into_iter().enumerate() {
        if id & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        while let Some(Reverse((last, col, old))) = expiry.peek().copied() {
            if last >= r.first_row {
                break;
            }
            expiry.pop();
            if active.get(&col).is_some_and(|(_, current)| *current == old) {
                active.remove(&col);
            }
        }
        if active
            .range(..=r.last_col)
            .next_back()
            .is_some_and(|(_, (last, _))| *last >= r.first_col)
        {
            return Ok(false);
        }
        active.insert(r.first_col, (r.last_col, id));
        expiry.push(Reverse((r.last_row, r.first_col, id)));
    }
    Ok(true)
}
fn data_table_children(
    index: &SourceIndex,
    cells: &[FormulaMark],
    own: &SheetOwnership,
    options: &XlsxRecalculateOptions,
) -> Result<Vec<usize>, IoError> {
    let rects: Vec<_> = cells
        .iter()
        .filter_map(FormulaMark::data_table_ref)
        .collect();
    if rects.is_empty() {
        return Ok(Vec::new());
    }
    if !index.merges.is_empty() {
        return Err(unsupported(
            "data-table masking in a merged worksheet is unverified",
            "worksheet",
        ));
    }
    if !disjoint_rects(
        rects
            .iter()
            .copied()
            .chain(own.anchors.values().map(|a| a.footprint))
            .collect(),
        options,
    )? {
        return Err(unsupported(
            "overlapping data-table/array ownership",
            "worksheet",
        ));
    }
    let mut active: BTreeMap<u32, usize> = BTreeMap::new();
    let mut expiry = BinaryHeap::new();
    let mut next = 0;
    let mut children = Vec::new();
    for (i, cell) in index.cells.iter().enumerate() {
        if i & 1023 == 0 {
            checkpoint(&options.cancel)?;
        }
        while let Some(Reverse((last, col, id))) = expiry.peek().copied() {
            if last >= cell.row {
                break;
            }
            expiry.pop();
            if active.get(&col) == Some(&id) {
                active.remove(&col);
            }
        }
        while next < rects.len() && rects[next].first_row <= cell.row {
            let r = rects[next];
            if r.last_row >= cell.row {
                active.insert(r.first_col, next);
                expiry.push(Reverse((r.last_row, r.first_col, next)));
            }
            next += 1;
        }
        if let Some((_, id)) = active.range(..=cell.col).next_back() {
            let r = rects[*id];
            if r.contains(cell.row, cell.col) && (r.first_row, r.first_col) != (cell.row, cell.col)
            {
                if cell.formula.is_some() {
                    return Err(unsupported(
                        "formula inside data-table owned outputs",
                        "worksheet",
                    ));
                }
                children.push(i);
            }
        }
    }
    Ok(children)
}
