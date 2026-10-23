//! Test-only projected XML input for the independent legacy-ingestion oracle.
//! Production ingestion never constructs this view.
use crate::IoError;
use crate::xlsx::recalculate::publication::patches::remove;
use crate::xlsx::recalculate::publication::{coalesce, mask_child};
use crate::xlsx::recalculate::{
    Cache, Patch, SheetPlan, cache_patches, checkpoint, sheet, unsupported,
};
use formualizer_eval::engine::CancelToken;

/// The ingestion-view patches for one worksheet, nonoverlapping and in
/// source order.
#[cfg(test)]
pub(in crate::xlsx) fn patches(
    plan: &SheetPlan,
    cancel: &Option<CancelToken>,
) -> Result<Vec<Patch>, IoError> {
    let data = &plan.data;
    let mut out = Vec::new();
    let mut cell_patches = Vec::new();
    for (i, cell) in plan.cells.iter().enumerate() {
        if !sheet::readable_scalar_cache(cell, data)
            || matches!(cell.kind.as_deref(), Some("s" | "inlineStr" | "d"))
        {
            cache_patches(data, cell, &Cache::Empty, &mut cell_patches);
        }
        if plan
            .ownership
            .anchors
            .get(&(cell.row, cell.col))
            .is_some_and(|anchor| anchor.formula == i)
        {
            for span in [
                cell.cm_span(),
                cell.formula_kind_span.as_ref(),
                cell.array_ref_span(),
            ]
            .into_iter()
            .flatten()
            {
                cell_patches.push(remove(span));
            }
        }
        flush_cell(&mut cell_patches, &mut out)?;
    }
    for &(row, col) in plan.ownership.children.keys() {
        checkpoint(cancel)?;
        let child = plan
            .index
            .as_ref()
            .and_then(|index| index.cell_at(row, col))
            .ok_or_else(|| unsupported("dynamic array child is not indexed", "ingestion view"))?;
        mask_child(child, &mut cell_patches);
        flush_cell(&mut cell_patches, &mut out)?;
    }
    out.sort_by_key(|p| (p.span.start, p.span.end));
    Ok(out)
}

#[cfg(test)]
fn flush_cell(cell: &mut Vec<Patch>, out: &mut Vec<Patch>) -> Result<(), IoError> {
    coalesce(cell)?;
    out.append(cell);
    Ok(())
}
