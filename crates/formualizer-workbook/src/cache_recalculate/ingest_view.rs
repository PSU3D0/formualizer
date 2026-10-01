//! Transient ingestion view of one worksheet (FORM211-B).
//!
//! Calamine reads a patched copy of the worksheet; the authoritative package
//! bytes and the source index are never edited. The view:
//! * clears formula caches Calamine 0.36 cannot decode (the pre-existing
//!   unreadable-cache rule, unchanged);
//! * with spill support, masks proven old-child caches (`c/@t`, `<v>`,
//!   `<is>`), so stale generated values neither enter the value plane nor
//!   obstruct the anchor's new spill, while styled/empty shells stay;
//! * strips admitted anchors' `c/@cm`, `f/@t="array"` and `f/@ref`, so the
//!   anchor replays as an ordinary formula. The formula text is untouched.
//!
//! Edits are collected and coalesced per cell, so no two patches overlap or
//! conflict at one offset.
use super::sheet::{self, IndexedCell};
use super::{Cache, Patch, SheetPlan, cache_patches, checkpoint, unsupported};
use crate::IoError;
use formualizer_eval::engine::CancelToken;
use std::ops::Range;

/// The ingestion-view patches for one worksheet, nonoverlapping and in
/// source order.
pub(super) fn patches(
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
        if let Some(ownership) = &plan.ownership
            && ownership
                .anchors
                .get(&(cell.row, cell.col))
                .is_some_and(|anchor| anchor.formula == i)
        {
            for span in [&cell.cm_span, &cell.formula_kind_span, &cell.array_ref_span]
                .into_iter()
                .flatten()
            {
                cell_patches.push(remove(span));
            }
        }
        flush_cell(&mut cell_patches, &mut out)?;
    }
    if let (Some(ownership), Some(index)) = (&plan.ownership, &plan.index) {
        for &(row, col) in ownership.children.keys() {
            checkpoint(cancel)?;
            let child = index.cell_at(row, col).ok_or_else(|| {
                unsupported("dynamic array child is not indexed", "ingestion view")
            })?;
            mask_child(child, &mut cell_patches);
            flush_cell(&mut cell_patches, &mut out)?;
        }
    }
    out.sort_by_key(|p| (p.span.start, p.span.end));
    Ok(out)
}

/// Remove every cache payload of a proven generated child cell. Style and
/// other attributes, and the element itself, are kept.
pub(super) fn mask_child(child: &IndexedCell, patches: &mut Vec<Patch>) {
    for span in [&child.kind_span, &child.value, &child.inline]
        .into_iter()
        .flatten()
    {
        patches.push(remove(span));
    }
}

fn remove(span: &Range<usize>) -> Patch {
    Patch {
        span: span.clone(),
        replacement: Vec::new(),
    }
}

fn flush_cell(cell: &mut Vec<Patch>, out: &mut Vec<Patch>) -> Result<(), IoError> {
    coalesce(cell)?;
    out.append(cell);
    Ok(())
}

/// Order one cell's patches and merge duplicates. Two sources may emit the
/// same edit (identical span and replacement); that edit is kept once.
/// Different edits of one span, two insertions at one offset, or any
/// partial overlap are refused rather than applied in an arbitrary order.
/// A zero-length insertion sorts before a replacement starting at the same
/// offset, which `apply_patches` accepts.
pub(super) fn coalesce(patches: &mut Vec<Patch>) -> Result<(), IoError> {
    patches.sort_by_key(|p| (p.span.start, p.span.end));
    let mut merged: Vec<Patch> = Vec::with_capacity(patches.len());
    for patch in patches.drain(..) {
        if let Some(last) = merged.last() {
            if last.span == patch.span {
                if last.replacement == patch.replacement {
                    continue;
                }
                return Err(unsupported(
                    "conflicting ingestion view edits",
                    "ingestion view",
                ));
            }
            if patch.span.start < last.span.end {
                return Err(unsupported(
                    "overlapping ingestion view edits",
                    "ingestion view",
                ));
            }
        }
        merged.push(patch);
    }
    *patches = merged;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_recalculate::apply_patches;

    fn patch(span: Range<usize>, text: &str) -> Patch {
        Patch {
            span,
            replacement: text.as_bytes().to_vec(),
        }
    }

    #[test]
    fn duplicate_edits_from_two_sources_are_kept_once() {
        let xml = b"<c r=\"A1\" t=\"e\"><v>#SPILL!</v></c>";
        // A child mask and a cache clear both remove `t="e"` and `<v>`.
        let mut patches = vec![
            patch(16..30, ""),
            patch(10..15, ""),
            patch(10..15, ""),
            patch(16..30, ""),
        ];
        // Unmerged, the duplicates are an overlap.
        assert!(
            apply_patches(
                xml,
                patches.iter().map(|p| patch(p.span.clone(), "")).collect(),
                1 << 20
            )
            .is_err()
        );
        coalesce(&mut patches).unwrap();
        assert_eq!(patches.len(), 2);
        assert_eq!(
            apply_patches(xml, patches, 1 << 20).unwrap(),
            b"<c r=\"A1\" ></c>"
        );
    }

    #[test]
    fn conflicting_or_overlapping_edits_are_refused() {
        for mut patches in [
            vec![patch(3..8, ""), patch(3..8, "<v/>")],
            vec![patch(3..8, ""), patch(5..9, "")],
            vec![patch(4..4, "a"), patch(4..4, "b")],
            vec![patch(2..9, ""), patch(4..4, "x")],
        ] {
            assert!(coalesce(&mut patches).is_err());
        }
    }

    #[test]
    fn insertion_sorts_before_a_replacement_at_the_same_offset() {
        let xml = b"0123456789";
        let mut patches = vec![patch(4..6, "R"), patch(4..4, "I")];
        coalesce(&mut patches).unwrap();
        assert_eq!(apply_patches(xml, patches, 64).unwrap(), b"0123IR6789");
    }
}
