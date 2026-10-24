//! Source patch ordering, duplicate coalescing and conflict refusal.
//! Publication patches original source bytes, never evaluation text.
use super::super::{IoError, Patch, unsupported};
#[cfg(test)]
use std::ops::Range;

/// Order one cell's patches and merge duplicates. Two sources may emit the
/// same edit (identical span and replacement); that edit is kept once.
/// Different edits of one span, two insertions at one offset, or any
/// partial overlap are refused rather than applied in an arbitrary order.
/// A zero-length insertion sorts before a replacement starting at the same
/// offset, which `apply_patches` accepts.
pub(in crate::xlsx) fn coalesce(patches: &mut Vec<Patch>) -> Result<(), IoError> {
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
    use crate::xlsx::recalculate::apply_patches;

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
