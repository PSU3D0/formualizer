//! Remove payloads only from proven generated-child source marks.
//! Styled cell shells and unrelated source attributes remain unchanged.
use super::super::sheet::IndexedCell;
use super::patches::{Patch, remove};

/// Remove every cache payload of a proven generated child cell. Style and
/// other attributes, and the element itself, are kept.
pub(in crate::xlsx) fn mask_child(child: &IndexedCell, patches: &mut Vec<Patch>) {
    for span in [&child.kind_span, &child.value, &child.inline]
        .into_iter()
        .flatten()
    {
        patches.push(remove(span));
    }
}
