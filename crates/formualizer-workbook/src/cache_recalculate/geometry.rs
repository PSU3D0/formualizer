//! Worksheet geometry for dynamic-array publication (FORM211-D).
//!
//! One ordered patch plan per worksheet, built in one bounded pass over the
//! desired cell states (row-major) merged with the source index:
//! * members of a current spill get typed caches through the cache encoders;
//!   missing `<c>`/`<row>` elements are inserted in coordinate order with the
//!   source namespace prefix;
//! * obsolete old-child caches are cleared with `mask_child`; styled or
//!   commented shells (the elements and their other attributes) stay;
//! * genuine inputs and unrelated formulas are never edited; a member landing
//!   on an unowned serialized value or formula is refused;
//! * each anchor gets `t="array"` and `ref="<current extent>"` (the anchor
//!   cell when collapsed, blocked or erroring); its cell-metadata binding is
//!   kept, or requested from the binding resolver through [`Binding`];
//! * the dimension grows when needed (a still-valid old one is kept), and
//!   optional row `spans` follow the policy in [`row_spans`];
//! * a successful spill intersecting a merge rectangle is refused.
//!
//! All insertions at one offset are concatenated in coordinate order into a
//! single patch, and every cell's edits are coalesced, so `apply_patches`
//! sees no equal-offset insertions or overlaps.
use super::ingest_view::{coalesce, mask_child};
use super::sheet::{IndexedCell, SourceIndex, SourceRect};
use super::{
    Cache, IoError, Patch, SheetPlan, XlsxRecalculateOptions, cache_patches, checkpoint,
    unsupported,
};
use formualizer_eval::engine::CancelToken;
use std::collections::BTreeMap;

/// The anchor's cell-metadata (`c/@cm`) binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Binding {
    /// Keep the source `cm`: an admitted anchor whose XLDAPR record still
    /// describes its current shape.
    Keep,
    /// The anchor needs an XLDAPR binding that the source does not supply
    /// (a new anchor, or a collapsed record now describing a multi-cell
    /// spill). The binding resolver supplies a one-based `cm`.
    NeedsXldapr,
}

/// A binding the geometry plan asks the binding resolver for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BindingRequest {
    pub row: u32,
    pub col: u32,
    /// Whether the current result is a multi-cell spill (else 1x1).
    pub multi_cell: bool,
}

/// The binding resolver: a one-based `cm` of a compatible XLDAPR record
/// for the request (see `dynamic_metadata::Binder`).
pub(super) type BindingResolver<'a> = dyn FnMut(BindingRequest) -> Result<u32, IoError> + 'a;
/// Reads the current value of one generated member of an anchor.
pub(super) type MemberReader<'a> = dyn FnMut(&AnchorEdit, u32, u32) -> Result<Cache, IoError> + 'a;

/// The geometry input for one anchor.
#[derive(Debug)]
pub(super) struct AnchorEdit {
    pub row: u32,
    pub col: u32,
    /// Index into `SheetPlan::cells`.
    pub formula: usize,
    /// The current multi-cell extent, `None` when collapsed/blocked/error.
    pub spill: Option<SourceRect>,
    /// The anchor's own cache.
    pub cache: Cache,
    pub binding: Binding,
}
impl AnchorEdit {
    /// The `ref` to publish: the spill extent or the anchor cell.
    pub fn extent(&self) -> SourceRect {
        self.spill.unwrap_or(SourceRect {
            first_row: self.row,
            first_col: self.col,
            last_row: self.row,
            last_col: self.col,
        })
    }
}

/// The ordered worksheet edits.
pub(super) struct SheetEdits {
    pub patches: Vec<Patch>,
    /// Physical caches inserted, replaced or cleared (anchors and members).
    pub caches_changed: usize,
}

/// Bounds of one sheet's current spills, checked before any member value
/// is read.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Preflight {
    /// Cells a successful spill would add to the serialized source.
    pub inserted: u64,
    /// The largest row/column a current extent reaches.
    pub bounds: (u32, u32),
}

/// Width, merge and generated-cell bounds of one sheet's current extents.
/// Nothing is materialized; this only reads the source index.
pub(super) fn preflight(
    index: &SourceIndex,
    anchors: &[AnchorEdit],
    options: &XlsxRecalculateOptions,
) -> Result<Preflight, IoError> {
    let mut out = Preflight::default();
    let mut polled = 0usize;
    let mut poll = |options: &XlsxRecalculateOptions| {
        polled += 1;
        if polled & 1023 == 0 {
            checkpoint(&options.cancel)
        } else {
            Ok(())
        }
    };
    for anchor in anchors {
        checkpoint(&options.cancel)?;
        let Some(extent) = anchor.spill else {
            continue;
        };
        if extent.last_col > options.limits.max_columns {
            return Err(unsupported("dynamic array extent width limit", "worksheet"));
        }
        // Merged cells are not hydrated into the engine: never publish a
        // successful spill across them.
        for merge in &index.merges {
            poll(options)?;
            if merge.intersects(extent) {
                return Err(unsupported(
                    "dynamic spill intersects a merged cell range",
                    "worksheet",
                ));
            }
        }
        let area = extent
            .cell_count()
            .ok_or_else(|| unsupported("dynamic array extent cell limit", "worksheet"))?;
        let first = index.rows.partition_point(|r| r.row < extent.first_row);
        let mut serialized = 0u64;
        for row in index.rows[first..]
            .iter()
            .take_while(|r| r.row <= extent.last_row)
        {
            poll(options)?;
            let cells = &index.cells[row.cells.clone()];
            let lo = cells.partition_point(|c| c.col < extent.first_col);
            let hi = cells.partition_point(|c| c.col <= extent.last_col);
            serialized += (hi - lo) as u64;
        }
        out.inserted = out
            .inserted
            .checked_add(area.saturating_sub(serialized))
            .ok_or_else(|| unsupported("generated spill cell limit", "worksheet"))?;
        out.bounds.0 = out.bounds.0.max(extent.last_row);
        out.bounds.1 = out.bounds.1.max(extent.last_col);
    }
    Ok(out)
}

/// One pending insertion (or self-closing element expansion) at an offset.
struct Pending {
    end: usize,
    head: Vec<u8>,
    body: Vec<u8>,
    tail: Vec<u8>,
}

/// Build the ordered patch plan for one worksheet. `member` reads the
/// current value of one generated member; `bind` resolves a requested
/// XLDAPR binding to a one-based `cm`. The caller has already
/// run [`preflight`] and the workbook-wide generated-cell budget.
pub(super) fn plan(
    plan: &SheetPlan,
    anchors: &[AnchorEdit],
    member: &mut MemberReader<'_>,
    bind: &mut BindingResolver<'_>,
    cancel: &Option<CancelToken>,
) -> Result<SheetEdits, IoError> {
    let data = &plan.data;
    let index = &plan.index;
    let ownership = &plan.ownership;
    let mut polled = 0usize;
    let mut poll = || {
        polled += 1;
        if polled & 1023 == 0 {
            checkpoint(cancel)
        } else {
            Ok(())
        }
    };

    // Desired final state of every generated cell: `None` clears an old
    // child, `Some` writes a member. Bounded by the source children plus the
    // budget-checked extents.
    let mut desired: BTreeMap<(u32, u32), Option<Cache>> = BTreeMap::new();
    for &key in ownership.children.keys() {
        poll()?;
        desired.insert(key, None);
    }
    for anchor in anchors {
        checkpoint(cancel)?;
        let Some(extent) = anchor.spill else {
            continue;
        };
        for row in extent.first_row..=extent.last_row {
            for col in extent.first_col..=extent.last_col {
                if (row, col) == (anchor.row, anchor.col) {
                    continue;
                }
                poll()?;
                if let Some(cell) = index.cell_at(row, col) {
                    if cell.formula.is_some() {
                        return Err(unsupported(
                            "dynamic spill over a source formula",
                            "worksheet",
                        ));
                    }
                    let owned = ownership.owner_of(row, col).is_some();
                    if !owned && (cell.value.is_some() || cell.inline.is_some()) {
                        return Err(unsupported(
                            "dynamic spill over an unowned source value",
                            "worksheet",
                        ));
                    }
                }
                let cache = member(anchor, row, col)?;
                if let Some(Some(_)) = desired.insert((row, col), Some(cache)) {
                    return Err(unsupported(
                        "overlapping dynamic spill extents",
                        "worksheet",
                    ));
                }
            }
        }
    }

    let prefix = element_prefix(data, index.sheet_data.start);
    let mut patches = Vec::new();
    let mut changed = 0usize;
    let mut pending: BTreeMap<usize, Pending> = BTreeMap::new();
    // Missing row currently open in `pending`, keyed by its offset.
    let mut open_row: Option<(u32, usize)> = None;
    // Existing rows (index into `index.rows`) gaining inserted cells.
    let mut widened: BTreeMap<usize, (u32, u32)> = BTreeMap::new();
    let mut cell_patches = Vec::new();
    for ((row, col), want) in desired {
        poll()?;
        if let Some((open, key)) = open_row
            && open != row
        {
            close_row(&mut pending, key, &prefix);
            open_row = None;
        }
        if let Some(cell) = index.cell_at(row, col) {
            match &want {
                None => mask_child(cell, &mut cell_patches),
                Some(cache) => write_existing(data, cell, cache, &mut cell_patches),
            }
            if !cell_patches.is_empty() {
                changed += 1;
                coalesce(&mut cell_patches)?;
                patches.append(&mut cell_patches);
            }
            continue;
        }
        // Old children are serialized by construction; an empty member
        // needs no cell.
        let Some(cache) = want.filter(|c| !matches!(c, Cache::Empty)) else {
            continue;
        };
        let xml = new_cell(&prefix, row, col, &cache);
        changed += 1;
        match index.rows.binary_search_by_key(&row, |r| r.row) {
            Ok(r) => {
                let entry = &index.rows[r];
                let cells = &index.cells[entry.cells.clone()];
                let at = cells.partition_point(|c| c.col < col);
                let slot = if let Some(next) = cells.get(at) {
                    insertion(next.span.start)
                } else if let Some(last) = cells.last() {
                    insertion(last.span.end)
                } else if !entry.empty {
                    insertion(entry.open_end)
                } else {
                    expansion(entry.span.end, format!("</{}>", entry.qualified))
                };
                push(&mut pending, slot)?.body.extend_from_slice(&xml);
                let w = widened.entry(r).or_insert((col, col));
                w.0 = w.0.min(col);
                w.1 = w.1.max(col);
            }
            Err(r) => {
                if open_row.is_none() {
                    let slot = if let Some(next) = index.rows.get(r) {
                        insertion(next.span.start)
                    } else if let Some(last) = index.rows.last() {
                        insertion(last.span.end)
                    } else if !index.sheet_data_empty {
                        insertion(index.sheet_data_open_end)
                    } else {
                        expansion(index.sheet_data.end, format!("</{prefix}sheetData>"))
                    };
                    let key = slot.0;
                    push(&mut pending, slot)?
                        .body
                        .extend_from_slice(format!("<{prefix}row r=\"{row}\">").as_bytes());
                    open_row = Some((row, key));
                }
                let key = open_row.expect("open row").1;
                pending
                    .get_mut(&key)
                    .expect("pending row")
                    .body
                    .extend_from_slice(&xml);
            }
        }
    }
    if let Some((_, key)) = open_row {
        close_row(&mut pending, key, &prefix);
    }
    for (start, p) in pending {
        let mut replacement = p.head;
        replacement.extend_from_slice(&p.body);
        replacement.extend_from_slice(&p.tail);
        patches.push(Patch {
            span: start..p.end,
            replacement,
        });
    }
    for (r, (lo, hi)) in widened {
        poll()?;
        if let Some(patch) = row_spans(data, &index.rows[r].spans_attr, lo, hi) {
            patches.push(patch);
        }
    }

    // Anchors: own cache, `t="array"`/`ref`, cell-metadata binding.
    for anchor in anchors {
        checkpoint(cancel)?;
        let cell = &plan.cells[anchor.formula];
        if anchor.cache.matches(cell) {
            // No cache edit.
        } else {
            changed += 1;
            cache_patches(data, cell, &anchor.cache, &mut cell_patches);
        }
        let extent = anchor.extent();
        let reference = format!("ref=\"{}\"", a1_rect(extent));
        if cell.formula_kind == "array" {
            let span = cell
                .array_ref_span
                .clone()
                .ok_or_else(|| unsupported("missing dynamic array extent", "worksheet"))?;
            if cell.array_ref != Some(extent) {
                cell_patches.push(Patch {
                    span,
                    replacement: reference.into_bytes(),
                });
            }
        } else if cell.formula_kind == "normal" {
            // A new anchor: the source `<f>` start tag gains the array kind
            // and extent; its text and other attributes are unchanged.
            let at = cell.formula_open.end - 1;
            if data.get(at) != Some(&b'>') || data.get(at - 1) == Some(&b'/') {
                return Err(unsupported("empty new dynamic array formula", "worksheet"));
            }
            match &cell.formula_kind_span {
                Some(span) => {
                    cell_patches.push(Patch {
                        span: span.clone(),
                        replacement: b"t=\"array\"".to_vec(),
                    });
                    cell_patches.push(Patch {
                        span: at..at,
                        replacement: format!(" {reference}").into_bytes(),
                    });
                }
                None => cell_patches.push(Patch {
                    span: at..at,
                    replacement: format!(" t=\"array\" {reference}").into_bytes(),
                }),
            }
        } else {
            return Err(unsupported(
                "multi-cell dynamic spill from a shared formula family member",
                "worksheet",
            ));
        }
        if anchor.binding == Binding::NeedsXldapr {
            let request = BindingRequest {
                row: anchor.row,
                col: anchor.col,
                multi_cell: anchor.spill.is_some(),
            };
            let cm = bind(request)?;
            set_cell_metadata(&mut cell_patches, cell, cm);
        }
        coalesce(&mut cell_patches)?;
        patches.append(&mut cell_patches);
    }

    // Dimension: grow to cover every current extent; keep a valid old one.
    if let Some((dimension, span)) = &index.dimension {
        let mut grown = *dimension;
        for extent in anchors.iter().filter_map(|a| a.spill) {
            grown = union(grown, extent);
        }
        if grown != *dimension {
            patches.push(Patch {
                span: span.clone(),
                replacement: format!("ref=\"{}\"", a1_rect(grown)).into_bytes(),
            });
        }
    }
    patches.sort_by_key(|p| (p.span.start, p.span.end));
    Ok(SheetEdits {
        patches,
        caches_changed: changed,
    })
}

fn insertion(at: usize) -> (usize, usize, Vec<u8>, Vec<u8>) {
    (at, at, Vec::new(), Vec::new())
}
/// Expand a self-closing element ending at `end` (`/>`) into an open/close
/// pair around the inserted content.
fn expansion(end: usize, close: String) -> (usize, usize, Vec<u8>, Vec<u8>) {
    (end - 2, end, b">".to_vec(), close.into_bytes())
}
fn push(
    pending: &mut BTreeMap<usize, Pending>,
    (start, end, head, tail): (usize, usize, Vec<u8>, Vec<u8>),
) -> Result<&mut Pending, IoError> {
    let entry = pending.entry(start).or_insert_with(|| Pending {
        end,
        head: head.clone(),
        body: Vec::new(),
        tail: tail.clone(),
    });
    if entry.end != end || entry.head != head || entry.tail != tail {
        return Err(unsupported(
            "conflicting worksheet geometry insertions",
            "worksheet",
        ));
    }
    Ok(entry)
}
fn close_row(pending: &mut BTreeMap<usize, Pending>, key: usize, prefix: &str) {
    pending
        .get_mut(&key)
        .expect("pending row")
        .body
        .extend_from_slice(format!("</{prefix}row>").as_bytes());
}

/// `p:` for an element whose start tag begins at `start`, or empty.
fn element_prefix(data: &[u8], start: usize) -> String {
    let name: Vec<u8> = data[start + 1..]
        .iter()
        .take_while(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n' | b'/' | b'>'))
        .copied()
        .collect();
    let name = String::from_utf8_lossy(&name);
    name.rsplit_once(':')
        .map(|(p, _)| format!("{p}:"))
        .unwrap_or_default()
}

fn column_name(mut col: u32) -> String {
    let mut out = Vec::new();
    while col > 0 {
        col -= 1;
        out.push(b'A' + (col % 26) as u8);
        col /= 26;
    }
    out.reverse();
    String::from_utf8(out).expect("ASCII column")
}
fn a1(row: u32, col: u32) -> String {
    format!("{}{row}", column_name(col))
}
pub(super) fn a1_rect(rect: SourceRect) -> String {
    let first = a1(rect.first_row, rect.first_col);
    if rect.cell_count() == Some(1) {
        first
    } else {
        format!("{first}:{}", a1(rect.last_row, rect.last_col))
    }
}
fn union(a: SourceRect, b: SourceRect) -> SourceRect {
    SourceRect {
        first_row: a.first_row.min(b.first_row),
        first_col: a.first_col.min(b.first_col),
        last_row: a.last_row.max(b.last_row),
        last_col: a.last_col.max(b.last_col),
    }
}

fn kind_attribute(cache: &Cache) -> String {
    cache
        .kind()
        .map(|t| format!(" t=\"{t}\""))
        .unwrap_or_default()
}
/// A new member cell; `cache` is never `Empty` here.
fn new_cell(prefix: &str, row: u32, col: u32, cache: &Cache) -> Vec<u8> {
    format!(
        "<{prefix}c r=\"{}\"{}><{prefix}v>{}</{prefix}v></{prefix}c>",
        a1(row, col),
        kind_attribute(cache),
        cache.text()
    )
    .into_bytes()
}

/// Write a member cache into an existing non-formula cell: type attribute,
/// `<v>` and no `<is>`. Other attributes and children are kept. An
/// identical existing cache yields no patch.
fn write_existing(data: &[u8], cell: &IndexedCell, cache: &Cache, out: &mut Vec<Patch>) {
    if matches!(cache, Cache::Empty) {
        mask_child(cell, out);
        return;
    }
    let qualified_prefix = element_prefix(data, cell.span.start);
    let wanted = cache.kind();
    let current = cell.kind.as_deref().filter(|t| *t != "n");
    if wanted != current {
        match &cell.kind_span {
            Some(span) => out.push(Patch {
                span: span.clone(),
                replacement: wanted
                    .map(|t| format!("t=\"{t}\"").into_bytes())
                    .unwrap_or_default(),
            }),
            None => {
                let at = if cell.empty {
                    cell.span.end - 2
                } else {
                    cell.open_end - 1
                };
                out.push(Patch {
                    span: at..at,
                    replacement: kind_attribute(cache).into_bytes(),
                });
            }
        }
    }
    if let Some(span) = &cell.inline {
        out.push(Patch {
            span: span.clone(),
            replacement: Vec::new(),
        });
    }
    let v = format!(
        "<{qualified_prefix}v>{}</{qualified_prefix}v>",
        cache.text()
    );
    match &cell.value {
        Some(span) => {
            if data[span.clone()] != *v.as_bytes() {
                out.push(Patch {
                    span: span.clone(),
                    replacement: v.into_bytes(),
                });
            }
        }
        None if cell.empty => {
            let name = cell_name(data, cell.span.start);
            out.push(Patch {
                span: cell.span.end - 2..cell.span.end,
                replacement: format!(">{v}</{name}>").into_bytes(),
            });
        }
        None => out.push(Patch {
            span: cell.open_end..cell.open_end,
            replacement: v.into_bytes(),
        }),
    }
}
fn cell_name(data: &[u8], start: usize) -> String {
    let name: Vec<u8> = data[start + 1..]
        .iter()
        .take_while(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n' | b'/' | b'>'))
        .copied()
        .collect();
    String::from_utf8_lossy(&name).into_owned()
}

/// Set `c/@cm` on an anchor: replace an existing binding or insert one at
/// the end of the start tag, after any type attribute inserted there.
fn set_cell_metadata(out: &mut Vec<Patch>, cell: &super::sheet::Cell, cm: u32) {
    let attribute = format!("cm=\"{cm}\"");
    if let Some(span) = &cell.cm_span {
        out.push(Patch {
            span: span.clone(),
            replacement: attribute.into_bytes(),
        });
        return;
    }
    let at = cell.open_end - 1;
    if let Some(insert) = out.iter_mut().find(|p| p.span == (at..at)) {
        insert.replacement.push(b' ');
        insert.replacement.extend_from_slice(attribute.as_bytes());
    } else {
        out.push(Patch {
            span: at..at,
            replacement: format!(" {attribute}").into_bytes(),
        });
    }
}

/// Row `spans` policy. `spans` is an optional optimization hint. When a row
/// gains inserted cells in columns `lo..=hi`:
/// * a single `a:b` range that already covers them is kept;
/// * a single `a:b` range is widened to `min(a,lo):max(b,hi)`;
/// * any other value (several ranges, unparsable) is removed.
///
/// Rows without `spans` and inserted rows get none. Cleared cells keep
/// their shells, so no `spans` ever needs to shrink.
fn row_spans(
    data: &[u8],
    attribute: &Option<std::ops::Range<usize>>,
    lo: u32,
    hi: u32,
) -> Option<Patch> {
    let span = attribute.clone()?;
    let text = std::str::from_utf8(&data[span.clone()]).ok();
    let value = text
        .and_then(|t| t.split_once('='))
        .map(|(_, v)| v.trim().trim_matches(|c| c == '"' || c == '\''));
    let single = value.and_then(|v| {
        let (a, b) = v.split_once(':')?;
        Some((a.parse::<u32>().ok()?, b.parse::<u32>().ok()?))
    });
    let replacement = match single {
        Some((a, b)) if a <= lo && hi <= b => return None,
        Some((a, b)) if a >= 1 && a <= b => {
            format!("spans=\"{}:{}\"", a.min(lo), b.max(hi)).into_bytes()
        }
        _ => Vec::new(),
    };
    Some(Patch { span, replacement })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a1_and_columns() {
        assert_eq!(column_name(1), "A");
        assert_eq!(column_name(26), "Z");
        assert_eq!(column_name(27), "AA");
        assert_eq!(column_name(16_384), "XFD");
        let rect = SourceRect {
            first_row: 2,
            first_col: 3,
            last_row: 6,
            last_col: 3,
        };
        assert_eq!(a1_rect(rect), "C2:C6");
        assert_eq!(
            a1_rect(SourceRect {
                last_row: 2,
                ..rect
            }),
            "C2"
        );
    }

    #[test]
    fn row_spans_policy() {
        let data = b"<row r=\"4\" spans=\"3:3\">";
        let attr = Some(11..22);
        assert_eq!(&data[11..22], b"spans=\"3:3\"");
        assert!(row_spans(data, &attr, 3, 3).is_none());
        assert_eq!(
            row_spans(data, &attr, 4, 4).unwrap().replacement,
            b"spans=\"3:4\""
        );
        let data = b"<row r=\"4\" spans=\"1:2 5:6\">";
        assert!(
            row_spans(data, &Some(11..26), 3, 3)
                .unwrap()
                .replacement
                .is_empty()
        );
        assert!(row_spans(data, &None, 3, 3).is_none());
    }

    #[test]
    fn prefixes_follow_the_source_element() {
        assert_eq!(element_prefix(b"<x:sheetData>", 0), "x:");
        assert_eq!(element_prefix(b"<sheetData/>", 0), "");
        assert_eq!(cell_name(b"<x:c r=\"A1\"/>", 0), "x:c");
    }
}
