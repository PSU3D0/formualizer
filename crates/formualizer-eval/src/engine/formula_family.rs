use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

use super::{
    FormulaMetadataEnvelope, FormulaSourceEvent, FormulaSourceKind, SourceCoord, SourceFamilyId,
    SourceRect, WorkbookLoadLimits,
};

const MAX_FAMILY_EVIDENCE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_SHEET_EVIDENCE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct FormulaFamilyLimits {
    max_points_per_sheet: usize,
    max_family_bytes: u64,
    max_sheet_bytes: u64,
    max_rows: u32,
    max_cols: u32,
}

impl FormulaFamilyLimits {
    pub(crate) fn from_workbook(limits: &WorkbookLoadLimits) -> Self {
        Self {
            max_points_per_sheet: usize::try_from(limits.max_sheet_logical_cells)
                .unwrap_or(usize::MAX),
            max_family_bytes: MAX_FAMILY_EVIDENCE_BYTES,
            max_sheet_bytes: MAX_SHEET_EVIDENCE_BYTES,
            max_rows: limits.max_sheet_rows,
            max_cols: limits.max_sheet_cols,
        }
    }

    #[cfg(test)]
    pub(crate) fn testing(points: usize, family_bytes: u64, sheet_bytes: u64) -> Self {
        Self {
            max_points_per_sheet: points,
            max_family_bytes: family_bytes,
            max_sheet_bytes: sheet_bytes,
            max_rows: u32::MAX,
            max_cols: u32::MAX,
        }
    }

    #[cfg(test)]
    fn testing_with_bounds(max_rows: u32, max_cols: u32) -> Self {
        Self {
            max_rows,
            max_cols,
            ..Self::testing(10_000, u64::MAX, u64::MAX)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EligibleSourceFamily {
    pub sheet_name: String,
    pub source_id: SourceFamilyId,
    pub coords0: Vec<SourceCoord>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FormulaFamilyCollection {
    pub report: FormulaFamilyCollectionReport,
    pub eligible: Vec<EligibleSourceFamily>,
}

impl std::ops::Deref for FormulaFamilyCollection {
    type Target = FormulaFamilyCollectionReport;

    fn deref(&self) -> &Self::Target {
        &self.report
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FormulaFamilyCollectionReport {
    pub families_seen: u64,
    pub family_cells_seen: u64,
    pub eligible_families: u64,
    pub eligible_cells: u64,
    pub fallback_families: u64,
    pub fallback_cells: u64,
    pub forward_descendants: u64,
    pub evidence_limit_fallbacks: u64,
    pub evidence_peak_bytes: u64,
    pub fallback_reasons: BTreeMap<String, u64>,
}

impl FormulaFamilyCollectionReport {
    fn reason(&mut self, reason: &'static str) {
        let count = self.fallback_reasons.entry(reason.to_string()).or_default();
        *count = count.saturating_add(1);
    }
}

#[derive(Clone, Copy, Debug)]
struct Anchor {
    coord: SourceCoord,
    sequence: u64,
    declared_range: Option<SourceRect>,
    empty: bool,
}

#[derive(Clone, Copy)]
struct PointIndex {
    coord: SourceCoord,
    event: u32,
}

// For a simple one-family sheet this is the complete collector high-water payload per
// member: sheet reference, point and family indices, unique coordinate, Fenwick
// column/tree, and the candidate/output coordinate vectors.
const FAMILY_MEMBER_EVIDENCE_BYTES: u64 = (size_of::<&FormulaSourceEvent>()
    + size_of::<PointIndex>()
    + size_of::<u32>()
    + size_of::<SourceCoord>()
    + size_of::<u32>()
    + size_of::<u64>()
    + 2 * size_of::<SourceCoord>()) as u64;
const FAMILY_EVIDENCE_OVERHEAD_BYTES: u64 = 256;

/// Classifies source-declared families without reading or mutating FormulaPlane authority.
/// Declared rectangles are only queried; no operation is proportional to their area.
pub(crate) fn collect_formula_families(
    events: &[FormulaSourceEvent],
    limits: FormulaFamilyLimits,
) -> FormulaFamilyCollection {
    let mut collection = FormulaFamilyCollection::default();
    let mut by_sheet: BTreeMap<&str, Vec<&FormulaSourceEvent>> = BTreeMap::new();
    for event in events {
        by_sheet
            .entry(event.sheet_name.as_ref())
            .or_default()
            .push(event);
    }

    for (sheet_name, sheet_events) in by_sheet {
        collect_sheet(sheet_name, &sheet_events, limits, &mut collection);
    }
    collection
}

fn collect_sheet(
    sheet_name: &str,
    events: &[&FormulaSourceEvent],
    limits: FormulaFamilyLimits,
    collection: &mut FormulaFamilyCollection,
) {
    let report = &mut collection.report;

    // Events remain authoritative. The collector retains only compact indices, and the
    // coordinate index is sorted once and then shared by occupancy and range checks.
    let family_records = events
        .iter()
        .filter(|event| event_family(event).is_some())
        .count();
    let base_bytes = (events.len() as u64)
        .saturating_mul(size_of::<PointIndex>() as u64)
        .saturating_add((family_records as u64).saturating_mul(size_of::<u32>() as u64))
        .saturating_add(
            (events.len() as u64).saturating_mul(size_of::<&FormulaSourceEvent>() as u64),
        );
    if events.len() > limits.max_points_per_sheet
        || events.len() > u32::MAX as usize
        || base_bytes > limits.max_sheet_bytes
    {
        record_sheet_limit(report, family_records, base_bytes);
        return;
    }

    let mut points = Vec::with_capacity(events.len());
    let mut family_indices = Vec::with_capacity(family_records);
    for (index, event) in events.iter().enumerate() {
        let index = index as u32;
        points.push(PointIndex {
            coord: event.coord0,
            event: index,
        });
        if event_family(event).is_some() {
            family_indices.push(index);
        }
    }
    points.sort_unstable_by_key(|point| (point.coord.row, point.coord.col));
    family_indices.sort_unstable_by_key(|index| {
        let event = events[*index as usize];
        (
            event_family(event).expect("family index"),
            event.coord0.row,
            event.coord0.col,
            event.source_sequence,
        )
    });

    let family_count = family_group_count(events, &family_indices);
    // Account for every collector-owned payload at the range-query high-water mark.
    // Using the observed-point count before allocating the unique-coordinate vector is
    // conservative when duplicate source records share a coordinate and keeps the cap
    // fail-closed before the larger query structures are retained.
    let working_bytes = base_bytes
        .saturating_add((points.len() as u64).saturating_mul(size_of::<SourceCoord>() as u64))
        .saturating_add(
            (points.len() as u64).saturating_mul((size_of::<u32>() + size_of::<u64>()) as u64),
        )
        .saturating_add(
            (family_records as u64).saturating_mul((2 * size_of::<SourceCoord>()) as u64),
        )
        .saturating_add((family_count as u64).saturating_mul(256));
    report.evidence_peak_bytes = report.evidence_peak_bytes.max(working_bytes);
    if working_bytes > limits.max_sheet_bytes {
        record_grouped_sheet_limit(report, events, &family_indices);
        return;
    }

    let groups = family_groups(events, &family_indices);
    let mut unique_points = Vec::with_capacity(points.len());
    for point in &points {
        if unique_points.last().copied() != Some(point.coord) {
            unique_points.push(point.coord);
        }
    }
    let query_rects: Vec<Option<SourceRect>> = groups
        .iter()
        .map(|&(start, end)| usable_rect(events, &family_indices[start..end], limits))
        .collect();
    let occupied_in_rect = rectangle_point_counts(&unique_points, &query_rects);

    for (((start, end), rect_for_query), occupied_count) in
        groups.into_iter().zip(query_rects).zip(occupied_in_rect)
    {
        let indices = &family_indices[start..end];
        let source_id = event_family(events[indices[0] as usize]).expect("family index");
        let records = indices.len() as u64;
        report.families_seen = report.families_seen.saturating_add(1);
        report.family_cells_seen = report.family_cells_seen.saturating_add(records);
        if records
            .saturating_mul(FAMILY_MEMBER_EVIDENCE_BYTES)
            .saturating_add(FAMILY_EVIDENCE_OVERHEAD_BYTES)
            > limits.max_family_bytes
        {
            report.fallback_families = report.fallback_families.saturating_add(1);
            report.fallback_cells = report.fallback_cells.saturating_add(records);
            report.evidence_limit_fallbacks = report.evidence_limit_fallbacks.saturating_add(1);
            report.reason("EvidenceLimit");
            continue;
        }

        let mut reasons = BTreeSet::new();
        let anchors: Vec<_> = indices
            .iter()
            .filter_map(|index| event_anchor(events[*index as usize]))
            .collect();
        if anchors.is_empty() {
            reasons.insert("MissingAnchor");
        } else if anchors.len() > 1 {
            reasons.insert("DuplicateAnchor");
        }
        if anchors.iter().any(|anchor| anchor.empty) {
            reasons.insert("ExpansionFailed");
        }
        if indices
            .iter()
            .any(|index| !metadata_valid(events[*index as usize]))
        {
            reasons.insert("UnknownMetadata");
        }

        let mut unique = Vec::with_capacity(indices.len());
        for index in indices {
            let coord = events[*index as usize].coord0;
            if unique.last().copied() == Some(coord) {
                reasons.insert("MixedSourceRecords");
            } else {
                unique.push(coord);
            }
        }

        if let Some(first_anchor) = anchors.iter().map(|anchor| anchor.sequence).min() {
            let forward = indices
                .iter()
                .map(|index| events[*index as usize])
                .filter(|event| is_descendant(event) && event.source_sequence < first_anchor)
                .count() as u64;
            report.forward_descendants = report.forward_descendants.saturating_add(forward);
        }

        let rect = if anchors.len() == 1 {
            let anchor = anchors[0];
            match anchor.declared_range {
                None => {
                    reasons.insert("MissingDeclaredRange");
                    None
                }
                Some(rect) if !valid_rect(rect, limits) || !contains(rect, anchor.coord) => {
                    reasons.insert("InvalidDeclaredRange");
                    None
                }
                Some(rect) => Some(rect),
            }
        } else {
            None
        };
        debug_assert_eq!(rect, rect_for_query);

        if let Some(rect) = rect {
            if indices
                .iter()
                .map(|index| events[*index as usize])
                .any(|event| is_descendant(event) && !contains(rect, event.coord0))
            {
                reasons.insert("OutOfRangeDescendant");
            }

            let in_range: Vec<_> = unique
                .iter()
                .copied()
                .filter(|coord| contains(rect, *coord))
                .collect();
            if !proves_full_rectangle(rect, &in_range) {
                reasons.insert("Hole");
            }
            let mixed_coordinate = in_range
                .iter()
                .any(|coord| coordinate_has_exception(&points, events, *coord, source_id));
            if occupied_count > in_range.len() as u64 || mixed_coordinate {
                reasons.insert("Exception");
            }
        }

        if reasons.is_empty() {
            report.eligible_families = report.eligible_families.saturating_add(1);
            report.eligible_cells = report.eligible_cells.saturating_add(records);
            collection.eligible.push(EligibleSourceFamily {
                sheet_name: sheet_name.to_string(),
                source_id,
                coords0: unique,
            });
        } else {
            report.fallback_families = report.fallback_families.saturating_add(1);
            report.fallback_cells = report.fallback_cells.saturating_add(records);
            for reason in reasons {
                report.reason(reason);
            }
        }
    }
}

fn event_anchor(event: &FormulaSourceEvent) -> Option<Anchor> {
    match &event.formula {
        FormulaSourceKind::SharedAnchor {
            declared_range,
            formula,
            ..
        } => Some(Anchor {
            coord: event.coord0,
            sequence: event.source_sequence,
            declared_range: *declared_range,
            empty: formula.is_empty(),
        }),
        _ => None,
    }
}

fn metadata_valid(event: &FormulaSourceEvent) -> bool {
    match &event.formula {
        FormulaSourceKind::SharedAnchor {
            family,
            declared_range,
            metadata,
            ..
        } => matches!(
            metadata,
            FormulaMetadataEnvelope::XlsxShared { shared_index, parsed_range }
                if *shared_index == family.shared_index && parsed_range == declared_range
        ),
        FormulaSourceKind::SharedDescendant { family, metadata } => matches!(
            metadata,
            FormulaMetadataEnvelope::XlsxShared { shared_index, .. }
                if *shared_index == family.shared_index
        ),
        FormulaSourceKind::Ordinary { .. } | FormulaSourceKind::Unsupported { .. } => true,
    }
}

fn is_descendant(event: &FormulaSourceEvent) -> bool {
    matches!(event.formula, FormulaSourceKind::SharedDescendant { .. })
}

fn event_family(event: &FormulaSourceEvent) -> Option<SourceFamilyId> {
    match &event.formula {
        FormulaSourceKind::SharedAnchor { family, .. }
        | FormulaSourceKind::SharedDescendant { family, .. } => Some(*family),
        FormulaSourceKind::Ordinary { .. } | FormulaSourceKind::Unsupported { .. } => None,
    }
}

fn family_group_count(events: &[&FormulaSourceEvent], indices: &[u32]) -> usize {
    let mut count = 0;
    let mut previous = None;
    for index in indices {
        let family = event_family(events[*index as usize]);
        if family != previous {
            count += 1;
            previous = family;
        }
    }
    count
}

fn family_groups(events: &[&FormulaSourceEvent], indices: &[u32]) -> Vec<(usize, usize)> {
    let mut groups = Vec::new();
    let mut start = 0;
    while start < indices.len() {
        let family = event_family(events[indices[start] as usize]).expect("family index");
        let mut end = start + 1;
        while end < indices.len() && event_family(events[indices[end] as usize]) == Some(family) {
            end += 1;
        }
        groups.push((start, end));
        start = end;
    }
    groups
}

fn usable_rect(
    events: &[&FormulaSourceEvent],
    indices: &[u32],
    limits: FormulaFamilyLimits,
) -> Option<SourceRect> {
    let mut anchors = indices
        .iter()
        .filter_map(|index| event_anchor(events[*index as usize]));
    let anchor = anchors.next()?;
    if anchors.next().is_some() {
        return None;
    }
    anchor
        .declared_range
        .filter(|rect| valid_rect(*rect, limits) && contains(*rect, anchor.coord))
}

fn coordinate_has_exception(
    points: &[PointIndex],
    events: &[&FormulaSourceEvent],
    coord: SourceCoord,
    family: SourceFamilyId,
) -> bool {
    let start = points.partition_point(|point| point.coord < coord);
    points[start..]
        .iter()
        .take_while(|point| point.coord == coord)
        .any(|point| event_family(events[point.event as usize]) != Some(family))
}

fn record_sheet_limit(
    report: &mut FormulaFamilyCollectionReport,
    family_records: usize,
    estimated_bytes: u64,
) {
    if family_records != 0 {
        report.family_cells_seen = report
            .family_cells_seen
            .saturating_add(family_records as u64);
        report.families_seen = report.families_seen.saturating_add(1);
        report.fallback_families = report.fallback_families.saturating_add(1);
        report.fallback_cells = report.fallback_cells.saturating_add(family_records as u64);
        report.evidence_limit_fallbacks = report.evidence_limit_fallbacks.saturating_add(1);
        report.reason("EvidenceLimit");
    }
    report.evidence_peak_bytes = report.evidence_peak_bytes.max(estimated_bytes);
}

fn record_grouped_sheet_limit(
    report: &mut FormulaFamilyCollectionReport,
    events: &[&FormulaSourceEvent],
    indices: &[u32],
) {
    let mut start = 0;
    while start < indices.len() {
        let family = event_family(events[indices[start] as usize]).expect("family index");
        let mut end = start + 1;
        while end < indices.len() && event_family(events[indices[end] as usize]) == Some(family) {
            end += 1;
        }
        let records = (end - start) as u64;
        report.families_seen = report.families_seen.saturating_add(1);
        report.fallback_families = report.fallback_families.saturating_add(1);
        report.evidence_limit_fallbacks = report.evidence_limit_fallbacks.saturating_add(1);
        report.family_cells_seen = report.family_cells_seen.saturating_add(records);
        report.fallback_cells = report.fallback_cells.saturating_add(records);
        report.reason("EvidenceLimit");
        start = end;
    }
}

fn valid_rect(rect: SourceRect, limits: FormulaFamilyLimits) -> bool {
    rect.start.row <= rect.end.row
        && rect.start.col <= rect.end.col
        && rect.end.row < limits.max_rows
        && rect.end.col < limits.max_cols
        && rectangle_area(rect).is_some()
}

fn contains(rect: SourceRect, coord: SourceCoord) -> bool {
    coord.row >= rect.start.row
        && coord.row <= rect.end.row
        && coord.col >= rect.start.col
        && coord.col <= rect.end.col
}

fn rectangle_area(rect: SourceRect) -> Option<u64> {
    let rows = u64::from(rect.end.row)
        .checked_sub(u64::from(rect.start.row))?
        .checked_add(1)?;
    let cols = u64::from(rect.end.col)
        .checked_sub(u64::from(rect.start.col))?
        .checked_add(1)?;
    rows.checked_mul(cols)
}

fn proves_full_rectangle(rect: SourceRect, sorted_unique: &[SourceCoord]) -> bool {
    let Some(area) = rectangle_area(rect) else {
        return false;
    };
    if area != sorted_unique.len() as u64
        || sorted_unique.first().copied() != Some(rect.start)
        || sorted_unique.last().copied() != Some(rect.end)
    {
        return false;
    }

    let width = u64::from(rect.end.col) - u64::from(rect.start.col) + 1;
    for (index, coord) in sorted_unique.iter().enumerate() {
        let index = index as u64;
        let expected_row = u64::from(rect.start.row) + index / width;
        let expected_col = u64::from(rect.start.col) + index % width;
        if u64::from(coord.row) != expected_row || u64::from(coord.col) != expected_col {
            return false;
        }
    }
    true
}

// Offline orthogonal range counting: four prefix queries per rectangle and one Fenwick sweep.
fn rectangle_point_counts(points: &[SourceCoord], rects: &[Option<SourceRect>]) -> Vec<u64> {
    #[derive(Clone, Copy)]
    struct Query {
        row: i64,
        col: i64,
        rect: usize,
        sign: i8,
    }

    let mut queries = Vec::with_capacity(rects.len() * 4);
    for (index, rect) in rects.iter().enumerate() {
        let Some(rect) = rect else { continue };
        let r0 = i64::from(rect.start.row) - 1;
        let c0 = i64::from(rect.start.col) - 1;
        let r1 = i64::from(rect.end.row);
        let c1 = i64::from(rect.end.col);
        queries.extend([
            Query {
                row: r1,
                col: c1,
                rect: index,
                sign: 1,
            },
            Query {
                row: r0,
                col: c1,
                rect: index,
                sign: -1,
            },
            Query {
                row: r1,
                col: c0,
                rect: index,
                sign: -1,
            },
            Query {
                row: r0,
                col: c0,
                rect: index,
                sign: 1,
            },
        ]);
    }
    queries.sort_unstable_by_key(|query| query.row);

    let mut columns: Vec<u32> = points.iter().map(|point| point.col).collect();
    columns.sort_unstable();
    columns.dedup();
    let mut tree = vec![0_u64; columns.len() + 1];
    let mut totals = vec![0_i128; rects.len()];
    let mut point_index = 0;
    for query in queries {
        while point_index < points.len() && i64::from(points[point_index].row) <= query.row {
            let mut slot = columns.partition_point(|col| *col < points[point_index].col) + 1;
            while slot < tree.len() {
                tree[slot] += 1;
                slot += slot & slot.wrapping_neg();
            }
            point_index += 1;
        }
        let mut slot = columns.partition_point(|col| i64::from(*col) <= query.col);
        let mut count = 0_u64;
        while slot > 0 {
            count += tree[slot];
            slot &= slot - 1;
        }
        totals[query.rect] += i128::from(query.sign) * i128::from(count);
    }
    totals
        .into_iter()
        .map(|count| count.max(0) as u64)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{FormulaSourceKind, SourceCachedValue};
    use std::sync::Arc;

    fn id(index: usize) -> SourceFamilyId {
        SourceFamilyId {
            sheet_instance: 0,
            shared_index: index,
        }
    }

    fn rect(r0: u32, c0: u32, r1: u32, c1: u32) -> SourceRect {
        SourceRect {
            start: SourceCoord { row: r0, col: c0 },
            end: SourceCoord { row: r1, col: c1 },
        }
    }

    fn anchor(
        index: usize,
        sequence: u64,
        row: u32,
        col: u32,
        range: Option<SourceRect>,
    ) -> FormulaSourceEvent {
        FormulaSourceEvent {
            sheet_name: Arc::from("Sheet1"),
            coord0: SourceCoord { row, col },
            source_sequence: sequence,
            formula: FormulaSourceKind::SharedAnchor {
                family: id(index),
                declared_range: range,
                formula: Arc::from("A1+1"),
                metadata: FormulaMetadataEnvelope::XlsxShared {
                    shared_index: index,
                    parsed_range: range,
                },
            },
            cached: SourceCachedValue::AbsentOrEmpty,
        }
    }

    fn descendant(index: usize, sequence: u64, row: u32, col: u32) -> FormulaSourceEvent {
        FormulaSourceEvent {
            sheet_name: Arc::from("Sheet1"),
            coord0: SourceCoord { row, col },
            source_sequence: sequence,
            formula: FormulaSourceKind::SharedDescendant {
                family: id(index),
                metadata: FormulaMetadataEnvelope::XlsxShared {
                    shared_index: index,
                    parsed_range: None,
                },
            },
            cached: SourceCachedValue::AbsentOrEmpty,
        }
    }

    fn ordinary(sequence: u64, row: u32, col: u32) -> FormulaSourceEvent {
        FormulaSourceEvent {
            sheet_name: Arc::from("Sheet1"),
            coord0: SourceCoord { row, col },
            source_sequence: sequence,
            formula: FormulaSourceKind::Ordinary {
                formula: Arc::from("1"),
                metadata: FormulaMetadataEnvelope::XlsxOrdinary,
            },
            cached: SourceCachedValue::AbsentOrEmpty,
        }
    }

    fn classify(events: &[FormulaSourceEvent]) -> FormulaFamilyCollection {
        collect_formula_families(
            events,
            FormulaFamilyLimits::testing(10_000, u64::MAX, u64::MAX),
        )
    }

    fn has(report: &FormulaFamilyCollectionReport, reason: &str) -> bool {
        report.fallback_reasons.contains_key(reason)
    }

    #[test]
    fn valid_family_is_permutation_invariant_and_reconciles() {
        let events = vec![
            anchor(7, 2, 0, 0, Some(rect(0, 0, 1, 1))),
            descendant(7, 1, 0, 1),
            descendant(7, 3, 1, 0),
            descendant(7, 4, 1, 1),
        ];
        let expected = classify(&events);
        assert_eq!(expected.eligible_families, 1);
        assert_eq!(expected.eligible_cells, 4);
        assert_eq!(expected.forward_descendants, 1);
        assert_eq!(
            expected.eligible_cells + expected.fallback_cells,
            expected.family_cells_seen
        );
        for order in [[3, 2, 1, 0], [1, 0, 3, 2], [2, 3, 0, 1]] {
            let permuted: Vec<_> = order.into_iter().map(|i| events[i].clone()).collect();
            assert_eq!(classify(&permuted), expected);
        }
    }

    #[test]
    fn anchor_and_coordinate_anomalies_are_detected() {
        let missing = classify(&[descendant(1, 0, 0, 0)]);
        assert!(has(&missing, "MissingAnchor"));

        let duplicate_anchor = classify(&[
            anchor(1, 0, 0, 0, Some(rect(0, 0, 0, 1))),
            anchor(1, 1, 0, 1, Some(rect(0, 0, 0, 1))),
        ]);
        assert!(has(&duplicate_anchor, "DuplicateAnchor"));

        let duplicate_member = classify(&[
            anchor(1, 0, 0, 0, Some(rect(0, 0, 0, 1))),
            descendant(1, 1, 0, 1),
            descendant(1, 2, 0, 1),
        ]);
        assert!(has(&duplicate_member, "MixedSourceRecords"));

        let mut empty = anchor(1, 0, 0, 0, Some(rect(0, 0, 0, 0)));
        if let FormulaSourceKind::SharedAnchor { formula, .. } = &mut empty.formula {
            *formula = Arc::from("");
        }
        assert!(has(&classify(&[empty]), "ExpansionFailed"));
    }

    #[test]
    fn range_hole_exception_and_out_of_range_anomalies_are_detected() {
        assert!(has(
            &classify(&[anchor(1, 0, 0, 0, None)]),
            "MissingDeclaredRange"
        ));
        assert!(has(
            &classify(&[anchor(1, 0, 1, 1, Some(rect(2, 2, 1, 1)))]),
            "InvalidDeclaredRange"
        ));

        let hole = classify(&[
            anchor(1, 0, 0, 0, Some(rect(0, 0, 0, 2))),
            descendant(1, 1, 0, 2),
        ]);
        assert!(has(&hole, "Hole"));

        let ordinary_exception = classify(&[
            anchor(1, 0, 0, 0, Some(rect(0, 0, 0, 2))),
            ordinary(1, 0, 1),
            descendant(1, 2, 0, 2),
        ]);
        assert!(has(&ordinary_exception, "Exception"));

        let other_family_exception = classify(&[
            anchor(1, 0, 0, 0, Some(rect(0, 0, 0, 2))),
            anchor(2, 1, 0, 1, Some(rect(0, 1, 0, 1))),
            descendant(1, 2, 0, 2),
        ]);
        assert!(has(&other_family_exception, "Exception"));

        let outside = classify(&[
            anchor(1, 0, 0, 0, Some(rect(0, 0, 0, 0))),
            descendant(1, 1, 0, 1),
        ]);
        assert!(has(&outside, "OutOfRangeDescendant"));
    }

    #[test]
    fn unsupported_family_metadata_and_checked_area_are_fail_closed() {
        let mut bad = descendant(1, 1, 0, 1);
        bad.formula = FormulaSourceKind::SharedDescendant {
            family: id(1),
            metadata: FormulaMetadataEnvelope::XlsxUnknown,
        };
        let report = classify(&[anchor(1, 0, 0, 0, Some(rect(0, 0, 0, 1))), bad]);
        assert!(has(&report, "UnknownMetadata"));

        let overflow = classify(&[anchor(1, 0, 0, 0, Some(rect(0, 0, u32::MAX, u32::MAX)))]);
        assert!(has(&overflow, "InvalidDeclaredRange"));

        let outside_limits = collect_formula_families(
            &[anchor(1, 0, 0, 0, Some(rect(0, 0, 10, 0)))],
            FormulaFamilyLimits::testing_with_bounds(10, 10),
        );
        assert!(has(&outside_limits, "InvalidDeclaredRange"));
    }

    #[test]
    fn huge_declared_rectangle_uses_observed_points_only() {
        let report = classify(&[
            anchor(1, 0, 0, 0, Some(rect(0, 0, 1_048_575, 16_383))),
            descendant(1, 1, 1_048_575, 16_383),
        ]);
        assert!(has(&report, "Hole"));
        assert_eq!(report.family_cells_seen, 2);
        assert!(report.evidence_peak_bytes < 1024);
    }

    #[test]
    fn compact_evidence_keeps_a_100k_family_below_the_promotion_cap() {
        let members = 100_000_u32;
        let declared = rect(0, 0, members - 1, 0);
        let mut events = Vec::with_capacity(members as usize);
        events.push(anchor(1, 0, 0, 0, Some(declared)));
        events.extend((1..members).map(|row| descendant(1, u64::from(row), row, 0)));

        let result = collect_formula_families(
            &events,
            FormulaFamilyLimits::testing(
                members as usize,
                MAX_FAMILY_EVIDENCE_BYTES,
                MAX_SHEET_EVIDENCE_BYTES,
            ),
        );

        assert_eq!(result.eligible_families, 1);
        assert_eq!(result.eligible_cells, u64::from(members));
        assert_eq!(result.evidence_limit_fallbacks, 0);
        assert!(result.evidence_peak_bytes < MAX_FAMILY_EVIDENCE_BYTES);
    }

    #[test]
    fn pathological_many_family_evidence_caps_with_exact_reconciliation() {
        let families = 1_000_usize;
        let events: Vec<_> = (0..families)
            .map(|index| {
                anchor(
                    index,
                    index as u64,
                    index as u32,
                    0,
                    Some(rect(index as u32, 0, index as u32, 0)),
                )
            })
            .collect();

        let result = collect_formula_families(
            &events,
            FormulaFamilyLimits::testing(families, u64::MAX, 100_000),
        );

        assert_eq!(result.families_seen, families as u64);
        assert_eq!(result.evidence_limit_fallbacks, families as u64);
        assert_eq!(result.fallback_cells, result.family_cells_seen);
        assert_eq!(result.eligible_families, 0);
        assert!(result.evidence_peak_bytes > 100_000);
    }

    #[test]
    fn point_and_byte_caps_fallback_without_losing_reconciliation() {
        let events = [
            anchor(1, 0, 0, 0, Some(rect(0, 0, 0, 1))),
            descendant(1, 1, 0, 1),
        ];
        for limits in [
            FormulaFamilyLimits::testing(1, u64::MAX, u64::MAX),
            FormulaFamilyLimits::testing(10, 1, u64::MAX),
            FormulaFamilyLimits::testing(10, u64::MAX, 1),
        ] {
            let report = collect_formula_families(&events, limits);
            assert_eq!(report.evidence_limit_fallbacks, 1);
            assert!(has(&report, "EvidenceLimit"));
            assert_eq!(report.fallback_cells, report.family_cells_seen);
        }
    }
}
