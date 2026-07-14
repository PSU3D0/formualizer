use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

use formualizer_eval::engine::{
    FormulaCompressedSourceReport, PlacementDomainTransport, SourceCoord, SourceFamilyId,
    SourceFamilyMembers, SourceFormulaFamily, SourceFormulaOrder, SourceRect,
};

const DEFAULT_EVIDENCE_LIMIT: u64 = 8 * 1024 * 1024;
const FAMILY_BYTES: u64 = 256;
const RUN_BYTES: u64 = size_of::<SourceRect>() as u64;
const EXCLUSION_BYTES: u64 = size_of::<SourceExclusion>() as u64;
const FRAGMENT_BYTES: u64 = size_of::<PlacementDomainTransport>() as u64;
pub(super) const DEFAULT_MAX_FAMILY_EXCLUSIONS: usize = 64;
pub(super) const DEFAULT_MAX_FAMILY_FRAGMENTS: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SourceExclusion {
    Hole(SourceCoord),
    OrdinaryFormula(SourceCoord),
}

impl SourceExclusion {
    fn coord(self) -> SourceCoord {
        match self {
            Self::Hole(coord) | Self::OrdinaryFormula(coord) => coord,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct FragmentedFamilyProposal {
    pub(super) source_id: SourceFamilyId,
    pub(super) source_order: SourceFormulaOrder,
    pub(super) anchor_coord0: SourceCoord,
    pub(super) anchor_text: Arc<str>,
    pub(super) declared: SourceRect,
    pub(super) fragments: Vec<PlacementDomainTransport>,
    pub(super) fallback_members: Vec<SourceCoord>,
    pub(super) exclusions: Vec<SourceExclusion>,
    pub(super) member_count: u64,
}

#[derive(Clone, Copy)]
pub(super) enum EvidenceRecord<'a> {
    Ordinary,
    Anchor {
        family: SourceFamilyId,
        range: Option<SourceRect>,
        text: &'a str,
    },
    Descendant {
        family: SourceFamilyId,
    },
    Unsupported,
}

#[derive(Default)]
struct FamilyEvidence {
    source_order: Option<SourceFormulaOrder>,
    members: u64,
    anchors: u32,
    anchor: Option<SourceCoord>,
    anchor_text: Option<Arc<str>>,
    range: Option<SourceRect>,
    next: Option<SourceCoord>,
    exclusions: Vec<SourceExclusion>,
    anomaly: Option<&'static str>,
}

pub(super) struct MonotonicFormulaEvidence {
    families: BTreeMap<SourceFamilyId, FamilyEvidence>,
    active_families: Vec<SourceFamilyId>,
    last_coord: Option<SourceCoord>,
    halted: Option<&'static str>,
    evidence_bytes: u64,
    evidence_peak_bytes: u64,
    limit: u64,
    max_exclusions: usize,
    max_fragments: usize,
    counters: [u64; 5],
}

impl MonotonicFormulaEvidence {
    pub(super) fn new() -> Self {
        Self::with_limit(DEFAULT_EVIDENCE_LIMIT)
    }

    fn with_limit(limit: u64) -> Self {
        Self {
            families: BTreeMap::new(),
            active_families: Vec::new(),
            last_coord: None,
            halted: None,
            evidence_bytes: 0,
            evidence_peak_bytes: 0,
            limit,
            max_exclusions: DEFAULT_MAX_FAMILY_EXCLUSIONS,
            max_fragments: DEFAULT_MAX_FAMILY_FRAGMENTS,
            counters: [0; 5],
        }
    }

    #[cfg(test)]
    fn with_partition_limits(limit: u64, max_exclusions: usize, max_fragments: usize) -> Self {
        let mut evidence = Self::with_limit(limit);
        evidence.max_exclusions = max_exclusions;
        evidence.max_fragments = max_fragments;
        evidence
    }

    #[cfg(test)]
    pub(super) fn observe(&mut self, coord: SourceCoord, record: EvidenceRecord<'_>) {
        let source_order = SourceFormulaOrder::new(self.counters[0]);
        self.observe_ordered(coord, source_order, record);
    }

    pub(super) fn observe_ordered(
        &mut self,
        coord: SourceCoord,
        source_order: SourceFormulaOrder,
        record: EvidenceRecord<'_>,
    ) {
        self.counters[0] = self.counters[0].saturating_add(1);
        match record {
            EvidenceRecord::Ordinary => self.counters[1] = self.counters[1].saturating_add(1),
            EvidenceRecord::Anchor { .. } => self.counters[2] = self.counters[2].saturating_add(1),
            EvidenceRecord::Descendant { .. } => {
                self.counters[3] = self.counters[3].saturating_add(1)
            }
            EvidenceRecord::Unsupported => self.counters[4] = self.counters[4].saturating_add(1),
        }

        if let Some(previous) = self.last_coord
            && coord <= previous
        {
            self.halt(if coord == previous {
                "DuplicateCoordinate"
            } else {
                "CoordinateDisorder"
            });
        }
        self.last_coord = Some(coord);

        let owner = match record {
            EvidenceRecord::Anchor { family, .. } | EvidenceRecord::Descendant { family } => {
                Some(family)
            }
            _ => None,
        };
        if let Some(family) = owner {
            if !self.ensure_family(family) {
                return;
            }
            let evidence = self
                .families
                .get_mut(&family)
                .expect("retained family exists");
            evidence.source_order = Some(
                evidence
                    .source_order
                    .map_or(source_order, |current| current.min(source_order)),
            );
            evidence.members = evidence.members.saturating_add(1);
        }
        if self.halted.is_some() {
            return;
        }

        self.advance_active_families(coord, source_order, record, owner);
        if self.halted.is_some() {
            return;
        }

        match record {
            EvidenceRecord::Ordinary | EvidenceRecord::Unsupported => {}
            EvidenceRecord::Anchor {
                family,
                range,
                text,
            } => {
                self.mark_range_conflicts(family, range);
                {
                    let evidence = self.families.get_mut(&family).unwrap();
                    evidence.anchors = evidence.anchors.saturating_add(1);
                    if evidence.members != 1 || evidence.anchors != 1 {
                        self.halt("ForwardAnchor");
                        return;
                    }
                    if text.is_empty() {
                        evidence.anomaly = Some("EmptyAnchor");
                    }
                    let Some(range) = range else {
                        evidence.anomaly = Some("MissingDeclaredRange");
                        return;
                    };
                    if !valid_rect(range) {
                        evidence.anomaly = Some("InvalidDeclaredRange");
                        return;
                    }
                    if coord != range.start {
                        evidence.anomaly = Some("RangeStartMismatch");
                    }
                }
                let growth = RUN_BYTES.saturating_add(text.len() as u64);
                let next_bytes = self.evidence_bytes.saturating_add(growth);
                if next_bytes > self.limit {
                    self.halt("EvidenceLimit");
                    return;
                }
                self.evidence_bytes = next_bytes;
                self.evidence_peak_bytes = self.evidence_peak_bytes.max(next_bytes);
                let evidence = self.families.get_mut(&family).unwrap();
                let range = range.expect("validated shared formula range");
                evidence.anchor = Some(coord);
                evidence.anchor_text = Some(Arc::from(text));
                evidence.range = Some(range);
                evidence.next = next_coord(coord, range);
                self.active_families.push(family);
            }
            EvidenceRecord::Descendant { family } => {
                let evidence = self.families.get_mut(&family).unwrap();
                if evidence.anchor.is_none() {
                    self.halt("ForwardAnchor");
                    return;
                }
                if !evidence.range.is_some_and(|range| contains(range, coord)) {
                    evidence.anomaly.get_or_insert("OutOfRange");
                }
            }
        }
    }

    fn advance_active_families(
        &mut self,
        coord: SourceCoord,
        source_order: SourceFormulaOrder,
        record: EvidenceRecord<'_>,
        owner: Option<SourceFamilyId>,
    ) {
        let active = self.active_families.clone();
        let remaining_bytes = self.limit.saturating_sub(self.evidence_bytes);
        let byte_capacity =
            usize::try_from(remaining_bytes / EXCLUSION_BYTES).unwrap_or(usize::MAX);
        let max_exclusions = self.max_exclusions.min(byte_capacity);
        let mut retained = Vec::with_capacity(active.len());
        let mut added = 0usize;
        for family in active {
            let evidence = self
                .families
                .get_mut(&family)
                .expect("active family exists");
            let Some(range) = evidence.range else {
                continue;
            };
            let disposition = if owner == Some(family) {
                ActivePointDisposition::FamilyMember
            } else {
                match record {
                    EvidenceRecord::Ordinary => ActivePointDisposition::Ordinary,
                    EvidenceRecord::Unsupported => ActivePointDisposition::Unsupported,
                    EvidenceRecord::Anchor { .. } | EvidenceRecord::Descendant { .. } => {
                        ActivePointDisposition::OtherFamily
                    }
                }
            };
            if matches!(disposition, ActivePointDisposition::Ordinary)
                && range.start <= coord
                && coord <= range.end
            {
                evidence.source_order = Some(
                    evidence
                        .source_order
                        .map_or(source_order, |current| current.min(source_order)),
                );
            }
            added = added.saturating_add(advance_family_evidence(
                evidence,
                range,
                coord,
                disposition,
                max_exclusions,
            ));
            if range.end >= coord {
                retained.push(family);
            }
        }
        self.active_families = retained;
        let growth = (added as u64).saturating_mul(EXCLUSION_BYTES);
        self.evidence_bytes = self.evidence_bytes.saturating_add(growth);
        self.evidence_peak_bytes = self.evidence_peak_bytes.max(self.evidence_bytes);
        if self.evidence_bytes > self.limit {
            self.halt("EvidenceLimit");
        }
    }

    fn mark_range_conflicts(&mut self, owner: SourceFamilyId, owner_range: Option<SourceRect>) {
        let Some(owner_range) = owner_range else {
            return;
        };
        let mut conflict = false;
        for family in &self.active_families {
            if *family == owner {
                continue;
            }
            let evidence = self.families.get_mut(family).expect("active family exists");
            if evidence
                .range
                .is_some_and(|range| rectangles_overlap(owner_range, range))
            {
                evidence.anomaly.get_or_insert("OtherFamilyConflict");
                conflict = true;
            }
        }
        if conflict {
            self.families
                .get_mut(&owner)
                .expect("owner family exists")
                .anomaly
                .get_or_insert("OtherFamilyConflict");
        }
    }

    fn ensure_family(&mut self, family: SourceFamilyId) -> bool {
        if self.families.contains_key(&family) {
            return true;
        }
        if self.halted.is_some() {
            return false;
        }
        self.grow(FAMILY_BYTES);
        if self.halted.is_some() {
            return false;
        }
        self.families.insert(family, FamilyEvidence::default());
        true
    }

    fn grow(&mut self, bytes: u64) {
        let next = self.evidence_bytes.saturating_add(bytes);
        if next > self.limit {
            self.halt("EvidenceLimit");
            return;
        }
        self.evidence_bytes = next;
        self.evidence_peak_bytes = self.evidence_peak_bytes.max(next);
    }

    fn halt(&mut self, reason: &'static str) {
        if self.halted.is_none() {
            self.halted = Some(reason);
        }
    }

    pub(super) fn finish(mut self) -> CompressedEvidenceOutput {
        if self.halted.is_none() {
            let family_ids: Vec<_> = self.families.keys().copied().collect();
            for family_id in family_ids {
                let remaining_bytes = self.limit.saturating_sub(self.evidence_bytes);
                let byte_capacity =
                    usize::try_from(remaining_bytes / EXCLUSION_BYTES).unwrap_or(usize::MAX);
                let max_exclusions = self.max_exclusions.min(byte_capacity);
                let evidence = self.families.get_mut(&family_id).expect("family exists");
                let Some(range) = evidence.range else {
                    continue;
                };
                let added = finish_family_evidence(evidence, range, max_exclusions);
                self.evidence_bytes = self
                    .evidence_bytes
                    .saturating_add((added as u64).saturating_mul(EXCLUSION_BYTES));
                self.evidence_peak_bytes = self.evidence_peak_bytes.max(self.evidence_bytes);
            }
            if self.evidence_bytes > self.limit {
                self.halt("EvidenceLimit");
            }
        }

        let mut report = FormulaCompressedSourceReport {
            source_formula_events: self.counters[0],
            source_ordinary_events: self.counters[1],
            source_shared_anchor_events: self.counters[2],
            source_shared_descendant_events: self.counters[3],
            source_unknown_events: self.counters[4],
            evidence_peak_bytes: self.evidence_peak_bytes,
            ..FormulaCompressedSourceReport::default()
        };
        let mut families = Vec::new();
        let mut fragmented = Vec::new();
        let halted = self.halted;
        let mut retained_bytes = self.evidence_bytes;
        for (source_id, family) in self.families {
            report.families_seen = report.families_seen.saturating_add(1);
            report.family_cells_seen = report.family_cells_seen.saturating_add(family.members);
            report.replay_families = report.replay_families.saturating_add(1);
            report.replay_cells = report.replay_cells.saturating_add(family.members);

            let area = family.range.and_then(checked_area);
            let accounted = u64::try_from(family.exclusions.len())
                .ok()
                .and_then(|excluded| family.members.checked_add(excluded));
            let structurally_valid = halted.is_none()
                && family.anomaly.is_none()
                && family.anchors == 1
                && family.next.is_none()
                && area == accounted;
            let complete = structurally_valid && family.exclusions.is_empty();
            let mut reason = halted.or(family.anomaly);
            if reason.is_none() && family.anchors != 1 {
                reason = Some("MissingOrDuplicateAnchor");
            }
            if reason.is_none() && (!structurally_valid || area != accounted) {
                reason = Some("PartitionProofFailed");
            }

            if complete {
                report.source_clean_families = report.source_clean_families.saturating_add(1);
                report.source_clean_cells =
                    report.source_clean_cells.saturating_add(family.members);
                let rect = family.range.expect("clean family has range");
                families.push(SourceFormulaFamily {
                    source_id,
                    source_order: family.source_order.expect("family has source order"),
                    anchor_coord0: family.anchor.expect("clean family has anchor"),
                    anchor_text: family.anchor_text.expect("clean family has anchor text"),
                    members: SourceFamilyMembers::CompleteDomain(domain_for_rect(rect)),
                    member_count: family.members,
                });
            } else if structurally_valid {
                let range = family.range.expect("fragmentable family has range");
                match partition_declared_rect(
                    range,
                    &family.exclusions,
                    family.members,
                    self.max_fragments,
                ) {
                    Ok((fragments, fallback_members)) => {
                        let partition_bytes = (fragments.len() as u64)
                            .saturating_mul(FRAGMENT_BYTES)
                            .saturating_add(
                                (fallback_members.len() as u64)
                                    .saturating_mul(size_of::<SourceCoord>() as u64),
                            );
                        if retained_bytes.saturating_add(partition_bytes) > self.limit {
                            reason = Some("EvidenceLimit");
                        } else {
                            retained_bytes = retained_bytes.saturating_add(partition_bytes);
                            report.evidence_peak_bytes =
                                report.evidence_peak_bytes.max(retained_bytes);
                            report.source_fragmentable_families =
                                report.source_fragmentable_families.saturating_add(1);
                            report.source_fragmentable_cells = report
                                .source_fragmentable_cells
                                .saturating_add(family.members);
                            report.source_fragment_count = report
                                .source_fragment_count
                                .saturating_add(fragments.len() as u64);
                            report.source_isolated_fallback_cells = report
                                .source_isolated_fallback_cells
                                .saturating_add(fallback_members.len() as u64);
                            for exclusion in &family.exclusions {
                                match exclusion {
                                    SourceExclusion::Hole(_) => {
                                        report.source_hole_exclusions =
                                            report.source_hole_exclusions.saturating_add(1)
                                    }
                                    SourceExclusion::OrdinaryFormula(_) => {
                                        report.source_ordinary_exclusions =
                                            report.source_ordinary_exclusions.saturating_add(1)
                                    }
                                }
                            }
                            fragmented.push(FragmentedFamilyProposal {
                                source_id,
                                source_order: family.source_order.expect("family has source order"),
                                anchor_coord0: family
                                    .anchor
                                    .expect("fragmentable family has anchor"),
                                anchor_text: family
                                    .anchor_text
                                    .expect("fragmentable family has anchor text"),
                                declared: range,
                                fragments,
                                fallback_members,
                                exclusions: family.exclusions,
                                member_count: family.members,
                            });
                        }
                    }
                    Err(partition_reason) => reason = Some(partition_reason),
                }
            }

            if let Some(reason) = reason {
                if matches!(
                    reason,
                    "ExclusionCapExceeded"
                        | "FragmentCapExceeded"
                        | "PartitionAreaOverflow"
                        | "PartitionProofFailed"
                ) {
                    report.source_partition_failures =
                        report.source_partition_failures.saturating_add(1);
                }
                *report
                    .fallback_reasons
                    .entry(reason.to_string())
                    .or_default() += 1;
                if reason == "ForwardAnchor" {
                    report.forward_descendants = report.forward_descendants.saturating_add(1);
                }
                if reason == "EvidenceLimit" {
                    report.evidence_limit_fallbacks =
                        report.evidence_limit_fallbacks.saturating_add(1);
                }
            }
        }
        if let Some(reason) = halted {
            report.replay_cells = report
                .source_shared_anchor_events
                .saturating_add(report.source_shared_descendant_events);
            if report.replay_families == 0 && report.replay_cells != 0 {
                report.replay_families = 1;
                *report
                    .fallback_reasons
                    .entry(reason.to_string())
                    .or_default() += 1;
                if reason == "EvidenceLimit" {
                    report.evidence_limit_fallbacks = 1;
                }
            }
        }
        CompressedEvidenceOutput {
            report,
            families,
            fragmented,
        }
    }
}

pub(super) struct CompressedEvidenceOutput {
    pub(super) report: FormulaCompressedSourceReport,
    pub(super) families: Vec<SourceFormulaFamily>,
    pub(super) fragmented: Vec<FragmentedFamilyProposal>,
}

#[derive(Clone, Copy)]
enum ActivePointDisposition {
    FamilyMember,
    Ordinary,
    OtherFamily,
    Unsupported,
}

fn advance_family_evidence(
    evidence: &mut FamilyEvidence,
    range: SourceRect,
    coord: SourceCoord,
    disposition: ActivePointDisposition,
    max_exclusions: usize,
) -> usize {
    if evidence.anomaly.is_some() {
        return 0;
    }
    let before = evidence.exclusions.len();
    if coord > range.end {
        if let Some(expected) = evidence.next {
            append_holes(evidence, range, expected, None, max_exclusions);
        }
        return evidence.exclusions.len().saturating_sub(before);
    }
    if !contains(range, coord) {
        return 0;
    }
    if let Some(expected) = evidence.next {
        append_holes(evidence, range, expected, Some(coord), max_exclusions);
    }
    if evidence.anomaly.is_some() {
        return evidence.exclusions.len().saturating_sub(before);
    }
    match disposition {
        ActivePointDisposition::FamilyMember => {
            evidence.next = next_coord(coord, range);
        }
        ActivePointDisposition::Ordinary => {
            push_exclusion(
                evidence,
                SourceExclusion::OrdinaryFormula(coord),
                max_exclusions,
            );
            evidence.next = next_coord(coord, range);
        }
        ActivePointDisposition::OtherFamily => {
            evidence.anomaly = Some("OtherFamilyConflict");
        }
        ActivePointDisposition::Unsupported => {
            evidence.anomaly = Some("UnsupportedConflict");
        }
    }
    evidence.exclusions.len().saturating_sub(before)
}

fn finish_family_evidence(
    evidence: &mut FamilyEvidence,
    range: SourceRect,
    max_exclusions: usize,
) -> usize {
    if evidence.anomaly.is_some() {
        return 0;
    }
    let before = evidence.exclusions.len();
    if let Some(expected) = evidence.next {
        append_holes(evidence, range, expected, None, max_exclusions);
    }
    evidence.exclusions.len().saturating_sub(before)
}

fn append_holes(
    evidence: &mut FamilyEvidence,
    range: SourceRect,
    mut cursor: SourceCoord,
    stop_exclusive: Option<SourceCoord>,
    max_exclusions: usize,
) {
    let Some(area) = checked_area(range) else {
        evidence.anomaly = Some("PartitionAreaOverflow");
        return;
    };
    let Some(start_offset) = coord_offset(range, cursor) else {
        evidence.anomaly = Some("PartitionProofFailed");
        return;
    };
    let stop_offset = match stop_exclusive {
        Some(stop) if contains(range, stop) => coord_offset(range, stop).unwrap_or(area),
        Some(stop) if stop <= range.start => start_offset,
        Some(_) | None => area,
    };
    let missing = stop_offset.saturating_sub(start_offset);
    let remaining = max_exclusions.saturating_sub(evidence.exclusions.len()) as u64;
    if missing > remaining {
        evidence.anomaly = Some("ExclusionCapExceeded");
        return;
    }
    for _ in 0..missing {
        evidence.exclusions.push(SourceExclusion::Hole(cursor));
        let Some(next) = next_coord(cursor, range) else {
            evidence.next = None;
            return;
        };
        cursor = next;
    }
    evidence.next = if stop_offset == area {
        None
    } else {
        stop_exclusive
    };
}

fn push_exclusion(
    evidence: &mut FamilyEvidence,
    exclusion: SourceExclusion,
    max_exclusions: usize,
) {
    if evidence.exclusions.len() >= max_exclusions {
        evidence.anomaly = Some("ExclusionCapExceeded");
    } else {
        evidence.exclusions.push(exclusion);
    }
}

fn coord_offset(rect: SourceRect, coord: SourceCoord) -> Option<u64> {
    if !contains(rect, coord) {
        return None;
    }
    let width = u64::from(rect.end.col.checked_sub(rect.start.col)?.checked_add(1)?);
    let row = u64::from(coord.row.checked_sub(rect.start.row)?);
    let col = u64::from(coord.col.checked_sub(rect.start.col)?);
    row.checked_mul(width)?.checked_add(col)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RowBand {
    row_start: u32,
    row_end: u32,
    intervals: Vec<(u32, u32)>,
}

fn partition_declared_rect(
    declared: SourceRect,
    exclusions: &[SourceExclusion],
    member_count: u64,
    max_fragments: usize,
) -> Result<(Vec<PlacementDomainTransport>, Vec<SourceCoord>), &'static str> {
    let area = checked_area(declared).ok_or("PartitionAreaOverflow")?;
    let excluded_count = u64::try_from(exclusions.len()).map_err(|_| "PartitionAreaOverflow")?;
    if member_count.checked_add(excluded_count) != Some(area) {
        return Err("PartitionProofFailed");
    }
    if exclusions
        .windows(2)
        .any(|window| window[0].coord() >= window[1].coord())
        || exclusions
            .iter()
            .any(|point| !contains(declared, point.coord()))
    {
        return Err("PartitionProofFailed");
    }

    let mut excluded_by_row: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for exclusion in exclusions {
        excluded_by_row
            .entry(exclusion.coord().row)
            .or_default()
            .push(exclusion.coord().col);
    }

    let full = vec![(declared.start.col, declared.end.col)];
    let mut bands = Vec::<RowBand>::new();
    let mut next_row = Some(declared.start.row);
    for (row, columns) in excluded_by_row {
        if let Some(start) = next_row
            && start < row
        {
            push_band(&mut bands, start, row - 1, full.clone());
        }
        let mut intervals = Vec::new();
        let mut next_col = declared.start.col;
        for col in columns {
            if next_col < col {
                intervals.push((next_col, col - 1));
            }
            next_col = col.checked_add(1).ok_or("PartitionAreaOverflow")?;
        }
        if next_col <= declared.end.col {
            intervals.push((next_col, declared.end.col));
        }
        push_band(&mut bands, row, row, intervals);
        next_row = row.checked_add(1);
    }
    if let Some(start) = next_row
        && start <= declared.end.row
    {
        push_band(&mut bands, start, declared.end.row, full);
    }

    let mut fragments = Vec::new();
    let mut fallback = Vec::new();
    let mut surviving = 0u64;
    for band in bands {
        for (col_start, col_end) in band.intervals {
            let rect = SourceRect {
                start: SourceCoord {
                    row: band.row_start,
                    col: col_start,
                },
                end: SourceCoord {
                    row: band.row_end,
                    col: col_end,
                },
            };
            let cells = checked_area(rect).ok_or("PartitionAreaOverflow")?;
            surviving = surviving
                .checked_add(cells)
                .ok_or("PartitionAreaOverflow")?;
            if cells == 1 {
                fallback.push(rect.start);
            } else {
                if fragments.len() >= max_fragments {
                    return Err("FragmentCapExceeded");
                }
                fragments.push(domain_for_rect(rect));
            }
        }
    }
    if surviving != member_count {
        return Err("PartitionProofFailed");
    }
    Ok((fragments, fallback))
}

fn push_band(bands: &mut Vec<RowBand>, row_start: u32, row_end: u32, intervals: Vec<(u32, u32)>) {
    if intervals.is_empty() {
        return;
    }
    if let Some(last) = bands.last_mut()
        && last.row_end.checked_add(1) == Some(row_start)
        && last.intervals == intervals
    {
        last.row_end = row_end;
        return;
    }
    bands.push(RowBand {
        row_start,
        row_end,
        intervals,
    });
}

fn domain_for_rect(rect: SourceRect) -> PlacementDomainTransport {
    if rect.start.col == rect.end.col {
        PlacementDomainTransport::RowRun {
            row_start: rect.start.row,
            row_end: rect.end.row,
            col: rect.start.col,
        }
    } else if rect.start.row == rect.end.row {
        PlacementDomainTransport::ColRun {
            row: rect.start.row,
            col_start: rect.start.col,
            col_end: rect.end.col,
        }
    } else {
        PlacementDomainTransport::Rect(rect)
    }
}

fn valid_rect(rect: SourceRect) -> bool {
    rect.start.row <= rect.end.row && rect.start.col <= rect.end.col
}

fn contains(rect: SourceRect, coord: SourceCoord) -> bool {
    coord.row >= rect.start.row
        && coord.row <= rect.end.row
        && coord.col >= rect.start.col
        && coord.col <= rect.end.col
}

fn rectangles_overlap(a: SourceRect, b: SourceRect) -> bool {
    a.start.row <= b.end.row
        && b.start.row <= a.end.row
        && a.start.col <= b.end.col
        && b.start.col <= a.end.col
}

fn checked_area(rect: SourceRect) -> Option<u64> {
    let rows = u64::from(rect.end.row.checked_sub(rect.start.row)?.checked_add(1)?);
    let cols = u64::from(rect.end.col.checked_sub(rect.start.col)?.checked_add(1)?);
    rows.checked_mul(cols)
}

fn next_coord(coord: SourceCoord, rect: SourceRect) -> Option<SourceCoord> {
    if coord.col < rect.end.col {
        Some(SourceCoord {
            row: coord.row,
            col: coord.col + 1,
        })
    } else if coord.row < rect.end.row {
        Some(SourceCoord {
            row: coord.row + 1,
            col: rect.start.col,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord(row: u32, col: u32) -> SourceCoord {
        SourceCoord { row, col }
    }
    fn rect(sr: u32, sc: u32, er: u32, ec: u32) -> SourceRect {
        SourceRect {
            start: coord(sr, sc),
            end: coord(er, ec),
        }
    }
    fn family(index: usize) -> SourceFamilyId {
        SourceFamilyId {
            sheet_instance: 0,
            source_index: index,
        }
    }

    fn clean(range: SourceRect) -> MonotonicFormulaEvidence {
        let mut evidence = MonotonicFormulaEvidence::new();
        let id = family(1);
        for row in range.start.row..=range.end.row {
            for col in range.start.col..=range.end.col {
                let point = coord(row, col);
                if point == range.start {
                    evidence.observe(
                        point,
                        EvidenceRecord::Anchor {
                            family: id,
                            range: Some(range),
                            text: "A1",
                        },
                    );
                } else {
                    evidence.observe(point, EvidenceRecord::Descendant { family: id });
                }
            }
        }
        evidence
    }

    #[test]
    fn vertical_horizontal_and_row_major_rectangles_are_constant_state_clean() {
        for range in [
            rect(0, 0, 999_999, 0),
            rect(0, 0, 0, 999),
            rect(0, 0, 99, 99),
        ] {
            let report = clean(range).finish().report;
            assert_eq!(report.source_clean_families, 1);
            assert!(report.evidence_peak_bytes < 1024);
            assert!(report.fallback_reasons.is_empty());
            assert_eq!(report.replay_families, 1);
        }
    }

    #[test]
    fn source_order_tokens_do_not_follow_shared_index_sorting() {
        let mut evidence = MonotonicFormulaEvidence::new();
        evidence.observe_ordered(
            coord(0, 0),
            SourceFormulaOrder::new(0),
            EvidenceRecord::Anchor {
                family: family(99),
                range: Some(rect(0, 0, 0, 2)),
                text: "A1",
            },
        );
        evidence.observe_ordered(
            coord(0, 1),
            SourceFormulaOrder::new(1),
            EvidenceRecord::Ordinary,
        );
        evidence.observe_ordered(
            coord(0, 2),
            SourceFormulaOrder::new(2),
            EvidenceRecord::Descendant { family: family(99) },
        );
        evidence.observe_ordered(
            coord(0, 4),
            SourceFormulaOrder::new(3),
            EvidenceRecord::Anchor {
                family: family(2),
                range: Some(rect(0, 4, 0, 5)),
                text: "A1",
            },
        );
        evidence.observe_ordered(
            coord(0, 5),
            SourceFormulaOrder::new(4),
            EvidenceRecord::Descendant { family: family(2) },
        );
        let result = evidence.finish();
        assert_eq!(result.fragmented[0].source_id.source_index, 99);
        assert_eq!(
            result.fragmented[0].source_order,
            SourceFormulaOrder::new(0)
        );
        assert_eq!(result.families[0].source_id.source_index, 2);
        assert_eq!(result.families[0].source_order, SourceFormulaOrder::new(3));
    }

    #[test]
    fn adjacent_interleaved_families_remain_clean_in_source_order() {
        for (left, right) in [
            (rect(0, 0, 99, 0), rect(0, 2, 99, 2)),
            (rect(0, 0, 0, 99), rect(0, 101, 0, 200)),
            (rect(0, 0, 49, 1), rect(0, 3, 49, 4)),
        ] {
            let mut events = Vec::new();
            for (id, range) in [(family(1), left), (family(2), right)] {
                for row in range.start.row..=range.end.row {
                    for col in range.start.col..=range.end.col {
                        let point = coord(row, col);
                        events.push((point, id, range, point == range.start));
                    }
                }
            }
            events.sort_by_key(|event| event.0);

            let mut evidence = MonotonicFormulaEvidence::new();
            for (point, id, range, is_anchor) in events {
                if is_anchor {
                    evidence.observe(
                        point,
                        EvidenceRecord::Anchor {
                            family: id,
                            range: Some(range),
                            text: "A1",
                        },
                    );
                } else {
                    evidence.observe(point, EvidenceRecord::Descendant { family: id });
                }
            }

            let output = evidence.finish();
            assert_eq!(output.report.source_clean_families, 2, "{left:?}/{right:?}");
            assert_eq!(
                output.report.source_clean_cells,
                checked_area(left).unwrap() + checked_area(right).unwrap()
            );
            assert_eq!(output.report.source_fragmentable_families, 0);
            assert_eq!(output.report.source_hole_exclusions, 0);
            assert_eq!(output.report.source_ordinary_exclusions, 0);
            assert_eq!(output.report.source_partition_failures, 0);
            assert!(output.report.fallback_reasons.is_empty());
            assert!(output.fragmented.is_empty());
            assert_eq!(
                output
                    .families
                    .iter()
                    .map(|candidate| candidate.source_id)
                    .collect::<Vec<_>>(),
                vec![family(1), family(2)]
            );
        }
    }

    #[test]
    fn outside_coordinates_before_range_end_do_not_advance_family_evidence() {
        let range = rect(10, 10, 20, 12);
        let expected = coord(11, 10);
        for row in range.start.row..=range.end.row {
            for col in 0..=20 {
                let point = coord(row, col);
                if contains(range, point) || point > range.end {
                    continue;
                }
                let mut evidence = FamilyEvidence {
                    next: Some(expected),
                    ..FamilyEvidence::default()
                };
                let added = advance_family_evidence(
                    &mut evidence,
                    range,
                    point,
                    ActivePointDisposition::OtherFamily,
                    DEFAULT_MAX_FAMILY_EXCLUSIONS,
                );
                assert_eq!(added, 0, "outside point {point:?}");
                assert_eq!(evidence.next, Some(expected), "outside point {point:?}");
                assert!(evidence.exclusions.is_empty(), "outside point {point:?}");
                assert!(evidence.anomaly.is_none(), "outside point {point:?}");
            }
        }
    }

    #[test]
    fn full_sheet_two_point_proves_checked_area_without_scanning() {
        let mut evidence = MonotonicFormulaEvidence::new();
        let range = rect(0, 0, u32::MAX, u32::MAX);
        evidence.observe(
            coord(0, 0),
            EvidenceRecord::Anchor {
                family: family(1),
                range: Some(range),
                text: "A1",
            },
        );
        evidence.observe(
            coord(u32::MAX, u32::MAX),
            EvidenceRecord::Descendant { family: family(1) },
        );
        let report = evidence.finish().report;
        assert_eq!(report.fallback_reasons["PartitionAreaOverflow"], 1);
    }

    #[test]
    fn late_hole_conflict_duplicate_and_range_start_mismatch_replay() {
        let mut hole = MonotonicFormulaEvidence::new();
        hole.observe(
            coord(0, 0),
            EvidenceRecord::Anchor {
                family: family(1),
                range: Some(rect(0, 0, 2, 0)),
                text: "A1",
            },
        );
        hole.observe(
            coord(2, 0),
            EvidenceRecord::Descendant { family: family(1) },
        );
        assert_eq!(hole.finish().report.replay_families, 1);

        let mut conflict = clean(rect(0, 0, 2, 0));
        conflict.observe(coord(2, 0), EvidenceRecord::Ordinary);
        assert_eq!(
            conflict.finish().report.fallback_reasons["DuplicateCoordinate"],
            1
        );

        let mut mismatch = MonotonicFormulaEvidence::new();
        mismatch.observe(
            coord(1, 0),
            EvidenceRecord::Anchor {
                family: family(1),
                range: Some(rect(0, 0, 1, 0)),
                text: "A1",
            },
        );
        assert_eq!(
            mismatch.finish().report.fallback_reasons["RangeStartMismatch"],
            1
        );
    }

    #[test]
    fn backward_forward_anchor_and_evidence_cap_replay_every_family() {
        let mut backward = clean(rect(0, 0, 1, 0));
        backward.observe(coord(0, 2), EvidenceRecord::Ordinary);
        assert_eq!(
            backward.finish().report.fallback_reasons["CoordinateDisorder"],
            1
        );

        let mut forward = MonotonicFormulaEvidence::new();
        forward.observe(
            coord(0, 1),
            EvidenceRecord::Descendant { family: family(1) },
        );
        forward.observe(
            coord(0, 2),
            EvidenceRecord::Anchor {
                family: family(1),
                range: Some(rect(0, 1, 0, 2)),
                text: "A1",
            },
        );
        assert_eq!(forward.finish().report.fallback_reasons["ForwardAnchor"], 1);

        let mut capped = MonotonicFormulaEvidence::with_limit(FAMILY_BYTES - 1);
        capped.observe(
            coord(0, 0),
            EvidenceRecord::Anchor {
                family: family(1),
                range: Some(rect(0, 0, 0, 0)),
                text: "A1",
            },
        );
        capped.observe(
            coord(0, 1),
            EvidenceRecord::Anchor {
                family: family(2),
                range: Some(rect(0, 1, 0, 1)),
                text: "A1",
            },
        );
        assert!(capped.families.is_empty());
        let report = capped.finish().report;
        assert_eq!(report.evidence_limit_fallbacks, 1);
        assert_eq!(report.replay_cells, 2);
    }

    #[test]
    fn oversized_anchor_text_is_rejected_before_arc_retention() {
        let mut evidence = MonotonicFormulaEvidence::with_limit(FAMILY_BYTES + 8);
        let oversized = "x".repeat((FAMILY_BYTES + 9) as usize);
        evidence.observe(
            coord(0, 0),
            EvidenceRecord::Anchor {
                family: family(1),
                range: Some(rect(0, 0, 0, 0)),
                text: &oversized,
            },
        );
        let retained = evidence
            .families
            .get(&family(1))
            .and_then(|family| family.anchor_text.as_ref());
        assert!(retained.is_none());
        let report = evidence.finish().report;
        assert_eq!(report.evidence_limit_fallbacks, 1);
        assert!(report.evidence_peak_bytes <= FAMILY_BYTES + 8);
    }

    #[test]
    fn evidence_cap_stops_retaining_new_families_and_uses_bounded_reconciliation() {
        let mut evidence = MonotonicFormulaEvidence::with_limit(FAMILY_BYTES);
        for index in 0..100_000 {
            evidence.observe(
                coord(index as u32, 0),
                EvidenceRecord::Anchor {
                    family: family(index),
                    range: Some(rect(index as u32, 0, index as u32, 0)),
                    text: "1",
                },
            );
        }
        assert_eq!(evidence.families.len(), 1);
        let report = evidence.finish().report;
        assert_eq!(report.replay_cells, 100_000);
        assert_eq!(report.source_clean_families, 0);
        assert_eq!(report.evidence_limit_fallbacks, 1);
    }

    #[test]
    fn fragmented_evidence_retains_only_points_and_partitions_deterministically() {
        let range = rect(0, 0, 4, 4);
        let hole = coord(3, 1);
        let ordinary = coord(1, 2);
        let mut evidence = MonotonicFormulaEvidence::new();
        for row in range.start.row..=range.end.row {
            for col in range.start.col..=range.end.col {
                let point = coord(row, col);
                if point == hole {
                    continue;
                }
                if point == range.start {
                    evidence.observe(
                        point,
                        EvidenceRecord::Anchor {
                            family: family(1),
                            range: Some(range),
                            text: "A1",
                        },
                    );
                } else if point == ordinary {
                    evidence.observe(point, EvidenceRecord::Ordinary);
                } else {
                    evidence.observe(point, EvidenceRecord::Descendant { family: family(1) });
                }
            }
        }

        let output = evidence.finish();
        assert!(
            output.families.is_empty(),
            "fragmented families stay replay-only"
        );
        assert_eq!(output.fragmented.len(), 1);
        let proposal = &output.fragmented[0];
        assert_eq!(
            proposal.exclusions,
            vec![
                SourceExclusion::OrdinaryFormula(ordinary),
                SourceExclusion::Hole(hole),
            ]
        );
        assert_eq!(proposal.member_count, 23);
        assert!(proposal.fragments.len() <= DEFAULT_MAX_FAMILY_FRAGMENTS);
        assert_partition_proof(proposal);
    }

    #[test]
    fn exclusion_and_fragment_caps_fail_the_whole_family() {
        let mut at_cap = MonotonicFormulaEvidence::with_partition_limits(
            u64::MAX,
            DEFAULT_MAX_FAMILY_EXCLUSIONS,
            DEFAULT_MAX_FAMILY_FRAGMENTS,
        );
        let range = rect(0, 0, 65, 0);
        at_cap.observe(
            range.start,
            EvidenceRecord::Anchor {
                family: family(1),
                range: Some(range),
                text: "A1",
            },
        );
        at_cap.observe(range.end, EvidenceRecord::Descendant { family: family(1) });
        let output = at_cap.finish();
        assert_eq!(output.fragmented.len(), 1);
        assert_eq!(output.fragmented[0].exclusions.len(), 64);

        let mut over_cap = MonotonicFormulaEvidence::with_partition_limits(
            u64::MAX,
            DEFAULT_MAX_FAMILY_EXCLUSIONS,
            DEFAULT_MAX_FAMILY_FRAGMENTS,
        );
        let range = rect(0, 0, 66, 0);
        over_cap.observe(
            range.start,
            EvidenceRecord::Anchor {
                family: family(1),
                range: Some(range),
                text: "A1",
            },
        );
        over_cap.observe(range.end, EvidenceRecord::Descendant { family: family(1) });
        let output = over_cap.finish();
        assert!(output.fragmented.is_empty());
        assert_eq!(output.report.fallback_reasons["ExclusionCapExceeded"], 1);

        let exclusions = [SourceExclusion::Hole(coord(0, 2))];
        assert_eq!(
            partition_declared_rect(rect(0, 0, 0, 4), &exclusions, 4, 1),
            Err("FragmentCapExceeded")
        );
    }

    #[test]
    fn partition_union_and_disjointness_hold_without_declared_area_scans() {
        for rows in 1..=12u32 {
            for cols in 1..=9u32 {
                let declared = rect(0, 0, rows - 1, cols - 1);
                let area = u64::from(rows) * u64::from(cols);
                let mut points = Vec::new();
                for seed in 0..=8u32 {
                    let point = coord(seed.wrapping_mul(7) % rows, seed.wrapping_mul(11) % cols);
                    if point != declared.start && !points.contains(&point) {
                        points.push(point);
                    }
                }
                points.sort_unstable();
                let exclusions: Vec<_> = points
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(index, point)| {
                        if index % 2 == 0 {
                            SourceExclusion::Hole(point)
                        } else {
                            SourceExclusion::OrdinaryFormula(point)
                        }
                    })
                    .collect();
                let member_count = area - exclusions.len() as u64;
                let (domains, fallback) = partition_declared_rect(
                    declared,
                    &exclusions,
                    member_count,
                    DEFAULT_MAX_FAMILY_FRAGMENTS,
                )
                .expect("bounded partition");
                let proposal = FragmentedFamilyProposal {
                    source_id: family(1),
                    source_order: SourceFormulaOrder::new(0),
                    anchor_coord0: declared.start,
                    anchor_text: Arc::from("A1"),
                    declared,
                    fragments: domains,
                    fallback_members: fallback,
                    exclusions,
                    member_count,
                };
                assert_partition_proof(&proposal);
            }
        }
    }

    fn assert_partition_proof(proposal: &FragmentedFamilyProposal) {
        let domain_rects: Vec<_> = proposal
            .fragments
            .iter()
            .map(|domain| domain.rect())
            .collect();
        for (index, left) in domain_rects.iter().enumerate() {
            for right in domain_rects.iter().skip(index + 1) {
                assert!(!rectangles_overlap(*left, *right));
            }
            for exclusion in &proposal.exclusions {
                assert!(!contains(*left, exclusion.coord()));
            }
            for fallback in &proposal.fallback_members {
                assert!(!contains(*left, *fallback));
            }
        }
        for (index, left) in proposal.fallback_members.iter().enumerate() {
            assert!(!proposal.fallback_members[index + 1..].contains(left));
            assert!(
                !proposal
                    .exclusions
                    .iter()
                    .any(|point| point.coord() == *left)
            );
        }
        let domain_cells = domain_rects
            .iter()
            .map(|rect| checked_area(*rect).expect("domain area"))
            .sum::<u64>();
        assert_eq!(
            domain_cells + proposal.fallback_members.len() as u64,
            proposal.member_count
        );
        assert_eq!(
            proposal.member_count + proposal.exclusions.len() as u64,
            checked_area(proposal.declared).expect("declared area")
        );
    }

    #[test]
    fn many_tiny_families_have_deterministic_family_bounded_evidence() {
        let mut evidence = MonotonicFormulaEvidence::with_limit(u64::MAX);
        for index in 0..10_000 {
            let point = coord(index as u32, 0);
            evidence.observe(
                point,
                EvidenceRecord::Anchor {
                    family: family(index),
                    range: Some(SourceRect {
                        start: point,
                        end: point,
                    }),
                    text: "1",
                },
            );
        }
        let report = evidence.finish().report;
        assert_eq!(report.families_seen, 10_000);
        assert_eq!(report.source_clean_families, 10_000);
        assert_eq!(
            report.evidence_peak_bytes,
            10_000 * (FAMILY_BYTES + RUN_BYTES + 1)
        );
    }
}
