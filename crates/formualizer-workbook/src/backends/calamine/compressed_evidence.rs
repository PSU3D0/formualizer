use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

use formualizer_eval::engine::{
    FormulaCompressedSourceReport, PlacementDomainTransport, SourceCoord, SourceFamilyId,
    SourceFamilyMembers, SourceFormulaFamily, SourceRect,
};

const DEFAULT_EVIDENCE_LIMIT: u64 = 8 * 1024 * 1024;
const FAMILY_BYTES: u64 = 256;
const RUN_BYTES: u64 = size_of::<SourceRect>() as u64;

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
    members: u64,
    anchors: u32,
    anchor: Option<SourceCoord>,
    anchor_text: Option<Arc<str>>,
    range: Option<SourceRect>,
    next: Option<SourceCoord>,
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
            counters: [0; 5],
        }
    }

    pub(super) fn observe(&mut self, coord: SourceCoord, record: EvidenceRecord<'_>) {
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
        if self.halted.is_none() {
            self.active_families.retain(|family| {
                self.families
                    .get(family)
                    .and_then(|evidence| evidence.range)
                    .is_some_and(|range| range.end >= coord)
            });
        }

        let family_id = match record {
            EvidenceRecord::Anchor { family, .. } | EvidenceRecord::Descendant { family } => {
                Some(family)
            }
            _ => None,
        };
        if let Some(family) = family_id {
            if !self.ensure_family(family) {
                return;
            }
            let evidence = self
                .families
                .get_mut(&family)
                .expect("retained family exists");
            evidence.members = evidence.members.saturating_add(1);
        }
        if self.halted.is_some() {
            return;
        }

        match record {
            EvidenceRecord::Ordinary => self.mark_conflicts(coord, None, "OrdinaryConflict"),
            EvidenceRecord::Unsupported => self.mark_conflicts(coord, None, "UnsupportedConflict"),
            EvidenceRecord::Anchor {
                family,
                range,
                text,
            } => {
                self.mark_conflicts(coord, Some(family), "OtherFamilyConflict");
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
                let Some(range) = evidence.range else {
                    return;
                };
                if !contains(range, coord) {
                    evidence.anomaly.get_or_insert("OutOfRange");
                } else if evidence.next != Some(coord) {
                    evidence.anomaly.get_or_insert("HoleOrOrderingAmbiguity");
                } else {
                    evidence.next = next_coord(coord, range);
                }
                self.mark_conflicts(coord, Some(family), "OtherFamilyConflict");
            }
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

    fn mark_conflicts(
        &mut self,
        coord: SourceCoord,
        owner: Option<SourceFamilyId>,
        reason: &'static str,
    ) {
        let mut owner_conflict = false;
        let owner_range = owner.and_then(|owner| self.families.get(&owner)?.range);
        for family in &self.active_families {
            if Some(*family) == owner {
                continue;
            }
            let evidence = self.families.get_mut(family).expect("active family exists");
            let point_conflict = evidence.range.is_some_and(|range| contains(range, coord));
            let range_conflict = owner_range.is_some_and(|owner_range| {
                evidence
                    .range
                    .is_some_and(|range| rectangles_overlap(owner_range, range))
            });
            if point_conflict || range_conflict {
                evidence.anomaly.get_or_insert(reason);
                owner_conflict = true;
            }
        }
        if let Some(owner) = owner
            && owner_conflict
        {
            self.families
                .get_mut(&owner)
                .expect("owner family exists")
                .anomaly
                .get_or_insert(reason);
        }
    }

    pub(super) fn finish(self) -> CompressedEvidenceOutput {
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
        let halted = self.halted;
        for (source_id, family) in self.families {
            report.families_seen = report.families_seen.saturating_add(1);
            report.family_cells_seen = report.family_cells_seen.saturating_add(family.members);
            let area = family.range.and_then(checked_area);
            let complete = self.halted.is_none()
                && family.anomaly.is_none()
                && family.anchors == 1
                && area == Some(family.members)
                && family.next.is_none();
            let reason = if let Some(reason) = self.halted {
                Some(reason)
            } else if let Some(reason) = family.anomaly {
                Some(reason)
            } else if family.anchors != 1 {
                Some("MissingOrDuplicateAnchor")
            } else if area != Some(family.members) || family.next.is_some() {
                Some("Hole")
            } else {
                None
            };
            report.replay_families = report.replay_families.saturating_add(1);
            report.replay_cells = report.replay_cells.saturating_add(family.members);
            if complete {
                report.source_clean_families = report.source_clean_families.saturating_add(1);
                report.source_clean_cells =
                    report.source_clean_cells.saturating_add(family.members);
                let rect = family.range.expect("clean family has range");
                let domain = if rect.start.col == rect.end.col {
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
                };
                families.push(SourceFormulaFamily {
                    source_id,
                    anchor_coord0: family.anchor.expect("clean family has anchor"),
                    anchor_text: family.anchor_text.expect("clean family has anchor text"),
                    members: SourceFamilyMembers::CompleteDomain(domain),
                    member_count: family.members,
                });
            }
            if let Some(reason) = reason {
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
                // Once evidence halts, unseen family ids are deliberately not
                // retained. One bounded reconciliation bucket records that the
                // complete sheet spool, rather than any inferred family set,
                // must be replayed.
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
        CompressedEvidenceOutput { report, families }
    }
}

pub(super) struct CompressedEvidenceOutput {
    pub(super) report: FormulaCompressedSourceReport,
    pub(super) families: Vec<SourceFormulaFamily>,
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
        assert_eq!(report.fallback_reasons["HoleOrOrderingAmbiguity"], 1);
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
