use super::producer::{AxisProjection, DirtyProjectionRule, SpanReadSummary};
use super::region_index::{AxisRange, Region};
use super::runtime::{FormulaSpan, PlacementDomain};
use crate::SheetId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StructuralOp {
    InsertRows {
        sheet_id: SheetId,
        before: u32,
        count: u32,
    },
    DeleteRows {
        sheet_id: SheetId,
        start: u32,
        count: u32,
    },
    InsertColumns {
        sheet_id: SheetId,
        before: u32,
        count: u32,
    },
    DeleteColumns {
        sheet_id: SheetId,
        start: u32,
        count: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AxisShiftCase {
    OtherSheet,
    EntirelyBelow,
    EntirelyAboveShift { delta: i64 },
    Straddles,
    DeleteFullyContains,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpanShiftPlan {
    NoOp,
    Shift {
        row_delta: i64,
        col_delta: i64,
        origin_row_delta: i64,
        origin_col_delta: i64,
        /// The op displaces the target of at least one absolute-anchored
        /// read. Origin relocation cannot repoint absolute references, so
        /// the apply site must rewrite the template AST through the shared
        /// `ReferenceAdjuster` (issue #168). Whenever this is set the origin
        /// deltas equal the result deltas: a full-AST rewrite shifts the
        /// template's displaced relative references too, and moving the
        /// origin with the block exactly cancels that for stationary
        /// relative reads while composing correctly with shifted ones —
        /// replicating per-cell adjustment semantics.
        rewrite_absolute_reads: bool,
    },
    /// A mid-domain insert straddles the span's result domain. The apply site
    /// may split the domain at the insert boundary into an unshifted upper
    /// half and a shifted lower half, re-derive each half's read summary, and
    /// re-classify each half; it must fall back to demoting the whole span
    /// when either half does not classify cleanly.
    Split,
    Demote {
        reason: SpanDemoteReason,
    },
    Remove,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpanDemoteReason {
    ReadRegionStraddles,
    OriginNotShiftedButReadRegionShifts,
    DeletePartiallyOverlaps,
    /// The template mixes relative reads whose targets the op displaces
    /// with relative reads whose targets stay put. No origin choice can
    /// satisfy both, and (unlike absolute reads) a template rewrite for
    /// relative references is deferred, so the span demotes.
    MixedReadRegionShift,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AxisKindForOp {
    Row,
    Col,
}

impl StructuralOp {
    pub(crate) fn sheet_id(self) -> SheetId {
        match self {
            Self::InsertRows { sheet_id, .. }
            | Self::DeleteRows { sheet_id, .. }
            | Self::InsertColumns { sheet_id, .. }
            | Self::DeleteColumns { sheet_id, .. } => sheet_id,
        }
    }

    pub(crate) fn count(self) -> u32 {
        match self {
            Self::InsertRows { count, .. }
            | Self::DeleteRows { count, .. }
            | Self::InsertColumns { count, .. }
            | Self::DeleteColumns { count, .. } => count,
        }
    }

    pub(crate) fn axis_shift_delta(self) -> (i64, i64) {
        match self {
            Self::InsertRows { count, .. } => (i64::from(count), 0),
            Self::DeleteRows { count, .. } => (-i64::from(count), 0),
            Self::InsertColumns { count, .. } => (0, i64::from(count)),
            Self::DeleteColumns { count, .. } => (0, -i64::from(count)),
        }
    }

    pub(crate) fn classify_axis(self, axis: AxisRange) -> AxisShiftCase {
        let (min, max) = axis.query_bounds();
        match self {
            Self::InsertRows { before, count, .. } | Self::InsertColumns { before, count, .. } => {
                if max < before {
                    AxisShiftCase::EntirelyBelow
                } else if min >= before {
                    AxisShiftCase::EntirelyAboveShift {
                        delta: i64::from(count),
                    }
                } else {
                    AxisShiftCase::Straddles
                }
            }
            Self::DeleteRows { start, count, .. } | Self::DeleteColumns { start, count, .. } => {
                let end = start.saturating_add(count);
                if max < start {
                    AxisShiftCase::EntirelyBelow
                } else if min >= end {
                    AxisShiftCase::EntirelyAboveShift {
                        delta: -i64::from(count),
                    }
                } else if min >= start && max < end {
                    AxisShiftCase::DeleteFullyContains
                } else {
                    AxisShiftCase::Straddles
                }
            }
        }
    }

    pub(crate) fn classify_region(self, region: Region) -> AxisShiftCase {
        if region.sheet_id() != self.sheet_id() {
            return AxisShiftCase::OtherSheet;
        }
        let (rows, cols) = region.axis_ranges();
        match self.axis_kind() {
            AxisKindForOp::Row => self.classify_axis(rows),
            AxisKindForOp::Col => self.classify_axis(cols),
        }
    }

    fn axis_kind(self) -> AxisKindForOp {
        match self {
            Self::InsertRows { .. } | Self::DeleteRows { .. } => AxisKindForOp::Row,
            Self::InsertColumns { .. } | Self::DeleteColumns { .. } => AxisKindForOp::Col,
        }
    }

    fn is_delete(self) -> bool {
        matches!(self, Self::DeleteRows { .. } | Self::DeleteColumns { .. })
    }
}

/// The anchor kinds a dirty-projection rule uses on the op's axis:
/// `(has_relative, has_absolute)`. Relative bounds are relocated by the
/// span's origin choice; absolute bounds can only be repointed by a template
/// AST rewrite. Bounds on the other axis are unaffected by the op.
///
/// `WholeResult` is a test-only rule with no axis information; it is treated
/// as relative to preserve the legacy region-level classification.
fn rule_axis_bound_kinds(rule: DirtyProjectionRule, axis: AxisKindForOp) -> (bool, bool) {
    fn is_abs(projection: AxisProjection) -> bool {
        matches!(projection, AxisProjection::Absolute { .. })
    }
    fn kinds(bounds: &[AxisProjection]) -> (bool, bool) {
        let has_absolute = bounds.iter().copied().any(is_abs);
        let has_relative = bounds.iter().copied().any(|bound| !is_abs(bound));
        (has_relative, has_absolute)
    }
    match (rule, axis) {
        (DirtyProjectionRule::AffineCell { row, .. }, AxisKindForOp::Row) => kinds(&[row]),
        (DirtyProjectionRule::AffineCell { col, .. }, AxisKindForOp::Col) => kinds(&[col]),
        (
            DirtyProjectionRule::AffineRange {
                row_start, row_end, ..
            },
            AxisKindForOp::Row,
        ) => kinds(&[row_start, row_end]),
        (
            DirtyProjectionRule::AffineRange {
                col_start, col_end, ..
            },
            AxisKindForOp::Col,
        ) => kinds(&[col_start, col_end]),
        (DirtyProjectionRule::WholeColumnRange { col_start, col_end }, AxisKindForOp::Col) => {
            kinds(&[col_start, col_end])
        }
        // A whole-column read has no row bounds to displace; row-op regions
        // for it are whole-axis and classify as straddles before this check.
        (DirtyProjectionRule::WholeColumnRange { .. }, AxisKindForOp::Row) => (true, false),
        (DirtyProjectionRule::WholeResult, _) => (true, false),
    }
}

/// Whether the op displaces the target of any absolute-anchored read in the
/// summary. Used by the delete-compaction path (which never reaches
/// `classify_span_for_op`'s read partition because the result-region
/// straddle is classified first): compaction projects read REGIONS through
/// the delete but keeps the template AST, so a displaced absolute read must
/// force a demote to the per-cell path where the shared adjuster repoints
/// the reference (issue #168).
pub(crate) fn summary_has_displaced_absolute_read(
    read_summary: &SpanReadSummary,
    op: StructuralOp,
) -> bool {
    read_summary.dependencies.iter().any(|dependency| {
        matches!(
            op.classify_region(dependency.read_region),
            AxisShiftCase::EntirelyAboveShift { .. }
        ) && rule_axis_bound_kinds(dependency.projection, op.axis_kind()).1
    })
}

pub(crate) fn classify_span_for_op(
    span: &FormulaSpan,
    read_summary: Option<&SpanReadSummary>,
    op: StructuralOp,
) -> SpanShiftPlan {
    let result_region = Region::from_domain(span.result_region.domain());
    let result_case = if span.sheet_id == op.sheet_id() {
        op.classify_region(result_region)
    } else {
        AxisShiftCase::OtherSheet
    };

    let (row_delta, col_delta) = match result_case {
        AxisShiftCase::OtherSheet | AxisShiftCase::EntirelyBelow => (0, 0),
        AxisShiftCase::EntirelyAboveShift { .. } => op.axis_shift_delta(),
        AxisShiftCase::DeleteFullyContains => return SpanShiftPlan::Remove,
        AxisShiftCase::Straddles => {
            return if op.is_delete() {
                SpanShiftPlan::Demote {
                    reason: SpanDemoteReason::DeletePartiallyOverlaps,
                }
            } else {
                SpanShiftPlan::Split
            };
        }
    };
    let result_shifts = row_delta != 0 || col_delta != 0;

    // Partition read dependencies by (displaced-by-op × anchor kind on the
    // op axis). Stationary absolute reads impose no constraint: the AST
    // coordinate already points at the unmoved target and origin relocation
    // never touches absolute references.
    let mut saw_shifting_relative = false;
    let mut saw_stationary_relative = false;
    let mut saw_displaced_absolute = false;
    if let Some(read_summary) = read_summary {
        for dependency in &read_summary.dependencies {
            let (has_relative, has_absolute) =
                rule_axis_bound_kinds(dependency.projection, op.axis_kind());
            match op.classify_region(dependency.read_region) {
                AxisShiftCase::OtherSheet | AxisShiftCase::EntirelyBelow => {
                    if has_relative {
                        saw_stationary_relative = true;
                    }
                }
                AxisShiftCase::EntirelyAboveShift { .. } => {
                    if has_relative {
                        saw_shifting_relative = true;
                    }
                    if has_absolute {
                        saw_displaced_absolute = true;
                    }
                }
                AxisShiftCase::Straddles | AxisShiftCase::DeleteFullyContains => {
                    return SpanShiftPlan::Demote {
                        reason: SpanDemoteReason::ReadRegionStraddles,
                    };
                }
            }
        }
    }

    if !result_shifts {
        // The span itself does not move. Any displaced read target (relative
        // or absolute) would require per-read relocation that origin choice
        // cannot express for a stationary span; demote conservatively.
        return if saw_shifting_relative || saw_displaced_absolute {
            SpanShiftPlan::Demote {
                reason: SpanDemoteReason::OriginNotShiftedButReadRegionShifts,
            }
        } else {
            SpanShiftPlan::NoOp
        };
    }

    if saw_displaced_absolute {
        // A displaced absolute read needs a template AST rewrite (issue
        // #168). The rewrite runs the shared `ReferenceAdjuster` over the
        // whole template, which also relocates displaced relative reads and
        // leaves stationary ones alone — so pairing it with an origin that
        // moves with the block is correct for every read kind (see the
        // `rewrite_absolute_reads` field docs).
        return SpanShiftPlan::Shift {
            row_delta,
            col_delta,
            origin_row_delta: row_delta,
            origin_col_delta: col_delta,
            rewrite_absolute_reads: true,
        };
    }

    match (saw_shifting_relative, saw_stationary_relative) {
        // Mixed relative kinds: no origin choice satisfies both, and the
        // relative-rewrite upgrade is deferred.
        (true, true) => SpanShiftPlan::Demote {
            reason: SpanDemoteReason::MixedReadRegionShift,
        },
        // All displaced reads are relative and move with the block: keep the
        // origin stationary so evaluated relative references shift with the
        // placements. Stationary absolute reads (e.g. the flagship `$F$1`)
        // are untouched by the origin and stay correct — this is the P2.5
        // mixed-read fast path.
        (true, false) => SpanShiftPlan::Shift {
            row_delta,
            col_delta,
            origin_row_delta: 0,
            origin_col_delta: 0,
            rewrite_absolute_reads: false,
        },
        // No displaced reads at all: move the origin with the block so
        // stationary relative reads keep pointing at their unmoved targets.
        (false, _) => SpanShiftPlan::Shift {
            row_delta,
            col_delta,
            origin_row_delta: row_delta,
            origin_col_delta: col_delta,
            rewrite_absolute_reads: false,
        },
    }
}

fn split_axis_at(min: u32, max: u32, before: u32) -> Option<((u32, u32), (u32, u32))> {
    if min < before && before <= max {
        Some(((min, before - 1), (before, max)))
    } else {
        None
    }
}

/// Split `domain` at an insert boundary into (upper, lower) halves in the
/// PRE-insert coordinate frame: the upper half is untouched by the insert,
/// the lower half starts at the insert boundary and shifts by the insert
/// count. Sibling of the delete-compaction domain surgery in
/// `demote_spans_for_structural_op_impl` (deliberately not merged: compaction
/// yields one domain, splitting yields two). Returns None whenever the domain
/// cannot be split cleanly along the insert axis; callers fall back to
/// demoting the whole span.
pub(crate) fn split_domain_at(
    domain: &PlacementDomain,
    op: StructuralOp,
) -> Option<(PlacementDomain, PlacementDomain)> {
    if domain.sheet_id() != op.sheet_id() {
        return None;
    }
    match (domain, op) {
        (
            PlacementDomain::RowRun {
                sheet_id,
                row_start,
                row_end,
                col,
            },
            StructuralOp::InsertRows { before, .. },
        ) => {
            let ((upper_start, upper_end), (lower_start, lower_end)) =
                split_axis_at(*row_start, *row_end, before)?;
            Some((
                PlacementDomain::row_run(*sheet_id, upper_start, upper_end, *col),
                PlacementDomain::row_run(*sheet_id, lower_start, lower_end, *col),
            ))
        }
        (
            PlacementDomain::Rect {
                sheet_id,
                row_start,
                row_end,
                col_start,
                col_end,
            },
            StructuralOp::InsertRows { before, .. },
        ) => {
            let ((upper_start, upper_end), (lower_start, lower_end)) =
                split_axis_at(*row_start, *row_end, before)?;
            Some((
                PlacementDomain::rect(*sheet_id, upper_start, upper_end, *col_start, *col_end),
                PlacementDomain::rect(*sheet_id, lower_start, lower_end, *col_start, *col_end),
            ))
        }
        (
            PlacementDomain::ColRun {
                sheet_id,
                row,
                col_start,
                col_end,
            },
            StructuralOp::InsertColumns { before, .. },
        ) => {
            let ((upper_start, upper_end), (lower_start, lower_end)) =
                split_axis_at(*col_start, *col_end, before)?;
            Some((
                PlacementDomain::col_run(*sheet_id, *row, upper_start, upper_end),
                PlacementDomain::col_run(*sheet_id, *row, lower_start, lower_end),
            ))
        }
        (
            PlacementDomain::Rect {
                sheet_id,
                row_start,
                row_end,
                col_start,
                col_end,
            },
            StructuralOp::InsertColumns { before, .. },
        ) => {
            let ((upper_start, upper_end), (lower_start, lower_end)) =
                split_axis_at(*col_start, *col_end, before)?;
            Some((
                PlacementDomain::rect(*sheet_id, *row_start, *row_end, upper_start, upper_end),
                PlacementDomain::rect(*sheet_id, *row_start, *row_end, lower_start, lower_end),
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formula_plane::ids::FormulaTemplateId;
    use crate::formula_plane::producer::{DirtyProjectionRule, SpanReadDependency};
    use crate::formula_plane::runtime::{
        FormulaSpan, FormulaSpanId, PlacementDomain, ResultRegion, SpanState,
    };

    fn span(domain: PlacementDomain) -> FormulaSpan {
        FormulaSpan {
            id: FormulaSpanId(0),
            generation: 0,
            sheet_id: domain.sheet_id(),
            template_id: FormulaTemplateId(0),
            result_region: ResultRegion::scalar_cells(domain.clone()),
            domain,
            intrinsic_mask_id: None,
            read_summary_id: None,
            binding_set_id: None,
            is_constant_result: false,
            state: SpanState::Active,
            version: 0,
        }
    }

    fn summary(read_regions: Vec<Region>) -> SpanReadSummary {
        SpanReadSummary {
            result_region: Region::point(0, 0, 0),
            dependencies: read_regions
                .into_iter()
                .map(|read_region| SpanReadDependency {
                    read_region,
                    projection: DirtyProjectionRule::WholeResult,
                })
                .collect(),
        }
    }

    #[test]
    fn classify_axis_insert_columns_covers_each_axis_range_kind() {
        let op = StructuralOp::InsertColumns {
            sheet_id: 0,
            before: 5,
            count: 2,
        };
        assert_eq!(
            op.classify_axis(AxisRange::Point(4)),
            AxisShiftCase::EntirelyBelow
        );
        assert_eq!(
            op.classify_axis(AxisRange::Point(5)),
            AxisShiftCase::EntirelyAboveShift { delta: 2 }
        );
        assert_eq!(
            op.classify_axis(AxisRange::Span(2, 6)),
            AxisShiftCase::Straddles
        );
        assert_eq!(
            op.classify_axis(AxisRange::From(5)),
            AxisShiftCase::EntirelyAboveShift { delta: 2 }
        );
        assert_eq!(
            op.classify_axis(AxisRange::To(4)),
            AxisShiftCase::EntirelyBelow
        );
        assert_eq!(op.classify_axis(AxisRange::All), AxisShiftCase::Straddles);
    }

    #[test]
    fn classify_axis_insert_before_zero_shifts_all_axis_range_kinds() {
        let op = StructuralOp::InsertRows {
            sheet_id: 0,
            before: 0,
            count: 5,
        };
        for axis in [
            AxisRange::Point(0),
            AxisRange::Span(0, 7),
            AxisRange::From(0),
            AxisRange::To(7),
            AxisRange::All,
        ] {
            assert_eq!(
                op.classify_axis(axis),
                AxisShiftCase::EntirelyAboveShift { delta: 5 }
            );
        }
    }

    #[test]
    fn classify_axis_delete_columns_covers_below_above_full_and_partial() {
        let op = StructuralOp::DeleteColumns {
            sheet_id: 0,
            start: 5,
            count: 3,
        };
        assert_eq!(
            op.classify_axis(AxisRange::Span(0, 4)),
            AxisShiftCase::EntirelyBelow
        );
        assert_eq!(
            op.classify_axis(AxisRange::Point(8)),
            AxisShiftCase::EntirelyAboveShift { delta: -3 }
        );
        assert_eq!(
            op.classify_axis(AxisRange::Span(5, 7)),
            AxisShiftCase::DeleteFullyContains
        );
        assert_eq!(
            op.classify_axis(AxisRange::Span(4, 5)),
            AxisShiftCase::Straddles
        );
        assert_eq!(
            op.classify_axis(AxisRange::From(8)),
            AxisShiftCase::EntirelyAboveShift { delta: -3 }
        );
        assert_eq!(op.classify_axis(AxisRange::All), AxisShiftCase::Straddles);
    }

    #[test]
    fn classify_region_uses_only_affected_axis_and_sheet() {
        let op = StructuralOp::InsertColumns {
            sheet_id: 7,
            before: 3,
            count: 1,
        };
        assert_eq!(
            op.classify_region(Region::rect(7, 0, 9, 0, 2)),
            AxisShiftCase::EntirelyBelow
        );
        assert_eq!(
            op.classify_region(Region::rect(7, 0, 9, 3, 6)),
            AxisShiftCase::EntirelyAboveShift { delta: 1 }
        );
        assert_eq!(
            op.classify_region(Region::rect(7, 0, 9, 2, 3)),
            AxisShiftCase::Straddles
        );
        assert_eq!(
            op.classify_region(Region::rect(8, 0, 9, 3, 6)),
            AxisShiftCase::OtherSheet
        );
    }

    #[test]
    fn classify_span_case3_origin_moves_when_reads_stay_below_insert() {
        let s = span(PlacementDomain::col_run(0, 0, 2, 4));
        let rs = summary(vec![Region::point(0, 0, 0)]);
        let op = StructuralOp::InsertColumns {
            sheet_id: 0,
            before: 2,
            count: 1,
        };
        assert_eq!(
            classify_span_for_op(&s, Some(&rs), op),
            SpanShiftPlan::Shift {
                row_delta: 0,
                col_delta: 1,
                origin_row_delta: 0,
                origin_col_delta: 1,
                rewrite_absolute_reads: false,
            }
        );
    }

    #[test]
    fn classify_span_shifts_with_stationary_origin_when_reads_shift() {
        let s = span(PlacementDomain::col_run(0, 0, 2, 4));
        let rs = summary(vec![Region::point(0, 0, 2)]);
        let op = StructuralOp::InsertColumns {
            sheet_id: 0,
            before: 2,
            count: 1,
        };
        assert_eq!(
            classify_span_for_op(&s, Some(&rs), op),
            SpanShiftPlan::Shift {
                row_delta: 0,
                col_delta: 1,
                origin_row_delta: 0,
                origin_col_delta: 0,
                rewrite_absolute_reads: false,
            }
        );
    }

    #[test]
    fn classify_span_demotes_result_straddle_read_straddle_and_delete_overlap() {
        let insert = StructuralOp::InsertColumns {
            sheet_id: 0,
            before: 3,
            count: 1,
        };
        let result_straddle = span(PlacementDomain::col_run(0, 0, 2, 4));
        assert_eq!(
            classify_span_for_op(&result_straddle, None, insert),
            SpanShiftPlan::Split
        );

        let result_above = span(PlacementDomain::col_run(0, 0, 5, 7));
        let read_straddle = summary(vec![Region::row_interval(0, 0, 2, 4)]);
        assert_eq!(
            classify_span_for_op(&result_above, Some(&read_straddle), insert),
            SpanShiftPlan::Demote {
                reason: SpanDemoteReason::ReadRegionStraddles
            }
        );

        let delete = StructuralOp::DeleteColumns {
            sheet_id: 0,
            start: 3,
            count: 2,
        };
        assert_eq!(
            classify_span_for_op(&result_straddle, None, delete),
            SpanShiftPlan::Demote {
                reason: SpanDemoteReason::DeletePartiallyOverlaps
            }
        );
    }

    #[test]
    fn classify_span_returns_split_for_mid_domain_row_insert() {
        let s = span(PlacementDomain::row_run(0, 0, 99, 1));
        let op = StructuralOp::InsertRows {
            sheet_id: 0,
            before: 50,
            count: 2,
        };
        // Result-domain straddle on an insert is now a split candidate even
        // when reads would straddle: the apply site re-classifies each half
        // and falls back to demoting the whole span.
        assert_eq!(classify_span_for_op(&s, None, op), SpanShiftPlan::Split);
        let read_straddle = summary(vec![Region::col_interval(0, 0, 0, 99)]);
        assert_eq!(
            classify_span_for_op(&s, Some(&read_straddle), op),
            SpanShiftPlan::Split
        );
    }

    #[test]
    fn classify_span_remove_for_delete_fully_contains_result() {
        let s = span(PlacementDomain::col_run(0, 0, 3, 4));
        let op = StructuralOp::DeleteColumns {
            sheet_id: 0,
            start: 2,
            count: 5,
        };
        assert_eq!(classify_span_for_op(&s, None, op), SpanShiftPlan::Remove);
    }

    #[test]
    fn classify_rect_span_partial_row_and_column_deletes_demote_for_compaction_path() {
        let s = span(PlacementDomain::rect(0, 0, 9, 2, 3));
        let delete_rows = StructuralOp::DeleteRows {
            sheet_id: 0,
            start: 4,
            count: 2,
        };
        assert_eq!(
            classify_span_for_op(&s, None, delete_rows),
            SpanShiftPlan::Demote {
                reason: SpanDemoteReason::DeletePartiallyOverlaps
            }
        );

        let delete_columns = StructuralOp::DeleteColumns {
            sheet_id: 0,
            start: 2,
            count: 1,
        };
        assert_eq!(
            classify_span_for_op(&s, None, delete_columns),
            SpanShiftPlan::Demote {
                reason: SpanDemoteReason::DeletePartiallyOverlaps
            }
        );
    }

    #[test]
    fn split_domain_at_covers_row_and_column_axes() {
        // RowRun / row insert: rows [10, 99] split before row 40.
        assert_eq!(
            split_domain_at(
                &PlacementDomain::row_run(0, 10, 99, 2),
                StructuralOp::InsertRows {
                    sheet_id: 0,
                    before: 40,
                    count: 3,
                },
            ),
            Some((
                PlacementDomain::row_run(0, 10, 39, 2),
                PlacementDomain::row_run(0, 40, 99, 2),
            ))
        );
        // Rect / row insert keeps the column extent on both halves.
        assert_eq!(
            split_domain_at(
                &PlacementDomain::rect(0, 0, 99, 1, 3),
                StructuralOp::InsertRows {
                    sheet_id: 0,
                    before: 50,
                    count: 1,
                },
            ),
            Some((
                PlacementDomain::rect(0, 0, 49, 1, 3),
                PlacementDomain::rect(0, 50, 99, 1, 3),
            ))
        );
        // ColRun / column insert.
        assert_eq!(
            split_domain_at(
                &PlacementDomain::col_run(0, 5, 1, 100),
                StructuralOp::InsertColumns {
                    sheet_id: 0,
                    before: 49,
                    count: 2,
                },
            ),
            Some((
                PlacementDomain::col_run(0, 5, 1, 48),
                PlacementDomain::col_run(0, 5, 49, 100),
            ))
        );
        // Rect / column insert keeps the row extent on both halves.
        assert_eq!(
            split_domain_at(
                &PlacementDomain::rect(0, 0, 9, 2, 8),
                StructuralOp::InsertColumns {
                    sheet_id: 0,
                    before: 4,
                    count: 1,
                },
            ),
            Some((
                PlacementDomain::rect(0, 0, 9, 2, 3),
                PlacementDomain::rect(0, 0, 9, 4, 8),
            ))
        );
    }

    #[test]
    fn split_domain_at_rejects_non_straddling_and_cross_axis_cases() {
        let row_run = PlacementDomain::row_run(0, 10, 99, 2);
        // Boundary at/above the start or beyond the end is not a straddle.
        for before in [5, 10, 100, 200] {
            assert_eq!(
                split_domain_at(
                    &row_run,
                    StructuralOp::InsertRows {
                        sheet_id: 0,
                        before,
                        count: 1,
                    },
                ),
                None
            );
        }
        // A row insert cannot split a single-row ColRun; a column insert
        // cannot split a single-column RowRun; deletes never split.
        assert_eq!(
            split_domain_at(
                &PlacementDomain::col_run(0, 5, 1, 100),
                StructuralOp::InsertRows {
                    sheet_id: 0,
                    before: 5,
                    count: 1,
                },
            ),
            None
        );
        assert_eq!(
            split_domain_at(
                &row_run,
                StructuralOp::InsertColumns {
                    sheet_id: 0,
                    before: 2,
                    count: 1,
                },
            ),
            None
        );
        assert_eq!(
            split_domain_at(
                &row_run,
                StructuralOp::DeleteRows {
                    sheet_id: 0,
                    start: 40,
                    count: 1,
                },
            ),
            None
        );
        // Other-sheet ops never split.
        assert_eq!(
            split_domain_at(
                &row_run,
                StructuralOp::InsertRows {
                    sheet_id: 1,
                    before: 40,
                    count: 1,
                },
            ),
            None
        );
    }

    #[test]
    fn classify_span_routes_absolute_reads_to_rewrite_or_mixed_fast_path() {
        // Flagship shape (issue #168 / P2.5): a vertical run reading
        // relative inputs plus an absolute scalar `$F$1`.
        let s = span(PlacementDomain::row_run(0, 10, 99, 2));
        let relative_dep = SpanReadDependency {
            read_region: Region::col_interval(0, 0, 10, 99),
            projection: DirtyProjectionRule::AffineCell {
                row: AxisProjection::Relative { offset: 0 },
                col: AxisProjection::Relative { offset: -2 },
            },
        };
        let absolute_dep = SpanReadDependency {
            read_region: Region::point(0, 0, 5),
            projection: DirtyProjectionRule::AffineCell {
                row: AxisProjection::Absolute { index: 0 },
                col: AxisProjection::Absolute { index: 5 },
            },
        };
        let rs = SpanReadSummary {
            result_region: Region::col_interval(0, 2, 10, 99),
            dependencies: vec![relative_dep, absolute_dep],
        };

        // Insert above the absolute target: everything (span, relative
        // inputs, scalar target) shifts. The absolute read requires a
        // template AST rewrite, which pairs with an origin that moves with
        // the block.
        let insert_above_all = StructuralOp::InsertRows {
            sheet_id: 0,
            before: 0,
            count: 2,
        };
        assert_eq!(
            classify_span_for_op(&s, Some(&rs), insert_above_all),
            SpanShiftPlan::Shift {
                row_delta: 2,
                col_delta: 0,
                origin_row_delta: 2,
                origin_col_delta: 0,
                rewrite_absolute_reads: true,
            }
        );

        // Insert between the absolute target and the span: the scalar's
        // target is stationary (absolute reads impose no constraint), the
        // relative reads shift with the block — the P2.5 mixed-read fast
        // path: shift with a stationary origin, no rewrite, no demote.
        let insert_between = StructuralOp::InsertRows {
            sheet_id: 0,
            before: 5,
            count: 2,
        };
        assert_eq!(
            classify_span_for_op(&s, Some(&rs), insert_between),
            SpanShiftPlan::Shift {
                row_delta: 2,
                col_delta: 0,
                origin_row_delta: 0,
                origin_col_delta: 0,
                rewrite_absolute_reads: false,
            }
        );
    }

    #[test]
    fn classify_span_column_absolute_read_displaced_routes_to_rewrite() {
        // Column-axis mirror: a horizontal run reading `$A$1` under a
        // column insert that displaces column A.
        let s = span(PlacementDomain::col_run(0, 2, 10, 99));
        let absolute_dep = SpanReadDependency {
            read_region: Region::point(0, 0, 0),
            projection: DirtyProjectionRule::AffineCell {
                row: AxisProjection::Absolute { index: 0 },
                col: AxisProjection::Absolute { index: 0 },
            },
        };
        let relative_dep = SpanReadDependency {
            read_region: Region::row_interval(0, 0, 10, 99),
            projection: DirtyProjectionRule::AffineCell {
                row: AxisProjection::Relative { offset: -2 },
                col: AxisProjection::Relative { offset: 0 },
            },
        };
        let rs = SpanReadSummary {
            result_region: Region::row_interval(0, 2, 10, 99),
            dependencies: vec![absolute_dep, relative_dep],
        };
        let op = StructuralOp::InsertColumns {
            sheet_id: 0,
            before: 0,
            count: 2,
        };
        assert_eq!(
            classify_span_for_op(&s, Some(&rs), op),
            SpanShiftPlan::Shift {
                row_delta: 0,
                col_delta: 2,
                origin_row_delta: 0,
                origin_col_delta: 2,
                rewrite_absolute_reads: true,
            }
        );
    }

    #[test]
    fn classify_span_mixed_relative_reads_still_demote() {
        // One relative read shifts with the block, another relative read's
        // target stays put: no origin choice satisfies both and the
        // relative-rewrite upgrade is deferred, so the span demotes.
        // Span rows 50..=80 reading A{r} (shifts with the block) and a far
        // stationary relative read at rows 0..=5; the insert at row 20 sits
        // strictly between them so both classify uniformly.
        let s = span(PlacementDomain::row_run(0, 50, 80, 2));
        let shifting_rel = SpanReadDependency {
            read_region: Region::col_interval(0, 0, 50, 80),
            projection: DirtyProjectionRule::AffineCell {
                row: AxisProjection::Relative { offset: 0 },
                col: AxisProjection::Relative { offset: -2 },
            },
        };
        let stationary_rel = SpanReadDependency {
            read_region: Region::col_interval(0, 1, 0, 5),
            projection: DirtyProjectionRule::AffineCell {
                row: AxisProjection::Relative { offset: -50 },
                col: AxisProjection::Relative { offset: -1 },
            },
        };
        let rs = SpanReadSummary {
            result_region: Region::col_interval(0, 2, 50, 80),
            dependencies: vec![shifting_rel, stationary_rel],
        };
        let op = StructuralOp::InsertRows {
            sheet_id: 0,
            before: 20,
            count: 1,
        };
        assert_eq!(
            classify_span_for_op(&s, Some(&rs), op),
            SpanShiftPlan::Demote {
                reason: SpanDemoteReason::MixedReadRegionShift
            }
        );
    }

    #[test]
    fn classify_span_cross_sheet_read_demotes_when_references_shift() {
        let s = span(PlacementDomain::col_run(1, 0, 3, 5));
        let rs = summary(vec![Region::point(0, 0, 4)]);
        let op = StructuralOp::InsertColumns {
            sheet_id: 0,
            before: 2,
            count: 1,
        };
        assert_eq!(
            classify_span_for_op(&s, Some(&rs), op),
            SpanShiftPlan::Demote {
                reason: SpanDemoteReason::OriginNotShiftedButReadRegionShifts
            }
        );
    }
}
