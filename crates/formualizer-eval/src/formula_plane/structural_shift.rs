use super::producer::SpanReadSummary;
use super::region_index::{AxisRange, Region};
use super::runtime::FormulaSpan;
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
    },
    Demote {
        reason: SpanDemoteReason,
    },
    Remove,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpanDemoteReason {
    ResultDomainStraddles,
    ReadRegionStraddles,
    OriginNotShiftedButReadRegionShifts,
    DeletePartiallyOverlaps,
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
            return SpanShiftPlan::Demote {
                reason: if op.is_delete() {
                    SpanDemoteReason::DeletePartiallyOverlaps
                } else {
                    SpanDemoteReason::ResultDomainStraddles
                },
            };
        }
    };
    let result_shifts = row_delta != 0 || col_delta != 0;

    let mut saw_read_shift = false;
    let mut saw_read_no_shift = false;
    if let Some(read_summary) = read_summary {
        for dependency in &read_summary.dependencies {
            match op.classify_region(dependency.read_region) {
                AxisShiftCase::OtherSheet | AxisShiftCase::EntirelyBelow => {
                    saw_read_no_shift = true;
                }
                AxisShiftCase::EntirelyAboveShift { .. } => {
                    saw_read_shift = true;
                }
                AxisShiftCase::Straddles | AxisShiftCase::DeleteFullyContains => {
                    return SpanShiftPlan::Demote {
                        reason: SpanDemoteReason::ReadRegionStraddles,
                    };
                }
            }
        }
    }

    match (result_shifts, saw_read_shift, saw_read_no_shift) {
        (false, false, _) => SpanShiftPlan::NoOp,
        (false, true, _) => SpanShiftPlan::Demote {
            reason: SpanDemoteReason::OriginNotShiftedButReadRegionShifts,
        },
        (true, false, _) => SpanShiftPlan::Shift {
            row_delta,
            col_delta,
            origin_row_delta: row_delta,
            origin_col_delta: col_delta,
        },
        (true, true, false) => SpanShiftPlan::Shift {
            row_delta,
            col_delta,
            origin_row_delta: 0,
            origin_col_delta: 0,
        },
        (true, true, true) => SpanShiftPlan::Demote {
            reason: SpanDemoteReason::MixedReadRegionShift,
        },
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
            SpanShiftPlan::Demote {
                reason: SpanDemoteReason::ResultDomainStraddles
            }
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
