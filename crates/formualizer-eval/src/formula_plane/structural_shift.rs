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

/// Relative-anchored bounds of `rule` on the op axis, as authored-frame
/// offsets from the template origin. Returns `None` when the rule carries no
/// usable axis information (`WholeResult`), which callers must treat as
/// unverifiable.
fn rule_relative_offsets_on_op_axis(
    rule: DirtyProjectionRule,
    axis: AxisKindForOp,
) -> Option<[Option<i64>; 2]> {
    fn offset(projection: AxisProjection) -> Option<i64> {
        match projection {
            AxisProjection::Relative { offset } => Some(offset),
            AxisProjection::Absolute { .. } => None,
        }
    }
    match (rule, axis) {
        (DirtyProjectionRule::AffineCell { row, .. }, AxisKindForOp::Row) => {
            Some([offset(row), None])
        }
        (DirtyProjectionRule::AffineCell { col, .. }, AxisKindForOp::Col) => {
            Some([offset(col), None])
        }
        (
            DirtyProjectionRule::AffineRange {
                row_start, row_end, ..
            },
            AxisKindForOp::Row,
        ) => Some([offset(row_start), offset(row_end)]),
        (
            DirtyProjectionRule::AffineRange {
                col_start, col_end, ..
            },
            AxisKindForOp::Col,
        ) => Some([offset(col_start), offset(col_end)]),
        (DirtyProjectionRule::WholeColumnRange { col_start, col_end }, AxisKindForOp::Col) => {
            Some([offset(col_start), offset(col_end)])
        }
        // A whole-column read has no row bounds; row-op regions for it are
        // whole-axis and classify as straddles before rewrite planning.
        (DirtyProjectionRule::WholeColumnRange { .. }, AxisKindForOp::Row) => Some([None, None]),
        (DirtyProjectionRule::WholeResult, _) => None,
    }
}

/// FRAME INVARIANT (issue #168 rewrite arm). A template's AST authoring
/// frame and its `origin_{row,col}` fields may legitimately diverge:
/// `intern_shifted_origin_clone` reuses the source `ast_id` with a new
/// origin, the mixed-read fast path shifts a domain while pinning the origin
/// (`origin_delta: 0`), and split lower halves keep the original anchor
/// while their domain starts at the split boundary. Evaluation only
/// guarantees that `authored_coord + (placement - origin)` names the right
/// cell for placements IN THE DOMAIN — the authored coordinates themselves
/// need not lie inside the read regions.
///
/// The shared `ReferenceAdjuster` relocates by AUTHORED coordinate, so a
/// whole-template rewrite is only equivalent to per-cell adjustment when
/// every relative-anchored bound on the op axis classifies against the op
/// boundary exactly the way its read region does (displaced ⟺ displaced,
/// stationary ⟺ stationary, and never inside a deleted band). When the
/// frames disagree — e.g. a split lower half whose stale origin sits below a
/// later insert point that displaces its reads — the rewrite would shear
/// every relative read by the op count. This check refuses those rewrites
/// (the caller demotes, which is correct via the per-cell adjuster).
///
/// Absolute bounds need no check: their authored coordinate IS the read
/// region coordinate. Cross-sheet reads are refused outright: the adjuster
/// relocates by coordinate without honoring the reference's sheet, so their
/// frame cannot be verified here.
pub(crate) fn rewrite_frame_is_sound(
    read_summary: &SpanReadSummary,
    origin_row: u32,
    origin_col: u32,
    op: StructuralOp,
) -> bool {
    let axis = op.axis_kind();
    // Template origins are 1-based; regions and op boundaries are 0-based.
    let origin_axis0 = match axis {
        AxisKindForOp::Row => i64::from(origin_row) - 1,
        AxisKindForOp::Col => i64::from(origin_col) - 1,
    };
    for dependency in &read_summary.dependencies {
        let region_displaced = match op.classify_region(dependency.read_region) {
            AxisShiftCase::EntirelyBelow => false,
            AxisShiftCase::EntirelyAboveShift { .. } => true,
            AxisShiftCase::OtherSheet
            | AxisShiftCase::Straddles
            | AxisShiftCase::DeleteFullyContains => return false,
        };
        let Some(offsets) = rule_relative_offsets_on_op_axis(dependency.projection, axis) else {
            return false;
        };
        for offset in offsets.into_iter().flatten() {
            let authored0 = origin_axis0 + offset;
            if authored0 < 0 {
                return false;
            }
            let authored_displaced = match op {
                StructuralOp::InsertRows { before, .. }
                | StructuralOp::InsertColumns { before, .. } => authored0 >= i64::from(before),
                StructuralOp::DeleteRows { start, count, .. }
                | StructuralOp::DeleteColumns { start, count, .. } => {
                    let start = i64::from(start);
                    let end = start + i64::from(count);
                    if authored0 >= start && authored0 < end {
                        // The authored coordinate would become #REF! while
                        // the read region is uniformly intact.
                        return false;
                    }
                    authored0 >= end
                }
            };
            if authored_displaced != region_displaced {
                return false;
            }
        }
    }
    true
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

/// An inclusive `[min, max]` run on the op axis.
type AxisRun = (u32, u32);

/// The surviving halves of `[min, max]` through a delete of `count` units at
/// `start`, expressed in the PRE-delete frame: everything strictly above the
/// deleted band, and everything at/below its end. Either half may be absent.
fn delete_survivor_axis(
    min: u32,
    max: u32,
    start: u32,
    count: u32,
) -> (Option<AxisRun>, Option<AxisRun>) {
    let end = start.saturating_add(count);
    let upper = if min < start {
        Some((min, start - 1))
    } else {
        None
    };
    let lower = if max >= end {
        Some((min.max(end), max))
    } else {
        None
    };
    (upper, lower)
}

fn domain_axis_bounds(domain: &PlacementDomain, axis: AxisKindForOp) -> Option<AxisRun> {
    match (domain, axis) {
        (
            PlacementDomain::RowRun {
                row_start, row_end, ..
            },
            AxisKindForOp::Row,
        )
        | (
            PlacementDomain::Rect {
                row_start, row_end, ..
            },
            AxisKindForOp::Row,
        ) => Some((*row_start, *row_end)),
        (
            PlacementDomain::ColRun {
                col_start, col_end, ..
            },
            AxisKindForOp::Col,
        )
        | (
            PlacementDomain::Rect {
                col_start, col_end, ..
            },
            AxisKindForOp::Col,
        ) => Some((*col_start, *col_end)),
        // A domain whose op-axis extent is a single coordinate (a RowRun under
        // a column op, a ColRun under a row op) never straddles an op
        // boundary, so callers only reach this arm on unreachable geometry.
        _ => None,
    }
}

fn domain_with_axis_bounds(
    domain: &PlacementDomain,
    axis: AxisKindForOp,
    min: u32,
    max: u32,
) -> Option<PlacementDomain> {
    match (domain, axis) {
        (PlacementDomain::RowRun { sheet_id, col, .. }, AxisKindForOp::Row) => {
            Some(PlacementDomain::row_run(*sheet_id, min, max, *col))
        }
        (PlacementDomain::ColRun { sheet_id, row, .. }, AxisKindForOp::Col) => {
            Some(PlacementDomain::col_run(*sheet_id, *row, min, max))
        }
        (
            PlacementDomain::Rect {
                sheet_id,
                col_start,
                col_end,
                ..
            },
            AxisKindForOp::Row,
        ) => Some(PlacementDomain::rect(
            *sheet_id, min, max, *col_start, *col_end,
        )),
        (
            PlacementDomain::Rect {
                sheet_id,
                row_start,
                row_end,
                ..
            },
            AxisKindForOp::Col,
        ) => Some(PlacementDomain::rect(
            *sheet_id, *row_start, *row_end, min, max,
        )),
        _ => None,
    }
}

/// Split `domain` at an op boundary into (upper, lower) halves in the PRE-op
/// coordinate frame.
///
/// For an INSERT the upper half is untouched and the lower half starts at the
/// insert boundary and shifts by the insert count.
///
/// For a DELETE the two halves are the survivors on either side of the
/// deleted band: the upper half is untouched and the lower half starts at the
/// band's end and shifts up by the delete count. `None` is returned when
/// either half is empty — a one-sided survivor is handled by the
/// delete-compaction path (which keeps a single span) rather than by
/// splitting.
pub(crate) fn split_domain_at(
    domain: &PlacementDomain,
    op: StructuralOp,
) -> Option<(PlacementDomain, PlacementDomain)> {
    if domain.sheet_id() != op.sheet_id() {
        return None;
    }
    let axis = op.axis_kind();
    let (min, max) = domain_axis_bounds(domain, axis)?;
    let ((upper_min, upper_max), (lower_min, lower_max)) = match op {
        StructuralOp::InsertRows { before, .. } | StructuralOp::InsertColumns { before, .. } => {
            split_axis_at(min, max, before)?
        }
        StructuralOp::DeleteRows { start, count, .. }
        | StructuralOp::DeleteColumns { start, count, .. } => {
            let (upper, lower) = delete_survivor_axis(min, max, start, count);
            (upper?, lower?)
        }
    };
    Some((
        domain_with_axis_bounds(domain, axis, upper_min, upper_max)?,
        domain_with_axis_bounds(domain, axis, lower_min, lower_max)?,
    ))
}

/// DELETE-COMPACTION FRAME INVARIANT (issue #171).
///
/// Compaction keeps ONE span with ONE template across a delete that straddles
/// its result domain: the domain closes over the deleted band, the template
/// AST is untouched, and the origin is carried through the delete
/// (`origin_through_delete`). Placement-relative reads are therefore
/// evaluated at `placement + offset` with `offset = authored - origin`
/// unchanged, so compaction is only equivalent to per-cell adjustment when
/// that single offset stays right for EVERY surviving placement.
///
/// Write `d_p` for "this placement moves up by the delete count", `d_r` for
/// "this placement's read target moves up", and `d_o` for "the origin moves
/// up". Per-cell semantics require the post-delete offset to be
/// `offset + count * (d_r - d_p)`; compaction delivers `offset + count * d_o`
/// via the carried origin. The two agree exactly when `d_r == d_p - d_o` for
/// every surviving placement and every relative read bound.
///
/// The typical failure is a span whose reads sit ABOVE a mid-domain delete
/// (`d_r = 0`, `d_o = 0`) while placements exist on both sides of the band:
/// the upper placements need the authored offset and the lower ones need
/// `offset + count`. No single template can serve both, so the caller must
/// split the span (or demote it).
///
/// A read bound that lands INSIDE the deleted band is refused outright: the
/// per-cell result is `#REF!` for some placements and a live reference for
/// others, which no uniform template expresses. Cross-sheet reads never move,
/// so they are checked with `d_r = 0` — a delete that moves this span's origin
/// would shear them.
pub(crate) fn delete_compaction_frame_is_sound(
    read_summary: &SpanReadSummary,
    domain: &PlacementDomain,
    origin_row: u32,
    origin_col: u32,
    op: StructuralOp,
) -> bool {
    let (start_u32, count_u32) = match op {
        StructuralOp::DeleteRows { start, count, .. }
        | StructuralOp::DeleteColumns { start, count, .. } => (start, count),
        StructuralOp::InsertRows { .. } | StructuralOp::InsertColumns { .. } => return false,
    };
    let start = i64::from(start_u32);
    let end = start + i64::from(count_u32);
    let axis = op.axis_kind();
    // Template origins are 1-based; domains and op boundaries are 0-based.
    let origin_axis0 = match axis {
        AxisKindForOp::Row => i64::from(origin_row) - 1,
        AxisKindForOp::Col => i64::from(origin_col) - 1,
    };
    if origin_axis0 < 0 {
        return false;
    }
    let origin_displaced = if origin_axis0 < start {
        0
    } else if origin_axis0 >= end {
        1
    } else {
        // The origin itself is deleted; `origin_through_delete` refuses to
        // guess a new anchor and the caller demotes.
        return false;
    };
    let Some((min, max)) = domain_axis_bounds(domain, axis) else {
        return false;
    };
    let (upper, lower) = delete_survivor_axis(min, max, start_u32, count_u32);
    let survivors = [(upper, 0i64), (lower, 1i64)];
    for dependency in &read_summary.dependencies {
        let Some(offsets) = rule_relative_offsets_on_op_axis(dependency.projection, axis) else {
            return false;
        };
        // Cross-sheet read targets are never displaced by this op.
        let cross_sheet = dependency.read_region.sheet_id() != op.sheet_id();
        for offset in offsets.into_iter().flatten() {
            for (survivor, placement_displaced) in survivors {
                let Some((lo, hi)) = survivor else {
                    continue;
                };
                let required = placement_displaced - origin_displaced;
                if !(0..=1).contains(&required) {
                    return false;
                }
                // `read = placement + offset` is monotone in the placement, so
                // checking both ends of a surviving run proves the whole run
                // reads on one side of the deleted band.
                for placement in [i64::from(lo), i64::from(hi)] {
                    let read = placement + offset;
                    if read < 0 {
                        return false;
                    }
                    let read_displaced = if cross_sheet || read < start {
                        0
                    } else if read >= end {
                        1
                    } else {
                        return false;
                    };
                    if read_displaced != required {
                        return false;
                    }
                }
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formula_plane::ids::FormulaTemplateId;
    use crate::formula_plane::producer::{DirtyProjectionRule, SpanReadDependency};
    use crate::formula_plane::runtime::{
        FormulaSpan, FormulaSpanId, PlacementDomain, ResultRegion, SpanAstRelocation, SpanState,
    };

    fn span(domain: PlacementDomain) -> FormulaSpan {
        FormulaSpan {
            id: FormulaSpanId(0),
            generation: 0,
            sheet_id: domain.sheet_id(),
            template_id: FormulaTemplateId(0),
            ast_relocation: SpanAstRelocation {
                ast_id: crate::engine::arena::AstNodeId::from_u32(0),
                anchor_row: 1,
                anchor_col: 1,
            },
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
    fn split_domain_at_splits_op_straddles_and_rejects_cross_axis_cases() {
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
        // A delete splits into the survivors on either side of the band...
        assert_eq!(
            split_domain_at(
                &row_run,
                StructuralOp::DeleteRows {
                    sheet_id: 0,
                    start: 40,
                    count: 1,
                },
            ),
            Some((
                PlacementDomain::row_run(0, 10, 39, 2),
                PlacementDomain::row_run(0, 41, 99, 2),
            ))
        );
        // ...but a one-sided survivor is not a split (the delete-compaction
        // path keeps a single span for those).
        assert_eq!(
            split_domain_at(
                &row_run,
                StructuralOp::DeleteRows {
                    sheet_id: 0,
                    start: 90,
                    count: 20,
                },
            ),
            None
        );
        assert_eq!(
            split_domain_at(
                &row_run,
                StructuralOp::DeleteRows {
                    sheet_id: 0,
                    start: 5,
                    count: 20,
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
    fn rewrite_frame_soundness_matrix() {
        fn rel_cell_dep(
            read_region: Region,
            row_offset: i64,
            col_offset: i64,
        ) -> SpanReadDependency {
            SpanReadDependency {
                read_region,
                projection: DirtyProjectionRule::AffineCell {
                    row: AxisProjection::Relative { offset: row_offset },
                    col: AxisProjection::Relative { offset: col_offset },
                },
            }
        }
        fn summary_of(dependencies: Vec<SpanReadDependency>) -> SpanReadSummary {
            SpanReadSummary {
                result_region: Region::col_interval(0, 2, 50, 119),
                dependencies,
            }
        }
        let insert_rows = |before: u32| StructuralOp::InsertRows {
            sheet_id: 0,
            before,
            count: 3,
        };

        // Split-lower-half divergence (the #168 composition repro): domain
        // reads rows 50..119 but the stale origin authors A1 (row 0); an
        // insert at row 9 displaces the region while the authored coordinate
        // stays put — rewriting would shear relative reads.
        let displaced_region = summary_of(vec![rel_cell_dep(
            Region::col_interval(0, 0, 50, 119),
            0,
            -2,
        )]);
        assert!(!rewrite_frame_is_sound(
            &displaced_region,
            1,
            3,
            insert_rows(9)
        ));
        // Aligned frame (origin at the domain start): sound.
        assert!(rewrite_frame_is_sound(
            &displaced_region,
            51,
            3,
            insert_rows(9)
        ));

        // Column-axis mirror: relative column bound authored at
        // origin_col-1; the insert displaces the read region's column but
        // not the authored column.
        let displaced_col_region =
            summary_of(vec![rel_cell_dep(Region::row_interval(0, 10, 3, 3), 0, -1)]);
        let insert_cols = StructuralOp::InsertColumns {
            sheet_id: 0,
            before: 3,
            count: 1,
        };
        assert!(!rewrite_frame_is_sound(
            &displaced_col_region,
            11,
            4,
            insert_cols
        ));
        assert!(rewrite_frame_is_sound(
            &displaced_col_region,
            11,
            5,
            insert_cols
        ));

        // Spurious-stationary direction: the region is stationary but the
        // authored coordinate sits at/above the insert point (requires
        // origin > domain end, unreachable via engine ops today, covered
        // defensively).
        let stationary_region = summary_of(vec![rel_cell_dep(
            Region::col_interval(0, 0, 44, 54),
            0,
            -2,
        )]);
        assert!(!rewrite_frame_is_sound(
            &stationary_region,
            61,
            3,
            insert_rows(56)
        ));
        assert!(rewrite_frame_is_sound(
            &stationary_region,
            45,
            3,
            insert_rows(56)
        ));

        // Delete removing the authored coordinate while the region is
        // uniformly displaced.
        let delete = StructuralOp::DeleteRows {
            sheet_id: 0,
            start: 0,
            count: 1,
        };
        assert!(!rewrite_frame_is_sound(&displaced_region, 1, 3, delete));

        // Absolute-only dependencies carry their coordinate in the rule and
        // never need frame verification.
        let absolute_only = summary_of(vec![SpanReadDependency {
            read_region: Region::point(0, 29, 5),
            projection: DirtyProjectionRule::AffineCell {
                row: AxisProjection::Absolute { index: 29 },
                col: AxisProjection::Absolute { index: 5 },
            },
        }]);
        assert!(rewrite_frame_is_sound(&absolute_only, 1, 3, insert_rows(9)));

        // WholeResult rules have no axis information: unverifiable.
        let whole_result = summary_of(vec![SpanReadDependency {
            read_region: Region::col_interval(0, 0, 50, 119),
            projection: DirtyProjectionRule::WholeResult,
        }]);
        assert!(!rewrite_frame_is_sound(
            &whole_result,
            51,
            3,
            insert_rows(9)
        ));

        // Cross-sheet reads are refused: the shared adjuster relocates by
        // coordinate without honoring the reference's sheet.
        let cross_sheet = summary_of(vec![rel_cell_dep(
            Region::col_interval(1, 0, 50, 119),
            0,
            -2,
        )]);
        assert!(!rewrite_frame_is_sound(&cross_sheet, 51, 3, insert_rows(9)));
    }

    #[test]
    fn delete_compaction_frame_soundness_matrix() {
        // The issue #171 fixture, in 0-based region/domain coordinates: C150:C270
        // holds `=A{r-140}`, i.e. placements 149..=269 reading rows 9..=129 at a
        // constant offset of -140. The template origin is row 150 (1-based).
        fn displaced_summary() -> SpanReadSummary {
            SpanReadSummary {
                result_region: Region::col_interval(0, 2, 149, 269),
                dependencies: vec![SpanReadDependency {
                    read_region: Region::col_interval(0, 0, 9, 129),
                    projection: DirtyProjectionRule::AffineCell {
                        row: AxisProjection::Relative { offset: -140 },
                        col: AxisProjection::Absolute { index: 0 },
                    },
                }],
            }
        }
        let domain = PlacementDomain::row_run(0, 149, 269, 2);
        let delete = |start: u32, count: u32| StructuralOp::DeleteRows {
            sheet_id: 0,
            start,
            count,
        };

        // Mid-domain delete below every read: the two survivor runs need
        // different offsets, so compaction is unsound (the span must split).
        assert!(!delete_compaction_frame_is_sound(
            &displaced_summary(),
            &domain,
            150,
            3,
            delete(199, 2)
        ));
        // Head trim that keeps the origin row: same story.
        assert!(!delete_compaction_frame_is_sound(
            &displaced_summary(),
            &domain,
            150,
            3,
            delete(150, 10)
        ));
        // Tail trim: only the (unmoved) head survives and no read moves.
        assert!(delete_compaction_frame_is_sound(
            &displaced_summary(),
            &domain,
            150,
            3,
            delete(264, 20)
        ));
        // The delete removes the origin row itself: no anchor to carry.
        assert!(!delete_compaction_frame_is_sound(
            &displaced_summary(),
            &domain,
            150,
            3,
            delete(144, 10)
        ));

        // Lockstep reads (`=A{r}`, offset 0) compact cleanly through a
        // mid-domain delete: placements and reads move together.
        let lockstep = SpanReadSummary {
            result_region: Region::col_interval(0, 1, 41, 101),
            dependencies: vec![SpanReadDependency {
                read_region: Region::col_interval(0, 0, 41, 101),
                projection: DirtyProjectionRule::AffineCell {
                    row: AxisProjection::Relative { offset: 0 },
                    col: AxisProjection::Absolute { index: 0 },
                },
            }],
        };
        let lockstep_domain = PlacementDomain::row_run(0, 41, 101, 1);
        assert!(delete_compaction_frame_is_sound(
            &lockstep,
            &lockstep_domain,
            1,
            2,
            delete(59, 2)
        ));
        // ...but a read bound landing inside the deleted band is `#REF!` for
        // some placements and live for others.
        let straddling_bound = SpanReadSummary {
            result_region: Region::col_interval(0, 1, 41, 101),
            dependencies: vec![SpanReadDependency {
                read_region: Region::col_interval(0, 0, 36, 96),
                projection: DirtyProjectionRule::AffineCell {
                    row: AxisProjection::Relative { offset: -5 },
                    col: AxisProjection::Absolute { index: 0 },
                },
            }],
        };
        assert!(!delete_compaction_frame_is_sound(
            &straddling_bound,
            &lockstep_domain,
            1,
            2,
            delete(59, 2)
        ));

        // WholeResult rules carry no axis information: unverifiable.
        let whole_result = SpanReadSummary {
            result_region: Region::col_interval(0, 1, 41, 101),
            dependencies: vec![SpanReadDependency {
                read_region: Region::col_interval(0, 0, 41, 101),
                projection: DirtyProjectionRule::WholeResult,
            }],
        };
        assert!(!delete_compaction_frame_is_sound(
            &whole_result,
            &lockstep_domain,
            1,
            2,
            delete(59, 2)
        ));
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
