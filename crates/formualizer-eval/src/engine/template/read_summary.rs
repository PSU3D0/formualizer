//! Formula producer planning substrate for FP6.5R.
//!
//! This module is intentionally inert: it defines producer identities, result
//! and read-region indexes, retained span read summaries, and executable V1
//! dirty projections. It does not wire FormulaPlane into graph dirty routing,
//! scheduling, ingest cut-over, or evaluation.

use crate::SheetId;
use crate::engine::sheet_registry::SheetRegistry;

use super::canonical::{AxisRef, SheetBinding};
use super::dependency_summary::{FormulaClass, FormulaDependencySummary, PrecedentPattern};
use super::domain::ResultRegion;
use super::region::{AxisRange, Region};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpanReadSummary {
    pub(crate) result_region: Region,
    pub(crate) dependencies: Vec<SpanReadDependency>,
}

impl SpanReadSummary {
    pub(crate) fn from_formula_summary(
        sheet_id: SheetId,
        result_region: &ResultRegion,
        summary: &FormulaDependencySummary,
        sheet_registry: &SheetRegistry,
    ) -> Result<Self, ProjectionFallbackReason> {
        if summary.formula_class != FormulaClass::StaticPointwise
            || !summary.reject_reasons.is_empty()
        {
            return Err(ProjectionFallbackReason::UnsupportedDependencySummary);
        }

        let result_region_pattern = Region::from_domain(result_region.domain());
        let mut dependencies = Vec::new();
        for precedent in &summary.precedent_patterns {
            match precedent {
                PrecedentPattern::Cell(cell) => {
                    let target_sheet_id = match &cell.sheet {
                        SheetBinding::CurrentSheet => sheet_id,
                        SheetBinding::ExplicitName { name } => sheet_registry
                            .get_id(name)
                            .ok_or(ProjectionFallbackReason::UnsupportedSheetBinding)?,
                    };
                    let projection = DirtyProjectionRule::AffineCell {
                        row: AxisProjection::from_axis_ref(&cell.row)?,
                        col: AxisProjection::from_axis_ref(&cell.col)?,
                    };
                    let read_region = projection
                        .read_region_for_result(target_sheet_id, result_region_pattern)?;
                    let dependency = SpanReadDependency {
                        read_region,
                        projection,
                    };
                    if !dependencies.contains(&dependency) {
                        dependencies.push(dependency);
                    }
                }
                PrecedentPattern::Range(range) => {
                    let target_sheet_id = match &range.sheet {
                        SheetBinding::CurrentSheet => sheet_id,
                        SheetBinding::ExplicitName { name } => sheet_registry
                            .get_id(name)
                            .ok_or(ProjectionFallbackReason::UnsupportedSheetBinding)?,
                    };
                    let whole_column = matches!(range.start_row, AxisRef::WholeAxis)
                        && matches!(range.end_row, AxisRef::WholeAxis)
                        && axis_ref_is_finite_projection(&range.start_col)
                        && axis_ref_is_finite_projection(&range.end_col);
                    let whole_row = matches!(range.start_col, AxisRef::WholeAxis)
                        && matches!(range.end_col, AxisRef::WholeAxis);
                    if whole_row {
                        return Err(ProjectionFallbackReason::UnsupportedAxis);
                    }
                    let projection = if whole_column {
                        DirtyProjectionRule::WholeColumnRange {
                            col_start: AxisProjection::from_axis_ref(&range.start_col)?,
                            col_end: AxisProjection::from_axis_ref(&range.end_col)?,
                        }
                    } else {
                        DirtyProjectionRule::AffineRange {
                            row_start: AxisProjection::from_axis_ref(&range.start_row)?,
                            row_end: AxisProjection::from_axis_ref(&range.end_row)?,
                            col_start: AxisProjection::from_axis_ref(&range.start_col)?,
                            col_end: AxisProjection::from_axis_ref(&range.end_col)?,
                        }
                    };
                    for read_region in projection
                        .read_regions_for_result(target_sheet_id, result_region_pattern)?
                    {
                        let dependency = SpanReadDependency {
                            read_region,
                            projection,
                        };
                        if !dependencies.contains(&dependency) {
                            dependencies.push(dependency);
                        }
                    }
                }
            }
        }

        Ok(Self {
            result_region: result_region_pattern,
            dependencies,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpanReadDependency {
    pub(crate) read_region: Region,
    pub(crate) projection: DirtyProjectionRule,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReadProjection {
    pub(crate) target_sheet_id: SheetId,
    pub(crate) rule: DirtyProjectionRule,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DirtyProjectionRule {
    AffineCell {
        row: AxisProjection,
        col: AxisProjection,
    },
    AffineRange {
        row_start: AxisProjection,
        row_end: AxisProjection,
        col_start: AxisProjection,
        col_end: AxisProjection,
    },
    WholeColumnRange {
        col_start: AxisProjection,
        col_end: AxisProjection,
    },
    WholeResult,
}

impl DirtyProjectionRule {
    pub(crate) fn read_region_for_result(
        self,
        sheet_id: SheetId,
        result_region: Region,
    ) -> Result<Region, ProjectionFallbackReason> {
        match self {
            Self::WholeResult => Err(ProjectionFallbackReason::RequiresExplicitReadRegion),
            Self::WholeColumnRange { .. } => Err(ProjectionFallbackReason::UnsupportedAxis),
            Self::AffineCell { row, col } => {
                let (result_rows, result_cols) = bounded_extents(result_region)
                    .ok_or(ProjectionFallbackReason::UnboundedResultRegion)?;
                let source_rows = row.source_extent_for_result(result_rows)?;
                let source_cols = col.source_extent_for_result(result_cols)?;
                region_from_bounded_extents(sheet_id, source_rows, source_cols)
            }
            Self::AffineRange {
                row_start,
                row_end,
                col_start,
                col_end,
            } => {
                let (result_rows, result_cols) = bounded_extents(result_region)
                    .ok_or(ProjectionFallbackReason::UnboundedResultRegion)?;
                let source_rows = range_source_extent_for_result(row_start, row_end, result_rows)?;
                let source_cols = range_source_extent_for_result(col_start, col_end, result_cols)?;
                region_from_bounded_extents(sheet_id, source_rows, source_cols)
            }
        }
    }

    pub(crate) fn read_regions_for_result(
        self,
        sheet_id: SheetId,
        result_region: Region,
    ) -> Result<Vec<Region>, ProjectionFallbackReason> {
        match self {
            Self::AffineCell { .. } | Self::AffineRange { .. } => {
                Ok(vec![self.read_region_for_result(sheet_id, result_region)?])
            }
            Self::WholeResult => Err(ProjectionFallbackReason::RequiresExplicitReadRegion),
            Self::WholeColumnRange { col_start, col_end } => {
                let (_, result_cols) = bounded_extents(result_region)
                    .ok_or(ProjectionFallbackReason::UnboundedResultRegion)?;
                let source_cols = range_source_extent_for_result(col_start, col_end, result_cols)?;
                let col_count = source_cols
                    .high
                    .checked_sub(source_cols.low)
                    .and_then(|width| width.checked_add(1))
                    .ok_or(ProjectionFallbackReason::CoordinateOverflow)?;
                if col_count > 256 {
                    return Err(ProjectionFallbackReason::UnsupportedAxis);
                }
                Ok((source_cols.low..=source_cols.high)
                    .map(|col| Region::whole_col(sheet_id, col))
                    .collect())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AxisProjection {
    Relative { offset: i64 },
    Absolute { index: u32 },
}

impl AxisProjection {
    fn from_axis_ref(axis: &AxisRef) -> Result<Self, ProjectionFallbackReason> {
        match axis {
            AxisRef::RelativeToPlacement { offset } => Ok(Self::Relative { offset: *offset }),
            AxisRef::AbsoluteVc { index } => Ok(Self::Absolute {
                index: index
                    .checked_sub(1)
                    .ok_or(ProjectionFallbackReason::CoordinateOverflow)?,
            }),
            AxisRef::OpenStart | AxisRef::OpenEnd | AxisRef::WholeAxis | AxisRef::Unsupported => {
                Err(ProjectionFallbackReason::UnsupportedAxis)
            }
        }
    }

    fn source_extent_for_result(
        self,
        result: BoundedRange,
    ) -> Result<BoundedRange, ProjectionFallbackReason> {
        match self {
            Self::Relative { offset } => Ok(BoundedRange::new(
                add_offset(result.low, offset)?,
                add_offset(result.high, offset)?,
            )),
            Self::Absolute { index } => Ok(BoundedRange::new(index, index)),
        }
    }
}

fn axis_ref_is_finite_projection(axis: &AxisRef) -> bool {
    matches!(
        axis,
        AxisRef::RelativeToPlacement { .. } | AxisRef::AbsoluteVc { .. }
    )
}

fn range_source_extent_for_result(
    start: AxisProjection,
    end: AxisProjection,
    result: BoundedRange,
) -> Result<BoundedRange, ProjectionFallbackReason> {
    // Per placement the read interval on this axis is
    // [min(start(p), end(p)), max(start(p), end(p))]; both bounds are affine
    // in the placement index (Relative tracks p, Absolute is constant), so the
    // union over the bounded result extent is exactly the union of each
    // bound's own extent. This holds for uniform AND mixed-anchor bounds
    // (e.g. `$A$2:$A{r}` running totals or `$A{r}:$A$N` tail reads).
    let start_extent = start.source_extent_for_result(result)?;
    let end_extent = end.source_extent_for_result(result)?;
    Ok(start_extent.union(end_extent))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ProjectionFallbackReason {
    UnsupportedDependencySummary,
    UnsupportedSheetBinding,
    /// A defined-name reference whose definition is not a concrete Cell or
    /// Range (Literal/Formula definitions), or which does not resolve at all
    /// in the placement's scope. Such formulas stay on the legacy graph.
    NamedReferenceUnsupported,
    UnsupportedAxis,
    UnboundedResultRegion,
    UnsupportedChangedRegion,
    CoordinateOverflow,
    RequiresExplicitReadRegion,
    MissingProducerResultRegion,
    FixedPointIterationLimit,
}

/// Finite axis range — guaranteed `Point` or `Span` (not `From`/`To`/`All`).
/// Invariant: low <= high.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BoundedRange {
    pub(crate) low: u32,
    pub(crate) high: u32,
}

impl BoundedRange {
    #[inline]
    pub(crate) fn new(low: u32, high: u32) -> Self {
        debug_assert!(low <= high);
        Self { low, high }
    }

    #[inline]
    pub(crate) fn from_axis_range(range: AxisRange) -> Option<Self> {
        match range {
            AxisRange::Point(point) => Some(Self::new(point, point)),
            AxisRange::Span(low, high) => Some(Self::new(low, high)),
            AxisRange::From(_) | AxisRange::To(_) | AxisRange::All => None,
        }
    }

    #[inline]
    fn is_point(self) -> bool {
        self.low == self.high
    }

    #[inline]
    fn union(self, other: Self) -> Self {
        Self {
            low: self.low.min(other.low),
            high: self.high.max(other.high),
        }
    }
}

fn bounded_extents(pattern: Region) -> Option<(BoundedRange, BoundedRange)> {
    let (rows, cols) = pattern.axis_ranges();
    Some((
        BoundedRange::from_axis_range(rows)?,
        BoundedRange::from_axis_range(cols)?,
    ))
}

fn region_from_bounded_extents(
    sheet_id: SheetId,
    rows: BoundedRange,
    cols: BoundedRange,
) -> Result<Region, ProjectionFallbackReason> {
    Ok(match (rows.is_point(), cols.is_point()) {
        (true, true) => Region::point(sheet_id, rows.low, cols.low),
        (false, true) => Region::col_interval(sheet_id, cols.low, rows.low, rows.high),
        (true, false) => Region::row_interval(sheet_id, rows.low, cols.low, cols.high),
        (false, false) => Region::rect(sheet_id, rows.low, rows.high, cols.low, cols.high),
    })
}

fn add_offset(value: u32, offset: i64) -> Result<u32, ProjectionFallbackReason> {
    let value = i64::from(value);
    let shifted = value
        .checked_add(offset)
        .ok_or(ProjectionFallbackReason::CoordinateOverflow)?;
    u32::try_from(shifted).map_err(|_| ProjectionFallbackReason::CoordinateOverflow)
}

#[cfg(test)]
mod tests {

    use formualizer_parse::parser::parse;

    use super::super::canonical::canonicalize_template;
    use super::super::dependency_summary::summarize_canonical_template;
    use super::super::domain::PlacementDomain;
    use super::*;

    fn dependency_summary(formula: &str, row: u32, col: u32) -> FormulaDependencySummary {
        let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
        let template = canonicalize_template(&ast, row, col);
        summarize_canonical_template(&template)
    }

    #[test]
    fn formula_plane_span_read_summary_resolves_cross_sheet_binding() {
        let mut sheet_registry = SheetRegistry::new();
        let sheet1_id = sheet_registry.id_for("Sheet1");
        let data_id = sheet_registry.id_for("Data");
        let result_region =
            ResultRegion::scalar_cells(PlacementDomain::row_run(sheet1_id, 0, 0, 1));
        let summary = dependency_summary("=Data!A1", 1, 2);

        let read_summary = SpanReadSummary::from_formula_summary(
            sheet1_id,
            &result_region,
            &summary,
            &sheet_registry,
        )
        .expect("cross-sheet read summary");

        assert_eq!(read_summary.dependencies.len(), 1);
        assert_eq!(
            read_summary.dependencies[0].read_region,
            Region::point(data_id, 0, 0)
        );
    }

    #[test]
    fn formula_plane_span_read_summary_rejects_unknown_sheet() {
        let mut sheet_registry = SheetRegistry::new();
        let sheet1_id = sheet_registry.id_for("Sheet1");
        let result_region =
            ResultRegion::scalar_cells(PlacementDomain::row_run(sheet1_id, 0, 0, 1));
        let summary = dependency_summary("=Data!A1", 1, 2);

        let err = SpanReadSummary::from_formula_summary(
            sheet1_id,
            &result_region,
            &summary,
            &sheet_registry,
        )
        .expect_err("unknown sheet should reject");

        assert_eq!(err, ProjectionFallbackReason::UnsupportedSheetBinding);
    }

    #[test]
    fn whole_column_range_read_regions_emit_whole_cols() {
        let result = Region::col_interval(7, 5, 0, 99);
        let single_col = DirtyProjectionRule::WholeColumnRange {
            col_start: AxisProjection::Absolute { index: 0 },
            col_end: AxisProjection::Absolute { index: 0 },
        };
        assert_eq!(
            single_col.read_regions_for_result(7, result).unwrap(),
            vec![Region::whole_col(7, 0)]
        );
        assert_eq!(
            single_col.read_region_for_result(7, result),
            Err(ProjectionFallbackReason::UnsupportedAxis)
        );

        let multi_col = DirtyProjectionRule::WholeColumnRange {
            col_start: AxisProjection::Absolute { index: 0 },
            col_end: AxisProjection::Absolute { index: 3 },
        };
        assert_eq!(
            multi_col.read_regions_for_result(7, result).unwrap(),
            vec![
                Region::whole_col(7, 0),
                Region::whole_col(7, 1),
                Region::whole_col(7, 2),
                Region::whole_col(7, 3),
            ]
        );
    }

    #[test]
    fn whole_column_range_rejects_projected_column_count_above_bound() {
        let projection = DirtyProjectionRule::WholeColumnRange {
            col_start: AxisProjection::Absolute { index: 0 },
            col_end: AxisProjection::Absolute { index: 702 },
        };
        let result = Region::col_interval(0, 5, 0, 99);

        assert_eq!(
            projection.read_regions_for_result(0, result),
            Err(ProjectionFallbackReason::UnsupportedAxis)
        );
    }

    #[test]
    fn relative_projection_rejects_underflowing_read_region() {
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: -1 },
            col: AxisProjection::Relative { offset: 0 },
        };
        let result = Region::col_interval(0, 2, 0, 9);

        assert_eq!(
            projection.read_region_for_result(0, result),
            Err(ProjectionFallbackReason::CoordinateOverflow)
        );
    }
}
