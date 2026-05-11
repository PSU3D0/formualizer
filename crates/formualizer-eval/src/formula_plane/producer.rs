//! Formula producer planning substrate for FP6.5R.
//!
//! This module is intentionally inert: it defines producer identities, result
//! and read-region indexes, retained span read summaries, and executable V1
//! dirty projections. It does not wire FormulaPlane into graph dirty routing,
//! scheduling, ingest cut-over, or evaluation.

use std::collections::{BTreeMap, VecDeque};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::SheetId;
use crate::engine::VertexId;
use crate::engine::sheet_registry::SheetRegistry;

use super::dependency_summary::{FormulaClass, FormulaDependencySummary, PrecedentPattern};
use super::region_index::{
    AxisRange, Region, RegionKey, RegionMatch, RegionQueryResult, SheetRegionIndex,
};
use super::runtime::{FormulaSpanId, ResultRegion};
use super::template_canonical::{AxisRef, SheetBinding};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FormulaProducerId {
    Legacy(VertexId),
    Span(FormulaSpanId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaProducerWork {
    pub(crate) producer: FormulaProducerId,
    pub(crate) dirty: ProducerDirtyDomain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProducerDirtyDomain {
    Whole,
    Cells(Vec<RegionKey>),
    Regions(Vec<Region>),
}

impl ProducerDirtyDomain {
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Whole => false,
            Self::Cells(cells) => cells.is_empty(),
            Self::Regions(regions) => regions.is_empty(),
        }
    }

    pub(crate) fn merge_changed(&mut self, other: Self) -> bool {
        let before = self.clone();
        self.merge(other);
        *self != before
    }

    pub(crate) fn merge(&mut self, other: Self) {
        match (&mut *self, other) {
            (Self::Whole, _) | (_, Self::Whole) => {
                *self = Self::Whole;
            }
            (Self::Cells(existing), Self::Cells(incoming)) => {
                append_unique(existing, incoming);
            }
            (Self::Regions(existing), Self::Regions(incoming)) => {
                append_unique(existing, incoming);
            }
            (Self::Cells(existing_cells), Self::Regions(incoming_regions)) => {
                let mut regions = existing_cells
                    .iter()
                    .copied()
                    .map(|key| Region::point(key.sheet_id, key.row, key.col))
                    .collect::<Vec<_>>();
                append_unique(&mut regions, incoming_regions);
                *self = Self::Regions(regions);
            }
            (Self::Regions(existing_regions), Self::Cells(incoming_cells)) => {
                append_unique(
                    existing_regions,
                    incoming_cells
                        .into_iter()
                        .map(|key| Region::point(key.sheet_id, key.row, key.col)),
                );
            }
        }
    }

    pub(crate) fn result_regions(&self, producer_result_region: Region) -> Vec<Region> {
        match self {
            Self::Whole => vec![producer_result_region],
            Self::Cells(cells) => cells
                .iter()
                .copied()
                .map(|key| Region::point(key.sheet_id, key.row, key.col))
                .collect(),
            Self::Regions(regions) => regions.clone(),
        }
    }
}

fn append_unique<T, I>(existing: &mut Vec<T>, incoming: I)
where
    T: Eq + std::hash::Hash + Clone,
    I: IntoIterator<Item = T>,
{
    let mut seen = existing.iter().cloned().collect::<FxHashSet<_>>();
    for item in incoming {
        if seen.insert(item.clone()) {
            existing.push(item);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaProducerResultEntryId(usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaProducerResultEntry {
    pub(crate) producer: FormulaProducerId,
    pub(crate) result_region: Region,
}

#[derive(Debug, Default)]
pub(crate) struct FormulaProducerResultIndex {
    index: SheetRegionIndex<FormulaProducerResultEntryId>,
    entries: Vec<FormulaProducerResultEntry>,
    by_producer: FxHashMap<FormulaProducerId, Region>,
    epoch: u64,
}

impl FormulaProducerResultIndex {
    pub(crate) fn insert_producer(
        &mut self,
        producer: FormulaProducerId,
        result_region: Region,
    ) -> FormulaProducerResultEntryId {
        let id = FormulaProducerResultEntryId(self.entries.len());
        self.entries.push(FormulaProducerResultEntry {
            producer,
            result_region,
        });
        self.by_producer.insert(producer, result_region);
        self.index.insert(result_region, id);
        self.epoch = self.epoch.saturating_add(1);
        id
    }

    pub(crate) fn query(
        &self,
        read_region: Region,
    ) -> RegionQueryResult<FormulaProducerResultEntry> {
        let result = self.index.query(read_region);
        RegionQueryResult {
            matches: result
                .matches
                .into_iter()
                .map(|matched| RegionMatch {
                    value: self.entries[matched.value.0].clone(),
                    indexed_region: matched.indexed_region,
                })
                .collect(),
            stats: result.stats,
        }
    }

    pub(crate) fn producer_result_region(&self, producer: FormulaProducerId) -> Option<Region> {
        self.by_producer.get(&producer).copied()
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaConsumerReadEntryId(usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaConsumerReadEntry {
    pub(crate) consumer: FormulaProducerId,
    pub(crate) read_region: Region,
    pub(crate) consumer_result_region: Region,
    pub(crate) projection: DirtyProjectionRule,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaConsumerDirtyCandidate {
    pub(crate) consumer: FormulaProducerId,
    pub(crate) read_region: Region,
    pub(crate) consumer_result_region: Region,
    pub(crate) projection: DirtyProjectionRule,
    pub(crate) dirty: ProjectionResult,
}

#[derive(Debug, Default)]
pub(crate) struct FormulaConsumerReadIndex {
    index: SheetRegionIndex<FormulaConsumerReadEntryId>,
    entries: Vec<FormulaConsumerReadEntry>,
    epoch: u64,
}

impl FormulaConsumerReadIndex {
    pub(crate) fn insert_read(
        &mut self,
        consumer: FormulaProducerId,
        read_region: Region,
        consumer_result_region: Region,
        projection: DirtyProjectionRule,
    ) -> FormulaConsumerReadEntryId {
        let id = FormulaConsumerReadEntryId(self.entries.len());
        self.entries.push(FormulaConsumerReadEntry {
            consumer,
            read_region,
            consumer_result_region,
            projection,
        });
        self.index.insert(read_region, id);
        self.epoch = self.epoch.saturating_add(1);
        id
    }

    /// Query consumers whose read regions intersect `changed` and attach a
    /// projection result for each candidate.
    ///
    /// Callers that build dirty work must inspect `ProjectionResult`: `Exact`
    /// and `Conservative` produce dirty work, `Unsupported` must fail closed or
    /// demote, and `NoIntersection` must be ignored. The latter can occur after
    /// clipping even when the geometric read-index query over-returned.
    pub(crate) fn query_changed_region(
        &self,
        changed: Region,
    ) -> RegionQueryResult<FormulaConsumerDirtyCandidate> {
        let result = self.index.query(changed);
        RegionQueryResult {
            matches: result
                .matches
                .into_iter()
                .map(|matched| {
                    let entry = self.entries[matched.value.0].clone();
                    let dirty = entry.projection.project_changed_region(
                        changed,
                        entry.read_region,
                        entry.consumer_result_region,
                    );
                    RegionMatch {
                        indexed_region: matched.indexed_region,
                        value: FormulaConsumerDirtyCandidate {
                            consumer: entry.consumer,
                            read_region: entry.read_region,
                            consumer_result_region: entry.consumer_result_region,
                            projection: entry.projection,
                            dirty,
                        },
                    }
                })
                .collect(),
            stats: result.stats,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

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

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SpanReadSummaryId(pub(crate) u32);

#[derive(Debug, Default)]
pub(crate) struct SpanReadSummaryStore {
    records: Vec<SpanReadSummary>,
    epoch: u64,
}

impl SpanReadSummaryStore {
    pub(crate) fn insert(&mut self, summary: SpanReadSummary) -> SpanReadSummaryId {
        let id = SpanReadSummaryId(self.records.len() as u32);
        self.records.push(summary);
        self.epoch = self.epoch.saturating_add(1);
        id
    }

    pub(crate) fn get(&self, id: SpanReadSummaryId) -> Option<&SpanReadSummary> {
        self.records.get(id.0 as usize)
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
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

    pub(crate) fn project_changed_region(
        self,
        changed: Region,
        read_region: Region,
        result_region: Region,
    ) -> ProjectionResult {
        if !changed.intersects(&read_region) {
            return ProjectionResult::NoIntersection;
        }
        if self == Self::WholeResult {
            return ProjectionResult::Exact(ProducerDirtyDomain::Whole);
        }
        if matches!(self, Self::WholeColumnRange { .. }) {
            return ProjectionResult::Exact(ProducerDirtyDomain::Regions(vec![result_region]));
        }

        let (changed_rows, changed_cols) = changed.axis_ranges();
        let (result_rows, result_cols) = result_region.axis_ranges();
        let result_is_bounded = bounded_extents(result_region).is_some();
        let sheet_id = result_region.sheet_id();

        match self {
            Self::AffineCell { row, col } => {
                let dirty_rows = match row.project_changed_axis(changed_rows, result_rows) {
                    Ok(Some(dirty_rows)) => dirty_rows,
                    Ok(None) => return ProjectionResult::NoIntersection,
                    Err(reason) => return ProjectionResult::Unsupported(reason),
                };
                let dirty_cols = match col.project_changed_axis(changed_cols, result_cols) {
                    Ok(Some(dirty_cols)) => dirty_cols,
                    Ok(None) => return ProjectionResult::NoIntersection,
                    Err(reason) => return ProjectionResult::Unsupported(reason),
                };
                projection_result_from_axis_ranges(
                    sheet_id,
                    dirty_rows,
                    dirty_cols,
                    result_region,
                    result_is_bounded,
                )
            }
            Self::AffineRange {
                row_start,
                row_end,
                col_start,
                col_end,
            } => {
                if !row_start.same_kind(row_end) || !col_start.same_kind(col_end) {
                    return ProjectionResult::Unsupported(
                        ProjectionFallbackReason::UnsupportedAxis,
                    );
                }
                let dirty_rows =
                    match project_changed_range_axis(row_start, row_end, changed_rows, result_rows)
                    {
                        Ok(Some(dirty_rows)) => dirty_rows,
                        Ok(None) => return ProjectionResult::NoIntersection,
                        Err(reason) => return ProjectionResult::Unsupported(reason),
                    };
                let dirty_cols =
                    match project_changed_range_axis(col_start, col_end, changed_cols, result_cols)
                    {
                        Ok(Some(dirty_cols)) => dirty_cols,
                        Ok(None) => return ProjectionResult::NoIntersection,
                        Err(reason) => return ProjectionResult::Unsupported(reason),
                    };
                projection_result_from_axis_ranges(
                    sheet_id,
                    dirty_rows,
                    dirty_cols,
                    result_region,
                    result_is_bounded,
                )
            }
            Self::WholeColumnRange { .. } | Self::WholeResult => unreachable!("handled above"),
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

    fn same_kind(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Relative { .. }, Self::Relative { .. })
                | (Self::Absolute { .. }, Self::Absolute { .. })
        )
    }

    fn project_changed_axis(
        self,
        changed: AxisRange,
        result: AxisRange,
    ) -> Result<Option<AxisRange>, ProjectionFallbackReason> {
        match self {
            Self::Relative { offset } => {
                let projection_offset = offset
                    .checked_neg()
                    .ok_or(ProjectionFallbackReason::CoordinateOverflow)?;
                let Some(dirty) = project_axis_range_through_offset(changed, projection_offset)
                else {
                    return Ok(None);
                };
                Ok(intersect_axis_ranges(dirty, result))
            }
            Self::Absolute { index } => Ok(changed.contains(index).then_some(result)),
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
    if !start.same_kind(end) {
        return Err(ProjectionFallbackReason::UnsupportedAxis);
    }
    let start_extent = start.source_extent_for_result(result)?;
    let end_extent = end.source_extent_for_result(result)?;
    Ok(start_extent.union(end_extent))
}

fn project_changed_range_axis(
    start: AxisProjection,
    end: AxisProjection,
    changed: AxisRange,
    result: AxisRange,
) -> Result<Option<AxisRange>, ProjectionFallbackReason> {
    match (start, end) {
        (
            AxisProjection::Relative {
                offset: start_offset,
            },
            AxisProjection::Relative { offset: end_offset },
        ) => {
            let min_offset = start_offset.min(end_offset);
            let max_offset = start_offset.max(end_offset);
            let dirty = match changed {
                AxisRange::All => Some(AxisRange::All),
                AxisRange::Point(point) => {
                    project_axis_interval_through_offsets(point, point, max_offset, min_offset)?
                }
                AxisRange::Span(start, end) => {
                    project_axis_interval_through_offsets(start, end, max_offset, min_offset)?
                }
                AxisRange::From(start) => {
                    let projection_offset = max_offset
                        .checked_neg()
                        .ok_or(ProjectionFallbackReason::CoordinateOverflow)?;
                    project_axis_range_through_offset(AxisRange::From(start), projection_offset)
                }
                AxisRange::To(end) => {
                    let projection_offset = min_offset
                        .checked_neg()
                        .ok_or(ProjectionFallbackReason::CoordinateOverflow)?;
                    project_axis_range_through_offset(AxisRange::To(end), projection_offset)
                }
            };
            Ok(dirty.and_then(|dirty| intersect_axis_ranges(dirty, result)))
        }
        (
            AxisProjection::Absolute { index: start_index },
            AxisProjection::Absolute { index: end_index },
        ) => {
            let source = AxisRange::Span(start_index.min(end_index), start_index.max(end_index));
            Ok(source.intersects(changed).then_some(result))
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionResult {
    Exact(ProducerDirtyDomain),
    Conservative {
        dirty: ProducerDirtyDomain,
        reason: ProjectionFallbackReason,
    },
    NoIntersection,
    Unsupported(ProjectionFallbackReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ProjectionFallbackReason {
    UnsupportedDependencySummary,
    UnsupportedSheetBinding,
    UnsupportedAxis,
    UnboundedResultRegion,
    UnsupportedChangedRegion,
    CoordinateOverflow,
    RequiresExplicitReadRegion,
    MissingProducerResultRegion,
    FixedPointIterationLimit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaDirtyClosure {
    pub(crate) work: Vec<FormulaProducerWork>,
    pub(crate) changed_result_regions: Vec<Region>,
    pub(crate) stats: FormulaDirtyClosureStats,
    pub(crate) fallbacks: Vec<FormulaDirtyFallback>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FormulaDirtyClosureStats {
    pub(crate) input_changed_regions: usize,
    pub(crate) read_index_query_count: usize,
    pub(crate) read_index_candidate_count: usize,
    pub(crate) exact_filter_drop_count: usize,
    pub(crate) projection_exact_count: usize,
    pub(crate) projection_conservative_count: usize,
    pub(crate) projection_no_intersection_count: usize,
    pub(crate) projection_unsupported_count: usize,
    pub(crate) merged_dirty_domains: usize,
    pub(crate) emitted_changed_regions: usize,
    pub(crate) duplicate_changed_regions_skipped: usize,
    pub(crate) fixed_point_iterations: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaDirtyFallback {
    pub(crate) consumer: FormulaProducerId,
    pub(crate) changed_region: Region,
    pub(crate) reason: ProjectionFallbackReason,
}

const DIRTY_CLOSURE_ITERATION_LIMIT: usize = 100_000;

pub(crate) fn compute_dirty_closure(
    consumer_reads: &FormulaConsumerReadIndex,
    changed_regions: impl IntoIterator<Item = Region>,
    result_region: impl Fn(FormulaProducerId) -> Option<Region>,
) -> FormulaDirtyClosure {
    let mut stats = FormulaDirtyClosureStats::default();
    let mut queue = VecDeque::new();
    let mut seen_changed_regions = FxHashSet::default();
    for region in changed_regions {
        stats.input_changed_regions = stats.input_changed_regions.saturating_add(1);
        if seen_changed_regions.insert(region) {
            queue.push_back(region);
        } else {
            stats.duplicate_changed_regions_skipped =
                stats.duplicate_changed_regions_skipped.saturating_add(1);
        }
    }

    let mut dirty_by_producer: BTreeMap<FormulaProducerId, ProducerDirtyDomain> = BTreeMap::new();
    let mut changed_result_regions = Vec::new();
    let mut fallbacks = Vec::new();

    while let Some(changed_region) = queue.pop_front() {
        stats.fixed_point_iterations = stats.fixed_point_iterations.saturating_add(1);
        if stats.fixed_point_iterations > DIRTY_CLOSURE_ITERATION_LIMIT {
            fallbacks.push(FormulaDirtyFallback {
                consumer: FormulaProducerId::Legacy(VertexId(0)),
                changed_region,
                reason: ProjectionFallbackReason::FixedPointIterationLimit,
            });
            break;
        }

        stats.read_index_query_count = stats.read_index_query_count.saturating_add(1);
        let query = consumer_reads.query_changed_region(changed_region);
        stats.read_index_candidate_count = stats
            .read_index_candidate_count
            .saturating_add(query.stats.candidate_count);
        stats.exact_filter_drop_count = stats
            .exact_filter_drop_count
            .saturating_add(query.stats.exact_filter_drop_count);

        for matched in query.matches {
            let candidate = matched.value;
            match candidate.dirty {
                ProjectionResult::Exact(dirty) => {
                    stats.projection_exact_count = stats.projection_exact_count.saturating_add(1);
                    apply_dirty_projection(
                        candidate.consumer,
                        changed_region,
                        dirty,
                        &result_region,
                        &mut dirty_by_producer,
                        &mut queue,
                        &mut seen_changed_regions,
                        &mut changed_result_regions,
                        &mut fallbacks,
                        &mut stats,
                    );
                }
                ProjectionResult::Conservative { dirty, reason } => {
                    stats.projection_conservative_count =
                        stats.projection_conservative_count.saturating_add(1);
                    fallbacks.push(FormulaDirtyFallback {
                        consumer: candidate.consumer,
                        changed_region,
                        reason,
                    });
                    apply_dirty_projection(
                        candidate.consumer,
                        changed_region,
                        dirty,
                        &result_region,
                        &mut dirty_by_producer,
                        &mut queue,
                        &mut seen_changed_regions,
                        &mut changed_result_regions,
                        &mut fallbacks,
                        &mut stats,
                    );
                }
                ProjectionResult::NoIntersection => {
                    stats.projection_no_intersection_count =
                        stats.projection_no_intersection_count.saturating_add(1);
                }
                ProjectionResult::Unsupported(reason) => {
                    stats.projection_unsupported_count =
                        stats.projection_unsupported_count.saturating_add(1);
                    fallbacks.push(FormulaDirtyFallback {
                        consumer: candidate.consumer,
                        changed_region,
                        reason,
                    });
                }
            }
        }
    }

    let work = dirty_by_producer
        .into_iter()
        .map(|(producer, dirty)| FormulaProducerWork { producer, dirty })
        .collect();

    FormulaDirtyClosure {
        work,
        changed_result_regions,
        stats,
        fallbacks,
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_dirty_projection(
    consumer: FormulaProducerId,
    changed_region: Region,
    dirty: ProducerDirtyDomain,
    result_region: &impl Fn(FormulaProducerId) -> Option<Region>,
    dirty_by_producer: &mut BTreeMap<FormulaProducerId, ProducerDirtyDomain>,
    queue: &mut VecDeque<Region>,
    seen_changed_regions: &mut FxHashSet<Region>,
    changed_result_regions: &mut Vec<Region>,
    fallbacks: &mut Vec<FormulaDirtyFallback>,
    stats: &mut FormulaDirtyClosureStats,
) {
    let merged = if let Some(existing) = dirty_by_producer.get_mut(&consumer) {
        existing.merge_changed(dirty.clone())
    } else {
        dirty_by_producer.insert(consumer, dirty.clone());
        true
    };

    if !merged {
        return;
    }

    stats.merged_dirty_domains = stats.merged_dirty_domains.saturating_add(1);
    let Some(producer_result_region) = result_region(consumer) else {
        fallbacks.push(FormulaDirtyFallback {
            consumer,
            changed_region,
            reason: ProjectionFallbackReason::MissingProducerResultRegion,
        });
        return;
    };

    for emitted in dirty.result_regions(producer_result_region) {
        if seen_changed_regions.insert(emitted) {
            queue.push_back(emitted);
            changed_result_regions.push(emitted);
            stats.emitted_changed_regions = stats.emitted_changed_regions.saturating_add(1);
        } else {
            stats.duplicate_changed_regions_skipped =
                stats.duplicate_changed_regions_skipped.saturating_add(1);
        }
    }
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
    pub(crate) fn to_axis_range(self) -> AxisRange {
        if self.low == self.high {
            AxisRange::Point(self.low)
        } else {
            AxisRange::Span(self.low, self.high)
        }
    }

    #[inline]
    fn is_point(self) -> bool {
        self.low == self.high
    }

    #[inline]
    fn intersect(self, other: Self) -> Option<Self> {
        let low = self.low.max(other.low);
        let high = self.high.min(other.high);
        (low <= high).then_some(Self { low, high })
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

fn projection_result_from_axis_ranges(
    sheet_id: SheetId,
    rows: AxisRange,
    cols: AxisRange,
    result_region: Region,
    result_is_bounded: bool,
) -> ProjectionResult {
    if !result_is_bounded && rows.is_bounded() && cols.is_bounded() {
        return ProjectionResult::Unsupported(ProjectionFallbackReason::UnboundedResultRegion);
    }

    match region_from_axis_ranges(sheet_id, rows, cols) {
        Ok(region) => ProjectionResult::Exact(dirty_domain_from_region(region)),
        Err(reason) => ProjectionResult::Conservative {
            dirty: ProducerDirtyDomain::Regions(vec![result_region]),
            reason,
        },
    }
}

fn region_from_axis_ranges(
    sheet_id: SheetId,
    rows: AxisRange,
    cols: AxisRange,
) -> Result<Region, ProjectionFallbackReason> {
    Ok(match (rows, cols) {
        (AxisRange::Point(row), AxisRange::Point(col)) => Region::point(sheet_id, row, col),
        (AxisRange::Span(row_start, row_end), AxisRange::Point(col)) => {
            Region::col_interval(sheet_id, col, row_start, row_end)
        }
        (AxisRange::Point(row), AxisRange::Span(col_start, col_end)) => {
            Region::row_interval(sheet_id, row, col_start, col_end)
        }
        (AxisRange::Span(row_start, row_end), AxisRange::Span(col_start, col_end)) => {
            Region::rect(sheet_id, row_start, row_end, col_start, col_end)
        }
        (AxisRange::From(row_start), AxisRange::All) => Region::rows_from(sheet_id, row_start),
        (AxisRange::All, AxisRange::From(col_start)) => Region::cols_from(sheet_id, col_start),
        (AxisRange::Point(row), AxisRange::All) => Region::whole_row(sheet_id, row),
        (AxisRange::All, AxisRange::Point(col)) => Region::whole_col(sheet_id, col),
        (AxisRange::All, AxisRange::All) => Region::whole_sheet(sheet_id),
        (rows, cols) => {
            let (row_low, row_high) = rows.query_bounds();
            let (col_low, col_high) = cols.query_bounds();
            return region_from_bounded_extents(
                sheet_id,
                BoundedRange::new(row_low, row_high),
                BoundedRange::new(col_low, col_high),
            );
        }
    })
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

fn dirty_domain_from_region(region: Region) -> ProducerDirtyDomain {
    match region.as_point() {
        Some(key) => ProducerDirtyDomain::Cells(vec![key]),
        None => ProducerDirtyDomain::Regions(vec![region]),
    }
}

fn add_offset(value: u32, offset: i64) -> Result<u32, ProjectionFallbackReason> {
    let value = i64::from(value);
    let shifted = value
        .checked_add(offset)
        .ok_or(ProjectionFallbackReason::CoordinateOverflow)?;
    u32::try_from(shifted).map_err(|_| ProjectionFallbackReason::CoordinateOverflow)
}

fn project_axis_range_through_offset(range: AxisRange, offset: i64) -> Option<AxisRange> {
    match range {
        AxisRange::Point(point) => checked_shift_to_u32(point, offset).map(AxisRange::Point),
        AxisRange::Span(start, end) => axis_range_from_shifted_bounds(
            shifted_coord_for_clip(start, offset),
            shifted_coord_for_clip(end, offset),
        ),
        AxisRange::From(start) => {
            let projected_start = shifted_coord_for_clip(start, offset);
            if projected_start < 0 {
                Some(AxisRange::All)
            } else {
                Some(normalize_axis_range(AxisRange::From(clamp_i64_to_u32(
                    projected_start,
                ))))
            }
        }
        AxisRange::To(end) => {
            let projected_end = shifted_coord_for_clip(end, offset);
            if projected_end < 0 {
                None
            } else {
                Some(normalize_axis_range(AxisRange::To(clamp_i64_to_u32(
                    projected_end,
                ))))
            }
        }
        AxisRange::All => Some(AxisRange::All),
    }
}

fn project_axis_interval_through_offsets(
    start: u32,
    end: u32,
    low_source_offset: i64,
    high_source_offset: i64,
) -> Result<Option<AxisRange>, ProjectionFallbackReason> {
    let low_projection_offset = low_source_offset
        .checked_neg()
        .ok_or(ProjectionFallbackReason::CoordinateOverflow)?;
    let high_projection_offset = high_source_offset
        .checked_neg()
        .ok_or(ProjectionFallbackReason::CoordinateOverflow)?;
    Ok(axis_range_from_shifted_bounds(
        shifted_coord_for_clip(start, low_projection_offset),
        shifted_coord_for_clip(end, high_projection_offset),
    ))
}

fn intersect_axis_ranges(a: AxisRange, b: AxisRange) -> Option<AxisRange> {
    match (a, b) {
        (AxisRange::All, range) | (range, AxisRange::All) => Some(normalize_axis_range(range)),
        (AxisRange::From(left), AxisRange::From(right)) => {
            Some(normalize_axis_range(AxisRange::From(left.max(right))))
        }
        (AxisRange::To(left), AxisRange::To(right)) => {
            Some(normalize_axis_range(AxisRange::To(left.min(right))))
        }
        (left, right) => {
            let (left_low, left_high) = left.query_bounds();
            let (right_low, right_high) = right.query_bounds();
            let low = left_low.max(right_low);
            let high = left_high.min(right_high);
            (low <= high).then(|| axis_range_from_u32_bounds(low, high))
        }
    }
}

fn axis_range_from_shifted_bounds(low: i64, high: i64) -> Option<AxisRange> {
    if low > high || high < 0 || low > i64::from(u32::MAX) {
        None
    } else {
        Some(axis_range_from_u32_bounds(
            clamp_i64_to_u32(low),
            clamp_i64_to_u32(high),
        ))
    }
}

fn axis_range_from_u32_bounds(low: u32, high: u32) -> AxisRange {
    debug_assert!(low <= high);
    normalize_axis_range(if low == high {
        AxisRange::Point(low)
    } else if low == 0 && high == u32::MAX {
        AxisRange::All
    } else if high == u32::MAX {
        AxisRange::From(low)
    } else if low == 0 {
        AxisRange::To(high)
    } else {
        AxisRange::Span(low, high)
    })
}

fn normalize_axis_range(range: AxisRange) -> AxisRange {
    match range {
        AxisRange::Span(start, end) if start == end => AxisRange::Point(start),
        AxisRange::From(0) | AxisRange::To(u32::MAX) => AxisRange::All,
        other => other,
    }
}

fn shifted_coord_for_clip(value: u32, offset: i64) -> i64 {
    i64::from(value).checked_add(offset).unwrap_or(i64::MAX)
}

fn checked_shift_to_u32(value: u32, offset: i64) -> Option<u32> {
    let shifted = i64::from(value).checked_add(offset)?;
    u32::try_from(shifted).ok()
}

fn clamp_i64_to_u32(value: i64) -> u32 {
    if value < 0 {
        0
    } else {
        u32::try_from(value).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use formualizer_parse::parser::parse;

    use super::super::dependency_summary::summarize_canonical_template;
    use super::super::runtime::{FormulaSpanId, PlacementDomain};
    use super::super::template_canonical::canonicalize_template;
    use super::*;

    fn span(id: u32) -> FormulaProducerId {
        FormulaProducerId::Span(FormulaSpanId(id))
    }

    fn legacy(id: u32) -> FormulaProducerId {
        FormulaProducerId::Legacy(VertexId(id))
    }

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
    fn producer_result_index_finds_legacy_and_span_producers() {
        let mut index = FormulaProducerResultIndex::default();
        index.insert_producer(legacy(1), Region::point(0, 9, 2));
        index.insert_producer(span(2), Region::col_interval(0, 1, 0, 99));

        let point = index.query(Region::point(0, 9, 2));
        assert!(point.matches.iter().any(|m| m.value.producer == legacy(1)));

        let span_hit = index.query(Region::point(0, 50, 1));
        assert_eq!(span_hit.matches.len(), 1);
        assert_eq!(span_hit.matches[0].value.producer, span(2));
    }

    #[test]
    fn consumer_read_index_projects_same_row_dirty_cell() {
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };
        let result = Region::col_interval(0, 2, 0, 99);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, Region::col_interval(0, 1, 0, 99));

        let mut index = FormulaConsumerReadIndex::default();
        index.insert_read(span(1), read, result, projection);
        let dirty = index.query_changed_region(Region::point(0, 50, 1));
        assert_eq!(dirty.matches.len(), 1);
        assert_eq!(dirty.matches[0].value.consumer, span(1));
        assert_eq!(
            dirty.matches[0].value.dirty,
            ProjectionResult::Exact(ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 50, 2)]))
        );
    }

    #[test]
    fn shifted_ref_projects_to_neighbor_result_cell() {
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 1 },
            col: AxisProjection::Relative { offset: -1 },
        };
        let result = Region::col_interval(0, 2, 0, 99);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, Region::col_interval(0, 1, 1, 100));

        let dirty = projection.project_changed_region(Region::point(0, 50, 1), read, result);
        assert_eq!(
            dirty,
            ProjectionResult::Exact(ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 49, 2)]))
        );
    }

    #[test]
    fn absolute_ref_projects_to_whole_result_region_when_source_changes() {
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Absolute { index: 0 },
            col: AxisProjection::Absolute { index: 0 },
        };
        let result = Region::col_interval(0, 2, 0, 99);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, Region::point(0, 0, 0));

        let dirty = projection.project_changed_region(Region::point(0, 0, 0), read, result);
        assert_eq!(
            dirty,
            ProjectionResult::Exact(ProducerDirtyDomain::Regions(vec![result]))
        );
    }

    #[test]
    fn absolute_range_projects_to_whole_result_on_intersection() {
        let projection = DirtyProjectionRule::AffineRange {
            row_start: AxisProjection::Absolute { index: 0 },
            row_end: AxisProjection::Absolute { index: 9 },
            col_start: AxisProjection::Absolute { index: 0 },
            col_end: AxisProjection::Absolute { index: 1 },
        };
        let result = Region::col_interval(0, 3, 0, 19);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, Region::rect(0, 0, 9, 0, 1));

        assert_eq!(
            projection.project_changed_region(Region::point(0, 4, 1), read, result),
            ProjectionResult::Exact(ProducerDirtyDomain::Regions(vec![result]))
        );
        assert_eq!(
            projection.project_changed_region(Region::point(0, 10, 1), read, result),
            ProjectionResult::NoIntersection
        );
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
    fn whole_column_range_projects_any_intersecting_edit_to_whole_result_region() {
        let projection = DirtyProjectionRule::WholeColumnRange {
            col_start: AxisProjection::Absolute { index: 0 },
            col_end: AxisProjection::Absolute { index: 0 },
        };
        let result = Region::col_interval(0, 2, 0, 99);
        let read = projection.read_regions_for_result(0, result).unwrap()[0];

        assert_eq!(
            projection.project_changed_region(Region::point(0, 50, 0), read, result),
            ProjectionResult::Exact(ProducerDirtyDomain::Regions(vec![result]))
        );
        assert_eq!(
            projection.project_changed_region(Region::point(0, 50, 1), read, result),
            ProjectionResult::NoIntersection
        );
    }

    #[test]
    fn relative_range_projects_changed_source_to_overlapping_results() {
        let projection = DirtyProjectionRule::AffineRange {
            row_start: AxisProjection::Relative { offset: 0 },
            row_end: AxisProjection::Relative { offset: 5 },
            col_start: AxisProjection::Relative { offset: -1 },
            col_end: AxisProjection::Relative { offset: -1 },
        };
        let result = Region::col_interval(0, 2, 10, 20);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, Region::col_interval(0, 1, 10, 25));

        let dirty = projection.project_changed_region(Region::point(0, 12, 1), read, result);
        assert_eq!(
            dirty,
            ProjectionResult::Exact(ProducerDirtyDomain::Regions(vec![Region::col_interval(
                0, 2, 10, 12
            )]))
        );
    }

    #[test]
    fn mixed_axis_range_projection_rejects() {
        let projection = DirtyProjectionRule::AffineRange {
            row_start: AxisProjection::Absolute { index: 0 },
            row_end: AxisProjection::Relative { offset: 0 },
            col_start: AxisProjection::Absolute { index: 0 },
            col_end: AxisProjection::Absolute { index: 0 },
        };
        let result = Region::col_interval(0, 2, 0, 9);

        assert_eq!(
            projection.read_region_for_result(0, result),
            Err(ProjectionFallbackReason::UnsupportedAxis)
        );
    }

    #[test]
    fn mixed_absolute_relative_axis_projects_to_result_rect() {
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Absolute { index: 0 },
        };
        let result = Region::rect(0, 0, 9, 2, 4);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, Region::col_interval(0, 0, 0, 9));

        let dirty =
            projection.project_changed_region(Region::col_interval(0, 0, 5, 6), read, result);
        assert_eq!(
            dirty,
            ProjectionResult::Exact(ProducerDirtyDomain::Regions(vec![Region::rect(
                0, 5, 6, 2, 4
            )]))
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

    #[test]
    fn dirty_domain_merge_preserves_sparse_cells_without_widening() {
        let mut dirty = ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 1, 2)]);
        dirty.merge(ProducerDirtyDomain::Cells(vec![
            RegionKey::new(0, 1, 2),
            RegionKey::new(0, 10, 2),
        ]));
        assert_eq!(
            dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 1, 2), RegionKey::new(0, 10, 2)])
        );
    }

    #[test]
    fn dirty_domain_merge_changed_reports_growth_only() {
        let mut dirty = ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 1, 2)]);
        assert!(!dirty.merge_changed(ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 1, 2)])));
        assert!(dirty.merge_changed(ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 2, 2)])));
        assert_eq!(
            dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 1, 2), RegionKey::new(0, 2, 2)])
        );
    }

    #[test]
    fn whole_result_projection_dirties_entire_consumer_result() {
        let projection = DirtyProjectionRule::WholeResult;
        let read = Region::col_interval(0, 1, 0, 99);
        let result = Region::point(0, 0, 3);

        assert_eq!(
            projection.read_region_for_result(0, result),
            Err(ProjectionFallbackReason::RequiresExplicitReadRegion)
        );
        assert_eq!(
            projection.project_changed_region(Region::point(0, 50, 1), read, result),
            ProjectionResult::Exact(ProducerDirtyDomain::Whole)
        );
        assert_eq!(
            projection.project_changed_region(Region::point(0, 50, 2), read, result),
            ProjectionResult::NoIntersection
        );
    }

    #[test]
    fn dirty_closure_same_row_source_dirties_single_span_cell() {
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };
        let result = Region::col_interval(0, 1, 0, 9);
        let read = projection.read_region_for_result(0, result).unwrap();
        let mut index = FormulaConsumerReadIndex::default();
        index.insert_read(span(1), read, result, projection);

        let closure = compute_dirty_closure(&index, [Region::point(0, 5, 0)], |producer| {
            (producer == span(1)).then_some(result)
        });

        assert_eq!(closure.fallbacks, Vec::new());
        assert_eq!(closure.work.len(), 1);
        assert_eq!(closure.work[0].producer, span(1));
        assert_eq!(
            closure.work[0].dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 1)])
        );
        assert_eq!(closure.changed_result_regions, vec![Region::point(0, 5, 1)]);
    }

    #[test]
    fn dirty_closure_composes_span_to_span_single_cell() {
        let mut index = FormulaConsumerReadIndex::default();
        let b_result = Region::col_interval(0, 1, 0, 9);
        let c_result = Region::col_interval(0, 2, 0, 9);
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };
        index.insert_read(
            span(1),
            projection.read_region_for_result(0, b_result).unwrap(),
            b_result,
            projection,
        );
        index.insert_read(
            span(2),
            projection.read_region_for_result(0, c_result).unwrap(),
            c_result,
            projection,
        );

        let closure =
            compute_dirty_closure(
                &index,
                [Region::point(0, 5, 0)],
                |producer| match producer {
                    producer if producer == span(1) => Some(b_result),
                    producer if producer == span(2) => Some(c_result),
                    _ => None,
                },
            );

        assert_eq!(closure.fallbacks, Vec::new());
        assert_eq!(closure.work.len(), 2);
        assert_eq!(
            closure.work[0].dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 1)])
        );
        assert_eq!(
            closure.work[1].dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 2)])
        );
        assert_eq!(
            closure.changed_result_regions,
            vec![Region::point(0, 5, 1), Region::point(0, 5, 2)]
        );
    }

    #[test]
    fn dirty_closure_reaches_legacy_range_consumer_after_span_cell_dirty() {
        let mut index = FormulaConsumerReadIndex::default();
        let b_result = Region::col_interval(0, 1, 0, 99);
        let b_projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };
        index.insert_read(
            span(1),
            b_projection.read_region_for_result(0, b_result).unwrap(),
            b_result,
            b_projection,
        );
        let legacy_result = Region::point(0, 0, 3);
        index.insert_read(
            legacy(10),
            b_result,
            legacy_result,
            DirtyProjectionRule::WholeResult,
        );

        let closure =
            compute_dirty_closure(
                &index,
                [Region::point(0, 50, 0)],
                |producer| match producer {
                    producer if producer == span(1) => Some(b_result),
                    producer if producer == legacy(10) => Some(legacy_result),
                    _ => None,
                },
            );

        assert_eq!(closure.fallbacks, Vec::new());
        assert_eq!(closure.work.len(), 2);
        assert_eq!(closure.work[0].producer, legacy(10));
        assert_eq!(closure.work[0].dirty, ProducerDirtyDomain::Whole);
        assert_eq!(closure.work[1].producer, span(1));
        assert_eq!(
            closure.work[1].dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 50, 1)])
        );
        assert!(
            closure
                .changed_result_regions
                .contains(&Region::point(0, 50, 1))
        );
        assert!(
            closure
                .changed_result_regions
                .contains(&Region::point(0, 0, 3))
        );
    }

    #[test]
    fn dirty_closure_filters_no_intersection_candidates() {
        let mut index = FormulaConsumerReadIndex::default();
        let result = Region::col_interval(0, 1, 0, 9);
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };
        // Deliberately over-broad read region to prove closure honors the
        // projection result rather than treating index candidates as dirty work.
        index.insert_read(span(1), Region::whole_sheet(0), result, projection);

        let closure = compute_dirty_closure(&index, [Region::point(0, 50, 25)], |producer| {
            (producer == span(1)).then_some(result)
        });

        assert!(closure.work.is_empty());
        assert!(closure.changed_result_regions.is_empty());
        assert_eq!(closure.stats.projection_no_intersection_count, 1);
        assert_eq!(closure.stats.projection_exact_count, 0);
    }

    #[test]
    fn dirty_closure_records_unsupported_projection_without_silent_dirty() {
        let mut index = FormulaConsumerReadIndex::default();
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: 0 },
        };
        index.insert_read(
            span(1),
            Region::whole_sheet(0),
            Region::whole_sheet(0),
            projection,
        );

        let closure = compute_dirty_closure(&index, [Region::point(0, 5, 5)], |_| {
            Some(Region::whole_sheet(0))
        });

        assert!(closure.work.is_empty());
        assert_eq!(closure.stats.projection_unsupported_count, 1);
        assert_eq!(closure.fallbacks.len(), 1);
        assert_eq!(closure.fallbacks[0].consumer, span(1));
        assert_eq!(
            closure.fallbacks[0].reason,
            ProjectionFallbackReason::UnboundedResultRegion
        );
    }

    #[test]
    fn dirty_closure_whole_col_changed_region_is_producer_bounded_not_value_bounded() {
        let mut index = FormulaConsumerReadIndex::default();
        let legacy_result = Region::point(0, 0, 3);
        index.insert_read(
            legacy(10),
            Region::whole_col(0, 1),
            legacy_result,
            DirtyProjectionRule::WholeResult,
        );

        let closure = compute_dirty_closure(&index, [Region::whole_col(0, 1)], |producer| {
            (producer == legacy(10)).then_some(legacy_result)
        });

        assert_eq!(closure.work.len(), 1);
        assert_eq!(closure.work[0].dirty, ProducerDirtyDomain::Whole);
        assert_eq!(closure.changed_result_regions, vec![legacy_result]);
        assert_eq!(closure.stats.read_index_candidate_count, 1);
        assert_eq!(closure.stats.emitted_changed_regions, 1);
    }

    #[test]
    fn dirty_closure_multi_precedent_summary_merges_sparse_cells_without_widening() {
        let mut index = FormulaConsumerReadIndex::default();
        let result = Region::row_interval(0, 5, 0, 9);
        let near_projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: 10 },
        };
        let far_projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: 20 },
        };
        index.insert_read(
            span(1),
            near_projection.read_region_for_result(0, result).unwrap(),
            result,
            near_projection,
        );
        index.insert_read(
            span(1),
            far_projection.read_region_for_result(0, result).unwrap(),
            result,
            far_projection,
        );

        let closure = compute_dirty_closure(
            &index,
            [Region::point(0, 5, 15), Region::point(0, 5, 26)],
            |producer| (producer == span(1)).then_some(result),
        );

        assert_eq!(closure.fallbacks, Vec::new());
        assert_eq!(closure.work.len(), 1);
        assert_eq!(
            closure.work[0].dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 5), RegionKey::new(0, 5, 6)])
        );
    }

    #[test]
    fn dirty_closure_dedups_fixed_point_regions() {
        let mut index = FormulaConsumerReadIndex::default();
        let result = Region::point(0, 0, 1);
        index.insert_read(
            span(1),
            Region::point(0, 0, 0),
            result,
            DirtyProjectionRule::WholeResult,
        );

        let closure = compute_dirty_closure(
            &index,
            [Region::point(0, 0, 0), Region::point(0, 0, 0)],
            |producer| (producer == span(1)).then_some(result),
        );

        assert_eq!(closure.work.len(), 1);
        assert_eq!(closure.changed_result_regions, vec![result]);
        assert_eq!(closure.stats.duplicate_changed_regions_skipped, 1);
    }

    #[test]
    fn dirty_closure_propagates_from_changed_region() {
        let mut index = FormulaConsumerReadIndex::default();
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: -10 },
            col: AxisProjection::Relative { offset: 0 },
        };
        let result = Region::whole_sheet(1);
        index.insert_read(span(1), Region::rows_from(0, 20), result, projection);

        let closure = compute_dirty_closure(&index, [Region::rows_from(0, 20)], |producer| {
            (producer == span(1)).then_some(result)
        });

        let expected = Region::rows_from(1, 30);
        assert_eq!(closure.fallbacks, Vec::new());
        assert_eq!(closure.work.len(), 1);
        assert_eq!(
            closure.work[0].dirty,
            ProducerDirtyDomain::Regions(vec![expected])
        );
        assert_eq!(closure.changed_result_regions, vec![expected]);
    }

    #[test]
    fn from_projection_no_overflow_in_dirty_closure() {
        let mut index = FormulaConsumerReadIndex::default();
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: -100 },
            col: AxisProjection::Relative { offset: 0 },
        };
        let result = Region::whole_sheet(1);
        let changed = Region::rows_from(0, u32::MAX - 10);
        index.insert_read(span(1), changed, result, projection);

        let closure = compute_dirty_closure(&index, [changed], |producer| {
            (producer == span(1)).then_some(result)
        });

        let expected = Region::rows_from(1, u32::MAX);
        assert_eq!(closure.fallbacks, Vec::new());
        assert_eq!(closure.work.len(), 1);
        assert_eq!(
            closure.work[0].dirty,
            ProducerDirtyDomain::Regions(vec![expected])
        );
        assert_eq!(closure.changed_result_regions, vec![expected]);
    }

    #[test]
    fn compute_dirty_closure_handles_unbounded_changed() {
        let mut index = FormulaConsumerReadIndex::default();
        let first_projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: -3 },
            col: AxisProjection::Relative { offset: 0 },
        };
        let second_projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: 0 },
        };
        let first_result = Region::whole_sheet(1);
        let second_result = Region::whole_sheet(2);
        index.insert_read(
            span(1),
            Region::rows_from(0, 12),
            first_result,
            first_projection,
        );
        index.insert_read(
            span(2),
            Region::rows_from(1, 15),
            second_result,
            second_projection,
        );

        let closure =
            compute_dirty_closure(
                &index,
                [Region::rows_from(0, 12)],
                |producer| match producer {
                    producer if producer == span(1) => Some(first_result),
                    producer if producer == span(2) => Some(second_result),
                    _ => None,
                },
            );

        assert_eq!(closure.fallbacks, Vec::new());
        assert_eq!(closure.work.len(), 2);
        assert_eq!(
            closure.work[0].dirty,
            ProducerDirtyDomain::Regions(vec![Region::rows_from(1, 15)])
        );
        assert_eq!(
            closure.work[1].dirty,
            ProducerDirtyDomain::Regions(vec![Region::rows_from(2, 15)])
        );
        assert_eq!(
            closure.changed_result_regions,
            vec![Region::rows_from(1, 15), Region::rows_from(2, 15)]
        );
    }

    #[test]
    fn dirty_projection_rule_handles_to_axis_range() {
        let projection = AxisProjection::Relative { offset: -4 };
        let projected = projection
            .project_changed_axis(AxisRange::To(10), AxisRange::All)
            .unwrap();
        assert_eq!(projected, Some(AxisRange::To(14)));

        let projection = AxisProjection::Relative { offset: 4 };
        let projected = projection
            .project_changed_axis(AxisRange::To(3), AxisRange::All)
            .unwrap();
        assert_eq!(projected, None);
    }

    #[test]
    fn dirty_closure_affine_projection_no_under_return_bruteforce_small_grid() {
        for row_offset in -2..=2 {
            for col_offset in -2..=2 {
                let projection = DirtyProjectionRule::AffineCell {
                    row: AxisProjection::Relative { offset: row_offset },
                    col: AxisProjection::Relative { offset: col_offset },
                };
                let result = Region::rect(0, 3, 6, 3, 6);
                let read = projection.read_region_for_result(0, result).unwrap();

                for source_row in 1..=8 {
                    for source_col in 1..=8 {
                        let dirty = projection.project_changed_region(
                            Region::point(0, source_row, source_col),
                            read,
                            result,
                        );
                        let expected_row = i64::from(source_row) - row_offset;
                        let expected_col = i64::from(source_col) - col_offset;
                        let in_result =
                            (3..=6).contains(&expected_row) && (3..=6).contains(&expected_col);

                        if in_result {
                            assert_eq!(
                                dirty,
                                ProjectionResult::Exact(ProducerDirtyDomain::Cells(vec![
                                    RegionKey::new(0, expected_row as u32, expected_col as u32),
                                ])),
                                "offset=({row_offset},{col_offset}) source=({source_row},{source_col})"
                            );
                        } else {
                            assert_eq!(
                                dirty,
                                ProjectionResult::NoIntersection,
                                "offset=({row_offset},{col_offset}) source=({source_row},{source_col})"
                            );
                        }
                    }
                }
            }
        }
    }
}
