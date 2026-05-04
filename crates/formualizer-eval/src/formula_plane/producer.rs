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

use super::dependency_summary::{FormulaClass, FormulaDependencySummary, PrecedentPattern};
use super::region_index::{
    RegionKey, RegionMatch, RegionPattern, RegionQueryResult, SheetRegionIndex,
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
    Regions(Vec<RegionPattern>),
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
                    .map(RegionPattern::Point)
                    .collect::<Vec<_>>();
                append_unique(&mut regions, incoming_regions);
                *self = Self::Regions(regions);
            }
            (Self::Regions(existing_regions), Self::Cells(incoming_cells)) => {
                append_unique(
                    existing_regions,
                    incoming_cells.into_iter().map(RegionPattern::Point),
                );
            }
        }
    }

    pub(crate) fn result_regions(
        &self,
        producer_result_region: RegionPattern,
    ) -> Vec<RegionPattern> {
        match self {
            Self::Whole => vec![producer_result_region],
            Self::Cells(cells) => cells.iter().copied().map(RegionPattern::Point).collect(),
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
    pub(crate) result_region: RegionPattern,
}

#[derive(Debug, Default)]
pub(crate) struct FormulaProducerResultIndex {
    index: SheetRegionIndex<FormulaProducerResultEntryId>,
    entries: Vec<FormulaProducerResultEntry>,
    by_producer: FxHashMap<FormulaProducerId, RegionPattern>,
    epoch: u64,
}

impl FormulaProducerResultIndex {
    pub(crate) fn insert_producer(
        &mut self,
        producer: FormulaProducerId,
        result_region: RegionPattern,
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
        read_region: RegionPattern,
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

    pub(crate) fn producer_result_region(
        &self,
        producer: FormulaProducerId,
    ) -> Option<RegionPattern> {
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
    pub(crate) read_region: RegionPattern,
    pub(crate) consumer_result_region: RegionPattern,
    pub(crate) projection: DirtyProjectionRule,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaConsumerDirtyCandidate {
    pub(crate) consumer: FormulaProducerId,
    pub(crate) read_region: RegionPattern,
    pub(crate) consumer_result_region: RegionPattern,
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
        read_region: RegionPattern,
        consumer_result_region: RegionPattern,
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
        changed: RegionPattern,
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
    pub(crate) result_region: RegionPattern,
    pub(crate) dependencies: Vec<SpanReadDependency>,
}

impl SpanReadSummary {
    pub(crate) fn from_formula_summary(
        sheet_id: SheetId,
        result_region: &ResultRegion,
        summary: &FormulaDependencySummary,
    ) -> Result<Self, ProjectionFallbackReason> {
        if summary.formula_class != FormulaClass::StaticPointwise
            || !summary.reject_reasons.is_empty()
        {
            return Err(ProjectionFallbackReason::UnsupportedDependencySummary);
        }

        let result_region_pattern = RegionPattern::from_domain(result_region.domain());
        let mut dependencies = Vec::new();
        for precedent in &summary.precedent_patterns {
            match precedent {
                PrecedentPattern::Cell(cell) => {
                    if cell.sheet != SheetBinding::CurrentSheet {
                        return Err(ProjectionFallbackReason::UnsupportedSheetBinding);
                    }
                    let projection = DirtyProjectionRule::AffineCell {
                        row: AxisProjection::from_axis_ref(&cell.row)?,
                        col: AxisProjection::from_axis_ref(&cell.col)?,
                    };
                    let read_region =
                        projection.read_region_for_result(sheet_id, result_region_pattern)?;
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

        Ok(Self {
            result_region: result_region_pattern,
            dependencies,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpanReadDependency {
    pub(crate) read_region: RegionPattern,
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
pub(crate) enum DirtyProjectionRule {
    AffineCell {
        row: AxisProjection,
        col: AxisProjection,
    },
    WholeResult,
}

impl DirtyProjectionRule {
    pub(crate) fn read_region_for_result(
        self,
        sheet_id: SheetId,
        result_region: RegionPattern,
    ) -> Result<RegionPattern, ProjectionFallbackReason> {
        match self {
            Self::WholeResult => Err(ProjectionFallbackReason::RequiresExplicitReadRegion),
            Self::AffineCell { row, col } => {
                let (result_rows, result_cols) = bounded_extents(result_region)
                    .ok_or(ProjectionFallbackReason::UnboundedResultRegion)?;
                let source_rows = row.source_extent_for_result(result_rows)?;
                let source_cols = col.source_extent_for_result(result_cols)?;
                region_from_bounded_extents(sheet_id, source_rows, source_cols)
            }
        }
    }

    pub(crate) fn project_changed_region(
        self,
        changed: RegionPattern,
        read_region: RegionPattern,
        result_region: RegionPattern,
    ) -> ProjectionResult {
        if !changed.intersects(&read_region) {
            return ProjectionResult::NoIntersection;
        }
        if self == Self::WholeResult {
            return ProjectionResult::Exact(ProducerDirtyDomain::Whole);
        }

        let Some((changed_rows, changed_cols)) = query_extents(changed) else {
            return ProjectionResult::Unsupported(
                ProjectionFallbackReason::UnsupportedChangedRegion,
            );
        };
        let Some((result_rows, result_cols)) = bounded_extents(result_region) else {
            return ProjectionResult::Unsupported(ProjectionFallbackReason::UnboundedResultRegion);
        };
        let sheet_id = result_region.sheet_id();

        match self {
            Self::AffineCell { row, col } => {
                let Some(dirty_rows) = row.project_changed_axis(changed_rows, result_rows) else {
                    return ProjectionResult::NoIntersection;
                };
                let Some(dirty_cols) = col.project_changed_axis(changed_cols, result_cols) else {
                    return ProjectionResult::NoIntersection;
                };
                match region_from_bounded_extents(sheet_id, dirty_rows, dirty_cols) {
                    Ok(region) => ProjectionResult::Exact(dirty_domain_from_region(region)),
                    Err(reason) => ProjectionResult::Unsupported(reason),
                }
            }
            Self::WholeResult => unreachable!("handled above"),
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
        result: BoundedAxisExtent,
    ) -> Result<BoundedAxisExtent, ProjectionFallbackReason> {
        match self {
            Self::Relative { offset } => Ok(BoundedAxisExtent::new(
                add_offset(result.start, offset)?,
                add_offset(result.end, offset)?,
            )),
            Self::Absolute { index } => Ok(BoundedAxisExtent::new(index, index)),
        }
    }

    fn project_changed_axis(
        self,
        changed: QueryAxisExtent,
        result: BoundedAxisExtent,
    ) -> Option<BoundedAxisExtent> {
        match self {
            Self::Relative { offset } => {
                let dirty = match changed {
                    QueryAxisExtent::All => result,
                    QueryAxisExtent::Span(start, end) => BoundedAxisExtent::new(
                        add_offset(start, -offset).ok()?,
                        add_offset(end, -offset).ok()?,
                    ),
                };
                dirty.intersect(result)
            }
            Self::Absolute { index } => changed.contains(index).then_some(result),
        }
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
    pub(crate) changed_result_regions: Vec<RegionPattern>,
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
    pub(crate) changed_region: RegionPattern,
    pub(crate) reason: ProjectionFallbackReason,
}

const DIRTY_CLOSURE_ITERATION_LIMIT: usize = 100_000;

pub(crate) fn compute_dirty_closure(
    consumer_reads: &FormulaConsumerReadIndex,
    changed_regions: impl IntoIterator<Item = RegionPattern>,
    result_region: impl Fn(FormulaProducerId) -> Option<RegionPattern>,
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
    changed_region: RegionPattern,
    dirty: ProducerDirtyDomain,
    result_region: &impl Fn(FormulaProducerId) -> Option<RegionPattern>,
    dirty_by_producer: &mut BTreeMap<FormulaProducerId, ProducerDirtyDomain>,
    queue: &mut VecDeque<RegionPattern>,
    seen_changed_regions: &mut FxHashSet<RegionPattern>,
    changed_result_regions: &mut Vec<RegionPattern>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BoundedAxisExtent {
    start: u32,
    end: u32,
}

impl BoundedAxisExtent {
    fn new(start: u32, end: u32) -> Self {
        debug_assert!(start <= end);
        Self { start, end }
    }

    fn is_point(self) -> bool {
        self.start == self.end
    }

    fn intersect(self, other: Self) -> Option<Self> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        (start <= end).then_some(Self { start, end })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryAxisExtent {
    Span(u32, u32),
    All,
}

impl QueryAxisExtent {
    fn contains(self, coord: u32) -> bool {
        match self {
            Self::Span(start, end) => coord >= start && coord <= end,
            Self::All => true,
        }
    }
}

fn bounded_extents(pattern: RegionPattern) -> Option<(BoundedAxisExtent, BoundedAxisExtent)> {
    match pattern {
        RegionPattern::Point(key) => Some((
            BoundedAxisExtent::new(key.row, key.row),
            BoundedAxisExtent::new(key.col, key.col),
        )),
        RegionPattern::ColInterval {
            row_start,
            row_end,
            col,
            ..
        } => Some((
            BoundedAxisExtent::new(row_start, row_end),
            BoundedAxisExtent::new(col, col),
        )),
        RegionPattern::RowInterval {
            row,
            col_start,
            col_end,
            ..
        } => Some((
            BoundedAxisExtent::new(row, row),
            BoundedAxisExtent::new(col_start, col_end),
        )),
        RegionPattern::Rect(rect) => Some((
            BoundedAxisExtent::new(rect.row_start, rect.row_end),
            BoundedAxisExtent::new(rect.col_start, rect.col_end),
        )),
        RegionPattern::WholeRow { .. }
        | RegionPattern::WholeCol { .. }
        | RegionPattern::WholeSheet { .. } => None,
    }
}

fn query_extents(pattern: RegionPattern) -> Option<(QueryAxisExtent, QueryAxisExtent)> {
    match pattern {
        RegionPattern::Point(key) => Some((
            QueryAxisExtent::Span(key.row, key.row),
            QueryAxisExtent::Span(key.col, key.col),
        )),
        RegionPattern::ColInterval {
            row_start,
            row_end,
            col,
            ..
        } => Some((
            QueryAxisExtent::Span(row_start, row_end),
            QueryAxisExtent::Span(col, col),
        )),
        RegionPattern::RowInterval {
            row,
            col_start,
            col_end,
            ..
        } => Some((
            QueryAxisExtent::Span(row, row),
            QueryAxisExtent::Span(col_start, col_end),
        )),
        RegionPattern::Rect(rect) => Some((
            QueryAxisExtent::Span(rect.row_start, rect.row_end),
            QueryAxisExtent::Span(rect.col_start, rect.col_end),
        )),
        RegionPattern::WholeRow { row, .. } => {
            Some((QueryAxisExtent::Span(row, row), QueryAxisExtent::All))
        }
        RegionPattern::WholeCol { col, .. } => {
            Some((QueryAxisExtent::All, QueryAxisExtent::Span(col, col)))
        }
        RegionPattern::WholeSheet { .. } => Some((QueryAxisExtent::All, QueryAxisExtent::All)),
    }
}

fn region_from_bounded_extents(
    sheet_id: SheetId,
    rows: BoundedAxisExtent,
    cols: BoundedAxisExtent,
) -> Result<RegionPattern, ProjectionFallbackReason> {
    Ok(match (rows.is_point(), cols.is_point()) {
        (true, true) => RegionPattern::point(sheet_id, rows.start, cols.start),
        (false, true) => RegionPattern::col_interval(sheet_id, cols.start, rows.start, rows.end),
        (true, false) => RegionPattern::row_interval(sheet_id, rows.start, cols.start, cols.end),
        (false, false) => RegionPattern::rect(sheet_id, rows.start, rows.end, cols.start, cols.end),
    })
}

fn dirty_domain_from_region(region: RegionPattern) -> ProducerDirtyDomain {
    match region {
        RegionPattern::Point(key) => ProducerDirtyDomain::Cells(vec![key]),
        other => ProducerDirtyDomain::Regions(vec![other]),
    }
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
    use super::super::runtime::FormulaSpanId;
    use super::*;

    fn span(id: u32) -> FormulaProducerId {
        FormulaProducerId::Span(FormulaSpanId(id))
    }

    fn legacy(id: u32) -> FormulaProducerId {
        FormulaProducerId::Legacy(VertexId(id))
    }

    #[test]
    fn producer_result_index_finds_legacy_and_span_producers() {
        let mut index = FormulaProducerResultIndex::default();
        index.insert_producer(legacy(1), RegionPattern::point(0, 9, 2));
        index.insert_producer(span(2), RegionPattern::col_interval(0, 1, 0, 99));

        let point = index.query(RegionPattern::point(0, 9, 2));
        assert!(point.matches.iter().any(|m| m.value.producer == legacy(1)));

        let span_hit = index.query(RegionPattern::point(0, 50, 1));
        assert_eq!(span_hit.matches.len(), 1);
        assert_eq!(span_hit.matches[0].value.producer, span(2));
    }

    #[test]
    fn consumer_read_index_projects_same_row_dirty_cell() {
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };
        let result = RegionPattern::col_interval(0, 2, 0, 99);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, RegionPattern::col_interval(0, 1, 0, 99));

        let mut index = FormulaConsumerReadIndex::default();
        index.insert_read(span(1), read, result, projection);
        let dirty = index.query_changed_region(RegionPattern::point(0, 50, 1));
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
        let result = RegionPattern::col_interval(0, 2, 0, 99);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, RegionPattern::col_interval(0, 1, 1, 100));

        let dirty = projection.project_changed_region(RegionPattern::point(0, 50, 1), read, result);
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
        let result = RegionPattern::col_interval(0, 2, 0, 99);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, RegionPattern::point(0, 0, 0));

        let dirty = projection.project_changed_region(RegionPattern::point(0, 0, 0), read, result);
        assert_eq!(
            dirty,
            ProjectionResult::Exact(ProducerDirtyDomain::Regions(vec![result]))
        );
    }

    #[test]
    fn mixed_absolute_relative_axis_projects_to_result_rect() {
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Absolute { index: 0 },
        };
        let result = RegionPattern::rect(0, 0, 9, 2, 4);
        let read = projection.read_region_for_result(0, result).unwrap();
        assert_eq!(read, RegionPattern::col_interval(0, 0, 0, 9));

        let dirty = projection.project_changed_region(
            RegionPattern::col_interval(0, 0, 5, 6),
            read,
            result,
        );
        assert_eq!(
            dirty,
            ProjectionResult::Exact(ProducerDirtyDomain::Regions(vec![RegionPattern::rect(
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
        let result = RegionPattern::col_interval(0, 2, 0, 9);

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
        let read = RegionPattern::col_interval(0, 1, 0, 99);
        let result = RegionPattern::point(0, 0, 3);

        assert_eq!(
            projection.read_region_for_result(0, result),
            Err(ProjectionFallbackReason::RequiresExplicitReadRegion)
        );
        assert_eq!(
            projection.project_changed_region(RegionPattern::point(0, 50, 1), read, result),
            ProjectionResult::Exact(ProducerDirtyDomain::Whole)
        );
        assert_eq!(
            projection.project_changed_region(RegionPattern::point(0, 50, 2), read, result),
            ProjectionResult::NoIntersection
        );
    }

    #[test]
    fn dirty_closure_same_row_source_dirties_single_span_cell() {
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };
        let result = RegionPattern::col_interval(0, 1, 0, 9);
        let read = projection.read_region_for_result(0, result).unwrap();
        let mut index = FormulaConsumerReadIndex::default();
        index.insert_read(span(1), read, result, projection);

        let closure = compute_dirty_closure(&index, [RegionPattern::point(0, 5, 0)], |producer| {
            (producer == span(1)).then_some(result)
        });

        assert_eq!(closure.fallbacks, Vec::new());
        assert_eq!(closure.work.len(), 1);
        assert_eq!(closure.work[0].producer, span(1));
        assert_eq!(
            closure.work[0].dirty,
            ProducerDirtyDomain::Cells(vec![RegionKey::new(0, 5, 1)])
        );
        assert_eq!(
            closure.changed_result_regions,
            vec![RegionPattern::point(0, 5, 1)]
        );
    }

    #[test]
    fn dirty_closure_composes_span_to_span_single_cell() {
        let mut index = FormulaConsumerReadIndex::default();
        let b_result = RegionPattern::col_interval(0, 1, 0, 9);
        let c_result = RegionPattern::col_interval(0, 2, 0, 9);
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
                [RegionPattern::point(0, 5, 0)],
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
            vec![RegionPattern::point(0, 5, 1), RegionPattern::point(0, 5, 2)]
        );
    }

    #[test]
    fn dirty_closure_reaches_legacy_range_consumer_after_span_cell_dirty() {
        let mut index = FormulaConsumerReadIndex::default();
        let b_result = RegionPattern::col_interval(0, 1, 0, 99);
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
        let legacy_result = RegionPattern::point(0, 0, 3);
        index.insert_read(
            legacy(10),
            b_result,
            legacy_result,
            DirtyProjectionRule::WholeResult,
        );

        let closure = compute_dirty_closure(&index, [RegionPattern::point(0, 50, 0)], |producer| {
            match producer {
                producer if producer == span(1) => Some(b_result),
                producer if producer == legacy(10) => Some(legacy_result),
                _ => None,
            }
        });

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
                .contains(&RegionPattern::point(0, 50, 1))
        );
        assert!(
            closure
                .changed_result_regions
                .contains(&RegionPattern::point(0, 0, 3))
        );
    }

    #[test]
    fn dirty_closure_filters_no_intersection_candidates() {
        let mut index = FormulaConsumerReadIndex::default();
        let result = RegionPattern::col_interval(0, 1, 0, 9);
        let projection = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 0 },
            col: AxisProjection::Relative { offset: -1 },
        };
        // Deliberately over-broad read region to prove closure honors the
        // projection result rather than treating index candidates as dirty work.
        index.insert_read(
            span(1),
            RegionPattern::WholeSheet { sheet_id: 0 },
            result,
            projection,
        );

        let closure =
            compute_dirty_closure(&index, [RegionPattern::point(0, 50, 25)], |producer| {
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
            RegionPattern::WholeSheet { sheet_id: 0 },
            RegionPattern::WholeSheet { sheet_id: 0 },
            projection,
        );

        let closure = compute_dirty_closure(&index, [RegionPattern::point(0, 5, 5)], |_| {
            Some(RegionPattern::WholeSheet { sheet_id: 0 })
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
        let legacy_result = RegionPattern::point(0, 0, 3);
        index.insert_read(
            legacy(10),
            RegionPattern::WholeCol {
                sheet_id: 0,
                col: 1,
            },
            legacy_result,
            DirtyProjectionRule::WholeResult,
        );

        let closure = compute_dirty_closure(
            &index,
            [RegionPattern::WholeCol {
                sheet_id: 0,
                col: 1,
            }],
            |producer| (producer == legacy(10)).then_some(legacy_result),
        );

        assert_eq!(closure.work.len(), 1);
        assert_eq!(closure.work[0].dirty, ProducerDirtyDomain::Whole);
        assert_eq!(closure.changed_result_regions, vec![legacy_result]);
        assert_eq!(closure.stats.read_index_candidate_count, 1);
        assert_eq!(closure.stats.emitted_changed_regions, 1);
    }

    #[test]
    fn dirty_closure_multi_precedent_summary_merges_sparse_cells_without_widening() {
        let mut index = FormulaConsumerReadIndex::default();
        let result = RegionPattern::row_interval(0, 5, 0, 9);
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
            [
                RegionPattern::point(0, 5, 15),
                RegionPattern::point(0, 5, 26),
            ],
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
        let result = RegionPattern::point(0, 0, 1);
        index.insert_read(
            span(1),
            RegionPattern::point(0, 0, 0),
            result,
            DirtyProjectionRule::WholeResult,
        );

        let closure = compute_dirty_closure(
            &index,
            [RegionPattern::point(0, 0, 0), RegionPattern::point(0, 0, 0)],
            |producer| (producer == span(1)).then_some(result),
        );

        assert_eq!(closure.work.len(), 1);
        assert_eq!(closure.changed_result_regions, vec![result]);
        assert_eq!(closure.stats.duplicate_changed_regions_skipped, 1);
    }

    #[test]
    fn dirty_closure_affine_projection_no_under_return_bruteforce_small_grid() {
        for row_offset in -2..=2 {
            for col_offset in -2..=2 {
                let projection = DirtyProjectionRule::AffineCell {
                    row: AxisProjection::Relative { offset: row_offset },
                    col: AxisProjection::Relative { offset: col_offset },
                };
                let result = RegionPattern::rect(0, 3, 6, 3, 6);
                let read = projection.read_region_for_result(0, result).unwrap();

                for source_row in 1..=8 {
                    for source_col in 1..=8 {
                        let dirty = projection.project_changed_region(
                            RegionPattern::point(0, source_row, source_col),
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
