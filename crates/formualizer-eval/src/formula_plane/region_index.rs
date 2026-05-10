//! Inert FormulaPlane sidecar region indexes for FP6.3.
//!
//! This module is internal FormulaPlane substrate only. It does not wire dirty
//! routing into the engine, graph, scheduler, or evaluator.

use std::collections::BTreeMap;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::SheetId;
use crate::engine::interval_tree::IntervalTree;

use super::runtime::{FormulaOverlayRef, FormulaSpanRef, PlacementCoord, PlacementDomain};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RegionKey {
    pub(crate) sheet_id: SheetId,
    pub(crate) row: u32,
    pub(crate) col: u32,
}

impl RegionKey {
    pub(crate) fn new(sheet_id: SheetId, row: u32, col: u32) -> Self {
        Self { sheet_id, row, col }
    }
}

impl From<PlacementCoord> for RegionKey {
    fn from(coord: PlacementCoord) -> Self {
        Self::new(coord.sheet_id, coord.row, coord.col)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RectRegion {
    pub(crate) sheet_id: SheetId,
    pub(crate) row_start: u32,
    pub(crate) row_end: u32,
    pub(crate) col_start: u32,
    pub(crate) col_end: u32,
}

impl RectRegion {
    pub(crate) fn new(
        sheet_id: SheetId,
        row_start: u32,
        row_end: u32,
        col_start: u32,
        col_end: u32,
    ) -> Self {
        assert!(row_start <= row_end, "row_start must be <= row_end");
        assert!(col_start <= col_end, "col_start must be <= col_end");
        Self {
            sheet_id,
            row_start,
            row_end,
            col_start,
            col_end,
        }
    }

    pub(crate) fn point(key: RegionKey) -> Self {
        Self::new(key.sheet_id, key.row, key.row, key.col, key.col)
    }

    pub(crate) fn contains_key(&self, key: RegionKey) -> bool {
        self.sheet_id == key.sheet_id
            && key.row >= self.row_start
            && key.row <= self.row_end
            && key.col >= self.col_start
            && key.col <= self.col_end
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RegionPattern {
    Point(RegionKey),
    ColInterval {
        sheet_id: SheetId,
        col: u32,
        row_start: u32,
        row_end: u32,
    },
    RowInterval {
        sheet_id: SheetId,
        row: u32,
        col_start: u32,
        col_end: u32,
    },
    Rect(RectRegion),
    RowsFrom {
        sheet_id: SheetId,
        row_start: u32,
    },
    ColsFrom {
        sheet_id: SheetId,
        col_start: u32,
    },
    WholeRow {
        sheet_id: SheetId,
        row: u32,
    },
    WholeCol {
        sheet_id: SheetId,
        col: u32,
    },
    WholeSheet {
        sheet_id: SheetId,
    },
}

impl RegionPattern {
    pub(crate) fn point(sheet_id: SheetId, row: u32, col: u32) -> Self {
        Self::Point(RegionKey::new(sheet_id, row, col))
    }

    pub(crate) fn col_interval(sheet_id: SheetId, col: u32, row_start: u32, row_end: u32) -> Self {
        assert!(row_start <= row_end, "row_start must be <= row_end");
        Self::ColInterval {
            sheet_id,
            col,
            row_start,
            row_end,
        }
    }

    pub(crate) fn row_interval(sheet_id: SheetId, row: u32, col_start: u32, col_end: u32) -> Self {
        assert!(col_start <= col_end, "col_start must be <= col_end");
        Self::RowInterval {
            sheet_id,
            row,
            col_start,
            col_end,
        }
    }

    pub(crate) fn rect(
        sheet_id: SheetId,
        row_start: u32,
        row_end: u32,
        col_start: u32,
        col_end: u32,
    ) -> Self {
        Self::Rect(RectRegion::new(
            sheet_id, row_start, row_end, col_start, col_end,
        ))
    }

    pub(crate) fn rows_from(sheet_id: SheetId, row_start: u32) -> Self {
        Self::RowsFrom {
            sheet_id,
            row_start,
        }
    }

    pub(crate) fn cols_from(sheet_id: SheetId, col_start: u32) -> Self {
        Self::ColsFrom {
            sheet_id,
            col_start,
        }
    }

    pub(crate) fn whole_row(sheet_id: SheetId, row: u32) -> Self {
        Self::WholeRow { sheet_id, row }
    }

    pub(crate) fn whole_col(sheet_id: SheetId, col: u32) -> Self {
        Self::WholeCol { sheet_id, col }
    }

    pub(crate) fn whole_sheet(sheet_id: SheetId) -> Self {
        Self::WholeSheet { sheet_id }
    }

    pub(crate) fn from_domain(domain: &PlacementDomain) -> Self {
        match domain {
            PlacementDomain::RowRun {
                sheet_id,
                row_start,
                row_end,
                col,
            } => Self::col_interval(*sheet_id, *col, *row_start, *row_end),
            PlacementDomain::ColRun {
                sheet_id,
                row,
                col_start,
                col_end,
            } => Self::row_interval(*sheet_id, *row, *col_start, *col_end),
            PlacementDomain::Rect {
                sheet_id,
                row_start,
                row_end,
                col_start,
                col_end,
            } => Self::rect(*sheet_id, *row_start, *row_end, *col_start, *col_end),
        }
    }

    pub(crate) fn sheet_id(&self) -> SheetId {
        match self {
            Self::Point(key) => key.sheet_id,
            Self::ColInterval { sheet_id, .. }
            | Self::RowInterval { sheet_id, .. }
            | Self::Rect(RectRegion { sheet_id, .. })
            | Self::RowsFrom { sheet_id, .. }
            | Self::ColsFrom { sheet_id, .. }
            | Self::WholeRow { sheet_id, .. }
            | Self::WholeCol { sheet_id, .. }
            | Self::WholeSheet { sheet_id } => *sheet_id,
        }
    }

    pub(crate) fn intersects(&self, other: &Self) -> bool {
        if self.sheet_id() != other.sheet_id() {
            return false;
        }
        let (self_rows, self_cols) = self.axis_extents();
        let (other_rows, other_cols) = other.axis_extents();
        self_rows.intersects(other_rows) && self_cols.intersects(other_cols)
    }

    pub(crate) fn contains_key(&self, key: RegionKey) -> bool {
        self.intersects(&Self::Point(key))
    }

    fn axis_extents(&self) -> (AxisExtent, AxisExtent) {
        match *self {
            Self::Point(key) => (
                AxisExtent::Span(key.row, key.row),
                AxisExtent::Span(key.col, key.col),
            ),
            Self::ColInterval {
                row_start,
                row_end,
                col,
                ..
            } => (
                AxisExtent::Span(row_start, row_end),
                AxisExtent::Span(col, col),
            ),
            Self::RowInterval {
                row,
                col_start,
                col_end,
                ..
            } => (
                AxisExtent::Span(row, row),
                AxisExtent::Span(col_start, col_end),
            ),
            Self::Rect(rect) => (
                AxisExtent::Span(rect.row_start, rect.row_end),
                AxisExtent::Span(rect.col_start, rect.col_end),
            ),
            Self::RowsFrom { row_start, .. } => (AxisExtent::From(row_start), AxisExtent::All),
            Self::ColsFrom { col_start, .. } => (AxisExtent::All, AxisExtent::From(col_start)),
            Self::WholeRow { row, .. } => (AxisExtent::Span(row, row), AxisExtent::All),
            Self::WholeCol { col, .. } => (AxisExtent::All, AxisExtent::Span(col, col)),
            Self::WholeSheet { .. } => (AxisExtent::All, AxisExtent::All),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AxisExtent {
    Span(u32, u32),
    From(u32),
    All,
}

impl AxisExtent {
    fn contains(self, coord: u32) -> bool {
        match self {
            Self::Span(start, end) => coord >= start && coord <= end,
            Self::From(start) => coord >= start,
            Self::All => true,
        }
    }

    fn intersects(self, other: Self) -> bool {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => true,
            (Self::From(_), Self::From(_)) => true,
            (Self::From(start), Self::Span(_, other_end))
            | (Self::Span(_, other_end), Self::From(start)) => other_end >= start,
            (Self::Span(a_start, a_end), Self::Span(b_start, b_end)) => {
                a_start <= b_end && b_start <= a_end
            }
        }
    }

    fn query_bounds(self) -> (u32, u32) {
        match self {
            Self::Span(start, end) => (start, end),
            Self::From(start) => (start, u32::MAX),
            Self::All => (0, u32::MAX),
        }
    }

    fn query_max(self) -> u32 {
        match self {
            Self::Span(_, end) => end,
            Self::From(_) | Self::All => u32::MAX,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RegionSet {
    One(RegionPattern),
    Many(Vec<RegionPattern>),
}

impl RegionSet {
    pub(crate) fn patterns(&self) -> &[RegionPattern] {
        match self {
            Self::One(pattern) => std::slice::from_ref(pattern),
            Self::Many(patterns) => patterns.as_slice(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RegionQueryStats {
    pub(crate) candidate_count: usize,
    pub(crate) exact_filter_drop_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegionMatch<T> {
    pub(crate) value: T,
    pub(crate) indexed_region: RegionPattern,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegionQueryResult<T> {
    pub(crate) matches: Vec<RegionMatch<T>>,
    pub(crate) stats: RegionQueryStats,
}

#[derive(Clone, Debug)]
struct RegionEntry<T> {
    value: T,
    region: RegionPattern,
}

#[derive(Debug)]
pub(crate) struct SheetRegionIndex<T: Clone> {
    entries: Vec<RegionEntry<T>>,
    points: FxHashMap<RegionKey, Vec<usize>>,
    col_intervals: FxHashMap<(SheetId, u32), IntervalTree<usize>>,
    row_intervals: FxHashMap<(SheetId, u32), IntervalTree<usize>>,
    rect_buckets: FxHashMap<(SheetId, u32, u32), Vec<usize>>,
    rows_from: FxHashMap<SheetId, BTreeMap<u32, Vec<usize>>>,
    cols_from: FxHashMap<SheetId, BTreeMap<u32, Vec<usize>>>,
    whole_rows: FxHashMap<(SheetId, u32), Vec<usize>>,
    whole_cols: FxHashMap<(SheetId, u32), Vec<usize>>,
    whole_sheets: FxHashMap<SheetId, Vec<usize>>,
    rect_bucket_rows: u32,
    rect_bucket_cols: u32,
    epoch: u64,
    rebuild_count: u64,
    stale_epoch_count: u64,
}

impl<T: Clone> Default for SheetRegionIndex<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> SheetRegionIndex<T> {
    const DEFAULT_RECT_BUCKET_ROWS: u32 = 64;
    const DEFAULT_RECT_BUCKET_COLS: u32 = 16;

    pub(crate) fn new() -> Self {
        Self::with_rect_bucket_size(
            Self::DEFAULT_RECT_BUCKET_ROWS,
            Self::DEFAULT_RECT_BUCKET_COLS,
        )
    }

    pub(crate) fn with_rect_bucket_size(rect_bucket_rows: u32, rect_bucket_cols: u32) -> Self {
        assert!(rect_bucket_rows > 0, "rect_bucket_rows must be nonzero");
        assert!(rect_bucket_cols > 0, "rect_bucket_cols must be nonzero");
        Self {
            entries: Vec::new(),
            points: FxHashMap::default(),
            col_intervals: FxHashMap::default(),
            row_intervals: FxHashMap::default(),
            rect_buckets: FxHashMap::default(),
            rows_from: FxHashMap::default(),
            cols_from: FxHashMap::default(),
            whole_rows: FxHashMap::default(),
            whole_cols: FxHashMap::default(),
            whole_sheets: FxHashMap::default(),
            rect_bucket_rows,
            rect_bucket_cols,
            epoch: 0,
            rebuild_count: 0,
            stale_epoch_count: 0,
        }
    }

    pub(crate) fn insert(&mut self, region: RegionPattern, value: T) -> usize {
        let id = self.entries.len();
        self.entries.push(RegionEntry { value, region });
        self.index_entry(id, region);
        self.epoch = self.epoch.saturating_add(1);
        id
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.points.clear();
        self.col_intervals.clear();
        self.row_intervals.clear();
        self.rect_buckets.clear();
        self.rows_from.clear();
        self.cols_from.clear();
        self.whole_rows.clear();
        self.whole_cols.clear();
        self.whole_sheets.clear();
        self.epoch = self.epoch.saturating_add(1);
        self.rebuild_count = self.rebuild_count.saturating_add(1);
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    pub(crate) fn rebuild_count(&self) -> u64 {
        self.rebuild_count
    }

    pub(crate) fn stale_epoch_count(&self) -> u64 {
        self.stale_epoch_count
    }

    pub(crate) fn query(&self, query: RegionPattern) -> RegionQueryResult<T> {
        let mut candidate_ids = FxHashSet::default();
        self.collect_candidates(query, &mut candidate_ids);

        let candidate_count = candidate_ids.len();
        let mut matches = Vec::new();
        for id in candidate_ids {
            let entry = &self.entries[id];
            if entry.region.intersects(&query) {
                matches.push(RegionMatch {
                    value: entry.value.clone(),
                    indexed_region: entry.region,
                });
            }
        }
        let exact_filter_drop_count = candidate_count.saturating_sub(matches.len());

        RegionQueryResult {
            matches,
            stats: RegionQueryStats {
                candidate_count,
                exact_filter_drop_count,
            },
        }
    }

    fn index_entry(&mut self, id: usize, region: RegionPattern) {
        match region {
            RegionPattern::Point(key) => {
                self.points.entry(key).or_default().push(id);
            }
            RegionPattern::ColInterval {
                sheet_id,
                col,
                row_start,
                row_end,
            } => {
                self.col_intervals
                    .entry((sheet_id, col))
                    .or_default()
                    .insert(row_start, row_end, id);
            }
            RegionPattern::RowInterval {
                sheet_id,
                row,
                col_start,
                col_end,
            } => {
                self.row_intervals
                    .entry((sheet_id, row))
                    .or_default()
                    .insert(col_start, col_end, id);
            }
            RegionPattern::Rect(rect) => {
                for bucket in self.rect_buckets_for_rect(rect) {
                    self.rect_buckets.entry(bucket).or_default().push(id);
                }
            }
            RegionPattern::RowsFrom {
                sheet_id,
                row_start,
            } => {
                self.rows_from
                    .entry(sheet_id)
                    .or_default()
                    .entry(row_start)
                    .or_default()
                    .push(id);
            }
            RegionPattern::ColsFrom {
                sheet_id,
                col_start,
            } => {
                self.cols_from
                    .entry(sheet_id)
                    .or_default()
                    .entry(col_start)
                    .or_default()
                    .push(id);
            }
            RegionPattern::WholeRow { sheet_id, row } => {
                self.whole_rows.entry((sheet_id, row)).or_default().push(id);
            }
            RegionPattern::WholeCol { sheet_id, col } => {
                self.whole_cols.entry((sheet_id, col)).or_default().push(id);
            }
            RegionPattern::WholeSheet { sheet_id } => {
                self.whole_sheets.entry(sheet_id).or_default().push(id);
            }
        }
    }

    fn collect_candidates(&self, query: RegionPattern, out: &mut FxHashSet<usize>) {
        self.collect_point_candidates(query, out);
        self.collect_col_interval_candidates(query, out);
        self.collect_row_interval_candidates(query, out);
        self.collect_rect_candidates(query, out);
        self.collect_tail_axis_candidates(query, out);
        self.collect_whole_axis_candidates(query, out);
    }

    fn collect_point_candidates(&self, query: RegionPattern, out: &mut FxHashSet<usize>) {
        let sheet_id = query.sheet_id();
        for (key, ids) in &self.points {
            if key.sheet_id == sheet_id && query.contains_key(*key) {
                out.extend(ids.iter().copied());
            }
        }
    }

    fn collect_col_interval_candidates(&self, query: RegionPattern, out: &mut FxHashSet<usize>) {
        let sheet_id = query.sheet_id();
        let (row_extent, col_extent) = query.axis_extents();
        let (row_start, row_end) = row_extent.query_bounds();
        for (&(entry_sheet, col), tree) in &self.col_intervals {
            if entry_sheet != sheet_id || !col_extent.contains(col) {
                continue;
            }
            for (_low, _high, values) in tree.query(row_start, row_end) {
                out.extend(values.into_iter());
            }
        }
    }

    fn collect_row_interval_candidates(&self, query: RegionPattern, out: &mut FxHashSet<usize>) {
        let sheet_id = query.sheet_id();
        let (row_extent, col_extent) = query.axis_extents();
        let (col_start, col_end) = col_extent.query_bounds();
        for (&(entry_sheet, row), tree) in &self.row_intervals {
            if entry_sheet != sheet_id || !row_extent.contains(row) {
                continue;
            }
            for (_low, _high, values) in tree.query(col_start, col_end) {
                out.extend(values.into_iter());
            }
        }
    }

    fn collect_rect_candidates(&self, query: RegionPattern, out: &mut FxHashSet<usize>) {
        let sheet_id = query.sheet_id();
        let (row_extent, col_extent) = query.axis_extents();
        match (row_extent, col_extent) {
            (AxisExtent::Span(row_start, row_end), AxisExtent::Span(col_start, col_end)) => {
                let rect = RectRegion::new(sheet_id, row_start, row_end, col_start, col_end);
                for bucket in self.rect_buckets_for_rect(rect) {
                    if let Some(ids) = self.rect_buckets.get(&bucket) {
                        out.extend(ids.iter().copied());
                    }
                }
            }
            _ => {
                for (&(entry_sheet, _row_bucket, _col_bucket), ids) in &self.rect_buckets {
                    if entry_sheet == sheet_id {
                        out.extend(ids.iter().copied());
                    }
                }
            }
        }
    }

    fn collect_tail_axis_candidates(&self, query: RegionPattern, out: &mut FxHashSet<usize>) {
        let sheet_id = query.sheet_id();
        let (row_extent, col_extent) = query.axis_extents();

        if let Some(rows_from) = self.rows_from.get(&sheet_id) {
            for (_row_start, ids) in rows_from.range(..=row_extent.query_max()) {
                out.extend(ids.iter().copied());
            }
        }

        if let Some(cols_from) = self.cols_from.get(&sheet_id) {
            for (_col_start, ids) in cols_from.range(..=col_extent.query_max()) {
                out.extend(ids.iter().copied());
            }
        }
    }

    fn collect_whole_axis_candidates(&self, query: RegionPattern, out: &mut FxHashSet<usize>) {
        let sheet_id = query.sheet_id();
        let (row_extent, col_extent) = query.axis_extents();

        if let Some(ids) = self.whole_sheets.get(&sheet_id) {
            out.extend(ids.iter().copied());
        }

        for (&(entry_sheet, row), ids) in &self.whole_rows {
            if entry_sheet == sheet_id && row_extent.contains(row) {
                out.extend(ids.iter().copied());
            }
        }

        for (&(entry_sheet, col), ids) in &self.whole_cols {
            if entry_sheet == sheet_id && col_extent.contains(col) {
                out.extend(ids.iter().copied());
            }
        }
    }

    fn rect_buckets_for_rect(&self, rect: RectRegion) -> Vec<(SheetId, u32, u32)> {
        let row_start_bucket = rect.row_start / self.rect_bucket_rows;
        let row_end_bucket = rect.row_end / self.rect_bucket_rows;
        let col_start_bucket = rect.col_start / self.rect_bucket_cols;
        let col_end_bucket = rect.col_end / self.rect_bucket_cols;
        let mut buckets = Vec::new();
        for row_bucket in row_start_bucket..=row_end_bucket {
            for col_bucket in col_start_bucket..=col_end_bucket {
                buckets.push((rect.sheet_id, row_bucket, col_bucket));
            }
        }
        buckets
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SpanDomainEntryId(usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpanDomainEntry {
    pub(crate) span: FormulaSpanRef,
    pub(crate) domain: RegionPattern,
}

#[derive(Debug, Default)]
pub(crate) struct SpanDomainIndex {
    index: SheetRegionIndex<SpanDomainEntryId>,
    entries: Vec<SpanDomainEntry>,
    epoch: u64,
}

impl SpanDomainIndex {
    pub(crate) fn insert_domain(&mut self, span: FormulaSpanRef, domain: PlacementDomain) {
        let region = RegionPattern::from_domain(&domain);
        let id = SpanDomainEntryId(self.entries.len());
        self.entries.push(SpanDomainEntry {
            span,
            domain: region,
        });
        self.index.insert(region, id);
        self.epoch = self.epoch.saturating_add(1);
    }

    pub(crate) fn find_at(&self, coord: PlacementCoord) -> RegionQueryResult<SpanDomainEntry> {
        self.find_intersections(RegionPattern::Point(coord.into()))
    }

    pub(crate) fn find_intersections(
        &self,
        region: RegionPattern,
    ) -> RegionQueryResult<SpanDomainEntry> {
        let result = self.index.query(region);
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

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DirtyProjection {
    WholeTarget,
    SameRow,
    SameCol,
    Shifted { row_delta: i32, col_delta: i32 },
    PrefixFromSource,
    SuffixFromSource,
    FixedRangeToWhole,
    ConservativeWhole,
    UnsupportedUnbounded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DirtyDomain {
    WholeSpan(FormulaSpanRef),
    Cells(Vec<RegionKey>),
    Regions(Vec<RegionPattern>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SpanDependencyEntryId(usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpanDependencyEntry {
    pub(crate) span: FormulaSpanRef,
    pub(crate) precedent_region: RegionPattern,
    pub(crate) projection: DirtyProjection,
    pub(crate) span_version: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpanDirtyCandidate {
    pub(crate) entry: SpanDependencyEntry,
    pub(crate) dirty_domain: DirtyDomain,
}

#[derive(Debug, Default)]
pub(crate) struct SpanDependencyIndex {
    index: SheetRegionIndex<SpanDependencyEntryId>,
    entries: Vec<SpanDependencyEntry>,
    built_from_plane_epoch: u64,
    epoch: u64,
    stale_epoch_count: u64,
}

impl SpanDependencyIndex {
    pub(crate) fn insert_dependency(
        &mut self,
        span: FormulaSpanRef,
        precedent_region: RegionPattern,
        projection: DirtyProjection,
    ) -> Option<SpanDependencyEntryId> {
        if projection == DirtyProjection::UnsupportedUnbounded {
            return None;
        }
        let id = SpanDependencyEntryId(self.entries.len());
        let entry = SpanDependencyEntry {
            span,
            precedent_region,
            projection,
            span_version: span.version,
        };
        self.entries.push(entry);
        self.index.insert(precedent_region, id);
        self.epoch = self.epoch.saturating_add(1);
        Some(id)
    }

    pub(crate) fn query_changed_region(
        &self,
        changed: RegionPattern,
    ) -> RegionQueryResult<SpanDirtyCandidate> {
        let result = self.index.query(changed);
        RegionQueryResult {
            matches: result
                .matches
                .into_iter()
                .map(|matched| {
                    let entry = self.entries[matched.value.0].clone();
                    RegionMatch {
                        indexed_region: matched.indexed_region,
                        value: SpanDirtyCandidate {
                            dirty_domain: DirtyDomain::WholeSpan(entry.span),
                            entry,
                        },
                    }
                })
                .collect(),
            stats: result.stats,
        }
    }

    pub(crate) fn mark_built_from_plane_epoch(&mut self, plane_epoch: u64) {
        self.built_from_plane_epoch = plane_epoch;
        self.epoch = self.epoch.saturating_add(1);
    }

    pub(crate) fn built_from_plane_epoch(&self) -> u64 {
        self.built_from_plane_epoch
    }

    pub(crate) fn stale_epoch_count(&self) -> u64 {
        self.stale_epoch_count
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FormulaOverlayIndexEntryId(usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FormulaOverlayIndexEntry {
    pub(crate) overlay: FormulaOverlayRef,
    pub(crate) domain: RegionPattern,
}

#[derive(Debug, Default)]
pub(crate) struct FormulaOverlayIndex {
    index: SheetRegionIndex<FormulaOverlayIndexEntryId>,
    entries: Vec<FormulaOverlayIndexEntry>,
    built_from_overlay_epoch: u64,
    epoch: u64,
    stale_epoch_count: u64,
}

impl FormulaOverlayIndex {
    pub(crate) fn insert_overlay(&mut self, overlay: FormulaOverlayRef, domain: PlacementDomain) {
        let region = RegionPattern::from_domain(&domain);
        let id = FormulaOverlayIndexEntryId(self.entries.len());
        self.entries.push(FormulaOverlayIndexEntry {
            overlay,
            domain: region,
        });
        self.index.insert(region, id);
        self.epoch = self.epoch.saturating_add(1);
    }

    pub(crate) fn find_at(
        &self,
        coord: PlacementCoord,
    ) -> RegionQueryResult<FormulaOverlayIndexEntry> {
        self.find_intersections(RegionPattern::Point(coord.into()))
    }

    pub(crate) fn find_intersections(
        &self,
        region: RegionPattern,
    ) -> RegionQueryResult<FormulaOverlayIndexEntry> {
        let result = self.index.query(region);
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

    pub(crate) fn mark_built_from_overlay_epoch(&mut self, overlay_epoch: u64) {
        self.built_from_overlay_epoch = overlay_epoch;
        self.epoch = self.epoch.saturating_add(1);
    }

    pub(crate) fn built_from_overlay_epoch(&self) -> u64 {
        self.built_from_overlay_epoch
    }

    pub(crate) fn stale_epoch_count(&self) -> u64 {
        self.stale_epoch_count
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::formula_plane::producer::{
        AxisProjection, DirtyProjectionRule, ProducerDirtyDomain, ProjectionResult,
    };

    use super::super::runtime::{FormulaOverlayEntryId, FormulaSpanId};
    use super::*;

    fn span_ref(id: u32) -> FormulaSpanRef {
        FormulaSpanRef {
            id: FormulaSpanId(id),
            generation: 0,
            version: 0,
        }
    }

    fn overlay_ref(id: u32) -> FormulaOverlayRef {
        FormulaOverlayRef {
            id: FormulaOverlayEntryId(id),
            generation: 0,
            overlay_epoch: 0,
        }
    }

    #[test]
    fn rows_from_intersection_arithmetic() {
        let tail = RegionPattern::rows_from(1, 5);

        assert!(tail.intersects(&RegionPattern::rect(1, 3, 10, 0, 5)));
        assert!(!tail.intersects(&RegionPattern::rect(1, 0, 4, 0, 5)));
        assert!(tail.intersects(&RegionPattern::whole_sheet(1)));
        assert!(tail.intersects(&RegionPattern::point(1, 7, 3)));
        assert!(!tail.intersects(&RegionPattern::point(1, 2, 3)));
        assert!(tail.intersects(&RegionPattern::rows_from(1, 8)));
        assert!(tail.intersects(&RegionPattern::rows_from(1, 2)));
    }

    #[test]
    fn cols_from_intersection_arithmetic() {
        let tail = RegionPattern::cols_from(1, 5);

        assert!(tail.intersects(&RegionPattern::rect(1, 0, 5, 3, 10)));
        assert!(!tail.intersects(&RegionPattern::rect(1, 0, 5, 0, 4)));
        assert!(tail.intersects(&RegionPattern::whole_sheet(1)));
        assert!(tail.intersects(&RegionPattern::point(1, 3, 7)));
        assert!(!tail.intersects(&RegionPattern::point(1, 3, 2)));
        assert!(tail.intersects(&RegionPattern::cols_from(1, 8)));
        assert!(tail.intersects(&RegionPattern::cols_from(1, 2)));
    }

    #[test]
    fn rows_from_index_does_not_explode() {
        let start = Instant::now();
        let mut index = SheetRegionIndex::with_rect_bucket_size(64, 16);
        index.insert(RegionPattern::rows_from(1, 0), "rows_from_zero");
        index.insert(RegionPattern::rows_from(1, u32::MAX), "rows_from_max");

        let all_tail = index.query(RegionPattern::rows_from(1, 0));
        let max_point = index.query(RegionPattern::point(1, u32::MAX, 3));
        let before_max = index.query(RegionPattern::point(1, u32::MAX - 1, 3));
        let elapsed = start.elapsed();

        let all_values: FxHashSet<_> = all_tail.matches.iter().map(|m| m.value).collect();
        assert!(all_values.contains("rows_from_zero"));
        assert!(all_values.contains("rows_from_max"));
        assert_eq!(max_point.matches.len(), 2);
        assert_eq!(before_max.matches.len(), 1);
        assert!(elapsed.as_millis() < 100, "elapsed={elapsed:?}");
    }

    #[test]
    fn cols_from_index_does_not_explode() {
        let start = Instant::now();
        let mut index = SheetRegionIndex::with_rect_bucket_size(64, 16);
        index.insert(RegionPattern::cols_from(1, 0), "cols_from_zero");
        index.insert(RegionPattern::cols_from(1, u32::MAX), "cols_from_max");

        let all_tail = index.query(RegionPattern::cols_from(1, 0));
        let max_point = index.query(RegionPattern::point(1, 3, u32::MAX));
        let before_max = index.query(RegionPattern::point(1, 3, u32::MAX - 1));
        let elapsed = start.elapsed();

        let all_values: FxHashSet<_> = all_tail.matches.iter().map(|m| m.value).collect();
        assert!(all_values.contains("cols_from_zero"));
        assert!(all_values.contains("cols_from_max"));
        assert_eq!(max_point.matches.len(), 2);
        assert_eq!(before_max.matches.len(), 1);
        assert!(elapsed.as_millis() < 100, "elapsed={elapsed:?}");
    }

    #[test]
    fn from_axis_projection_no_overflow() {
        let changed = RegionPattern::rows_from(1, u32::MAX - 10);
        let read = RegionPattern::rows_from(1, u32::MAX - 10);
        let result = RegionPattern::rect(1, u32::MAX - 30, u32::MAX, 0, 0);
        let positive_offset = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: 20 },
            col: AxisProjection::Relative { offset: 0 },
        };
        assert!(matches!(
            positive_offset.project_changed_region(changed, read, result),
            ProjectionResult::Exact(ProducerDirtyDomain::Regions(_))
                | ProjectionResult::Exact(ProducerDirtyDomain::Cells(_))
                | ProjectionResult::NoIntersection
        ));

        let overflowing_offset = DirtyProjectionRule::AffineCell {
            row: AxisProjection::Relative { offset: -20 },
            col: AxisProjection::Relative { offset: 0 },
        };
        let projected = overflowing_offset.project_changed_region(changed, read, result);
        assert_eq!(
            projected,
            ProjectionResult::Exact(ProducerDirtyDomain::Cells(vec![RegionKey::new(
                1,
                u32::MAX,
                0,
            )]))
        );
    }

    #[test]
    fn sheet_region_index_finds_point_interval_rect_and_whole_axis_entries() {
        let mut index = SheetRegionIndex::with_rect_bucket_size(10, 10);
        index.insert(RegionPattern::point(1, 3, 4), "point");
        index.insert(RegionPattern::col_interval(1, 2, 0, 10), "col_interval");
        index.insert(RegionPattern::row_interval(1, 4, 0, 10), "row_interval");
        index.insert(RegionPattern::rect(1, 20, 29, 20, 29), "rect");
        index.insert(RegionPattern::whole_row(1, 7), "whole_row");
        index.insert(RegionPattern::whole_col(1, 8), "whole_col");
        index.insert(RegionPattern::whole_sheet(1), "whole_sheet");

        let point = index.query(RegionPattern::point(1, 3, 4));
        assert!(point.matches.iter().any(|m| m.value == "point"));
        assert!(point.matches.iter().any(|m| m.value == "whole_sheet"));

        let col = index.query(RegionPattern::point(1, 5, 2));
        assert!(col.matches.iter().any(|m| m.value == "col_interval"));

        let row = index.query(RegionPattern::point(1, 4, 5));
        assert!(row.matches.iter().any(|m| m.value == "row_interval"));

        let rect = index.query(RegionPattern::point(1, 25, 25));
        assert!(rect.matches.iter().any(|m| m.value == "rect"));

        let whole_row = index.query(RegionPattern::point(1, 7, 99));
        assert!(whole_row.matches.iter().any(|m| m.value == "whole_row"));

        let whole_col = index.query(RegionPattern::point(1, 99, 8));
        assert!(whole_col.matches.iter().any(|m| m.value == "whole_col"));
    }

    #[test]
    fn whole_sheet_query_returns_point_interval_rect_and_axis_entries() {
        let mut index = SheetRegionIndex::with_rect_bucket_size(10, 10);
        index.insert(RegionPattern::point(1, 3, 4), "point");
        index.insert(RegionPattern::col_interval(1, 2, 0, 10), "col_interval");
        index.insert(RegionPattern::row_interval(1, 4, 0, 10), "row_interval");
        index.insert(RegionPattern::rect(1, 20, 29, 20, 29), "rect");
        index.insert(RegionPattern::whole_row(1, 7), "whole_row");
        index.insert(RegionPattern::whole_col(1, 8), "whole_col");
        index.insert(RegionPattern::whole_sheet(1), "whole_sheet");

        let result = index.query(RegionPattern::whole_sheet(1));
        let values: FxHashSet<_> = result.matches.iter().map(|m| m.value).collect();

        assert_eq!(values.len(), 7);
        assert!(values.contains("point"));
        assert!(values.contains("col_interval"));
        assert!(values.contains("row_interval"));
        assert!(values.contains("rect"));
        assert!(values.contains("whole_row"));
        assert!(values.contains("whole_col"));
        assert!(values.contains("whole_sheet"));
    }

    #[test]
    fn sheet_region_index_does_not_return_other_sheet_entries() {
        let mut index = SheetRegionIndex::with_rect_bucket_size(10, 10);
        index.insert(RegionPattern::point(1, 0, 0), "sheet_1_point");
        index.insert(RegionPattern::col_interval(1, 0, 0, 10), "sheet_1_col");
        index.insert(RegionPattern::rect(1, 0, 10, 0, 10), "sheet_1_rect");
        index.insert(RegionPattern::whole_sheet(1), "sheet_1_whole");
        index.insert(RegionPattern::point(2, 0, 0), "sheet_2_point");

        let result = index.query(RegionPattern::whole_sheet(2));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value, "sheet_2_point");
    }

    #[test]
    fn sheet_region_index_no_under_return_matches_bruteforce_table() {
        let inserted = vec![
            (RegionPattern::point(1, 0, 0), "s1_point_origin"),
            (RegionPattern::point(1, 7, 7), "s1_point_far"),
            (RegionPattern::col_interval(1, 2, 1, 6), "s1_col_interval"),
            (RegionPattern::row_interval(1, 5, 1, 6), "s1_row_interval"),
            (RegionPattern::rect(1, 3, 5, 3, 5), "s1_rect_cross_bucket"),
            (RegionPattern::whole_row(1, 9), "s1_whole_row"),
            (RegionPattern::whole_col(1, 10), "s1_whole_col"),
            (RegionPattern::rows_from(1, 8), "s1_rows_from"),
            (RegionPattern::cols_from(1, 8), "s1_cols_from"),
            (RegionPattern::whole_sheet(1), "s1_whole_sheet"),
            (RegionPattern::point(2, 0, 0), "s2_point_origin"),
            (RegionPattern::rect(2, 3, 5, 3, 5), "s2_rect_cross_bucket"),
            (RegionPattern::whole_sheet(2), "s2_whole_sheet"),
        ];
        let queries = vec![
            RegionPattern::point(1, 0, 0),
            RegionPattern::point(1, 4, 4),
            RegionPattern::point(1, 5, 2),
            RegionPattern::point(1, 9, 99),
            RegionPattern::point(1, 99, 10),
            RegionPattern::rect(1, 4, 6, 4, 6),
            RegionPattern::whole_row(1, 5),
            RegionPattern::whole_col(1, 2),
            RegionPattern::rows_from(1, 8),
            RegionPattern::cols_from(1, 8),
            RegionPattern::whole_sheet(1),
            RegionPattern::point(2, 4, 4),
            RegionPattern::whole_sheet(2),
        ];
        let mut index = SheetRegionIndex::with_rect_bucket_size(4, 4);
        for (region, value) in &inserted {
            index.insert(*region, *value);
        }

        for query in queries {
            let expected: FxHashSet<_> = inserted
                .iter()
                .filter_map(|(region, value)| region.intersects(&query).then_some(*value))
                .collect();
            let actual: FxHashSet<_> = index
                .query(query)
                .matches
                .into_iter()
                .map(|matched| matched.value)
                .collect();

            assert_eq!(actual, expected, "query {query:?}");
        }
    }

    #[test]
    fn sheet_region_index_rect_bucket_boundary_queries_do_not_under_return() {
        let mut index = SheetRegionIndex::with_rect_bucket_size(4, 4);
        index.insert(RegionPattern::rect(1, 3, 4, 3, 4), "crossing_rect");
        index.insert(RegionPattern::rect(1, 0, 2, 0, 2), "unrelated_rect");

        for point in [
            RegionPattern::point(1, 3, 3),
            RegionPattern::point(1, 3, 4),
            RegionPattern::point(1, 4, 3),
            RegionPattern::point(1, 4, 4),
        ] {
            let result = index.query(point);
            let values: FxHashSet<_> = result.matches.iter().map(|matched| matched.value).collect();
            assert!(values.contains("crossing_rect"), "query {point:?}");
            assert!(!values.contains("unrelated_rect"), "query {point:?}");
        }
    }

    #[test]
    fn sheet_region_index_may_overreturn_rect_bucket_but_never_misses_intersection() {
        let mut index = SheetRegionIndex::with_rect_bucket_size(10, 10);
        index.insert(RegionPattern::rect(1, 0, 0, 0, 0), "hit");
        index.insert(RegionPattern::rect(1, 9, 9, 9, 9), "same_bucket_drop");

        let result = index.query(RegionPattern::point(1, 0, 0));

        assert_eq!(result.stats.candidate_count, 2);
        assert_eq!(result.stats.exact_filter_drop_count, 1);
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value, "hit");
    }

    #[test]
    fn span_domain_index_finds_row_run_owner() {
        let mut index = SpanDomainIndex::default();
        let span = span_ref(1);
        index.insert_domain(span, PlacementDomain::row_run(2, 0, 9, 4));

        let result = index.find_at(PlacementCoord::new(2, 5, 4));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.span, span);
    }

    #[test]
    fn span_domain_index_finds_col_run_owner() {
        let mut index = SpanDomainIndex::default();
        let span = span_ref(2);
        index.insert_domain(span, PlacementDomain::col_run(2, 4, 0, 9));

        let result = index.find_at(PlacementCoord::new(2, 4, 5));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.span, span);
    }

    #[test]
    fn span_domain_index_finds_rect_intersections() {
        let mut index = SpanDomainIndex::default();
        let span = span_ref(3);
        index.insert_domain(span, PlacementDomain::rect(2, 10, 12, 20, 22));

        let result = index.find_intersections(RegionPattern::rect(2, 11, 11, 21, 21));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.span, span);
    }

    #[test]
    fn span_domain_index_does_not_apply_formula_overlay_semantics() {
        let mut index = SpanDomainIndex::default();
        let span = span_ref(4);
        index.insert_domain(span, PlacementDomain::row_run(2, 0, 9, 4));

        let result = index.find_at(PlacementCoord::new(2, 5, 4));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.span, span);
    }

    #[test]
    fn span_dependency_index_indexes_same_row_static_precedent_regions() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(5);
        index.insert_dependency(
            span,
            RegionPattern::col_interval(2, 0, 0, 9),
            DirtyProjection::SameRow,
        );

        let result = index.query_changed_region(RegionPattern::point(2, 4, 0));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.entry.span, span);
        assert_eq!(
            result.matches[0].value.dirty_domain,
            DirtyDomain::WholeSpan(span)
        );
    }

    #[test]
    fn span_dependency_index_indexes_absolute_precedent_regions() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(6);
        index.insert_dependency(
            span,
            RegionPattern::point(2, 0, 5),
            DirtyProjection::WholeTarget,
        );

        let result = index.query_changed_region(RegionPattern::point(2, 0, 5));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.entry.span, span);
    }

    #[test]
    fn span_dependency_index_keeps_whole_column_bucket_separate() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(7);
        index.insert_dependency(
            span,
            RegionPattern::whole_col(2, 3),
            DirtyProjection::ConservativeWhole,
        );

        let result = index.query_changed_region(RegionPattern::point(2, 99, 3));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.entry.span, span);
    }

    #[test]
    fn formula_overlay_index_finds_cell_punchout() {
        let mut index = FormulaOverlayIndex::default();
        let overlay = overlay_ref(1);
        index.insert_overlay(overlay, PlacementDomain::row_run(3, 0, 9, 2));

        let result = index.find_at(PlacementCoord::new(3, 5, 2));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.overlay, overlay);
    }

    #[test]
    fn formula_overlay_index_finds_region_punchouts_without_domain_entries() {
        let mut index = FormulaOverlayIndex::default();
        let overlay = overlay_ref(2);
        index.insert_overlay(overlay, PlacementDomain::rect(3, 10, 20, 10, 20));

        let result = index.find_intersections(RegionPattern::rect(3, 15, 16, 15, 16));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.overlay, overlay);
    }

    #[test]
    fn rect_dependency_query_exact_filters_bucket_candidates() {
        let mut index = SpanDependencyIndex::default();
        let hit = span_ref(8);
        let drop = span_ref(9);
        index.insert_dependency(
            hit,
            RegionPattern::rect(4, 0, 0, 0, 0),
            DirtyProjection::WholeTarget,
        );
        index.insert_dependency(
            drop,
            RegionPattern::rect(4, 63, 63, 15, 15),
            DirtyProjection::WholeTarget,
        );

        let result = index.query_changed_region(RegionPattern::point(4, 0, 0));

        assert_eq!(result.stats.candidate_count, 2);
        assert_eq!(result.stats.exact_filter_drop_count, 1);
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.entry.span, hit);
    }

    #[test]
    fn span_dependency_index_rejects_unsupported_unbounded_dependency() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(13);

        let inserted = index.insert_dependency(
            span,
            RegionPattern::whole_sheet(2),
            DirtyProjection::UnsupportedUnbounded,
        );
        let result = index.query_changed_region(RegionPattern::point(2, 0, 0));

        assert!(inserted.is_none());
        assert!(result.matches.is_empty());
    }

    #[test]
    fn whole_column_dependency_query_marks_candidate_span() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(10);
        index.insert_dependency(
            span,
            RegionPattern::whole_col(5, 1),
            DirtyProjection::ConservativeWhole,
        );

        let result = index.query_changed_region(RegionPattern::rect(5, 100, 110, 1, 1));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.entry.span, span);
    }

    #[test]
    fn whole_row_dependency_query_marks_candidate_span() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(14);
        index.insert_dependency(
            span,
            RegionPattern::whole_row(2, 7),
            DirtyProjection::WholeTarget,
        );

        let result = index.query_changed_region(RegionPattern::point(2, 7, 99));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.entry.span, span);
    }

    #[test]
    fn unrelated_edit_does_not_mark_span_dirty() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(11);
        index.insert_dependency(
            span,
            RegionPattern::col_interval(6, 0, 0, 9),
            DirtyProjection::SameRow,
        );

        let result = index.query_changed_region(RegionPattern::point(6, 4, 1));

        assert!(result.matches.is_empty());
    }
}
