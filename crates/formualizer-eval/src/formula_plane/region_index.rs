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
pub(crate) struct Region {
    pub(crate) sheet_id: SheetId,
    pub(crate) rows: AxisRange,
    pub(crate) cols: AxisRange,
}

impl Region {
    #[inline]
    pub(crate) fn sheet_id(self) -> SheetId {
        self.sheet_id
    }

    #[inline]
    pub(crate) fn axis_ranges(self) -> (AxisRange, AxisRange) {
        (self.rows, self.cols)
    }

    #[inline]
    pub(crate) fn kind_pair(self) -> (AxisKind, AxisKind) {
        (self.rows.kind(), self.cols.kind())
    }

    #[inline]
    pub(crate) fn intersects(&self, other: &Self) -> bool {
        self.sheet_id == other.sheet_id
            && self.rows.intersects(other.rows)
            && self.cols.intersects(other.cols)
    }

    #[inline]
    pub(crate) fn contains_key(&self, key: RegionKey) -> bool {
        self.sheet_id == key.sheet_id && self.rows.contains(key.row) && self.cols.contains(key.col)
    }

    pub(crate) fn point(sheet_id: SheetId, row: u32, col: u32) -> Self {
        Self {
            sheet_id,
            rows: AxisRange::Point(row),
            cols: AxisRange::Point(col),
        }
    }

    pub(crate) fn col_interval(sheet_id: SheetId, col: u32, row_start: u32, row_end: u32) -> Self {
        assert!(row_start <= row_end, "row_start must be <= row_end");
        Self {
            sheet_id,
            rows: AxisRange::Span(row_start, row_end),
            cols: AxisRange::Point(col),
        }
    }

    pub(crate) fn row_interval(sheet_id: SheetId, row: u32, col_start: u32, col_end: u32) -> Self {
        assert!(col_start <= col_end, "col_start must be <= col_end");
        Self {
            sheet_id,
            rows: AxisRange::Point(row),
            cols: AxisRange::Span(col_start, col_end),
        }
    }

    pub(crate) fn rect(
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
            rows: AxisRange::Span(row_start, row_end),
            cols: AxisRange::Span(col_start, col_end),
        }
    }

    pub(crate) fn rows_from(sheet_id: SheetId, row_start: u32) -> Self {
        Self {
            sheet_id,
            rows: AxisRange::From(row_start),
            cols: AxisRange::All,
        }
    }

    pub(crate) fn cols_from(sheet_id: SheetId, col_start: u32) -> Self {
        Self {
            sheet_id,
            rows: AxisRange::All,
            cols: AxisRange::From(col_start),
        }
    }

    pub(crate) fn whole_row(sheet_id: SheetId, row: u32) -> Self {
        Self {
            sheet_id,
            rows: AxisRange::Point(row),
            cols: AxisRange::All,
        }
    }

    pub(crate) fn whole_col(sheet_id: SheetId, col: u32) -> Self {
        Self {
            sheet_id,
            rows: AxisRange::All,
            cols: AxisRange::Point(col),
        }
    }

    pub(crate) fn whole_sheet(sheet_id: SheetId) -> Self {
        Self {
            sheet_id,
            rows: AxisRange::All,
            cols: AxisRange::All,
        }
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

    #[inline]
    pub(crate) fn as_point(self) -> Option<RegionKey> {
        if let (AxisRange::Point(row), AxisRange::Point(col)) = (self.rows, self.cols) {
            Some(RegionKey {
                sheet_id: self.sheet_id,
                row,
                col,
            })
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn project_through_axis_shift(self, row_delta: i64, col_delta: i64) -> Option<Self> {
        Some(Self {
            sheet_id: self.sheet_id,
            rows: self.rows.project_through_offset(row_delta)?,
            cols: self.cols.project_through_offset(col_delta)?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AxisRange {
    Point(u32),
    Span(u32, u32),
    From(u32),
    To(u32),
    All,
}

impl AxisRange {
    #[inline]
    pub(crate) fn intersects(self, other: Self) -> bool {
        match (self, other) {
            (Self::Point(p1), Self::Point(p2)) => p1 == p2,
            (Self::Point(p1), Self::Span(s2, e2)) => s2 <= p1 && p1 <= e2,
            (Self::Point(p1), Self::From(s2)) => p1 >= s2,
            (Self::Point(p1), Self::To(e2)) => p1 <= e2,
            (Self::Point(_), Self::All) => true,

            (Self::Span(s1, e1), Self::Point(p2)) => s1 <= p2 && p2 <= e1,
            (Self::Span(s1, e1), Self::Span(s2, e2)) => s1 <= e2 && s2 <= e1,
            (Self::Span(_, e1), Self::From(s2)) => s2 <= e1,
            (Self::Span(s1, _), Self::To(e2)) => s1 <= e2,
            (Self::Span(_, _), Self::All) => true,

            (Self::From(s1), Self::Point(p2)) => p2 >= s1,
            (Self::From(s1), Self::Span(_, e2)) => s1 <= e2,
            (Self::From(_), Self::From(_)) => true,
            (Self::From(s1), Self::To(e2)) => s1 <= e2,
            (Self::From(_), Self::All) => true,

            (Self::To(e1), Self::Point(p2)) => p2 <= e1,
            (Self::To(e1), Self::Span(s2, _)) => s2 <= e1,
            (Self::To(e1), Self::From(s2)) => s2 <= e1,
            (Self::To(_), Self::To(_)) => true,
            (Self::To(_), Self::All) => true,

            (Self::All, Self::Point(_)) => true,
            (Self::All, Self::Span(_, _)) => true,
            (Self::All, Self::From(_)) => true,
            (Self::All, Self::To(_)) => true,
            (Self::All, Self::All) => true,
        }
    }

    #[inline]
    pub(crate) fn contains(self, coord: u32) -> bool {
        match self {
            Self::Point(point) => coord == point,
            Self::Span(start, end) => start <= coord && coord <= end,
            Self::From(start) => coord >= start,
            Self::To(end) => coord <= end,
            Self::All => true,
        }
    }

    #[inline]
    pub(crate) fn query_bounds(self) -> (u32, u32) {
        match self {
            Self::Point(point) => (point, point),
            Self::Span(start, end) => (start, end),
            Self::From(start) => (start, u32::MAX),
            Self::To(end) => (0, end),
            Self::All => (0, u32::MAX),
        }
    }

    #[inline]
    pub(crate) fn is_bounded(self) -> bool {
        matches!(self, Self::Point(_) | Self::Span(_, _))
    }

    pub(crate) fn project_through_offset(self, offset: i64) -> Option<Self> {
        fn shifted(value: u32, offset: i64) -> i64 {
            i64::from(value).saturating_add(offset)
        }

        fn clamp_u32(value: i64) -> u32 {
            if value < 0 {
                0
            } else {
                u32::try_from(value).unwrap_or(u32::MAX)
            }
        }

        match self {
            Self::Point(point) => {
                let projected = shifted(point, offset);
                (0..=i64::from(u32::MAX))
                    .contains(&projected)
                    .then_some(Self::Point(projected as u32))
            }
            Self::Span(start, end) => {
                let projected_start = shifted(start, offset);
                let projected_end = shifted(end, offset);
                if projected_end < 0 || projected_start > i64::from(u32::MAX) {
                    None
                } else {
                    Some(Self::Span(
                        clamp_u32(projected_start),
                        clamp_u32(projected_end),
                    ))
                }
            }
            Self::From(start) => {
                let projected_start = shifted(start, offset);
                if projected_start < 0 {
                    Some(Self::All)
                } else {
                    Some(Self::From(clamp_u32(projected_start)))
                }
            }
            Self::To(end) => {
                let projected_end = shifted(end, offset);
                if projected_end < 0 {
                    None
                } else {
                    Some(Self::To(clamp_u32(projected_end)))
                }
            }
            Self::All => Some(Self::All),
        }
    }

    #[inline]
    pub(crate) fn kind(self) -> AxisKind {
        match self {
            Self::Point(_) => AxisKind::Point,
            Self::Span(_, _) => AxisKind::Span,
            Self::From(_) => AxisKind::From,
            Self::To(_) => AxisKind::To,
            Self::All => AxisKind::All,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AxisKind {
    Point,
    Span,
    From,
    To,
    All,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RegionSet {
    One(Region),
    Many(Vec<Region>),
}

impl RegionSet {
    pub(crate) fn regions(&self) -> &[Region] {
        match self {
            Self::One(region) => std::slice::from_ref(region),
            Self::Many(regions) => regions.as_slice(),
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
    pub(crate) indexed_region: Region,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegionQueryResult<T> {
    pub(crate) matches: Vec<RegionMatch<T>>,
    pub(crate) stats: RegionQueryStats,
}

#[derive(Clone, Debug)]
struct RegionEntry<T> {
    value: T,
    region: Region,
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

    pub(crate) fn insert(&mut self, region: Region, value: T) -> usize {
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

    pub(crate) fn query(&self, query: Region) -> RegionQueryResult<T> {
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

    fn index_entry(&mut self, id: usize, region: Region) {
        let sheet_id = region.sheet_id();
        let (rows, cols) = region.axis_ranges();
        match (rows.kind(), cols.kind()) {
            (AxisKind::Point, AxisKind::Point) => {
                let (AxisRange::Point(row), AxisRange::Point(col)) = (rows, cols) else {
                    unreachable!()
                };
                self.points
                    .entry(RegionKey::new(sheet_id, row, col))
                    .or_default()
                    .push(id);
            }
            (AxisKind::Span, AxisKind::Point) => {
                let (AxisRange::Span(row_start, row_end), AxisRange::Point(col)) = (rows, cols)
                else {
                    unreachable!()
                };
                self.col_intervals
                    .entry((sheet_id, col))
                    .or_default()
                    .insert(row_start, row_end, id);
            }
            (AxisKind::Point, AxisKind::Span) => {
                let (AxisRange::Point(row), AxisRange::Span(col_start, col_end)) = (rows, cols)
                else {
                    unreachable!()
                };
                self.row_intervals
                    .entry((sheet_id, row))
                    .or_default()
                    .insert(col_start, col_end, id);
            }
            (AxisKind::Span, AxisKind::Span) => {
                let (AxisRange::Span(row_start, row_end), AxisRange::Span(col_start, col_end)) =
                    (rows, cols)
                else {
                    unreachable!()
                };
                let rect = RectRegion::new(sheet_id, row_start, row_end, col_start, col_end);
                for bucket in self.rect_buckets_for_rect(rect) {
                    self.rect_buckets.entry(bucket).or_default().push(id);
                }
            }
            (AxisKind::From, AxisKind::All) => {
                let (AxisRange::From(row_start), AxisRange::All) = (rows, cols) else {
                    unreachable!()
                };
                self.rows_from
                    .entry(sheet_id)
                    .or_default()
                    .entry(row_start)
                    .or_default()
                    .push(id);
            }
            (AxisKind::All, AxisKind::From) => {
                let (AxisRange::All, AxisRange::From(col_start)) = (rows, cols) else {
                    unreachable!()
                };
                self.cols_from
                    .entry(sheet_id)
                    .or_default()
                    .entry(col_start)
                    .or_default()
                    .push(id);
            }
            (AxisKind::Point, AxisKind::All) => {
                let (AxisRange::Point(row), AxisRange::All) = (rows, cols) else {
                    unreachable!()
                };
                self.whole_rows.entry((sheet_id, row)).or_default().push(id);
            }
            (AxisKind::All, AxisKind::Point) => {
                let (AxisRange::All, AxisRange::Point(col)) = (rows, cols) else {
                    unreachable!()
                };
                self.whole_cols.entry((sheet_id, col)).or_default().push(id);
            }
            (AxisKind::All, AxisKind::All) => {
                let (AxisRange::All, AxisRange::All) = (rows, cols) else {
                    unreachable!()
                };
                self.whole_sheets.entry(sheet_id).or_default().push(id);
            }
            _ => panic!(
                "unsupported SheetRegionIndex insertion kind pair in Phase 2: ({:?}, {:?})",
                rows.kind(),
                cols.kind()
            ),
        }
    }

    fn collect_candidates(&self, query: Region, out: &mut FxHashSet<usize>) {
        let sheet_id = query.sheet_id();
        let (rows, cols) = query.axis_ranges();
        match (rows.kind(), cols.kind()) {
            (AxisKind::Point, AxisKind::Point) => {
                let (AxisRange::Point(row), AxisRange::Point(col)) = (rows, cols) else {
                    unreachable!()
                };
                if let Some(ids) = self.points.get(&RegionKey::new(sheet_id, row, col)) {
                    Self::extend_ids(out, ids);
                }
                self.collect_col_interval_exact_col(sheet_id, col, row, row, out);
                self.collect_row_interval_exact_row(sheet_id, row, col, col, out);
                self.collect_rect_bucket_key(
                    sheet_id,
                    self.row_bucket(row),
                    self.col_bucket(col),
                    out,
                );
                self.collect_rows_from_through(sheet_id, row, out);
                self.collect_cols_from_through(sheet_id, col, out);
                self.collect_whole_row_exact(sheet_id, row, out);
                self.collect_whole_col_exact(sheet_id, col, out);
                self.collect_whole_sheet(sheet_id, out);
            }
            (AxisKind::Span, AxisKind::Point) => {
                let (AxisRange::Span(row_start, row_end), AxisRange::Point(col)) = (rows, cols)
                else {
                    unreachable!()
                };
                self.collect_points_matching(
                    sheet_id,
                    |key| row_start <= key.row && key.row <= row_end && key.col == col,
                    out,
                );
                self.collect_col_interval_exact_col(sheet_id, col, row_start, row_end, out);
                self.collect_row_intervals_matching(
                    sheet_id,
                    |row| row_start <= row && row <= row_end,
                    col,
                    col,
                    out,
                );
                self.collect_rect_bucket_row_span_point_col(sheet_id, row_start, row_end, col, out);
                self.collect_rows_from_through(sheet_id, row_end, out);
                self.collect_cols_from_through(sheet_id, col, out);
                self.collect_whole_rows_matching(
                    sheet_id,
                    |row| row_start <= row && row <= row_end,
                    out,
                );
                self.collect_whole_col_exact(sheet_id, col, out);
                self.collect_whole_sheet(sheet_id, out);
            }
            (AxisKind::Point, AxisKind::Span) => {
                let (AxisRange::Point(row), AxisRange::Span(col_start, col_end)) = (rows, cols)
                else {
                    unreachable!()
                };
                self.collect_points_matching(
                    sheet_id,
                    |key| key.row == row && col_start <= key.col && key.col <= col_end,
                    out,
                );
                self.collect_col_intervals_matching(
                    sheet_id,
                    |col| col_start <= col && col <= col_end,
                    row,
                    row,
                    out,
                );
                self.collect_row_interval_exact_row(sheet_id, row, col_start, col_end, out);
                self.collect_rect_bucket_point_row_col_span(sheet_id, row, col_start, col_end, out);
                self.collect_rows_from_through(sheet_id, row, out);
                self.collect_cols_from_through(sheet_id, col_end, out);
                self.collect_whole_row_exact(sheet_id, row, out);
                self.collect_whole_cols_matching(
                    sheet_id,
                    |col| col_start <= col && col <= col_end,
                    out,
                );
                self.collect_whole_sheet(sheet_id, out);
            }
            (AxisKind::Span, AxisKind::Span) => {
                let (AxisRange::Span(row_start, row_end), AxisRange::Span(col_start, col_end)) =
                    (rows, cols)
                else {
                    unreachable!()
                };
                self.collect_points_matching(
                    sheet_id,
                    |key| {
                        row_start <= key.row
                            && key.row <= row_end
                            && col_start <= key.col
                            && key.col <= col_end
                    },
                    out,
                );
                self.collect_col_intervals_matching(
                    sheet_id,
                    |col| col_start <= col && col <= col_end,
                    row_start,
                    row_end,
                    out,
                );
                self.collect_row_intervals_matching(
                    sheet_id,
                    |row| row_start <= row && row <= row_end,
                    col_start,
                    col_end,
                    out,
                );
                let rect = RectRegion::new(sheet_id, row_start, row_end, col_start, col_end);
                for bucket in self.rect_buckets_for_rect(rect) {
                    if let Some(ids) = self.rect_buckets.get(&bucket) {
                        Self::extend_ids(out, ids);
                    }
                }
                self.collect_rows_from_through(sheet_id, row_end, out);
                self.collect_cols_from_through(sheet_id, col_end, out);
                self.collect_whole_rows_matching(
                    sheet_id,
                    |row| row_start <= row && row <= row_end,
                    out,
                );
                self.collect_whole_cols_matching(
                    sheet_id,
                    |col| col_start <= col && col <= col_end,
                    out,
                );
                self.collect_whole_sheet(sheet_id, out);
            }
            (AxisKind::From, AxisKind::All) => {
                let (AxisRange::From(row_start), AxisRange::All) = (rows, cols) else {
                    unreachable!()
                };
                self.collect_points_matching(sheet_id, |key| key.row >= row_start, out);
                self.collect_col_intervals_matching(sheet_id, |_| true, row_start, u32::MAX, out);
                self.collect_row_intervals_matching(
                    sheet_id,
                    |row| row >= row_start,
                    0,
                    u32::MAX,
                    out,
                );
                let start_bucket = self.row_bucket(row_start);
                self.collect_rect_buckets_matching(
                    sheet_id,
                    |row_bucket, _col_bucket| row_bucket >= start_bucket,
                    out,
                );
                self.collect_rows_from_through(sheet_id, u32::MAX, out);
                self.collect_cols_from_through(sheet_id, u32::MAX, out);
                self.collect_whole_rows_matching(sheet_id, |row| row >= row_start, out);
                self.collect_whole_cols_matching(sheet_id, |_| true, out);
                self.collect_whole_sheet(sheet_id, out);
            }
            (AxisKind::All, AxisKind::From) => {
                let (AxisRange::All, AxisRange::From(col_start)) = (rows, cols) else {
                    unreachable!()
                };
                self.collect_points_matching(sheet_id, |key| key.col >= col_start, out);
                self.collect_col_intervals_matching(
                    sheet_id,
                    |col| col >= col_start,
                    0,
                    u32::MAX,
                    out,
                );
                self.collect_row_intervals_matching(sheet_id, |_| true, col_start, u32::MAX, out);
                let start_bucket = self.col_bucket(col_start);
                self.collect_rect_buckets_matching(
                    sheet_id,
                    |_row_bucket, col_bucket| col_bucket >= start_bucket,
                    out,
                );
                self.collect_rows_from_through(sheet_id, u32::MAX, out);
                self.collect_cols_from_through(sheet_id, u32::MAX, out);
                self.collect_whole_rows_matching(sheet_id, |_| true, out);
                self.collect_whole_cols_matching(sheet_id, |col| col >= col_start, out);
                self.collect_whole_sheet(sheet_id, out);
            }
            (AxisKind::Point, AxisKind::All) => {
                let (AxisRange::Point(row), AxisRange::All) = (rows, cols) else {
                    unreachable!()
                };
                self.collect_points_matching(sheet_id, |key| key.row == row, out);
                self.collect_col_intervals_matching(sheet_id, |_| true, row, row, out);
                self.collect_row_interval_exact_row(sheet_id, row, 0, u32::MAX, out);
                let row_bucket = self.row_bucket(row);
                self.collect_rect_buckets_matching(
                    sheet_id,
                    |entry_row_bucket, _col_bucket| entry_row_bucket == row_bucket,
                    out,
                );
                self.collect_rows_from_through(sheet_id, row, out);
                self.collect_cols_from_through(sheet_id, u32::MAX, out);
                self.collect_whole_row_exact(sheet_id, row, out);
                self.collect_whole_cols_matching(sheet_id, |_| true, out);
                self.collect_whole_sheet(sheet_id, out);
            }
            (AxisKind::All, AxisKind::Point) => {
                let (AxisRange::All, AxisRange::Point(col)) = (rows, cols) else {
                    unreachable!()
                };
                self.collect_points_matching(sheet_id, |key| key.col == col, out);
                self.collect_col_interval_exact_col(sheet_id, col, 0, u32::MAX, out);
                self.collect_row_intervals_matching(sheet_id, |_| true, col, col, out);
                let col_bucket = self.col_bucket(col);
                self.collect_rect_buckets_matching(
                    sheet_id,
                    |_row_bucket, entry_col_bucket| entry_col_bucket == col_bucket,
                    out,
                );
                self.collect_rows_from_through(sheet_id, u32::MAX, out);
                self.collect_cols_from_through(sheet_id, col, out);
                self.collect_whole_rows_matching(sheet_id, |_| true, out);
                self.collect_whole_col_exact(sheet_id, col, out);
                self.collect_whole_sheet(sheet_id, out);
            }
            (AxisKind::All, AxisKind::All) => {
                let (AxisRange::All, AxisRange::All) = (rows, cols) else {
                    unreachable!()
                };
                self.collect_points_matching(sheet_id, |_| true, out);
                self.collect_col_intervals_matching(sheet_id, |_| true, 0, u32::MAX, out);
                self.collect_row_intervals_matching(sheet_id, |_| true, 0, u32::MAX, out);
                self.collect_rect_buckets_matching(sheet_id, |_row_bucket, _col_bucket| true, out);
                self.collect_rows_from_through(sheet_id, u32::MAX, out);
                self.collect_cols_from_through(sheet_id, u32::MAX, out);
                self.collect_whole_rows_matching(sheet_id, |_| true, out);
                self.collect_whole_cols_matching(sheet_id, |_| true, out);
                self.collect_whole_sheet(sheet_id, out);
            }
            _ => panic!(
                "unsupported SheetRegionIndex query kind pair in Phase 2: ({:?}, {:?})",
                rows.kind(),
                cols.kind()
            ),
        }
    }

    fn extend_ids(out: &mut FxHashSet<usize>, ids: &[usize]) {
        out.extend(ids.iter().copied());
    }

    fn extend_tree_query(
        out: &mut FxHashSet<usize>,
        tree: &IntervalTree<usize>,
        low: u32,
        high: u32,
    ) {
        for (_low, _high, values) in tree.query(low, high) {
            out.extend(values.into_iter());
        }
    }

    fn row_bucket(&self, row: u32) -> u32 {
        row / self.rect_bucket_rows
    }

    fn col_bucket(&self, col: u32) -> u32 {
        col / self.rect_bucket_cols
    }

    fn collect_points_matching<F>(
        &self,
        sheet_id: SheetId,
        matches_key: F,
        out: &mut FxHashSet<usize>,
    ) where
        F: Fn(RegionKey) -> bool,
    {
        for (&key, ids) in &self.points {
            if key.sheet_id == sheet_id && matches_key(key) {
                Self::extend_ids(out, ids);
            }
        }
    }

    fn collect_col_interval_exact_col(
        &self,
        sheet_id: SheetId,
        col: u32,
        row_start: u32,
        row_end: u32,
        out: &mut FxHashSet<usize>,
    ) {
        if let Some(tree) = self.col_intervals.get(&(sheet_id, col)) {
            Self::extend_tree_query(out, tree, row_start, row_end);
        }
    }

    fn collect_col_intervals_matching<F>(
        &self,
        sheet_id: SheetId,
        matches_col: F,
        row_start: u32,
        row_end: u32,
        out: &mut FxHashSet<usize>,
    ) where
        F: Fn(u32) -> bool,
    {
        for (&(entry_sheet, col), tree) in &self.col_intervals {
            if entry_sheet == sheet_id && matches_col(col) {
                Self::extend_tree_query(out, tree, row_start, row_end);
            }
        }
    }

    fn collect_row_interval_exact_row(
        &self,
        sheet_id: SheetId,
        row: u32,
        col_start: u32,
        col_end: u32,
        out: &mut FxHashSet<usize>,
    ) {
        if let Some(tree) = self.row_intervals.get(&(sheet_id, row)) {
            Self::extend_tree_query(out, tree, col_start, col_end);
        }
    }

    fn collect_row_intervals_matching<F>(
        &self,
        sheet_id: SheetId,
        matches_row: F,
        col_start: u32,
        col_end: u32,
        out: &mut FxHashSet<usize>,
    ) where
        F: Fn(u32) -> bool,
    {
        for (&(entry_sheet, row), tree) in &self.row_intervals {
            if entry_sheet == sheet_id && matches_row(row) {
                Self::extend_tree_query(out, tree, col_start, col_end);
            }
        }
    }

    fn collect_rect_bucket_key(
        &self,
        sheet_id: SheetId,
        row_bucket: u32,
        col_bucket: u32,
        out: &mut FxHashSet<usize>,
    ) {
        if let Some(ids) = self.rect_buckets.get(&(sheet_id, row_bucket, col_bucket)) {
            Self::extend_ids(out, ids);
        }
    }

    fn collect_rect_bucket_row_span_point_col(
        &self,
        sheet_id: SheetId,
        row_start: u32,
        row_end: u32,
        col: u32,
        out: &mut FxHashSet<usize>,
    ) {
        let col_bucket = self.col_bucket(col);
        for row_bucket in self.row_bucket(row_start)..=self.row_bucket(row_end) {
            self.collect_rect_bucket_key(sheet_id, row_bucket, col_bucket, out);
        }
    }

    fn collect_rect_bucket_point_row_col_span(
        &self,
        sheet_id: SheetId,
        row: u32,
        col_start: u32,
        col_end: u32,
        out: &mut FxHashSet<usize>,
    ) {
        let row_bucket = self.row_bucket(row);
        for col_bucket in self.col_bucket(col_start)..=self.col_bucket(col_end) {
            self.collect_rect_bucket_key(sheet_id, row_bucket, col_bucket, out);
        }
    }

    fn collect_rect_buckets_matching<F>(
        &self,
        sheet_id: SheetId,
        matches_bucket: F,
        out: &mut FxHashSet<usize>,
    ) where
        F: Fn(u32, u32) -> bool,
    {
        for (&(entry_sheet, row_bucket, col_bucket), ids) in &self.rect_buckets {
            if entry_sheet == sheet_id && matches_bucket(row_bucket, col_bucket) {
                Self::extend_ids(out, ids);
            }
        }
    }

    fn collect_rows_from_through(
        &self,
        sheet_id: SheetId,
        row_end: u32,
        out: &mut FxHashSet<usize>,
    ) {
        if let Some(rows_from) = self.rows_from.get(&sheet_id) {
            for (_row_start, ids) in rows_from.range(..=row_end) {
                Self::extend_ids(out, ids);
            }
        }
    }

    fn collect_cols_from_through(
        &self,
        sheet_id: SheetId,
        col_end: u32,
        out: &mut FxHashSet<usize>,
    ) {
        if let Some(cols_from) = self.cols_from.get(&sheet_id) {
            for (_col_start, ids) in cols_from.range(..=col_end) {
                Self::extend_ids(out, ids);
            }
        }
    }

    fn collect_whole_row_exact(&self, sheet_id: SheetId, row: u32, out: &mut FxHashSet<usize>) {
        if let Some(ids) = self.whole_rows.get(&(sheet_id, row)) {
            Self::extend_ids(out, ids);
        }
    }

    fn collect_whole_col_exact(&self, sheet_id: SheetId, col: u32, out: &mut FxHashSet<usize>) {
        if let Some(ids) = self.whole_cols.get(&(sheet_id, col)) {
            Self::extend_ids(out, ids);
        }
    }

    fn collect_whole_rows_matching<F>(
        &self,
        sheet_id: SheetId,
        matches_row: F,
        out: &mut FxHashSet<usize>,
    ) where
        F: Fn(u32) -> bool,
    {
        for (&(entry_sheet, row), ids) in &self.whole_rows {
            if entry_sheet == sheet_id && matches_row(row) {
                Self::extend_ids(out, ids);
            }
        }
    }

    fn collect_whole_cols_matching<F>(
        &self,
        sheet_id: SheetId,
        matches_col: F,
        out: &mut FxHashSet<usize>,
    ) where
        F: Fn(u32) -> bool,
    {
        for (&(entry_sheet, col), ids) in &self.whole_cols {
            if entry_sheet == sheet_id && matches_col(col) {
                Self::extend_ids(out, ids);
            }
        }
    }

    fn collect_whole_sheet(&self, sheet_id: SheetId, out: &mut FxHashSet<usize>) {
        if let Some(ids) = self.whole_sheets.get(&sheet_id) {
            Self::extend_ids(out, ids);
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
    pub(crate) domain: Region,
}

#[derive(Debug, Default)]
pub(crate) struct SpanDomainIndex {
    index: SheetRegionIndex<SpanDomainEntryId>,
    entries: Vec<SpanDomainEntry>,
    epoch: u64,
}

impl SpanDomainIndex {
    pub(crate) fn insert_domain(&mut self, span: FormulaSpanRef, domain: PlacementDomain) {
        let region = Region::from_domain(&domain);
        let id = SpanDomainEntryId(self.entries.len());
        self.entries.push(SpanDomainEntry {
            span,
            domain: region,
        });
        self.index.insert(region, id);
        self.epoch = self.epoch.saturating_add(1);
    }

    pub(crate) fn find_at(&self, coord: PlacementCoord) -> RegionQueryResult<SpanDomainEntry> {
        self.find_intersections(Region::point(coord.sheet_id, coord.row, coord.col))
    }

    pub(crate) fn find_intersections(&self, region: Region) -> RegionQueryResult<SpanDomainEntry> {
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
    Regions(Vec<Region>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SpanDependencyEntryId(usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpanDependencyEntry {
    pub(crate) span: FormulaSpanRef,
    pub(crate) precedent_region: Region,
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
        precedent_region: Region,
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
        changed: Region,
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
    pub(crate) domain: Region,
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
        let region = Region::from_domain(&domain);
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
        self.find_intersections(Region::point(coord.sheet_id, coord.row, coord.col))
    }

    pub(crate) fn find_intersections(
        &self,
        region: Region,
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
    fn axis_range_intersects_truth_table() {
        use AxisRange::*;

        assert!(Point(5).intersects(Point(5)));
        assert!(!Point(5).intersects(Point(6)));
        assert!(Point(5).intersects(Span(5, 8)));
        assert!(Point(5).intersects(Span(2, 5)));
        assert!(!Point(5).intersects(Span(6, 8)));
        assert!(Point(5).intersects(From(5)));
        assert!(!Point(5).intersects(From(6)));
        assert!(Point(5).intersects(To(5)));
        assert!(!Point(5).intersects(To(4)));
        assert!(Point(5).intersects(All));

        assert!(Span(3, 7).intersects(Point(3)));
        assert!(Span(3, 7).intersects(Point(7)));
        assert!(!Span(3, 7).intersects(Point(8)));
        assert!(Span(3, 7).intersects(Span(7, 9)));
        assert!(Span(3, 7).intersects(Span(1, 3)));
        assert!(!Span(3, 7).intersects(Span(8, 9)));
        assert!(Span(3, 7).intersects(From(7)));
        assert!(!Span(3, 7).intersects(From(8)));
        assert!(Span(3, 7).intersects(To(3)));
        assert!(!Span(3, 7).intersects(To(2)));
        assert!(Span(3, 7).intersects(All));

        assert!(From(10).intersects(Point(10)));
        assert!(!From(10).intersects(Point(9)));
        assert!(From(10).intersects(Span(8, 10)));
        assert!(!From(10).intersects(Span(8, 9)));
        assert!(From(10).intersects(From(20)));
        assert!(From(10).intersects(To(10)));
        assert!(!From(10).intersects(To(9)));
        assert!(From(10).intersects(All));

        assert!(To(10).intersects(Point(10)));
        assert!(!To(10).intersects(Point(11)));
        assert!(To(10).intersects(Span(10, 12)));
        assert!(!To(10).intersects(Span(11, 12)));
        assert!(To(10).intersects(From(10)));
        assert!(!To(10).intersects(From(11)));
        assert!(To(10).intersects(To(0)));
        assert!(To(10).intersects(All));

        assert!(All.intersects(Point(42)));
        assert!(All.intersects(Span(2, 3)));
        assert!(All.intersects(From(42)));
        assert!(All.intersects(To(42)));
        assert!(All.intersects(All));
    }

    #[test]
    fn axis_range_contains_each_kind() {
        use AxisRange::*;

        assert!(Point(5).contains(5));
        assert!(!Point(5).contains(4));
        assert!(Span(5, 8).contains(5));
        assert!(Span(5, 8).contains(8));
        assert!(!Span(5, 8).contains(9));
        assert!(From(5).contains(5));
        assert!(From(5).contains(u32::MAX));
        assert!(!From(5).contains(4));
        assert!(To(5).contains(5));
        assert!(To(5).contains(0));
        assert!(!To(5).contains(6));
        assert!(All.contains(0));
        assert!(All.contains(u32::MAX));
    }

    #[test]
    fn axis_range_query_bounds_each_kind() {
        use AxisRange::*;

        assert_eq!(Point(7).query_bounds(), (7, 7));
        assert_eq!(Span(3, 9).query_bounds(), (3, 9));
        assert_eq!(From(4).query_bounds(), (4, u32::MAX));
        assert_eq!(To(4).query_bounds(), (0, 4));
        assert_eq!(All.query_bounds(), (0, u32::MAX));
    }

    #[test]
    fn axis_range_is_bounded_only_for_point_and_span() {
        use AxisRange::*;

        assert!(Point(7).is_bounded());
        assert!(Span(3, 9).is_bounded());
        assert!(!From(4).is_bounded());
        assert!(!To(4).is_bounded());
        assert!(!All.is_bounded());
    }

    #[test]
    fn axis_range_project_through_offset_cases() {
        use AxisRange::*;

        for range in [Point(7), Span(10, 20), From(10), To(20), All] {
            assert_eq!(range.project_through_offset(0), Some(range));
        }

        assert_eq!(Span(10, 20).project_through_offset(5), Some(Span(15, 25)));
        assert_eq!(Span(10, 20).project_through_offset(-5), Some(Span(5, 15)));
        assert_eq!(Span(0, 10).project_through_offset(-5), Some(Span(0, 5)));
        assert_eq!(
            From(u32::MAX - 10).project_through_offset(100),
            Some(From(u32::MAX))
        );
        assert_eq!(
            To(10).project_through_offset(i64::from(u32::MAX)),
            Some(To(u32::MAX))
        );
        assert_eq!(Point(0).project_through_offset(-1), None);
        assert_eq!(Point(u32::MAX).project_through_offset(1), None);
        assert!(matches!(
            From(0).project_through_offset(100),
            Some(From(100))
        ));
    }

    #[test]
    fn axis_range_kind_tags() {
        use AxisRange::*;

        assert_eq!(Point(1).kind(), AxisKind::Point);
        assert_eq!(Span(1, 2).kind(), AxisKind::Span);
        assert_eq!(From(1).kind(), AxisKind::From);
        assert_eq!(To(1).kind(), AxisKind::To);
        assert_eq!(All.kind(), AxisKind::All);
    }

    #[test]
    fn region_pattern_axis_ranges_match_conversion_table() {
        use AxisRange::*;

        assert_eq!(Region::point(1, 2, 3).axis_ranges(), (Point(2), Point(3)));
        assert_eq!(
            Region::col_interval(1, 4, 5, 6).axis_ranges(),
            (Span(5, 6), Point(4))
        );
        assert_eq!(
            Region::row_interval(1, 4, 5, 6).axis_ranges(),
            (Point(4), Span(5, 6))
        );
        assert_eq!(
            Region::rect(1, 2, 3, 4, 5).axis_ranges(),
            (Span(2, 3), Span(4, 5))
        );
        assert_eq!(Region::rows_from(1, 9).axis_ranges(), (From(9), All));
        assert_eq!(Region::cols_from(1, 8).axis_ranges(), (All, From(8)));
        assert_eq!(Region::whole_row(1, 7).axis_ranges(), (Point(7), All));
        assert_eq!(Region::whole_col(1, 6).axis_ranges(), (All, Point(6)));
        assert_eq!(Region::whole_sheet(1).axis_ranges(), (All, All));
    }

    #[test]
    fn region_constructors_produce_expected_axis_ranges() {
        use AxisRange::*;

        assert_eq!(
            Region::point(1, 2, 3),
            Region {
                sheet_id: 1,
                rows: Point(2),
                cols: Point(3)
            }
        );
        assert_eq!(
            Region::rect(1, 2, 3, 4, 5),
            Region {
                sheet_id: 1,
                rows: Span(2, 3),
                cols: Span(4, 5)
            }
        );
        assert_eq!(
            Region::rows_from(1, 9),
            Region {
                sheet_id: 1,
                rows: From(9),
                cols: All
            }
        );
        assert_eq!(
            Region::cols_from(1, 8),
            Region {
                sheet_id: 1,
                rows: All,
                cols: From(8)
            }
        );
        assert_eq!(
            Region::whole_row(1, 7),
            Region {
                sheet_id: 1,
                rows: Point(7),
                cols: All
            }
        );
        assert_eq!(
            Region::whole_col(1, 6),
            Region {
                sheet_id: 1,
                rows: All,
                cols: Point(6)
            }
        );
        assert_eq!(
            Region::whole_sheet(1),
            Region {
                sheet_id: 1,
                rows: All,
                cols: All
            }
        );
        assert_eq!(
            Region::col_interval(1, 4, 5, 6),
            Region {
                sheet_id: 1,
                rows: Span(5, 6),
                cols: Point(4)
            }
        );
        assert_eq!(
            Region::row_interval(1, 4, 5, 6),
            Region {
                sheet_id: 1,
                rows: Point(4),
                cols: Span(5, 6)
            }
        );
    }

    #[test]
    fn rows_from_intersection_arithmetic() {
        let tail = Region::rows_from(1, 5);

        assert!(tail.intersects(&Region::rect(1, 3, 10, 0, 5)));
        assert!(!tail.intersects(&Region::rect(1, 0, 4, 0, 5)));
        assert!(tail.intersects(&Region::whole_sheet(1)));
        assert!(tail.intersects(&Region::point(1, 7, 3)));
        assert!(!tail.intersects(&Region::point(1, 2, 3)));
        assert!(tail.intersects(&Region::rows_from(1, 8)));
        assert!(tail.intersects(&Region::rows_from(1, 2)));
    }

    #[test]
    fn cols_from_intersection_arithmetic() {
        let tail = Region::cols_from(1, 5);

        assert!(tail.intersects(&Region::rect(1, 0, 5, 3, 10)));
        assert!(!tail.intersects(&Region::rect(1, 0, 5, 0, 4)));
        assert!(tail.intersects(&Region::whole_sheet(1)));
        assert!(tail.intersects(&Region::point(1, 3, 7)));
        assert!(!tail.intersects(&Region::point(1, 3, 2)));
        assert!(tail.intersects(&Region::cols_from(1, 8)));
        assert!(tail.intersects(&Region::cols_from(1, 2)));
    }

    #[test]
    fn rows_from_index_does_not_explode() {
        let start = Instant::now();
        let mut index = SheetRegionIndex::with_rect_bucket_size(64, 16);
        index.insert(Region::rows_from(1, 0), "rows_from_zero");
        index.insert(Region::rows_from(1, u32::MAX), "rows_from_max");

        let all_tail = index.query(Region::rows_from(1, 0));
        let max_point = index.query(Region::point(1, u32::MAX, 3));
        let before_max = index.query(Region::point(1, u32::MAX - 1, 3));
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
        index.insert(Region::cols_from(1, 0), "cols_from_zero");
        index.insert(Region::cols_from(1, u32::MAX), "cols_from_max");

        let all_tail = index.query(Region::cols_from(1, 0));
        let max_point = index.query(Region::point(1, 3, u32::MAX));
        let before_max = index.query(Region::point(1, 3, u32::MAX - 1));
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
        let changed = Region::rows_from(1, u32::MAX - 10);
        let read = Region::rows_from(1, u32::MAX - 10);
        let result = Region::rect(1, u32::MAX - 30, u32::MAX, 0, 0);
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
        index.insert(Region::point(1, 3, 4), "point");
        index.insert(Region::col_interval(1, 2, 0, 10), "col_interval");
        index.insert(Region::row_interval(1, 4, 0, 10), "row_interval");
        index.insert(Region::rect(1, 20, 29, 20, 29), "rect");
        index.insert(Region::whole_row(1, 7), "whole_row");
        index.insert(Region::whole_col(1, 8), "whole_col");
        index.insert(Region::whole_sheet(1), "whole_sheet");

        let point = index.query(Region::point(1, 3, 4));
        assert!(point.matches.iter().any(|m| m.value == "point"));
        assert!(point.matches.iter().any(|m| m.value == "whole_sheet"));

        let col = index.query(Region::point(1, 5, 2));
        assert!(col.matches.iter().any(|m| m.value == "col_interval"));

        let row = index.query(Region::point(1, 4, 5));
        assert!(row.matches.iter().any(|m| m.value == "row_interval"));

        let rect = index.query(Region::point(1, 25, 25));
        assert!(rect.matches.iter().any(|m| m.value == "rect"));

        let whole_row = index.query(Region::point(1, 7, 99));
        assert!(whole_row.matches.iter().any(|m| m.value == "whole_row"));

        let whole_col = index.query(Region::point(1, 99, 8));
        assert!(whole_col.matches.iter().any(|m| m.value == "whole_col"));
    }

    #[test]
    fn whole_sheet_query_returns_point_interval_rect_and_axis_entries() {
        let mut index = SheetRegionIndex::with_rect_bucket_size(10, 10);
        index.insert(Region::point(1, 3, 4), "point");
        index.insert(Region::col_interval(1, 2, 0, 10), "col_interval");
        index.insert(Region::row_interval(1, 4, 0, 10), "row_interval");
        index.insert(Region::rect(1, 20, 29, 20, 29), "rect");
        index.insert(Region::whole_row(1, 7), "whole_row");
        index.insert(Region::whole_col(1, 8), "whole_col");
        index.insert(Region::whole_sheet(1), "whole_sheet");

        let result = index.query(Region::whole_sheet(1));
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
        index.insert(Region::point(1, 0, 0), "sheet_1_point");
        index.insert(Region::col_interval(1, 0, 0, 10), "sheet_1_col");
        index.insert(Region::rect(1, 0, 10, 0, 10), "sheet_1_rect");
        index.insert(Region::whole_sheet(1), "sheet_1_whole");
        index.insert(Region::point(2, 0, 0), "sheet_2_point");

        let result = index.query(Region::whole_sheet(2));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value, "sheet_2_point");
    }

    #[test]
    fn sheet_region_index_no_under_return_matches_bruteforce_table() {
        let inserted = vec![
            (Region::point(1, 0, 0), "s1_point_origin"),
            (Region::point(1, 7, 7), "s1_point_far"),
            (Region::col_interval(1, 2, 1, 6), "s1_col_interval"),
            (Region::row_interval(1, 5, 1, 6), "s1_row_interval"),
            (Region::rect(1, 3, 5, 3, 5), "s1_rect_cross_bucket"),
            (Region::whole_row(1, 9), "s1_whole_row"),
            (Region::whole_col(1, 10), "s1_whole_col"),
            (Region::rows_from(1, 8), "s1_rows_from"),
            (Region::cols_from(1, 8), "s1_cols_from"),
            (Region::whole_sheet(1), "s1_whole_sheet"),
            (Region::point(2, 0, 0), "s2_point_origin"),
            (Region::rect(2, 3, 5, 3, 5), "s2_rect_cross_bucket"),
            (Region::whole_sheet(2), "s2_whole_sheet"),
        ];
        let queries = vec![
            Region::point(1, 0, 0),
            Region::point(1, 4, 4),
            Region::point(1, 5, 2),
            Region::point(1, 9, 99),
            Region::point(1, 99, 10),
            Region::rect(1, 4, 6, 4, 6),
            Region::whole_row(1, 5),
            Region::whole_col(1, 2),
            Region::rows_from(1, 8),
            Region::cols_from(1, 8),
            Region::whole_sheet(1),
            Region::point(2, 4, 4),
            Region::whole_sheet(2),
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
    fn axis_kind_dispatch_matrix_returns_correct_intersections() {
        let inserted_shapes = [
            ("Point", Region::point(1, 10, 10)),
            ("ColInterval", Region::col_interval(1, 12, 8, 14)),
            ("RowInterval", Region::row_interval(1, 12, 8, 14)),
            ("Rect", Region::rect(1, 20, 30, 20, 30)),
            ("RowsFrom", Region::rows_from(1, 40)),
            ("ColsFrom", Region::cols_from(1, 40)),
            ("WholeRow", Region::whole_row(1, 50)),
            ("WholeCol", Region::whole_col(1, 50)),
            ("WholeSheet", Region::whole_sheet(1)),
        ];
        let query_shapes = [
            ("Point", Region::point(1, 10, 10)),
            ("ColInterval", Region::col_interval(1, 12, 13, 15)),
            ("RowInterval", Region::row_interval(1, 12, 13, 15)),
            ("Rect", Region::rect(1, 25, 26, 25, 26)),
            ("RowsFrom", Region::rows_from(1, 45)),
            ("ColsFrom", Region::cols_from(1, 45)),
            ("WholeRow", Region::whole_row(1, 50)),
            ("WholeCol", Region::whole_col(1, 50)),
            ("WholeSheet", Region::whole_sheet(1)),
        ];

        for (insert_name, insert_region) in inserted_shapes {
            for (query_name, query_region) in query_shapes {
                let mut index = SheetRegionIndex::with_rect_bucket_size(4, 4);
                index.insert(insert_region, "inserted");

                let actual = index
                    .query(query_region)
                    .matches
                    .iter()
                    .any(|matched| matched.value == "inserted");
                let expected = insert_region.intersects(&query_region);

                assert_eq!(
                    actual, expected,
                    "insert={insert_name} query={query_name} insert_region={insert_region:?} query_region={query_region:?}"
                );
            }
        }
    }

    #[test]
    fn sheet_region_index_rect_bucket_boundary_queries_do_not_under_return() {
        let mut index = SheetRegionIndex::with_rect_bucket_size(4, 4);
        index.insert(Region::rect(1, 3, 4, 3, 4), "crossing_rect");
        index.insert(Region::rect(1, 0, 2, 0, 2), "unrelated_rect");

        for point in [
            Region::point(1, 3, 3),
            Region::point(1, 3, 4),
            Region::point(1, 4, 3),
            Region::point(1, 4, 4),
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
        index.insert(Region::rect(1, 0, 0, 0, 0), "hit");
        index.insert(Region::rect(1, 9, 9, 9, 9), "same_bucket_drop");

        let result = index.query(Region::point(1, 0, 0));

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

        let result = index.find_intersections(Region::rect(2, 11, 11, 21, 21));

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
            Region::col_interval(2, 0, 0, 9),
            DirtyProjection::SameRow,
        );

        let result = index.query_changed_region(Region::point(2, 4, 0));

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
        index.insert_dependency(span, Region::point(2, 0, 5), DirtyProjection::WholeTarget);

        let result = index.query_changed_region(Region::point(2, 0, 5));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.entry.span, span);
    }

    #[test]
    fn span_dependency_index_keeps_whole_column_bucket_separate() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(7);
        index.insert_dependency(
            span,
            Region::whole_col(2, 3),
            DirtyProjection::ConservativeWhole,
        );

        let result = index.query_changed_region(Region::point(2, 99, 3));

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

        let result = index.find_intersections(Region::rect(3, 15, 16, 15, 16));

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
            Region::rect(4, 0, 0, 0, 0),
            DirtyProjection::WholeTarget,
        );
        index.insert_dependency(
            drop,
            Region::rect(4, 63, 63, 15, 15),
            DirtyProjection::WholeTarget,
        );

        let result = index.query_changed_region(Region::point(4, 0, 0));

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
            Region::whole_sheet(2),
            DirtyProjection::UnsupportedUnbounded,
        );
        let result = index.query_changed_region(Region::point(2, 0, 0));

        assert!(inserted.is_none());
        assert!(result.matches.is_empty());
    }

    #[test]
    fn whole_column_dependency_query_marks_candidate_span() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(10);
        index.insert_dependency(
            span,
            Region::whole_col(5, 1),
            DirtyProjection::ConservativeWhole,
        );

        let result = index.query_changed_region(Region::rect(5, 100, 110, 1, 1));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.entry.span, span);
    }

    #[test]
    fn whole_row_dependency_query_marks_candidate_span() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(14);
        index.insert_dependency(span, Region::whole_row(2, 7), DirtyProjection::WholeTarget);

        let result = index.query_changed_region(Region::point(2, 7, 99));

        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].value.entry.span, span);
    }

    #[test]
    fn unrelated_edit_does_not_mark_span_dirty() {
        let mut index = SpanDependencyIndex::default();
        let span = span_ref(11);
        index.insert_dependency(
            span,
            Region::col_interval(6, 0, 0, 9),
            DirtyProjection::SameRow,
        );

        let result = index.query_changed_region(Region::point(6, 4, 1));

        assert!(result.matches.is_empty());
    }
}
