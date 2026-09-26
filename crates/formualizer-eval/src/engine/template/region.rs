//! Inert FormulaPlane sidecar region indexes for FP6.3.
//!
//! This module is internal FormulaPlane substrate only. It does not wire dirty
//! routing into the engine, graph, scheduler, or evaluator.

use crate::SheetId;

use super::domain::PlacementDomain;

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
    pub(crate) fn intersects(&self, other: &Self) -> bool {
        self.sheet_id == other.sheet_id
            && self.rows.intersects(other.rows)
            && self.cols.intersects(other.cols)
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

    /// Canonical axis-kind form: degenerate one-cell spans (`Span(x, x)`)
    /// become `Point(x)` on each axis. Semantically a no-op
    /// (`intersects`/`contains` already treat them identically), but the
    /// index's routing is kind-driven: a single-column finite range left as
    /// `(Span, Span)` lands in the coarse 64x16 rect buckets where it
    /// over-candidates every point query in the same bucket column, instead
    /// of the precise per-column interval tree.
    #[inline]
    pub(crate) fn normalized(self) -> Self {
        Self {
            sheet_id: self.sheet_id,
            rows: self.rows.normalized(),
            cols: self.cols.normalized(),
        }
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
    pub(crate) fn query_bounds(self) -> (u32, u32) {
        match self {
            Self::Point(point) => (point, point),
            Self::Span(start, end) => (start, end),
            Self::From(start) => (start, u32::MAX),
            Self::To(end) => (0, end),
            Self::All => (0, u32::MAX),
        }
    }

    /// Degenerate one-coordinate spans become points; see
    /// [`Region::normalized`].
    #[inline]
    pub(crate) fn normalized(self) -> Self {
        match self {
            Self::Span(start, end) if start == end => Self::Point(start),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

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
    fn axis_range_query_bounds_each_kind() {
        use AxisRange::*;

        assert_eq!(Point(7).query_bounds(), (7, 7));
        assert_eq!(Span(3, 9).query_bounds(), (3, 9));
        assert_eq!(From(4).query_bounds(), (4, u32::MAX));
        assert_eq!(To(4).query_bounds(), (0, 4));
        assert_eq!(All.query_bounds(), (0, u32::MAX));
    }
}

#[cfg(test)]
mod structural_stripe_tests {}
