//! Shared policy-driven resolution of open spreadsheet ranges.
//!
//! Used-coordinate discovery remains owned by each storage/index authority. This
//! module centralizes how omitted sides, used minima/maxima, and compatibility
//! fallbacks turn those hints into a finite rectangle.

/// Inclusive bounds of a potentially open range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OpenRangeBounds {
    pub(crate) start_row: Option<u32>,
    pub(crate) start_column: Option<u32>,
    pub(crate) end_row: Option<u32>,
    pub(crate) end_column: Option<u32>,
}

/// Inclusive finite bounds produced by [`resolve_used_extent`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedExtent {
    pub(crate) start_row: u32,
    pub(crate) start_column: u32,
    pub(crate) end_row: u32,
    pub(crate) end_column: u32,
}

/// Compatibility policy for resolving omitted range sides.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExtentPolicy {
    /// Runtime evaluation behavior: omitted starts anchor at 1 and omitted ends
    /// use a used maximum, then the configured fallback when the sheet exists.
    EvaluationCompat {
        fallback_row: Option<u32>,
        fallback_column: Option<u32>,
    },
    /// Virtual dependency discovery historically narrows omitted starts to a
    /// used minimum when available. This is intentionally not evaluation policy.
    VirtualDependencyCompat {
        fallback_row: Option<u32>,
        fallback_column: Option<u32>,
    },
    /// Graph self-range checks use 0-based coordinates and query both used axes
    /// against the original range before resolving either axis.
    GraphCompat {
        fallback_row: u32,
        fallback_column: u32,
    },
    /// Inspection semantics: anchor omitted starts at 1, clamp omitted ends to
    /// used maxima, and return no extent for an empty constrained area.
    #[allow(dead_code)]
    Semantic,
}

/// Resolve a potentially open range without materializing any cells.
///
/// The callbacks return inclusive min/max used coordinates in the same basis as
/// `bounds`. They are invoked only for an omitted side that needs a used hint,
/// except under `GraphCompat`, which preserves its eager two-axis queries.
pub(crate) fn resolve_used_extent(
    bounds: OpenRangeBounds,
    policy: ExtentPolicy,
    mut used_rows_for_columns: impl FnMut(u32, u32) -> Option<(u32, u32)>,
    mut used_columns_for_rows: impl FnMut(u32, u32) -> Option<(u32, u32)>,
) -> Option<ResolvedExtent> {
    if let ExtentPolicy::GraphCompat {
        fallback_row,
        fallback_column,
    } = policy
    {
        let row_query = (
            bounds.start_row.unwrap_or(0),
            bounds.end_row.unwrap_or(fallback_row),
        );
        let column_query = (
            bounds.start_column.unwrap_or(0),
            bounds.end_column.unwrap_or(fallback_column),
        );
        let used_rows = used_rows_for_columns(column_query.0, column_query.1);
        let used_columns = used_columns_for_rows(row_query.0, row_query.1);
        let extent = ResolvedExtent {
            start_row: bounds.start_row.unwrap_or(0),
            end_row: bounds
                .end_row
                .or_else(|| used_rows.map(|used| used.1))
                .unwrap_or(fallback_row),
            start_column: bounds.start_column.unwrap_or(0),
            end_column: bounds
                .end_column
                .or_else(|| used_columns.map(|used| used.1))
                .unwrap_or(fallback_column),
        };
        return extent.is_ordered().then_some(extent);
    }

    let (origin, fallback_row, fallback_column, use_used_minimum) = match policy {
        ExtentPolicy::EvaluationCompat {
            fallback_row,
            fallback_column,
        } => (1, fallback_row, fallback_column, false),
        ExtentPolicy::VirtualDependencyCompat {
            fallback_row,
            fallback_column,
        } => (1, fallback_row, fallback_column, true),
        ExtentPolicy::Semantic => (1, None, None, false),
        ExtentPolicy::GraphCompat { .. } => unreachable!(),
    };

    let mut start_row = bounds.start_row;
    let mut start_column = bounds.start_column;
    let mut end_row = bounds.end_row;
    let mut end_column = bounds.end_column;

    if start_row.is_none() && end_row.is_none() {
        let first_column = start_column.unwrap_or(origin);
        let last_column = end_column.unwrap_or(first_column);
        if let Some((used_minimum, used_maximum)) = used_rows_for_columns(first_column, last_column)
        {
            start_row = Some(if use_used_minimum {
                used_minimum
            } else {
                origin
            });
            end_row = Some(used_maximum);
        } else if let Some(fallback) = fallback_row {
            start_row = Some(origin);
            end_row = Some(fallback);
        } else {
            start_row = Some(origin);
        }
    }

    if start_column.is_none() && end_column.is_none() {
        let first_row = start_row.unwrap_or(origin);
        let last_row = end_row.unwrap_or(first_row);
        if let Some((used_minimum, used_maximum)) = used_columns_for_rows(first_row, last_row) {
            start_column = Some(if use_used_minimum {
                used_minimum
            } else {
                origin
            });
            end_column = Some(used_maximum);
        } else if let Some(fallback) = fallback_column {
            start_column = Some(origin);
            end_column = Some(fallback);
        } else {
            start_column = Some(origin);
        }
    }

    if start_row.is_some() && end_row.is_none() {
        let first_column = start_column.unwrap_or(origin);
        let last_column = end_column.unwrap_or(first_column);
        end_row = used_rows_for_columns(first_column, last_column)
            .map(|used| used.1)
            .or(fallback_row);
    }
    if end_row.is_some() && start_row.is_none() {
        if use_used_minimum {
            let first_column = start_column.unwrap_or(origin);
            let last_column = end_column.unwrap_or(first_column);
            start_row = used_rows_for_columns(first_column, last_column)
                .map(|used| used.0)
                .or(Some(origin));
        } else {
            start_row = Some(origin);
        }
    }
    if start_column.is_some() && end_column.is_none() {
        let first_row = start_row.unwrap_or(origin);
        let last_row = end_row.unwrap_or(first_row);
        end_column = used_columns_for_rows(first_row, last_row)
            .map(|used| used.1)
            .or(fallback_column);
    }
    if end_column.is_some() && start_column.is_none() {
        if use_used_minimum {
            let first_row = start_row.unwrap_or(origin);
            let last_row = end_row.unwrap_or(first_row);
            start_column = used_columns_for_rows(first_row, last_row)
                .map(|used| used.0)
                .or(Some(origin));
        } else {
            start_column = Some(origin);
        }
    }

    let extent = ResolvedExtent {
        start_row: start_row.unwrap_or(origin),
        start_column: start_column.unwrap_or(origin),
        end_row: end_row.unwrap_or_else(|| start_row.unwrap_or(origin).saturating_sub(1)),
        end_column: end_column.unwrap_or_else(|| start_column.unwrap_or(origin).saturating_sub(1)),
    };
    extent.is_ordered().then_some(extent)
}

impl ResolvedExtent {
    fn is_ordered(self) -> bool {
        self.start_row <= self.end_row && self.start_column <= self.end_column
    }

    #[allow(dead_code)]
    pub(crate) fn cell_count(self) -> u64 {
        u64::from(self.end_row - self.start_row + 1)
            * u64::from(self.end_column - self.start_column + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic(
        bounds: OpenRangeBounds,
        used_rows: Option<(u32, u32)>,
        used_columns: Option<(u32, u32)>,
    ) -> Option<ResolvedExtent> {
        resolve_used_extent(
            bounds,
            ExtentPolicy::Semantic,
            |_, _| used_rows,
            |_, _| used_columns,
        )
    }

    #[test]
    fn semantic_whole_column_anchors_at_one_and_includes_interior_empty_cells() {
        let extent = semantic(
            OpenRangeBounds {
                start_row: None,
                start_column: Some(1),
                end_row: None,
                end_column: Some(1),
            },
            Some((100, 100)),
            None,
        )
        .unwrap();
        assert_eq!(
            extent,
            ResolvedExtent {
                start_row: 1,
                start_column: 1,
                end_row: 100,
                end_column: 1,
            }
        );
        assert_eq!(extent.cell_count(), 100);
    }

    #[test]
    fn semantic_empty_whole_axis_has_no_extent_and_zero_cells() {
        let extent = semantic(
            OpenRangeBounds {
                start_row: None,
                start_column: Some(1),
                end_row: None,
                end_column: Some(1),
            },
            None,
            None,
        );
        assert_eq!(extent, None);
        assert_eq!(extent.map(ResolvedExtent::cell_count).unwrap_or(0), 0);
    }

    #[test]
    fn semantic_clamps_only_the_omitted_side() {
        let extent = semantic(
            OpenRangeBounds {
                start_row: Some(40),
                start_column: Some(2),
                end_row: None,
                end_column: Some(3),
            },
            Some((9, 75)),
            None,
        )
        .unwrap();
        assert_eq!((extent.start_row, extent.end_row), (40, 75));
        assert_eq!((extent.start_column, extent.end_column), (2, 3));
    }

    #[test]
    fn semantic_whole_row_anchors_column_at_one() {
        let extent = semantic(
            OpenRangeBounds {
                start_row: Some(7),
                start_column: None,
                end_row: Some(7),
                end_column: None,
            },
            None,
            Some((12, 20)),
        )
        .unwrap();
        assert_eq!((extent.start_column, extent.end_column), (1, 20));
        assert_eq!(extent.cell_count(), 20);
    }
}
