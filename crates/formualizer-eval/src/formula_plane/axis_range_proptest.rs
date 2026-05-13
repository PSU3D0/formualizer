use std::collections::HashSet;

use proptest::prelude::*;

use super::region_index::{AxisKind, AxisRange, Region, SheetRegionIndex};

fn any_axis_range() -> impl Strategy<Value = AxisRange> {
    prop_oneof![
        any::<u32>().prop_map(AxisRange::Point),
        (any::<u32>(), any::<u32>()).prop_map(|(a, b)| {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            AxisRange::Span(lo, hi)
        }),
        any::<u32>().prop_map(AxisRange::From),
        any::<u32>().prop_map(AxisRange::To),
        Just(AxisRange::All),
    ]
}

fn axis_range_projection_stays_in_bounds(range: AxisRange, offset: i64) -> bool {
    fn coord_stays_in_bounds(coord: u32, offset: i64) -> bool {
        let shifted = i128::from(coord) + i128::from(offset);
        (0..=i128::from(u32::MAX)).contains(&shifted)
    }

    match range {
        AxisRange::Point(point) => coord_stays_in_bounds(point, offset),
        AxisRange::Span(start, end) => {
            coord_stays_in_bounds(start, offset) && coord_stays_in_bounds(end, offset)
        }
        AxisRange::From(start) => coord_stays_in_bounds(start, offset),
        AxisRange::To(end) => coord_stays_in_bounds(end, offset),
        AxisRange::All => true,
    }
}

fn any_currently_constructible_region() -> impl Strategy<Value = Region> {
    let small = 0u32..20;
    prop_oneof![
        (1u16..3, small.clone(), small.clone())
            .prop_map(|(sheet_id, row, col)| Region::point(sheet_id, row, col)),
        (1u16..3, small.clone(), small.clone(), small.clone()).prop_map(
            |(sheet_id, col, row_start, row_end)| {
                let (lo, hi) = if row_start <= row_end {
                    (row_start, row_end)
                } else {
                    (row_end, row_start)
                };
                Region::col_interval(sheet_id, col, lo, hi)
            },
        ),
        (1u16..3, small.clone(), small.clone(), small.clone()).prop_map(
            |(sheet_id, row, col_start, col_end)| {
                let (lo, hi) = if col_start <= col_end {
                    (col_start, col_end)
                } else {
                    (col_end, col_start)
                };
                Region::row_interval(sheet_id, row, lo, hi)
            },
        ),
        (
            1u16..3,
            small.clone(),
            small.clone(),
            small.clone(),
            small.clone(),
        )
            .prop_map(|(sheet_id, row_start, row_end, col_start, col_end)| {
                let (row_lo, row_hi) = if row_start <= row_end {
                    (row_start, row_end)
                } else {
                    (row_end, row_start)
                };
                let (col_lo, col_hi) = if col_start <= col_end {
                    (col_start, col_end)
                } else {
                    (col_end, col_start)
                };
                Region::rect(sheet_id, row_lo, row_hi, col_lo, col_hi)
            }),
        (1u16..3, small.clone()).prop_map(|(sheet_id, row)| Region::rows_from(sheet_id, row)),
        (1u16..3, small.clone()).prop_map(|(sheet_id, col)| Region::cols_from(sheet_id, col)),
        (1u16..3, small.clone()).prop_map(|(sheet_id, row)| Region::whole_row(sheet_id, row)),
        (1u16..3, small.clone()).prop_map(|(sheet_id, col)| Region::whole_col(sheet_id, col)),
        (1u16..3).prop_map(Region::whole_sheet),
    ]
}

proptest! {
    #[test]
    fn intersects_commutes(a in any_axis_range(), b in any_axis_range()) {
        prop_assert_eq!(a.intersects(b), b.intersects(a));
    }

    #[test]
    fn contains_iff_intersects_with_point(r in any_axis_range(), c: u32) {
        prop_assert_eq!(r.contains(c), r.intersects(AxisRange::Point(c)));
    }

    #[test]
    fn project_zero_offset_is_identity(r in any_axis_range()) {
        prop_assert_eq!(r.project_through_offset(0), Some(r));
    }

    #[test]
    fn from_projection_no_overflow(start: u32, offset in -10_000i64..10_000) {
        let r = AxisRange::From(start);
        let _ = r.project_through_offset(offset);
        let r = AxisRange::To(start);
        let _ = r.project_through_offset(offset);
        let r = AxisRange::Span(start, start.saturating_add(100));
        let _ = r.project_through_offset(offset);
    }

    #[test]
    fn projection_composition_is_offset_sum(
        r in any_axis_range(),
        o1 in -1_000_000i64..1_000_000,
        o2 in -1_000_000i64..1_000_000,
    ) {
        let Some(sum) = o1.checked_add(o2) else {
            return Ok(());
        };
        prop_assume!(axis_range_projection_stays_in_bounds(r, o1));
        prop_assume!(axis_range_projection_stays_in_bounds(r, sum));

        let first = r.project_through_offset(o1);
        if let Some(first_range) = first {
            prop_assume!(axis_range_projection_stays_in_bounds(first_range, o2));
        }

        let composed = first.and_then(|first_range| first_range.project_through_offset(o2));
        let direct = r.project_through_offset(sum);
        prop_assert_eq!(composed, direct);
    }

    #[test]
    fn projection_no_panic_for_any_axis_range_and_bounded_offset(
        r in any_axis_range(),
        offset in -2_147_483_648i64..=2_147_483_647i64,
    ) {
        let _ = r.project_through_offset(offset);
    }

    #[test]
    fn intersect_query_bounds_consistent(a in any_axis_range(), b in any_axis_range()) {
        if a.intersects(b) {
            let (a_lo, a_hi) = a.query_bounds();
            let (b_lo, b_hi) = b.query_bounds();
            prop_assert!(a_lo <= b_hi && b_lo <= a_hi);
        }
    }

    #[test]
    fn kind_matches_variant(r in any_axis_range()) {
        let expected = match r {
            AxisRange::Point(_) => AxisKind::Point,
            AxisRange::Span(_, _) => AxisKind::Span,
            AxisRange::From(_) => AxisKind::From,
            AxisRange::To(_) => AxisKind::To,
            AxisRange::All => AxisKind::All,
        };
        prop_assert_eq!(r.kind(), expected);
    }

    #[test]
    fn region_index_query_returns_all_intersecting(
        indexed in proptest::collection::vec(any_currently_constructible_region(), 0..50),
        query in any_currently_constructible_region(),
    ) {
        let mut index = SheetRegionIndex::new();
        for (value, region) in indexed.iter().enumerate() {
            index.insert(*region, value);
        }

        let result = index.query(query);
        let result_values: HashSet<usize> = result.matches.iter().map(|matched| matched.value).collect();
        let expected: HashSet<usize> = indexed
            .iter()
            .enumerate()
            .filter(|(_value, region)| region.intersects(&query))
            .map(|(value, _region)| value)
            .collect();

        prop_assert_eq!(expected, result_values);
    }

    #[test]
    fn shifted_region_intersects_consistently(
        region in any_currently_constructible_region(),
        row_delta in -1000i64..1000,
        col_delta in -1000i64..1000,
    ) {
        if let Some(shifted) = region.project_through_axis_shift(row_delta, col_delta) {
            prop_assert_eq!(shifted.sheet_id, region.sheet_id);
            if row_delta == 0 && col_delta == 0 {
                prop_assert_eq!(shifted, region);
            }
        }
    }
}
