use proptest::prelude::*;

use super::region_index::{AxisKind, AxisRange};

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
}
