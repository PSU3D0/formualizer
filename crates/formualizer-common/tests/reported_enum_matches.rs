//! These integration tests compile as an external downstream crate. The wildcard
//! arms prove the required match shape; assertions cover only variants that exist now.

use formualizer_common::{
    A1ParseError, CoordError, ExcelErrorExtra, ExcelErrorKind, PlanStaleReason,
    PreparationStaleReason, ResourceExhaustionReason, SheetAddressError, ValueError,
};

fn error_kind_label(value: ExcelErrorKind) -> &'static str {
    match value {
        ExcelErrorKind::Value => "value",
        _ => "other",
    }
}

fn resource_reason_label(value: ResourceExhaustionReason) -> &'static str {
    match value {
        ResourceExhaustionReason::GraphEdges => "graph_edges",
        _ => "other",
    }
}

fn preparation_reason_label(value: PreparationStaleReason) -> &'static str {
    match value {
        PreparationStaleReason::Semantic => "semantic",
        _ => "other",
    }
}

fn plan_reason_label(value: PlanStaleReason) -> &'static str {
    match value {
        PlanStaleReason::Graph => "graph",
        _ => "other",
    }
}

fn extra_label(value: &ExcelErrorExtra) -> &'static str {
    match value {
        ExcelErrorExtra::None => "none",
        _ => "other",
    }
}

fn coord_error_label(value: CoordError) -> &'static str {
    match value {
        CoordError::NegativeRow(_) => "negative_row",
        _ => "other",
    }
}

fn a1_error_label(value: A1ParseError) -> &'static str {
    match value {
        A1ParseError::Empty => "empty",
        _ => "other",
    }
}

fn sheet_address_error_label(value: SheetAddressError) -> &'static str {
    match value {
        SheetAddressError::ZeroIndex => "zero_index",
        _ => "other",
    }
}

fn value_error_label(value: ValueError) -> &'static str {
    match value {
        ValueError::ImplicitIntersection(_) => "implicit_intersection",
        _ => "other",
    }
}

#[test]
fn external_wildcard_matches_compile_and_map_known_variants() {
    assert_eq!(error_kind_label(ExcelErrorKind::Value), "value");
    assert_eq!(
        resource_reason_label(ResourceExhaustionReason::GraphEdges),
        "graph_edges"
    );
    assert_eq!(
        preparation_reason_label(PreparationStaleReason::Semantic),
        "semantic"
    );
    assert_eq!(plan_reason_label(PlanStaleReason::Graph), "graph");
    assert_eq!(extra_label(&ExcelErrorExtra::None), "none");
    assert_eq!(
        coord_error_label(CoordError::NegativeRow(-1)),
        "negative_row"
    );
    assert_eq!(a1_error_label(A1ParseError::Empty), "empty");
    assert_eq!(
        sheet_address_error_label(SheetAddressError::ZeroIndex),
        "zero_index"
    );
    assert_eq!(
        value_error_label(ValueError::ImplicitIntersection("range".to_string())),
        "implicit_intersection"
    );
}
