//! Centralized re-exports for Arrow compute affordances used by fast paths.
//! Pin to arrow-rs 56.x modules so call sites stay tidy.

pub use arrow_cast::cast::cast_with_options;

pub use arrow_select::concat::concat as concat_arrays;
pub use arrow_select::filter::filter as filter_array;
pub use arrow_select::zip::zip as zip_select;

pub use arrow_array::ArrayRef;
pub use arrow_schema::DataType;

pub mod boolean {
    pub use arrow::compute::kernels::boolean::{and_kleene, not, or_kleene};
}

pub mod cmp {
    pub use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
}

pub mod comparison {
    pub use arrow::compute::kernels::comparison::{
        like, regexp_is_match, regexp_is_match_scalar, starts_with,
    };
}

/// Temporal casting affordance – call when a temporal kernel is required.
pub fn cast_temporal_if_needed(arr: &ArrayRef, target: &DataType) -> ArrayRef {
    use arrow_cast::cast::CastOptions;
    if matches!(
        target,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    ) {
        cast_with_options(
            arr,
            target,
            &CastOptions {
                safe: false,
                format_options: Default::default(),
            },
        )
        .expect("temporal cast")
    } else {
        arr.clone()
    }
}
