pub mod aggregate;
pub mod combinatorics;
pub mod criteria_aggregates;
pub mod numeric;
pub mod reduction;
pub mod trig;

pub use aggregate::*;
pub use combinatorics::*;
pub use criteria_aggregates::*;
pub use trig::*;

pub(super) enum AggregateArgument<'a> {
    Range(crate::engine::range_view::RangeView<'a>),
    Scalar(formualizer_common::LiteralValue),
    ReferenceError(formualizer_common::ExcelError),
}

pub(super) fn resolve_aggregate_argument<'a, 'b>(
    arg: &crate::traits::ArgumentHandle<'a, 'b>,
    _ctx: &dyn crate::traits::FunctionContext<'b>,
) -> Result<AggregateArgument<'b>, formualizer_common::ExcelError> {
    use crate::traits::{CalcValue, ResolvedArgument};

    match arg.resolve_once()? {
        ResolvedArgument::Range(view) | ResolvedArgument::Value(CalcValue::Range(view)) => {
            Ok(AggregateArgument::Range(view))
        }
        ResolvedArgument::ReferenceError(error) => Ok(AggregateArgument::ReferenceError(error)),
        ResolvedArgument::Value(value) => Ok(AggregateArgument::Scalar(value.into_literal())),
    }
}

/// Call the nested registration functions for built-in math functions.
pub fn register_builtins() {
    aggregate::register_builtins();
    combinatorics::register_builtins();
    criteria_aggregates::register_builtins();
    reduction::register_builtins();
    numeric::register_builtins();
    trig::register_builtins();
}
