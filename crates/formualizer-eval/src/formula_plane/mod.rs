//! FormulaPlane bridge primitives.
//!
//! These descriptors are intentionally hosted in `formualizer-eval` while the
//! FormulaPlane bridge is experimental. They are runtime/planning concepts, not
//! stable cross-crate common types yet.

#[cfg(test)]
mod axis_range_proptest;

pub(crate) mod authority;
pub(crate) mod dependency_summary;
#[cfg(feature = "formula_plane_diagnostics")]
#[doc(hidden)]
pub mod diagnostics;
pub mod grid;
pub mod ids;
pub mod partition;
pub(crate) mod placement;
pub(crate) mod producer;
pub(crate) mod region_index;
pub(crate) mod runtime;
pub(crate) mod scheduler;
pub mod span_counters;
pub(crate) mod span_eval;
pub mod span_store;
pub(crate) mod structural;
pub(crate) mod template_canonical;
pub mod virtual_ref;

pub use grid::*;
pub use ids::*;
pub use partition::*;
pub use span_counters::*;
pub use span_store::*;
pub use virtual_ref::*;
