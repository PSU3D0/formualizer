//! Passive formula-run detection used by `scan-formula-templates`.
//!
//! Moved here from `formualizer_eval::formula_plane` (Program 2, P2-M0): the
//! engine no longer uses it. It groups same-template formula cells into row and
//! column runs and counts candidate partitions. It is descriptive only.

pub mod ids;
#[allow(clippy::too_many_arguments)]
pub mod span_counters;
#[allow(clippy::too_many_arguments)]
pub mod span_store;

pub use ids::*;
pub use span_counters::*;
pub use span_store::*;
