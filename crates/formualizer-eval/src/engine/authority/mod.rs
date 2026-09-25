//! Program 1 unified dependency authority (FORM-000169, design rev 3 §4–§5).
//!
//! Temporary and behind the never-default `unified_authority` feature. The
//! legacy graph stays the runtime evaluation path until M1b.

// Remove the temporary allowance when the evaluator calls the driver.
#[allow(dead_code)]
pub(crate) mod arc_emit;
// Remove the temporary allowance when the evaluator calls the driver.
#[allow(dead_code)]
pub(crate) mod arc_sweep;
// Remove the temporary allowance when the evaluator calls the driver.
#[allow(dead_code)]
pub(crate) mod arc_topology;
pub mod avl;
pub mod canon;
// Remove the temporary allowance when the evaluator calls the driver.
#[allow(dead_code)]
pub(crate) mod candidates;
pub mod dir;
pub mod dirty;
pub mod extract;
pub mod geom;
pub mod groups;
pub mod history;
pub mod host;
pub mod identity;
pub mod level_index;
// Remove the temporary allowance when the evaluator calls the driver.
#[allow(dead_code)]
pub(crate) mod plan_control;
pub(crate) mod plan_graph;
// Executor adaptation, pending complete runtime/resource cutover.
#[allow(dead_code)]
pub(crate) mod plan_schedule;
// Complete per-cell planning driver, pending evaluator/resource cutover.
#[allow(dead_code)]
pub(crate) mod planner;
pub mod probe;
pub mod proj;
// Remove the temporary allowance when the evaluator calls the driver.
#[allow(dead_code)]
pub(crate) mod refine;
pub mod slots;
pub mod store;
// Maintained symbol identities; executable relation/planner integration follows.
#[allow(dead_code)]
pub(crate) mod symbols;
pub mod template;

#[cfg(test)]
mod tests;
