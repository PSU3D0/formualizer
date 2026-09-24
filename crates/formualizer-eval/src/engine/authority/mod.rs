//! Program 1 unified dependency authority (FORM-000169, design rev 3 §4–§5).
//!
//! Temporary and behind the never-default `unified_authority` feature. The
//! legacy graph stays the runtime evaluation path until M1b.

pub mod avl;
pub mod canon;
pub mod dir;
pub mod dirty;
pub mod extract;
pub mod geom;
pub mod groups;
pub mod host;
pub mod identity;
pub mod level_index;
// S2 graph-kernel checkpoint; remove when the ARC driver lands.
#[allow(dead_code)]
pub(crate) mod plan_graph;
pub mod probe;
pub mod proj;
// S2 input checkpoint; remove this allowance when the ARC driver lands.
#[allow(dead_code)]
pub(crate) mod refine;
pub mod slots;
pub mod store;
pub mod template;

#[cfg(test)]
mod tests;
