//! Program 1 unified dependency authority (FORM-000169, design rev 3 §4–§5).
//!
//! Temporary and behind the never-default `unified_authority` feature. The
//! legacy graph stays the runtime evaluation path until M1b.

pub mod avl;
pub mod geom;
pub mod identity;
pub mod proj;

#[cfg(test)]
mod tests;
