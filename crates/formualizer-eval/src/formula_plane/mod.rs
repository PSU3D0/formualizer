//! FormulaPlane bridge primitives.
//!
//! These descriptors are intentionally hosted in `formualizer-eval` while the
//! FormulaPlane bridge is experimental. They are runtime/planning concepts, not
//! stable cross-crate common types yet.

pub mod grid;
pub mod ids;
pub mod partition;
pub mod virtual_ref;

pub use grid::*;
pub use ids::*;
pub use partition::*;
pub use virtual_ref::*;
