//! Formula templates: the parts of the retired FormulaPlane that ingest and
//! the dependency authority still use.
//!
//! - `canonical`: canonical (relative, literal-erased) template form.
//! - `dependency_summary`: the per-template dependency analyzer.
//! - `slots`: literal and value-reference slot maps for templates.
//! - `read_summary`: read summaries and projections (shape memo).
//! - `region`: sheet regions used by dirty/virtual-dependency tracking.
//! - `domain`: placement domains and result regions.
//! - `relocate`: relocate a template AST to another placement.

pub(crate) mod canonical;
pub(crate) mod dependency_summary;
#[cfg(feature = "formula_plane_diagnostics")]
#[doc(hidden)]
pub mod diagnostics;
pub(crate) mod domain;
pub(crate) mod read_summary;
pub(crate) mod region;
#[doc(hidden)]
pub mod relocate;
pub(crate) mod slots;
