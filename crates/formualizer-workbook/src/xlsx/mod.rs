//! XLSX source parsing and ingestion shared by loading and recalculation.
//! Source bytes remain immutable; evaluation text and publication edits are separate.
mod error;
pub(crate) mod ingest;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod loader;

pub(crate) mod metadata;
pub(crate) mod options;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod package;
#[cfg(feature = "xlsx-recalc")]
pub mod recalculate;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod worksheet;
pub(crate) mod xml;

pub(crate) mod source;
pub mod types;
