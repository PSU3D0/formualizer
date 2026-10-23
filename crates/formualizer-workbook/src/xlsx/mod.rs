//! XLSX source parsing and ingestion shared by loading and recalculation.
//! Source bytes remain immutable; evaluation text and publication edits are separate.
pub(crate) mod ingest;
#[cfg(feature = "xlsx-recalc")]
pub mod recalculate;
