//! In-crate tests of the source-preserving spill internals: admission,
//! ingestion, geometry plans, metadata binding and ZIP additions. Public
//! end-to-end cases live in `tests/xlsx_source_recalculate_spills.rs`.
mod dynamic_admission;
mod dynamic_ingestion;
mod dynamic_publication;
mod index_scope;
mod metadata_binding;
mod native_ingestion;
mod native_metadata;
mod native_names;
mod native_values;
pub(in crate::xlsx) mod scan_differential;
mod zip_additions;
mod zip_containers;
