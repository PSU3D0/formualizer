//! Frozen pre-streaming implementations, compiled only by the parent test module.
//! Live production readers must never route through these reference paths.
pub(in crate::xlsx) mod ingestion_view;

pub(in crate::xlsx) mod worksheet_legacy;
pub(in crate::xlsx) use crate::xlsx::worksheet::oracle_loop as worksheet_intermediate;
pub(in crate::xlsx) mod xml_legacy;
pub(in crate::xlsx) use super::scan_differential::{shadow_scan, shadow_walk};
