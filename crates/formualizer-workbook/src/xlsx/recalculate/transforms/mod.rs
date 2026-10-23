//! Evaluation-only formula text transformations.
//! Source marks and text remain immutable; publication always patches original bytes.
pub(in crate::xlsx) mod legacy;
pub(in crate::xlsx) mod tables;
