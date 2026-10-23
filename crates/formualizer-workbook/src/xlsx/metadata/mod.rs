//! Workbook metadata shared by source ingestion and strict publication admission.
//! Parsed declarations never change source bytes or evaluation formula text.
pub(crate) mod dynamic;
pub(crate) mod strings;
pub(crate) mod styles;
pub(crate) mod tables;
pub(in crate::xlsx) use strings::SharedStringTable;
pub(in crate::xlsx) use styles::Styles;
#[cfg(test)]
pub(in crate::xlsx) use styles::raw_attribute;
