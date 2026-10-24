//! Workbook metadata shared by source ingestion and strict publication admission.
//! Parsed declarations never change source bytes or evaluation formula text.
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod dynamic;
pub(crate) mod names;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod strings;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod styles;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod tables;
#[cfg(feature = "xlsx-recalc")]
pub(in crate::xlsx) use strings::SharedStringTable;
#[cfg(feature = "xlsx-recalc")]
pub(in crate::xlsx) use styles::Styles;
#[cfg(test)]
#[cfg(feature = "xlsx-recalc")]
pub(in crate::xlsx) use styles::raw_attribute;
