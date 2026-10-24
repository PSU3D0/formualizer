//! Source-order formula transport, evidence and name-reference analysis.
//! Replay preserves source identity independently of engine family grouping.
pub(crate) mod evidence;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod formulas;
pub(crate) mod name_analysis;
pub(crate) mod name_references;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod names;
pub(crate) mod partition;
pub(crate) mod replay;
pub(crate) mod staging;
pub(crate) mod stream;
pub(crate) mod utilities;
#[cfg(feature = "xlsx-recalc")]
pub(crate) mod values;

pub(crate) fn shared_source_index(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}
