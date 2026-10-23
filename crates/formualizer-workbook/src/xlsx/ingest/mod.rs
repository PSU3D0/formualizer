//! Source-order formula transport, evidence and name-reference analysis.
//! Replay preserves source identity independently of engine family grouping.
pub(crate) mod evidence;
pub(crate) mod name_references;
pub(crate) mod replay;
pub(crate) mod utilities;

pub(crate) fn shared_source_index(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}
