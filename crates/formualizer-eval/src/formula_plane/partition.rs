//! Provider-neutral partition identifiers for future Phase 8 partitioning.
//!
//! See PHASE_8_COMPATIBILITY_NOTES.md.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Opaque session-local identifier for a future Phase 8 partition.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PartitionId(pub u32);
