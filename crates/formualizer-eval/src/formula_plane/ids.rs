//! Dependency-light Formula Plane V2 descriptors.
//!
//! See FORMULA_PLANE_V2.md.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Opaque identifier for a future Formula Plane V2 template.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FormulaTemplateId(pub u32);

/// Opaque identifier for a future Formula Plane V2 placement or run.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FormulaRunId(pub u32);

/// 128-bit formula-content fingerprint; construction is deferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FormulaFingerprint {
    pub hi: u64,
    pub lo: u64,
}

/// 128-bit dependency-shape fingerprint; construction is deferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DependencyShapeFingerprint {
    pub hi: u64,
    pub lo: u64,
}
