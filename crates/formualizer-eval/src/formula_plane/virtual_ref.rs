//! Provider-neutral descriptors for future provider-backed virtual references.
//!
//! See VIRTUAL_REFERENCES.md.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Session-local handle for a future provider-backed virtual source.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VirtualSourceId(pub u32);

/// Session-local handle for a future provider-backed virtual reference.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VirtualRangeId(pub u32);

/// Stable persisted identity for a future virtual source.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VirtualSourceKey {
    pub namespace: String,
    pub name: String,
}

/// Provider-supplied version token for immutable or versioned virtual data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct VirtualProviderVersion {
    pub fingerprint_hi: u64,
    pub fingerprint_lo: u64,
}

/// Descriptor kind for a future virtual reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum VirtualReferenceKind {
    Scalar,
    Range,
    Column,
    Table,
    DataFrame,
}

/// Versioning semantics for a future virtual reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum VirtualReferenceVolatility {
    Immutable,
    Versioned,
    Volatile,
}

/// Error category descriptor for future virtual provider failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum VirtualReferenceErrorKind {
    MissingProvider,
    MissingReference,
    ShapeMismatch,
    UnsupportedCapability,
    ProviderError,
}
