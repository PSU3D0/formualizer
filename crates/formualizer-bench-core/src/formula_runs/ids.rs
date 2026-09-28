//! Opaque ids for templates and runs in a [`super::FormulaRunStore`].

/// Opaque identifier for a formula template in a run store.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FormulaTemplateId(pub u32);

impl FormulaTemplateId {
    /// The store-local value of this id.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

/// Opaque identifier for a formula run in a run store.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FormulaRunId(pub u32);

impl FormulaRunId {
    /// The store-local value of this id.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}
