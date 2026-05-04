//! Graph-owned FormulaPlane authority shell.
//!
//! This is intentionally inert in FP6.5R Tranche 3: normal formula ingest still
//! materializes every formula into the legacy dependency graph. The authority
//! shell establishes ownership on `DependencyGraph` without making FormulaPlane
//! a runtime source of truth yet.

use super::runtime::FormulaPlane;

#[derive(Debug, Default)]
pub(crate) struct FormulaAuthority {
    pub(crate) plane: FormulaPlane,
}
