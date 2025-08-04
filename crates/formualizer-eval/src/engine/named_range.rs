use formualizer_core::{ASTNode, parser::ReferenceType};
use rustc_hash::FxHashSet;

use crate::{CellRef, RangeRef, SheetId, engine::VertexId};

/// Scope of a named range
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NameScope {
    /// Available throughout workbook
    Workbook,
    /// Only available in specific sheet
    Sheet(SheetId),
}

/// Definition of what a name refers to
#[derive(Debug, Clone, PartialEq)]
pub enum NamedDefinition {
    /// Direct reference to a single cell
    Cell(CellRef),
    /// Reference to a range of cells
    Range(RangeRef),
    /// Named formula (evaluates to value/range)
    Formula {
        ast: ASTNode,
        /// Cached dependencies from last parse
        dependencies: Vec<VertexId>,
        /// Cached range dependencies
        range_deps: Vec<ReferenceType>,
    },
}

/// Complete named range entry
#[derive(Debug, Clone)]
pub struct NamedRange {
    pub definition: NamedDefinition,
    pub scope: NameScope,
    /// Formulas that reference this name (for invalidation)
    pub dependents: FxHashSet<VertexId>,
}
