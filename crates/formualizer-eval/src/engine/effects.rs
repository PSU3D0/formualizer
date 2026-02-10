//! Formal effects pipeline for evaluation (ticket 603).
//!
//! An `Effect` is an explicit, inspectable side-effect produced by formula
//! evaluation.  Effects are *planned* deterministically from
//! `(computed_value, workbook_state)` and *applied* sequentially.
//!
//! This separation enables:
//! - Parallel computation with sequential apply
//! - ChangeLog integration at the effect layer
//! - Deterministic replay without re-evaluation

use crate::engine::vertex::VertexId;
use crate::reference::CellRef;
use formualizer_common::LiteralValue;

/// An explicit, inspectable side-effect produced by formula evaluation.
#[derive(Debug, Clone)]
pub enum Effect {
    /// Write a scalar value (or error) to a formula/named vertex.
    WriteCell {
        vertex_id: VertexId,
        value: LiteralValue,
    },
    /// Commit a dynamic array spill to the grid.
    SpillCommit {
        anchor_vertex: VertexId,
        anchor_cell: CellRef,
        target_cells: Vec<CellRef>,
        values: Vec<Vec<LiteralValue>>,
    },
    /// Clear a previous spill region (before resize or scalar downgrade).
    SpillClear { anchor_vertex: VertexId },
}

/// A batch of effects from evaluating a single layer.
/// Each entry pairs a vertex with its planned effects.
pub type EffectBatch = Vec<(VertexId, Vec<Effect>)>;
