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
use std::sync::{Arc, Mutex};

/// Opaque, one-use read evidence paired with its final planned anchor value.
/// Only the dynamic-read planner creates these; ordinary and synthetic writes
/// cannot borrow pending evidence by vertex ID.
#[doc(hidden)]
#[derive(Debug)]
pub struct RecordedResult {
    vertex: VertexId,
    value: LiteralValue,
    pass: Arc<()>,
    reads: Mutex<Option<Vec<crate::engine::authority::host::ObservedRect>>>,
}

impl RecordedResult {
    pub(crate) fn new(
        vertex: VertexId,
        value: LiteralValue,
        pass: Arc<()>,
        reads: Vec<crate::engine::authority::host::ObservedRect>,
    ) -> Self {
        Self {
            vertex,
            value,
            pass,
            reads: Mutex::new(Some(reads)),
        }
    }

    pub(crate) fn vertex(&self) -> VertexId {
        self.vertex
    }
    pub(crate) fn value(&self) -> &LiteralValue {
        &self.value
    }
    pub(crate) fn belongs_to_pass(&self, pass: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.pass, pass)
    }
    pub(crate) fn take_reads(&self) -> Option<Vec<crate::engine::authority::host::ObservedRect>> {
        self.reads.lock().unwrap().take()
    }
}

/// An explicit, inspectable side-effect produced by formula evaluation.
#[derive(Debug, Clone)]
pub enum Effect {
    /// Write a scalar value (or error) to a formula/named vertex.
    WriteCell {
        vertex_id: VertexId,
        value: LiteralValue,
    },
    /// Write the designated evaluated result, carrying its own read proof.
    /// Clones share one-use evidence and cannot change the paired value.
    #[doc(hidden)]
    WriteRecordedCell { result: Arc<RecordedResult> },
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
