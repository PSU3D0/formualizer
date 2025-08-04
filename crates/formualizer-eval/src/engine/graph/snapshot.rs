use super::{AstNodeId, ValueRef};
use crate::{
    SheetId,
    engine::packed_coord::PackedCoord,
    engine::vertex::{VertexId, VertexKind},
};

/// Snapshot of a vertex's complete state for rollback purposes
#[derive(Debug, Clone)]
pub struct VertexSnapshot {
    pub coord: PackedCoord,
    pub sheet_id: SheetId,
    pub kind: VertexKind,
    pub flags: u8,
    pub value_ref: Option<ValueRef>,
    pub formula_ref: Option<AstNodeId>,
    pub out_edges: Vec<VertexId>,
}
