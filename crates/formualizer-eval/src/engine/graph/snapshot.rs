use super::{AstNodeId, ValueRef};
use crate::{
    SheetId,
    engine::vertex::{VertexId, VertexKind},
};
use formualizer_common::Coord as AbsCoord;

/// Snapshot of a vertex's complete state for rollback purposes
#[derive(Debug, Clone)]
pub struct VertexSnapshot {
    pub coord: AbsCoord,
    pub sheet_id: SheetId,
    pub kind: VertexKind,
    pub flags: u8,
    pub value_ref: Option<ValueRef>,
    pub formula_ref: Option<AstNodeId>,
    pub out_edges: Vec<VertexId>,
}
