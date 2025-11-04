use super::AstNodeId;
use crate::{
    SheetId,
    engine::vertex::{VertexId, VertexKind},
};
use formualizer_common::{Coord as AbsCoord, LiteralValue};

/// Snapshot of a vertex's complete state for rollback purposes
#[derive(Debug, Clone)]
pub struct VertexSnapshot {
    pub coord: AbsCoord,
    pub sheet_id: SheetId,
    pub kind: VertexKind,
    pub flags: u8,
    pub value: Option<LiteralValue>,
    pub formula_ref: Option<AstNodeId>,
    pub out_edges: Vec<VertexId>,
}
