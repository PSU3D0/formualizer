pub mod change_log;
pub mod reference_adjuster;
pub mod transaction_context;
pub mod transaction_manager;
pub mod undo_engine;
pub mod vertex_editor;

pub use vertex_editor::{
    DataUpdateSummary, EditorError, MetaUpdateSummary, RangeSummary, ShiftSummary, TransactionId,
    VertexDataPatch, VertexEditor, VertexMeta, VertexMetaPatch,
};
