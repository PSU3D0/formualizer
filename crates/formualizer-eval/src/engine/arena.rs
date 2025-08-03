pub mod array;
pub mod ast;
pub mod data_store;
pub mod error_arena;
/// Arena-based storage for values and AST nodes
/// Phase 1 of the SoA implementation plan
pub mod scalar;
pub mod string_interner;
pub mod value_ref;

// Re-export commonly used types
pub use array::{ArrayArena, ArrayRef};
pub use ast::{AstArena, AstNodeData, AstNodeId, CompactRefType};
pub use data_store::{DataStore, DataStoreStats};
pub use error_arena::{ErrorArena, ErrorRef};
pub use scalar::{ScalarArena, ScalarRef};
pub use string_interner::{StringId, StringInterner};
pub use value_ref::{ValueRef, ValueType};
