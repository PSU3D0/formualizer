//! SheetPort runtime bindings.
//!
//! This crate links [`sheetport_spec::Manifest`] definitions to concrete workbook
//! data structures supplied by `formualizer-workbook`. It focuses solely on the
//! pure I/O contract: resolving selectors, describing typed ports, and preparing
//! the groundwork for deterministic reads and writes.

mod batch;
mod binding;
mod context;
mod error;
mod layout;
mod location;
mod resolver;
mod runtime;
mod session;
mod validation;
mod value;

pub use batch::{BatchExecutor, BatchInput, BatchOptions, BatchProgress, BatchResult};
pub use binding::{
    BoundPort, ManifestBindings, PortBinding, RangeBinding, RecordBinding, RecordFieldBinding,
    ScalarBinding, TableBinding, TableColumnBinding,
};
pub use error::SheetPortError;
pub use location::{AreaLocation, FieldLocation, ScalarLocation, TableLocation};
pub use runtime::{EvalMode, EvalOptions, SheetPort};
pub use session::SheetPortSession;
pub use validation::{ConstraintViolation, ValidationScope};
pub use value::{InputSnapshot, InputUpdate, OutputSnapshot, PortValue, TableRow, TableValue};
