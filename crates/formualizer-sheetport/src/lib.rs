//! SheetPort runtime bindings.
//!
//! This crate links [`sheetport_spec::Manifest`] definitions to concrete workbook
//! data structures supplied by `formualizer-workbook`. It focuses solely on the
//! pure I/O contract: resolving selectors, describing typed ports, and preparing
//! the groundwork for deterministic reads and writes.

mod binding;
mod error;
mod location;
mod resolver;
mod runtime;

pub use binding::{
    BoundPort, ManifestBindings, PortBinding, RecordBinding, RecordFieldBinding, ScalarBinding,
    TableBinding,
};
pub use error::SheetPortError;
pub use location::{AreaLocation, FieldLocation, ScalarLocation, TableLocation};
pub use runtime::SheetPort;
