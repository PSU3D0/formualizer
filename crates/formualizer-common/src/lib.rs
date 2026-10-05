// This crate keeps `if let ... { if cond { ... } }` nesting in a few places,
// written when the Pyodide wheel built on a nightly without let chains. Allow clippy's collapse-suggestion globally rather than annotating
// each site and risking drift.
#![allow(clippy::collapsible_if)]

pub mod address;
pub mod coord;
pub mod coord_hash;
pub mod date_serial;
pub mod error;
pub mod function;
pub mod grid_address;
pub mod numfmt;
pub mod range;
pub mod value;

pub use address::*;
pub use coord::*;
pub use coord_hash::*;
pub use date_serial::*;
pub use error::*;
pub use function::*;
pub use grid_address::*;
pub use numfmt::*;
pub use range::*;
pub use value::*;
