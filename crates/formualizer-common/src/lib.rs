#![cfg_attr(target_os = "emscripten", feature(let_chains))]
// The Pyodide-matched Rust nightly predates let-chain stabilization, so this
// crate intentionally keeps `if let ... { if cond { ... } }` nesting in a few
// places. Allow clippy's collapse-suggestion globally rather than annotating
// each site and risking drift.
#![allow(clippy::collapsible_if)]

pub mod address;
pub mod coord;
pub mod error;
pub mod function;
pub mod range;
pub mod value;

pub use address::*;
pub use coord::*;
pub use error::*;
pub use function::*;
pub use range::*;
pub use value::*;
