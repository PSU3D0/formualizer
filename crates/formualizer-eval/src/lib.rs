pub mod args;
pub mod broadcast;
pub mod coercion;
pub mod error_policy;
pub mod function;
pub mod function_registry;
pub mod interpreter;
pub mod locale;
pub mod rng;
pub mod stripes;
pub mod timezone;
pub mod traits;

pub mod builtins;
pub mod reference;

pub use reference::CellRef;
pub use reference::Coord;
pub use reference::RangeRef;
pub use reference::SheetId;

mod macros;
#[cfg(test)]
pub mod test_utils;
pub mod test_workbook;

pub mod engine;
pub mod planner;
pub mod telemetry;

// Arrow-backed storage (Phase A)
pub mod arrow_store;
// Arrow compute affordances (Phase B ready)
pub mod compute_prelude;

#[cfg(test)]
mod tests;
