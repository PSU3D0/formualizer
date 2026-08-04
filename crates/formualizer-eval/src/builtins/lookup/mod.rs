//! Lookup and reference functions module
//!
//! This module contains all lookup and reference functions:
//! - Classic lookup: MATCH, VLOOKUP, HLOOKUP, CHOOSE
//! - Reference info: ROW, ROWS, COLUMN, COLUMNS
//! - Reference creation: ADDRESS

mod address;
mod array_shape;
mod choose;
mod core;
mod dynamic;
mod legacy; // classic LOOKUP function (vector & array forms)
pub(crate) mod lookup_utils; // shared helper utilities for lookup family
mod reference_info; // modern lookup & dynamic array subset (XLOOKUP, FILTER, UNIQUE)
mod stack; // stacking & concatenation functions (HSTACK, VSTACK)

#[cfg(test)]
pub use choose::ChooseFn;
#[cfg(test)]
pub use core::MatchFn;
#[cfg(test)]
pub use dynamic::RandArrayFn;

/// Register all lookup and reference functions
pub fn register_builtins() {
    use crate::function_registry::register_builtin;
    use std::sync::Arc;

    // Classic lookup functions (from parent lookup.rs)
    register_builtin(Arc::new(core::MatchFn));
    register_builtin(Arc::new(core::VLookupFn));
    register_builtin(Arc::new(core::HLookupFn));

    // Legacy LOOKUP (vector & array forms)
    register_builtin(Arc::new(legacy::LookupFn));

    // Choose function
    register_builtin(Arc::new(choose::ChooseFn));

    // Reference info functions
    register_builtin(Arc::new(reference_info::RowFn));
    register_builtin(Arc::new(reference_info::RowsFn));
    register_builtin(Arc::new(reference_info::ColumnFn));
    register_builtin(Arc::new(reference_info::ColumnsFn));

    // Address function
    register_builtin(Arc::new(address::AddressFn));

    // Dynamic / modern lookup subset (Sprint 5 initial)
    dynamic::register_builtins();

    // Stack and array-shaping functions
    stack::register_builtins();
    array_shape::register_builtins();

    // CHOOSECOLS / CHOOSEROWS
    register_builtin(Arc::new(choose::ChooseColsFn));
    register_builtin(Arc::new(choose::ChooseRowsFn));
}
