//! Lookup and reference functions module
//!
//! This module contains all lookup and reference functions:
//! - Classic lookup: MATCH, VLOOKUP, HLOOKUP, CHOOSE
//! - Reference info: ROW, ROWS, COLUMN, COLUMNS
//! - Reference creation: ADDRESS

mod address;
mod choose;
mod core;
mod dynamic;
mod lookup_utils; // shared helper utilities for lookup family
mod reference_info; // modern lookup & dynamic array subset (XLOOKUP, FILTER, UNIQUE)
mod stack; // stacking & concatenation functions (HSTACK, VSTACK)

pub use address::AddressFn;
pub use choose::ChooseFn;
pub use core::{HLookupFn, MatchFn, VLookupFn};
pub use dynamic::{
    FilterFn, GroupByFn, PivotByFn, RandArrayFn, SortByFn, SortFn, UniqueFn, XLookupFn, XMatchFn,
};
pub use reference_info::{ColumnFn, ColumnsFn, RowFn, RowsFn};
pub use stack::{HStackFn, VStackFn};
// CHOOSECOLS / CHOOSEROWS live in choose.rs alongside CHOOSE
pub use choose::{ChooseColsFn, ChooseRowsFn};

/// Register all lookup and reference functions
pub fn register_builtins() {
    use crate::function_registry::register_function;
    use std::sync::Arc;

    // Classic lookup functions (from parent lookup.rs)
    register_function(Arc::new(MatchFn));
    register_function(Arc::new(VLookupFn));
    register_function(Arc::new(HLookupFn));

    // Choose function
    register_function(Arc::new(ChooseFn));

    // Reference info functions
    register_function(Arc::new(RowFn));
    register_function(Arc::new(RowsFn));
    register_function(Arc::new(ColumnFn));
    register_function(Arc::new(ColumnsFn));

    // Address function
    register_function(Arc::new(AddressFn));

    // Dynamic / modern lookup subset (Sprint 5 initial)
    dynamic::register_builtins();

    // Stack functions
    stack::register_builtins();

    // CHOOSECOLS / CHOOSEROWS
    register_function(Arc::new(choose::ChooseColsFn));
    register_function(Arc::new(choose::ChooseRowsFn));
}
