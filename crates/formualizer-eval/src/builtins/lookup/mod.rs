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

pub use address::AddressFn;
pub use array_shape::{ToColFn, ToRowFn};
pub use choose::ChooseFn;
pub use core::{HLookupFn, MatchFn, VLookupFn};
pub use dynamic::{
    FilterFn, GroupByFn, PivotByFn, RandArrayFn, SortByFn, SortFn, UniqueFn, XLookupFn, XMatchFn,
};
pub use legacy::LookupFn;
pub use reference_info::{ColumnFn, ColumnsFn, RowFn, RowsFn};
pub use stack::{HStackFn, VStackFn};
// CHOOSECOLS / CHOOSEROWS live in choose.rs alongside CHOOSE
pub use choose::{ChooseColsFn, ChooseRowsFn};

/// Register all lookup and reference functions
pub fn register_builtins() {
    use crate::function_registry::register_builtin;
    use std::sync::Arc;

    // Classic lookup functions (from parent lookup.rs)
    register_builtin(Arc::new(MatchFn));
    register_builtin(Arc::new(VLookupFn));
    register_builtin(Arc::new(HLookupFn));

    // Legacy LOOKUP (vector & array forms)
    register_builtin(Arc::new(LookupFn));

    // Choose function
    register_builtin(Arc::new(ChooseFn));

    // Reference info functions
    register_builtin(Arc::new(RowFn));
    register_builtin(Arc::new(RowsFn));
    register_builtin(Arc::new(ColumnFn));
    register_builtin(Arc::new(ColumnsFn));

    // Address function
    register_builtin(Arc::new(AddressFn));

    // Dynamic / modern lookup subset (Sprint 5 initial)
    dynamic::register_builtins();

    // Stack and array-shaping functions
    stack::register_builtins();
    array_shape::register_builtins();

    // CHOOSECOLS / CHOOSEROWS
    register_builtin(Arc::new(choose::ChooseColsFn));
    register_builtin(Arc::new(choose::ChooseRowsFn));
}
