//! Core text functions (Phase 2)
//! Functions implemented (initial subset): LEN, LEFT, RIGHT, MID, TRIM, UPPER, LOWER, PROPER,
//! CONCAT, CONCATENATE, TEXTJOIN, SUBSTITUTE, REPLACE, FIND, SEARCH, EXACT, VALUE, TEXT (limited formats)

mod find_search_exact; // FIND, SEARCH, EXACT
mod len_left_right; // LEN, LEFT, RIGHT
mod mid_sub_replace; // MID, SUBSTITUTE, REPLACE
mod trim_case_concat; // TRIM, UPPER, LOWER, PROPER, CONCAT, CONCATENATE, TEXTJOIN
mod value_text; // VALUE, TEXT

#[cfg(test)]
mod text_tests; // Comprehensive test suite

pub use find_search_exact::*;
pub use len_left_right::*;
pub use mid_sub_replace::*;
pub use trim_case_concat::*;
pub use value_text::*;

pub fn register_builtins() {
    len_left_right::register_builtins();
    mid_sub_replace::register_builtins();
    trim_case_concat::register_builtins();
    find_search_exact::register_builtins();
    value_text::register_builtins();
}
