//! Core text functions (Phase 2)
//! Functions implemented (initial subset): LEN, LEFT, RIGHT, MID, TRIM, UPPER, LOWER, PROPER,
//! CONCAT, CONCATENATE, TEXTJOIN, SUBSTITUTE, REPLACE, FIND, SEARCH, EXACT, VALUE, TEXT (limited formats)
//! CHAR, CODE, REPT, CLEAN, UNICHAR, UNICODE, TEXTBEFORE, TEXTAFTER, DOLLAR, FIXED
//! TEXTSPLIT, VALUETOTEXT, ARRAYTOTEXT (array text functions)

use crate::traits::{ArgumentHandle, CalcValue};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

fn scalar_text_value(arg: &ArgumentHandle<'_, '_>) -> Result<LiteralValue, ExcelError> {
    Ok(match arg.value_for_text()? {
        CalcValue::Scalar(value) | CalcValue::AnnotatedScalar(value, _) => value,
        CalcValue::Range(view) => view.get_cell(0, 0),
        CalcValue::Callable(_) => LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Calc).with_message("LAMBDA value must be invoked"),
        ),
    })
}

mod array_text; // TEXTSPLIT, VALUETOTEXT, ARRAYTOTEXT
mod byte; // FINDB, LEFTB, LENB, MIDB, REPLACEB, RIGHTB, SEARCHB
mod char_code_rept; // CHAR, CODE, REPT
mod extended; // CLEAN, UNICHAR, UNICODE, TEXTBEFORE, TEXTAFTER, DOLLAR, FIXED
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
#[cfg(test)]
pub use trim_case_concat::*;
#[cfg(test)]
pub use value_text::*;

pub fn register_builtins() {
    array_text::register_builtins();
    byte::register_builtins();
    char_code_rept::register_builtins();
    extended::register_builtins();
    len_left_right::register_builtins();
    mid_sub_replace::register_builtins();
    trim_case_concat::register_builtins();
    find_search_exact::register_builtins();
    value_text::register_builtins();
}
