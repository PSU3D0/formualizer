//! Safety helpers shared by the C ABI surface.
//!
//! Two concerns are handled here:
//!
//! 1. **Unwinding must not cross the FFI boundary.** The engine is a large
//!    codebase that uses `assert!`/`unwrap` internally (for example
//!    `Coord::new` asserts that a row fits in 20 bits). If such a panic
//!    propagated out of an `extern "C"` function the process would abort --
//!    Rust defines unwinding out of an `extern "C"` frame as an abort, so a C
//!    or Python host has no chance to recover. [`catch_ffi`] converts a panic
//!    into a normal error status instead.
//!
//! 2. **Coordinates arriving from C are untrusted.** The public API is 1-based
//!    and bounded by Excel's grid; values outside that range must be rejected
//!    with a status code rather than tripping an internal assertion.

use crate::fz_status;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Excel's maximum row count (1-based), matching `formualizer_common::coord`.
pub const FZ_MAX_ROW: u32 = 1_048_576;
/// Excel's maximum column count (1-based), matching `formualizer_common::coord`.
pub const FZ_MAX_COL: u32 = 16_384;

/// Validate a 1-based cell coordinate supplied by a C caller.
pub fn validate_cell(row: u32, col: u32) -> Result<(), String> {
    if row == 0 || col == 0 {
        return Err("row and column indices are 1-based".to_string());
    }
    if row > FZ_MAX_ROW {
        return Err(format!("row {row} exceeds maximum {FZ_MAX_ROW}"));
    }
    if col > FZ_MAX_COL {
        return Err(format!("col {col} exceeds maximum {FZ_MAX_COL}"));
    }
    Ok(())
}

/// Validate the top-left anchor of a batch write plus the block extent, so a
/// batch that would run off the end of the grid is rejected up front.
pub fn validate_block(
    start_row: u32,
    start_col: u32,
    rows: usize,
    cols: usize,
) -> Result<(), String> {
    validate_cell(start_row, start_col)?;
    let last_row = (start_row as u64) + rows.saturating_sub(1) as u64;
    let last_col = (start_col as u64) + cols.saturating_sub(1) as u64;
    if last_row > FZ_MAX_ROW as u64 {
        return Err(format!(
            "block ending at row {last_row} exceeds maximum {FZ_MAX_ROW}"
        ));
    }
    if last_col > FZ_MAX_COL as u64 {
        return Err(format!(
            "block ending at col {last_col} exceeds maximum {FZ_MAX_COL}"
        ));
    }
    Ok(())
}

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Run `f`, converting both `Err` returns and panics into an error `fz_status`.
///
/// On success the caller's `status` (when non-null) is set to OK and the
/// closure's value is returned. On failure `status` is populated with the
/// error message and `fallback` is returned, so the C caller always observes a
/// well-defined value.
pub fn catch_ffi<T, F>(status: *mut fz_status, fallback: T, f: F) -> T
where
    F: FnOnce() -> Result<T, String>,
{
    let outcome = catch_unwind(AssertUnwindSafe(f));

    let result = match outcome {
        Ok(inner) => inner,
        Err(payload) => Err(format!("internal panic: {}", panic_message(&*payload))),
    };

    match result {
        Ok(value) => {
            if !status.is_null() {
                // SAFETY: the caller contract for every `extern "C"` entry
                // point in this crate is that `status` is either null or a
                // valid, writable, properly aligned `fz_status`.
                unsafe { *status = fz_status::ok() };
            }
            value
        }
        Err(message) => {
            if !status.is_null() {
                // SAFETY: as above.
                unsafe { *status = fz_status::error(message) };
            }
            fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fz_status_code;

    #[test]
    fn rejects_zero_based_coordinates() {
        assert!(validate_cell(0, 1).is_err());
        assert!(validate_cell(1, 0).is_err());
        assert!(validate_cell(1, 1).is_ok());
    }

    #[test]
    fn rejects_out_of_grid_coordinates() {
        assert!(validate_cell(FZ_MAX_ROW + 1, 1).is_err());
        assert!(validate_cell(1, FZ_MAX_COL + 1).is_err());
        assert!(validate_cell(FZ_MAX_ROW, FZ_MAX_COL).is_ok());
    }

    #[test]
    fn block_extent_is_bounds_checked() {
        assert!(validate_block(FZ_MAX_ROW, 1, 2, 1).is_err());
        assert!(validate_block(1, FZ_MAX_COL, 1, 2).is_err());
        assert!(validate_block(1, 1, 10, 10).is_ok());
        // An empty block must not underflow.
        assert!(validate_block(1, 1, 0, 0).is_ok());
    }

    #[test]
    fn catch_ffi_reports_panics_as_errors() {
        let mut status = fz_status::ok();
        let value = catch_ffi(&mut status, -1i32, || -> Result<i32, String> {
            panic!("boom");
        });
        assert_eq!(value, -1);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        assert!(!status.error.data.is_null());
        unsafe { crate::fz_buffer_free(status.error) };
    }

    #[test]
    fn catch_ffi_tolerates_null_status() {
        let value = catch_ffi(std::ptr::null_mut(), 0i32, || -> Result<i32, String> {
            panic!("boom");
        });
        assert_eq!(value, 0);
    }

    #[test]
    fn catch_ffi_passes_through_success() {
        let mut status = fz_status::ok();
        let value = catch_ffi(&mut status, 0i32, || Ok(42));
        assert_eq!(value, 42);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    }
}
