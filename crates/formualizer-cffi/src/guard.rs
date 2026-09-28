//! Panic safety for the C ABI surface.
//!
//! Unwinding out of an `extern "C"` frame aborts the process. [`catch_ffi`]
//! converts panics (and `Err` returns) into a normal `fz_status` so a C or
//! Python host can recover. Coordinate validation stays in the existing
//! `checked_excel_coordinates` / `validate_cffi_range` helpers — this module
//! does not grow a second validator.

use crate::fz_status;
use std::panic::{AssertUnwindSafe, catch_unwind};

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
/// error message and `fallback` is returned.
///
/// `status` must be null or a valid, writable, properly aligned `fz_status`
/// (same contract as every other CFFI entry that takes a status out-param).
#[allow(clippy::not_unsafe_ptr_arg_deref)]
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
                // SAFETY: caller guarantees `status` is null or a valid writable `fz_status`.
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

/// Catch panics on entry points that have no `status` out-parameter.
pub fn catch_unwind_silent<F: FnOnce()>(f: F) {
    let _ = catch_unwind(AssertUnwindSafe(f));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fz_status_code;

    #[test]
    fn catch_ffi_converts_panic_to_status() {
        let mut status = fz_status::ok();
        let out = catch_ffi(
            &mut status as *mut fz_status,
            -1,
            || -> Result<i32, String> {
                panic!("deliberate");
            },
        );
        assert_eq!(out, -1);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        let buf = std::mem::replace(&mut status.error, crate::fz_buffer::empty());
        let bytes = unsafe { std::slice::from_raw_parts(buf.data, buf.len) };
        let raw = std::str::from_utf8(bytes).unwrap();
        assert!(raw.contains("internal panic"), "unexpected: {raw}");
        assert!(raw.contains("deliberate"), "unexpected: {raw}");
        unsafe { crate::fz_buffer_free(buf) };
    }

    #[test]
    fn catch_ffi_forwards_ok_and_err() {
        let mut status = fz_status::ok();
        assert_eq!(catch_ffi(&mut status as *mut _, 0, || Ok(7)), 7);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let out = catch_ffi(&mut status as *mut _, 0, || Err("nope".to_string()));
        assert_eq!(out, 0);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
    }
}
