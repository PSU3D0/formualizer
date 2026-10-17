//! Last-resort isolation of panics in workbook readers.
//!
//! The pinned umya-spreadsheet reader and calamine panic on some malformed
//! parts, and a reader panic used to unwind through `Workbook::from_reader`
//! into the caller (a Python `PanicException`, a crashed CLI). Loads run
//! under [`catch_load_panic`], which turns a panic into the load's ordinary
//! error with the panic message. The panics themselves are still bugs.
//!
//! Native builds only: `wasm32-unknown-unknown` is built with
//! `panic = "abort"`, where a panic ends the instance and cannot be caught,
//! so the guard is a plain call there. Allocation failures abort on every
//! target and are not caught either.

/// Runs `load`; a panic becomes `on_panic(message)`.
pub(crate) fn catch_load_panic<T, E>(
    load: impl FnOnce() -> Result<T, E>,
    on_panic: impl FnOnce(String) -> E,
) -> Result<T, E> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(load))
            .unwrap_or_else(|payload| Err(on_panic(panic_message(payload.as_ref()))))
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = on_panic;
        load()
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    let detail = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned());
    format!("the workbook reader panicked: {detail}")
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::catch_load_panic;

    #[test]
    fn a_panic_becomes_the_load_error() {
        let result: Result<(), String> =
            catch_load_panic(|| panic!("bad part at {}", 7), |message| message);
        assert_eq!(
            result.unwrap_err(),
            "the workbook reader panicked: bad part at 7"
        );
        let ok: Result<u8, String> = catch_load_panic(|| Ok(3), |m| m);
        assert_eq!(ok, Ok(3));
    }
}
