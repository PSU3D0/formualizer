pub fn set_panic_hook() {
    #[cfg(feature = "console_panic")]
    console_error_panic_hook::set_once();
}
