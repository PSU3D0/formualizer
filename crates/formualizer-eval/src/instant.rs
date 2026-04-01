/// Portable timing abstraction for the evaluation engine.
///
/// - Default / native / portable-wasm: wraps `std::time::Instant` (zero JS deps).
/// - `js-runtime` feature (browser WASM via wasm-bindgen): wraps `web_time::Instant`,
///   which calls `performance.now()` in the browser where `std::time::Instant` panics.
///
/// All engine code should use `FzInstant` rather than either concrete type directly.

#[cfg(feature = "js-runtime")]
pub use web_time::Instant as FzInstant;

#[cfg(not(feature = "js-runtime"))]
pub use std::time::Instant as FzInstant;
