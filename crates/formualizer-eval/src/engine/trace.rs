//! Feature-gated tracing vocabulary for engine boundaries.
//!
//! The disabled definitions deliberately discard their token arguments without
//! evaluating them, keeping instrumentation zero-cost when `tracing` is off.

#[cfg(feature = "tracing")]
macro_rules! fz_event {
    ($level:expr, $area:literal, $name:literal $(, $($fields:tt)+)?) => {
        tracing::event!(
            name: concat!("fz.", $name),
            target: concat!("fz.", $area),
            $level
            $(, $($fields)+)?
        )
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! fz_event {
    ($($tokens:tt)*) => {{}};
}

#[cfg(feature = "tracing")]
macro_rules! fz_span {
    ($level:expr, $area:literal, $name:literal $(, $($fields:tt)+)?) => {
        tracing::span!(
            target: concat!("fz.", $area),
            $level,
            concat!("fz.", $name)
            $(, $($fields)+)?
        )
        .entered()
    };
}

#[cfg(not(feature = "tracing"))]
pub(crate) struct NoopSpanGuard;

#[cfg(not(feature = "tracing"))]
impl Drop for NoopSpanGuard {
    #[inline]
    fn drop(&mut self) {}
}

#[cfg(not(feature = "tracing"))]
macro_rules! fz_span {
    ($($tokens:tt)*) => {{ $crate::engine::trace::NoopSpanGuard }};
}

pub(crate) use fz_event;
pub(crate) use fz_span;
