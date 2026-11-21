//! Internal Bessel-function approximations used by Excel engineering builtins.
//!
//! The J/Y implementations are Rust ports of the Sun/openlibm algorithms; the original
//! permissive notice is preserved in the source files. The I/K implementations use
//! full double-precision series, asymptotic/CF1/CF2 continued fractions, Wronskian
//! normalization, and scaled recurrences.

mod bessel_i;
mod bessel_j0_y0;
mod bessel_j1_y1;
mod bessel_jn_yn;
mod bessel_k;
mod bessel_util;

#[cfg(test)]
mod tests;

pub(crate) use bessel_i::bessel_i;
pub(crate) use bessel_jn_yn::jn as bessel_j;
pub(crate) use bessel_jn_yn::yn as bessel_y;
pub(crate) use bessel_k::bessel_k;
