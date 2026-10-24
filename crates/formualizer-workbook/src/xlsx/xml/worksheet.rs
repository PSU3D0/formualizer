//! Worksheet-specific lexical helpers; the scanner owns reader state.
//! Lexical shortcuts do not edit source bytes or bypass semantic admission.
pub(crate) mod attributes;
