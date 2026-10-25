//! Compatibility names for the native XLSX reader. No Calamine parser is used.
pub use crate::xlsx::loader::NativeXlsxAdapter as CalamineAdapter;
pub use crate::xlsx::options::XlsxLoadLimits;
pub use crate::xlsx::source::XlsxPathSource;
