//! Meta crate that re-exports the primary Formualizer building blocks with
//! sensible defaults. Downstream users can depend on this crate and opt into
//! specific layers via feature flags while keeping access to the underlying
//! crates when deeper integration is required.

#[cfg(feature = "eval")]
pub use formualizer_eval as eval;

#[cfg(feature = "workbook")]
pub use formualizer_workbook as workbook;

#[cfg(feature = "sheetport")]
pub use formualizer_sheetport as sheetport;

#[cfg(feature = "sheetport")]
pub use sheetport_spec;

#[cfg(feature = "sheetport")]
pub use formualizer_sheetport::{
    AreaLocation, BoundPort, ManifestBindings, PortBinding, RecordBinding, RecordFieldBinding,
    ScalarBinding, ScalarLocation, SheetPort, TableBinding, TableLocation,
};

#[cfg(feature = "workbook")]
pub use formualizer_workbook::{WorkbookConfig, WorkbookMode};
