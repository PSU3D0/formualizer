pub mod backends;
pub mod error;
pub mod loader;
pub mod resolver;
pub mod session;
pub mod traits;
pub mod transaction;

#[cfg(feature = "calamine")]
pub use backends::CalamineAdapter;
#[cfg(feature = "umya")]
pub use backends::UmyaAdapter;
pub use error::{with_cell_context, IoError};
pub use loader::{LoaderStats, WorkbookLoader};
pub use resolver::IoResolver;
pub use session::{EditorSession, IoConfig};
pub use traits::{
    AccessGranularity, BackendCaps, CellData, LoadStrategy, MergedRange, NamedRange, SheetData,
    SpreadsheetIO, SpreadsheetReader, SpreadsheetWriter, TableDefinition,
};
pub use transaction::{WriteOp, WriteTransaction};

// Re-export for convenience
pub use formualizer_common::LiteralValue;
