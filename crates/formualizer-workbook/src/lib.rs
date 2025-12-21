pub mod backends;
pub mod builtins;
pub mod error;
pub mod resolver;
pub mod session;
pub mod traits;
pub mod transaction;
pub mod workbook;
pub mod worksheet;

#[cfg(feature = "calamine")]
pub use backends::CalamineAdapter;
#[cfg(feature = "json")]
pub use backends::JsonAdapter;
#[cfg(feature = "umya")]
pub use backends::UmyaAdapter;
pub use builtins::{ensure_builtins_loaded, register_function_dynamic, try_load_builtins};
pub use error::{IoError, with_cell_context};
pub use resolver::IoResolver;
pub use session::{EditorSession, IoConfig};
pub use traits::{
    AccessGranularity, BackendCaps, CellData, LoadStrategy, MergedRange, NamedRange,
    NamedRangeScope, SheetData, SpreadsheetIO, SpreadsheetReader, SpreadsheetWriter,
    TableDefinition,
};
pub use transaction::{WriteOp, WriteTransaction};

// Re-export for convenience
pub use formualizer_common::{LiteralValue, RangeAddress};
pub use workbook::{Workbook, WorkbookConfig, WorkbookMode};
pub use worksheet::WorksheetHandle;
