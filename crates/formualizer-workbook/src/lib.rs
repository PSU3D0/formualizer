pub mod backends;
pub mod builtins;
pub mod error;
#[cfg(feature = "umya")]
pub mod recalculate;
pub mod resolver;
pub mod session;
pub mod traits;
pub mod transaction;
pub mod workbook;
pub mod worksheet;

#[cfg(feature = "calamine")]
pub use backends::CalamineAdapter;
#[cfg(feature = "csv")]
pub use backends::CsvAdapter;
#[cfg(feature = "json")]
pub use backends::JsonAdapter;
#[cfg(feature = "umya")]
pub use backends::UmyaAdapter;
#[cfg(feature = "csv")]
pub use backends::csv::CsvArrayPolicy;
#[cfg(feature = "json")]
pub use backends::json::JsonReadOptions;
pub use builtins::{ensure_builtins_loaded, register_function_dynamic, try_load_builtins};
pub use error::{IoError, with_cell_context};
#[cfg(feature = "umya")]
pub use recalculate::{
    DEFAULT_ERROR_LOCATION_LIMIT, RecalculateErrorSummary, RecalculateSheetSummary,
    RecalculateStatus, RecalculateSummary, recalculate_file, recalculate_file_with_limit,
};
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
pub use workbook::{
    CustomFnHandler, CustomFnInfo, CustomFnOptions, WASM_ABI_VERSION_V1, WASM_CODEC_VERSION_V1,
    WASM_MANIFEST_SCHEMA_V1, WASM_MANIFEST_SECTION_V1, WasmFunctionSpec, WasmManifestFunction,
    WasmManifestModule, WasmManifestParam, WasmManifestReturn, WasmModuleInfo, WasmModuleManifest,
    WasmRuntimeHint, WasmUdfRuntime, Workbook, WorkbookConfig, WorkbookMode,
    validate_wasm_manifest,
};

#[cfg(feature = "wasm_plugins")]
pub use workbook::{extract_wasm_manifest_json_from_module, parse_wasm_manifest_json};
pub use worksheet::WorksheetHandle;
