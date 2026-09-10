#![cfg_attr(target_os = "emscripten", feature(let_chains, unsigned_is_multiple_of))]

pub mod backends;
pub mod builtins;
#[cfg(feature = "xlsx-recalc")]
pub mod cache_recalculate;
pub mod calc_pr;
pub mod error;
#[cfg(any(
    feature = "calamine",
    feature = "json",
    feature = "umya",
    feature = "umya3"
))]
pub(crate) mod load_limits;
#[cfg(any(feature = "umya", feature = "xlsx-recalc"))]
pub mod recalculate;
pub mod resolver;
pub mod session;
pub mod traits;
pub mod transaction;
#[cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]
mod wasm_runtime_wasmtime;
pub mod workbook;
pub mod worksheet;
#[cfg(any(feature = "umya3", feature = "xlsx-recalc"))]
pub(crate) mod xlsx_path;

#[cfg(feature = "csv")]
pub use backends::CsvAdapter;
#[cfg(feature = "json")]
pub use backends::JsonAdapter;
#[cfg(feature = "umya3")]
pub use backends::Umya3Adapter;
#[cfg(feature = "umya")]
pub use backends::UmyaAdapter;
#[cfg(feature = "csv")]
pub use backends::csv::CsvArrayPolicy;
#[cfg(feature = "json")]
pub use backends::json::JsonReadOptions;
#[cfg(feature = "calamine")]
pub use backends::{CalamineAdapter, XlsxPathSource};
#[cfg(any(feature = "umya", feature = "umya3"))]
pub use backends::{FormulaCacheUpdate, FormulaCacheUpdateRef};
pub use builtins::{ensure_builtins_loaded, register_function_dynamic, try_load_builtins};
#[cfg(all(feature = "xlsx-recalc", not(target_arch = "wasm32")))]
pub use cache_recalculate::recalculate_xlsx_file;
#[cfg(feature = "xlsx-recalc")]
pub use cache_recalculate::{
    XlsxRecalculateLimits, XlsxRecalculateOptions, XlsxRecalculateResult, recalculate_xlsx_bytes,
};
pub use error::{IoError, with_cell_context};
#[cfg(any(feature = "umya", feature = "xlsx-recalc"))]
pub use recalculate::{
    DEFAULT_ERROR_LOCATION_LIMIT, RecalculateErrorSummary, RecalculateSheetSummary,
    RecalculateStatus, RecalculateSummary,
};
#[cfg(feature = "umya")]
pub use recalculate::{recalculate_file, recalculate_file_with_limit};
pub use resolver::IoResolver;
pub use session::{EditorSession, IoConfig};
pub use traits::{
    AccessGranularity, AdapterLoadStats, BackendCaps, CalcSettings, CellData, LoadStrategy,
    MergedRange, NamedRange, NamedRangeScope, SheetData, SpreadsheetIO, SpreadsheetReader,
    SpreadsheetWriter, TableDefinition,
};
pub use transaction::{WriteOp, WriteTransaction};

// Re-export for convenience
pub use formualizer_common::{LiteralValue, RangeAddress};
pub use formualizer_eval::engine::{
    CancelToken, EvaluationTarget, FormulaSpoolDiskPolicy, OpaquePreparePolicy, OpaqueReason,
    PreparationOutcome, PreparationRevision, PrepareScope, PreparedTargetGraphReport,
    TableSelection, TargetEvalOptions, WorkbookLoadLimits,
};
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
