//! Strict cache-only XLSX recalculation, without a rich document model.
//! Public types retain their stable paths; implementation lives in the XLSX layer.
use crate::recalculate::{DEFAULT_ERROR_LOCATION_LIMIT, RecalculateSummary};
use chrono::{DateTime, Utc};
use formualizer_eval::engine::{CancelToken, EvalConfig};

pub use crate::xlsx::recalculate::recalculate_xlsx_bytes;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::xlsx::recalculate::recalculate_xlsx_file;

/// Bounds apply to actual decompression, XML depth/cells and output, not only ZIP headers.
#[derive(Debug, Clone)]
pub struct XlsxRecalculateLimits {
    pub max_input_bytes: usize,
    pub max_entries: usize,
    pub max_expanded_bytes: usize,
    pub max_worksheet_bytes: usize,
    pub max_formula_cells: usize,
    pub max_output_bytes: usize,
    pub max_xml_depth: usize,
    pub max_cells: usize,
    /// Limits the width of the native per-column ingestion builders.
    pub max_columns: u32,
    /// Exact logical operation evidence roots and distinct owned text capacity.
    pub max_operation_roots: usize,
    pub max_operation_text_bytes: usize,
    /// Dependency query plus a pre-admitted publication membership work bound,
    /// root/returned items, and requested owned-vector capacity. These are not
    /// total RSS/CPU bounds.
    pub max_affected_work: u64,
    pub max_affected_regions: usize,
    pub max_affected_scratch_bytes: usize,
}
impl Default for XlsxRecalculateLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 64 << 20,
            max_entries: 10_000,
            max_expanded_bytes: 256 << 20,
            max_worksheet_bytes: 128 << 20,
            max_formula_cells: 100_000,
            max_output_bytes: 64 << 20,
            max_xml_depth: 128,
            max_cells: 8_000_000,
            max_columns: 256,
            max_operation_roots: 100_000,
            max_operation_text_bytes: 8 << 20,
            max_affected_work: 10_000_000,
            max_affected_regions: 100_000,
            max_affected_scratch_bytes: 32 << 20,
        }
    }
}
/// What recalculation does when formulas read external workbook links.
/// Links are never refreshed under either policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExternalLinkPolicy {
    /// Read the values Excel last stored in the workbook for each link
    /// (reported in [`XlsxRecalculateResult::external_links_used`]).
    #[default]
    Cached,
    /// Refuse (`IoError::Unsupported`, feature `external link values`) a
    /// workbook whose calculation reads any external link value; workbooks
    /// whose links nothing reads still recalculate. Decided after
    /// calculation, so other refusals keep their reasons; nothing is
    /// published.
    Refuse,
}
/// Source admission/publication policy. Strict preserves the historical guards.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum XlsxRecalculateProfile {
    #[default]
    Strict,
    Workbook,
}

/// The source date system is authoritative; other evaluation policies come from `eval_config`.
#[derive(Debug, Clone)]
pub struct XlsxRecalculateOptions {
    pub profile: XlsxRecalculateProfile,
    /// Require no known feature/initial-state gaps; valid Excel errors are allowed.
    pub require_complete: bool,
    pub eval_config: EvalConfig,
    /// Portable source-aware overrides. None preserves the legacy raw config route.
    pub calculation: Option<crate::SourceCalculationPolicy>,
    pub cancel: Option<CancelToken>,
    pub limits: XlsxRecalculateLimits,
    pub error_location_limit: usize,
    /// Opt in to measured physical dispatch/observation counters. Disabled
    /// metrics are reported as None, distinct from a measured zero.
    pub physical_telemetry: bool,
    /// See [`ExternalLinkPolicy`]; defaults to reading cached values.
    pub external_links: ExternalLinkPolicy,
}
impl Default for XlsxRecalculateOptions {
    fn default() -> Self {
        Self {
            profile: XlsxRecalculateProfile::Strict,
            require_complete: false,
            eval_config: EvalConfig::default(),
            calculation: None,
            cancel: None,
            limits: XlsxRecalculateLimits::default(),
            error_location_limit: DEFAULT_ERROR_LOCATION_LIMIT,
            physical_telemetry: false,
            external_links: ExternalLinkPolicy::default(),
        }
    }
}
impl XlsxRecalculateOptions {
    /// Opt into workbook-aware settings and sparse source admission.
    /// Byte, actual-cell, formula and output limits remain explicit and bounded.
    pub fn workbook() -> Self {
        let mut options = Self {
            profile: XlsxRecalculateProfile::Workbook,
            calculation: Some(crate::SourceCalculationPolicy::default()),
            ..Default::default()
        };
        options.limits.max_columns = 16_384;
        options
    }
}

/// `cache_cells_changed` counts physical caches actually patched, not engine deltas.
#[derive(Debug, Clone)]
pub struct XlsxRecalculateResult {
    pub bytes: Vec<u8>,
    pub summary: RecalculateSummary,
    pub formula_cells: usize,
    pub cache_cells_changed: usize,
    pub worksheet_parts_changed: usize,
    /// The UTC instant `NOW()`/`TODAY()` observed in this run, interpreted in
    /// `eval_config.deterministic_mode.timezone()`. It is the fixed timestamp
    /// in deterministic mode, otherwise the single system-clock sample (whole
    /// seconds) taken immediately before evaluation. `None` only when the build has no
    /// system clock and no fixed timestamp was supplied (such builds refuse
    /// `TODAY`/`NOW` workbooks).
    pub clock_now_utc: Option<DateTime<Utc>>,
    /// External links whose cached values the calculation read (each
    /// `xl/externalLinks` part a formula or used defined name references).
    /// Links are never refreshed: these values are the ones Excel last
    /// stored in the workbook.
    pub external_links_used: usize,
    /// Effective portable settings and their origins, when explicitly requested.
    pub calculation: Option<crate::ResolvedSourceCalculation>,
    pub coverage: crate::SourceCalculationCoverage,
    pub coverage_scope: crate::SourceCoverageScope,
    pub caches_invalidated: usize,
    pub calculation_flags_changed: bool,
    pub operation_diagnostics: crate::SourceOperationDiagnostics,
}
