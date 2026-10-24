//! Bounded XML/package reads and cancellation shared by XLSX consumers.
//! Source immutability and source-preserving publication do not relax these bounds.
use crate::IoError;
use formualizer_eval::engine::CancelToken;

/// Admission guarantees, selected once at a read boundary.
#[cfg(feature = "xlsx-recalc")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ReadPolicy {
    Strict,
    #[default]
    Compatible,
}

/// Independent outputs requested from a worksheet traversal.
#[cfg(feature = "xlsx-recalc")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanRetention {
    pub(crate) values: bool,
    pub(crate) formula_events: bool,
    pub(crate) marks: bool,
    pub(crate) geometry: bool,
}

/// Shared parser input; evaluation and publication options do not enter the scanner.
pub(crate) struct ReadOptions {
    #[cfg(feature = "xlsx-recalc")]
    pub(crate) policy: ReadPolicy,
    pub(crate) cancel: Option<CancelToken>,
    pub(crate) limits: ReadLimits,
}

/// Bounds applied regardless of source-admission policy.
pub(crate) struct ReadLimits {
    #[cfg(feature = "xlsx-recalc")]
    pub(crate) max_input_bytes: usize,
    pub(crate) max_cells: usize,
    #[cfg(feature = "xlsx-recalc")]
    pub(crate) max_columns: u32,
    #[cfg(feature = "xlsx-recalc")]
    pub(crate) max_formula_cells: usize,
    pub(crate) max_xml_depth: usize,
    pub(crate) max_worksheet_bytes: usize,
    pub(crate) max_entries: usize,
    pub(crate) max_expanded_bytes: usize,
}

// Existing generic-XML consumers use the same bounded read input.
pub(crate) type CacheOptions = ReadOptions;

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature = "xlsx-recalc")]
            policy: ReadPolicy::Compatible,
            cancel: None,
            limits: ReadLimits {
                #[cfg(feature = "xlsx-recalc")]
                max_input_bytes: 64 << 20,
                max_cells: 8_000_000,
                #[cfg(feature = "xlsx-recalc")]
                max_columns: 16_384,
                #[cfg(feature = "xlsx-recalc")]
                max_formula_cells: 8_000_000,
                max_xml_depth: 128,
                max_worksheet_bytes: 128 << 20,
                max_entries: 10_000,
                max_expanded_bytes: 256 << 20,
            },
        }
    }
}

#[cfg(feature = "xlsx-recalc")]
impl From<&crate::cache_recalculate::XlsxRecalculateOptions> for ReadOptions {
    fn from(options: &crate::cache_recalculate::XlsxRecalculateOptions) -> Self {
        Self {
            policy: ReadPolicy::Strict,
            cancel: options.cancel.clone(),
            limits: ReadLimits {
                max_input_bytes: options.limits.max_input_bytes,
                max_cells: options.limits.max_cells,
                max_columns: options.limits.max_columns,
                max_formula_cells: options.limits.max_formula_cells,
                max_xml_depth: options.limits.max_xml_depth,
                max_worksheet_bytes: options.limits.max_worksheet_bytes,
                max_entries: options.limits.max_entries,
                max_expanded_bytes: options.limits.max_expanded_bytes,
            },
        }
    }
}

#[cfg(all(test, feature = "xlsx-recalc"))]
#[test]
fn recalculate_options_map_every_read_bound_and_share_cancellation() {
    let token = CancelToken::new();
    let mut source = crate::cache_recalculate::XlsxRecalculateOptions {
        cancel: Some(token.clone()),
        ..Default::default()
    };
    source.limits.max_input_bytes = 10;
    source.limits.max_cells = 11;
    source.limits.max_columns = 12;
    source.limits.max_formula_cells = 13;
    source.limits.max_xml_depth = 14;
    source.limits.max_worksheet_bytes = 15;
    source.limits.max_entries = 16;
    source.limits.max_expanded_bytes = 17;
    let read = ReadOptions::from(&source);
    assert_eq!(read.limits.max_input_bytes, 10);
    assert_eq!(read.limits.max_cells, 11);
    assert_eq!(read.limits.max_columns, 12);
    assert_eq!(read.limits.max_formula_cells, 13);
    assert_eq!(read.limits.max_xml_depth, 14);
    assert_eq!(read.limits.max_worksheet_bytes, 15);
    assert_eq!(read.limits.max_entries, 16);
    assert_eq!(read.limits.max_expanded_bytes, 17);
    assert!(checkpoint(&read.cancel).is_ok());
    token.cancel();
    assert!(checkpoint(&read.cancel).is_err());
}

pub(crate) fn unsupported(feature: impl Into<String>, context: impl Into<String>) -> IoError {
    IoError::Unsupported {
        feature: feature.into(),
        context: context.into(),
    }
}

pub(crate) fn checkpoint(token: &Option<CancelToken>) -> Result<(), IoError> {
    if token.as_ref().is_some_and(CancelToken::is_cancelled) {
        Err(IoError::Engine(formualizer_common::ExcelError::new(
            formualizer_common::ExcelErrorKind::Cancelled,
        )))
    } else {
        Ok(())
    }
}
