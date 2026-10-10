use crate::IoError;
use formualizer_eval::engine::CancelToken;

pub(crate) struct CacheOptions {
    pub(crate) cancel: Option<CancelToken>,
    pub(crate) limits: CacheLimits,
}

pub(crate) struct CacheLimits {
    pub(crate) max_cells: usize,
    pub(crate) max_xml_depth: usize,
    pub(crate) max_worksheet_bytes: usize,
    pub(crate) max_entries: usize,
    pub(crate) max_expanded_bytes: usize,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            cancel: None,
            limits: CacheLimits {
                max_cells: 8_000_000,
                max_xml_depth: 128,
                max_worksheet_bytes: 128 << 20,
                max_entries: 10_000,
                max_expanded_bytes: 256 << 20,
            },
        }
    }
}

#[cfg(feature = "xlsx-recalc")]
impl From<&crate::cache_recalculate::XlsxRecalculateOptions> for CacheOptions {
    fn from(options: &crate::cache_recalculate::XlsxRecalculateOptions) -> Self {
        Self {
            cancel: options.cancel.clone(),
            limits: CacheLimits {
                max_cells: options.limits.max_cells,
                max_xml_depth: options.limits.max_xml_depth,
                max_worksheet_bytes: options.limits.max_worksheet_bytes,
                max_entries: options.limits.max_entries,
                max_expanded_bytes: options.limits.max_expanded_bytes,
            },
        }
    }
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
