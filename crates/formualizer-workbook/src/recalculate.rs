use crate::error::col_to_a1;
use crate::{IoError, SpreadsheetReader, SpreadsheetWriter, UmyaAdapter, workbook::WBResolver};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine, EvalConfig};
use std::collections::BTreeMap;
use std::path::Path;

pub const DEFAULT_ERROR_LOCATION_LIMIT: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecalculateStatus {
    Success,
    ErrorsFound,
}

impl RecalculateStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ErrorsFound => "errors_found",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecalculateSheetSummary {
    pub evaluated: usize,
    pub errors: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecalculateErrorSummary {
    pub count: usize,
    pub locations: Vec<String>,
    pub locations_truncated: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecalculateSummary {
    pub status: RecalculateStatus,
    pub evaluated: usize,
    pub errors: usize,
    pub sheets: BTreeMap<String, RecalculateSheetSummary>,
    pub error_summary: BTreeMap<String, RecalculateErrorSummary>,
}

impl Default for RecalculateSummary {
    fn default() -> Self {
        Self {
            status: RecalculateStatus::Success,
            evaluated: 0,
            errors: 0,
            sheets: BTreeMap::new(),
            error_summary: BTreeMap::new(),
        }
    }
}

impl RecalculateSummary {
    pub fn has_errors(&self) -> bool {
        self.errors > 0
    }
}

/// Recalculate an XLSX file and write formula cached values back through Umya.
///
/// - `input`: source workbook path
/// - `output`: optional destination path; when `None`, updates `input` in-place.
///
/// # Current limitation
///
/// With current `umya-spreadsheet`, cached values for formula cells are persisted as
/// string-typed payloads (`t="str"`) even when the computed value is numeric/bool/error.
/// Formula text is preserved.
///
/// We plan to remove this limitation via an upstream umya patch that allows explicit typed
/// formula-cache serialization.
pub fn recalculate_file(
    input: &Path,
    output: Option<&Path>,
) -> Result<RecalculateSummary, IoError> {
    recalculate_file_with_limit(input, output, DEFAULT_ERROR_LOCATION_LIMIT)
}

pub fn recalculate_file_with_limit(
    input: &Path,
    output: Option<&Path>,
    error_location_limit: usize,
) -> Result<RecalculateSummary, IoError> {
    let mut adapter =
        UmyaAdapter::open_path(input).map_err(|e| IoError::from_backend("umya", e))?;

    let mut engine: Engine<WBResolver> = Engine::new(WBResolver, EvalConfig::default());
    adapter.stream_into_engine(&mut engine)?;
    engine.evaluate_all().map_err(IoError::Engine)?;

    let formula_cells = adapter.formula_cells();
    let date_system = engine.config.date_system;

    let mut summary = RecalculateSummary::default();

    for (sheet, row, col) in formula_cells {
        let value = engine
            .get_cell_value(&sheet, row, col)
            .unwrap_or(LiteralValue::Empty);

        adapter
            .set_formula_cached_value(&sheet, row, col, &value, date_system)
            .map_err(|e| IoError::from_backend("umya", e))?;

        let sheet_stats = summary.sheets.entry(sheet.clone()).or_default();
        sheet_stats.evaluated += 1;
        summary.evaluated += 1;

        if let LiteralValue::Error(err) = &value {
            summary.errors += 1;
            sheet_stats.errors += 1;

            let token = err.kind.to_string();
            let entry = summary.error_summary.entry(token).or_default();
            entry.count += 1;

            if entry.locations.len() < error_location_limit {
                entry
                    .locations
                    .push(format!("{sheet}!{}{}", col_to_a1(col), row));
            } else {
                entry.locations_truncated += 1;
            }
        }
    }

    summary.status = if summary.errors == 0 {
        RecalculateStatus::Success
    } else {
        RecalculateStatus::ErrorsFound
    };

    if let Some(path) = output {
        adapter
            .save_as_path(path)
            .map_err(|e| IoError::from_backend("umya", e))?;
    } else {
        adapter
            .save()
            .map_err(|e| IoError::from_backend("umya", e))?;
    }

    Ok(summary)
}
