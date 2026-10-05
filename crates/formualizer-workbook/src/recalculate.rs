#[cfg(feature = "umya")]
use crate::IoError;
#[cfg(feature = "umya")]
use crate::backends::umya::FormulaCacheUpdate;
#[cfg(feature = "umya")]
use crate::error::col_to_a1;
#[cfg(feature = "umya")]
use crate::{SpreadsheetReader, SpreadsheetWriter, UmyaAdapter, workbook::WBResolver};
use formualizer_common::LiteralValue;
#[cfg(feature = "umya")]
use formualizer_common::PackedSheetCell;
#[cfg(feature = "umya")]
use formualizer_eval::engine::ingest::EngineLoadStream;
#[cfg(feature = "umya")]
use formualizer_eval::engine::{Engine, EvalConfig};
use std::collections::BTreeMap;
#[cfg(feature = "umya")]
use std::{collections::HashSet, path::Path};

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
    /// The engine's reason for each listed location, parallel to
    /// `locations` (e.g. `Unknown function: SPDVOL`); `None` when the error
    /// carries no reason.
    pub messages: Vec<Option<String>>,
    pub locations_truncated: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecalculateSummary {
    pub status: RecalculateStatus,
    pub evaluated: usize,
    pub errors: usize,
    pub sheets: BTreeMap<String, RecalculateSheetSummary>,
    pub error_summary: BTreeMap<String, RecalculateErrorSummary>,
    /// Every function the engine does not implement that a formula cell's
    /// error names (`Unknown function: NAME`), with the number of error
    /// cells attributed to it. Complete regardless of the location limit.
    pub unknown_functions: BTreeMap<String, usize>,
}

impl Default for RecalculateSummary {
    fn default() -> Self {
        Self {
            status: RecalculateStatus::Success,
            evaluated: 0,
            errors: 0,
            sheets: BTreeMap::new(),
            error_summary: BTreeMap::new(),
            unknown_functions: BTreeMap::new(),
        }
    }
}

impl RecalculateSummary {
    pub fn has_errors(&self) -> bool {
        self.errors > 0
    }

    /// Count one evaluated formula cell in `sheet` at `location`
    /// (`Sheet!A1`); errors are recorded with their reason.
    pub(crate) fn record(
        &mut self,
        sheet: &str,
        location: impl FnOnce() -> String,
        value: &LiteralValue,
        limit: usize,
    ) {
        let stats = self.sheets.entry(sheet.to_owned()).or_default();
        stats.evaluated += 1;
        self.evaluated += 1;
        let LiteralValue::Error(error) = value else {
            return;
        };
        self.errors += 1;
        stats.errors += 1;
        let message = error.message.as_deref().filter(|m| !m.is_empty());
        if let Some(name) = message.and_then(unknown_function) {
            *self.unknown_functions.entry(name.to_owned()).or_default() += 1;
        }
        let entry = self
            .error_summary
            .entry(error.kind.to_string())
            .or_default();
        entry.count += 1;
        if entry.locations.len() < limit {
            entry.locations.push(location());
            entry.messages.push(message.map(str::to_owned));
        } else {
            entry.locations_truncated += 1;
        }
    }
}

/// The function an engine reason names as unknown, if any.
fn unknown_function(message: &str) -> Option<&str> {
    message
        .strip_prefix("Unknown function: ")
        .map(str::trim)
        .filter(|name| !name.is_empty())
}

/// Recalculate an XLSX file and write formula cached values back through Umya.
///
/// - `input`: source workbook path
/// - `output`: optional destination path; when `None`, updates `input` in-place.
///
/// Formula text is preserved. Cached-value typing is delegated to the active
/// `umya-spreadsheet` implementation.
#[cfg(feature = "umya")]
pub fn recalculate_file(
    input: &Path,
    output: Option<&Path>,
) -> Result<RecalculateSummary, IoError> {
    recalculate_file_with_limit(input, output, DEFAULT_ERROR_LOCATION_LIMIT)
}

#[cfg(feature = "umya")]
pub fn recalculate_file_with_limit(
    input: &Path,
    output: Option<&Path>,
    error_location_limit: usize,
) -> Result<RecalculateSummary, IoError> {
    let mut adapter =
        UmyaAdapter::open_path(input).map_err(|e| IoError::from_backend("umya", e))?;

    let mut engine: Engine<WBResolver> = Engine::new(WBResolver::default(), EvalConfig::default());
    adapter.stream_into_engine(&mut engine)?;
    let (_eval_result, delta) = engine.evaluate_all_with_delta().map_err(IoError::Engine)?;

    let changed_cells: HashSet<PackedSheetCell> = delta.changed_cells.into_iter().collect();
    let formula_cells = adapter.formula_cells();
    let date_system = engine.config.date_system;

    let mut summary = RecalculateSummary::default();
    let mut updates: Vec<FormulaCacheUpdate> = Vec::new();

    for (sheet, row, col) in formula_cells {
        let value = engine
            .get_cell_value(&sheet, row, col)
            .unwrap_or(LiteralValue::Empty);

        summary.record(
            &sheet,
            || format!("{sheet}!{}{}", col_to_a1(col), row),
            &value,
            error_location_limit,
        );

        let should_write = engine
            .sheet_id(&sheet)
            .and_then(|sid| PackedSheetCell::try_from_excel_1based(sid, row, col))
            .is_some_and(|packed| changed_cells.contains(&packed));

        if should_write {
            updates.push(FormulaCacheUpdate {
                sheet,
                row,
                col,
                value,
            });
        }
    }

    summary.status = if summary.errors == 0 {
        RecalculateStatus::Success
    } else {
        RecalculateStatus::ErrorsFound
    };

    if updates.is_empty() {
        if let Some(path) = output
            && path != input
        {
            std::fs::copy(input, path)?;
        }
        return Ok(summary);
    }

    adapter
        .write_formula_caches_batch(&updates, date_system)
        .map_err(|e| IoError::from_backend("umya", e))?;

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
