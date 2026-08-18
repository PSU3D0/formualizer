use super::Engine;
use crate::SheetId;
use crate::traits::EvaluationContext;
use formualizer_common::{ExcelError, ExcelErrorKind};

/// Trait implemented by data sources that can stream workbook contents into an Engine.
/// This lives in formualizer-eval so IO backends can depend on it without creating cycles.
pub trait EngineLoadStream<R>
where
    R: EvaluationContext,
{
    type Error;
    fn stream_into_engine(&mut self, engine: &mut Engine<R>) -> Result<(), Self::Error>;
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    /// Register the sheets of a file being loaded, folding the engine's seeded
    /// default sheet into the file's first sheet when that is safe.
    ///
    /// This is the single entry point every [`EngineLoadStream`] implementation
    /// must use instead of looping over `add_sheet`. A freshly constructed
    /// [`Engine`] is seeded with one default sheet (`Sheet1` unless configured
    /// otherwise). Appending the file's sheets next to that seed leaves a
    /// phantom sheet that does not exist in the file and shifts every
    /// `SHEET()`/`SHEETS()` result and every [`SheetId`] by one (issue #332).
    ///
    /// Behaviour:
    ///
    /// * **Duplicate names are rejected.** Sheet names are unique and
    ///   case-insensitive in Excel; `add_sheet` is idempotent, so two file
    ///   sheets sharing a name would silently merge and the second sheet's
    ///   cells would overwrite the first's. That is silent data loss, so it is
    ///   an error instead.
    /// * **The default sheet is only folded into a fresh engine.** If the
    ///   engine already holds user content the default sheet is left exactly
    ///   as-is and the file's sheets are added alongside it. Renaming a
    ///   populated sheet would hand the user's sheet to the file, letting the
    ///   file's cells overwrite it and rewriting formulas that mention it;
    ///   existing data is always preserved instead.
    /// * The fold is a rename, so the default [`SheetId`] and the sheet's
    ///   position in the Arrow store are reused by the file's first sheet and
    ///   the remaining sheets keep the file's order.
    ///
    /// Returns the [`SheetId`] of each name, in the order given.
    pub fn adopt_file_sheets<'a, I>(&mut self, names: I) -> Result<Vec<SheetId>, ExcelError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let names: Vec<&str> = names.into_iter().collect();

        // Reject duplicates before mutating anything so a malformed file cannot
        // leave the engine half-registered.
        let mut seen: std::collections::HashMap<String, &str> =
            std::collections::HashMap::with_capacity(names.len());
        for name in &names {
            if let Some(previous) = seen.insert(name.to_lowercase(), name) {
                return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                    "Duplicate sheet name in workbook: '{name}' collides with '{previous}' \
                     (sheet names are case-insensitive)"
                )));
            }
        }

        if let Some(first) = names.first()
            && *first != self.default_sheet_name()
            && self.is_fresh_for_load()
        {
            let default_id = self.default_sheet_id();
            self.rename_sheet(default_id, first)?;
        }

        let mut ids = Vec::with_capacity(names.len());
        for name in &names {
            ids.push(self.add_sheet(name)?);
        }
        Ok(ids)
    }

    /// True when the engine still looks exactly as [`Engine::new`] left it: one
    /// sheet, which is the default sheet, with no cells, no values, no named
    /// ranges and no row-visibility state.
    ///
    /// Only in that state can the default sheet be renamed without taking
    /// something away from the caller.
    fn is_fresh_for_load(&self) -> bool {
        let default_id = self.default_sheet_id();
        let sheets = self.graph.sheet_reg().all_sheets();
        if sheets.len() != 1 || sheets[0].0 != default_id {
            return false;
        }
        if self
            .graph
            .grid_vertices_in_sheet(default_id)
            .next()
            .is_some()
        {
            return false;
        }
        if self.graph.named_ranges_iter().next().is_some()
            || self.graph.sheet_named_ranges_iter().next().is_some()
        {
            return false;
        }
        if self.has_row_visibility_state() || self.has_staged_formulas() {
            return false;
        }
        match self.sheet_store().sheets.len() {
            0 => true,
            1 => {
                let sheet = &self.sheet_store().sheets[0];
                sheet.nrows == 0 && sheet.columns.is_empty()
            }
            _ => false,
        }
    }
}
