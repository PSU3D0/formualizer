use crate::arrow_store::{ArrowSheet, IngestBuilder};
use crate::engine::Engine;
use crate::traits::EvaluationContext;
use formualizer_common::{ExcelError, LiteralValue};
use rustc_hash::FxHashMap;

#[derive(Debug, Clone, Default)]
pub struct ArrowBulkIngestSummary {
    pub sheets: usize,
    pub total_rows: usize,
}

/// Bulk Arrow ingest builder for Phase A base values.
pub struct ArrowBulkIngestBuilder<'e, R: EvaluationContext> {
    engine: &'e mut Engine<R>,
    builders: FxHashMap<String, IngestBuilder>,
    rows: FxHashMap<String, usize>,
}

impl<'e, R: EvaluationContext> ArrowBulkIngestBuilder<'e, R> {
    pub fn new(engine: &'e mut Engine<R>) -> Self {
        Self {
            engine,
            builders: FxHashMap::default(),
            rows: FxHashMap::default(),
        }
    }

    /// Add a sheet ingest target. Creates or replaces any existing Arrow sheet on finish.
    pub fn add_sheet(&mut self, name: &str, ncols: usize, chunk_rows: usize) {
        let ib = IngestBuilder::new(name, ncols, chunk_rows, self.engine.config.date_system);
        self.builders.insert(name.to_string(), ib);
        self.rows.insert(name.to_string(), 0);
        // Ensure the graph knows about the sheet name now for consistent IDs/references
        self.engine.graph.sheet_id_mut(name);
    }

    /// Append a single row of values for the given sheet (0-based col order, length == ncols).
    pub fn append_row(&mut self, name: &str, row: &[LiteralValue]) -> Result<(), ExcelError> {
        let ib = self
            .builders
            .get_mut(name)
            .expect("sheet must be added before append_row");
        ib.append_row(row)?;
        *self.rows.get_mut(name).unwrap() += 1;
        Ok(())
    }

    /// Finish all sheet builders, installing ArrowSheets into the engine store.
    pub fn finish(mut self) -> Result<ArrowBulkIngestSummary, ExcelError> {
        let mut sheets: Vec<(String, ArrowSheet)> = Vec::with_capacity(self.builders.len());
        for (name, builder) in self.builders.drain() {
            let sheet = builder.finish();
            sheets.push((name, sheet));
        }
        // Insert or replace by name
        for (name, sheet) in sheets {
            let store = self.engine.sheet_store_mut();
            if let Some(pos) = store.sheets.iter().position(|s| s.name.as_ref() == name) {
                store.sheets[pos] = sheet;
            } else {
                store.sheets.push(sheet);
            }
        }
        let total_rows = self.rows.values().copied().sum();
        Ok(ArrowBulkIngestSummary {
            sheets: self.rows.len(),
            total_rows,
        })
    }
}

/// Bulk Arrow update builder for Phase C. Chooses overlay vs rebuild per chunk.
pub struct ArrowBulkUpdateBuilder<'e, R: EvaluationContext> {
    engine: &'e mut Engine<R>,
    // sheet -> col0 -> row0 -> value
    updates: FxHashMap<String, FxHashMap<usize, FxHashMap<usize, LiteralValue>>>,
}

impl<'e, R: EvaluationContext> ArrowBulkUpdateBuilder<'e, R> {
    pub fn new(engine: &'e mut Engine<R>) -> Self {
        Self {
            engine,
            updates: FxHashMap::default(),
        }
    }

    pub fn update_cell(&mut self, sheet: &str, row: u32, col: u32, value: LiteralValue) {
        let s = self.updates.entry(sheet.to_string()).or_default();
        let c = s.entry(col.saturating_sub(1) as usize).or_default();
        c.insert(row.saturating_sub(1) as usize, value);
    }

    pub fn finish(mut self) -> Result<usize, ExcelError> {
        let date_system = self.engine.config.date_system;
        let mut total = 0usize;
        for (sheet_name, by_col) in self.updates.drain() {
            let sheet_exists = self.engine.sheet_store().sheet(&sheet_name).is_some();
            if !sheet_exists {
                continue;
            }
            {
                let store = self.engine.sheet_store_mut();
                for (col0, rows_map) in by_col {
                    total += rows_map.len();
                    let col = (col0 as u32) + 1;
                    for (row0, value) in rows_map {
                        let row = (row0 as u32) + 1;
                        store.write_cell_value(&sheet_name, row, col, &value, date_system);
                    }
                }
            }
        }
        // Advance snapshot and mark edited
        self.engine.mark_data_edited();
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EvalConfig;
    use crate::test_workbook::TestWorkbook;

    #[test]
    fn arrow_bulk_ingest_basic() {
        let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 3, 2);
        ab.append_row(
            "S",
            &[
                LiteralValue::Number(1.0),
                LiteralValue::Text("a".into()),
                LiteralValue::Empty,
            ],
        )
        .unwrap();
        ab.append_row(
            "S",
            &[
                LiteralValue::Boolean(true),
                LiteralValue::Text("".into()),
                LiteralValue::Error(formualizer_common::ExcelError::new_value()),
            ],
        )
        .unwrap();
        let summary = ab.finish().unwrap();
        assert_eq!(summary.sheets, 1);
        assert_eq!(summary.total_rows, 2);

        let sheet = engine
            .sheet_store()
            .sheet("S")
            .expect("arrow sheet present");
        assert_eq!(sheet.columns.len(), 3);
        assert_eq!(sheet.nrows, 2);
        // Validate chunking (chunk_rows=2 => single chunk)
        for col in &sheet.columns {
            assert_eq!(col.chunks.len(), 1);
            assert_eq!(col.chunks[0].len(), 2);
        }
    }
}
