use crate::error::{col_to_a1, IoError};
use crate::traits::{LoadStrategy, SpreadsheetReader};
use formualizer_core::parser;
use formualizer_eval::engine::Engine;
use formualizer_eval::traits::EvaluationContext;
use std::time::Instant;

#[derive(Debug, Default)]
pub struct LoaderStats {
    pub cells_loaded: usize,
    pub formulas_loaded: usize,
    pub sheets_loaded: usize,
    pub load_time_ms: u64,
    // New granular metrics (aggregated across all sheets loaded in this session)
    pub backend_read_time_ms: u64,
    pub engine_insert_time_ms: u64,
    pub vertex_alloc_time_ms: u64,
    pub sheet_index_time_ms: u64,
    pub edges_rebuild_time_ms: u64,
}

pub struct WorkbookLoader<B: SpreadsheetReader> {
    backend: B,
    strategy: LoadStrategy,
    stats: LoaderStats,
}

impl<B: SpreadsheetReader> WorkbookLoader<B> {
    pub fn new(backend: B, strategy: LoadStrategy) -> Self {
        Self {
            backend,
            strategy,
            stats: LoaderStats::default(),
        }
    }

    pub fn stats(&self) -> &LoaderStats {
        &self.stats
    }

    pub fn load_into_engine<R>(&mut self, engine: &mut Engine<R>) -> Result<(), IoError>
    where
        R: EvaluationContext,
    {
        let start = Instant::now();

        // Use batch API for performance
        engine.begin_batch();

        let result = match self.strategy {
            LoadStrategy::EagerAll => {
                // Load all sheets
                for sheet in self
                    .backend
                    .sheet_names()
                    .map_err(|e| IoError::from_backend("backend", e))?
                {
                    self.load_sheet_into_graph(&sheet, engine)?;
                }
                Ok(())
            }
            LoadStrategy::EagerSheet => {
                // Load default or first sheet
                let sheets = self
                    .backend
                    .sheet_names()
                    .map_err(|e| IoError::from_backend("backend", e))?;
                if let Some(sheet) = sheets.first() {
                    self.load_sheet_into_graph(sheet, engine)
                } else {
                    Ok(())
                }
            }
            LoadStrategy::LazyCell | LoadStrategy::LazyRange { .. } => {
                // For v1, treat as EagerSheet since engine doesn't support on-demand
                eprintln!("Warning: LazyCell/LazyRange not yet supported, using EagerSheet");
                let sheets = self
                    .backend
                    .sheet_names()
                    .map_err(|e| IoError::from_backend("backend", e))?;
                if let Some(sheet) = sheets.first() {
                    self.load_sheet_into_graph(sheet, engine)
                } else {
                    Ok(())
                }
            }
            LoadStrategy::WriteOnly => Ok(()),
        };

        engine.end_batch();

        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.stats.load_time_ms = if elapsed_ms == 0 { 1 } else { elapsed_ms };
        result
    }

    fn load_sheet_into_graph<R>(
        &mut self,
        sheet: &str,
        engine: &mut Engine<R>,
    ) -> Result<(), IoError>
    where
        R: EvaluationContext,
    {
        use std::time::Instant as _Instant;
        let t_read_start = _Instant::now();
        let sheet_data = self
            .backend
            .read_sheet(sheet)
            .map_err(|e| IoError::from_backend("backend", e))?;
        let read_elapsed = t_read_start.elapsed();
        self.stats.backend_read_time_ms += read_elapsed.as_millis() as u64;

        self.stats.cells_loaded += sheet_data.cells.len();

        let t_insert_start = _Instant::now();
        // Process cells in batches for better performance
        let has_any_formula = sheet_data
            .cells
            .iter()
            .any(|(_, c)| c.formula.as_ref().map(|s| !s.is_empty()).unwrap_or(false));

        if !has_any_formula {
            // Fast path: reserve and bulk insert values
            engine.begin_batch();
            let to_insert = sheet_data
                .cells
                .into_iter()
                .filter_map(|((r, c), cell)| cell.value.map(|v| (r, c, v)));
            engine.graph.bulk_insert_values(sheet, to_insert);
            engine.end_batch();
        } else {
            for ((row, col), cell) in sheet_data.cells {
                // Value first (if present) using normal API (ensures snapshot / consistency)
                if let Some(value) = cell.value {
                    engine
                        .set_cell_value(sheet, row, col, value)
                        .map_err(|e| IoError::Engine(e))?;
                }
                if let Some(formula_str) = cell.formula {
                    if formula_str.is_empty() {
                        continue;
                    }
                    let ast = parser::parse(&formula_str).map_err(|e| IoError::FormulaParser {
                        sheet: sheet.to_string(),
                        row,
                        col: col_to_a1(col),
                        message: e.to_string(),
                    })?;
                    engine
                        .set_cell_formula(sheet, row, col, ast)
                        .map_err(IoError::Engine)?;
                    self.stats.formulas_loaded += 1;
                }
            }
        }

        self.stats.sheets_loaded += 1;
        let insert_elapsed = t_insert_start.elapsed();
        self.stats.engine_insert_time_ms += insert_elapsed.as_millis() as u64;
        Ok(())
    }
}
