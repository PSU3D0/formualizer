use crate::error::{col_to_a1, IoError};
use crate::traits::{LoadStrategy, NamedRange, NamedRangeScope, SpreadsheetReader};
use formualizer_eval::engine::Engine;
use formualizer_eval::traits::EvaluationContext;
use formualizer_eval::{reference::CellRef, reference::Coord};
use formualizer_parse::parser;
use rustc_hash::FxHashSet;
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
    pending_named_ranges: Vec<NamedRange>,
    seen_named_ranges: FxHashSet<(NamedRangeScope, String, String)>,
}

impl<B: SpreadsheetReader> WorkbookLoader<B> {
    pub fn new(backend: B, strategy: LoadStrategy) -> Self {
        Self {
            backend,
            strategy,
            stats: LoaderStats::default(),
            pending_named_ranges: Vec::new(),
            seen_named_ranges: FxHashSet::default(),
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

        if result.is_ok() {
            self.register_named_ranges(engine)?;
        }

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
                        .map_err(IoError::Engine)?;
                }
                if let Some(formula_str) = cell.formula {
                    if formula_str.is_empty() {
                        continue;
                    }
                    if engine.config.defer_graph_building {
                        engine.stage_formula_text(sheet, row, col, formula_str);
                        self.stats.formulas_loaded += 1;
                    } else {
                        let ast =
                            parser::parse(&formula_str).map_err(|e| IoError::FormulaParser {
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
        }

        self.stats.sheets_loaded += 1;
        let insert_elapsed = t_insert_start.elapsed();
        self.stats.engine_insert_time_ms += insert_elapsed.as_millis() as u64;

        self.collect_named_ranges(sheet, &sheet_data.named_ranges);
        Ok(())
    }

    fn collect_named_ranges(&mut self, sheet: &str, ranges: &[NamedRange]) {
        for named in ranges {
            if named.address.sheet != sheet {
                // Defer to the sheet the range references to avoid duplicating entries.
                continue;
            }
            let key = (
                named.scope.clone(),
                named.address.sheet.clone(),
                named.name.clone(),
            );
            if !self.seen_named_ranges.insert(key) {
                continue;
            }
            self.pending_named_ranges.push(named.clone());
        }
    }

    fn register_named_ranges<R>(&mut self, engine: &mut Engine<R>) -> Result<(), IoError>
    where
        R: EvaluationContext,
    {
        for named in self.pending_named_ranges.drain(..) {
            let addr = &named.address;
            let sheet_id = match engine.graph.sheet_id(&addr.sheet) {
                Some(id) => id,
                None => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        name = %named.name,
                        sheet = %addr.sheet,
                        "named range references sheet that was not loaded; skipping"
                    );
                    continue;
                }
            };

            let sr0 = addr.start_row.saturating_sub(1);
            let sc0 = addr.start_col.saturating_sub(1);
            let er0 = addr.end_row.saturating_sub(1);
            let ec0 = addr.end_col.saturating_sub(1);

            let start_coord = Coord::new(sr0, sc0, true, true);
            let end_coord = Coord::new(er0, ec0, true, true);
            let start_ref = CellRef::new(sheet_id, start_coord);
            let end_ref = CellRef::new(sheet_id, end_coord);

            let definition = if sr0 == er0 && sc0 == ec0 {
                formualizer_eval::engine::named_range::NamedDefinition::Cell(start_ref)
            } else {
                let range_ref = formualizer_eval::reference::RangeRef::new(start_ref, end_ref);
                formualizer_eval::engine::named_range::NamedDefinition::Range(range_ref)
            };

            let scope = match named.scope {
                NamedRangeScope::Workbook => {
                    formualizer_eval::engine::named_range::NameScope::Workbook
                }
                NamedRangeScope::Sheet => {
                    formualizer_eval::engine::named_range::NameScope::Sheet(sheet_id)
                }
            };

            engine
                .graph
                .define_name(&named.name, definition, scope)
                .map_err(IoError::Engine)?;
        }

        self.seen_named_ranges.clear();
        Ok(())
    }
}
