#![cfg(feature = "calamine")]

use crate::traits::{
    AccessGranularity, BackendCaps, CellData, MergedRange, SheetData, SpreadsheetReader,
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use calamine::{open_workbook, Data, Range, Reader, Xlsx};
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::Engine as EvalEngine;
use formualizer_eval::traits::EvaluationContext;

pub struct CalamineAdapter {
    workbook: RwLock<Xlsx<BufReader<File>>>,
    loaded_sheets: HashSet<String>,
    cached_names: Option<Vec<String>>,
}

impl CalamineAdapter {
    fn convert_value(data: &Data) -> LiteralValue {
        match data {
            Data::Empty => LiteralValue::Empty,
            Data::String(s) => LiteralValue::Text(s.clone()),
            Data::Float(f) => LiteralValue::Number(*f),
            Data::Int(i) => LiteralValue::Int(*i as i64),
            Data::Bool(b) => LiteralValue::Boolean(*b),
            Data::Error(e) => {
                let kind = match e {
                    calamine::CellErrorType::Div0 => ExcelErrorKind::Div,
                    calamine::CellErrorType::NA => ExcelErrorKind::Na,
                    calamine::CellErrorType::Name => ExcelErrorKind::Name,
                    calamine::CellErrorType::Null => ExcelErrorKind::Null,
                    calamine::CellErrorType::Num => ExcelErrorKind::Num,
                    calamine::CellErrorType::Ref => ExcelErrorKind::Ref,
                    calamine::CellErrorType::Value => ExcelErrorKind::Value,
                    _ => ExcelErrorKind::Value,
                };
                LiteralValue::Error(ExcelError::new(kind))
            }
            Data::DateTime(dt) => {
                // Convert to Excel serial number for now (no chrono conversion here)
                LiteralValue::Number(dt.as_f64())
            }
            Data::DateTimeIso(s) => LiteralValue::Text(s.clone()),
            Data::DurationIso(s) => LiteralValue::Text(s.clone()),
        }
    }

    fn range_to_cells(
        range: &Range<Data>,
        formulas: Option<&Range<String>>,
    ) -> BTreeMap<(u32, u32), CellData> {
        let mut cells = BTreeMap::new();

        // We use the cells() iterator which gives us actual positions

        // Process values using actual positions

        let start_row = range.start().unwrap_or_default().0 as usize;
        let start_col = range.start().unwrap_or_default().1 as usize;

        for (row, col, val) in range.used_cells() {
            // Calamine uses 0-based indexing, convert to 1-based for Excel
            let excel_row = (row + start_row + 1) as u32;
            let excel_col = (col + start_col + 1) as u32;

            // Convert value (skip empty cells and empty strings)
            let value = match val {
                Data::Empty => None,
                Data::String(s) if s.is_empty() => None, // Treat empty strings as no value
                _ => Some(Self::convert_value(val)),
            };

            if value.is_some() {
                cells.insert(
                    (excel_row, excel_col),
                    CellData {
                        value,
                        formula: None,
                        style: None,
                    },
                );
            }
        }

        // Process formulas using their actual positions
        if let Some(frm_range) = formulas {
            let start_row = frm_range.start().unwrap_or_default().0 as usize;
            let start_col = frm_range.start().unwrap_or_default().1 as usize;

            for (row, col, formula) in frm_range.used_cells() {
                if !formula.is_empty() {
                    // Convert to 1-based Excel coordinates
                    let excel_row = (row + start_row + 1) as u32;
                    let excel_col = (col + start_col + 1) as u32;

                    // Ensure formula starts with '=' for proper parsing
                    let formula_with_eq = if formula.starts_with('=') {
                        formula.clone()
                    } else {
                        format!("={}", formula)
                    };

                    // Update existing cell or create new one with formula
                    cells
                        .entry((excel_row, excel_col))
                        .and_modify(|cell| cell.formula = Some(formula_with_eq.clone()))
                        .or_insert_with(|| CellData {
                            value: None,
                            formula: Some(formula_with_eq),
                            style: None,
                        });
                }
            }
        }

        cells
    }
}

impl SpreadsheetReader for CalamineAdapter {
    type Error = calamine::Error;

    fn access_granularity(&self) -> AccessGranularity {
        AccessGranularity::Sheet
    }

    fn capabilities(&self) -> BackendCaps {
        BackendCaps {
            read: true,
            formulas: true,
            lazy_loading: false,
            random_access: false,
            styles: false,
            bytes_input: false,
            // conservative defaults
            date_system_1904: false,
            merged_cells: false,
            rich_text: false,
            hyperlinks: false,
            data_validations: false,
            shared_formulas: false,
            ..Default::default()
        }
    }

    fn sheet_names(&self) -> Result<Vec<String>, Self::Error> {
        if let Some(names) = &self.cached_names {
            return Ok(names.clone());
        }
        let names = self.workbook.read().sheet_names().to_vec();
        Ok(names)
    }

    fn open_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let workbook: Xlsx<BufReader<File>> = open_workbook(path)?;
        Ok(Self {
            workbook: RwLock::new(workbook),
            loaded_sheets: HashSet::new(),
            cached_names: None,
        })
    }

    fn open_reader(_reader: Box<dyn Read + Send + Sync>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // calamine expects concrete Read + Seek; not easily supported via trait object
        Err(calamine::Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "open_reader not supported for CalamineAdapter",
        )))
    }

    fn open_bytes(_data: Vec<u8>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Err(calamine::Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "open_bytes not supported for CalamineAdapter",
        )))
    }

    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, Self::Error> {
        // Calamine loads entire sheet; filter after read_sheet
        let data = self.read_sheet(sheet)?;
        Ok(data
            .cells
            .into_iter()
            .filter(|((r, c), _)| *r >= start.0 && *r <= end.0 && *c >= start.1 && *c <= end.1)
            .collect())
    }

    fn read_sheet(&mut self, sheet: &str) -> Result<SheetData, Self::Error> {
        // Values
        let mut wb = self.workbook.write();
        let range = wb.worksheet_range(sheet)?;
        // Formulas (same dims as range, may be empty strings)
        let formulas = wb.worksheet_formula(sheet).ok();

        let dims = (range.height() as u32, range.width() as u32);
        let cells = Self::range_to_cells(&range, formulas.as_ref());

        self.loaded_sheets.insert(sheet.to_string());

        Ok(SheetData {
            cells,
            dimensions: Some(dims),
            tables: vec![],
            named_ranges: vec![],
            date_system_1904: false, // calamine XLSX currently doesn’t expose this
            merged_cells: Vec::<MergedRange>::new(),
            hidden: false,
        })
    }

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        let mut wb = self.workbook.write();
        wb.worksheet_range(sheet)
            .ok()
            .map(|r| (r.height() as u32, r.width() as u32))
    }

    fn is_loaded(&self, sheet: &str, _row: Option<u32>, _col: Option<u32>) -> bool {
        self.loaded_sheets.contains(sheet)
    }
}

impl<R> EngineLoadStream<R> for CalamineAdapter
where
    R: EvaluationContext,
{
    type Error = calamine::Error;

    fn stream_into_engine(&mut self, engine: &mut EvalEngine<R>) -> Result<(), Self::Error> {
        // Simple eager load: iterate sheets, add, bulk insert values, then formulas
        let debug = std::env::var("FZ_DEBUG_LOAD")
            .ok()
            .map_or(false, |v| v != "0");
        let t0 = std::time::Instant::now();
        let names = self.sheet_names()?;
        if debug {
            eprintln!("[fz][load] calamine: {} sheets", names.len());
        }
        for n in &names {
            engine.graph.add_sheet(n.as_str()).map_err(|e| {
                calamine::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
        }
        // Speed up load: lazy sheet index + no range expansion during ingestion
        let prev_index_mode = engine.config.sheet_index_mode;
        engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::Lazy);
        let prev_range_limit = engine.config.range_expansion_limit;
        engine.config.range_expansion_limit = 0; // keep ranges compressed while loading

        engine.begin_batch();
        let mut total_values = 0usize;
        let mut total_formulas = 0usize;
        for n in &names {
            let t_sheet = std::time::Instant::now();
            if debug {
                eprintln!("[fz][load] >> sheet '{}'", n);
            }
            let sheet = self.read_sheet(n)?;
            if debug {
                let dims = sheet.dimensions.unwrap_or((0, 0));
                eprintln!(
                    "[fz][load]    dims={}x{}, used_cells={}",
                    dims.0,
                    dims.1,
                    sheet.cells.len()
                );
            }
            if !sheet.cells.is_empty() {
                let values: Vec<(u32, u32, formualizer_common::LiteralValue)> = sheet
                    .cells
                    .iter()
                    .filter_map(|(&(r, c), cell)| cell.value.clone().map(|v| (r, c, v)))
                    .collect();
                total_values += values.len();
                if !values.is_empty() {
                    let tv0 = std::time::Instant::now();
                    engine.graph.bulk_insert_values(n, values);
                    if debug {
                        eprintln!(
                            "[fz][load]    bulk values in {} ms",
                            tv0.elapsed().as_millis()
                        );
                    }
                }
            }
            let mut formulas = 0usize;
            let mut cache: std::collections::HashMap<&str, formualizer_core::ASTNode> =
                std::collections::HashMap::new();
            let tf0 = std::time::Instant::now();
            let mut batch: Vec<(u32, u32, formualizer_core::ASTNode)> = Vec::new();
            batch.reserve(sheet.cells.len());
            for (&(r, c), cell) in &sheet.cells {
                if let Some(ref f) = cell.formula {
                    let key = f.as_str();
                    let ast = if let Some(ast) = cache.get(key) {
                        ast.clone()
                    } else {
                        let parsed = formualizer_core::parser::parse(key).map_err(|e| {
                            calamine::Error::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            ))
                        })?;
                        cache.insert(key, parsed.clone());
                        parsed
                    };
                    batch.push((r, c, ast));
                    formulas += 1;
                    if debug && (formulas % 5000 == 0) {
                        eprintln!("[fz][load]    parsed formulas: {}", formulas);
                    }
                }
            }
            if !batch.is_empty() {
                engine.bulk_set_formulas(n.as_str(), batch).map_err(|e| {
                    calamine::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;
            }
            total_formulas += formulas;
            if debug {
                eprintln!(
                    "[fz][load]    formulas={} in {} ms",
                    formulas,
                    tf0.elapsed().as_millis()
                );
                eprintln!(
                    "[fz][load] << sheet '{}' done in {} ms",
                    n,
                    t_sheet.elapsed().as_millis()
                );
            }
        }
        let tend0 = std::time::Instant::now();
        engine.end_batch();
        // Restore config after load
        engine.set_sheet_index_mode(prev_index_mode);
        engine.config.range_expansion_limit = prev_range_limit;
        if debug {
            eprintln!(
                "[fz][load] done: values={}, formulas={}, batch_close={} ms, total={} ms",
                total_values,
                total_formulas,
                tend0.elapsed().as_millis(),
                t0.elapsed().as_millis(),
            );
        }
        Ok(())
    }
}
