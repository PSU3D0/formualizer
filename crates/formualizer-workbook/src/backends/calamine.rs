use crate::traits::{
    AccessGranularity, BackendCaps, CellData, MergedRange, SheetData, SpreadsheetReader,
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use calamine::{Data, Range, Reader, Xlsx, open_workbook};
use formualizer_eval::arrow_store::{CellIngest, IngestBuilder, map_error_code};
use formualizer_eval::engine::Engine as EvalEngine;
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::traits::EvaluationContext;
use zip::ZipArchive;

pub struct CalamineAdapter {
    workbook: RwLock<Xlsx<BufReader<File>>>,
    loaded_sheets: HashSet<String>,
    cached_names: Option<Vec<String>>,
    external_link_targets: BTreeMap<u32, String>,
}

impl CalamineAdapter {
    pub fn external_link_target(&self, index: u32) -> Option<&str> {
        self.external_link_targets.get(&index).map(|s| s.as_str())
    }

    fn scan_external_link_targets(path: &Path) -> BTreeMap<u32, String> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return BTreeMap::new(),
        };
        let reader = BufReader::new(file);
        let mut archive = match ZipArchive::new(reader) {
            Ok(a) => a,
            Err(_) => return BTreeMap::new(),
        };

        fn extract_target(xml: &str) -> Option<String> {
            let key = "Target=\"";
            let start = xml.find(key)? + key.len();
            let end = xml[start..].find('"')? + start;
            Some(xml[start..end].to_string())
        }

        let mut out = BTreeMap::new();
        for i in 0..archive.len() {
            let mut entry = match archive.by_index(i) {
                Ok(e) => e,
                Err(_) => continue,
            };
            let name = entry.name().to_string();
            let Some(rest) = name.strip_prefix("xl/externalLinks/_rels/externalLink") else {
                continue;
            };
            let Some(num_str) = rest.strip_suffix(".xml.rels") else {
                continue;
            };
            let Ok(idx) = num_str.parse::<u32>() else {
                continue;
            };

            let mut xml = String::new();
            if entry.read_to_string(&mut xml).is_ok()
                && let Some(target) = extract_target(&xml)
            {
                out.insert(idx, target);
            }
        }
        out
    }

    fn calamine_error_code(e: &calamine::CellErrorType) -> u8 {
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
        map_error_code(kind)
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
                Data::String(s) => Some(LiteralValue::Text(s.clone())),
                Data::Float(f) => Some(LiteralValue::Number(*f)),
                Data::Int(i) => Some(LiteralValue::Int(*i)),
                Data::Bool(b) => Some(LiteralValue::Boolean(*b)),
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
                    Some(LiteralValue::Error(ExcelError::new(kind)))
                }
                Data::DateTime(dt) => Some(LiteralValue::Number(dt.as_f64())),
                Data::DateTimeIso(s) => Some(LiteralValue::Text(s.clone())),
                Data::DurationIso(s) => Some(LiteralValue::Text(s.clone())),
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
                        format!("={formula}")
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
        let path = path.as_ref();
        let external_link_targets = Self::scan_external_link_targets(path);
        let workbook: Xlsx<BufReader<File>> = open_workbook(path)?;
        Ok(Self {
            workbook: RwLock::new(workbook),
            loaded_sheets: HashSet::new(),
            cached_names: None,
            external_link_targets,
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
        #[cfg(feature = "tracing")]
        let _span_load =
            tracing::info_span!("io_stream_into_engine", backend = "calamine").entered();
        // Simple eager load: iterate sheets, add, bulk insert values, then formulas
        let debug = std::env::var("FZ_DEBUG_LOAD")
            .ok()
            .is_some_and(|v| v != "0");
        let t0 = std::time::Instant::now();
        let names = self.sheet_names()?;
        if debug {
            eprintln!("[fz][load] calamine: {} sheets", names.len());
        }
        for n in &names {
            #[cfg(feature = "tracing")]
            let _span_sheet = tracing::info_span!("io_load_sheet", sheet = n.as_str()).entered();
            engine
                .add_sheet(n.as_str())
                .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
        }
        // Speed up load: lazy sheet index + no range expansion during ingestion
        let prev_index_mode = engine.config.sheet_index_mode;
        engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::Lazy);
        let prev_range_limit = engine.config.range_expansion_limit;
        engine.config.range_expansion_limit = 0; // keep ranges compressed while loading

        // Use builders: Arrow for base values; graph builder for formulas/edges
        // Hint the graph to assume new cells during this initial ingest
        engine.set_first_load_assume_new(true);
        engine.reset_ensure_touched();
        let mut total_values = 0usize;
        let mut total_formulas = 0usize;
        // Route formula ingest through the engine's bulk ingest builder for optimal edge construction
        // Arrow bulk ingest for base values (Phase A) is built per-sheet without borrowing the engine
        // Default Arrow chunk rows
        let chunk_rows: usize = 32 * 1024;
        for n in &names {
            let t_sheet = std::time::Instant::now();
            if debug {
                eprintln!("[fz][load] >> sheet '{n}'");
            }
            #[cfg(feature = "tracing")]
            let _span_sheet =
                tracing::info_span!("io_populate_sheet", sheet = n.as_str()).entered();
            // Read directly from calamine ranges to avoid building a BTreeMap
            let (range, formulas_range, dims);
            {
                let mut wb = self.workbook.write();
                let r = wb.worksheet_range(n)?;
                let f = wb.worksheet_formula(n).ok();
                // Respect potential non-(1,1) starts in calamine ranges
                let sr0 = r.start().unwrap_or_default().0; // 0-based
                let sc0 = r.start().unwrap_or_default().1; // 0-based
                // Total logical dimensions include top/left padding
                dims = (r.height() as u32 + sr0, r.width() as u32 + sc0);
                range = r;
                formulas_range = f;
            }
            if debug {
                eprintln!("[fz][load]    dims={}x{}", dims.0, dims.1);
            }
            // Local Arrow ingest builder for this sheet
            // Compute absolute alignment from range start offsets.
            let sr0 = range.start().unwrap_or_default().0 as usize; // top padding (rows)
            let sc0 = range.start().unwrap_or_default().1 as usize; // left padding (cols)
            let width = range.width();
            let height = range.height();
            let abs_cols = sc0 + width;

            let mut aib: IngestBuilder =
                IngestBuilder::new(n, abs_cols, chunk_rows, engine.config.date_system);
            // Helpers: streaming empty-row and used-cells row emitters
            struct RepeatEmptyRow {
                len: usize,
                emitted: usize,
            }
            impl Iterator for RepeatEmptyRow {
                type Item = CellIngest<'static>;
                fn next(&mut self) -> Option<Self::Item> {
                    if self.emitted >= self.len {
                        None
                    } else {
                        self.emitted += 1;
                        Some(CellIngest::Empty)
                    }
                }
                fn size_hint(&self) -> (usize, Option<usize>) {
                    let rem = self.len - self.emitted;
                    (rem, Some(rem))
                }
            }
            impl ExactSizeIterator for RepeatEmptyRow {}

            #[inline]
            fn data_to_cell<'a>(d: &'a Data) -> CellIngest<'a> {
                match d {
                    Data::Empty => CellIngest::Empty,
                    Data::String(s) if s.is_empty() => CellIngest::Empty,
                    Data::String(s) => CellIngest::Text(s.as_str()),
                    Data::Float(f) => CellIngest::Number(*f),
                    Data::Int(i) => CellIngest::Number(*i as f64),
                    Data::Bool(b) => CellIngest::Boolean(*b),
                    Data::Error(e) => {
                        CellIngest::ErrorCode(CalamineAdapter::calamine_error_code(e))
                    }
                    Data::DateTime(dt) => CellIngest::DateSerial(dt.as_f64()),
                    Data::DateTimeIso(s) => CellIngest::Text(s.as_str()),
                    Data::DurationIso(s) => CellIngest::Text(s.as_str()),
                }
            }

            struct RowEmit<'a, 'b, I>
            where
                I: Iterator<Item = (usize, usize, &'a Data)>,
            {
                sc0: usize,
                abs_cols: usize,
                row_rel: usize,
                cur_col: usize,
                used_iter: I,
                carry: &'b mut Option<(usize, usize, &'a Data)>,
            }
            impl<'a, 'b, I> RowEmit<'a, 'b, I>
            where
                I: Iterator<Item = (usize, usize, &'a Data)>,
            {
                #[inline]
                fn pull_next(&mut self) -> Option<(usize, usize, &'a Data)> {
                    if let Some(c) = self.carry.take() {
                        Some(c)
                    } else {
                        self.used_iter.next()
                    }
                }
            }
            impl<'a, 'b, I> Iterator for RowEmit<'a, 'b, I>
            where
                I: Iterator<Item = (usize, usize, &'a Data)>,
            {
                type Item = CellIngest<'a>;
                fn next(&mut self) -> Option<Self::Item> {
                    if self.cur_col >= self.abs_cols {
                        return None;
                    }
                    // Left pad region yields empties
                    if self.cur_col < self.sc0 {
                        self.cur_col += 1;
                        return Some(CellIngest::Empty);
                    }
                    // Consume used cells for this row at the correct columns; fill gaps with empties
                    loop {
                        let peek = self.pull_next();
                        match peek {
                            None => {
                                // No more used cells globally: fill remainder with empties
                                self.cur_col += 1;
                                return Some(CellIngest::Empty);
                            }
                            Some((r, c, v)) => {
                                if r > self.row_rel {
                                    // next used cell is for a future row: emit empty here and keep carry
                                    *self.carry = Some((r, c, v));
                                    self.cur_col += 1;
                                    return Some(CellIngest::Empty);
                                } else if r < self.row_rel {
                                    // advance used cells until we reach this row
                                    continue;
                                } else {
                                    // same row
                                    let target_col_abs = self.sc0 + c;
                                    if self.cur_col < target_col_abs {
                                        // gap before next used cell in this row
                                        *self.carry = Some((r, c, v));
                                        self.cur_col += 1;
                                        return Some(CellIngest::Empty);
                                    } else if self.cur_col == target_col_abs {
                                        // consume this cell and emit value (empty strings turn into Empty)
                                        self.cur_col += 1;
                                        return Some(data_to_cell(v));
                                    } else {
                                        // we somehow passed the target; keep scanning (shouldn't happen)
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                fn size_hint(&self) -> (usize, Option<usize>) {
                    let rem = self.abs_cols - self.cur_col;
                    (rem, Some(rem))
                }
            }
            impl<'a, 'b, I> ExactSizeIterator for RowEmit<'a, 'b, I> where
                I: Iterator<Item = (usize, usize, &'a Data)>
            {
            }

            // Values: iterate rows and append to Arrow builder with absolute row/col alignment
            let tv0 = std::time::Instant::now();
            let mut row_count = 0usize;
            // Prepend top padding rows (absolute alignment)
            for _ in 0..sr0 {
                aib.append_row_cells_iter(RepeatEmptyRow {
                    len: abs_cols,
                    emitted: 0,
                })
                .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
                row_count += 1;
            }

            // Stream rows using used_cells() with per-row gap filling
            let mut used_iter = range.used_cells();
            let mut carry: Option<(usize, usize, &Data)> = None;
            for rr in 0..height {
                let iter = RowEmit {
                    sc0,
                    abs_cols,
                    row_rel: rr,
                    cur_col: 0,
                    used_iter: used_iter.by_ref(),
                    carry: &mut carry,
                };
                // Append row; RowEmit consumes iterator until end-of-row
                aib.append_row_cells_iter(iter)
                    .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
                row_count += 1;
            }
            // Install Arrow sheet into the engine store now
            {
                let asheet = aib.finish();
                let store = engine.sheet_store_mut();
                if let Some(pos) = store.sheets.iter().position(|s| s.name.as_ref() == n) {
                    store.sheets[pos] = asheet;
                } else {
                    store.sheets.push(asheet);
                }
            }
            // Defer adding values until after formulas staging below
            total_values += row_count * abs_cols;
            if debug {
                eprintln!(
                    "[fz][load]    rows={} → arrow in {} ms",
                    row_count,
                    tv0.elapsed().as_millis()
                );
            }

            // Formulas: iterate formulas_range and either stage or parse with caching
            let tf0 = std::time::Instant::now();
            let mut parsed_n = 0usize;
            if let Some(frm_range) = &formulas_range {
                let start_row = frm_range.start().unwrap_or_default().0 as usize;
                let start_col = frm_range.start().unwrap_or_default().1 as usize;
                // cache to reuse parsed AST for shared formulas text
                if engine.config.defer_graph_building {
                    for (row, col, formula) in frm_range.used_cells() {
                        if formula.is_empty() {
                            continue;
                        }
                        let excel_row = (row + start_row + 1) as u32;
                        let excel_col = (col + start_col + 1) as u32;
                        if debug && parsed_n < 16 {
                            eprintln!("[fz][load] formula R{excel_row}C{excel_col} = {formula:?}");
                        }
                        engine.stage_formula_text(n, excel_row, excel_col, formula.clone());
                        parsed_n += 1;
                    }
                } else {
                    let mut cache: rustc_hash::FxHashMap<String, formualizer_parse::ASTNode> =
                        rustc_hash::FxHashMap::default();
                    cache.reserve(4096);
                    let mut builder = engine.begin_bulk_ingest();
                    let sid = builder.add_sheet(n);
                    for (row, col, formula) in frm_range.used_cells() {
                        if formula.is_empty() {
                            continue;
                        }
                        let excel_row = (row + start_row + 1) as u32;
                        let excel_col = (col + start_col + 1) as u32;
                        let key_owned: String = if formula.starts_with('=') {
                            formula.clone()
                        } else {
                            format!("={formula}")
                        };
                        if debug && parsed_n < 16 {
                            eprintln!(
                                "[fz][load] formula R{excel_row}C{excel_col} = {key_owned:?}"
                            );
                        }
                        let ast = if let Some(ast) = cache.get(&key_owned) {
                            ast.clone()
                        } else {
                            let parsed =
                                formualizer_parse::parser::parse(&key_owned).map_err(|e| {
                                    calamine::Error::Io(std::io::Error::other(e.to_string()))
                                })?;
                            cache.insert(key_owned, parsed.clone());
                            parsed
                        };
                        builder.add_formulas(sid, std::iter::once((excel_row, excel_col, ast)));
                        parsed_n += 1;
                        if debug && parsed_n.is_multiple_of(5000) {
                            eprintln!("[fz][load]    parsed formulas: {parsed_n}");
                        }
                    }
                    let _ = builder.finish();
                }
            }
            total_formulas += parsed_n;
            if debug {
                eprintln!(
                    "[fz][load]    formulas={} in {} ms",
                    parsed_n,
                    tf0.elapsed().as_millis()
                );
                eprintln!(
                    "[fz][load] << sheet '{}' staged in {} ms",
                    n,
                    t_sheet.elapsed().as_millis()
                );
            }
            // Mark as loaded for API parity
            self.loaded_sheets.insert(n.to_string());
        }
        let tend0 = std::time::Instant::now();
        // Finish builder and finalize ingestion
        let tcommit0 = std::time::Instant::now();
        // (graph ingest finished per-sheet above)
        // Finish Arrow ingest after formulas are staged (stores ArrowSheets into engine)
        // (Arrow sheets are installed per-sheet above)
        if debug {
            eprintln!(
                "[fz][load] commit: builder finish in {} ms",
                tcommit0.elapsed().as_millis()
            );
            eprintln!(
                "[fz][load] done: values={}, formulas={}, batch_close={} ms, total={} ms",
                total_values,
                total_formulas,
                tend0.elapsed().as_millis(),
                t0.elapsed().as_millis(),
            );
        }
        // Build sheet indexes after load to accelerate used-region queries
        for n in &names {
            engine.finalize_sheet_index(n);
        }

        engine.set_first_load_assume_new(false);
        engine.reset_ensure_touched();
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
