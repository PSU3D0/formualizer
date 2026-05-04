use crate::load_limits::enforce_sheet_load_limits;
use crate::traits::{
    AccessGranularity, AdapterLoadStats, BackendCaps, CellData, DefinedName, DefinedNameDefinition,
    DefinedNameScope, MergedRange, SheetData, SpreadsheetReader,
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Seek};
use std::path::Path;
use std::sync::Arc;

use calamine::{Data, Range, Reader, Xlsx, open_workbook, open_workbook_from_rs};
use formualizer_common::RangeAddress;
use formualizer_eval::arrow_store::{CellIngest, IngestBuilder, map_error_code};
use formualizer_eval::engine::ingest::EngineLoadStream;
use formualizer_eval::engine::{Engine as EvalEngine, FormulaIngestBatch, FormulaIngestRecord};
use formualizer_eval::traits::EvaluationContext;
use formualizer_parse::parser::ReferenceType;
use quick_xml::Reader as XmlReader;
use quick_xml::events::{BytesRef, BytesStart, Event};
use quick_xml::name::QName;
use zip::ZipArchive;

type FormulaBatch = FormulaIngestBatch;

enum CalamineWorkbook {
    File(Xlsx<BufReader<File>>),
    Bytes(Xlsx<Cursor<Vec<u8>>>),
}

impl CalamineWorkbook {
    fn worksheet_range(&mut self, sheet: &str) -> Result<Range<Data>, calamine::Error> {
        match self {
            Self::File(workbook) => workbook.worksheet_range(sheet).map_err(Into::into),
            Self::Bytes(workbook) => workbook.worksheet_range(sheet).map_err(Into::into),
        }
    }

    fn worksheet_formula(&mut self, sheet: &str) -> Result<Range<String>, calamine::Error> {
        match self {
            Self::File(workbook) => workbook.worksheet_formula(sheet).map_err(Into::into),
            Self::Bytes(workbook) => workbook.worksheet_formula(sheet).map_err(Into::into),
        }
    }
}

struct DebugTimer {
    #[cfg(not(target_arch = "wasm32"))]
    started: std::time::Instant,
}

impl DebugTimer {
    fn start() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            started: std::time::Instant::now(),
        }
    }

    fn elapsed_millis(&self) -> u128 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.started.elapsed().as_millis()
        }
        #[cfg(target_arch = "wasm32")]
        {
            0
        }
    }
}

pub struct CalamineAdapter {
    workbook: RwLock<CalamineWorkbook>,
    loaded_sheets: HashSet<String>,
    cached_names: Option<Vec<String>>,
    defined_names: Vec<DefinedName>,
    external_link_targets: BTreeMap<u32, String>,
    load_stats: AdapterLoadStats,
}

impl CalamineAdapter {
    const EXCEL_MAX_ROWS: u32 = 1_048_576;
    const EXCEL_MAX_COLS: u32 = 16_384;

    pub fn external_link_target(&self, index: u32) -> Option<&str> {
        self.external_link_targets.get(&index).map(|s| s.as_str())
    }

    fn normalize_open_ended_bounds(
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
    ) -> Option<(u32, u32, u32, u32)> {
        let mut sr = start_row;
        let mut sc = start_col;
        let mut er = end_row;
        let mut ec = end_col;

        if sr.is_none() && er.is_none() {
            sr = Some(1);
            er = Some(Self::EXCEL_MAX_ROWS);
        }
        if sc.is_none() && ec.is_none() {
            sc = Some(1);
            ec = Some(Self::EXCEL_MAX_COLS);
        }

        if sr.is_some() && er.is_none() {
            er = Some(Self::EXCEL_MAX_ROWS);
        }
        if er.is_some() && sr.is_none() {
            sr = Some(1);
        }

        if sc.is_some() && ec.is_none() {
            ec = Some(Self::EXCEL_MAX_COLS);
        }
        if ec.is_some() && sc.is_none() {
            sc = Some(1);
        }

        let sr = sr?;
        let sc = sc?;
        let er = er?;
        let ec = ec?;

        if er < sr || ec < sc {
            return None;
        }

        Some((sr, sc, er, ec))
    }

    fn convert_defined_name(
        name: &str,
        raw_formula: &str,
        local_sheet_id: Option<usize>,
        sheet_names: &[String],
    ) -> Option<DefinedName> {
        let mut trimmed = raw_formula.trim();
        if let Some(rest) = trimmed.strip_prefix('=') {
            trimmed = rest.trim();
        }
        if trimmed.is_empty() || trimmed.contains(',') {
            return None;
        }

        let reference = ReferenceType::from_string(trimmed).ok()?;
        let scope_sheet = local_sheet_id.and_then(|idx| sheet_names.get(idx).cloned());
        let scope = if scope_sheet.is_some() {
            DefinedNameScope::Sheet
        } else {
            DefinedNameScope::Workbook
        };
        let base_sheet = scope_sheet.as_deref();

        let (sheet_name, start_row, start_col, end_row, end_col) = match reference {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet = sheet.or_else(|| base_sheet.map(|s| s.to_string()))?;
                (sheet, row, col, row, col)
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                let (sr, sc, er, ec) =
                    Self::normalize_open_ended_bounds(start_row, start_col, end_row, end_col)?;
                let sheet = sheet.or_else(|| base_sheet.map(|s| s.to_string()))?;
                (sheet, sr, sc, er, ec)
            }
            _ => return None,
        };

        let address = RangeAddress::new(sheet_name, start_row, start_col, end_row, end_col).ok()?;

        Some(DefinedName {
            name: name.to_string(),
            scope,
            scope_sheet,
            definition: DefinedNameDefinition::Range { address },
        })
    }

    fn decode_attr<R: BufRead>(
        reader: &XmlReader<R>,
        start: &BytesStart<'_>,
        key: &[u8],
    ) -> Option<String> {
        start
            .attributes()
            .filter_map(Result::ok)
            .find(|attr| attr.key == QName(key))
            .and_then(|attr| {
                attr.decode_and_unescape_value(reader.decoder())
                    .ok()
                    .map(|v| v.into_owned())
            })
    }

    fn append_xml_entity(
        entity: &BytesRef<'_>,
        buffer: &mut String,
    ) -> Result<(), quick_xml::Error> {
        let decoded = entity.decode()?;
        match decoded.as_ref() {
            "lt" => buffer.push('<'),
            "gt" => buffer.push('>'),
            "amp" => buffer.push('&'),
            "apos" => buffer.push('\''),
            "quot" => buffer.push('"'),
            _ => {
                if let Some(ch) = entity.resolve_char_ref()? {
                    buffer.push(ch);
                } else {
                    return Err(quick_xml::Error::Escape(
                        quick_xml::escape::EscapeError::UnrecognizedEntity(
                            0..0,
                            format!("&{decoded};"),
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    fn fallback_defined_names_from_workbook<R>(
        workbook: &Xlsx<R>,
        sheet_names: &[String],
    ) -> Vec<DefinedName>
    where
        R: Read + Seek,
    {
        let mut out = Vec::new();
        let mut seen: HashSet<(DefinedNameScope, Option<String>, String)> = HashSet::new();

        for (name, formula) in workbook.defined_names() {
            if let Some(converted) = Self::convert_defined_name(name, formula, None, sheet_names) {
                let key = (
                    converted.scope.clone(),
                    converted.scope_sheet.clone(),
                    converted.name.clone(),
                );
                if seen.insert(key) {
                    out.push(converted);
                }
            }
        }

        out
    }

    fn scan_defined_names_from_reader<R>(reader: R, sheet_names: &[String]) -> Vec<DefinedName>
    where
        R: Read + Seek,
    {
        let mut archive = match ZipArchive::new(reader) {
            Ok(a) => a,
            Err(_) => return Vec::new(),
        };
        let entry = match archive.by_name("xl/workbook.xml") {
            Ok(e) => e,
            Err(_) => return Vec::new(),
        };

        // Calamine's public defined_names() surface flattens OOXML defined names to
        // (name, formula_text) and drops localSheetId. We recover only the scoped
        // defined-name metadata we need here with a targeted streaming pass over
        // workbook.xml, avoiding a full file String allocation or any sheet XML reparse.
        let mut xml = XmlReader::from_reader(BufReader::new(entry));
        xml.config_mut().trim_text(true);

        let mut out = Vec::new();
        let mut seen: HashSet<(DefinedNameScope, Option<String>, String)> = HashSet::new();
        let mut buf = Vec::new();
        let mut inner_buf = Vec::new();
        let mut in_defined_names = false;

        loop {
            buf.clear();
            match xml.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) if e.local_name().as_ref() == b"definedNames" => {
                    in_defined_names = true;
                }
                Ok(Event::End(ref e)) if e.local_name().as_ref() == b"definedNames" => {
                    break;
                }
                Ok(Event::Start(ref e))
                    if in_defined_names && e.local_name().as_ref() == b"definedName" =>
                {
                    let name = Self::decode_attr(&xml, e, b"name");
                    let local_sheet_id = Self::decode_attr(&xml, e, b"localSheetId")
                        .and_then(|v| v.parse::<usize>().ok());
                    let mut value = String::new();

                    loop {
                        inner_buf.clear();
                        match xml.read_event_into(&mut inner_buf) {
                            Ok(Event::Text(t)) => match t.xml10_content() {
                                Ok(text) => value.push_str(&text),
                                Err(_) => return Vec::new(),
                            },
                            Ok(Event::GeneralRef(entity)) => {
                                if Self::append_xml_entity(&entity, &mut value).is_err() {
                                    return Vec::new();
                                }
                            }
                            Ok(Event::End(end)) if end.name() == e.name() => break,
                            Ok(Event::Eof) => return Vec::new(),
                            Err(_) => return Vec::new(),
                            _ => {}
                        }
                    }

                    if let Some(name) = name
                        && let Some(converted) =
                            Self::convert_defined_name(&name, &value, local_sheet_id, sheet_names)
                    {
                        let key = (
                            converted.scope.clone(),
                            converted.scope_sheet.clone(),
                            converted.name.clone(),
                        );
                        if seen.insert(key) {
                            out.push(converted);
                        }
                    }
                }
                Ok(Event::Eof) => break,
                Err(_) => return Vec::new(),
                _ => {}
            }
        }

        out
    }

    fn scan_external_link_targets_from_reader<R>(reader: R) -> BTreeMap<u32, String>
    where
        R: Read + Seek,
    {
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
            named_ranges: true,
            lazy_loading: false,
            random_access: false,
            styles: false,
            bytes_input: true,
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
        Ok(self.cached_names.clone().unwrap_or_default())
    }

    fn load_stats(&self) -> Option<AdapterLoadStats> {
        Some(self.load_stats.clone())
    }

    fn defined_names(&mut self) -> Result<Vec<DefinedName>, Self::Error> {
        Ok(self.defined_names.clone())
    }

    fn open_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let path = path.as_ref();
        let external_link_targets = match File::open(path) {
            Ok(file) => Self::scan_external_link_targets_from_reader(BufReader::new(file)),
            Err(_) => BTreeMap::new(),
        };
        let workbook: Xlsx<BufReader<File>> = open_workbook(path)?;
        let sheet_names = workbook.sheet_names().to_vec();
        let defined_names = if workbook.defined_names().is_empty() {
            Vec::new()
        } else {
            let parsed = match File::open(path) {
                Ok(file) => {
                    Self::scan_defined_names_from_reader(BufReader::new(file), &sheet_names)
                }
                Err(_) => Vec::new(),
            };
            if parsed.is_empty() {
                Self::fallback_defined_names_from_workbook(&workbook, &sheet_names)
            } else {
                parsed
            }
        };
        Ok(Self {
            workbook: RwLock::new(CalamineWorkbook::File(workbook)),
            loaded_sheets: HashSet::new(),
            cached_names: Some(sheet_names),
            defined_names,
            external_link_targets,
            load_stats: AdapterLoadStats::default(),
        })
    }

    fn open_reader(mut reader: Box<dyn Read + Send + Sync>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let mut data = Vec::new();
        reader.read_to_end(&mut data).map_err(calamine::Error::Io)?;
        Self::open_bytes(data)
    }

    fn open_bytes(data: Vec<u8>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let external_link_targets =
            Self::scan_external_link_targets_from_reader(Cursor::new(data.as_slice()));
        let workbook: Xlsx<Cursor<Vec<u8>>> = open_workbook_from_rs(Cursor::new(data.clone()))?;
        let sheet_names = workbook.sheet_names().to_vec();
        let defined_names = if workbook.defined_names().is_empty() {
            Vec::new()
        } else {
            let parsed =
                Self::scan_defined_names_from_reader(Cursor::new(data.as_slice()), &sheet_names);
            if parsed.is_empty() {
                Self::fallback_defined_names_from_workbook(&workbook, &sheet_names)
            } else {
                parsed
            }
        };

        Ok(Self {
            workbook: RwLock::new(CalamineWorkbook::Bytes(workbook)),
            loaded_sheets: HashSet::new(),
            cached_names: Some(sheet_names),
            defined_names,
            external_link_targets,
            load_stats: AdapterLoadStats::default(),
        })
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
            // Explicit fallback: calamine does not expose row visibility metadata.
            row_hidden_manual: vec![],
            // Explicit fallback: filter-hidden row state is unavailable via calamine.
            row_hidden_filter: vec![],
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
        use formualizer_eval::engine::named_range::{NameScope, NamedDefinition};
        use formualizer_eval::reference::{CellRef, Coord};

        #[cfg(feature = "tracing")]
        let _span_load =
            tracing::info_span!("io_stream_into_engine", backend = "calamine").entered();
        // Simple eager load: iterate sheets, add, bulk insert values, then formulas
        let debug = std::env::var("FZ_DEBUG_LOAD")
            .ok()
            .is_some_and(|v| v != "0");
        let t0 = DebugTimer::start();
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
        let mut total_value_cells_observed = 0usize;
        let mut total_formulas = 0usize;
        let mut total_formula_handed_to_engine = 0usize;
        let mut eager_formula_batches: Vec<FormulaBatch> = Vec::new();
        // Route formula ingest through the engine's bulk ingest builder for optimal edge construction
        // Arrow bulk ingest for base values (Phase A) is built per-sheet without borrowing the engine
        // Default Arrow chunk rows
        let chunk_rows: usize = 32 * 1024;
        for n in &names {
            let t_sheet = DebugTimer::start();
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
                let value_sr0 = r.start().unwrap_or_default().0; // 0-based
                let value_sc0 = r.start().unwrap_or_default().1; // 0-based
                let mut max_rows = r.height() as u32 + value_sr0;
                let mut max_cols = r.width() as u32 + value_sc0;
                if let Some(frm_range) = f.as_ref() {
                    let formula_sr0 = frm_range.start().unwrap_or_default().0;
                    let formula_sc0 = frm_range.start().unwrap_or_default().1;
                    max_rows = max_rows.max(frm_range.height() as u32 + formula_sr0);
                    max_cols = max_cols.max(frm_range.width() as u32 + formula_sc0);
                }
                dims = (max_rows, max_cols);
                range = r;
                formulas_range = f;
            }
            if debug {
                eprintln!("[fz][load]    dims={}x{}", dims.0, dims.1);
            }
            let value_cells_observed = range
                .used_cells()
                .filter(|(_, _, data)| {
                    !matches!(*data, Data::Empty)
                        && !matches!(*data, Data::String(s) if s.is_empty())
                })
                .count();
            total_value_cells_observed += value_cells_observed;
            let populated_cells = value_cells_observed
                + formulas_range
                    .as_ref()
                    .map(|frm_range| frm_range.used_cells().count())
                    .unwrap_or(0);
            enforce_sheet_load_limits(
                "calamine",
                n,
                dims.0,
                dims.1,
                populated_cells,
                engine.workbook_load_limits(),
            )
            .map_err(|err| calamine::Error::Io(std::io::Error::other(err.to_string())))?;
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
            let tv0 = DebugTimer::start();
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
                    tv0.elapsed_millis()
                );
            }

            // Formulas: iterate formulas_range and either stage or parse with caching
            let tf0 = DebugTimer::start();
            let mut parsed_n = 0usize;
            let mut formula_handed_to_engine = 0usize;
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
                        formula_handed_to_engine += 1;
                    }
                } else {
                    let mut cache: rustc_hash::FxHashMap<
                        String,
                        Option<formualizer_eval::engine::AstNodeId>,
                    > = rustc_hash::FxHashMap::default();
                    cache.reserve(4096);
                    let mut formulas: Vec<FormulaIngestRecord> = Vec::new();
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
                        let ast_id = if let Some(cached) = cache.get(&key_owned) {
                            *cached
                        } else {
                            let parsed = match formualizer_parse::parser::parse(&key_owned) {
                                Ok(parsed) => Some(parsed),
                                Err(e) => engine
                                    .handle_formula_parse_error(
                                        n,
                                        excel_row,
                                        excel_col,
                                        &key_owned,
                                        e.to_string(),
                                    )
                                    .map_err(|e| {
                                        calamine::Error::Io(std::io::Error::other(e.to_string()))
                                    })?,
                            };
                            let ast_id = parsed.as_ref().map(|ast| engine.intern_formula_ast(ast));
                            cache.insert(key_owned.clone(), ast_id);
                            ast_id
                        };
                        if let Some(ast_id) = ast_id {
                            formulas.push(FormulaIngestRecord::new(
                                excel_row,
                                excel_col,
                                ast_id,
                                Some(Arc::<str>::from(key_owned.clone())),
                            ));
                        }
                        parsed_n += 1;
                        if debug && parsed_n.is_multiple_of(5000) {
                            eprintln!("[fz][load]    parsed formulas: {parsed_n}");
                        }
                    }
                    formula_handed_to_engine += formulas.len();
                    if !formulas.is_empty() {
                        eager_formula_batches.push(FormulaIngestBatch::new(n.clone(), formulas));
                    }
                }
            }
            total_formulas += parsed_n;
            total_formula_handed_to_engine += formula_handed_to_engine;
            if debug {
                eprintln!(
                    "[fz][load]    formulas={} in {} ms",
                    parsed_n,
                    tf0.elapsed_millis()
                );
                eprintln!(
                    "[fz][load] << sheet '{}' staged in {} ms",
                    n,
                    t_sheet.elapsed_millis()
                );
            }
            // Mark as loaded for API parity
            self.loaded_sheets.insert(n.to_string());

            // Explicit fallback: calamine cannot currently read row visibility metadata,
            // so we intentionally seed no hidden rows.
            let row_hidden_manual: &[u32] = &[];
            let row_hidden_filter: &[u32] = &[];
            for row in row_hidden_manual {
                engine
                    .set_row_hidden(
                        n,
                        *row,
                        true,
                        formualizer_eval::engine::RowVisibilitySource::Manual,
                    )
                    .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
            }
            for row in row_hidden_filter {
                engine
                    .set_row_hidden(
                        n,
                        *row,
                        true,
                        formualizer_eval::engine::RowVisibilitySource::Filter,
                    )
                    .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
            }
        }

        if !engine.config.defer_graph_building && !eager_formula_batches.is_empty() {
            engine
                .ingest_formula_batches(eager_formula_batches)
                .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
        }

        // Register defined names into the dependency graph.
        {
            use rustc_hash::FxHashSet;

            let defined = self.defined_names()?;
            let mut seen: FxHashSet<(DefinedNameScope, Option<String>, String)> =
                FxHashSet::default();

            for dn in defined {
                let key = (dn.scope.clone(), dn.scope_sheet.clone(), dn.name.clone());
                if !seen.insert(key) {
                    continue;
                }

                let scope = match dn.scope {
                    DefinedNameScope::Workbook => NameScope::Workbook,
                    DefinedNameScope::Sheet => {
                        let sheet_name = dn.scope_sheet.as_deref().ok_or_else(|| {
                            calamine::Error::Io(std::io::Error::other(format!(
                                "sheet-scoped defined name `{}` missing scope_sheet",
                                dn.name
                            )))
                        })?;
                        let sid = engine.sheet_id(sheet_name).ok_or_else(|| {
                            calamine::Error::Io(std::io::Error::other(format!(
                                "scope sheet not found: {sheet_name}"
                            )))
                        })?;
                        NameScope::Sheet(sid)
                    }
                };

                let definition = match dn.definition {
                    DefinedNameDefinition::Range { address } => {
                        let sheet_id = engine
                            .sheet_id(&address.sheet)
                            .or_else(|| engine.add_sheet(&address.sheet).ok())
                            .ok_or_else(|| {
                                calamine::Error::Io(std::io::Error::other(format!(
                                    "sheet not found: {}",
                                    address.sheet
                                )))
                            })?;

                        let sr0 = address.start_row.saturating_sub(1);
                        let sc0 = address.start_col.saturating_sub(1);
                        let er0 = address.end_row.saturating_sub(1);
                        let ec0 = address.end_col.saturating_sub(1);

                        let start_ref = CellRef::new(sheet_id, Coord::new(sr0, sc0, true, true));
                        if sr0 == er0 && sc0 == ec0 {
                            NamedDefinition::Cell(start_ref)
                        } else {
                            let end_ref = CellRef::new(sheet_id, Coord::new(er0, ec0, true, true));
                            let range_ref =
                                formualizer_eval::reference::RangeRef::new(start_ref, end_ref);
                            NamedDefinition::Range(range_ref)
                        }
                    }
                    DefinedNameDefinition::Literal { value } => NamedDefinition::Literal(value),
                };

                engine
                    .define_name(&dn.name, definition, scope)
                    .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
            }
        }

        let tend0 = DebugTimer::start();
        // Finish builder and finalize ingestion
        let tcommit0 = DebugTimer::start();
        // (graph ingest finished per-sheet above)
        // Finish Arrow ingest after formulas are staged (stores ArrowSheets into engine)
        // (Arrow sheets are installed per-sheet above)
        if debug {
            eprintln!(
                "[fz][load] commit: builder finish in {} ms",
                tcommit0.elapsed_millis()
            );
            eprintln!(
                "[fz][load] done: values={}, formulas={}, batch_close={} ms, total={} ms",
                total_values,
                total_formulas,
                tend0.elapsed_millis(),
                t0.elapsed_millis(),
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
        self.load_stats = AdapterLoadStats {
            formula_cells_observed: Some(total_formulas as u64),
            value_cells_observed: Some(total_value_cells_observed as u64),
            value_slots_handed_to_engine: Some(total_values as u64),
            formula_cells_handed_to_engine: Some(total_formula_handed_to_engine as u64),
            shared_formula_tags_observed: None,
        };
        if debug {
            eprintln!(
                "[fz][load] done: values={} value_cells={} formulas={} formula_handed={} batch_close={} ms, total={} ms",
                total_values,
                total_value_cells_observed,
                total_formulas,
                total_formula_handed_to_engine,
                tend0.elapsed_millis(),
                t0.elapsed_millis(),
            );
        }
        Ok(())
    }
}
