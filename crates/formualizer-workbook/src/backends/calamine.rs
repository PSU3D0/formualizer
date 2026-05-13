use crate::load_limits::enforce_sheet_dimension_limits;
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
use formualizer_eval::arrow_store::{IngestBuilder, OverlayValue, map_error_code};
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
                Data::DateTime(dt) => Some(LiteralValue::from_serial_number(dt.as_f64())),
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
        let _span_load = tracing::info_span!(
            "io_stream_into_engine",
            backend = "calamine",
            formula_records = false,
        )
        .entered();

        // Publishable Calamine path: crates.io Calamine currently exposes values
        // and formulas as separate ranges. Keep this path compatible with that API
        // while preserving the same sparse/dense engine ingest semantics used by
        // the FormulaPlane-aware loaders. Once Calamine publishes XLSX formula
        // record streaming, this seam can switch back to a single-pass reader.
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

        let prev_index_mode = engine.config.sheet_index_mode;
        engine.set_sheet_index_mode(formualizer_eval::engine::SheetIndexMode::Lazy);
        let prev_range_limit = engine.config.range_expansion_limit;
        engine.config.range_expansion_limit = 0;
        engine.set_first_load_assume_new(true);
        engine.reset_ensure_touched();

        let chunk_rows: usize = 32 * 1024;
        let mut total_values = 0usize;
        let mut total_value_cells_observed = 0usize;
        let mut total_formulas = 0usize;
        let mut total_formula_handed_to_engine = 0usize;
        let mut eager_formula_batches: Vec<FormulaBatch> = Vec::new();

        #[inline]
        fn data_to_literal(value: &Data) -> Option<LiteralValue> {
            match value {
                Data::Empty => None,
                Data::String(s) if s.is_empty() => None,
                Data::String(s) => Some(LiteralValue::Text(s.clone())),
                Data::Float(f) => Some(LiteralValue::Number(*f)),
                Data::Int(i) => Some(LiteralValue::Number(*i as f64)),
                Data::Bool(b) => Some(LiteralValue::Boolean(*b)),
                Data::Error(e) => Some(LiteralValue::Error(ExcelError::new(
                    match CalamineAdapter::calamine_error_code(e) {
                        1 => ExcelErrorKind::Null,
                        2 => ExcelErrorKind::Ref,
                        3 => ExcelErrorKind::Name,
                        4 => ExcelErrorKind::Value,
                        5 => ExcelErrorKind::Div,
                        6 => ExcelErrorKind::Na,
                        7 => ExcelErrorKind::Num,
                        _ => ExcelErrorKind::Error,
                    },
                ))),
                Data::DateTime(dt) => Some(LiteralValue::from_serial_number(dt.as_f64())),
                Data::DateTimeIso(s) => Some(LiteralValue::Text(s.clone())),
                Data::DurationIso(s) => Some(LiteralValue::Text(s.clone())),
            }
        }

        #[inline]
        fn data_to_overlay(value: &Data) -> Option<OverlayValue> {
            match value {
                Data::Empty => None,
                Data::String(s) if s.is_empty() => None,
                Data::String(s) => Some(OverlayValue::Text(Arc::from(s.as_str()))),
                Data::Float(f) => Some(OverlayValue::Number(*f)),
                Data::Int(i) => Some(OverlayValue::Number(*i as f64)),
                Data::Bool(b) => Some(OverlayValue::Boolean(*b)),
                Data::Error(e) => {
                    Some(OverlayValue::Error(CalamineAdapter::calamine_error_code(e)))
                }
                Data::DateTime(dt) => Some(OverlayValue::DateTime(dt.as_f64())),
                Data::DateTimeIso(s) => Some(OverlayValue::Text(Arc::from(s.as_str()))),
                Data::DurationIso(s) => Some(OverlayValue::Text(Arc::from(s.as_str()))),
            }
        }

        struct DenseState {
            aib: IngestBuilder,
            row_vals: Vec<LiteralValue>,
            current_row0: usize,
            rows_appended: usize,
            row_started: bool,
        }

        for n in &names {
            let t_sheet = DebugTimer::start();
            if debug {
                eprintln!("[fz][load] >> sheet '{n}'");
            }
            #[cfg(feature = "tracing")]
            let _span_sheet =
                tracing::info_span!("io_populate_sheet", sheet = n.as_str()).entered();

            let (range, formula_range) = {
                let mut wb_guard = self.workbook.write();
                let range = wb_guard.worksheet_range(n)?;
                let formula_range = wb_guard.worksheet_formula(n).ok();
                (range, formula_range)
            };

            let mut dims_rows = range.end().map(|end| end.0 + 1).unwrap_or(1) as usize;
            let mut abs_cols = range.end().map(|end| end.1 + 1).unwrap_or(1) as usize;
            if let Some(frm_range) = formula_range.as_ref()
                && let Some(end) = frm_range.end()
            {
                dims_rows = dims_rows.max(end.0 as usize + 1);
                abs_cols = abs_cols.max(end.1 as usize + 1);
            }
            dims_rows = dims_rows.max(1);
            abs_cols = abs_cols.max(1);

            let tf0 = DebugTimer::start();
            let mut parsed_n = 0usize;
            let mut formula_handed_to_engine = 0usize;
            let mut parse_cache: rustc_hash::FxHashMap<
                String,
                Option<formualizer_eval::engine::AstNodeId>,
            > = rustc_hash::FxHashMap::default();
            parse_cache.reserve(4096);
            let mut formulas: Vec<FormulaIngestRecord> = Vec::new();
            let mut formula_coords: rustc_hash::FxHashSet<(usize, usize)> =
                rustc_hash::FxHashSet::default();

            if let Some(frm_range) = formula_range.as_ref() {
                let start_row = frm_range.start().unwrap_or_default().0 as usize;
                let start_col = frm_range.start().unwrap_or_default().1 as usize;
                for (row, col, formula) in frm_range.used_cells() {
                    if formula.is_empty() {
                        continue;
                    }
                    let row0 = row + start_row;
                    let col0 = col + start_col;
                    formula_coords.insert((row0, col0));
                    dims_rows = dims_rows.max(row0 + 1);
                    abs_cols = abs_cols.max(col0 + 1);
                    let excel_row = (row0 + 1) as u32;
                    let excel_col = (col0 + 1) as u32;
                    let key_owned: String = if formula.starts_with('=') {
                        formula.clone()
                    } else {
                        format!("={formula}")
                    };
                    if debug && parsed_n < 16 {
                        eprintln!("[fz][load] formula R{excel_row}C{excel_col} = {key_owned:?}");
                    }
                    if engine.config.defer_graph_building {
                        engine.stage_formula_text(n, excel_row, excel_col, key_owned);
                        formula_handed_to_engine += 1;
                    } else {
                        let ast_id = if let Some(cached) = parse_cache.get(&key_owned) {
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
                            parse_cache.insert(key_owned.clone(), ast_id);
                            ast_id
                        };
                        if let Some(ast_id) = ast_id {
                            formulas.push(FormulaIngestRecord::new(
                                excel_row,
                                excel_col,
                                ast_id,
                                Some(Arc::<str>::from(key_owned.clone())),
                            ));
                            formula_handed_to_engine += 1;
                        }
                    }
                    parsed_n += 1;
                    if debug && parsed_n.is_multiple_of(5000) {
                        eprintln!("[fz][load]    parsed formulas: {parsed_n}");
                    }
                }
            }

            if debug {
                eprintln!("[fz][load]    dims={}x{}", dims_rows, abs_cols);
            }
            enforce_sheet_dimension_limits(
                "calamine",
                n,
                dims_rows as u32,
                abs_cols as u32,
                engine.workbook_load_limits(),
            )
            .map_err(|err| calamine::Error::Io(std::io::Error::other(err.to_string())))?;

            let logical_cells = (dims_rows as u64).saturating_mul(abs_cols as u64);
            let force_sparse_from_start =
                logical_cells > engine.workbook_load_limits().max_sheet_logical_cells;

            let tv0 = DebugTimer::start();
            let mut dense = (!force_sparse_from_start).then(|| DenseState {
                aib: IngestBuilder::new(n, abs_cols, chunk_rows, engine.config.date_system),
                row_vals: vec![LiteralValue::Empty; abs_cols],
                current_row0: 0,
                rows_appended: 0,
                row_started: false,
            });
            let mut sparse: Option<formualizer_eval::arrow_store::ArrowSheet> =
                force_sparse_from_start.then(|| {
                    formualizer_eval::arrow_store::ArrowSheet::new_sparse(
                        n, abs_cols, dims_rows, chunk_rows,
                    )
                });
            let mut used_sparse_fallback = force_sparse_from_start;
            let mut max_row_seen = 0usize;
            let mut max_col_seen = 0usize;
            let mut sheet_value_cells_observed = 0usize;

            let start_row = range.start().unwrap_or_default().0 as usize;
            let start_col = range.start().unwrap_or_default().1 as usize;
            for (row, col, val) in range.used_cells() {
                let row0 = row + start_row;
                let col0 = col + start_col;
                if formula_coords.contains(&(row0, col0)) {
                    continue;
                }
                let Some(literal) = data_to_literal(val) else {
                    continue;
                };
                max_row_seen = max_row_seen.max(row0);
                max_col_seen = max_col_seen.max(col0);
                dims_rows = dims_rows.max(row0 + 1);
                abs_cols = abs_cols.max(col0 + 1);

                total_value_cells_observed += 1;
                sheet_value_cells_observed += 1;
                if sheet_value_cells_observed.saturating_add(parsed_n)
                    > engine.workbook_load_limits().max_sheet_logical_cells as usize
                {
                    return Err(calamine::Error::Io(std::io::Error::other(format!(
                        "Workbook load budget exceeded in calamine for sheet {n}: observed populated cell count exceeds configured logical-cell budget of {}",
                        engine.workbook_load_limits().max_sheet_logical_cells
                    ))));
                }

                if let Some(asheet) = sparse.as_mut() {
                    if let Some(value) = data_to_overlay(val) {
                        asheet.set_sparse_overlay_value(row0, col0, value);
                        total_values += 1;
                    }
                    continue;
                }

                let state = dense.as_mut().expect("dense or sparse ingest mode");
                let non_monotonic = state.row_started && row0 < state.current_row0;
                let col_overflow = col0 >= state.row_vals.len();
                let gap_rows = if state.row_started {
                    row0.saturating_sub(state.current_row0)
                } else {
                    row0
                };
                let large_gap = gap_rows > 128;
                let would_exceed_dense_budget =
                    state.rows_appended.saturating_mul(state.row_vals.len())
                        > engine.workbook_load_limits().max_sheet_logical_cells as usize;

                if non_monotonic || col_overflow || large_gap || would_exceed_dense_budget {
                    let mut state = dense.take().expect("dense state present");
                    if state.row_started && state.current_row0 == state.rows_appended {
                        state.aib.append_row(&state.row_vals).map_err(|e| {
                            calamine::Error::Io(std::io::Error::other(e.to_string()))
                        })?;
                        state.rows_appended += 1;
                    }
                    let mut asheet = state.aib.finish();
                    asheet.ensure_row_capacity(dims_rows.max(row0 + 1));
                    if col0 >= asheet.columns.len() {
                        asheet.insert_columns(
                            asheet.columns.len(),
                            (col0 + 1) - asheet.columns.len(),
                        );
                    }
                    if let Some(value) = data_to_overlay(val) {
                        asheet.set_sparse_overlay_value(row0, col0, value);
                        total_values += 1;
                    }
                    sparse = Some(asheet);
                    used_sparse_fallback = true;
                    continue;
                }

                if !state.row_started {
                    while state.rows_appended < row0 {
                        state
                            .aib
                            .append_row(&vec![LiteralValue::Empty; state.row_vals.len()])
                            .map_err(|e| {
                                calamine::Error::Io(std::io::Error::other(e.to_string()))
                            })?;
                        state.rows_appended += 1;
                    }
                    state.current_row0 = row0;
                    state.row_started = true;
                } else if row0 > state.current_row0 {
                    state
                        .aib
                        .append_row(&state.row_vals)
                        .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
                    state.rows_appended += 1;
                    state.row_vals.fill(LiteralValue::Empty);
                    while state.rows_appended < row0 {
                        state
                            .aib
                            .append_row(&vec![LiteralValue::Empty; state.row_vals.len()])
                            .map_err(|e| {
                                calamine::Error::Io(std::io::Error::other(e.to_string()))
                            })?;
                        state.rows_appended += 1;
                    }
                    state.current_row0 = row0;
                }

                state.row_vals[col0] = literal;
                total_values += 1;
            }

            let asheet = if let Some(mut asheet) = sparse {
                asheet.ensure_row_capacity(dims_rows.max(max_row_seen + 1));
                asheet
            } else {
                let mut state = dense.take().expect("dense state present");
                if state.row_started {
                    state
                        .aib
                        .append_row(&state.row_vals)
                        .map_err(|e| calamine::Error::Io(std::io::Error::other(e.to_string())))?;
                }
                let mut asheet = state.aib.finish();
                asheet.ensure_row_capacity(dims_rows.max(max_row_seen + 1));
                asheet
            };

            let store = engine.sheet_store_mut();
            if let Some(pos) = store.sheets.iter().position(|s| s.name.as_ref() == n) {
                store.sheets[pos] = asheet;
            } else {
                store.sheets.push(asheet);
            }

            if !engine.config.defer_graph_building && !formulas.is_empty() {
                eager_formula_batches.push(FormulaIngestBatch::new(n.clone(), formulas));
            }

            total_formulas += parsed_n;
            total_formula_handed_to_engine += formula_handed_to_engine;
            if debug {
                eprintln!(
                    "[fz][load]    rows={} cols={} sparse_fallback={} values={} → arrow in {} ms",
                    dims_rows,
                    max_col_seen + 1,
                    used_sparse_fallback,
                    sheet_value_cells_observed,
                    tv0.elapsed_millis()
                );
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
            self.loaded_sheets.insert(n.to_string());

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

        if debug {
            eprintln!(
                "[fz][load] done: values={}, formulas={}, total={} ms",
                total_values,
                total_formulas,
                t0.elapsed_millis(),
            );
        }
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
        Ok(())
    }
}
