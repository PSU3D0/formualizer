use crate::error::IoError;
use crate::traits::{
    AccessGranularity, BackendCaps, CellData, SaveDestination, SheetData, SpreadsheetReader,
    SpreadsheetWriter,
};
use formualizer_common::LiteralValue;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[derive(Default)]
pub enum CsvEncoding {
    /// CSV v1 supports UTF-8 only.
    #[default]
    Utf8,
}


#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[derive(Default)]
pub enum CsvTrim {
    #[default]
    None,
    All,
}


#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[derive(Default)]
pub enum CsvTypeInference {
    /// Do not infer: treat all non-empty fields as text.
    Off,
    /// Infer booleans + numbers when unambiguous.
    #[default]
    Basic,
    /// Like `Basic`, plus conservative date/date-time parsing.
    BasicWithDates,
}


#[derive(Clone, Debug)]
pub struct CsvReadOptions {
    /// Field delimiter as a single byte. Use `b'\t'` for TSV.
    pub delimiter: u8,
    /// When true, the first record is treated as a header row.
    ///
    /// v1 behavior: headers are still loaded into row 1, but type inference is disabled
    /// for that row to avoid surprising coercions.
    pub has_headers: bool,
    pub trim: CsvTrim,
    pub encoding: CsvEncoding,
    pub type_inference: CsvTypeInference,
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            has_headers: false,
            trim: CsvTrim::None,
            encoding: CsvEncoding::Utf8,
            type_inference: CsvTypeInference::Basic,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[derive(Default)]
pub enum CsvNewline {
    #[default]
    Lf,
    Crlf,
}


#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[derive(Default)]
pub enum CsvQuoteStyle {
    #[default]
    Necessary,
    Always,
    Never,
    NonNumeric,
}


#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[derive(Default)]
pub enum CsvArrayPolicy {
    /// Reject exporting arrays to CSV.
    #[default]
    Error,
    /// Export only the top-left element (`[0][0]`).
    TopLeft,
    /// Export an empty string.
    Blank,
}


#[derive(Clone, Debug)]
pub struct CsvWriteOptions {
    /// Field delimiter as a single byte. Use `b'\t'` for TSV.
    pub delimiter: u8,
    pub newline: CsvNewline,
    pub quote_style: CsvQuoteStyle,

    /// Policy for exporting `LiteralValue::Array` into a single CSV field.
    pub array_policy: CsvArrayPolicy,
}

impl Default for CsvWriteOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            newline: CsvNewline::Lf,
            quote_style: CsvQuoteStyle::Necessary,
            array_policy: CsvArrayPolicy::Error,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CsvSheet {
    /// Only non-empty cells are stored.
    cells: BTreeMap<(u32, u32), LiteralValue>,
    /// Maximum row index seen (1-based).
    max_row: u32,
    /// Maximum column index seen (1-based).
    max_col: u32,
}

impl CsvSheet {
    fn bounds(&self) -> Option<(u32, u32)> {
        if self.max_row == 0 || self.max_col == 0 {
            None
        } else {
            Some((self.max_row, self.max_col))
        }
    }

    fn set_bounds(&mut self, rows: u32, cols: u32) {
        self.max_row = self.max_row.max(rows);
        self.max_col = self.max_col.max(cols);
    }
}

/// CSV backend adapter.
///
/// Semantics:
/// - A CSV file is treated as a single-sheet workbook (default sheet name: `Sheet1`).
/// - UTF-8 only.
/// - Formulas/styles/tables/named ranges are not supported.
pub struct CsvAdapter {
    sheet_name: String,
    sheet: CsvSheet,
    path: Option<PathBuf>,
    read_options: CsvReadOptions,
    write_options: CsvWriteOptions,
    caps: BackendCaps,
}

impl Default for CsvAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl CsvAdapter {
    pub fn new() -> Self {
        Self::new_with_options(CsvReadOptions::default(), CsvWriteOptions::default())
    }

    pub fn new_with_options(read_options: CsvReadOptions, write_options: CsvWriteOptions) -> Self {
        Self {
            sheet_name: "Sheet1".to_string(),
            sheet: CsvSheet::default(),
            path: None,
            read_options,
            write_options,
            caps: BackendCaps {
                read: true,
                write: true,
                streaming: false,
                tables: false,
                named_ranges: false,
                formulas: false,
                styles: false,
                lazy_loading: false,
                random_access: true,
                bytes_input: true,
                date_system_1904: false,
                merged_cells: false,
                rich_text: false,
                hyperlinks: false,
                data_validations: false,
                shared_formulas: false,
            },
        }
    }

    pub fn read_options(&self) -> &CsvReadOptions {
        &self.read_options
    }

    pub fn write_options(&self) -> &CsvWriteOptions {
        &self.write_options
    }

    pub fn set_write_options(&mut self, opts: CsvWriteOptions) {
        self.write_options = opts;
    }

    pub fn open_path_with_options<P: AsRef<Path>>(
        path: P,
        read_options: CsvReadOptions,
    ) -> Result<Self, IoError> {
        let mut adapter = Self::new_with_options(read_options, CsvWriteOptions::default());
        adapter.open_from_path(path.as_ref())?;
        adapter.path = Some(path.as_ref().to_path_buf());
        Ok(adapter)
    }

    pub fn open_reader_with_options(
        reader: Box<dyn Read + Send + Sync>,
        read_options: CsvReadOptions,
    ) -> Result<Self, IoError> {
        let mut adapter = Self::new_with_options(read_options, CsvWriteOptions::default());
        adapter.open_from_reader(reader)?;
        Ok(adapter)
    }

    pub fn open_bytes_with_options(
        bytes: Vec<u8>,
        read_options: CsvReadOptions,
    ) -> Result<Self, IoError> {
        let mut adapter = Self::new_with_options(read_options, CsvWriteOptions::default());
        adapter.open_from_reader(Box::new(std::io::Cursor::new(bytes)))?;
        Ok(adapter)
    }

    fn open_from_path(&mut self, path: &Path) -> Result<(), IoError> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        self.open_from_reader(Box::new(reader))
    }

    fn open_from_reader(&mut self, reader: Box<dyn Read + Send + Sync>) -> Result<(), IoError> {
        if self.read_options.encoding != CsvEncoding::Utf8 {
            return Err(IoError::Unsupported {
                feature: "encoding".to_string(),
                context: "csv: only UTF-8 is supported".to_string(),
            });
        }

        let mut rb = csv::ReaderBuilder::new();
        rb.delimiter(self.read_options.delimiter)
            .has_headers(self.read_options.has_headers)
            // Allow ragged rows; we pad missing cells as empty on export.
            .flexible(true);

        match self.read_options.trim {
            CsvTrim::None => rb.trim(csv::Trim::None),
            CsvTrim::All => rb.trim(csv::Trim::All),
        };

        let mut rdr = rb.from_reader(reader);
        self.sheet = CsvSheet::default();

        let mut row: u32 = 1;

        if self.read_options.has_headers {
            let headers = rdr.headers().map_err(|e| IoError::from_backend("csv", e))?;
            let cols = headers.len() as u32;
            self.sheet.set_bounds(1, cols);
            for (ci, field) in headers.iter().enumerate() {
                let col = (ci as u32) + 1;
                if let Some(v) = infer_field(field, CsvTypeInference::Off, true) {
                    self.sheet.cells.insert((row, col), v);
                }
            }
            row += 1;
        }

        for rec in rdr.records() {
            let rec = rec.map_err(|e| IoError::from_backend("csv", e))?;
            let cols = rec.len() as u32;
            self.sheet.set_bounds(row, cols);

            for (ci, field) in rec.iter().enumerate() {
                let col = (ci as u32) + 1;
                if let Some(v) = infer_field(field, self.read_options.type_inference, false) {
                    self.sheet.cells.insert((row, col), v);
                }
            }
            row += 1;
        }

        Ok(())
    }

    pub fn write_sheet_to<'a>(
        &self,
        sheet: &str,
        dest: SaveDestination<'a>,
        opts: CsvWriteOptions,
    ) -> Result<Option<Vec<u8>>, IoError> {
        let Some((rows, cols)) = self.sheet_bounds(sheet) else {
            return match dest {
                SaveDestination::InPlace => {
                    let Some(path) = self.path.as_ref() else {
                        return Err(IoError::Backend {
                            backend: "csv".to_string(),
                            message: "no known path for in-place save".to_string(),
                        });
                    };
                    let _ = File::create(path)?;
                    Ok(None)
                }
                SaveDestination::Path(path) => {
                    let _ = File::create(path)?;
                    Ok(None)
                }
                SaveDestination::Writer(_writer) => Ok(None),
                SaveDestination::Bytes => Ok(Some(Vec::new())),
            };
        };
        self.write_range_to(sheet, (1, 1), (rows, cols), dest, opts)
    }

    pub fn write_range_to<'a>(
        &self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
        dest: SaveDestination<'a>,
        opts: CsvWriteOptions,
    ) -> Result<Option<Vec<u8>>, IoError> {
        if sheet != self.sheet_name {
            return Err(IoError::Backend {
                backend: "csv".to_string(),
                message: format!("sheet not found: {sheet}"),
            });
        }
        let (sr, sc) = start;
        let (er, ec) = end;
        if sr == 0 || sc == 0 || er == 0 || ec == 0 {
            return Err(IoError::Backend {
                backend: "csv".to_string(),
                message: "range coordinates are 1-based".to_string(),
            });
        }
        if er < sr || ec < sc {
            return Err(IoError::Backend {
                backend: "csv".to_string(),
                message: "invalid range (end before start)".to_string(),
            });
        }

        match dest {
            SaveDestination::InPlace => {
                let Some(path) = self.path.as_ref() else {
                    return Err(IoError::Backend {
                        backend: "csv".to_string(),
                        message: "no known path for in-place save".to_string(),
                    });
                };
                let mut file = File::create(path)?;
                write_rect_csv(&mut file, opts, (sr, sc), (er, ec), |r, c| {
                    self.sheet.cells.get(&(r, c)).cloned()
                })?;
                Ok(None)
            }
            SaveDestination::Path(path) => {
                let mut file = File::create(path)?;
                write_rect_csv(&mut file, opts, (sr, sc), (er, ec), |r, c| {
                    self.sheet.cells.get(&(r, c)).cloned()
                })?;
                Ok(None)
            }
            SaveDestination::Writer(writer) => {
                write_rect_csv(writer, opts, (sr, sc), (er, ec), |r, c| {
                    self.sheet.cells.get(&(r, c)).cloned()
                })?;
                Ok(None)
            }
            SaveDestination::Bytes => {
                let mut buf: Vec<u8> = Vec::new();
                write_rect_csv(&mut buf, opts, (sr, sc), (er, ec), |r, c| {
                    self.sheet.cells.get(&(r, c)).cloned()
                })?;
                Ok(Some(buf))
            }
        }
    }
}

impl SpreadsheetReader for CsvAdapter {
    type Error = IoError;

    fn access_granularity(&self) -> AccessGranularity {
        AccessGranularity::Workbook
    }

    fn capabilities(&self) -> BackendCaps {
        self.caps.clone()
    }

    fn sheet_names(&self) -> Result<Vec<String>, Self::Error> {
        Ok(vec![self.sheet_name.clone()])
    }

    fn open_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Self::open_path_with_options(path, CsvReadOptions::default())
    }

    fn open_reader(reader: Box<dyn Read + Send + Sync>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Self::open_reader_with_options(reader, CsvReadOptions::default())
    }

    fn open_bytes(data: Vec<u8>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Self::open_bytes_with_options(data, CsvReadOptions::default())
    }

    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, Self::Error> {
        if sheet != self.sheet_name {
            return Ok(BTreeMap::new());
        }
        let mut out: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
        let (sr, sc) = start;
        let (er, ec) = end;
        for ((r, c), v) in self.sheet.cells.iter() {
            if *r >= sr && *r <= er && *c >= sc && *c <= ec {
                out.insert(
                    (*r, *c),
                    CellData {
                        value: Some(v.clone()),
                        formula: None,
                        style: None,
                    },
                );
            }
        }
        Ok(out)
    }

    fn read_sheet(&mut self, sheet: &str) -> Result<SheetData, Self::Error> {
        if sheet != self.sheet_name {
            return Ok(SheetData {
                cells: BTreeMap::new(),
                dimensions: None,
                tables: vec![],
                named_ranges: vec![],
                date_system_1904: false,
                merged_cells: vec![],
                hidden: false,
            });
        }

        let mut cells: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
        for (k, v) in self.sheet.cells.iter() {
            cells.insert(
                *k,
                CellData {
                    value: Some(v.clone()),
                    formula: None,
                    style: None,
                },
            );
        }

        Ok(SheetData {
            cells,
            dimensions: self.sheet.bounds(),
            tables: vec![],
            named_ranges: vec![],
            date_system_1904: false,
            merged_cells: vec![],
            hidden: false,
        })
    }

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        if sheet == self.sheet_name {
            self.sheet.bounds()
        } else {
            None
        }
    }

    fn is_loaded(&self, _sheet: &str, _row: Option<u32>, _col: Option<u32>) -> bool {
        true
    }
}

impl SpreadsheetWriter for CsvAdapter {
    type Error = IoError;

    fn write_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        data: CellData,
    ) -> Result<(), Self::Error> {
        if sheet != self.sheet_name {
            return Err(IoError::Backend {
                backend: "csv".to_string(),
                message: format!("sheet not found: {sheet}"),
            });
        }
        if data.formula.is_some() {
            return Err(IoError::Unsupported {
                feature: "formulas".to_string(),
                context: "csv".to_string(),
            });
        }
        if data.style.is_some() {
            return Err(IoError::Unsupported {
                feature: "styles".to_string(),
                context: "csv".to_string(),
            });
        }

        self.sheet.set_bounds(row, col);
        match data.value {
            None => {
                self.sheet.cells.remove(&(row, col));
            }
            Some(LiteralValue::Empty) => {
                self.sheet.cells.remove(&(row, col));
            }
            Some(v) => {
                self.sheet.cells.insert((row, col), v);
            }
        }
        Ok(())
    }

    fn write_range(
        &mut self,
        sheet: &str,
        cells: BTreeMap<(u32, u32), CellData>,
    ) -> Result<(), Self::Error> {
        for ((r, c), d) in cells {
            self.write_cell(sheet, r, c, d)?;
        }
        Ok(())
    }

    fn clear_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<(), Self::Error> {
        if sheet != self.sheet_name {
            return Ok(());
        }
        let (sr, sc) = start;
        let (er, ec) = end;
        let keys: Vec<(u32, u32)> = self
            .sheet
            .cells
            .keys()
            .copied()
            .filter(|(r, c)| *r >= sr && *r <= er && *c >= sc && *c <= ec)
            .collect();
        for k in keys {
            self.sheet.cells.remove(&k);
        }
        Ok(())
    }

    fn create_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        if name == self.sheet_name {
            return Ok(());
        }
        Err(IoError::Unsupported {
            feature: "multiple sheets".to_string(),
            context: "csv".to_string(),
        })
    }

    fn delete_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        if name == self.sheet_name {
            self.sheet = CsvSheet::default();
        }
        Ok(())
    }

    fn rename_sheet(&mut self, old: &str, new: &str) -> Result<(), Self::Error> {
        if old == self.sheet_name {
            self.sheet_name = new.to_string();
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn save_to<'a>(&mut self, dest: SaveDestination<'a>) -> Result<Option<Vec<u8>>, Self::Error> {
        let sheet = self.sheet_name.clone();
        let opts = self.write_options.clone();
        self.write_sheet_to(&sheet, dest, opts)
    }
}

// Stream CSV contents into the evaluation engine (values only).
impl<R> formualizer_eval::engine::ingest::EngineLoadStream<R> for CsvAdapter
where
    R: formualizer_eval::traits::EvaluationContext,
{
    type Error = IoError;

    fn stream_into_engine(
        &mut self,
        engine: &mut formualizer_eval::engine::Engine<R>,
    ) -> Result<(), Self::Error> {
        // CSV is values only; keep engine date system default.
        let sheet_name = self.sheet_name.clone();
        engine
            .add_sheet(&sheet_name)
            .map_err(|e| IoError::from_backend("csv", e))?;

        let Some((rows_u32, cols_u32)) = self.sheet.bounds() else {
            return Ok(());
        };
        let rows = rows_u32 as usize;
        let cols = cols_u32 as usize;

        let chunk_rows: usize = 32 * 1024;
        let mut aib = formualizer_eval::arrow_store::IngestBuilder::new(
            &sheet_name,
            cols,
            chunk_rows,
            engine.config.date_system,
        );

        for r0 in 0..rows {
            let r = (r0 as u32) + 1;
            let mut row_vals: Vec<LiteralValue> = vec![LiteralValue::Empty; cols];
            for c0 in 0..cols {
                let c = (c0 as u32) + 1;
                if let Some(v) = self.sheet.cells.get(&(r, c)) {
                    row_vals[c0] = v.clone();
                }
            }
            aib.append_row(&row_vals)
                .map_err(|e| IoError::from_backend("csv", e))?;
        }

        let asheet = aib.finish();
        let store = engine.sheet_store_mut();
        if let Some(pos) = store
            .sheets
            .iter()
            .position(|s| s.name.as_ref() == sheet_name)
        {
            store.sheets[pos] = asheet;
        } else {
            store.sheets.push(asheet);
        }
        engine.finalize_sheet_index(&sheet_name);
        engine.set_first_load_assume_new(false);
        engine.reset_ensure_touched();
        Ok(())
    }
}

/// Export a workbook sheet as CSV.
///
/// Notes:
/// - Uses the workbook's current stored values (after evaluation/overlays).
/// - The exported rectangle is `1..=rows` x `1..=cols` based on `Workbook::sheet_dimensions`.
pub fn write_workbook_sheet_to_path(
    wb: &crate::Workbook,
    sheet: &str,
    path: impl AsRef<Path>,
    opts: CsvWriteOptions,
) -> Result<(), IoError> {
    let Some((rows, cols)) = wb.sheet_dimensions(sheet) else {
        let _ = File::create(path.as_ref())?;
        return Ok(());
    };
    let addr = crate::RangeAddress::new(sheet.to_string(), 1, 1, rows, cols).map_err(|e| {
        IoError::Backend {
            backend: "csv".to_string(),
            message: e.to_string(),
        }
    })?;
    write_workbook_range_to_path(wb, &addr, path, opts)
}

/// Export a workbook range as CSV.
pub fn write_workbook_range_to_path(
    wb: &crate::Workbook,
    addr: &crate::RangeAddress,
    path: impl AsRef<Path>,
    opts: CsvWriteOptions,
) -> Result<(), IoError> {
    let values = wb.read_range(addr);
    let mut file = File::create(path.as_ref())?;
    write_values_csv(&mut file, opts, &values)
}

/// Export a workbook range as CSV bytes.
pub fn write_workbook_range_to_bytes(
    wb: &crate::Workbook,
    addr: &crate::RangeAddress,
    opts: CsvWriteOptions,
) -> Result<Vec<u8>, IoError> {
    let values = wb.read_range(addr);
    let mut buf: Vec<u8> = Vec::new();
    write_values_csv(&mut buf, opts, &values)?;
    Ok(buf)
}

fn infer_field(field: &str, mode: CsvTypeInference, force_text: bool) -> Option<LiteralValue> {
    if field.is_empty() {
        return None;
    }
    if force_text || mode == CsvTypeInference::Off {
        return Some(LiteralValue::Text(field.to_string()));
    }

    if let Some(b) = parse_bool(field) {
        return Some(LiteralValue::Boolean(b));
    }
    if let Some(i) = parse_unambiguous_i64(field) {
        return Some(LiteralValue::Int(i));
    }
    if let Some(n) = parse_unambiguous_f64(field) {
        return Some(LiteralValue::Number(n));
    }
    if mode == CsvTypeInference::BasicWithDates {
        if let Some(d) = parse_date(field) {
            return Some(LiteralValue::Date(d));
        }
        if let Some(dt) = parse_datetime(field) {
            return Some(LiteralValue::DateTime(dt));
        }
    }
    Some(LiteralValue::Text(field.to_string()))
}

fn parse_bool(s: &str) -> Option<bool> {
    if s.eq_ignore_ascii_case("true") {
        Some(true)
    } else if s.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

fn parse_unambiguous_i64(s: &str) -> Option<i64> {
    // Conservative: reject leading zeros (except exactly "0" or "-0").
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let (sign, digits) = match bytes[0] {
        b'+' => (1i64, &s[1..]),
        b'-' => (-1i64, &s[1..]),
        _ => (1i64, s),
    };
    if digits.is_empty() {
        return None;
    }
    if digits.len() > 1 && digits.starts_with('0') {
        return None;
    }
    if !digits.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let parsed: i64 = digits.parse().ok()?;
    Some(sign * parsed)
}

fn parse_unambiguous_f64(s: &str) -> Option<f64> {
    // Only consider float if it actually looks like one (contains '.' or exponent).
    if !(s.contains('.') || s.contains('e') || s.contains('E')) {
        return None;
    }
    // Reject leading zeros like "01.2" (conservative).
    let s2 = s.strip_prefix('+').unwrap_or(s);
    let s2 = s2.strip_prefix('-').unwrap_or(s2);
    if s2.len() > 1 && s2.starts_with('0') && !s2.starts_with("0.") {
        return None;
    }
    let n: f64 = s.parse().ok()?;
    if !n.is_finite() {
        return None;
    }
    Some(n)
}

fn parse_date(s: &str) -> Option<chrono::NaiveDate> {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

fn parse_datetime(s: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .ok()
}

fn csv_terminator(nl: CsvNewline) -> csv::Terminator {
    match nl {
        CsvNewline::Lf => csv::Terminator::Any(b'\n'),
        CsvNewline::Crlf => csv::Terminator::CRLF,
    }
}

fn csv_quote_style(q: CsvQuoteStyle) -> csv::QuoteStyle {
    match q {
        CsvQuoteStyle::Necessary => csv::QuoteStyle::Necessary,
        CsvQuoteStyle::Always => csv::QuoteStyle::Always,
        CsvQuoteStyle::Never => csv::QuoteStyle::Never,
        CsvQuoteStyle::NonNumeric => csv::QuoteStyle::NonNumeric,
    }
}

fn write_rect_csv<W: Write + ?Sized>(
    writer: &mut W,
    opts: CsvWriteOptions,
    start: (u32, u32),
    end: (u32, u32),
    mut get: impl FnMut(u32, u32) -> Option<LiteralValue>,
) -> Result<(), IoError> {
    let mut wb = csv::WriterBuilder::new();
    wb.delimiter(opts.delimiter)
        .terminator(csv_terminator(opts.newline))
        .quote_style(csv_quote_style(opts.quote_style));
    let mut wtr = wb.from_writer(writer);

    let (sr, sc) = start;
    let (er, ec) = end;
    for r in sr..=er {
        let mut record: Vec<String> = Vec::with_capacity((ec - sc + 1) as usize);
        for c in sc..=ec {
            let s = match get(r, c) {
                Some(v) => literal_to_csv_field(&v, &opts)?,
                None => String::new(),
            };
            record.push(s);
        }
        wtr.write_record(record)
            .map_err(|e| IoError::from_backend("csv", e))?;
    }
    wtr.flush().map_err(|e| IoError::from_backend("csv", e))?;
    Ok(())
}

fn write_values_csv<W: Write>(
    writer: &mut W,
    opts: CsvWriteOptions,
    values: &[Vec<LiteralValue>],
) -> Result<(), IoError> {
    let mut wb = csv::WriterBuilder::new();
    wb.delimiter(opts.delimiter)
        .terminator(csv_terminator(opts.newline))
        .quote_style(csv_quote_style(opts.quote_style));
    let mut wtr = wb.from_writer(writer);
    for row in values {
        let record: Vec<String> = row
            .iter()
            .map(|v| literal_to_csv_field(v, &opts))
            .collect::<Result<Vec<_>, IoError>>()?;
        wtr.write_record(record)
            .map_err(|e| IoError::from_backend("csv", e))?;
    }
    wtr.flush().map_err(|e| IoError::from_backend("csv", e))?;
    Ok(())
}

fn literal_to_csv_field(v: &LiteralValue, opts: &CsvWriteOptions) -> Result<String, IoError> {
    literal_to_csv_field_inner(v, opts, 0)
}

fn literal_to_csv_field_inner(
    v: &LiteralValue,
    opts: &CsvWriteOptions,
    depth: u8,
) -> Result<String, IoError> {
    if depth > 4 {
        return Err(IoError::Backend {
            backend: "csv".to_string(),
            message: "Array nesting too deep for CSV export".to_string(),
        });
    }

    Ok(match v {
        LiteralValue::Empty => String::new(),
        LiteralValue::Text(s) => s.clone(),
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Number(n) => n.to_string(),
        LiteralValue::Boolean(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        LiteralValue::Date(d) => d.format("%Y-%m-%d").to_string(),
        LiteralValue::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        LiteralValue::Time(t) => t.format("%H:%M:%S").to_string(),
        LiteralValue::Duration(d) => d.num_seconds().to_string(),
        LiteralValue::Error(e) => e.kind.to_string(),
        LiteralValue::Pending => "Pending".to_string(),
        LiteralValue::Array(a) => match opts.array_policy {
            CsvArrayPolicy::Error => {
                return Err(IoError::Backend {
                    backend: "csv".to_string(),
                    message: "Cannot export array value to CSV (array_policy=Error)".to_string(),
                });
            }
            CsvArrayPolicy::Blank => String::new(),
            CsvArrayPolicy::TopLeft => {
                if let Some(row0) = a.first()
                    && let Some(v0) = row0.first() {
                        return literal_to_csv_field_inner(v0, opts, depth + 1);
                    }
                String::new()
            }
        },
    })
}
