use formualizer_common::LiteralValue;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;

#[derive(Clone, Debug)]
pub struct CellData {
    pub value: Option<LiteralValue>,
    pub formula: Option<String>,
    pub style: Option<StyleId>,
}

impl CellData {
    pub fn from_value<V: IntoLiteral>(value: V) -> Self {
        Self {
            value: Some(value.into_literal()),
            formula: None,
            style: None,
        }
    }

    pub fn from_formula(formula: impl Into<String>) -> Self {
        Self {
            value: None,
            formula: Some(formula.into()),
            style: None,
        }
    }
}

/// Local conversion trait so tests and callers can pass primitives directly
pub trait IntoLiteral {
    fn into_literal(self) -> LiteralValue;
}

impl IntoLiteral for LiteralValue {
    fn into_literal(self) -> LiteralValue {
        self
    }
}

impl IntoLiteral for f64 {
    fn into_literal(self) -> LiteralValue {
        LiteralValue::Number(self)
    }
}

impl IntoLiteral for i64 {
    fn into_literal(self) -> LiteralValue {
        LiteralValue::Int(self)
    }
}

impl IntoLiteral for i32 {
    fn into_literal(self) -> LiteralValue {
        LiteralValue::Int(self as i64)
    }
}

impl IntoLiteral for bool {
    fn into_literal(self) -> LiteralValue {
        LiteralValue::Boolean(self)
    }
}

impl IntoLiteral for String {
    fn into_literal(self) -> LiteralValue {
        LiteralValue::Text(self)
    }
}

impl<'a> IntoLiteral for &'a str {
    fn into_literal(self) -> LiteralValue {
        LiteralValue::Text(self.to_string())
    }
}

pub type StyleId = u32;

#[derive(Clone, Debug, Default)]
pub struct BackendCaps {
    pub read: bool,
    pub write: bool,
    pub streaming: bool,
    pub tables: bool,
    pub named_ranges: bool,
    pub formulas: bool,
    pub styles: bool,
    pub lazy_loading: bool,
    pub random_access: bool,
    pub bytes_input: bool,

    // Excel-specific nuances
    pub date_system_1904: bool,
    pub merged_cells: bool,
    pub rich_text: bool,
    pub hyperlinks: bool,
    pub data_validations: bool,
    pub shared_formulas: bool,
}

#[derive(Clone, Debug)]
pub struct SheetData {
    pub cells: BTreeMap<(u32, u32), CellData>,
    pub dimensions: Option<(u32, u32)>,
    pub tables: Vec<TableDefinition>,
    pub named_ranges: Vec<NamedRange>,
    pub date_system_1904: bool,
    pub merged_cells: Vec<MergedRange>,
    pub hidden: bool,
}

#[derive(Clone, Debug)]
pub struct NamedRange {
    pub name: String,
    pub sheet: Option<String>,
    pub range: (u32, u32, u32, u32), // (start_row, start_col, end_row, end_col)
}

#[derive(Clone, Debug)]
pub struct TableDefinition {
    pub name: String,
    pub range: (u32, u32, u32, u32),
    pub headers: Vec<String>,
    pub totals_row: bool,
}

#[derive(Clone, Debug)]
pub struct MergedRange {
    pub start_row: u32,
    pub start_col: u32,
    pub end_row: u32,
    pub end_col: u32,
}

impl MergedRange {
    pub fn contains(&self, row: u32, col: u32) -> bool {
        row >= self.start_row && row <= self.end_row && col >= self.start_col && col <= self.end_col
    }
}

#[derive(Clone, Copy, Debug)]
pub enum AccessGranularity {
    Cell,     // Random cell access (mmap)
    Range,    // Range-based access (columnar)
    Sheet,    // Sheet-at-a-time (umya, Calamine)
    Workbook, // All-or-nothing (JSON)
}

#[derive(Clone, Debug)]
pub enum LoadStrategy {
    /// Load entire workbook immediately (small files, testing)
    EagerAll,

    /// Load sheet when first accessed (Calamine, umya default)
    EagerSheet,

    /// Load row/column chunks on access (columnar formats)
    LazyRange { row_chunk: usize, col_chunk: usize },

    /// Load individual cells on access (mmap, remote APIs)
    LazyCell,

    /// Never load - write-only mode
    WriteOnly,
}

pub trait SpreadsheetReader: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn access_granularity(&self) -> AccessGranularity;
    fn capabilities(&self) -> BackendCaps;
    fn sheet_names(&self) -> Result<Vec<String>, Self::Error>;

    /// Constructor variants for different environments
    fn open_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn open_reader(reader: Box<dyn Read + Send + Sync>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn open_bytes(data: Vec<u8>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn read_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> Result<Option<CellData>, Self::Error> {
        // Default: fallback to range read
        let mut range = self.read_range(sheet, (row, col), (row, col))?;
        Ok(range.remove(&(row, col)))
    }

    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, Self::Error>;

    fn read_sheet(&mut self, sheet: &str) -> Result<SheetData, Self::Error>;

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)>;
    fn is_loaded(&self, sheet: &str, row: Option<u32>, col: Option<u32>) -> bool;
}

pub trait SpreadsheetWriter: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn write_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        data: CellData,
    ) -> Result<(), Self::Error>;

    fn write_range(
        &mut self,
        sheet: &str,
        cells: BTreeMap<(u32, u32), CellData>,
    ) -> Result<(), Self::Error>;

    fn clear_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<(), Self::Error>;

    fn create_sheet(&mut self, name: &str) -> Result<(), Self::Error>;
    fn delete_sheet(&mut self, name: &str) -> Result<(), Self::Error>;
    fn rename_sheet(&mut self, old: &str, new: &str) -> Result<(), Self::Error>;

    fn flush(&mut self) -> Result<(), Self::Error>;
    fn save(&mut self) -> Result<(), Self::Error>;
}

pub trait SpreadsheetIO: SpreadsheetReader + SpreadsheetWriter {}
