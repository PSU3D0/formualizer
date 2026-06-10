use formualizer_common::{LiteralValue, RangeAddress};
#[cfg(feature = "json")]
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Read, Write};
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

impl IntoLiteral for &str {
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
    pub row_hidden_manual: Vec<u32>,
    pub row_hidden_filter: Vec<u32>,
}

#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "json", serde(rename_all = "lowercase"))]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum NamedRangeScope {
    #[default]
    Workbook,
    Sheet,
}

#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct NamedRange {
    pub name: String,
    #[cfg_attr(feature = "json", serde(default))]
    pub scope: NamedRangeScope,
    pub address: RangeAddress,
}

/// Stable representation of workbook/sheet scoped defined names.
///
/// Stage 1 supports only range-backed and literal-backed names.
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "json", serde(rename_all = "lowercase"))]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum DefinedNameScope {
    #[default]
    Workbook,
    Sheet,
}

#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "json", serde(tag = "type", rename_all = "lowercase"))]
#[derive(Clone, Debug, PartialEq)]
pub enum DefinedNameDefinition {
    Range { address: RangeAddress },
    Literal { value: LiteralValue },
}

#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, PartialEq)]
pub struct DefinedName {
    pub name: String,

    #[cfg_attr(feature = "json", serde(default))]
    pub scope: DefinedNameScope,

    /// Sheet name for sheet-scoped names.
    ///
    /// For workbook-scoped names, this must be None.
    #[cfg_attr(
        feature = "json",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub scope_sheet: Option<String>,

    pub definition: DefinedNameDefinition,
}

#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct TableDefinition {
    pub name: String,
    pub range: (u32, u32, u32, u32),
    /// Whether the first row of `range` is a headers row.
    ///
    /// Deterministic resize rule:
    /// - Tables are metadata-only; writing values just below/next to a table does NOT auto-expand
    ///   the table. Callers must explicitly update table metadata (range/flags) if they want a
    ///   resize.
    #[cfg_attr(feature = "json", serde(default = "default_true"))]
    pub header_row: bool,
    pub headers: Vec<String>,
    pub totals_row: bool,
}

#[cfg(feature = "json")]
fn default_true() -> bool {
    true
}

#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdapterLoadStats {
    pub formula_cells_observed: Option<u64>,
    pub value_cells_observed: Option<u64>,
    pub value_slots_handed_to_engine: Option<u64>,
    pub formula_cells_handed_to_engine: Option<u64>,
    pub shared_formula_tags_observed: Option<u64>,
}

/// Workbook-level calculation properties parsed from `xl/workbook.xml`'s
/// `<calcPr .../>` element (spec §9, RFC #113).
///
/// This mirrors the OOXML attributes verbatim — it is a *transport* struct
/// (parsed values, not yet mapped to engine semantics). The mapping to
/// [`formualizer_eval::engine::CycleConfig`] is applied during
/// [`crate::Workbook::from_reader`] (see
/// `CalcSettings::apply_to_cycle_config`), keeping the backend free of engine
/// dependencies.
///
/// `calc_mode` / `full_calc_on_load` are captured for round-trip fidelity only
/// and are not interpreted semantically (spec §9: "out of scope semantically").
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CalcSettings {
    /// `iterate` attribute: `true` when iterative calculation is enabled
    /// (`iterate="1"` or `iterate="true"`).
    pub iterate: bool,
    /// `iterateCount` attribute (Excel default 100 when iterate is on but the
    /// attribute is absent).
    pub iterate_count: Option<u32>,
    /// `iterateDelta` attribute (Excel default 0.001 when iterate is on but the
    /// attribute is absent).
    pub iterate_delta: Option<f64>,
    /// `calcMode` attribute (e.g. "auto", "manual"). Preserved for round-trip
    /// only.
    pub calc_mode: Option<String>,
    /// `fullCalcOnLoad` attribute. Preserved for round-trip only.
    pub full_calc_on_load: Option<bool>,
}

pub trait SpreadsheetReader: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn access_granularity(&self) -> AccessGranularity;
    fn capabilities(&self) -> BackendCaps;
    fn sheet_names(&self) -> Result<Vec<String>, Self::Error>;

    fn load_stats(&self) -> Option<AdapterLoadStats> {
        None
    }

    /// Workbook-level defined names (workbook scoped or sheet scoped).
    ///
    /// Default: no defined names.
    fn defined_names(&mut self) -> Result<Vec<DefinedName>, Self::Error> {
        Ok(Vec::new())
    }

    /// Workbook-level calculation properties (`<calcPr>`), spec §9.
    ///
    /// `None` means the backend does not surface calc settings (no `<calcPr>`
    /// or no support); callers must leave the engine cycle config untouched in
    /// that case. Only the XLSX backends populate this today.
    fn calc_settings(&self) -> Option<CalcSettings> {
        None
    }

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
    fn save(&mut self) -> Result<(), Self::Error> {
        self.save_to(SaveDestination::InPlace).map(|_| ())
    }

    /// Advanced save: specify destination (in place, path, writer, or bytes in memory).
    /// Returns Ok(Some(bytes)) only for Bytes destination, else Ok(None).
    fn save_to<'a>(&mut self, dest: SaveDestination<'a>) -> Result<Option<Vec<u8>>, Self::Error> {
        let _ = dest;
        unreachable!("save_to must be implemented by writer backends that expose persistence");
    }

    fn save_as_path<P: AsRef<std::path::Path>>(&mut self, path: P) -> Result<(), Self::Error> {
        self.save_to(SaveDestination::Path(path.as_ref()))
            .map(|_| ())
    }

    fn save_to_bytes(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.save_to(SaveDestination::Bytes)
            .map(|opt| opt.unwrap_or_default())
    }

    fn write_to<W: Write>(&mut self, writer: &mut W) -> Result<(), Self::Error> {
        self.save_to(SaveDestination::Writer(writer)).map(|_| ())
    }
}

/// Enum describing where a workbook should be saved.
pub enum SaveDestination<'a> {
    InPlace,                   // Use original path, if known
    Path(&'a std::path::Path), // Write to provided filesystem path
    Writer(&'a mut dyn Write), // Stream to arbitrary writer
    Bytes,                     // Return bytes in memory
}

pub trait SpreadsheetIO: SpreadsheetReader + SpreadsheetWriter {}
