use crate::traits::{
    AccessGranularity, BackendCaps, CellData, MergedRange, NamedRange, SaveDestination, SheetData,
    SpreadsheetReader, SpreadsheetWriter, TableDefinition,
};
use crate::IoError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct JsonWorkbook {
    #[serde(default = "default_version")]
    version: u32,
    #[serde(default)]
    compression: Option<CompressionType>,
    #[serde(default)]
    sheets: BTreeMap<String, JsonSheet>,
}

fn default_version() -> u32 {
    1
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum CompressionType {
    None,
    Lz4,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::None
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
struct JsonSheet {
    #[serde(default)]
    cells: Vec<JsonCell>,
    #[serde(default)]
    dimensions: Option<(u32, u32)>,
    #[serde(default)]
    hidden: bool,
    #[serde(default)]
    date_system_1904: bool,
    #[serde(default)]
    merged_cells: Vec<MergedRange>,
    #[serde(default)]
    tables: Vec<TableDefinition>,
    #[serde(default)]
    named_ranges: Vec<NamedRange>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct JsonCell {
    row: u32,
    col: u32,
    #[serde(default)]
    value: Option<JsonValue>,
    #[serde(default)]
    formula: Option<String>,
    #[serde(default)]
    style: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "value")]
enum JsonValue {
    Int(i64),
    Number(f64),
    Text(String),
    Boolean(bool),
    Empty,
    Date(String),
    DateTime(String),
    Time(String),
    Duration(i64),
    Array(Vec<Vec<JsonValue>>),
    Error(String),
    Pending,
}

pub struct JsonAdapter {
    data: JsonWorkbook,
    path: Option<PathBuf>,
    caps: BackendCaps,
}

impl Default for JsonAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonAdapter {
    pub fn new() -> Self {
        Self {
            data: JsonWorkbook::default(),
            path: None,
            caps: BackendCaps {
                read: true,
                write: true,
                streaming: false,
                tables: true,
                named_ranges: true,
                formulas: true,
                styles: true,
                lazy_loading: false,
                random_access: true,
                bytes_input: true,
                date_system_1904: false,
                merged_cells: true,
                rich_text: false,
                hyperlinks: false,
                data_validations: false,
                shared_formulas: false,
            },
        }
    }

    fn to_sheet_data(js: &JsonSheet) -> SheetData {
        let mut cells: BTreeMap<(u32, u32), CellData> = BTreeMap::new();
        for c in &js.cells {
            let lit = c.value.as_ref().map(json_to_literal);
            cells.insert(
                (c.row, c.col),
                CellData {
                    value: lit,
                    formula: c.formula.clone(),
                    style: c.style,
                },
            );
        }
        SheetData {
            cells,
            dimensions: js.dimensions,
            tables: js.tables.clone(),
            named_ranges: js.named_ranges.clone(),
            date_system_1904: js.date_system_1904,
            merged_cells: js.merged_cells.clone(),
            hidden: js.hidden,
        }
    }

    fn from_sheet_data(sd: &SheetData) -> JsonSheet {
        let mut cells: Vec<JsonCell> = Vec::with_capacity(sd.cells.len());
        for (&(r, c), d) in &sd.cells {
            cells.push(JsonCell {
                row: r,
                col: c,
                value: d.value.as_ref().map(literal_to_json),
                formula: d.formula.clone(),
                style: d.style,
            });
        }
        JsonSheet {
            cells,
            dimensions: sd.dimensions,
            hidden: sd.hidden,
            date_system_1904: sd.date_system_1904,
            merged_cells: sd.merged_cells.clone(),
            tables: sd.tables.clone(),
            named_ranges: sd.named_ranges.clone(),
        }
    }

    pub fn to_json_string(&self) -> Result<String, IoError> {
        Ok(serde_json::to_string_pretty(&self.data)?)
    }

    // Backend-specific helpers (not part of SpreadsheetWriter)
    fn ensure_sheet_mut(&mut self, name: &str) -> &mut JsonSheet {
        self.data
            .sheets
            .entry(name.to_string())
            .or_insert_with(JsonSheet::default)
    }

    pub fn set_dimensions(&mut self, sheet: &str, dims: Option<(u32, u32)>) {
        self.ensure_sheet_mut(sheet).dimensions = dims;
    }

    pub fn set_date_system_1904(&mut self, sheet: &str, value: bool) {
        self.ensure_sheet_mut(sheet).date_system_1904 = value;
    }

    pub fn set_merged_cells(&mut self, sheet: &str, merged: Vec<MergedRange>) {
        self.ensure_sheet_mut(sheet).merged_cells = merged;
    }

    pub fn set_tables(&mut self, sheet: &str, tables: Vec<TableDefinition>) {
        self.ensure_sheet_mut(sheet).tables = tables;
    }

    pub fn set_named_ranges(&mut self, sheet: &str, named: Vec<NamedRange>) {
        self.ensure_sheet_mut(sheet).named_ranges = named;
    }
}

impl SpreadsheetReader for JsonAdapter {
    type Error = IoError;

    fn access_granularity(&self) -> AccessGranularity {
        AccessGranularity::Workbook
    }

    fn capabilities(&self) -> BackendCaps {
        self.caps.clone()
    }

    fn sheet_names(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.data.sheets.keys().cloned().collect())
    }

    fn open_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let file = File::open(path.as_ref())?;
        let reader = BufReader::new(file);
        let data: JsonWorkbook = serde_json::from_reader(reader)?;
        Ok(JsonAdapter {
            data,
            path: Some(path.as_ref().to_path_buf()),
            ..JsonAdapter::new()
        })
    }

    fn open_reader(reader: Box<dyn Read + Send + Sync>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let data: JsonWorkbook = serde_json::from_reader(reader)?;
        Ok(JsonAdapter {
            data,
            ..JsonAdapter::new()
        })
    }

    fn open_bytes(bytes: Vec<u8>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let data: JsonWorkbook = serde_json::from_slice(&bytes)?;
        Ok(JsonAdapter {
            data,
            ..JsonAdapter::new()
        })
    }

    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, Self::Error> {
        if let Some(js) = self.data.sheets.get(sheet) {
            let mut out = BTreeMap::new();
            for c in &js.cells {
                if c.row >= start.0 && c.row <= end.0 && c.col >= start.1 && c.col <= end.1 {
                    let lit = c.value.as_ref().map(json_to_literal);
                    out.insert(
                        (c.row, c.col),
                        CellData {
                            value: lit,
                            formula: c.formula.clone(),
                            style: c.style,
                        },
                    );
                }
            }
            Ok(out)
        } else {
            Ok(BTreeMap::new())
        }
    }

    fn read_sheet(&mut self, sheet: &str) -> Result<SheetData, Self::Error> {
        if let Some(js) = self.data.sheets.get(sheet) {
            Ok(Self::to_sheet_data(js))
        } else {
            Ok(SheetData {
                cells: BTreeMap::new(),
                dimensions: None,
                tables: vec![],
                named_ranges: vec![],
                date_system_1904: false,
                merged_cells: vec![],
                hidden: false,
            })
        }
    }

    fn sheet_bounds(&self, sheet: &str) -> Option<(u32, u32)> {
        self.data.sheets.get(sheet).and_then(|s| s.dimensions)
    }

    fn is_loaded(&self, _sheet: &str, _row: Option<u32>, _col: Option<u32>) -> bool {
        true
    }
}

impl SpreadsheetWriter for JsonAdapter {
    type Error = IoError;

    fn write_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        data: CellData,
    ) -> Result<(), Self::Error> {
        let sheet_entry = self
            .data
            .sheets
            .entry(sheet.to_string())
            .or_insert_with(JsonSheet::default);
        if let Some(cell) = sheet_entry
            .cells
            .iter_mut()
            .find(|c| c.row == row && c.col == col)
        {
            cell.value = data.value.as_ref().map(literal_to_json);
            cell.formula = data.formula;
            cell.style = data.style;
        } else {
            sheet_entry.cells.push(JsonCell {
                row,
                col,
                value: data.value.as_ref().map(literal_to_json),
                formula: data.formula,
                style: data.style,
            });
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
        if let Some(js) = self.data.sheets.get_mut(sheet) {
            js.cells.retain(|c| {
                !(c.row >= start.0 && c.row <= end.0 && c.col >= start.1 && c.col <= end.1)
            });
        }
        Ok(())
    }

    fn create_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        self.data
            .sheets
            .entry(name.to_string())
            .or_insert_with(JsonSheet::default);
        Ok(())
    }

    fn delete_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        self.data.sheets.remove(name);
        Ok(())
    }

    fn rename_sheet(&mut self, old: &str, new: &str) -> Result<(), Self::Error> {
        if let Some(sheet) = self.data.sheets.remove(old) {
            self.data.sheets.insert(new.to_string(), sheet);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn save(&mut self) -> Result<(), Self::Error> {
        if let Some(path) = &self.path {
            let mut file = File::create(path)?;
            let s = serde_json::to_string_pretty(&self.data)?;
            file.write_all(s.as_bytes())?;
            Ok(())
        } else {
            Ok(())
        }
    }

    fn save_to<'a>(&mut self, dest: SaveDestination<'a>) -> Result<Option<Vec<u8>>, Self::Error> {
        match dest {
            SaveDestination::InPlace => self.save().map(|_| None),
            SaveDestination::Path(path) => {
                let mut file = File::create(path)?;
                let s = serde_json::to_string_pretty(&self.data)?;
                file.write_all(s.as_bytes())?;
                self.path = Some(path.to_path_buf());
                Ok(None)
            }
            SaveDestination::Writer(writer) => {
                let s = serde_json::to_string_pretty(&self.data)?;
                writer.write_all(s.as_bytes())?;
                Ok(None)
            }
            SaveDestination::Bytes => {
                let s = serde_json::to_vec_pretty(&self.data)?;
                Ok(Some(s))
            }
        }
    }
}

fn literal_to_json(v: &formualizer_common::LiteralValue) -> JsonValue {
    use formualizer_common::LiteralValue as L;
    match v {
        L::Int(i) => JsonValue::Int(*i),
        L::Number(n) => JsonValue::Number(*n),
        L::Text(s) => JsonValue::Text(s.clone()),
        L::Boolean(b) => JsonValue::Boolean(*b),
        L::Empty => JsonValue::Empty,
        L::Array(arr) => JsonValue::Array(
            arr.iter()
                .map(|row| row.iter().map(literal_to_json).collect())
                .collect(),
        ),
        L::Date(d) => JsonValue::Date(d.to_string()),
        L::DateTime(dt) => JsonValue::DateTime(dt.to_string()),
        L::Time(t) => JsonValue::Time(t.to_string()),
        L::Duration(dur) => JsonValue::Duration(dur.num_seconds()),
        L::Error(e) => JsonValue::Error(e.kind.to_string()),
        L::Pending => JsonValue::Pending,
    }
}

fn json_to_literal(v: &JsonValue) -> formualizer_common::LiteralValue {
    use formualizer_common::LiteralValue as L;
    match v {
        JsonValue::Int(i) => L::Int(*i),
        JsonValue::Number(n) => L::Number(*n),
        JsonValue::Text(s) => L::Text(s.clone()),
        JsonValue::Boolean(b) => L::Boolean(*b),
        JsonValue::Empty => L::Empty,
        JsonValue::Array(arr) => L::Array(
            arr.iter()
                .map(|row| row.iter().map(json_to_literal).collect())
                .collect(),
        ),
        JsonValue::Date(s) => {
            let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .unwrap_or_else(|_| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            L::Date(d)
        }
        JsonValue::DateTime(s) => {
            let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                .unwrap_or_else(|_| {
                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                });
            L::DateTime(dt)
        }
        JsonValue::Time(s) => {
            let t = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S")
                .unwrap_or_else(|_| chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
            L::Time(t)
        }
        JsonValue::Duration(secs) => L::Duration(chrono::Duration::seconds(*secs)),
        JsonValue::Error(code) => L::Error(
            formualizer_common::error::ExcelError::from_error_string(code),
        ),
        JsonValue::Pending => L::Pending,
    }
}
