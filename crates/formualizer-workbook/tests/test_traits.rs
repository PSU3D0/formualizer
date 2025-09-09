use formualizer_workbook::{
    AccessGranularity, BackendCaps, CellData, LiteralValue, MergedRange, SheetData,
    SpreadsheetReader,
};
use std::collections::BTreeMap;

#[test]
fn test_cell_data_conversion() {
    let cell = CellData {
        value: Some(LiteralValue::Number(42.0)),
        formula: Some("=A1+B1".to_string()),
        style: None,
    };

    assert_eq!(cell.value, Some(LiteralValue::Number(42.0)));
    assert_eq!(cell.formula, Some("=A1+B1".to_string()));
}

#[test]
fn test_cell_data_builders() {
    let value_cell = CellData::from_value(42.0);
    assert_eq!(value_cell.value, Some(LiteralValue::Number(42.0)));
    assert_eq!(value_cell.formula, None);

    let formula_cell = CellData::from_formula("=SUM(A1:A10)");
    assert_eq!(formula_cell.value, None);
    assert_eq!(formula_cell.formula, Some("=SUM(A1:A10)".to_string()));
}

#[test]
fn test_merged_range_contains() {
    let merged = MergedRange {
        start_row: 1,
        start_col: 1,
        end_row: 3,
        end_col: 2,
    };

    assert!(merged.contains(1, 1)); // Top-left
    assert!(merged.contains(3, 2)); // Bottom-right
    assert!(merged.contains(2, 1)); // Middle
    assert!(!merged.contains(4, 1)); // Outside row
    assert!(!merged.contains(1, 3)); // Outside col
}

// Mock reader for testing
struct MockReader {
    granularity: AccessGranularity,
    cells: BTreeMap<(String, u32, u32), CellData>,
}

impl MockReader {
    fn new(granularity: AccessGranularity) -> Self {
        Self {
            granularity,
            cells: BTreeMap::new(),
        }
    }

    fn with_cell(mut self, sheet: &str, row: u32, col: u32, data: CellData) -> Self {
        self.cells.insert((sheet.to_string(), row, col), data);
        self
    }
}

impl SpreadsheetReader for MockReader {
    type Error = std::io::Error;

    fn access_granularity(&self) -> AccessGranularity {
        self.granularity
    }

    fn capabilities(&self) -> BackendCaps {
        BackendCaps {
            read: true,
            formulas: true,
            ..Default::default()
        }
    }

    fn sheet_names(&self) -> Result<Vec<String>, Self::Error> {
        let mut sheets: Vec<String> = self
            .cells
            .keys()
            .map(|(sheet, _, _)| sheet.clone())
            .collect();
        sheets.sort();
        sheets.dedup();
        Ok(sheets)
    }

    fn open_path<P: AsRef<std::path::Path>>(_path: P) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new(AccessGranularity::Cell))
    }

    fn open_reader(_reader: Box<dyn std::io::Read + Send + Sync>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new(AccessGranularity::Cell))
    }

    fn open_bytes(_data: Vec<u8>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new(AccessGranularity::Cell))
    }

    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, Self::Error> {
        let mut result = BTreeMap::new();

        for ((s, r, c), data) in &self.cells {
            if s == sheet && *r >= start.0 && *r <= end.0 && *c >= start.1 && *c <= end.1 {
                result.insert((*r, *c), data.clone());
            }
        }

        Ok(result)
    }

    fn read_sheet(&mut self, sheet: &str) -> Result<SheetData, Self::Error> {
        let mut cells = BTreeMap::new();

        for ((s, r, c), data) in &self.cells {
            if s == sheet {
                cells.insert((*r, *c), data.clone());
            }
        }

        Ok(SheetData {
            cells,
            dimensions: None,
            tables: vec![],
            named_ranges: vec![],
            date_system_1904: false,
            merged_cells: vec![],
            hidden: false,
        })
    }

    fn sheet_bounds(&self, _sheet: &str) -> Option<(u32, u32)> {
        None
    }

    fn is_loaded(&self, _sheet: &str, _row: Option<u32>, _col: Option<u32>) -> bool {
        true
    }
}

#[test]
fn test_access_granularity() {
    let reader = MockReader::new(AccessGranularity::Cell);
    assert!(matches!(
        reader.access_granularity(),
        AccessGranularity::Cell
    ));

    let reader = MockReader::new(AccessGranularity::Sheet);
    assert!(matches!(
        reader.access_granularity(),
        AccessGranularity::Sheet
    ));
}

#[test]
fn test_mock_reader_basic() {
    let mut reader = MockReader::new(AccessGranularity::Cell)
        .with_cell("Sheet1", 1, 1, CellData::from_value(42.0))
        .with_cell("Sheet1", 1, 2, CellData::from_formula("=A1*2"));

    let sheets = reader.sheet_names().unwrap();
    assert_eq!(sheets, vec!["Sheet1"]);

    let cell = reader.read_cell("Sheet1", 1, 1).unwrap();
    assert_eq!(cell.unwrap().value, Some(LiteralValue::Number(42.0)));

    let range = reader.read_range("Sheet1", (1, 1), (1, 2)).unwrap();
    assert_eq!(range.len(), 2);
}
