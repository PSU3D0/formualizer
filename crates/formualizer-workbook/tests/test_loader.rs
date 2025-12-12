use formualizer_eval::engine::Engine;
use formualizer_eval::engine::EvalConfig;
use formualizer_workbook::{
    AccessGranularity, BackendCaps, CellData, LiteralValue, LoadStrategy, SheetData,
    SpreadsheetReader, SpreadsheetWriter, WorkbookLoader,
};
use std::collections::BTreeMap;
// no std::time here

// Mock backend for testing
struct MockSheetBackend {
    data: BTreeMap<(String, u32, u32), CellData>,
}

impl MockSheetBackend {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    fn with_data(cells: Vec<((u32, u32), CellData)>) -> Self {
        let mut backend = Self::new();
        for ((row, col), data) in cells {
            backend.data.insert(("Sheet1".to_string(), row, col), data);
        }
        backend
    }
}

impl SpreadsheetReader for MockSheetBackend {
    type Error = std::io::Error;

    fn access_granularity(&self) -> AccessGranularity {
        AccessGranularity::Sheet
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
            .data
            .keys()
            .map(|(sheet, _, _)| sheet.clone())
            .collect();
        sheets.sort();
        sheets.dedup();
        if sheets.is_empty() {
            sheets.push("Sheet1".to_string());
        }
        Ok(sheets)
    }

    fn open_path<P: AsRef<std::path::Path>>(_path: P) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new())
    }

    fn open_reader(_reader: Box<dyn std::io::Read + Send + Sync>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new())
    }

    fn open_bytes(_data: Vec<u8>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new())
    }

    fn read_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<BTreeMap<(u32, u32), CellData>, Self::Error> {
        let mut result = BTreeMap::new();

        for ((s, r, c), data) in &self.data {
            if s == sheet && *r >= start.0 && *r <= end.0 && *c >= start.1 && *c <= end.1 {
                result.insert((*r, *c), data.clone());
            }
        }

        Ok(result)
    }

    fn read_sheet(&mut self, sheet: &str) -> Result<SheetData, Self::Error> {
        let mut cells = BTreeMap::new();

        for ((s, r, c), data) in &self.data {
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

fn create_test_engine() -> Engine<formualizer_eval::test_workbook::TestWorkbook> {
    let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
    Engine::new(ctx, EvalConfig::default())
}

#[test]
fn test_workbook_loader_empty() {
    let backend = MockSheetBackend::new();
    let mut engine = create_test_engine();
    let mut loader = WorkbookLoader::new(backend, LoadStrategy::EagerSheet);

    // Should not error on empty workbook
    loader.load_into_engine(&mut engine).unwrap();

    assert_eq!(loader.stats().cells_loaded, 0);
    assert_eq!(loader.stats().formulas_loaded, 0);
}

#[test]
fn test_workbook_loader_populates_graph() {
    let backend = MockSheetBackend::with_data(vec![
        ((1, 1), CellData::from_value(42.0)),
        ((1, 2), CellData::from_formula("=A1*2")),
    ]);

    let mut engine = create_test_engine();
    let mut loader = WorkbookLoader::new(backend, LoadStrategy::EagerSheet);

    loader.load_into_engine(&mut engine).unwrap();

    // Check stats
    assert_eq!(loader.stats().cells_loaded, 2);
    assert_eq!(loader.stats().formulas_loaded, 1);
    assert_eq!(loader.stats().sheets_loaded, 1);

    // Values should be in graph
    let value = engine.get_cell_value("Sheet1", 1, 1);
    assert_eq!(value, Some(LiteralValue::Number(42.0)));
}

#[test]
fn test_loader_strategies() {
    let backend = MockSheetBackend::with_data(vec![((1, 1), CellData::from_value(1.0))]);

    // Test EagerAll
    let mut engine = create_test_engine();
    let mut loader = WorkbookLoader::new(backend, LoadStrategy::EagerAll);
    loader.load_into_engine(&mut engine).unwrap();
    assert_eq!(loader.stats().sheets_loaded, 1);

    // Test WriteOnly
    let backend = MockSheetBackend::with_data(vec![((1, 1), CellData::from_value(1.0))]);
    let mut engine = create_test_engine();
    let mut loader = WorkbookLoader::new(backend, LoadStrategy::WriteOnly);
    loader.load_into_engine(&mut engine).unwrap();
    assert_eq!(loader.stats().sheets_loaded, 0);
}

#[test]
fn test_loader_performance_tracking() {
    let mut data = vec![];
    for i in 1..=100 {
        data.push(((i, 1), CellData::from_value(i as f64)));
    }

    let backend = MockSheetBackend::with_data(data);
    let mut engine = create_test_engine();
    let mut loader = WorkbookLoader::new(backend, LoadStrategy::EagerSheet);

    loader.load_into_engine(&mut engine).unwrap();

    // Should track timing
    assert!(loader.stats().load_time_ms > 0);
    assert_eq!(loader.stats().cells_loaded, 100);
}

#[cfg(feature = "json")]
#[test]
fn test_loader_registers_named_ranges() {
    use formualizer_common::RangeAddress;
    use formualizer_eval::engine::named_range::NamedDefinition;
    use formualizer_workbook::traits::NamedRangeScope;
    use formualizer_workbook::{CellData, JsonAdapter, NamedRange};

    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Sheet1").unwrap();
    adapter.create_sheet("Sheet2").unwrap();
    adapter
        .write_cell("Sheet2", 1, 1, CellData::from_value(0.0))
        .unwrap();

    adapter.set_named_ranges(
        "Sheet1",
        vec![
            NamedRange {
                name: "GlobalName".into(),
                scope: NamedRangeScope::Workbook,
                address: RangeAddress::new("Sheet1", 1, 1, 1, 1).unwrap(),
            },
            NamedRange {
                name: "LocalName".into(),
                scope: NamedRangeScope::Sheet,
                address: RangeAddress::new("Sheet1", 2, 1, 2, 2).unwrap(),
            },
        ],
    );

    let sheet = adapter.read_sheet("Sheet1").expect("read sheet1");
    assert_eq!(sheet.named_ranges.len(), 2);

    assert_eq!(sheet.named_ranges[0].name, "GlobalName");
    assert_eq!(sheet.named_ranges[0].scope, NamedRangeScope::Workbook);
    assert_eq!(
        sheet.named_ranges[0].address,
        RangeAddress::new("Sheet1", 1, 1, 1, 1).unwrap()
    );

    let mut engine = create_test_engine();
    let mut loader = WorkbookLoader::new(adapter, LoadStrategy::EagerAll);
    loader.load_into_engine(&mut engine).unwrap();

    let sheet_id = engine.graph.sheet_id("Sheet1").expect("sheet present");

    let global = engine
        .graph
        .resolve_name("GlobalName", sheet_id)
        .expect("global name registered");
    match global {
        NamedDefinition::Cell(cell) => {
            assert_eq!(engine.graph.sheet_name(cell.sheet_id), "Sheet1");
            assert_eq!(format!("{}", cell.coord), "$A$1");
        }
        other => panic!("expected cell definition, got {:?}", other),
    }

    let local = engine
        .graph
        .resolve_name("LocalName", sheet_id)
        .expect("local name registered");
    match local {
        NamedDefinition::Range(range) => {
            assert_eq!(range.start.sheet_id, sheet_id);
            assert_eq!(range.end.sheet_id, sheet_id);
            assert_eq!(format!("{}", range.start.coord), "$A$2");
            assert_eq!(format!("{}", range.end.coord), "$B$2");
        }
        other => panic!("expected range definition, got {:?}", other),
    }

    // Workbook-scoped name should also resolve from another sheet.
    let sheet2_id = engine.graph.sheet_id("Sheet2").expect("second sheet");
    let global_from_sheet2 = engine
        .graph
        .resolve_name("GlobalName", sheet2_id)
        .expect("global name visible to other sheets");
    assert!(matches!(global_from_sheet2, NamedDefinition::Cell(_)));
}
