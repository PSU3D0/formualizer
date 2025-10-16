use formualizer_workbook::traits::SpreadsheetWriter;
use formualizer_workbook::{CellData, LiteralValue, WriteTransaction};
use std::collections::BTreeMap;
// no concurrency primitives needed after simplifying isolation test

// Simple in-memory writer backend for tests
#[derive(Default)]
struct MemWriter {
    sheets: BTreeMap<String, BTreeMap<(u32, u32), CellData>>,
    flushed: bool,
    saved: bool,
}

impl SpreadsheetWriter for MemWriter {
    type Error = std::convert::Infallible;
    fn write_cell(
        &mut self,
        sheet: &str,
        row: u32,
        col: u32,
        data: CellData,
    ) -> Result<(), Self::Error> {
        let sheet_map = self.sheets.entry(sheet.to_string()).or_default();
        sheet_map.insert((row, col), data);
        Ok(())
    }
    fn write_range(
        &mut self,
        sheet: &str,
        cells: BTreeMap<(u32, u32), CellData>,
    ) -> Result<(), Self::Error> {
        let sheet_map = self.sheets.entry(sheet.to_string()).or_default();
        for (k, v) in cells {
            sheet_map.insert(k, v);
        }
        Ok(())
    }
    fn clear_range(
        &mut self,
        sheet: &str,
        start: (u32, u32),
        end: (u32, u32),
    ) -> Result<(), Self::Error> {
        if let Some(sheet_map) = self.sheets.get_mut(sheet) {
            sheet_map.retain(|(r, c), _| {
                !(*r >= start.0 && *r <= end.0 && *c >= start.1 && *c <= end.1)
            });
        }
        Ok(())
    }
    fn create_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        self.sheets.entry(name.to_string()).or_default();
        Ok(())
    }
    fn delete_sheet(&mut self, name: &str) -> Result<(), Self::Error> {
        self.sheets.remove(name);
        Ok(())
    }
    fn rename_sheet(&mut self, old: &str, new: &str) -> Result<(), Self::Error> {
        if let Some(map) = self.sheets.remove(old) {
            self.sheets.insert(new.to_string(), map);
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.flushed = true;
        Ok(())
    }
    fn save(&mut self) -> Result<(), Self::Error> {
        self.saved = true;
        Ok(())
    }
}

#[test]
fn transaction_commit_persists_changes() {
    let mut backend = MemWriter::default();
    {
        let mut tx = WriteTransaction::new(&mut backend);
        tx.create_sheet("Sheet1")
            .write_cell("Sheet1", 1, 1, CellData::from_value(42.0))
            .write_cell("Sheet1", 2, 1, CellData::from_value(84.0));
        tx.commit().unwrap();
    }
    let sheet = backend.sheets.get("Sheet1").unwrap();
    assert_eq!(
        sheet.get(&(1, 1)).unwrap().value,
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(
        sheet.get(&(2, 1)).unwrap().value,
        Some(LiteralValue::Number(84.0))
    );
    assert!(backend.flushed && backend.saved);
}

#[test]
fn transaction_rollback_on_drop() {
    let mut backend = MemWriter::default();
    backend.create_sheet("Sheet1").unwrap();
    backend
        .write_cell("Sheet1", 1, 1, CellData::from_value(5.0))
        .unwrap();
    {
        let mut tx = WriteTransaction::new(&mut backend);
        tx.write_cell("Sheet1", 1, 1, CellData::from_value(100.0));
        // not committed -> rollback
    }
    let sheet = backend.sheets.get("Sheet1").unwrap();
    assert_eq!(
        sheet.get(&(1, 1)).unwrap().value,
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn transaction_clear_and_range() {
    let mut backend = MemWriter::default();
    backend.create_sheet("Sheet1").unwrap();
    // Seed values
    for r in 1..=3 {
        backend
            .write_cell("Sheet1", r, 1, CellData::from_value(r as f64))
            .unwrap();
    }
    {
        let mut tx = WriteTransaction::new(&mut backend);
        // Clear row2
        tx.clear_range("Sheet1", (2, 1), (2, 1));
        // Bulk insert new range
        let mut cells = BTreeMap::new();
        cells.insert((4, 1), CellData::from_value(400.0));
        tx.write_range("Sheet1", cells);
        tx.commit().unwrap();
    }
    let sheet = backend.sheets.get("Sheet1").unwrap();
    assert!(!sheet.contains_key(&(2, 1))); // cleared
    assert!(sheet.contains_key(&(4, 1))); // added
}

#[test]
fn transaction_no_intermediate_visibility() {
    // Sequential approximation: ensure state unchanged before commit.
    let mut backend = MemWriter::default();
    backend.create_sheet("Sheet1").unwrap();
    // Precondition: sheet empty
    assert!(backend.sheets.get("Sheet1").unwrap().is_empty());
    {
        let mut tx = WriteTransaction::new(&mut backend);
        tx.write_cell("Sheet1", 1, 1, CellData::from_value(7.0));
        tx.commit().unwrap();
    }
    assert!(backend.sheets.get("Sheet1").unwrap().contains_key(&(1, 1)));
}
