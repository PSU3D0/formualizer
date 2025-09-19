use std::sync::{Arc, RwLock};

use crate::{RangeAddress, Workbook};
use formualizer_common::LiteralValue;
use std::collections::BTreeMap;

/// A bindings-friendly worksheet handle backed by an Arc<RwLock<Workbook>>.
/// For native Rust, prefer borrowing APIs on `Workbook` directly for better ergonomics.
#[derive(Clone)]
pub struct WorksheetHandle {
    wb: Arc<RwLock<Workbook>>,
    name: Arc<str>,
}

impl WorksheetHandle {
    pub fn new(wb: Arc<RwLock<Workbook>>, name: impl Into<Arc<str>>) -> Self {
        Self {
            wb,
            name: name.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn get_value(&self, row: u32, col: u32) -> Option<LiteralValue> {
        self.wb
            .read()
            .ok()
            .and_then(|w| w.get_value(&self.name, row, col))
    }
    pub fn get_formula(&self, row: u32, col: u32) -> Option<String> {
        self.wb
            .read()
            .ok()
            .and_then(|w| w.get_formula(&self.name, row, col))
    }
    pub fn set_value(&self, row: u32, col: u32, v: LiteralValue) -> Result<(), crate::IoError> {
        self.wb
            .write()
            .map_err(|_| crate::IoError::Io(std::io::Error::other("lock")))?
            .set_value(&self.name, row, col, v)
    }
    pub fn set_formula(&self, row: u32, col: u32, f: &str) -> Result<(), crate::IoError> {
        self.wb
            .write()
            .map_err(|_| crate::IoError::Io(std::io::Error::other("lock")))?
            .set_formula(&self.name, row, col, f)
    }
    pub fn set_values(
        &self,
        start_row: u32,
        start_col: u32,
        rows: &[Vec<LiteralValue>],
    ) -> Result<(), crate::IoError> {
        self.wb
            .write()
            .map_err(|_| crate::IoError::Io(std::io::Error::other("lock")))?
            .set_values(&self.name, start_row, start_col, rows)
    }
    pub fn set_formulas(
        &self,
        start_row: u32,
        start_col: u32,
        rows: &[Vec<String>],
    ) -> Result<(), crate::IoError> {
        self.wb
            .write()
            .map_err(|_| crate::IoError::Io(std::io::Error::other("lock")))?
            .set_formulas(&self.name, start_row, start_col, rows)
    }
    pub fn read_range(&self, addr: &RangeAddress) -> Vec<Vec<LiteralValue>> {
        self.wb
            .read()
            .map(|w| w.read_range(addr))
            .unwrap_or_default()
    }
    pub fn write_range(
        &self,
        cells: BTreeMap<(u32, u32), crate::traits::CellData>,
    ) -> Result<(), crate::IoError> {
        self.wb
            .write()
            .map_err(|_| crate::IoError::Io(std::io::Error::other("lock")))?
            .write_range(&self.name, (1, 1), cells)
    }
    pub fn evaluate_cell(&self, row: u32, col: u32) -> Result<LiteralValue, crate::IoError> {
        self.wb
            .write()
            .map_err(|_| crate::IoError::Io(std::io::Error::other("lock")))?
            .evaluate_cell(&self.name, row, col)
    }
}
