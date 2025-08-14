use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use formualizer_common::LiteralValue;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::value::PyLiteralValue;

/// Represents a cell's data
#[derive(Clone, Debug)]
pub struct CellData {
    pub value: Option<LiteralValue>,
    pub formula: Option<String>,
}

/// Sheet data structure
#[derive(Clone, Debug)]
pub struct SheetData {
    pub name: String,
    pub cells: HashMap<(u32, u32), CellData>,
}

/// RangeAddress as defined in CanonIDL
#[gen_stub_pyclass]
#[pyclass(name = "RangeAddress", module = "formualizer")]
#[derive(Clone, Debug)]
pub struct PyRangeAddress {
    #[pyo3(get)]
    pub sheet: String,
    #[pyo3(get)]
    pub start_row: u32,
    #[pyo3(get)]
    pub start_col: u32,
    #[pyo3(get)]
    pub end_row: u32,
    #[pyo3(get)]
    pub end_col: u32,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRangeAddress {
    #[new]
    #[pyo3(signature = (sheet, start_row, start_col, end_row, end_col))]
    pub fn new(
        sheet: String,
        start_row: u32,
        start_col: u32,
        end_row: u32,
        end_col: u32,
    ) -> PyResult<Self> {
        if start_row == 0 || start_col == 0 || end_row == 0 || end_col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row and column indices must be 1-based (minimum value is 1)",
            ));
        }
        if start_row > end_row || start_col > end_col {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "RangeAddress must be inclusive and ordered (start <= end)",
            ));
        }
        Ok(Self {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "RangeAddress(sheet='{}', start_row={}, start_col={}, end_row={}, end_col={})",
            self.sheet, self.start_row, self.start_col, self.end_row, self.end_col
        )
    }
}

/// Cell struct per CanonIDL (value + optional formula)
#[gen_stub_pyclass]
#[pyclass(name = "Cell", module = "formualizer")]
pub struct PyCell {
    #[pyo3(get)]
    pub value: PyLiteralValue,
    #[pyo3(get)]
    pub formula: Option<String>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCell {
    #[new]
    #[pyo3(signature = (value, formula=None))]
    pub fn new(value: PyLiteralValue, formula: Option<String>) -> Self {
        Self { value, formula }
    }

    fn __repr__(&self) -> String {
        match &self.formula {
            Some(f) => format!("Cell(value={:?}, formula={:?})", self.value.inner, f),
            None => format!("Cell(value={:?}, formula=None)", self.value.inner),
        }
    }
}

/// NamedRange per CanonIDL (limited to cell/range definitions for now)
#[gen_stub_pyclass]
#[pyclass(name = "NamedRange", module = "formualizer")]
#[derive(Clone)]
pub struct PyNamedRange {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub sheet: Option<String>,
    #[pyo3(get)]
    pub range: PyRangeAddress,
}

/// Workbook class - represents data storage only
#[gen_stub_pyclass]
#[pyclass(name = "Workbook", module = "formualizer")]
#[derive(Clone)]
pub struct PyWorkbook {
    pub(crate) sheets: Arc<RwLock<HashMap<String, SheetData>>>,
    pub(crate) named_ranges: Arc<RwLock<HashMap<String, PyNamedRange>>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWorkbook {
    /// Create a new empty workbook
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(Self {
            sheets: Arc::new(RwLock::new(HashMap::new())),
            named_ranges: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Load a workbook from a file path (returns loaded data)
    #[classmethod]
    #[pyo3(signature = (path, strategy=None))]
    pub fn load_path(
        _cls: &Bound<'_, pyo3::types::PyType>,
        path: &str,
        strategy: Option<&str>,
    ) -> PyResult<Self> {
        use formualizer_io::{backends::CalamineAdapter, LoadStrategy, SpreadsheetReader};
        use std::path::Path;

        // Parse load strategy
        let load_strategy = match strategy {
            Some("eager_all") => LoadStrategy::EagerAll,
            Some("eager_sheet") | None => LoadStrategy::EagerSheet, // Default
            Some("lazy_cell") => LoadStrategy::LazyCell,
            Some("write_only") => LoadStrategy::WriteOnly,
            Some(s) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid strategy: {}. Use 'eager_all', 'eager_sheet', 'lazy_cell', or 'write_only'",
                    s
                )))
            }
        };

        // Create the adapter
        let mut adapter = CalamineAdapter::open_path(Path::new(path)).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open workbook: {}", e))
        })?;

        // Read sheets into our data structure
        let sheet_names = adapter.sheet_names().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Failed to read sheet names: {}",
                e
            ))
        })?;

        let mut sheets = HashMap::new();

        for sheet_name in &sheet_names {
            if matches!(load_strategy, LoadStrategy::WriteOnly) {
                // Just create empty sheet
                sheets.insert(
                    sheet_name.clone(),
                    SheetData {
                        name: sheet_name.clone(),
                        cells: HashMap::new(),
                    },
                );
                continue;
            }

            let sheet_data = adapter.read_sheet(sheet_name).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to read sheet {}: {}",
                    sheet_name, e
                ))
            })?;

            let mut cells = HashMap::new();
            for ((row, col), cell) in sheet_data.cells {
                cells.insert(
                    (row, col),
                    CellData {
                        value: cell.value,
                        formula: cell.formula,
                    },
                );
            }

            sheets.insert(
                sheet_name.clone(),
                SheetData {
                    name: sheet_name.clone(),
                    cells,
                },
            );
        }

        Ok(Self {
            sheets: Arc::new(RwLock::new(sheets)),
            named_ranges: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Add a sheet (idempotent if exists)
    pub fn add_sheet(&self, name: &str) -> PyResult<()> {
        let mut sheets = self.sheets.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;

        if !sheets.contains_key(name) {
            sheets.insert(
                name.to_string(),
                SheetData {
                    name: name.to_string(),
                    cells: HashMap::new(),
                },
            );
        }
        Ok(())
    }

    /// Remove a sheet by name
    pub fn remove_sheet(&self, name: &str) -> PyResult<()> {
        let mut sheets = self.sheets.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;

        if sheets.remove(name).is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Sheet not found: {name}"
            )));
        }
        Ok(())
    }

    /// Get a Sheet handle
    pub fn sheet(&self, name: &str) -> PyResult<crate::sheet::PySheet> {
        // Ensure sheet exists, create if needed
        {
            let mut sheets = self.sheets.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
            })?;

            if !sheets.contains_key(name) {
                sheets.insert(
                    name.to_string(),
                    SheetData {
                        name: name.to_string(),
                        cells: HashMap::new(),
                    },
                );
            }
        }

        Ok(crate::sheet::PySheet {
            workbook: self.clone(),
            name: name.to_string(),
        })
    }

    /// List sheet names
    #[getter]
    pub fn sheet_names(&self) -> PyResult<Vec<String>> {
        let sheets = self.sheets.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        Ok(sheets.keys().cloned().collect())
    }

    /// Check if a sheet exists
    pub fn has_sheet(&self, name: &str) -> PyResult<bool> {
        let sheets = self.sheets.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        Ok(sheets.contains_key(name))
    }

    fn __repr__(&self) -> String {
        let sheets = self.sheets.read().unwrap();
        format!("Workbook(sheets={})", sheets.len())
    }
}

// Internal methods not exposed to Python
impl PyWorkbook {
    /// Get cell data (internal use)
    pub(crate) fn get_cell_data(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> PyResult<Option<CellData>> {
        let sheets = self.sheets.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;

        let sheet_data = sheets.get(sheet).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Sheet not found: {sheet}"))
        })?;

        Ok(sheet_data.cells.get(&(row, col)).cloned())
    }

    /// Set cell data (internal use)
    pub(crate) fn set_cell_data(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
        data: CellData,
    ) -> PyResult<()> {
        let mut sheets = self.sheets.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;

        let sheet_data = sheets.get_mut(sheet).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Sheet not found: {sheet}"))
        })?;

        sheet_data.cells.insert((row, col), data);
        Ok(())
    }
}

/// Register the workbook module with Python
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRangeAddress>()?;
    m.add_class::<PyCell>()?;
    m.add_class::<PyNamedRange>()?;
    m.add_class::<PyWorkbook>()?;
    Ok(())
}
