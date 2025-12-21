use pyo3::prelude::*;
use pyo3::types::{PyAny, PyList};

type PyObject = pyo3::Py<pyo3::PyAny>;

use crate::value::literal_to_py;
use crate::workbook::{PyCell, PyWorkbook};
use formualizer::workbook::WorksheetHandle;

/// Sheet class - represents a view into workbook data
#[pyclass(name = "Sheet", module = "formualizer")]
#[derive(Clone)]
pub struct PySheet {
    pub(crate) workbook: PyWorkbook,
    #[pyo3(get)]
    pub name: String,
    pub(crate) handle: WorksheetHandle,
}

#[pymethods]
impl PySheet {
    /// Set a single value (stores in workbook, doesn't evaluate)
    pub fn set_value(
        &self,
        py: Python<'_>,
        row: u32,
        col: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        // Delegate to workbook so compatibility cache stays in sync
        self.workbook.set_value(py, &self.name, row, col, value)
    }

    /// Set a single formula (stores in workbook, doesn't evaluate)
    pub fn set_formula(&self, row: u32, col: u32, formula: &str) -> PyResult<()> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        // Delegate to workbook so compatibility cache stays in sync
        self.workbook.set_formula(&self.name, row, col, formula)
    }

    /// Get a single cell's stored data (no evaluation)
    pub fn get_cell(&self, row: u32, col: u32) -> PyResult<PyCell> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        let cached = {
            let sheets = self.workbook.sheets.read().unwrap();
            sheets
                .get(&self.name)
                .and_then(|m| m.get(&(row, col)).cloned())
        };

        if let Some(data) = cached {
            let value = data
                .value
                .clone()
                .or_else(|| self.handle.get_value(row, col))
                .unwrap_or(formualizer::common::LiteralValue::Empty);
            let formula = data
                .formula
                .clone()
                .or_else(|| self.handle.get_formula(row, col));
            return Ok(PyCell::new(value, formula));
        }

        let value = self
            .handle
            .get_value(row, col)
            .unwrap_or(formualizer::common::LiteralValue::Empty);
        let formula = self.handle.get_formula(row, col);
        Ok(PyCell::new(value, formula))
    }

    /// Batch set values into a rectangle
    pub fn set_values_batch(
        &self,
        py: Python<'_>,
        start_row: u32,
        start_col: u32,
        rows: u32,
        _cols: u32,
        data: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        if start_row == 0 || start_col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        if data.len() as u32 != rows {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Expected {} rows, got {}",
                rows,
                data.len()
            )));
        }

        self.workbook
            .set_values_batch(py, &self.name, start_row, start_col, data)
    }

    /// Batch set formulas into a rectangle
    pub fn set_formulas_batch(
        &self,
        start_row: u32,
        start_col: u32,
        rows: u32,
        _cols: u32,
        formulas: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        if start_row == 0 || start_col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        // Validate shape
        if formulas.len() as u32 != rows {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Expected {} rows, got {}",
                rows,
                formulas.len()
            )));
        }

        // Delegate to workbook batch API (handles cache)
        self.workbook
            .set_formulas_batch(&self.name, start_row, start_col, formulas)
    }

    /// Get values from a range (no evaluation, just stored values)
    pub fn get_values(
        &self,
        py: Python<'_>,
        range: &crate::workbook::PyRangeAddress,
    ) -> PyResult<Vec<Vec<PyObject>>> {
        let ra = formualizer::workbook::RangeAddress::new(
            &range.sheet,
            range.start_row,
            range.start_col,
            range.end_row,
            range.end_col,
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let vals = self.handle.read_range(&ra);
        vals.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| literal_to_py(py, &v))
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect()
    }

    /// Get formulas from a range (returns formula strings, empty strings for non-formula cells)
    pub fn get_formulas(
        &self,
        range: &crate::workbook::PyRangeAddress,
    ) -> PyResult<Vec<Vec<String>>> {
        let ra = formualizer::workbook::RangeAddress::new(
            &range.sheet,
            range.start_row,
            range.start_col,
            range.end_row,
            range.end_col,
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let height = ra.height();
        let width = ra.width();
        let mut out = Vec::with_capacity(height as usize);
        for r in 0..height {
            let mut row_vec = Vec::with_capacity(width as usize);
            for c in 0..width {
                let rr = ra.start_row + r;
                let cc = ra.start_col + c;
                let formula = self.handle.get_formula(rr, cc).unwrap_or_default();
                let formula = formula.strip_prefix('=').unwrap_or(&formula).to_string();
                row_vec.push(formula);
            }
            out.push(row_vec);
        }
        Ok(out)
    }

    fn __repr__(&self) -> String {
        format!("Sheet(name='{}')", self.name)
    }
}

/// Register the sheet module with Python
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySheet>()?;
    Ok(())
}
