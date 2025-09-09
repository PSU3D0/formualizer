use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::value::PyLiteralValue;
use crate::workbook::{CellData, PyCell, PyWorkbook};
use formualizer_workbook::WorksheetHandle;

/// Sheet class - represents a view into workbook data
#[gen_stub_pyclass]
#[pyclass(name = "Sheet", module = "formualizer")]
#[derive(Clone)]
pub struct PySheet {
    pub(crate) workbook: PyWorkbook,
    #[pyo3(get)]
    pub name: String,
    pub(crate) handle: WorksheetHandle,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySheet {
    /// Set a single value (stores in workbook, doesn't evaluate)
    pub fn set_value(&self, row: u32, col: u32, value: PyLiteralValue) -> PyResult<()> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        // Delegate to workbook so compatibility cache stays in sync
        self.workbook.set_value(&self.name, row, col, value)
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

        let value = PyLiteralValue {
            inner: self
                .handle
                .get_value(row, col)
                .unwrap_or(formualizer_common::LiteralValue::Empty),
        };
        let formula = self.handle.get_formula(row, col);
        Ok(PyCell { value, formula })
    }

    /// Batch set values into a rectangle
    pub fn set_values_batch(
        &self,
        start_row: u32,
        start_col: u32,
        rows: u32,
        cols: u32,
        data: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        if start_row == 0 || start_col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        // Validate rectangular shape
        if data.len() as u32 != rows {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Expected {} rows, got {}",
                rows,
                data.len()
            )));
        }

        // Delegate to workbook batch API (handles cache)
        self.workbook
            .set_values_batch(&self.name, start_row, start_col, data)
    }

    /// Batch set formulas into a rectangle
    pub fn set_formulas_batch(
        &self,
        start_row: u32,
        start_col: u32,
        rows: u32,
        cols: u32,
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
        range: &crate::workbook::PyRangeAddress,
    ) -> PyResult<Vec<Vec<PyLiteralValue>>> {
        let ra = formualizer_workbook::RangeAddress::new(
            &range.sheet,
            range.start_row,
            range.start_col,
            range.end_row,
            range.end_col,
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let vals = self.handle.read_range(&ra);
        Ok(vals
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| PyLiteralValue { inner: v })
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    /// Get formulas from a range (returns formula strings, empty strings for non-formula cells)
    pub fn get_formulas(
        &self,
        range: &crate::workbook::PyRangeAddress,
    ) -> PyResult<Vec<Vec<String>>> {
        let ra = formualizer_workbook::RangeAddress::new(
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
