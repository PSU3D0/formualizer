use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::value::PyLiteralValue;
use crate::workbook::{CellData, PyCell, PyWorkbook};

/// Sheet class - represents a view into workbook data
#[gen_stub_pyclass]
#[pyclass(name = "Sheet", module = "formualizer")]
#[derive(Clone)]
pub struct PySheet {
    pub(crate) workbook: PyWorkbook,
    #[pyo3(get)]
    pub name: String,
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

        let data = CellData {
            value: Some(value.inner),
            formula: None,
        };

        self.workbook.set_cell_data(&self.name, row, col, data)
    }

    /// Set a single formula (stores in workbook, doesn't evaluate)
    pub fn set_formula(&self, row: u32, col: u32, formula: &str) -> PyResult<()> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        let data = CellData {
            value: None,
            formula: Some(formula.to_string()),
        };

        self.workbook.set_cell_data(&self.name, row, col, data)
    }

    /// Get a single cell's stored data (no evaluation)
    pub fn get_cell(&self, row: u32, col: u32) -> PyResult<PyCell> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        let cell_data = self.workbook.get_cell_data(&self.name, row, col)?;

        match cell_data {
            Some(data) => {
                let value = data
                    .value
                    .unwrap_or(formualizer_common::LiteralValue::Empty);
                Ok(PyCell {
                    value: PyLiteralValue { inner: value },
                    formula: data.formula,
                })
            }
            None => Ok(PyCell {
                value: PyLiteralValue {
                    inner: formualizer_common::LiteralValue::Empty,
                },
                formula: None,
            }),
        }
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

        for (i, row_data) in data.iter().enumerate() {
            let row_list: &Bound<'_, PyList> = row_data.downcast()?;
            if row_list.len() as u32 != cols {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Row {} has {} columns, expected {}",
                    i,
                    row_list.len(),
                    cols
                )));
            }

            for (j, val) in row_list.iter().enumerate() {
                let py_val: PyLiteralValue = val.extract()?;
                let row = start_row + i as u32;
                let col = start_col + j as u32;

                let cell_data = CellData {
                    value: Some(py_val.inner),
                    formula: None,
                };

                self.workbook
                    .set_cell_data(&self.name, row, col, cell_data)?;
            }
        }

        Ok(())
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

        for (i, row_data) in formulas.iter().enumerate() {
            let row_list: &Bound<'_, PyList> = row_data.downcast()?;
            if row_list.len() as u32 != cols {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Row {} has {} columns, expected {}",
                    i,
                    row_list.len(),
                    cols
                )));
            }

            for (j, formula) in row_list.iter().enumerate() {
                let formula_str: String = formula.extract()?;
                let row = start_row + i as u32;
                let col = start_col + j as u32;

                let cell_data = CellData {
                    value: None,
                    formula: Some(formula_str),
                };

                self.workbook
                    .set_cell_data(&self.name, row, col, cell_data)?;
            }
        }

        Ok(())
    }

    /// Get values from a range (no evaluation, just stored values)
    pub fn get_values(
        &self,
        range: &crate::workbook::PyRangeAddress,
    ) -> PyResult<Vec<Vec<PyLiteralValue>>> {
        let mut result = Vec::new();

        for row in range.start_row..=range.end_row {
            let mut row_vec = Vec::new();
            for col in range.start_col..=range.end_col {
                let cell_data = self.workbook.get_cell_data(&self.name, row, col)?;
                let value = match cell_data {
                    Some(data) => data
                        .value
                        .unwrap_or(formualizer_common::LiteralValue::Empty),
                    None => formualizer_common::LiteralValue::Empty,
                };
                row_vec.push(PyLiteralValue { inner: value });
            }
            result.push(row_vec);
        }

        Ok(result)
    }

    /// Get formulas from a range (returns formula strings, empty strings for non-formula cells)
    pub fn get_formulas(
        &self,
        range: &crate::workbook::PyRangeAddress,
    ) -> PyResult<Vec<Vec<String>>> {
        let mut result = Vec::new();

        for row in range.start_row..=range.end_row {
            let mut row_vec = Vec::new();
            for col in range.start_col..=range.end_col {
                let cell_data = self.workbook.get_cell_data(&self.name, row, col)?;
                let formula = match cell_data {
                    Some(data) => data.formula.unwrap_or_default(),
                    None => String::new(),
                };
                // Strip leading '=' if present
                let formula = formula.strip_prefix('=').unwrap_or(&formula).to_string();
                row_vec.push(formula);
            }
            result.push(row_vec);
        }

        Ok(result)
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
