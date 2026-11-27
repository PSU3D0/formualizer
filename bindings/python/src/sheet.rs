use pyo3::prelude::*;
use pyo3::types::{PyAny, PyList};
#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

type PyObject = pyo3::Py<pyo3::PyAny>;

use crate::value::literal_to_py;
use crate::workbook::{PyCell, PyWorkbook, PyWriteRangeReport};
use formualizer::workbook::WorksheetHandle;

/// A per-sheet facade for interacting with a [`Workbook`].
///
/// Rows and columns are **1-based**.
///
/// In most cases you obtain a `Sheet` by calling `Workbook.sheet(name)`.
///
/// Example:
/// ```python
///     import formualizer as fz
///
///     wb = fz.Workbook()
///     s = wb.sheet("Data")
///     s.set_value(1, 1, 10)
///     s.set_value(2, 1, 20)
///     s.set_formula(3, 1, "=SUM(A1:A2)")
///     print(wb.evaluate_cell("Data", 3, 1))
/// ```
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "Sheet", module = "formualizer.formualizer_py", from_py_object)]
#[derive(Clone)]
pub struct PySheet {
    pub(crate) workbook: PyWorkbook,
    #[pyo3(get)]
    pub name: String,
    pub(crate) handle: WorksheetHandle,
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PySheet {
    /// Set a single value (stores in workbook, doesn't evaluate).
    ///
    /// The `value` may be a Python primitive (int/float/bool/str/None) or a
    /// [`LiteralValue`].
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

    /// Set a single formula (stores in workbook, doesn't evaluate).
    ///
    /// Formulas should be Excel-style and typically start with `=`.
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

        // Read atomically from the engine via `read_inner`, which enforces the
        // reentrancy check and poison reporting.
        let wb = self.workbook.read_inner()?;
        let value = wb
            .get_value(&self.name, row, col)
            .unwrap_or(formualizer::common::LiteralValue::Empty);
        let formula = wb.get_formula(&self.name, row, col);
        Ok(PyCell::new(value, formula))
    }

    /// Write a dense, row-major 2D sequence of values starting at 1-based `(start_row, start_col)`.
    ///
    /// Ragged rows are supported without overwriting trailing cells past each row's length.
    /// Returns a [`WriteRangeReport`] describing the written bounds and whether undo history
    /// was cleared.
    pub fn set_values(
        &self,
        py: Python<'_>,
        start_row: u32,
        start_col: u32,
        rows: &Bound<'_, PyAny>,
    ) -> PyResult<PyWriteRangeReport> {
        if start_row == 0 || start_col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }
        self.workbook
            .set_values(py, &self.name, start_row, start_col, rows)
    }

    /// Write dense, column-major data starting at 1-based `(start_row, start_col)`.
    ///
    /// Accepts:
    /// - Any Arrow PyCapsule producer implementing `__arrow_c_stream__` or `__arrow_c_array__`
    ///   (Polars, PyArrow, DuckDB, Nanoarrow, pandas when `pyarrow` is installed).
    /// - A mapping of column names to columns (`dict[str, column]`).
    /// - A sequence of columns, where each column is a 1D buffer-protocol array (`array.array`,
    ///   `memoryview`, NumPy `ndarray`), an Arrow array, or a Python sequence.
    ///
    /// When `headers=True`, schema or mapping column names are written into `start_row`
    /// and data starts on `start_row + 1`.
    #[pyo3(signature = (start_row, start_col, data, *, headers = false))]
    pub fn set_columns(
        &self,
        py: Python<'_>,
        start_row: u32,
        start_col: u32,
        data: &Bound<'_, PyAny>,
        headers: bool,
    ) -> PyResult<PyWriteRangeReport> {
        if start_row == 0 || start_col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }
        self.workbook
            .set_columns(py, &self.name, start_row, start_col, data, headers)
    }

    /// Write sparse cell patches on this sheet.
    ///
    /// Accepts either an iterable of 1-based `(row, col, value)` triples or a mapping
    /// `{(row, col): value}`. Duplicate coordinates resolve deterministically in
    /// last-write-wins order.
    pub fn set_cells(
        &self,
        py: Python<'_>,
        cells: &Bound<'_, PyAny>,
    ) -> PyResult<PyWriteRangeReport> {
        self.workbook.set_cells(py, &self.name, cells)
    }

    /// Deprecated alias for [`Sheet.set_values`].
    pub fn set_values_batch(
        &self,
        py: Python<'_>,
        start_row: u32,
        start_col: u32,
        rows: u32,
        _cols: u32,
        data: &Bound<'_, PyList>,
    ) -> PyResult<PyWriteRangeReport> {
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

        PyErr::warn(
            py,
            &py.get_type::<pyo3::exceptions::PyDeprecationWarning>(),
            c"Sheet.set_values_batch is deprecated; use Sheet.set_values instead",
            1,
        )?;
        self.workbook
            .set_values(py, &self.name, start_row, start_col, data.as_any())
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

        // Delegate to workbook batch API
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
        self.workbook.check_reentrancy()?;
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
        self.workbook.check_reentrancy()?;
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
