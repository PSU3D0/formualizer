use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use std::sync::{Arc, RwLock};

use crate::resolver::PyResolver;
use crate::value::PyLiteralValue;

/// Sheet class per CanonIDL Tier 1
#[gen_stub_pyclass]
#[pyclass(name = "Sheet", module = "formualizer")]
pub struct PySheet {
    pub(crate) engine: Arc<RwLock<formualizer_eval::engine::Engine<PyResolver>>>,
    #[pyo3(get)]
    pub name: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySheet {
    /// Set a single value
    pub fn set_value(&self, row: u32, col: u32, value: PyLiteralValue) -> PyResult<()> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        eng.set_cell_value(&self.name, row, col, value.inner)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("set_value: {e}"))
            })
    }

    /// Set a single formula
    pub fn set_formula(&self, row: u32, col: u32, formula: &str) -> PyResult<()> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }
        let ast = formualizer_core::parser::parse(formula)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        eng.set_cell_formula(&self.name, row, col, ast)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("set_formula: {e}"))
            })
    }

    /// Get a single cell (value + optional formula pretty) — simplified: returns value only for now
    pub fn get_cell(&self, row: u32, col: u32) -> PyResult<crate::workbook::PyCell> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }
        // Demand-evaluate this cell to ensure its formula is up-to-date
        {
            let mut eng = self.engine.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
            })?;
            eng.evaluate_cell(&self.name, row, col).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("evaluate_cell: {:?}", e))
            })?;
        }
        let eng = self.engine.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let value = eng
            .get_cell_value(&self.name, row, col)
            .unwrap_or(formualizer_common::LiteralValue::Empty);
        Ok(crate::workbook::PyCell {
            value: PyLiteralValue { inner: value },
            formula: None,
        })
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
        // Validate rectangular shape per CanonIDL
        if data.len() as u32 != rows {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Data row count mismatch",
            ));
        }
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        eng.begin_batch();
        for (r_idx, r_any) in data.iter().enumerate() {
            let r_list: Bound<'_, PyList> = r_any.downcast_into()?;
            if r_list.len() as u32 != cols {
                eng.end_batch();
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Data col count mismatch",
                ));
            }
            for (c_idx, item) in r_list.iter().enumerate() {
                let v = item.extract::<PyLiteralValue>()?;
                let row = start_row + r_idx as u32;
                let col = start_col + c_idx as u32;
                eng.set_cell_value(&self.name, row, col, v.inner.clone())
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("set_value: {e}"))
                    })?;
            }
        }
        eng.end_batch();
        Ok(())
    }

    /// Batch set formulas into a rectangle
    pub fn set_formulas_batch(
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
        if data.len() as u32 != rows {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Data row count mismatch",
            ));
        }
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        eng.begin_batch();
        for (r_idx, r_any) in data.iter().enumerate() {
            let r_list: Bound<'_, PyList> = r_any.downcast_into()?;
            if r_list.len() as u32 != cols {
                eng.end_batch();
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Data col count mismatch",
                ));
            }
            for (c_idx, item) in r_list.iter().enumerate() {
                let s = item.extract::<String>()?;
                let ast = formualizer_core::parser::parse(&s)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
                let row = start_row + r_idx as u32;
                let col = start_col + c_idx as u32;
                eng.set_cell_formula(&self.name, row, col, ast)
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "set_formula: {e}"
                        ))
                    })?;
            }
        }
        eng.end_batch();
        Ok(())
    }

    /// Get values for a range (rows x cols list[list])
    pub fn get_values(
        &self,
        range: &crate::workbook::PyRangeAddress,
    ) -> PyResult<Vec<Vec<PyLiteralValue>>> {
        // Demand evaluate the rectangle first
        {
            let mut eng = self.engine.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
            })?;
            for r in range.start_row..=range.end_row {
                for c in range.start_col..=range.end_col {
                    let _ = eng.evaluate_cell(&range.sheet, r, c);
                }
            }
        }
        let eng = self.engine.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let mut out: Vec<Vec<PyLiteralValue>> =
            Vec::with_capacity((range.end_row - range.start_row + 1) as usize);
        for r in range.start_row..=range.end_row {
            let mut row_vec = Vec::with_capacity((range.end_col - range.start_col + 1) as usize);
            for c in range.start_col..=range.end_col {
                let v = eng
                    .get_cell_value(&range.sheet, r, c)
                    .unwrap_or(formualizer_common::LiteralValue::Empty);
                row_vec.push(PyLiteralValue { inner: v });
            }
            out.push(row_vec);
        }
        Ok(out)
    }

    /// Get formulas for a range (strings; empty when not a formula)
    pub fn get_formulas(
        &self,
        range: &crate::workbook::PyRangeAddress,
    ) -> PyResult<Vec<Vec<String>>> {
        let eng = self.engine.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let mut out: Vec<Vec<String>> =
            Vec::with_capacity((range.end_row - range.start_row + 1) as usize);
        for r in range.start_row..=range.end_row {
            let mut row_vec = Vec::with_capacity((range.end_col - range.start_col + 1) as usize);
            for c in range.start_col..=range.end_col {
                // Try to find formula text by using Engine evaluate path with pretty print
                let sheet_id = match eng.graph.sheet_id(&range.sheet) {
                    Some(s) => s,
                    None => {
                        row_vec.push(String::new());
                        continue;
                    }
                };
                let cref = eng.graph.make_cell_ref(&range.sheet, r, c);
                let text = eng
                    .graph
                    .get_vertex_id_for_address(&cref)
                    .and_then(|&vid| eng.graph.get_formula(vid))
                    .map(|ast| formualizer_core::pretty::pretty_print(&ast))
                    .unwrap_or_default();
                row_vec.push(text);
            }
            out.push(row_vec);
        }
        Ok(out)
    }

    /// Clear range
    pub fn clear_range(&self, range: &crate::workbook::PyRangeAddress) -> PyResult<()> {
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let Some(sid) = eng.graph.sheet_id(&range.sheet) else {
            return Ok(());
        };
        let mut editor = formualizer_eval::engine::VertexEditor::new(&mut eng.graph);
        editor
            .clear_range(
                sid,
                range.start_row,
                range.start_col,
                range.end_row,
                range.end_col,
            )
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("clear_range: {e}"))
            })?;
        Ok(())
    }

    /// Used range (min/max bounds over occupied vertices)
    pub fn used_range(&self) -> PyResult<crate::workbook::PyRangeAddress> {
        let eng = self.engine.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let Some(sid) = eng.graph.sheet_id(&self.name) else {
            return Ok(crate::workbook::PyRangeAddress {
                sheet: self.name.clone(),
                start_row: 1,
                start_col: 1,
                end_row: 0,
                end_col: 0,
            });
        };
        // Heuristic: derive min/max from index if present
        let mut min_r = u32::MAX;
        let mut min_c = u32::MAX;
        let mut max_r = 0;
        let mut max_c = 0;
        let mut any = false;
        for vid in eng.graph.vertices_in_sheet(sid) {
            let coord = eng.graph.get_coord(vid);
            any = true;
            min_r = min_r.min(coord.row());
            max_r = max_r.max(coord.row());
            min_c = min_c.min(coord.col());
            max_c = max_c.max(coord.col());
        }
        if !any {
            return Ok(crate::workbook::PyRangeAddress {
                sheet: self.name.clone(),
                start_row: 1,
                start_col: 1,
                end_row: 0,
                end_col: 0,
            });
        }
        Ok(crate::workbook::PyRangeAddress {
            sheet: self.name.clone(),
            start_row: min_r,
            start_col: min_c,
            end_row: max_r,
            end_col: max_c,
        })
    }

    /// Dimensions (rows, cols) based on used range
    pub fn dimensions(&self) -> PyResult<(u32, u32)> {
        let ur = self.used_range()?;
        if ur.end_row == 0 || ur.end_col == 0 {
            return Ok((0, 0));
        }
        Ok((ur.end_row, ur.end_col))
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySheet>()?;
    Ok(())
}
