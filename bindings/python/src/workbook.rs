use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use formualizer_common::LiteralValue;
use formualizer_eval::engine::{named_range::NameScope, named_range::NamedDefinition};

use std::sync::{Arc, RwLock};

use crate::engine::PyEvaluationConfig;
use crate::resolver::PyResolver;
use crate::sheet::PySheet;
use crate::value::PyLiteralValue;

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

/// Workbook class per CanonIDL Tier 1
#[gen_stub_pyclass]
#[pyclass(name = "Workbook", module = "formualizer")]
pub struct PyWorkbook {
    pub(crate) engine: Arc<RwLock<formualizer_eval::engine::Engine<PyResolver>>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWorkbook {
    /// Create a new workbook (with a backing Engine)
    #[new]
    #[pyo3(signature = (config=None))]
    pub fn new(config: Option<PyEvaluationConfig>) -> PyResult<Self> {
        let cfg = config.map(|c| c.inner).unwrap_or_default();
        let engine = formualizer_eval::engine::Engine::new(PyResolver, cfg);
        Ok(Self {
            engine: Arc::new(RwLock::new(engine)),
        })
    }

    /// Add a sheet (idempotent if exists)
    pub fn add_sheet(&self, name: &str) -> PyResult<()> {
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        eng.graph.add_sheet(name).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("add_sheet: {e}"))
        })?;
        Ok(())
    }

    /// Remove a sheet by name
    pub fn remove_sheet(&self, name: &str) -> PyResult<()> {
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let Some(id) = eng.graph.sheet_id(name) else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Sheet not found: {name}"
            )));
        };
        eng.graph.remove_sheet(id).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("remove_sheet: {e}"))
        })?;
        Ok(())
    }

    /// Get a Sheet handle
    pub fn sheet(&self, name: &str) -> PyResult<PySheet> {
        // Ensure sheet exists, create if needed
        {
            let mut eng = self.engine.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
            })?;
            if eng.graph.sheet_id(name).is_none() {
                eng.graph.add_sheet(name).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("add_sheet: {e}"))
                })?;
            }
        }
        Ok(PySheet {
            engine: self.engine.clone(),
            name: name.to_string(),
        })
    }

    /// List sheet names
    pub fn sheet_names(&self) -> PyResult<Vec<String>> {
        let eng = self.engine.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let names: Vec<String> = eng
            .graph
            .sheet_reg()
            .all_sheets()
            .into_iter()
            .map(|(_, n)| n)
            .collect();
        Ok(names)
    }

    /// Check if a sheet exists
    pub fn has_sheet(&self, name: &str) -> PyResult<bool> {
        let eng = self.engine.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        Ok(eng.graph.sheet_id(name).is_some())
    }

    /// Add a named range (range-only variant)
    pub fn add_named_range(&self, nr: &PyNamedRange) -> PyResult<()> {
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let sheet_id = eng.graph.sheet_id(&nr.range.sheet).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unknown sheet: {}",
                nr.range.sheet
            ))
        })?;

        let start = formualizer_eval::reference::CellRef::new(
            sheet_id,
            formualizer_eval::reference::Coord::new(
                nr.range.start_row,
                nr.range.start_col,
                true,
                true,
            ),
        );
        let end = formualizer_eval::reference::CellRef::new(
            sheet_id,
            formualizer_eval::reference::Coord::new(nr.range.end_row, nr.range.end_col, true, true),
        );
        let range_ref = formualizer_eval::reference::RangeRef::new(start, end);

        let scope = match &nr.sheet {
            Some(s) => {
                let sid = eng.graph.sheet_id(s).ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Unknown sheet: {s}"))
                })?;
                NameScope::Sheet(sid)
            }
            None => NameScope::Workbook,
        };

        eng.graph
            .define_name(&nr.name, NamedDefinition::Range(range_ref), scope)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    /// Remove a named range by name; returns True if found
    pub fn remove_named_range(&self, name: &str) -> PyResult<bool> {
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        // Try workbook scope first
        if eng.graph.delete_name(name, NameScope::Workbook).is_ok() {
            return Ok(true);
        }
        // Try each sheet scope
        let any = eng
            .graph
            .sheet_reg()
            .all_sheets()
            .into_iter()
            .any(|(sid, _)| eng.graph.delete_name(name, NameScope::Sheet(sid)).is_ok());
        Ok(any)
    }

    /// List defined named ranges (range/cell types only)
    pub fn list_named_ranges(&self) -> PyResult<Vec<PyNamedRange>> {
        let eng = self.engine.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let mut out = Vec::new();
        // Workbook scope
        for (name, nr) in eng.graph.named_ranges_iter() {
            if let Some(n) = super::workbook::convert_named_range(&eng, name, nr, None) {
                out.push(n);
            }
        }
        // Sheet-scoped
        for ((sid, name), nr) in eng.graph.sheet_named_ranges_iter() {
            let sheet_name = eng.graph.sheet_name(*sid).to_string();
            if let Some(n) = super::workbook::convert_named_range(&eng, name, nr, Some(sheet_name))
            {
                out.push(n);
            }
        }
        Ok(out)
    }

    /// Clear formulas in a range or entire workbook (sets affected cells to Empty)
    pub fn clear_formulas(&self, range: Option<&PyRangeAddress>) -> PyResult<()> {
        let mut eng = self.engine.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        // Set values to Empty for any formula vertices within the target
        match range {
            Some(r) => {
                let Some(sid) = eng.graph.sheet_id(&r.sheet) else {
                    return Ok(());
                };
                // Collect targets first to avoid borrow conflicts
                let mut targets = Vec::new();
                for vid in eng.graph.vertices_in_sheet(sid) {
                    let c = eng.graph.get_coord(vid);
                    if c.row() >= r.start_row
                        && c.row() <= r.end_row
                        && c.col() >= r.start_col
                        && c.col() <= r.end_col
                    {
                        if eng.graph.vertex_has_formula(vid) {
                            targets.push((c.row(), c.col()));
                        }
                    }
                }
                drop(eng);
                let mut eng2 = self.engine.write().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
                })?;
                for (row, col) in targets {
                    let _ = eng2.set_cell_value(&r.sheet, row, col, LiteralValue::Empty);
                }
            }
            None => {
                // Collect all targets across sheets
                let mut all_targets: Vec<(String, u32, u32)> = Vec::new();
                for (_, name) in eng.graph.sheet_reg().all_sheets() {
                    let sid = eng.graph.sheet_id(&name).unwrap();
                    for vid in eng.graph.vertices_in_sheet(sid) {
                        if eng.graph.vertex_has_formula(vid) {
                            let c = eng.graph.get_coord(vid);
                            all_targets.push((name.clone(), c.row(), c.col()));
                        }
                    }
                }
                drop(eng);
                let mut eng2 = self.engine.write().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
                })?;
                for (sheet, row, col) in all_targets {
                    let _ = eng2.set_cell_value(&sheet, row, col, LiteralValue::Empty);
                }
            }
        }
        Ok(())
    }

    /// List tables (not yet supported)
    pub fn list_tables(&self) -> PyResult<Vec<PyObject>> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Tables not yet supported",
        ))
    }

    pub fn add_table(&self, _table: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Tables not yet supported",
        ))
    }

    pub fn remove_table(&self, _name: &str) -> PyResult<bool> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Tables not yet supported",
        ))
    }
}

// Internal helpers and iterators exposure
impl PyWorkbook {
    pub(crate) fn engine_arc(&self) -> Arc<RwLock<formualizer_eval::engine::Engine<PyResolver>>> {
        self.engine.clone()
    }
}

// Convert internal named range to PyNamedRange if possible
pub(crate) fn convert_named_range(
    eng: &formualizer_eval::engine::Engine<PyResolver>,
    name: &str,
    nr: &formualizer_eval::engine::named_range::NamedRange,
    sheet_scope: Option<String>,
) -> Option<PyNamedRange> {
    match &nr.definition {
        NamedDefinition::Cell(c) => {
            let sheet = eng.graph.sheet_name(c.sheet_id).to_string();
            Some(PyNamedRange {
                name: name.to_string(),
                sheet: sheet_scope.clone(),
                range: PyRangeAddress {
                    sheet,
                    start_row: c.coord.row,
                    start_col: c.coord.col,
                    end_row: c.coord.row,
                    end_col: c.coord.col,
                },
            })
        }
        NamedDefinition::Range(r) => {
            let sheet = eng.graph.sheet_name(r.start.sheet_id).to_string();
            Some(PyNamedRange {
                name: name.to_string(),
                sheet: sheet_scope.clone(),
                range: PyRangeAddress {
                    sheet,
                    start_row: r.start.coord.row,
                    start_col: r.start.coord.col,
                    end_row: r.end.coord.row,
                    end_col: r.end.coord.col,
                },
            })
        }
        NamedDefinition::Formula { .. } => None,
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRangeAddress>()?;
    m.add_class::<PyCell>()?;
    m.add_class::<PyNamedRange>()?;
    m.add_class::<PyWorkbook>()?;
    Ok(())
}
