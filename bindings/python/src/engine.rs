use formualizer_eval::engine::{Engine as RustEngine, EvalConfig};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
// use formualizer_common::value::LiteralValue;
use std::sync::{Arc, RwLock};

use crate::resolver::PyResolver;
use crate::value::PyLiteralValue;

/// Python wrapper for the evaluation engine
#[gen_stub_pyclass]
#[pyclass(name = "Engine")]
pub struct PyEngine {
    inner: Arc<RwLock<RustEngine<PyResolver>>>,
}

/// Configuration for the evaluation engine
#[gen_stub_pyclass]
#[pyclass(name = "EvaluationConfig")]
#[derive(Clone)]
pub struct PyEvaluationConfig {
    pub(crate) inner: EvalConfig,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyEvaluationConfig {
    /// Create a new evaluation configuration
    #[new]
    pub fn new() -> Self {
        PyEvaluationConfig {
            inner: EvalConfig::default(),
        }
    }

    /// Enable parallel evaluation
    #[setter]
    pub fn set_enable_parallel(&mut self, value: bool) {
        self.inner.enable_parallel = value;
    }

    #[getter]
    pub fn get_enable_parallel(&self) -> bool {
        self.inner.enable_parallel
    }

    /// Set maximum threads for parallel evaluation
    #[setter]
    pub fn set_max_threads(&mut self, value: Option<u32>) {
        self.inner.max_threads = value.map(|v| v as usize);
    }

    #[getter]
    pub fn get_max_threads(&self) -> Option<u32> {
        self.inner.max_threads.map(|v| v as u32)
    }

    /// Set range expansion limit
    #[setter]
    pub fn set_range_expansion_limit(&mut self, value: u32) {
        self.inner.range_expansion_limit = value as usize;
    }

    #[getter]
    pub fn get_range_expansion_limit(&self) -> u32 {
        self.inner.range_expansion_limit as u32
    }

    /// Set workbook seed for random functions
    #[setter]
    pub fn set_workbook_seed(&mut self, value: u64) {
        self.inner.workbook_seed = value;
    }

    #[getter]
    pub fn get_workbook_seed(&self) -> u64 {
        self.inner.workbook_seed
    }

    fn __repr__(&self) -> String {
        format!(
            "EvaluationConfig(parallel={}, max_threads={:?}, range_limit={}, seed={})",
            self.inner.enable_parallel,
            self.inner.max_threads,
            self.inner.range_expansion_limit,
            self.inner.workbook_seed
        )
    }
}

/// Result from an evaluation pass
#[gen_stub_pyclass]
#[pyclass(name = "EvaluationResult")]
pub struct PyEvaluationResult {
    pub(crate) computed_vertices: u64,
    pub(crate) cycle_errors: u32,
    pub(crate) elapsed_ms: u64,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyEvaluationResult {
    /// Number of vertices computed
    #[getter]
    pub fn computed_vertices(&self) -> u64 {
        self.computed_vertices
    }

    /// Number of cycle errors encountered
    #[getter]
    pub fn cycle_errors(&self) -> u32 {
        self.cycle_errors
    }

    /// Elapsed time in milliseconds
    #[getter]
    pub fn elapsed_ms(&self) -> u64 {
        self.elapsed_ms
    }

    /// Number of reference errors added
    #[getter]
    pub fn ref_errors_added(&self) -> u32 {
        0
    }

    /// Number of volatile functions invoked
    #[getter]
    pub fn volatile_functions_invoked(&self) -> u64 {
        0
    }

    fn __repr__(&self) -> String {
        format!(
            "EvaluationResult(computed={}, cycles={}, elapsed_ms={}, ref_errors={}, volatile={})",
            self.computed_vertices, self.cycle_errors, self.elapsed_ms, 0, 0
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyEngine {
    /// Create a new evaluation engine with a dependency graph
    #[new]
    pub fn new(config: Option<PyEvaluationConfig>) -> PyResult<Self> {
        let eval_config = config.map(|c| c.inner).unwrap_or_default();
        let engine = RustEngine::new(PyResolver, eval_config);

        Ok(PyEngine {
            inner: Arc::new(RwLock::new(engine)),
        })
    }

    /// Evaluate all cells in the workbook
    pub fn evaluate_all(&self) -> PyResult<PyEvaluationResult> {
        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;

        let result = engine.evaluate_all().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Evaluation failed: {:?}", e))
        })?;
        Ok(PyEvaluationResult {
            computed_vertices: result.computed_vertices as u64,
            cycle_errors: result.cycle_errors as u32,
            elapsed_ms: result.elapsed.as_millis() as u64,
        })
    }

    /// Evaluate a specific cell and return its value
    pub fn evaluate_cell(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> PyResult<Option<PyLiteralValue>> {
        // Validate 1-based indexing
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row and column indices must be 1-based (minimum value is 1)",
            ));
        }

        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;

        // Engine already uses 1-based indexing
        let value = engine.evaluate_cell(sheet, row, col).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to evaluate cell: {:?}",
                e
            ))
        })?;

        Ok(value.map(PyLiteralValue::from))
    }

    /// Evaluate multiple cells and return their values in the same order
    pub fn evaluate_cells(
        &self,
        _py: Python,
        cells: &Bound<'_, PyList>,
    ) -> PyResult<Vec<Option<PyLiteralValue>>> {
        let mut targets = Vec::new();

        for item in cells.iter() {
            let tuple = item.extract::<(String, u32, u32)>()?;
            let (sheet, row, col) = tuple;

            // Validate 1-based indexing
            if row == 0 || col == 0 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Row and column indices must be 1-based (minimum value is 1)",
                ));
            }

            targets.push((sheet, row, col));
        }

        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;

        // Convert targets to the format expected by evaluate_cells
        let target_refs: Vec<(&str, u32, u32)> = targets
            .iter()
            .map(|(s, r, c)| (s.as_str(), *r, *c))
            .collect();

        let values = engine.evaluate_cells(&target_refs).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to evaluate cells: {:?}",
                e
            ))
        })?;

        Ok(values
            .into_iter()
            .map(|v| v.map(PyLiteralValue::from))
            .collect())
    }

    /// Get the value of a cell without evaluating
    pub fn get_value(&self, sheet: &str, row: u32, col: u32) -> PyResult<Option<PyLiteralValue>> {
        // Validate 1-based indexing
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row and column indices must be 1-based (minimum value is 1)",
            ));
        }

        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;

        // Engine already uses 1-based indexing
        let value = engine.get_cell_value(sheet, row, col);
        Ok(value.map(PyLiteralValue::from))
    }

    /// Set a cell value
    pub fn set_value(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
        value: PyLiteralValue,
    ) -> PyResult<()> {
        // Validate 1-based indexing
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row and column indices must be 1-based (minimum value is 1)",
            ));
        }

        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;

        // Engine already uses 1-based indexing
        engine
            .set_cell_value(sheet, row, col, value.inner)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to set cell value: {:?}",
                    e
                ))
            })?;

        Ok(())
    }

    /// Set a cell formula
    pub fn set_formula(&self, sheet: &str, row: u32, col: u32, formula: &str) -> PyResult<()> {
        // Validate 1-based indexing
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row and column indices must be 1-based (minimum value is 1)",
            ));
        }

        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;

        // Parse the formula
        let ast = formualizer_core::parser::parse(formula).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to parse formula: {:?}",
                e
            ))
        })?;

        // Engine already uses 1-based indexing
        engine.set_cell_formula(sheet, row, col, ast).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to set cell formula: {:?}",
                e
            ))
        })?;

        Ok(())
    }

    /// Get the current recalculation epoch
    pub fn recalc_epoch(&self) -> PyResult<u64> {
        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;

        Ok(engine.recalc_epoch as u64)
    }

    /// Set the workbook seed for random functions
    pub fn set_workbook_seed(&self, seed: u64) -> PyResult<()> {
        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;

        engine.set_workbook_seed(seed);
        Ok(())
    }

    /// Ensure builtin functions are loaded (no-op; builtins load at engine construction)
    pub fn ensure_builtins_loaded(&self) -> PyResult<()> {
        Ok(())
    }

    /// Get the default sheet name
    pub fn default_sheet_name(&self) -> String {
        let engine = self.inner.read().expect("engine lock");
        engine.default_sheet_name().to_string()
    }

    /// Begin a batch mutation window
    pub fn begin_batch(&self) -> PyResult<()> {
        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;
        engine.begin_batch();
        Ok(())
    }

    /// End a batch mutation window
    pub fn end_batch(&self) -> PyResult<()> {
        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {}",
                e
            ))
        })?;
        engine.end_batch();
        Ok(())
    }

    fn __repr__(&self) -> String {
        "Engine(formualizer evaluation engine)".to_string()
    }
}

/// Register the engine module with Python
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEngine>()?;
    m.add_class::<PyEvaluationConfig>()?;
    m.add_class::<PyEvaluationResult>()?;
    Ok(())
}
