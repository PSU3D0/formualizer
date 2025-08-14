use formualizer_eval::engine::{Engine as RustEngine, EvalConfig};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::{Arc, RwLock};

use crate::resolver::PyResolver;
use crate::value::PyLiteralValue;
use crate::workbook::{PyCell, PyWorkbook};

/// Python wrapper for the evaluation engine
#[gen_stub_pyclass]
#[pyclass(name = "Engine")]
pub struct PyEngine {
    inner: Arc<RwLock<RustEngine<PyResolver>>>,
    workbook: Option<PyWorkbook>,
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

/// Helper function to load workbook data into an engine
fn load_workbook_into_engine(
    workbook: &PyWorkbook,
    engine: &mut RustEngine<PyResolver>,
) -> PyResult<()> {
    let sheets = workbook.sheets.read().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
    })?;
    // Defer CSR and scheduling rebuilds while bulk-loading
    engine.begin_batch();
    // Pass 1: ensure all sheets exist in the engine before loading any formulas.
    // This avoids panics when parsing formulas that reference other sheets that
    // would otherwise not be registered yet due to HashMap iteration order.
    for sheet_name in sheets.keys() {
        engine.graph.add_sheet(sheet_name).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("add_sheet: {e}"))
        })?;
    }

    // Pass 2: load cell values and formulas for each sheet.
    for (sheet_name, sheet_data) in sheets.iter() {
        // Bulk-insert plain values first for efficiency
        let mut values: Vec<(u32, u32, formualizer_common::LiteralValue)> = Vec::new();
        let mut formulas: Vec<(u32, u32, &String)> = Vec::new();
        for ((row, col), cell_data) in &sheet_data.cells {
            if let Some(ref value) = cell_data.value {
                values.push((*row, *col, value.clone()));
            }
            if let Some(ref formula) = cell_data.formula {
                formulas.push((*row, *col, formula));
            }
        }
        if !values.is_empty() {
            engine.graph.bulk_insert_values(sheet_name, values);
        }
        // Now add formulas (dependency extraction needs ASTs)
        // Use a batch parser with a volatility classifier so the ASTs carry contains_volatile.
        let mut parser = formualizer_core::parser::BatchParser::builder()
            .with_volatility_classifier(|name: &str| {
                formualizer_eval::function_registry::get("", name)
                    .map(|f| {
                        f.caps()
                            .contains(formualizer_eval::function::FnCaps::VOLATILE)
                    })
                    .unwrap_or(false)
            })
            .build();
        for (row, col, formula) in formulas {
            let ast = parser
                .parse(formula)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
            engine
                .set_cell_formula(sheet_name, row, col, ast)
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("set_formula: {e}"))
                })?;
        }
    }
    // Finalize batch operations
    engine.end_batch();

    Ok(())
}

#[pymethods]
impl PyEngine {
    /// Create a new evaluation engine
    #[new]
    #[pyo3(signature = (workbook=None, config=None))]
    pub fn new(workbook: Option<PyWorkbook>, config: Option<PyEvaluationConfig>) -> PyResult<Self> {
        let eval_config = config.map(|c| c.inner).unwrap_or_default();
        let mut engine = RustEngine::new(PyResolver, eval_config);

        // If a workbook is provided, load its data into the engine
        if let Some(ref wb) = workbook {
            load_workbook_into_engine(wb, &mut engine)?;
        }

        Ok(PyEngine {
            inner: Arc::new(RwLock::new(engine)),
            workbook,
        })
    }

    /// Create an engine from a workbook
    #[classmethod]
    #[pyo3(signature = (workbook, config=None))]
    pub fn from_workbook(
        _cls: &Bound<'_, pyo3::types::PyType>,
        workbook: PyWorkbook,
        config: Option<PyEvaluationConfig>,
    ) -> PyResult<Self> {
        let eval_config = config.map(|c| c.inner).unwrap_or_default();
        let mut engine = RustEngine::new(PyResolver, eval_config);

        // Load the workbook data into the engine
        load_workbook_into_engine(&workbook, &mut engine)?;

        Ok(PyEngine {
            inner: Arc::new(RwLock::new(engine)),
            workbook: Some(workbook),
        })
    }

    /// Create an engine by streaming from a file path using a specific backend.
    /// backend: "calamine" for now. strategy is backend-specific (optional).
    #[classmethod]
    #[pyo3(signature = (path, backend=None, _strategy=None, config=None))]
    pub fn from_path(
        _cls: &Bound<'_, pyo3::types::PyType>,
        path: &str,
        backend: Option<&str>,
        _strategy: Option<&str>,
        config: Option<PyEvaluationConfig>,
    ) -> PyResult<Self> {
        let eval_config = config.map(|c| c.inner).unwrap_or_default();
        let mut engine = RustEngine::new(PyResolver, eval_config);
        let backend = backend.unwrap_or("calamine");
        match backend {
            "calamine" => {
                use formualizer_eval::engine::ingest::EngineLoadStream;
                use formualizer_io::backends::CalamineAdapter;
                use formualizer_io::traits::SpreadsheetReader;
                let mut adapter =
                    <CalamineAdapter as SpreadsheetReader>::open_path(std::path::Path::new(path))
                        .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("open failed: {}", e))
                    })?;
                adapter.stream_into_engine(&mut engine).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("load failed: {}", e))
                })?;
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unsupported backend: {}",
                    backend
                )));
            }
        }
        Ok(PyEngine {
            inner: Arc::new(RwLock::new(engine)),
            workbook: None,
        })
    }

    /// Set a single cell value after load.
    pub fn set_value(
        &self,
        sheet: &str,
        row: u32,
        col: u32,
        value: PyLiteralValue,
    ) -> PyResult<()> {
        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        engine
            .set_cell_value(sheet, row, col, value.inner)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("set_value: {e}"))
            })?;
        Ok(())
    }

    /// Set a single cell formula after load.
    pub fn set_formula(&self, sheet: &str, row: u32, col: u32, formula: &str) -> PyResult<()> {
        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        // Use single-shot parse with volatility classification
        let ast =
            formualizer_core::parser::parse_with_volatility_classifier(formula, |name: &str| {
                formualizer_eval::function_registry::get("", name)
                    .map(|f| {
                        f.caps()
                            .contains(formualizer_eval::function::FnCaps::VOLATILE)
                    })
                    .unwrap_or(false)
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        engine.set_cell_formula(sheet, row, col, ast).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("set_formula: {e}"))
        })?;
        Ok(())
    }

    /// Get a single cell (value + formula string if present via AST pretty-print).
    pub fn get_cell_after_load(&self, sheet: &str, row: u32, col: u32) -> PyResult<PyCell> {
        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let (ast, value) = engine.get_cell(sheet, row, col).unwrap_or((None, None));
        let value = PyLiteralValue {
            inner: value.unwrap_or(formualizer_common::LiteralValue::Empty),
        };
        let formula = ast.map(|a| a.to_string());
        Ok(PyCell { value, formula })
    }

    /// Set or change the workbook
    pub fn set_workbook(&mut self, workbook: PyWorkbook) -> PyResult<()> {
        // Clear existing data and load new workbook
        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;

        // Clear existing sheets
        // Note: There's no clear method in the engine, so we'd need to create a new one
        // For now, we'll just load the new data on top

        load_workbook_into_engine(&workbook, &mut engine)?;
        self.workbook = Some(workbook);
        Ok(())
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
    pub fn evaluate_cell(&self, sheet: &str, row: u32, col: u32) -> PyResult<PyLiteralValue> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
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

        Ok(PyLiteralValue {
            inner: value.unwrap_or(formualizer_common::LiteralValue::Empty),
        })
    }

    /// Get an evaluated cell (value + formula)
    pub fn get_cell(&self, sheet: &str, row: u32, col: u32) -> PyResult<PyCell> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        // Evaluate the cell
        let value = self.evaluate_cell(sheet, row, col)?;

        // Get the formula if it exists from the workbook
        let formula = if let Some(ref wb) = self.workbook {
            let cell_data = wb.get_cell_data(sheet, row, col)?;
            cell_data.and_then(|d| d.formula)
        } else {
            None
        };

        Ok(PyCell { value, formula })
    }

    /// Evaluate multiple cells and return their values in the same order
    pub fn evaluate_cells(
        &self,
        _py: Python,
        targets: Vec<(String, u32, u32)>,
    ) -> PyResult<Vec<PyLiteralValue>> {
        // Validate that all are 1-based
        for (_, row, col) in &targets {
            if *row == 0 || *col == 0 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Row/col are 1-based",
                ));
            }
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
            .map(|v| PyLiteralValue {
                inner: v.unwrap_or(formualizer_common::LiteralValue::Empty),
            })
            .collect())
    }

    fn __repr__(&self) -> String {
        let has_workbook = self.workbook.is_some();
        format!("Engine(has_workbook={})", has_workbook)
    }
}

/// Register the engine module with Python
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEvaluationConfig>()?;
    m.add_class::<PyEvaluationResult>()?;
    m.add_class::<PyEngine>()?;
    Ok(())
}
