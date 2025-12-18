use formualizer_eval::engine::{DateSystem, Engine as RustEngine, EvalConfig};
use pyo3::prelude::*;
use std::sync::{Arc, RwLock};

use crate::errors::excel_eval_pyerr;
use crate::resolver::PyResolver;
use crate::value::{literal_to_py, py_to_literal};
use crate::workbook::{CellData, PyCell, PyWorkbook};

type PyObject = pyo3::Py<pyo3::PyAny>;

/// Python wrapper for the evaluation engine
#[pyclass(name = "Engine")]
pub struct PyEngine {
    inner: Arc<RwLock<RustEngine<PyResolver>>>,
    workbook: Option<pyo3::Py<PyWorkbook>>,
}

/// Configuration for the evaluation engine
#[pyclass(name = "EvaluationConfig")]
#[derive(Clone)]
pub struct PyEvaluationConfig {
    pub(crate) inner: EvalConfig,
}

impl Default for PyEvaluationConfig {
    fn default() -> Self {
        Self::new()
    }
}

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
            "EvaluationConfig(parallel={parallel}, max_threads={max_threads:?}, range_limit={range_limit}, seed={seed})",
            parallel = self.inner.enable_parallel,
            max_threads = self.inner.max_threads,
            range_limit = self.inner.range_expansion_limit,
            seed = self.inner.workbook_seed
        )
    }

    // ----- Warmup (global pass planning) configuration -----

    /// Enable or disable global warmup (pre-build flats/masks/indexes before evaluation)
    #[setter]
    pub fn set_warmup_enabled(&mut self, value: bool) {
        self.inner.warmup.warmup_enabled = value;
    }

    #[getter]
    pub fn get_warmup_enabled(&self) -> bool {
        self.inner.warmup.warmup_enabled
    }

    /// Warmup time budget in milliseconds per evaluation invocation
    #[setter]
    pub fn set_warmup_time_budget_ms(&mut self, value: u64) {
        self.inner.warmup.warmup_time_budget_ms = value;
    }

    #[getter]
    pub fn get_warmup_time_budget_ms(&self) -> u64 {
        self.inner.warmup.warmup_time_budget_ms
    }

    /// Maximum parallelism for warmup building
    #[setter]
    pub fn set_warmup_parallelism_cap(&mut self, value: u32) {
        self.inner.warmup.warmup_parallelism_cap = value as usize;
    }

    #[getter]
    pub fn get_warmup_parallelism_cap(&self) -> u32 {
        self.inner.warmup.warmup_parallelism_cap as u32
    }

    /// Maximum top-K references to consider for flattening during warmup
    #[setter]
    pub fn set_warmup_topk_refs(&mut self, value: u32) {
        self.inner.warmup.warmup_topk_refs = value as usize;
    }

    #[getter]
    pub fn get_warmup_topk_refs(&self) -> u32 {
        self.inner.warmup.warmup_topk_refs as u32
    }

    /// Minimum number of cells in a range to consider flattening during warmup
    #[setter]
    pub fn set_min_flat_cells(&mut self, value: u32) {
        self.inner.warmup.min_flat_cells = value as usize;
    }

    #[getter]
    pub fn get_min_flat_cells(&self) -> u32 {
        self.inner.warmup.min_flat_cells as u32
    }

    /// Memory budget (MB) for pass-scoped flat cache during warmup
    #[setter]
    pub fn set_flat_cache_mb_cap(&mut self, value: u32) {
        self.inner.warmup.flat_cache_mb_cap = value as usize;
    }

    #[getter]
    pub fn get_flat_cache_mb_cap(&self) -> u32 {
        self.inner.warmup.flat_cache_mb_cap as u32
    }

    #[getter]
    pub fn get_date_system(&self) -> String {
        self.inner.date_system.to_string()
    }

    #[setter]
    pub fn set_date_system(&mut self, value: String) -> PyResult<()> {
        let date_system: DateSystem = match value.as_str() {
            "1900" => DateSystem::Excel1900,
            "1904" => DateSystem::Excel1904,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid date system: {value}. Use '1900' or '1904'."
                )));
            }
        };
        self.inner.date_system = date_system;
        Ok(())
    }
}

/// Information about a single evaluation layer
#[pyclass(name = "LayerInfo")]
#[derive(Clone)]
pub struct PyLayerInfo {
    #[pyo3(get)]
    pub vertex_count: usize,
    #[pyo3(get)]
    pub parallel_eligible: bool,
    #[pyo3(get)]
    pub sample_cells: Vec<String>,
}

#[pymethods]
impl PyLayerInfo {
    fn __repr__(&self) -> String {
        format!(
            "LayerInfo(vertices={}, parallel={}, samples={:?})",
            self.vertex_count, self.parallel_eligible, self.sample_cells
        )
    }
}

/// Evaluation plan showing how cells would be evaluated
#[pyclass(name = "EvaluationPlan")]
pub struct PyEvaluationPlan {
    #[pyo3(get)]
    pub total_vertices_to_evaluate: usize,
    #[pyo3(get)]
    pub layers: Vec<PyLayerInfo>,
    #[pyo3(get)]
    pub cycles_detected: usize,
    #[pyo3(get)]
    pub dirty_count: usize,
    #[pyo3(get)]
    pub volatile_count: usize,
    #[pyo3(get)]
    pub parallel_enabled: bool,
    #[pyo3(get)]
    pub estimated_parallel_layers: usize,
    #[pyo3(get)]
    pub target_cells: Vec<String>,
}

#[pymethods]
impl PyEvaluationPlan {
    fn __repr__(&self) -> String {
        format!(
            "EvaluationPlan(vertices={}, layers={}, parallel_layers={}, cycles={}, targets={})",
            self.total_vertices_to_evaluate,
            self.layers.len(),
            self.estimated_parallel_layers,
            self.cycles_detected,
            self.target_cells.len()
        )
    }

    fn __str__(&self) -> String {
        let mut s = format!(
            "Evaluation Plan for {} target(s):\n",
            self.target_cells.len()
        );
        s.push_str(&format!(
            "  Total vertices to evaluate: {}\n",
            self.total_vertices_to_evaluate
        ));
        s.push_str(&format!("  Dirty vertices: {}\n", self.dirty_count));
        s.push_str(&format!("  Volatile vertices: {}\n", self.volatile_count));
        s.push_str(&format!("  Cycles detected: {}\n", self.cycles_detected));
        s.push_str(&format!(
            "  Parallel evaluation: {}\n",
            if self.parallel_enabled {
                "enabled"
            } else {
                "disabled"
            }
        ));
        s.push_str(&format!(
            "  Layers: {} (parallel: {})\n",
            self.layers.len(),
            self.estimated_parallel_layers
        ));

        for (i, layer) in self.layers.iter().enumerate() {
            s.push_str(&format!(
                "    Layer {}: {} vertices{}\n",
                i + 1,
                layer.vertex_count,
                if layer.parallel_eligible {
                    " (parallel)"
                } else {
                    ""
                }
            ));
            if !layer.sample_cells.is_empty() {
                s.push_str(&format!(
                    "      Samples: {}\n",
                    layer.sample_cells.join(", ")
                ));
            }
        }
        s
    }
}

/// Result from an evaluation pass
#[pyclass(name = "EvaluationResult")]
pub struct PyEvaluationResult {
    pub(crate) computed_vertices: u64,
    pub(crate) cycle_errors: u32,
    pub(crate) elapsed_ms: u64,
}

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
    // Use bulk ingest builder to avoid per-cell graph mutations
    let mut arrow_builder = engine.begin_bulk_ingest_arrow();

    // Batch parser with volatility classifier so ASTs carry contains_volatile
    let mut parser = formualizer_parse::parser::BatchParser::builder()
        .with_volatility_classifier(|name: &str| {
            formualizer_eval::function_registry::get("", name)
                .map(|f| {
                    f.caps()
                        .contains(formualizer_eval::function::FnCaps::VOLATILE)
                })
                .unwrap_or(false)
        })
        .build();

    // Snapshot sheet names then ingest from the engine-backed workbook
    let (names, dims): (Vec<String>, std::collections::HashMap<String, (u32, u32)>) = {
        let names = workbook.sheet_names_snapshot()?;
        let mut dims = std::collections::HashMap::new();
        for n in &names {
            if let Some((rows, cols)) = workbook.sheet_dimensions(n)? {
                dims.insert(n.clone(), (rows, cols));
            }
        }
        (names, dims)
    };

    // Add sheets and stage base values via Arrow ingest first.
    // This ensures SheetIds exist before BulkIngestBuilder is used for formulas.
    for sheet_name in &names {
        if let Some((rows, cols)) = dims.get(sheet_name) {
            let ncols = *cols as usize;
            let chunk_rows = 1024usize;
            arrow_builder.add_sheet(sheet_name, ncols, chunk_rows);

            for r in 1..=*rows {
                let mut row_vals = vec![formualizer_common::LiteralValue::Empty; ncols];
                for c in 1..=*cols {
                    if let Some(v) = workbook.get_value_inner(sheet_name, r, c)? {
                        row_vals[(c - 1) as usize] = v;
                    }
                }
                arrow_builder
                    .append_row(sheet_name, &row_vals)
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                    })?;
            }
        }
    }

    arrow_builder
        .finish()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let mut builder = engine.begin_bulk_ingest();

    // Now stage formulas.
    for sheet_name in &names {
        if let Some((rows, cols)) = dims.get(sheet_name) {
            let sid = builder.add_sheet(sheet_name);
            let mut staged_asts: Vec<(u32, u32, formualizer_parse::ASTNode)> = Vec::new();
            for r in 1..=*rows {
                for c in 1..=*cols {
                    if let Some(formula) = workbook.get_formula_inner(sheet_name, r, c)? {
                        let ast = parser.parse(&formula).map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
                        })?;
                        staged_asts.push((r, c, ast));
                    }
                }
            }
            if !staged_asts.is_empty() {
                builder.add_formulas(sid, staged_asts);
            }
        }
    }

    builder.finish().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("bulk finish: {e}"))
    })?;
    let _ = engine.evaluate_all();
    Ok(())
}

#[pymethods]
impl PyEngine {
    /// Create a new evaluation engine
    /// Can be constructed as Engine() or Engine(workbook, config=None)
    #[new]
    #[pyo3(signature = (workbook=None, config=None))]
    pub fn new(
        workbook: Option<pyo3::Py<PyWorkbook>>,
        config: Option<PyEvaluationConfig>,
    ) -> PyResult<Self> {
        let eval_config = config.map(|c| c.inner).unwrap_or_default();
        let mut engine = RustEngine::new(PyResolver, eval_config);

        let wb_py = if let Some(wb) = workbook {
            // Load the workbook data into the engine
            Python::attach(|py| {
                let wb_ref = wb.borrow(py);
                load_workbook_into_engine(&wb_ref, &mut engine)
            })?;
            Some(wb)
        } else {
            None
        };

        Ok(PyEngine {
            inner: Arc::new(RwLock::new(engine)),
            workbook: wb_py,
        })
    }

    /// Create an engine from a workbook
    #[classmethod]
    #[pyo3(signature = (workbook, config=None))]
    pub fn from_workbook(
        _cls: &Bound<'_, pyo3::types::PyType>,
        workbook: pyo3::Py<PyWorkbook>,
        config: Option<PyEvaluationConfig>,
    ) -> PyResult<Self> {
        // Initialize tracing subscriber if requested via env (no-op when disabled)
        let _ = formualizer_eval::telemetry::init_tracing_from_env();
        let eval_config = config.map(|c| c.inner).unwrap_or_default();
        let mut engine = RustEngine::new(PyResolver, eval_config);

        // Load the workbook data into the engine
        Python::attach(|py| {
            let wb_ref = workbook.borrow(py);
            load_workbook_into_engine(&wb_ref, &mut engine)
        })?;

        Ok(PyEngine {
            inner: Arc::new(RwLock::new(engine)),
            workbook: Some(workbook),
        })
    }

    #[getter]
    pub fn config(&self) -> PyResult<PyEvaluationConfig> {
        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        Ok(PyEvaluationConfig {
            inner: engine.config.clone(),
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
        // Initialize tracing subscriber if requested via env (no-op when disabled)
        let _ = formualizer_eval::telemetry::init_tracing_from_env();
        let eval_config = config.map(|c| c.inner).unwrap_or_default();
        let mut engine = RustEngine::new(PyResolver, eval_config);
        let backend = backend.unwrap_or("calamine");
        match backend {
            "calamine" => {
                use formualizer_eval::engine::ingest::EngineLoadStream;
                use formualizer_workbook::backends::CalamineAdapter;
                use formualizer_workbook::traits::SpreadsheetReader;
                let mut adapter =
                    <CalamineAdapter as SpreadsheetReader>::open_path(std::path::Path::new(path))
                        .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("open failed: {e}"))
                    })?;
                adapter.stream_into_engine(&mut engine).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("load failed: {e}"))
                })?;
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unsupported backend: {backend}"
                )));
            }
        }
        Ok(PyEngine {
            inner: Arc::new(RwLock::new(engine)),
            workbook: None,
        })
    }

    /// Set a single cell value after load (clears any formula).
    pub fn set_value(
        &self,
        py: Python<'_>,
        sheet: &str,
        row: u32,
        col: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let literal = py_to_literal(value)?;
        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        engine
            .set_cell_value(sheet, row, col, literal.clone())
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("set_value: {e}"))
            })?;

        if let Some(ref wb) = self.workbook {
            let wb_ref = wb.borrow(py);
            wb_ref.set_cell_data(
                sheet,
                row,
                col,
                CellData {
                    value: Some(literal),
                    formula: None,
                },
            )?;
        }

        Ok(())
    }

    /// Set a single cell formula after load.
    /// Formula must start with '=' sign.
    pub fn set_formula(&self, sheet: &str, row: u32, col: u32, formula: &str) -> PyResult<()> {
        // Validate formula starts with '='
        if !formula.starts_with('=') {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Formula must start with '=' sign",
            ));
        }

        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        // Use single-shot parse with volatility classification
        let ast =
            formualizer_parse::parser::parse_with_volatility_classifier(formula, |name: &str| {
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

        // Update workbook if present to keep it in sync
        if let Some(ref wb) = self.workbook {
            Python::attach(|py| {
                let wb_ref = wb.borrow(py);
                wb_ref.set_cell_data(
                    sheet,
                    row,
                    col,
                    CellData {
                        value: None,
                        formula: Some(formula.to_string()),
                    },
                )
            })?;
        }

        Ok(())
    }

    /// Get a single cell (value + formula string if present via AST pretty-print).
    pub fn get_cell_after_load(&self, sheet: &str, row: u32, col: u32) -> PyResult<PyCell> {
        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let (ast, value) = engine.get_cell(sheet, row, col).unwrap_or((None, None));
        let value = value.unwrap_or(formualizer_common::LiteralValue::Empty);
        let formula = ast.map(|a| formualizer_parse::pretty::canonical_formula(&a));
        Ok(PyCell::new(value, formula))
    }

    /// Set or change the workbook
    pub fn set_workbook(&mut self, workbook: pyo3::Py<PyWorkbook>) -> PyResult<()> {
        // Clear existing data and load new workbook
        let mut engine = self.inner.write().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;

        // Clear existing sheets
        // Note: There's no clear method in the engine, so we'd need to create a new one
        // For now, we'll just load the new data on top

        Python::attach(|py| {
            let wb_ref = workbook.borrow(py);
            load_workbook_into_engine(&wb_ref, &mut engine)
        })?;
        self.workbook = Some(workbook);
        Ok(())
    }

    /// Evaluate all cells in the workbook
    pub fn evaluate_all(&self, py: Python) -> PyResult<PyEvaluationResult> {
        // Drop GIL while Rust runs parallel work
        py.detach(|| {
            let mut engine = self.inner.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to acquire engine lock: {e}"
                ))
            })?;

            let result = engine.evaluate_all().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Evaluation failed: {e:?}"
                ))
            })?;
            Ok(PyEvaluationResult {
                computed_vertices: result.computed_vertices as u64,
                cycle_errors: result.cycle_errors as u32,
                elapsed_ms: result.elapsed.as_millis() as u64,
            })
        })
    }

    /// Evaluate a specific cell and return its value
    pub fn evaluate_cell(
        &self,
        py: Python<'_>,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> PyResult<PyObject> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        if let Some(ref wb) = self.workbook {
            let w = wb.borrow(py);
            return w.evaluate_cell(py, sheet, row, col);
        }

        let value = py.detach(|| {
            let mut engine = self.inner.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to acquire engine lock: {e}"
                ))
            })?;
            engine
                .evaluate_cell(sheet, row, col)
                .map_err(|e| excel_eval_pyerr(Some(sheet), Some(row), Some(col), &e))
        })?;

        let literal = value.unwrap_or(formualizer_common::LiteralValue::Empty);
        literal_to_py(py, &literal)
    }

    /// Evaluate a cell and return a native Python value; raise if the result is an Excel error.
    ///
    /// Returns:
    /// - int/float/bool/str for scalar values
    /// - None for empty
    /// - list[list[Any]] for arrays (nested lists)
    #[pyo3(name = "evaluate_cell_value")]
    pub fn evaluate_cell_value_py(
        &self,
        py: Python,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> PyResult<PyObject> {
        self.evaluate_cell(py, sheet, row, col)
    }

    /// Get a cell without evaluation (value from last evaluation + formula)
    pub fn get_cell(&self, sheet: &str, row: u32, col: u32) -> PyResult<PyCell> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;

        // Get formula and value from engine (without evaluation)
        let (ast, value) = engine.get_cell(sheet, row, col).unwrap_or((None, None));
        let value = value.unwrap_or(formualizer_common::LiteralValue::Empty);
        let formula = ast.map(|a| formualizer_parse::pretty::canonical_formula(&a));

        Ok(PyCell::new(value, formula))
    }

    /// Get only the formula for a cell (without evaluation)
    pub fn get_formula(&self, sheet: &str, row: u32, col: u32) -> PyResult<Option<String>> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;

        let (ast, _) = engine.get_cell(sheet, row, col).unwrap_or((None, None));
        Ok(ast.map(|a| formualizer_parse::pretty::canonical_formula(&a)))
    }

    /// Get only the value for a cell (without evaluation, returns last computed value)
    pub fn get_value(&self, py: Python<'_>, sheet: &str, row: u32, col: u32) -> PyResult<PyObject> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;

        let (_, value) = engine.get_cell(sheet, row, col).unwrap_or((None, None));
        let literal = value.unwrap_or(formualizer_common::LiteralValue::Empty);
        literal_to_py(py, &literal)
    }

    /// Get an evaluated cell (triggers evaluation + formula)
    pub fn get_cell_evaluated(
        &self,
        py: Python,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> PyResult<PyCell> {
        if row == 0 || col == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Row/col are 1-based",
            ));
        }

        // Evaluate the cell value directly
        let literal = py.detach(|| {
            let mut engine = self.inner.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to acquire engine lock: {e}"
                ))
            })?;
            engine
                .evaluate_cell(sheet, row, col)
                .map_err(|e| excel_eval_pyerr(Some(sheet), Some(row), Some(col), &e))
        })?;

        let literal = literal.unwrap_or(formualizer_common::LiteralValue::Empty);

        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock error: {e}"))
        })?;
        let (ast, _) = engine.get_cell(sheet, row, col).unwrap_or((None, None));
        let formula = ast.map(|a| formualizer_parse::pretty::canonical_formula(&a));

        Ok(PyCell::new(literal, formula))
    }

    /// Evaluate multiple cells and return their values in the same order
    pub fn evaluate_cells(
        &self,
        py: Python,
        targets: Vec<(String, u32, u32)>,
    ) -> PyResult<Vec<PyObject>> {
        for (_, row, col) in &targets {
            if *row == 0 || *col == 0 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Row/col are 1-based",
                ));
            }
        }

        let values = py.detach(|| {
            let mut engine = self.inner.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to acquire engine lock: {e}"
                ))
            })?;
            let target_refs: Vec<(&str, u32, u32)> = targets
                .iter()
                .map(|(s, r, c)| (s.as_str(), *r, *c))
                .collect();
            engine
                .evaluate_cells(&target_refs)
                .map_err(|e| excel_eval_pyerr(None, None, None, &e))
        })?;

        values
            .into_iter()
            .map(|v| {
                let literal = v.unwrap_or(formualizer_common::LiteralValue::Empty);
                literal_to_py(py, &literal)
            })
            .collect()
    }

    /// Get the evaluation plan for cells without actually evaluating them
    pub fn get_eval_plan(
        &self,
        _py: Python,
        targets: Vec<(String, u32, u32)>,
    ) -> PyResult<PyEvaluationPlan> {
        // Validate that all are 1-based
        for (_, row, col) in &targets {
            if *row == 0 || *col == 0 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Row/col are 1-based",
                ));
            }
        }

        let engine = self.inner.read().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire engine lock: {e}"
            ))
        })?;

        // Convert targets to the format expected by get_eval_plan
        let target_refs: Vec<(&str, u32, u32)> = targets
            .iter()
            .map(|(s, r, c)| (s.as_str(), *r, *c))
            .collect();

        let plan = engine
            .get_eval_plan(&target_refs)
            .map_err(|e| excel_eval_pyerr(None, None, None, &e))?;

        // Convert Rust plan to Python plan
        let py_layers: Vec<PyLayerInfo> = plan
            .layers
            .into_iter()
            .map(|layer| PyLayerInfo {
                vertex_count: layer.vertex_count,
                parallel_eligible: layer.parallel_eligible,
                sample_cells: layer.sample_cells,
            })
            .collect();

        Ok(PyEvaluationPlan {
            total_vertices_to_evaluate: plan.total_vertices_to_evaluate,
            layers: py_layers,
            cycles_detected: plan.cycles_detected,
            dirty_count: plan.dirty_count,
            volatile_count: plan.volatile_count,
            parallel_enabled: plan.parallel_enabled,
            estimated_parallel_layers: plan.estimated_parallel_layers,
            target_cells: plan.target_cells,
        })
    }

    fn __repr__(&self) -> String {
        let has_workbook = self.workbook.is_some();
        format!("Engine(has_workbook={has_workbook})")
    }
}

/// Register the engine module with Python
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEvaluationConfig>()?;
    m.add_class::<PyEvaluationResult>()?;
    m.add_class::<PyLayerInfo>()?;
    m.add_class::<PyEvaluationPlan>()?;
    m.add_class::<PyEngine>()?;
    Ok(())
}
