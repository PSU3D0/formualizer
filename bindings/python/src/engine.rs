use formualizer::eval::engine::{DateSystem, EvalConfig, FormulaPlaneMode};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// Configuration for workbook-backed evaluation.
///
/// You typically pass this via `WorkbookConfig(eval_config=...)`.
///
/// Example:
/// ```python
///     import formualizer as fz
///
///     eval_cfg = fz.EvaluationConfig()
///     eval_cfg.enable_parallel = True
///
///     wb = fz.Workbook(config=fz.WorkbookConfig(eval_config=eval_cfg))
/// ```
#[gen_stub_pyclass]
#[pyclass(name = "EvaluationConfig", module = "formualizer")]
#[derive(Clone)]
pub struct PyEvaluationConfig {
    pub(crate) inner: EvalConfig,
}

impl Default for PyEvaluationConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn apply_binding_eval_defaults(config: &mut EvalConfig) {
    if cfg!(target_os = "emscripten") {
        config.enable_parallel = false;
    }
}

pub(crate) fn binding_default_eval_config() -> EvalConfig {
    let mut config = EvalConfig::default();
    apply_binding_eval_defaults(&mut config);
    config
}

pub(crate) fn merge_python_eval_config(base: &mut EvalConfig, python_config: &EvalConfig) {
    base.enable_parallel = python_config.enable_parallel;
    base.max_threads = python_config.max_threads;
    base.range_expansion_limit = python_config.range_expansion_limit;
    base.workbook_seed = python_config.workbook_seed;
    base.case_sensitive_names = python_config.case_sensitive_names;
    base.case_sensitive_tables = python_config.case_sensitive_tables;
    base.warmup = python_config.warmup.clone();
    base.date_system = python_config.date_system;
    base.formula_plane_mode = python_config.formula_plane_mode;
}

#[gen_stub_pymethods]
#[pymethods]
impl PyEvaluationConfig {
    /// Create a new evaluation configuration
    #[new]
    pub fn new() -> Self {
        PyEvaluationConfig {
            inner: binding_default_eval_config(),
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

    /// Enable case-sensitive defined-name resolution.
    #[setter]
    pub fn set_case_sensitive_names(&mut self, value: bool) {
        self.inner.case_sensitive_names = value;
    }

    #[getter]
    pub fn get_case_sensitive_names(&self) -> bool {
        self.inner.case_sensitive_names
    }

    /// Enable case-sensitive table-name resolution.
    #[setter]
    pub fn set_case_sensitive_tables(&mut self, value: bool) {
        self.inner.case_sensitive_tables = value;
    }

    #[getter]
    pub fn get_case_sensitive_tables(&self) -> bool {
        self.inner.case_sensitive_tables
    }

    /// Opt in to experimental FormulaPlane span evaluation.
    ///
    /// Disabled by default. When enabled, copied formula spans may be evaluated
    /// by the experimental FormulaPlane runtime instead of materialized as
    /// per-cell graph formulas.
    #[setter]
    pub fn set_span_evaluation(&mut self, value: bool) {
        self.inner.formula_plane_mode = if value {
            FormulaPlaneMode::AuthoritativeExperimental
        } else {
            FormulaPlaneMode::Off
        };
    }

    #[getter]
    pub fn get_span_evaluation(&self) -> bool {
        self.inner.formula_plane_mode == FormulaPlaneMode::AuthoritativeExperimental
    }

    fn __repr__(&self) -> String {
        format!(
            "EvaluationConfig(parallel={parallel}, max_threads={max_threads:?}, range_limit={range_limit}, seed={seed}, span_evaluation={span_evaluation})",
            parallel = self.inner.enable_parallel,
            max_threads = self.inner.max_threads,
            range_limit = self.inner.range_expansion_limit,
            seed = self.inner.workbook_seed,
            span_evaluation =
                self.inner.formula_plane_mode == FormulaPlaneMode::AuthoritativeExperimental
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
#[gen_stub_pyclass]
#[pyclass(name = "LayerInfo", module = "formualizer")]
#[derive(Clone)]
pub struct PyLayerInfo {
    #[pyo3(get)]
    pub vertex_count: usize,
    #[pyo3(get)]
    pub parallel_eligible: bool,
    #[pyo3(get)]
    pub sample_cells: Vec<String>,
}

#[gen_stub_pymethods]
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
#[gen_stub_pyclass]
#[pyclass(name = "EvaluationPlan", module = "formualizer")]
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

#[gen_stub_pymethods]
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
}

pub(crate) fn eval_plan_to_py(plan: formualizer::eval::engine::eval::EvalPlan) -> PyEvaluationPlan {
    let py_layers: Vec<PyLayerInfo> = plan
        .layers
        .into_iter()
        .map(|layer| PyLayerInfo {
            vertex_count: layer.vertex_count,
            parallel_eligible: layer.parallel_eligible,
            sample_cells: layer.sample_cells,
        })
        .collect();

    PyEvaluationPlan {
        total_vertices_to_evaluate: plan.total_vertices_to_evaluate,
        layers: py_layers,
        cycles_detected: plan.cycles_detected,
        dirty_count: plan.dirty_count,
        volatile_count: plan.volatile_count,
        parallel_enabled: plan.parallel_enabled,
        estimated_parallel_layers: plan.estimated_parallel_layers,
        target_cells: plan.target_cells,
    }
}

/// Register the evaluation config + plan types with Python
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEvaluationConfig>()?;
    m.add_class::<PyLayerInfo>()?;
    m.add_class::<PyEvaluationPlan>()?;
    Ok(())
}
