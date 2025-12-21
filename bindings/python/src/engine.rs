use formualizer::eval::engine::{DateSystem, EvalConfig};
use pyo3::prelude::*;

/// Configuration for workbook-backed evaluation.
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
