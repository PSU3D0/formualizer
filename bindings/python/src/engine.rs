use formualizer::eval::engine::{
    CycleConfig, CycleDetection, CyclePolicy, DateSystem, EvalConfig, FormulaPlaneMode,
};
use pyo3::prelude::*;
#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::time::Duration;

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
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "EvaluationConfig",
    module = "formualizer.formualizer_py",
    from_py_object
)]
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
    base.evaluation_budgets = python_config.evaluation_budgets.clone();
    base.cycle = python_config.cycle;
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
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

    /// Opt in to experimental FormulaPlane span evaluation. Disabled by default.
    /// When enabled, copied formula spans may be evaluated
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

    /// Maximum evaluation work units for one outer request.
    #[setter]
    pub fn set_max_work_units(&mut self, value: Option<u64>) {
        self.inner.evaluation_budgets.work.max_work_units = value;
    }

    #[getter]
    pub fn get_max_work_units(&self) -> Option<u64> {
        self.inner.evaluation_budgets.work.max_work_units
    }

    /// Maximum elapsed evaluation time in milliseconds for one outer request.
    #[setter]
    pub fn set_max_eval_time_ms(&mut self, value: Option<u64>) {
        self.inner.evaluation_budgets.deadline.max_elapsed = value.map(Duration::from_millis);
    }

    #[getter]
    pub fn get_max_eval_time_ms(&self) -> Option<u64> {
        self.inner
            .evaluation_budgets
            .deadline
            .max_elapsed
            .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
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

    // ----- Cycle / iterative-calculation configuration (RFC #113, spec §2/§9) -----

    /// Cycle detection mode: `"static"` (every static SCC is stamped `#CIRC`,
    /// today's compat behavior) or `"runtime"` (only live cycles get the policy
    /// verdict; required for iterative calculation).
    #[getter]
    pub fn get_cycle_detection(&self) -> String {
        match self.inner.cycle.detection {
            CycleDetection::Static => "static".to_string(),
            CycleDetection::Runtime => "runtime".to_string(),
        }
    }

    #[setter]
    pub fn set_cycle_detection(&mut self, value: String) -> PyResult<()> {
        let detection = match value.as_str() {
            "static" => CycleDetection::Static,
            "runtime" => CycleDetection::Runtime,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid cycle_detection: {value}. Use 'static' or 'runtime'."
                )));
            }
        };
        self.set_cycle(CycleConfig {
            detection,
            ..self.inner.cycle
        })
    }

    /// Cycle policy for witnessed (live) cycles under runtime detection:
    /// `"error"` (stamp `#CIRC`) or `"iterate"` (Excel-style iterative
    /// calculation). Iterative calculation requires runtime detection (spec
    /// §2), so setting `"iterate"` also promotes `cycle_detection` to
    /// `"runtime"` (mirroring the engine's `CycleConfig::iterate` helper and the
    /// XLSX `calcPr` load mapping).
    #[getter]
    pub fn get_cycle_policy(&self) -> String {
        match self.inner.cycle.policy {
            CyclePolicy::Error => "error".to_string(),
            CyclePolicy::Iterate { .. } => "iterate".to_string(),
        }
    }

    #[setter]
    pub fn set_cycle_policy(&mut self, value: String) -> PyResult<()> {
        match value.as_str() {
            "error" => self.set_cycle(CycleConfig {
                policy: CyclePolicy::Error,
                ..self.inner.cycle
            }),
            "iterate" => {
                // Preserve existing knobs if already iterating, else Excel defaults.
                let policy = match self.inner.cycle.policy {
                    CyclePolicy::Iterate { .. } => self.inner.cycle.policy,
                    CyclePolicy::Error => CyclePolicy::iterate_excel_defaults(),
                };
                self.enable_iterate(policy)
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid cycle_policy: {value}. Use 'error' or 'iterate'."
            ))),
        }
    }

    /// Maximum iterative-calculation passes per SCC per recalc (Excel default
    /// 100; `1` is the accumulator pattern). Only meaningful when
    /// `cycle_policy = "iterate"`; reads `100` otherwise.
    #[getter]
    pub fn get_iterate_max_iterations(&self) -> u32 {
        match self.inner.cycle.policy {
            CyclePolicy::Iterate { max_iterations, .. } => max_iterations,
            CyclePolicy::Error => CyclePolicy::EXCEL_DEFAULT_MAX_ITERATIONS,
        }
    }

    #[setter]
    pub fn set_iterate_max_iterations(&mut self, value: u32) -> PyResult<()> {
        let max_change = match self.inner.cycle.policy {
            CyclePolicy::Iterate { max_change, .. } => max_change,
            CyclePolicy::Error => CyclePolicy::EXCEL_DEFAULT_MAX_CHANGE,
        };
        self.enable_iterate(CyclePolicy::Iterate {
            max_iterations: value,
            max_change,
        })
    }

    /// Absolute per-member convergence threshold (`|Δ| < max_change`, Excel
    /// default 0.001). Only meaningful when `cycle_policy = "iterate"`; reads
    /// `0.001` otherwise.
    #[getter]
    pub fn get_iterate_max_change(&self) -> f64 {
        match self.inner.cycle.policy {
            CyclePolicy::Iterate { max_change, .. } => max_change,
            CyclePolicy::Error => CyclePolicy::EXCEL_DEFAULT_MAX_CHANGE,
        }
    }

    #[setter]
    pub fn set_iterate_max_change(&mut self, value: f64) -> PyResult<()> {
        let max_iterations = match self.inner.cycle.policy {
            CyclePolicy::Iterate { max_iterations, .. } => max_iterations,
            CyclePolicy::Error => CyclePolicy::EXCEL_DEFAULT_MAX_ITERATIONS,
        };
        self.enable_iterate(CyclePolicy::Iterate {
            max_iterations,
            max_change: value,
        })
    }
}

impl PyEvaluationConfig {
    /// Validate then store a cycle config, surfacing spec-§2 rejection as a
    /// Python `ValueError` rather than the engine's build-time panic.
    fn set_cycle(&mut self, cycle: CycleConfig) -> PyResult<()> {
        cycle
            .validate()
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        self.inner.cycle = cycle;
        Ok(())
    }

    /// Apply an `Iterate` policy, auto-promoting detection to `Runtime` (spec
    /// §2: iteration requires runtime detection), then validate the knobs.
    fn enable_iterate(&mut self, policy: CyclePolicy) -> PyResult<()> {
        self.set_cycle(CycleConfig {
            detection: CycleDetection::Runtime,
            policy,
        })
    }
}

/// Information about a single evaluation layer
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "LayerInfo",
    module = "formualizer.formualizer_py",
    from_py_object
)]
#[derive(Clone)]
pub struct PyLayerInfo {
    #[pyo3(get)]
    pub vertex_count: usize,
    #[pyo3(get)]
    pub parallel_eligible: bool,
    #[pyo3(get)]
    pub sample_cells: Vec<String>,
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
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
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "EvaluationPlan", module = "formualizer.formualizer_py")]
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

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::merge_python_eval_config;

    #[test]
    fn defaults_match_engine_cycle_defaults() {
        let cfg = PyEvaluationConfig::new();
        // Engine default is detection=static, policy=error.
        assert_eq!(cfg.get_cycle_detection(), "static");
        assert_eq!(cfg.get_cycle_policy(), "error");
        // Knob getters read Excel defaults even when not iterating.
        assert_eq!(cfg.get_iterate_max_iterations(), 100);
        assert!((cfg.get_iterate_max_change() - 0.001).abs() < f64::EPSILON);
    }

    #[test]
    fn setting_iterate_policy_promotes_detection_to_runtime() {
        let mut cfg = PyEvaluationConfig::new();
        cfg.set_cycle_policy("iterate".to_string()).unwrap();
        assert_eq!(cfg.get_cycle_policy(), "iterate");
        // spec §2: iterate requires runtime; the setter auto-promotes it so the
        // resulting config validates (and would not panic at engine build).
        assert_eq!(cfg.get_cycle_detection(), "runtime");
        cfg.inner.cycle.validate().expect("config must be valid");
    }

    #[test]
    fn iterate_knobs_round_trip() {
        let mut cfg = PyEvaluationConfig::new();
        cfg.set_iterate_max_iterations(7).unwrap();
        cfg.set_iterate_max_change(0.25).unwrap();
        assert_eq!(cfg.get_cycle_policy(), "iterate");
        assert_eq!(cfg.get_cycle_detection(), "runtime");
        assert_eq!(cfg.get_iterate_max_iterations(), 7);
        assert!((cfg.get_iterate_max_change() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn invalid_detection_string_is_value_error() {
        let mut cfg = PyEvaluationConfig::new();
        assert!(cfg.set_cycle_detection("bogus".to_string()).is_err());
    }

    #[test]
    fn invalid_iterate_knobs_are_rejected() {
        let mut cfg = PyEvaluationConfig::new();
        // max_iterations = 0 is a config error (spec §2).
        assert!(cfg.set_iterate_max_iterations(0).is_err());
        // negative max_change is a config error.
        assert!(cfg.set_iterate_max_change(-1.0).is_err());
    }

    #[test]
    fn static_with_iterate_policy_is_rejected() {
        // Force detection back to static while keeping an iterate policy: the
        // combined config must be rejected at apply time (no engine panic).
        let mut cfg = PyEvaluationConfig::new();
        cfg.set_cycle_policy("iterate".to_string()).unwrap();
        assert!(cfg.set_cycle_detection("static".to_string()).is_err());
    }

    #[test]
    fn merge_python_eval_config_carries_cycle() {
        let mut python = PyEvaluationConfig::new();
        python.set_iterate_max_iterations(42).unwrap();
        python.set_iterate_max_change(0.5).unwrap();

        let mut base = EvalConfig::default();
        merge_python_eval_config(&mut base, &python.inner);
        match base.cycle.policy {
            CyclePolicy::Iterate {
                max_iterations,
                max_change,
            } => {
                assert_eq!(max_iterations, 42);
                assert!((max_change - 0.5).abs() < f64::EPSILON);
            }
            other => panic!("expected iterate policy, got {other:?}"),
        }
        assert_eq!(base.cycle.detection, CycleDetection::Runtime);
    }
}
