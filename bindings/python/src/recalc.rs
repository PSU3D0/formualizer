//! Python lifecycle handles. Generation comparisons happen under the engine lock.
use pyo3::prelude::*;
#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use formualizer::eval::engine::{self, EvaluationStop, EvaluationTarget};
use crate::workbook::PyWorkbook;
use crate::errors::{NoActiveJobError, recalc_error_to_pyerr};
use std::sync::atomic::Ordering;

pub(crate) type Rect = (String, u32, u32, u32, u32);

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "CancelToken", module = "formualizer.formualizer_py", frozen)]
#[derive(Clone, Default)]
pub struct PyCancelToken { pub(crate) inner: engine::CancelToken }
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyCancelToken {
    #[new]
    fn new() -> Self { Self::default() }
    fn cancel(&self) { self.inner.cancel(); }
    #[getter]
    fn cancelled(&self) -> bool { self.inner.is_cancelled() }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "StepProgress", module = "formualizer.formualizer_py", frozen, get_all)]
#[derive(Clone)]
pub struct PyStepProgress {
    pub pass: u32,
    pub completed_in_pass: usize,
    pub scheduled_in_pass: usize,
    pub cumulative: usize,
    pub total_exact: bool,
    pub staged_remaining: usize,
    pub priority_pending: bool,
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "RecalcResult", module = "formualizer.formualizer_py", frozen, get_all)]
#[derive(Clone)]
pub struct PyRecalcResult {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed_ms: f64,
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "SlowUnitReport", module = "formualizer.formualizer_py", frozen, get_all)]
#[derive(Clone)]
pub struct PySlowUnitReport {
    pub kind: String,
    pub representative_cell: Option<(String, u32, u32)>,
    pub vertex_count: usize,
    pub elapsed_ms: f64,
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "StepReport", module = "formualizer.formualizer_py", frozen, get_all)]
#[derive(Clone)]
pub struct PyStepReport {
    pub phase: String,
    pub progress: PyStepProgress,
    pub step_computed: usize,
    pub step_elapsed_ms: f64,
    pub changed: Vec<Rect>,
    pub overflow_sheets: Vec<String>,
    pub longest_unit: Option<PySlowUnitReport>,
    pub result: Option<PyRecalcResult>,
}
pub(crate) fn rectangles(wb: &formualizer::workbook::Workbook, delta: &engine::TargetEvalDelta) -> Vec<Rect> {
    delta.records.iter().map(|r| {
        let (r0,c0,r1,c1) = r.bounds();
        (wb.engine().sheet_name(r.sheet_id()).to_owned(), r0+1,c0+1,r1+1,c1+1)
    }).collect()
}
impl PyStepReport {
    fn from_engine(wb: &formualizer::workbook::Workbook, r: engine::StepReport) -> Self {
        let p = r.progress;
        Self {
            phase: format!("{:?}", r.phase),
            progress: PyStepProgress { pass:p.pass, completed_in_pass:p.completed_in_pass,
                scheduled_in_pass:p.scheduled_in_pass, cumulative:p.cumulative_computed_vertices,
                total_exact:p.total_exact, staged_remaining:p.staged_formulas_remaining,
                priority_pending:p.priority_pending },
            step_computed:r.step_computed_vertices,
            step_elapsed_ms:r.step_elapsed.as_secs_f64()*1000.0,
            changed:rectangles(wb, &r.changed.delta),
            overflow_sheets:r.changed.overflow_sheets.iter().map(|s| wb.engine().sheet_name(*s).to_owned()).collect(),
            longest_unit:r.longest_unit.map(|u| PySlowUnitReport {
                kind:format!("{:?}",u.kind),
                representative_cell:u.representative_cell.map(|c| (wb.engine().sheet_name(c.sheet_id()).to_owned(),c.row0()+1,c.col0()+1)),
                vertex_count:u.vertex_count, elapsed_ms:u.elapsed.as_secs_f64()*1000.0 }),
            result:r.result.map(|v| PyRecalcResult {computed_vertices:v.computed_vertices,cycle_errors:v.cycle_errors,elapsed_ms:v.elapsed.as_secs_f64()*1000.0}),
        }
    }
}
pub(crate) fn targets(ranges: Vec<Rect>) -> PyResult<Vec<EvaluationTarget>> {
    ranges.into_iter().map(|(sheet,r0,c0,r1,c1)| {
        validate_rect(r0,c0,r1,c1)?;
        Ok(EvaluationTarget::Range(formualizer::common::RangeAddress { sheet, start_row:r0, start_col:c0, end_row:r1, end_col:c1 }))
    }).collect()
}
pub(crate) fn validate_rect(r0:u32,c0:u32,r1:u32,c1:u32) -> PyResult<()> {
    if r0 == 0 || c0 == 0 || r1 < r0 || c1 < c0 {
        return Err(pyo3::exceptions::PyValueError::new_err("rectangles are 1-based inclusive with ordered bounds"));
    }
    Ok(())
}
pub(crate) fn stop(budget_ms:Option<f64>, token:engine::CancelToken) -> PyResult<EvaluationStop> {
    let stop = match budget_ms {
        None => EvaluationStop::none(),
        Some(ms) => EvaluationStop::after(std::time::Duration::try_from_secs_f64(ms/1000.0)
            .map_err(|_| pyo3::exceptions::PyValueError::new_err("budget_ms must be finite and non-negative"))?),
    };
    Ok(stop.with_cancel(token))
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "RecalcJob", module = "formualizer.formualizer_py", frozen)]
#[derive(Clone)]
pub struct PyRecalcJob {
    pub(crate) workbook: PyWorkbook,
    pub(crate) generation:u64,
}
impl PyRecalcJob {
    fn ensure_current(&self, wb:&formualizer::workbook::Workbook) -> PyResult<()> {
        if self.workbook.job_generation.load(Ordering::SeqCst) != self.generation || !wb.has_active_recalc_job() {
            return Err(NoActiveJobError::new_err("this recalculation job has ended"));
        }
        Ok(())
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyRecalcJob {
    #[pyo3(signature = (budget_ms=None, *, cancel=None))]
    fn step(&self, py:Python<'_>, budget_ms:Option<f64>, cancel:Option<PyCancelToken>) -> PyResult<PyStepReport> {
        self.workbook.check_reentrancy()?;
        let token = cancel.map(|c| c.inner).unwrap_or_else(|| engine::CancelToken::from_flag(self.workbook.cancel_flag.clone()));
        let _stop = stop(budget_ms,token)?;
        py.detach(|| {
            let mut wb = self.workbook.write_inner_detached().map_err(crate::errors::workbook_error_to_pyerr)?;
            self.ensure_current(&wb)?;
            // Compiling behavioral-red skeleton: deliberately ignores the soft budget/token.
            let report = wb.step_recalc(EvaluationStop::none()).map_err(recalc_error_to_pyerr)?;
            Ok(PyStepReport::from_engine(&wb,report))
        })
    }
    fn prioritize(&self, py:Python<'_>, ranges:Vec<Rect>) -> PyResult<()> {
        self.workbook.check_reentrancy()?;
        let targets = targets(ranges)?;
        py.detach(|| {
            let mut wb = self.workbook.write_inner_detached().map_err(crate::errors::workbook_error_to_pyerr)?;
            self.ensure_current(&wb)?;
            wb.prioritize_recalc(&targets).map_err(recalc_error_to_pyerr)
        })
    }
    fn cancel(&self, py:Python<'_>) -> PyResult<Option<PyStepReport>> {
        self.workbook.check_reentrancy()?;
        py.detach(|| {
            let mut wb = self.workbook.write_inner_detached().map_err(crate::errors::workbook_error_to_pyerr)?;
            self.ensure_current(&wb)?;
            Ok(wb.cancel_recalc().map(|r| PyStepReport::from_engine(&wb,r)))
        })
    }
    #[getter]
    fn active(&self,py:Python<'_>) -> PyResult<bool> {
        self.workbook.check_reentrancy()?;
        py.detach(|| {
            let wb = self.workbook.inner.read().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock: {e}")))?;
            Ok(self.workbook.job_generation.load(Ordering::SeqCst) == self.generation && wb.has_active_recalc_job())
        })
    }
    fn __enter__(&self) -> Self { self.clone() }
    fn __exit__(&self,py:Python<'_>, _exc_type: &Bound<'_,PyAny>, _exc: &Bound<'_,PyAny>, _traceback: &Bound<'_,PyAny>) -> PyResult<()> {
        if self.active(py)? { self.cancel(py)?; }
        Ok(())
    }
    #[pyo3(signature = (budget_ms=Some(5.0)))]
    fn iter(&self,budget_ms:Option<f64>) -> PyResult<PyRecalcIter> {
        stop(budget_ms,engine::CancelToken::new())?;
        Ok(PyRecalcIter { job:self.clone(), budget_ms, done:false })
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "RecalcIterator", module = "formualizer.formualizer_py")]
pub struct PyRecalcIter { job:PyRecalcJob, budget_ms:Option<f64>, done:bool }
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyRecalcIter {
    fn __iter__(slf:PyRef<'_,Self>) -> PyRef<'_,Self> { slf }
    fn __next__(&mut self, py:Python<'_>) -> PyResult<Option<PyStepReport>> {
        if self.done { return Ok(None); }
        let r = self.job.step(py,self.budget_ms,None)?;
        self.done = r.phase == "Finished" || r.phase == "Cancelled";
        Ok(Some(r))
    }
}
pub(crate) fn register(m:&Bound<'_,PyModule>) -> PyResult<()> {
    m.add_class::<PyCancelToken>()?;
    m.add_class::<PyStepProgress>()?;
    m.add_class::<PyRecalcResult>()?;
    m.add_class::<PySlowUnitReport>()?;
    m.add_class::<PyStepReport>()?;
    m.add_class::<PyRecalcJob>()?;
    m.add_class::<PyRecalcIter>()?;
    Ok(())
}
