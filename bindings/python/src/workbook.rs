use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use formualizer::common::LiteralValue;
use formualizer::common::error::{ExcelError, ExcelErrorKind};

use crate::arrow_ingest::{
    PreparedColumns, prepare_columns_input, prepare_sparse_cells, prepare_values_rows,
};
use crate::engine::{
    PyEvaluationConfig, apply_binding_eval_defaults, eval_plan_to_py, merge_python_eval_config,
};
use crate::enums::{PyWorkbookMode, PyXlsxPathSource};
use crate::errors::workbook_error_to_pyerr;
use crate::value::{literal_to_py, py_to_cell_literal, py_to_literal};

type PyObject = pyo3::Py<pyo3::PyAny>;

pub(crate) const DEFAULT_XLSX_BYTE_BACKEND: &str = "native";

/// Convert only the five ordinary package limits; engine ingest limits remain separate.
fn xlsx_load_limits(
    overrides: Option<&Bound<'_, PyDict>>,
    backend: &str,
) -> PyResult<formualizer::workbook::XlsxLoadLimits> {
    let mut limits = formualizer::workbook::XlsxLoadLimits::default();
    let Some(overrides) = overrides else {
        return Ok(limits);
    };
    if !matches!(backend, "native" | "calamine") {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "load_limits requires a native XLSX source (backend='native' or 'calamine')",
        ));
    }
    for (key, value) in overrides.iter() {
        let key = key.extract::<String>().map_err(|_| {
            pyo3::exceptions::PyValueError::new_err("load_limits keys must be strings")
        })?;
        let target = match key.as_str() {
            "max_input_bytes" => &mut limits.max_input_bytes,
            "max_entries" => &mut limits.max_entries,
            "max_expanded_bytes" => &mut limits.max_expanded_bytes,
            "max_part_bytes" => &mut limits.max_part_bytes,
            "max_xml_depth" => &mut limits.max_xml_depth,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown load_limits field: {key}"
                )));
            }
        };
        // Python bool is an int subclass; no coercion, rounding or saturation.
        if value.is_instance_of::<pyo3::types::PyBool>()
            || !value.is_instance_of::<pyo3::types::PyInt>()
        {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "load_limits.{key} must be a non-negative integer (not bool)"
            )));
        }
        let value = value.extract::<usize>().map_err(|_| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "load_limits.{key} must be a non-negative host-representable integer"
            ))
        })?;
        // The core's allocation/index representation also excludes values above
        // isize::MAX, even when Python and usize can represent them.
        if value > isize::MAX as usize {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "load_limits.{key} exceeds the core host representation"
            )));
        }
        *target = value;
    }
    Ok(limits)
}

fn validate_cell_coords(row: u32, col: u32) -> PyResult<()> {
    if row == 0 || col == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Row/col are 1-based",
        ));
    }
    Ok(())
}

/// Map a poisoned-lock error to a workbook `IoError` so it can cross the thread
/// boundary of a `py.detach` region without touching Python objects.
fn lock_error_to_io<E: std::fmt::Display>(e: &E) -> formualizer::workbook::IoError {
    formualizer::workbook::IoError::Backend {
        backend: "lock".to_string(),
        message: e.to_string(),
    }
}

/// Tracks, per OS thread, which workbooks are currently inside one of their own
/// Python custom-function callbacks.
///
/// `Workbook`'s state lives behind a plain `std::sync::RwLock`, which is not
/// reentrant: a callback that calls back into the workbook it is registered on
/// blocks forever on a lock its own evaluation already holds. The callback runs
/// on whichever thread the engine dispatched it to (the main thread serially,
/// or a rayon worker in parallel mode), and any re-entrant call necessarily
/// happens on that same thread, so a thread-local marker sees every case.
mod reentrancy {
    use std::cell::RefCell;

    thread_local! {
        static ACTIVE: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
    }

    /// Marks `id` as evaluating on this thread for the guard's lifetime.
    pub(super) struct ActiveCallback(usize);

    impl ActiveCallback {
        pub(super) fn enter(id: usize) -> Self {
            ACTIVE.with(|active| active.borrow_mut().push(id));
            Self(id)
        }
    }

    impl Drop for ActiveCallback {
        fn drop(&mut self) {
            ACTIVE.with(|active| {
                let mut active = active.borrow_mut();
                if let Some(pos) = active.iter().rposition(|id| *id == self.0) {
                    active.remove(pos);
                }
            });
        }
    }

    pub(super) fn is_active(id: usize) -> bool {
        ACTIVE.with(|active| active.borrow().contains(&id))
    }
}

pub(crate) const REENTRANCY_MESSAGE: &str = "cannot use this Workbook from inside one of its own \
     custom functions: the evaluation that invoked the callback already holds the workbook lock, so \
     the call would deadlock. Only cancel() and reset_cancel() are safe from a callback.";

struct PyCustomFnHandler {
    callback: PyObject,
    /// Identity of the workbook this handler is registered on, used to detect
    /// re-entrant access. The handler is owned by that workbook, so the `Arc`
    /// is always alive while the handler runs and the address is stable.
    workbook_id: usize,
}

impl PyCustomFnHandler {
    fn new(callback: PyObject, workbook_id: usize) -> Self {
        Self {
            callback,
            workbook_id,
        }
    }

    fn pyerr_to_excel_value(err: pyo3::PyErr, py: Python<'_>) -> ExcelError {
        let exc_name = err
            .get_type(py)
            .name()
            .ok()
            .map(|name| name.to_string())
            .unwrap_or_else(|| "Exception".to_string());

        let mut detail = err.to_string().replace(['\r', '\n'], " ");
        if let Some(stripped) = detail.strip_prefix(&format!("{exc_name}:")) {
            detail = stripped.trim().to_string();
        } else {
            detail = detail.trim().to_string();
        }

        if detail.len() > 240 {
            detail.truncate(240);
            detail.push_str("...");
        }

        let message = if detail.is_empty() {
            format!("Python callback raised {exc_name}")
        } else {
            format!("Python callback raised {exc_name}: {detail}")
        };

        ExcelError::new(ExcelErrorKind::Value).with_message(message)
    }
}

impl formualizer::workbook::CustomFnHandler for PyCustomFnHandler {
    fn call(&self, args: &[LiteralValue]) -> Result<LiteralValue, ExcelError> {
        // Held for the whole callback so any workbook method this callback
        // reaches raises instead of deadlocking on the non-reentrant lock.
        let _active = reentrancy::ActiveCallback::enter(self.workbook_id);
        Python::attach(|py| {
            let callback = self.callback.bind(py);
            let py_args = args
                .iter()
                .map(|arg| literal_to_py(py, arg))
                .collect::<PyResult<Vec<_>>>()
                .map_err(|err| Self::pyerr_to_excel_value(err, py))?;
            let tuple =
                PyTuple::new(py, py_args).map_err(|err| Self::pyerr_to_excel_value(err, py))?;
            let result = callback
                .call1(tuple)
                .map_err(|err| Self::pyerr_to_excel_value(err, py))?;
            py_to_literal(&result).map_err(|err| Self::pyerr_to_excel_value(err, py))
        })
    }
}

/// Configuration for creating a [`Workbook`].
///
/// You typically pass this into `Workbook(config=...)`.
///
/// Example:
/// ```python
///     import formualizer as fz
///
///     cfg = fz.WorkbookConfig(
///         mode=fz.WorkbookMode.Interactive,
///         enable_changelog=True,
///         eval_config=fz.EvaluationConfig(),
///     )
///     wb = fz.Workbook(config=cfg)
/// ```
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "WorkbookConfig",
    module = "formualizer.formualizer_py",
    from_py_object
)]
#[derive(Clone)]
pub struct PyWorkbookConfig {
    mode: PyWorkbookMode,
    eval: Option<formualizer::eval::engine::EvalConfig>,
    enable_changelog: Option<bool>,
    span_evaluation: Option<bool>,
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyWorkbookConfig {
    #[new]
    #[pyo3(signature = (*, mode = PyWorkbookMode::Interactive, eval_config = None, enable_changelog = None, span_evaluation = None))]
    pub fn new(
        mode: PyWorkbookMode,
        eval_config: Option<PyEvaluationConfig>,
        enable_changelog: Option<bool>,
        span_evaluation: Option<bool>,
    ) -> Self {
        Self {
            mode,
            eval: eval_config.map(|c| c.inner),
            enable_changelog,
            span_evaluation,
        }
    }

    fn __repr__(&self) -> String {
        let mode = match self.mode {
            PyWorkbookMode::Ephemeral => "ephemeral",
            PyWorkbookMode::Interactive => "interactive",
        };
        format!(
            "WorkbookConfig(mode={}, enable_changelog={:?}, span_evaluation={:?})",
            mode, self.enable_changelog, self.span_evaluation
        )
    }
}

/// An in-memory Excel-like workbook which can store values and formulas and evaluate them.
///
/// Rows and columns are **1-based** (as in Excel).
///
/// The workbook supports setting values and formulas, evaluating individual cells,
/// and (optionally) tracking a changelog for undo/redo.
///
/// Quick start:
/// ```python
///     import formualizer as fz
///
///     wb = fz.Workbook()
///     s = wb.sheet("Sheet1")
///
///     s.set_value(1, 1, fz.LiteralValue.number(1000.0))  # A1
///     s.set_value(2, 1, fz.LiteralValue.number(0.05))    # A2
///     s.set_value(3, 1, fz.LiteralValue.number(12.0))    # A3
///
///     s.set_formula(1, 2, "=PMT(A2/12, A3, -A1)")
///     print(wb.evaluate_cell("Sheet1", 1, 2))
/// ```
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "Workbook",
    module = "formualizer.formualizer_py",
    from_py_object
)]
#[derive(Clone)]
pub struct PyWorkbook {
    pub(crate) inner: std::sync::Arc<std::sync::RwLock<formualizer::workbook::Workbook>>,
    pub(crate) cancel_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(crate) job_generation: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(crate) recalc_admission: std::sync::Arc<std::sync::atomic::AtomicU8>,
    pub(crate) step_cancel:
        std::sync::Arc<std::sync::Mutex<Option<formualizer::eval::engine::CancelToken>>>,
}

impl PyWorkbook {
    /// Runs one blocking evaluation under the workbook's cancel flag and
    /// consumes the flag when it returns (see `cancel()` for the contract).
    ///
    /// The flag is deliberately not cleared on entry: a `cancel()` that
    /// arrives before the evaluation starts must stop it, not be lost. With a
    /// cooperative recalculation job active the flag belongs to the job and is
    /// left alone.
    fn evaluate_under_cancel_flag<T>(
        &self,
        wb: &mut formualizer::workbook::Workbook,
        evaluate: impl FnOnce(
            &mut formualizer::workbook::Workbook,
            formualizer::eval::engine::CancelToken,
        ) -> Result<T, formualizer::workbook::IoError>,
    ) -> Result<T, formualizer::workbook::IoError> {
        let result = evaluate(
            wb,
            formualizer::eval::engine::CancelToken::from_flag(self.cancel_flag.clone()),
        );
        if !wb.has_active_recalc_job() {
            self.cancel_flag
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        result
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyWorkbook {
    #[new]
    #[pyo3(signature = (*, mode=None, config=None, enable_changelog=None, span_evaluation=None))]
    pub fn new(
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
        enable_changelog: Option<bool>,
        span_evaluation: Option<bool>,
    ) -> PyResult<Self> {
        let cfg = resolve_workbook_config(mode, config, enable_changelog, span_evaluation)?;
        Ok(Self::from_inner_workbook(
            formualizer::workbook::Workbook::new_with_config(cfg),
        ))
    }

    /// Class method: load an XLSX workbook from a file path.
    ///
    /// This is equivalent to the top-level `formualizer.load_workbook(...)`.
    ///
    /// Args:
    ///     path: Path to the `.xlsx` file.
    ///     backend: `native` (default) or `calamine` (native compatibility alias).
    ///     path_source: `XlsxPathSource.SHARED_FILE` (the safe default) or
    ///         `XlsxPathSource.DIRECT_MMAP`. Direct mmap requires a native
    ///         Calamine `.xlsx` path whose underlying file is not destructively
    ///         modified or truncated while the workbook loads.
    ///     mode/config: Optional workbook configuration.
    ///     load_limits: Optional native XLSX package limits; see `load_workbook`.
    ///
    /// Example:
    /// ```python
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook.load_path(
    ///         "model.xlsx", path_source=fz.XlsxPathSource.DIRECT_MMAP
    ///     )
    ///     print(wb.sheet_names)
    /// ```
    #[classmethod]
    #[pyo3(signature = (path, strategy=None, backend=None, *, path_source=None, mode=None, config=None, enable_changelog=None, span_evaluation=None, load_limits=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn load_path(
        _cls: &Bound<'_, pyo3::types::PyType>,
        path: &str,
        strategy: Option<&str>,
        backend: Option<&str>,
        path_source: Option<PyXlsxPathSource>,
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
        enable_changelog: Option<bool>,
        span_evaluation: Option<bool>,
        load_limits: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let _ = strategy; // currently unused, default eager
        Self::from_path(
            _cls,
            path,
            backend,
            path_source,
            mode,
            config,
            enable_changelog,
            span_evaluation,
            load_limits,
        )
    }

    /// Get or create a sheet by name.
    ///
    /// This returns a lightweight handle which forwards operations to the parent workbook.
    ///
    /// Notes:
    /// - Sheet names are case-sensitive.
    /// - The sheet is created if it doesn't exist.
    ///
    /// Example:
    /// ```python
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook()
    ///     s = wb.sheet("Data")
    ///     s.set_value(1, 1, 123)
    /// ```
    pub fn sheet(&self, name: &str) -> PyResult<crate::sheet::PySheet> {
        // Ensure sheet exists
        {
            let mut wb = self.write_inner()?;
            // add_sheet is idempotent on duplicate names
            wb.add_sheet(name)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        }
        let handle =
            formualizer::workbook::WorksheetHandle::new(self.inner.clone(), name.to_string());
        Ok(crate::sheet::PySheet {
            workbook: self.clone(),
            name: name.to_string(),
            handle,
        })
    }

    #[classmethod]
    // Keep the existing Python call shape while adding a keyword-only option.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, backend=None, *, path_source=None, mode=None, config=None, enable_changelog=None, span_evaluation=None, load_limits=None))]
    pub fn from_path(
        _cls: &Bound<'_, pyo3::types::PyType>,
        path: &str,
        backend: Option<&str>,
        path_source: Option<PyXlsxPathSource>,
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
        enable_changelog: Option<bool>,
        span_evaluation: Option<bool>,
        load_limits: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let backend = backend.unwrap_or(DEFAULT_XLSX_BYTE_BACKEND);
        let limits = xlsx_load_limits(load_limits, backend)?;
        let path_source = path_source.unwrap_or_default();
        if path_source == PyXlsxPathSource::DirectMmap && !matches!(backend, "calamine" | "native")
        {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "path_source=XlsxPathSource.DIRECT_MMAP requires backend='calamine' or backend='native', not backend='{backend}'"
            )));
        }
        if path_source == PyXlsxPathSource::DirectMmap
            && !std::path::Path::new(path)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("xlsx"))
        {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "path_source=XlsxPathSource.DIRECT_MMAP supports only Calamine `.xlsx` filesystem paths; use SHARED_FILE or an in-memory byte API for other formats",
            ));
        }
        let cfg = resolve_workbook_config(mode, config, enable_changelog, span_evaluation)?;
        match backend {
            "native" | "calamine" => {
                #[cfg(target_os = "emscripten")]
                if path_source == PyXlsxPathSource::DirectMmap {
                    return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                        "XlsxPathSource.DIRECT_MMAP is unavailable in the Pyodide build; use SHARED_FILE or an in-memory XLSX byte API",
                    ));
                }
                use formualizer::workbook::backends::NativeXlsxAdapter;
                let adapter = NativeXlsxAdapter::open_path_with_source_and_limits(
                    std::path::Path::new(path),
                    path_source.into(),
                    limits,
                )
                .map_err(|e| {
                    let source = match path_source {
                        PyXlsxPathSource::SharedFile => "SHARED_FILE",
                        PyXlsxPathSource::DirectMmap => "DIRECT_MMAP",
                    };
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                        "open failed with XlsxPathSource.{source}: {e}"
                    ))
                })?;
                let wb = formualizer::workbook::Workbook::from_reader(
                    adapter,
                    formualizer::workbook::LoadStrategy::EagerAll,
                    cfg,
                )
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("load failed: {e}"))
                })?;
                Ok(Self::from_inner_workbook(wb))
            }
            "umya" => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "backend='umya' has been removed from Python builds in v0.16.0; use backend='native' (the default)",
            )),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported backend: {backend}"
            ))),
        }
    }

    /// Class method: load an XLSX workbook from in-memory bytes.
    ///
    /// This is the Pyodide-friendly counterpart to `Workbook.from_path(...)`.
    ///
    /// Args:
    ///     data: XLSX payload as `bytes`.
    ///     backend: Backend name. Defaults to `native` on all platforms. `calamine` is a compatibility alias.
    ///     mode/config: Optional workbook configuration.
    ///     load_limits: Optional native XLSX package limits; see `load_workbook`.
    #[classmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (data, backend=None, *, mode=None, config=None, enable_changelog=None, span_evaluation=None, load_limits=None))]
    pub fn from_bytes<'py>(
        _cls: &Bound<'py, pyo3::types::PyType>,
        data: &Bound<'py, PyBytes>,
        backend: Option<&str>,
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
        enable_changelog: Option<bool>,
        span_evaluation: Option<bool>,
        load_limits: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Self> {
        let backend = backend.unwrap_or(DEFAULT_XLSX_BYTE_BACKEND);
        let limits = xlsx_load_limits(load_limits, backend)?;
        // Reject before the avoidable Python bytes -> Vec copy.
        if matches!(backend, "native" | "calamine")
            && data.as_bytes().len() > limits.max_input_bytes
        {
            return Err(pyo3::exceptions::PyIOError::new_err(
                "open failed: XLSX input exceeds max_input_bytes",
            ));
        }
        let cfg = resolve_workbook_config(mode, config, enable_changelog, span_evaluation)?;
        Self::from_bytes_impl(data.as_bytes().to_vec(), backend, cfg, limits)
    }

    /// Serialize the current workbook contents into XLSX bytes.
    ///
    /// Notes:
    /// - Uses Formualizer's built-in native OOXML serializer (`backend="native"`).
    /// - Output is generated from the in-memory workbook model; original XLSX styling
    ///   and unrelated package metadata are not preserved.
    #[pyo3(signature = (backend=None))]
    pub fn to_xlsx_bytes<'py>(
        &self,
        py: Python<'py>,
        backend: Option<&str>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        match backend.unwrap_or("native") {
            "native" => {
                let wb = self.read_inner()?;
                let bytes = wb.to_xlsx_bytes().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("save failed: {e}"))
                })?;
                Ok(PyBytes::new(py, &bytes))
            }
            "calamine" => Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                "backend='calamine' does not support XLSX byte export; use backend='native' (the default)",
            )),
            "umya" => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "backend='umya' has been removed from Python builds in v0.16.0; use backend='native' (the default)",
            )),
            other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported backend: {other}"
            ))),
        }
    }

    /// Add a sheet to the workbook.
    ///
    /// This is idempotent: adding an existing sheet name is a no-op.
    ///
    /// Example:
    /// ```python
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook()
    ///     wb.add_sheet("Inputs")
    ///     wb.add_sheet("Outputs")
    /// ```
    pub fn add_sheet(&self, name: &str) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.add_sheet(name)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    /// Define a native table over cells that already exist.
    ///
    /// `cell_range` is `(first_row, first_col, last_row, last_col)`, 1-based and
    /// inclusive, covering the header row when `header_row` is true. Tables are
    /// metadata over existing cells, so populate the region first; structured
    /// references such as `=SUM(Sales[Amount])` resolve immediately afterwards.
    ///
    /// ```python
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook()
    ///     wb.add_sheet("S")
    ///     wb.set_value("S", 1, 1, "Name")
    ///     wb.set_value("S", 1, 2, "Score")
    ///     wb.set_value("S", 2, 1, "Ani")
    ///     wb.set_value("S", 2, 2, 10)
    ///     wb.add_table("Scores", "S", (1, 1, 2, 2), ["Name", "Score"])
    ///     wb.set_formula("S", 4, 2, "=SUM(Scores[Score])")
    ///     wb.evaluate_all()
    /// ```
    #[pyo3(signature = (name, sheet, cell_range, headers, *, header_row = true, totals_row = false))]
    pub fn add_table(
        &self,
        name: &str,
        sheet: &str,
        cell_range: (u32, u32, u32, u32),
        headers: Vec<String>,
        header_row: bool,
        totals_row: bool,
    ) -> PyResult<()> {
        self.write_inner()?
            .define_table(name, sheet, cell_range, headers, header_row, totals_row)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    /// Metadata for every defined table, ordered by name.
    ///
    /// Each entry is a dict with `name`, `sheet`, `range` (a 1-based inclusive
    /// `(first_row, first_col, last_row, last_col)` tuple), `headers`,
    /// `header_row` and `totals_row`.
    pub fn tables(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let wb = self.read_inner()?;
        let mut out = Vec::new();
        for table in wb.tables() {
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("name", &table.name)?;
            dict.set_item("sheet", &table.sheet)?;
            dict.set_item(
                "range",
                (
                    table.start_row,
                    table.start_col,
                    table.end_row,
                    table.end_col,
                ),
            )?;
            dict.set_item("headers", &table.headers)?;
            dict.set_item("header_row", table.header_row)?;
            dict.set_item("totals_row", table.totals_row)?;
            out.push(dict.into());
        }
        Ok(out)
    }

    /// Non-calculating sheets omitted by the loader.
    #[getter]
    pub fn sheet_import_diagnostics(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let wb = self.read_inner()?;
        wb.sheet_import_diagnostics()
            .iter()
            .map(|diagnostic| {
                let dict = PyDict::new(py);
                dict.set_item("name", &diagnostic.name)?;
                dict.set_item("kind", &diagnostic.kind)?;
                Ok(dict.unbind())
            })
            .collect()
    }

    /// Non-fatal defined-name import diagnostics from the loader.
    #[getter]
    pub fn name_import_diagnostics(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let wb = self.read_inner()?;
        wb.name_import_diagnostics()
            .iter()
            .map(|diagnostic| {
                let dict = PyDict::new(py);
                dict.set_item("name", &diagnostic.name)?;
                dict.set_item("definition", &diagnostic.definition)?;
                dict.set_item("scope_sheet", &diagnostic.scope_sheet)?;
                dict.set_item("local_sheet_id", diagnostic.local_sheet_id)?;
                dict.set_item("message", &diagnostic.message)?;
                Ok(dict.unbind())
            })
            .collect()
    }

    /// Linked workbook indices whose cached values were loaded, never refreshed.
    #[getter]
    pub fn cached_external_link_indices(&self) -> PyResult<Vec<usize>> {
        Ok(self.read_inner()?.cached_external_link_indices().to_vec())
    }

    #[getter]
    pub fn sheet_names(&self) -> PyResult<Vec<String>> {
        let wb = self.read_inner()?;
        Ok(wb.sheet_names())
    }

    /// Register a workbook-local custom function backed by a Python callable.
    ///
    /// Re-entrancy contract
    /// --------------------
    /// **The callback must not touch the workbook it is registered on.** While
    /// a custom function runs, the evaluation that invoked it holds the
    /// workbook's lock, and that lock is not reentrant — a call back into the
    /// same workbook can never be granted. Only `cancel()` and
    /// `reset_cancel()` are safe from inside a callback: they flip an atomic
    /// flag and never take the lock.
    ///
    /// Every other method (`get_value`, `set_value`, `sheet_names`,
    /// `evaluate_cell`, `to_xlsx_bytes`, `Sheet.get_cell`, ...) raises
    /// `RuntimeError` when called from a callback rather than hanging forever.
    ///
    /// A callback *may* use a different `Workbook` instance, and may of course
    /// use any non-formualizer Python state.
    ///
    /// The callback receives the evaluated arguments as Python values and must
    /// return a Python value; raising an exception yields `#VALUE!` carrying
    /// the exception text.
    ///
    /// Example:
    /// ```python
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook()
    ///     wb.register_function("double", lambda x: x * 2, min_args=1, max_args=1)
    ///     s = wb.sheet("S")
    ///     s.set_value(1, 1, 21)
    ///     s.set_formula(1, 2, "=DOUBLE(A1)")
    ///     print(wb.evaluate_cell("S", 1, 2))  # 42.0
    /// ```
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, callback, *, min_args = 0, max_args = None, volatile = false, thread_safe = false, deterministic = true, allow_override_builtin = false))]
    pub fn register_function(
        &self,
        name: &str,
        callback: &Bound<'_, PyAny>,
        min_args: usize,
        max_args: Option<usize>,
        volatile: bool,
        thread_safe: bool,
        deterministic: bool,
        allow_override_builtin: bool,
    ) -> PyResult<()> {
        if !callback.is_callable() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "callback must be callable",
            ));
        }

        let handler = std::sync::Arc::new(PyCustomFnHandler::new(
            callback.clone().unbind(),
            self.workbook_id(),
        ));
        let options = formualizer::workbook::CustomFnOptions {
            min_args,
            max_args,
            volatile,
            thread_safe,
            deterministic,
            allow_override_builtin,
        };

        let mut wb = self.write_inner()?;
        wb.register_custom_function(name, options, handler)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Unregister a previously registered workbook-local custom function.
    pub fn unregister_function(&self, name: &str) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.unregister_custom_function(name)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// List registered workbook-local custom functions and their options.
    pub fn list_functions(&self, py: Python<'_>) -> PyResult<PyObject> {
        let wb = self.read_inner()?;
        let out = PyList::empty(py);

        for info in wb.list_custom_functions() {
            let row = PyDict::new(py);
            row.set_item("name", info.name)?;
            row.set_item("min_args", info.options.min_args)?;
            row.set_item("max_args", info.options.max_args)?;
            row.set_item("volatile", info.options.volatile)?;
            row.set_item("thread_safe", info.options.thread_safe)?;
            row.set_item("deterministic", info.options.deterministic)?;
            row.set_item(
                "allow_override_builtin",
                info.options.allow_override_builtin,
            )?;
            out.append(row)?;
        }

        Ok(out.into())
    }

    /// Return named ranges visible to the workbook or a specific sheet.
    ///
    /// Args:
    ///     sheet: Optional sheet name. When provided, returns workbook-scoped names plus
    ///         sheet-scoped names visible on that sheet.
    ///
    /// Returns:
    ///     A list of dictionaries with keys:
    ///     - `name`
    ///     - `scope` (`"workbook" | "sheet"`)
    ///     - `scope_sheet` (optional)
    ///     - `kind` (`"cell" | "range" | "literal" | "formula"`)
    ///     - address fields for `cell`/`range` kinds
    #[pyo3(signature = (sheet=None))]
    pub fn get_named_ranges(&self, py: Python<'_>, sheet: Option<&str>) -> PyResult<PyObject> {
        let wb = self.read_inner()?;

        let engine = wb.engine();
        let entries = if let Some(sheet_name) = sheet {
            let sheet_id = engine.sheet_id(sheet_name).ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Sheet not found: {sheet_name}"
                ))
            })?;
            engine.named_ranges_snapshot_for_sheet(sheet_id)
        } else {
            engine.named_ranges_snapshot()
        };

        let out = PyList::empty(py);
        for entry in entries {
            if let formualizer::eval::engine::named_range::NamedDefinition::Literal(
                LiteralValue::Error(error),
            ) = &entry.definition
            {
                let scope_sheet = match entry.scope {
                    formualizer::eval::engine::named_range::NameScope::Workbook => None,
                    formualizer::eval::engine::named_range::NameScope::Sheet(id) => {
                        Some(engine.sheet_name(id))
                    }
                };
                if error.message()
                    == Some(&format!(
                        "Defined name `{}` could not be evaluated",
                        entry.name
                    ))
                    && wb
                        .name_import_diagnostics()
                        .iter()
                        .any(|d| d.name == entry.name && d.scope_sheet.as_deref() == scope_sheet)
                {
                    continue;
                }
            }
            let row = PyDict::new(py);
            row.set_item("name", entry.name)?;

            match entry.scope {
                formualizer::eval::engine::named_range::NameScope::Workbook => {
                    row.set_item("scope", "workbook")?;
                    row.set_item("scope_sheet", py.None())?;
                }
                formualizer::eval::engine::named_range::NameScope::Sheet(sheet_id) => {
                    row.set_item("scope", "sheet")?;
                    row.set_item("scope_sheet", engine.sheet_name(sheet_id))?;
                }
            }

            match entry.definition {
                formualizer::eval::engine::named_range::NamedDefinition::Cell(cell) => {
                    row.set_item("kind", "cell")?;
                    row.set_item("sheet", engine.sheet_name(cell.sheet_id))?;
                    let r = cell.coord.row() + 1;
                    let c = cell.coord.col() + 1;
                    row.set_item("start_row", r)?;
                    row.set_item("start_col", c)?;
                    row.set_item("end_row", r)?;
                    row.set_item("end_col", c)?;
                }
                formualizer::eval::engine::named_range::NamedDefinition::Range(range) => {
                    row.set_item("kind", "range")?;
                    row.set_item("start_sheet", engine.sheet_name(range.start.sheet_id))?;
                    row.set_item("end_sheet", engine.sheet_name(range.end.sheet_id))?;
                    row.set_item("start_row", range.start.coord.row() + 1)?;
                    row.set_item("start_col", range.start.coord.col() + 1)?;
                    row.set_item("end_row", range.end.coord.row() + 1)?;
                    row.set_item("end_col", range.end.coord.col() + 1)?;
                    if range.start.sheet_id == range.end.sheet_id {
                        row.set_item("sheet", engine.sheet_name(range.start.sheet_id))?;
                    }
                }
                formualizer::eval::engine::named_range::NamedDefinition::Literal(value) => {
                    row.set_item("kind", "literal")?;
                    row.set_item("value", literal_to_py(py, &value)?)?;
                }
                formualizer::eval::engine::named_range::NamedDefinition::Formula { .. } => {
                    row.set_item("kind", "formula")?;
                }
            }

            out.append(row)?;
        }

        Ok(out.into())
    }

    /// Set a single cell value.
    ///
    /// Rows and columns are **1-based**.
    ///
    /// The `value` may be a Python primitive (int/float/bool/str/None), a
    /// `datetime/date/time/timedelta`, or a [`LiteralValue`].
    ///
    /// Example:
    /// ```python
    ///     import datetime
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook()
    ///     wb.add_sheet("Sheet1")
    ///
    ///     wb.set_value("Sheet1", 1, 1, 123)
    ///     wb.set_value("Sheet1", 2, 1, 3.14)
    ///     wb.set_value("Sheet1", 3, 1, datetime.date(2024, 1, 1))
    ///     wb.set_value("Sheet1", 4, 1, fz.LiteralValue.text("hello"))
    /// ```
    pub fn set_value(
        &self,
        _py: Python<'_>,
        sheet: &str,
        row: u32,
        col: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        validate_cell_coords(row, col)?;

        let literal = py_to_cell_literal(value, None)?;
        let mut wb = self.write_inner()?;
        wb.set_value(sheet, row, col, literal)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    /// Set a single cell formula.
    ///
    /// Rows and columns are **1-based**. Formulas should be Excel-style and typically
    /// begin with `=`.
    ///
    /// Example:
    /// ```python
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook()
    ///     s = wb.sheet("Sheet1")
    ///     s.set_value(1, 1, 10)
    ///     s.set_value(2, 1, 20)
    ///     s.set_formula(3, 1, "=SUM(A1:A2)")
    ///     print(wb.evaluate_cell("Sheet1", 3, 1))
    /// ```
    pub fn set_formula(&self, sheet: &str, row: u32, col: u32, formula: &str) -> PyResult<()> {
        validate_cell_coords(row, col)?;

        let mut wb = self.write_inner()?;
        wb.set_formula(sheet, row, col, formula)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    /// Evaluate a single cell and return the computed value.
    ///
    /// Rows and columns are **1-based**.
    ///
    /// Returns:
    ///     A Python value converted from the engine's internal [`LiteralValue`].
    ///     For example: `float`, `int`, `str`, `bool`, `datetime.*`, `None`, or
    ///     nested lists for arrays.
    ///
    /// Example:
    /// ```python
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook()
    ///     s = wb.sheet("Data")
    ///     s.set_value(1, 1, 100)
    ///     s.set_value(2, 1, 200)
    ///     s.set_formula(3, 1, "=SUM(A1:A2)")
    ///     print(wb.evaluate_cell("Data", 3, 1))
    /// ```
    pub fn evaluate_cell(
        &self,
        py: Python<'_>,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> PyResult<PyObject> {
        validate_cell_coords(row, col)?;

        let res = py.detach(|| {
            let mut wb = self.write_inner_detached()?;
            self.evaluate_under_cancel_flag(&mut wb, |wb, token| {
                wb.evaluate_cells_cancellable(&[(sheet, row, col)], token)
            })
        });
        let v = res
            .map_err(workbook_error_to_pyerr)?
            .into_iter()
            .next()
            .unwrap_or(formualizer::common::LiteralValue::Empty);
        literal_to_py(py, &v)
    }

    /// Pin the evaluation clock to a caller-supplied instant, so the
    /// volatile date/time builtins (TODAY, NOW) evaluate deterministically
    /// on the next recalculation. Takes effect on a live workbook; no
    /// reload is required.
    ///
    /// `deterministic_timezone` accepts `"utc"`, `"local"`, a fixed offset
    /// such as `"+02:00"`, or an offset in seconds — the same spelling as
    /// `SheetPortSession.evaluate_once(deterministic_timezone=...)`.
    /// Omitted means UTC.
    #[pyo3(signature = (deterministic_timestamp_utc, deterministic_timezone=None))]
    pub fn set_deterministic_clock(
        &self,
        deterministic_timestamp_utc: chrono::DateTime<chrono::Utc>,
        deterministic_timezone: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let timezone = match deterministic_timezone {
            Some(obj) => crate::sheetport::parse_timezone_spec(obj)?,
            None => formualizer::eval::timezone::TimeZoneSpec::Utc,
        };
        let mut wb = self.write_inner()?;
        wb.set_deterministic_mode(formualizer::eval::engine::DeterministicMode::Enabled {
            timestamp_utc: deterministic_timestamp_utc,
            timezone,
        })
        .map_err(workbook_error_to_pyerr)?;
        Ok(())
    }

    /// Recalculate cooperatively. on_step(report) may return False to pause.
    #[pyo3(signature = (budget_ms=Some(5.0), *, on_step=None, priority=None), text_signature = "($self, budget_ms=5.0, *, on_step=None, priority=None)")]
    pub fn recalc(
        &self,
        py: Python<'_>,
        budget_ms: Option<f64>,
        on_step: Option<PyObject>,
        priority: Option<Vec<crate::recalc::Rect>>,
    ) -> PyResult<crate::recalc::PyRecalcOutcome> {
        self.check_reentrancy()?;
        crate::recalc::stop(budget_ms, formualizer::eval::engine::CancelToken::new())?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("on_step", on_step)?;
        kwargs.set_item("priority", priority)?;
        Ok(py
            .import("formualizer._recalc")?
            .getattr("recalc")?
            .call((self.clone(), budget_ms), Some(&kwargs))?
            .extract()?)
    }

    /// Await cooperative recalculation; progress callbacks run on the event loop.
    #[pyo3(signature = (budget_ms=Some(5.0), *, on_progress=None, priority=None), text_signature = "($self, budget_ms=5.0, *, on_progress=None, priority=None)")]
    pub fn evaluate_async(
        &self,
        py: Python<'_>,
        budget_ms: Option<f64>,
        on_progress: Option<PyObject>,
        priority: Option<Vec<crate::recalc::Rect>>,
    ) -> PyResult<PyObject> {
        self.check_reentrancy()?;
        crate::recalc::stop(budget_ms, formualizer::eval::engine::CancelToken::new())?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("on_progress", on_progress)?;
        kwargs.set_item("priority", priority)?;
        Ok(py
            .import("formualizer._recalc")?
            .getattr("evaluate_async")?
            .call((self.clone(), budget_ms), Some(&kwargs))?
            .unbind())
    }

    /// Start a cooperative recalculation. Priority uses 1-based inclusive rectangles.
    #[pyo3(signature = (*, collect_deltas=true, max_delta_records=1024, priority=None))]
    pub fn begin_recalc(
        &self,
        py: Python<'_>,
        collect_deltas: bool,
        max_delta_records: usize,
        priority: Option<Vec<crate::recalc::Rect>>,
    ) -> PyResult<crate::recalc::PyRecalcJob> {
        self.begin_recalc_impl(py, collect_deltas, max_delta_records, priority, false)
    }

    // Only the Python async driver owns this reservation. These methods never
    // wait for the engine lock; native admission itself runs on its worker.
    pub fn _reserve_async_recalc(&self) -> PyResult<()> {
        self.check_reentrancy()?;
        self.recalc_admission
            .compare_exchange(
                0,
                crate::recalc::ASYNC_DRIVER,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .map_err(|_| {
                crate::errors::recalc_error_to_pyerr(
                    formualizer::eval::engine::RecalcJobError::AlreadyPending { explicit: true },
                )
            })?;
        Ok(())
    }

    pub fn _release_async_recalc(&self) {
        self.recalc_admission.fetch_and(
            !crate::recalc::ASYNC_DRIVER,
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    #[pyo3(signature = (*, priority=None))]
    pub fn _begin_async_recalc(
        &self,
        py: Python<'_>,
        priority: Option<Vec<crate::recalc::Rect>>,
    ) -> PyResult<crate::recalc::PyRecalcJob> {
        self.begin_recalc_impl(py, true, 1024, priority, true)
    }

    #[getter]
    pub fn has_active_recalc_job(&self, py: Python<'_>) -> PyResult<bool> {
        self.check_reentrancy()?;
        py.detach(|| {
            let wb = self
                .inner
                .read()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock: {e}")))?;
            Ok(wb.has_active_recalc_job())
        })
    }

    /// Engine or workbook-lock poison state. True means discard this workbook.
    /// This health query reports a poisoned lock without recovering its contents.
    #[getter]
    pub fn is_poisoned(&self, py: Python<'_>) -> PyResult<bool> {
        self.check_reentrancy()?;
        Ok(py.detach(|| match self.inner.read() {
            Ok(wb) => wb.is_poisoned(),
            Err(_) => true,
        }))
    }

    /// Raw formula dirtiness in a 1-based inclusive viewport.
    pub fn has_pending_in_rect(
        &self,
        py: Python<'_>,
        sheet: &str,
        r0: u32,
        c0: u32,
        r1: u32,
        c1: u32,
    ) -> PyResult<bool> {
        self.check_reentrancy()?;
        crate::recalc::validate_rect(r0, c0, r1, c1)?;
        py.detach(|| {
            let wb = self
                .inner
                .read()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock: {e}")))?;
            let id = wb
                .engine()
                .sheet_id(sheet)
                .ok_or_else(|| crate::errors::SheetNotFoundError::new_err(sheet.to_owned()))?;
            Ok(wb.has_pending_in_rect(id, r0 - 1, c0 - 1, r1 - 1, c1 - 1))
        })
    }

    /// Conservative pending rectangles; coordinates are 1-based inclusive.
    pub fn pending_cells_in_rect(
        &self,
        py: Python<'_>,
        sheet: &str,
        r0: u32,
        c0: u32,
        r1: u32,
        c1: u32,
    ) -> PyResult<Vec<crate::recalc::Rect>> {
        self.check_reentrancy()?;
        crate::recalc::validate_rect(r0, c0, r1, c1)?;
        py.detach(|| {
            let wb = self
                .inner
                .read()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock: {e}")))?;
            let id = wb
                .engine()
                .sheet_id(sheet)
                .ok_or_else(|| crate::errors::SheetNotFoundError::new_err(sheet.to_owned()))?;
            Ok(crate::recalc::rectangles(
                &wb,
                &wb.pending_cells_in_rect(id, r0 - 1, c0 - 1, r1 - 1, c1 - 1),
            ))
        })
    }

    pub fn evaluate_all(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let mut wb = self.write_inner_detached()?;
            self.evaluate_under_cancel_flag(&mut wb, |wb, token| wb.evaluate_all_cancellable(token))
        })
        .map_err(workbook_error_to_pyerr)?;
        Ok(())
    }

    /// Telemetry from runtime SCC / iterative-calculation evaluation during
    /// the most recent evaluation request (RFC #113, spec §10).
    ///
    /// Mirrors the engine accessor of the same name: counters reset at the
    /// start of every evaluation request, so this always describes the LAST
    /// `evaluate_all()` / `evaluate_cell(s)` call. All-zero when cycle
    /// detection is `"static"` or nothing cyclic was evaluated.
    ///
    /// Example:
    /// ```python
    ///     import formualizer as fz
    ///
    ///     cfg = fz.EvaluationConfig()
    ///     cfg.cycle_policy = "iterate"
    ///     wb = fz.Workbook(config=fz.WorkbookConfig(eval_config=cfg))
    ///     s = wb.sheet("S")
    ///     s.set_formula(1, 1, "=B1+1")
    ///     s.set_formula(1, 2, "=A1/2")
    ///     wb.evaluate_all()
    ///     t = wb.last_cycle_telemetry()
    ///     print(t.iterated_sccs, t.converged_sccs, t.capped_sccs)
    /// ```
    pub fn last_cycle_telemetry(&self) -> PyResult<PyCycleTelemetry> {
        let wb = self.read_inner()?;
        Ok(PyCycleTelemetry::from_engine(
            wb.engine().last_cycle_telemetry(),
        ))
    }

    pub fn evaluate_cells(
        &self,
        py: Python<'_>,
        targets: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<PyObject> {
        let mut target_vec = Vec::with_capacity(targets.len());
        for item in targets.iter() {
            let tuple: &Bound<'_, pyo3::types::PyTuple> = item.cast()?;
            let sheet: String = tuple.get_item(0)?.extract()?;
            let row: u32 = tuple.get_item(1)?.extract()?;
            let col: u32 = tuple.get_item(2)?.extract()?;
            validate_cell_coords(row, col)?;
            target_vec.push((sheet, row, col));
        }

        // We use a temporary vector of (&str, u32, u32) because Workbook::evaluate_cells expects that
        let refs: Vec<(&str, u32, u32)> = target_vec
            .iter()
            .map(|(s, r, c)| (s.as_str(), *r, *c))
            .collect();

        let results = py
            .detach(|| {
                let mut wb = self.write_inner_detached()?;
                self.evaluate_under_cancel_flag(&mut wb, |wb, token| {
                    wb.evaluate_cells_cancellable(&refs, token)
                })
            })
            .map_err(workbook_error_to_pyerr)?;

        let py_results = pyo3::types::PyList::empty(py);
        for v in results {
            py_results.append(literal_to_py(py, &v)?)?;
        }
        Ok(py_results.into())
    }

    #[pyo3(signature = (targets, *, build_graph_if_needed=true))]
    pub fn get_eval_plan(
        &self,
        targets: &Bound<'_, pyo3::types::PyList>,
        build_graph_if_needed: bool,
    ) -> PyResult<crate::engine::PyEvaluationPlan> {
        let mut target_vec = Vec::with_capacity(targets.len());
        for item in targets.iter() {
            let tuple: &Bound<'_, pyo3::types::PyTuple> = item.cast()?;
            let sheet: String = tuple.get_item(0)?.extract()?;
            let row: u32 = tuple.get_item(1)?.extract()?;
            let col: u32 = tuple.get_item(2)?.extract()?;
            validate_cell_coords(row, col)?;
            target_vec.push((sheet, row, col));
        }

        let refs: Vec<(&str, u32, u32)> = target_vec
            .iter()
            .map(|(s, r, c)| (s.as_str(), *r, *c))
            .collect();

        let mut wb = self.write_inner()?;
        let plan = wb
            .get_eval_plan_with_options(&refs, build_graph_if_needed)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(eval_plan_to_py(plan))
    }

    /// Request cancellation of an evaluation on this workbook.
    ///
    /// Safe to call from any thread and from inside a custom-function
    /// callback (it only sets a flag; it never takes the workbook lock).
    ///
    /// Contract
    /// --------
    /// A cancel request stops exactly one blocking evaluation
    /// (`evaluate_all`, `evaluate_cell`, `evaluate_cells`):
    ///
    /// - If one is running, it stops at its next checkpoint (between formula
    ///   cells, and inside long-running functions) and raises
    ///   `ExcelEvaluationError` with `kind == "Cancelled"`. Cells that had not
    ///   finished are left dirty: they keep their previous values and are
    ///   recomputed by the next evaluation.
    /// - If none is running, the request stays pending and the *next* blocking
    ///   evaluation raises the same error without computing anything. A
    ///   request is therefore never lost to a race with the start of an
    ///   evaluation. `reset_cancel()` withdraws a pending request.
    ///
    /// Either way the request is consumed when that evaluation returns, so
    /// the evaluation after it runs normally. An evaluation that finishes
    /// before it observes a late request returns normally and still consumes
    /// it.
    ///
    /// Cooperative recalculation (`begin_recalc`, `recalc`, `evaluate_async`)
    /// has its own handle: `cancel()` also stops the step that is running,
    /// but starting a job discards a pending request; use `RecalcJob.cancel()`
    /// or the step's `CancelToken` there.
    ///
    /// Example:
    /// ```python
    ///     wb.cancel()
    ///     try:
    ///         wb.evaluate_all()
    ///     except fz.ExcelEvaluationError as e:
    ///         assert e.kind == "Cancelled"
    ///     wb.evaluate_all()  # the request was consumed; this one runs
    /// ```
    pub fn cancel(&self) {
        self.cancel_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(token) = self.step_cancel.lock().expect("step token lock").as_ref()
            // A step using the workbook's own flag already has the signal.
            // Cancelling it again here could re-set the flag after that step
            // consumed it, leaving a request pending for the next evaluation.
            && !std::sync::Arc::ptr_eq(token.as_flag(), &self.cancel_flag)
        {
            token.cancel();
        }
    }

    /// Withdraw a pending `cancel()` request, so the next evaluation runs.
    ///
    /// This is for a request no evaluation has started on. Calling it from
    /// another thread while an evaluation is running races with that
    /// evaluation: it may or may not still be stopped.
    pub fn reset_cancel(&self) {
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Choose temporal output as native Python datetime values (default) or floats.
    pub fn set_temporal_egress(&self, policy: &str) -> PyResult<()> {
        let policy = match policy.to_ascii_lowercase().as_str() {
            "native" => formualizer::eval::engine::TemporalEgress::Native,
            "serial" => formualizer::eval::engine::TemporalEgress::Serial,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "temporal egress must be 'native' or 'serial'",
                ));
            }
        };
        self.write_inner()?.engine_mut().set_temporal_egress(policy);
        Ok(())
    }

    pub fn get_value(
        &self,
        py: Python<'_>,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> PyResult<Option<PyObject>> {
        validate_cell_coords(row, col)?;

        let wb = self.read_inner()?;
        Ok(match wb.get_value(sheet, row, col) {
            Some(v) => Some(literal_to_py(py, &v)?),
            None => None,
        })
    }

    pub fn get_formula(&self, sheet: &str, row: u32, col: u32) -> PyResult<Option<String>> {
        validate_cell_coords(row, col)?;

        let wb = self.read_inner()?;
        Ok(wb.get_formula(sheet, row, col))
    }

    // Changelog controls
    /// Whether changelog recording for undo/redo is currently enabled.
    #[getter]
    pub fn changelog_enabled(&self) -> PyResult<bool> {
        let wb = self.read_inner()?;
        Ok(wb.changelog_enabled())
    }

    /// Whether `undo()` currently has an action/change to revert.
    #[getter]
    pub fn can_undo(&self) -> PyResult<bool> {
        let wb = self.read_inner()?;
        Ok(wb.can_undo())
    }

    /// Whether `redo()` currently has an undone action/change to replay.
    #[getter]
    pub fn can_redo(&self) -> PyResult<bool> {
        let wb = self.read_inner()?;
        Ok(wb.can_redo())
    }

    pub fn set_changelog_enabled(&self, enabled: bool) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.set_changelog_enabled(enabled);
        Ok(())
    }

    // Changelog metadata
    #[pyo3(signature = (actor_id=None))]
    pub fn set_actor_id(&self, actor_id: Option<String>) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.set_actor_id(actor_id);
        Ok(())
    }

    #[pyo3(signature = (correlation_id=None))]
    pub fn set_correlation_id(&self, correlation_id: Option<String>) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.set_correlation_id(correlation_id);
        Ok(())
    }

    #[pyo3(signature = (reason=None))]
    pub fn set_reason(&self, reason: Option<String>) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.set_reason(reason);
        Ok(())
    }

    /// Begin grouping multiple edits into a single undo/redo action.
    ///
    /// This is only relevant when the changelog is enabled.
    ///
    /// Example:
    /// ```python
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook()
    ///     wb.set_changelog_enabled(True)
    ///     s = wb.sheet("Data")
    ///
    ///     wb.begin_action("update prices")
    ///     s.set_value(1, 1, 100)
    ///     s.set_value(2, 1, 200)
    ///     wb.end_action()
    ///
    ///     wb.undo()  # reverts both values at once
    /// ```
    pub fn begin_action(&self, description: &str) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.begin_action(description.to_string());
        Ok(())
    }

    /// End the current grouped undo/redo action.
    pub fn end_action(&self) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.end_action();
        Ok(())
    }

    /// Undo the most recent workbook edit.
    ///
    /// Raises `RuntimeError` if changelog recording is disabled and no explicit
    /// action journal is present.
    pub fn undo(&self) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.undo().map_err(crate::errors::workbook_error_to_pyerr)?;
        Ok(())
    }

    /// Redo the most recently undone edit.
    ///
    /// Raises `RuntimeError` if changelog recording is disabled and no explicit
    /// action journal is present.
    pub fn redo(&self) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        wb.redo().map_err(crate::errors::workbook_error_to_pyerr)?;
        Ok(())
    }

    // Bulk write ops

    /// Write a dense, row-major 2D sequence of values starting at 1-based `(start_row, start_col)`.
    ///
    /// Ragged rows are supported without overwriting trailing cells past each row's length.
    /// Returns a [`WriteRangeReport`] with the written bounds and whether undo history was cleared.
    pub fn set_values(
        &self,
        py: Python<'_>,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        rows: &Bound<'_, PyAny>,
    ) -> PyResult<PyWriteRangeReport> {
        validate_cell_coords(start_row, start_col)?;
        self.check_reentrancy()?;

        let rows_vec = prepare_values_rows(rows)?;
        let sheet_owned = sheet.to_owned();
        let report = py
            .detach(move || {
                let mut wb = self.write_inner_detached()?;
                wb.set_values(&sheet_owned, start_row, start_col, &rows_vec)
            })
            .map_err(workbook_error_to_pyerr)?;
        Ok(report.into())
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
    /// Arrow streams are drained and converted into engine-owned column buffers before the
    /// workbook write lock is acquired. When `headers=True`, schema or mapping column names
    /// are written into `start_row` and data starts on `start_row + 1`.
    #[pyo3(signature = (sheet, start_row, start_col, data, *, headers = false))]
    pub fn set_columns(
        &self,
        py: Python<'_>,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        data: &Bound<'_, PyAny>,
        headers: bool,
    ) -> PyResult<PyWriteRangeReport> {
        validate_cell_coords(start_row, start_col)?;
        let (date_system, limits) = {
            let wb = self.read_inner()?;
            (
                wb.engine().config.date_system,
                wb.engine().workbook_load_limits().clone(),
            )
        };

        // CRITICAL: Drain and convert Arrow streams / Python iterables completely
        // BEFORE acquiring the workbook write lock.
        let prepared =
            prepare_columns_input(py, data, start_row, start_col, headers, date_system, limits)?;
        let sheet_owned = sheet.to_owned();
        let report = py
            .detach(move || {
                let mut wb = self.write_inner_detached()?;
                commit_prepared_columns(&mut wb, &sheet_owned, start_row, start_col, prepared)
            })
            .map_err(workbook_error_to_pyerr)?;
        Ok(report.into())
    }

    /// Write sparse cell patches onto `sheet`.
    ///
    /// Accepts either an iterable of 1-based `(row, col, value)` triples or a mapping
    /// `{(row, col): value}`. Duplicate coordinates resolve deterministically in
    /// last-write-wins order.
    pub fn set_cells(
        &self,
        py: Python<'_>,
        sheet: &str,
        cells: &Bound<'_, PyAny>,
    ) -> PyResult<PyWriteRangeReport> {
        let date_system = {
            let wb = self.read_inner()?;
            wb.engine().config.date_system
        };

        // Drain the Python iterable/mapping BEFORE acquiring the workbook write lock.
        let owned_batch = prepare_sparse_cells(py, cells, date_system)?;
        let sheet_owned = sheet.to_owned();
        let report = py
            .detach(move || {
                let mut wb = self.write_inner_detached()?;
                let batch = owned_batch.as_cell_batch();
                wb.set_cells(&sheet_owned, &batch)
            })
            .map_err(workbook_error_to_pyerr)?;
        Ok(report.into())
    }

    /// Deprecated alias for [`Workbook.set_values`].
    pub fn set_values_batch(
        &self,
        py: Python<'_>,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        data: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<PyWriteRangeReport> {
        validate_cell_coords(start_row, start_col)?;
        PyErr::warn(
            py,
            &py.get_type::<pyo3::exceptions::PyDeprecationWarning>(),
            c"Workbook.set_values_batch is deprecated; use Workbook.set_values instead",
            1,
        )?;
        self.set_values(py, sheet, start_row, start_col, data.as_any())
    }

    pub fn set_formulas_batch(
        &self,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        formulas: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<()> {
        validate_cell_coords(start_row, start_col)?;

        let mut rows_vec: Vec<Vec<String>> = Vec::with_capacity(formulas.len());
        for row in formulas.iter() {
            let list: &Bound<'_, pyo3::types::PyList> = row.cast()?;
            let mut row_vals: Vec<String> = Vec::with_capacity(list.len());
            for v in list.iter() {
                let s: String = v.extract()?;
                row_vals.push(s);
            }
            rows_vec.push(row_vals);
        }
        let mut wb = self.write_inner()?;
        wb.begin_action("batch: set formulas".to_string());
        let res = wb
            .set_formulas(sheet, start_row, start_col, &rows_vec)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()));
        wb.end_action();
        res?;
        Ok(())
    }

    /// Indexing to get a Sheet view (compatibility)
    fn __getitem__(&self, name: &str) -> PyResult<crate::sheet::PySheet> {
        {
            let mut wb = self.write_inner()?;
            wb.add_sheet(name)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        }
        let handle =
            formualizer::workbook::WorksheetHandle::new(self.inner.clone(), name.to_string());
        Ok(crate::sheet::PySheet {
            workbook: self.clone(),
            name: name.to_string(),
            handle,
        })
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWorkbook>()?;
    m.add_class::<PyWorkbookConfig>()?;
    m.add_class::<PyRangeAddress>()?;
    m.add_class::<PyWriteRangeReport>()?;
    m.add_class::<PyCycleTelemetry>()?;
    m.add_class::<PyCell>()?;
    Ok(())
}

/// Summary returned by bulk cell-write operations (`set_values`, `set_columns`, `set_cells`).
///
/// Coordinates are 1-based inclusive bounds of the written cells. When an
/// operation writes zero cells, all four coordinate fields are `0`.
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "WriteRangeReport",
    module = "formualizer.formualizer_py",
    from_py_object
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyWriteRangeReport {
    #[pyo3(get)]
    pub start_row: u32,
    #[pyo3(get)]
    pub start_col: u32,
    #[pyo3(get)]
    pub end_row: u32,
    #[pyo3(get)]
    pub end_col: u32,
    #[pyo3(get)]
    pub history_cleared: bool,
}

impl From<formualizer::workbook::WriteRangeReport> for PyWriteRangeReport {
    fn from(r: formualizer::workbook::WriteRangeReport) -> Self {
        Self {
            start_row: r.start_row,
            start_col: r.start_col,
            end_row: r.end_row,
            end_col: r.end_col,
            history_cleared: r.history_cleared,
        }
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyWriteRangeReport {
    fn __repr__(&self) -> String {
        format!(
            "WriteRangeReport(start_row={}, start_col={}, end_row={}, end_col={}, history_cleared={})",
            self.start_row,
            self.start_col,
            self.end_row,
            self.end_col,
            if self.history_cleared {
                "True"
            } else {
                "False"
            },
        )
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other
            .extract::<PyRef<'_, PyWriteRangeReport>>()
            .is_ok_and(|o| *self == *o)
    }
}

fn commit_prepared_columns(
    wb: &mut formualizer::workbook::Workbook,
    sheet: &str,
    start_row: u32,
    start_col: u32,
    prepared: PreparedColumns,
) -> Result<formualizer::workbook::WriteRangeReport, formualizer::workbook::IoError> {
    if prepared.ncols == 0 || (prepared.headers.is_none() && prepared.batches.is_empty()) {
        return wb.set_columns(sheet, start_row, start_col, &[]);
    }

    let step_count = usize::from(prepared.headers.is_some()) + prepared.batches.len();
    let total_cells = prepared.total_cells();
    let was_changelog_enabled = wb.changelog_enabled();
    let fits_history = was_changelog_enabled
        && wb
            .history_limit()
            .is_none_or(|limit| total_cells.saturating_add(2) <= limit);

    let opened_compound = fits_history && step_count > 1;
    let bypassed_changelog_for_oversized_multistep =
        was_changelog_enabled && !fits_history && step_count > 1;

    if opened_compound {
        wb.begin_action("batch: set columns".to_string());
    } else if bypassed_changelog_for_oversized_multistep {
        // Oversized multi-step write (e.g. headers + data or multi-batch Arrow stream):
        // clear history once and take the fast bulk path for every step, then re-enable.
        wb.set_changelog_enabled(false);
    }

    let mut next_row = start_row;
    let mut combined: Option<formualizer::workbook::WriteRangeReport> = None;

    let res =
        (|| -> Result<formualizer::workbook::WriteRangeReport, formualizer::workbook::IoError> {
            if let Some(header_names) = prepared.headers {
                let header_row: Vec<LiteralValue> =
                    header_names.into_iter().map(LiteralValue::Text).collect();
                let hdr_report = wb.set_values(sheet, start_row, start_col, &[header_row])?;
                combined = Some(hdr_report);
                next_row = start_row.checked_add(1).ok_or_else(|| {
                    formualizer::workbook::IoError::Engine(
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message("Column header row extent overflows grid"),
                    )
                })?;
            }

            for (nrows, owned_cols) in prepared.batches {
                if nrows == 0 {
                    continue;
                }
                let col_views: Vec<formualizer::workbook::ColumnData<'_>> = owned_cols
                    .iter()
                    .map(formualizer::workbook::OwnedColumnData::as_column_data)
                    .collect();
                let batch_report = wb.set_columns(sheet, next_row, start_col, &col_views)?;
                next_row = next_row.checked_add(nrows as u32).ok_or_else(|| {
                    formualizer::workbook::IoError::Engine(
                        ExcelError::new(ExcelErrorKind::Ref)
                            .with_message("Column row extent overflows grid"),
                    )
                })?;
                combined = Some(match combined {
                    None => batch_report,
                    Some(prev) => formualizer::workbook::WriteRangeReport {
                        start_row: prev.start_row.min(batch_report.start_row),
                        start_col: prev.start_col.min(batch_report.start_col),
                        end_row: prev.end_row.max(batch_report.end_row),
                        end_col: prev.end_col.max(batch_report.end_col),
                        history_cleared: prev.history_cleared || batch_report.history_cleared,
                    },
                });
            }

            let mut report = combined.unwrap_or_default();
            if bypassed_changelog_for_oversized_multistep {
                report.history_cleared = true;
            }
            Ok(report)
        })();

    if opened_compound {
        wb.end_action();
    } else if bypassed_changelog_for_oversized_multistep {
        wb.set_changelog_enabled(true);
    }

    res
}

/// Per-recalc telemetry from runtime SCC evaluation (RFC #113, spec §10).
///
/// Read-only snapshot of the engine's `CycleTelemetry`, taken by
/// `Workbook.last_cycle_telemetry()`. Counters reset at the start of every
/// evaluation request.
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "CycleTelemetry",
    module = "formualizer.formualizer_py",
    from_py_object
)]
#[derive(Clone, Debug)]
pub struct PyCycleTelemetry {
    /// SCC tasks executed (static SCCs that reached Runtime evaluation).
    #[pyo3(get)]
    pub static_sccs: usize,
    /// SCC tasks whose live subgraph was acyclic — values produced.
    #[pyo3(get)]
    pub phantom_sccs: usize,
    /// Distinct live cycles witnessed across all SCC tasks.
    #[pyo3(get)]
    pub live_cycles_witnessed: usize,
    /// Cells stamped `#CIRC!` by Runtime SCC tasks.
    #[pyo3(get)]
    pub circ_cells_stamped: usize,
    /// Evaluation sweeps over (subsets of) SCC members, totalled across tasks.
    #[pyo3(get)]
    pub settle_passes_total: usize,
    /// Largest pass count any single SCC task needed.
    #[pyo3(get)]
    pub max_passes_single_scc: usize,
    /// SCC tasks that entered iterative calculation.
    #[pyo3(get)]
    pub iterated_sccs: usize,
    /// Iterating SCC tasks that stopped because every member converged.
    #[pyo3(get)]
    pub converged_sccs: usize,
    /// SCC tasks that stopped at a pass cap (NOT an error under iterate).
    #[pyo3(get)]
    pub capped_sccs: usize,
    /// Largest |delta| observed in any member's final-pass convergence check.
    #[pyo3(get)]
    pub max_abs_delta_at_stop: f64,
    /// Identical-bit NaN comparisons treated as converged (spec §6 NaN rule).
    #[pyo3(get)]
    pub nan_converged: usize,
    /// Retained exactly-converged SCCs that had no dirty member at the start of this request and were
    /// therefore served without a re-run (#368).
    #[pyo3(get)]
    pub reused_sccs: usize,
    /// Members of the SCCs counted in `reused_sccs`.
    #[pyo3(get)]
    pub reused_scc_members: usize,
    /// Wall-clock milliseconds spent inside Runtime SCC tasks.
    #[pyo3(get)]
    pub elapsed_ms: u64,
}

impl PyCycleTelemetry {
    pub(crate) fn from_engine(t: &formualizer::eval::engine::CycleTelemetry) -> Self {
        Self {
            static_sccs: t.static_sccs,
            phantom_sccs: t.phantom_sccs,
            live_cycles_witnessed: t.live_cycles_witnessed,
            circ_cells_stamped: t.circ_cells_stamped,
            settle_passes_total: t.settle_passes_total,
            max_passes_single_scc: t.max_passes_single_scc,
            iterated_sccs: t.iterated_sccs,
            converged_sccs: t.converged_sccs,
            capped_sccs: t.capped_sccs,
            max_abs_delta_at_stop: t.max_abs_delta_at_stop,
            nan_converged: t.nan_converged,
            reused_sccs: t.reused_sccs,
            reused_scc_members: t.reused_scc_members,
            elapsed_ms: u64::try_from(t.elapsed_ms).unwrap_or(u64::MAX),
        }
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyCycleTelemetry {
    fn __repr__(&self) -> String {
        format!(
            "CycleTelemetry(static_sccs={}, phantom_sccs={}, live_cycles_witnessed={}, \
             circ_cells_stamped={}, settle_passes_total={}, max_passes_single_scc={}, \
             iterated_sccs={}, converged_sccs={}, capped_sccs={}, \
             max_abs_delta_at_stop={}, nan_converged={}, reused_sccs={}, \
             reused_scc_members={}, elapsed_ms={})",
            self.static_sccs,
            self.phantom_sccs,
            self.live_cycles_witnessed,
            self.circ_cells_stamped,
            self.settle_passes_total,
            self.max_passes_single_scc,
            self.iterated_sccs,
            self.converged_sccs,
            self.capped_sccs,
            self.max_abs_delta_at_stop,
            self.nan_converged,
            self.reused_sccs,
            self.reused_scc_members,
            self.elapsed_ms,
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "Cell", module = "formualizer.formualizer_py")]
pub struct PyCell {
    value: LiteralValue,
    formula: Option<String>,
}

impl PyCell {
    pub(crate) fn new(value: LiteralValue, formula: Option<String>) -> Self {
        Self { value, formula }
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyCell {
    #[getter]
    pub fn value(&self, py: Python<'_>) -> PyResult<PyObject> {
        literal_to_py(py, &self.value)
    }

    #[getter]
    pub fn formula(&self) -> Option<String> {
        self.formula.clone()
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "RangeAddress",
    module = "formualizer.formualizer_py",
    from_py_object
)]
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

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
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
        // Validate via core type
        formualizer::workbook::RangeAddress::new(
            sheet.clone(),
            start_row,
            start_col,
            end_row,
            end_col,
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        Ok(Self {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
        })
    }
}

// Non-Python methods for internal use
impl PyWorkbook {
    fn begin_recalc_impl(
        &self,
        py: Python<'_>,
        collect_deltas: bool,
        max_delta_records: usize,
        priority: Option<Vec<crate::recalc::Rect>>,
        async_owner: bool,
    ) -> PyResult<crate::recalc::PyRecalcJob> {
        self.check_reentrancy()?;
        let priority = crate::recalc::targets(priority.unwrap_or_default())?;
        let mut admission =
            crate::recalc::JobAdmission::reserve(&self.recalc_admission, async_owner)?;
        py.detach(|| {
            let mut wb = self
                .write_inner_detached()
                .map_err(workbook_error_to_pyerr)?;
            let mut options = formualizer::eval::engine::RecalcJobOptions::default();
            options.collect_deltas = collect_deltas;
            options.max_delta_records = max_delta_records;
            options.initial_priority = priority;
            wb.begin_recalc(options)
                .map_err(crate::errors::recalc_error_to_pyerr)?;
            self.cancel_flag
                .store(false, std::sync::atomic::Ordering::SeqCst);
            let generation = self
                .job_generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            admission.keep = true;
            Ok(crate::recalc::PyRecalcJob {
                workbook: self.clone(),
                generation,
            })
        })
    }

    fn from_inner_workbook(inner: formualizer::workbook::Workbook) -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::RwLock::new(inner)),
            cancel_flag: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            job_generation: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            recalc_admission: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)),
            step_cancel: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }

    fn from_bytes_impl(
        data: Vec<u8>,
        backend: &str,
        cfg: formualizer::workbook::WorkbookConfig,
        limits: formualizer::workbook::XlsxLoadLimits,
    ) -> PyResult<Self> {
        match backend {
            "native" | "calamine" => {
                use formualizer::workbook::backends::NativeXlsxAdapter;
                let adapter =
                    NativeXlsxAdapter::open_bytes_with_limits(data, limits).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("open failed: {e}"))
                    })?;
                let wb = formualizer::workbook::Workbook::from_reader(
                    adapter,
                    formualizer::workbook::LoadStrategy::EagerAll,
                    cfg,
                )
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("load failed: {e}"))
                })?;
                Ok(Self::from_inner_workbook(wb))
            }
            "umya" => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "backend='umya' has been removed from Python builds in v0.16.0; use backend='native' (the default)",
            )),
            other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported backend: {other}"
            ))),
        }
    }

    /// Stable identity for this workbook, shared by every clone of the handle
    /// (`PyWorkbook` is `Clone` and `PySheet` holds one).
    pub(crate) fn workbook_id(&self) -> usize {
        std::sync::Arc::as_ptr(&self.inner) as usize
    }

    /// Fail fast when a Python custom-function callback re-enters the workbook
    /// it is registered on. Without this the call would block forever on the
    /// non-reentrant `RwLock` the running evaluation already holds.
    pub(crate) fn check_reentrancy(&self) -> PyResult<()> {
        if reentrancy::is_active(self.workbook_id()) {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                REENTRANCY_MESSAGE,
            ));
        }
        Ok(())
    }

    /// Shared access to the engine state, re-entrancy checked.
    pub(crate) fn read_inner(
        &self,
    ) -> PyResult<std::sync::RwLockReadGuard<'_, formualizer::workbook::Workbook>> {
        self.check_reentrancy()?;
        loop {
            match self.inner.try_read() {
                Ok(guard) => return Ok(guard),
                Err(std::sync::TryLockError::Poisoned(e)) => {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "lock: {e}"
                    )));
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    // A step may need the GIL for a Python UDF. Never wait for
                    // its engine lock while holding the GIL. Guards never cross
                    // the detach boundary: wait, drop, then retry.
                    Python::attach(|py| py.detach(|| drop(self.inner.read())));
                }
            }
        }
    }

    /// Exclusive access to the engine state, re-entrancy checked.
    pub(crate) fn write_inner(
        &self,
    ) -> PyResult<std::sync::RwLockWriteGuard<'_, formualizer::workbook::Workbook>> {
        self.check_reentrancy()?;
        loop {
            match self.inner.try_write() {
                Ok(guard) => return Ok(guard),
                Err(std::sync::TryLockError::Poisoned(e)) => {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "lock: {e}"
                    )));
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    Python::attach(|py| py.detach(|| drop(self.inner.write())));
                }
            }
        }
    }

    /// Exclusive access from inside a `py.detach` region: the error type must
    /// not touch Python, so lock failures and re-entrancy both surface as
    /// `IoError::Backend`, which `workbook_error_to_pyerr` maps to
    /// `RuntimeError` exactly as the direct paths above do.
    pub(crate) fn write_inner_detached(
        &self,
    ) -> Result<
        std::sync::RwLockWriteGuard<'_, formualizer::workbook::Workbook>,
        formualizer::workbook::IoError,
    > {
        if reentrancy::is_active(self.workbook_id()) {
            return Err(formualizer::workbook::IoError::Backend {
                backend: "workbook".to_string(),
                message: REENTRANCY_MESSAGE.to_string(),
            });
        }
        self.inner.write().map_err(|e| lock_error_to_io(&e))
    }

    pub(crate) fn with_workbook_mut<T, F>(&self, f: F) -> PyResult<T>
    where
        F: FnOnce(&mut formualizer::workbook::Workbook) -> PyResult<T>,
    {
        let mut wb = self.write_inner()?;
        f(&mut wb)
    }
}

fn resolve_workbook_config(
    mode: Option<PyWorkbookMode>,
    config: Option<PyWorkbookConfig>,
    enable_changelog: Option<bool>,
    span_evaluation: Option<bool>,
) -> PyResult<formualizer::workbook::WorkbookConfig> {
    let resolved = if let Some(cfg) = config {
        if let Some(requested) = mode {
            if requested != cfg.mode {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "mode conflicts with WorkbookConfig.mode",
                ));
            }
        }
        let mut base = match cfg.mode {
            PyWorkbookMode::Ephemeral => formualizer::workbook::WorkbookConfig::ephemeral(),
            PyWorkbookMode::Interactive => formualizer::workbook::WorkbookConfig::interactive(),
        };
        if let Some(eval) = cfg.eval {
            merge_python_eval_config(&mut base.eval, &eval);
        } else {
            apply_binding_eval_defaults(&mut base.eval);
        }
        match (cfg.enable_changelog, enable_changelog) {
            (Some(config_value), Some(argument_value)) if config_value != argument_value => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "enable_changelog conflicts with WorkbookConfig.enable_changelog",
                ));
            }
            (Some(enabled), None) | (None, Some(enabled)) | (Some(enabled), Some(_)) => {
                base = base.with_changelog(enabled);
            }
            (None, None) => {}
        }
        match (cfg.span_evaluation, span_evaluation) {
            (Some(config_value), Some(argument_value)) if config_value != argument_value => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "span_evaluation conflicts with WorkbookConfig.span_evaluation",
                ));
            }
            (Some(enabled), None) | (None, Some(enabled)) => {
                base = base.with_span_evaluation(enabled);
            }
            (Some(_), Some(_)) | (None, None) => {}
        }
        base
    } else {
        let mut base = match mode.unwrap_or(PyWorkbookMode::Interactive) {
            PyWorkbookMode::Ephemeral => formualizer::workbook::WorkbookConfig::ephemeral(),
            PyWorkbookMode::Interactive => formualizer::workbook::WorkbookConfig::interactive(),
        };
        apply_binding_eval_defaults(&mut base.eval);
        if let Some(enabled) = enable_changelog {
            base = base.with_changelog(enabled);
        }
        if let Some(enabled) = span_evaluation {
            base = base.with_span_evaluation(enabled);
        }
        base
    };

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_XLSX_BYTE_BACKEND, PyWorkbook, PyWorkbookConfig, resolve_workbook_config};

    /// Poisons `lock` by panicking while holding its write guard.
    fn poison<T>(lock: &std::sync::RwLock<T>) {
        let _ = std::panic::catch_unwind(|| {
            let _guard = lock.write().unwrap();
            panic!("poison the lock");
        });
        assert!(lock.is_poisoned());
    }

    #[test]
    fn a_poisoned_workbook_lock_is_not_recovered() {
        let wb = PyWorkbook::from_inner_workbook(formualizer::workbook::Workbook::new());
        assert!(wb.read_inner().is_ok());
        poison(&wb.inner);
        assert!(wb.write_inner().is_err());
        assert!(wb.read_inner().is_err());
    }
    use crate::enums::PyWorkbookMode;
    use formualizer::eval::engine::{EvalConfig, FormulaPlaneMode};

    #[test]
    fn resolve_workbook_config_applies_host_default_without_explicit_eval_config() {
        let resolved =
            resolve_workbook_config(None, None, None, None).expect("resolve workbook config");
        assert_eq!(
            resolved.eval.enable_parallel,
            !cfg!(target_os = "emscripten")
        );
        assert!(!resolved.enable_changelog);
        assert!(resolved.eval.defer_graph_building);
        assert_eq!(resolved.eval.formula_plane_mode, FormulaPlaneMode::Off);
    }

    #[cfg(not(target_os = "emscripten"))]
    #[test]
    fn xlsx_byte_loads_default_to_native() {
        assert_eq!(DEFAULT_XLSX_BYTE_BACKEND, "native");
    }

    #[test]
    fn resolve_workbook_config_preserves_explicit_eval_override() {
        let explicit = EvalConfig {
            enable_parallel: true,
            ..EvalConfig::default()
        };
        let cfg = PyWorkbookConfig::new(PyWorkbookMode::Interactive, None, Some(false), None);
        let cfg = PyWorkbookConfig {
            eval: Some(explicit.clone()),
            ..cfg
        };

        let resolved =
            resolve_workbook_config(None, Some(cfg), None, None).expect("resolve workbook config");
        assert_eq!(resolved.eval.enable_parallel, explicit.enable_parallel);
        assert!(!resolved.enable_changelog);
        assert!(resolved.eval.defer_graph_building);
        assert_eq!(resolved.eval.formula_plane_mode, FormulaPlaneMode::Off);
    }

    #[test]
    fn resolve_workbook_config_accepts_span_evaluation_opt_in_argument() {
        let resolved =
            resolve_workbook_config(None, None, None, Some(true)).expect("resolve workbook config");
        assert_eq!(
            resolved.eval.formula_plane_mode,
            FormulaPlaneMode::AuthoritativeExperimental
        );
    }

    #[test]
    fn resolve_workbook_config_accepts_span_evaluation_opt_in_config() {
        let cfg = PyWorkbookConfig::new(PyWorkbookMode::Interactive, None, None, Some(true));
        let resolved =
            resolve_workbook_config(None, Some(cfg), None, None).expect("resolve workbook config");
        assert_eq!(
            resolved.eval.formula_plane_mode,
            FormulaPlaneMode::AuthoritativeExperimental
        );
    }

    #[test]
    fn resolve_workbook_config_accepts_enable_changelog_opt_in_argument_and_detects_conflict() {
        let resolved =
            resolve_workbook_config(None, None, Some(true), None).expect("resolve workbook config");
        assert!(resolved.enable_changelog);

        let cfg = PyWorkbookConfig::new(PyWorkbookMode::Interactive, None, Some(true), None);
        assert!(resolve_workbook_config(None, Some(cfg), Some(false), None).is_err());
    }
}
