use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use formualizer::common::LiteralValue;
use formualizer::common::error::{ExcelError, ExcelErrorKind};

use crate::engine::{
    PyEvaluationConfig, apply_binding_eval_defaults, eval_plan_to_py, merge_python_eval_config,
};
use crate::enums::PyWorkbookMode;
use crate::errors::workbook_error_to_pyerr;
use crate::value::{literal_to_py, py_to_literal};
use std::collections::HashMap;

type SheetCellMap = HashMap<(u32, u32), CellData>;
type SheetCache = HashMap<String, SheetCellMap>;

type PyObject = pyo3::Py<pyo3::PyAny>;

#[cfg(not(target_os = "emscripten"))]
pub(crate) const DEFAULT_XLSX_BYTE_BACKEND: &str = "calamine";

// Pyodide currently excludes Calamine because its Rust sysroot is older than
// Calamine 0.36's MSRV. Keep the existing Umya byte path as its default.
#[cfg(target_os = "emscripten")]
pub(crate) const DEFAULT_XLSX_BYTE_BACKEND: &str = "umya";

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
    // Compatibility cache for old sheet API used by some wrappers
    pub(crate) sheets: std::sync::Arc<std::sync::RwLock<SheetCache>>,
    cancel_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyWorkbook {
    #[new]
    #[pyo3(signature = (*, mode=None, config=None, span_evaluation=None))]
    pub fn new(
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
        span_evaluation: Option<bool>,
    ) -> PyResult<Self> {
        let cfg = resolve_workbook_config(mode, config, span_evaluation)?;
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
    ///     backend: Backend name (currently defaults to `calamine`).
    ///     mode/config: Optional workbook configuration.
    ///
    /// Example:
    /// ```python
    ///     import formualizer as fz
    ///
    ///     wb = fz.Workbook.load_path("model.xlsx")
    ///     print(wb.sheet_names)
    /// ```
    #[classmethod]
    #[pyo3(signature = (path, strategy=None, backend=None, *, mode=None, config=None, span_evaluation=None))]
    pub fn load_path(
        _cls: &Bound<'_, pyo3::types::PyType>,
        path: &str,
        strategy: Option<&str>,
        backend: Option<&str>,
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
        span_evaluation: Option<bool>,
    ) -> PyResult<Self> {
        let _ = strategy; // currently unused, default eager
        Self::from_path(_cls, path, backend, mode, config, span_evaluation)
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
    #[pyo3(signature = (path, backend=None, *, mode=None, config=None, span_evaluation=None))]
    pub fn from_path(
        _cls: &Bound<'_, pyo3::types::PyType>,
        path: &str,
        backend: Option<&str>,
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
        span_evaluation: Option<bool>,
    ) -> PyResult<Self> {
        let backend = backend.unwrap_or("calamine");
        let cfg = resolve_workbook_config(mode, config, span_evaluation)?;
        match backend {
            "calamine" => {
                #[cfg(target_os = "emscripten")]
                {
                    let _ = (path, cfg);
                    Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                        "backend='calamine' is unavailable in the Pyodide build; use backend='umya' with in-memory XLSX bytes",
                    ))
                }
                #[cfg(not(target_os = "emscripten"))]
                {
                    use formualizer::workbook::backends::CalamineAdapter;
                    use formualizer::workbook::traits::SpreadsheetReader;
                    let adapter = <CalamineAdapter as SpreadsheetReader>::open_path(
                        std::path::Path::new(path),
                    )
                    .map_err(|e| {
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
            }
            "umya" => {
                use formualizer::workbook::backends::UmyaAdapter;
                use formualizer::workbook::traits::SpreadsheetReader;
                let adapter =
                    <UmyaAdapter as SpreadsheetReader>::open_path(std::path::Path::new(path))
                        .map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                                "open failed: {e}"
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
    ///     backend: Backend name. Defaults to `calamine` in native Python builds
    ///         and `umya` in Pyodide builds where Calamine is unavailable.
    ///     mode/config: Optional workbook configuration.
    #[classmethod]
    #[pyo3(signature = (data, backend=None, *, mode=None, config=None, span_evaluation=None))]
    pub fn from_bytes<'py>(
        _cls: &Bound<'py, pyo3::types::PyType>,
        data: &Bound<'py, PyBytes>,
        backend: Option<&str>,
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
        span_evaluation: Option<bool>,
    ) -> PyResult<Self> {
        let cfg = resolve_workbook_config(mode, config, span_evaluation)?;
        Self::from_bytes_impl(
            data.as_bytes().to_vec(),
            backend.unwrap_or(DEFAULT_XLSX_BYTE_BACKEND),
            cfg,
        )
    }

    /// Serialize the current workbook contents into XLSX bytes.
    ///
    /// Notes:
    /// - This currently uses the `umya` backend.
    /// - Output is generated from the in-memory workbook model; original XLSX styling
    ///   and package metadata are not preserved by the Python binding.
    #[pyo3(signature = (backend=None))]
    pub fn to_xlsx_bytes<'py>(
        &self,
        py: Python<'py>,
        backend: Option<&str>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        match backend.unwrap_or("umya") {
            "umya" => {
                let wb = self.read_inner()?;
                let bytes = wb.to_xlsx_bytes().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("save failed: {e}"))
                })?;
                Ok(PyBytes::new(py, &bytes))
            }
            "calamine" => Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                "backend='calamine' does not currently support XLSX byte export; use backend='umya'",
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
        let mut sheets = self.sheets.write().unwrap();
        sheets.entry(name.to_string()).or_default();
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
    /// `evaluate_cell`, `to_xlsx_bytes`, `Sheet.get_cell`, ...) now raises
    /// `RuntimeError` when called from a callback rather than hanging forever.
    /// `get_value` may still return from the compatibility cache without
    /// raising, but that is a cache hit, not a supported operation — do not
    /// rely on it.
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

        let literal = py_to_literal(value)?;
        let mut wb = self.write_inner()?;
        wb.set_value(sheet, row, col, literal.clone())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        // Update compatibility cache
        let mut sheets = self.sheets.write().unwrap();
        let sheet_map = sheets.entry(sheet.to_string()).or_default();
        sheet_map.insert(
            (row, col),
            CellData {
                value: Some(literal),
                formula: None,
            },
        );
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
        // Update compatibility cache
        let mut sheets = self.sheets.write().unwrap();
        let sheet_map = sheets.entry(sheet.to_string()).or_default();
        sheet_map.insert(
            (row, col),
            CellData {
                value: None,
                formula: Some(formula.to_string()),
            },
        );
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

        let res = py.detach(|| self.write_inner_detached()?.evaluate_cell(sheet, row, col));
        let v = res.map_err(workbook_error_to_pyerr)?;
        literal_to_py(py, &v)
    }

    /// Pin the evaluation clock to a caller-supplied instant, so the
    /// volatile date/time builtins (TODAY, NOW) evaluate deterministically
    /// on the next recalculation. Takes effect on a live workbook; no
    /// reload is required.
    ///
    /// `deterministic_timezone` accepts `"utc"`, `"local"`, or a fixed
    /// offset in seconds — the same spelling as
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

    pub fn evaluate_all(&self, py: Python<'_>) -> PyResult<()> {
        // Ensure flag is reset before starting
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);

        py.detach(|| {
            self.write_inner_detached()?.evaluate_all_cancellable(
                formualizer::eval::engine::CancelToken::from_flag(self.cancel_flag.clone()),
            )
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

        // Ensure flag is reset
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // We use a temporary vector of (&str, u32, u32) because Workbook::evaluate_cells expects that
        let refs: Vec<(&str, u32, u32)> = target_vec
            .iter()
            .map(|(s, r, c)| (s.as_str(), *r, *c))
            .collect();

        let results = py
            .detach(|| {
                self.write_inner_detached()?.evaluate_cells_cancellable(
                    &refs,
                    formualizer::eval::engine::CancelToken::from_flag(self.cancel_flag.clone()),
                )
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

    pub fn cancel(&self) {
        self.cancel_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

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
        self.sheets.write().unwrap().clear();
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

        if let Some(cached) = {
            let sheets = self.sheets.read().unwrap();
            sheets.get(sheet).and_then(|m| m.get(&(row, col)).cloned())
        } {
            if let Some(value) = cached.value {
                return Ok(Some(literal_to_py(py, &value)?));
            }
        }
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
    pub fn undo(&self) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        {
            let mut sheets = self.sheets.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}"))
            })?;
            sheets.clear();
        }
        wb.undo()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    /// Redo the most recently undone edit.
    pub fn redo(&self) -> PyResult<()> {
        let mut wb = self.write_inner()?;
        {
            let mut sheets = self.sheets.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}"))
            })?;
            sheets.clear();
        }
        wb.redo()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    // Batch ops
    pub fn set_values_batch(
        &self,
        _py: Python<'_>,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        data: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<()> {
        validate_cell_coords(start_row, start_col)?;

        let mut rows_vec: Vec<Vec<LiteralValue>> = Vec::with_capacity(data.len());
        for row in data.iter() {
            let list: &Bound<'_, pyo3::types::PyList> = row.cast()?;
            let mut row_vals: Vec<LiteralValue> = Vec::with_capacity(list.len());
            for v in list.iter() {
                row_vals.push(py_to_literal(&v)?);
            }
            rows_vec.push(row_vals);
        }
        let mut wb = self.write_inner()?;
        // Auto-group batch changes into a single undoable action when changelog is enabled
        wb.begin_action("batch: set values".to_string());
        let res = wb
            .set_values(sheet, start_row, start_col, &rows_vec)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()));
        wb.end_action();
        res?;
        // Update compatibility cache
        {
            let mut sheets = self.sheets.write().unwrap();
            let sheet_map = sheets.entry(sheet.to_string()).or_default();
            for (r_off, row_vals) in rows_vec.into_iter().enumerate() {
                for (c_off, v) in row_vals.into_iter().enumerate() {
                    let r = start_row + (r_off as u32);
                    let c = start_col + (c_off as u32);
                    sheet_map.insert(
                        (r, c),
                        CellData {
                            value: Some(v),
                            formula: None,
                        },
                    );
                }
            }
        }
        Ok(())
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
        // Update compatibility cache
        {
            let mut sheets = self.sheets.write().unwrap();
            let sheet_map = sheets.entry(sheet.to_string()).or_default();
            for (r_off, row_vals) in rows_vec.into_iter().enumerate() {
                for (c_off, s) in row_vals.into_iter().enumerate() {
                    let r = start_row + (r_off as u32);
                    let c = start_col + (c_off as u32);
                    sheet_map.insert(
                        (r, c),
                        CellData {
                            value: None,
                            formula: Some(s),
                        },
                    );
                }
            }
        }
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
    m.add_class::<PyCycleTelemetry>()?;
    m.add_class::<PyCell>()?;
    Ok(())
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
             max_abs_delta_at_stop={}, nan_converged={}, elapsed_ms={})",
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
            self.elapsed_ms,
        )
    }
}

// Compatibility types used by engine/sheet wrappers
#[derive(Clone, Debug)]
pub struct CellData {
    pub value: Option<LiteralValue>,
    pub formula: Option<String>,
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
    fn from_inner_workbook(inner: formualizer::workbook::Workbook) -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::RwLock::new(inner)),
            sheets: std::sync::Arc::new(std::sync::RwLock::new(HashMap::new())),
            cancel_flag: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn from_bytes_impl(
        data: Vec<u8>,
        backend: &str,
        cfg: formualizer::workbook::WorkbookConfig,
    ) -> PyResult<Self> {
        match backend {
            "umya" => {
                use formualizer::workbook::backends::UmyaAdapter;
                use formualizer::workbook::traits::SpreadsheetReader;

                let adapter =
                    <UmyaAdapter as SpreadsheetReader>::open_bytes(data).map_err(|e| {
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
            "calamine" => {
                #[cfg(target_os = "emscripten")]
                {
                    let _ = (data, cfg);
                    Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                        "backend='calamine' is unavailable in the Pyodide build; use backend='umya' with in-memory XLSX bytes",
                    ))
                }
                #[cfg(not(target_os = "emscripten"))]
                {
                    use formualizer::workbook::backends::CalamineAdapter;
                    use formualizer::workbook::traits::SpreadsheetReader;

                    let adapter = <CalamineAdapter as SpreadsheetReader>::open_bytes(data)
                        .map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                                "open failed: {e}"
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
            }
            other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported backend: {other}"
            ))),
        }
    }

    /// Drop the legacy `sheets` compatibility cache. Mutations made through
    /// internal helpers (e.g. SheetPort) bypass it, so it must be invalidated
    /// for `get_value()` to stay correct.
    pub(crate) fn clear_sheet_cache(&self) {
        if let Ok(mut sheets) = self.sheets.write() {
            sheets.clear();
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
        self.inner
            .read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))
    }

    /// Exclusive access to the engine state, re-entrancy checked.
    pub(crate) fn write_inner(
        &self,
    ) -> PyResult<std::sync::RwLockWriteGuard<'_, formualizer::workbook::Workbook>> {
        self.check_reentrancy()?;
        self.inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))
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
        // Mutations performed through internal helpers (e.g. SheetPort) bypass the
        // legacy `sheets` cache; invalidate it so `get_value()` stays correct.
        self.sheets.write().unwrap().clear();

        let mut wb = self.write_inner()?;
        f(&mut wb)
    }
}

fn resolve_workbook_config(
    mode: Option<PyWorkbookMode>,
    config: Option<PyWorkbookConfig>,
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
        if let Some(enabled) = cfg.enable_changelog {
            base.enable_changelog = enabled;
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
        if let Some(enabled) = span_evaluation {
            base = base.with_span_evaluation(enabled);
        }
        base
    };

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_XLSX_BYTE_BACKEND, PyWorkbookConfig, resolve_workbook_config};
    use crate::enums::PyWorkbookMode;
    use formualizer::eval::engine::{EvalConfig, FormulaPlaneMode};

    #[test]
    fn resolve_workbook_config_applies_host_default_without_explicit_eval_config() {
        let resolved = resolve_workbook_config(None, None, None).expect("resolve workbook config");
        assert_eq!(
            resolved.eval.enable_parallel,
            !cfg!(target_os = "emscripten")
        );
        assert!(resolved.enable_changelog);
        assert!(resolved.eval.defer_graph_building);
        assert_eq!(resolved.eval.formula_plane_mode, FormulaPlaneMode::Off);
    }

    #[cfg(not(target_os = "emscripten"))]
    #[test]
    fn native_xlsx_byte_loads_default_to_calamine() {
        assert_eq!(DEFAULT_XLSX_BYTE_BACKEND, "calamine");
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
            resolve_workbook_config(None, Some(cfg), None).expect("resolve workbook config");
        assert_eq!(resolved.eval.enable_parallel, explicit.enable_parallel);
        assert!(!resolved.enable_changelog);
        assert!(resolved.eval.defer_graph_building);
        assert_eq!(resolved.eval.formula_plane_mode, FormulaPlaneMode::Off);
    }

    #[test]
    fn resolve_workbook_config_accepts_span_evaluation_opt_in_argument() {
        let resolved =
            resolve_workbook_config(None, None, Some(true)).expect("resolve workbook config");
        assert_eq!(
            resolved.eval.formula_plane_mode,
            FormulaPlaneMode::AuthoritativeExperimental
        );
    }

    #[test]
    fn resolve_workbook_config_accepts_span_evaluation_opt_in_config() {
        let cfg = PyWorkbookConfig::new(PyWorkbookMode::Interactive, None, None, Some(true));
        let resolved =
            resolve_workbook_config(None, Some(cfg), None).expect("resolve workbook config");
        assert_eq!(
            resolved.eval.formula_plane_mode,
            FormulaPlaneMode::AuthoritativeExperimental
        );
    }
}
