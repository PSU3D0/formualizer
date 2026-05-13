use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use formualizer::common::LiteralValue;
use formualizer::common::error::{ExcelError, ExcelErrorKind};

use crate::engine::{
    PyEvaluationConfig, apply_binding_eval_defaults, eval_plan_to_py, merge_python_eval_config,
};
use crate::enums::PyWorkbookMode;
use crate::value::{literal_to_py, py_to_literal};
use std::collections::HashMap;

type SheetCellMap = HashMap<(u32, u32), CellData>;
type SheetCache = HashMap<String, SheetCellMap>;

type PyObject = pyo3::Py<pyo3::PyAny>;

fn validate_cell_coords(row: u32, col: u32) -> PyResult<()> {
    if row == 0 || col == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Row/col are 1-based",
        ));
    }
    Ok(())
}

struct PyCustomFnHandler {
    callback: PyObject,
}

impl PyCustomFnHandler {
    fn new(callback: PyObject) -> Self {
        Self { callback }
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
#[gen_stub_pyclass]
#[pyclass(name = "WorkbookConfig", module = "formualizer")]
#[derive(Clone)]
pub struct PyWorkbookConfig {
    mode: PyWorkbookMode,
    eval: Option<formualizer::eval::engine::EvalConfig>,
    enable_changelog: Option<bool>,
    span_evaluation: Option<bool>,
}

#[gen_stub_pymethods]
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
#[gen_stub_pyclass]
#[pyclass(name = "Workbook", module = "formualizer")]
#[derive(Clone)]
pub struct PyWorkbook {
    inner: std::sync::Arc<std::sync::RwLock<formualizer::workbook::Workbook>>,
    // Compatibility cache for old sheet API used by some wrappers
    pub(crate) sheets: std::sync::Arc<std::sync::RwLock<SheetCache>>,
    cancel_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[gen_stub_pymethods]
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
            let mut wb = self.inner.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}"))
            })?;
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
                use formualizer::workbook::backends::CalamineAdapter;
                use formualizer::workbook::traits::SpreadsheetReader;
                let adapter =
                    <CalamineAdapter as SpreadsheetReader>::open_path(std::path::Path::new(path))
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
    ///     backend: Backend name. Defaults to `umya` because `calamine` byte-open
    ///         is not currently supported in this repository.
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
        Self::from_bytes_impl(data.as_bytes().to_vec(), backend.unwrap_or("umya"), cfg)
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
                let wb = self.inner.read().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}"))
                })?;
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
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.add_sheet(name)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let mut sheets = self.sheets.write().unwrap();
        sheets.entry(name.to_string()).or_default();
        Ok(())
    }

    #[getter]
    pub fn sheet_names(&self) -> PyResult<Vec<String>> {
        let wb = self
            .inner
            .read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        Ok(wb.sheet_names())
    }

    /// Register a workbook-local custom function backed by a Python callable.
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

        let handler = std::sync::Arc::new(PyCustomFnHandler::new(callback.clone().unbind()));
        let options = formualizer::workbook::CustomFnOptions {
            min_args,
            max_args,
            volatile,
            thread_safe,
            deterministic,
            allow_override_builtin,
        };

        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.register_custom_function(name, options, handler)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Unregister a previously registered workbook-local custom function.
    pub fn unregister_function(&self, name: &str) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.unregister_custom_function(name)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// List registered workbook-local custom functions and their options.
    pub fn list_functions(&self, py: Python<'_>) -> PyResult<PyObject> {
        let wb = self
            .inner
            .read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
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
        let wb = self
            .inner
            .read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;

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
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
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

        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
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

        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        let v = wb
            .evaluate_cell(sheet, row, col)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        literal_to_py(py, &v)
    }

    pub fn evaluate_all(&self) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;

        // Ensure flag is reset before starting
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);

        wb.evaluate_all_cancellable(self.cancel_flag.clone())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
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

        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;

        // Ensure flag is reset
        self.cancel_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // We use a temporary vector of (&str, u32, u32) because Workbook::evaluate_cells expects that
        let refs: Vec<(&str, u32, u32)> = target_vec
            .iter()
            .map(|(s, r, c)| (s.as_str(), *r, *c))
            .collect();

        let results = wb
            .evaluate_cells_cancellable(&refs, self.cancel_flag.clone())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

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

        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
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
        let wb = self
            .inner
            .read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        Ok(match wb.get_value(sheet, row, col) {
            Some(v) => Some(literal_to_py(py, &v)?),
            None => None,
        })
    }

    pub fn get_formula(&self, sheet: &str, row: u32, col: u32) -> PyResult<Option<String>> {
        validate_cell_coords(row, col)?;

        let wb = self
            .inner
            .read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        Ok(wb.get_formula(sheet, row, col))
    }

    // Changelog controls
    pub fn set_changelog_enabled(&self, enabled: bool) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.set_changelog_enabled(enabled);
        Ok(())
    }

    // Changelog metadata
    #[pyo3(signature = (actor_id=None))]
    pub fn set_actor_id(&self, actor_id: Option<String>) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.set_actor_id(actor_id);
        Ok(())
    }

    #[pyo3(signature = (correlation_id=None))]
    pub fn set_correlation_id(&self, correlation_id: Option<String>) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.set_correlation_id(correlation_id);
        Ok(())
    }

    #[pyo3(signature = (reason=None))]
    pub fn set_reason(&self, reason: Option<String>) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
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
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.begin_action(description.to_string());
        Ok(())
    }

    /// End the current grouped undo/redo action.
    pub fn end_action(&self) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.end_action();
        Ok(())
    }

    /// Undo the most recent workbook edit.
    pub fn undo(&self) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.undo()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Redo the most recently undone edit.
    pub fn redo(&self) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.redo()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
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
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
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
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
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
            let mut wb = self.inner.write().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}"))
            })?;
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
    Ok(())
}

// Compatibility types used by engine/sheet wrappers
#[derive(Clone, Debug)]
pub struct CellData {
    pub value: Option<LiteralValue>,
    pub formula: Option<String>,
}

#[gen_stub_pyclass]
#[pyclass(name = "Cell", module = "formualizer")]
pub struct PyCell {
    value: LiteralValue,
    formula: Option<String>,
}

impl PyCell {
    pub(crate) fn new(value: LiteralValue, formula: Option<String>) -> Self {
        Self { value, formula }
    }
}

#[gen_stub_pymethods]
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
            "calamine" => Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                "backend='calamine' does not currently support XLSX byte open; use backend='umya'",
            )),
            other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported backend: {other}"
            ))),
        }
    }

    pub(crate) fn with_workbook_mut<T, F>(&self, f: F) -> PyResult<T>
    where
        F: FnOnce(&mut formualizer::workbook::Workbook) -> PyResult<T>,
    {
        // Mutations performed through internal helpers (e.g. SheetPort) bypass the
        // legacy `sheets` cache; invalidate it so `get_value()` stays correct.
        self.sheets.write().unwrap().clear();

        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
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
    use super::{PyWorkbookConfig, resolve_workbook_config};
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
