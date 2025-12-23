use pyo3::prelude::*;

use formualizer::common::LiteralValue;

use crate::engine::{PyEvaluationConfig, eval_plan_to_py};
use crate::enums::PyWorkbookMode;
use crate::value::{literal_to_py, py_to_literal};
use std::collections::HashMap;

type SheetCellMap = HashMap<(u32, u32), CellData>;
type SheetCache = HashMap<String, SheetCellMap>;

type PyObject = pyo3::Py<pyo3::PyAny>;

#[pyclass(name = "WorkbookConfig", module = "formualizer")]
#[derive(Clone)]
pub struct PyWorkbookConfig {
    mode: PyWorkbookMode,
    eval: Option<formualizer::eval::engine::EvalConfig>,
    enable_changelog: Option<bool>,
}

#[pymethods]
impl PyWorkbookConfig {
    #[new]
    #[pyo3(signature = (*, mode = PyWorkbookMode::Interactive, eval_config = None, enable_changelog = None))]
    pub fn new(
        mode: PyWorkbookMode,
        eval_config: Option<PyEvaluationConfig>,
        enable_changelog: Option<bool>,
    ) -> Self {
        Self {
            mode,
            eval: eval_config.map(|c| c.inner),
            enable_changelog,
        }
    }

    fn __repr__(&self) -> String {
        let mode = match self.mode {
            PyWorkbookMode::Ephemeral => "ephemeral",
            PyWorkbookMode::Interactive => "interactive",
        };
        format!(
            "WorkbookConfig(mode={}, enable_changelog={:?})",
            mode, self.enable_changelog
        )
    }
}

#[pyclass(name = "Workbook", module = "formualizer")]
#[derive(Clone)]
pub struct PyWorkbook {
    inner: std::sync::Arc<std::sync::RwLock<formualizer::workbook::Workbook>>,
    // Compatibility cache for old sheet API used by some wrappers
    pub(crate) sheets: std::sync::Arc<std::sync::RwLock<SheetCache>>,
    cancel_flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[pymethods]
impl PyWorkbook {
    #[new]
    #[pyo3(signature = (*, mode=None, config=None))]
    pub fn new(mode: Option<PyWorkbookMode>, config: Option<PyWorkbookConfig>) -> PyResult<Self> {
        let cfg = resolve_workbook_config(mode, config)?;
        Ok(Self {
            inner: std::sync::Arc::new(std::sync::RwLock::new(
                formualizer::workbook::Workbook::new_with_config(cfg),
            )),
            sheets: std::sync::Arc::new(std::sync::RwLock::new(HashMap::new())),
            cancel_flag: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Class method: load a workbook from a file path
    #[classmethod]
    #[pyo3(signature = (path, strategy=None, backend=None, *, mode=None, config=None))]
    pub fn load_path(
        _cls: &Bound<'_, pyo3::types::PyType>,
        path: &str,
        strategy: Option<&str>,
        backend: Option<&str>,
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
    ) -> PyResult<Self> {
        let _ = strategy; // currently unused, default eager
        Self::from_path(_cls, path, backend, mode, config)
    }

    /// Get or create a sheet by name
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
    #[pyo3(signature = (path, backend=None, *, mode=None, config=None))]
    pub fn from_path(
        _cls: &Bound<'_, pyo3::types::PyType>,
        path: &str,
        backend: Option<&str>,
        mode: Option<PyWorkbookMode>,
        config: Option<PyWorkbookConfig>,
    ) -> PyResult<Self> {
        let backend = backend.unwrap_or("calamine");
        let cfg = resolve_workbook_config(mode, config)?;
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
                Ok(Self {
                    inner: std::sync::Arc::new(std::sync::RwLock::new(wb)),
                    sheets: std::sync::Arc::new(std::sync::RwLock::new(HashMap::new())),
                    cancel_flag: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                })
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported backend: {backend}"
            ))),
        }
    }

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

    pub fn set_value(
        &self,
        _py: Python<'_>,
        sheet: &str,
        row: u32,
        col: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
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

    pub fn set_formula(&self, sheet: &str, row: u32, col: u32, formula: &str) -> PyResult<()> {
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

    pub fn evaluate_cell(
        &self,
        py: Python<'_>,
        sheet: &str,
        row: u32,
        col: u32,
    ) -> PyResult<PyObject> {
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

    pub fn get_eval_plan(
        &self,
        targets: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<crate::engine::PyEvaluationPlan> {
        let mut target_vec = Vec::with_capacity(targets.len());
        for item in targets.iter() {
            let tuple: &Bound<'_, pyo3::types::PyTuple> = item.cast()?;
            let sheet: String = tuple.get_item(0)?.extract()?;
            let row: u32 = tuple.get_item(1)?.extract()?;
            let col: u32 = tuple.get_item(2)?.extract()?;
            if row == 0 || col == 0 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Row/col are 1-based",
                ));
            }
            target_vec.push((sheet, row, col));
        }

        let refs: Vec<(&str, u32, u32)> = target_vec
            .iter()
            .map(|(s, r, c)| (s.as_str(), *r, *c))
            .collect();

        let wb = self
            .inner
            .read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        let plan = wb
            .get_eval_plan(&refs)
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
        if let Some(cached) = {
            let sheets = self.sheets.read().unwrap();
            sheets.get(sheet).and_then(|m| m.get(&(row, col)).cloned())
        } && let Some(value) = cached.value
        {
            return Ok(Some(literal_to_py(py, &value)?));
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
    pub fn begin_action(&self, description: &str) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.begin_action(description.to_string());
        Ok(())
    }
    pub fn end_action(&self) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.end_action();
        Ok(())
    }
    pub fn undo(&self) -> PyResult<()> {
        let mut wb = self
            .inner
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("lock: {e}")))?;
        wb.undo()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }
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
) -> PyResult<formualizer::workbook::WorkbookConfig> {
    let resolved = if let Some(cfg) = config {
        if let Some(requested) = mode
            && requested != cfg.mode
        {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "mode conflicts with WorkbookConfig.mode",
            ));
        }
        let mut base = match cfg.mode {
            PyWorkbookMode::Ephemeral => formualizer::workbook::WorkbookConfig::ephemeral(),
            PyWorkbookMode::Interactive => formualizer::workbook::WorkbookConfig::interactive(),
        };
        if let Some(eval) = cfg.eval {
            base.eval = eval;
        }
        if let Some(enabled) = cfg.enable_changelog {
            base.enable_changelog = enabled;
        }
        base
    } else {
        match mode.unwrap_or(PyWorkbookMode::Interactive) {
            PyWorkbookMode::Ephemeral => formualizer::workbook::WorkbookConfig::ephemeral(),
            PyWorkbookMode::Interactive => formualizer::workbook::WorkbookConfig::interactive(),
        }
    };

    Ok(resolved)
}
