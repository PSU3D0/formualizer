use formualizer::common::error::{
    ErrorContext, ExcelError as RustExcelError, ExcelErrorExtra, ExcelErrorKind,
};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

// Create custom exception types
pyo3::create_exception!(formualizer, TokenizerError, PyException);
pyo3::create_exception!(formualizer, ParserError, PyException);
pyo3::create_exception!(formualizer, FormualizerHostError, PyException);
// Raised when evaluating a cell returns an Excel error value
pyo3::create_exception!(formualizer, ExcelEvaluationError, PyException);

type PyObject = pyo3::Py<pyo3::PyAny>;

fn error_context_to_py(py: Python<'_>, context: &ErrorContext) -> PyObject {
    let dict = PyDict::new(py);
    let _ = dict.set_item("row", context.row);
    let _ = dict.set_item("col", context.col);
    let _ = dict.set_item("origin_row", context.origin_row);
    let _ = dict.set_item("origin_col", context.origin_col);
    let _ = dict.set_item("origin_sheet", &context.origin_sheet);
    dict.into_any().unbind()
}

fn error_extra_to_py(py: Python<'_>, extra: &ExcelErrorExtra) -> Option<PyObject> {
    let dict = PyDict::new(py);
    match extra {
        ExcelErrorExtra::None => return None,
        ExcelErrorExtra::Spill {
            expected_rows,
            expected_cols,
        } => {
            let _ = dict.set_item("expected_rows", expected_rows);
            let _ = dict.set_item("expected_cols", expected_cols);
        }
        ExcelErrorExtra::Resource { detail } => {
            let _ = dict.set_item("resource_reason", detail.reason.as_str());
            let _ = dict.set_item("limit", detail.limit);
            let _ = dict.set_item("observed", detail.observed);
            let _ = dict.set_item("request_id", detail.request_id);
        }
        ExcelErrorExtra::PreparationStale { reason } => {
            let _ = dict.set_item("preparation_stale_reason", reason.as_str());
        }
    }
    Some(dict.into_any().unbind())
}

/// Map a canonical engine error to the Python evaluation exception without
/// flattening its structured Excel-domain fields.
pub(crate) fn excel_error_to_pyerr(error: RustExcelError) -> PyErr {
    let pyerr = ExcelEvaluationError::new_err(error.to_string());
    Python::attach(|py| {
        let value = pyerr.value(py);
        let kind = format!("{:?}", error.kind);
        let _ = value.setattr("kind", &kind);
        let _ = value.setattr("excel_kind", kind);
        let _ = value.setattr("message", error.message.clone());
        let _ = value.setattr("excel_message", error.message.clone());

        let context = error
            .context
            .as_ref()
            .map(|context| error_context_to_py(py, context));
        let _ = value.setattr("context", context);
        if let Some(context) = &error.context {
            let _ = value.setattr("row", context.row);
            let _ = value.setattr("col", context.col);
            let _ = value.setattr("origin_row", context.origin_row);
            let _ = value.setattr("origin_col", context.origin_col);
            let _ = value.setattr("origin_sheet", &context.origin_sheet);
        }

        let extra = error_extra_to_py(py, &error.extra);
        let _ = value.setattr("extra", extra.as_ref().map(|extra| extra.bind(py)));
        if let ExcelErrorExtra::Resource { detail } = &error.extra {
            let _ = value.setattr("resource_reason", detail.reason.as_str());
            let _ = value.setattr("limit", detail.limit);
            let _ = value.setattr("observed", detail.observed);
            let _ = value.setattr("request_id", detail.request_id);
        }
        if let ExcelErrorExtra::PreparationStale { reason } = &error.extra {
            let _ = value.setattr("preparation_stale_reason", reason.as_str());
        }
    });
    pyerr
}

pub(crate) fn workbook_error_to_pyerr(error: formualizer::workbook::IoError) -> PyErr {
    match error {
        formualizer::workbook::IoError::Engine(error) => excel_error_to_pyerr(error),
        other => PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(other.to_string()),
    }
}

// Helper functions to create errors with position information
impl TokenizerError {
    pub fn new_with_pos(message: String, pos: Option<usize>) -> PyErr {
        let error_msg = if let Some(p) = pos {
            format!("TokenizerError at position {p}: {message}")
        } else {
            format!("TokenizerError: {message}")
        };
        PyErr::new::<TokenizerError, _>(error_msg)
    }
}

impl ParserError {
    pub fn new_with_pos(message: String, pos: Option<usize>) -> PyErr {
        let error_msg = if let Some(p) = pos {
            format!("ParserError at position {p}: {message}")
        } else {
            format!("ParserError: {message}")
        };
        PyErr::new::<ParserError, _>(error_msg)
    }
}

/// Python representation of Excel domain errors
#[gen_stub_pyclass]
#[pyclass(name = "ExcelError", module = "formualizer")]
#[derive(Clone, Debug)]
pub struct PyExcelError {
    pub(crate) inner: RustExcelError,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyExcelError {
    /// Create a new Excel error
    #[new]
    pub fn new(
        kind: &str,
        message: Option<String>,
        row: Option<u32>,
        col: Option<u32>,
        spill_rows: Option<u32>,
        spill_cols: Option<u32>,
    ) -> PyResult<Self> {
        let error_kind = match kind {
            "Div" | "Div0" => ExcelErrorKind::Div,
            "Ref" => ExcelErrorKind::Ref,
            "Name" => ExcelErrorKind::Name,
            "Value" => ExcelErrorKind::Value,
            "Num" => ExcelErrorKind::Num,
            "Null" => ExcelErrorKind::Null,
            "Na" => ExcelErrorKind::Na,
            "Spill" => ExcelErrorKind::Spill,
            "Calc" => ExcelErrorKind::Calc,
            "Circ" => ExcelErrorKind::Circ,
            "Cancelled" => ExcelErrorKind::Cancelled,
            "Error" => ExcelErrorKind::Error,
            "NImpl" => ExcelErrorKind::NImpl,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid error kind: {kind}"
                )));
            }
        };

        let context = if row.is_some() || col.is_some() {
            Some(ErrorContext {
                row,
                col,
                origin_row: None,
                origin_col: None,
                origin_sheet: None,
            })
        } else {
            None
        };

        let extra = if error_kind == ExcelErrorKind::Spill {
            if let (Some(rows), Some(cols)) = (spill_rows, spill_cols) {
                ExcelErrorExtra::Spill {
                    expected_rows: rows,
                    expected_cols: cols,
                }
            } else {
                ExcelErrorExtra::None
            }
        } else {
            ExcelErrorExtra::None
        };

        Ok(PyExcelError {
            inner: RustExcelError {
                kind: error_kind,
                message,
                context,
                extra,
            },
        })
    }

    /// Get the error kind
    #[getter]
    pub fn kind(&self) -> String {
        format!("{:?}", self.inner.kind)
    }

    /// Get the error message
    #[getter]
    pub fn message(&self) -> Option<String> {
        self.inner.message.clone()
    }

    /// Get error row (if set)
    #[getter]
    pub fn row(&self) -> Option<u32> {
        self.inner.context.as_ref().and_then(|c| c.row)
    }

    /// Get error column (if set)
    #[getter]
    pub fn col(&self) -> Option<u32> {
        self.inner.context.as_ref().and_then(|c| c.col)
    }

    /// Get extra error data
    #[getter]
    pub fn extra(&self, py: Python) -> Option<PyObject> {
        error_extra_to_py(py, &self.inner.extra)
    }

    /// Check if this is a #DIV/0! error
    #[getter]
    pub fn is_div(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Div)
    }

    /// Check if this is a #REF! error
    #[getter]
    pub fn is_ref(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Ref)
    }

    /// Check if this is a #NAME? error
    #[getter]
    pub fn is_name(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Name)
    }

    /// Check if this is a #VALUE! error
    #[getter]
    pub fn is_value(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Value)
    }

    /// Check if this is a #NUM! error
    #[getter]
    pub fn is_num(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Num)
    }

    /// Check if this is a #NULL! error
    #[getter]
    pub fn is_null(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Null)
    }

    /// Check if this is a #N/A error
    #[getter]
    pub fn is_na(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Na)
    }

    /// Check if this is a #SPILL! error
    #[getter]
    pub fn is_spill(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Spill)
    }

    /// Check if this is a #CALC! error
    #[getter]
    pub fn is_calc(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Calc)
    }

    /// Check if this is a circular reference error
    #[getter]
    pub fn is_circ(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Circ)
    }

    /// Check if this is a cancellation error
    #[getter]
    pub fn is_cancelled(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Cancelled)
    }

    /// Check if this is a #ERROR! error
    #[getter]
    pub fn is_error(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::Error)
    }

    /// Check if this is a #N/IMPL! error
    #[getter]
    pub fn is_nimpl(&self) -> bool {
        matches!(self.inner.kind, ExcelErrorKind::NImpl)
    }

    fn __repr__(&self) -> String {
        if let Some(msg) = &self.inner.message {
            format!("ExcelError({:?}, {:?})", self.inner.kind, msg)
        } else {
            format!("ExcelError({:?})", self.inner.kind)
        }
    }

    fn __str__(&self) -> String {
        match self.inner.kind {
            ExcelErrorKind::Div => "#DIV/0!".to_string(),
            ExcelErrorKind::Ref => "#REF!".to_string(),
            ExcelErrorKind::Name => "#NAME?".to_string(),
            ExcelErrorKind::Value => "#VALUE!".to_string(),
            ExcelErrorKind::Num => "#NUM!".to_string(),
            ExcelErrorKind::Null => "#NULL!".to_string(),
            ExcelErrorKind::Na => "#N/A".to_string(),
            ExcelErrorKind::Spill => "#SPILL!".to_string(),
            ExcelErrorKind::Calc => "#CALC!".to_string(),
            ExcelErrorKind::Circ => "#CIRC!".to_string(),
            ExcelErrorKind::Cancelled => "#CANCELLED!".to_string(),
            ExcelErrorKind::Error => "#ERROR!".to_string(),
            ExcelErrorKind::NImpl => "#N/IMPL!".to_string(),
        }
    }
}

impl From<RustExcelError> for PyExcelError {
    fn from(error: RustExcelError) -> Self {
        PyExcelError { inner: error }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("TokenizerError", m.py().get_type::<TokenizerError>())?;
    m.add("ParserError", m.py().get_type::<ParserError>())?;
    m.add(
        "FormualizerHostError",
        m.py().get_type::<FormualizerHostError>(),
    )?;
    m.add(
        "ExcelEvaluationError",
        m.py().get_type::<ExcelEvaluationError>(),
    )?;
    m.add_class::<PyExcelError>()?;
    Ok(())
}
