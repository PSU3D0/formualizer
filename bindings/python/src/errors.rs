use formualizer::common::error::{
    ErrorContext, ExcelError as RustExcelError, ExcelErrorExtra, ExcelErrorKind,
};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;

// Create custom exception types
pyo3::create_exception!(formualizer, TokenizerError, PyException);
pyo3::create_exception!(formualizer, ParserError, PyException);
pyo3::create_exception!(formualizer, FormualizerHostError, PyException);
// Raised when evaluating a cell returns an Excel error value
pyo3::create_exception!(formualizer, ExcelEvaluationError, PyException);

type PyObject = pyo3::Py<pyo3::PyAny>;

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
#[pyclass(name = "ExcelError")]
#[derive(Clone, Debug)]
pub struct PyExcelError {
    pub(crate) inner: RustExcelError,
}

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
        match &self.inner.extra {
            ExcelErrorExtra::None => None,
            ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            } => {
                let dict = PyDict::new(py);
                let _ = dict.set_item("expected_rows", expected_rows);
                let _ = dict.set_item("expected_cols", expected_cols);
                Some(dict.into_pyobject(py).unwrap().unbind().into())
            }
        }
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
