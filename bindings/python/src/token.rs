use crate::enums::{PyTokenSubType, PyTokenType};
use formualizer::parse::tokenizer::Token;
use pyo3::prelude::*;
use pyo3::types::PyDict;

type PyObject = pyo3::Py<pyo3::PyAny>;

#[pyclass(module = "formualizer")]
#[derive(Clone)]
pub struct PyToken {
    inner: Token,
}

impl PyToken {
    pub fn new(inner: Token) -> Self {
        PyToken { inner }
    }
}

#[pymethods]
impl PyToken {
    #[getter]
    pub fn value(&self) -> &str {
        &self.inner.value
    }

    #[getter]
    pub fn token_type(&self) -> PyTokenType {
        self.inner.token_type.into()
    }

    #[getter]
    pub fn subtype(&self) -> PyTokenSubType {
        self.inner.subtype.into()
    }

    #[getter]
    pub fn start(&self) -> usize {
        self.inner.start
    }

    #[getter]
    pub fn end(&self) -> usize {
        self.inner.end
    }

    fn __repr__(&self) -> String {
        format!(
            "Token(value='{}', token_type={:?}, subtype={:?})",
            self.inner.value,
            self.token_type(),
            self.subtype()
        )
    }

    fn __str__(&self) -> String {
        format!(
            "<{} subtype: {:?} value: {}>",
            self.token_type(),
            self.subtype(),
            self.inner.value
        )
    }

    fn to_dict(&self, py: Python<'_>) -> PyObject {
        let dict = PyDict::new(py);
        dict.set_item("value", self.value()).unwrap();
        dict.set_item("token_type", self.token_type().to_string())
            .unwrap();
        dict.set_item("subtype", self.subtype().to_string())
            .unwrap();
        dict.into()
    }

    /// Check if this token is an operator
    fn is_operator(&self) -> bool {
        self.inner.is_operator()
    }

    /// Get the precedence of this token (if it's an operator)
    fn get_precedence(&self) -> Option<(u8, String)> {
        self.inner.get_precedence().map(|(prec, assoc)| {
            let assoc_str = match assoc {
                formualizer::parse::tokenizer::Associativity::Left => "Left".to_string(),
                formualizer::parse::tokenizer::Associativity::Right => "Right".to_string(),
            };
            (prec, assoc_str)
        })
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyToken>()?;
    Ok(())
}
