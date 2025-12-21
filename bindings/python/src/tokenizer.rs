use crate::{enums::PyFormulaDialect, errors::TokenizerError, token::PyToken};
use formualizer::parse::tokenizer::Tokenizer;
use formualizer::parse::types::FormulaDialect;
use pyo3::prelude::*;

#[pyclass(module = "formualizer")]
pub struct PyTokenizer {
    inner: Tokenizer,
}

impl PyTokenizer {
    pub fn new(inner: Tokenizer) -> Self {
        PyTokenizer { inner }
    }
}

#[pymethods]
impl PyTokenizer {
    #[new]
    #[pyo3(signature = (formula, dialect = None))]
    pub fn from_formula(formula: &str, dialect: Option<PyFormulaDialect>) -> PyResult<Self> {
        let dialect: FormulaDialect = dialect.map(Into::into).unwrap_or_default();
        let tokenizer = Tokenizer::new_with_dialect(formula, dialect)
            .map_err(|e| TokenizerError::new_with_pos(e.message, Some(e.pos)))?;
        Ok(PyTokenizer::new(tokenizer))
    }

    /// Get all tokens as a list
    pub fn tokens(&self) -> Vec<PyToken> {
        self.inner
            .items
            .iter()
            .map(|token| PyToken::new(token.clone()))
            .collect()
    }

    /// Reconstruct the original formula from tokens
    fn render(&self) -> String {
        self.inner.render()
    }

    #[getter]
    pub fn dialect(&self) -> PyFormulaDialect {
        self.inner.dialect().into()
    }

    /// Make the tokenizer iterable
    fn __iter__(slf: PyRef<'_, Self>) -> PyTokenizerIter {
        let tokens = slf.tokens();
        PyTokenizerIter { tokens, index: 0 }
    }

    fn __len__(&self) -> usize {
        self.inner.items.len()
    }

    fn __getitem__(&self, index: isize) -> PyResult<PyToken> {
        let len = self.inner.items.len() as isize;
        let idx = if index < 0 { len + index } else { index };

        if idx < 0 || idx >= len {
            Err(pyo3::exceptions::PyIndexError::new_err(
                "Index out of range",
            ))
        } else {
            Ok(PyToken::new(self.inner.items[idx as usize].clone()))
        }
    }

    fn __repr__(&self) -> String {
        format!("Tokenizer({} tokens)", self.inner.items.len())
    }
}

#[pyclass(module = "formualizer")]
pub struct PyTokenizerIter {
    tokens: Vec<PyToken>,
    index: usize,
}

#[pymethods]
impl PyTokenizerIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyToken> {
        if slf.index < slf.tokens.len() {
            let token = slf.tokens[slf.index].clone();
            slf.index += 1;
            Some(token)
        } else {
            None
        }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTokenizer>()?;
    m.add_class::<PyTokenizerIter>()?;
    Ok(())
}
