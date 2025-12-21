use crate::ast::PyASTNode;
use crate::enums::PyFormulaDialect;
use crate::errors::ParserError;
use crate::tokenizer::PyTokenizer;
use formualizer::parse::parser::{Parser, parse_with_dialect};
use formualizer::parse::types::FormulaDialect;
use pyo3::prelude::*;

#[pyclass(module = "formualizer")]
pub struct PyParser {
    _phantom: std::marker::PhantomData<()>,
}

impl Default for PyParser {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl PyParser {
    #[new]
    pub fn new() -> Self {
        PyParser {
            _phantom: std::marker::PhantomData,
        }
    }

    /// Parse a formula string into an AST
    #[pyo3(signature = (formula, dialect = None))]
    pub fn parse_string(
        &self,
        formula: &str,
        dialect: Option<PyFormulaDialect>,
    ) -> PyResult<PyASTNode> {
        parse_formula_impl(formula, dialect)
    }

    /// Parse from a tokenizer
    #[pyo3(signature = (tokenizer, include_whitespace = false, dialect = None))]
    pub fn parse_tokens(
        &self,
        tokenizer: &PyTokenizer,
        include_whitespace: bool,
        dialect: Option<PyFormulaDialect>,
    ) -> PyResult<PyASTNode> {
        let tokens = tokenizer
            .tokens()
            .into_iter()
            .map(|py_token| {
                // Extract the inner token - we need to access the inner field
                // This is a bit of a hack since we can't directly access private fields
                // Instead, we'll recreate the token from the public interface
                formualizer::parse::tokenizer::Token::new(
                    py_token.value().to_string(),
                    py_token.token_type().into(),
                    py_token.subtype().into(),
                )
            })
            .collect();

        let dialect: FormulaDialect = dialect
            .map(Into::into)
            .unwrap_or_else(|| tokenizer.dialect().into());
        let mut parser = Parser::new_with_dialect(tokens, include_whitespace, dialect);
        let ast = parser
            .parse()
            .map_err(|e| ParserError::new_with_pos(e.message, e.position))?;
        Ok(PyASTNode::new(ast))
    }
}

/// Convenience function to parse a formula string directly
#[pyfunction]
#[pyo3(signature = (formula, dialect = None))]
pub fn parse_formula(formula: &str, dialect: Option<PyFormulaDialect>) -> PyResult<PyASTNode> {
    parse_formula_impl(formula, dialect)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyParser>()?;
    m.add_function(wrap_pyfunction!(parse_formula, m)?)?;

    Ok(())
}

fn parse_formula_impl(formula: &str, dialect: Option<PyFormulaDialect>) -> PyResult<PyASTNode> {
    let dialect: FormulaDialect = dialect.map(Into::into).unwrap_or_default();
    let ast = parse_with_dialect(formula, dialect)
        .map_err(|e| ParserError::new_with_pos(e.message, e.position))?;
    Ok(PyASTNode::new(ast))
}
