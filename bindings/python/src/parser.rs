use crate::ast::PyASTNode;
use crate::errors::{ParserError, TokenizerError};
use crate::tokenizer::PyTokenizer;
use formualizer_core::parser::Parser;
use pyo3::prelude::*;

#[pyclass(module = "formualizer")]
pub struct PyParser {
    _phantom: std::marker::PhantomData<()>,
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
    pub fn parse_string(&self, formula: &str) -> PyResult<PyASTNode> {
        parse_formula(formula)
    }

    /// Parse from a tokenizer
    #[pyo3(signature = (tokenizer, include_whitespace = false))]
    pub fn parse_tokens(
        &self,
        tokenizer: &PyTokenizer,
        include_whitespace: bool,
    ) -> PyResult<PyASTNode> {
        let tokens = tokenizer
            .tokens()
            .into_iter()
            .map(|py_token| {
                // Extract the inner token - we need to access the inner field
                // This is a bit of a hack since we can't directly access private fields
                // Instead, we'll recreate the token from the public interface
                formualizer_core::tokenizer::Token::new(
                    py_token.value().to_string(),
                    py_token.token_type().into(),
                    py_token.subtype().into(),
                )
            })
            .collect();

        let mut parser = Parser::new(tokens, include_whitespace);
        let ast = parser
            .parse()
            .map_err(|e| ParserError::new_with_pos(e.message, e.position))?;
        Ok(PyASTNode::new(ast))
    }
}

/// Convenience function to parse a formula string directly
#[pyfunction]
pub fn parse_formula(formula: &str) -> PyResult<PyASTNode> {
    let mut parser = Parser::from(formula);
    let ast = parser
        .parse()
        .map_err(|e| ParserError::new_with_pos(e.message, e.position))?;
    Ok(PyASTNode::new(ast))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyParser>()?;
    m.add_function(wrap_pyfunction!(parse_formula, m)?)?;

    Ok(())
}
