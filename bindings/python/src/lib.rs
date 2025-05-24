use pyo3::prelude::*;

mod ast;
mod enums;
mod errors;
mod parser;
mod reference;
mod token;
mod tokenizer;

use ast::PyASTNode;
use tokenizer::PyTokenizer;

/// Convenience function to tokenize a formula string
#[pyfunction]
fn tokenize(formula: &str) -> PyResult<PyTokenizer> {
    PyTokenizer::from_formula(formula)
}

/// Convenience function to parse a formula string
#[pyfunction]
fn parse(formula: &str) -> PyResult<PyASTNode> {
    parser::parse_formula(formula)
}

/// The main formualizer Python module
#[pymodule]
fn formualizer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register all submodules
    enums::register(m)?;
    errors::register(m)?;
    token::register(m)?;
    tokenizer::register(m)?;
    ast::register(m)?;
    parser::register(m)?;
    reference::register(m)?;

    // Add convenience functions
    m.add_function(wrap_pyfunction!(tokenize, m)?)?;
    m.add_function(wrap_pyfunction!(parse, m)?)?;

    Ok(())
}
