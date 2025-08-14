use pyo3::prelude::*;
use pyo3_stub_gen::{define_stub_info_gatherer, derive::gen_stub_pyfunction};

mod ast;
mod engine;
mod enums;
mod errors;
mod parser;
mod reference;
mod resolver;
mod sheet;
mod token;
mod tokenizer;
mod value;
mod workbook;

use ast::PyASTNode;
use tokenizer::PyTokenizer;

/// Convenience function to tokenize a formula string
#[gen_stub_pyfunction]
#[pyfunction]
fn tokenize(formula: &str) -> PyResult<PyTokenizer> {
    PyTokenizer::from_formula(formula)
}

/// Convenience function to parse a formula string
#[gen_stub_pyfunction]
#[pyfunction]
fn parse(formula: &str) -> PyResult<PyASTNode> {
    parser::parse_formula(formula)
}

/// Load a workbook from a file path (convenience function)
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (path, strategy=None))]
fn load_workbook(py: Python, path: &str, strategy: Option<&str>) -> PyResult<workbook::PyWorkbook> {
    // Call the classmethod directly
    workbook::PyWorkbook::load_path(&py.get_type::<workbook::PyWorkbook>(), path, strategy)
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
    value::register(m)?;
    engine::register(m)?;
    workbook::register(m)?;
    sheet::register(m)?;

    // Add convenience functions
    m.add_function(wrap_pyfunction!(tokenize, m)?)?;
    m.add_function(wrap_pyfunction!(parse, m)?)?;
    m.add_function(wrap_pyfunction!(load_workbook, m)?)?;

    Ok(())
}

// Define a function to gather stub information
define_stub_info_gatherer!(stub_info);
