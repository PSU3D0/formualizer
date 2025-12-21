use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

mod ast;
mod engine;
mod enums;
mod errors;
mod parser;
mod reference;
mod sheet; // retain for compatibility
mod sheetport;
mod token;
mod tokenizer;
mod value;
mod workbook;

use ast::PyASTNode;
use enums::PyFormulaDialect;
use tokenizer::PyTokenizer;

/// Convenience function to tokenize a formula string
#[pyfunction]
#[pyo3(signature = (formula, dialect = None))]
fn tokenize(formula: &str, dialect: Option<PyFormulaDialect>) -> PyResult<PyTokenizer> {
    PyTokenizer::from_formula(formula, dialect)
}

/// Convenience function to parse a formula string
#[pyfunction]
#[pyo3(signature = (formula, dialect = None))]
fn parse(formula: &str, dialect: Option<PyFormulaDialect>) -> PyResult<PyASTNode> {
    parser::parse_formula(formula, dialect)
}

/// Load a workbook from a file path (convenience function)
#[pyfunction]
#[pyo3(signature = (path, strategy=None))]
fn load_workbook(py: Python, path: &str, strategy: Option<&str>) -> PyResult<workbook::PyWorkbook> {
    // Backward-compat convenience
    let _ = strategy; // placeholder, backend currently fixed to calamine
    workbook::PyWorkbook::from_path(
        &py.get_type::<workbook::PyWorkbook>(),
        path,
        Some("calamine"),
        None,
        None,
    )
}

/// The main formualizer Python module
#[pymodule]
fn formualizer_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
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
    sheetport::register(m)?;
    // Convenience functions
    m.add_function(wrap_pyfunction!(tokenize, m)?)?;
    m.add_function(wrap_pyfunction!(parse, m)?)?;
    m.add_function(wrap_pyfunction!(load_workbook, m)?)?;
    if let Ok(dialect) = m.getattr("PyFormulaDialect") {
        m.add("FormulaDialect", dialect)?;
    }

    Ok(())
}
