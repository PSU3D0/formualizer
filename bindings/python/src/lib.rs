use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::gen_stub_pyfunction;

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

/// Tokenize a formula string into a structured [`Tokenizer`].
///
/// This is a convenience wrapper around `Tokenizer(formula, dialect=...)`.
///
/// Args:
///     formula: The formula string. It may optionally start with `=`.
///     dialect: Optional dialect hint (`FormulaDialect.Excel` or `FormulaDialect.OpenFormula`).
///
/// Returns:
///     A [`Tokenizer`] which can be iterated to yield [`Token`] objects.
///
/// Example:
///     ```python
///     import formualizer as fz
///
///     t = fz.tokenize("=SUM(A1:A3)")
///     print(t.render())
///
///     for tok in t:
///         print(tok.value, tok.token_type, tok.subtype, tok.start, tok.end)
///     ```
#[gen_stub_pyfunction(module = "formualizer")]
#[pyfunction]
#[pyo3(signature = (formula, dialect = None))]
fn tokenize(formula: &str, dialect: Option<PyFormulaDialect>) -> PyResult<PyTokenizer> {
    PyTokenizer::from_formula(formula, dialect)
}

/// Parse a formula string into an [`ASTNode`].
///
/// The returned AST supports analysis helpers like `.pretty()`, `.to_formula()`,
/// `.fingerprint()`, `.walk_refs()`, and reference extraction.
///
/// Args:
///     formula: The formula string. It may optionally start with `=`.
///     dialect: Optional dialect hint.
///
/// Example:
///     ```python
///     from formualizer import parse
///     from formualizer.visitor import collect_references, collect_function_names
///
///     ast = parse("=SUMIFS(Revenue,Region,A1,Year,B1)")
///     print(ast.pretty())
///     print(ast.to_formula())
///     print(collect_references(ast))
///     print(collect_function_names(ast))
///     ```
#[gen_stub_pyfunction(module = "formualizer")]
#[pyfunction]
#[pyo3(signature = (formula, dialect = None))]
fn parse(formula: &str, dialect: Option<PyFormulaDialect>) -> PyResult<PyASTNode> {
    parser::parse_formula(formula, dialect)
}

/// Load an XLSX workbook from a filesystem path.
///
/// This is a convenience wrapper around `Workbook.from_path(...)`.
///
/// Args:
///     path: Path to an `.xlsx` file.
///     strategy: Currently accepted for backward compatibility.
///         (The backend/strategy is currently fixed to `calamine` + eager load.)
///
/// Example:
///     ```python
///     import formualizer as fz
///
///     wb = fz.load_workbook("financial_model.xlsx")
///     print(wb.evaluate_cell("Summary", 1, 2))
///     ```
#[gen_stub_pyfunction(module = "formualizer")]
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

    // Backward-compatible aliases for older names which started with `Py...`.
    // These are not the preferred API, but keeping them avoids breaking existing callers.
    //
    // NOTE: keep in sync with `bindings/python/src/bin/stub_gen.rs` post-processing which
    // adds corresponding typing aliases.
    if let Ok(v) = m.getattr("Token") {
        m.add("PyToken", v)?;
    }
    if let Ok(v) = m.getattr("Tokenizer") {
        m.add("PyTokenizer", v)?;
    }
    if let Ok(v) = m.getattr("TokenizerIter") {
        m.add("PyTokenizerIter", v)?;
    }
    if let Ok(v) = m.getattr("RefWalker") {
        m.add("PyRefWalker", v)?;
    }
    if let Ok(v) = m.getattr("TokenType") {
        m.add("PyTokenType", v)?;
    }
    if let Ok(v) = m.getattr("TokenSubType") {
        m.add("PyTokenSubType", v)?;
    }
    if let Ok(v) = m.getattr("FormulaDialect") {
        m.add("PyFormulaDialect", v)?;
    }

    Ok(())
}

// Define a function to gather stub information.
// The function name `stub_info` is used by `src/bin/stub_gen.rs`.
define_stub_info_gatherer!(stub_info);
