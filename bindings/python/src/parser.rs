use crate::ast::PyASTNode;
use crate::enums::PyFormulaDialect;
use crate::errors::ParserError;
use crate::tokenizer::PyTokenizer;
use formualizer::parse::parser::parse_with_dialect;
use formualizer::parse::types::FormulaDialect;
use pyo3::prelude::*;
#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};

/// Stateful formula parser.
///
/// Most users can use the top-level [`parse`] function, but `Parser` is useful
/// if you want to parse multiple formulas with the same instance.
///
/// Example:
/// ```python
///     import formualizer as fz
///
///     p = fz.Parser()
///     ast = p.parse_string("=1+2")
///     print(ast.pretty())
/// ```
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "Parser", module = "formualizer.formualizer_py")]
pub struct PyParser {
    _phantom: std::marker::PhantomData<()>,
}

impl Default for PyParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
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
        let _ = include_whitespace;
        let dialect: FormulaDialect = dialect
            .map(Into::into)
            .unwrap_or_else(|| tokenizer.dialect().into());
        let formula = tokenizer.render_formula();
        let ast = parse_with_dialect(&formula, dialect)
            .map_err(|e| ParserError::new_with_pos(e.message, e.position))?;
        Ok(PyASTNode::new(ast))
    }
}

/// Convenience function to parse a formula string directly
#[cfg_attr(
    not(target_os = "emscripten"),
    gen_stub_pyfunction(module = "formualizer.formualizer_py")
)]
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
