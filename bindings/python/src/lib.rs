// See crates/formualizer-common/src/lib.rs for rationale. The nested-if form
// is kept so the Pyodide-matched Rust nightly (pre let-chain stabilization)
// still builds this crate.
#![allow(clippy::collapsible_if)]

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::wrap_pyfunction;
#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::define_stub_info_gatherer;
#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::derive::gen_stub_pyfunction;

// Swap the wheel's global allocator to jemalloc to avoid glibc per-thread
// arena fragmentation that causes the cross-call RSS staircase observed in
// issue #63. This only affects allocations inside this cdylib; CPython and
// other extension modules continue to use their own allocators.
//
// The cfg-gate mirrors `tikv-jemallocator`'s supported platforms:
//   * not target_env = "msvc"  (Windows MSVC is incompatible)
//   * not target_arch = "wasm32" (jemalloc cannot build for wasm)
// On unsupported platforms the feature is a no-op and the system allocator
// is used.
#[cfg(all(
    feature = "allocator-jemalloc",
    not(target_env = "msvc"),
    not(target_arch = "wasm32")
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod ast;
mod engine;
mod enums;
mod errors;
mod inspect;
mod parser;
mod reference;
mod sheet; // retain for compatibility
mod sheetport;
mod token;
mod tokenizer;
mod value;
mod workbook;

use ast::PyASTNode;
use enums::{PyFormulaDialect, PyXlsxPathSource};
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
/// ```python
///     import formualizer as fz
///
///     t = fz.tokenize("=SUM(A1:A3)")
///     print(t.render())
///
///     for tok in t:
///         print(tok.value, tok.token_type, tok.subtype, tok.start, tok.end)
/// ```
#[cfg_attr(
    not(target_os = "emscripten"),
    gen_stub_pyfunction(module = "formualizer.formualizer_py")
)]
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
/// ```python
///     from formualizer import parse
///     from formualizer.visitor import collect_references, collect_function_names
///
///     ast = parse("=SUMIFS(Revenue,Region,A1,Year,B1)")
///     print(ast.pretty())
///     print(ast.to_formula())
///     print(collect_references(ast))
///     print(collect_function_names(ast))
/// ```
#[cfg_attr(
    not(target_os = "emscripten"),
    gen_stub_pyfunction(module = "formualizer.formualizer_py")
)]
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
///     path_source: `XlsxPathSource.SHARED_FILE` (the safe default) or
///         `XlsxPathSource.DIRECT_MMAP`. Direct mmap retains an actual read-only
///         mapping; the underlying file must not be destructively modified or
///         truncated while the workbook loads.
///
/// Example:
/// ```python
///     import formualizer as fz
///
///     wb = fz.load_workbook(
///         "financial_model.xlsx",
///         path_source=fz.XlsxPathSource.DIRECT_MMAP,
///     )
///     print(wb.evaluate_cell("Summary", 1, 2))
/// ```
#[cfg_attr(
    not(target_os = "emscripten"),
    gen_stub_pyfunction(module = "formualizer.formualizer_py")
)]
#[pyfunction]
#[pyo3(signature = (path, strategy=None, *, path_source=None, span_evaluation=None))]
fn load_workbook(
    py: Python,
    path: &str,
    strategy: Option<&str>,
    path_source: Option<PyXlsxPathSource>,
    span_evaluation: Option<bool>,
) -> PyResult<workbook::PyWorkbook> {
    // Backward-compat convenience
    let _ = strategy; // placeholder, backend currently fixed to calamine
    workbook::PyWorkbook::from_path(
        &py.get_type::<workbook::PyWorkbook>(),
        path,
        Some("calamine"),
        path_source,
        None,
        None,
        span_evaluation,
    )
}

/// Load an XLSX workbook from in-memory bytes.
///
/// This is the byte-oriented counterpart to `load_workbook(...)`. Native Python
/// builds default to `calamine`; Pyodide defaults to `umya` because Calamine is
/// not currently compiled into that target.
#[cfg_attr(
    not(target_os = "emscripten"),
    gen_stub_pyfunction(module = "formualizer.formualizer_py")
)]
#[pyfunction]
#[pyo3(signature = (data, strategy=None, backend=None, *, span_evaluation=None))]
fn load_workbook_bytes<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyBytes>,
    strategy: Option<&str>,
    backend: Option<&str>,
    span_evaluation: Option<bool>,
) -> PyResult<workbook::PyWorkbook> {
    let _ = strategy; // placeholder, backend currently fixed to eager load
    workbook::PyWorkbook::from_bytes(
        &py.get_type::<workbook::PyWorkbook>(),
        data,
        Some(backend.unwrap_or(workbook::DEFAULT_XLSX_BYTE_BACKEND)),
        None,
        None,
        span_evaluation,
    )
}

/// Recalculate an XLSX workbook and write formula cached values back to file.
///
/// Args:
///     path: Input `.xlsx` path.
///     output: Optional output path. If omitted, updates `path` in-place.
///
/// Returns:
///     A summary dictionary containing total/per-sheet evaluated counts and errors.
///
/// Note:
///     Formula text is preserved. Cached-value typing follows the active
///     `umya-spreadsheet` implementation.
#[cfg_attr(
    not(target_os = "emscripten"),
    gen_stub_pyfunction(module = "formualizer.formualizer_py")
)]
#[pyfunction]
#[pyo3(signature = (path, output=None))]
fn recalculate_file(py: Python<'_>, path: &str, output: Option<&str>) -> PyResult<Py<PyAny>> {
    let input = std::path::Path::new(path);
    let output_path = output.map(std::path::Path::new);

    let summary = formualizer::workbook::recalculate_file(input, output_path).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("recalculate failed: {e}"))
    })?;

    let out = pyo3::types::PyDict::new(py);
    out.set_item("status", summary.status.as_str())?;
    out.set_item("evaluated", summary.evaluated)?;
    out.set_item("errors", summary.errors)?;
    out.set_item("total_formulas", summary.evaluated)?;
    out.set_item("total_errors", summary.errors)?;

    let sheets = pyo3::types::PyDict::new(py);
    for (name, stats) in summary.sheets {
        let s = pyo3::types::PyDict::new(py);
        s.set_item("evaluated", stats.evaluated)?;
        s.set_item("errors", stats.errors)?;
        sheets.set_item(name, s)?;
    }
    out.set_item("sheets", sheets)?;

    if !summary.error_summary.is_empty() {
        let errors = pyo3::types::PyDict::new(py);
        for (token, info) in summary.error_summary {
            let e = pyo3::types::PyDict::new(py);
            e.set_item("count", info.count)?;
            e.set_item("locations", info.locations)?;
            if info.locations_truncated > 0 {
                e.set_item("locations_truncated", info.locations_truncated)?;
            }
            errors.set_item(token, e)?;
        }
        out.set_item("error_summary", errors)?;
    }

    Ok(out.into_any().unbind())
}

#[cfg(not(target_os = "emscripten"))]
fn xlsx_result_to_py(
    py: Python<'_>,
    result: formualizer::workbook::XlsxRecalculateResult,
) -> PyResult<Py<PyAny>> {
    let out = pyo3::types::PyDict::new(py);
    out.set_item("bytes", PyBytes::new(py, &result.bytes))?;
    let summary = pyo3::types::PyDict::new(py);
    summary.set_item("status", result.summary.status.as_str())?;
    summary.set_item("evaluated", result.summary.evaluated)?;
    summary.set_item("errors", result.summary.errors)?;
    summary.set_item("total_formulas", result.summary.evaluated)?;
    summary.set_item("total_errors", result.summary.errors)?;
    let sheets = pyo3::types::PyDict::new(py);
    for (name, stats) in result.summary.sheets {
        let sheet = pyo3::types::PyDict::new(py);
        sheet.set_item("evaluated", stats.evaluated)?;
        sheet.set_item("errors", stats.errors)?;
        sheets.set_item(name, sheet)?;
    }
    summary.set_item("sheets", sheets)?;
    if !result.summary.error_summary.is_empty() {
        let errors = pyo3::types::PyDict::new(py);
        for (token, info) in result.summary.error_summary {
            let error = pyo3::types::PyDict::new(py);
            error.set_item("count", info.count)?;
            error.set_item("locations", info.locations)?;
            if info.locations_truncated > 0 {
                error.set_item("locations_truncated", info.locations_truncated)?;
            }
            errors.set_item(token, error)?;
        }
        summary.set_item("error_summary", errors)?;
    }
    out.set_item("summary", summary)?;
    out.set_item("formula_cells", result.formula_cells)?;
    out.set_item("cache_cells_changed", result.cache_cells_changed)?;
    out.set_item("worksheet_parts_changed", result.worksheet_parts_changed)?;
    Ok(out.into_any().unbind())
}

#[cfg(not(target_os = "emscripten"))]
/// Recalculate XLSX formula caches in memory without rewriting unrelated package parts.
/// Returns a dictionary with output ``bytes``, a ``summary``, and formula/cache/worksheet counts.
#[cfg_attr(
    not(target_os = "emscripten"),
    gen_stub_pyfunction(module = "formualizer.formualizer_py")
)]
#[pyfunction]
#[pyo3(signature = (data, *, error_location_limit=None))]
fn recalculate_xlsx_bytes(
    py: Python<'_>,
    data: &Bound<'_, PyBytes>,
    error_location_limit: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let mut options = formualizer::workbook::XlsxRecalculateOptions::default();
    if let Some(limit) = error_location_limit {
        options.error_location_limit = limit;
    }
    // Reject above the core's safe default before copying Python-owned bytes.
    if data.as_bytes().len() > options.limits.max_input_bytes {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "recalculate XLSX failed: input size limit exceeded",
        ));
    }
    let input = data.as_bytes().to_vec();
    let result = py
        .detach(move || formualizer::workbook::recalculate_xlsx_bytes(&input, options))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("recalculate XLSX failed: {e}"))
        })?;
    xlsx_result_to_py(py, result)
}

#[cfg(not(target_os = "emscripten"))]
/// Recalculate XLSX formula caches from a path using atomic output replacement.
#[cfg_attr(
    not(target_os = "emscripten"),
    gen_stub_pyfunction(module = "formualizer.formualizer_py")
)]
#[pyfunction]
#[pyo3(signature = (path, output=None, *, error_location_limit=None))]
fn recalculate_xlsx_file(
    py: Python<'_>,
    path: &str,
    output: Option<&str>,
    error_location_limit: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let mut options = formualizer::workbook::XlsxRecalculateOptions::default();
    if let Some(limit) = error_location_limit {
        options.error_location_limit = limit;
    }
    let input = std::path::PathBuf::from(path);
    let output = output.map(std::path::PathBuf::from);
    let result = py
        .detach(move || {
            formualizer::workbook::recalculate_xlsx_file(&input, output.as_deref(), options)
        })
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("recalculate XLSX failed: {e}"))
        })?;
    xlsx_result_to_py(py, result)
}

/// The main formualizer Python module
#[pymodule]
fn formualizer_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register all submodules
    enums::register(m)?;
    errors::register(m)?;
    inspect::register(m)?;
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
    m.add_function(wrap_pyfunction!(load_workbook_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(recalculate_file, m)?)?;
    #[cfg(not(target_os = "emscripten"))]
    m.add_function(wrap_pyfunction!(recalculate_xlsx_bytes, m)?)?;
    #[cfg(not(target_os = "emscripten"))]
    m.add_function(wrap_pyfunction!(recalculate_xlsx_file, m)?)?;

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
#[cfg(not(target_os = "emscripten"))]
define_stub_info_gatherer!(stub_info);
