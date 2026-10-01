//! Private CLI bridge; Python owns signal handling and output streams.
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass(name = "_CliCancelToken", module = "formualizer.formualizer_py")]
pub struct CliCancelToken(formualizer_cli::CancelToken);

#[pymethods]
impl CliCancelToken {
    #[new]
    fn new() -> Self {
        Self(formualizer_cli::CancelToken::new())
    }

    fn cancel(&self) {
        self.0.cancel();
    }
}

#[pyfunction]
fn _run_cli<'py>(
    py: Python<'py>,
    argv: Vec<String>,
    cancel: PyRef<'_, CliCancelToken>,
) -> (i32, Bound<'py, PyBytes>, Bound<'py, PyBytes>) {
    let token = cancel.0.clone();
    let (code, stdout, stderr) = py.detach(move || {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let code = formualizer_cli::run(argv, &mut stdout, &mut stderr, Some(token));
        (code, stdout, stderr)
    });
    (code, PyBytes::new(py, &stdout), PyBytes::new(py, &stderr))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CliCancelToken>()?;
    m.add_function(wrap_pyfunction!(_run_cli, m)?)?;
    Ok(())
}
