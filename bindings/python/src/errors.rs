use pyo3::exceptions::PyException;
use pyo3::prelude::*;

// Create custom exception types
pyo3::create_exception!(formualizer, TokenizerError, PyException);
pyo3::create_exception!(formualizer, ParserError, PyException);

// Helper functions to create errors with position information
impl TokenizerError {
    pub fn new_with_pos(message: String, pos: Option<usize>) -> PyErr {
        let error_msg = if let Some(p) = pos {
            format!("TokenizerError at position {}: {}", p, message)
        } else {
            format!("TokenizerError: {}", message)
        };
        PyErr::new::<TokenizerError, _>(error_msg)
    }
}

impl ParserError {
    pub fn new_with_pos(message: String, pos: Option<usize>) -> PyErr {
        let error_msg = if let Some(p) = pos {
            format!("ParserError at position {}: {}", p, message)
        } else {
            format!("ParserError: {}", message)
        };
        PyErr::new::<ParserError, _>(error_msg)
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("TokenizerError", m.py().get_type::<TokenizerError>())?;
    m.add("ParserError", m.py().get_type::<ParserError>())?;
    Ok(())
}
