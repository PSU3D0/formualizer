use formualizer_core::parser::{ReferenceType, TableReference, TableSpecifier};
use pyo3::prelude::*;

#[pyclass(module = "formualizer")]
#[derive(Clone, Debug)]
pub struct CellRef {
    #[pyo3(get)]
    pub sheet: Option<String>,
    #[pyo3(get)]
    pub row: u32,
    #[pyo3(get)]
    pub col: u32,
    #[pyo3(get)]
    pub abs_row: bool,
    #[pyo3(get)]
    pub abs_col: bool,
}

#[pymethods]
impl CellRef {
    #[new]
    #[pyo3(signature = (sheet, row, col, abs_row, abs_col))]
    fn new(sheet: Option<String>, row: u32, col: u32, abs_row: bool, abs_col: bool) -> Self {
        CellRef {
            sheet,
            row,
            col,
            abs_row,
            abs_col,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "CellRef(sheet={:?}, row={}, col={}, abs_row={}, abs_col={})",
            self.sheet, self.row, self.col, self.abs_row, self.abs_col
        )
    }

    fn __str__(&self) -> String {
        let col_str = number_to_column(self.col);
        let col_ref = if self.abs_col {
            format!("${}", col_str)
        } else {
            col_str
        };
        let row_ref = if self.abs_row {
            format!("${}", self.row)
        } else {
            self.row.to_string()
        };

        if let Some(ref sheet) = self.sheet {
            if sheet.contains(' ') || sheet.contains('!') {
                format!("'{}'!{}{}", sheet, col_ref, row_ref)
            } else {
                format!("{}!{}{}", sheet, col_ref, row_ref)
            }
        } else {
            format!("{}{}", col_ref, row_ref)
        }
    }
}

#[pyclass(module = "formualizer")]
#[derive(Clone)]
pub struct RangeRef {
    #[pyo3(get)]
    pub sheet: Option<String>,
    #[pyo3(get)]
    pub start: Option<CellRef>,
    #[pyo3(get)]
    pub end: Option<CellRef>,
}

#[pymethods]
impl RangeRef {
    #[new]
    #[pyo3(signature = (sheet, start, end))]
    fn new(sheet: Option<String>, start: Option<CellRef>, end: Option<CellRef>) -> Self {
        RangeRef { sheet, start, end }
    }

    fn __repr__(&self) -> String {
        format!(
            "RangeRef(sheet={:?}, start={:?}, end={:?})",
            self.sheet, self.start, self.end
        )
    }

    fn __str__(&self) -> String {
        let start_str = self.start.as_ref().map_or("".to_string(), |s| s.__str__());
        let end_str = self.end.as_ref().map_or("".to_string(), |e| e.__str__());
        format!("{}:{}", start_str, end_str)
    }
}

#[pyclass(module = "formualizer")]
#[derive(Clone)]
pub struct TableRef {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub spec: Option<String>,
}

#[pymethods]
impl TableRef {
    #[new]
    #[pyo3(signature = (name, spec))]
    fn new(name: String, spec: Option<String>) -> Self {
        TableRef { name, spec }
    }

    fn __repr__(&self) -> String {
        format!("TableRef(name='{}', spec={:?})", self.name, self.spec)
    }

    fn __str__(&self) -> String {
        if let Some(ref spec) = self.spec {
            format!("{}[{}]", self.name, spec)
        } else {
            self.name.clone()
        }
    }
}

#[pyclass(module = "formualizer")]
#[derive(Clone)]
pub struct NamedRangeRef {
    #[pyo3(get)]
    pub name: String,
}

#[pymethods]
impl NamedRangeRef {
    #[new]
    #[pyo3(signature = (name))]
    fn new(name: String) -> Self {
        NamedRangeRef { name }
    }

    fn __repr__(&self) -> String {
        format!("NamedRangeRef(name='{}')", self.name)
    }

    fn __str__(&self) -> String {
        self.name.clone()
    }
}

#[pyclass(module = "formualizer")]
#[derive(Clone)]
pub struct UnknownRef {
    #[pyo3(get)]
    pub raw: String,
}

#[pymethods]
impl UnknownRef {
    #[new]
    #[pyo3(signature = (raw))]
    fn new(raw: String) -> Self {
        UnknownRef { raw }
    }

    fn __repr__(&self) -> String {
        format!("UnknownRef(raw='{}')", self.raw)
    }

    fn __str__(&self) -> String {
        self.raw.clone()
    }
}

// Union type for all reference types
#[derive(FromPyObject)]
pub enum PyReferenceLike {
    Cell(CellRef),
    Range(RangeRef),
    Table(TableRef),
    NamedRange(NamedRangeRef),
    Unknown(UnknownRef),
}

impl IntoPy<PyObject> for PyReferenceLike {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            PyReferenceLike::Cell(cell) => cell.into_py(py),
            PyReferenceLike::Range(range) => range.into_py(py),
            PyReferenceLike::Table(table) => table.into_py(py),
            PyReferenceLike::NamedRange(named) => named.into_py(py),
            PyReferenceLike::Unknown(unknown) => unknown.into_py(py),
        }
    }
}

/// Convert a column number to Excel column letters (A, B, ..., Z, AA, AB, ...)
fn number_to_column(mut col: u32) -> String {
    let mut result = String::new();
    while col > 0 {
        col -= 1; // Adjust for 0-based indexing
        result.insert(0, (b'A' + (col % 26) as u8) as char);
        col /= 26;
    }
    result
}

/// Convert a ReferenceType to a PyReferenceLike
pub fn reference_type_to_py(ref_type: &ReferenceType, original: &str) -> PyReferenceLike {
    match ref_type {
        ReferenceType::Cell { sheet, row, col } => {
            // For now, assume absolute references (we could parse the original to detect $)
            let abs_row = original.contains(&format!("${}", row));
            let abs_col = original.contains(&format!("${}", number_to_column(*col)));

            PyReferenceLike::Cell(CellRef::new(sheet.clone(), *row, *col, abs_row, abs_col))
        }
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
        } => {
            let start = match (start_col, start_row) {
                (Some(col), Some(row)) => {
                    let abs_row = original.contains(&format!("${}", row));
                    let abs_col = original.contains(&format!("${}", number_to_column(*col)));
                    Some(CellRef::new(None, *row, *col, abs_row, abs_col))
                }
                _ => None,
            };

            let end = match (end_col, end_row) {
                (Some(col), Some(row)) => {
                    let abs_row = original.contains(&format!("${}", row));
                    let abs_col = original.contains(&format!("${}", number_to_column(*col)));
                    Some(CellRef::new(None, *row, *col, abs_row, abs_col))
                }
                _ => None,
            };

            PyReferenceLike::Range(RangeRef::new(sheet.clone(), start, end))
        }
        ReferenceType::Table(table_ref) => {
            let spec = table_ref.specifier.as_ref().map(|s| format!("{}", s));
            PyReferenceLike::Table(TableRef::new(table_ref.name.clone(), spec))
        }
        ReferenceType::NamedRange(name) => {
            PyReferenceLike::NamedRange(NamedRangeRef::new(name.clone()))
        }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CellRef>()?;
    m.add_class::<RangeRef>()?;
    m.add_class::<TableRef>()?;
    m.add_class::<NamedRangeRef>()?;
    m.add_class::<UnknownRef>()?;
    Ok(())
}
