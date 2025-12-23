use formualizer::parse::parser::ReferenceType;
use pyo3::conversion::IntoPyObject;
use pyo3::{prelude::*, types::PyType};

use crate::errors::ParserError;

#[derive(pyo3::FromPyObject, Clone, PartialEq, Eq, Hash)]
enum NumericOrStringColumn {
    Numeric(u32),
    String(String),
}

#[pyclass(frozen, eq, hash, module = "formualizer")]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
    #[pyo3(signature = (sheet, row, col, abs_row = true, abs_col = true))]
    fn new(
        sheet: Option<String>,
        row: u32,
        col: NumericOrStringColumn,
        abs_row: bool,
        abs_col: bool,
    ) -> Self {
        CellRef {
            sheet,
            row,
            col: match col {
                NumericOrStringColumn::Numeric(c) => c,
                NumericOrStringColumn::String(c) => {
                    let col_str = c.to_uppercase();

                    col_str
                        .chars()
                        .map(|c| c as u32 - b'A' as u32 + 1)
                        .sum::<u32>()
                }
            },
            abs_row,
            abs_col,
        }
    }

    #[classmethod]
    #[pyo3(signature = (reference, default_sheet = None))]
    fn from_string(
        _cls: &Bound<'_, PyType>,
        reference: &str,
        default_sheet: Option<&str>,
    ) -> Result<Self, PyErr> {
        match ReferenceType::from_string(reference) {
            Ok(ReferenceType::Cell {
                sheet,
                row,
                col,
                row_abs,
                col_abs,
            }) => {
                let sheet =
                    sheet.or_else(|| default_sheet.map(|default_sheet| default_sheet.to_string()));
                Ok(CellRef::new(
                    sheet,
                    row,
                    NumericOrStringColumn::Numeric(col),
                    row_abs,
                    col_abs,
                ))
            }
            Ok(_) => Err(ParserError::new_with_pos(
                "Invalid cell reference".to_string(),
                None,
            )),
            Err(e) => Err(PyErr::new::<ParserError, _>(e.to_string())),
        }
    }

    #[getter]
    fn str_col(&self) -> String {
        number_to_column(self.col)
    }

    fn __repr__(&self) -> String {
        let sheet = match &self.sheet {
            Some(s) => format!("{s:?}"),
            None => "None".to_string(),
        };
        format!(
            "CellRef(sheet={sheet}, row={row}, col={col})",
            row = self.row,
            col = self.col
        )
    }

    fn __str__(&self) -> String {
        let col_str = number_to_column(self.col);
        let col_ref = if self.abs_col {
            format!("${col_str}")
        } else {
            col_str
        };
        let row = self.row;
        let row_ref = if self.abs_row {
            format!("${row}")
        } else {
            row.to_string()
        };

        if let Some(ref sheet) = self.sheet {
            if sheet.contains(' ') || sheet.contains('!') {
                format!("'{sheet}'!{col_ref}{row_ref}")
            } else {
                format!("{sheet}!{col_ref}{row_ref}")
            }
        } else {
            format!("{col_ref}{row_ref}")
        }
    }
}

#[pyclass(frozen, eq, hash, module = "formualizer")]
#[derive(Clone, PartialEq, Eq, Hash)]
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
        let sheet = match &self.sheet {
            Some(s) => format!("{s:?}"),
            None => "None".to_string(),
        };
        let start = match &self.start {
            Some(s) => s.__str__(),
            None => "None".to_string(),
        };
        let end = match &self.end {
            Some(e) => e.__str__(),
            None => "None".to_string(),
        };
        format!("RangeRef(sheet={sheet}, start={start}, end={end})")
    }

    fn __str__(&self) -> String {
        let start_str = self.start.as_ref().map_or("".to_string(), |s| s.__str__());
        let end_str = self.end.as_ref().map_or("".to_string(), |e| e.__str__());
        format!("{start_str}:{end_str}")
    }
}

#[pyclass(frozen, eq, hash, module = "formualizer")]
#[derive(Clone, PartialEq, Eq, Hash)]
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
        format!(
            "TableRef(name='{name}', spec={spec:?})",
            name = self.name,
            spec = self.spec
        )
    }

    fn __str__(&self) -> String {
        if let Some(ref spec) = self.spec {
            format!("{name}[{spec}]", name = self.name, spec = spec)
        } else {
            self.name.clone()
        }
    }
}

#[pyclass(frozen, eq, hash, module = "formualizer")]
#[derive(Clone, PartialEq, Eq, Hash)]
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
        format!("NamedRangeRef(name='{name}')", name = self.name)
    }

    fn __str__(&self) -> String {
        self.name.clone()
    }
}

#[pyclass(frozen, eq, hash, module = "formualizer")]
#[derive(Clone, PartialEq, Eq, Hash)]
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
        format!("UnknownRef(raw='{raw}')", raw = self.raw)
    }

    fn __str__(&self) -> String {
        self.raw.clone()
    }
}

// Union type for all reference types
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ReferenceLike {
    Cell(CellRef),
    Range(RangeRef),
    Table(TableRef),
    NamedRange(NamedRangeRef),
    #[allow(dead_code)]
    Unknown(UnknownRef),
}

impl<'py> IntoPyObject<'py> for ReferenceLike {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ReferenceLike::Cell(cell) => cell.into_pyobject(py).map(Bound::into_any),
            ReferenceLike::Range(range) => range.into_pyobject(py).map(Bound::into_any),
            ReferenceLike::Table(table) => table.into_pyobject(py).map(Bound::into_any),
            ReferenceLike::NamedRange(named) => named.into_pyobject(py).map(Bound::into_any),
            ReferenceLike::Unknown(unknown) => unknown.into_pyobject(py).map(Bound::into_any),
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

/// Convert a ReferenceType to a ReferenceLike
pub fn reference_type_to_py(ref_type: &ReferenceType, _original: &str) -> ReferenceLike {
    match ref_type {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => ReferenceLike::Cell(CellRef::new(
            sheet.clone(),
            *row,
            NumericOrStringColumn::Numeric(*col),
            *row_abs,
            *col_abs,
        )),
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => {
            let start = match (start_col, start_row) {
                (Some(col), Some(row)) => Some(CellRef::new(
                    None,
                    *row,
                    NumericOrStringColumn::Numeric(*col),
                    *start_row_abs,
                    *start_col_abs,
                )),
                _ => None,
            };

            let end = match (end_col, end_row) {
                (Some(col), Some(row)) => Some(CellRef::new(
                    None,
                    *row,
                    NumericOrStringColumn::Numeric(*col),
                    *end_row_abs,
                    *end_col_abs,
                )),
                _ => None,
            };

            ReferenceLike::Range(RangeRef::new(sheet.clone(), start, end))
        }
        ReferenceType::External(ext) => ReferenceLike::Unknown(UnknownRef::new(ext.raw.clone())),
        ReferenceType::Table(table_ref) => {
            let spec = table_ref.specifier.as_ref().map(|s| format!("{s}"));
            ReferenceLike::Table(TableRef::new(table_ref.name.clone(), spec))
        }
        ReferenceType::NamedRange(name) => {
            ReferenceLike::NamedRange(NamedRangeRef::new(name.clone()))
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
