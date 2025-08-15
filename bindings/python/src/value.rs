use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use formualizer_common::error::{ExcelError, ExcelErrorKind};
use formualizer_common::value::LiteralValue;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::HashMap;

/// Python representation of a LiteralValue from the formula engine
#[gen_stub_pyclass]
#[pyclass(name = "LiteralValue")]
#[derive(Clone, Debug)]
pub struct PyLiteralValue {
    pub(crate) inner: LiteralValue,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLiteralValue {
    /// Extract as Python int; errors if not an Int
    pub fn as_int(&self) -> PyResult<i64> {
        match self.inner {
            LiteralValue::Int(v) => Ok(v),
            LiteralValue::Number(n) if n.fract() == 0.0 => Ok(n as i64),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "LiteralValue is not an Int",
            )),
        }
    }

    /// Extract as Python float; errors if not a Number/Int/Boolean
    pub fn as_number(&self) -> PyResult<f64> {
        match self.inner {
            LiteralValue::Number(n) => Ok(n),
            LiteralValue::Int(i) => Ok(i as f64),
            LiteralValue::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "LiteralValue is not a Number",
            )),
        }
    }
    /// Create an Int value
    #[staticmethod]
    pub fn int(value: i64) -> Self {
        PyLiteralValue {
            inner: LiteralValue::Int(value),
        }
    }

    /// Create a Number (float) value
    #[staticmethod]
    pub fn number(value: f64) -> Self {
        PyLiteralValue {
            inner: LiteralValue::Number(value),
        }
    }

    /// Create a Boolean value
    #[staticmethod]
    pub fn boolean(value: bool) -> Self {
        PyLiteralValue {
            inner: LiteralValue::Boolean(value),
        }
    }

    /// Create a Text value
    #[staticmethod]
    pub fn text(value: String) -> Self {
        PyLiteralValue {
            inner: LiteralValue::Text(value),
        }
    }

    /// Create an Empty value
    #[staticmethod]
    pub fn empty() -> Self {
        PyLiteralValue {
            inner: LiteralValue::Empty,
        }
    }

    /// Create a Date value
    #[staticmethod]
    pub fn date(year: i32, month: u32, day: u32) -> PyResult<Self> {
        NaiveDate::from_ymd_opt(year, month, day)
            .map(|d| PyLiteralValue {
                inner: LiteralValue::Date(d),
            })
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid date"))
    }

    /// Create a Time value
    #[staticmethod]
    pub fn time(hour: u32, minute: u32, second: u32) -> PyResult<Self> {
        NaiveTime::from_hms_opt(hour, minute, second)
            .map(|t| PyLiteralValue {
                inner: LiteralValue::Time(t),
            })
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid time"))
    }

    /// Create a DateTime value
    #[staticmethod]
    pub fn datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> PyResult<Self> {
        let date = NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid date"))?;
        let time = NaiveTime::from_hms_opt(hour, minute, second)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid time"))?;
        Ok(PyLiteralValue {
            inner: LiteralValue::DateTime(NaiveDateTime::new(date, time)),
        })
    }

    /// Create a Duration value
    #[staticmethod]
    pub fn duration(seconds: i64) -> Self {
        use chrono::Duration;
        PyLiteralValue {
            inner: LiteralValue::Duration(Duration::seconds(seconds)),
        }
    }

    /// Create an Array value from a 2D list
    #[staticmethod]
    pub fn array(_py: Python, values: Vec<Vec<PyLiteralValue>>) -> PyResult<Self> {
        // Validate rectangular array
        if !values.is_empty() {
            let expected_cols = values[0].len();
            for row in &values {
                if row.len() != expected_cols {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Array must be rectangular",
                    ));
                }
            }
        }

        let rust_values: Vec<Vec<LiteralValue>> = values
            .into_iter()
            .map(|row| row.into_iter().map(|v| v.inner).collect())
            .collect();

        Ok(PyLiteralValue {
            inner: LiteralValue::Array(rust_values),
        })
    }

    /// Create an Error value
    #[staticmethod]
    pub fn error(kind: &str, message: Option<String>) -> PyResult<Self> {
        let error_kind = match kind {
            "Div0" | "Div" => ExcelErrorKind::Div,
            "Ref" => ExcelErrorKind::Ref,
            "Name" => ExcelErrorKind::Name,
            "Value" => ExcelErrorKind::Value,
            "Num" => ExcelErrorKind::Num,
            "Null" => ExcelErrorKind::Null,
            "Na" | "NA" => ExcelErrorKind::Na,
            "Spill" => ExcelErrorKind::Spill,
            "Calc" => ExcelErrorKind::Calc,
            "Circ" => ExcelErrorKind::Circ,
            "Cancelled" => ExcelErrorKind::Cancelled,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid error kind: {}",
                    kind
                )))
            }
        };

        use formualizer_common::error::ExcelErrorExtra;
        Ok(PyLiteralValue {
            inner: LiteralValue::Error(ExcelError {
                kind: error_kind,
                message,
                context: None,
                extra: ExcelErrorExtra::None,
            }),
        })
    }

    /// Check if value is an Int
    #[getter]
    pub fn is_int(&self) -> bool {
        matches!(self.inner, LiteralValue::Int(_))
    }

    /// Check if value is a Number
    #[getter]
    pub fn is_number(&self) -> bool {
        matches!(self.inner, LiteralValue::Number(_))
    }

    /// Check if value is Boolean
    #[getter]
    pub fn is_boolean(&self) -> bool {
        matches!(self.inner, LiteralValue::Boolean(_))
    }

    /// Check if value is Text
    #[getter]
    pub fn is_text(&self) -> bool {
        matches!(self.inner, LiteralValue::Text(_))
    }

    /// Check if value is Empty
    #[getter]
    pub fn is_empty(&self) -> bool {
        matches!(self.inner, LiteralValue::Empty)
    }

    /// Check if value is Date
    #[getter]
    pub fn is_date(&self) -> bool {
        matches!(self.inner, LiteralValue::Date(_))
    }

    /// Check if value is Time
    #[getter]
    pub fn is_time(&self) -> bool {
        matches!(self.inner, LiteralValue::Time(_))
    }

    /// Check if value is DateTime
    #[getter]
    pub fn is_datetime(&self) -> bool {
        matches!(self.inner, LiteralValue::DateTime(_))
    }

    /// Check if value is Duration
    #[getter]
    pub fn is_duration(&self) -> bool {
        matches!(self.inner, LiteralValue::Duration(_))
    }

    /// Check if value is Array
    #[getter]
    pub fn is_array(&self) -> bool {
        matches!(self.inner, LiteralValue::Array(_))
    }

    /// Check if value is Error
    #[getter]
    pub fn is_error(&self) -> bool {
        matches!(self.inner, LiteralValue::Error(_))
    }

    /// Check if value is Pending
    #[getter]
    pub fn is_pending(&self) -> bool {
        matches!(self.inner, LiteralValue::Pending)
    }

    /// Get the type name of the value
    #[getter]
    pub fn type_name(&self) -> &str {
        match &self.inner {
            LiteralValue::Int(_) => "Int",
            LiteralValue::Number(_) => "Number",
            LiteralValue::Boolean(_) => "Boolean",
            LiteralValue::Text(_) => "Text",
            LiteralValue::Empty => "Empty",
            LiteralValue::Date(_) => "Date",
            LiteralValue::Time(_) => "Time",
            LiteralValue::DateTime(_) => "DateTime",
            LiteralValue::Duration(_) => "Duration",
            LiteralValue::Array(_) => "Array",
            LiteralValue::Error(_) => "Error",
            LiteralValue::Pending => "Pending",
        }
    }

    /// Convert to a Python object
    pub fn to_python(&self, py: Python) -> PyResult<PyObject> {
        match &self.inner {
            LiteralValue::Int(v) => Ok((*v).into_pyobject(py)?.into_any().to_object(py)),
            LiteralValue::Number(v) => Ok((*v).into_pyobject(py)?.into_any().to_object(py)),
            LiteralValue::Boolean(v) => Ok((*v).to_object(py)),
            LiteralValue::Text(v) => Ok(v.clone().into_pyobject(py)?.into_any().to_object(py)),
            LiteralValue::Empty => Ok(py.None()),
            LiteralValue::Date(d) => {
                let dict = PyDict::new(py);
                dict.set_item("type", "Date")?;
                dict.set_item("year", d.year())?;
                dict.set_item("month", d.month())?;
                dict.set_item("day", d.day())?;
                Ok(dict.into_pyobject(py)?.into_any().to_object(py))
            }
            LiteralValue::Time(t) => {
                let dict = PyDict::new(py);
                dict.set_item("type", "Time")?;
                dict.set_item("hour", t.hour())?;
                dict.set_item("minute", t.minute())?;
                dict.set_item("second", t.second())?;
                Ok(dict.into_pyobject(py)?.into_any().to_object(py))
            }
            LiteralValue::DateTime(dt) => {
                let dict = PyDict::new(py);
                dict.set_item("type", "DateTime")?;
                dict.set_item("year", dt.year())?;
                dict.set_item("month", dt.month())?;
                dict.set_item("day", dt.day())?;
                dict.set_item("hour", dt.hour())?;
                dict.set_item("minute", dt.minute())?;
                dict.set_item("second", dt.second())?;
                Ok(dict.into_pyobject(py)?.into_any().to_object(py))
            }
            LiteralValue::Duration(d) => {
                let dict = PyDict::new(py);
                dict.set_item("type", "Duration")?;
                dict.set_item("seconds", d.num_seconds())?;
                Ok(dict.into_pyobject(py)?.into_any().to_object(py))
            }
            LiteralValue::Array(arr) => {
                let py_list = PyList::empty(py);
                for row in arr {
                    let py_row = PyList::empty(py);
                    for val in row {
                        let py_val = PyLiteralValue { inner: val.clone() };
                        py_row.append(py_val.to_python(py)?)?;
                    }
                    py_list.append(py_row)?;
                }
                Ok(py_list.into_pyobject(py)?.into_any().to_object(py))
            }
            LiteralValue::Error(e) => {
                let dict = PyDict::new(py);
                dict.set_item("type", "Error")?;
                dict.set_item("kind", format!("{:?}", e.kind))?;
                if let Some(msg) = &e.message {
                    dict.set_item("message", msg)?;
                }
                if let Some(ctx) = &e.context {
                    if let Some(r) = ctx.row {
                        dict.set_item("row", r)?;
                    }
                    if let Some(c) = ctx.col {
                        dict.set_item("col", c)?;
                    }
                }
                Ok(dict.into_pyobject(py)?.into_any().to_object(py))
            }
            LiteralValue::Pending => {
                let dict = PyDict::new(py);
                dict.set_item("type", "Pending")?;
                Ok(dict.into_pyobject(py)?.into_any().to_object(py))
            }
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            LiteralValue::Int(v) => format!("LiteralValue.int({})", v),
            LiteralValue::Number(v) => format!("LiteralValue.number({})", v),
            LiteralValue::Boolean(v) => format!("LiteralValue.boolean({})", v),
            LiteralValue::Text(v) => format!("LiteralValue.text({:?})", v),
            LiteralValue::Empty => "LiteralValue.empty()".to_string(),
            LiteralValue::Date(d) => {
                format!(
                    "LiteralValue.date({}, {}, {})",
                    d.year(),
                    d.month(),
                    d.day()
                )
            }
            LiteralValue::Time(t) => {
                format!(
                    "LiteralValue.time({}, {}, {})",
                    t.hour(),
                    t.minute(),
                    t.second()
                )
            }
            LiteralValue::DateTime(dt) => {
                format!(
                    "LiteralValue.datetime({}, {}, {}, {}, {}, {})",
                    dt.year(),
                    dt.month(),
                    dt.day(),
                    dt.hour(),
                    dt.minute(),
                    dt.second()
                )
            }
            LiteralValue::Duration(d) => {
                format!("LiteralValue.duration({})", d.num_seconds())
            }
            LiteralValue::Array(arr) => {
                format!(
                    "LiteralValue.array({}x{})",
                    arr.len(),
                    arr.first().map_or(0, |r| r.len())
                )
            }
            LiteralValue::Error(e) => {
                if let Some(msg) = &e.message {
                    format!("LiteralValue.error({:?}, {:?})", e.kind, msg)
                } else {
                    format!("LiteralValue.error({:?})", e.kind)
                }
            }
            LiteralValue::Pending => "LiteralValue.pending()".to_string(),
        }
    }

    fn __str__(&self) -> String {
        match &self.inner {
            LiteralValue::Int(v) => v.to_string(),
            LiteralValue::Number(v) => v.to_string(),
            LiteralValue::Boolean(v) => v.to_string(),
            LiteralValue::Text(v) => v.clone(),
            LiteralValue::Empty => String::new(),
            LiteralValue::Date(d) => d.format("%Y-%m-%d").to_string(),
            LiteralValue::Time(t) => t.format("%H:%M:%S").to_string(),
            LiteralValue::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            LiteralValue::Duration(d) => format!("{}s", d.num_seconds()),
            LiteralValue::Array(_) => "[Array]".to_string(),
            LiteralValue::Error(e) => match &e.message {
                Some(m) if !m.is_empty() => format!("{}: {}", e.kind, m),
                _ => format!("{}", e.kind),
            },
            LiteralValue::Pending => "[Pending]".to_string(),
        }
    }

    /// If this is an error, return the error kind string; otherwise None
    #[getter]
    pub fn error_kind(&self) -> Option<String> {
        match &self.inner {
            LiteralValue::Error(e) => Some(format!("{:?}", e.kind)),
            _ => None,
        }
    }

    /// If this is an error, return the error message; otherwise None
    #[getter]
    pub fn error_message(&self) -> Option<String> {
        match &self.inner {
            LiteralValue::Error(e) => e.message.clone(),
            _ => None,
        }
    }

    /// If this is an error and has location, return (row, col); otherwise None
    #[getter]
    pub fn error_location(&self) -> Option<(u32, u32)> {
        match &self.inner {
            LiteralValue::Error(e) => e.context.as_ref().and_then(|c| Some((c.row?, c.col?))),
            _ => None,
        }
    }
}

impl From<LiteralValue> for PyLiteralValue {
    fn from(value: LiteralValue) -> Self {
        PyLiteralValue { inner: value }
    }
}

impl From<PyLiteralValue> for LiteralValue {
    fn from(value: PyLiteralValue) -> Self {
        value.inner
    }
}

/// Register the value module with Python
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyLiteralValue>()?;
    Ok(())
}
