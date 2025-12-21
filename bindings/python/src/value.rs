use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use formualizer::common::error::{ErrorContext, ExcelError, ExcelErrorKind};
use formualizer::common::value::LiteralValue;
use pyo3::conversion::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBool, PyDate, PyDateAccess, PyDateTime, PyDelta, PyDeltaAccess, PyDict, PyFloat,
    PyInt, PyList, PyString, PyTime, PyTimeAccess,
};

type PyObject = pyo3::Py<pyo3::PyAny>;

/// Python representation of a LiteralValue from the formula engine
#[pyclass(name = "LiteralValue")]
#[derive(Clone, Debug)]
pub struct PyLiteralValue {
    pub(crate) inner: LiteralValue,
}

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
                    "Invalid error kind: {kind}"
                )));
            }
        };

        use formualizer::common::error::ExcelErrorExtra;
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
        literal_to_py(py, &self.inner)
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            LiteralValue::Int(v) => format!("LiteralValue.int({v})"),
            LiteralValue::Number(v) => format!("LiteralValue.number({v})"),
            LiteralValue::Boolean(v) => format!("LiteralValue.boolean({v})"),
            LiteralValue::Text(v) => format!("LiteralValue.text({v:?})"),
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

    /// If this is an error and has an origin location, return (sheet, row, col); otherwise None
    #[getter]
    pub fn error_origin(&self) -> Option<(Option<String>, u32, u32)> {
        match &self.inner {
            LiteralValue::Error(e) => e
                .context
                .as_ref()
                .and_then(|c| Some((c.origin_sheet.clone(), c.origin_row?, c.origin_col?))),
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

pub(crate) fn literal_to_py(py: Python<'_>, value: &LiteralValue) -> PyResult<PyObject> {
    match value {
        LiteralValue::Int(v) => Ok((*v).into_py_any(py)?),
        LiteralValue::Number(v) => Ok((*v).into_py_any(py)?),
        LiteralValue::Boolean(v) => Ok((*v).into_py_any(py)?),
        LiteralValue::Text(v) => Ok(v.clone().into_py_any(py)?),
        LiteralValue::Empty => Ok(py.None()),
        LiteralValue::Date(d) => {
            let date = PyDate::new(py, d.year(), d.month() as u8, d.day() as u8)?;
            Ok(date.into())
        }
        LiteralValue::Time(t) => {
            let time = PyTime::new(
                py,
                t.hour() as u8,
                t.minute() as u8,
                t.second() as u8,
                t.nanosecond() / 1_000,
                None,
            )?;
            Ok(time.into())
        }
        LiteralValue::DateTime(dt) => {
            let datetime = PyDateTime::new(
                py,
                dt.date().year(),
                dt.date().month() as u8,
                dt.date().day() as u8,
                dt.time().hour() as u8,
                dt.time().minute() as u8,
                dt.time().second() as u8,
                dt.time().nanosecond() / 1_000,
                None,
            )?;
            Ok(datetime.into_pyobject(py)?.into_any().unbind())
        }
        LiteralValue::Duration(d) => {
            if let Some(micros) = d.num_microseconds() {
                let days = micros / 86_400_000_000;
                let secs = (micros % 86_400_000_000) / 1_000_000;
                let micros = (micros % 1_000_000) as i32;
                let delta = PyDelta::new(py, days as i32, secs as i32, micros, true)?;
                Ok(delta.into_pyobject(py)?.into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        LiteralValue::Array(rows) => {
            let out = PyList::empty(py);
            for row in rows {
                let py_row = PyList::empty(py);
                for cell in row {
                    py_row.append(literal_to_py(py, cell)?)?;
                }
                out.append(py_row)?;
            }
            Ok(out.into_pyobject(py)?.into_any().unbind())
        }
        LiteralValue::Error(err) => {
            let dict = PyDict::new(py);
            dict.set_item("type", "Error")?;
            dict.set_item("kind", format!("{:?}", err.kind))?;
            if let Some(msg) = &err.message {
                dict.set_item("message", msg)?;
            }
            if let Some(ctx) = &err.context {
                if let Some(r) = ctx.row {
                    dict.set_item("row", r)?;
                }
                if let Some(c) = ctx.col {
                    dict.set_item("col", c)?;
                }
                if let Some(sheet) = &ctx.origin_sheet {
                    dict.set_item("sheet", sheet)?;
                }
                if let Some(origin_row) = ctx.origin_row {
                    dict.set_item("origin_row", origin_row)?;
                }
                if let Some(origin_col) = ctx.origin_col {
                    dict.set_item("origin_col", origin_col)?;
                }
            }
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
        LiteralValue::Pending => Ok(py.None()),
    }
}

pub(crate) fn py_to_literal(value: &Bound<'_, PyAny>) -> PyResult<LiteralValue> {
    if let Ok(py_literal) = value.extract::<PyRef<'_, PyLiteralValue>>() {
        return Ok(py_literal.inner.clone());
    }
    if value.is_none() {
        return Ok(LiteralValue::Empty);
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(LiteralValue::Boolean(value.extract::<bool>()?));
    }
    if value.is_instance_of::<PyInt>() {
        return Ok(LiteralValue::Int(value.extract::<i64>()?));
    }
    if value.is_instance_of::<PyFloat>() {
        return Ok(LiteralValue::Number(value.extract::<f64>()?));
    }
    if value.is_instance_of::<PyString>() {
        return Ok(LiteralValue::Text(value.extract::<String>()?));
    }
    if let Ok(py_dt) = value.cast::<PyDateTime>() {
        let date = NaiveDate::from_ymd_opt(
            py_dt.get_year(),
            py_dt.get_month() as u32,
            py_dt.get_day() as u32,
        )
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid datetime"))?;
        let time = NaiveTime::from_hms_micro_opt(
            py_dt.get_hour() as u32,
            py_dt.get_minute() as u32,
            py_dt.get_second() as u32,
            py_dt.get_microsecond(),
        )
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid datetime"))?;
        return Ok(LiteralValue::DateTime(NaiveDateTime::new(date, time)));
    }
    if let Ok(py_date) = value.cast::<PyDate>() {
        let date = NaiveDate::from_ymd_opt(
            py_date.get_year(),
            py_date.get_month() as u32,
            py_date.get_day() as u32,
        )
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid date"))?;
        return Ok(LiteralValue::Date(date));
    }
    if let Ok(py_time) = value.cast::<PyTime>() {
        let time = NaiveTime::from_hms_micro_opt(
            py_time.get_hour() as u32,
            py_time.get_minute() as u32,
            py_time.get_second() as u32,
            py_time.get_microsecond(),
        )
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid time"))?;
        return Ok(LiteralValue::Time(time));
    }
    if let Ok(py_delta) = value.cast::<PyDelta>() {
        let secs = (py_delta.get_days() * 86_400 + py_delta.get_seconds()) as i64;
        let micros = py_delta.get_microseconds() as i64;
        let duration = chrono::Duration::seconds(secs) + chrono::Duration::microseconds(micros);
        return Ok(LiteralValue::Duration(duration));
    }
    if let Ok(dict) = value.cast::<PyDict>()
        && let Some(kind_obj) = dict.get_item("type")?
    {
        let type_tag: String = kind_obj.extract()?;
        let type_lower = type_tag.to_ascii_lowercase();
        match type_lower.as_str() {
            "error" => {
                let kind_obj = dict.get_item("kind")?.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Error dict missing 'kind'")
                })?;
                let kind_str: String = kind_obj.extract()?;
                let kind_key = kind_str.trim().to_ascii_lowercase();
                let excel_kind = match kind_key.as_str() {
                    "div" | "div0" | "#div/0!" => ExcelErrorKind::Div,
                    "ref" | "#ref!" => ExcelErrorKind::Ref,
                    "name" | "#name?" => ExcelErrorKind::Name,
                    "value" | "#value!" => ExcelErrorKind::Value,
                    "num" | "#num!" => ExcelErrorKind::Num,
                    "null" | "#null!" => ExcelErrorKind::Null,
                    "na" | "n/a" | "#n/a" => ExcelErrorKind::Na,
                    "spill" | "#spill!" => ExcelErrorKind::Spill,
                    "calc" | "#calc!" => ExcelErrorKind::Calc,
                    "circ" | "#circ!" => ExcelErrorKind::Circ,
                    "cancelled" | "#cancelled!" => ExcelErrorKind::Cancelled,
                    "error" | "#error!" => ExcelErrorKind::Error,
                    "nimpl" | "n/impl" | "#n/impl!" => ExcelErrorKind::NImpl,
                    other => {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Unknown error kind: {other}"
                        )));
                    }
                };
                let mut error = ExcelError::new(excel_kind);
                if let Some(message) = dict
                    .get_item("message")?
                    .map(|m| m.extract::<String>())
                    .transpose()?
                {
                    error = error.with_message(message);
                }
                let row = match dict.get_item("row")? {
                    Some(obj) => Some(obj.extract::<u32>().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Error 'row' must be an int",
                        )
                    })?),
                    None => None,
                };
                let col = match dict.get_item("col")? {
                    Some(obj) => Some(obj.extract::<u32>().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Error 'col' must be an int",
                        )
                    })?),
                    None => None,
                };
                let origin_row = match dict.get_item("origin_row")? {
                    Some(obj) => Some(obj.extract::<u32>().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Error 'origin_row' must be an int",
                        )
                    })?),
                    None => None,
                };
                let origin_col = match dict.get_item("origin_col")? {
                    Some(obj) => Some(obj.extract::<u32>().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Error 'origin_col' must be an int",
                        )
                    })?),
                    None => None,
                };
                let origin_sheet = match dict.get_item("sheet")? {
                    Some(obj) => Some(obj.extract::<String>().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Error 'sheet' must be a string",
                        )
                    })?),
                    None => None,
                };
                if row.is_some()
                    || col.is_some()
                    || origin_row.is_some()
                    || origin_col.is_some()
                    || origin_sheet.is_some()
                {
                    error.context = Some(ErrorContext {
                        row,
                        col,
                        origin_row,
                        origin_col,
                        origin_sheet,
                    });
                }
                return Ok(LiteralValue::Error(error));
            }
            "date" => {
                let year: i32 = dict
                    .get_item("year")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Date dict missing 'year'")
                    })?
                    .extract()?;
                let month: u32 = dict
                    .get_item("month")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Date dict missing 'month'")
                    })?
                    .extract()?;
                let day: u32 = dict
                    .get_item("day")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Date dict missing 'day'")
                    })?
                    .extract()?;
                let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid date")
                })?;
                return Ok(LiteralValue::Date(date));
            }
            "time" => {
                let hour: u32 = dict
                    .get_item("hour")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Time dict missing 'hour'")
                    })?
                    .extract()?;
                let minute: u32 = dict
                    .get_item("minute")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Time dict missing 'minute'",
                        )
                    })?
                    .extract()?;
                let second: u32 = dict
                    .get_item("second")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Time dict missing 'second'",
                        )
                    })?
                    .extract()?;
                let microsecond: u32 = dict
                    .get_item("microsecond")?
                    .map(|v| v.extract::<u32>())
                    .transpose()?
                    .unwrap_or(0);
                let time = NaiveTime::from_hms_micro_opt(hour, minute, second, microsecond)
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid time")
                    })?;
                return Ok(LiteralValue::Time(time));
            }
            "datetime" => {
                let year: i32 = dict
                    .get_item("year")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "DateTime dict missing 'year'",
                        )
                    })?
                    .extract()?;
                let month: u32 = dict
                    .get_item("month")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "DateTime dict missing 'month'",
                        )
                    })?
                    .extract()?;
                let day: u32 = dict
                    .get_item("day")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "DateTime dict missing 'day'",
                        )
                    })?
                    .extract()?;
                let hour: u32 = dict
                    .get_item("hour")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "DateTime dict missing 'hour'",
                        )
                    })?
                    .extract()?;
                let minute: u32 = dict
                    .get_item("minute")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "DateTime dict missing 'minute'",
                        )
                    })?
                    .extract()?;
                let second: u32 = dict
                    .get_item("second")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "DateTime dict missing 'second'",
                        )
                    })?
                    .extract()?;
                let microsecond: u32 = dict
                    .get_item("microsecond")?
                    .map(|v| v.extract::<u32>())
                    .transpose()?
                    .unwrap_or(0);
                let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid datetime")
                })?;
                let time = NaiveTime::from_hms_micro_opt(hour, minute, second, microsecond)
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid datetime")
                    })?;
                return Ok(LiteralValue::DateTime(NaiveDateTime::new(date, time)));
            }
            "duration" => {
                let seconds: i64 = dict
                    .get_item("seconds")?
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Duration dict missing 'seconds'",
                        )
                    })?
                    .extract()?;
                let microseconds: i64 = dict
                    .get_item("microseconds")?
                    .map(|v| v.extract::<i64>())
                    .transpose()?
                    .unwrap_or(0);
                let duration = chrono::Duration::seconds(seconds)
                    + chrono::Duration::microseconds(microseconds);
                return Ok(LiteralValue::Duration(duration));
            }
            "pending" => return Ok(LiteralValue::Pending),
            "empty" => return Ok(LiteralValue::Empty),
            "array" => {
                let values = dict.get_item("values")?.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Array dict missing 'values'")
                })?;
                let arr_literal = py_to_literal(&values)?;
                if let LiteralValue::Array(rows) = arr_literal {
                    return Ok(LiteralValue::Array(rows));
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Array 'values' must be a 2D list",
                    ));
                }
            }
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Unsupported LiteralValue dict type: {other}"
                )));
            }
        }
    }
    if let Ok(list) = value.cast::<PyList>() {
        let mut rows = Vec::with_capacity(list.len());
        for (row_idx, item) in list.iter().enumerate() {
            if let Ok(sub) = item.cast::<PyList>() {
                let mut row = Vec::with_capacity(sub.len());
                for cell in sub.iter() {
                    row.push(py_to_literal(&cell)?);
                }
                rows.push(row);
            } else {
                let display_idx = row_idx + 1;
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Array row {display_idx} must be a list"
                )));
            }
        }
        if let Some(first_row) = rows.first() {
            let expected_len = first_row.len();
            for (row_idx, row) in rows.iter().enumerate() {
                let display_idx = row_idx + 1;
                if row.len() != expected_len {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Array rows must be rectangular (row {display_idx} has length {}, expected {expected_len})",
                        row.len()
                    )));
                }
            }
        }
        return Ok(LiteralValue::Array(rows));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Unsupported value type for LiteralValue",
    ))
}

/// Register the value module with Python
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyLiteralValue>()?;
    Ok(())
}
