use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::errors::ExcelEvaluationError;
use crate::value::{literal_to_py, py_to_literal};
use crate::workbook::PyWorkbook;
use formualizer::common::LiteralValue;
use formualizer::sheetport::{
    ConstraintViolation, ManifestBindings, PortBinding, PortValue, SheetPort,
    SheetPortError as RuntimeSheetPortError, TableRow, TableValue,
};
use formualizer::sheetport_spec::{Direction, Manifest, ManifestIssue};
use pyo3::conversion::IntoPyObjectExt;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use serde_json::Value as JsonValue;

pyo3::create_exception!(formualizer, SheetPortError, PyException);
pyo3::create_exception!(formualizer, SheetPortManifestError, SheetPortError);
pyo3::create_exception!(formualizer, SheetPortConstraintError, SheetPortError);
pyo3::create_exception!(formualizer, SheetPortWorkbookError, SheetPortError);

type PyObject = pyo3::Py<pyo3::PyAny>;

type RuntimeResult<T> = Result<T, RuntimeSheetPortError>;

#[pyclass(name = "SheetPortSession", module = "formualizer")]
pub struct PySheetPortSession {
    workbook: PyWorkbook,
    bindings: ManifestBindings,
}

#[pymethods]
impl PySheetPortSession {
    #[classmethod]
    #[pyo3(signature = (manifest_path, workbook_path, backend=None))]
    pub fn from_manifest_path(
        _cls: &Bound<'_, pyo3::types::PyType>,
        manifest_path: &str,
        workbook_path: &str,
        backend: Option<&str>,
    ) -> PyResult<Self> {
        let py = _cls.py();
        let manifest_path_ref = Path::new(manifest_path);
        let manifest_yaml = fs::read_to_string(manifest_path_ref).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "failed to read manifest `{}`: {err}",
                manifest_path_ref.display()
            ))
        })?;
        let manifest = Manifest::from_yaml_str(&manifest_yaml)
            .map_err(|err| SheetPortManifestError::new_err(format!("{err}")))?;
        let workbook = PyWorkbook::from_path(
            &py.get_type::<PyWorkbook>(),
            workbook_path,
            backend,
            None,
            None,
        )?;
        Self::from_components(py, workbook, manifest)
    }

    #[classmethod]
    pub fn from_manifest_yaml(
        _cls: &Bound<'_, pyo3::types::PyType>,
        manifest_yaml: &str,
        workbook: PyWorkbook,
    ) -> PyResult<Self> {
        let py = _cls.py();
        let manifest = Manifest::from_yaml_str(manifest_yaml)
            .map_err(|err| SheetPortManifestError::new_err(format!("{err}")))?;
        Self::from_components(py, workbook, manifest)
    }

    /// Manifest metadata as a Python dictionary (mirrors the YAML structure).
    #[getter]
    pub fn manifest<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        let manifest = self.bindings.manifest().clone();
        let json = serde_json::to_value(&manifest).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "manifest serialization failed: {err}"
            ))
        })?;
        json_to_py(py, &json)
    }

    /// Describe each port with direction, shape, constraints, and resolved defaults.
    pub fn describe_ports<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        let list = PyList::empty(py);
        for binding in self.bindings.bindings() {
            let entry = PyDict::new(py);
            entry.set_item("id", &binding.id)?;
            entry.set_item(
                "direction",
                match binding.direction {
                    Direction::In => "in",
                    Direction::Out => "out",
                },
            )?;
            entry.set_item("required", binding.required)?;
            if let Some(desc) = &binding.description {
                entry.set_item("description", desc)?;
            }
            entry.set_item(
                "shape",
                match binding.kind {
                    formualizer::sheetport::BoundPort::Scalar(_) => "scalar",
                    formualizer::sheetport::BoundPort::Record(_) => "record",
                    formualizer::sheetport::BoundPort::Range(_) => "range",
                    formualizer::sheetport::BoundPort::Table(_) => "table",
                },
            )?;
            if let Some(constraints) = &binding.constraints {
                let value = serde_json::to_value(constraints).map_err(|err| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "constraint serialization failed: {err}"
                    ))
                })?;
                entry.set_item("constraints", json_to_py(py, &value)?)?;
            }
            if let Some(units) = &binding.units {
                let value = serde_json::to_value(units).map_err(|err| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "units serialization failed: {err}"
                    ))
                })?;
                entry.set_item("units", json_to_py(py, &value)?)?;
            }
            if let Some(default) = &binding.resolved_default {
                entry.set_item("default", port_value_to_py(py, default)?)?;
            } else if let Some(raw) = &binding.default {
                entry.set_item("default", json_to_py(py, raw)?)?;
            }
            entry.set_item("location", location_summary(py, binding)?)?;
            list.append(entry)?;
        }
        Ok(list.into_pyobject(py)?.into_any().unbind())
    }

    pub fn read_inputs<'py>(&mut self, py: Python<'py>) -> PyResult<PyObject> {
        self.with_sheetport(py, |sheetport| sheetport.read_inputs())
            .and_then(|snapshot| snapshot_to_py(py, snapshot.inner()))
    }

    pub fn read_outputs<'py>(&mut self, py: Python<'py>) -> PyResult<PyObject> {
        self.with_sheetport(py, |sheetport| sheetport.read_outputs())
            .and_then(|snapshot| snapshot_to_py(py, snapshot.inner()))
    }

    pub fn write_inputs(&mut self, py: Python<'_>, update: &Bound<'_, PyAny>) -> PyResult<()> {
        let dict = update.cast::<PyDict>().map_err(|_| {
            PyErr::new::<PyTypeError, _>("input updates must be provided as a dict")
        })?;
        let converted = py_to_input_update(&self.bindings, dict)?;
        self.with_sheetport(py, |sheetport| sheetport.write_inputs(converted))
            .map(|_| ())
    }

    #[pyo3(signature = (*, freeze_volatile=false, rng_seed=None))]
    pub fn evaluate_once<'py>(
        &mut self,
        py: Python<'py>,
        freeze_volatile: bool,
        rng_seed: Option<u64>,
    ) -> PyResult<PyObject> {
        let options = formualizer::sheetport::EvalOptions {
            freeze_volatile,
            rng_seed,
            ..Default::default()
        };
        self.with_sheetport(py, |sheetport| sheetport.evaluate_once(options))
            .and_then(|snapshot| snapshot_to_py(py, snapshot.inner()))
    }
}

impl PySheetPortSession {
    fn from_components(py: Python<'_>, workbook: PyWorkbook, manifest: Manifest) -> PyResult<Self> {
        let bindings = bind_manifest(py, &workbook, manifest)?;
        Ok(Self { workbook, bindings })
    }

    fn with_sheetport<'py, F, T>(&mut self, py: Python<'py>, f: F) -> PyResult<T>
    where
        F: FnOnce(&mut SheetPort<'_>) -> RuntimeResult<T>,
    {
        let bindings_clone = self.bindings.clone();
        let mut updated: Option<ManifestBindings> = None;
        let result = self.workbook.with_workbook_mut(|workbook| {
            let mut sheetport = SheetPort::from_bindings(workbook, bindings_clone)
                .map_err(|err| map_sheetport_err(py, err))?;
            let output = f(&mut sheetport).map_err(|err| map_sheetport_err(py, err))?;
            let (_, bindings) = sheetport.into_parts();
            updated = Some(bindings);
            Ok(output)
        })?;
        if let Some(new_bindings) = updated {
            self.bindings = new_bindings;
        }
        Ok(result)
    }
}

fn bind_manifest(
    py: Python<'_>,
    workbook: &PyWorkbook,
    manifest: Manifest,
) -> PyResult<ManifestBindings> {
    workbook.with_workbook_mut(move |wb| {
        let sheetport = SheetPort::new(wb, manifest).map_err(|err| map_sheetport_err(py, err))?;
        let (_, bindings) = sheetport.into_parts();
        Ok(bindings)
    })
}

fn py_to_input_update(
    bindings: &ManifestBindings,
    dict: &Bound<'_, PyDict>,
) -> PyResult<formualizer::sheetport::InputUpdate> {
    let mut update = formualizer::sheetport::InputUpdate::default();
    for (key, value) in dict.iter() {
        let port_id: String = key.extract()?;
        let binding = bindings
            .get(&port_id)
            .ok_or_else(|| PyErr::new::<PyTypeError, _>(format!("unknown port id `{port_id}`")))?;
        let port_value = py_to_port_value(binding, &value)?;
        update.insert(port_id, port_value);
    }
    Ok(update)
}

fn port_value_to_py(py: Python<'_>, value: &PortValue) -> PyResult<PyObject> {
    match value {
        PortValue::Scalar(lit) => literal_to_py(py, lit),
        PortValue::Record(map) => {
            let dict = PyDict::new(py);
            for (field, value) in map.iter() {
                dict.set_item(field, literal_to_py(py, value)?)?;
            }
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
        PortValue::Range(rows) => {
            let outer = PyList::empty(py);
            for row in rows {
                let inner = PyList::empty(py);
                for cell in row {
                    inner.append(literal_to_py(py, cell)?)?;
                }
                outer.append(inner)?;
            }
            Ok(outer.into_pyobject(py)?.into_any().unbind())
        }
        PortValue::Table(table) => table_value_to_py(py, table),
    }
}

fn table_value_to_py(py: Python<'_>, table: &TableValue) -> PyResult<PyObject> {
    let outer = PyList::empty(py);
    for row in &table.rows {
        let dict = PyDict::new(py);
        for (column, value) in &row.values {
            dict.set_item(column, literal_to_py(py, value)?)?;
        }
        outer.append(dict)?;
    }
    Ok(outer.into_pyobject(py)?.into_any().unbind())
}

fn py_to_port_value(binding: &PortBinding, value: &Bound<'_, PyAny>) -> PyResult<PortValue> {
    if value.is_none() {
        return Ok(match &binding.kind {
            formualizer::sheetport::BoundPort::Scalar(_) => PortValue::Scalar(LiteralValue::Empty),
            formualizer::sheetport::BoundPort::Record(_) => PortValue::Record(BTreeMap::new()),
            formualizer::sheetport::BoundPort::Range(_) => PortValue::Range(Vec::new()),
            formualizer::sheetport::BoundPort::Table(_) => PortValue::Table(TableValue::default()),
        });
    }

    match &binding.kind {
        formualizer::sheetport::BoundPort::Scalar(_) => {
            let literal = py_to_literal(value)?;
            Ok(PortValue::Scalar(literal))
        }
        formualizer::sheetport::BoundPort::Record(record) => {
            let dict = value
                .cast::<PyDict>()
                .map_err(|_| PyErr::new::<PyTypeError, _>("record inputs must be dictionaries"))?;
            let mut map = BTreeMap::new();
            for (key, val) in dict.iter() {
                let field: String = key.extract()?;
                if !record.fields.contains_key(&field) {
                    return Err(PyErr::new::<PyTypeError, _>(format!(
                        "record update includes unknown field `{field}`"
                    )));
                }
                let literal = py_to_literal(&val)?;
                map.insert(field.clone(), literal);
            }
            Ok(PortValue::Record(map))
        }
        formualizer::sheetport::BoundPort::Range(_) => {
            let iter = value.try_iter().map_err(|_| {
                PyErr::new::<PyTypeError, _>("range inputs must be an iterable of rows")
            })?;
            let mut rows: Vec<Vec<LiteralValue>> = Vec::new();
            let mut expected_width: Option<usize> = None;
            for (row_idx, row_obj) in iter.enumerate() {
                let row_any = row_obj?;
                let row_iter = row_any.try_iter().map_err(|_| {
                    PyErr::new::<PyTypeError, _>(format!(
                        "range row {} must be iterable",
                        row_idx + 1
                    ))
                })?;
                let mut converted: Vec<LiteralValue> = Vec::new();
                for cell in row_iter {
                    let cell_any = cell?;
                    let literal = py_to_literal(&cell_any)?;
                    converted.push(literal);
                }
                if let Some(width) = expected_width {
                    if width != converted.len() {
                        return Err(PyErr::new::<PyTypeError, _>(format!(
                            "range rows must be rectangular (row {} has {}, expected {})",
                            row_idx + 1,
                            converted.len(),
                            width
                        )));
                    }
                } else {
                    expected_width = Some(converted.len());
                }
                rows.push(converted);
            }
            Ok(PortValue::Range(rows))
        }
        formualizer::sheetport::BoundPort::Table(table) => {
            let iter = value.try_iter().map_err(|_| {
                PyErr::new::<PyTypeError, _>("table inputs must be an iterable of row mappings")
            })?;
            let mut rows = Vec::new();
            let known_columns: BTreeMap<&str, &formualizer::sheetport::TableColumnBinding> = table
                .columns
                .iter()
                .map(|col| (col.name.as_str(), col))
                .collect();
            for (row_idx, row_obj) in iter.enumerate() {
                let row_any = row_obj?;
                let mapping = row_any.cast::<PyDict>().map_err(|_| {
                    PyErr::new::<PyTypeError, _>(format!(
                        "table row {} must be a dict of column values",
                        row_idx + 1
                    ))
                })?;
                let mut values = BTreeMap::new();
                for (key, val) in mapping.iter() {
                    let column: String = key.extract()?;
                    if !known_columns.contains_key(column.as_str()) {
                        return Err(PyErr::new::<PyTypeError, _>(format!(
                            "table update references unknown column `{}`",
                            column
                        )));
                    }
                    let literal = py_to_literal(&val)?;
                    values.insert(column, literal);
                }
                rows.push(TableRow::new(values));
            }
            Ok(PortValue::Table(TableValue::new(rows)))
        }
    }
}

fn snapshot_to_py(py: Python<'_>, map: &BTreeMap<String, PortValue>) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    for (port_id, value) in map {
        dict.set_item(port_id, port_value_to_py(py, value)?)?;
    }
    Ok(dict.into_pyobject(py)?.into_any().unbind())
}

fn location_summary(py: Python<'_>, binding: &PortBinding) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    match &binding.kind {
        formualizer::sheetport::BoundPort::Scalar(scalar) => {
            dict.set_item("kind", "scalar")?;
            dict.set_item("selector", scalar_location_to_py(py, &scalar.location)?)?;
        }
        formualizer::sheetport::BoundPort::Record(record) => {
            dict.set_item("kind", "record")?;
            dict.set_item("selector", area_location_to_py(py, &record.location)?)?;
        }
        formualizer::sheetport::BoundPort::Range(range) => {
            dict.set_item("kind", "range")?;
            dict.set_item("selector", area_location_to_py(py, &range.location)?)?;
        }
        formualizer::sheetport::BoundPort::Table(table) => {
            dict.set_item("kind", "table")?;
            dict.set_item("selector", table_location_to_py(py, &table.location)?)?;
            let columns = PyList::empty(py);
            for column in &table.columns {
                let col = PyDict::new(py);
                col.set_item("name", &column.name)?;
                col.set_item(
                    "value_type",
                    format!("{:?}", column.value_type).to_lowercase(),
                )?;
                if let Some(hint) = &column.column_hint {
                    col.set_item("column", hint)?;
                }
                columns.append(col)?;
            }
            dict.set_item("columns", columns)?;
        }
    }
    Ok(dict.into_pyobject(py)?.into_any().unbind())
}

fn scalar_location_to_py(
    py: Python<'_>,
    location: &formualizer::sheetport::ScalarLocation,
) -> PyResult<PyObject> {
    match location {
        formualizer::sheetport::ScalarLocation::Cell(addr) => range_address_to_py(py, addr),
        formualizer::sheetport::ScalarLocation::Name(name) => {
            let dict = PyDict::new(py);
            dict.set_item("name", name)?;
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
        formualizer::sheetport::ScalarLocation::StructRef(reference) => {
            let dict = PyDict::new(py);
            dict.set_item("struct_ref", reference)?;
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
    }
}

fn area_location_to_py(
    py: Python<'_>,
    location: &formualizer::sheetport::AreaLocation,
) -> PyResult<PyObject> {
    match location {
        formualizer::sheetport::AreaLocation::Range(addr) => range_address_to_py(py, addr),
        formualizer::sheetport::AreaLocation::Name(name) => {
            let dict = PyDict::new(py);
            dict.set_item("name", name)?;
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
        formualizer::sheetport::AreaLocation::StructRef(reference) => {
            let dict = PyDict::new(py);
            dict.set_item("struct_ref", reference)?;
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
        formualizer::sheetport::AreaLocation::Layout(layout) => {
            let value = serde_json::to_value(layout).map_err(|err| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "layout serialization failed: {err}"
                ))
            })?;
            json_to_py(py, &value)
        }
    }
}

fn table_location_to_py(
    py: Python<'_>,
    location: &formualizer::sheetport::TableLocation,
) -> PyResult<PyObject> {
    match location {
        formualizer::sheetport::TableLocation::Table(selector) => {
            let value = serde_json::to_value(selector).map_err(|err| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "table selector serialization failed: {err}"
                ))
            })?;
            json_to_py(py, &value)
        }
        formualizer::sheetport::TableLocation::Layout(layout) => {
            let value = serde_json::to_value(layout).map_err(|err| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "layout serialization failed: {err}"
                ))
            })?;
            json_to_py(py, &value)
        }
    }
}

fn range_address_to_py(
    py: Python<'_>,
    addr: &formualizer::common::RangeAddress,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("sheet", &addr.sheet)?;
    dict.set_item("start_row", addr.start_row)?;
    dict.set_item("start_col", addr.start_col)?;
    dict.set_item("end_row", addr.end_row)?;
    dict.set_item("end_col", addr.end_col)?;
    Ok(dict.into_pyobject(py)?.into_any().unbind())
}

fn json_to_py(py: Python<'_>, value: &JsonValue) -> PyResult<PyObject> {
    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => (*b).into_py_any(py),
        JsonValue::Number(num) => {
            if let Some(int) = num.as_i64() {
                Ok(int.into_pyobject(py)?.into_any().unbind())
            } else if let Some(uint) = num.as_u64() {
                Ok((uint as i64).into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = num.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        JsonValue::String(s) => Ok(s.clone().into_pyobject(py)?.into_any().unbind()),
        JsonValue::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.into_pyobject(py)?.into_any().unbind())
        }
        JsonValue::Object(map) => {
            let dict = PyDict::new(py);
            for (key, item) in map {
                dict.set_item(key, json_to_py(py, item)?)?;
            }
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
    }
}

fn map_sheetport_err(py: Python<'_>, err: RuntimeSheetPortError) -> PyErr {
    match err {
        RuntimeSheetPortError::InvalidManifest { issues } => {
            match manifest_issues_to_py(py, &issues) {
                Ok(details) => {
                    SheetPortManifestError::new_err(("manifest validation failed", details))
                }
                Err(_) => SheetPortManifestError::new_err("manifest validation failed"),
            }
        }
        RuntimeSheetPortError::ConstraintViolation { violations } => {
            match constraint_details_to_py(py, &violations) {
                Ok(details) => SheetPortConstraintError::new_err((
                    "value did not satisfy manifest constraints",
                    details,
                )),
                Err(_) => {
                    SheetPortConstraintError::new_err("value did not satisfy manifest constraints")
                }
            }
        }
        RuntimeSheetPortError::Workbook { source } => {
            SheetPortWorkbookError::new_err(source.to_string())
        }
        RuntimeSheetPortError::Engine { source } => {
            ExcelEvaluationError::new_err(source.to_string())
        }
        RuntimeSheetPortError::UnsupportedSelector { port, reason } => {
            SheetPortError::new_err(format!("port `{port}` uses unsupported selector: {reason}"))
        }
        RuntimeSheetPortError::InvalidReference {
            port,
            reference,
            details,
        } => SheetPortError::new_err(format!(
            "port `{port}` reference `{reference}` is invalid: {details}"
        )),
        RuntimeSheetPortError::MissingSheet { port, sheet } => SheetPortError::new_err(format!(
            "sheet `{sheet}` referenced by port `{port}` was not found"
        )),
        RuntimeSheetPortError::InvariantViolation { port, message } => {
            SheetPortError::new_err(format!("port `{port}` invariant violation: {message}"))
        }
    }
}

fn manifest_issues_to_py(py: Python<'_>, issues: &[ManifestIssue]) -> PyResult<PyObject> {
    let list = PyList::empty(py);
    for issue in issues {
        let dict = PyDict::new(py);
        dict.set_item("path", issue.path.clone())?;
        dict.set_item("message", issue.message.clone())?;
        list.append(dict)?;
    }
    Ok(list.into_pyobject(py)?.into_any().unbind())
}

fn constraint_details_to_py(
    py: Python<'_>,
    violations: &[ConstraintViolation],
) -> PyResult<PyObject> {
    let list = PyList::empty(py);
    for violation in violations {
        let dict = PyDict::new(py);
        dict.set_item("port", violation.port.clone())?;
        dict.set_item("path", violation.path.clone())?;
        dict.set_item("message", violation.message.clone())?;
        list.append(dict)?;
    }
    Ok(list.into_pyobject(py)?.into_any().unbind())
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("SheetPortError", m.py().get_type::<SheetPortError>())?;
    m.add(
        "SheetPortManifestError",
        m.py().get_type::<SheetPortManifestError>(),
    )?;
    m.add(
        "SheetPortConstraintError",
        m.py().get_type::<SheetPortConstraintError>(),
    )?;
    m.add(
        "SheetPortWorkbookError",
        m.py().get_type::<SheetPortWorkbookError>(),
    )?;
    m.add_class::<PySheetPortSession>()?;
    Ok(())
}
