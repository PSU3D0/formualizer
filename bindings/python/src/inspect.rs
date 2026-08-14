use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use formualizer::common::{
    CellAddress, RangeAddress, RangeArea, col_index_from_letters_1based, col_letters_from_1based,
    format_a1_sheet_name,
};
use formualizer::eval::engine::inspect as core;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
#[cfg(not(target_os = "emscripten"))]
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::errors::{
    inspect_error_to_pyerr, invalid_inspection_address, unknown_inspection_variant,
};
use crate::value::literal_to_py;
use crate::workbook::PyWorkbook;

type PyObject = Py<PyAny>;

fn address_text(address: &CellAddress) -> String {
    address.to_string()
}

fn range_text(range: &RangeAddress) -> String {
    let sheet = format_a1_sheet_name(&range.sheet);
    let endpoint = |row, column| {
        format!(
            "{}{row}",
            col_letters_from_1based(column).unwrap_or_else(|_| "#REF!".to_string())
        )
    };
    let start = endpoint(range.start_row, range.start_col);
    let end = endpoint(range.end_row, range.end_col);
    format!("{sheet}!{start}:{end}")
}

fn area_text(area: &RangeArea) -> String {
    fn endpoint(row: Option<u32>, column: Option<u32>) -> String {
        match (row, column) {
            (Some(row), Some(column)) => CellAddress {
                sheet: "_".to_string(),
                row,
                column,
            }
            .to_string()
            .split_once('!')
            .unwrap()
            .1
            .to_string(),
            (None, Some(column)) => formualizer::common::col_letters_from_1based(column)
                .unwrap_or_else(|_| "#REF!".to_string()),
            (Some(row), None) => row.to_string(),
            (None, None) => String::new(),
        }
    }
    format!(
        "{}!{}:{}",
        format_a1_sheet_name(&area.sheet),
        endpoint(area.start_row, area.start_column),
        endpoint(area.end_row, area.end_column)
    )
}

fn split_sheet(input: &str) -> Result<(String, &str), PyErr> {
    let mut quoted = false;
    let mut split = None;
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            if quoted && index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                index += 2;
                continue;
            }
            quoted = !quoted;
        } else if bytes[index] == b'!' && !quoted {
            split = Some(index);
        }
        index += 1;
    }
    if quoted {
        return Err(invalid_inspection_address("unterminated sheet quote"));
    }
    let index = split.ok_or_else(|| {
        invalid_inspection_address("address must be sheet-qualified, for example 'Sheet1!A1'")
    })?;
    let raw_sheet = &input[..index];
    let reference = &input[index + 1..];
    if raw_sheet.is_empty() || reference.is_empty() {
        return Err(invalid_inspection_address(
            "address must contain a sheet and A1 reference",
        ));
    }
    let sheet = if raw_sheet.starts_with('\'') && raw_sheet.ends_with('\'') && raw_sheet.len() >= 2
    {
        raw_sheet[1..raw_sheet.len() - 1].replace("''", "'")
    } else {
        raw_sheet.to_string()
    };
    Ok((sheet, reference))
}

fn parse_endpoint(input: &str) -> Result<(Option<u32>, Option<u32>), PyErr> {
    let input = input.replace('$', "");
    if input.is_empty() {
        return Ok((None, None));
    }
    let letter_count = input
        .bytes()
        .take_while(|byte| byte.is_ascii_alphabetic())
        .count();
    let (letters, digits) = input.split_at(letter_count);
    if !digits.bytes().all(|byte| byte.is_ascii_digit())
        || (letters.is_empty() && digits.is_empty())
    {
        return Err(invalid_inspection_address(format!(
            "invalid A1 endpoint: {input}"
        )));
    }
    let column = if letters.is_empty() {
        None
    } else {
        Some(
            col_index_from_letters_1based(letters)
                .map_err(|error| invalid_inspection_address(error.to_string()))?,
        )
    };
    let row = if digits.is_empty() {
        None
    } else {
        let row = digits
            .parse::<u32>()
            .map_err(|_| invalid_inspection_address(format!("invalid row: {digits}")))?;
        Some(row)
    };
    Ok((row, column))
}

pub(crate) fn parse_cell(input: &str) -> PyResult<CellAddress> {
    let (sheet, reference) = split_sheet(input)?;
    if reference.contains(':') {
        return Err(invalid_inspection_address("expected one cell, not a range"));
    }
    let (row, column) = parse_endpoint(reference)?;
    CellAddress::new(
        sheet,
        row.ok_or_else(|| invalid_inspection_address("missing row"))?,
        column.ok_or_else(|| invalid_inspection_address("missing column"))?,
    )
    .map_err(|error| invalid_inspection_address(error.to_string()))
}

pub(crate) fn parse_area(input: &str) -> PyResult<RangeArea> {
    let (sheet, reference) = split_sheet(input)?;
    let (start, end) = reference.split_once(':').unwrap_or((reference, reference));
    let (start_row, start_column) = parse_endpoint(start)?;
    let (end_row, end_column) = parse_endpoint(end)?;
    if start_column.is_some() != end_column.is_some() {
        return Err(invalid_inspection_address("invalid range bounds"));
    }
    RangeArea::new(sheet, start_row, start_column, end_row, end_column)
        .map_err(|error| invalid_inspection_address(error.to_string()))
}

fn python_hash(value: &impl Hash) -> isize {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish() as isize
}

fn unhashable(name: &str) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!("unhashable type: '{name}'"))
}

macro_rules! value_semantics_hash {
    ($wrapper:ty, $python:literal) => {
        #[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
        #[pymethods]
        impl $wrapper {
            fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
                other
                    .extract::<PyRef<'_, Self>>()
                    .is_ok_and(|other| self.inner == other.inner)
            }
            fn __ne__(&self, other: &Bound<'_, PyAny>) -> bool {
                !self.__eq__(other)
            }
            fn __hash__(&self) -> isize {
                python_hash(&self.inner)
            }
        }
    };
}

macro_rules! value_semantics_unhashable {
    ($wrapper:ty, $python:literal) => {
        #[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
        #[pymethods]
        impl $wrapper {
            fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
                other
                    .extract::<PyRef<'_, Self>>()
                    .is_ok_and(|other| self.inner == other.inner)
            }
            fn __ne__(&self, other: &Bound<'_, PyAny>) -> bool {
                !self.__eq__(other)
            }
            fn __hash__(&self) -> PyResult<isize> {
                Err(unhashable($python))
            }
        }
    };
}

macro_rules! binding_enum {
    ($name:ident, $python:literal, $core:ty, {$($variant:ident),+ $(,)?}) => {
        #[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass_enum)]
        #[pyclass(name = $python, module = "formualizer.formualizer_py", frozen, from_py_object)]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum $name { $($variant),+ }

        #[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
        #[pymethods]
        impl $name {
            fn __str__(&self) -> &'static str {
                match self { $(Self::$variant => stringify!($variant)),+ }
            }
            fn __repr__(&self) -> String { format!("{}.{}", $python, self.__str__()) }
        }
    };
}

binding_enum!(PyStaleness, "Staleness", core::Staleness, { Current, Dirty, NeverEvaluated, Unknown });
binding_enum!(PyProvenance, "Provenance", core::Provenance, { Declared, Observed, Unknown });
binding_enum!(PyLinkDisposition, "LinkDisposition", core::LinkDisposition, { Expanded, Convergent, Cycle, Elided, Unknown });
binding_enum!(PyTraceDirection, "TraceDirection", core::TraceDirection, { Precedents, Dependents });
binding_enum!(PyOmittedCountKind, "OmittedCountKind", core::OmittedCount, { Exact, AtLeast, Unknown });
binding_enum!(PySpillRoleKind, "SpillRoleKind", core::SpillRole, { Anchor, Member, Unknown });
binding_enum!(PyReferenceKind, "ReferenceKind", core::SemanticReference, { Cell, Range, Name, Table, External, Unsupported, Unknown });
binding_enum!(PyNameResolutionKind, "NameResolutionKind", core::NameResolution, { Cell, Range, Literal, Formula, Unresolved, Unknown });
binding_enum!(PyTraceLinkKindType, "TraceLinkKindType", core::TraceLinkKind, { Formula, SpillAnchor, SpillReader, Unknown });

impl From<core::Staleness> for PyStaleness {
    fn from(value: core::Staleness) -> Self {
        match value {
            core::Staleness::Current => Self::Current,
            core::Staleness::Dirty => Self::Dirty,
            core::Staleness::NeverEvaluated => Self::NeverEvaluated,
            _ => Self::Unknown,
        }
    }
}
impl From<core::Provenance> for PyProvenance {
    fn from(value: core::Provenance) -> Self {
        match value {
            core::Provenance::Declared => Self::Declared,
            core::Provenance::Observed => Self::Observed,
            _ => Self::Unknown,
        }
    }
}
impl From<core::LinkDisposition> for PyLinkDisposition {
    fn from(value: core::LinkDisposition) -> Self {
        match value {
            core::LinkDisposition::Expanded => Self::Expanded,
            core::LinkDisposition::Convergent => Self::Convergent,
            core::LinkDisposition::Cycle => Self::Cycle,
            core::LinkDisposition::Elided => Self::Elided,
            _ => Self::Unknown,
        }
    }
}
impl TryFrom<core::TraceDirection> for PyTraceDirection {
    type Error = PyErr;

    fn try_from(value: core::TraceDirection) -> Result<Self, Self::Error> {
        match value {
            core::TraceDirection::Precedents => Ok(Self::Precedents),
            core::TraceDirection::Dependents => Ok(Self::Dependents),
            _ => Err(unknown_inspection_variant("TraceDirection")),
        }
    }
}
impl From<PyTraceDirection> for core::TraceDirection {
    fn from(value: PyTraceDirection) -> Self {
        match value {
            PyTraceDirection::Precedents => Self::Precedents,
            PyTraceDirection::Dependents => Self::Dependents,
        }
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "StateStamp",
    module = "formualizer.formualizer_py",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub struct PyStateStamp {
    inner: core::StateStamp,
}
impl From<core::StateStamp> for PyStateStamp {
    fn from(inner: core::StateStamp) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyStateStamp {
    #[getter]
    fn mutation_revision(&self) -> u64 {
        self.inner.mutation_revision
    }
    #[getter]
    fn recalc_epoch(&self) -> u64 {
        self.inner.recalc_epoch
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("mutation_revision", self.inner.mutation_revision)?;
        out.set_item("recalc_epoch", self.inner.recalc_epoch)?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "StateStamp(mutation_revision={}, recalc_epoch={})",
            self.inner.mutation_revision, self.inner.recalc_epoch
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "OmittedCount",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyOmittedCount {
    inner: core::OmittedCount,
}
impl From<core::OmittedCount> for PyOmittedCount {
    fn from(inner: core::OmittedCount) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyOmittedCount {
    /// Whether the count is exact or only a witnessed lower bound. Read with `count`.
    #[getter]
    fn kind(&self) -> PyOmittedCountKind {
        match self.inner {
            core::OmittedCount::Exact(_) => PyOmittedCountKind::Exact,
            core::OmittedCount::AtLeast(_) => PyOmittedCountKind::AtLeast,
            _ => PyOmittedCountKind::Unknown,
        }
    }
    /// The known exact count or witnessed lower bound; `None` for an unknown future variant.
    #[getter]
    fn count(&self) -> Option<u64> {
        match self.inner {
            core::OmittedCount::Exact(value) | core::OmittedCount::AtLeast(value) => Some(value),
            _ => None,
        }
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("kind", self.kind().__str__())?;
        out.set_item("count", self.count())?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "OmittedCount(kind={}, count={:?})",
            self.kind().__str__(),
            self.count()
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "TruncationReport",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyTruncationReport {
    inner: core::TruncationReport,
}
impl From<core::TruncationReport> for PyTruncationReport {
    fn from(inner: core::TruncationReport) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTruncationReport {
    #[getter]
    fn incomplete(&self) -> bool {
        self.inner.incomplete
    }
    #[getter]
    fn omitted(&self) -> Option<PyOmittedCount> {
        self.inner.omitted.map(Into::into)
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        truncation_dict(py, &self.inner)
    }
    fn __repr__(&self) -> String {
        format!(
            "TruncationReport(incomplete={}, omitted={})",
            self.inner.incomplete,
            self.inner.omitted.is_some()
        )
    }
}

fn truncation_dict(py: Python<'_>, value: &core::TruncationReport) -> PyResult<PyObject> {
    let out = PyDict::new(py);
    out.set_item("incomplete", value.incomplete)?;
    match value.omitted {
        Some(value) => out.set_item("omitted", PyOmittedCount::from(value).to_dict(py)?)?,
        None => out.set_item("omitted", py.None())?,
    }
    Ok(out.into_any().unbind())
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "SpillRole",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PySpillRole {
    inner: core::SpillRole,
}
impl From<core::SpillRole> for PySpillRole {
    fn from(inner: core::SpillRole) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PySpillRole {
    #[getter]
    fn kind(&self) -> PySpillRoleKind {
        match self.inner {
            core::SpillRole::Anchor { .. } => PySpillRoleKind::Anchor,
            core::SpillRole::Member { .. } => PySpillRoleKind::Member,
            _ => PySpillRoleKind::Unknown,
        }
    }
    #[getter]
    fn extent(&self) -> Option<String> {
        match &self.inner {
            core::SpillRole::Anchor { extent } => Some(range_text(extent)),
            _ => None,
        }
    }
    #[getter]
    fn anchor(&self) -> Option<String> {
        match &self.inner {
            core::SpillRole::Member { anchor } => Some(address_text(anchor)),
            _ => None,
        }
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("kind", self.kind().__str__())?;
        out.set_item("extent", self.extent())?;
        out.set_item("anchor", self.anchor())?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!("SpillRole(kind={})", self.kind().__str__())
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "CellSnapshot",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyCellSnapshot {
    inner: core::CellSnapshot,
}
impl From<core::CellSnapshot> for PyCellSnapshot {
    fn from(inner: core::CellSnapshot) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyCellSnapshot {
    #[getter]
    fn address(&self) -> String {
        address_text(&self.inner.address)
    }
    #[getter]
    fn formula(&self) -> Option<String> {
        self.inner.formula.clone()
    }
    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        self.inner
            .value
            .as_ref()
            .map(|value| literal_to_py(py, value))
            .transpose()
    }
    #[getter]
    fn value_included(&self) -> bool {
        self.inner.value_included
    }
    #[getter]
    fn staleness(&self) -> PyStaleness {
        self.inner.staleness.into()
    }
    #[getter]
    fn volatile(&self) -> bool {
        self.inner.volatile
    }
    #[getter]
    fn spill(&self) -> Option<PySpillRole> {
        self.inner.spill.clone().map(Into::into)
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        cell_dict(py, &self.inner)
    }
    fn __repr__(&self) -> String {
        format!(
            "CellSnapshot(address={}, staleness={}, value_included={})",
            self.address(),
            self.staleness().__str__(),
            self.inner.value_included
        )
    }
}

fn cell_dict(py: Python<'_>, value: &core::CellSnapshot) -> PyResult<PyObject> {
    let out = PyDict::new(py);
    out.set_item("address", address_text(&value.address))?;
    out.set_item("formula", &value.formula)?;
    match &value.value {
        Some(value) => out.set_item("value", literal_to_py(py, value)?)?,
        None => out.set_item("value", py.None())?,
    }
    out.set_item("value_included", value.value_included)?;
    out.set_item("staleness", PyStaleness::from(value.staleness).__str__())?;
    out.set_item("volatile", value.volatile)?;
    match &value.spill {
        Some(spill) => out.set_item("spill", PySpillRole::from(spill.clone()).to_dict(py)?)?,
        None => out.set_item("spill", py.None())?,
    }
    Ok(out.into_any().unbind())
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "CellSnapshotReport",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyCellSnapshotReport {
    inner: core::CellSnapshotReport,
}
impl From<core::CellSnapshotReport> for PyCellSnapshotReport {
    fn from(inner: core::CellSnapshotReport) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyCellSnapshotReport {
    #[getter]
    fn stamp(&self) -> PyStateStamp {
        self.inner.stamp.into()
    }
    #[getter]
    fn cell(&self) -> PyCellSnapshot {
        self.inner.cell.clone().into()
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("stamp", self.stamp().to_dict(py)?)?;
        out.set_item("cell", self.cell().to_dict(py)?)?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "CellSnapshotReport(cell={}, staleness={})",
            address_text(&self.inner.cell.address),
            PyStaleness::from(self.inner.cell.staleness).__str__()
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "NameResolution",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyNameResolution {
    inner: core::NameResolution,
}
impl From<core::NameResolution> for PyNameResolution {
    fn from(inner: core::NameResolution) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyNameResolution {
    #[getter]
    fn kind(&self) -> PyNameResolutionKind {
        match self.inner {
            core::NameResolution::Cell(_) => PyNameResolutionKind::Cell,
            core::NameResolution::Range { .. } => PyNameResolutionKind::Range,
            core::NameResolution::Literal(_) => PyNameResolutionKind::Literal,
            core::NameResolution::Formula { .. } => PyNameResolutionKind::Formula,
            core::NameResolution::Unresolved => PyNameResolutionKind::Unresolved,
            _ => PyNameResolutionKind::Unknown,
        }
    }
    #[getter]
    fn address(&self) -> Option<String> {
        match &self.inner {
            core::NameResolution::Cell(value) => Some(address_text(value)),
            _ => None,
        }
    }
    #[getter]
    fn declared(&self) -> Option<String> {
        match &self.inner {
            core::NameResolution::Range { declared, .. } => Some(area_text(declared)),
            _ => None,
        }
    }
    #[getter]
    fn resolved(&self) -> Option<String> {
        match &self.inner {
            core::NameResolution::Range { resolved, .. } => resolved.as_ref().map(range_text),
            _ => None,
        }
    }
    #[getter]
    fn formula(&self) -> Option<String> {
        match &self.inner {
            core::NameResolution::Formula { formula, .. } => Some(formula.clone()),
            _ => None,
        }
    }
    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        match &self.inner {
            core::NameResolution::Literal(value) => Ok(Some(literal_to_py(py, value)?)),
            core::NameResolution::Formula {
                value: Some(value), ..
            } => Ok(Some(literal_to_py(py, value)?)),
            _ => Ok(None),
        }
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("kind", self.kind().__str__())?;
        out.set_item("address", self.address())?;
        out.set_item("declared", self.declared())?;
        out.set_item("resolved", self.resolved())?;
        out.set_item("formula", self.formula())?;
        out.set_item("value", self.value(py)?)?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!("NameResolution(kind={})", self.kind().__str__())
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "SemanticReference",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PySemanticReference {
    inner: core::SemanticReference,
}
impl From<core::SemanticReference> for PySemanticReference {
    fn from(inner: core::SemanticReference) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PySemanticReference {
    #[getter]
    fn kind(&self) -> PyReferenceKind {
        match self.inner {
            core::SemanticReference::Cell(_) => PyReferenceKind::Cell,
            core::SemanticReference::Range { .. } => PyReferenceKind::Range,
            core::SemanticReference::Name { .. } => PyReferenceKind::Name,
            core::SemanticReference::Table { .. } => PyReferenceKind::Table,
            core::SemanticReference::External { .. } => PyReferenceKind::External,
            core::SemanticReference::Unsupported { .. } => PyReferenceKind::Unsupported,
            _ => PyReferenceKind::Unknown,
        }
    }
    #[getter]
    fn address(&self) -> Option<String> {
        match &self.inner {
            core::SemanticReference::Cell(value) => Some(address_text(value)),
            _ => None,
        }
    }
    #[getter]
    fn declared(&self) -> Option<String> {
        match &self.inner {
            core::SemanticReference::Range { declared, .. } => Some(area_text(declared)),
            _ => None,
        }
    }
    #[getter]
    fn resolved(&self) -> Option<String> {
        match &self.inner {
            core::SemanticReference::Range { resolved, .. } => resolved.as_ref().map(range_text),
            core::SemanticReference::Table { resolved, .. } => Some(range_text(resolved)),
            _ => None,
        }
    }
    #[getter]
    fn cell_count(&self) -> Option<u64> {
        match self.inner {
            core::SemanticReference::Range { cell_count, .. } => Some(cell_count),
            _ => None,
        }
    }
    #[getter]
    fn name(&self) -> Option<String> {
        match &self.inner {
            core::SemanticReference::Name { name, .. }
            | core::SemanticReference::Table { name, .. } => Some(name.clone()),
            _ => None,
        }
    }
    #[getter]
    fn resolution(&self) -> Option<PyNameResolution> {
        match &self.inner {
            core::SemanticReference::Name { resolution, .. } => Some(resolution.clone().into()),
            _ => None,
        }
    }
    #[getter]
    fn specifier(&self) -> Option<String> {
        match &self.inner {
            core::SemanticReference::Table { specifier, .. } => Some(specifier.clone()),
            _ => None,
        }
    }
    #[getter]
    fn raw(&self) -> Option<String> {
        match &self.inner {
            core::SemanticReference::External { raw } => Some(raw.clone()),
            _ => None,
        }
    }
    #[getter]
    fn text(&self) -> Option<String> {
        match &self.inner {
            core::SemanticReference::Unsupported { text, .. } => Some(text.clone()),
            _ => None,
        }
    }
    #[getter]
    fn reason(&self) -> Option<String> {
        match &self.inner {
            core::SemanticReference::Unsupported { reason, .. } => Some(reason.clone()),
            _ => None,
        }
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        reference_dict(py, &self.inner)
    }
    fn __repr__(&self) -> String {
        format!("SemanticReference(kind={})", self.kind().__str__())
    }
}

fn reference_dict(py: Python<'_>, value: &core::SemanticReference) -> PyResult<PyObject> {
    let wrapper = PySemanticReference::from(value.clone());
    let out = PyDict::new(py);
    out.set_item("kind", wrapper.kind().__str__())?;
    out.set_item("address", wrapper.address())?;
    out.set_item("declared", wrapper.declared())?;
    out.set_item("resolved", wrapper.resolved())?;
    out.set_item("cell_count", wrapper.cell_count())?;
    out.set_item("name", wrapper.name())?;
    out.set_item("specifier", wrapper.specifier())?;
    out.set_item("raw", wrapper.raw())?;
    out.set_item("text", wrapper.text())?;
    out.set_item("reason", wrapper.reason())?;
    match wrapper.resolution() {
        Some(value) => out.set_item("resolution", value.to_dict(py)?)?,
        None => out.set_item("resolution", py.None())?,
    }
    Ok(out.into_any().unbind())
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "Precedent",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyPrecedent {
    inner: core::Precedent,
}
impl From<core::Precedent> for PyPrecedent {
    fn from(inner: core::Precedent) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyPrecedent {
    #[getter]
    fn reference(&self) -> PySemanticReference {
        self.inner.reference.clone().into()
    }
    #[getter]
    fn provenance(&self) -> PyProvenance {
        self.inner.provenance.into()
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("reference", self.reference().to_dict(py)?)?;
        out.set_item("provenance", self.provenance().__str__())?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "Precedent(reference={}, provenance={})",
            self.reference().kind().__str__(),
            self.provenance().__str__()
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "PrecedentReport",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyPrecedentReport {
    inner: core::PrecedentReport,
}
impl From<core::PrecedentReport> for PyPrecedentReport {
    fn from(inner: core::PrecedentReport) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyPrecedentReport {
    #[getter]
    fn stamp(&self) -> PyStateStamp {
        self.inner.stamp.into()
    }
    #[getter]
    fn cell(&self) -> String {
        address_text(&self.inner.cell)
    }
    #[getter]
    fn precedents(&self) -> Vec<PyPrecedent> {
        self.inner
            .precedents
            .clone()
            .into_iter()
            .map(Into::into)
            .collect()
    }
    #[getter]
    fn truncation(&self) -> PyTruncationReport {
        self.inner.truncation.clone().into()
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("stamp", self.stamp().to_dict(py)?)?;
        out.set_item("cell", self.cell())?;
        let items = PyList::empty(py);
        for item in self.precedents() {
            items.append(item.to_dict(py)?)?;
        }
        out.set_item("precedents", items)?;
        out.set_item("truncation", self.truncation().to_dict(py)?)?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "PrecedentReport(cell={}, precedents={}, truncated={})",
            self.cell(),
            self.inner.precedents.len(),
            self.inner.truncation.incomplete
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "Dependent",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyDependent {
    inner: core::Dependent,
}
impl From<core::Dependent> for PyDependent {
    fn from(inner: core::Dependent) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyDependent {
    #[getter]
    fn cell(&self) -> String {
        address_text(&self.inner.cell)
    }
    #[getter]
    fn via(&self) -> Vec<String> {
        self.inner.via.iter().map(address_text).collect()
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("cell", self.cell())?;
        out.set_item("via", self.via())?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "Dependent(cell={}, via={})",
            self.cell(),
            self.inner.via.len()
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "DependentsReport",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyDependentsReport {
    inner: core::DependentsReport,
}
impl From<core::DependentsReport> for PyDependentsReport {
    fn from(inner: core::DependentsReport) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyDependentsReport {
    #[getter]
    fn stamp(&self) -> PyStateStamp {
        self.inner.stamp.into()
    }
    #[getter]
    fn cell(&self) -> String {
        address_text(&self.inner.cell)
    }
    #[getter]
    fn dependents(&self) -> Vec<PyDependent> {
        self.inner
            .dependents
            .clone()
            .into_iter()
            .map(Into::into)
            .collect()
    }
    #[getter]
    fn truncation(&self) -> PyTruncationReport {
        self.inner.truncation.clone().into()
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("stamp", self.stamp().to_dict(py)?)?;
        out.set_item("cell", self.cell())?;
        let items = PyList::empty(py);
        for item in self.dependents() {
            items.append(item.to_dict(py)?)?;
        }
        out.set_item("dependents", items)?;
        out.set_item("truncation", self.truncation().to_dict(py)?)?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "DependentsReport(cell={}, dependents={}, truncated={})",
            self.cell(),
            self.inner.dependents.len(),
            self.inner.truncation.incomplete
        )
    }
}

struct TraceGraphData {
    graph: core::TraceGraph,
    addresses: Vec<String>,
    by_address: HashMap<String, usize>,
}
impl TraceGraphData {
    fn new(graph: core::TraceGraph) -> Self {
        let addresses: Vec<_> = graph
            .nodes
            .iter()
            .map(|node| address_text(&node.cell.address))
            .collect();
        let by_address = addresses
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, address)| (address, index))
            .collect();
        Self {
            graph,
            addresses,
            by_address,
        }
    }
}

fn trace_node_index(graph: &TraceGraphData, id: core::TraceNodeId) -> PyResult<usize> {
    // Core assigns TraceNodeId(nodes.len()) immediately before each push in
    // inspect.rs trace_precedents/trace_dependents and indexes IDs the same way.
    let index = id.0 as usize;
    graph
        .graph
        .nodes
        .get(index)
        .filter(|node| node.id == id)
        .map(|_| index)
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "invalid trace node id {}: response node index invariant was violated",
                id.0
            ))
        })
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "TraceNode",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyTraceNode {
    graph: Arc<TraceGraphData>,
    index: usize,
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceNode {
    #[getter]
    fn id(&self) -> u32 {
        self.graph.graph.nodes[self.index].id.0
    }
    #[getter]
    fn cell(&self) -> PyCellSnapshot {
        self.graph.graph.nodes[self.index].cell.clone().into()
    }
    #[getter]
    fn address(&self) -> String {
        self.graph.addresses[self.index].clone()
    }
    #[getter]
    fn links(&self) -> Vec<PyTraceLink> {
        (0..self.graph.graph.nodes[self.index].links.len())
            .map(|link| PyTraceLink {
                graph: self.graph.clone(),
                source: self.index,
                link,
            })
            .collect()
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("id", self.id())?;
        out.set_item("cell", self.cell().to_dict(py)?)?;
        let links = PyList::empty(py);
        for link in self.links() {
            links.append(link.to_dict(py)?)?;
        }
        out.set_item("links", links)?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "TraceNode(address={}, links={})",
            self.address(),
            self.graph.graph.nodes[self.index].links.len()
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "TraceLinkKind",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyTraceLinkKind {
    inner: core::TraceLinkKind,
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceLinkKind {
    #[getter]
    fn kind(&self) -> PyTraceLinkKindType {
        match self.inner {
            core::TraceLinkKind::Formula { .. } => PyTraceLinkKindType::Formula,
            core::TraceLinkKind::SpillAnchor => PyTraceLinkKindType::SpillAnchor,
            core::TraceLinkKind::SpillReader => PyTraceLinkKindType::SpillReader,
            _ => PyTraceLinkKindType::Unknown,
        }
    }
    #[getter]
    fn provenance(&self) -> Option<PyProvenance> {
        match self.inner {
            core::TraceLinkKind::Formula { provenance } => Some(provenance.into()),
            _ => None,
        }
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("kind", self.kind().__str__())?;
        out.set_item("provenance", self.provenance().map(|value| value.__str__()))?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!("TraceLinkKind(kind={})", self.kind().__str__())
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "TraceLinkTarget",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyTraceLinkTarget {
    graph: Arc<TraceGraphData>,
    node: usize,
    disposition: core::LinkDisposition,
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceLinkTarget {
    #[getter]
    fn node(&self) -> PyTraceNode {
        PyTraceNode {
            graph: self.graph.clone(),
            index: self.node,
        }
    }
    #[getter]
    fn disposition(&self) -> PyLinkDisposition {
        self.disposition.into()
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("node", self.graph.addresses[self.node].clone())?;
        out.set_item("disposition", self.disposition().__str__())?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "TraceLinkTarget(node={}, disposition={})",
            self.graph.addresses[self.node],
            self.disposition().__str__()
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "TraceLink",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyTraceLink {
    graph: Arc<TraceGraphData>,
    source: usize,
    link: usize,
}
impl PyTraceLink {
    fn core(&self) -> &core::TraceLink {
        &self.graph.graph.nodes[self.source].links[self.link]
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceLink {
    #[getter]
    fn source(&self) -> PyTraceNode {
        PyTraceNode {
            graph: self.graph.clone(),
            index: self.source,
        }
    }
    #[getter]
    fn reference(&self) -> PySemanticReference {
        self.core().reference.clone().into()
    }
    #[getter]
    fn kind(&self) -> PyTraceLinkKind {
        PyTraceLinkKind {
            inner: self.core().kind,
        }
    }
    #[getter]
    fn targets(&self) -> PyResult<Vec<PyTraceLinkTarget>> {
        self.core()
            .targets
            .iter()
            .map(|target| {
                Ok(PyTraceLinkTarget {
                    graph: self.graph.clone(),
                    node: trace_node_index(&self.graph, target.node)?,
                    disposition: target.disposition,
                })
            })
            .collect()
    }
    #[getter]
    fn omitted(&self) -> Option<PyOmittedCount> {
        self.core().omitted.map(Into::into)
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("source", self.source().address())?;
        out.set_item("reference", self.reference().to_dict(py)?)?;
        out.set_item("kind", self.kind().to_dict(py)?)?;
        let targets = PyList::empty(py);
        for target in self.targets()? {
            targets.append(target.to_dict(py)?)?;
        }
        out.set_item("targets", targets)?;
        match self.omitted() {
            Some(value) => out.set_item("omitted", value.to_dict(py)?)?,
            None => out.set_item("omitted", py.None())?,
        }
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "TraceLink(source={}, targets={}, omitted={})",
            self.graph.addresses[self.source],
            self.core().targets.len(),
            self.core().omitted.is_some()
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "TraceNodes",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyTraceNodes {
    graph: Arc<TraceGraphData>,
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceNodes {
    fn __len__(&self) -> usize {
        self.graph.graph.nodes.len()
    }
    fn __getitem__(&self, address: &str) -> PyResult<PyTraceNode> {
        let original = address;
        let address = address_text(&parse_cell(address)?);
        let address = address.as_str();
        let index =
            self.graph.by_address.get(address).copied().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyKeyError, _>(original.to_string())
            })?;
        Ok(PyTraceNode {
            graph: self.graph.clone(),
            index,
        })
    }
    #[pyo3(signature = (address, default=None))]
    fn get(&self, py: Python<'_>, address: &str, default: Option<PyObject>) -> PyResult<PyObject> {
        let canonical = address_text(&parse_cell(address)?);
        match self.graph.by_address.get(&canonical).copied() {
            Some(index) => Ok(Py::new(
                py,
                PyTraceNode {
                    graph: self.graph.clone(),
                    index,
                },
            )?
            .into_any()),
            None => Ok(default.unwrap_or_else(|| py.None())),
        }
    }
    fn keys(&self) -> Vec<String> {
        self.graph.addresses.clone()
    }
    fn values(&self) -> Vec<PyTraceNode> {
        (0..self.graph.graph.nodes.len())
            .map(|index| PyTraceNode {
                graph: self.graph.clone(),
                index,
            })
            .collect()
    }
    fn items(&self) -> Vec<(String, PyTraceNode)> {
        self.graph
            .addresses
            .iter()
            .cloned()
            .zip(self.values())
            .collect()
    }
    fn __contains__(&self, address: &str) -> PyResult<bool> {
        let canonical = address_text(&parse_cell(address)?);
        Ok(self.graph.by_address.contains_key(&canonical))
    }
    fn __iter__(&self) -> PyTraceNodesIter {
        PyTraceNodesIter {
            addresses: self.graph.addresses.clone(),
            index: 0,
        }
    }
    fn __repr__(&self) -> String {
        format!("TraceNodes(len={})", self.graph.graph.nodes.len())
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(name = "TraceNodesIter", module = "formualizer.formualizer_py")]
pub struct PyTraceNodesIter {
    addresses: Vec<String>,
    index: usize,
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceNodesIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self) -> Option<String> {
        let value = self.addresses.get(self.index).cloned();
        self.index += usize::from(value.is_some());
        value
    }
}

/// An immutable, owned trace graph. `nodes` is mapping-like and keyed by A1 strings.
///
/// NetworkX recipe (no dependency is required):
///
/// ```python
/// graph = nx.MultiDiGraph()
/// graph.add_nodes_from(trace.nodes)
/// for link in trace.links:
///     graph.add_edges_from(
///         (link.source.address, target.node.address, {"kind": str(link.kind.kind)})
///         for target in link.targets
///     )
/// ```
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "TraceGraph",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyTraceGraph {
    inner: Arc<TraceGraphData>,
}
impl From<core::TraceGraph> for PyTraceGraph {
    fn from(graph: core::TraceGraph) -> Self {
        Self {
            inner: Arc::new(TraceGraphData::new(graph)),
        }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceGraph {
    #[getter]
    fn stamp(&self) -> PyStateStamp {
        self.inner.graph.stamp.into()
    }
    #[getter]
    fn direction(&self) -> PyResult<PyTraceDirection> {
        self.inner.graph.direction.try_into()
    }
    #[getter]
    fn nodes(&self) -> PyTraceNodes {
        PyTraceNodes {
            graph: self.inner.clone(),
        }
    }
    #[getter]
    #[rustfmt::skip]
    fn roots(&self) -> PyResult<Vec<PyTraceNode>> {
        let checked_indices = self
            .inner
            .graph
            .roots
            .iter()
            .map(|id| Ok((*id, trace_node_index(&self.inner, *id)?)))
            .collect::<PyResult<HashMap<_, _>>>()?;
        let nodes =
        self.inner
            .graph
            .roots
            .iter()
            .map(|id| PyTraceNode {
                graph: self.inner.clone(),
                index: checked_indices[id],
            })
            .collect();
        Ok(nodes)
    }
    #[getter]
    fn links(&self) -> Vec<PyTraceLink> {
        self.inner
            .graph
            .nodes
            .iter()
            .enumerate()
            .flat_map(|(source, node)| {
                (0..node.links.len()).map(move |link| PyTraceLink {
                    graph: self.inner.clone(),
                    source,
                    link,
                })
            })
            .collect()
    }
    #[getter]
    fn truncation(&self) -> PyTruncationReport {
        self.inner.graph.truncation.clone().into()
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("stamp", self.stamp().to_dict(py)?)?;
        out.set_item("direction", self.direction()?.__str__())?;
        out.set_item(
            "roots",
            self.roots()?
                .iter()
                .map(PyTraceNode::address)
                .collect::<Vec<_>>(),
        )?;
        let nodes = PyDict::new(py);
        for address in &self.inner.addresses {
            nodes.set_item(address, self.nodes().__getitem__(address)?.to_dict(py)?)?;
        }
        out.set_item("nodes", nodes)?;
        let links = PyList::empty(py);
        for link in self.links() {
            links.append(link.to_dict(py)?)?;
        }
        out.set_item("links", links)?;
        out.set_item("truncation", self.truncation().to_dict(py)?)?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "TraceGraph(nodes={}, links={}, roots={}, truncated={})",
            self.inner.graph.nodes.len(),
            self.inner
                .graph
                .nodes
                .iter()
                .map(|node| node.links.len())
                .sum::<usize>(),
            self.inner.graph.roots.len(),
            self.inner.graph.truncation.incomplete
        )
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pyclass)]
#[pyclass(
    name = "RangePage",
    module = "formualizer.formualizer_py",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyRangePage {
    inner: core::RangePage,
}
impl From<core::RangePage> for PyRangePage {
    fn from(inner: core::RangePage) -> Self {
        Self { inner }
    }
}
#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyRangePage {
    #[getter]
    fn stamp(&self) -> PyStateStamp {
        self.inner.stamp.into()
    }
    #[getter]
    fn declared(&self) -> String {
        area_text(&self.inner.declared)
    }
    #[getter]
    fn resolved(&self) -> Option<String> {
        self.inner.resolved.as_ref().map(range_text)
    }
    #[getter]
    fn total(&self) -> u64 {
        self.inner.total
    }
    #[getter]
    fn offset(&self) -> u64 {
        self.inner.offset
    }
    #[getter]
    fn items(&self) -> Vec<PyCellSnapshot> {
        self.inner
            .items
            .clone()
            .into_iter()
            .map(Into::into)
            .collect()
    }
    #[getter]
    fn next_offset(&self) -> Option<u64> {
        self.inner.next_offset
    }
    /// Return a nested dict whose schema matches this report and its nested reports.
    fn to_dict(&self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyDict::new(py);
        out.set_item("stamp", self.stamp().to_dict(py)?)?;
        out.set_item("declared", self.declared())?;
        out.set_item("resolved", self.resolved())?;
        out.set_item("total", self.total())?;
        out.set_item("offset", self.offset())?;
        let items = PyList::empty(py);
        for item in self.items() {
            items.append(item.to_dict(py)?)?;
        }
        out.set_item("items", items)?;
        out.set_item("next_offset", self.next_offset())?;
        Ok(out.into_any().unbind())
    }
    fn __repr__(&self) -> String {
        format!(
            "RangePage(items={}, total={}, offset={}, has_next={})",
            self.inner.items.len(),
            self.inner.total,
            self.inner.offset,
            self.inner.next_offset.is_some()
        )
    }
}

value_semantics_hash!(PyStateStamp, "StateStamp");
value_semantics_hash!(PyOmittedCount, "OmittedCount");
value_semantics_hash!(PySpillRole, "SpillRole");
value_semantics_hash!(PyTraceLinkKind, "TraceLinkKind");
value_semantics_unhashable!(PyTruncationReport, "TruncationReport");
value_semantics_unhashable!(PyCellSnapshot, "CellSnapshot");
value_semantics_unhashable!(PyCellSnapshotReport, "CellSnapshotReport");
value_semantics_unhashable!(PyNameResolution, "NameResolution");
value_semantics_unhashable!(PySemanticReference, "SemanticReference");
value_semantics_unhashable!(PyPrecedent, "Precedent");
value_semantics_unhashable!(PyPrecedentReport, "PrecedentReport");
value_semantics_unhashable!(PyDependent, "Dependent");
value_semantics_unhashable!(PyDependentsReport, "DependentsReport");
value_semantics_unhashable!(PyRangePage, "RangePage");

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceNode {
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other.extract::<PyRef<'_, Self>>().is_ok_and(|other| {
            self.graph.graph.nodes[self.index] == other.graph.graph.nodes[other.index]
        })
    }
    fn __ne__(&self, other: &Bound<'_, PyAny>) -> bool {
        !self.__eq__(other)
    }
    fn __hash__(&self) -> PyResult<isize> {
        Err(unhashable("TraceNode"))
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceLinkTarget {
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other.extract::<PyRef<'_, Self>>().is_ok_and(|other| {
            self.graph.graph.nodes[self.node].id == other.graph.graph.nodes[other.node].id
                && self.disposition == other.disposition
        })
    }
    fn __ne__(&self, other: &Bound<'_, PyAny>) -> bool {
        !self.__eq__(other)
    }
    fn __hash__(&self) -> isize {
        python_hash(&(self.graph.graph.nodes[self.node].id, self.disposition))
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceLink {
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other
            .extract::<PyRef<'_, Self>>()
            .is_ok_and(|other| self.core() == other.core())
    }
    fn __ne__(&self, other: &Bound<'_, PyAny>) -> bool {
        !self.__eq__(other)
    }
    fn __hash__(&self) -> PyResult<isize> {
        Err(unhashable("TraceLink"))
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyTraceGraph {
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other
            .extract::<PyRef<'_, Self>>()
            .is_ok_and(|other| self.inner.graph == other.inner.graph)
    }
    fn __ne__(&self, other: &Bound<'_, PyAny>) -> bool {
        !self.__eq__(other)
    }
    fn __hash__(&self) -> PyResult<isize> {
        Err(unhashable("TraceGraph"))
    }
}

#[cfg_attr(not(target_os = "emscripten"), gen_stub_pymethods)]
#[pymethods]
impl PyWorkbook {
    /// Inspect one cell without evaluating it. Raises `InspectionError`; inspect its `code` attribute for a stable category.
    #[pyo3(signature = (address, *, include_values=true))]
    fn inspect_cell(&self, address: &str, include_values: bool) -> PyResult<PyCellSnapshotReport> {
        let address = parse_cell(address)?;
        let wb = self.read_inner()?;
        wb.engine()
            .inspect_cell(
                &address,
                &core::SnapshotOptions::default().with_include_values(include_values),
            )
            .map(Into::into)
            .map_err(inspect_error_to_pyerr)
    }
    /// Return source-ordered declared precedents. Budgets truncate in-band.
    #[pyo3(signature = (address, *, max_links=256, max_work=100_000))]
    fn precedents(
        &self,
        address: &str,
        max_links: u32,
        max_work: u64,
    ) -> PyResult<PyPrecedentReport> {
        let address = parse_cell(address)?;
        let options = core::PrecedentOptions::default()
            .with_max_links(max_links)
            .with_max_work(max_work);
        let wb = self.read_inner()?;
        wb.engine()
            .precedents(&address, &options)
            .map(Into::into)
            .map_err(inspect_error_to_pyerr)
    }
    /// Return address-sorted direct dependents, retaining the address-least `max_results` entries.
    #[pyo3(signature = (address, *, max_results=256, max_work=100_000))]
    fn dependents(
        &self,
        address: &str,
        max_results: u32,
        max_work: u64,
    ) -> PyResult<PyDependentsReport> {
        let address = parse_cell(address)?;
        let options = core::DependentsOptions::default()
            .with_max_results(max_results)
            .with_max_work(max_work);
        let wb = self.read_inner()?;
        wb.engine()
            .dependents(&address, &options)
            .map(Into::into)
            .map_err(inspect_error_to_pyerr)
    }
    /// Build a bounded owned trace. `roots` contains sheet-qualified A1 cell strings.
    #[pyo3(signature = (roots, *, direction=PyTraceDirection::Precedents, max_depth=6, max_nodes=512, max_links=1024, max_work=100_000, range_member_budget=256, include_values=true))]
    #[allow(clippy::too_many_arguments)]
    fn trace(
        &self,
        roots: Vec<String>,
        direction: PyTraceDirection,
        max_depth: u32,
        max_nodes: u32,
        max_links: u32,
        max_work: u64,
        range_member_budget: u32,
        include_values: bool,
    ) -> PyResult<PyTraceGraph> {
        let roots = roots
            .iter()
            .map(|root| parse_cell(root))
            .collect::<PyResult<Vec<_>>>()?;
        let options = core::TraceOptions::default()
            .with_direction(direction.into())
            .with_max_depth(max_depth)
            .with_max_nodes(max_nodes)
            .with_max_links(max_links)
            .with_max_work(max_work)
            .with_range_member_budget(range_member_budget)
            .with_include_values(include_values);
        let wb = self.read_inner()?;
        wb.engine()
            .trace(&roots, &options)
            .map(Into::into)
            .map_err(inspect_error_to_pyerr)
    }
    /// Page row-major through a finite or open A1 range without evaluating it.
    #[pyo3(signature = (area, *, offset=0, limit=100, include_values=true, expected_stamp=None))]
    fn range_page(
        &self,
        area: &str,
        offset: u64,
        limit: u32,
        include_values: bool,
        expected_stamp: Option<PyStateStamp>,
    ) -> PyResult<PyRangePage> {
        let area = parse_area(area)?;
        let mut options = core::RangePageOptions::default()
            .with_offset(offset)
            .with_limit(limit)
            .with_include_values(include_values);
        if let Some(stamp) = expected_stamp {
            options = options.with_expected_stamp(stamp.inner);
        }
        let wb = self.read_inner()?;
        wb.engine()
            .range_page(&area, &options)
            .map(Into::into)
            .map_err(inspect_error_to_pyerr)
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyStaleness>()?;
    m.add_class::<PyProvenance>()?;
    m.add_class::<PyLinkDisposition>()?;
    m.add_class::<PyTraceDirection>()?;
    m.add_class::<PyOmittedCountKind>()?;
    m.add_class::<PySpillRoleKind>()?;
    m.add_class::<PyReferenceKind>()?;
    m.add_class::<PyNameResolutionKind>()?;
    m.add_class::<PyTraceLinkKindType>()?;
    m.add_class::<PyStateStamp>()?;
    m.add_class::<PyOmittedCount>()?;
    m.add_class::<PyTruncationReport>()?;
    m.add_class::<PySpillRole>()?;
    m.add_class::<PyCellSnapshot>()?;
    m.add_class::<PyCellSnapshotReport>()?;
    m.add_class::<PyNameResolution>()?;
    m.add_class::<PySemanticReference>()?;
    m.add_class::<PyPrecedent>()?;
    m.add_class::<PyPrecedentReport>()?;
    m.add_class::<PyDependent>()?;
    m.add_class::<PyDependentsReport>()?;
    m.add_class::<PyTraceNode>()?;
    m.add_class::<PyTraceLinkKind>()?;
    m.add_class::<PyTraceLinkTarget>()?;
    m.add_class::<PyTraceLink>()?;
    m.add_class::<PyTraceNodes>()?;
    m.add_class::<PyTraceNodesIter>()?;
    m.add_class::<PyTraceGraph>()?;
    m.add_class::<PyRangePage>()?;
    let mapping = m.py().import("collections.abc")?.getattr("Mapping")?;
    mapping.call_method1("register", (m.getattr("TraceNodes")?,))?;
    Ok(())
}
