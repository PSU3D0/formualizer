use crate::Workbook;
use crate::errors::inspect_error_to_js;
use crate::utils::js_error;
use crate::workbook::{BindingValue, binding_value, reject_unknown_keys};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, prelude::*};

use formualizer::{
    CellAddress, CellSnapshot, DependentsOptions, InspectError, LinkDisposition, LiteralValue,
    NameResolution, OmittedCount, PrecedentOptions, Provenance, RangeArea, RangePageOptions,
    SemanticReference, SnapshotOptions, SpillRole, Staleness, StateStamp, TraceDirection,
    TraceLinkKind, TraceOptions,
};

const JS_MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

const CELL_ADDRESS_KEYS: &[&str] = &["sheet", "row", "column"];
const RANGE_AREA_KEYS: &[&str] = &["sheet", "startRow", "startColumn", "endRow", "endColumn"];
const SNAPSHOT_OPTION_KEYS: &[&str] = &["includeValues"];
const PRECEDENT_OPTION_KEYS: &[&str] = &["maxLinks", "maxWork"];
const DEPENDENTS_OPTION_KEYS: &[&str] = &["maxResults", "maxWork"];
const TRACE_OPTION_KEYS: &[&str] = &[
    "direction",
    "maxDepth",
    "maxNodes",
    "maxLinks",
    "maxWork",
    "rangeMemberBudget",
    "includeValues",
];
const RANGE_PAGE_OPTION_KEYS: &[&str] = &["offset", "limit", "includeValues", "expectedStamp"];
const STATE_STAMP_KEYS: &[&str] = &["mutationRevision", "recalcEpoch"];

#[derive(Clone, Copy)]
enum ValidationKind {
    Address,
    Options,
}

fn validation_error(kind: ValidationKind, message: impl Into<String>) -> JsValue {
    let message = message.into();
    match kind {
        ValidationKind::Address => inspect_error_to_js(InspectError::InvalidAddress { message }),
        ValidationKind::Options => inspect_error_to_js(InspectError::InvalidOptions { message }),
    }
}

fn js_error_message(error: JsValue) -> String {
    error
        .dyn_ref::<js_sys::Error>()
        .and_then(|error| error.message().as_string())
        .or_else(|| error.as_string())
        .unwrap_or_else(|| "input validation failed".to_string())
}

fn serde_error_message(error: serde_wasm_bindgen::Error) -> String {
    let message = error.to_string();
    message
        .strip_prefix("Error: ")
        .unwrap_or(&message)
        .to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JsCellAddress {
    sheet: String,
    row: u32,
    column: u32,
}

impl TryFrom<JsCellAddress> for CellAddress {
    type Error = JsValue;

    fn try_from(value: JsCellAddress) -> Result<Self, Self::Error> {
        CellAddress::new(value.sheet, value.row, value.column).map_err(|error| {
            validation_error(ValidationKind::Address, format!("cell address: {error}"))
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JsRangeArea {
    sheet: String,
    start_row: Option<u32>,
    start_column: Option<u32>,
    end_row: Option<u32>,
    end_column: Option<u32>,
}

impl TryFrom<JsRangeArea> for RangeArea {
    type Error = JsValue;

    fn try_from(value: JsRangeArea) -> Result<Self, Self::Error> {
        RangeArea::new(
            value.sheet,
            value.start_row,
            value.start_column,
            value.end_row,
            value.end_column,
        )
        .map_err(|error| validation_error(ValidationKind::Address, format!("range area: {error}")))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JsSnapshotOptions {
    include_values: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JsPrecedentOptions {
    max_links: Option<u32>,
    max_work: Option<f64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JsDependentsOptions {
    max_results: Option<u32>,
    max_work: Option<f64>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum JsTraceDirection {
    Precedents,
    Dependents,
}

impl From<JsTraceDirection> for TraceDirection {
    fn from(value: JsTraceDirection) -> Self {
        match value {
            JsTraceDirection::Precedents => Self::Precedents,
            JsTraceDirection::Dependents => Self::Dependents,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JsTraceOptions {
    direction: Option<JsTraceDirection>,
    max_depth: Option<u32>,
    max_nodes: Option<u32>,
    max_links: Option<u32>,
    max_work: Option<f64>,
    range_member_budget: Option<u32>,
    include_values: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JsStateStamp {
    mutation_revision: String,
    recalc_epoch: String,
}

impl TryFrom<JsStateStamp> for StateStamp {
    type Error = JsValue;

    fn try_from(value: JsStateStamp) -> Result<Self, Self::Error> {
        let parse = |field: &str, raw: String| {
            raw.parse::<u64>().map_err(|_| {
                validation_error(
                    ValidationKind::Options,
                    format!("expectedStamp.{field}: expected a decimal u64 string"),
                )
            })
        };
        Ok(Self {
            mutation_revision: parse("mutationRevision", value.mutation_revision)?,
            recalc_epoch: parse("recalcEpoch", value.recalc_epoch)?,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JsRangePageOptions {
    offset: Option<f64>,
    limit: Option<u32>,
    include_values: Option<bool>,
    expected_stamp: Option<JsStateStamp>,
}

fn parse_object<T: for<'de> Deserialize<'de>>(
    raw: JsValue,
    context: &str,
    allowed: &[&str],
    kind: ValidationKind,
) -> Result<T, JsValue> {
    reject_unknown_keys(&raw, context, allowed)
        .map_err(|error| validation_error(kind, js_error_message(error)))?;
    serde_wasm_bindgen::from_value(raw).map_err(|error| {
        validation_error(kind, format!("{context}: {}", serde_error_message(error)))
    })
}

fn parse_options<T: for<'de> Deserialize<'de> + Default>(
    raw: Option<JsValue>,
    context: &str,
    allowed: &[&str],
) -> Result<T, JsValue> {
    match raw {
        Some(value) if !value.is_null() && !value.is_undefined() => {
            parse_object(value, context, allowed, ValidationKind::Options)
        }
        _ => Ok(T::default()),
    }
}

fn parse_safe_u64(value: f64, field: &str) -> Result<u64, JsValue> {
    if !value.is_finite() || value.fract() != 0.0 || !(0.0..=JS_MAX_SAFE_INTEGER).contains(&value) {
        return Err(validation_error(
            ValidationKind::Options,
            format!("{field}: expected a non-negative safe integer"),
        ));
    }
    Ok(value as u64)
}

fn snapshot_options(raw: Option<JsValue>) -> Result<SnapshotOptions, JsValue> {
    let raw: JsSnapshotOptions = parse_options(raw, "snapshot options", SNAPSHOT_OPTION_KEYS)?;
    let mut options = SnapshotOptions::default();
    if let Some(value) = raw.include_values {
        options = options.with_include_values(value);
    }
    Ok(options)
}

fn precedent_options(raw: Option<JsValue>) -> Result<PrecedentOptions, JsValue> {
    let raw: JsPrecedentOptions = parse_options(raw, "precedent options", PRECEDENT_OPTION_KEYS)?;
    let mut options = PrecedentOptions::default();
    if let Some(value) = raw.max_links {
        options = options.with_max_links(value);
    }
    if let Some(value) = raw.max_work {
        options = options.with_max_work(parse_safe_u64(value, "maxWork")?);
    }
    Ok(options)
}

fn dependents_options(raw: Option<JsValue>) -> Result<DependentsOptions, JsValue> {
    let raw: JsDependentsOptions =
        parse_options(raw, "dependents options", DEPENDENTS_OPTION_KEYS)?;
    let mut options = DependentsOptions::default();
    if let Some(value) = raw.max_results {
        options = options.with_max_results(value);
    }
    if let Some(value) = raw.max_work {
        options = options.with_max_work(parse_safe_u64(value, "maxWork")?);
    }
    Ok(options)
}

fn trace_options(raw: Option<JsValue>) -> Result<TraceOptions, JsValue> {
    let raw: JsTraceOptions = parse_options(raw, "trace options", TRACE_OPTION_KEYS)?;
    let mut options = TraceOptions::default();
    if let Some(value) = raw.direction {
        options = options.with_direction(value.into());
    }
    if let Some(value) = raw.max_depth {
        options = options.with_max_depth(value);
    }
    if let Some(value) = raw.max_nodes {
        options = options.with_max_nodes(value);
    }
    if let Some(value) = raw.max_links {
        options = options.with_max_links(value);
    }
    if let Some(value) = raw.max_work {
        options = options.with_max_work(parse_safe_u64(value, "maxWork")?);
    }
    if let Some(value) = raw.range_member_budget {
        options = options.with_range_member_budget(value);
    }
    if let Some(value) = raw.include_values {
        options = options.with_include_values(value);
    }
    Ok(options)
}

fn range_page_options(raw: Option<JsValue>) -> Result<RangePageOptions, JsValue> {
    if let Some(value) = raw.as_ref()
        && !value.is_null()
        && !value.is_undefined()
    {
        reject_unknown_keys(value, "range page options", RANGE_PAGE_OPTION_KEYS)
            .map_err(|error| validation_error(ValidationKind::Options, js_error_message(error)))?;
        let expected_stamp = js_sys::Reflect::get(value, &JsValue::from_str("expectedStamp"))
            .map_err(|_| {
                validation_error(
                    ValidationKind::Options,
                    "range page options: could not read expectedStamp",
                )
            })?;
        if !expected_stamp.is_null() && !expected_stamp.is_undefined() {
            reject_unknown_keys(&expected_stamp, "expectedStamp", STATE_STAMP_KEYS).map_err(
                |error| validation_error(ValidationKind::Options, js_error_message(error)),
            )?;
        }
    }
    let raw: JsRangePageOptions = parse_options(raw, "range page options", RANGE_PAGE_OPTION_KEYS)?;
    let mut options = RangePageOptions::default();
    if let Some(value) = raw.offset {
        options = options.with_offset(parse_safe_u64(value, "offset")?);
    }
    if let Some(value) = raw.limit {
        options = options.with_limit(value);
    }
    if let Some(value) = raw.include_values {
        options = options.with_include_values(value);
    }
    if let Some(value) = raw.expected_stamp {
        options = options.with_expected_stamp(value.try_into()?);
    }
    Ok(options)
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireStateStamp {
    mutation_revision: String,
    recalc_epoch: String,
}

impl From<StateStamp> for WireStateStamp {
    fn from(value: StateStamp) -> Self {
        Self {
            mutation_revision: value.mutation_revision.to_string(),
            recalc_epoch: value.recalc_epoch.to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireCellAddress {
    sheet: String,
    row: u32,
    column: u32,
}

impl From<&CellAddress> for WireCellAddress {
    fn from(value: &CellAddress) -> Self {
        Self {
            sheet: value.sheet.clone(),
            row: value.row,
            column: value.column,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireRangeArea {
    sheet: String,
    start_row: Option<u32>,
    start_column: Option<u32>,
    end_row: Option<u32>,
    end_column: Option<u32>,
}

impl From<&RangeArea> for WireRangeArea {
    fn from(value: &RangeArea) -> Self {
        Self {
            sheet: value.sheet.clone(),
            start_row: value.start_row,
            start_column: value.start_column,
            end_row: value.end_row,
            end_column: value.end_column,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireRangeAddress {
    sheet: String,
    start_row: u32,
    start_column: u32,
    end_row: u32,
    end_column: u32,
}

impl From<&formualizer::common::RangeAddress> for WireRangeAddress {
    fn from(value: &formualizer::common::RangeAddress) -> Self {
        Self {
            sheet: value.sheet.clone(),
            start_row: value.start_row,
            start_column: value.start_col,
            end_row: value.end_row,
            end_column: value.end_col,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
enum WireTraceValue {
    Null(()),
    Boolean(bool),
    Number(f64),
    Text(String),
    Array(Vec<Vec<WireTraceValue>>),
    Tagged(WireTaggedValue),
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum WireTaggedValue {
    Error {
        code: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    Date {
        value: String,
    },
    Datetime {
        value: String,
    },
    Time {
        value: String,
    },
    Duration {
        value: String,
    },
    Pending,
}

impl From<BindingValue> for WireTraceValue {
    fn from(value: BindingValue) -> Self {
        match value {
            BindingValue::Empty => Self::Null(()),
            BindingValue::Boolean(value) => Self::Boolean(value),
            BindingValue::Number(value) => Self::Number(value),
            BindingValue::Text(value) => Self::Text(value),
            BindingValue::Array(rows) => Self::Array(
                rows.into_iter()
                    .map(|row| row.into_iter().map(Self::from).collect())
                    .collect(),
            ),
            BindingValue::Error { code, message, .. } => {
                Self::Tagged(WireTaggedValue::Error { code, message })
            }
            BindingValue::Date(value) => Self::Tagged(WireTaggedValue::Date { value }),
            BindingValue::DateTime(value) => Self::Tagged(WireTaggedValue::Datetime { value }),
            BindingValue::Time(value) => Self::Tagged(WireTaggedValue::Time { value }),
            BindingValue::Duration(value) => Self::Tagged(WireTaggedValue::Duration { value }),
            BindingValue::Pending => Self::Tagged(WireTaggedValue::Pending),
        }
    }
}

impl From<&LiteralValue> for WireTraceValue {
    fn from(value: &LiteralValue) -> Self {
        binding_value(value).into()
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum WireStaleness {
    Current,
    Dirty,
    NeverEvaluated,
}

fn wire_staleness(value: Staleness) -> Result<WireStaleness, JsValue> {
    match value {
        Staleness::Current => Ok(WireStaleness::Current),
        Staleness::Dirty => Ok(WireStaleness::Dirty),
        Staleness::NeverEvaluated => Ok(WireStaleness::NeverEvaluated),
        _ => Err(js_error("unsupported staleness variant")),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "role", rename_all = "camelCase")]
enum WireSpillRole {
    Anchor { extent: WireRangeAddress },
    Member { anchor: WireCellAddress },
}

fn wire_spill(value: &SpillRole) -> Result<WireSpillRole, JsValue> {
    match value {
        SpillRole::Anchor { extent } => Ok(WireSpillRole::Anchor {
            extent: extent.into(),
        }),
        SpillRole::Member { anchor } => Ok(WireSpillRole::Member {
            anchor: anchor.into(),
        }),
        _ => Err(js_error("unsupported spill role variant")),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireCellSnapshot {
    address: WireCellAddress,
    formula: Option<String>,
    value: Option<WireTraceValue>,
    value_included: bool,
    staleness: WireStaleness,
    volatile: bool,
    spill: Option<WireSpillRole>,
}

fn wire_cell(value: &CellSnapshot) -> Result<WireCellSnapshot, JsValue> {
    Ok(WireCellSnapshot {
        address: (&value.address).into(),
        formula: value.formula.clone(),
        value: value.value.as_ref().map(Into::into),
        value_included: value.value_included,
        staleness: wire_staleness(value.staleness)?,
        volatile: value.volatile,
        spill: value.spill.as_ref().map(wire_spill).transpose()?,
    })
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum WireProvenance {
    Declared,
    Observed,
}

fn wire_provenance(value: Provenance) -> Result<WireProvenance, JsValue> {
    match value {
        Provenance::Declared => Ok(WireProvenance::Declared),
        Provenance::Observed => Ok(WireProvenance::Observed),
        _ => Err(js_error("unsupported provenance variant")),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum WireNameResolution {
    Cell {
        address: WireCellAddress,
    },
    Range {
        declared: WireRangeArea,
        resolved: Option<WireRangeAddress>,
    },
    Literal {
        value: WireTraceValue,
    },
    Formula {
        formula: String,
        value: Option<WireTraceValue>,
    },
    Unresolved,
}

fn wire_name_resolution(value: &NameResolution) -> Result<WireNameResolution, JsValue> {
    match value {
        NameResolution::Cell(address) => Ok(WireNameResolution::Cell {
            address: address.into(),
        }),
        NameResolution::Range { declared, resolved } => Ok(WireNameResolution::Range {
            declared: declared.into(),
            resolved: resolved.as_ref().map(Into::into),
        }),
        NameResolution::Literal(value) => Ok(WireNameResolution::Literal {
            value: value.into(),
        }),
        NameResolution::Formula { formula, value } => Ok(WireNameResolution::Formula {
            formula: formula.clone(),
            value: value.as_ref().map(Into::into),
        }),
        NameResolution::Unresolved => Ok(WireNameResolution::Unresolved),
        _ => Err(js_error("unsupported name resolution variant")),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum WireSemanticReference {
    Cell {
        address: WireCellAddress,
    },
    Range {
        declared: WireRangeArea,
        resolved: Option<WireRangeAddress>,
        cell_count: f64,
    },
    Name {
        name: String,
        resolution: WireNameResolution,
    },
    Table {
        name: String,
        specifier: String,
        resolved: WireRangeAddress,
    },
    External {
        raw: String,
    },
    Unsupported {
        text: String,
        reason: String,
    },
}

fn wire_reference(value: &SemanticReference) -> Result<WireSemanticReference, JsValue> {
    match value {
        SemanticReference::Cell(address) => Ok(WireSemanticReference::Cell {
            address: address.into(),
        }),
        SemanticReference::Range {
            declared,
            resolved,
            cell_count,
        } => Ok(WireSemanticReference::Range {
            declared: declared.into(),
            resolved: resolved.as_ref().map(Into::into),
            // Grid-bounded to 1,048,576 * 16,384, well below 2^53.
            cell_count: *cell_count as f64,
        }),
        SemanticReference::Name { name, resolution } => Ok(WireSemanticReference::Name {
            name: name.clone(),
            resolution: wire_name_resolution(resolution)?,
        }),
        SemanticReference::Table {
            name,
            specifier,
            resolved,
        } => Ok(WireSemanticReference::Table {
            name: name.clone(),
            specifier: specifier.clone(),
            resolved: resolved.into(),
        }),
        SemanticReference::External { raw } => {
            Ok(WireSemanticReference::External { raw: raw.clone() })
        }
        SemanticReference::Unsupported { text, reason } => Ok(WireSemanticReference::Unsupported {
            text: text.clone(),
            reason: reason.clone(),
        }),
        _ => Err(js_error("unsupported semantic reference variant")),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum WireOmittedCount {
    Exact { count: String },
    AtLeast { count: String },
}

fn wire_omitted(value: OmittedCount) -> Result<WireOmittedCount, JsValue> {
    match value {
        OmittedCount::Exact(count) => Ok(WireOmittedCount::Exact {
            count: count.to_string(),
        }),
        OmittedCount::AtLeast(count) => Ok(WireOmittedCount::AtLeast {
            count: count.to_string(),
        }),
        _ => Err(js_error("unsupported omitted count variant")),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireTruncation {
    incomplete: bool,
    omitted: Option<WireOmittedCount>,
}

fn wire_truncation(value: &formualizer::TruncationReport) -> Result<WireTruncation, JsValue> {
    Ok(WireTruncation {
        incomplete: value.incomplete,
        omitted: value.omitted.map(wire_omitted).transpose()?,
    })
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireCellSnapshotReport {
    stamp: WireStateStamp,
    cell: WireCellSnapshot,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WirePrecedent {
    reference: WireSemanticReference,
    provenance: WireProvenance,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WirePrecedentReport {
    stamp: WireStateStamp,
    cell: WireCellAddress,
    precedents: Vec<WirePrecedent>,
    truncation: WireTruncation,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireDependent {
    cell: WireCellAddress,
    via: Vec<WireCellAddress>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireDependentsReport {
    stamp: WireStateStamp,
    cell: WireCellAddress,
    dependents: Vec<WireDependent>,
    truncation: WireTruncation,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum WireDirection {
    Precedents,
    Dependents,
}

fn wire_direction(value: TraceDirection) -> Result<WireDirection, JsValue> {
    match value {
        TraceDirection::Precedents => Ok(WireDirection::Precedents),
        TraceDirection::Dependents => Ok(WireDirection::Dependents),
        _ => Err(js_error("unsupported trace direction variant")),
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum WireDisposition {
    Expanded,
    Convergent,
    Cycle,
    Elided,
}

fn wire_disposition(value: LinkDisposition) -> Result<WireDisposition, JsValue> {
    match value {
        LinkDisposition::Expanded => Ok(WireDisposition::Expanded),
        LinkDisposition::Convergent => Ok(WireDisposition::Convergent),
        LinkDisposition::Cycle => Ok(WireDisposition::Cycle),
        LinkDisposition::Elided => Ok(WireDisposition::Elided),
        _ => Err(js_error("unsupported link disposition variant")),
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum WireLinkKind {
    Formula,
    SpillAnchor,
    SpillReader,
}

fn wire_link_kind(value: TraceLinkKind) -> Result<(WireLinkKind, Option<WireProvenance>), JsValue> {
    match value {
        TraceLinkKind::Formula { provenance } => {
            Ok((WireLinkKind::Formula, Some(wire_provenance(provenance)?)))
        }
        TraceLinkKind::SpillAnchor => Ok((WireLinkKind::SpillAnchor, None)),
        TraceLinkKind::SpillReader => Ok((WireLinkKind::SpillReader, None)),
        _ => Err(js_error("unsupported trace link kind variant")),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireTraceLinkTarget {
    node: u32,
    disposition: WireDisposition,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireTraceLink {
    reference: WireSemanticReference,
    kind: WireLinkKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance: Option<WireProvenance>,
    targets: Vec<WireTraceLinkTarget>,
    omitted: Option<WireOmittedCount>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireTraceNode {
    id: u32,
    cell: WireCellSnapshot,
    links: Vec<WireTraceLink>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireTraceGraph {
    stamp: WireStateStamp,
    direction: WireDirection,
    roots: Vec<u32>,
    nodes: Vec<WireTraceNode>,
    truncation: WireTruncation,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireRangePage {
    stamp: WireStateStamp,
    declared: WireRangeArea,
    resolved: Option<WireRangeAddress>,
    total: f64,
    offset: f64,
    items: Vec<WireCellSnapshot>,
    next_offset: Option<f64>,
}

fn report_to_js<T: Serialize>(report: &T) -> Result<JsValue, JsValue> {
    report
        .serialize(
            &serde_wasm_bindgen::Serializer::new()
                .serialize_maps_as_objects(true)
                .serialize_missing_as_null(true),
        )
        .map_err(|error| js_error(format!("inspection serialization failed: {error}")))
}

fn map_inspect_error(error: InspectError) -> JsValue {
    inspect_error_to_js(error)
}

#[wasm_bindgen]
impl Workbook {
    /// Return an owned, read-only snapshot of one cell.
    #[wasm_bindgen(js_name = "inspectCell")]
    pub fn inspect_cell_js(
        &self,
        cell: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let cell: CellAddress = parse_object::<JsCellAddress>(
            cell,
            "cell address",
            CELL_ADDRESS_KEYS,
            ValidationKind::Address,
        )?
        .try_into()?;
        let options = snapshot_options(options)?;
        let workbook_arc = self.inner_arc();
        let workbook = workbook_arc
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?;
        let report = workbook
            .engine()
            .inspect_cell(&cell, &options)
            .map_err(map_inspect_error)?;
        let wire = WireCellSnapshotReport {
            stamp: report.stamp.into(),
            cell: wire_cell(&report.cell)?,
        };
        report_to_js(&wire)
    }

    /// Return source-ordered, first-occurrence-deduplicated declared precedents.
    #[wasm_bindgen(js_name = "precedents")]
    pub fn precedents_js(
        &self,
        cell: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let cell: CellAddress = parse_object::<JsCellAddress>(
            cell,
            "cell address",
            CELL_ADDRESS_KEYS,
            ValidationKind::Address,
        )?
        .try_into()?;
        let options = precedent_options(options)?;
        let workbook_arc = self.inner_arc();
        let workbook = workbook_arc
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?;
        let report = workbook
            .engine()
            .precedents(&cell, &options)
            .map_err(map_inspect_error)?;
        let precedents = report
            .precedents
            .iter()
            .map(|precedent| {
                Ok(WirePrecedent {
                    reference: wire_reference(&precedent.reference)?,
                    provenance: wire_provenance(precedent.provenance)?,
                })
            })
            .collect::<Result<_, JsValue>>()?;
        let wire = WirePrecedentReport {
            stamp: report.stamp.into(),
            cell: (&report.cell).into(),
            precedents,
            truncation: wire_truncation(&report.truncation)?,
        };
        report_to_js(&wire)
    }

    /// Return bounded dependents in canonical address order.
    #[wasm_bindgen(js_name = "dependents")]
    pub fn dependents_js(
        &self,
        cell: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let cell: CellAddress = parse_object::<JsCellAddress>(
            cell,
            "cell address",
            CELL_ADDRESS_KEYS,
            ValidationKind::Address,
        )?
        .try_into()?;
        let options = dependents_options(options)?;
        let workbook_arc = self.inner_arc();
        let workbook = workbook_arc
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?;
        let report = workbook
            .engine()
            .dependents(&cell, &options)
            .map_err(map_inspect_error)?;
        let wire = WireDependentsReport {
            stamp: report.stamp.into(),
            cell: (&report.cell).into(),
            dependents: report
                .dependents
                .iter()
                .map(|dependent| WireDependent {
                    cell: (&dependent.cell).into(),
                    via: dependent.via.iter().map(Into::into).collect(),
                })
                .collect(),
            truncation: wire_truncation(&report.truncation)?,
        };
        report_to_js(&wire)
    }

    /// Build a bounded, owned trace graph for one or more roots.
    #[wasm_bindgen(js_name = "trace")]
    pub fn trace_js(&self, roots: JsValue, options: Option<JsValue>) -> Result<JsValue, JsValue> {
        if !js_sys::Array::is_array(&roots) {
            return Err(validation_error(
                ValidationKind::Options,
                "trace roots: expected an array",
            ));
        }
        let roots: js_sys::Array = roots.unchecked_into();
        let mut parsed_roots = Vec::with_capacity(roots.length() as usize);
        for (index, root) in roots.iter().enumerate() {
            let root = parse_object::<JsCellAddress>(
                root,
                &format!("trace root at index {index}"),
                CELL_ADDRESS_KEYS,
                ValidationKind::Address,
            )?;
            parsed_roots.push(root.try_into()?);
        }
        let options = trace_options(options)?;
        let workbook_arc = self.inner_arc();
        let workbook = workbook_arc
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?;
        let report = workbook
            .engine()
            .trace(&parsed_roots, &options)
            .map_err(map_inspect_error)?;
        let nodes = report
            .nodes
            .iter()
            .map(|node| {
                let links = node
                    .links
                    .iter()
                    .map(|link| {
                        let (kind, provenance) = wire_link_kind(link.kind)?;
                        Ok(WireTraceLink {
                            reference: wire_reference(&link.reference)?,
                            kind,
                            provenance,
                            targets: link
                                .targets
                                .iter()
                                .map(|target| {
                                    Ok(WireTraceLinkTarget {
                                        node: target.node.0,
                                        disposition: wire_disposition(target.disposition)?,
                                    })
                                })
                                .collect::<Result<_, JsValue>>()?,
                            omitted: link.omitted.map(wire_omitted).transpose()?,
                        })
                    })
                    .collect::<Result<_, JsValue>>()?;
                Ok(WireTraceNode {
                    id: node.id.0,
                    cell: wire_cell(&node.cell)?,
                    links,
                })
            })
            .collect::<Result<_, JsValue>>()?;
        let wire = WireTraceGraph {
            stamp: report.stamp.into(),
            direction: wire_direction(report.direction)?,
            roots: report.roots.iter().map(|root| root.0).collect(),
            nodes,
            truncation: wire_truncation(&report.truncation)?,
        };
        report_to_js(&wire)
    }

    /// Return a row-major page over a finite or semantically resolved open area.
    #[wasm_bindgen(js_name = "rangePage")]
    pub fn range_page_js(
        &self,
        area: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let area: RangeArea = parse_object::<JsRangeArea>(
            area,
            "range area",
            RANGE_AREA_KEYS,
            ValidationKind::Address,
        )?
        .try_into()?;
        let options = range_page_options(options)?;
        let workbook_arc = self.inner_arc();
        let workbook = workbook_arc
            .read()
            .map_err(|_| js_error("failed to lock workbook for read"))?;
        let report = workbook
            .engine()
            .range_page(&area, &options)
            .map_err(map_inspect_error)?;
        let wire = WireRangePage {
            stamp: report.stamp.into(),
            declared: (&report.declared).into(),
            resolved: report.resolved.as_ref().map(Into::into),
            // A resolved grid area contains at most 2^34 cells.
            total: report.total as f64,
            // Binding validation restricts the caller's offset to <= 2^53 - 1.
            offset: report.offset as f64,
            items: report
                .items
                .iter()
                .map(wire_cell)
                .collect::<Result<_, JsValue>>()?,
            // Core bounds next_offset by the grid-bounded total.
            next_offset: report.next_offset.map(|offset| offset as f64),
        };
        report_to_js(&wire)
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    fn stamp_and_omitted_u64_values_cross_as_exact_decimal_strings() {
        // A public call cannot feasibly advance revisions beyond 2^53 in a test;
        // direct wire construction proves the only precision-sensitive boundary.
        let above_safe_integer = 9_007_199_254_740_993_u64;
        let stamp = report_to_js(&WireStateStamp::from(StateStamp {
            mutation_revision: above_safe_integer,
            recalc_epoch: above_safe_integer + 1,
        }))
        .unwrap();
        for (key, expected) in [
            ("mutationRevision", above_safe_integer),
            ("recalcEpoch", above_safe_integer + 1),
        ] {
            assert_eq!(
                js_sys::Reflect::get(&stamp, &JsValue::from_str(key))
                    .unwrap()
                    .as_string()
                    .unwrap(),
                expected.to_string()
            );
        }
        assert!(!stamp.is_instance_of::<js_sys::Map>());

        let omitted =
            report_to_js(&wire_omitted(OmittedCount::AtLeast(above_safe_integer)).unwrap())
                .unwrap();
        assert_eq!(
            js_sys::Reflect::get(&omitted, &JsValue::from_str("count"))
                .unwrap()
                .as_string()
                .unwrap(),
            above_safe_integer.to_string()
        );
    }
}
