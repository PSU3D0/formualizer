use std::collections::BTreeMap;
use std::fmt;

use regex::Regex;
use schemars::json_schema;
use schemars::{JsonSchema, SchemaGenerator};
use semver::Version;
use serde::de::{self, Deserializer, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::borrow::Cow;

use crate::validation::{ManifestIssue, ValidationError};

/// Current supported FIO specification version.
pub const CURRENT_SPEC_VERSION: &str = "0.3.0";
/// Constant identifier for this spec.
pub const SPEC_IDENT: &str = "fio";

/// Conformance profile advertised by a manifest.
///
/// Profiles gate optional/forward-looking features so runtimes can safely reject
/// manifests that use selectors they don't support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Profile {
    /// Core profile: A1, named range, and layout selectors only.
    #[default]
    CoreV0,
    /// Full profile (reserved): enables structured refs and workbook table selectors.
    FullV0,
}

/// Optional capabilities block for feature gating.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    #[serde(default)]
    pub profile: Profile,
    #[serde(default)]
    pub features: Option<Vec<String>>,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            profile: Profile::CoreV0,
            features: None,
        }
    }
}

/// Canonical manifest representation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(
    title = "Formualizer I/O Manifest (SheetPort)",
    description = "Specification that binds typed input/output ports to a spreadsheet so it can be treated as a pure function.",
    example = crate::manifest::example_data::supply_planning_example()
)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// Identifier for this specification (must be `fio`).
    pub spec: String,
    #[serde(rename = "spec_version")]
    pub spec_version: SpecVersion,
    #[serde(default)]
    /// Optional conformance capabilities for this manifest.
    pub capabilities: Option<Capabilities>,
    /// Human-facing metadata describing the manifest.
    pub manifest: ManifestMeta,
    /// Ordered list of typed ports.
    pub ports: Vec<Port>,
}

impl Manifest {
    /// Construct a manifest by reading YAML from any reader.
    pub fn from_yaml_reader<R: std::io::Read>(reader: R) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_reader(reader)
    }

    /// Construct a manifest from a YAML string slice.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    /// Serialize this manifest to YAML.
    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    /// Normalize the manifest in-place for deterministic comparison.
    ///
    /// - Ports are sorted lexicographically by id.
    /// - Tags (if any) are sorted and deduplicated.
    /// - Table keys (if any) are sorted and deduplicated.
    /// - Enumerated constraint values are sorted and deduplicated.
    pub fn normalize(&mut self) {
        if let Some(tags) = &mut self.manifest.tags {
            tags.sort();
            tags.dedup();
        }

        if let Some(capabilities) = &mut self.capabilities
            && let Some(features) = &mut capabilities.features
        {
            features.sort();
            features.dedup();
        }

        self.ports.sort_by(|a, b| a.id.cmp(&b.id));

        for port in &mut self.ports {
            if let Some(constraints) = &mut port.constraints {
                canonicalize_enum(&mut constraints.r#enum);
            }

            match &mut port.schema {
                Schema::Record(record) => {
                    for field in record.fields.values_mut() {
                        if let Some(constraints) = &mut field.constraints {
                            canonicalize_enum(&mut constraints.r#enum);
                        }
                    }
                }
                Schema::Table(table) => {
                    if let Some(keys) = &mut table.keys {
                        keys.sort();
                        keys.dedup();
                    }
                }
                _ => {}
            }
        }
    }

    /// Return a normalized copy of the manifest.
    pub fn normalized(mut self) -> Self {
        self.normalize();
        self
    }

    /// Return the effective conformance profile for this manifest.
    ///
    /// When capabilities are omitted, the manifest is treated as `core-v0`.
    pub fn effective_profile(&self) -> Profile {
        self.capabilities
            .as_ref()
            .map(|c| c.profile)
            .unwrap_or_default()
    }

    /// Validate the manifest and return granular issues when invariants fail.
    pub fn validate(&self) -> Result<(), ValidationError> {
        let mut issues = Vec::new();

        if self.spec != SPEC_IDENT {
            issues.push(ManifestIssue::new(
                "spec",
                format!(
                    "expected spec identifier `{}`, found `{}`",
                    SPEC_IDENT, self.spec
                ),
            ));
        }

        let current_version = Version::parse(CURRENT_SPEC_VERSION)
            .expect("CURRENT_SPEC_VERSION must be valid semver");
        let spec_version = &self.spec_version.0;
        if spec_version.major != current_version.major {
            issues.push(ManifestIssue::new(
                "spec_version",
                format!(
                    "incompatible major version `{}` (expected `{}`)",
                    spec_version, current_version.major
                ),
            ));
        }

        let id_pattern = Regex::new(r"^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$")
            .expect("manifest id regex must compile");
        if !id_pattern.is_match(&self.manifest.id) {
            issues.push(ManifestIssue::new(
                "manifest.id",
                "id must be lowercase alphanumeric with hyphens, 3-64 chars".to_string(),
            ));
        }

        let mut seen_ids = std::collections::HashSet::new();
        let port_id_pattern =
            Regex::new(r"^[a-z0-9]+([_-][a-z0-9]+)*$").expect("port id regex must compile");

        let profile = self.effective_profile();

        for (idx, port) in self.ports.iter().enumerate() {
            let path = format!("ports[{}].id", idx);
            if !port_id_pattern.is_match(&port.id) {
                issues.push(ManifestIssue::new(
                    &path,
                    "port id must contain lowercase alphanumeric characters optionally separated by '-' or '_'"
                        .to_string(),
                ));
            }
            if !seen_ids.insert(&port.id) {
                issues.push(ManifestIssue::new(
                    &path,
                    format!("duplicate port id `{}`", port.id),
                ));
            }

            validate_port_selector(profile, port, idx, &mut issues);

            if port.dir == Direction::Out && port.default.is_some() {
                issues.push(ManifestIssue::new(
                    format!("ports[{}].default", idx),
                    "defaults may only be defined on `in` ports".to_string(),
                ));
            }

            if let Selector::Layout(layout) = &port.location
                && matches!(layout.layout.terminate, LayoutTermination::UntilMarker)
                && layout
                    .layout
                    .marker_text
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
            {
                issues.push(ManifestIssue::new(
                    format!("ports[{}].location.layout.marker_text", idx),
                    "marker_text must be provided when terminate == \"until_marker\"".to_string(),
                ));
            }

            if let Some(constraints) = &port.constraints {
                let value_type = match &port.schema {
                    Schema::Scalar(schema) => Some(schema.value_type),
                    Schema::Range(schema) => Some(schema.cell_type),
                    _ => None,
                };
                validate_constraints(
                    constraints,
                    value_type,
                    format!("ports[{}].constraints", idx),
                    &mut issues,
                );
            }

            if port.shape == Shape::Record {
                if let Schema::Record(record) = &port.schema {
                    if record.fields.is_empty() {
                        issues.push(ManifestIssue::new(
                            format!("ports[{}].schema.fields", idx),
                            "record schema must define at least one field".to_string(),
                        ));
                    }
                } else {
                    issues.push(ManifestIssue::new(
                        format!("ports[{}].schema", idx),
                        "record shape must use a record schema".to_string(),
                    ));
                }
            }

            if port.shape == Shape::Table {
                if let Schema::Table(table) = &port.schema {
                    if table.columns.is_empty() {
                        issues.push(ManifestIssue::new(
                            format!("ports[{}].schema.columns", idx),
                            "table schema must define at least one column".to_string(),
                        ));
                    }
                    if let Some(keys) = &table.keys {
                        for key in keys {
                            if !table.columns.iter().any(|c| &c.name == key) {
                                issues.push(ManifestIssue::new(
                                    format!("ports[{}].schema.keys", idx),
                                    format!(
                                        "key `{}` not found among table columns ({:?})",
                                        key,
                                        table
                                            .columns
                                            .iter()
                                            .map(|c| c.name.clone())
                                            .collect::<Vec<_>>()
                                    ),
                                ));
                            }
                        }
                    }
                } else {
                    issues.push(ManifestIssue::new(
                        format!("ports[{}].schema", idx),
                        "table shape must use a table schema".to_string(),
                    ));
                }
            }

            if let Schema::Record(record) = &port.schema {
                for (field_name, field) in &record.fields {
                    if profile == Profile::CoreV0
                        && matches!(field.location, FieldSelector::StructRef(_))
                    {
                        issues.push(ManifestIssue::new(
                            format!("ports[{}].schema.fields.{}.location", idx, field_name),
                            format!(
                                "structured references are not permitted under profile `{}`",
                                profile_label(profile)
                            ),
                        ));
                    }

                    if let Some(constraints) = &field.constraints {
                        validate_constraints(
                            constraints,
                            Some(field.value_type),
                            format!("ports[{}].schema.fields.{}.constraints", idx, field_name),
                            &mut issues,
                        );
                    }
                }
            }
        }

        if issues.is_empty() {
            Ok(())
        } else {
            Err(ValidationError::new(issues))
        }
    }
}

/// Manifest metadata block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManifestMeta {
    /// Stable identifier for the manifest (lowercase alphanumeric + hyphen).
    pub id: String,
    /// Human readable manifest name.
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub workbook: Option<WorkbookMeta>,
    #[serde(default)]
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

/// Optional workbook descriptors. These fields are advisory hints for runtimes and may be ignored unless a runtime explicitly documents support.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkbookMeta {
    #[serde(default)]
    /// Optional workbook URI (sharepoint://, file://, etc.).
    pub uri: Option<String>,
    #[serde(default)]
    /// Locale hint for parsing numbers/dates.
    pub locale: Option<String>,
    #[serde(default)]
    /// Expected Excel date system (1900 or 1904).
    pub date_system: Option<i32>,
    #[serde(default)]
    /// Time zone identifier for datetime interpretation.
    pub timezone: Option<String>,
}

/// Port direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// Input port (values provided to the workbook).
    In,
    /// Output port (values read from the workbook).
    Out,
}

/// Port shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Shape {
    /// Scalar value (single cell).
    Scalar,
    /// Record of named scalar fields.
    Record,
    /// Rectangular range with uniform type.
    Range,
    /// Table with named columns.
    Table,
}

/// Port definition.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Port {
    /// Unique identifier for the port within the manifest.
    pub id: String,
    /// Direction (input or output).
    pub dir: Direction,
    /// Structural shape of the port.
    pub shape: Shape,
    #[serde(default)]
    /// Optional documentation.
    pub description: Option<String>,
    #[serde(default = "default_true")]
    /// Whether the port value is required (defaults to true).
    pub required: bool,
    /// Selector binding the port to a workbook region.
    pub location: Selector,
    /// Type information for values carried by the port.
    pub schema: Schema,
    #[serde(default)]
    /// Optional constraints applied to values.
    pub constraints: Option<Constraints>,
    #[serde(default)]
    /// Optional units metadata.
    pub units: Option<Units>,
    #[serde(default)]
    /// Optional default value (inputs only).
    pub default: Option<JsonValue>,
    #[serde(default)]
    /// Reserved hint for future partitioning/sharding semantics. No effect in `core-v0` runtimes.
    pub partition_key: Option<bool>,
}

fn default_true() -> bool {
    true
}

/// Selector union.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Selector {
    A1(SelectorA1),
    Name(SelectorName),
    Table(SelectorTable),
    StructRef(SelectorStructRef),
    Layout(SelectorLayout),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SelectorA1 {
    /// Absolute A1-style reference to a cell or range (e.g., `Sheet1!A1:C10`).
    pub a1: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SelectorName {
    /// Workbook-defined name (global or sheet scoped).
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SelectorStructRef {
    /// Excel structured reference syntax (e.g., `TblOrders[Qty]`). Reserved in `core-v0`; requires `full-v0` profile.
    pub struct_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SelectorTable {
    /// Workbook table selector (by Excel table name). Reserved in `core-v0`; requires `full-v0` profile.
    pub table: TableSelector,
}

/// Selector for an Excel table. Reserved in `core-v0`; requires `full-v0` profile.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableSelector {
    /// Excel table name.
    pub name: String,
    #[serde(default)]
    /// Optional target area within the table.
    pub area: Option<TableArea>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum TableArea {
    /// Header row of the table.
    Header,
    /// Table body rows (default).
    Body,
    /// Totals row.
    Totals,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SelectorLayout {
    /// Declarative layout descriptor for header-based regions.
    pub layout: LayoutDescriptor,
}

/// Layout resolution behavior for layout selectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum LayoutKind {
    /// Header-driven layout using contiguous columns starting at `anchor_col`.
    #[default]
    HeaderContiguousV1,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LayoutDescriptor {
    #[serde(default)]
    /// Layout resolution behavior (defaults to `header_contiguous_v1`).
    pub kind: LayoutKind,
    /// Sheet containing the layout.
    pub sheet: String,
    /// 1-based index of the header row.
    pub header_row: u32,
    /// Column letter where the layout begins.
    pub anchor_col: String,
    /// Termination rule for the layout.
    pub terminate: LayoutTermination,
    #[serde(default)]
    /// Marker text required when `terminate` equals `until_marker`.
    pub marker_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LayoutTermination {
    FirstBlankRow,
    SheetEnd,
    UntilMarker,
}

/// Schema union.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Schema {
    Scalar(ScalarSchema),
    Record(RecordSchema),
    Range(RangeSchema),
    Table(TableSchema),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScalarSchema {
    #[serde(rename = "type")]
    /// Scalar value type.
    pub value_type: ValueType,
    #[serde(default)]
    /// Optional format hint.
    pub format: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordSchema {
    #[serde(default)]
    pub kind: RecordKind,
    /// Mapping of field names to scalar schema definitions.
    pub fields: BTreeMap<String, RecordField>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum RecordKind {
    #[default]
    Record,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordField {
    #[serde(rename = "type")]
    /// Scalar value type for the field.
    pub value_type: ValueType,
    /// Selector resolving to the field cell.
    pub location: FieldSelector,
    #[serde(default)]
    pub constraints: Option<Constraints>,
    #[serde(default)]
    pub units: Option<Units>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum FieldSelector {
    A1(SelectorA1),
    Name(SelectorName),
    StructRef(SelectorStructRef),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RangeSchema {
    #[serde(default)]
    pub kind: RangeKind,
    /// Value type enforced for each cell.
    pub cell_type: ValueType,
    #[serde(default)]
    pub format: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum RangeKind {
    #[default]
    Range,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableSchema {
    #[serde(default)]
    pub kind: TableKind,
    /// Column definitions.
    pub columns: Vec<TableColumn>,
    #[serde(default)]
    /// Optional list of column names forming a logical primary key.
    pub keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum TableKind {
    #[default]
    Table,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableColumn {
    /// Column name exposed to clients.
    pub name: String,
    #[serde(rename = "type")]
    /// Scalar type for column cells.
    pub value_type: ValueType,
    #[serde(default)]
    /// Optional column letter hint when using layout selectors.
    pub col: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub units: Option<Units>,
}

/// Scalar value types.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    String,
    Number,
    Integer,
    Boolean,
    Date,
    Datetime,
}

/// Constraints applied to a port or field.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Constraints {
    #[serde(default)]
    /// Minimum allowed numeric value.
    pub min: Option<f64>,
    #[serde(default)]
    /// Maximum allowed numeric value.
    pub max: Option<f64>,
    #[serde(default)]
    /// Enumerated set of allowed categorical values. Entries are compared by exact JSON equality after type checking; numeric values are not normalized (e.g., 5 != 5.0).
    pub r#enum: Option<Vec<JsonValue>>,
    #[serde(default)]
    /// Regular expression pattern string.
    pub pattern: Option<String>,
    #[serde(default)]
    /// Whether null/blank values are permitted.
    pub nullable: Option<bool>,
}

/// Units metadata (extensible).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Units {
    #[serde(default)]
    /// Currency code (ISO 4217).
    pub currency: Option<String>,
}

/// Wrapper around semver::Version for serde compatibility.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SpecVersion(pub Version);

impl SpecVersion {
    pub fn new(version: Version) -> Self {
        Self(version)
    }
}

impl Serialize for SpecVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for SpecVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionVisitor;

        impl<'de> Visitor<'de> for VersionVisitor {
            type Value = SpecVersion;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("semantic version string (e.g. 0.3.0)")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Version::parse(v)
                    .map(SpecVersion)
                    .map_err(|err| de::Error::custom(format!("invalid spec_version: {err}")))
            }
        }

        deserializer.deserialize_str(VersionVisitor)
    }
}

impl JsonSchema for SpecVersion {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("SpecVersion")
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> schemars::Schema {
        json_schema!({
            "type": "string",
            "pattern": r"^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z-.]+)?(?:\+[0-9A-Za-z-.]+)?$"
        })
    }
}

impl std::str::FromStr for Manifest {
    type Err = serde_yaml::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Manifest::from_yaml_str(s)
    }
}

fn profile_label(profile: Profile) -> &'static str {
    match profile {
        Profile::CoreV0 => "core-v0",
        Profile::FullV0 => "full-v0",
    }
}

fn profile_allows_struct_ref(profile: Profile) -> bool {
    matches!(profile, Profile::FullV0)
}

fn profile_allows_table(profile: Profile) -> bool {
    matches!(profile, Profile::FullV0)
}

fn validate_port_selector(
    profile: Profile,
    port: &Port,
    idx: usize,
    issues: &mut Vec<ManifestIssue>,
) {
    let path = format!("ports[{}].location", idx);
    match port.shape {
        Shape::Scalar => match &port.location {
            Selector::A1(_) | Selector::Name(_) => {}
            Selector::StructRef(_) if profile_allows_struct_ref(profile) => {}
            Selector::StructRef(_) => issues.push(ManifestIssue::new(
                &path,
                format!(
                    "structured references are not permitted under profile `{}`",
                    profile_label(profile)
                ),
            )),
            Selector::Layout(_) | Selector::Table(_) => issues.push(ManifestIssue::new(
                &path,
                "scalar ports may only use `a1`, `name`, or `struct_ref` selectors".to_string(),
            )),
        },
        Shape::Record | Shape::Range => match &port.location {
            Selector::A1(_) | Selector::Name(_) | Selector::Layout(_) => {}
            Selector::StructRef(_) if profile_allows_struct_ref(profile) => {}
            Selector::StructRef(_) => issues.push(ManifestIssue::new(
                &path,
                format!(
                    "structured references are not permitted under profile `{}`",
                    profile_label(profile)
                ),
            )),
            Selector::Table(_) => issues.push(ManifestIssue::new(
                &path,
                "record/range ports may not use `table` selectors".to_string(),
            )),
        },
        Shape::Table => match &port.location {
            Selector::Layout(_) => {}
            Selector::Table(_) if profile_allows_table(profile) => {}
            Selector::Table(_) => issues.push(ManifestIssue::new(
                &path,
                format!(
                    "`table` selectors are reserved and not permitted under profile `{}`",
                    profile_label(profile)
                ),
            )),
            Selector::A1(_) | Selector::Name(_) | Selector::StructRef(_) => {
                issues.push(ManifestIssue::new(
                    &path,
                    "table ports must use `layout` selectors (or `table` selectors under full-v0)"
                        .to_string(),
                ))
            }
        },
    }
}

fn canonicalize_enum(values: &mut Option<Vec<JsonValue>>) {
    if let Some(list) = values {
        list.sort_by_key(value_sort_key);
        list.dedup();
    }
}

fn value_sort_key(value: &JsonValue) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn validate_constraints(
    constraints: &Constraints,
    value_type: Option<ValueType>,
    base_path: String,
    issues: &mut Vec<ManifestIssue>,
) {
    if let (Some(min), Some(max)) = (constraints.min, constraints.max)
        && min > max
    {
        issues.push(ManifestIssue::new(
            format!("{}.min", base_path),
            format!("`min` value {min} exceeds `max` value {max}"),
        ));
    }

    if let Some(vt) = value_type {
        if constraints.min.is_some() && !is_numeric_type(vt) {
            issues.push(ManifestIssue::new(
                format!("{}.min", base_path),
                format!("`min` constraint requires numeric type, found `{vt:?}`"),
            ));
        }
        if constraints.max.is_some() && !is_numeric_type(vt) {
            issues.push(ManifestIssue::new(
                format!("{}.max", base_path),
                format!("`max` constraint requires numeric type, found `{vt:?}`"),
            ));
        }
    }

    if let Some(enum_values) = &constraints.r#enum {
        if enum_values.is_empty() {
            issues.push(ManifestIssue::new(
                format!("{}.enum", base_path),
                "enumerated values must contain at least one entry".to_string(),
            ));
        } else if let Some(vt) = value_type {
            for (i, candidate) in enum_values.iter().enumerate() {
                if let Err(message) = validate_enum_candidate(vt, candidate) {
                    issues.push(ManifestIssue::new(
                        format!("{}.enum[{}]", base_path, i),
                        message,
                    ));
                }
            }
        }
    }

    if let Some(pattern) = &constraints.pattern
        && let Err(err) = Regex::new(pattern)
    {
        issues.push(ManifestIssue::new(
            format!("{}.pattern", base_path),
            format!("invalid regex pattern `{pattern}`: {err}"),
        ));
    }
}

fn is_numeric_type(vt: ValueType) -> bool {
    matches!(vt, ValueType::Number | ValueType::Integer)
}

fn validate_enum_candidate(vt: ValueType, candidate: &JsonValue) -> Result<(), String> {
    use serde_json::Value as J;
    match vt {
        ValueType::String => match candidate {
            J::String(_) => Ok(()),
            other => Err(format!(
                "enum value `{}` is not a string",
                value_sort_key(other)
            )),
        },
        ValueType::Boolean => match candidate {
            J::Bool(_) => Ok(()),
            other => Err(format!(
                "enum value `{}` is not a boolean",
                value_sort_key(other)
            )),
        },
        ValueType::Number => match candidate {
            J::Number(n) if n.as_f64().is_some() => Ok(()),
            other => Err(format!(
                "enum value `{}` is not numeric",
                value_sort_key(other)
            )),
        },
        ValueType::Integer => match candidate {
            J::Number(n) => {
                if n.as_i64().is_some() {
                    Ok(())
                } else if let Some(f) = n.as_f64() {
                    if (f - f.trunc()).abs() < f64::EPSILON {
                        Ok(())
                    } else {
                        Err(format!(
                            "enum value `{}` is not an integer",
                            value_sort_key(candidate)
                        ))
                    }
                } else {
                    Err(format!(
                        "enum value `{}` is not numeric",
                        value_sort_key(candidate)
                    ))
                }
            }
            other => Err(format!(
                "enum value `{}` is not numeric",
                value_sort_key(other)
            )),
        },
        ValueType::Date => match candidate {
            J::String(s) if parse_date_string(s) => Ok(()),
            other => Err(format!(
                "enum value `{}` is not a valid date",
                value_sort_key(other)
            )),
        },
        ValueType::Datetime => match candidate {
            J::String(s) if parse_datetime_string(s) => Ok(()),
            other => Err(format!(
                "enum value `{}` is not a valid datetime",
                value_sort_key(other)
            )),
        },
    }
}

fn parse_date_string(raw: &str) -> bool {
    chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d").is_ok()
}

fn parse_datetime_string(raw: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(raw).is_ok()
        || chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S").is_ok()
        || chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S").is_ok()
}

pub(crate) mod example_data {
    use super::*;

    pub fn supply_planning_example() -> Manifest {
        serde_json::from_value(serde_json::json!({
            "spec": SPEC_IDENT,
            "spec_version": CURRENT_SPEC_VERSION,
            "capabilities": { "profile": "core-v0" },
            "manifest": {
                "id": "supply-planning-io",
                "name": "Supply Planning I/O",
                "description": "Expose the workbook as a function that ingests inventory data and produces restock recommendations.",
                "workbook": {
                    "uri": "file://Samples/SupplyPlan.xlsx",
                    "locale": "en-US",
                    "date_system": 1900
                }
            },
            "ports": [
                {
                    "id": "warehouse_code",
                    "dir": "in",
                    "shape": "scalar",
                    "description": "Warehouse identifier used for restock planning.",
                    "location": { "a1": "Inputs!B2" },
                    "schema": { "type": "string" },
                    "constraints": { "pattern": "^[A-Z]{2}-\\d{3}$" }
                },
                {
                    "id": "planning_window",
                    "dir": "in",
                    "shape": "record",
                    "description": "Planning horizon (month and year).",
                    "location": { "a1": "Inputs!B1:C1" },
                    "schema": {
                        "kind": "record",
                        "fields": {
                            "month": {
                                "type": "integer",
                                "location": { "a1": "Inputs!B1" },
                                "constraints": { "min": 1, "max": 12 }
                            },
                            "year": {
                                "type": "integer",
                                "location": { "a1": "Inputs!C1" }
                            }
                        }
                    }
                },
                {
                    "id": "sku_inventory",
                    "dir": "in",
                    "shape": "table",
                    "description": "Current inventory snapshot by SKU.",
                    "location": {
                        "layout": {
                            "sheet": "Inventory",
                            "header_row": 1,
                            "anchor_col": "A",
                            "terminate": "first_blank_row"
                        }
                    },
                    "schema": {
                        "kind": "table",
                        "columns": [
                            { "name": "sku", "type": "string", "col": "A" },
                            { "name": "description", "type": "string", "col": "B" },
                            { "name": "on_hand", "type": "integer", "col": "C" },
                            { "name": "safety_stock", "type": "integer", "col": "D" },
                            { "name": "lead_time_days", "type": "integer", "col": "E" }
                        ],
                        "keys": ["sku"]
                    }
                },
                {
                    "id": "restock_summary",
                    "dir": "out",
                    "shape": "record",
                    "description": "High-level metrics summarizing the recommended restock.",
                    "location": { "a1": "Outputs!B2:B6" },
                    "schema": {
                        "kind": "record",
                        "fields": {
                            "total_skus": { "type": "integer", "location": { "a1": "Outputs!B2" } },
                            "units_to_order": { "type": "integer", "location": { "a1": "Outputs!B3" } },
                            "estimated_cost": { "type": "number", "location": { "a1": "Outputs!B4" }, "units": { "currency": "USD" } },
                            "next_restock_date": { "type": "date", "location": { "a1": "Outputs!B5" } }
                        }
                    }
                }
            ]
        }))
        .expect("example manifest should be valid")
    }
}
