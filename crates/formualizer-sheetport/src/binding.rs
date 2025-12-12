use crate::error::SheetPortError;
use crate::location::{AreaLocation, FieldLocation, ScalarLocation, TableLocation};
use crate::resolver::{
    resolve_area_location, resolve_field_location, resolve_scalar_location, resolve_table_location,
};
use crate::value::{PortValue, TableRow, TableValue};
use chrono::{NaiveDate, NaiveDateTime};
use serde_json::Value as JsonValue;
use sheetport_spec::{
    Constraints, Direction, Manifest, ManifestIssue, Port, Profile, RecordSchema, Schema, Shape,
    TableSchema, Units, ValueType,
};
use std::collections::BTreeMap;

fn profile_label(profile: Profile) -> &'static str {
    match profile {
        Profile::CoreV0 => "core-v0",
        Profile::FullV0 => "full-v0",
    }
}

/// Bound manifest along with per-port selector metadata.
#[derive(Debug, Clone)]
pub struct ManifestBindings {
    manifest: Manifest,
    bindings: Vec<PortBinding>,
}

impl ManifestBindings {
    /// Validate and bind a manifest into runtime-friendly structures.
    pub fn new(manifest: Manifest) -> Result<Self, SheetPortError> {
        manifest.validate()?;

        let profile = manifest.effective_profile();
        if profile != Profile::CoreV0 {
            return Err(SheetPortError::InvalidManifest {
                issues: vec![ManifestIssue::new(
                    "capabilities.profile",
                    format!(
                        "profile `{}` is not supported by this runtime (supported: core-v0)",
                        profile_label(profile)
                    ),
                )],
            });
        }

        let mut bindings = Vec::with_capacity(manifest.ports.len());
        for (idx, port) in manifest.ports.iter().enumerate() {
            bindings.push(PortBinding::bind(idx, port)?);
        }
        Ok(Self { manifest, bindings })
    }

    /// Access the original manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Retrieve the bound ports in declaration order.
    pub fn bindings(&self) -> &[PortBinding] {
        &self.bindings
    }

    /// Consume the bindings and return owned components.
    pub fn into_parts(self) -> (Manifest, Vec<PortBinding>) {
        (self.manifest, self.bindings)
    }

    /// Locate a bound port by id.
    pub fn get(&self, id: &str) -> Option<&PortBinding> {
        self.bindings.iter().find(|binding| binding.id == id)
    }
}

/// Fully resolved port description.
#[derive(Debug, Clone)]
pub struct PortBinding {
    pub index: usize,
    pub id: String,
    pub direction: Direction,
    pub required: bool,
    pub description: Option<String>,
    pub constraints: Option<Constraints>,
    pub units: Option<Units>,
    pub default: Option<JsonValue>,
    pub resolved_default: Option<PortValue>,
    pub partition_key: bool,
    pub kind: BoundPort,
}

impl PortBinding {
    fn bind(index: usize, port: &Port) -> Result<Self, SheetPortError> {
        let (kind, resolved_default) = match (&port.shape, &port.schema) {
            (Shape::Scalar, Schema::Scalar(schema)) => {
                let location = resolve_scalar_location(&port.id, &port.location)?;
                let default = port
                    .default
                    .as_ref()
                    .map(|value| {
                        literal_from_json(&port.id, port.id.as_str(), schema.value_type, value)
                            .map(PortValue::Scalar)
                    })
                    .transpose()?;
                (
                    BoundPort::Scalar(ScalarBinding {
                        value_type: schema.value_type,
                        format: schema.format.clone(),
                        location,
                    }),
                    default,
                )
            }
            (Shape::Record, Schema::Record(schema)) => {
                let location = resolve_area_location(&port.id, &port.location)?;
                let mut fields = BTreeMap::new();
                for (name, field) in schema.fields.iter() {
                    let location = resolve_field_location(&port.id, name, &field.location)?;
                    fields.insert(
                        name.to_string(),
                        RecordFieldBinding {
                            value_type: field.value_type,
                            constraints: field.constraints.clone(),
                            units: field.units.clone(),
                            location,
                        },
                    );
                }
                let default = port
                    .default
                    .as_ref()
                    .map(|value| convert_record_default(&port.id, schema, value))
                    .transpose()?;
                (
                    BoundPort::Record(RecordBinding { location, fields }),
                    default,
                )
            }
            (Shape::Range, Schema::Range(schema)) => {
                let location = resolve_area_location(&port.id, &port.location)?;
                let default = port
                    .default
                    .as_ref()
                    .map(|value| convert_range_default(&port.id, schema.cell_type, value))
                    .transpose()?;
                (
                    BoundPort::Range(RangeBinding {
                        cell_type: schema.cell_type,
                        format: schema.format.clone(),
                        location,
                    }),
                    default,
                )
            }
            (Shape::Table, Schema::Table(schema)) => {
                let location = resolve_table_location(&port.id, &port.location)?;
                let columns = schema
                    .columns
                    .iter()
                    .map(|col| TableColumnBinding {
                        name: col.name.clone(),
                        value_type: col.value_type,
                        column_hint: col.col.clone(),
                        format: col.format.clone(),
                        units: col.units.clone(),
                    })
                    .collect::<Vec<_>>();
                let default = port
                    .default
                    .as_ref()
                    .map(|value| convert_table_default(&port.id, schema, value))
                    .transpose()?;
                let keys = schema.keys.clone().unwrap_or_default();
                (
                    BoundPort::Table(TableBinding {
                        location,
                        columns,
                        keys,
                    }),
                    default,
                )
            }
            _ => {
                return Err(SheetPortError::InvariantViolation {
                    port: port.id.clone(),
                    message: "port shape and schema are inconsistent".to_string(),
                });
            }
        };

        Ok(Self {
            index,
            id: port.id.clone(),
            direction: port.dir,
            required: port.required,
            description: port.description.clone(),
            constraints: port.constraints.clone(),
            units: port.units.clone(),
            default: port.default.clone(),
            resolved_default,
            partition_key: port.partition_key.unwrap_or(false),
            kind,
        })
    }
}

/// Union of bound port kinds.
#[derive(Debug, Clone)]
pub enum BoundPort {
    Scalar(ScalarBinding),
    Record(RecordBinding),
    Range(RangeBinding),
    Table(TableBinding),
}

/// Scalar port binding.
#[derive(Debug, Clone)]
pub struct ScalarBinding {
    pub value_type: ValueType,
    pub format: Option<String>,
    pub location: ScalarLocation,
}

/// Range port binding.
#[derive(Debug, Clone)]
pub struct RangeBinding {
    pub cell_type: ValueType,
    pub format: Option<String>,
    pub location: AreaLocation,
}

/// Record port binding with per-field metadata.
#[derive(Debug, Clone)]
pub struct RecordBinding {
    pub location: AreaLocation,
    pub fields: BTreeMap<String, RecordFieldBinding>,
}

/// Metadata describing an individual record field binding.
#[derive(Debug, Clone)]
pub struct RecordFieldBinding {
    pub value_type: ValueType,
    pub constraints: Option<Constraints>,
    pub units: Option<Units>,
    pub location: FieldLocation,
}

/// Table port binding with column descriptors.
#[derive(Debug, Clone)]
pub struct TableBinding {
    pub location: TableLocation,
    pub columns: Vec<TableColumnBinding>,
    pub keys: Vec<String>,
}

/// Individual table column binding.
#[derive(Debug, Clone)]
pub struct TableColumnBinding {
    pub name: String,
    pub value_type: ValueType,
    pub column_hint: Option<String>,
    pub format: Option<String>,
    pub units: Option<Units>,
}

fn convert_record_default(
    port_id: &str,
    schema: &RecordSchema,
    value: &JsonValue,
) -> Result<PortValue, SheetPortError> {
    let obj = value
        .as_object()
        .ok_or_else(|| SheetPortError::InvariantViolation {
            port: port_id.to_string(),
            message: "record defaults must be objects".to_string(),
        })?;
    let mut map = BTreeMap::new();
    for (key, json_value) in obj {
        let field = schema
            .fields
            .get(key)
            .ok_or_else(|| SheetPortError::InvariantViolation {
                port: port_id.to_string(),
                message: format!("record default references unknown field `{key}`"),
            })?;
        let literal = literal_from_json(
            port_id,
            &format!("{port_id}.{key}"),
            field.value_type,
            json_value,
        )?;
        map.insert(key.clone(), literal);
    }
    Ok(PortValue::Record(map))
}

fn convert_range_default(
    port_id: &str,
    cell_type: ValueType,
    value: &JsonValue,
) -> Result<PortValue, SheetPortError> {
    let rows = value
        .as_array()
        .ok_or_else(|| SheetPortError::InvariantViolation {
            port: port_id.to_string(),
            message: "range defaults must be arrays of arrays".to_string(),
        })?;
    let mut grid = Vec::with_capacity(rows.len());
    let mut expected_width: Option<usize> = None;
    for (row_idx, row_value) in rows.iter().enumerate() {
        let row = row_value
            .as_array()
            .ok_or_else(|| SheetPortError::InvariantViolation {
                port: port_id.to_string(),
                message: format!("range default row {row_idx} must be an array of scalar values"),
            })?;
        let mut converted_row = Vec::with_capacity(row.len());
        for (col_idx, cell_json) in row.iter().enumerate() {
            let literal = literal_from_json(
                port_id,
                &format!("{port_id}[r{},c{}]", row_idx + 1, col_idx + 1),
                cell_type,
                cell_json,
            )?;
            converted_row.push(literal);
        }
        if let Some(width) = expected_width {
            if width != converted_row.len() {
                return Err(SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: format!(
                        "range default row {row_idx} has width {}, expected {width}",
                        converted_row.len()
                    ),
                });
            }
        } else {
            expected_width = Some(converted_row.len());
        }
        grid.push(converted_row);
    }
    Ok(PortValue::Range(grid))
}

fn convert_table_default(
    port_id: &str,
    schema: &TableSchema,
    value: &JsonValue,
) -> Result<PortValue, SheetPortError> {
    let rows = value
        .as_array()
        .ok_or_else(|| SheetPortError::InvariantViolation {
            port: port_id.to_string(),
            message: "table defaults must be arrays of objects".to_string(),
        })?;
    let mut converted_rows = Vec::with_capacity(rows.len());
    for (row_idx, row_value) in rows.iter().enumerate() {
        let obj = row_value
            .as_object()
            .ok_or_else(|| SheetPortError::InvariantViolation {
                port: port_id.to_string(),
                message: format!("table default row {row_idx} must be an object"),
            })?;
        let mut values = BTreeMap::new();
        for column in &schema.columns {
            let key = &column.name;
            let cell_json = obj
                .get(key)
                .ok_or_else(|| SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: format!("table default row {row_idx} missing column `{key}`"),
                })?;
            let literal = literal_from_json(
                port_id,
                &format!("{port_id}[{row_idx}].{key}"),
                column.value_type,
                cell_json,
            )?;
            values.insert(key.clone(), literal);
        }

        for unknown in obj.keys() {
            if !schema.columns.iter().any(|col| col.name == *unknown) {
                return Err(SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: format!("table default references unknown column `{unknown}`"),
                });
            }
        }

        converted_rows.push(TableRow::new(values));
    }
    Ok(PortValue::Table(TableValue::new(converted_rows)))
}

fn literal_from_json(
    port_id: &str,
    path: &str,
    value_type: ValueType,
    value: &JsonValue,
) -> Result<formualizer_common::LiteralValue, SheetPortError> {
    use formualizer_common::LiteralValue as L;
    match value {
        JsonValue::Null => Ok(L::Empty),
        JsonValue::Bool(b) => match value_type {
            ValueType::Boolean => Ok(L::Boolean(*b)),
            _ => Err(default_type_error(port_id, path, "boolean", value_type)),
        },
        JsonValue::Number(n) => match value_type {
            ValueType::Number => {
                if let Some(num) = n.as_f64() {
                    Ok(L::Number(num))
                } else {
                    Err(default_message(
                        port_id,
                        path,
                        "number default must be a finite numeric value",
                    ))
                }
            }
            ValueType::Integer => {
                if let Some(i) = n.as_i64() {
                    Ok(L::Int(i))
                } else if let Some(f) = n.as_f64() {
                    if (f - f.trunc()).abs() < f64::EPSILON {
                        Ok(L::Int(f as i64))
                    } else {
                        Err(default_message(
                            port_id,
                            path,
                            "integer default must be a whole number",
                        ))
                    }
                } else {
                    Err(default_message(
                        port_id,
                        path,
                        "integer default must be representable as i64",
                    ))
                }
            }
            ValueType::String | ValueType::Date | ValueType::Datetime | ValueType::Boolean => {
                Err(default_type_error(port_id, path, "number", value_type))
            }
        },
        JsonValue::String(s) => match value_type {
            ValueType::String => Ok(L::Text(s.clone())),
            ValueType::Number => s
                .parse::<f64>()
                .map(L::Number)
                .map_err(|_| default_message(port_id, path, "number default must be numeric")),
            ValueType::Integer => s.parse::<i64>().map(L::Int).map_err(|_| {
                default_message(port_id, path, "integer default must be a whole number")
            }),
            ValueType::Boolean => match s.to_ascii_lowercase().as_str() {
                "true" => Ok(L::Boolean(true)),
                "false" => Ok(L::Boolean(false)),
                _ => Err(default_message(
                    port_id,
                    path,
                    "boolean default strings must be `true` or `false`",
                )),
            },
            ValueType::Date => {
                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| {
                    default_message(port_id, path, "date defaults must use YYYY-MM-DD format")
                })?;
                Ok(L::Date(date))
            }
            ValueType::Datetime => {
                let dt = parse_datetime_string(s)
                    .ok_or_else(|| default_message(port_id, path, "invalid datetime default"))?;
                Ok(L::DateTime(dt))
            }
        },
        JsonValue::Array(_) | JsonValue::Object(_) => Err(SheetPortError::InvariantViolation {
            port: port_id.to_string(),
            message: format!("invalid default at `{path}`: expected scalar value"),
        }),
    }
}

fn parse_datetime_string(raw: &str) -> Option<NaiveDateTime> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(dt.naive_utc());
    }
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S"))
        .ok()
}

fn default_type_error(
    port_id: &str,
    path: &str,
    expected: &str,
    actual: ValueType,
) -> SheetPortError {
    SheetPortError::InvariantViolation {
        port: port_id.to_string(),
        message: format!(
            "invalid default at `{path}`: expected {expected}, but port type is `{actual:?}`"
        ),
    }
}

fn default_message(port_id: &str, path: &str, message: &str) -> SheetPortError {
    SheetPortError::InvariantViolation {
        port: port_id.to_string(),
        message: format!("invalid default at `{path}`: {message}"),
    }
}
