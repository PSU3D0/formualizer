use crate::error::SheetPortError;
use crate::location::{AreaLocation, FieldLocation, ScalarLocation, TableLocation};
use crate::resolver::{
    resolve_area_location, resolve_field_location, resolve_scalar_location, resolve_table_location,
};
use serde_json::Value as JsonValue;
use sheetport_spec::{Constraints, Direction, Manifest, Port, Schema, Shape, Units, ValueType};
use std::collections::BTreeMap;

/// Bound manifest along with per-port selector metadata.
#[derive(Debug)]
pub struct ManifestBindings {
    manifest: Manifest,
    bindings: Vec<PortBinding>,
}

impl ManifestBindings {
    /// Validate and bind a manifest into runtime-friendly structures.
    pub fn new(manifest: Manifest) -> Result<Self, SheetPortError> {
        manifest.validate()?;
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
    pub partition_key: bool,
    pub kind: BoundPort,
}

impl PortBinding {
    fn bind(index: usize, port: &Port) -> Result<Self, SheetPortError> {
        let kind = match (&port.shape, &port.schema) {
            (Shape::Scalar, Schema::Scalar(schema)) => {
                let location = resolve_scalar_location(&port.id, &port.location)?;
                BoundPort::Scalar(ScalarBinding {
                    value_type: schema.value_type,
                    format: schema.format.clone(),
                    location,
                })
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
                BoundPort::Record(RecordBinding { location, fields })
            }
            (Shape::Range, Schema::Range(schema)) => {
                let location = resolve_area_location(&port.id, &port.location)?;
                BoundPort::Range(RangeBinding {
                    cell_type: schema.cell_type,
                    format: schema.format.clone(),
                    location,
                })
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
                    .collect();
                let keys = schema.keys.clone().unwrap_or_default();
                BoundPort::Table(TableBinding {
                    location,
                    columns,
                    keys,
                })
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
