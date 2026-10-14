//! Fluent constructors for the canonical manifest representation.
use crate::*;

impl Manifest {
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            spec: SPEC_IDENT.into(),
            spec_version: SpecVersion(
                semver::Version::parse(CURRENT_SPEC_VERSION).expect("valid spec version"),
            ),
            capabilities: None,
            manifest: ManifestMeta {
                name: id.clone(),
                id,
                description: None,
                tags: None,
                workbook: None,
                metadata: None,
            },
            ports: Vec::new(),
        }
    }
    pub fn from_json_value(value: serde_json::Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(value)
    }
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.manifest.name = name.into();
        self
    }
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.manifest.description = Some(description.into());
        self
    }
    pub fn with_workbook(mut self, workbook: WorkbookMeta) -> Self {
        self.manifest.workbook = Some(workbook);
        self
    }
    pub fn with_tags(mut self, tags: impl IntoIterator<Item = String>) -> Self {
        self.manifest.tags = Some(tags.into_iter().collect());
        self
    }
    pub fn port(mut self, port: Port) -> Self {
        self.ports.push(port);
        self
    }
    pub fn ports(mut self, ports: impl IntoIterator<Item = Port>) -> Self {
        self.ports.extend(ports);
        self
    }
}
impl Port {
    fn new(
        id: impl Into<String>,
        dir: Direction,
        location: impl Into<Selector>,
        schema: impl Into<Schema>,
    ) -> Self {
        let schema = schema.into();
        let shape = match schema {
            Schema::Scalar(_) => Shape::Scalar,
            Schema::Range(_) => Shape::Range,
            Schema::Table(_) => Shape::Table,
            Schema::Record(_) => Shape::Record,
        };
        Self {
            id: id.into(),
            dir,
            shape,
            description: None,
            required: true,
            location: location.into(),
            schema,
            constraints: None,
            units: None,
            default: None,
            partition_key: None,
        }
    }
    pub fn input(
        id: impl Into<String>,
        location: impl Into<Selector>,
        schema: impl Into<Schema>,
    ) -> Self {
        Self::new(id, Direction::In, location, schema)
    }
    pub fn output(
        id: impl Into<String>,
        location: impl Into<Selector>,
        schema: impl Into<Schema>,
    ) -> Self {
        Self::new(id, Direction::Out, location, schema)
    }
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
    pub fn optional(mut self) -> Self {
        self.required = false;
        self
    }
    pub fn with_default(mut self, value: serde_json::Value) -> Self {
        self.default = Some(value);
        self
    }
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }
    pub fn with_units(mut self, units: Units) -> Self {
        self.units = Some(units);
        self
    }
}
impl From<&str> for Selector {
    fn from(value: &str) -> Self {
        Self::a1(value)
    }
}
impl From<String> for Selector {
    fn from(value: String) -> Self {
        Self::a1(value)
    }
}
impl Selector {
    pub fn a1(value: impl Into<String>) -> Self {
        Self::A1(SelectorA1 { a1: value.into() })
    }
    pub fn name(value: impl Into<String>) -> Self {
        Self::Name(SelectorName { name: value.into() })
    }
    pub fn struct_ref(value: impl Into<String>) -> Self {
        Self::StructRef(SelectorStructRef {
            struct_ref: value.into(),
        })
    }
    pub fn table(name: impl Into<String>) -> Self {
        Self::Table(SelectorTable {
            table: TableSelector {
                name: name.into(),
                area: None,
            },
        })
    }
    pub fn with_area(mut self, area: TableArea) -> Self {
        if let Self::Table(ref mut selector) = self {
            selector.table.area = Some(area);
        }
        self
    }
    pub fn layout(layout: LayoutDescriptor) -> Self {
        Self::Layout(SelectorLayout { layout })
    }
}
impl From<ValueType> for Schema {
    fn from(value_type: ValueType) -> Self {
        Self::Scalar(ScalarSchema {
            value_type,
            format: None,
        })
    }
}
impl Schema {
    pub fn range(cell_type: ValueType) -> Self {
        Self::Range(RangeSchema {
            kind: RangeKind::Range,
            cell_type,
            format: None,
        })
    }
    pub fn table(columns: impl IntoIterator<Item = TableColumn>) -> Self {
        Self::Table(TableSchema {
            kind: TableKind::Table,
            columns: columns.into_iter().collect(),
            keys: None,
        })
    }
    pub fn with_keys(mut self, keys: impl IntoIterator<Item = String>) -> Self {
        if let Self::Table(ref mut schema) = self {
            schema.keys = Some(keys.into_iter().collect());
        }
        self
    }
    pub fn record(fields: impl IntoIterator<Item = (String, RecordField)>) -> Self {
        Self::Record(RecordSchema {
            kind: RecordKind::Record,
            fields: fields.into_iter().collect(),
        })
    }
}
impl TableColumn {
    pub fn new(name: impl Into<String>, value_type: ValueType) -> Self {
        Self {
            name: name.into(),
            value_type,
            col: None,
            format: None,
            units: None,
        }
    }
}
impl RecordField {
    pub fn new(value_type: ValueType, location: FieldSelector) -> Self {
        Self {
            value_type,
            location,
            constraints: None,
            units: None,
        }
    }
}
