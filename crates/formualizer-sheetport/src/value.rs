use formualizer_common::LiteralValue;
use std::collections::BTreeMap;

/// Generic value container for a manifest port.
#[derive(Debug, Clone, PartialEq)]
pub enum PortValue {
    Scalar(LiteralValue),
    Record(BTreeMap<String, LiteralValue>),
    Range(Vec<Vec<LiteralValue>>),
    Table(TableValue),
}

impl PortValue {
    pub fn as_scalar(&self) -> Option<&LiteralValue> {
        match self {
            PortValue::Scalar(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_record(&self) -> Option<&BTreeMap<String, LiteralValue>> {
        match self {
            PortValue::Record(map) => Some(map),
            _ => None,
        }
    }

    pub fn as_range(&self) -> Option<&[Vec<LiteralValue>]> {
        match self {
            PortValue::Range(rows) => Some(rows),
            _ => None,
        }
    }

    pub fn as_table(&self) -> Option<&TableValue> {
        match self {
            PortValue::Table(table) => Some(table),
            _ => None,
        }
    }

    /// Returns true when the port carries no concrete data.
    pub fn is_empty(&self) -> bool {
        match self {
            PortValue::Scalar(value) => matches!(value, LiteralValue::Empty),
            PortValue::Record(fields) => fields
                .values()
                .all(|value| matches!(value, LiteralValue::Empty)),
            PortValue::Range(rows) => {
                if rows.is_empty() {
                    return true;
                }
                rows.iter()
                    .all(|row| row.iter().all(|cell| matches!(cell, LiteralValue::Empty)))
            }
            PortValue::Table(table) => table.is_empty(),
        }
    }
}

/// Table-shaped value consisting of ordered rows.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableValue {
    pub rows: Vec<TableRow>,
}

impl TableValue {
    pub fn new(rows: Vec<TableRow>) -> Self {
        Self { rows }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() || self.rows.iter().all(TableRow::is_empty)
    }
}

/// Single logical table row.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableRow {
    pub values: BTreeMap<String, LiteralValue>,
}

impl TableRow {
    pub fn new(values: BTreeMap<String, LiteralValue>) -> Self {
        Self { values }
    }

    pub fn get(&self, column: &str) -> Option<&LiteralValue> {
        self.values.get(column)
    }

    pub fn is_empty(&self) -> bool {
        self.values
            .values()
            .all(|value| matches!(value, LiteralValue::Empty))
    }
}

/// Snapshot of current input values keyed by port id.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct InputSnapshot(pub BTreeMap<String, PortValue>);

impl InputSnapshot {
    pub fn new(map: BTreeMap<String, PortValue>) -> Self {
        Self(map)
    }

    pub fn inner(&self) -> &BTreeMap<String, PortValue> {
        &self.0
    }

    pub fn into_inner(self) -> BTreeMap<String, PortValue> {
        self.0
    }

    pub fn to_update(&self) -> InputUpdate {
        InputUpdate(self.0.clone())
    }

    pub fn get(&self, id: &str) -> Option<&PortValue> {
        self.0.get(id)
    }
}

/// Snapshot of outputs keyed by port id.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OutputSnapshot(pub BTreeMap<String, PortValue>);

impl OutputSnapshot {
    pub fn new(map: BTreeMap<String, PortValue>) -> Self {
        Self(map)
    }

    pub fn inner(&self) -> &BTreeMap<String, PortValue> {
        &self.0
    }

    pub fn into_inner(self) -> BTreeMap<String, PortValue> {
        self.0
    }

    pub fn get(&self, id: &str) -> Option<&PortValue> {
        self.0.get(id)
    }
}

/// Set of inputs to apply (partial updates allowed).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct InputUpdate(pub BTreeMap<String, PortValue>);

impl InputUpdate {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(&mut self, id: impl Into<String>, value: PortValue) {
        self.0.insert(id.into(), value);
    }

    pub fn into_inner(self) -> BTreeMap<String, PortValue> {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
