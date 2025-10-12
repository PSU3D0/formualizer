use formualizer_common::RangeAddress;
use sheetport_spec::{LayoutDescriptor, TableSelector};

/// Location for scalar ports.
#[derive(Debug, Clone)]
pub enum ScalarLocation {
    /// Single cell location (parsed from an A1 reference).
    Cell(RangeAddress),
    /// Workbook-defined name.
    Name(String),
    /// Structured reference (e.g., `Table[Column]`).
    StructRef(String),
}

/// Location for ports that span an area (records or rectangular ranges).
#[derive(Debug, Clone)]
pub enum AreaLocation {
    /// Explicit range address.
    Range(RangeAddress),
    /// Workbook-defined name resolving to an area.
    Name(String),
    /// Structured reference.
    StructRef(String),
    /// Layout descriptor for header-driven regions.
    Layout(LayoutDescriptor),
}

/// Location options for table-shaped ports.
#[derive(Debug, Clone)]
pub enum TableLocation {
    /// Reference to a workbook table.
    Table(TableSelector),
    /// Layout descriptor (implicit table).
    Layout(LayoutDescriptor),
}

/// Location for record field cells.
#[derive(Debug, Clone)]
pub enum FieldLocation {
    /// Explicit single-cell address.
    Cell(RangeAddress),
    /// Workbook-defined name.
    Name(String),
    /// Structured reference.
    StructRef(String),
}
