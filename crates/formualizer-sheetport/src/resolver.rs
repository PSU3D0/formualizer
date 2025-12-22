use crate::error::SheetPortError;
use crate::location::{AreaLocation, FieldLocation, ScalarLocation, TableLocation};
use formualizer_common::RangeAddress;
use formualizer_parse::parser::ReferenceType;
use sheetport_spec::{
    FieldSelector, LayoutDescriptor, Selector, SelectorA1, SelectorLayout, SelectorName,
    SelectorStructRef, TableSelector,
};

#[derive(Debug, Clone)]
pub enum ResolvedSelector {
    Range(RangeAddress),
    Name(String),
    StructRef(String),
    Table(TableSelector),
    Layout(LayoutDescriptor),
}

pub fn resolve_scalar_location(
    port_id: &str,
    selector: &Selector,
) -> Result<ScalarLocation, SheetPortError> {
    match resolve_selector(port_id, selector)? {
        ResolvedSelector::Range(range) => {
            if range.height() != 1 || range.width() != 1 {
                return Err(SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: format!(
                        "scalar ports must point to a single cell, got {}x{}",
                        range.height(),
                        range.width()
                    ),
                });
            }
            Ok(ScalarLocation::Cell(range))
        }
        ResolvedSelector::Name(name) => Ok(ScalarLocation::Name(name)),
        ResolvedSelector::StructRef(sr) => Ok(ScalarLocation::StructRef(sr)),
        ResolvedSelector::Table(_) | ResolvedSelector::Layout(_) => {
            Err(SheetPortError::UnsupportedSelector {
                port: port_id.to_string(),
                reason: "scalar ports cannot target table or layout selectors".to_string(),
            })
        }
    }
}

pub fn resolve_area_location(
    port_id: &str,
    selector: &Selector,
) -> Result<AreaLocation, SheetPortError> {
    match resolve_selector(port_id, selector)? {
        ResolvedSelector::Range(range) => Ok(AreaLocation::Range(range)),
        ResolvedSelector::Name(name) => Ok(AreaLocation::Name(name)),
        ResolvedSelector::StructRef(sr) => Ok(AreaLocation::StructRef(sr)),
        ResolvedSelector::Layout(layout) => Ok(AreaLocation::Layout(layout)),
        ResolvedSelector::Table(_) => Err(SheetPortError::UnsupportedSelector {
            port: port_id.to_string(),
            reason: "area selectors cannot directly reference workbook tables".to_string(),
        }),
    }
}

pub fn resolve_table_location(
    port_id: &str,
    selector: &Selector,
) -> Result<TableLocation, SheetPortError> {
    match resolve_selector(port_id, selector)? {
        ResolvedSelector::Table(table) => Ok(TableLocation::Table(table)),
        ResolvedSelector::Layout(layout) => Ok(TableLocation::Layout(layout)),
        other => Err(SheetPortError::UnsupportedSelector {
            port: port_id.to_string(),
            reason: format!("table ports require table or layout selectors, got {other:?}"),
        }),
    }
}

pub fn resolve_field_location(
    port_id: &str,
    field: &str,
    selector: &FieldSelector,
) -> Result<FieldLocation, SheetPortError> {
    match selector {
        FieldSelector::A1(SelectorA1 { a1 }) => {
            let range = parse_a1_range(port_id, a1)?;
            if range.height() != 1 || range.width() != 1 {
                return Err(SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: format!(
                        "record field `{field}` must resolve to a single cell, got {}x{}",
                        range.height(),
                        range.width()
                    ),
                });
            }
            Ok(FieldLocation::Cell(range))
        }
        FieldSelector::Name(SelectorName { name }) => Ok(FieldLocation::Name(name.to_string())),
        FieldSelector::StructRef(SelectorStructRef { struct_ref }) => {
            Ok(FieldLocation::StructRef(struct_ref.to_string()))
        }
    }
}

fn resolve_selector(
    port_id: &str,
    selector: &Selector,
) -> Result<ResolvedSelector, SheetPortError> {
    match selector {
        Selector::A1(SelectorA1 { a1 }) => parse_a1_range(port_id, a1).map(ResolvedSelector::Range),
        Selector::Name(SelectorName { name }) => Ok(ResolvedSelector::Name(name.to_string())),
        Selector::StructRef(SelectorStructRef { struct_ref }) => {
            Ok(ResolvedSelector::StructRef(struct_ref.to_string()))
        }
        Selector::Table(selector) => Ok(ResolvedSelector::Table(selector.table.clone())),
        Selector::Layout(SelectorLayout { layout }) => Ok(ResolvedSelector::Layout(layout.clone())),
    }
}

fn parse_a1_range(port_id: &str, raw: &str) -> Result<RangeAddress, SheetPortError> {
    match ReferenceType::from_string(raw) {
        Ok(ReferenceType::Cell {
            sheet, row, col, ..
        }) => {
            let sheet = sheet.unwrap_or_default();
            if sheet.is_empty() {
                return Err(SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: format!("reference `{raw}` must include a sheet name"),
                });
            }
            RangeAddress::new(sheet, row, col, row, col).map_err(|msg| {
                SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: msg.to_string(),
                }
            })
        }
        Ok(ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            ..
        }) => {
            let sheet = sheet.unwrap_or_default();
            if sheet.is_empty() {
                return Err(SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: format!("reference `{raw}` must include a sheet name"),
                });
            }
            let sr = require_coord(port_id, raw, "start_row", start_row)?;
            let sc = require_coord(port_id, raw, "start_col", start_col)?;
            let er = require_coord(port_id, raw, "end_row", end_row)?;
            let ec = require_coord(port_id, raw, "end_col", end_col)?;
            RangeAddress::new(sheet, sr, sc, er, ec).map_err(|msg| {
                SheetPortError::InvariantViolation {
                    port: port_id.to_string(),
                    message: msg.to_string(),
                }
            })
        }
        Ok(other) => Err(SheetPortError::UnsupportedSelector {
            port: port_id.to_string(),
            reason: format!("A1 selector `{raw}` resolved to unsupported reference `{other:?}`"),
        }),
        Err(source) => Err(SheetPortError::InvalidReference {
            port: port_id.to_string(),
            reference: raw.to_string(),
            details: source.to_string(),
        }),
    }
}

fn require_coord(
    port_id: &str,
    raw: &str,
    label: &str,
    value: Option<u32>,
) -> Result<u32, SheetPortError> {
    value.ok_or_else(|| SheetPortError::InvariantViolation {
        port: port_id.to_string(),
        message: format!("reference `{raw}` is missing `{label}`"),
    })
}
