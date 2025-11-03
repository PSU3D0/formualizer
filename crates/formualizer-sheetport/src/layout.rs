use crate::error::SheetPortError;
use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;
use sheetport_spec::{LayoutDescriptor, LayoutTermination};

const MAX_LAYOUT_SCAN_ROWS: u32 = 100_000;

#[derive(Debug, Clone)]
pub struct RangeLayoutBounds {
    pub sheet: String,
    pub start_row: u32,
    pub end_row: u32,
    pub start_col: u32,
    pub end_col: u32,
    pub columns: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct TableLayoutBounds {
    pub sheet: String,
    pub data_start_row: u32,
    pub data_end_row: u32,
    pub column_indices: Vec<u32>,
}

pub fn resolve_range_layout(
    port_id: &str,
    workbook: &Workbook,
    layout: &LayoutDescriptor,
) -> Result<RangeLayoutBounds, SheetPortError> {
    let sheet = layout.sheet.clone();
    let start_col = col_to_index(port_id, &layout.anchor_col)?;

    let mut end_col = start_col;
    let mut columns = Vec::new();
    // Discover contiguous header columns starting at anchor until first blank header cell.
    for offset in 0.. {
        let col = start_col + offset;
        let value = workbook
            .get_value(&sheet, layout.header_row, col)
            .unwrap_or(LiteralValue::Empty);
        if offset == 0 || !is_blank(&value) {
            columns.push(col);
            end_col = col;
            if offset > 0 && is_blank(&value) {
                break;
            }
        } else {
            break;
        }
    }

    if columns.is_empty() {
        columns.push(start_col);
        end_col = start_col;
    }

    let data_start_row = layout.header_row;
    let relevant_columns = columns.clone();
    let data_end_row = determine_end_row(EndRowParams {
        port_id,
        workbook,
        sheet: &sheet,
        start_row: data_start_row,
        columns: &relevant_columns,
        terminate: &layout.terminate,
        marker_text: layout.marker_text.as_deref(),
        include_header: true,
    })?;

    Ok(RangeLayoutBounds {
        sheet,
        start_row: data_start_row,
        end_row: data_end_row,
        start_col,
        end_col,
        columns,
    })
}

pub fn resolve_table_layout(
    port_id: &str,
    workbook: &Workbook,
    layout: &LayoutDescriptor,
    column_hints: &[Option<String>],
) -> Result<TableLayoutBounds, SheetPortError> {
    let sheet = layout.sheet.clone();
    let anchor_col = col_to_index(port_id, &layout.anchor_col)?;

    let mut column_indices = Vec::with_capacity(column_hints.len());
    for (idx, hint) in column_hints.iter().enumerate() {
        let col = match hint {
            Some(letter) => col_to_index(port_id, letter)?,
            None => anchor_col + idx as u32,
        };
        column_indices.push(col);
    }
    if column_indices.is_empty() {
        column_indices.push(anchor_col);
    }

    let data_start_row = layout.header_row + 1;
    let data_end_row = determine_end_row(EndRowParams {
        port_id,
        workbook,
        sheet: &sheet,
        start_row: data_start_row,
        columns: &column_indices,
        terminate: &layout.terminate,
        marker_text: layout.marker_text.as_deref(),
        include_header: false,
    })?;

    Ok(TableLayoutBounds {
        sheet,
        data_start_row,
        data_end_row,
        column_indices,
    })
}

fn col_to_index(port_id: &str, col: &str) -> Result<u32, SheetPortError> {
    if col.is_empty() {
        return Err(SheetPortError::InvariantViolation {
            port: port_id.to_string(),
            message: "layout column hint cannot be empty".to_string(),
        });
    }
    let mut result: u32 = 0;
    for ch in col.chars() {
        if !ch.is_ascii_alphabetic() {
            return Err(SheetPortError::InvariantViolation {
                port: port_id.to_string(),
                message: format!("invalid column letter `{col}`"),
            });
        }
        let value = (ch.to_ascii_uppercase() as u8 - b'A') as u32 + 1;
        result = result * 26 + value;
    }
    Ok(result)
}

struct EndRowParams<'a> {
    port_id: &'a str,
    workbook: &'a Workbook,
    sheet: &'a str,
    start_row: u32,
    columns: &'a [u32],
    terminate: &'a LayoutTermination,
    marker_text: Option<&'a str>,
    include_header: bool,
}

fn determine_end_row(params: EndRowParams<'_>) -> Result<u32, SheetPortError> {
    let EndRowParams {
        port_id,
        workbook,
        sheet,
        start_row: data_start_row,
        columns,
        terminate: termination,
        marker_text,
        include_header,
    } = params;
    match termination {
        LayoutTermination::FirstBlankRow => {
            let mut last_row = if include_header {
                data_start_row
            } else {
                data_start_row.saturating_sub(1)
            };
            for offset in 0..MAX_LAYOUT_SCAN_ROWS {
                let row = data_start_row + offset;
                if row_blank(workbook, sheet, row, columns) {
                    break;
                } else {
                    last_row = row;
                }
            }
            Ok(last_row)
        }
        LayoutTermination::SheetEnd => {
            if let Some((rows, _)) = workbook.sheet_dimensions(sheet) {
                Ok(rows)
            } else {
                // Fallback to scanning for blank rows when dimensions are unavailable.
                determine_end_row(EndRowParams {
                    port_id,
                    workbook,
                    sheet,
                    start_row: data_start_row,
                    columns,
                    terminate: &LayoutTermination::FirstBlankRow,
                    marker_text,
                    include_header,
                })
            }
        }
        LayoutTermination::UntilMarker => {
            let marker = marker_text.ok_or_else(|| SheetPortError::InvariantViolation {
                port: port_id.to_string(),
                message: "layout termination `until_marker` requires marker_text".to_string(),
            })?;
            let anchor_col =
                columns
                    .first()
                    .copied()
                    .ok_or_else(|| SheetPortError::InvariantViolation {
                        port: port_id.to_string(),
                        message: "layout must include at least one column".to_string(),
                    })?;
            let mut last_row = if include_header {
                data_start_row
            } else {
                data_start_row.saturating_sub(1)
            };
            for offset in 0..MAX_LAYOUT_SCAN_ROWS {
                let row = data_start_row + offset;
                let value = workbook
                    .get_value(sheet, row, anchor_col)
                    .unwrap_or(LiteralValue::Empty);
                match value {
                    LiteralValue::Text(ref text) if text.trim() == marker => {
                        break;
                    }
                    value => {
                        if !is_blank(&value) || !row_blank(workbook, sheet, row, columns) {
                            last_row = row;
                        } else {
                            // treat blank row before marker as end
                            break;
                        }
                    }
                }
            }
            Ok(last_row)
        }
    }
}

fn row_blank(workbook: &Workbook, sheet: &str, row: u32, columns: &[u32]) -> bool {
    columns.iter().all(|&col| {
        let value = workbook
            .get_value(sheet, row, col)
            .unwrap_or(LiteralValue::Empty);
        is_blank(&value)
    })
}

fn is_blank(value: &LiteralValue) -> bool {
    match value {
        LiteralValue::Empty => true,
        LiteralValue::Text(text) => text.trim().is_empty(),
        _ => false,
    }
}
