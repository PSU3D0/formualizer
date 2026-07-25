use crate::error::SheetPortError;
use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;
use sheetport_spec::{LayoutDescriptor, LayoutTermination};
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) const EXCEL_MAX_ROWS: u32 = 1_048_576;

/// Last row that can hold data on `sheet`, falling back to the format bound.
///
/// A layout scan can never need to look past this: every row beyond the used
/// range is empty, so both bounded termination rules stop there. Deriving the
/// bound from the workbook keeps it deterministic for a given workbook while
/// removing the need for a caller-declared row cap.
pub(crate) fn used_row_bound(workbook: &Workbook, sheet: &str) -> u32 {
    workbook
        .sheet_dimensions(sheet)
        .map(|(rows, _)| rows.min(EXCEL_MAX_ROWS))
        // Bound workbooks have an Arrow sheet. The format-bound fallback keeps
        // alternate stores from silently under-reading a live sheet.
        .unwrap_or(EXCEL_MAX_ROWS)
}
pub(crate) const EXCEL_MAX_COLUMNS: u32 = 16_384;

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
    resolve_range_layout_with_cancel(port_id, workbook, layout, None)
}

pub(crate) fn resolve_range_layout_with_cancel(
    port_id: &str,
    workbook: &Workbook,
    layout: &LayoutDescriptor,
    cancel: Option<&AtomicBool>,
) -> Result<RangeLayoutBounds, SheetPortError> {
    let sheet = layout.sheet.clone();
    let start_col = col_to_index(port_id, &layout.anchor_col)?;

    let mut columns = vec![start_col];
    let mut end_col = start_col;
    let mut col = start_col;
    while col < EXCEL_MAX_COLUMNS {
        cancellation_checkpoint(cancel, port_id)?;
        col = col
            .checked_add(1)
            .ok_or_else(|| SheetPortError::SelectorSafety {
                port: port_id.to_string(),
                reason: "layout header column arithmetic overflowed".to_string(),
            })?;
        let value = workbook
            .get_value(&sheet, layout.header_row, col)
            .unwrap_or(LiteralValue::Empty);
        if is_blank(&value) {
            break;
        }
        columns.push(col);
        end_col = col;
    }

    let data_start_row =
        layout
            .header_row
            .checked_add(1)
            .ok_or_else(|| SheetPortError::SelectorSafety {
                port: port_id.to_string(),
                reason: "range header leaves no row for layout data".to_string(),
            })?;
    if data_start_row > EXCEL_MAX_ROWS {
        return Err(SheetPortError::SelectorSafety {
            port: port_id.to_string(),
            reason: "range header leaves no row for layout data".to_string(),
        });
    }
    let data_end_row = determine_end_row(EndRowParams {
        port_id,
        workbook,
        sheet: &sheet,
        start_row: data_start_row,
        columns: &columns,
        terminate: &layout.terminate,
        marker_text: layout.marker_text.as_deref(),
        cancel,
    })?;

    Ok(RangeLayoutBounds {
        sheet,
        start_row: layout.header_row,
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
    resolve_table_layout_with_cancel(port_id, workbook, layout, column_hints, None)
}

pub(crate) fn resolve_table_layout_with_cancel(
    port_id: &str,
    workbook: &Workbook,
    layout: &LayoutDescriptor,
    column_hints: &[Option<String>],
    cancel: Option<&AtomicBool>,
) -> Result<TableLayoutBounds, SheetPortError> {
    let sheet = layout.sheet.clone();
    let anchor_col = col_to_index(port_id, &layout.anchor_col)?;

    let mut column_indices = Vec::with_capacity(column_hints.len().max(1));
    for (idx, hint) in column_hints.iter().enumerate() {
        let col = match hint {
            Some(letter) => col_to_index(port_id, letter)?,
            None => anchor_col
                .checked_add(
                    u32::try_from(idx).map_err(|_| SheetPortError::SelectorSafety {
                        port: port_id.to_string(),
                        reason: "table layout column count exceeds the format bound".to_string(),
                    })?,
                )
                .ok_or_else(|| SheetPortError::SelectorSafety {
                    port: port_id.to_string(),
                    reason: "table layout column arithmetic overflowed".to_string(),
                })?,
        };
        if col > EXCEL_MAX_COLUMNS {
            return Err(SheetPortError::SelectorSafety {
                port: port_id.to_string(),
                reason: "table layout columns exceed the Excel column bound".to_string(),
            });
        }
        column_indices.push(col);
    }
    if column_indices.is_empty() {
        column_indices.push(anchor_col);
    }

    let data_start_row =
        layout
            .header_row
            .checked_add(1)
            .ok_or_else(|| SheetPortError::SelectorSafety {
                port: port_id.to_string(),
                reason: "table data start row overflowed".to_string(),
            })?;
    if data_start_row > EXCEL_MAX_ROWS {
        return Err(SheetPortError::SelectorSafety {
            port: port_id.to_string(),
            reason: "table header leaves no row for table data".to_string(),
        });
    }
    let data_end_row = determine_end_row(EndRowParams {
        port_id,
        workbook,
        sheet: &sheet,
        start_row: data_start_row,
        columns: &column_indices,
        terminate: &layout.terminate,
        marker_text: layout.marker_text.as_deref(),
        cancel,
    })?;

    Ok(TableLayoutBounds {
        sheet,
        data_start_row,
        data_end_row,
        column_indices,
    })
}

pub(crate) fn col_to_index(port_id: &str, col: &str) -> Result<u32, SheetPortError> {
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
        result = result
            .checked_mul(26)
            .and_then(|value_so_far| value_so_far.checked_add(value))
            .ok_or_else(|| SheetPortError::SelectorSafety {
                port: port_id.to_string(),
                reason: "layout column arithmetic overflowed".to_string(),
            })?;
    }
    if result > EXCEL_MAX_COLUMNS {
        return Err(SheetPortError::InvariantViolation {
            port: port_id.to_string(),
            message: format!("column `{col}` exceeds the Excel column bound"),
        });
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
    cancel: Option<&'a AtomicBool>,
}

fn determine_end_row(params: EndRowParams<'_>) -> Result<u32, SheetPortError> {
    let EndRowParams {
        port_id,
        workbook,
        sheet,
        start_row,
        columns,
        terminate,
        marker_text,
        cancel,
    } = params;
    if start_row == 0 || start_row > EXCEL_MAX_ROWS {
        return Err(SheetPortError::SelectorSafety {
            port: port_id.to_string(),
            reason: "layout scan starts outside the Excel row bound".to_string(),
        });
    }

    let used_end = used_row_bound(workbook, sheet);
    if matches!(terminate, LayoutTermination::SheetEnd) {
        return Ok(used_end.max(start_row.saturating_sub(1)));
    }

    let marker = if matches!(terminate, LayoutTermination::UntilMarker) {
        Some(
            marker_text.ok_or_else(|| SheetPortError::InvariantViolation {
                port: port_id.to_string(),
                message: "layout termination `until_marker` requires marker_text".to_string(),
            })?,
        )
    } else {
        None
    };
    let anchor_col =
        columns
            .first()
            .copied()
            .ok_or_else(|| SheetPortError::InvariantViolation {
                port: port_id.to_string(),
                message: "layout must include at least one column".to_string(),
            })?;

    let available = EXCEL_MAX_ROWS - start_row + 1;
    // Scan one row past the used range: that row is empty by construction, so
    // both bounded termination rules resolve there at the latest.
    let scan_bound = used_end
        .saturating_add(1)
        .min(EXCEL_MAX_ROWS)
        .saturating_sub(start_row)
        .saturating_add(1);
    let candidate_count = scan_bound.min(available);
    let mut last_row = start_row.saturating_sub(1);
    for offset in 0..candidate_count {
        cancellation_checkpoint(cancel, port_id)?;
        let row = start_row + offset;
        let terminated = match terminate {
            LayoutTermination::FirstBlankRow => row_blank(workbook, sheet, row, columns),
            LayoutTermination::UntilMarker => {
                row_blank(workbook, sheet, row, columns)
                    || matches!(
                        workbook
                            .get_value(sheet, row, anchor_col)
                            .unwrap_or(LiteralValue::Empty),
                        LiteralValue::Text(ref text) if text.trim() == marker.unwrap_or_default()
                    )
            }
            LayoutTermination::SheetEnd => unreachable!(),
        };
        if terminated {
            return Ok(last_row);
        }
        last_row = row;
    }

    Err(SheetPortError::LayoutExhausted {
        port: port_id.to_string(),
        sheet: sheet.to_string(),
        termination: match terminate {
            LayoutTermination::FirstBlankRow => "first_blank_row",
            LayoutTermination::UntilMarker => "until_marker",
            LayoutTermination::SheetEnd => "sheet_end",
        }
        .to_string(),
        scan_start: start_row,
        limit: candidate_count,
        observed: candidate_count,
    })
}

fn cancellation_checkpoint(
    cancel: Option<&AtomicBool>,
    port_id: &str,
) -> Result<(), SheetPortError> {
    if cancel.is_some_and(|cancel| cancel.load(Ordering::Relaxed)) {
        return Err(SheetPortError::Engine {
            source: formualizer_common::ExcelError::new(
                formualizer_common::ExcelErrorKind::Cancelled,
            )
            .with_message(format!("layout scan cancelled for port `{port_id}`")),
        });
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use sheetport_spec::LayoutKind;

    fn layout(terminate: LayoutTermination) -> LayoutDescriptor {
        LayoutDescriptor {
            kind: LayoutKind::HeaderContiguousV1,
            sheet: "Sheet".to_string(),
            header_row: 1,
            anchor_col: "A".to_string(),
            terminate,
            marker_text: None,
        }
    }

    fn make_workbook(rows: &[(u32, &str)]) -> Workbook {
        let mut workbook = Workbook::new();
        workbook.add_sheet("Sheet").unwrap();
        for &(row, text) in rows {
            workbook
                .set_value("Sheet", row, 1, LiteralValue::Text(text.to_string()))
                .unwrap();
        }
        workbook
    }

    #[test]
    fn range_bounds_span_the_header_through_the_last_data_row() {
        let workbook = make_workbook(&[(1, "Header"), (2, "one"), (3, "two")]);
        let bounds =
            resolve_range_layout("rows", &workbook, &layout(LayoutTermination::FirstBlankRow))
                .unwrap();
        assert_eq!((bounds.start_row, bounds.end_row), (1, 3));

        // An interior blank still terminates; only the trailing bound is derived.
        let workbook = make_workbook(&[(1, "Header"), (2, "one"), (4, "detached")]);
        let bounds =
            resolve_range_layout("rows", &workbook, &layout(LayoutTermination::FirstBlankRow))
                .unwrap();
        assert_eq!((bounds.start_row, bounds.end_row), (1, 2));
    }

    #[test]
    fn until_marker_stops_at_marker_or_an_earlier_blank() {
        let workbook = make_workbook(&[(1, "Header"), (2, "row"), (4, "END")]);
        let mut descriptor = layout(LayoutTermination::UntilMarker);
        descriptor.marker_text = Some("END".to_string());
        assert_eq!(
            resolve_range_layout("rows", &workbook, &descriptor)
                .unwrap()
                .end_row,
            2
        );

        let workbook = make_workbook(&[(1, "Header"), (2, "row"), (3, "END")]);
        assert_eq!(
            resolve_range_layout("rows", &workbook, &descriptor)
                .unwrap()
                .end_row,
            2
        );

        // A missing marker is not an error: the scan is bounded by the used
        // range, so it resolves at the last data row instead of exhausting
        // against a declared cap.
        let workbook = make_workbook(&[(1, "Header"), (2, "one"), (3, "two")]);
        assert_eq!(
            resolve_range_layout("rows", &workbook, &descriptor)
                .unwrap()
                .end_row,
            3
        );
    }

    /// The scan bound follows the workbook rather than a declared row cap.
    ///
    /// `max_scan_rows` used to cap this scan, which made a table longer than the
    /// cap fail even though the sheet plainly ended earlier, and made a short
    /// table on a large sheet over-declare its preparation envelope. The bound is
    /// now the used range, so a contiguous table of any length resolves exactly.
    #[test]
    fn contiguous_table_longer_than_the_old_default_cap_resolves() {
        let mut rows: Vec<(u32, String)> = vec![(1, "Header".to_string())];
        rows.extend((2..=1_500u32).map(|row| (row, format!("row{row}"))));
        let borrowed: Vec<(u32, &str)> = rows.iter().map(|(r, t)| (*r, t.as_str())).collect();
        let workbook = make_workbook(&borrowed);
        let bounds =
            resolve_range_layout("rows", &workbook, &layout(LayoutTermination::FirstBlankRow))
                .unwrap();
        assert_eq!(bounds.end_row, 1_500);
    }

    #[test]
    fn sheet_end_uses_total_dimensions_across_interior_blanks() {
        let workbook = make_workbook(&[(1, "Header"), (2, "row"), (5, "tail")]);
        let bounds =
            resolve_range_layout("rows", &workbook, &layout(LayoutTermination::SheetEnd)).unwrap();
        assert_eq!(bounds.end_row, 5);
        assert!(workbook.sheet_dimensions("Sheet").is_some());
    }

    #[test]
    fn range_header_at_format_limit_is_rejected() {
        let workbook = make_workbook(&[]);
        let mut descriptor = layout(LayoutTermination::FirstBlankRow);
        descriptor.header_row = EXCEL_MAX_ROWS;
        assert!(matches!(
            resolve_range_layout("rows", &workbook, &descriptor),
            Err(SheetPortError::SelectorSafety { .. })
        ));
    }
}
