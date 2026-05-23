use crate::IoError;
use formualizer_eval::engine::WorkbookLoadLimits;

pub(crate) fn use_sparse_initial_ingest(
    rows: u32,
    cols: u32,
    populated_cells: usize,
    limits: &WorkbookLoadLimits,
) -> bool {
    if rows == 0 || cols == 0 {
        return false;
    }

    let logical_cells = u64::from(rows) * u64::from(cols);
    let populated = populated_cells.max(1) as u64;
    let sparse_limit = populated.saturating_mul(limits.max_sparse_cell_ratio);

    logical_cells > sparse_limit
        && (logical_cells >= limits.sparse_sheet_cell_threshold
            || logical_cells > limits.max_sheet_logical_cells)
}

pub(crate) fn enforce_sheet_dimension_limits(
    backend: &str,
    sheet: &str,
    rows: u32,
    cols: u32,
    limits: &WorkbookLoadLimits,
) -> Result<(), IoError> {
    if rows == 0 || cols == 0 {
        return Ok(());
    }

    if rows > limits.max_sheet_rows {
        return Err(IoError::load_budget_exceeded(
            backend,
            sheet,
            format!(
                "sheet has {rows} rows, which exceeds the configured row budget of {}",
                limits.max_sheet_rows
            ),
        ));
    }

    if cols > limits.max_sheet_cols {
        return Err(IoError::load_budget_exceeded(
            backend,
            sheet,
            format!(
                "sheet has {cols} columns, which exceeds the configured column budget of {}",
                limits.max_sheet_cols
            ),
        ));
    }

    Ok(())
}

pub(crate) fn enforce_sheet_load_limits(
    backend: &str,
    sheet: &str,
    rows: u32,
    cols: u32,
    populated_cells: usize,
    limits: &WorkbookLoadLimits,
) -> Result<(), IoError> {
    enforce_sheet_dimension_limits(backend, sheet, rows, cols, limits)?;

    let logical_cells = u64::from(rows) * u64::from(cols);
    if !use_sparse_initial_ingest(rows, cols, populated_cells, limits)
        && logical_cells > limits.max_sheet_logical_cells
    {
        return Err(IoError::load_budget_exceeded(
            backend,
            sheet,
            format!(
                "sheet logical rectangle {rows}x{cols} ({logical_cells} cells) exceeds the configured logical-cell budget of {}",
                limits.max_sheet_logical_cells
            ),
        ));
    }

    Ok(())
}
