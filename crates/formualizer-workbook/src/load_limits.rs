use crate::IoError;
use formualizer_eval::engine::WorkbookLoadLimits;

pub(crate) fn enforce_sheet_load_limits(
    backend: &str,
    sheet: &str,
    rows: u32,
    cols: u32,
    populated_cells: usize,
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

    let logical_cells = u64::from(rows) * u64::from(cols);
    if logical_cells > limits.max_sheet_logical_cells {
        return Err(IoError::load_budget_exceeded(
            backend,
            sheet,
            format!(
                "sheet logical rectangle {rows}x{cols} ({logical_cells} cells) exceeds the configured logical-cell budget of {}",
                limits.max_sheet_logical_cells
            ),
        ));
    }

    if logical_cells >= limits.sparse_sheet_cell_threshold {
        let populated = populated_cells.max(1) as u64;
        let sparse_limit = populated.saturating_mul(limits.max_sparse_cell_ratio);
        if logical_cells > sparse_limit {
            return Err(IoError::load_budget_exceeded(
                backend,
                sheet,
                format!(
                    "sheet logical rectangle {rows}x{cols} ({logical_cells} cells, {populated_cells} populated cells) exceeds the sparse-sheet guardrail ratio {} once the sheet exceeds {} logical cells",
                    limits.max_sparse_cell_ratio, limits.sparse_sheet_cell_threshold
                ),
            ));
        }
    }

    Ok(())
}
