//! XLSX corpus generation helpers.
//!
//! This module is feature-gated behind `xlsx` and uses `formualizer-testkit`
//! (umya-backed) fixture writing.

use std::path::Path;

use formualizer_testkit::write_workbook;

/// Generate a simple deterministic numeric grid workbook at `output`.
pub fn generate_numeric_grid_xlsx(
    output: impl AsRef<Path>,
    rows: u32,
    cols: u32,
) -> anyhow::Result<()> {
    let output = output.as_ref();
    write_workbook(output, |book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
        for r in 1..=rows {
            for c in 1..=cols {
                sh.get_cell_mut((c, r))
                    .set_value_number((r as f64) * 0.001 + (c as f64));
            }
        }
    });
    Ok(())
}

/// Generate the initial headline benchmark workbook shape.
///
/// Layout:
/// - `A1:A{rows}` inputs
/// - `B1:B{rows}` formulas `=A{row}*2`
/// - `C1` summary formula `=SUM(B1:B{rows})`
pub fn generate_headline_single_edit_xlsx(
    output: impl AsRef<Path>,
    rows: u32,
) -> anyhow::Result<()> {
    let output = output.as_ref();
    write_workbook(output, |book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
        for r in 1..=rows {
            sh.get_cell_mut((1, r)).set_value_number(r as f64);
            sh.get_cell_mut((2, r)).set_formula(format!("=A{r}*2"));
        }
        sh.get_cell_mut((3, 1))
            .set_formula(format!("=SUM(B1:B{rows})"));
    });
    Ok(())
}
