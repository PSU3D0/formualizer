//! Shared test utilities for formualizer-workbook integration tests.
//!
//! NOTE: umya-spreadsheet uses (col, row) ordering for tuple coordinates, i.e. (column, row),
//! which is the reverse of the conventional (row, col) used in much of the rest of the codebase.
//! Be careful when translating between engine (row, col) and umya (col, row).
#![allow(dead_code)]

use std::path::PathBuf;
use tempfile::tempdir;

/// Build a workbook invoking the provided closure to mutate the workbook before writing.
/// Returns a PathBuf to the written XLSX file (lives in a leaked tempdir for test lifetime).
pub fn build_workbook<F>(f: F) -> PathBuf
where
    F: FnOnce(&mut umya_spreadsheet::Spreadsheet),
{
    let tmp = tempdir().expect("tempdir");
    let path = tmp.path().join("fixture.xlsx");
    let mut book = umya_spreadsheet::new_file();
    f(&mut book);
    umya_spreadsheet::writer::xlsx::write(&book, &path).expect("write workbook");
    // Leak tempdir so path stays valid for duration of test process
    std::mem::forget(tmp);
    path
}

/// Build a numeric grid on Sheet1 with values provided by closure f(row, col) (1-based indices).
pub fn build_numeric_grid<F>(rows: u32, cols: u32, f: F) -> PathBuf
where
    F: Fn(u32, u32) -> f64,
{
    build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for r in 1..=rows {
            for c in 1..=cols {
                // umya expects (col, row)
                sh.get_cell_mut((c, r)).set_value_number(f(r, c));
            }
        }
    })
}

/// Convenience grid with deterministic value pattern: value = row * 0.001 + col.
pub fn build_standard_grid(rows: u32, cols: u32) -> PathBuf {
    build_numeric_grid(rows, cols, |r, c| (r as f64) * 0.001 + (c as f64))
}
