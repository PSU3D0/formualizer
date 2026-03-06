//! Reusable XLSX fixture helpers for tests and benchmarks.
//!
//! NOTE: umya-spreadsheet uses (col, row) ordering for tuple coordinates,
//! while much of the engine code uses (row, col).

use std::path::{Path, PathBuf};

use tempfile::tempdir;

/// Write a workbook to a specific output path.
///
/// Parent directories are created automatically.
pub fn write_workbook<P, F>(path: P, f: F)
where
    P: AsRef<Path>,
    F: FnOnce(&mut umya_spreadsheet::Spreadsheet),
{
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create workbook parent dir");
    }

    let mut book = umya_spreadsheet::new_file();
    f(&mut book);
    umya_spreadsheet::writer::xlsx::write(&book, path).expect("write workbook");
}

/// Build a workbook invoking the provided closure before writing to a temp path.
///
/// Returns a path to the generated `fixture.xlsx` file.
/// The tempdir is intentionally leaked so the file remains available for
/// the lifetime of the test process.
pub fn build_workbook<F>(f: F) -> PathBuf
where
    F: FnOnce(&mut umya_spreadsheet::Spreadsheet),
{
    let tmp = tempdir().expect("tempdir");
    let path = tmp.path().join("fixture.xlsx");
    write_workbook(&path, f);
    std::mem::forget(tmp);
    path
}

/// Build a numeric grid on Sheet1 using 1-based row and col indices.
pub fn build_numeric_grid<F>(rows: u32, cols: u32, f: F) -> PathBuf
where
    F: Fn(u32, u32) -> f64,
{
    build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
        for r in 1..=rows {
            for c in 1..=cols {
                sh.get_cell_mut((c, r)).set_value_number(f(r, c));
            }
        }
    })
}

/// Build a deterministic grid with value = row * 0.001 + col.
pub fn build_standard_grid(rows: u32, cols: u32) -> PathBuf {
    build_numeric_grid(rows, cols, |r, c| (r as f64) * 0.001 + (c as f64))
}
