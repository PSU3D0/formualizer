#![cfg(feature = "formualizer_runner")]

use formualizer_bench_core::parity_harness::{
    compare_workbooks, float_parity_equal, literal_parity_equal,
};
use formualizer_common::LiteralValue;
use formualizer_eval::engine::{EvalConfig, FormulaPlaneMode};
use formualizer_testkit::write_workbook;
use formualizer_workbook::{
    LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};

fn workbook_config(mode: FormulaPlaneMode) -> WorkbookConfig {
    let mut config = WorkbookConfig::ephemeral();
    config.eval = EvalConfig::default()
        .with_formula_plane_mode(mode)
        .with_parallel(false);
    config
}

fn open(path: &std::path::Path, mode: FormulaPlaneMode) -> Workbook {
    let backend = UmyaAdapter::open_path(path).expect("open synthetic fixture");
    Workbook::from_reader(backend, LoadStrategy::EagerAll, workbook_config(mode))
        .expect("load synthetic fixture")
}

#[test]
fn parity_harness_smoke_two_sheet_100_cells() {
    let path = std::env::temp_dir().join(format!(
        "formualizer-parity-smoke-{}.xlsx",
        std::process::id()
    ));
    write_workbook(&path, |book| {
        let sheet1 = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
        for row in 1..=10 {
            for col in 1..=5 {
                sheet1
                    .get_cell_mut((col, row))
                    .set_value_number((row * col) as f64);
            }
        }
        let _ = book.new_sheet("Sheet2");
        let sheet2 = book.get_sheet_by_name_mut("Sheet2").expect("Sheet2 exists");
        for row in 1..=10 {
            for col in 1..=5 {
                sheet2.get_cell_mut((col, row)).set_formula(format!(
                    "=Sheet1!{}{}*2",
                    col_name(col),
                    row
                ));
            }
        }
    });

    let mut off = open(&path, FormulaPlaneMode::Off);
    let mut auth = open(&path, FormulaPlaneMode::AuthoritativeExperimental);
    off.evaluate_all().expect("off evaluate_all");
    auth.evaluate_all().expect("auth evaluate_all");
    let (cells_compared, divergences) = compare_workbooks(&off, &auth, 10);
    assert_eq!(cells_compared, 100);
    assert!(divergences.is_empty(), "divergences: {divergences:?}");
    let _ = std::fs::remove_file(path);
}

#[test]
fn parity_harness_detects_divergence() {
    let path = std::env::temp_dir().join(format!(
        "formualizer-parity-divergence-{}.xlsx",
        std::process::id()
    ));
    write_workbook(&path, |book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
        sheet.get_cell_mut((1, 1)).set_value_number(1.0);
        sheet.get_cell_mut((2, 1)).set_formula("=A1+1");
    });

    let mut off = open(&path, FormulaPlaneMode::Off);
    let mut auth = open(&path, FormulaPlaneMode::AuthoritativeExperimental);
    off.evaluate_all().expect("off evaluate_all");
    auth.evaluate_all().expect("auth evaluate_all");
    auth.set_value("Sheet1", 1, 2, LiteralValue::Number(99.0))
        .expect("inject divergence");
    let (_cells_compared, divergences) = compare_workbooks(&off, &auth, 10);
    assert_eq!(divergences.len(), 1);
    assert_eq!(divergences[0].sheet, "Sheet1");
    assert_eq!(divergences[0].row, 1);
    assert_eq!(divergences[0].col, 2);
    let _ = std::fs::remove_file(path);
}

#[test]
fn float_parity_uses_exact_bits_with_nan_equivalence() {
    assert!(float_parity_equal(1.5, 1.5));
    assert!(!float_parity_equal(-0.0, 0.0));
    assert!(float_parity_equal(f64::NAN, f64::NAN));
    assert!(!float_parity_equal(f64::NAN, 1.0));
    assert!(!literal_parity_equal(
        &Some(LiteralValue::Number(-0.0)),
        &Some(LiteralValue::Number(0.0))
    ));
}

fn col_name(col: u32) -> String {
    let mut n = col;
    let mut chars = Vec::new();
    while n > 0 {
        n -= 1;
        chars.push((b'A' + (n % 26) as u8) as char);
        n /= 26;
    }
    chars.iter().rev().collect()
}
