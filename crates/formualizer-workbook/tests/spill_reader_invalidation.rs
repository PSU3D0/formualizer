//! FORM-192: a spill committed during evaluation invalidates its readers in
//! the same request, through the workbook entry points hosts use (the Python
//! and WASM bindings evaluate with `evaluate_all_cancellable`).

use formualizer_common::LiteralValue;
use formualizer_eval::engine::CancelToken;
use formualizer_workbook::{Workbook, WorkbookConfig};

fn configs() -> Vec<(&'static str, WorkbookConfig)> {
    let mut serial = WorkbookConfig::interactive();
    serial.eval.enable_parallel = false;
    let mut parallel = WorkbookConfig::interactive();
    parallel.eval.enable_parallel = true;
    vec![
        ("interactive", WorkbookConfig::interactive()),
        ("ephemeral", WorkbookConfig::ephemeral()),
        ("serial", serial),
        ("parallel", parallel),
    ]
}

fn build(cfg: WorkbookConfig, anchor_first: bool) -> Workbook {
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Int(2)).unwrap();
    if anchor_first {
        wb.set_formula("S", 1, 2, "=SEQUENCE(A1)").unwrap();
        wb.set_formula("S", 1, 3, "=B2*10").unwrap();
    } else {
        wb.set_formula("S", 1, 3, "=B2*10").unwrap();
        wb.set_formula("S", 1, 2, "=SEQUENCE(A1)").unwrap();
    }
    wb
}

fn num(wb: &Workbook, row: u32, col: u32) -> Option<f64> {
    match wb.get_value("S", row, col) {
        Some(LiteralValue::Number(n)) => Some(n),
        Some(LiteralValue::Int(i)) => Some(i as f64),
        _ => None,
    }
}

#[test]
fn spill_reader_updates_in_one_request_for_every_full_entry_point() {
    for (name, cfg) in configs() {
        for anchor_first in [true, false] {
            let ctx = format!("{name} anchor_first={anchor_first}");

            let mut wb = build(cfg.clone(), anchor_first);
            wb.evaluate_all().unwrap();
            assert_eq!(num(&wb, 2, 2), Some(2.0), "B2 evaluate_all {ctx}");
            assert_eq!(num(&wb, 1, 3), Some(20.0), "C1 evaluate_all {ctx}");

            let mut wb = build(cfg.clone(), anchor_first);
            wb.evaluate_all_cancellable(CancelToken::new()).unwrap();
            assert_eq!(num(&wb, 2, 2), Some(2.0), "B2 cancellable {ctx}");
            assert_eq!(num(&wb, 1, 3), Some(20.0), "C1 cancellable {ctx}");

            // Grow then shrink through the cancellable (binding) path.
            wb.set_value("S", 1, 1, LiteralValue::Int(3)).unwrap();
            wb.set_formula("S", 1, 4, "=B3*100").unwrap();
            wb.evaluate_all_cancellable(CancelToken::new()).unwrap();
            assert_eq!(num(&wb, 3, 2), Some(3.0), "B3 grown {ctx}");
            assert_eq!(num(&wb, 1, 4), Some(300.0), "D1 grown {ctx}");
            wb.set_value("S", 1, 1, LiteralValue::Int(1)).unwrap();
            wb.evaluate_all_cancellable(CancelToken::new()).unwrap();
            assert_eq!(num(&wb, 2, 2), None, "B2 shrunk {ctx}");
            assert_eq!(num(&wb, 3, 2), None, "B3 shrunk {ctx}");
            assert_eq!(num(&wb, 1, 3), Some(0.0), "C1 shrunk {ctx}");
            assert_eq!(num(&wb, 1, 4), Some(0.0), "D1 shrunk {ctx}");

            let mut wb = build(cfg.clone(), anchor_first);
            let (_, _delta) = wb.evaluate_all_with_delta().unwrap();
            assert_eq!(num(&wb, 1, 3), Some(20.0), "C1 with_delta {ctx}");

            let mut wb = build(cfg.clone(), anchor_first);
            let out = wb.evaluate_cells(&[("S", 1, 2), ("S", 1, 3)]).unwrap();
            assert_eq!(out[1], LiteralValue::Number(20.0), "C1 cells {ctx}");
        }
    }
}

#[cfg(feature = "json")]
#[test]
fn imported_spill_reader_updates_in_one_request() {
    use formualizer_workbook::{JsonAdapter, LoadStrategy, SpreadsheetReader};

    // Reader stored before the anchor, as a file importer would present it.
    let bytes = br#"{
        "version": 1,
        "sheets": {
            "S": {
                "cells": [
                    { "row": 1, "col": 1, "value": { "type": "Number", "value": 2 } },
                    { "row": 1, "col": 3, "formula": "=B2*10" },
                    { "row": 1, "col": 2, "formula": "=SEQUENCE(A1)" }
                ]
            }
        }
    }"#
    .to_vec();
    for (name, cfg) in configs() {
        let adapter = JsonAdapter::open_bytes(bytes.clone()).unwrap();
        let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, cfg).unwrap();
        wb.evaluate_all_cancellable(CancelToken::new()).unwrap();
        assert_eq!(num(&wb, 2, 2), Some(2.0), "B2 {name}");
        assert_eq!(num(&wb, 1, 3), Some(20.0), "C1 {name}");
    }
}
