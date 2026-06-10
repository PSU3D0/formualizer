//! `CycleTelemetry` must populate on the workbook/arrow-canonical evaluate
//! path (refs #112; found by the phantom-pairs probe, #123).
//!
//! The engine-direct tests (`engine/tests/scc_runtime_cycles.rs`) exercise
//! telemetry with `with_virtual_dep_telemetry(true)`; the workbook path uses
//! the default config, where Runtime SCC evaluation produced correct values
//! but left `last_cycle_telemetry()` at zero. These tests pin the workbook
//! contract: a #99 phantom pair resolves to values AND reports
//! `phantom_sccs == 1` through the engine accessor, for both the incremental
//! (`Workbook::new_with_config` + `set_formula`) and bulk-ingest
//! (`Workbook::from_reader`, EagerAll) constructions.

use formualizer_eval::engine::{CycleConfig, CycleDetection, CyclePolicy};
use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};

fn runtime_cycle() -> CycleConfig {
    CycleConfig {
        detection: CycleDetection::Runtime,
        policy: CyclePolicy::Error,
    }
}

fn runtime_config() -> WorkbookConfig {
    let mut config = WorkbookConfig::ephemeral();
    config.eval = config.eval.with_cycle(runtime_cycle());
    config
}

fn num(wb: &Workbook, sheet: &str, row: u32, col: u32) -> f64 {
    match wb.get_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet} r{row}c{col}, got {other:?}"),
    }
}

fn assert_phantom_pair_telemetry(wb: &mut Workbook, sheet: &str) {
    let res = wb.evaluate_all().expect("evaluate_all");
    assert_eq!(res.cycle_errors, 0, "phantom pair must not stamp #CIRC");

    // Guard TRUE → A keeps 100, B reads A → both 100; consumer 200.
    assert_eq!(num(wb, sheet, 1, 2), 100.0);
    assert_eq!(num(wb, sheet, 1, 3), 100.0);
    assert_eq!(num(wb, sheet, 1, 4), 200.0);

    let t = wb.engine().last_cycle_telemetry();
    assert_eq!(t.static_sccs, 1, "one SCC task must run: {t:?}");
    assert_eq!(t.phantom_sccs, 1, "pair must resolve as phantom: {t:?}");
    assert_eq!(t.live_cycles_witnessed, 0, "{t:?}");
    assert_eq!(t.circ_cells_stamped, 0, "{t:?}");
    assert!(t.settle_passes_total >= 1, "{t:?}");
}

/// Discussion-#99 guarded pair through the plain workbook construction:
/// `set_formula` ingest, default (telemetry-flag-off) config.
#[test]
fn phantom_pair_populates_cycle_telemetry_via_set_formula() {
    let mut wb = Workbook::new_with_config(runtime_config());
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Boolean(true))
        .unwrap();
    wb.set_formula("S", 1, 2, "=IF(A1,100,C1)").unwrap();
    wb.set_formula("S", 1, 3, "=IF(A1,B1,999)").unwrap();
    wb.set_formula("S", 1, 4, "=B1+C1").unwrap();

    assert_phantom_pair_telemetry(&mut wb, "S");
}

/// Same pair through the bulk-ingest path the phantom-pairs probe (#123)
/// uses: `Workbook::from_reader` with `LoadStrategy::EagerAll`, so the graph
/// (and the SCC) is built in one batched arrow-canonical ingest pass.
#[cfg(feature = "json")]
#[test]
fn phantom_pair_populates_cycle_telemetry_via_bulk_load() {
    use formualizer_workbook::{JsonAdapter, LoadStrategy, SpreadsheetReader};

    let bytes = br#"{
        "version": 1,
        "sheets": {
            "S": {
                "cells": [
                    { "row": 1, "col": 1, "value": { "type": "Boolean", "value": true } },
                    { "row": 1, "col": 2, "formula": "=IF(A1,100,C1)" },
                    { "row": 1, "col": 3, "formula": "=IF(A1,B1,999)" },
                    { "row": 1, "col": 4, "formula": "=B1+C1" }
                ]
            }
        }
    }"#
    .to_vec();

    let adapter = JsonAdapter::open_bytes(bytes).unwrap();
    let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, runtime_config()).unwrap();

    assert_phantom_pair_telemetry(&mut wb, "S");
}

/// A second `evaluate_all` with no dirty work must reset the counters: the
/// telemetry is per-recalc, not cumulative across requests.
#[test]
fn cycle_telemetry_resets_per_evaluation_request() {
    let mut wb = Workbook::new_with_config(runtime_config());
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Boolean(true))
        .unwrap();
    wb.set_formula("S", 1, 2, "=IF(A1,100,C1)").unwrap();
    wb.set_formula("S", 1, 3, "=IF(A1,B1,999)").unwrap();

    wb.evaluate_all().unwrap();
    assert_eq!(wb.engine().last_cycle_telemetry().phantom_sccs, 1);

    // Nothing dirty: the SCC task does not re-run, counters return to zero.
    wb.evaluate_all().unwrap();
    assert_eq!(wb.engine().last_cycle_telemetry().static_sccs, 0);
    assert_eq!(wb.engine().last_cycle_telemetry().phantom_sccs, 0);
}
