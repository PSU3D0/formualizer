//! Persistence round-trips for ingest-relaxed self-references (RFC #113).
//!
//! PR #130 relaxed the *interactive edit* rejection of direct self-references
//! (`A1 = =A1+1`) to ACCEPT them under `Runtime` detection +
//! `CyclePolicy::Iterate`. A workbook containing such a formula can be saved
//! through backends that do NOT persist cycle configuration (JSON, CSV) and
//! reloaded under a config where the interactive edit would be rejected.
//!
//! Contract pinned here (the "ingest-time rejection is an interactive-edit
//! nicety, never a load-path gate" shape):
//!
//! - Bulk load paths (`Workbook::from_reader` → `ingest_formula_batches`, both
//!   the eager and the `defer_graph_building` staged variants) accept
//!   self-referential formulas under ANY cycle config. Loading never
//!   hard-fails on a self-reference and never drops the formula.
//! - What the cell *evaluates to* is decided by the loaded engine's cycle
//!   policy: `#CIRC!` under the default (`Static` detection / `Error`
//!   policy), iterated values under `Runtime` + `Iterate`.
//! - Only the interactive `set_formula` / `set_cell_formula` edit path
//!   rejects self-references eagerly (and only when the active config
//!   disallows them) — that is a UX nicety for immediate feedback, not a
//!   persistence gate.

#![cfg(feature = "json")]

use formualizer_eval::engine::CycleConfig;
use formualizer_workbook::backends::JsonAdapter;
use formualizer_workbook::traits::{CellData, SpreadsheetReader, SpreadsheetWriter};
use formualizer_workbook::{LiteralValue, LoadStrategy, Workbook, WorkbookConfig};

fn iterate_config(max_iterations: u32, max_change: f64) -> WorkbookConfig {
    let mut config = WorkbookConfig::ephemeral();
    config.eval = config
        .eval
        .with_cycle(CycleConfig::iterate(max_iterations, max_change));
    config
}

fn num(wb: &Workbook, sheet: &str, row: u32, col: u32) -> f64 {
    match wb.get_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet} r{row}c{col}, got {other:?}"),
    }
}

/// JSON bytes for a workbook containing the self-reference `A1 = =A1+1` plus
/// an ordinary dependent `B1 = =A1*2`, authored through the backend writer
/// (which, like the on-disk format, performs no cycle validation).
fn json_with_self_reference() -> Vec<u8> {
    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Sheet1").unwrap();
    adapter
        .write_cell("Sheet1", 1, 1, CellData::from_formula("=A1+1"))
        .unwrap();
    adapter
        .write_cell("Sheet1", 1, 2, CellData::from_formula("=A1*2"))
        .unwrap();
    adapter.save_to_bytes().unwrap()
}

/* ──────────────── direction 1: iterate workbook → default reload ─────────── */

/// A workbook built under Runtime+Iterate containing `A1 = =A1+1`, saved via
/// the JSON backend (which does not carry cycle config), reloaded with the
/// DEFAULT config: the load must NOT hard-fail, the formula must survive, and
/// the cell yields `#CIRC!` under the default Static/Error policy.
#[test]
fn iterate_workbook_self_ref_json_reload_under_default_config_yields_circ() {
    // Build + iterate under Runtime+Iterate (interactive set_formula accepts
    // the self-reference under this config — the #130 relaxation).
    let mut wb = Workbook::new_with_config(iterate_config(10, 0.001));
    wb.add_sheet("Sheet1").unwrap();
    wb.set_formula("Sheet1", 1, 1, "=A1+1").unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(num(&wb, "Sheet1", 1, 1), 10.0, "capped accumulator");

    // Save through the JSON backend: formula text + last value, NO cycle config.
    let mut adapter = JsonAdapter::new();
    adapter.create_sheet("Sheet1").unwrap();
    adapter
        .write_cell(
            "Sheet1",
            1,
            1,
            CellData {
                value: wb.get_value("Sheet1", 1, 1),
                formula: wb.get_formula("Sheet1", 1, 1),
                style: None,
            },
        )
        .unwrap();
    let bytes = adapter.save_to_bytes().unwrap();

    // Reload with the DEFAULT config (Static detection, Error policy).
    let adapter = JsonAdapter::open_bytes(bytes).unwrap();
    let mut reloaded =
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .expect("bulk load must accept the self-reference (no load-path gate)");

    // The formula survived the round-trip… (text is canonicalized by the
    // printer — "=A1 + 1" — so compare whitespace-insensitively)
    let formula = reloaded
        .get_formula("Sheet1", 1, 1)
        .expect("self-referential formula must not be dropped on load");
    assert_eq!(formula.replace(' ', ""), "=A1+1");

    // …and the default cycle policy stamps it #CIRC at evaluation time.
    reloaded.evaluate_all().unwrap();
    match reloaded.get_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => {
            assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Circ)
        }
        other => panic!("expected #CIRC! under default policy, got {other:?}"),
    }
}

/* ──────────────── direction 2: default-authored file → iterate reload ────── */

/// A JSON file containing a self-reference (authored externally; the format
/// itself never validated it) loaded under Runtime+Iterate: the cycle
/// iterates per the loaded engine's config.
#[test]
fn json_self_ref_loaded_under_iterate_config_iterates() {
    let adapter = JsonAdapter::open_bytes(json_with_self_reference()).unwrap();
    let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, iterate_config(7, 0.001))
        .expect("load");
    wb.evaluate_all().unwrap();
    // Empty→0 seed, 7 capped passes of +1 (spec §4/§7.6).
    assert_eq!(num(&wb, "Sheet1", 1, 1), 7.0);
    assert_eq!(
        num(&wb, "Sheet1", 1, 2),
        14.0,
        "dependent reads final value"
    );
}

/// Same file loaded under the DEFAULT config: load succeeds, evaluation
/// stamps `#CIRC!` on the cycle member; the acyclic dependent consumes it.
#[test]
fn json_self_ref_loaded_under_default_config_yields_circ() {
    let adapter = JsonAdapter::open_bytes(json_with_self_reference()).unwrap();
    let mut wb =
        Workbook::from_reader(adapter, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .expect("bulk load must accept the self-reference (no load-path gate)");
    wb.evaluate_all().unwrap();
    match wb.get_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => {
            assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Circ)
        }
        other => panic!("expected #CIRC! under default policy, got {other:?}"),
    }
}

/// The staged/deferred ingest variant (`defer_graph_building: true`, the
/// Interactive-mode load path) must behave identically: no load-path gate,
/// `#CIRC!` at evaluation under the default policy.
#[test]
fn json_self_ref_deferred_graph_build_under_default_config_yields_circ() {
    let adapter = JsonAdapter::open_bytes(json_with_self_reference()).unwrap();
    let mut wb = Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("staged load must accept the self-reference");
    wb.evaluate_all().unwrap();
    match wb.get_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => {
            assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Circ)
        }
        other => panic!("expected #CIRC! under default policy, got {other:?}"),
    }
}

/* ─────────── contrast: the interactive edit path still rejects ───────────── */

/// The #130 relaxation is config-scoped: under the default config the
/// interactive `set_formula` path still rejects a direct self-reference
/// eagerly. This is the "edit nicety" half of the contract pinned above.
#[test]
fn interactive_set_formula_still_rejects_self_ref_under_default_config() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    wb.add_sheet("Sheet1").unwrap();
    let err = wb.set_formula("Sheet1", 1, 1, "=A1+1");
    assert!(
        err.is_err(),
        "interactive self-reference must be rejected under default config"
    );
}
