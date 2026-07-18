//! Regression test for a cross-sheet evaluation-order bug.
//!
//! When a workbook-level defined name spans multiple cells on Sheet B and
//! each cell of that range carries a formula that references a *formula*
//! cell on Sheet A, the engine should evaluate all cells correctly. Prior
//! to the fix, the first cell of the multi-cell defined name on Sheet B
//! evaluated to ``None``/``0`` while subsequent cells evaluated correctly
//! — the source cell on Sheet A was not yet materialized when the first
//! Sheet B cell was visited.
//!
//! Surfaced from supermod CalculatedSchedules (#1967) where a multi-period
//! rollup metric defined on the "Model" sheet referenced per-period totals
//! on a "Schedule" sheet; the first time period silently returned None.

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn build_repro_engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());

    // ── Sheet "Schedule" ─────────────────────────────────────────────
    engine.graph.add_sheet("Schedule").unwrap();
    for r in 2..=5u32 {
        engine
            .set_cell_value("Schedule", r, 1, LiteralValue::Number(r as f64 * 100.0))
            .unwrap();
        engine
            .set_cell_formula("Schedule", r, 3, parse(format!("=$A${r}")).unwrap())
            .unwrap();
    }
    engine
        .set_cell_formula("Schedule", 6, 3, parse("=SUM(C2:C5)").unwrap())
        .unwrap();

    // ── Sheet "Model" with a multi-cell workbook-level defined name ──
    engine.graph.add_sheet("Model").unwrap();
    let model_sheet = engine.graph.sheet_id("Model").unwrap();
    let metric_start = CellRef::new(model_sheet, Coord::new(7, 4, true, true));
    let metric_end = CellRef::new(model_sheet, Coord::new(7, 6, true, true));
    engine
        .define_name(
            "METRIC",
            NamedDefinition::Range(RangeRef::new(metric_start, metric_end)),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .set_cell_formula("Model", 7, 4, parse("='Schedule'!C6").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Model", 7, 5, parse("='Schedule'!C6").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Model", 7, 6, parse("='Schedule'!C6").unwrap())
        .unwrap();
    engine
}

#[test]
fn first_cell_of_cross_sheet_dn_range_resolves_via_evaluate_all() {
    let mut engine = build_repro_engine();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Model", 7, 4),
        Some(LiteralValue::Number(1400.0)),
        "Model!D7 (first cell of METRIC range) must resolve to the cross-sheet \
         formula-cell value"
    );
    assert_eq!(
        engine.get_cell_value("Model", 7, 5),
        Some(LiteralValue::Number(1400.0))
    );
    assert_eq!(
        engine.get_cell_value("Model", 7, 6),
        Some(LiteralValue::Number(1400.0))
    );
}

#[test]
fn first_cell_of_cross_sheet_dn_range_resolves_via_evaluate_cell() {
    // Demand-driven per-cell evaluation (the path used by the supermod
    // ``FormualizerModel.calculate()`` adapter, which iterates a range
    // calling ``evaluate_cell`` per slot). This is where the surfaced
    // bug actually appears: the first cell's cross-sheet ref returns
    // the wrong value because its source isn't materialized yet.
    let mut engine = build_repro_engine();

    let v_d7 = engine.evaluate_cell("Model", 7, 4).unwrap();
    let v_e7 = engine.evaluate_cell("Model", 7, 5).unwrap();
    let v_f7 = engine.evaluate_cell("Model", 7, 6).unwrap();

    assert_eq!(
        v_d7,
        Some(LiteralValue::Number(1400.0)),
        "Model!D7 (first cell of METRIC range) via evaluate_cell — got {:?}",
        v_d7
    );
    assert_eq!(v_e7, Some(LiteralValue::Number(1400.0)));
    assert_eq!(v_f7, Some(LiteralValue::Number(1400.0)));
}

#[test]
fn cross_sheet_evaluate_cell_prepares_transitive_staged_formulas() {
    // ``defer_graph_building`` mode is what the xlsx loader uses
    // (``Workbook::interactive()``). Formulas land in a staging map and
    // are prepared transactionally on first evaluation. This test mirrors
    // the xlsx-load path and verifies target preparation discovers the
    // complete cross-sheet precedent closure before evaluation.
    let cfg = EvalConfig {
        defer_graph_building: true,
        ..Default::default()
    };
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine.graph.add_sheet("Schedule").unwrap();
    engine.graph.add_sheet("Model").unwrap();
    for r in 2..=5u32 {
        engine
            .set_cell_value("Schedule", r, 1, LiteralValue::Number(r as f64 * 100.0))
            .unwrap();
    }
    // Stage formulas (the xlsx-load path uses ``stage_formula_text``).
    for r in 2..=5u32 {
        engine.stage_formula_text("Schedule", r, 3, format!("=$A${r}"));
    }
    engine.stage_formula_text("Schedule", 6, 3, "=SUM(C2:C5)".to_string());
    engine.stage_formula_text("Model", 7, 4, "='Schedule'!C6".to_string());

    // Target a cell on Model. The reachable Schedule formulas must be
    // prepared before the cross-sheet reference is evaluated.
    let v = engine.evaluate_cell("Model", 7, 4).unwrap();
    assert_eq!(
        v,
        Some(LiteralValue::Number(1400.0)),
        "evaluate_cell must drain all staged sheets so cross-sheet refs \
         to formula cells resolve; got {:?}",
        v
    );
}

fn build_delta_cross_sheet_engine(deferred: bool) -> Engine<TestWorkbook> {
    let cfg = EvalConfig {
        defer_graph_building: deferred,
        ..Default::default()
    };
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine.graph.add_sheet("Schedule").unwrap();
    engine.graph.add_sheet("Model").unwrap();
    for row in 2..=5u32 {
        engine
            .set_cell_value("Schedule", row, 1, LiteralValue::Number(row as f64 * 100.0))
            .unwrap();
    }

    let formulas = [
        ("Schedule", 2, 3, "=$A$2"),
        ("Schedule", 3, 3, "=$A$3"),
        ("Schedule", 4, 3, "=$A$4"),
        ("Schedule", 5, 3, "=$A$5"),
        ("Schedule", 6, 3, "=SUM(C2:C5)"),
        ("Model", 7, 4, "='Schedule'!C6"),
    ];
    for (sheet, row, col, formula) in formulas {
        if deferred {
            engine.stage_formula_text(sheet, row, col, formula.to_string());
        } else {
            engine
                .set_cell_formula(sheet, row, col, parse(formula).unwrap())
                .unwrap();
        }
    }
    engine
}

#[test]
fn cross_sheet_evaluate_cells_with_delta_drains_all_staged_sheets() {
    let mut deferred = build_delta_cross_sheet_engine(true);
    let mut oracle = build_delta_cross_sheet_engine(false);

    assert_eq!(deferred.staged_formula_count(), 6);
    let (deferred_values, deferred_delta) = deferred
        .evaluate_cells_with_delta(&[("Model", 7, 4)])
        .unwrap();
    let (oracle_values, oracle_delta) = oracle
        .evaluate_cells_with_delta(&[("Model", 7, 4)])
        .unwrap();

    assert_eq!(deferred_values, vec![Some(LiteralValue::Number(1400.0))]);
    assert_eq!(deferred_values, oracle_values);
    assert_eq!(deferred_delta, oracle_delta);
    assert_eq!(deferred_delta.changed_cells.len(), 6);
    assert_eq!(deferred.staged_formula_count(), 0);
}

#[test]
fn cross_sheet_evaluate_cells_with_delta_parse_failure_restores_all_staged_formulas() {
    let cfg = EvalConfig {
        defer_graph_building: true,
        ..Default::default()
    };
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.graph.add_sheet("Schedule").unwrap();
    engine.graph.add_sheet("Model").unwrap();
    engine.stage_formula_text("Schedule", 1, 1, "=1+".to_string());
    engine.stage_formula_text("Model", 1, 1, "=Schedule!A1+1".to_string());

    let before = engine.baseline_stats();
    assert!(
        engine
            .evaluate_cells_with_delta(&[("Model", 1, 1)])
            .is_err()
    );
    let after = engine.baseline_stats();

    assert_eq!(after.staged_formula_count, before.staged_formula_count);
    assert_eq!(
        after.graph_formula_vertex_count,
        before.graph_formula_vertex_count
    );
    assert_eq!(engine.get_cell_value("Model", 1, 1), None);
}
