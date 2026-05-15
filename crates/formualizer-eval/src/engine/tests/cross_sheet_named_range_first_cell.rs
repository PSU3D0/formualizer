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
fn cross_sheet_evaluate_cell_drains_all_staged_sheets() {
    // ``defer_graph_building`` mode is what the xlsx loader uses
    // (``Workbook::interactive()``). Formulas land in a staging map and
    // are promoted to the graph on first evaluate. Before the fix,
    // ``evaluate_cell`` only promoted the *target* sheet's staged
    // formulas — so a target cell whose formula referenced another
    // sheet would read the still-unevaluated source and silently
    // return ``None``. This test mirrors the xlsx-load path by staging
    // formulas explicitly, then evaluating a single cross-sheet target
    // cell. The fix in ``evaluate_cell`` calls ``build_graph_all`` so
    // every staged sheet is materialized before the target's formula
    // tries to read its dependencies.
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

    // Target a cell on Model. Before the fix, only ``Model``'s staged
    // formulas were promoted — so ``Schedule!C6`` remained unevaluated
    // and the target read ``None``. After the fix, all staged sheets
    // are promoted and the cross-sheet ref resolves cleanly.
    let v = engine.evaluate_cell("Model", 7, 4).unwrap();
    assert_eq!(
        v,
        Some(LiteralValue::Number(1400.0)),
        "evaluate_cell must drain all staged sheets so cross-sheet refs \
         to formula cells resolve; got {:?}",
        v
    );
}
