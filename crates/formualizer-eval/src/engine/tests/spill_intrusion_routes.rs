//! A formula placed into a live spill blocks it whichever engine entry point
//! places it, and the spill recovers once the formula is removed. Each route
//! runs sequential and parallel, with `family_execution` on and off.
//!
//! A committed spill re-plans its own cells without probing them for
//! formulas; the graph records an anchor whose spill a formula entered, and
//! only such an anchor is probed. These tests pin that every placement route
//! records it.

use crate::engine::{Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum Route {
    SetCellFormula,
    BulkSetFormulas,
    IngestFormulaBatches,
}

const ROUTES: [Route; 3] = [
    Route::SetCellFormula,
    Route::BulkSetFormulas,
    Route::IngestFormulaBatches,
];

fn engine(parallel: bool, family: bool) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: parallel,
            max_threads: if parallel { Some(4) } else { None },
            family_execution: family,
            ..Default::default()
        },
    )
}

fn place(e: &mut Engine<TestWorkbook>, route: Route, row: u32, col: u32, src: &str) {
    match route {
        Route::SetCellFormula => e
            .set_cell_formula("Sheet1", row, col, parse(src).unwrap())
            .unwrap(),
        Route::BulkSetFormulas => {
            let n = e
                .bulk_set_formulas("Sheet1", [(row, col, parse(src).unwrap())])
                .unwrap();
            assert_eq!(n, 1);
        }
        Route::IngestFormulaBatches => {
            let ast_id = e.intern_formula_ast(&parse(src).unwrap());
            let record = FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(src)));
            e.ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", vec![record])])
                .unwrap();
        }
    }
}

fn get(e: &Engine<TestWorkbook>, row: u32, col: u32) -> Option<LiteralValue> {
    match e.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Int(i)) => Some(LiteralValue::Number(i as f64)),
        Some(LiteralValue::Empty) | None => None,
        other => other,
    }
}

fn n(x: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(x))
}

#[track_caller]
fn assert_kind(value: Option<LiteralValue>, kind: ExcelErrorKind, ctx: &str) {
    match value {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, kind, "{ctx}: {e:?}"),
        other => panic!("{ctx}: expected {kind:?}, got {other:?}"),
    }
}

/// A1 `=SEQUENCE(4)` spills over A1:A4; a formula placed at A3 (inside the
/// committed spill, not its edge) blocks it, keeps its own value through the
/// clear, and the spill comes back once it is removed. Nothing else changes,
/// so the anchor only re-plans because the placement woke it.
#[test]
fn formula_placed_into_live_spill_blocks_through_every_route() {
    for route in ROUTES {
        for parallel in [false, true] {
            for family in [true, false] {
                let ctx = format!("{route:?} parallel={parallel} family={family}");
                let mut e = engine(parallel, family);
                e.set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(4)").unwrap())
                    .unwrap();
                e.set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1#)").unwrap())
                    .unwrap();
                e.evaluate_all().unwrap();
                assert_eq!(get(&e, 3, 1), n(3.0), "{ctx}: initial spill");
                assert_eq!(get(&e, 1, 2), n(10.0), "{ctx}: initial reader");

                place(&mut e, route, 3, 1, "=40+2");
                e.evaluate_all().unwrap();
                assert_kind(get(&e, 1, 1), ExcelErrorKind::Spill, &ctx);
                assert_eq!(get(&e, 2, 1), None, "{ctx}: spill cleared");
                assert_eq!(get(&e, 3, 1), n(42.0), "{ctx}: intruder kept");
                assert_eq!(get(&e, 4, 1), None, "{ctx}: spill cleared");
                assert_kind(get(&e, 1, 2), ExcelErrorKind::Ref, &ctx);

                // A second recalc keeps the blocked state.
                e.evaluate_all().unwrap();
                assert_kind(get(&e, 1, 1), ExcelErrorKind::Spill, &ctx);
                assert_eq!(get(&e, 3, 1), n(42.0), "{ctx}: intruder kept");

                e.set_cell_value("Sheet1", 3, 1, LiteralValue::Empty)
                    .unwrap();
                e.evaluate_all().unwrap();
                for row in 1..=4 {
                    assert_eq!(get(&e, row, 1), n(row as f64), "{ctx}: recovered A{row}");
                }
                assert_eq!(get(&e, 1, 2), n(10.0), "{ctx}: reader recovered");
            }
        }
    }
}

/// The intruder sits in the cells a shrinking spill gives up: the shrink
/// commit must not clear it, and once it is removed the spill grows back.
#[test]
fn formula_placed_into_shrinking_spill_survives_the_shrink() {
    for route in ROUTES {
        for parallel in [false, true] {
            for family in [true, false] {
                let ctx = format!("{route:?} parallel={parallel} family={family}");
                let mut e = engine(parallel, family);
                e.set_cell_value("Sheet1", 1, 4, LiteralValue::Number(4.0))
                    .unwrap();
                e.set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(D1)").unwrap())
                    .unwrap();
                e.evaluate_all().unwrap();
                assert_eq!(get(&e, 4, 1), n(4.0), "{ctx}: initial spill");

                place(&mut e, route, 4, 1, "=40+2");
                e.set_cell_value("Sheet1", 1, 4, LiteralValue::Number(2.0))
                    .unwrap();
                e.evaluate_all().unwrap();
                assert_eq!(get(&e, 1, 1), n(1.0), "{ctx}: shrunk spill");
                assert_eq!(get(&e, 2, 1), n(2.0), "{ctx}: shrunk spill");
                assert_eq!(get(&e, 3, 1), None, "{ctx}: released cell cleared");
                assert_eq!(get(&e, 4, 1), n(42.0), "{ctx}: intruder kept");

                e.set_cell_value("Sheet1", 4, 1, LiteralValue::Empty)
                    .unwrap();
                e.set_cell_value("Sheet1", 1, 4, LiteralValue::Number(4.0))
                    .unwrap();
                e.evaluate_all().unwrap();
                for row in 1..=4 {
                    assert_eq!(get(&e, row, 1), n(row as f64), "{ctx}: regrown A{row}");
                }
            }
        }
    }
}

/// Only an anchor a formula entered is recorded (and so probes its cells):
/// re-spills driven by value edits and formulas placed outside every spill
/// record nothing, and the record ends when the spill clears.
#[test]
fn only_intruded_anchors_are_recorded() {
    for parallel in [false, true] {
        let ctx = format!("parallel={parallel}");
        let mut e = engine(parallel, true);
        e.set_cell_value("Sheet1", 1, 4, LiteralValue::Number(4.0))
            .unwrap();
        e.set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(D1)").unwrap())
            .unwrap();
        e.set_cell_formula("Sheet1", 1, 2, parse("=SEQUENCE(D1)*2").unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        for rows in [6.0, 3.0, 5.0] {
            e.set_cell_value("Sheet1", 1, 4, LiteralValue::Number(rows))
                .unwrap();
            e.set_cell_formula("Sheet1", 1, 3, parse("=D1+1").unwrap())
                .unwrap();
            e.evaluate_all().unwrap();
            assert_eq!(get(&e, rows as u32, 2), n(rows * 2.0), "{ctx}: re-spilled");
            assert_eq!(get(&e, rows as u32 + 1, 2), None, "{ctx}: re-spilled");
            assert_eq!(e.graph.spill_intruded_anchor_count(), 0, "{ctx}");
        }

        e.set_cell_formula("Sheet1", 3, 2, parse("=1").unwrap())
            .unwrap();
        e.evaluate_all().unwrap();
        assert_kind(get(&e, 1, 2), ExcelErrorKind::Spill, &ctx);
        assert_eq!(get(&e, 5, 1), n(5.0), "{ctx}: other spill unaffected");
        assert_eq!(e.graph.spill_intruded_anchor_count(), 0, "{ctx}: cleared");
    }
}
