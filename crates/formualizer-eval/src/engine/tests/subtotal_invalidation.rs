//! SUBTOTAL and AGGREGATE are not volatile: value edits reach them through
//! their range dependencies, and row-visibility changes (manual or filter
//! hides, row inserts and deletes that shift hidden rows, undo/redo) dirty
//! them explicitly. A recalc with no changes, or after an edit outside every
//! subtotal range, evaluates none of them.

use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{ChangeLog, Engine, EvalConfig, RowVisibilitySource};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use super::common::arrow_eval_config;

type TestEngine = Engine<TestWorkbook>;

const FAMILY_EXECUTION: [bool; 2] = [true, false];

fn engine(family_execution: bool) -> TestEngine {
    Engine::new(
        TestWorkbook::default(),
        EvalConfig {
            family_execution,
            ..arrow_eval_config()
        },
    )
}

fn value(engine: &mut TestEngine, sheet: &str, row: u32, col: u32, v: f64) {
    engine
        .set_cell_value(sheet, row, col, LiteralValue::Number(v))
        .unwrap();
}

fn formula(engine: &mut TestEngine, sheet: &str, row: u32, col: u32, src: &str) {
    engine
        .set_cell_formula(sheet, row, col, parse(src).unwrap())
        .unwrap();
}

fn num(engine: &TestEngine, sheet: &str, row: u32, col: u32) -> f64 {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("{sheet}!R{row}C{col}: expected a number, got {other:?}"),
    }
}

fn hide(engine: &mut TestEngine, row: u32, hidden: bool, source: RowVisibilitySource) {
    engine
        .set_row_hidden("Sheet1", row, hidden, source)
        .unwrap();
}

/// A1:A10 = 1..10; column C holds 40 copied-down SUBTOTAL(109) formulas over
/// A1:A10 (a family candidate), D1 = SUBTOTAL(109,A1:A5), D2 = D1*2,
/// E1 = AGGREGATE(9,1,A1:A10), E2 = SUBTOTAL(109,F1:F3) over an unrelated
/// range, and G1 = SUM(A1:A10) as a non-subtotal reader.
fn workbook(family_execution: bool) -> TestEngine {
    let mut engine = engine(family_execution);
    for row in 1..=10 {
        value(&mut engine, "Sheet1", row, 1, row as f64);
    }
    for row in 1..=3 {
        value(&mut engine, "Sheet1", row, 6, 100.0);
    }
    let column: Vec<_> = (1..=40)
        .map(|row| (row, 3, parse("=SUBTOTAL(109,$A$1:$A$10)").unwrap()))
        .collect();
    engine.bulk_set_formulas("Sheet1", column).unwrap();
    formula(&mut engine, "Sheet1", 1, 4, "=SUBTOTAL(109,A1:A5)");
    formula(&mut engine, "Sheet1", 2, 4, "=D1*2");
    formula(&mut engine, "Sheet1", 1, 5, "=AGGREGATE(9,1,A1:A10)");
    formula(&mut engine, "Sheet1", 2, 5, "=SUBTOTAL(109,F1:F3)");
    formula(&mut engine, "Sheet1", 1, 7, "=SUM(A1:A10)");
    engine.evaluate_all().unwrap();
    engine
}

fn assert_column(engine: &TestEngine, expected: f64) {
    for row in 1..=40 {
        assert_eq!(num(engine, "Sheet1", row, 3), expected, "C{row}");
    }
}

#[test]
fn subtotal_and_aggregate_are_not_volatile() {
    crate::builtins::load_builtins();
    for name in ["SUBTOTAL", "AGGREGATE"] {
        let caps = crate::function_registry::get("", name).unwrap().caps();
        assert!(!caps.contains(crate::function::FnCaps::VOLATILE), "{name}");
    }
}

#[test]
fn warm_recalc_and_unrelated_edits_evaluate_no_subtotal() {
    for family in FAMILY_EXECUTION {
        let mut engine = workbook(family);
        assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 0);

        value(&mut engine, "Sheet1", 20, 1, 7.0);
        assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 0);

        // F2 feeds only E2.
        value(&mut engine, "Sheet1", 2, 6, 1.0);
        assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 1);
        assert_eq!(num(&engine, "Sheet1", 2, 5), 201.0);
    }
}

#[test]
fn value_edits_inside_a_range_update_its_subtotals() {
    for family in FAMILY_EXECUTION {
        let mut engine = workbook(family);
        value(&mut engine, "Sheet1", 3, 1, 103.0);
        // 40 column subtotals, D1, D2, E1 and G1; E2 is untouched.
        assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 44);
        assert_column(&engine, 155.0);
        assert_eq!(num(&engine, "Sheet1", 1, 4), 115.0);
        assert_eq!(num(&engine, "Sheet1", 2, 4), 230.0);
        assert_eq!(num(&engine, "Sheet1", 1, 5), 155.0);
    }
}

#[test]
fn manual_and_filter_hides_update_subtotals_and_their_readers() {
    for family in FAMILY_EXECUTION {
        let mut engine = workbook(family);

        hide(&mut engine, 2, true, RowVisibilitySource::Manual);
        engine.evaluate_all().unwrap();
        assert_column(&engine, 53.0);
        assert_eq!(num(&engine, "Sheet1", 1, 5), 53.0);
        assert_eq!(num(&engine, "Sheet1", 1, 4), 13.0);

        hide(&mut engine, 4, true, RowVisibilitySource::Filter);
        engine.evaluate_all().unwrap();
        assert_column(&engine, 49.0);
        assert_eq!(num(&engine, "Sheet1", 1, 4), 9.0);
        assert_eq!(num(&engine, "Sheet1", 2, 4), 18.0);
        assert_eq!(num(&engine, "Sheet1", 1, 5), 49.0);

        hide(&mut engine, 2, false, RowVisibilitySource::Manual);
        hide(&mut engine, 4, false, RowVisibilitySource::Filter);
        engine.evaluate_all().unwrap();
        assert_column(&engine, 55.0);
        assert_eq!(num(&engine, "Sheet1", 2, 4), 30.0);

        // Without further changes nothing re-evaluates.
        assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 0);
    }
}

#[test]
fn row_ranges_and_bursts_of_hides_update_subtotals() {
    for family in FAMILY_EXECUTION {
        let mut engine = workbook(family);
        engine
            .set_rows_hidden("Sheet1", 6, 10, true, RowVisibilitySource::Manual)
            .unwrap();
        for row in 1..=3 {
            hide(&mut engine, row, true, RowVisibilitySource::Manual);
        }
        engine.evaluate_all().unwrap();
        assert_column(&engine, 9.0);
        assert_eq!(num(&engine, "Sheet1", 1, 5), 9.0);
    }
}

#[test]
fn a_partial_recalc_between_hides_does_not_lose_later_hides() {
    for family in FAMILY_EXECUTION {
        let mut engine = workbook(family);
        hide(&mut engine, 1, true, RowVisibilitySource::Manual);
        assert_eq!(
            engine.evaluate_cell("Sheet1", 1, 3).unwrap(),
            Some(LiteralValue::Number(54.0))
        );
        hide(&mut engine, 10, true, RowVisibilitySource::Manual);
        engine.evaluate_all().unwrap();
        assert_column(&engine, 44.0);
        assert_eq!(num(&engine, "Sheet1", 1, 5), 44.0);
    }
}

#[test]
fn row_inserts_and_deletes_keep_hidden_rows_aligned_with_subtotals() {
    for family in FAMILY_EXECUTION {
        let mut engine = engine(family);
        for row in 1..=5 {
            value(&mut engine, "Sheet1", row, 1, row as f64);
        }
        formula(&mut engine, "Sheet1", 10, 2, "=SUBTOTAL(109,A1:A5)");
        hide(&mut engine, 5, true, RowVisibilitySource::Manual);
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 10, 2), 10.0);

        // A1:A6 = 1,_,2,3,4,5 with the hidden row now at 6; B10 moved to B11.
        engine.insert_rows("Sheet1", 2, 1).unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(engine.is_row_hidden("Sheet1", 6, None), Some(true));
        assert_eq!(num(&engine, "Sheet1", 11, 2), 10.0);
        hide(&mut engine, 3, true, RowVisibilitySource::Manual);
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 11, 2), 8.0);

        // Deleting row 6 leaves A1:A5 = 1,_,2(hidden),3,4.
        engine.delete_rows("Sheet1", 6, 1).unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 10, 2), 8.0);
        hide(&mut engine, 3, false, RowVisibilitySource::Manual);
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 10, 2), 10.0);
    }
}

#[test]
fn undo_and_redo_of_visibility_update_subtotals() {
    let mut engine = workbook(true);
    let mut log = ChangeLog::new();
    let mut undo = UndoEngine::new();
    engine
        .action_with_logger(&mut log, "hide", |tx| {
            tx.set_row_hidden("Sheet1", 1, true, RowVisibilitySource::Manual)?;
            tx.set_row_hidden("Sheet1", 2, true, RowVisibilitySource::Filter)?;
            Ok(())
        })
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_column(&engine, 52.0);

    assert_eq!(num(&engine, "Sheet1", 1, 4), 12.0);

    engine.undo_logged(&mut undo, &mut log).unwrap();
    engine.evaluate_all().unwrap();
    assert_column(&engine, 55.0);
    assert_eq!(num(&engine, "Sheet1", 1, 4), 15.0);

    engine.redo_logged(&mut undo, &mut log).unwrap();
    engine.evaluate_all().unwrap();
    assert_column(&engine, 52.0);
    assert_eq!(num(&engine, "Sheet1", 1, 4), 12.0);
}

#[test]
fn hides_on_another_sheet_and_in_named_formulas_reach_subtotals() {
    let mut engine = workbook(false);
    formula(&mut engine, "Sheet2", 1, 1, "=SUBTOTAL(109,Sheet1!A1:A10)");
    engine
        .define_name(
            "VisibleTotal",
            NamedDefinition::Formula {
                ast: parse("=SUBTOTAL(109,Sheet1!$A$1:$A$10)").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();
    formula(&mut engine, "Sheet2", 2, 1, "=VisibleTotal+0");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet2", 1, 1), 55.0);
    assert_eq!(num(&engine, "Sheet2", 2, 1), 55.0);

    hide(&mut engine, 10, true, RowVisibilitySource::Manual);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet2", 1, 1), 45.0);
    assert_eq!(num(&engine, "Sheet2", 2, 1), 45.0);
}

#[test]
fn nested_subtotals_follow_edits_to_the_inner_range() {
    for family in FAMILY_EXECUTION {
        let mut engine = engine(family);
        for row in 1..=4 {
            value(&mut engine, "Sheet1", row, 1, 10.0);
        }
        formula(&mut engine, "Sheet1", 5, 1, "=SUBTOTAL(9,A1:A4)");
        value(&mut engine, "Sheet1", 6, 1, 1.0);
        formula(&mut engine, "Sheet1", 7, 1, "=SUBTOTAL(9,A1:A6)");
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 7, 1), 41.0);

        value(&mut engine, "Sheet1", 2, 1, 20.0);
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 5, 1), 50.0);
        assert_eq!(num(&engine, "Sheet1", 7, 1), 51.0);

        // Replacing the inner subtotal by a plain value makes it count.
        value(&mut engine, "Sheet1", 5, 1, 5.0);
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 7, 1), 56.0);
    }
}

/// Structural edits move vertices in the sheet index too: region queries
/// (nested-subtotal detection, targeted evaluation) find every vertex at its
/// current address after rows and columns are inserted or deleted.
#[test]
fn the_sheet_index_follows_vertices_through_structural_edits() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            formula_compression: false,
            ..arrow_eval_config()
        },
    );
    for r in 1..=200 {
        value(&mut engine, "Sheet1", r, 1, r as f64);
        formula(
            &mut engine,
            "Sheet1",
            r,
            3,
            &format!("=SUBTOTAL(9,$A$1:A{r})"),
        );
    }
    formula(&mut engine, "Sheet1", 205, 5, "=SUBTOTAL(9,C150:C160)");
    engine.evaluate_all().unwrap();

    let sheet = engine.graph.sheet_id("Sheet1").unwrap();
    let assert_indexed = |engine: &TestEngine, step: &str| {
        for v in engine.graph.vertices_with_formulas() {
            let cell = engine.graph.get_cell_ref(v).unwrap();
            let (row, col) = (cell.coord.row(), cell.coord.col());
            assert!(
                engine
                    .graph
                    .vertices_in_region(sheet, row, row, col, col)
                    .contains(&v),
                "{step}: R{row}C{col} missing from the index",
            );
        }
    };

    engine.insert_rows("Sheet1", 61, 3).unwrap();
    assert_indexed(&engine, "insert rows");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 208, 5), 0.0);

    engine.delete_rows("Sheet1", 10, 5).unwrap();
    assert_indexed(&engine, "delete rows");
    engine.insert_columns("Sheet1", 2, 2).unwrap();
    assert_indexed(&engine, "insert columns");
    engine.delete_columns("Sheet1", 2, 1).unwrap();
    assert_indexed(&engine, "delete columns");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 203, 6), 0.0);
}
