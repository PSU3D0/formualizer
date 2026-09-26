//! Re-binding when a missing symbol appears later (Program 1 M4).
//!
//! Each case runs unchanged on the legacy graph (the oracle) and under
//! `unified_authority`: a reader bound to nothing (undefined name, missing
//! table, missing sheet) must pick up the symbol once it exists, evaluate in
//! the right order after it, and keep tracking its precedents afterwards.

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig, PreparationPolicy};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn num(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> Option<f64> {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => Some(n),
        Some(LiteralValue::Int(n)) => Some(n as f64),
        _ => None,
    }
}

fn set(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, v: f64) {
    engine
        .set_cell_value(sheet, row, col, LiteralValue::Number(v))
        .unwrap();
}

fn formula(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, f: &str) {
    engine
        .set_cell_formula(sheet, row, col, parse(f).unwrap())
        .unwrap();
}

#[test]
fn formula_name_defined_after_its_reader_binds_and_orders() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    set(&mut engine, "Sheet1", 1, 1, 5.0);
    // A2 is a formula the name reads, so the name must run after it.
    formula(&mut engine, "Sheet1", 2, 1, "=A1*10");
    formula(&mut engine, "Sheet1", 1, 2, "=Later+1");
    engine.evaluate_all().unwrap();
    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Name),
        other => panic!("unbound name should be #NAME?, got {other:?}"),
    }

    engine
        .define_name(
            "Later",
            NamedDefinition::Formula {
                ast: parse("=A2+A1").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 2), Some(56.0));

    set(&mut engine, "Sheet1", 1, 1, 2.0);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 2, 1), Some(20.0));
    assert_eq!(num(&engine, "Sheet1", 1, 2), Some(23.0));
}

#[test]
fn nested_name_redefinition_rebinds_through_the_outer_name() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sheet = engine.sheet_id("Sheet1").unwrap();
    let at = |r| CellRef::new(sheet, Coord::from_excel(r, 1, true, true));
    set(&mut engine, "Sheet1", 1, 1, 3.0);
    formula(&mut engine, "Sheet1", 2, 1, "=A1+100");
    engine
        .define_name("Inner", NamedDefinition::Cell(at(1)), NameScope::Workbook)
        .unwrap();
    engine
        .define_name(
            "Outer",
            NamedDefinition::Formula {
                ast: parse("=Inner*2").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();
    formula(&mut engine, "Sheet1", 1, 2, "=Outer+1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 2), Some(7.0));

    // Inner now reads a formula cell: Outer and its reader must follow it.
    engine
        .update_name("Inner", NamedDefinition::Cell(at(2)), NameScope::Workbook)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 2), Some(207.0));

    set(&mut engine, "Sheet1", 1, 1, 10.0);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 2), Some(221.0));
}

#[test]
fn name_redefined_to_a_new_target_rebinds_readers() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sheet = engine.sheet_id("Sheet1").unwrap();
    set(&mut engine, "Sheet1", 1, 1, 1.0);
    formula(&mut engine, "Sheet1", 2, 1, "=A1*100");
    let at = |r| CellRef::new(sheet, Coord::from_excel(r, 1, true, true));
    engine
        .define_name("Target", NamedDefinition::Cell(at(1)), NameScope::Workbook)
        .unwrap();
    formula(&mut engine, "Sheet1", 1, 3, "=Target+1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 3), Some(2.0));

    engine
        .update_name("Target", NamedDefinition::Cell(at(2)), NameScope::Workbook)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 3), Some(101.0));

    // Now tracks A2 (through A1), no longer A1 directly.
    set(&mut engine, "Sheet1", 1, 1, 4.0);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 3), Some(401.0));
}

#[test]
fn sheet_added_after_an_eager_value_reader_tracks_edits() {
    // The reader's sheet exists at ingest (implicitly created by a value),
    // but only gets a formula on it afterwards.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    set(&mut engine, "Other", 1, 2, 1.0);
    formula(&mut engine, "Sheet1", 1, 1, "=Other!A1+Other!B1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), Some(1.0));
    formula(&mut engine, "Other", 1, 1, "=B1*3");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), Some(4.0));
    set(&mut engine, "Other", 1, 2, 2.0);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), Some(8.0));
}

fn best_effort(defer: bool) -> EvalConfig {
    EvalConfig {
        defer_graph_building: defer,
        ..EvalConfig::default()
    }
    .with_preparation_policy(PreparationPolicy::BestEffort)
}

#[test]
fn strict_policy_still_rejects_missing_sheet_and_table() {
    assert_eq!(
        EvalConfig::default().preparation_policy,
        PreparationPolicy::BestEffort,
        "BestEffort is the default (decision 16)"
    );
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_preparation_policy(PreparationPolicy::Strict),
    );
    assert!(
        engine
            .set_cell_formula("Sheet1", 1, 1, parse("=Nope!A1").unwrap())
            .is_err()
    );
    assert!(
        engine
            .set_cell_formula("Sheet1", 1, 2, parse("=SUM(NoTable[X])").unwrap())
            .is_err()
    );
}

#[test]
fn best_effort_sheet_added_later_binds_and_orders() {
    for defer in [false, true] {
        let mut engine = Engine::new(TestWorkbook::new(), best_effort(defer));
        formula(&mut engine, "Sheet1", 1, 1, "=Later!A1*2");
        formula(&mut engine, "Sheet1", 1, 2, "=SUM(Later!A1:A1000)");
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 1, 1), None, "unbound while missing");

        engine.add_sheet("Later").unwrap();
        set(&mut engine, "Later", 1, 2, 4.0);
        formula(&mut engine, "Later", 1, 1, "=B1+1");
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 1, 1), Some(10.0), "defer={defer}");
        assert_eq!(num(&engine, "Sheet1", 1, 2), Some(5.0), "defer={defer}");

        set(&mut engine, "Later", 1, 2, 9.0);
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Later", 1, 1), Some(10.0));
        assert_eq!(num(&engine, "Sheet1", 1, 1), Some(20.0));
        assert_eq!(num(&engine, "Sheet1", 1, 2), Some(10.0));
    }
}

#[test]
fn best_effort_sheet_created_implicitly_binds() {
    let mut engine = Engine::new(TestWorkbook::new(), best_effort(false));
    formula(&mut engine, "Sheet1", 1, 1, "=Later!A1+1");
    engine.evaluate_all().unwrap();
    // A value write creates the sheet without add_sheet.
    set(&mut engine, "Later", 1, 1, 41.0);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), Some(42.0));
}

#[test]
fn best_effort_table_defined_later_binds_and_tracks_edits() {
    for defer in [false, true] {
        let mut engine = Engine::new(TestWorkbook::new(), best_effort(defer));
        set(&mut engine, "Sheet1", 2, 1, 5.0);
        set(&mut engine, "Sheet1", 3, 1, 7.0);
        set(&mut engine, "Sheet1", 4, 1, 10.0);
        formula(&mut engine, "Sheet1", 1, 4, "=SUM(Sales[Amount])");
        // A formula outside the table reads the reader, so it must follow it.
        formula(&mut engine, "Sheet1", 1, 5, "=D1+1");
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 1, 4), None, "unbound while missing");

        let sheet = engine.sheet_id("Sheet1").unwrap();
        let range = RangeRef::new(
            CellRef::new(sheet, Coord::from_excel(1, 1, true, true)),
            CellRef::new(sheet, Coord::from_excel(4, 1, true, true)),
        );
        engine
            .define_table("Sales", range, true, vec!["Amount".into()], false)
            .unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 1, 4), Some(22.0), "defer={defer}");

        assert_eq!(num(&engine, "Sheet1", 1, 5), Some(23.0));

        set(&mut engine, "Sheet1", 2, 1, 1.0);
        engine.evaluate_all().unwrap();
        assert_eq!(num(&engine, "Sheet1", 1, 4), Some(18.0));
        assert_eq!(num(&engine, "Sheet1", 1, 5), Some(19.0));
    }
}

/// A formula inside a table's body read through a structured reference must
/// run before the reader. Legacy orders the reader first (the table symbol
/// carries no edge to body formulas): 12 then 18 here. The authority's table
/// edge covers the body, so the reader follows A4. Expected Δ, legacy wrong.
#[test]
fn structured_reader_follows_formula_in_table_body() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    set(&mut engine, "Sheet1", 2, 1, 5.0);
    set(&mut engine, "Sheet1", 3, 1, 7.0);
    let sheet = engine.sheet_id("Sheet1").unwrap();
    let range = RangeRef::new(
        CellRef::new(sheet, Coord::from_excel(1, 1, true, true)),
        CellRef::new(sheet, Coord::from_excel(4, 1, true, true)),
    );
    engine
        .define_table("Sales", range, true, vec!["Amount".into()], false)
        .unwrap();
    formula(&mut engine, "Sheet1", 4, 1, "=A2*2");
    formula(&mut engine, "Sheet1", 1, 4, "=SUM(Sales[Amount])");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 4), Some(22.0));
    set(&mut engine, "Sheet1", 2, 1, 1.0);
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 4), Some(10.0));
}
