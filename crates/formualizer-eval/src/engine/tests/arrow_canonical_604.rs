//! Tests for ticket 604 — Arrow canonical values: unified read/write.
//!
//! Validates that value-read paths (get_cell_value, evaluation, range aggregates,
//! spills, named ranges) route through Arrow storage.

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{EvalConfig, eval::Engine};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn canonical_get_cell_value_routes_through_arrow() {
    let setup = |engine: &mut Engine<TestWorkbook>| {
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(42.0))
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 1, 2, parse("=A1*3").unwrap())
            .unwrap();
    };

    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());
    setup(&mut engine);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(126.0))
    );
}

#[test]
fn canonical_range_aggregate_parity() {
    let setup = |engine: &mut Engine<TestWorkbook>| {
        for r in 1..=10 {
            engine
                .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
                .unwrap();
        }
        engine
            .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1:A10)").unwrap())
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 2, 2, parse("=AVERAGE(A1:A10)").unwrap())
            .unwrap();
    };

    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());
    setup(&mut engine);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(55.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(5.5))
    );
}

#[test]
fn canonical_spill_parity() {
    let setup = |engine: &mut Engine<TestWorkbook>| {
        engine
            .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(5,2)").unwrap())
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 7, 1, parse("=SUM(A1:B5)").unwrap())
            .unwrap();
    };

    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());
    setup(&mut engine);
    engine.evaluate_all().unwrap();

    for r in 1..=5u32 {
        for c in 1..=2u32 {
            let expected = ((r - 1) * 2 + c) as f64;
            assert_eq!(
                engine.get_cell_value("Sheet1", r, c),
                Some(LiteralValue::Number(expected))
            );
        }
    }
    // SUM(1..10) = 55
    assert_eq!(
        engine.get_cell_value("Sheet1", 7, 1),
        Some(LiteralValue::Number(55.0))
    );
}

#[test]
fn canonical_constructor_forces_overlay_flags() {
    let wb = TestWorkbook::new();
    let cfg = EvalConfig {
        arrow_storage_enabled: false,
        delta_overlay_enabled: false,
        write_formula_overlay_enabled: false,
        ..EvalConfig::default()
    };
    let engine = Engine::new(wb, cfg);
    assert!(
        engine.config.arrow_storage_enabled,
        "arrow_storage_enabled must be forced true"
    );
    assert!(
        engine.config.delta_overlay_enabled,
        "delta_overlay_enabled must be forced true"
    );
    assert!(
        engine.config.write_formula_overlay_enabled,
        "write_formula_overlay_enabled must be forced true"
    );
}

#[test]
fn canonical_named_range_parity() {
    let setup = |engine: &mut Engine<TestWorkbook>| {
        for r in 1..=5u32 {
            engine
                .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64 * 10.0))
                .unwrap();
        }
        let sheet_id = engine.graph.sheet_id("Sheet1").unwrap();
        let start = CellRef::new(sheet_id, Coord::new(0, 0, true, true));
        let end = CellRef::new(sheet_id, Coord::new(4, 0, true, true));
        let range_def = NamedDefinition::Range(RangeRef::new(start, end));
        engine
            .define_name("prices", range_def, NameScope::Workbook)
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 1, 2, parse("=SUM(prices)").unwrap())
            .unwrap();
    };

    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());
    setup(&mut engine);
    engine.evaluate_all().unwrap();

    // SUM(10+20+30+40+50) = 150
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(150.0))
    );
}
