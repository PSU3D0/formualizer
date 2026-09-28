//! Range cell counts must not wrap before the expansion-limit check.
//!
//! A whole-sheet range is 1,048,576 x 16,384 = 2^34 cells. Computed in `u32`
//! the product wraps (to 0 for these extents), so the range passed the
//! `range_expansion_limit` check and was expanded cell by cell: an overflow
//! panic with debug assertions and an effectively unbounded loop in release.

use std::sync::Arc;

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const MAX_ROW: u32 = 1_048_576;
const MAX_COL: u32 = 16_384;

fn engine_with_inputs() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Data").unwrap();
    engine.add_sheet("Out").unwrap();
    engine
        .set_cell_value("Data", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Data", 7, 3, LiteralValue::Number(2.0))
        .unwrap();
    engine
}

fn range(engine: &Engine<TestWorkbook>, end_row: u32, end_col: u32) -> NamedDefinition {
    let sid = engine.sheet_id("Data").unwrap();
    let start = CellRef::new(sid, Coord::from_excel(1, 1, true, true));
    let end = CellRef::new(sid, Coord::from_excel(end_row, end_col, true, true));
    NamedDefinition::Range(RangeRef::new(start, end))
}

fn number(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
    engine.get_cell_value(sheet, row, col)
}

#[test]
fn define_name_whole_sheet_range_does_not_wrap_the_size_check() {
    for (end_row, end_col) in [(MAX_ROW, MAX_COL), (MAX_ROW, 4_096)] {
        let mut engine = engine_with_inputs();
        let definition = range(&engine, end_row, end_col);
        engine
            .define_name("AllData", definition, NameScope::Workbook)
            .unwrap();
        engine
            .set_cell_formula("Out", 1, 1, parse("=SUM(AllData)").unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(
            number(&engine, "Out", 1, 1),
            Some(LiteralValue::Number(3.0))
        );

        engine
            .set_cell_value("Data", 5_000, 300, LiteralValue::Number(10.0))
            .unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(
            number(&engine, "Out", 1, 1),
            Some(LiteralValue::Number(13.0))
        );
    }
}

#[test]
fn define_name_small_range_still_tracks_cell_edits() {
    let mut engine = engine_with_inputs();
    let definition = range(&engine, 8, 8);
    engine
        .define_name("Small", definition, NameScope::Workbook)
        .unwrap();
    engine
        .set_cell_formula("Out", 1, 1, parse("=SUM(Small)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        number(&engine, "Out", 1, 1),
        Some(LiteralValue::Number(3.0))
    );
    engine
        .set_cell_value("Data", 8, 8, LiteralValue::Number(4.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        number(&engine, "Out", 1, 1),
        Some(LiteralValue::Number(7.0))
    );
}

#[test]
fn formula_whole_sheet_range_does_not_wrap_the_size_check() {
    let mut engine = engine_with_inputs();
    engine
        .set_cell_formula("Out", 1, 1, parse("=SUM(Data!A1:XFD1048576)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        number(&engine, "Out", 1, 1),
        Some(LiteralValue::Number(3.0))
    );
    engine
        .set_cell_value("Data", 5_000, 300, LiteralValue::Number(10.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        number(&engine, "Out", 1, 1),
        Some(LiteralValue::Number(13.0))
    );
}

#[test]
fn ingested_whole_sheet_range_does_not_wrap_the_size_check() {
    let mut engine = engine_with_inputs();
    let formula = "=SUM(Data!A1:XFD1048576)";
    let ast_id = engine.intern_formula_ast(&parse(formula).unwrap());
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(
            "Out",
            vec![FormulaIngestRecord::new(
                1,
                1,
                ast_id,
                Some(Arc::<str>::from(formula)),
            )],
        )])
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        number(&engine, "Out", 1, 1),
        Some(LiteralValue::Number(3.0))
    );
    engine
        .set_cell_value("Data", 5_000, 300, LiteralValue::Number(10.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        number(&engine, "Out", 1, 1),
        Some(LiteralValue::Number(13.0))
    );
}
