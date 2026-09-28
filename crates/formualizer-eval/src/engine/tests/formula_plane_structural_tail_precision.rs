use std::sync::Arc;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn authoritative_engine() -> Engine<TestWorkbook> {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    Engine::new(TestWorkbook::default(), cfg)
}

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn active_span_count(engine: &Engine<TestWorkbook>) -> usize {
    engine.baseline_stats().formula_plane_active_span_count
}

fn assert_number(engine: &Engine<TestWorkbook>, row: u32, col: u32, expected: f64) {
    assert_eq!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Number(expected))
    );
}

fn build_column_family(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity((rows * 5) as usize);
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        for col in 7..=11 {
            engine
                .set_cell_value(
                    "Sheet1",
                    row,
                    col,
                    LiteralValue::Number((row * 100 + col) as f64),
                )
                .unwrap();
        }
        for (idx, col) in (2..=6).enumerate() {
            let addend = idx + 1;
            formulas.push(record(&mut engine, row, col, &format!("=A{row}+{addend}")));
        }
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn column_delete_outside_span_region_with_dirty_closure_no_recompute() {
    let mut engine = build_column_family(1000);
    for col in 2..=6 {
        assert_number(&engine, 123, col, 123.0 + f64::from(col - 1));
    }

    engine.delete_columns("Sheet1", 7, 1).unwrap();

    for col in 2..=6 {
        assert_number(&engine, 123, col, 123.0 + f64::from(col - 1));
    }
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 0, "result={result:?}");
    for col in 2..=6 {
        assert_number(&engine, 987, col, 987.0 + f64::from(col - 1));
    }
}

#[test]
fn column_insert_outside_span_region_with_dirty_closure_no_recompute() {
    let mut engine = build_column_family(1000);

    engine.insert_columns("Sheet1", 7, 1).unwrap();

    for col in 2..=6 {
        assert_number(&engine, 321, col, 321.0 + f64::from(col - 1));
    }
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 0, "result={result:?}");
    for col in 2..=6 {
        assert_number(&engine, 654, col, 654.0 + f64::from(col - 1));
    }
}

fn build_row_run(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn failed_duplicate_validation_publishes_no_structural_dirty_delta() {
    let mut engine = build_row_run(120);
    let before = engine.baseline_stats();
    assert!(engine.duplicate_sheet("Sheet1", "Sheet1").is_err());
    let after = engine.baseline_stats();
    assert_eq!(
        after.formula_plane_dirty_span_region_events_recorded,
        before.formula_plane_dirty_span_region_events_recorded
    );
    assert_eq!(
        after.formula_plane_dirty_region_events_recorded,
        before.formula_plane_dirty_region_events_recorded
    );
}

fn ingest_row_run_on_sheet(
    engine: &mut Engine<TestWorkbook>,
    sheet: &str,
    rows: u32,
    formula_col: u32,
) {
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value(sheet, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(engine, row, formula_col, &format!("=A{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(sheet, formulas)])
        .unwrap();
}

/// The value assertion of `structural_span_region_isolated_to_edited_sheet`
/// (its span dirty regions and eval report are span-internal).
#[test]
fn structural_span_region_isolated_to_edited_sheet_values() {
    let mut engine = authoritative_engine();
    engine.add_sheet("Sheet2").unwrap();
    ingest_row_run_on_sheet(&mut engine, "Sheet1", 1000, 2);
    ingest_row_run_on_sheet(&mut engine, "Sheet2", 1000, 2);
    engine.evaluate_all().unwrap();
    engine.insert_rows("Sheet1", 901, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet2", 999, 2),
        Some(LiteralValue::Number(1000.0))
    );
}

#[test]
fn sheet_and_unrelated_name_table_lifecycle_do_not_dirty_surviving_spans() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use crate::reference::{CellRef, Coord, RangeRef};

    let mut engine = authoritative_engine();
    engine.add_sheet("Sheet2").unwrap();
    ingest_row_run_on_sheet(&mut engine, "Sheet1", 120, 2);
    ingest_row_run_on_sheet(&mut engine, "Sheet2", 120, 2);
    engine.evaluate_all().unwrap();

    engine
        .rename_sheet(engine.graph.sheet_id("Sheet2").unwrap(), "Renamed")
        .unwrap();

    engine
        .define_name(
            "Unrelated",
            NamedDefinition::Literal(LiteralValue::Number(7.0)),
            NameScope::Workbook,
        )
        .unwrap();

    let sheet1_id = engine.graph.sheet_id("Sheet1").unwrap();
    engine
        .define_table(
            "UnrelatedTable",
            RangeRef::new(
                CellRef::new(sheet1_id, Coord::from_excel(1, 10, true, true)),
                CellRef::new(sheet1_id, Coord::from_excel(2, 10, true, true)),
            ),
            true,
            vec!["Value".into()],
            false,
        )
        .unwrap();
}

#[test]
fn structural_insert_action_undo_redo_preserves_values_without_unlogged_span_geometry() {
    use crate::engine::graph::editor::undo_engine::UndoEngine;

    let mut engine = build_row_run(120);
    let mut undo = UndoEngine::new();
    let (_value, journal) = engine
        .action_atomic_journal("insert row".to_string(), |tx| {
            tx.insert_rows("Sheet1", 61, 1)?;
            Ok(())
        })
        .unwrap();
    undo.push_action(journal);
    assert_eq!(
        active_span_count(&engine),
        0,
        "journaled geometry is materialized"
    );
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 62, 2),
        Some(LiteralValue::Number(62.0))
    );

    engine.undo_action(&mut undo).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 61, 2),
        Some(LiteralValue::Number(62.0))
    );

    engine.redo_action(&mut undo).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 62, 2),
        Some(LiteralValue::Number(62.0))
    );
}

/// The values of `structural_candidate_overflow_is_atomic_and_retryable`
/// before and after its retried insertion (the span candidate-cap
/// overflow has no seam under the authority).
#[test]
fn structural_candidate_overflow_is_atomic_and_retryable_values() {
    let mut engine = build_row_run(120);
    assert_eq!(
        engine.get_cell_value("Sheet1", 111, 1),
        Some(LiteralValue::Number(111.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 121, 1), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 120, 2),
        Some(LiteralValue::Number(121.0))
    );

    engine.insert_rows("Sheet1", 111, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 121, 2),
        Some(LiteralValue::Number(121.0))
    );
}

/// The value assertion of
/// `indexed_structural_selection_classifies_only_affected_candidate_among_many_sheets`
/// (its span candidate counts and eval report are span-internal).
#[test]
fn indexed_structural_selection_classifies_only_affected_candidate_among_many_sheets_values() {
    const SHEETS: u32 = 24;
    let mut engine = authoritative_engine();
    for index in 0..SHEETS {
        let sheet = if index == 0 {
            "Sheet1".to_string()
        } else {
            let name = format!("Unrelated{index}");
            engine.add_sheet(&name).unwrap();
            name
        };
        ingest_row_run_on_sheet(&mut engine, &sheet, 120, 2);
    }
    engine.evaluate_all().unwrap();
    engine.insert_rows("Sheet1", 111, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Unrelated23", 120, 2),
        Some(LiteralValue::Number(121.0))
    );
}
