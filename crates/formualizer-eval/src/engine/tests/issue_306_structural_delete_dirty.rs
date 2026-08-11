use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::{
    ChangeLog, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::function::{FnCaps, Function};
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parse;

const SHEET: &str = "Model";
const FORMULA_ROWS: u32 = 120;

fn issue_fixture() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=FORMULA_ROWS + 8 {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(f64::from(row)))
            .unwrap();
        engine
            .set_cell_value(SHEET, row, 2, LiteralValue::Number(f64::from(row * 2)))
            .unwrap();
    }

    let mut records = Vec::with_capacity(FORMULA_ROWS as usize);
    for row in 1..=FORMULA_ROWS {
        let formula = format!("=SUM($B:$B)+A{row}+7");
        let ast_id = engine.intern_formula_ast(&parse(&formula).unwrap());
        records.push(FormulaIngestRecord::new(
            row,
            4,
            ast_id,
            Some(Arc::<str>::from(formula)),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, records)])
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

fn number(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> f64 {
    match engine.get_cell_value(SHEET, row, col) {
        Some(LiteralValue::Number(value)) => value,
        value => panic!("expected number at {SHEET}!R{row}C{col}, got {value:?}"),
    }
}

#[test]
fn delete_rows_recomputes_really_ingested_whole_column_readers_for_issue_306() {
    let mut engine = issue_fixture();
    assert_eq!(number(&engine, 1, 4), 16_520.0);

    engine.delete_rows(SHEET, 60, 1).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, 1, 4), 16_400.0);
}

#[test]
fn delete_columns_recomputes_whole_row_readers() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    engine
        .set_cell_value(SHEET, 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    for col in 1..=FORMULA_ROWS + 8 {
        engine
            .set_cell_value(SHEET, 2, col, LiteralValue::Number(f64::from(col * 2)))
            .unwrap();
    }
    engine
        .set_cell_formula(SHEET, 1, 4, parse("=SUM($2:$2)+A1+7").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(number(&engine, 1, 4), 16_520.0);

    engine.delete_columns(SHEET, 60, 1).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, 1, 4), 16_400.0);
}

#[test]
fn delete_rows_keeps_bounded_range_recalculation_correct() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=FORMULA_ROWS {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(f64::from(row)))
            .unwrap();
        engine
            .set_cell_value(SHEET, row, 2, LiteralValue::Number(f64::from(row * 2)))
            .unwrap();
    }
    engine
        .set_cell_formula(SHEET, 1, 4, parse("=SUM(B1:B120)+A1+7").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(number(&engine, 1, 4), 14_528.0);

    engine.delete_rows(SHEET, 60, 1).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, 1, 4), 14_408.0);
}

#[test]
fn delete_rows_outside_bounded_read_region_does_not_change_result() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=30 {
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    engine
        .set_cell_formula(SHEET, 1, 1, parse("=SUM(Data!B10:B20)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(number(&engine, 1, 1), 165.0);

    engine.delete_rows("Data", 25, 1).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, 1, 1), 165.0);
}

fn cross_sheet_whole_column_fixture() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=FORMULA_ROWS + 8 {
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row * 2)))
            .unwrap();
    }
    engine
        .set_cell_formula(SHEET, 1, 1, parse("=SUM(Data!$B:$B)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn delete_rows_at_first_and_last_bounded_range_boundaries_remain_correct() {
    for (row, expected) in [(10, 155.0), (20, 145.0)] {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig::default().with_parallel(false),
        );
        for data_row in 1..=30 {
            engine
                .set_cell_value(
                    "Data",
                    data_row,
                    2,
                    LiteralValue::Number(f64::from(data_row)),
                )
                .unwrap();
        }
        engine
            .set_cell_formula(SHEET, 1, 1, parse("=SUM(Data!B10:B20)").unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(number(&engine, 1, 1), 165.0);

        engine.delete_rows("Data", row, 1).unwrap();
        engine.evaluate_all().unwrap();

        assert_eq!(number(&engine, 1, 1), expected, "deleted row {row}");
    }
}

#[test]
fn delete_rows_recomputes_whole_column_reader_after_multi_row_delete() {
    let mut engine = cross_sheet_whole_column_fixture();
    assert_eq!(number(&engine, 1, 1), 16_512.0);

    engine.delete_rows("Data", 60, 3).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, 1, 1), 16_146.0);
}

#[test]
fn delete_rows_recomputes_whole_column_reader_at_first_and_last_populated_boundaries() {
    for (row, expected) in [(1, 16_510.0), (FORMULA_ROWS + 8, 16_256.0)] {
        let mut engine = cross_sheet_whole_column_fixture();
        engine.delete_rows("Data", row, 1).unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(number(&engine, 1, 1), expected, "deleted row {row}");
    }
}

#[derive(Debug)]
struct CountFn {
    name: &'static str,
    calls: Arc<AtomicUsize>,
}

impl Function for CountFn {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CalcValue::Scalar(LiteralValue::Number(0.0)))
    }
}

#[test]
fn delete_rows_recomputes_only_formulas_depending_on_deleted_region() {
    let affected_calls = Arc::new(AtomicUsize::new(0));
    let unaffected_calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new()
        .with_function(Arc::new(CountFn {
            name: "ISSUE306_AFFECTED",
            calls: Arc::clone(&affected_calls),
        }))
        .with_function(Arc::new(CountFn {
            name: "ISSUE306_UNAFFECTED",
            calls: Arc::clone(&unaffected_calls),
        }));
    let mut engine = Engine::new(workbook, EvalConfig::default().with_parallel(false));
    for row in 1..=100 {
        engine
            .set_cell_value(SHEET, row, 2, LiteralValue::Number(f64::from(row)))
            .unwrap();
        engine
            .set_cell_value(SHEET, row, 26, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    engine
        .set_cell_formula(
            SHEET,
            1,
            4,
            parse("=ISSUE306_AFFECTED()+SUM($B:$B)").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            SHEET,
            1,
            5,
            parse("=ISSUE306_UNAFFECTED()+SUM(Z1:Z10)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(affected_calls.load(Ordering::SeqCst), 1);
    assert_eq!(unaffected_calls.load(Ordering::SeqCst), 1);

    engine.delete_rows(SHEET, 60, 1).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(affected_calls.load(Ordering::SeqCst), 2);
    assert_eq!(unaffected_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn legacy_delete_rows_matches_formula_plane_authority_on_issue_306_fixture() {
    let build = |mode| {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig::default()
                .with_formula_plane_mode(mode)
                .with_parallel(false),
        );
        for row in 1..=FORMULA_ROWS + 8 {
            engine
                .set_cell_value(SHEET, row, 1, LiteralValue::Number(f64::from(row)))
                .unwrap();
            engine
                .set_cell_value(SHEET, row, 2, LiteralValue::Number(f64::from(row * 2)))
                .unwrap();
        }
        let mut records = Vec::with_capacity(FORMULA_ROWS as usize);
        for row in 1..=FORMULA_ROWS {
            let formula = format!("=SUM($B:$B)+A{row}+7");
            let ast_id = engine.intern_formula_ast(&parse(&formula).unwrap());
            records.push(FormulaIngestRecord::new(
                row,
                4,
                ast_id,
                Some(Arc::<str>::from(formula)),
            ));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, records)])
            .unwrap();
        engine.evaluate_all().unwrap();
        engine
    };
    let mut legacy = build(FormulaPlaneMode::Off);
    let mut authoritative = build(FormulaPlaneMode::AuthoritativeExperimental);
    assert_eq!(
        authoritative
            .baseline_stats()
            .formula_plane_active_span_count,
        1
    );

    for engine in [&mut legacy, &mut authoritative] {
        engine.delete_rows(SHEET, 60, 1).unwrap();
        engine.evaluate_all().unwrap();
    }

    assert_eq!(number(&legacy, 1, 4), 16_400.0);
    assert_eq!(number(&legacy, 1, 4), number(&authoritative, 1, 4));
}

#[test]
fn undo_of_logged_delete_restores_whole_column_reader_value() {
    let mut engine = issue_fixture();
    let sheet_id = engine.sheet_id(SHEET).unwrap();
    let mut log = ChangeLog::new();
    engine
        .edit_with_logger(&mut log, |editor| editor.delete_rows(sheet_id, 59, 1))
        .unwrap()
        .unwrap();
    engine
        .sheet_store_mut()
        .sheet_mut(SHEET)
        .unwrap()
        .delete_rows(59, 1);
    engine.evaluate_all().unwrap();
    assert_eq!(number(&engine, 1, 4), 16_400.0);

    {
        let sheet = engine.sheet_store_mut().sheet_mut(SHEET).unwrap();
        sheet.insert_rows(59, 1);
        sheet.set_sparse_overlay_value(59, 0, crate::arrow_store::OverlayValue::Number(60.0));
        sheet.set_sparse_overlay_value(59, 1, crate::arrow_store::OverlayValue::Number(120.0));
    }
    let mut undo = UndoEngine::new();
    engine.undo_logged(&mut undo, &mut log).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, 1, 4), 16_520.0);
}
