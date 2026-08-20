use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::engine::graph::editor::undo_engine::UndoEngine;
use crate::engine::inspect::{SnapshotOptions, Staleness};
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
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    for row in 1..=12 {
        for col in 21..=26 {
            engine
                .set_cell_value("Data", row, col, LiteralValue::Number(f64::from(row)))
                .unwrap();
        }
    }
    engine
        .set_cell_formula(
            SHEET,
            1,
            4,
            parse("=ISSUE306_AFFECTED()+SUM(Data!$B:$B)").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            SHEET,
            1,
            5,
            // Area 72 exceeds the default expansion limit (64), so this is a
            // compressed registration on the edited sheet, disjoint from row 60.
            parse("=ISSUE306_UNAFFECTED()+SUM(Data!U1:Z12)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(affected_calls.load(Ordering::SeqCst), 1);
    assert_eq!(unaffected_calls.load(Ordering::SeqCst), 1);

    engine.delete_rows("Data", 60, 1).unwrap();
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

fn out_formula_vertex(engine: &Engine<TestWorkbook>, row: u32) -> crate::engine::vertex::VertexId {
    *engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Out", row, 1))
        .expect("formula vertex")
}

fn assert_exact_vertices(
    engine: &Engine<TestWorkbook>,
    mut actual: Vec<crate::engine::vertex::VertexId>,
    expected_rows: &[u32],
) {
    let mut expected = expected_rows
        .iter()
        .map(|&row| out_formula_vertex(engine, row))
        .collect::<Vec<_>>();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[test]
fn compressed_delete_queries_select_exact_shape_and_boundary_matrix() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=350 {
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Number(1.0))
            .unwrap();
    }
    for col in 1..=52 {
        engine
            .set_cell_value("Data", 1, col, LiteralValue::Number(1.0))
            .unwrap();
    }
    engine
        .set_cell_value("Other", 1, 1, LiteralValue::Number(1.0))
        .unwrap();

    // Every range below is compressed. Formulas live on Out so Data references
    // also cover the cross-sheet-positive case; rows 8 and 9 are the negative case.
    let cases = [
        "=SUM(Data!$B:$B)",
        "=SUM(Data!$2:$2)",
        "=SUM(Data!B:B65)",
        "=SUM(Data!B66:B)",
        "=SUM(Data!Z1:Z65)",
        "=SUM(Data!Z100:Z300)",
        "=SUM(Data!U1:Z12)",
        "=SUM(Other!$B:$B)",
        "=SUM(Other!Z1:Z65)",
        "=SUM(Data!$AH:$AN)",
    ];
    for (index, formula) in cases.into_iter().enumerate() {
        engine
            .set_cell_formula("Out", index as u32 + 1, 1, parse(formula).unwrap())
            .unwrap();
    }

    let data = engine.sheet_id("Data").unwrap();
    let occupancy = engine.graph.structural_occupancy(data);
    // Row windows touch each finite range's exact upper and lower boundaries,
    // and include whole-column, multi-column-open, half-open, and bounded shapes.
    for (start, end, expected_rows) in [
        (0, 0, vec![1, 3, 5, 7, 10]),
        (64, 64, vec![1, 3, 5, 10]),
        (65, 65, vec![1, 4, 10]),
        (64, 66, vec![1, 3, 4, 5, 10]),
        (349, 349, vec![1, 4, 10]),
    ] {
        assert_exact_vertices(
            &engine,
            engine
                .graph
                .compressed_range_dependents_for_structural_edit(
                    data,
                    crate::engine::graph::StructuralEdit::DeleteRows { start, end },
                    &occupancy,
                ),
            &expected_rows,
        );
    }

    // Column windows likewise pin both inclusive boundaries and exact exclusion
    // immediately outside the referenced interval.
    for (start, end, expected_rows) in [
        (0, 0, vec![2]),
        (1, 1, vec![1, 2, 3, 4]),
        (25, 25, vec![2, 5, 6, 7]),
        (33, 39, vec![2, 10]),
        (51, 51, vec![2]),
    ] {
        assert_exact_vertices(
            &engine,
            engine
                .graph
                .compressed_range_dependents_for_structural_edit(
                    data,
                    crate::engine::graph::StructuralEdit::DeleteColumns { start, end },
                    &occupancy,
                ),
            &expected_rows,
        );
    }
}

#[test]
fn half_open_upper_boundary_delete_recomputes_against_fresh_oracle() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=200 {
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    engine
        .set_cell_formula("Out", 1, 1, parse("=SUM(Data!B:B65)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Out", 1, 1),
        Some(LiteralValue::Number(2_145.0))
    );

    engine.delete_rows("Data", 65, 1).unwrap();
    engine.evaluate_all().unwrap();
    engine
        .set_cell_formula("Out", 1, 2, parse("=SUM(Data!B:B65)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    // The fully bounded analogue is dirtied by ReferenceAdjuster, masking this
    // boundary. This half-open shape leaves its AST unchanged, so only the query
    // pins the inclusive range_end == deleted-start overlap.
    assert_eq!(
        engine.get_cell_value("Out", 1, 1),
        Some(LiteralValue::Number(2_146.0))
    );
    assert_eq!(
        engine.get_cell_value("Out", 1, 1),
        engine.get_cell_value("Out", 1, 2)
    );
}

#[test]
fn delete_rows_marks_open_range_reader_dirty_before_recalculation() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=200 {
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    engine
        .set_cell_formula("Out", 1, 1, parse("=SUM(Data!$B:$B)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    engine.delete_rows("Data", 65, 1).unwrap();

    let inspected = engine
        .inspect_cell(
            &formualizer_common::CellAddress::new("Out", 1, 1).unwrap(),
            &SnapshotOptions::default(),
        )
        .unwrap()
        .cell;
    assert_eq!(inspected.value, Some(LiteralValue::Number(20_100.0)));
    assert_eq!(inspected.staleness, Staleness::Dirty);
}

#[test]
fn unrelated_sheet_deletes_do_not_recompute_open_range_readers_on_either_axis() {
    let row_calls = Arc::new(AtomicUsize::new(0));
    let col_calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new()
        .with_function(Arc::new(CountFn {
            name: "ISSUE306_OTHER_SHEET_ROW",
            calls: Arc::clone(&row_calls),
        }))
        .with_function(Arc::new(CountFn {
            name: "ISSUE306_OTHER_SHEET_COL",
            calls: Arc::clone(&col_calls),
        }));
    let mut engine = Engine::new(workbook, EvalConfig::default().with_parallel(false));
    for index in 1..=100 {
        for sheet in ["Data", "Other"] {
            engine
                .set_cell_value(sheet, index, 2, LiteralValue::Number(f64::from(index)))
                .unwrap();
            engine
                .set_cell_value(sheet, 2, index, LiteralValue::Number(f64::from(index)))
                .unwrap();
        }
    }
    engine
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=ISSUE306_OTHER_SHEET_ROW()+SUM(Data!$B:$B)").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Out",
            2,
            1,
            parse("=ISSUE306_OTHER_SHEET_COL()+SUM(Data!$2:$2)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(row_calls.load(Ordering::SeqCst), 1);
    assert_eq!(col_calls.load(Ordering::SeqCst), 1);

    engine.delete_rows("Other", 60, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(row_calls.load(Ordering::SeqCst), 1);
    assert_eq!(col_calls.load(Ordering::SeqCst), 1);

    engine.delete_columns("Other", 60, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(row_calls.load(Ordering::SeqCst), 1);
    assert_eq!(col_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn insert_rows_dirties_match_over_whole_column() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=200 {
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row * 10)))
            .unwrap();
    }
    engine
        .set_cell_formula("Out", 1, 1, parse("=MATCH(500,Data!$B:$B,0)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.insert_rows("Data", 2, 1).unwrap();
    engine.evaluate_all().unwrap();

    let inspected = engine
        .inspect_cell(
            &formualizer_common::CellAddress::new("Out", 1, 1).unwrap(),
            &SnapshotOptions::default(),
        )
        .unwrap()
        .cell;
    assert_eq!(inspected.staleness, Staleness::Current);
    assert_eq!(inspected.value, Some(LiteralValue::Number(51.0)));
}

#[test]
fn insert_rows_dirties_index_over_whole_column() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=200 {
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row * 10)))
            .unwrap();
    }
    engine
        .set_cell_formula("Out", 1, 1, parse("=INDEX(Data!$B:$B,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.insert_rows("Data", 2, 1).unwrap();
    engine.evaluate_all().unwrap();

    let inspected = engine
        .inspect_cell(
            &formualizer_common::CellAddress::new("Out", 1, 1).unwrap(),
            &SnapshotOptions::default(),
        )
        .unwrap()
        .cell;
    assert_eq!(inspected.staleness, Staleness::Current);
    assert_eq!(inspected.value, Some(LiteralValue::Number(60.0)));
}

#[test]
fn insert_columns_dirties_index_over_whole_row() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for col in 1..=200 {
        engine
            .set_cell_value("Data", 2, col, LiteralValue::Number(f64::from(200 + col)))
            .unwrap();
    }
    engine
        .set_cell_formula("Out", 1, 1, parse("=INDEX(Data!$2:$2,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.insert_columns("Data", 3, 1).unwrap();
    engine.evaluate_all().unwrap();

    let inspected = engine
        .inspect_cell(
            &formualizer_common::CellAddress::new("Out", 1, 1).unwrap(),
            &SnapshotOptions::default(),
        )
        .unwrap()
        .cell;
    assert_eq!(inspected.staleness, Staleness::Current);
    assert_eq!(inspected.value, Some(LiteralValue::Number(206.0)));
}
