use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::arrow_store::OverlayValue;
use crate::engine::{Engine, EvalConfig};
use crate::function::{FnCaps, Function};
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_parse::parse;

fn number(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> f64 {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(value)) => value,
        other => panic!("expected number at {sheet}!R{row}C{col}, got {other:?}"),
    }
}

fn formula_is_compressed(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> bool {
    let reference = engine.graph.make_cell_ref(sheet, row, col);
    let vertex = engine
        .graph
        .get_vertex_id_for_address(&reference)
        .expect("formula vertex");
    engine
        .graph
        .get_range_dependencies(*vertex)
        .is_some_and(|ranges| !ranges.is_empty())
}

#[test]
fn issue_313_row_insert_match_index_and_sum_equal_fresh_whole_column_oracles() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=200 {
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row * 10)))
            .unwrap();
    }
    for (row, formula) in [
        (1, "=MATCH(500,Data!$B:$B,0)"),
        (2, "=INDEX(Data!$B:$B,7)"),
        (3, "=SUM(Data!$B:$B)"),
    ] {
        engine
            .set_cell_formula("Out", row, 1, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));
    assert!(formula_is_compressed(&engine, "Out", 2, 1));
    assert!(formula_is_compressed(&engine, "Out", 3, 1));

    engine.insert_rows("Data", 2, 1).unwrap();
    engine.evaluate_all().unwrap();
    for (row, formula) in [
        (1, "=MATCH(500,Data!$B:$B,0)"),
        (2, "=INDEX(Data!$B:$B,7)"),
        (3, "=SUM(Data!$B:$B)"),
    ] {
        engine
            .set_cell_formula("Out", row, 2, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    assert_eq!(
        (
            number(&engine, "Out", 1, 1),
            number(&engine, "Out", 2, 1),
            number(&engine, "Out", 3, 1)
        ),
        (51.0, 60.0, 201_000.0)
    );
    for row in 1..=3 {
        assert_eq!(
            number(&engine, "Out", row, 1),
            number(&engine, "Out", row, 2)
        );
    }
}

#[test]
fn issue_313_column_insert_match_index_and_sum_equal_fresh_whole_row_oracles() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for col in 1..=200 {
        engine
            .set_cell_value("Data", 2, col, LiteralValue::Number(f64::from(200 + col)))
            .unwrap();
    }
    for (row, formula) in [
        (1, "=MATCH(207,Data!$2:$2,0)"),
        (2, "=INDEX(Data!$2:$2,7)"),
        (3, "=SUM(Data!$2:$2)"),
    ] {
        engine
            .set_cell_formula("Out", row, 1, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));

    engine.insert_columns("Data", 3, 1).unwrap();
    engine.evaluate_all().unwrap();
    for (row, formula) in [
        (1, "=MATCH(207,Data!$2:$2,0)"),
        (2, "=INDEX(Data!$2:$2,7)"),
        (3, "=SUM(Data!$2:$2)"),
    ] {
        engine
            .set_cell_formula("Out", row, 2, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    assert_eq!(
        (number(&engine, "Out", 1, 1), number(&engine, "Out", 2, 1)),
        (8.0, 206.0)
    );
    for row in 1..=3 {
        assert_eq!(
            number(&engine, "Out", row, 1),
            number(&engine, "Out", row, 2)
        );
    }
}

#[test]
fn issue_313_bounded_ranges_above_expansion_limit_recompute_on_both_insert_axes() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default()
            .with_parallel(false)
            .with_range_expansion_limit(64),
    );
    for index in 1..=200 {
        engine
            .set_cell_value("Data", index, 2, LiteralValue::Number(f64::from(index)))
            .unwrap();
        engine
            .set_cell_value(
                "Data",
                2,
                index,
                LiteralValue::Number(f64::from(1_000 + index)),
            )
            .unwrap();
    }
    engine
        .set_cell_formula("Out", 1, 1, parse("=INDEX(Data!B1:B200,7)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Out", 2, 1, parse("=INDEX(Data!A2:GR2,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));
    assert!(formula_is_compressed(&engine, "Out", 2, 1));

    engine.insert_rows("Data", 2, 1).unwrap();
    engine.insert_columns("Data", 3, 1).unwrap();
    engine.evaluate_all().unwrap();
    engine
        .set_cell_formula("Out", 1, 2, parse("=INDEX(Data!B1:B201,7)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Out", 2, 2, parse("=INDEX(Data!A3:GS3,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, "Out", 1, 1), number(&engine, "Out", 1, 2));
    assert_eq!(number(&engine, "Out", 2, 1), number(&engine, "Out", 2, 2));
}

fn assert_arrow_only_mode_dirties(sparse: bool) {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    {
        let mut ingest = engine.begin_bulk_ingest_arrow();
        ingest.add_sheet("Data", 2, if sparse { 4 } else { 100 });
        let rows = if sparse { 1 } else { 100 };
        for _ in 0..rows {
            ingest
                .append_row("Data", &[LiteralValue::Empty, LiteralValue::Empty])
                .unwrap();
        }
        ingest.finish().unwrap();
    }
    if sparse {
        engine
            .sheet_store_mut()
            .sheet_mut("Data")
            .unwrap()
            .set_sparse_overlay_value(10, 1, OverlayValue::Number(100.0));
    } else {
        let mut update = engine.begin_bulk_update_arrow();
        update.update_cell("Data", 11, 2, LiteralValue::Number(100.0));
        update.finish().unwrap();
    }
    let data = engine.sheet_id("Data").unwrap();
    assert!(
        !engine.sheet_store().sheet("Data").unwrap().shape()[1].has_num,
        "fixture must reproduce shape() blindness"
    );
    engine
        .set_cell_formula("Out", 1, 1, parse("=SUM(Data!$B:$B)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(number(&engine, "Out", 1, 1), 100.0);
    assert_eq!(
        engine
            .graph
            .grid_vertices_in_sheet(data)
            .filter(|(_, coord)| coord.col() == 1)
            .count(),
        0,
        "the occupied Arrow column must have zero graph vertices"
    );

    engine.delete_rows("Data", 11, 1).unwrap();
    engine.evaluate_all().unwrap();
    engine
        .set_cell_formula("Out", 1, 2, parse("=SUM(Data!$B:$B)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, "Out", 1, 1), 0.0);
    assert_eq!(number(&engine, "Out", 1, 1), number(&engine, "Out", 1, 2));
}

#[test]
fn sparse_loaded_arrow_value_with_zero_graph_vertices_counts_as_occupied() {
    assert_arrow_only_mode_dirties(true);
}

#[test]
fn overlay_written_arrow_value_with_zero_graph_vertices_counts_as_occupied() {
    assert_arrow_only_mode_dirties(false);
}

#[derive(Debug)]
struct CountFn(Arc<AtomicUsize>);

impl Function for CountFn {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "ISSUE314_COUNT"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(CalcValue::Scalar(LiteralValue::Number(0.0)))
    }
}

#[test]
fn issue_314_empty_column_readers_do_no_work_on_insert_or_delete_and_keep_equal_values() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new().with_function(Arc::new(CountFn(Arc::clone(&calls))));
    let mut engine = Engine::new(workbook, EvalConfig::default().with_parallel(false));
    for row in 1..=2_000 {
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    for (row, formula) in [
        (1, "=ISSUE314_COUNT()+SUM(Data!$Z:$Z)"),
        (2, "=ISSUE314_COUNT()+SUM(Data!Z1:Z100)"),
    ] {
        engine
            .set_cell_formula("Out", row, 1, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));
    assert!(formula_is_compressed(&engine, "Out", 2, 1));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let before = [number(&engine, "Out", 1, 1), number(&engine, "Out", 2, 1)];

    engine.insert_rows("Data", 500, 1).unwrap();
    let null_start = Instant::now();
    engine.evaluate_all().unwrap();
    let insert_eval = null_start.elapsed();
    engine.delete_rows("Data", 500, 1).unwrap();
    let delete_start = Instant::now();
    engine.evaluate_all().unwrap();
    let delete_eval = delete_start.elapsed();

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "empty-column readers must not be evaluated"
    );
    assert_eq!(
        before,
        [number(&engine, "Out", 1, 1), number(&engine, "Out", 2, 1)]
    );
    eprintln!(
        "issue314-work-equality before={before:?} after={:?}; insert-eval={insert_eval:?}; delete-eval={delete_eval:?}",
        [number(&engine, "Out", 1, 1), number(&engine, "Out", 2, 1)]
    );
}

#[test]
fn bounded_range_relocated_wholesale_is_still_recomputed_by_formula_adjustment() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new().with_function(Arc::new(CountFn(Arc::clone(&calls))));
    let mut engine = Engine::new(workbook, EvalConfig::default().with_parallel(false));
    engine
        .set_cell_value("Data", 100, 2, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=ISSUE314_COUNT()+SUM(Data!B100:B200)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    engine.insert_rows("Data", 50, 1).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the reference adjuster's blanket dirty mark remains correctness-first"
    );
}

// Mutation kill matrix:
// P_occupancy=occupied -> issue_314_empty_column_readers... fails its exact call count.
// P_occupancy=empty -> issue #306 tests and both Arrow-only conservatism tests fail.
// P_axis(insert)=false -> both issue_313 whole-axis oracle tests fail.
// P_axis(insert) widened by dropping range_start < before -> insert_axis_excludes_range_wholly_below_insertion_point fails.

fn column_label(mut one_based: usize) -> String {
    let mut label = String::new();
    while one_based > 0 {
        let digit = (one_based - 1) % 26;
        label.push((b'A' + digit as u8) as char);
        one_based = (one_based - 1) / 26;
    }
    label.chars().rev().collect()
}

#[test]
fn issue_314_thousand_empty_column_readers_delete_eval_matches_null_envelope_and_values() {
    const READERS: u32 = 1_000;
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    for row in 1..=2_000 {
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(f64::from(row)))
            .unwrap();
    }
    for reader in 0..READERS {
        let column = column_label(reader as usize + 3);
        let formula = format!("=SUM(Data!${column}:${column})");
        engine
            .set_cell_formula("Out", reader + 1, 1, parse(&formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));
    let before = [
        number(&engine, "Out", 1, 1),
        number(&engine, "Out", READERS, 1),
    ];
    assert_eq!(before, [0.0, 0.0]);

    let mut null_samples = Vec::new();
    for _ in 0..9 {
        let start = Instant::now();
        engine.evaluate_all().unwrap();
        null_samples.push(start.elapsed());
    }
    null_samples.sort_unstable();
    let null_median = null_samples[null_samples.len() / 2];

    engine.delete_rows("Data", 500, 1).unwrap();
    let start = Instant::now();
    engine.evaluate_all().unwrap();
    let delete_eval = start.elapsed();
    let after = [
        number(&engine, "Out", 1, 1),
        number(&engine, "Out", READERS, 1),
    ];

    assert_eq!(before, after, "empty-column values must be work-equivalent");
    eprintln!(
        "issue314-benchmark readers={READERS} null-median={null_median:?} delete-eval={delete_eval:?} work-equality={before:?}"
    );
}

#[test]
fn top_row_insert_recomputes_whole_column_match_and_index_against_fresh_oracles() {
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
    engine
        .set_cell_formula("Out", 2, 1, parse("=INDEX(Data!$B:$B,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));
    assert!(formula_is_compressed(&engine, "Out", 2, 1));

    engine.insert_rows("Data", 1, 1).unwrap();
    engine.evaluate_all().unwrap();
    engine
        .set_cell_formula("Out", 1, 2, parse("=MATCH(500,Data!$B:$B,0)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Out", 2, 2, parse("=INDEX(Data!$B:$B,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, "Out", 1, 1), 51.0);
    assert_eq!(number(&engine, "Out", 2, 1), 60.0);
    assert_eq!(number(&engine, "Out", 1, 1), number(&engine, "Out", 1, 2));
    assert_eq!(number(&engine, "Out", 2, 1), number(&engine, "Out", 2, 2));
}

#[test]
fn top_column_insert_recomputes_whole_row_match_and_index_against_fresh_oracles() {
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
        .set_cell_formula("Out", 1, 1, parse("=MATCH(207,Data!$2:$2,0)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Out", 2, 1, parse("=INDEX(Data!$2:$2,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));

    engine.insert_columns("Data", 1, 1).unwrap();
    engine.evaluate_all().unwrap();
    engine
        .set_cell_formula("Out", 1, 2, parse("=MATCH(207,Data!$2:$2,0)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Out", 2, 2, parse("=INDEX(Data!$2:$2,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(number(&engine, "Out", 1, 1), 8.0);
    assert_eq!(number(&engine, "Out", 2, 1), 206.0);
    assert_eq!(number(&engine, "Out", 1, 1), number(&engine, "Out", 1, 2));
    assert_eq!(number(&engine, "Out", 2, 1), number(&engine, "Out", 2, 2));
}

#[test]
fn bounded_range_starting_at_top_is_not_selected_when_insert_relocates_it_wholesale() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default()
            .with_parallel(false)
            .with_range_expansion_limit(64),
    );
    engine
        .set_cell_value("Data", 7, 2, LiteralValue::Number(70.0))
        .unwrap();
    engine
        .set_cell_formula("Out", 1, 1, parse("=INDEX(Data!B1:B200,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));
    let data = engine.sheet_id("Data").unwrap();
    let occupancy = engine.graph.structural_occupancy(data);
    assert!(
        engine
            .graph
            .compressed_range_dependents_for_structural_edit(
                data,
                crate::engine::graph::StructuralEdit::InsertRows { before: 0 },
                &occupancy,
            )
            .is_empty()
    );

    engine.insert_rows("Data", 1, 1).unwrap();
    engine.evaluate_all().unwrap();
    engine
        .set_cell_formula("Out", 1, 2, parse("=INDEX(Data!B2:B201,7)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(number(&engine, "Out", 1, 1), 70.0);
    assert_eq!(number(&engine, "Out", 1, 1), number(&engine, "Out", 1, 2));
}

#[test]
fn graph_index_occupancy_alone_dirties_a_compressed_reader() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new().with_function(Arc::new(CountFn(Arc::clone(&calls))));
    let mut engine = Engine::new(workbook, EvalConfig::default().with_parallel(false));
    engine
        .graph
        .set_cell_value("Data", 10, 2, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=ISSUE314_COUNT()+SUM(Data!$B:$B)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let data = engine.sheet_id("Data").unwrap();
    let occupancy = engine.graph.structural_occupancy(data);
    crate::engine::graph::editor::VertexEditor::new(&mut engine.graph)
        .with_structural_occupancy(occupancy)
        .delete_rows(data, 9, 1)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn base_chunk_metadata_occupancy_alone_dirties_a_compressed_reader() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new().with_function(Arc::new(CountFn(Arc::clone(&calls))));
    let mut engine = Engine::new(workbook, EvalConfig::default().with_parallel(false));
    {
        let mut ingest = engine.begin_bulk_ingest_arrow();
        ingest.add_sheet("Data", 2, 100);
        for row in 0..100 {
            ingest
                .append_row(
                    "Data",
                    &[
                        LiteralValue::Empty,
                        if row == 10 {
                            LiteralValue::Number(100.0)
                        } else {
                            LiteralValue::Empty
                        },
                    ],
                )
                .unwrap();
        }
        ingest.finish().unwrap();
    }
    let data = engine.sheet_id("Data").unwrap();
    assert_eq!(engine.graph.grid_vertices_in_sheet(data).count(), 0);
    assert!(engine.sheet_store().sheet("Data").unwrap().shape()[1].has_num);
    engine
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=ISSUE314_COUNT()+SUM(Data!$B:$B)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    engine.delete_rows("Data", 11, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(number(&engine, "Out", 1, 1), 0.0);
}

#[test]
fn direct_vertex_editor_without_occupancy_degrades_to_conservative() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new().with_function(Arc::new(CountFn(Arc::clone(&calls))));
    let mut engine = Engine::new(workbook, EvalConfig::default().with_parallel(false));
    let data = engine.graph.sheet_id_mut("Data");
    engine
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=ISSUE314_COUNT()+SUM(Data!$Z:$Z)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    crate::engine::graph::editor::VertexEditor::new(&mut engine.graph)
        .insert_rows(data, 10, 1)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn missing_arrow_sheet_degrades_to_occupied_on_public_row_edit() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new().with_function(Arc::new(CountFn(Arc::clone(&calls))));
    let mut engine = Engine::new(workbook, EvalConfig::default().with_parallel(false));
    engine.graph.sheet_id_mut("Data");
    engine
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=ISSUE314_COUNT()+SUM(Data!$Z:$Z)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(engine.sheet_store().sheet("Data").is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    engine.delete_rows("Data", 11, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn insert_at_compressed_range_end_selects_and_recomputes_via_dirty_probe() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new().with_function(Arc::new(CountFn(Arc::clone(&calls))));
    let mut engine = Engine::new(
        workbook,
        EvalConfig::default()
            .with_parallel(false)
            .with_range_expansion_limit(64),
    );
    engine
        .set_cell_value("Data", 100, 2, LiteralValue::Number(100.0))
        .unwrap();
    engine
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=ISSUE314_COUNT()+INDEX(Data!B1:B100,100)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let data = engine.sheet_id("Data").unwrap();
    let selected = engine
        .graph
        .compressed_range_dependents_for_structural_edit(
            data,
            crate::engine::graph::StructuralEdit::InsertRows { before: 99 },
            &crate::engine::graph::StructuralOccupancy::conservative(),
        );
    assert_eq!(selected.len(), 1);
    engine.graph.mark_dirty_many(&selected);
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn no_reader_dense_sheet_structural_edit_cost_is_constant_in_release() {
    if cfg!(debug_assertions) {
        return;
    }
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    );
    let row = vec![LiteralValue::Number(1.0); 50];
    {
        let mut ingest = engine.begin_bulk_ingest_arrow();
        ingest.add_sheet("Data", 50, 100_000);
        for _ in 0..100_000 {
            ingest.append_row("Data", &row).unwrap();
        }
        ingest.finish().unwrap();
    }
    let start = Instant::now();
    engine.insert_rows("Data", 50_000, 1).unwrap();
    let insert = start.elapsed();
    let start = Instant::now();
    engine.delete_rows("Data", 50_000, 1).unwrap();
    let delete = start.elapsed();
    eprintln!("structural-cost-noreaders rows=100000 cols=50 insert={insert:?} delete={delete:?}");
    assert!(insert.as_millis() < 5, "insert regressed to {insert:?}");
    assert!(delete.as_millis() < 5, "delete regressed to {delete:?}");
}

#[test]
fn sparse_chunk_metadata_occupancy_alone_dirties_a_compressed_reader() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workbook = TestWorkbook::new().with_function(Arc::new(CountFn(Arc::clone(&calls))));
    let mut engine = Engine::new(workbook, EvalConfig::default().with_parallel(false));
    {
        let mut ingest = engine.begin_bulk_ingest_arrow();
        ingest.add_sheet("Data", 2, 4);
        ingest
            .append_row("Data", &[LiteralValue::Empty, LiteralValue::Empty])
            .unwrap();
        ingest.finish().unwrap();
    }
    {
        let sheet = engine.sheet_store_mut().sheet_mut("Data").unwrap();
        sheet.set_sparse_overlay_value(10, 1, OverlayValue::Empty);
        let chunk = sheet.columns[1]
            .sparse_chunks
            .values_mut()
            .next()
            .expect("sparse chunk");
        chunk.overlay = crate::arrow_store::Overlay::new();
        chunk.meta.non_null_num = 1;
    }
    engine
        .set_cell_formula(
            "Out",
            1,
            1,
            parse("=ISSUE314_COUNT()+SUM(Data!$B:$B)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    engine.delete_rows("Data", 11, 1).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn insert_selection_uses_pre_edit_coordinates_before_any_shift() {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default()
            .with_parallel(false)
            .with_range_expansion_limit(64),
    );
    engine
        .set_cell_value("Data", 100, 2, LiteralValue::Number(100.0))
        .unwrap();
    engine
        .set_cell_formula("Out", 1, 1, parse("=SUM(Data!B1:B100)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert!(formula_is_compressed(&engine, "Out", 1, 1));
    let data = engine.sheet_id("Data").unwrap();
    let summary = crate::engine::graph::editor::VertexEditor::new(&mut engine.graph)
        .with_structural_occupancy(crate::engine::graph::StructuralOccupancy::conservative())
        .insert_rows(data, 50, 100)
        .unwrap();
    assert_eq!(
        summary.structural_dependents_dirtied.len(),
        1,
        "selection must use before=50, not the post-edit before+count=150"
    );
}
