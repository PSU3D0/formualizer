use std::sync::Arc;

use crate::SheetId;
use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

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

fn authoritative_engine() -> Engine<TestWorkbook> {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    Engine::new(TestWorkbook::default(), cfg)
}

fn build_three_formula_column_family(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
        formulas.push(record(&mut engine, row, 3, &format!("=A{row}*2")));
        formulas.push(record(&mut engine, row, 4, &format!("=A{row}-3")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 3);
    engine.evaluate_all().unwrap();
    engine
}

fn build_single_formula_column_family(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    add_single_formula_column_family(&mut engine, "Sheet1", rows);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    engine
}

fn add_single_formula_column_family(engine: &mut Engine<TestWorkbook>, sheet: &str, rows: u32) {
    if engine.sheet_id(sheet).is_none() {
        engine.add_sheet(sheet).unwrap();
    }
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value(sheet, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(engine, row, 2, &format!("=A{row}*2")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(sheet, formulas)])
        .unwrap();
}

fn only_active_span_is_constant(engine: &Engine<TestWorkbook>) -> bool {
    let spans = engine
        .graph
        .formula_authority()
        .plane
        .spans
        .active_spans()
        .collect::<Vec<_>>();
    assert_eq!(spans.len(), 1);
    spans[0].is_constant_result
}

fn build_cross_sheet_span_engine(rows: u32) -> (Engine<TestWorkbook>, SheetId, SheetId) {
    let mut engine = authoritative_engine();
    let data_a_sheet_id = engine.add_sheet("DataA").unwrap();
    let data_b_sheet_id = engine.add_sheet("DataB").unwrap();
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value("DataA", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("DataB", row, 1, LiteralValue::Number(row as f64 * 2.0))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            1,
            &format!("=DataA!A{row}+DataB!A{row}"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    let stats = engine.baseline_stats();
    assert_eq!(stats.graph_formula_vertex_count, 0);
    assert_eq!(stats.formula_plane_active_span_count, 1);
    assert_eq!(stats.formula_plane_consumer_read_entries, 2);
    (engine, data_a_sheet_id, data_b_sheet_id)
}

#[test]
fn formula_plane_authoritative_whole_column_sum_promotes_and_recalculates() {
    let rows = 200;
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, "=SUM($A:$A)"));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert!(only_active_span_is_constant(&engine));

    engine.evaluate_all().unwrap();
    let initial_sum = (rows * (rows + 1) / 2) as f64;
    for row in [1, 50, 200] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(initial_sum))
        );
    }

    engine
        .set_cell_value("Sheet1", 50, 1, LiteralValue::Number(1_000.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    let edited_sum = initial_sum - 50.0 + 1_000.0;
    for row in [1, 50, 200] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(edited_sum))
        );
    }
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
}

#[test]
fn formula_plane_authoritative_whole_column_sum_with_relative_cell_promotes_and_recalculates() {
    let rows = 200;
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 3, &format!("=SUM($A:$A)-A{row}")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert!(!only_active_span_is_constant(&engine));

    engine.evaluate_all().unwrap();
    let initial_sum = (rows * (rows + 1) / 2) as f64;
    for row in [1, 50, 200] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 3),
            Some(LiteralValue::Number(initial_sum - row as f64))
        );
    }

    engine
        .set_cell_value("Sheet1", 50, 1, LiteralValue::Number(1_000.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    let edited_sum = initial_sum - 50.0 + 1_000.0;
    for row in [1, 50, 200] {
        let row_value = if row == 50 { 1_000.0 } else { row as f64 };
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 3),
            Some(LiteralValue::Number(edited_sum - row_value))
        );
    }
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
}

#[test]
fn formula_plane_authoritative_cross_sheet_whole_column_sum_recalculates_on_data_edit() {
    let rows = 200;
    let mut engine = authoritative_engine();
    engine.add_sheet("DataA").unwrap();
    let mut formulas = Vec::new();
    for row in 1..=rows {
        engine
            .set_cell_value("DataA", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, "=SUM(DataA!$A:$A)"));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert!(only_active_span_is_constant(&engine));

    engine.evaluate_all().unwrap();
    let initial_sum = (rows * (rows + 1) / 2) as f64;
    assert_eq!(
        engine.get_cell_value("Sheet1", 100, 2),
        Some(LiteralValue::Number(initial_sum))
    );

    engine
        .set_cell_value("DataA", 75, 1, LiteralValue::Number(2_000.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    let edited_sum = initial_sum - 75.0 + 2_000.0;
    for row in [1, 100, 200] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(edited_sum))
        );
    }
}

#[test]
fn formula_plane_authoritative_sheet_rename_is_metadata_only_for_cross_sheet_span() {
    let (mut engine, data_a_sheet_id, _) = build_cross_sheet_span_engine(100);
    engine.evaluate_all().unwrap();

    let sample_rows = [1, 50, 100];
    let before: Vec<_> = sample_rows
        .iter()
        .map(|row| engine.get_cell_value("Sheet1", *row, 1))
        .collect();

    engine.rename_sheet(data_a_sheet_id, "DataAA").unwrap();
    let data_aa_sheet_id = engine.sheet_id("DataAA").unwrap();
    assert_eq!(data_aa_sheet_id, data_a_sheet_id);
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 0);
    for (row, value) in sample_rows.iter().zip(before.iter()) {
        assert_eq!(engine.get_cell_value("Sheet1", *row, 1), value.clone());
    }
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    engine.rename_sheet(data_aa_sheet_id, "DataA").unwrap();
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 0);
    for (row, value) in sample_rows.iter().zip(before.iter()) {
        assert_eq!(engine.get_cell_value("Sheet1", *row, 1), value.clone());
    }
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
}

#[test]
fn formula_plane_authoritative_value_edit_after_sheet_rename_dirties_bounded_span_work() {
    let (mut engine, data_a_sheet_id, _) = build_cross_sheet_span_engine(100);
    engine.evaluate_all().unwrap();

    let row_49_before = engine.get_cell_value("Sheet1", 49, 1);
    let row_51_before = engine.get_cell_value("Sheet1", 51, 1);

    engine.rename_sheet(data_a_sheet_id, "DataAA").unwrap();
    assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 0);
    let data_aa_sheet_id = engine.sheet_id("DataAA").unwrap();
    engine.rename_sheet(data_aa_sheet_id, "DataA").unwrap();
    assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 0);

    engine
        .set_cell_value("DataA", 50, 1, LiteralValue::Number(10_000.0))
        .unwrap();
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 1),
        Some(LiteralValue::Number(10_100.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 49, 1), row_49_before);
    assert_eq!(engine.get_cell_value("Sheet1", 51, 1), row_51_before);
}

#[test]
fn formula_plane_authoritative_sheet_rename_preserves_sheet_id_read_summaries() {
    let (mut engine, data_a_sheet_id, _) = build_cross_sheet_span_engine(100);
    engine.evaluate_all().unwrap();

    let row_9_before = engine.get_cell_value("Sheet1", 9, 1);
    let row_11_before = engine.get_cell_value("Sheet1", 11, 1);

    assert_eq!(
        engine.baseline_stats().formula_plane_consumer_read_entries,
        2
    );
    engine.rename_sheet(data_a_sheet_id, "DataAA").unwrap();
    assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 0);
    assert_eq!(
        engine.baseline_stats().formula_plane_consumer_read_entries,
        2
    );

    engine
        .set_cell_value("DataAA", 10, 1, LiteralValue::Number(999.0))
        .unwrap();
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.computed_vertices, 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 1),
        Some(LiteralValue::Number(1_019.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 9, 1), row_9_before);
    assert_eq!(engine.get_cell_value("Sheet1", 11, 1), row_11_before);
    assert_eq!(
        engine.baseline_stats().formula_plane_consumer_read_entries,
        2
    );
}

#[test]
fn formula_plane_authoritative_repeated_column_insert_after_demotion_15k_vertices_stays_linear() {
    let rows = 5_000;
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity(rows as usize * 3);
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
        formulas.push(record(&mut engine, row, 3, &format!("=A{row}*2")));
        formulas.push(record(&mut engine, row, 4, &format!("=A{row}-3")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 3);

    engine.evaluate_all().unwrap();

    let expected_columns_by_edit = [
        [(2, 0), (4, 1), (5, 2)],
        [(3, 0), (5, 1), (6, 2)],
        [(3, 0), (6, 1), (7, 2)],
        [(4, 0), (7, 1), (8, 2)],
        [(5, 0), (8, 1), (9, 2)],
    ];
    let value_column_by_edit = [1, 1, 1, 2, 2];
    let insert_sequence = [3, 2, 5, 1, 4];
    let rows_to_check = [1, 2_500, 5_000];

    for (edit_idx, before) in insert_sequence.into_iter().enumerate() {
        let started = std::time::Instant::now();
        engine.insert_columns("Sheet1", before, 1).unwrap();
        let elapsed = started.elapsed();

        if !cfg!(debug_assertions) {
            let limit = if edit_idx == 0 {
                std::time::Duration::from_secs(10)
            } else {
                std::time::Duration::from_secs(1)
            };
            assert!(
                elapsed < limit,
                "edit_{edit_idx} took {elapsed:?}, expected below {limit:?}"
            );
            if edit_idx == 3 {
                assert!(
                    elapsed < std::time::Duration::from_secs(1),
                    "insert-before-column-1 edit took {elapsed:?}"
                );
            }
        }

        engine.evaluate_all().unwrap();
        for row in rows_to_check {
            let row_f64 = row as f64;
            assert_eq!(
                engine.get_cell_value("Sheet1", row, value_column_by_edit[edit_idx]),
                Some(LiteralValue::Number(row_f64))
            );
            for (col, formula_kind) in expected_columns_by_edit[edit_idx] {
                let expected = match formula_kind {
                    0 => row_f64 + 1.0,
                    1 => row_f64 * 2.0,
                    2 => row_f64 - 3.0,
                    _ => unreachable!(),
                };
                assert_eq!(
                    engine.get_cell_value("Sheet1", row, col),
                    Some(LiteralValue::Number(expected)),
                    "edit_idx={edit_idx} before={before} row={row} col={col} kind={formula_kind}"
                );
            }
        }
    }
}

#[test]
fn formula_plane_authoritative_column_insert_shifts_span_outputs_correctly() {
    let mut engine = build_three_formula_column_family(100);

    engine.insert_columns("Sheet1", 3, 1).unwrap();
    // Span shifting preserves all three column-family spans: col B stays put,
    // while col C and col D shift right without materializing per-cell formulas.
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 3);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 1),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 5, 3), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 4),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 5),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn formula_plane_authoritative_column_delete_shifts_span_outputs_correctly() {
    let mut engine = build_three_formula_column_family(100);

    engine.delete_columns("Sheet1", 3, 1).unwrap();
    // Span shifting preserves col B and shifts col D into col C. The deleted
    // col C span is removed without materializing per-cell formulas.
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 1),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 3),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 5, 4), None);
}

#[test]
fn formula_plane_authoritative_row_insert_on_cross_sheet_read_sheet_demotes_span() {
    let mut engine = authoritative_engine();
    engine.add_sheet("Data").unwrap();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 1, &format!("=Data!A{row}*2")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    engine.insert_rows("Data", 3, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn formula_plane_authoritative_range_precedent_dirty_propagation_through_structural_op() {
    let mut engine = authoritative_engine();
    engine.add_sheet("Data").unwrap();
    for row in 1..=100 {
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=SUM(Data!$A$1:$A$100)+A{row}"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    engine.insert_rows("Data", 50, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
}

#[test]
fn formula_plane_authoritative_row_insert_shifts_span_outputs_correctly() {
    let mut engine = build_single_formula_column_family(100);

    engine.insert_rows("Sheet1", 3, 1).unwrap();
    // A mid-domain row insert splits the span at the boundary: the upper half
    // keeps its rows in place, the lower half shifts down. No placement is
    // materialized as a legacy graph formula.
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(4.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 3, 2), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 6, 2),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn formula_plane_authoritative_row_delete_shifts_span_outputs_correctly() {
    let mut engine = build_single_formula_column_family(100);

    engine.delete_rows("Sheet1", 3, 1).unwrap();
    // Row deletes compact a vertical span in place instead of demoting all
    // remaining placements to graph formulas.
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(4.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(8.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 100, 2), None);
}

#[test]
fn formula_plane_row_delete_demotes_unique_literal_bindings_instead_of_miscompacting() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+{row}")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    engine.delete_rows("Sheet1", 3, 1).unwrap();
    // Per-placement literal bindings need their binding-id vector compacted.
    // Until that exists, demote rather than keeping a shifted span with stale
    // ordinal-to-binding mappings.
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(4.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(8.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn formula_plane_column_delete_with_unique_literal_bindings_shifts_without_stale_memoization() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 3, &format!("=A{row}+{row}")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    engine.delete_columns("Sheet1", 2, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(100.0))
    );
}

#[test]
fn formula_plane_adjacent_constant_spans_row_delete_compacts_surviving_rows() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        formulas.push(record(&mut engine, row, 2, "=1+1"));
        formulas.push(record(&mut engine, row, 3, "=1+1"));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine.evaluate_all().unwrap();

    engine.delete_rows("Sheet1", 5, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 99, 3),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 100, 2), None);
}

#[test]
fn formula_plane_adjacent_constant_spans_column_delete_removes_deleted_column_span() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        formulas.push(record(&mut engine, row, 2, "=1+1"));
        formulas.push(record(&mut engine, row, 3, "=1+1"));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine.evaluate_all().unwrap();

    engine.delete_columns("Sheet1", 2, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 5, 3), None);
}

#[test]
fn formula_plane_delete_on_read_range_sheet_straddles_and_demotes() {
    let mut engine = authoritative_engine();
    engine.add_sheet("Data").unwrap();
    for row in 1..=20 {
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }
    let mut formulas = Vec::new();
    for row in 1..=100 {
        formulas.push(record(&mut engine, row, 1, "=SUM(Data!$A$1:$A$10)"));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    engine.delete_rows("Data", 5, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    engine.evaluate_all().unwrap();

    // Issue #168 policy: absolute bounds track structural deletes, so
    // `$A$1:$A$10` contracts to `$A$1:$A$9` when row 5 is deleted. The
    // surviving values in rows 1..=9 are 1,2,3,4,6,7,8,9,10 → 50.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(50.0))
    );
}

#[test]
fn formula_plane_delete_fully_contains_span_removes_it_and_clears_overlays() {
    let mut engine = build_single_formula_column_family(100);

    engine.delete_columns("Sheet1", 2, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 1),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 5, 2), None);
}

#[test]
fn formula_plane_ingest_rejects_unbounded_reference_to_unknown_sheet_without_creating_sheet() {
    let mut engine = authoritative_engine();
    let formula = record(&mut engine, 1, 1, "=SUM(MissingSheet!A:A)");

    let result =
        engine.ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", vec![formula])]);

    assert!(result.is_err());
    assert!(engine.sheet_id("MissingSheet").is_none());
}

#[test]
fn formula_plane_add_sheet_preserves_existing_active_spans() {
    let mut engine = build_single_formula_column_family(100);

    engine.add_sheet("Added").unwrap();

    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(100.0))
    );
}

#[test]
fn formula_plane_remove_unrelated_sheet_preserves_existing_active_spans() {
    let mut engine = authoritative_engine();
    let unrelated = engine.add_sheet("Unrelated").unwrap();
    add_single_formula_column_family(&mut engine, "Sheet1", 100);
    engine.evaluate_all().unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    engine.remove_sheet(unrelated).unwrap();

    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(100.0))
    );
}

#[test]
fn formula_plane_rename_sheet_preserves_existing_active_spans() {
    let mut engine = build_single_formula_column_family(100);
    let sheet = engine.sheet_id("Sheet1").unwrap();

    engine.rename_sheet(sheet, "Renamed").unwrap();

    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Renamed", 50, 2),
        Some(LiteralValue::Number(100.0))
    );
}

#[test]
fn formula_plane_duplicate_sheet_only_demotes_source_sheet_spans() {
    let mut engine = authoritative_engine();
    add_single_formula_column_family(&mut engine, "Sheet1", 100);
    add_single_formula_column_family(&mut engine, "Other", 100);
    engine.evaluate_all().unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);

    engine.duplicate_sheet("Sheet1", "Copy").unwrap();

    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Other", 50, 2),
        Some(LiteralValue::Number(100.0))
    );
    assert_eq!(
        engine.get_cell_value("Copy", 50, 2),
        Some(LiteralValue::Number(100.0))
    );
}

#[test]
fn formula_plane_zero_count_structural_ops_are_noops() {
    let mut engine = build_single_formula_column_family(100);
    engine.evaluate_all().unwrap();
    let before = engine.baseline_stats();
    let topology_before = engine.topology_epoch_for_test();

    engine.insert_rows("Sheet1", 3, 0).unwrap();
    engine.delete_rows("Sheet1", 3, 0).unwrap();
    engine.insert_columns("Sheet1", 2, 0).unwrap();
    engine.delete_columns("Sheet1", 2, 0).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
    assert_eq!(engine.topology_epoch_for_test(), topology_before);
    engine.evaluate_all().unwrap();
    let after = engine.baseline_stats();
    assert_eq!(
        after.formula_plane_mixed_topology_cache_builds,
        before.formula_plane_mixed_topology_cache_builds
    );
    assert!(engine.last_formula_plane_span_eval_report().is_none());

    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(100.0))
    );
}

#[test]
fn formula_plane_origin_shift_with_stationary_value_ref_does_not_memo_broadcast_stale_value() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    engine.insert_columns("Sheet1", 2, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 3),
        Some(LiteralValue::Number(51.0))
    );
}

// ---------------------------------------------------------------------------
// Mid-domain insert span splitting (SpanShiftPlan::Split, conservative v1)
// ---------------------------------------------------------------------------

/// Seed a span with an absolute `$A$1` read directly into the authority
/// plane. Engine batch ingest groups families per column (so it only ever
/// produces RowRun spans); this helper lets structural tests exercise ColRun
/// and Rect domains through the real engine insert path.
pub(super) fn seed_absolute_read_span(
    engine: &mut Engine<TestWorkbook>,
    formula: &str,
    domain: crate::formula_plane::runtime::PlacementDomain,
) {
    use crate::formula_plane::producer::{
        AxisProjection, DirtyProjectionRule, SpanReadDependency, SpanReadSummary,
    };
    use crate::formula_plane::region_index::Region;
    use crate::formula_plane::runtime::{NewFormulaSpan, PlacementDomain, ResultRegion};

    let ast = parse(formula).unwrap();
    let ast_id = engine.intern_formula_ast(&ast);
    let (origin_row, origin_col) = match &domain {
        PlacementDomain::RowRun { row_start, col, .. } => (*row_start + 1, *col + 1),
        PlacementDomain::ColRun { row, col_start, .. } => (*row + 1, *col_start + 1),
        PlacementDomain::Rect {
            row_start,
            col_start,
            ..
        } => (*row_start + 1, *col_start + 1),
    };
    let authority = engine.graph.formula_authority_mut();
    let template_id = authority.plane.intern_template(
        Arc::<str>::from(format!("seeded:{formula}:{domain:?}")),
        ast_id,
        origin_row,
        origin_col,
        Some(Arc::<str>::from(formula)),
    );
    let result_region = Region::from_domain(&domain);
    let summary = SpanReadSummary {
        result_region,
        dependencies: vec![SpanReadDependency {
            read_region: Region::point(domain.sheet_id(), 0, 0),
            projection: DirtyProjectionRule::AffineCell {
                row: AxisProjection::Absolute { index: 0 },
                col: AxisProjection::Absolute { index: 0 },
            },
        }],
    };
    let read_summary_id = authority.plane.insert_span_read_summary(summary);
    authority.plane.insert_span(NewFormulaSpan {
        sheet_id: domain.sheet_id(),
        template_id,
        result_region: ResultRegion::scalar_cells(domain.clone()),
        domain,
        intrinsic_mask_id: None,
        read_summary_id: Some(read_summary_id),
        binding_set_id: None,
        is_constant_result: false,
    });
    authority.rebuild_indexes();
}

/// Every active span must retain a read summary whose `result_region` exactly
/// equals the span's result region: `FormulaAuthority::rebuild_indexes` drops
/// mismatched spans from the consumer read index silently, and the mixed
/// scheduler treats the mismatch as a hard error.
fn assert_span_read_summaries_exact(engine: &Engine<TestWorkbook>) {
    use crate::formula_plane::region_index::Region;
    let plane = &engine.graph.formula_authority().plane;
    for span in plane.spans.active_spans() {
        let result_region = Region::from_domain(span.result_region.domain());
        let summary_id = span
            .read_summary_id
            .expect("split halves must retain read summaries");
        let summary = plane
            .span_read_summaries
            .get(summary_id)
            .expect("read summary id must resolve");
        assert_eq!(
            summary.result_region, result_region,
            "span {:?} read summary result region must match span geometry",
            span.id
        );
    }
}

#[test]
fn formula_plane_row_insert_split_halves_have_exact_read_summaries() {
    let mut engine = build_single_formula_column_family(100);

    engine.insert_rows("Sheet1", 40, 2).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_span_read_summaries_exact(&engine);

    {
        use crate::formula_plane::runtime::PlacementDomain;
        let plane = &engine.graph.formula_authority().plane;
        let mut domains: Vec<PlacementDomain> = plane
            .spans
            .active_spans()
            .map(|span| span.domain.clone())
            .collect();
        domains.sort_by_key(|domain| match domain {
            PlacementDomain::RowRun { row_start, .. } => *row_start,
            _ => u32::MAX,
        });
        // 0-based: upper rows 0..=38 stay; lower rows 39..=99 shift by +2.
        assert_eq!(domains[0], PlacementDomain::row_run(0, 0, 38, 1));
        assert_eq!(domains[1], PlacementDomain::row_run(0, 41, 101, 1));
    }

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 39, 2),
        Some(LiteralValue::Number(78.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 40, 2), None);
    assert_eq!(engine.get_cell_value("Sheet1", 41, 2), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 42, 2),
        Some(LiteralValue::Number(80.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 102, 2),
        Some(LiteralValue::Number(200.0))
    );

    // The rederived summaries must keep driving precedent dirty propagation
    // for both halves after the split.
    engine
        .set_cell_value("Sheet1", 10, 1, LiteralValue::Number(1000.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 50, 1, LiteralValue::Number(2000.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 2),
        Some(LiteralValue::Number(2000.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(4000.0))
    );
}

#[test]
fn formula_plane_column_insert_splits_col_run_span_with_stationary_reads() {
    use crate::formula_plane::runtime::PlacementDomain;
    let mut engine = authoritative_engine();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(21.0))
        .unwrap();
    // Engine batch ingest groups families per column, so ColRun spans never
    // form through the public path; seed one directly through the authority
    // plane to exercise the engine's structural split on the column axis.
    seed_absolute_read_span(
        &mut engine,
        "=$A$1*2",
        PlacementDomain::col_run(0, 0, 1, 100),
    );
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 51),
        Some(LiteralValue::Number(42.0))
    );

    // Insert before column 50 (1-based): straddles the col run. The read
    // ($A$1) stays put, so the lower half shifts with a moving origin.
    engine.insert_columns("Sheet1", 50, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_span_read_summaries_exact(&engine);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 49),
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 1, 50), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 51),
        Some(LiteralValue::Number(42.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 102),
        Some(LiteralValue::Number(42.0))
    );

    // Both halves must still track the shared precedent.
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 102),
        Some(LiteralValue::Number(20.0))
    );
}

#[test]
fn formula_plane_rect_span_row_insert_splits_into_two_rects() {
    use crate::formula_plane::runtime::PlacementDomain;
    let mut engine = authoritative_engine();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(7.0))
        .unwrap();
    // Engine batch ingest groups families per column, so Rect spans never
    // form through the public path; seed one directly through the authority
    // plane to exercise the engine's structural split on a rect domain.
    seed_absolute_read_span(
        &mut engine,
        "=$A$1+1",
        PlacementDomain::rect(0, 0, 99, 1, 3),
    );
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 3),
        Some(LiteralValue::Number(8.0))
    );

    engine.insert_rows("Sheet1", 50, 3).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_span_read_summaries_exact(&engine);
    {
        let plane = &engine.graph.formula_authority().plane;
        let mut domains: Vec<PlacementDomain> = plane
            .spans
            .active_spans()
            .map(|span| span.domain.clone())
            .collect();
        domains.sort_by_key(|domain| match domain {
            PlacementDomain::Rect { row_start, .. } => *row_start,
            _ => u32::MAX,
        });
        assert_eq!(domains[0], PlacementDomain::rect(0, 0, 48, 1, 3));
        assert_eq!(domains[1], PlacementDomain::rect(0, 52, 102, 1, 3));
    }
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 49, 3),
        Some(LiteralValue::Number(8.0))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 50, 3), None);
    assert_eq!(engine.get_cell_value("Sheet1", 52, 3), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 53, 3),
        Some(LiteralValue::Number(8.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 103, 4),
        Some(LiteralValue::Number(8.0))
    );
}

#[test]
fn formula_plane_row_insert_split_demotes_unique_literal_bindings() {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        // Per-placement literal parameters produce a multi-binding dictionary,
        // which is not safe to re-base across a split: the whole span must
        // fall back to demotion.
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+{}", row * 7)));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
    let promoted = engine.baseline_stats().formula_plane_active_span_count;

    engine.insert_rows("Sheet1", 40, 1).unwrap();
    if promoted == 1 {
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    }
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 2),
        Some(LiteralValue::Number(10.0 + 70.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 41, 2),
        Some(LiteralValue::Number(40.0 + 280.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 101, 2),
        Some(LiteralValue::Number(100.0 + 700.0))
    );
}

#[test]
fn formula_plane_repeated_mid_span_row_inserts_stay_split_and_linear() {
    let rows = 5_000;
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}*2")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    // Each mid-span insert splits exactly one span; nothing demotes to legacy
    // vertices and the structural edit itself stays fast.
    let insert_sequence = [2_500u32, 1_200, 3_800, 600, 4_600];
    for (edit_idx, before) in insert_sequence.into_iter().enumerate() {
        let started = std::time::Instant::now();
        engine.insert_rows("Sheet1", before, 1).unwrap();
        let elapsed = started.elapsed();

        assert_eq!(
            engine.baseline_stats().formula_plane_active_span_count,
            edit_idx + 2,
            "each mid-span insert must split one span into two"
        );
        assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);

        if !cfg!(debug_assertions) {
            let limit = std::time::Duration::from_secs(1);
            assert!(
                elapsed < limit,
                "split edit_{edit_idx} took {elapsed:?}, expected below {limit:?}"
            );
        }

        engine.evaluate_all().unwrap();
    }

    // Rows 1..=rows carried their values through five splices; spot-check the
    // moved tail against column A, which shifted congruently.
    for row in [1u32, 1_000, 2_400, 3_200, rows + 5] {
        let a = engine.get_cell_value("Sheet1", row, 1);
        let b = engine.get_cell_value("Sheet1", row, 2);
        match a {
            Some(LiteralValue::Number(a)) => {
                assert_eq!(
                    b,
                    Some(LiteralValue::Number(a * 2.0)),
                    "row {row} must track its shifted precedent"
                );
            }
            _ => assert_eq!(b, None, "row {row} must be empty after the splice"),
        }
    }
}

// ---------------------------------------------------------------------------
// Delete compaction must carry the template origin (not rebase to the new
// domain start), which the split path makes load-bearing: split lower halves
// keep their original origin while their domain starts elsewhere.
// ---------------------------------------------------------------------------

fn span_off_engine() -> Engine<TestWorkbook> {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Off);
    Engine::new(TestWorkbook::default(), cfg)
}

fn assert_value_parity(
    span_on: &Engine<TestWorkbook>,
    span_off: &Engine<TestWorkbook>,
    rows: u32,
    cols: u32,
) {
    for row in 1..=rows {
        for col in 1..=cols {
            assert_eq!(
                span_on.get_cell_value("Sheet1", row, col),
                span_off.get_cell_value("Sheet1", row, col),
                "span-on/off divergence at row={row} col={col}"
            );
        }
    }
}

#[test]
fn formula_plane_split_then_inner_delete_matches_span_off_engine() {
    // Split lower halves keep the original template origin (row 1) while
    // their domain starts at the split boundary. A later delete strictly
    // inside the lower half must carry that origin through compaction;
    // rebasing it to the compacted domain start silently re-anchors every
    // relative read (=A{p-41}*2 instead of =A{p}*2).
    let mut span_on = build_single_formula_column_family(100);
    let mut span_off = span_off_engine();
    add_single_formula_column_family(&mut span_off, "Sheet1", 100);
    span_off.evaluate_all().unwrap();

    for engine in [&mut span_on, &mut span_off] {
        engine.insert_rows("Sheet1", 40, 2).unwrap();
        engine.delete_rows("Sheet1", 60, 2).unwrap();
        engine.evaluate_all().unwrap();
    }

    // The upper half stays put and the lower half compacts in place: the
    // sequence never materializes legacy vertices on the span-on engine.
    assert_eq!(span_on.baseline_stats().formula_plane_active_span_count, 2);
    assert_eq!(span_on.baseline_stats().graph_formula_vertex_count, 0);
    assert_span_read_summaries_exact(&span_on);
    assert_value_parity(&span_on, &span_off, 104, 2);
}

#[test]
fn formula_plane_delete_overlapping_span_head_matches_span_off_engine() {
    // A delete that starts above the span and cuts into its head moves the
    // compacted domain start away from the template origin; the origin must
    // not be rebased to the new start. The delete removes the origin row
    // itself here, so the fixed path demotes rather than guessing an anchor.
    let mut span_on = authoritative_engine();
    let mut span_off = span_off_engine();
    for engine in [&mut span_on, &mut span_off] {
        let mut formulas = Vec::new();
        for row in 1..=120 {
            engine
                .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
                .unwrap();
        }
        // Promotion requires at least 100 non-constant cells; anchor the span
        // at row 5 so a delete starting at row 3 cuts into its head.
        for row in 5..=120 {
            formulas.push(record(engine, row, 2, &format!("=A{row}*2")));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(span_on.baseline_stats().formula_plane_active_span_count, 1);

    for engine in [&mut span_on, &mut span_off] {
        engine.delete_rows("Sheet1", 3, 4).unwrap();
        engine.evaluate_all().unwrap();
    }

    assert_value_parity(&span_on, &span_off, 120, 2);
}

#[test]
fn formula_plane_absolute_read_insert_above_rewrites_template_and_keeps_span() {
    // Issue #168, span-preserving fix: inserting rows above the whole span
    // displaces both the span and its absolute read target ($F$1 -> $F$3).
    // The span must SHIFT with a template AST rewrite — not demote — and
    // keep computing the same values against the physically moved scalar.
    let mut engine = authoritative_engine();
    engine
        .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(3.0))
        .unwrap();
    let mut formulas = Vec::new();
    // 120 rows: comfortably above the 100-cell span promotion threshold.
    for row in 2..=121 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(row as f64 + 1.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 3, &format!("=A{row}*B{row}*$F$1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    engine.insert_rows("Sheet1", 1, 2).unwrap();
    // The span survives as a single shifted span with a rewritten template.
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    engine.evaluate_all().unwrap();

    // The scalar physically moved to F3; every formula (now rows 4..=102)
    // must keep reading it through the rewritten $F$3.
    for row in [2u32, 50, 121] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row + 2, 3),
            Some(LiteralValue::Number(row as f64 * (row as f64 + 1.0) * 3.0)),
            "original row {row} (now {})",
            row + 2
        );
    }

    // A follow-up structural op on the rewritten span keeps working: a
    // mid-domain insert must split it (stationary $F$3 read, shifting
    // relative reads).
    engine.insert_rows("Sheet1", 50, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    engine.evaluate_all().unwrap();
    // Original row 50 sat at row 52 and shifted once more to row 53.
    assert_eq!(
        engine.get_cell_value("Sheet1", 53, 3),
        Some(LiteralValue::Number(50.0 * 51.0 * 3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 124, 3),
        Some(LiteralValue::Number(121.0 * 122.0 * 3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 3),
        Some(LiteralValue::Number(2.0 * 3.0 * 3.0))
    );
}

#[test]
fn formula_plane_absolute_read_column_insert_rewrites_template_and_keeps_span() {
    // Column-axis mirror of the #168 fix, on a vertical (RowRun) span:
    // engine ingest only forms column families, so the column-axis rewrite
    // is exercised by inserting columns before a RowRun span whose template
    // reads relative columns (A{r}, B{r}) plus an absolute column ($F$1).
    // Everything shifts right by 2: inputs to C/D, formulas to E, the
    // scalar to H — the template must be rewritten to read $H$1.
    let mut engine = authoritative_engine();
    engine
        .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(3.0))
        .unwrap();
    let mut formulas = Vec::new();
    for row in 2..=121 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(row as f64 + 1.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 3, &format!("=A{row}*B{row}*$F$1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    engine.insert_columns("Sheet1", 1, 2).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    engine.evaluate_all().unwrap();

    // Scalar physically moved to H1.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 8),
        Some(LiteralValue::Number(3.0))
    );
    for row in [2u32, 60, 121] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 5),
            Some(LiteralValue::Number(row as f64 * (row as f64 + 1.0) * 3.0)),
            "row {row} at shifted column E"
        );
    }
}
#[test]
fn formula_plane_partial_absolute_displacement_rewrites_selectively() {
    // Two absolute reads, only one displaced: $F$1 sits above the insert
    // (stationary), $F$5 below it (displaced), and the span shifts. The
    // template rewrite must repoint ONLY $F$5 (per-reference selectivity of
    // the shared adjuster) while $F$1 keeps its coordinate.
    let mut engine = authoritative_engine();
    engine
        .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(3.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 5, 6, LiteralValue::Number(7.0))
        .unwrap();
    let mut formulas = Vec::new();
    for row in 10..=150 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 3, &format!("=A{row}*$F$1*$F$5")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    // Insert two rows between the scalars (1-based row 3): $F$1 stays,
    // $F$5's value physically moves to F7, the span shifts to rows 12..=152.
    engine.insert_rows("Sheet1", 3, 2).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 6),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 7, 6),
        Some(LiteralValue::Number(7.0))
    );
    for row in [10u32, 80, 150] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row + 2, 3),
            Some(LiteralValue::Number(row as f64 * 3.0 * 7.0)),
            "original row {row} (now {})",
            row + 2
        );
    }
}

#[test]
fn formula_plane_mixed_read_row_insert_splits_span() {
    // P2.5: this INVERTS the v1 scope pin (previously
    // `formula_plane_mixed_read_row_insert_still_demotes`). A template with
    // mixed reads — relative A{r}/B{r} shift with the block, absolute $F$1
    // stays put — now SPLITS on a mid-domain insert: the stationary
    // absolute read imposes no constraint (its AST coordinate still points
    // at the unmoved target), so the lower half classifies as a clean
    // shift with a stationary origin and the span stays on the fast path.
    let mut engine = authoritative_engine();
    engine
        .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(3.0))
        .unwrap();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(row as f64 + 1.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 3, &format!("=A{row}*B{row}*$F$1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

    engine.insert_rows("Sheet1", 40, 1).unwrap();
    // Split, not demote: upper half rows 1..=39 keeps the span id, lower
    // half rows 41..=101 is a fresh span.
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    engine.evaluate_all().unwrap();

    // Upper half (unmoved).
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 3),
        Some(LiteralValue::Number(10.0 * 11.0 * 3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 39, 3),
        Some(LiteralValue::Number(39.0 * 40.0 * 3.0))
    );
    // The inserted row has no formula.
    assert_eq!(engine.get_cell_value("Sheet1", 40, 3), None);
    // Lower half (shifted down one row, still reading $F$1).
    assert_eq!(
        engine.get_cell_value("Sheet1", 41, 3),
        Some(LiteralValue::Number(40.0 * 41.0 * 3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 101, 3),
        Some(LiteralValue::Number(100.0 * 101.0 * 3.0))
    );
}

#[test]
fn formula_plane_split_then_insert_displacing_absolute_matches_span_off_engine() {
    // Frame-divergence composition regression (issue #168 review): op1
    // splits the span, leaving the lower half with its template origin at
    // row 1 while its domain starts at row 51. Op2 inserts rows BETWEEN
    // the stale origin and the domain start while displacing the absolute
    // target $F$30, routing the lower half into the
    // Shift{rewrite_absolute_reads} arm. The shared adjuster relocates by
    // AUTHORED coordinate (A1 sits below the insert point), so an
    // unguarded rewrite shears every relative read by the insert count.
    // rewrite_frame_is_sound must refuse the rewrite (demote instead) and
    // keep span-ON byte-identical to span-OFF.
    let mut span_on = authoritative_engine();
    let mut span_off = span_off_engine();
    for engine in [&mut span_on, &mut span_off] {
        let mut formulas = Vec::new();
        for row in 1..=120 {
            engine
                .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
                .unwrap();
        }
        engine
            .set_cell_value("Sheet1", 30, 6, LiteralValue::Number(1000.0))
            .unwrap();
        for row in 1..=120 {
            formulas.push(record(engine, row, 3, &format!("=A{row}+$F$30")));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(span_on.baseline_stats().formula_plane_active_span_count, 1);

    for engine in [&mut span_on, &mut span_off] {
        engine.insert_rows("Sheet1", 50, 1).unwrap();
        engine.insert_rows("Sheet1", 10, 3).unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_value_parity(&span_on, &span_off, 124, 6);
}

#[test]
fn formula_plane_origin_follows_shift_keeps_incremental_dirty_projection() {
    // Second frame bug pinned during the #168 review: projection rules
    // store PLACEMENT-relative offsets (= authored - origin). An
    // origin-follows shift (stationary relative reads, shifted span) moves
    // the origin over an unchanged AST, so the stored offsets must shift
    // by -origin_delta or incremental dirty projection maps changed
    // regions to the wrong result rows and the span silently keeps stale
    // values. Span rows 150..=270 read A10..A130 (stationary under the
    // insert at 140); after the shift, a write to A10 must still re-dirty
    // the span.
    let mut span_on = authoritative_engine();
    let mut span_off = span_off_engine();
    for engine in [&mut span_on, &mut span_off] {
        let mut formulas = Vec::new();
        for row in 10..=130 {
            engine
                .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
                .unwrap();
        }
        for row in 150..=270 {
            formulas.push(record(engine, row, 3, &format!("=A{}", row - 140)));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(span_on.baseline_stats().formula_plane_active_span_count, 1);
    for engine in [&mut span_on, &mut span_off] {
        engine.insert_rows("Sheet1", 140, 1).unwrap();
        engine.evaluate_all().unwrap();
    }
    // The span survives the shift (origin follows the block).
    assert_eq!(span_on.baseline_stats().formula_plane_active_span_count, 1);
    // Incremental dirty: change a read target, re-evaluate.
    for engine in [&mut span_on, &mut span_off] {
        engine
            .set_cell_value("Sheet1", 10, 1, LiteralValue::Number(999.0))
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(
        span_on.get_cell_value("Sheet1", 151, 3),
        Some(LiteralValue::Number(999.0)),
        "the shifted span must observe the incremental write"
    );
    assert_value_parity(&span_on, &span_off, 275, 6);
}

#[test]
fn formula_plane_rewrite_keeps_incremental_dirty_on_moved_absolute() {
    // Companion to the frame-invariant fix: after a template rewrite the
    // span's read summary must be RE-DERIVED from the rewritten canonical
    // template — a structural transform would keep the old Absolute rule
    // indices, and an incremental write to the MOVED scalar would silently
    // fail to dirty the span.
    let mut span_on = authoritative_engine();
    let mut span_off = span_off_engine();
    for engine in [&mut span_on, &mut span_off] {
        engine
            .set_cell_value("Sheet1", 1, 6, LiteralValue::Number(3.0))
            .unwrap();
        let mut formulas = Vec::new();
        for row in 2..=121 {
            engine
                .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
                .unwrap();
            formulas.push(record(engine, row, 3, &format!("=A{row}*$F$1")));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
            .unwrap();
        engine.evaluate_all().unwrap();
        engine.insert_rows("Sheet1", 1, 2).unwrap();
        engine.evaluate_all().unwrap();
        // Incremental write to the MOVED absolute target (now F3).
        engine
            .set_cell_value("Sheet1", 3, 6, LiteralValue::Number(5.0))
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(span_on.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(
        span_on.get_cell_value("Sheet1", 4, 3),
        Some(LiteralValue::Number(2.0 * 5.0)),
        "the rewritten span must observe the incremental write to $F$3"
    );
    assert_value_parity(&span_on, &span_off, 125, 6);
}

#[test]
fn formula_plane_column_pinned_origin_shifts_compose_and_match_span_off_engine() {
    // Column-axis mirror of the frame-divergence composition. Engine ingest
    // only forms vertical families, and every column-op sequence that could
    // displace an absolute target through a diverged column origin first
    // routes the span through the rewrite arm (which re-aligns the origin),
    // so the row repro's shear is not reachable on this axis — pinned at
    // unit level in rewrite_frame_soundness_matrix. What IS reachable is
    // composing pinned-origin column shifts: op1 diverges origin_col from
    // the domain column, op2 shifts again through the diverged frame. Both
    // must stay on the span fast path and match span-OFF.
    let mut span_on = authoritative_engine();
    let mut span_off = span_off_engine();
    for engine in [&mut span_on, &mut span_off] {
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1000.0))
            .unwrap();
        let mut formulas = Vec::new();
        for row in 1..=120 {
            engine
                .set_cell_value("Sheet1", row, 3, LiteralValue::Number(row as f64))
                .unwrap();
            formulas.push(record(engine, row, 4, &format!("=C{row}+$A$1")));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(span_on.baseline_stats().formula_plane_active_span_count, 1);

    for engine in [&mut span_on, &mut span_off] {
        // op1: insert between $A$1 and the inputs — inputs and formulas
        // shift right, the scalar stays: pinned-origin column shift
        // (origin_col keeps the authored anchor while the domain moves).
        engine.insert_columns("Sheet1", 2, 1).unwrap();
        // op2: insert at the (shifted) input column — shifts inputs and
        // formulas again through the diverged column frame.
        engine.insert_columns("Sheet1", 4, 1).unwrap();
        engine.evaluate_all().unwrap();
    }
    // Both ops are mixed-read fast-path shifts: the span survives.
    assert_eq!(span_on.baseline_stats().formula_plane_active_span_count, 1);
    // Incremental writes through the diverged frame must still re-dirty
    // the span (rule offsets track the pinned origin).
    for engine in [&mut span_on, &mut span_off] {
        engine
            .set_cell_value("Sheet1", 5, 5, LiteralValue::Number(500.0))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2000.0))
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(
        span_on.get_cell_value("Sheet1", 5, 6),
        Some(LiteralValue::Number(500.0 + 2000.0)),
        "shifted formula must track both incremental writes"
    );
    assert_value_parity(&span_on, &span_off, 125, 8);
}
