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
    let mut formulas = Vec::new();
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
    engine
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

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(61.0))
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
fn formula_plane_zero_count_structural_ops_are_noops() {
    let mut engine = build_single_formula_column_family(100);

    engine.insert_rows("Sheet1", 3, 0).unwrap();
    engine.delete_rows("Sheet1", 3, 0).unwrap();
    engine.insert_columns("Sheet1", 2, 0).unwrap();
    engine.delete_columns("Sheet1", 2, 0).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();

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
