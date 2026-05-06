use std::sync::Arc;

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
                    Some(LiteralValue::Number(expected))
                );
            }
        }
    }
}

#[test]
fn formula_plane_authoritative_column_insert_shifts_span_outputs_correctly() {
    let mut engine = build_three_formula_column_family(100);

    engine.insert_columns("Sheet1", 3, 1).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
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
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
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
    assert_eq!(engine.get_cell_value("Sheet1", 100, 2), None);
}
