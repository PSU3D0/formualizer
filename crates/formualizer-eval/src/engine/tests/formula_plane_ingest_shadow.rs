use std::sync::Arc;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
    RowVisibilitySource,
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

#[test]
fn formula_plane_off_ingest_reports_graph_materialized_formulas() {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Off);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 1, "=1+1"),
            record(&mut engine, 2, 1, "=A1+1"),
        ],
    )];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("ingest formulas");

    assert_eq!(report.mode, FormulaPlaneMode::Off);
    assert_eq!(report.formula_cells_seen, 2);
    assert_eq!(report.graph_formula_cells_materialized, 2);
    assert_eq!(engine.last_formula_ingest_report(), Some(&report));
    assert_eq!(engine.formula_ingest_report_total().formula_cells_seen, 2);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 2);

    engine.evaluate_all().expect("evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(3.0))
    );
}

#[test]
fn formula_plane_shadow_deferred_build_graph_all_materializes_all_formulas() {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(2.0))
        .unwrap();
    engine.stage_formula_text("Sheet1", 1, 2, "=A1+1".to_string());
    engine.stage_formula_text("Sheet1", 2, 2, "=A2+1".to_string());
    assert_eq!(engine.staged_formula_count(), 2);

    engine.build_graph_all().expect("build staged formulas");
    engine.evaluate_all().expect("evaluate staged formulas");

    let report = engine
        .last_formula_ingest_report()
        .expect("formula ingest report");
    assert_eq!(report.mode, FormulaPlaneMode::Shadow);
    assert_eq!(report.formula_cells_seen, 2);
    assert_eq!(report.graph_formula_cells_materialized, 2);
    assert_eq!(report.shadow_accepted_span_cells, 2);
    assert_eq!(report.graph_formula_vertices_avoided_shadow, 2);
    assert_eq!(engine.staged_formula_count(), 0);
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 2);
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(3.0))
    );
}

#[test]
fn formula_plane_authoritative_ingest_skips_accepted_span_graph_materialization() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(2.0))
        .unwrap();

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 2, "=A1+1"),
            record(&mut engine, 2, 2, "=A2+1"),
        ],
    )];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.mode, FormulaPlaneMode::AuthoritativeExperimental);
    assert_eq!(report.formula_cells_seen, 2);
    assert_eq!(report.shadow_accepted_span_cells, 2);
    assert_eq!(report.graph_formula_cells_materialized, 0);
    let stats = engine.baseline_stats();
    assert_eq!(stats.graph_formula_vertex_count, 0);
    assert_eq!(stats.formula_plane_active_span_count, 1);
    assert_eq!(stats.formula_plane_producer_result_entries, 1);
    assert_eq!(stats.formula_plane_consumer_read_entries, 1);

    engine.evaluate_all().expect("span-only mixed evaluate_all");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine
            .evaluate_cell("Sheet1", 1, 2)
            .expect("evaluate_cell routes through FormulaPlane coordinator"),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn formula_plane_authoritative_cross_sheet_family_promotes_and_dirty_propagates() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Data").unwrap();

    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 1, &format!("=Data!A{row}*2")));
    }

    let report = engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    assert_eq!(report.graph_formula_cells_materialized, 0);
    assert!(engine.baseline_stats().formula_plane_active_span_count > 0);

    engine.evaluate_all().expect("initial cross-sheet evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 1),
        Some(LiteralValue::Number(100.0))
    );

    engine
        .set_cell_value("Data", 50, 1, LiteralValue::Number(1000.0))
        .unwrap();
    engine.evaluate_all().expect("dirty cross-sheet evaluate");

    assert_eq!(
        engine.get_cell_value("Sheet1", 49, 1),
        Some(LiteralValue::Number(98.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 1),
        Some(LiteralValue::Number(2000.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 51, 1),
        Some(LiteralValue::Number(102.0))
    );
}

#[test]
fn formula_plane_authoritative_sum_static_range_family_promotes() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    for row in 1..=10 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=20 {
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=A{row} * SUM($A$1:$A$10)"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    assert!(engine.baseline_stats().formula_plane_active_span_count > 0);

    engine.evaluate_all().expect("initial range evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(5.0 * 55.0))
    );

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(100.0))
        .unwrap();
    engine.evaluate_all().expect("dirty range evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(100.0 * 150.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 2),
        Some(LiteralValue::Number(10.0 * 150.0))
    );
}

#[test]
fn formula_plane_authoritative_sumifs_family_promotes() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Data").unwrap();
    let mut expected = 0.0;
    for row in 1..=100 {
        let category = if row % 3 == 1 { "Type1" } else { "Type0" };
        let value = row as f64;
        if category == "Type1" {
            expected += value;
        }
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Text(category.to_string()))
            .unwrap();
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(value))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=50 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            2,
            &format!("=SUMIFS(Data!$B$1:$B$100, Data!$A$1:$A$100, \"Type1\") + A{row}"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert!(stats.formula_plane_active_span_count > 0);

    engine.evaluate_all().expect("initial sumifs evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(expected + 1.0))
    );

    engine
        .set_cell_value("Data", 4, 2, LiteralValue::Number(1000.0))
        .unwrap();
    let updated_expected = expected - 4.0 + 1000.0;
    engine.evaluate_all().expect("dirty sumifs evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(updated_expected + 50.0))
    );
}

#[test]
fn formula_plane_authoritative_constant_sumifs_family_promotes_via_broadcast() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Data").unwrap();
    let mut expected = 0.0;
    for row in 1..=100 {
        let category = if row % 3 == 1 { "Type1" } else { "Type0" };
        let value = row as f64;
        if category == "Type1" {
            expected += value;
        }
        engine
            .set_cell_value("Data", row, 1, LiteralValue::Text(category.to_string()))
            .unwrap();
        engine
            .set_cell_value("Data", row, 2, LiteralValue::Number(value))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=50 {
        formulas.push(record(
            &mut engine,
            row,
            2,
            "=SUMIFS(Data!$B$1:$B$100, Data!$A$1:$A$100, \"Type1\")",
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 1);

    engine
        .evaluate_all()
        .expect("initial constant sumifs evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(expected))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 25, 2),
        Some(LiteralValue::Number(expected))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(expected))
    );

    engine
        .set_cell_value("Data", 4, 2, LiteralValue::Number(1000.0))
        .unwrap();
    let updated_expected = expected - 4.0 + 1000.0;
    engine
        .evaluate_all()
        .expect("dirty constant sumifs evaluate");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(updated_expected))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 50, 2),
        Some(LiteralValue::Number(updated_expected))
    );
}

#[test]
fn formula_plane_authoritative_evaluate_all_orders_span_chain() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(2.0))
        .unwrap();

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 2, "=A1+1"),
            record(&mut engine, 2, 2, "=A2+1"),
            record(&mut engine, 1, 3, "=B1+2"),
            record(&mut engine, 2, 3, "=B2+2"),
        ],
    )];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.graph_formula_cells_materialized, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine.evaluate_all().expect("span chain evaluate_all");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(4.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn formula_plane_authoritative_mixed_accept_and_fallback_materializes_only_fallback() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 2, "=A1+1"),
            record(&mut engine, 2, 2, "=A2+1"),
            record(&mut engine, 1, 3, "=1+1"),
        ],
    )];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.formula_cells_seen, 3);
    assert_eq!(report.shadow_accepted_span_cells, 2);
    assert_eq!(report.shadow_fallback_cells, 1);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    let stats = engine.baseline_stats();
    assert_eq!(stats.graph_formula_vertex_count, 1);
    assert_eq!(stats.formula_plane_active_span_count, 1);
    engine
        .evaluate_all()
        .expect("mixed independent legacy/span runtime");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn formula_plane_authoritative_mixed_span_to_legacy_sum_evaluates() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(2.0))
        .unwrap();

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 2, "=A1+1"),
            record(&mut engine, 2, 2, "=A2+1"),
            record(&mut engine, 1, 4, "=SUM(B1:B2)"),
        ],
    )];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.shadow_accepted_span_cells, 2);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    engine.evaluate_all().expect("span to legacy runtime");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn formula_plane_authoritative_mixed_legacy_to_span_evaluates() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(2.0))
        .unwrap();

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 1, "=1+1"),
            record(&mut engine, 1, 2, "=A1+1"),
            record(&mut engine, 2, 2, "=A2+1"),
        ],
    )];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.shadow_accepted_span_cells, 2);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    engine.evaluate_all().expect("legacy to span runtime");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(3.0))
    );
}

#[test]
fn formula_plane_authoritative_fallback_only_still_evaluates_legacy() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![record(&mut engine, 1, 1, "=1+1")],
    )];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative fallback ingest");

    assert_eq!(report.formula_cells_seen, 1);
    assert_eq!(report.shadow_accepted_span_cells, 0);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    engine.evaluate_all().expect("fallback-only evaluation");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn formula_plane_shadow_build_graph_for_sheets_reports_selected_sheet_only() {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet("Other").unwrap();

    engine.stage_formula_text("Sheet1", 1, 1, "=1+1".to_string());
    engine.stage_formula_text("Other", 1, 1, "=1+2".to_string());

    engine
        .build_graph_for_sheets(["Sheet1"])
        .expect("build selected sheet");

    let report = engine
        .last_formula_ingest_report()
        .expect("formula ingest report");
    assert_eq!(report.mode, FormulaPlaneMode::Shadow);
    assert_eq!(report.formula_cells_seen, 1);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    assert_eq!(engine.staged_formula_count(), 1);
    assert!(engine.get_staged_formula_text("Other", 1, 1).is_some());
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 1);
}

#[test]
fn formula_plane_spill_commit_redirties_span_reading_spill_children() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 2, 2, "=A2+10"),
            record(&mut engine, 3, 2, "=A3+10"),
        ],
    )];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 2);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(10.0))
    );

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(3.0))
    );

    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 2,
        "expected spill-region notification to re-evaluate span, got {result:?}"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(12.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(13.0))
    );
}

#[test]
fn formula_plane_source_invalidation_uses_conservative_whole_span_dirty() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.define_source_scalar("Feed", Some(1)).unwrap();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 2, "=A1+1"),
            record(&mut engine, 2, 2, "=A2+1"),
        ],
    )];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 2);
    engine.evaluate_all().unwrap();

    engine.invalidate_source("Feed").unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 2,
        "expected source invalidation to re-evaluate active spans, got {result:?}"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(6.0))
    );
}

#[test]
fn formula_plane_row_visibility_change_redirties_absolute_anchor_span() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 2, "=$A$1+A1*0+1"),
            record(&mut engine, 2, 2, "=$A$1+A2*0+1"),
        ],
    )];
    engine.ingest_formula_batches(batches).unwrap();
    engine.evaluate_all().unwrap();

    engine
        .set_row_hidden("Sheet1", 1, true, RowVisibilitySource::Manual)
        .unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 2,
        "expected whole-row visibility notification to re-evaluate span, got {result:?}"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(6.0))
    );
}

#[test]
fn formula_plane_insert_rows_conservatively_redirties_sheet_spans() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 2, "=A1+1"),
            record(&mut engine, 2, 2, "=A2+1"),
        ],
    )];
    engine.ingest_formula_batches(batches).unwrap();
    engine.evaluate_all().unwrap();

    engine.insert_rows("Sheet1", 10, 1).unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 2,
        "expected sheet structural notification to re-evaluate span, got {result:?}"
    );
}

#[test]
fn formula_plane_remove_sheet_redirties_surviving_spans() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let data_id = engine.add_sheet("Data").unwrap();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();

    let batches = vec![FormulaIngestBatch::new(
        "Sheet1",
        vec![
            record(&mut engine, 1, 2, "=A1+1"),
            record(&mut engine, 2, 2, "=A2+1"),
        ],
    )];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 2);
    engine.evaluate_all().unwrap();

    engine.remove_sheet(data_id).unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 2,
        "expected remove-sheet notification to re-evaluate surviving spans, got {result:?}"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(6.0))
    );
}

#[test]
fn formula_plane_remove_sheet_hosting_span_removes_active_span() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let other_id = engine.add_sheet("Other").unwrap();
    engine
        .set_cell_value("Other", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Other", 2, 1, LiteralValue::Number(2.0))
        .unwrap();

    let batches = vec![FormulaIngestBatch::new(
        "Other",
        vec![
            record(&mut engine, 1, 2, "=A1+1"),
            record(&mut engine, 2, 2, "=A2+1"),
        ],
    )];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 2);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    engine.remove_sheet(other_id).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert!(engine.graph.sheet_id("Other").is_none());
}
