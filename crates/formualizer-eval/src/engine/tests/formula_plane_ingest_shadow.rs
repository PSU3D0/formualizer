use std::sync::Arc;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaMetadataEnvelope,
    FormulaPlaneMode, FormulaSourceEvent, FormulaSourceKind, RowVisibilitySource,
    SourceCachedValue, SourceCoord, SourceFamilyId, SourceRect,
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
    assert_eq!(report.shadow_accepted_span_cells, 0);
    assert_eq!(report.graph_formula_vertices_avoided_shadow, 0);
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
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.mode, FormulaPlaneMode::AuthoritativeExperimental);
    assert_eq!(report.formula_cells_seen, 100);
    assert_eq!(report.shadow_accepted_span_cells, 100);
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
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }

    let mut formulas = Vec::new();
    for row in 1..=100 {
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
    for row in 1..=100 {
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
fn formula_plane_authoritative_demotes_small_non_constant_domains() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let formula = match row % 10 {
            0 => format!("=A{row}*RAND()"),
            1 => format!("=A{row}+TODAY()"),
            2 => format!("=A{row}*NOW()"),
            _ => format!("=A{row}*2"),
        };
        formulas.push(record(&mut engine, row, 2, &formula));
    }

    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 0);
    assert_eq!(stats.graph_formula_vertex_count, 100);

    engine.evaluate_all().expect("evaluate demoted formulas");
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(6.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 9, 2),
        Some(LiteralValue::Number(18.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 99, 2),
        Some(LiteralValue::Number(198.0))
    );
}

#[test]
fn formula_plane_authoritative_demotes_99_cell_non_constant_runs() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=200 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let formula = if row % 100 == 0 {
            format!("=A{row}/0")
        } else {
            format!("=A{row}*2")
        };
        formulas.push(record(&mut engine, row, 2, &formula));
    }

    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 0);
    assert_eq!(stats.graph_formula_vertex_count, 200);

    engine.evaluate_all().expect("evaluate demoted formulas");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 99, 2),
        Some(LiteralValue::Number(198.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 101, 2),
        Some(LiteralValue::Number(202.0))
    );
    assert!(matches!(
        engine.get_cell_value("Sheet1", 100, 2),
        Some(LiteralValue::Error(_))
    ));
    assert!(matches!(
        engine.get_cell_value("Sheet1", 200, 2),
        Some(LiteralValue::Error(_))
    ));
}

#[test]
fn formula_plane_authoritative_promotes_100_cell_non_constant_run() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}*2")));
    }

    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
        .expect("authoritative ingest");
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 1);
    assert_eq!(stats.graph_formula_vertex_count, 0);

    engine.evaluate_all().expect("evaluate promoted formulas");
    for row in 1..=100 {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(row as f64 * 2.0))
        );
    }
}

#[test]
fn formula_plane_authoritative_evaluate_all_orders_span_chain() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
        formulas.push(record(&mut engine, row, 3, &format!("=B{row}+2")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
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

    let mut formulas = Vec::new();
    for row in 1..=100 {
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    formulas.push(record(&mut engine, 1, 3, "=1+1"));

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.formula_cells_seen, 101);
    assert_eq!(report.shadow_accepted_span_cells, 100);
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
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    formulas.push(record(&mut engine, 1, 4, "=SUM(B1:B100)"));

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.shadow_accepted_span_cells, 100);
    assert_eq!(report.graph_formula_cells_materialized, 1);
    engine.evaluate_all().expect("span to legacy runtime");
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(5150.0))
    );
}

#[test]
fn formula_plane_authoritative_mixed_legacy_to_span_evaluates() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut formulas = vec![record(&mut engine, 1, 1, "=1+1")];
    for row in 1..=100 {
        if row != 1 {
            engine
                .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
                .unwrap();
        }
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine
        .ingest_formula_batches(batches)
        .expect("authoritative ingest");

    assert_eq!(report.shadow_accepted_span_cells, 100);
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

    let mut formulas = Vec::new();
    for row in 2..=101 {
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+10")));
    }
    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 100);
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
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(5.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 100);
    engine.evaluate_all().unwrap();

    engine.invalidate_source("Feed").unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 100,
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
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(5.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=$A$1+A{row}*0+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    engine.ingest_formula_batches(batches).unwrap();
    engine.evaluate_all().unwrap();

    engine
        .set_row_hidden("Sheet1", 1, true, RowVisibilitySource::Manual)
        .unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 100,
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
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(5.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    engine.ingest_formula_batches(batches).unwrap();
    engine.evaluate_all().unwrap();

    engine.insert_rows("Sheet1", 10, 1).unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 100,
        "expected sheet structural notification to re-evaluate span, got {result:?}"
    );
}

#[test]
fn formula_plane_remove_sheet_redirties_surviving_spans() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let data_id = engine.add_sheet("Data").unwrap();
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(5.0))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Sheet1", formulas)];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 100);
    engine.evaluate_all().unwrap();

    engine.remove_sheet(data_id).unwrap();
    let result = engine.evaluate_all().unwrap();
    assert!(
        result.computed_vertices >= 100,
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
    let mut formulas = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Other", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }

    let batches = vec![FormulaIngestBatch::new("Other", formulas)];
    let report = engine.ingest_formula_batches(batches).unwrap();
    assert_eq!(report.shadow_accepted_span_cells, 100);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    engine.remove_sheet(other_id).unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert!(engine.graph.sheet_id("Other").is_none());
}

fn source_event(sheet: &str, row0: u32, col0: u32, sequence: u64) -> FormulaSourceEvent {
    FormulaSourceEvent {
        sheet_name: Arc::from(sheet),
        coord0: SourceCoord {
            row: row0,
            col: col0,
        },
        source_sequence: sequence,
        formula: FormulaSourceKind::Ordinary {
            formula: Arc::from("1+1"),
            metadata: FormulaMetadataEnvelope::XlsxOrdinary,
        },
        cached: SourceCachedValue::AbsentOrEmpty,
    }
}

fn shared_source_events(sheet: &str) -> Vec<FormulaSourceEvent> {
    let family = SourceFamilyId {
        sheet_instance: 0,
        shared_index: 9,
    };
    let range = SourceRect {
        start: SourceCoord { row: 0, col: 0 },
        end: SourceCoord { row: 1, col: 0 },
    };
    vec![
        FormulaSourceEvent {
            sheet_name: Arc::from(sheet),
            coord0: SourceCoord { row: 0, col: 0 },
            source_sequence: 0,
            formula: FormulaSourceKind::SharedAnchor {
                family,
                declared_range: Some(range),
                formula: Arc::from("1+1"),
                metadata: FormulaMetadataEnvelope::XlsxShared {
                    shared_index: 9,
                    parsed_range: Some(range),
                },
            },
            cached: SourceCachedValue::AbsentOrEmpty,
        },
        FormulaSourceEvent {
            sheet_name: Arc::from(sheet),
            coord0: SourceCoord { row: 1, col: 0 },
            source_sequence: 1,
            formula: FormulaSourceKind::SharedDescendant {
                family,
                metadata: FormulaMetadataEnvelope::XlsxShared {
                    shared_index: 9,
                    parsed_range: None,
                },
            },
            cached: SourceCachedValue::AbsentOrEmpty,
        },
    ]
}

#[test]
fn shadow_family_collection_has_eager_deferred_parity_and_no_output_authority() {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
    let mut eager = Engine::new(TestWorkbook::default(), cfg.clone());
    let eager_records = vec![
        record(&mut eager, 1, 1, "=1+1"),
        record(&mut eager, 2, 1, "=1+1"),
    ];
    let eager_report = eager
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            FormulaIngestBatch::new("Sheet1", eager_records),
            shared_source_events("Sheet1"),
        )])
        .unwrap();
    eager.evaluate_all().unwrap();

    let mut deferred = Engine::new(TestWorkbook::default(), cfg);
    deferred.stage_formula_text("Sheet1", 1, 1, "=1+1".into());
    deferred.stage_formula_text("Sheet1", 2, 1, "=1+1".into());
    deferred.stage_formula_source_events("Sheet1", shared_source_events("Sheet1"));
    deferred.build_graph_all().unwrap();
    deferred.evaluate_all().unwrap();

    assert_eq!(deferred.last_formula_ingest_report(), Some(&eager_report));
    assert_eq!(eager_report.source_families_seen, 1);
    assert_eq!(eager_report.source_family_shadow_eligible, 1);
    assert_eq!(eager_report.source_family_shadow_eligible_cells, 2);
    assert_eq!(eager_report.source_family_fallback_cells, 0);
    assert_eq!(eager_report.graph_formula_cells_materialized, 2);
    assert_eq!(eager.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(deferred.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(
        eager.get_cell_value("Sheet1", 2, 1),
        deferred.get_cell_value("Sheet1", 2, 1)
    );
}

#[test]
fn off_mode_skips_family_collection_but_preserves_source_event_counts() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    let records = vec![
        record(&mut engine, 1, 1, "=1+1"),
        record(&mut engine, 2, 1, "=1+1"),
    ];
    let report = engine
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            FormulaIngestBatch::new("Sheet1", records),
            shared_source_events("Sheet1"),
        )])
        .unwrap();
    assert_eq!(report.source_formula_events, 2);
    assert_eq!(report.source_families_seen, 0);
    assert_eq!(report.source_evidence_peak_bytes, 0);
    assert_eq!(report.graph_formula_cells_materialized, 2);
}

#[test]
fn authoritative_source_collection_does_not_change_existing_placement() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut plain = Engine::new(TestWorkbook::default(), cfg.clone());
    let mut sourced = Engine::new(TestWorkbook::default(), cfg);
    let mut plain_records = Vec::new();
    let mut sourced_records = Vec::new();
    let family = SourceFamilyId {
        sheet_instance: 0,
        shared_index: 3,
    };
    let range = SourceRect {
        start: SourceCoord { row: 0, col: 1 },
        end: SourceCoord { row: 99, col: 1 },
    };
    let mut events = Vec::new();
    for row in 1..=100 {
        plain
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        sourced
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let formula = format!("=A{row}+1");
        plain_records.push(record(&mut plain, row, 2, &formula));
        sourced_records.push(record(&mut sourced, row, 2, &formula));
        events.push(FormulaSourceEvent {
            sheet_name: Arc::from("Sheet1"),
            coord0: SourceCoord {
                row: row - 1,
                col: 1,
            },
            source_sequence: u64::from(row - 1),
            formula: if row == 1 {
                FormulaSourceKind::SharedAnchor {
                    family,
                    declared_range: Some(range),
                    formula: Arc::from("A1+1"),
                    metadata: FormulaMetadataEnvelope::XlsxShared {
                        shared_index: 3,
                        parsed_range: Some(range),
                    },
                }
            } else {
                FormulaSourceKind::SharedDescendant {
                    family,
                    metadata: FormulaMetadataEnvelope::XlsxShared {
                        shared_index: 3,
                        parsed_range: None,
                    },
                }
            },
            cached: SourceCachedValue::AbsentOrEmpty,
        });
    }
    let plain_report = plain
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", plain_records)])
        .unwrap();
    let source_report = sourced
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            FormulaIngestBatch::new("Sheet1", sourced_records),
            events,
        )])
        .unwrap();

    assert_eq!(
        source_report.graph_formula_cells_materialized,
        plain_report.graph_formula_cells_materialized
    );
    assert_eq!(
        source_report.shadow_accepted_span_cells,
        plain_report.shadow_accepted_span_cells
    );
    assert_eq!(sourced.baseline_stats(), plain.baseline_stats());
    assert_eq!(source_report.source_family_shadow_eligible, 1);
    plain.evaluate_all().unwrap();
    sourced.evaluate_all().unwrap();
    assert_eq!(
        sourced.get_cell_value("Sheet1", 100, 2),
        plain.get_cell_value("Sheet1", 100, 2)
    );
}

#[test]
fn deferred_source_events_reach_central_ingest_with_eager_parity() {
    let cfg = EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Shadow);
    let mut eager = Engine::new(TestWorkbook::default(), cfg.clone());
    let eager_record = record(&mut eager, 1, 1, "=1+1");
    let eager_report = eager
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            FormulaIngestBatch::new("Sheet1", vec![eager_record]),
            vec![source_event("Sheet1", 0, 0, 0)],
        )])
        .unwrap();

    let mut deferred = Engine::new(TestWorkbook::default(), cfg);
    deferred.stage_formula_text("Sheet1", 1, 1, "=1+1".into());
    deferred.stage_formula_source_events("Sheet1", vec![source_event("Sheet1", 0, 0, 0)]);
    deferred.build_graph_all().unwrap();
    let deferred_report = deferred.last_formula_ingest_report().unwrap();

    assert_eq!(deferred_report, &eager_report);
    assert_eq!(deferred_report.source_formula_events, 1);
    assert_eq!(deferred_report.source_ordinary_events, 1);
    assert!(!deferred.has_staged_formulas());
}

#[test]
fn deferred_selected_build_isolates_source_events() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    for sheet in ["Selected", "Other"] {
        engine.stage_formula_text(sheet, 1, 1, "=1+1".into());
        engine.stage_formula_source_events(sheet, vec![source_event(sheet, 0, 0, 0)]);
    }

    engine.build_graph_for_sheets(["Selected"]).unwrap();
    assert_eq!(
        engine
            .last_formula_ingest_report()
            .unwrap()
            .source_formula_events,
        1
    );
    assert_eq!(engine.staged_formula_count(), 1);
    assert_eq!(
        engine.get_staged_formula_text("Other", 1, 1).as_deref(),
        Some("=1+1")
    );

    engine.build_graph_all().unwrap();
    assert_eq!(
        engine
            .last_formula_ingest_report()
            .unwrap()
            .source_formula_events,
        1
    );
    assert!(!engine.has_staged_formulas());
}

#[test]
fn staged_rename_moves_and_remove_clears_source_events() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine.stage_formula_text("Old", 1, 1, "=1+1".into());
    engine.stage_formula_source_events("Old", vec![source_event("Old", 0, 0, 0)]);
    engine.rename_staged_formula_sheet("Old", "New");
    assert_eq!(
        engine.get_staged_formula_text("New", 1, 1).as_deref(),
        Some("=1+1")
    );
    engine.build_graph_all().unwrap();
    assert_eq!(
        engine
            .last_formula_ingest_report()
            .unwrap()
            .source_formula_events,
        1
    );

    engine.stage_formula_text("Gone", 1, 1, "=2+2".into());
    engine.stage_formula_source_events("Gone", vec![source_event("Gone", 0, 0, 0)]);
    assert_eq!(
        engine.clear_staged_formula_text("Gone", 1, 1).as_deref(),
        Some("=2+2")
    );
    assert!(!engine.has_staged_formulas());
}

#[test]
fn interactive_replacement_preserves_text_and_clears_source_provenance() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine.stage_formula_text("Sheet1", 1, 1, "=1+1".into());
    engine.stage_formula_source_events("Sheet1", vec![source_event("Sheet1", 0, 0, 0)]);
    engine.stage_formula_text("Sheet1", 1, 1, "=3+4".into());

    assert_eq!(
        engine.get_staged_formula_text("Sheet1", 1, 1).as_deref(),
        Some("=3+4")
    );
    engine.build_graph_all().unwrap();
    assert_eq!(
        engine
            .last_formula_ingest_report()
            .unwrap()
            .source_formula_events,
        0
    );
}

#[test]
fn duplicate_replacement_keeps_latest_text_and_raw_source_events() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine.stage_formula_text("Sheet1", 1, 1, "=1+1".into());
    engine.stage_formula_text("Sheet1", 1, 1, "=2+2".into());
    engine.stage_formula_source_events(
        "Sheet1",
        vec![
            source_event("Sheet1", 0, 0, 0),
            source_event("Sheet1", 0, 0, 1),
        ],
    );
    assert_eq!(engine.staged_formula_count(), 1);
    assert_eq!(
        engine.get_staged_formula_text("Sheet1", 1, 1).as_deref(),
        Some("=2+2")
    );
    engine.build_graph_all().unwrap();
    assert_eq!(
        engine
            .last_formula_ingest_report()
            .unwrap()
            .source_formula_events,
        2
    );
}

#[test]
fn deferred_parse_error_does_not_report_or_retain_source_events() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine.stage_formula_text("Sheet1", 1, 1, "=SUM(".into());
    engine.stage_formula_source_events("Sheet1", vec![source_event("Sheet1", 0, 0, 0)]);
    assert!(engine.build_graph_all().is_err());
    assert!(engine.last_formula_ingest_report().is_none());
    assert!(!engine.has_staged_formulas());
}

#[test]
fn source_metadata_validation_precedes_graph_ingest() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    let formula = record(&mut engine, 1, 1, "=1+1");
    let result =
        engine.ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            FormulaIngestBatch::new("Sheet1", vec![formula]),
            vec![source_event("WrongSheet", 0, 0, 0)],
        )]);
    assert!(result.is_err());
    assert_eq!(engine.baseline_stats().graph_formula_vertex_count, 0);
    assert!(engine.last_formula_ingest_report().is_none());
}

fn direct_source_family(
    engine: &mut Engine<TestWorkbook>,
    id: SourceFamilyId,
    coords0: &[(u32, u32)],
    formulas: &[&str],
) -> (FormulaIngestBatch, Vec<FormulaSourceEvent>) {
    let min_row = coords0.iter().map(|coord| coord.0).min().unwrap();
    let min_col = coords0.iter().map(|coord| coord.1).min().unwrap();
    let max_row = coords0.iter().map(|coord| coord.0).max().unwrap();
    let max_col = coords0.iter().map(|coord| coord.1).max().unwrap();
    let range = SourceRect {
        start: SourceCoord {
            row: min_row,
            col: min_col,
        },
        end: SourceCoord {
            row: max_row,
            col: max_col,
        },
    };
    let records = coords0
        .iter()
        .zip(formulas)
        .map(|(&(row, col), formula)| record(engine, row + 1, col + 1, formula))
        .collect();
    let events = coords0
        .iter()
        .zip(formulas)
        .enumerate()
        .map(|(sequence, (&(row, col), formula))| FormulaSourceEvent {
            sheet_name: Arc::from("Sheet1"),
            coord0: SourceCoord { row, col },
            source_sequence: sequence as u64,
            formula: if sequence == 0 {
                FormulaSourceKind::SharedAnchor {
                    family: id,
                    declared_range: Some(range),
                    formula: Arc::from(formula.trim_start_matches('=')),
                    metadata: FormulaMetadataEnvelope::XlsxShared {
                        shared_index: id.shared_index,
                        parsed_range: Some(range),
                    },
                }
            } else {
                FormulaSourceKind::SharedDescendant {
                    family: id,
                    metadata: FormulaMetadataEnvelope::XlsxShared {
                        shared_index: id.shared_index,
                        parsed_range: None,
                    },
                }
            },
            cached: SourceCachedValue::AbsentOrEmpty,
        })
        .collect();
    (FormulaIngestBatch::new("Sheet1", records), events)
}

#[test]
fn authoritative_direct_source_promotes_vertical_horizontal_and_rectangular_sets() {
    for coords in [
        vec![(0, 0), (1, 0)],
        vec![(0, 0), (0, 1)],
        vec![(0, 0), (0, 1), (1, 0), (1, 1)],
    ] {
        let cfg = EvalConfig::default()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
        let mut engine = Engine::new(TestWorkbook::default(), cfg);
        let formulas = vec!["=1+1"; coords.len()];
        let (batch, events) = direct_source_family(
            &mut engine,
            SourceFamilyId {
                sheet_instance: 0,
                shared_index: 41,
            },
            &coords,
            &formulas,
        );
        let report = engine
            .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
                batch, events,
            )])
            .unwrap();
        assert_eq!(report.source_family_promoted, 1);
        assert_eq!(report.source_family_promoted_cells, coords.len() as u64);
        assert_eq!(report.source_family_fallback_cells, 0);
        assert_eq!(
            report.source_family_promoted_cells + report.source_family_fallback_cells,
            report.source_family_cells_seen
        );
        assert_eq!(report.graph_formula_cells_materialized, 0);
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
        engine.evaluate_all().unwrap();
        for (row, col) in coords {
            assert_eq!(
                engine.get_cell_value("Sheet1", row + 1, col + 1),
                Some(LiteralValue::Number(2.0))
            );
        }
    }
}

#[test]
fn source_direct_force_fallback_and_canonical_rejection_are_atomic() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut forced = Engine::new(TestWorkbook::default(), cfg.clone());
    forced.force_source_family_fallback_for_test(true);
    let forced_coords: Vec<_> = (0..100).map(|row| (row, 0)).collect();
    let forced_formulas = vec!["=1+1"; forced_coords.len()];
    let (batch, events) = direct_source_family(
        &mut forced,
        SourceFamilyId {
            sheet_instance: 9,
            shared_index: 1,
        },
        &forced_coords,
        &forced_formulas,
    );
    let forced_report = forced
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            batch, events,
        )])
        .unwrap();
    assert_eq!(forced_report.source_family_promoted, 0);
    assert_eq!(forced_report.source_family_fallback_cells, 100);
    assert_eq!(forced_report.graph_formula_cells_materialized, 100);
    assert_eq!(forced.baseline_stats().formula_plane_active_span_count, 0);

    let mut mismatch = Engine::new(TestWorkbook::default(), cfg);
    let coords = [(0, 0), (0, 1)];
    let mismatch_formulas = ["=1+1", "=SUM(1,1)"];
    let (batch, events) = direct_source_family(
        &mut mismatch,
        SourceFamilyId {
            sheet_instance: 9,
            shared_index: 999,
        },
        &coords,
        &mismatch_formulas,
    );
    let report = mismatch
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            batch, events,
        )])
        .unwrap();
    assert_eq!(report.source_family_promoted, 0, "{report:?}");
    assert_eq!(report.source_family_fallback_cells, 2, "{report:?}");
    assert_eq!(report.fallback_reasons.get("CanonicalMismatch"), Some(&1));
    assert_eq!(report.graph_formula_cells_materialized, 2);
    assert_eq!(mismatch.baseline_stats().formula_plane_active_span_count, 0);
}

#[test]
fn collector_rejected_vertical_family_is_reserved_from_ordinary_authority() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let coords: Vec<_> = (0..100).map(|row| (row, 0)).collect();
    let formulas = vec!["=1+1"; coords.len()];
    let family = SourceFamilyId {
        sheet_instance: 0,
        shared_index: 314,
    };
    let (batch, mut events) = direct_source_family(&mut engine, family, &coords, &formulas);
    events[0].formula = FormulaSourceKind::SharedDescendant {
        family,
        metadata: FormulaMetadataEnvelope::XlsxShared {
            shared_index: family.shared_index,
            parsed_range: None,
        },
    };

    let report = engine
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            batch, events,
        )])
        .unwrap();
    assert_eq!(report.fallback_reasons.get("MissingAnchor"), Some(&1));
    assert_eq!(report.source_family_promoted, 0);
    assert_eq!(report.source_family_fallback_cells, 100);
    assert_eq!(report.graph_formula_cells_materialized, 100);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
}

#[test]
fn eligible_family_with_missing_analyzed_record_gets_no_partial_authority() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let coords: Vec<_> = (0..100).map(|row| (row, 0)).collect();
    let formulas = vec!["=1+1"; coords.len()];
    let (mut batch, events) = direct_source_family(
        &mut engine,
        SourceFamilyId {
            sheet_instance: 0,
            shared_index: 315,
        },
        &coords,
        &formulas,
    );
    batch.formulas.pop();

    let report = engine
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            batch, events,
        )])
        .unwrap();
    assert_eq!(
        report.fallback_reasons.get("ExactRecordResolution"),
        Some(&1)
    );
    assert_eq!(report.source_family_promoted, 0);
    assert_eq!(report.source_family_fallback_cells, 100);
    assert_eq!(report.graph_formula_cells_materialized, 99);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
}

#[test]
fn source_family_identity_does_not_merge_equal_templates() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let mut source_batches = Vec::new();
    for (shared_index, coords) in [(7, [(0, 0), (0, 1)]), (8, [(1, 0), (1, 1)])] {
        let formulas = ["=1+1", "=1+1"];
        let (batch, events) = direct_source_family(
            &mut engine,
            SourceFamilyId {
                sheet_instance: 0,
                shared_index,
            },
            &coords,
            &formulas,
        );
        source_batches.push(crate::engine::FormulaSourceIngestBatch::new(batch, events));
    }
    let report = engine
        .ingest_formula_source_batches(source_batches)
        .unwrap();
    assert_eq!(report.source_family_promoted, 2);
    assert_eq!(report.source_family_promoted_cells, 4);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
}

#[test]
fn source_direct_current_small_domain_gate_falls_back_without_partial_authority() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(3.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(4.0))
        .unwrap();
    let coords = [(9, 0), (9, 1)];
    let formulas = ["=A1+1", "=B1+1"];
    let (batch, events) = direct_source_family(
        &mut engine,
        SourceFamilyId {
            sheet_instance: 0,
            shared_index: 44,
        },
        &coords,
        &formulas,
    );
    let report = engine
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            batch, events,
        )])
        .unwrap();
    assert_eq!(report.source_family_promoted, 0, "{report:?}");
    assert_eq!(report.source_family_fallback_cells, 2, "{report:?}");
    assert_eq!(
        report.fallback_reasons.get("ExistingPlacementGate"),
        Some(&1)
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 1),
        Some(LiteralValue::Number(4.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 10, 2),
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn authoritative_direct_source_eager_deferred_parity() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let coords = [(0, 0), (0, 1)];
    let formulas = ["=1+1", "=1+1"];
    let family = SourceFamilyId {
        sheet_instance: 0,
        shared_index: 70,
    };

    let mut eager = Engine::new(TestWorkbook::default(), cfg.clone());
    let (batch, events) = direct_source_family(&mut eager, family, &coords, &formulas);
    let eager_report = eager
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            batch,
            events.clone(),
        )])
        .unwrap();

    let mut deferred = Engine::new(TestWorkbook::default(), cfg);
    for (&(row, col), formula) in coords.iter().zip(formulas) {
        deferred.stage_formula_text("Sheet1", row + 1, col + 1, formula.to_string());
    }
    deferred.stage_formula_source_events("Sheet1", events);
    deferred.build_graph_all().unwrap();
    assert_eq!(deferred.last_formula_ingest_report(), Some(&eager_report));
    assert_eq!(eager_report.source_family_promoted, 1);

    eager.evaluate_all().unwrap();
    deferred.evaluate_all().unwrap();
    for &(row, col) in &coords {
        assert_eq!(
            eager.get_cell_value("Sheet1", row + 1, col + 1),
            deferred.get_cell_value("Sheet1", row + 1, col + 1)
        );
    }
}

#[test]
fn source_direct_analysis_rejection_falls_back_the_entire_family() {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    let coords = [(0, 0), (0, 1)];
    let formulas = ["=RAND()", "=1+1"];
    let (batch, events) = direct_source_family(
        &mut engine,
        SourceFamilyId {
            sheet_instance: 0,
            shared_index: 91,
        },
        &coords,
        &formulas,
    );
    let report = engine
        .ingest_formula_source_batches(vec![crate::engine::FormulaSourceIngestBatch::new(
            batch, events,
        )])
        .unwrap();
    assert_eq!(report.source_family_promoted, 0);
    assert_eq!(report.source_family_fallback_cells, 2);
    assert_eq!(report.graph_formula_cells_materialized, 2);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
}
