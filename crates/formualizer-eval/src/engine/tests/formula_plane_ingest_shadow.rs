use std::sync::Arc;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn record(row: u32, col: u32, formula: &str) -> FormulaIngestRecord {
    FormulaIngestRecord::new(
        row,
        col,
        parse(formula).unwrap(),
        Some(Arc::<str>::from(formula)),
    )
}

#[test]
fn formula_plane_off_ingest_reports_graph_materialized_formulas() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    let report = engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(
            "Sheet1",
            vec![record(1, 1, "=1+1"), record(2, 1, "=A1+1")],
        )])
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
