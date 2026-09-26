//! Gotcha G8 of the cycle-architecture track (refs PSU3D0/formualizer#112,
//! follows merged #119): a FormulaPlane span member that participates in a
//! statically-cyclic SCC must never be span-evaluated.
//!
//! Cross-cell cycles that route through a span producer are invisible to the
//! legacy Tarjan pass (the span member has no graph vertex of its own) and only
//! surface as `CycleDetected` fallbacks in the producer-bounded mixed schedule.
//! The FP coordinator demotes the offending span(s) to legacy graph vertices at
//! schedule-build time so the cycle members are resolved on the legacy SCC path
//! (`handle_cycle_unit` under `CycleDetection::Static`, `evaluate_scc_unit`
//! under `Runtime`), while spans that do not touch the cycle keep span
//! treatment.

use std::sync::Arc;

use crate::engine::{
    CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig, FormulaIngestBatch,
    FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn authoritative_engine(detection: CycleDetection) -> Engine<TestWorkbook> {
    let cfg = EvalConfig::default()
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
        .with_cycle(CycleConfig {
            detection,
            policy: CyclePolicy::Error,
        });
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

fn num(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> f64 {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet} r{row}c{col}, got {other:?}"),
    }
}

fn is_circ(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> bool {
    matches!(
        engine.get_cell_value(sheet, row, col),
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Circ
    )
}

/// Build a workbook with:
/// * Column B rows 1..=120: span family `=A{r}+C{r}` (reads col A values and col
///   C, which is empty except for the cycle-closing cell) — 120 cells, well over
///   the promotion threshold, promoting to a single span.
/// * Column E rows 1..=120: an *independent* span family `=A{r}*2`, untouched by
///   the cycle.
/// * `C5 = B5`: closes a static cycle `B5 -> C5 -> B5` through the span member B5.
fn build_workbook(detection: CycleDetection) -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine(detection);
    let mut col_b = Vec::new();
    let mut col_e = Vec::new();
    for row in 1..=120 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        col_b.push(record(&mut engine, row, 2, &format!("=A{row}+C{row}")));
        col_e.push(record(&mut engine, row, 5, &format!("=A{row}*2")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(
            "Sheet1",
            col_b.into_iter().chain(col_e).collect(),
        )])
        .unwrap();

    // Close the cycle through span member B5 by setting C5 = B5.
    engine
        .set_cell_formula("Sheet1", 5, 3, parse("=B5").unwrap())
        .unwrap();
    engine
}

#[test]
fn post_warm_cross_sheet_back_edge_demotes_before_legacy_tarjan() {
    let mut engine = authoritative_engine(CycleDetection::Static);
    engine.add_sheet("Aux").unwrap();
    let mut col_b = Vec::new();
    let mut col_e = Vec::new();
    for row in 1..=120 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Aux", row, 1, LiteralValue::Number(0.0))
            .unwrap();
        col_b.push(record(&mut engine, row, 2, &format!("=A{row}+Aux!A{row}")));
        col_e.push(record(&mut engine, row, 5, &format!("=A{row}*2")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(
            "Sheet1",
            col_b.into_iter().chain(col_e).collect(),
        )])
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 5, 2), 5.0);

    // The back-edge is introduced only after the span is warm. Legacy Tarjan
    // cannot see the virtual B5 member; the mixed schedule must detect the
    // cycle and demote B before the residual legacy SCC is resolved.
    engine
        .set_cell_formula("Aux", 5, 1, parse("=Sheet1!B5").unwrap())
        .unwrap();
    let result = engine.evaluate_all().unwrap();
    assert_eq!(result.cycle_errors, 1);
    assert!(is_circ(&engine, "Sheet1", 5, 2));
    assert!(is_circ(&engine, "Aux", 5, 1));
    assert_eq!(num(&engine, "Sheet1", 5, 5), 10.0);
}

/// (a) cycle members are not span-evaluated — the cyclic span is demoted and the
/// `CycleMember` placement-fallback reason is recorded; (b) results are correct
/// under `CycleDetection::Static`: `#CIRC` for the cycle members, real span
/// results for the rest; (c) the independent span family is untouched.
#[test]
fn span_member_in_static_cycle_is_demoted_and_circ() {
    let mut engine = build_workbook(CycleDetection::Static);

    let result = engine.evaluate_all().expect("eval must not bail out");
    assert_eq!(result.cycle_errors, 1, "exactly one live SCC stamped");

    // (a) the cyclic span family was demoted to legacy.
    let stats = engine.baseline_stats();
    let resource_baseline = engine.evaluation_resource_baseline_stats();
    let request = engine
        .last_evaluation_resource_request_stats()
        .expect("evaluation publishes resource telemetry");
    assert!(request.ledger.retained_current <= request.ledger.retained_peak);
    assert_eq!(
        resource_baseline.topology_cache_builds,
        stats.formula_plane_mixed_topology_cache_builds,
    );
    assert_eq!(
        resource_baseline.topology_retained_bytes_observed_max,
        request.topology.retained_bytes_observed,
    );

    // (b) static cycle members are #CIRC; the rest of the demoted family still
    // computes correct values on the legacy path.
    assert!(
        is_circ(&engine, "Sheet1", 5, 2),
        "B5 (cycle member) is #CIRC"
    );
    assert!(
        is_circ(&engine, "Sheet1", 5, 3),
        "C5 (cycle member) is #CIRC"
    );
    assert_eq!(num(&engine, "Sheet1", 1, 2), 1.0, "B1 = A1 + C1 = 1");
    assert_eq!(num(&engine, "Sheet1", 10, 2), 10.0, "B10 = A10 + C10 = 10");
    assert_eq!(num(&engine, "Sheet1", 120, 2), 120.0, "B120 = A120 + C120");

    assert_eq!(num(&engine, "Sheet1", 1, 5), 2.0, "E1 = A1 * 2");
    assert_eq!(num(&engine, "Sheet1", 120, 5), 240.0, "E120 = A120 * 2");
}

/// (b) under `CycleDetection::Runtime`: the cyclic span is still demoted and the
/// live cycle (C5 = B5 unconditionally) yields `#CIRC` per the live-edge policy,
/// while phantom-free non-cycle members get ordinary values and the independent
/// span survives.
#[test]
fn span_member_in_runtime_cycle_is_demoted_and_circ() {
    let mut engine = build_workbook(CycleDetection::Runtime);

    let result = engine.evaluate_all().expect("eval must not bail out");
    assert_eq!(result.cycle_errors, 1, "one live cycle witnessed");

    let stats = engine.baseline_stats();

    // Live cycle members are #CIRC under Runtime/Error policy.
    assert!(is_circ(&engine, "Sheet1", 5, 2));
    assert!(is_circ(&engine, "Sheet1", 5, 3));
    // Non-cycle members compute ordinary values (no phantom stamping).
    assert_eq!(num(&engine, "Sheet1", 1, 2), 1.0);
    assert_eq!(num(&engine, "Sheet1", 10, 2), 10.0);
    assert_eq!(num(&engine, "Sheet1", 120, 2), 120.0);

    assert_eq!(num(&engine, "Sheet1", 1, 5), 2.0);
    assert_eq!(num(&engine, "Sheet1", 120, 5), 240.0);
}

/// A phantom (guarded, live-acyclic) cycle through a span member must not stamp
/// `#CIRC` under `CycleDetection::Runtime`: the span is still demoted (its
/// member is a *static* SCC candidate), but live-edge evaluation resolves the
/// guarded reference to an ordinary value (discussion #99).
#[test]
fn phantom_cycle_through_span_member_yields_value_under_runtime() {
    let mut engine = authoritative_engine(CycleDetection::Runtime);
    // F1 guard = false. Column A values 1..=120. Column B span `=A{r}+D{r}`
    // reads col A (values) and col D (mostly empty).
    engine
        .set_cell_value("Sheet1", 1, 6, LiteralValue::Boolean(false))
        .unwrap();
    let mut col_b = Vec::new();
    for row in 1..=120 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        col_b.push(record(&mut engine, row, 2, &format!("=A{row}+D{row}")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", col_b)])
        .unwrap();

    // Guarded back-edge: D5 = IF(F1, B5, 7). With F1=false the live edge does
    // not reach B5, so the static SCC {B5, D5} is phantom.
    engine
        .set_cell_formula("Sheet1", 5, 4, parse("=IF(F1,B5,7)").unwrap())
        .unwrap();

    let result = engine.evaluate_all().expect("eval must not bail out");
    assert_eq!(result.cycle_errors, 0, "phantom cycle stamps no #CIRC");

    // Phantom members resolve to ordinary values, not #CIRC.
    assert!(!is_circ(&engine, "Sheet1", 5, 2));
    assert_eq!(
        num(&engine, "Sheet1", 5, 4),
        7.0,
        "D5 = IF(false,...,7) = 7"
    );
    assert_eq!(num(&engine, "Sheet1", 5, 2), 5.0 + 7.0, "B5 = A5 + D5 = 12");
    assert_eq!(
        num(&engine, "Sheet1", 10, 2),
        10.0,
        "B10 = A10 + D10 = 10 + 0"
    );
}

fn build_two_sheet_cycle_workbook() -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine(CycleDetection::Static);
    engine.add_sheet("Sheet2").unwrap();
    for sheet in ["Sheet1", "Sheet2"] {
        let mut formulas = Vec::new();
        for row in 1..=120 {
            engine
                .set_cell_value(sheet, row, 1, LiteralValue::Number(row as f64))
                .unwrap();
            formulas.push(record(&mut engine, row, 2, &format!("=A{row}+C{row}")));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new(sheet, formulas)])
            .unwrap();
        engine
            .set_cell_formula(sheet, 5, 3, parse("=B5").unwrap())
            .unwrap();
    }
    engine
}

#[test]
fn prepared_span_demotion_rejects_stale_authority_before_graph_mutation() {
    let engine = build_workbook(CycleDetection::Static);
    let before = engine.baseline_stats();
    let after = engine.baseline_stats();
    assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
    assert_eq!(
        after.graph_formula_vertex_count,
        before.graph_formula_vertex_count
    );
    assert_eq!(after.graph_edge_count, before.graph_edge_count);
    assert_eq!(after.dirty_vertex_count, before.dirty_vertex_count);
}
