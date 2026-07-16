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
    // Both families promote to spans before the cycle is introduced.
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);

    // Close the cycle through span member B5 by setting C5 = B5.
    engine
        .set_cell_formula("Sheet1", 5, 3, parse("=B5").unwrap())
        .unwrap();
    // Setting an out-of-span cell does not eagerly demote: the cycle is only
    // observable once the mixed producer schedule exists at eval time.
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine
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
    assert_eq!(request.topology.cache_build_events, 2);
    assert_eq!(request.topology.cache_hit_events, 0);
    assert_eq!(request.topology.cache_skip_events, 0);
    assert_eq!(
        request.ledger.retained_peak, request.topology.retained_bytes_observed,
        "cycle retry cache replacement must not add both retained topologies",
    );
    assert!(request.ledger.retained_current <= request.ledger.retained_peak);
    assert_eq!(request.topology.producers_observed, 125);
    assert_eq!(request.topology.candidates_observed, 4);
    assert_eq!(request.topology.edges_observed, 4);
    assert_eq!(
        resource_baseline.topology_cache_builds,
        stats.formula_plane_mixed_topology_cache_builds,
    );
    assert_eq!(resource_baseline.topology_candidates_observed_total, 4);
    assert_eq!(resource_baseline.topology_edges_observed_total, 4);
    assert_eq!(
        resource_baseline.topology_retained_bytes_observed_max,
        request.topology.retained_bytes_observed,
    );
    assert_eq!(
        stats.formula_plane_cycle_member_span_demotions, 1,
        "the column-B span must be demoted for cycle membership"
    );
    // The CycleMember fallback reason is recorded in the cumulative ingest
    // report like every other placement fallback reason.
    assert_eq!(
        engine
            .formula_ingest_report_total()
            .fallback_reasons
            .get("CycleMember")
            .copied(),
        Some(1),
        "CycleMember fallback must be recorded in diagnostics"
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

    // (c) the independent column-E span family is unaffected: still a span and
    // still correct.
    assert_eq!(
        stats.formula_plane_active_span_count, 1,
        "the independent column-E span survives"
    );
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
    assert_eq!(stats.formula_plane_cycle_member_span_demotions, 1);
    assert_eq!(
        engine
            .formula_ingest_report_total()
            .fallback_reasons
            .get("CycleMember")
            .copied(),
        Some(1)
    );

    // Live cycle members are #CIRC under Runtime/Error policy.
    assert!(is_circ(&engine, "Sheet1", 5, 2));
    assert!(is_circ(&engine, "Sheet1", 5, 3));
    // Non-cycle members compute ordinary values (no phantom stamping).
    assert_eq!(num(&engine, "Sheet1", 1, 2), 1.0);
    assert_eq!(num(&engine, "Sheet1", 10, 2), 10.0);
    assert_eq!(num(&engine, "Sheet1", 120, 2), 120.0);

    // Independent span family survives and is correct.
    assert_eq!(stats.formula_plane_active_span_count, 1);
    assert_eq!(num(&engine, "Sheet1", 1, 5), 2.0);
    assert_eq!(num(&engine, "Sheet1", 120, 5), 240.0);
}

#[test]
fn cycle_retry_lease_extension_preserves_later_identical_span_event() {
    let mut engine = build_workbook(CycleDetection::Static);
    engine.rerecord_cycle_retry_span_after_lease_extension_for_test();

    engine
        .evaluate_all()
        .expect("cycle evaluation must succeed");

    let surviving_span = active_span_refs_by_sheet(&engine);
    assert_eq!(surviving_span.len(), 1);
    assert_eq!(
        engine
            .graph
            .pending_formula_dirty_whole_spans()
            .collect::<Vec<_>>(),
        surviving_span,
        "an identical event recorded after renewal must remain pending"
    );
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 1);

    engine
        .evaluate_all()
        .expect("retained event must be consumable on retry");
    assert_eq!(engine.graph.pending_formula_dirty_event_count(), 0);
    assert_eq!(
        engine
            .last_formula_plane_span_eval_report()
            .expect("retained whole-span event must schedule the survivor")
            .span_eval_placement_count,
        120
    );
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
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);

    // Guarded back-edge: D5 = IF(F1, B5, 7). With F1=false the live edge does
    // not reach B5, so the static SCC {B5, D5} is phantom.
    engine
        .set_cell_formula("Sheet1", 5, 4, parse("=IF(F1,B5,7)").unwrap())
        .unwrap();

    let result = engine.evaluate_all().expect("eval must not bail out");
    assert_eq!(result.cycle_errors, 0, "phantom cycle stamps no #CIRC");

    // The span was still demoted for static cycle membership.
    assert_eq!(
        engine
            .baseline_stats()
            .formula_plane_cycle_member_span_demotions,
        1
    );
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
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);
    engine
}

#[derive(Debug, PartialEq, Eq)]
struct OverlaySnapshot {
    overlay_ref: crate::formula_plane::runtime::FormulaOverlayRef,
    id: crate::formula_plane::runtime::FormulaOverlayEntryId,
    generation: u32,
    sheet_id: crate::SheetId,
    domain: crate::formula_plane::runtime::PlacementDomain,
    source_span: Option<crate::formula_plane::runtime::FormulaSpanRef>,
    kind: crate::formula_plane::runtime::FormulaOverlayEntryKind,
    created_epoch: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct GraphVertexSnapshot {
    cell: crate::reference::CellRef,
    vertex: crate::engine::VertexId,
    kind: crate::engine::VertexKind,
    formula: Option<crate::engine::arena::AstNodeId>,
    dependencies: Vec<crate::engine::VertexId>,
    dependents: Vec<crate::engine::VertexId>,
    dirty: bool,
    flags: u8,
}

#[derive(Debug, PartialEq)]
struct DemotionStateSnapshot {
    span_refs: Vec<crate::formula_plane::runtime::FormulaSpanRef>,
    plane_epoch: crate::formula_plane::runtime::FormulaPlaneEpoch,
    indexes_epoch: u64,
    indexed_plane_epoch: u64,
    overlays: Vec<OverlaySnapshot>,
    graph_vertices: Vec<GraphVertexSnapshot>,
    graph_counts: (usize, usize, usize, usize),
    visible_values: Vec<(String, u32, u32, Option<LiteralValue>)>,
    cycle_member_span_demotions: u64,
}

fn active_span_refs_by_sheet(
    engine: &Engine<TestWorkbook>,
) -> Vec<crate::formula_plane::runtime::FormulaSpanRef> {
    let authority = engine.graph.formula_authority();
    let mut refs = authority
        .active_span_refs()
        .into_iter()
        .map(|span_ref| {
            let sheet_id = authority.plane.spans.get(span_ref).unwrap().sheet_id;
            (sheet_id, span_ref)
        })
        .collect::<Vec<_>>();
    refs.sort_unstable_by_key(|(sheet_id, _)| *sheet_id);
    refs.into_iter().map(|(_, span_ref)| span_ref).collect()
}

fn add_two_sheet_source_overlays(engine: &mut Engine<TestWorkbook>) {
    use crate::formula_plane::runtime::{FormulaOverlayEntryKind, PlacementDomain};

    let refs = active_span_refs_by_sheet(engine);
    assert_eq!(refs.len(), 2);
    for (index, span_ref) in refs.into_iter().enumerate() {
        let sheet_id = engine
            .graph
            .formula_authority()
            .plane
            .spans
            .get(span_ref)
            .unwrap()
            .sheet_id;
        let (row, kind) = if index == 0 {
            (9, FormulaOverlayEntryKind::ValueOverride)
        } else {
            (10, FormulaOverlayEntryKind::Cleared)
        };
        engine.graph.formula_authority_mut().plane.insert_overlay(
            sheet_id,
            PlacementDomain::row_run(sheet_id, row, row, 1),
            kind,
            Some(span_ref),
        );
    }
    engine.graph.formula_authority_mut().rebuild_indexes();
}

fn snapshot_demotion_state(engine: &Engine<TestWorkbook>) -> DemotionStateSnapshot {
    let authority = engine.graph.formula_authority();
    let overlays = authority
        .plane
        .formula_overlay
        .active_entries()
        .map(|(entry, overlay_ref)| OverlaySnapshot {
            overlay_ref,
            id: entry.id,
            generation: entry.generation,
            sheet_id: entry.sheet_id,
            domain: entry.domain.clone(),
            source_span: entry.source_span,
            kind: entry.kind.clone(),
            created_epoch: entry.created_epoch,
        })
        .collect();

    let mut graph_vertices = engine
        .graph
        .cell_to_vertex()
        .iter()
        .map(|(&cell, &vertex)| {
            let mut dependencies = engine.graph.get_dependencies(vertex);
            dependencies.sort_unstable();
            let mut dependents = engine.graph.get_dependents(vertex);
            dependents.sort_unstable();
            GraphVertexSnapshot {
                cell,
                vertex,
                kind: engine.graph.get_vertex_kind(vertex),
                formula: engine.graph.get_formula_id(vertex),
                dependencies,
                dependents,
                dirty: engine.graph.is_dirty(vertex),
                flags: engine.graph.get_flags(vertex),
            }
        })
        .collect::<Vec<_>>();
    graph_vertices.sort_unstable_by_key(|snapshot| snapshot.cell);

    let mut visible_values = Vec::new();
    for sheet in ["Sheet1", "Sheet2"] {
        for (row, col) in [(5, 1), (5, 2), (5, 3), (10, 2), (11, 2), (120, 2)] {
            visible_values.push((
                sheet.to_string(),
                row,
                col,
                engine.get_cell_value(sheet, row, col),
            ));
        }
    }

    let stats = engine.baseline_stats();
    assert_eq!(
        graph_vertices.len(),
        stats.graph_vertex_count,
        "snapshot must cover every graph vertex"
    );
    DemotionStateSnapshot {
        span_refs: authority.active_span_refs(),
        plane_epoch: authority.plane.epoch(),
        indexes_epoch: authority.indexes_epoch(),
        indexed_plane_epoch: authority.indexed_plane_epoch(),
        overlays,
        graph_vertices,
        graph_counts: (
            stats.graph_vertex_count,
            stats.graph_formula_vertex_count,
            stats.graph_edge_count,
            stats.dirty_vertex_count,
        ),
        visible_values,
        cycle_member_span_demotions: stats.formula_plane_cycle_member_span_demotions,
    }
}

#[test]
fn two_sheet_span_demotion_fault_matrix_preserves_exact_transaction_state() {
    use crate::engine::eval::FormulaSpanDemotionFault;

    let faults = [
        FormulaSpanDemotionFault::AstPreparation,
        FormulaSpanDemotionFault::LegacyGraphPreparation,
        FormulaSpanDemotionFault::FinalLegacyGraphValidation,
        FormulaSpanDemotionFault::FinalAuthorityValidation,
        FormulaSpanDemotionFault::AllocationReservation,
        FormulaSpanDemotionFault::BeforeFirstMutation,
    ];
    for fault in faults {
        let mut engine = build_two_sheet_cycle_workbook();
        add_two_sheet_source_overlays(&mut engine);
        let refs = active_span_refs_by_sheet(&engine);
        let before = snapshot_demotion_state(&engine);

        engine.set_formula_span_demotion_fault_for_test(fault);
        let result = engine
            .prepare_formula_span_demotion(&refs)
            .and_then(|prepared| engine.commit_prepared_formula_span_demotion(prepared));
        assert!(result.is_err(), "fault {fault:?} must fail");
        assert_eq!(
            snapshot_demotion_state(&engine),
            before,
            "fault {fault:?} changed exact multi-sheet transaction state"
        );
    }
}

#[test]
fn stale_second_exact_ref_cannot_commit_first_span() {
    use crate::engine::eval::FormulaSpanDemotionError;

    let mut engine = build_two_sheet_cycle_workbook();
    add_two_sheet_source_overlays(&mut engine);
    let refs = active_span_refs_by_sheet(&engine);
    let prepared = engine.prepare_formula_span_demotion(&refs).unwrap();

    let first_ref = refs[0];
    let stale_second_ref = refs[1];
    assert!(
        engine
            .graph
            .formula_authority_mut()
            .plane
            .remove_span(stale_second_ref)
    );
    engine.graph.formula_authority_mut().rebuild_indexes();
    let before = snapshot_demotion_state(&engine);
    assert_eq!(before.span_refs, vec![first_ref]);

    assert!(matches!(
        engine.commit_prepared_formula_span_demotion(prepared),
        Err(FormulaSpanDemotionError::StaleAuthority)
    ));
    assert_eq!(snapshot_demotion_state(&engine), before);
    assert!(
        engine
            .graph
            .formula_authority()
            .plane
            .spans
            .get(first_ref)
            .is_some(),
        "the first exact ref must remain authoritative"
    );
}

#[test]
fn two_sheet_cyclic_demotion_is_one_atomic_batch() {
    use crate::engine::eval::FormulaSpanDemotionFault;

    let mut failed = build_two_sheet_cycle_workbook();
    let refs = failed.graph.formula_authority().active_span_refs();
    let pending = failed
        .graph
        .pending_formula_dirty_regions()
        .collect::<Vec<_>>()
        .to_vec();
    let before = failed.baseline_stats();
    failed.set_formula_span_demotion_fault_for_test(FormulaSpanDemotionFault::BeforeFirstMutation);
    assert!(failed.evaluate_all().is_err());
    assert_eq!(failed.graph.formula_authority().active_span_refs(), refs);
    assert_eq!(
        failed
            .graph
            .pending_formula_dirty_regions()
            .collect::<Vec<_>>(),
        pending
    );
    let after = failed.baseline_stats();
    assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
    assert_eq!(
        after.graph_formula_vertex_count,
        before.graph_formula_vertex_count
    );
    assert_eq!(after.graph_edge_count, before.graph_edge_count);
    assert_eq!(after.dirty_vertex_count, before.dirty_vertex_count);

    let mut committed = build_two_sheet_cycle_workbook();
    let result = committed.evaluate_all().expect("batch demotion commits");
    assert_eq!(result.cycle_errors, 2);
    assert_eq!(
        committed
            .baseline_stats()
            .formula_plane_cycle_member_span_demotions,
        2
    );
    assert!(
        committed
            .graph
            .formula_authority()
            .active_span_refs()
            .is_empty()
    );
    assert!(
        committed
            .graph
            .pending_formula_dirty_regions()
            .collect::<Vec<_>>()
            .is_empty(),
        "successful cyclic demotion plus legacy completion acknowledges the lease"
    );
    for sheet in ["Sheet1", "Sheet2"] {
        assert!(is_circ(&committed, sheet, 5, 2));
        assert!(is_circ(&committed, sheet, 5, 3));
        assert_eq!(num(&committed, sheet, 10, 2), 10.0);
    }
}

#[test]
fn prepared_span_demotion_rejects_stale_authority_before_graph_mutation() {
    let mut engine = build_workbook(CycleDetection::Static);
    let refs = engine.graph.formula_authority().active_span_refs();
    let prepared = engine.prepare_formula_span_demotion(&refs).unwrap();
    engine.graph.formula_authority_mut().rebuild_indexes();
    let before = engine.baseline_stats();
    let live_refs = engine.graph.formula_authority().active_span_refs();

    assert!(
        engine
            .commit_prepared_formula_span_demotion(prepared)
            .is_err()
    );
    let after = engine.baseline_stats();
    assert_eq!(
        engine.graph.formula_authority().active_span_refs(),
        live_refs
    );
    assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
    assert_eq!(
        after.graph_formula_vertex_count,
        before.graph_formula_vertex_count
    );
    assert_eq!(after.graph_edge_count, before.graph_edge_count);
    assert_eq!(after.dirty_vertex_count, before.dirty_vertex_count);
}

#[test]
fn exact_ref_preparation_rejects_invalid_generation_without_mutation() {
    let mut engine = build_workbook(CycleDetection::Static);
    let mut refs = engine.graph.formula_authority().active_span_refs();
    refs[0].generation = refs[0].generation.wrapping_add(1);
    let before = engine.baseline_stats();
    let live_refs = engine.graph.formula_authority().active_span_refs();

    assert!(engine.prepare_formula_span_demotion(&refs).is_err());
    let after = engine.baseline_stats();
    assert_eq!(
        engine.graph.formula_authority().active_span_refs(),
        live_refs
    );
    assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
    assert_eq!(
        after.graph_formula_vertex_count,
        before.graph_formula_vertex_count
    );
    assert_eq!(after.graph_edge_count, before.graph_edge_count);
    assert_eq!(after.dirty_vertex_count, before.dirty_vertex_count);
}

#[test]
fn span_demotion_preparation_checks_existing_load_limits_without_mutation() {
    let mut engine = build_workbook(CycleDetection::Static);
    let refs = engine.graph.formula_authority().active_span_refs();
    let before = engine.baseline_stats();
    let mut limits = engine.workbook_load_limits().clone();
    limits.max_sheet_rows = 100;
    engine.set_workbook_load_limits(limits);

    assert!(engine.prepare_formula_span_demotion(&refs).is_err());
    let after = engine.baseline_stats();
    assert_eq!(engine.graph.formula_authority().active_span_refs(), refs);
    assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
    assert_eq!(
        after.graph_formula_vertex_count,
        before.graph_formula_vertex_count
    );
    assert_eq!(after.graph_edge_count, before.graph_edge_count);
    assert_eq!(after.dirty_vertex_count, before.dirty_vertex_count);
}
