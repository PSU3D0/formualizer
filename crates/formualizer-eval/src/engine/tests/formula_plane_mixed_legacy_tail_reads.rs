//! Mixed-mode legacy-interaction regression net (perf):
//! legacy RANGE-READING cells coexisting with ACTIVE SPANS.
//!
//! Measured bug: on a workbook mixing span-accepted formulas with legacy
//! tail-range readers (`=SUM($A{r}:$A$N)`), authoritative mode was ~50x
//! slower than `Off`. Chain:
//!
//! 1. `shared_range_to_region_pattern` mapped finite single-column reads to
//!    `Region::rect`, whose degenerate `Span(c, c)` col axis routed them into
//!    the coarse 64x16 rect buckets of `SheetRegionIndex` instead of the
//!    per-column interval trees. Every legacy producer's point-result query
//!    in the same bucket column then collected O(overlapping tail reads)
//!    candidates (all dropped by the exact filter), tripping the mixed
//!    scheduler's `max_candidates` fail-closed cap.
//! 2. The resulting `MaxCandidatesExceeded` fallback made the schedule
//!    non-authoritative-safe, and the only non-safe handler — the cyclic-span
//!    demote loop — cannot make progress on capacity fallbacks. It rebuilt
//!    the identical schedule `MAX_CYCLE_DEMOTE_ITERS` (64) times (each with a
//!    full legacy Tarjan prepass) before bailing to the legacy primitive,
//!    which never evaluates span cells, so the *next* recalc re-evaluated
//!    every span whole.
//!
//! These tests assert behavior shape via reports/counters, never wall time:
//! - the mixed corpus completes in ONE authoritative pass (span eval report
//!   present, zero capacity bailouts);
//! - a quiescent recalc does not re-evaluate spans;
//! - a corpus that legitimately trips the candidate cap bails to legacy
//!   exactly once per evaluate_all instead of spinning the demote loop.

use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;

const SHEET: &str = "Sheet1";
/// Enough overlapping tail reads that cumulative pre-filter candidates would
/// exceed the scheduler's `max_candidates = 100_000` cap under the old rect
/// bucketing (sum 1..=600 of O(r) candidates ≈ 180k), and comfortably above
/// the 100-cell non-constant span promotion threshold.
const ROWS: u32 = 600;

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"));
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn numeric_value(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> f64 {
    match engine
        .get_cell_value(SHEET, row, col)
        .unwrap_or_else(|| panic!("missing {SHEET}!R{row}C{col}"))
    {
        LiteralValue::Int(value) => value as f64,
        LiteralValue::Number(value) => value,
        value => panic!("expected numeric {SHEET}!R{row}C{col}, got {value:?}"),
    }
}

/// `A{r} = r`; span-accepted `B{r} = A{r}+1`; legacy tail readers in the
/// given column reading the given range template.
///
/// Mixed-anchor tail-read families are span-supported now, so a uniform
/// `=SUM($A{r}:$A$N)` column would be promoted and stop exercising the
/// legacy-interaction path this net pins. Alternate odd rows to a structurally
/// different but value-identical template (`...+0`): each resulting family has
/// row gaps (`UnsupportedShapeOrGaps`), keeping all tail readers legacy while
/// preserving the original read-region geometry and candidate counts.
fn build_mixed_engine(
    mode: FormulaPlaneMode,
    tail_formula: impl Fn(u32) -> String,
) -> Engine<TestWorkbook> {
    let config = EvalConfig::default().with_formula_plane_mode(mode);
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut formulas = Vec::with_capacity(2 * ROWS as usize);
    for row in 1..=ROWS {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
        let tail = if row % 2 == 0 {
            format!("{}+0", tail_formula(row))
        } else {
            tail_formula(row)
        };
        formulas.push(record(&mut engine, row, 4, &tail));
    }
    let report = engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .expect("ingest formulas");
    if mode == FormulaPlaneMode::AuthoritativeExperimental {
        assert_eq!(
            report.shadow_accepted_span_cells,
            u64::from(ROWS),
            "only the B column family may span; tail readers must stay legacy \
             (histogram: {:?})",
            report.fallback_reasons
        );
    }
    engine
}

fn tail_sum(row: u32) -> f64 {
    // SUM of r..=ROWS with A{r} = r.
    ((ROWS as u64 + row as u64) * (ROWS as u64 - row as u64 + 1) / 2) as f64
}

#[test]
fn mixed_tail_reads_complete_in_one_authoritative_pass() {
    // Single-column tail reads: with degenerate-span normalization these
    // index as per-column intervals, so legacy point-result queries on other
    // columns see zero candidates and the schedule stays authoritative-safe.
    let mut engine = build_mixed_engine(FormulaPlaneMode::AuthoritativeExperimental, |row| {
        format!("=SUM($A{row}:$A${ROWS})")
    });

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.formula_plane_capacity_bailouts(),
        0,
        "single-column tail reads must not trip the candidate cap"
    );
    assert!(
        engine.last_formula_plane_span_eval_report().is_some(),
        "first evaluate_all must run the authoritative mixed pass \
         (a None report means it bailed to the legacy primitive)"
    );

    for row in [1, 2, ROWS / 2, ROWS] {
        assert_eq!(numeric_value(&engine, row, 2), row as f64 + 1.0);
        assert_eq!(numeric_value(&engine, row, 4), tail_sum(row));
    }

    // Quiescent recalc: nothing dirty, no pending changed regions — spans
    // must NOT be re-evaluated whole (the failed first pass used to leave
    // `formula_plane_indexes_epoch_seen` stale, forcing WholeAll re-eval).
    engine.evaluate_all().unwrap();
    assert!(
        engine.last_formula_plane_span_eval_report().is_none(),
        "quiescent recalc must not re-evaluate spans"
    );
    assert_eq!(engine.formula_plane_capacity_bailouts(), 0);
}

fn assert_full_capacity_corpus_parity(
    off: &Engine<TestWorkbook>,
    authoritative: &Engine<TestWorkbook>,
) {
    for row in 1..=ROWS {
        for col in [2, 4] {
            assert_eq!(
                authoritative.get_cell_value(SHEET, row, col),
                off.get_cell_value(SHEET, row, col),
                "Off/authoritative mismatch at {SHEET}!R{row}C{col}"
            );
        }
    }
}

#[test]
fn capacity_fallback_matches_off_first_warm_and_post_edit() {
    let build = |mode| build_mixed_engine(mode, |row| format!("=SUM($A{row}:$B${ROWS})"));
    let mut off = build(FormulaPlaneMode::Off);
    let mut authoritative = build(FormulaPlaneMode::AuthoritativeExperimental);

    let default_fallback_cap = authoritative
        .workbook_load_limits()
        .max_formula_plane_fallback_cells;
    assert!(default_fallback_cap < u64::MAX);
    assert!(
        default_fallback_cap >= u64::from(ROWS),
        "the finite default fallback cap must admit the #144 corpus"
    );

    off.evaluate_all().unwrap();
    authoritative.evaluate_all().unwrap();
    assert_eq!(authoritative.formula_plane_capacity_bailouts(), 1);
    assert_eq!(
        authoritative
            .baseline_stats()
            .formula_plane_active_span_count,
        0,
        "first-eval WholeAll must select and demote every scheduled span"
    );
    assert_full_capacity_corpus_parity(&off, &authoritative);

    off.evaluate_all().unwrap();
    authoritative.evaluate_all().unwrap();
    assert_eq!(
        authoritative.formula_plane_capacity_bailouts(),
        1,
        "successful fallback telemetry increments once, not on warm legacy recalc"
    );
    assert_full_capacity_corpus_parity(&off, &authoritative);

    for engine in [&mut off, &mut authoritative] {
        engine
            .set_cell_value(SHEET, ROWS / 2, 1, LiteralValue::Number(10_000.0))
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(authoritative.formula_plane_capacity_bailouts(), 1);
    assert_full_capacity_corpus_parity(&off, &authoritative);
}

#[derive(Debug, PartialEq, Eq)]
struct CapacityOverlaySnapshot {
    overlay_ref: crate::formula_plane::runtime::FormulaOverlayRef,
    id: crate::formula_plane::runtime::FormulaOverlayEntryId,
    generation: u32,
    sheet_id: crate::SheetId,
    domain: crate::formula_plane::runtime::PlacementDomain,
    source_span: Option<crate::formula_plane::runtime::FormulaSpanRef>,
    kind: crate::formula_plane::runtime::FormulaOverlayEntryKind,
    created_epoch: u64,
}

fn add_capacity_source_overlay(engine: &mut Engine<TestWorkbook>) {
    use crate::formula_plane::runtime::{FormulaOverlayEntryKind, PlacementDomain};

    let span_ref = engine.graph.formula_authority().active_span_refs()[0];
    let sheet_id = engine
        .graph
        .formula_authority()
        .plane
        .spans
        .get(span_ref)
        .unwrap()
        .sheet_id;
    engine.graph.formula_authority_mut().plane.insert_overlay(
        sheet_id,
        PlacementDomain::row_run(sheet_id, 9, 9, 1),
        FormulaOverlayEntryKind::ValueOverride,
        Some(span_ref),
    );
    engine.graph.formula_authority_mut().rebuild_indexes();
}

fn capacity_overlay_snapshot(engine: &Engine<TestWorkbook>) -> Vec<CapacityOverlaySnapshot> {
    engine
        .graph
        .formula_authority()
        .plane
        .formula_overlay
        .active_entries()
        .map(|(entry, overlay_ref)| CapacityOverlaySnapshot {
            overlay_ref,
            id: entry.id,
            generation: entry.generation,
            sheet_id: entry.sheet_id,
            domain: entry.domain.clone(),
            source_span: entry.source_span,
            kind: entry.kind.clone(),
            created_epoch: entry.created_epoch,
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct CapacityGraphVertexSnapshot {
    cell: crate::reference::CellRef,
    vertex: crate::engine::VertexId,
    formula: Option<crate::engine::arena::AstNodeId>,
    dependencies: Vec<crate::engine::VertexId>,
    dirty: bool,
}

fn capacity_graph_snapshot(engine: &Engine<TestWorkbook>) -> Vec<CapacityGraphVertexSnapshot> {
    let mut snapshot = engine
        .graph
        .cell_to_vertex()
        .iter()
        .map(|(&cell, &vertex)| {
            let mut dependencies = engine.graph.get_dependencies(vertex);
            dependencies.sort_unstable();
            CapacityGraphVertexSnapshot {
                cell,
                vertex,
                formula: engine.graph.get_formula_id(vertex),
                dependencies,
                dirty: engine.graph.is_dirty(vertex),
            }
        })
        .collect::<Vec<_>>();
    snapshot.sort_unstable_by_key(|entry| entry.cell);
    snapshot
}

fn capacity_visible_snapshot(
    engine: &Engine<TestWorkbook>,
) -> Vec<(u32, u32, Option<LiteralValue>)> {
    let mut values = Vec::with_capacity(ROWS as usize * 3);
    for row in 1..=ROWS {
        for col in [1, 2, 4] {
            values.push((row, col, engine.get_cell_value(SHEET, row, col)));
        }
    }
    values
}

#[test]
fn fallback_cap_failure_retains_exact_state_and_pending_lease_for_retry() {
    let mut engine = build_mixed_engine(FormulaPlaneMode::AuthoritativeExperimental, |row| {
        format!("=SUM($A{row}:$B${ROWS})")
    });
    add_capacity_source_overlay(&mut engine);
    let mut limits = engine.workbook_load_limits().clone();
    limits.max_formula_plane_fallback_cells = u64::from(ROWS - 1);
    engine.set_workbook_load_limits(limits);

    let refs = engine.graph.formula_authority().active_span_refs();
    let pending = engine
        .graph
        .formula_authority()
        .pending_changed_regions()
        .to_vec();
    let epochs = (
        engine.graph.formula_authority().plane.epoch(),
        engine.graph.formula_authority().indexes_epoch(),
        engine.graph.formula_authority().indexed_plane_epoch(),
    );
    let graph = capacity_graph_snapshot(&engine);
    let overlays = capacity_overlay_snapshot(&engine);
    let values = capacity_visible_snapshot(&engine);
    let before = engine.baseline_stats();

    assert!(engine.evaluate_all().is_err());
    let after = engine.baseline_stats();
    assert_eq!(engine.formula_plane_capacity_bailouts(), 0);
    assert_eq!(engine.graph.formula_authority().active_span_refs(), refs);
    assert_eq!(
        engine.graph.formula_authority().pending_changed_regions(),
        pending
    );
    assert_eq!(
        (
            engine.graph.formula_authority().plane.epoch(),
            engine.graph.formula_authority().indexes_epoch(),
            engine.graph.formula_authority().indexed_plane_epoch(),
        ),
        epochs
    );
    assert_eq!(capacity_graph_snapshot(&engine), graph);
    assert_eq!(capacity_overlay_snapshot(&engine), overlays);
    assert_eq!(capacity_visible_snapshot(&engine), values);
    assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
    assert_eq!(
        after.graph_formula_vertex_count,
        before.graph_formula_vertex_count
    );
    assert_eq!(after.graph_edge_count, before.graph_edge_count);
    assert_eq!(after.dirty_vertex_count, before.dirty_vertex_count);

    let mut limits = engine.workbook_load_limits().clone();
    limits.max_formula_plane_fallback_cells = u64::from(ROWS);
    engine.set_workbook_load_limits(limits);
    engine
        .evaluate_all()
        .expect("raised-cap retry must succeed");
    assert_eq!(engine.formula_plane_capacity_bailouts(), 1);
    assert!(
        engine
            .graph
            .formula_authority()
            .pending_changed_regions()
            .is_empty()
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
}

#[test]
fn capacity_faults_retain_pending_lease_and_count_only_successful_retry() {
    use crate::engine::eval::{FormulaSpanDemotionFault, FormulaSpanDemotionFault::*};

    let faults: [FormulaSpanDemotionFault; 6] = [
        AstPreparation,
        LegacyGraphPreparation,
        FinalLegacyGraphValidation,
        FinalAuthorityValidation,
        AllocationReservation,
        BeforeFirstMutation,
    ];
    for fault in faults {
        let mut engine = build_mixed_engine(FormulaPlaneMode::AuthoritativeExperimental, |row| {
            format!("=SUM($A{row}:$B${ROWS})")
        });
        add_capacity_source_overlay(&mut engine);
        let refs = engine.graph.formula_authority().active_span_refs();
        let pending = engine
            .graph
            .formula_authority()
            .pending_changed_regions()
            .to_vec();
        let epochs = (
            engine.graph.formula_authority().plane.epoch(),
            engine.graph.formula_authority().indexes_epoch(),
            engine.graph.formula_authority().indexed_plane_epoch(),
        );
        let graph = capacity_graph_snapshot(&engine);
        let overlays = capacity_overlay_snapshot(&engine);
        let values = capacity_visible_snapshot(&engine);

        engine.set_formula_span_demotion_fault_for_test(fault);
        assert!(engine.evaluate_all().is_err(), "fault {fault:?} must fail");
        assert_eq!(engine.formula_plane_capacity_bailouts(), 0);
        assert_eq!(engine.graph.formula_authority().active_span_refs(), refs);
        assert_eq!(
            engine.graph.formula_authority().pending_changed_regions(),
            pending
        );
        assert_eq!(
            (
                engine.graph.formula_authority().plane.epoch(),
                engine.graph.formula_authority().indexes_epoch(),
                engine.graph.formula_authority().indexed_plane_epoch(),
            ),
            epochs
        );
        assert_eq!(capacity_graph_snapshot(&engine), graph);
        assert_eq!(capacity_overlay_snapshot(&engine), overlays);
        assert_eq!(capacity_visible_snapshot(&engine), values);

        engine.evaluate_all().expect("fault retry must succeed");
        assert_eq!(engine.formula_plane_capacity_bailouts(), 1);
        assert!(
            engine
                .graph
                .formula_authority()
                .pending_changed_regions()
                .is_empty()
        );
    }
}

fn build_selective_capacity_engine() -> (Engine<TestWorkbook>, Vec<crate::engine::VertexId>) {
    use crate::reference::{CellRef, Coord};

    let config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut formulas = Vec::with_capacity(3 * ROWS as usize);
    for row in 1..=ROWS {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value(SHEET, row, 3, LiteralValue::Number((row * 10) as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
        formulas.push(record(&mut engine, row, 5, &format!("=C{row}+1")));
        let tail = if row % 2 == 0 {
            format!("=SUM($A{row}:$B${ROWS})+0")
        } else {
            format!("=SUM($A{row}:$B${ROWS})")
        };
        formulas.push(record(&mut engine, row, 7, &tail));
    }
    let report = engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .unwrap();
    assert_eq!(report.shadow_accepted_span_cells, u64::from(2 * ROWS));

    let sheet_id = engine.graph.sheet_id(SHEET).unwrap();
    let tail_vertices = (1..=ROWS)
        .map(|row| {
            let cell = CellRef::new(sheet_id, Coord::from_excel(row, 7, true, true));
            *engine.graph.get_vertex_id_for_address(&cell).unwrap()
        })
        .collect::<Vec<_>>();
    engine.graph.clear_dirty_flags(&tail_vertices);
    (engine, tail_vertices)
}

#[test]
fn capacity_fallback_demotes_only_scheduled_dirty_span() {
    use crate::formula_plane::runtime::PlacementCoord;

    let (mut engine, tail_vertices) = build_selective_capacity_engine();
    engine.evaluate_all().expect("initial span-only evaluation");
    assert_eq!(engine.formula_plane_capacity_bailouts(), 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 2);

    let topology_epoch = engine.topology_epoch_for_test();
    engine
        .set_cell_value(SHEET, ROWS / 2, 1, LiteralValue::Number(10_000.0))
        .unwrap();
    engine.graph.mark_dirty_many(&tail_vertices);
    engine.evaluate_all().expect("selective capacity fallback");

    assert_eq!(engine.formula_plane_capacity_bailouts(), 1);
    assert_eq!(engine.topology_epoch_for_test(), topology_epoch + 1);
    let refs = engine.graph.formula_authority().active_span_refs();
    assert_eq!(refs.len(), 1, "the unrelated clean span must remain active");
    let survivor = engine
        .graph
        .formula_authority()
        .plane
        .spans
        .get(refs[0])
        .unwrap();
    assert!(
        survivor
            .domain
            .contains(PlacementCoord::new(survivor.sheet_id, ROWS / 2 - 1, 4,)),
        "the clean column-E span must retain authority"
    );
    assert_eq!(numeric_value(&engine, ROWS / 2, 2), 10_001.0);
    assert_eq!(numeric_value(&engine, ROWS / 2, 5), 3_001.0);
    assert!(
        engine
            .graph
            .formula_authority()
            .pending_changed_regions()
            .is_empty()
    );
}

#[test]
fn clean_spans_remain_authoritative_when_only_legacy_work_is_dirty() {
    let (mut engine, tail_vertices) = build_selective_capacity_engine();
    engine.evaluate_all().expect("initial span-only evaluation");
    let refs = engine.graph.formula_authority().active_span_refs();

    engine.graph.mark_dirty_many(&tail_vertices);
    engine.evaluate_all().expect("pure legacy completion");

    assert_eq!(engine.formula_plane_capacity_bailouts(), 0);
    assert_eq!(engine.graph.formula_authority().active_span_refs(), refs);
    assert!(
        engine
            .graph
            .formula_authority()
            .pending_changed_regions()
            .is_empty()
    );
}
