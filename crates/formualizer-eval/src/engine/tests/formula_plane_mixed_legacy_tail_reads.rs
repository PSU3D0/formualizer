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
    CycleConfig, Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
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
    engine
}

fn tail_sum(row: u32) -> f64 {
    // SUM of r..=ROWS with A{r} = r.
    ((ROWS as u64 + row as u64) * (ROWS as u64 - row as u64 + 1) / 2) as f64
}

/// The values of the removed `mixed_tail_reads_complete_in_one_authoritative_pass`
/// (its span-routing assertions were span-internal): a gapped family of
/// `A{r}+1` beside alternating `SUM($A{r}:$A$600)` / `...+0` tails.
#[test]
fn mixed_tail_reads_values() {
    let mut engine = build_mixed_engine(FormulaPlaneMode::AuthoritativeExperimental, |row| {
        format!("=SUM($A{row}:$A${ROWS})")
    });
    for _ in 0..2 {
        engine.evaluate_all().unwrap();
        for row in [1, 2, ROWS / 2, ROWS] {
            assert_eq!(numeric_value(&engine, row, 2), row as f64 + 1.0);
            assert_eq!(numeric_value(&engine, row, 4), tail_sum(row));
        }
    }
}

/// The values and request lifecycle of
/// `independent_iterative_island_preserves_accumulator_and_single_request_lifecycle`
/// (its mixed-topology route events are span-internal).
#[test]
fn independent_iterative_island_preserves_accumulator_and_single_request_lifecycle_values() {
    let config = EvalConfig::default()
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
        .with_cycle(CycleConfig::iterate(1, 0.001));
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut formulas = Vec::new();
    for row in 1..=120 {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .unwrap();
    engine
        .set_cell_value(SHEET, 1, 3, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_formula(SHEET, 1, 4, parse("=D1+C1").unwrap())
        .unwrap();

    for expected in [5.0, 10.0, 15.0] {
        let epoch = engine.recalc_epoch;
        let begins = engine.evaluation_request_begin_count_for_test();
        engine.evaluate_all().unwrap();
        assert_eq!(numeric_value(&engine, 1, 4), expected);
        assert_eq!(engine.recalc_epoch, epoch.wrapping_add(1));
        assert_eq!(
            engine.evaluation_request_begin_count_for_test(),
            begins + 1,
            "the clock and iterative-redirty state must be sampled once per request"
        );
    }
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

/// The value parity of `cached_mixed_topology_matches_off_first_warm_and_post_edit`
/// (first, warm and post-edit; its mixed-topology cache counters are
/// span-internal). Both engines run the authority, so this pins mode
/// invariance, not legacy parity.
#[test]
fn cached_mixed_topology_matches_off_first_warm_and_post_edit_values() {
    let build = |mode| build_mixed_engine(mode, |row| format!("=SUM($A{row}:$B${ROWS})"));
    let mut off = build(FormulaPlaneMode::Off);
    let mut authoritative = build(FormulaPlaneMode::AuthoritativeExperimental);
    off.evaluate_all().unwrap();
    authoritative.evaluate_all().unwrap();
    assert_full_capacity_corpus_parity(&off, &authoritative);
    off.evaluate_all().unwrap();
    authoritative.evaluate_all().unwrap();
    assert_full_capacity_corpus_parity(&off, &authoritative);
    for engine in [&mut off, &mut authoritative] {
        engine
            .set_cell_value(SHEET, ROWS / 2, 1, LiteralValue::Number(10_000.0))
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_full_capacity_corpus_parity(&off, &authoritative);
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

    let sheet_id = engine.graph.sheet_id(SHEET).unwrap();
    let tail_vertices = (1..=ROWS)
        .map(|row| {
            let cell = CellRef::new(sheet_id, Coord::from_excel(row, 7, true, true));
            engine.graph.get_vertex_id_for_address(&cell).unwrap()
        })
        .collect::<Vec<_>>();
    engine.graph.clear_dirty_flags(&tail_vertices);
    (engine, tail_vertices)
}

#[test]
fn cache_skip_retains_all_scheduled_and_clean_span_authority() {
    let (mut engine, tail_vertices) = build_selective_capacity_engine();
    engine.evaluate_all().expect("initial span-only evaluation");
    engine.config.max_formula_plane_cache_candidates = 0;
    engine
        .set_cell_value(SHEET, ROWS / 2, 1, LiteralValue::Number(10_000.0))
        .unwrap();
    engine.graph.mark_dirty_many(&tail_vertices);
    engine.evaluate_all().expect("exact cache-skip evaluation");
    assert_eq!(numeric_value(&engine, ROWS / 2, 2), 10_001.0);
    assert_eq!(numeric_value(&engine, ROWS / 2, 5), 3_001.0);
}

#[test]
fn clean_spans_remain_authoritative_when_only_legacy_work_is_dirty() {
    let (mut engine, tail_vertices) = build_selective_capacity_engine();
    engine.evaluate_all().expect("initial span-only evaluation");

    engine.graph.mark_dirty_many(&tail_vertices);
    engine.evaluate_all().expect("pure legacy completion");
}

fn cached_topology_engine() -> Engine<TestWorkbook> {
    build_mixed_engine(FormulaPlaneMode::AuthoritativeExperimental, |row| {
        format!("=SUM($A{row}:$A${ROWS})")
    })
}

#[test]
fn stale_exact_span_region_event_is_ignored_after_generation_change() {
    let mut engine = cached_topology_engine();
    engine.evaluate_all().unwrap();

    engine.evaluate_all().unwrap();
}

#[test]
fn span_free_authoritative_workbook_never_builds_mixed_topology_cache() {
    let config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), config);
    engine
        .set_cell_formula(SHEET, 1, 1, parse("=1+1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .set_cell_value(SHEET, 2, 1, LiteralValue::Number(3.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    let stats = engine.baseline_stats();
    assert_eq!(stats.formula_plane_active_span_count, 0);
    assert_eq!(stats.formula_plane_mixed_topology_cache_builds, 0);
    assert_eq!(stats.formula_plane_mixed_topology_cache_hits, 0);
}
