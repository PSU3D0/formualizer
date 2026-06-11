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
fn build_mixed_engine(tail_formula: impl Fn(u32) -> String) -> Engine<TestWorkbook> {
    let config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut formulas = Vec::with_capacity(2 * ROWS as usize);
    for row in 1..=ROWS {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 2, &format!("=A{row}+1")));
        formulas.push(record(&mut engine, row, 4, &tail_formula(row)));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .expect("ingest formulas");
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
    let mut engine = build_mixed_engine(|row| format!("=SUM($A{row}:$A${ROWS})"));

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

#[test]
fn capacity_fallback_bails_to_legacy_once_per_eval_not_demote_spin() {
    // Two-column tail reads stay rect-bucketed (a real rect has no precise
    // interval-tree form), so the cumulative candidate cap legitimately
    // trips. The coordinator must fail over to the legacy primitive exactly
    // once per evaluate_all — the cyclic-span demote loop cannot make
    // progress on capacity fallbacks and used to spin its full 64-iteration
    // budget (each iteration an O(graph) schedule build + Tarjan prepass).
    let mut engine = build_mixed_engine(|row| format!("=SUM($A{row}:$B${ROWS})"));

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.formula_plane_capacity_bailouts(),
        1,
        "capacity fallback must bail exactly once, not iterate the demote loop"
    );

    // The legacy bail cannot evaluate span cells; the follow-up recalc
    // (WholeAll seeding, no dirty legacy producers) picks them up.
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.formula_plane_capacity_bailouts(),
        1,
        "span-only follow-up schedule must be authoritative-safe"
    );
    assert!(
        engine.last_formula_plane_span_eval_report().is_some(),
        "follow-up recalc must evaluate the spans the bail pass skipped"
    );

    for row in [1, 2, ROWS / 2, ROWS] {
        assert_eq!(numeric_value(&engine, row, 2), row as f64 + 1.0);
    }
    // KNOWN PRE-EXISTING GAP (not pinned here): the legacy tail readers in
    // column D evaluated during the bail pass while the span cells in column
    // B were still empty, and computed-overlay flushes do not re-dirty
    // dependent legacy vertices, so their values remain stale
    // (`tail_sum(A)` only) on every subsequent quiescent recalc. The old
    // demote-spin path reached the identical state after its 64 iterations;
    // fixing it requires the capacity-bail path to either demote affected
    // spans or re-dirty legacy readers of span result regions.
}
