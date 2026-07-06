//! W0 oracle harness (P3 recon follow-up): pins the two behaviors that the
//! WholeAll-after-structural-op crutch currently masks, for every
//! structural-op outcome class:
//!
//! (a) `*_post_op_values_without_eval` — structural op, then read values
//!     WITHOUT an intervening `evaluate_all`: pins what the op itself must
//!     leave behind (relocated inputs, retained above-boundary results,
//!     span-ON == span-OFF for everything else).
//! (b) `*_incremental_write_after_relocation` — structural op, evaluate,
//!     targeted incremental write to a precedent (including moved absolute
//!     targets), evaluate: pins incremental dirty routing through relocated
//!     geometry — the class that produced the origin-follows and
//!     rewrite-summary staleness bugs (issue #168 review).
//!
//! Outcome classes covered (row axis; the column-axis classes are pinned by
//! the edge matrix in `formula_plane_structural_split_acceptance.rs`):
//! NoOp, Shift with origin following (stationary relative reads), Shift with
//! pinned origin (mixed reads), Shift with template rewrite (displaced
//! absolute), Split, delete-compaction, and whole-span demote.
//!
//! Every probe asserts full-rect span-ON == span-OFF parity PLUS hardcoded
//! oracle values: parity alone is blind to corruption shared by both modes
//! (see the oracle block in `formula_plane_structural_split_acceptance.rs`).
//!
//! Formula-overlay punch-outs (`FormulaOverlayEntryKind`) have no public
//! construction path today (only unit tests build them via
//! `FormulaPlane::insert_overlay`), so the punch-out domain-relocation gap
//! flagged in the P3 recon is not reachable through this harness and has no
//! repro test here.

use std::sync::Arc;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const SHEET: &str = "Sheet1";

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

fn engine_pair() -> (Engine<TestWorkbook>, Engine<TestWorkbook>) {
    let on = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental),
    );
    let off = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::Off),
    );
    (on, off)
}

fn assert_rect_parity(
    span_on: &Engine<TestWorkbook>,
    span_off: &Engine<TestWorkbook>,
    rows: u32,
    cols: u32,
) {
    for row in 1..=rows {
        for col in 1..=cols {
            assert_eq!(
                span_on.get_cell_value(SHEET, row, col),
                span_off.get_cell_value(SHEET, row, col),
                "span-on/off divergence at row={row} col={col}"
            );
        }
    }
}

fn num(value: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(value))
}

// ---------------------------------------------------------------------------
// Class fixtures. Each builder ingests the same workload into both engines
// and runs the baseline evaluate_all.
// ---------------------------------------------------------------------------

/// Flagship mixed-read family: `C{r} = A{r}*B{r}*$F$1` over `rows`, with
/// `A{r} = r`, `B{r} = 2r`, `F1 = 3`, and a tail `SUM` over the whole span
/// output in E1 (a legacy consumer of span results).
fn build_flagship(engine: &mut Engine<TestWorkbook>, span_start: u32, span_end: u32) {
    engine.add_sheet(SHEET).ok();
    engine
        .set_cell_value(SHEET, 1, 6, LiteralValue::Number(3.0))
        .unwrap();
    let mut formulas = Vec::new();
    for r in span_start..=span_end {
        engine
            .set_cell_value(SHEET, r, 1, LiteralValue::Number(r as f64))
            .unwrap();
        engine
            .set_cell_value(SHEET, r, 2, LiteralValue::Number((2 * r) as f64))
            .unwrap();
        formulas.push(record(engine, r, 3, &format!("=A{r}*B{r}*$F$1")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .unwrap();
    engine
        .set_cell_formula(
            SHEET,
            1,
            5,
            parse(&format!("=SUM(C{span_start}:C{span_end})")).unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
}

fn flagship_c(r: u32, scalar: f64) -> Option<LiteralValue> {
    num(r as f64 * (2 * r) as f64 * scalar)
}

fn flagship_pair(span_start: u32, span_end: u32) -> (Engine<TestWorkbook>, Engine<TestWorkbook>) {
    let (mut on, mut off) = engine_pair();
    build_flagship(&mut on, span_start, span_end);
    build_flagship(&mut off, span_start, span_end);
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 1);
    (on, off)
}

// ---------------------------------------------------------------------------
// Class 1: NoOp — insert below the span; nothing about the span changes.
// ---------------------------------------------------------------------------

#[test]
fn w0_noop_post_op_values_without_eval() {
    let (mut on, mut off) = flagship_pair(2, 121);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 130, 2).unwrap();
    }
    // The op boundary (row 130) is below everything: the whole span's
    // computed values must survive the op itself, readable with NO eval.
    for r in [2u32, 60, 121] {
        assert_eq!(on.get_cell_value(SHEET, r, 3), flagship_c(r, 3.0));
    }
    assert_rect_parity(&on, &off, 135, 6);
}

#[test]
fn w0_noop_incremental_write_after_relocation() {
    let (mut on, mut off) = flagship_pair(2, 121);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 130, 2).unwrap();
        engine.evaluate_all().unwrap();
        engine
            .set_cell_value(SHEET, 51, 1, LiteralValue::Number(1000.0))
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(on.get_cell_value(SHEET, 51, 3), num(1000.0 * 102.0 * 3.0));
    assert_rect_parity(&on, &off, 135, 6);
}

// ---------------------------------------------------------------------------
// Class 2: Shift with the origin FOLLOWING the block (all relative reads
// stationary): span rows 150..=270 read A10..A130 via `=A{r-140}`; the
// insert at 140 sits between the reads and the span.
// ---------------------------------------------------------------------------

fn build_origin_follows(engine: &mut Engine<TestWorkbook>) {
    engine.add_sheet(SHEET).ok();
    let mut formulas = Vec::new();
    for row in 10..=130 {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
    }
    for row in 150..=270 {
        formulas.push(record(engine, row, 3, &format!("=A{}", row - 140)));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
}

#[test]
fn w0_shift_origin_follows_post_op_values_without_eval() {
    let (mut on, mut off) = engine_pair();
    build_origin_follows(&mut on);
    build_origin_follows(&mut off);
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 1);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 140, 1).unwrap();
    }
    // Inputs sit entirely above the boundary: values must survive in place.
    assert_eq!(on.get_cell_value(SHEET, 10, 1), num(10.0));
    assert_eq!(on.get_cell_value(SHEET, 130, 1), num(130.0));
    assert_rect_parity(&on, &off, 275, 6);
}

#[test]
fn w0_shift_origin_follows_incremental_write_after_relocation() {
    let (mut on, mut off) = engine_pair();
    build_origin_follows(&mut on);
    build_origin_follows(&mut off);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 140, 1).unwrap();
        engine.evaluate_all().unwrap();
        engine
            .set_cell_value(SHEET, 10, 1, LiteralValue::Number(999.0))
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 1);
    // Original C150 (reading A10) moved to row 151 and must observe the write.
    assert_eq!(on.get_cell_value(SHEET, 151, 3), num(999.0));
    assert_rect_parity(&on, &off, 275, 6);
}

// ---------------------------------------------------------------------------
// Class 3: Shift with the origin PINNED (mixed reads: relative inputs shift
// with the block, absolute $F$1 stays): insert between the scalar and the
// span.
// ---------------------------------------------------------------------------

#[test]
fn w0_shift_origin_pinned_post_op_values_without_eval() {
    let (mut on, mut off) = flagship_pair(3, 122);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 2, 1).unwrap();
    }
    // The scalar sits above the boundary and must survive in place; the
    // relocated inputs carry their values without an eval.
    assert_eq!(on.get_cell_value(SHEET, 1, 6), num(3.0));
    assert_eq!(on.get_cell_value(SHEET, 4, 1), num(3.0)); // original A3
    assert_eq!(on.get_cell_value(SHEET, 123, 2), num(244.0)); // original B122
    assert_rect_parity(&on, &off, 128, 6);
}

#[test]
fn w0_shift_origin_pinned_incremental_write_after_relocation() {
    let (mut on, mut off) = flagship_pair(3, 122);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 2, 1).unwrap();
        engine.evaluate_all().unwrap();
        // Write BOTH kinds of precedent: the stationary absolute target and
        // a relocated relative input.
        engine
            .set_cell_value(SHEET, 1, 6, LiteralValue::Number(5.0))
            .unwrap();
        engine
            .set_cell_value(SHEET, 61, 1, LiteralValue::Number(1000.0))
            .unwrap(); // original A60
        engine.evaluate_all().unwrap();
    }
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 1);
    // Original row 60 now sits at 61: A was overwritten, B kept 2*60.
    assert_eq!(on.get_cell_value(SHEET, 61, 3), num(1000.0 * 120.0 * 5.0));
    // An untouched row must track only the scalar change.
    assert_eq!(on.get_cell_value(SHEET, 11, 3), flagship_c(10, 5.0));
    assert_rect_parity(&on, &off, 128, 6);
}

// ---------------------------------------------------------------------------
// Class 4: Shift with template rewrite (displaced absolute): insert above
// everything; $F$1's target physically moves to F3.
// ---------------------------------------------------------------------------

#[test]
fn w0_shift_rewrite_post_op_values_without_eval() {
    let (mut on, mut off) = flagship_pair(2, 121);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 1, 2).unwrap();
    }
    // Everything is at/after the boundary: computed results are flushed in
    // both modes, but the raw inputs and the scalar must arrive relocated
    // with their values, with NO eval.
    assert_eq!(on.get_cell_value(SHEET, 3, 6), num(3.0)); // scalar at F3
    assert_eq!(on.get_cell_value(SHEET, 4, 1), num(2.0)); // original A2
    assert_eq!(on.get_cell_value(SHEET, 123, 2), num(242.0)); // original B121
    assert_rect_parity(&on, &off, 128, 6);
}

#[test]
fn w0_shift_rewrite_incremental_write_after_relocation() {
    let (mut on, mut off) = flagship_pair(2, 121);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 1, 2).unwrap();
        engine.evaluate_all().unwrap();
        // Write the MOVED absolute target (now F3) and a relocated input.
        engine
            .set_cell_value(SHEET, 3, 6, LiteralValue::Number(5.0))
            .unwrap();
        engine
            .set_cell_value(SHEET, 54, 1, LiteralValue::Number(1000.0))
            .unwrap(); // original A52
        engine.evaluate_all().unwrap();
    }
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(on.get_cell_value(SHEET, 54, 3), num(1000.0 * 104.0 * 5.0));
    assert_eq!(on.get_cell_value(SHEET, 4, 3), flagship_c(2, 5.0));
    assert_rect_parity(&on, &off, 128, 6);
}

// ---------------------------------------------------------------------------
// Class 5: Split — mid-span insert; upper half keeps the span id, lower half
// is a fresh shifted span.
// ---------------------------------------------------------------------------

#[test]
fn w0_split_post_op_values_without_eval() {
    let (mut on, mut off) = flagship_pair(2, 121);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 60, 1).unwrap();
    }
    // Above the boundary the computed results must survive the op itself.
    for r in [2u32, 30, 59] {
        assert_eq!(on.get_cell_value(SHEET, r, 3), flagship_c(r, 3.0));
    }
    // Relocated inputs below the boundary carry values without an eval.
    assert_eq!(on.get_cell_value(SHEET, 61, 1), num(60.0)); // original A60
    assert_rect_parity(&on, &off, 128, 6);
}

#[test]
fn w0_split_incremental_write_after_relocation() {
    let (mut on, mut off) = flagship_pair(2, 121);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 60, 1).unwrap();
        engine.evaluate_all().unwrap();
        // One precedent in each half, plus the shared absolute scalar.
        engine
            .set_cell_value(SHEET, 10, 1, LiteralValue::Number(100.0))
            .unwrap(); // upper half input
        engine
            .set_cell_value(SHEET, 101, 1, LiteralValue::Number(200.0))
            .unwrap(); // lower half input (original A100)
        engine
            .set_cell_value(SHEET, 1, 6, LiteralValue::Number(5.0))
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 2);
    assert_eq!(on.get_cell_value(SHEET, 10, 3), num(100.0 * 20.0 * 5.0));
    assert_eq!(on.get_cell_value(SHEET, 101, 3), num(200.0 * 200.0 * 5.0));
    // Untouched rows in each half track only the scalar change.
    assert_eq!(on.get_cell_value(SHEET, 30, 3), flagship_c(30, 5.0));
    assert_eq!(on.get_cell_value(SHEET, 122, 3), flagship_c(121, 5.0));
    assert_rect_parity(&on, &off, 128, 6);
}

// ---------------------------------------------------------------------------
// Class 6: delete-compaction — delete strictly inside a pure-relative span;
// the span survives with a compacted domain.
// ---------------------------------------------------------------------------

fn build_relative_double(engine: &mut Engine<TestWorkbook>) {
    engine.add_sheet(SHEET).ok();
    let mut formulas = Vec::new();
    for r in 2..=121 {
        engine
            .set_cell_value(SHEET, r, 1, LiteralValue::Number(r as f64))
            .unwrap();
        formulas.push(record(engine, r, 3, &format!("=A{r}*2")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
}

#[test]
fn w0_delete_compaction_post_op_values_without_eval() {
    let (mut on, mut off) = engine_pair();
    build_relative_double(&mut on);
    build_relative_double(&mut off);
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 1);
    for engine in [&mut on, &mut off] {
        engine.delete_rows(SHEET, 60, 1).unwrap();
    }
    // Above the boundary the computed results must survive the op itself,
    // and relocated inputs below carry their values.
    assert_eq!(on.get_cell_value(SHEET, 10, 3), num(20.0));
    assert_eq!(on.get_cell_value(SHEET, 59, 3), num(118.0));
    assert_eq!(on.get_cell_value(SHEET, 60, 1), num(61.0)); // original A61
    assert_rect_parity(&on, &off, 125, 6);
}

#[test]
fn w0_delete_compaction_incremental_write_after_relocation() {
    let (mut on, mut off) = engine_pair();
    build_relative_double(&mut on);
    build_relative_double(&mut off);
    for engine in [&mut on, &mut off] {
        engine.delete_rows(SHEET, 60, 1).unwrap();
        engine.evaluate_all().unwrap();
        engine
            .set_cell_value(SHEET, 10, 1, LiteralValue::Number(100.0))
            .unwrap(); // above the deleted band
        engine
            .set_cell_value(SHEET, 100, 1, LiteralValue::Number(500.0))
            .unwrap(); // below (original row 101)
        engine.evaluate_all().unwrap();
    }
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(on.get_cell_value(SHEET, 10, 3), num(200.0));
    assert_eq!(on.get_cell_value(SHEET, 100, 3), num(1000.0));
    // An untouched compacted row: original row 80 now sits at 79.
    assert_eq!(on.get_cell_value(SHEET, 79, 3), num(160.0));
    assert_rect_parity(&on, &off, 125, 6);
}

// ---------------------------------------------------------------------------
// Class 7: whole-span demote — the absolute target sits inside the span's
// row range and the insert lands exactly on it (target-in-inserted-gap).
// ---------------------------------------------------------------------------

fn build_in_span_absolute(engine: &mut Engine<TestWorkbook>) {
    engine.add_sheet(SHEET).ok();
    engine
        .set_cell_value(SHEET, 100, 6, LiteralValue::Number(3.0))
        .unwrap();
    let mut formulas = Vec::new();
    for r in 2..=201 {
        engine
            .set_cell_value(SHEET, r, 1, LiteralValue::Number(r as f64))
            .unwrap();
        formulas.push(record(engine, r, 3, &format!("=A{r}*$F$100")));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .unwrap();
    engine.evaluate_all().unwrap();
}

#[test]
fn w0_demote_post_op_values_without_eval() {
    let (mut on, mut off) = engine_pair();
    build_in_span_absolute(&mut on);
    build_in_span_absolute(&mut off);
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 1);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 100, 1).unwrap();
    }
    // Above the boundary the computed results must survive the op itself.
    assert_eq!(on.get_cell_value(SHEET, 10, 3), num(30.0));
    assert_eq!(on.get_cell_value(SHEET, 99, 3), num(297.0));
    // The scalar's value physically moved to F101.
    assert_eq!(on.get_cell_value(SHEET, 101, 6), num(3.0));
    assert_rect_parity(&on, &off, 208, 6);
}

#[test]
fn w0_demote_incremental_write_after_relocation() {
    let (mut on, mut off) = engine_pair();
    build_in_span_absolute(&mut on);
    build_in_span_absolute(&mut off);
    for engine in [&mut on, &mut off] {
        engine.insert_rows(SHEET, 100, 1).unwrap();
        engine.evaluate_all().unwrap();
        // Write the MOVED absolute target (now F101) and one input.
        engine
            .set_cell_value(SHEET, 101, 6, LiteralValue::Number(7.0))
            .unwrap();
        engine
            .set_cell_value(SHEET, 10, 1, LiteralValue::Number(100.0))
            .unwrap();
        engine.evaluate_all().unwrap();
    }
    // Demoted: no active spans, values on the per-cell path.
    assert_eq!(on.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(on.get_cell_value(SHEET, 10, 3), num(700.0));
    assert_eq!(on.get_cell_value(SHEET, 99, 3), num(99.0 * 7.0));
    // Original row 150 (shifted to 151) tracks the new scalar.
    assert_eq!(on.get_cell_value(SHEET, 151, 3), num(150.0 * 7.0));
    assert_rect_parity(&on, &off, 208, 6);
}
