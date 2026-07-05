//! Shadow-parity acceptance harness for row structural ops against a
//! ColRun-spanned formula family.
//!
//! These tests currently pass because FormulaPlane demotes the span to
//! per-vertex legacy formulas on any row structural op that touches its
//! sheet (conservative correctness path — see
//! `formula_plane_insert_rows_conservatively_redirties_sheet_spans` in
//! `formula_plane_ingest_shadow.rs`). They are pinned here as the **Split
//! work acceptance harness**: once row insert/delete gain span-aware
//! splitting (instead of blanket demotion), these same value assertions must
//! still hold — span-ON full-sheet values must stay byte-identical to
//! span-OFF for every one of these structural-op shapes. If Split changes
//! behavior, these tests are the contract that must not regress.
//!
//! Workload: header/scalar row 1 (`$F$1`), a ColRun-eligible formula family
//! `C{r} = A{r}*B{r}*$F$1` spanning rows 2..=N+1, and a tail read
//! `SUM(C2:C{N+1})` that consumes the whole span output. N is kept small so
//! the suite stays fast; the standing performance probe
//! (`probe-fp-structural`) covers the same shapes at 200k+ rows.

use std::sync::Arc;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

const SHEET: &str = "Sheet1";
const N: u32 = 200;
const SPAN_START: u32 = 2;

fn span_end() -> u32 {
    SPAN_START + N - 1
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

fn build_engine(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let cfg = EvalConfig::default().with_formula_plane_mode(mode);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet(SHEET).ok();

    // Scalar multiplier read by every row in the span.
    engine
        .set_cell_value(SHEET, 1, 6, LiteralValue::Number(3.0))
        .unwrap(); // F1

    let mut formulas = Vec::with_capacity(N as usize);
    for r in SPAN_START..=span_end() {
        engine
            .set_cell_value(SHEET, r, 1, LiteralValue::Number(r as f64))
            .unwrap(); // A{r}
        engine
            .set_cell_value(SHEET, r, 2, LiteralValue::Number((r * 2) as f64))
            .unwrap(); // B{r}
        formulas.push(record(&mut engine, r, 3, &format!("=A{r}*B{r}*$F$1"))); // C{r}
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .expect("ingest span formulas");

    // Tail read: SUM over the whole span output, in a separate cell (E1).
    engine
        .set_cell_formula(
            SHEET,
            1,
            5,
            parse(format!("=SUM(C{}:C{})", SPAN_START, span_end())).unwrap(),
        )
        .unwrap();

    engine.evaluate_all().expect("baseline evaluate_all");
    engine
}

/// Snapshot every cell in the rectangle rows `1..=rows_hi`, cols `1..=6`
/// (A..F), which covers the whole fixture plus row-shift slack.
fn snapshot(engine: &Engine<TestWorkbook>, rows_hi: u32) -> Vec<Option<LiteralValue>> {
    let mut out = Vec::new();
    for r in 1..=rows_hi {
        for c in 1..=6u32 {
            out.push(engine.get_cell_value(SHEET, r, c));
        }
    }
    out
}

/// Apply the given structural op identically to both a span-ON and a
/// span-OFF engine, then assert the full-sheet snapshots are identical
/// after the op and after `evaluate_all()`.
fn assert_structural_op_parity(op: impl Fn(&mut Engine<TestWorkbook>)) {
    let rows_hi = span_end() + 8; // slack for count-up-to-3 inserts

    let mut off = build_engine(FormulaPlaneMode::Off);
    let mut on = build_engine(FormulaPlaneMode::AuthoritativeExperimental);

    op(&mut off);
    op(&mut on);

    off.evaluate_all().expect("post-op evaluate_all (off)");
    on.evaluate_all().expect("post-op evaluate_all (on)");

    let off_snapshot = snapshot(&off, rows_hi);
    let on_snapshot = snapshot(&on, rows_hi);

    assert_eq!(
        off_snapshot, on_snapshot,
        "span-ON vs span-OFF diverged after structural op"
    );
}

#[test]
fn split_acceptance_insert_mid_span() {
    assert_structural_op_parity(|engine| {
        let mid = SPAN_START + N / 2;
        engine.insert_rows(SHEET, mid, 1).unwrap();
    });
}

#[test]
fn split_acceptance_insert_at_span_first_row() {
    assert_structural_op_parity(|engine| {
        engine.insert_rows(SHEET, SPAN_START, 1).unwrap();
    });
}

#[test]
fn split_acceptance_insert_just_past_span_end() {
    assert_structural_op_parity(|engine| {
        // One row below the span's last row: does not shift any span row,
        // but a naive structural op still walks every formula vertex.
        engine.insert_rows(SHEET, span_end() + 1, 1).unwrap();
    });
}

#[test]
fn split_acceptance_insert_count_three_mid_span() {
    assert_structural_op_parity(|engine| {
        let mid = SPAN_START + N / 2;
        engine.insert_rows(SHEET, mid, 3).unwrap();
    });
}

#[test]
fn split_acceptance_insert_before_row_zero() {
    // 0-based `before = 0`: insert above everything, including the header
    // row holding `$F$1` (1-based `before = 1`).
    assert_structural_op_parity(|engine| {
        engine.insert_rows(SHEET, 1, 1).unwrap();
    });
}

#[test]
fn split_acceptance_delete_overlapping_span_first_row() {
    assert_structural_op_parity(|engine| {
        engine.delete_rows(SHEET, SPAN_START, 1).unwrap();
    });
}

#[test]
fn split_acceptance_delete_overlapping_span_last_row() {
    assert_structural_op_parity(|engine| {
        engine.delete_rows(SHEET, span_end(), 1).unwrap();
    });
}

#[test]
fn split_acceptance_delete_strictly_inside_span() {
    assert_structural_op_parity(|engine| {
        let mid = SPAN_START + N / 2;
        engine.delete_rows(SHEET, mid, 1).unwrap();
    });
}
