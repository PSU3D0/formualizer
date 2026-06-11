//! Mixed-anchor range span support: promotion, read-region geometry, dirty
//! precision, and the InternalDependency guard.
//!
//! Mixed-anchor ranges have one bound of an axis relative to the placement and
//! the other absolute. The two everyday idioms are:
//!
//! - tail reads `=SUM($A{r}:$A$N)` — per-placement read region [r..N]
//!   (shrinking), and
//! - running totals `=SUM($B$2:$B{r})` — per-placement read region [2..r]
//!   (expanding).
//!
//! Both are affine in the placement index, so the dirty inversion ("changed
//! rows [a, b] -> which placements read it?") is a half-open placement
//! interval. These tests pin (counter-style, never wall time):
//!
//! - both idioms promote to spans and evaluate to legacy-identical values;
//! - the span's union read region is the full single-column bounding interval
//!   (a `col_interval`, which `SheetRegionIndex` routes into the per-column
//!   interval trees — the #143 degenerate-rect regression surface);
//! - after the first eval, a single-cell edit re-evaluates only the affected
//!   placement interval, not the whole span;
//! - an expanding range that covers the family's own result region still
//!   rejects with `InternalDependency` (the union read region keeps the
//!   intersect check conservative).

use std::sync::Arc;

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use crate::formula_plane::region_index::Region;
use crate::test_workbook::TestWorkbook;

const SHEET: &str = "Sheet1";
const ROWS: u32 = 400;

fn authoritative_engine() -> Engine<TestWorkbook> {
    let cfg =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    Engine::new(TestWorkbook::default(), cfg)
}

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

/// `A{r} = r` for rows 1..=ROWS; tail readers `C{r} = SUM($A{r}:$A$ROWS)`.
fn build_tail_read_engine() -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity(ROWS as usize);
    for row in 1..=ROWS {
        engine
            .set_cell_value(SHEET, row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(
            &mut engine,
            row,
            3,
            &format!("=SUM($A{row}:$A${ROWS})"),
        ));
    }
    let report = engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .expect("ingest formulas");
    assert_eq!(
        report.shadow_accepted_span_cells,
        u64::from(ROWS),
        "tail-read family must span; histogram: {:?}",
        report.fallback_reasons
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine
}

/// `B{r} = r` for rows 2..=ROWS+1; running totals `C{r} = SUM($B$2:$B{r})`.
fn build_running_total_engine() -> Engine<TestWorkbook> {
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity(ROWS as usize);
    for row in 2..=ROWS + 1 {
        engine
            .set_cell_value(SHEET, row, 2, LiteralValue::Number(row as f64))
            .unwrap();
        formulas.push(record(&mut engine, row, 3, &format!("=SUM($B$2:$B{row})")));
    }
    let report = engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .expect("ingest formulas");
    assert_eq!(
        report.shadow_accepted_span_cells,
        u64::from(ROWS),
        "running-total family must span; histogram: {:?}",
        report.fallback_reasons
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine
}

/// SUM of a..=b.
fn interval_sum(a: u64, b: u64) -> f64 {
    ((a + b) * (b - a + 1) / 2) as f64
}

fn single_span_read_regions(engine: &Engine<TestWorkbook>) -> Vec<Region> {
    let authority = engine.graph.formula_authority();
    let spans: Vec<_> = authority.plane.spans.active_spans().collect();
    assert_eq!(spans.len(), 1);
    let read_summary = authority
        .plane
        .span_read_summaries
        .get(spans[0].read_summary_id.expect("read summary id"))
        .expect("read summary");
    read_summary
        .dependencies
        .iter()
        .map(|dependency| dependency.read_region)
        .collect()
}

#[test]
fn tail_read_span_union_read_region_is_single_column_interval() {
    let engine = build_tail_read_engine();
    let sheet_id = engine.graph.sheet_id(SHEET).expect("sheet id for Sheet1");

    // Union read region must be the full bounding interval [1..=ROWS] x col A
    // as a degenerate single-column region (Point col axis), so that
    // SheetRegionIndex routes it into the per-column interval trees rather
    // than the coarse rect buckets (#143).
    assert_eq!(
        single_span_read_regions(&engine),
        vec![Region::col_interval(sheet_id, 0, 0, ROWS - 1)]
    );
}

#[test]
fn running_total_span_union_read_region_is_single_column_interval() {
    let engine = build_running_total_engine();
    let sheet_id = engine.graph.sheet_id(SHEET).expect("sheet id for Sheet1");

    // Rows 2..=ROWS+1 are 1..=ROWS 0-based; column B is 1.
    assert_eq!(
        single_span_read_regions(&engine),
        vec![Region::col_interval(sheet_id, 1, 1, ROWS)]
    );
}

#[test]
fn tail_read_edit_recalc_is_bounded_by_affected_placement_interval() {
    let mut engine = build_tail_read_engine();

    engine.evaluate_all().unwrap();
    let first = engine
        .last_formula_plane_span_eval_report()
        .expect("first eval must run the authoritative span pass");
    assert_eq!(first.span_eval_placement_count, u64::from(ROWS));
    for row in [1, 2, ROWS / 2, ROWS] {
        assert_eq!(
            numeric_value(&engine, row, 3),
            interval_sum(u64::from(row), u64::from(ROWS))
        );
    }

    // Edit one cell inside the read region. A change at A10 is read only by
    // the placements whose shrinking tail still covers row 10: rows 1..=10.
    const EDIT_ROW: u32 = 10;
    engine
        .action_atomic_journal("edit A10".to_string(), |tx| {
            tx.set_cell_value(SHEET, EDIT_ROW, 1, LiteralValue::Number(1_000.0))?;
            Ok(())
        })
        .unwrap();
    engine.evaluate_all().unwrap();

    let report = engine
        .last_formula_plane_span_eval_report()
        .expect("edit recalc must evaluate span work");
    assert_eq!(
        report.span_eval_placement_count,
        u64::from(EDIT_ROW),
        "tail-read dirty work must be the affected placement interval, not \
         the whole span: {report:?}"
    );

    let delta = 1_000.0 - EDIT_ROW as f64;
    assert_eq!(
        numeric_value(&engine, 1, 3),
        interval_sum(1, u64::from(ROWS)) + delta
    );
    assert_eq!(
        numeric_value(&engine, EDIT_ROW, 3),
        interval_sum(u64::from(EDIT_ROW), u64::from(ROWS)) + delta
    );
    // Placements past the edit row never read it and keep their values.
    assert_eq!(
        numeric_value(&engine, EDIT_ROW + 1, 3),
        interval_sum(u64::from(EDIT_ROW) + 1, u64::from(ROWS))
    );
}

#[test]
fn running_total_edit_recalc_is_bounded_by_affected_placement_interval() {
    let mut engine = build_running_total_engine();

    engine.evaluate_all().unwrap();
    let first = engine
        .last_formula_plane_span_eval_report()
        .expect("first eval must run the authoritative span pass");
    assert_eq!(first.span_eval_placement_count, u64::from(ROWS));
    for row in [2, ROWS / 2, ROWS + 1] {
        assert_eq!(
            numeric_value(&engine, row, 3),
            interval_sum(2, u64::from(row))
        );
    }

    // A change at B395 is read only by the placements whose expanding range
    // has reached it: rows 395..=ROWS+1.
    const EDIT_ROW: u32 = ROWS - 5; // 395
    engine
        .action_atomic_journal("edit B395".to_string(), |tx| {
            tx.set_cell_value(SHEET, EDIT_ROW, 2, LiteralValue::Number(5_000.0))?;
            Ok(())
        })
        .unwrap();
    engine.evaluate_all().unwrap();

    let report = engine
        .last_formula_plane_span_eval_report()
        .expect("edit recalc must evaluate span work");
    assert_eq!(
        report.span_eval_placement_count,
        u64::from(ROWS + 1 - EDIT_ROW + 1), // rows 395..=401 inclusive
        "running-total dirty work must be the affected placement interval, \
         not the whole span: {report:?}"
    );

    let delta = 5_000.0 - EDIT_ROW as f64;
    // Placements before the edit row never read it and keep their values.
    assert_eq!(
        numeric_value(&engine, EDIT_ROW - 1, 3),
        interval_sum(2, u64::from(EDIT_ROW) - 1)
    );
    assert_eq!(
        numeric_value(&engine, EDIT_ROW, 3),
        interval_sum(2, u64::from(EDIT_ROW)) + delta
    );
    assert_eq!(
        numeric_value(&engine, ROWS + 1, 3),
        interval_sum(2, u64::from(ROWS) + 1) + delta
    );
}

#[test]
fn self_reading_expanding_range_family_rejects_with_internal_dependency() {
    // `=SUM($C$1:$C{r-1})*0+B{r}` placed in column C: the expanding range
    // reads the family's own result column. The union read region
    // [1..=ROWS] x C intersects the result region [2..=ROWS+1] x C, so the
    // InternalDependency guard must keep the whole family legacy.
    let mut engine = authoritative_engine();
    let mut formulas = Vec::with_capacity(ROWS as usize);
    for row in 2..=ROWS + 1 {
        engine
            .set_cell_value(SHEET, row, 2, LiteralValue::Number(row as f64))
            .unwrap();
        let prev = row - 1;
        formulas.push(record(
            &mut engine,
            row,
            3,
            &format!("=SUM($C$1:$C{prev})*0+B{row}"),
        ));
    }
    let report = engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .expect("ingest formulas");

    assert_eq!(report.shadow_accepted_span_cells, 0);
    assert_eq!(
        report
            .fallback_reasons
            .get("InternalDependency")
            .copied()
            .unwrap_or(0),
        u64::from(ROWS),
        "histogram: {:?}",
        report.fallback_reasons
    );
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);

    engine.evaluate_all().unwrap();
    for row in [2, ROWS / 2, ROWS + 1] {
        assert_eq!(numeric_value(&engine, row, 3), row as f64);
    }
}
