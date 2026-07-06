//! Standing probe for structural row-op cost on a ColRun-spanned formula
//! column, with and without FormulaPlane span evaluation.
//!
//! Workload: N rows of numeric inputs `A{r}`, `B{r}` plus a scalar `$F$1`,
//! one ColRun-eligible formula family `C{r} = A{r}*B{r}*$F$1` spanning rows
//! 2..=N+1 (row 1 is a header/scalar row), and a tail read
//! `SUM(C2:C{N+1})` in a separate cell that consumes the whole span output.
//!
//! For each of four structural ops — insert mid-span, insert above the span,
//! insert below the span, delete mid-span — the probe builds a fresh
//! workbook, evaluates it once (baseline), then times:
//!   (1) the structural op call itself (`Engine::insert_rows` /
//!       `Engine::delete_rows`), and
//!   (2) the `evaluate_all()` that follows it (recalculating whatever the op
//!       dirtied).
//! Each (op, mode) pair is measured `--runs` times (default 3) from a fresh
//! workbook and reported as medians, for FormulaPlane span evaluation ON
//! (`AuthoritativeExperimental`) and OFF.
//!
//! This is the standing before/after probe for the row-op port of
//! `adjust_ast_if_changed` (previously column-ops only; see
//! `perf(graph): O(1) edge-coord update + change-aware AST adjustment in
//! structural ops`, commit ac8ffd3b) into `VertexEditor::insert_rows` /
//! `::delete_rows`.
//!
//! Run (release):
//! ```bash
//! cargo run --release -p formualizer-bench-core --bin probe-fp-structural -- \
//!   --rows 200000 --runs 3
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Parser;
use formualizer_common::LiteralValue;
use formualizer_eval::engine::{
    Engine, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(about = "Structural row-op probe over a ColRun-spanned formula column (span ON vs OFF)")]
struct Cli {
    /// Number of data rows in the span (span occupies rows 2..=rows+1).
    #[arg(long, default_value_t = 200_000)]
    rows: u32,
    /// Number of measured runs per (op, mode); the reported value is the
    /// median.
    #[arg(long, default_value_t = 3)]
    runs: u32,
    #[arg(long, default_value = "phase0-row-op-port")]
    label: String,
}

const SHEET: &str = "Sheet1";

/// Row where the span starts (1-based). Row 1 holds the scalar `$F$1`.
const SPAN_START: u32 = 2;

fn span_end(rows: u32) -> u32 {
    SPAN_START + rows - 1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    InsertMid,
    InsertAbove,
    InsertBelow,
    DeleteMid,
}

impl Op {
    fn all() -> [Op; 4] {
        [
            Op::InsertMid,
            Op::InsertAbove,
            Op::InsertBelow,
            Op::DeleteMid,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Op::InsertMid => "insert_mid_span",
            Op::InsertAbove => "insert_above_span",
            Op::InsertBelow => "insert_below_span",
            Op::DeleteMid => "delete_mid_span",
        }
    }

    /// (before/start row 1-based, count) applied to Engine::insert_rows /
    /// Engine::delete_rows, plus whether it's an insert.
    fn apply(self, engine: &mut Engine<TestWorkbook>, rows: u32) -> Result<()> {
        let end = span_end(rows);
        match self {
            Op::InsertMid => {
                let mid = SPAN_START + rows / 2;
                engine.insert_rows(SHEET, mid, 1)?;
            }
            Op::InsertAbove => {
                // Strictly above the span: before the span's first row.
                engine.insert_rows(SHEET, SPAN_START, 1)?;
            }
            Op::InsertBelow => {
                // Strictly below the span: after its last row (no shift of
                // the span itself, but still walks every formula vertex in
                // the legacy path).
                engine.insert_rows(SHEET, end + 2, 1)?;
            }
            Op::DeleteMid => {
                let mid = SPAN_START + rows / 2;
                engine.delete_rows(SHEET, mid, 1)?;
            }
        }
        Ok(())
    }
}

fn record(
    engine: &mut Engine<TestWorkbook>,
    row: u32,
    col: u32,
    formula: &str,
) -> FormulaIngestRecord {
    let ast = parse(formula).expect("parse formula");
    let ast_id = engine.intern_formula_ast(&ast);
    FormulaIngestRecord::new(row, col, ast_id, Some(Arc::<str>::from(formula)))
}

fn build_engine(mode: FormulaPlaneMode, rows: u32) -> Result<Engine<TestWorkbook>> {
    let cfg = EvalConfig::default().with_formula_plane_mode(mode);
    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine.add_sheet(SHEET).ok();

    // Scalar multiplier read by every row in the span.
    engine.set_cell_value(SHEET, 1, 6, LiteralValue::Number(3.0))?; // F1

    let mut formulas = Vec::with_capacity(rows as usize);
    for r in SPAN_START..=span_end(rows) {
        engine.set_cell_value(SHEET, r, 1, LiteralValue::Number(r as f64))?; // A{r}
        engine.set_cell_value(SHEET, r, 2, LiteralValue::Number((r * 2) as f64))?; // B{r}
        formulas.push(record(
            &mut engine,
            r,
            3, // C{r}
            &format!("=A{r}*B{r}*$F$1"),
        ));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new(SHEET, formulas)])
        .context("ingest span formulas")?;

    // Tail read: SUM over the whole span output, in a separate cell.
    engine.set_cell_formula(
        SHEET,
        1,
        5, // E1
        parse(format!("=SUM(C{}:C{})", SPAN_START, span_end(rows)))?,
    )?;

    engine.evaluate_all().context("baseline evaluate_all")?;
    Ok(engine)
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = xs.len();
    if n == 0 {
        0.0
    } else if n % 2 == 1 {
        xs[n / 2]
    } else {
        (xs[n / 2 - 1] + xs[n / 2]) / 2.0
    }
}

#[derive(Debug, Serialize)]
struct OpResult {
    op: &'static str,
    mode: &'static str,
    op_ms_median: f64,
    post_eval_ms_median: f64,
    op_ms_samples: Vec<f64>,
    post_eval_ms_samples: Vec<f64>,
}

#[derive(Debug, Serialize)]
struct Report {
    label: String,
    rows: u32,
    runs: u32,
    results: Vec<OpResult>,
}

fn run_op(
    mode: FormulaPlaneMode,
    mode_name: &'static str,
    op: Op,
    rows: u32,
    runs: u32,
) -> Result<OpResult> {
    let mut op_samples = Vec::with_capacity(runs as usize);
    let mut eval_samples = Vec::with_capacity(runs as usize);

    for _ in 0..runs {
        let mut engine = build_engine(mode, rows)?;

        let t = Instant::now();
        op.apply(&mut engine, rows)?;
        let op_elapsed = t.elapsed();

        let t = Instant::now();
        engine.evaluate_all().context("post-op evaluate_all")?;
        let eval_elapsed = t.elapsed();

        op_samples.push(dur_ms(op_elapsed));
        eval_samples.push(dur_ms(eval_elapsed));
    }

    Ok(OpResult {
        op: op.name(),
        mode: mode_name,
        op_ms_median: median(op_samples.clone()),
        post_eval_ms_median: median(eval_samples.clone()),
        op_ms_samples: op_samples,
        post_eval_ms_samples: eval_samples,
    })
}

fn dur_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.rows < 10 {
        bail!("--rows must be at least 10");
    }
    if cli.runs == 0 {
        bail!("--runs must be at least 1");
    }

    eprintln!(
        "[probe-fp-structural] rows={} runs={} label={}",
        cli.rows, cli.runs, cli.label
    );

    let modes: [(FormulaPlaneMode, &'static str); 2] = [
        (FormulaPlaneMode::Off, "off"),
        (FormulaPlaneMode::AuthoritativeExperimental, "span_on"),
    ];

    let mut results = Vec::new();
    for (mode, mode_name) in modes {
        for op in Op::all() {
            let result = run_op(mode, mode_name, op, cli.rows, cli.runs)?;
            eprintln!(
                "  {:>10} {:<18} op {:>8.1} ms (median), post-eval {:>8.1} ms (median)",
                result.mode, result.op, result.op_ms_median, result.post_eval_ms_median
            );
            results.push(result);
        }
    }

    let report = Report {
        label: cli.label,
        rows: cli.rows,
        runs: cli.runs,
        results,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);

    Ok(())
}
