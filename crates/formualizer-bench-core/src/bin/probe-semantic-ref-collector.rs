//! Formula-analysis ingest and edit-time re-analysis probe.
//!
//! This probe isolates formulas that stress small-range expansion, compressed
//! ranges, deep AST traversal, and cross-sheet resolution. Run each process once
//! and interleave binaries from the revisions being compared.

#[cfg(feature = "formualizer_runner")]
use std::{sync::Arc, time::Instant};

#[cfg(feature = "formualizer_runner")]
use anyhow::{Result, ensure};
#[cfg(feature = "formualizer_runner")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "formualizer_runner")]
use formualizer_eval::engine::{
    EngineBaselineStats, EvalConfig, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
};
#[cfg(feature = "formualizer_runner")]
use formualizer_parse::parse;
#[cfg(feature = "formualizer_runner")]
use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};
#[cfg(feature = "formualizer_runner")]
use serde::Serialize;

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("This binary requires feature `formualizer_runner`");
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut workbook = workbook(cli.mode);
    seed(&mut workbook, cli.formulas)?;

    let (ingest_ms, edit_ms, stats_after_ingest) = if matches!(cli.workload, Workload::SpanIngest) {
        bulk_span_ingest(&mut workbook, cli.mode, cli.formulas, cli.edits)?
    } else {
        let ingest_start = Instant::now();
        for row in 1..=cli.formulas {
            workbook.set_formula("Bench", row, 30, &formula(cli.workload, row, false))?;
        }
        let ingest_ms = ingest_start.elapsed().as_secs_f64() * 1_000.0;
        let stats_after_ingest = workbook.engine().baseline_stats();

        let edit_start = Instant::now();
        for edit in 0..cli.edits {
            let row = edit % cli.formulas + 1;
            workbook.set_formula("Bench", row, 30, &formula(cli.workload, row, true))?;
        }
        let edit_ms = edit_start.elapsed().as_secs_f64() * 1_000.0;
        (ingest_ms, edit_ms, stats_after_ingest)
    };
    let stats_after_edit = workbook.engine().baseline_stats();

    println!(
        "{}",
        serde_json::to_string(&Report {
            mode: cli.mode,
            workload: cli.workload,
            formulas: cli.formulas,
            edits: cli.edits,
            ingest_ms,
            edit_ms,
            fp_spans_after_ingest: stats_after_ingest.formula_plane_active_span_count,
            fp_spans_after_edit: stats_after_edit.formula_plane_active_span_count,
            fp_candidates_after_edit: stats_after_edit.formula_plane_structural_span_candidates,
        })?
    );
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, value_enum)]
    mode: Mode,
    #[arg(long, value_enum)]
    workload: Workload,
    #[arg(long, default_value_t = 3_000)]
    formulas: u32,
    #[arg(long, default_value_t = 3_000)]
    edits: u32,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Off,
    Authoritative,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum Workload {
    SmallRanges,
    CompressedRanges,
    DeepNesting,
    CrossSheet,
    SpanIngest,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct Report {
    mode: Mode,
    workload: Workload,
    formulas: u32,
    edits: u32,
    ingest_ms: f64,
    edit_ms: f64,
    fp_spans_after_ingest: usize,
    fp_spans_after_edit: usize,
    fp_candidates_after_edit: u64,
}

#[cfg(feature = "formualizer_runner")]
fn span_batch(workbook: &mut Workbook, rows: u32, edited: bool) -> Result<FormulaIngestBatch> {
    let mut records = Vec::with_capacity(rows as usize);
    for row in 1..=rows {
        let text = formula(Workload::SpanIngest, row, edited);
        let ast = parse(&text)?;
        let ast_id = workbook.engine_mut().intern_formula_ast(&ast);
        records.push(FormulaIngestRecord::new(
            row,
            30,
            ast_id,
            Some(Arc::<str>::from(text)),
        ));
    }
    Ok(FormulaIngestBatch::new("Bench", records))
}

#[cfg(feature = "formualizer_runner")]
fn bulk_span_ingest(
    workbook: &mut Workbook,
    mode: Mode,
    formulas: u32,
    edits: u32,
) -> Result<(f64, f64, EngineBaselineStats)> {
    ensure!(
        edits <= formulas,
        "span-ingest requires edits ({edits}) <= formulas ({formulas})"
    );
    let initial = span_batch(workbook, formulas, false)?;
    let ingest_start = Instant::now();
    workbook
        .engine_mut()
        .ingest_formula_batches(vec![initial])?;
    let ingest_ms = ingest_start.elapsed().as_secs_f64() * 1_000.0;
    let stats_after_ingest = workbook.engine().baseline_stats();
    if matches!(mode, Mode::Authoritative) {
        ensure!(
            stats_after_ingest.formula_plane_active_span_count > 0,
            "authoritative bulk ingest formed no FormulaPlane spans"
        );
    }

    let replacements = span_batch(workbook, edits, true)?;
    let edit_start = Instant::now();
    workbook
        .engine_mut()
        .ingest_formula_batches(vec![replacements])?;
    let edit_ms = edit_start.elapsed().as_secs_f64() * 1_000.0;
    if matches!(mode, Mode::Authoritative) {
        ensure!(
            workbook
                .engine()
                .baseline_stats()
                .formula_plane_active_span_count
                > 0,
            "authoritative bulk edit retained no FormulaPlane spans"
        );
    }

    Ok((ingest_ms, edit_ms, stats_after_ingest))
}

#[cfg(feature = "formualizer_runner")]
fn workbook(mode: Mode) -> Workbook {
    let formula_plane_mode = match mode {
        Mode::Off => FormulaPlaneMode::Off,
        Mode::Authoritative => FormulaPlaneMode::AuthoritativeExperimental,
    };
    let mut config = WorkbookConfig::ephemeral();
    config.eval = EvalConfig::default()
        .with_formula_plane_mode(formula_plane_mode)
        .with_parallel(false);
    Workbook::new_with_config(config)
}

#[cfg(feature = "formualizer_runner")]
fn seed(workbook: &mut Workbook, rows: u32) -> Result<()> {
    workbook.add_sheet("Bench")?;
    workbook.add_sheet("Lookup")?;
    let values: Vec<Vec<LiteralValue>> = (1..=rows.saturating_add(128))
        .map(|row| {
            vec![
                LiteralValue::Int(i64::from(row)),
                LiteralValue::Int(2),
                LiteralValue::Int(3),
                LiteralValue::Int(4),
            ]
        })
        .collect();
    workbook.set_values("Bench", 1, 1, &values)?;
    workbook.set_values("Lookup", 1, 1, &values)?;
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
fn formula(workload: Workload, row: u32, edited: bool) -> String {
    let delta = u32::from(edited);
    match workload {
        Workload::SmallRanges => {
            format!("=SUM(A{row}:D{})+SUM(B{row}:C{})+{delta}", row + 3, row + 1)
        }
        Workload::CompressedRanges => {
            format!("=SUM(A:A)+SUM(1:1)+SUM(A1:Z100)+A{row}+{delta}")
        }
        Workload::DeepNesting => {
            let mut expression = format!("A{row}+{delta}");
            for depth in 0..24 {
                let referenced_row = row + depth % 7;
                expression = format!(
                    "SUM(A{referenced_row},IF(B{referenced_row}>0,{expression},C{referenced_row}))"
                );
            }
            format!("={expression}")
        }
        Workload::CrossSheet => format!(
            "=SUM(Lookup!A{row}:D{})+Lookup!A{}+SUM(Lookup!A1:Z100)+{delta}",
            row + 3,
            row + 1
        ),
        Workload::SpanIngest => format!("=SUM(A{row}:D{})+{delta}", row + 3),
    }
}
