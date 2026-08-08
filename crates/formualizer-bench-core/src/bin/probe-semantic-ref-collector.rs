//! Formula-analysis ingest and edit-time re-analysis probe.
//!
//! This probe isolates formulas that stress small-range expansion, compressed
//! ranges, deep AST traversal, and cross-sheet resolution. Run each process once
//! and interleave binaries from the revisions being compared.

#[cfg(feature = "formualizer_runner")]
use std::time::Instant;

#[cfg(feature = "formualizer_runner")]
use anyhow::Result;
#[cfg(feature = "formualizer_runner")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "formualizer_runner")]
use formualizer_eval::engine::{EvalConfig, FormulaPlaneMode};
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

    let ingest_start = Instant::now();
    for row in 1..=cli.formulas {
        workbook.set_formula("Bench", row, 30, &formula(cli.workload, row, false))?;
    }
    let ingest_ms = ingest_start.elapsed().as_secs_f64() * 1_000.0;

    let edit_start = Instant::now();
    for edit in 0..cli.edits {
        let row = edit % cli.formulas + 1;
        workbook.set_formula("Bench", row, 30, &formula(cli.workload, row, true))?;
    }
    let edit_ms = edit_start.elapsed().as_secs_f64() * 1_000.0;

    println!(
        "{}",
        serde_json::to_string(&Report {
            mode: cli.mode,
            workload: cli.workload,
            formulas: cli.formulas,
            edits: cli.edits,
            ingest_ms,
            edit_ms,
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
    }
}
