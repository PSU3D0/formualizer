//! FormulaPlane scenarios probe.
//!
//! Compares `FormulaPlaneMode::Off` vs `AuthoritativeExperimental` across
//! several workbook shapes to find where FP genuinely lands a radical win
//! and where it doesn't.
//!
//! Run:
//!
//! ```bash
//! cargo run -p formualizer-bench-core --features formualizer_runner \
//!   --release --bin probe-fp-scenarios -- --rows 100000
//! ```

#[cfg(feature = "formualizer_runner")]
use std::time::Instant;

#[cfg(feature = "formualizer_runner")]
use anyhow::Result;
#[cfg(feature = "formualizer_runner")]
use clap::Parser;
#[cfg(feature = "formualizer_runner")]
use formualizer_eval::engine::{EvalConfig, FormulaPlaneMode};
#[cfg(feature = "formualizer_runner")]
use formualizer_testkit::build_workbook;
#[cfg(feature = "formualizer_runner")]
use formualizer_workbook::{
    LiteralValue, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
#[cfg(feature = "formualizer_runner")]
use serde::Serialize;

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --release --bin probe-fp-scenarios"
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Parser)]
#[command(about = "FormulaPlane scenarios probe: family detection across workbook shapes")]
struct Cli {
    #[arg(long, default_value_t = 100_000)]
    rows: u32,
    #[arg(long, default_value_t = 5)]
    edit_cycles: u32,
    /// Subset of scenarios to run; comma separated.
    #[arg(long, default_value = "")]
    only: String,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize, Clone)]
struct ModeReport {
    mode: String,
    load_ms: u128,
    eval_first_ms: u128,
    eval_warm_ms: u128,
    edit_recalc_ms_avg: f64,
    graph_formula_vertex_count: usize,
    formula_plane_active_span_count: usize,
    formula_plane_producer_result_entries: usize,
    formula_plane_consumer_read_entries: usize,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct ScenarioReport {
    scenario: String,
    formula_count: u32,
    off: ModeReport,
    auth: ModeReport,
    speedup_load: f64,
    speedup_first: f64,
    speedup_warm: f64,
    speedup_edit: f64,
    fp_engaged: bool,
}

#[cfg(feature = "formualizer_runner")]
type FixtureBuilder = Box<dyn Fn(u32) -> std::path::PathBuf>;

#[cfg(feature = "formualizer_runner")]
struct Scenario {
    name: &'static str,
    description: &'static str,
    formula_count_fn: fn(u32) -> u32,
    builder: FixtureBuilder,
}

#[cfg(feature = "formualizer_runner")]
fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "two-trivial-families",
            description: "B=A+1 anchored, C=B*2 anchored. The original probe.",
            formula_count_fn: |rows| rows * 2,
            builder: Box::new(|rows| {
                build_workbook(|book| {
                    let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
                    for r in 1..=rows {
                        sh.get_cell_mut((1u32, r)).set_value_number(r as f64);
                        sh.get_cell_mut((2u32, r)).set_formula(format!("=A{r}+1"));
                        sh.get_cell_mut((3u32, r)).set_formula(format!("=B{r}*2"));
                    }
                })
            }),
        },
        Scenario {
            name: "single-trivial-family",
            description: "Single family B=A+1. Tightest FP best case.",
            formula_count_fn: |rows| rows,
            builder: Box::new(|rows| {
                build_workbook(|book| {
                    let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
                    for r in 1..=rows {
                        sh.get_cell_mut((1u32, r)).set_value_number(r as f64);
                        sh.get_cell_mut((2u32, r)).set_formula(format!("=A{r}+1"));
                    }
                })
            }),
        },
        Scenario {
            name: "fixed-anchor-family",
            description: "B=$A$1+1 over N rows. Same anchor, identical template.",
            formula_count_fn: |rows| rows,
            builder: Box::new(|rows| {
                build_workbook(|book| {
                    let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
                    sh.get_cell_mut((1u32, 1u32)).set_value_number(42.0);
                    for r in 1..=rows {
                        sh.get_cell_mut((2u32, r)).set_formula("=$A$1+1");
                    }
                })
            }),
        },
        Scenario {
            name: "five-families",
            description: "Five offset families: B=A+1, C=A*2, D=A-1, E=A/2, F=A+A.",
            formula_count_fn: |rows| rows * 5,
            builder: Box::new(|rows| {
                build_workbook(|book| {
                    let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
                    for r in 1..=rows {
                        sh.get_cell_mut((1u32, r)).set_value_number(r as f64);
                        sh.get_cell_mut((2u32, r)).set_formula(format!("=A{r}+1"));
                        sh.get_cell_mut((3u32, r)).set_formula(format!("=A{r}*2"));
                        sh.get_cell_mut((4u32, r)).set_formula(format!("=A{r}-1"));
                        sh.get_cell_mut((5u32, r)).set_formula(format!("=A{r}/2"));
                        sh.get_cell_mut((6u32, r))
                            .set_formula(format!("=A{r}+A{r}"));
                    }
                })
            }),
        },
        Scenario {
            name: "heavy-arith-family",
            description: "B=A+A*2-A/3+A*A. Heavier per-cell arithmetic.",
            formula_count_fn: |rows| rows,
            builder: Box::new(|rows| {
                build_workbook(|book| {
                    let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
                    for r in 1..=rows {
                        sh.get_cell_mut((1u32, r)).set_value_number(r as f64);
                        sh.get_cell_mut((2u32, r))
                            .set_formula(format!("=A{r}+A{r}*2-A{r}/3+A{r}*A{r}"));
                    }
                })
            }),
        },
        Scenario {
            name: "all-unique-singletons",
            description: "Every formula is unique, e.g. =A1+1, =A2*2, =A3-3 ... no families. Pure legacy path under FP.",
            formula_count_fn: |rows| rows,
            builder: Box::new(|rows| {
                build_workbook(|book| {
                    let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
                    for r in 1..=rows {
                        sh.get_cell_mut((1u32, r)).set_value_number(r as f64);
                        // Vary the operator and constant so each formula is structurally unique.
                        let op = match r % 4 {
                            0 => format!("=A{r}+{r}"),
                            1 => format!("=A{r}*{r}"),
                            2 => format!("=A{r}-{r}"),
                            _ => format!("=A{r}/({r}+1)"),
                        };
                        sh.get_cell_mut((2u32, r)).set_formula(op);
                    }
                })
            }),
        },
        Scenario {
            name: "long-chain-family",
            description: "Sequential dependency: B1=A1, B2=B1+A2, B3=B2+A3 ... internal-dep family. Likely fallback.",
            formula_count_fn: |rows| rows,
            builder: Box::new(|rows| {
                build_workbook(|book| {
                    let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
                    for r in 1..=rows {
                        sh.get_cell_mut((1u32, r)).set_value_number(r as f64);
                    }
                    sh.get_cell_mut((2u32, 1u32)).set_formula("=A1");
                    for r in 2..=rows {
                        let prev = r - 1;
                        sh.get_cell_mut((2u32, r))
                            .set_formula(format!("=B{prev}+A{r}"));
                    }
                })
            }),
        },
        Scenario {
            // Bounded variant: ~10x rows columns capped at 10 to keep total
            // formula count comparable to single-family scenarios. Each row
            // shares the same template across columns -> 10 spans per family.
            name: "rect-family-10cols",
            description: "Rect family across 10 columns × N rows: =$A$1+col_offset (anchored).",
            formula_count_fn: |rows| rows * 10,
            builder: Box::new(|rows| {
                build_workbook(|book| {
                    let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
                    sh.get_cell_mut((1u32, 1u32)).set_value_number(1.0);
                    for r in 1..=rows {
                        for c in 2u32..=11u32 {
                            sh.get_cell_mut((c, r)).set_formula(format!("=$A$1+{c}"));
                        }
                    }
                })
            }),
        },
        Scenario {
            // Anchored 2-family variant of the original probe: avoids any
            // self-dependency, so FP path is exercised without falling back.
            name: "two-anchored-families",
            description: "B=$A$1+1, C=$A$1*2 over N rows. Anchored, two spans.",
            formula_count_fn: |rows| rows * 2,
            builder: Box::new(|rows| {
                build_workbook(|book| {
                    let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
                    sh.get_cell_mut((1u32, 1u32)).set_value_number(1.0);
                    for r in 1..=rows {
                        sh.get_cell_mut((2u32, r)).set_formula("=$A$1+1");
                        sh.get_cell_mut((3u32, r)).set_formula("=$A$1*2");
                    }
                })
            }),
        },
    ]
}

#[cfg(feature = "formualizer_runner")]
fn run_mode(
    edit_cycles: u32,
    mode: FormulaPlaneMode,
    path: &std::path::Path,
    edit_targets: &[(u32, u32)],
) -> Result<ModeReport> {
    let mut config = WorkbookConfig::ephemeral();
    config.eval = EvalConfig::default().with_formula_plane_mode(mode);

    let load_start = Instant::now();
    let backend = UmyaAdapter::open_path(path)?;
    let mut wb = Workbook::from_reader(backend, LoadStrategy::EagerAll, config)?;
    let load_ms = load_start.elapsed().as_millis();

    let t = Instant::now();
    wb.evaluate_all()?;
    let eval_first_ms = t.elapsed().as_millis();

    let t = Instant::now();
    wb.evaluate_all()?;
    let eval_warm_ms = t.elapsed().as_millis();

    let mut edit_total_us = 0u128;
    let mut edit_runs = 0u32;
    for c in 0..edit_cycles {
        let (row, col) = edit_targets[(c as usize) % edit_targets.len()];
        wb.set_value(
            "Sheet1",
            row,
            col,
            LiteralValue::Number(((c + 1) as f64) * 1.5),
        )?;
        let t = Instant::now();
        wb.evaluate_all()?;
        edit_total_us += t.elapsed().as_micros();
        edit_runs += 1;
    }
    let edit_recalc_ms_avg = if edit_runs == 0 {
        0.0
    } else {
        (edit_total_us as f64 / edit_runs as f64) / 1000.0
    };

    let stats = wb.engine().baseline_stats();

    Ok(ModeReport {
        mode: format!("{mode:?}"),
        load_ms,
        eval_first_ms,
        eval_warm_ms,
        edit_recalc_ms_avg,
        graph_formula_vertex_count: stats.graph_formula_vertex_count,
        formula_plane_active_span_count: stats.formula_plane_active_span_count,
        formula_plane_producer_result_entries: stats.formula_plane_producer_result_entries,
        formula_plane_consumer_read_entries: stats.formula_plane_consumer_read_entries,
    })
}

#[cfg(feature = "formualizer_runner")]
fn ratio(off: f64, auth: f64) -> f64 {
    if auth <= 0.0 {
        if off <= 0.0 { 1.0 } else { f64::INFINITY }
    } else {
        off / auth
    }
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    let only: Vec<&str> = if cli.only.trim().is_empty() {
        vec![]
    } else {
        cli.only.split(',').map(|s| s.trim()).collect()
    };

    let mut reports: Vec<ScenarioReport> = Vec::new();
    for s in scenarios() {
        if !only.is_empty() && !only.contains(&s.name) {
            continue;
        }
        eprintln!(
            "[probe-fp-scenarios] {}: {} ({} formulas)",
            s.name,
            s.description,
            (s.formula_count_fn)(cli.rows)
        );
        let path = (s.builder)(cli.rows);

        // Edit the A column at a few positions so dirty closure has work to project.
        let edit_targets: Vec<(u32, u32)> = (0..cli.edit_cycles)
            .map(|i| (1 + (i % cli.rows), 1))
            .collect();

        let off = run_mode(cli.edit_cycles, FormulaPlaneMode::Off, &path, &edit_targets)?;
        let auth = run_mode(
            cli.edit_cycles,
            FormulaPlaneMode::AuthoritativeExperimental,
            &path,
            &edit_targets,
        )?;

        let report = ScenarioReport {
            scenario: s.name.to_string(),
            formula_count: (s.formula_count_fn)(cli.rows),
            speedup_load: ratio(off.load_ms as f64, auth.load_ms as f64),
            speedup_first: ratio(off.eval_first_ms as f64, auth.eval_first_ms as f64),
            speedup_warm: ratio(off.eval_warm_ms as f64, auth.eval_warm_ms as f64),
            speedup_edit: ratio(off.edit_recalc_ms_avg, auth.edit_recalc_ms_avg),
            fp_engaged: auth.formula_plane_active_span_count > 0,
            off,
            auth,
        };
        reports.push(report);
    }

    println!("{}", serde_json::to_string_pretty(&reports)?);

    eprintln!();
    eprintln!("# FP scenarios summary (rows={})", cli.rows);
    eprintln!();
    eprintln!(
        "| scenario | formulas | spans | vertices Off | vertices Auth | load Off→Auth | first Off→Auth | edit Off→Auth |"
    );
    eprintln!("|---|---:|---:|---:|---:|---|---|---|");
    for r in &reports {
        eprintln!(
            "| {} | {} | {} | {} | {} | {}→{} ({:.2}x) | {}→{} ({:.2}x) | {:.2}→{:.2} ({:.2}x) |",
            r.scenario,
            r.formula_count,
            r.auth.formula_plane_active_span_count,
            r.off.graph_formula_vertex_count,
            r.auth.graph_formula_vertex_count,
            r.off.load_ms,
            r.auth.load_ms,
            r.speedup_load,
            r.off.eval_first_ms,
            r.auth.eval_first_ms,
            r.speedup_first,
            r.off.edit_recalc_ms_avg,
            r.auth.edit_recalc_ms_avg,
            r.speedup_edit,
        );
    }

    Ok(())
}
