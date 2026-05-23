//! FormulaPlane radical-cutover probe.
//!
//! Builds an XLSX fixture via the standard `umya_spreadsheet` path with two
//! large formula families (B = =A+1, C = =B*2) over N rows, then loads it
//! through `Workbook` under each `FormulaPlaneMode` and reports load/eval
//! timings, vertex/span counts, and cell sample correctness.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p formualizer-bench-core --features formualizer_runner \
//!   --release --bin probe-fp-radical -- --rows 100000
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
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --release --bin probe-fp-radical -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Parser)]
#[command(about = "FormulaPlane radical-cutover probe (Off vs AuthoritativeExperimental)")]
struct Cli {
    /// Number of formula rows per family.
    #[arg(long, default_value_t = 100_000)]
    rows: u32,
    /// Edit-recalc cycles to time after first eval.
    #[arg(long, default_value_t = 1)]
    edit_cycles: u32,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
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
    sample_b1: Option<f64>,
    sample_b_mid: Option<f64>,
    sample_b_last: Option<f64>,
    sample_c_last: Option<f64>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct ProbeReport {
    rows: u32,
    edit_cycles: u32,
    off: ModeReport,
    auth: ModeReport,
    speedup_eval_first: f64,
    speedup_eval_warm: f64,
}

#[cfg(feature = "formualizer_runner")]
fn build_fixture(rows: u32) -> std::path::PathBuf {
    build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
        for r in 1..=rows {
            // A column: numeric input
            sh.get_cell_mut((1u32, r)).set_value_number(r as f64);
            // B column: family =A{r}+1 (anchored, no internal dep)
            sh.get_cell_mut((2u32, r)).set_formula(format!("=A{r}+1"));
            // C column: family =B{r}*2 (chains to B family)
            sh.get_cell_mut((3u32, r)).set_formula(format!("=B{r}*2"));
        }
    })
}

#[cfg(feature = "formualizer_runner")]
fn run_mode(
    rows: u32,
    edit_cycles: u32,
    mode: FormulaPlaneMode,
    path: &std::path::Path,
) -> Result<ModeReport> {
    let mut config = WorkbookConfig::ephemeral();
    config.eval = EvalConfig::default().with_formula_plane_mode(mode);

    let load_start = Instant::now();
    let backend = UmyaAdapter::open_path(path)?;
    let mut wb = Workbook::from_reader(backend, LoadStrategy::EagerAll, config)?;
    let load_ms = load_start.elapsed().as_millis();

    let eval_first = Instant::now();
    wb.evaluate_all()?;
    let eval_first_ms = eval_first.elapsed().as_millis();

    let eval_warm = Instant::now();
    wb.evaluate_all()?;
    let eval_warm_ms = eval_warm.elapsed().as_millis();

    let mut edit_total_ms = 0u128;
    for c in 0..edit_cycles {
        // Edit a single A cell, recalc, time it.
        let row = 1 + (c % rows);
        wb.set_value("Sheet1", row, 1, LiteralValue::Number((row as f64) * 10.0))?;
        let t = Instant::now();
        wb.evaluate_all()?;
        edit_total_ms += t.elapsed().as_millis();
    }
    let edit_recalc_ms_avg = if edit_cycles == 0 {
        0.0
    } else {
        edit_total_ms as f64 / edit_cycles as f64
    };

    let stats = wb.engine().baseline_stats();
    let mode_label = format!("{mode:?}");

    let sample_value = |row: u32, col: u32| -> Option<f64> {
        match wb.get_value("Sheet1", row, col) {
            Some(LiteralValue::Number(n)) => Some(n),
            Some(LiteralValue::Int(i)) => Some(i as f64),
            _ => None,
        }
    };

    let mid = (rows / 2).max(1);
    let last = rows.max(1);

    Ok(ModeReport {
        mode: mode_label,
        load_ms,
        eval_first_ms,
        eval_warm_ms,
        edit_recalc_ms_avg,
        graph_formula_vertex_count: stats.graph_formula_vertex_count,
        formula_plane_active_span_count: stats.formula_plane_active_span_count,
        formula_plane_producer_result_entries: stats.formula_plane_producer_result_entries,
        formula_plane_consumer_read_entries: stats.formula_plane_consumer_read_entries,
        sample_b1: sample_value(1, 2),
        sample_b_mid: sample_value(mid, 2),
        sample_b_last: sample_value(last, 2),
        sample_c_last: sample_value(last, 3),
    })
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    eprintln!("[probe-fp-radical] building fixture rows={}", cli.rows);
    let path = build_fixture(cli.rows);
    eprintln!("[probe-fp-radical] fixture: {}", path.display());

    eprintln!("[probe-fp-radical] running Off ...");
    let off = run_mode(cli.rows, cli.edit_cycles, FormulaPlaneMode::Off, &path)?;
    eprintln!("[probe-fp-radical] running AuthoritativeExperimental ...");
    let auth = run_mode(
        cli.rows,
        cli.edit_cycles,
        FormulaPlaneMode::AuthoritativeExperimental,
        &path,
    )?;

    let speedup_eval_first = if auth.eval_first_ms == 0 {
        f64::INFINITY
    } else {
        off.eval_first_ms as f64 / auth.eval_first_ms as f64
    };
    let speedup_eval_warm = if auth.eval_warm_ms == 0 {
        f64::INFINITY
    } else {
        off.eval_warm_ms as f64 / auth.eval_warm_ms as f64
    };

    let report = ProbeReport {
        rows: cli.rows,
        edit_cycles: cli.edit_cycles,
        off,
        auth,
        speedup_eval_first,
        speedup_eval_warm,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);

    // Markdown summary on stderr for human readability.
    eprintln!();
    eprintln!("# FormulaPlane radical probe");
    eprintln!();
    eprintln!("rows={} edit_cycles={}", report.rows, report.edit_cycles);
    eprintln!();
    eprintln!("| metric | Off | AuthoritativeExperimental |");
    eprintln!("|---|---:|---:|");
    eprintln!(
        "| load_ms | {} | {} |",
        report.off.load_ms, report.auth.load_ms
    );
    eprintln!(
        "| eval_first_ms | {} | {} |",
        report.off.eval_first_ms, report.auth.eval_first_ms
    );
    eprintln!(
        "| eval_warm_ms | {} | {} |",
        report.off.eval_warm_ms, report.auth.eval_warm_ms
    );
    eprintln!(
        "| edit_recalc_ms_avg | {:.2} | {:.2} |",
        report.off.edit_recalc_ms_avg, report.auth.edit_recalc_ms_avg
    );
    eprintln!(
        "| graph_formula_vertex_count | {} | {} |",
        report.off.graph_formula_vertex_count, report.auth.graph_formula_vertex_count
    );
    eprintln!(
        "| formula_plane_active_span_count | {} | {} |",
        report.off.formula_plane_active_span_count, report.auth.formula_plane_active_span_count
    );
    eprintln!(
        "| formula_plane_producer_result_entries | {} | {} |",
        report.off.formula_plane_producer_result_entries,
        report.auth.formula_plane_producer_result_entries
    );
    eprintln!(
        "| formula_plane_consumer_read_entries | {} | {} |",
        report.off.formula_plane_consumer_read_entries,
        report.auth.formula_plane_consumer_read_entries
    );
    eprintln!();
    eprintln!(
        "speedup_eval_first={:.2}x speedup_eval_warm={:.2}x",
        report.speedup_eval_first, report.speedup_eval_warm
    );
    eprintln!();
    eprintln!(
        "samples (Auth): B1={:?} B[{}]={:?} B[{}]={:?} C[{}]={:?}",
        report.auth.sample_b1,
        report.rows / 2,
        report.auth.sample_b_mid,
        report.rows,
        report.auth.sample_b_last,
        report.rows,
        report.auth.sample_c_last,
    );

    Ok(())
}
