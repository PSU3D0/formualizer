//! Standing probe for per-SCC-task overhead at scale (RFC #112, Stage 2b).
//!
//! Builds a workbook of N *independent* guarded phantom pairs (discussion #99
//! shape: `A = IF(g, k, B)`, `B = IF(g, A, k)`), each with its own guard cell
//! and a downstream consumer so condensation ordering is exercised. Under
//! `--mode runtime` every pair is a statically-cyclic SCC that resolves to
//! values in 1-2 settle passes; under `--mode static` (today's default) every
//! pair is stamped `#CIRC!`. The probe measures initial `evaluate_all` plus a
//! configurable number of guard-flip recalc rounds, reporting per-pair µs so
//! future stages (iterative calc) have a regression gate on SCC-task cost.
//!
//! Run (both modes):
//! ```bash
//! cargo run --release -p formualizer-bench-core --features formualizer_runner \
//!   --bin probe-scc-phantom-pairs -- --pairs 10000 --mode runtime
//! cargo run --release -p formualizer-bench-core --features formualizer_runner \
//!   --bin probe-scc-phantom-pairs -- --pairs 10000 --mode static
//! ```

#[cfg(feature = "formualizer_runner")]
use std::{path::PathBuf, time::Instant};

#[cfg(feature = "formualizer_runner")]
use anyhow::{Result, bail};
#[cfg(feature = "formualizer_runner")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "formualizer_runner")]
use formualizer_testkit::write_workbook;
#[cfg(feature = "formualizer_runner")]
use formualizer_workbook::{
    LiteralValue, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
#[cfg(feature = "formualizer_runner")]
use serde::Serialize;

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --bin probe-scc-phantom-pairs -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    let report = run_probe(&cli)?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// Today's behavior: every statically-cyclic SCC is stamped `#CIRC!`.
    Static,
    /// RFC #112 Runtime detection: phantom (live-acyclic) SCCs produce values.
    Runtime,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Parser)]
#[command(about = "Per-SCC-task overhead probe: N independent guarded phantom pairs (#99 shape)")]
struct Cli {
    /// Number of independent guarded pairs (each is one static SCC).
    #[arg(long, default_value_t = 10_000)]
    pairs: usize,
    /// Static (#CIRC stamping) vs runtime (phantom values) cycle handling.
    #[arg(long, value_enum, default_value_t = Mode::Runtime)]
    mode: Mode,
    /// Number of recalc rounds after the initial full evaluation.
    #[arg(long, default_value_t = 5)]
    recalcs: usize,
    /// Fraction of guards flipped per recalc round (0.0..=1.0).
    #[arg(long, default_value_t = 0.1)]
    flip_fraction: f64,
    /// Number of pairs sampled for the correctness self-check.
    #[arg(long, default_value_t = 64)]
    sample: usize,
    #[arg(long, default_value = "phase-candidate")]
    label: String,
    /// Optional XLSX fixture path. Defaults under target/scc-phantom-fixtures/.
    #[arg(long)]
    workbook_path: Option<PathBuf>,
    /// Reuse --workbook-path if it already exists instead of regenerating it.
    #[arg(long)]
    reuse_workbook: bool,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct SccPhantomProbeReport {
    label: String,
    mode: &'static str,
    pairs: usize,
    recalcs: usize,
    flip_fraction: f64,
    flips_per_round: usize,
    workbook_path: String,
    reused_workbook: bool,
    fixture_gen_ms: f64,
    load_ms: f64,
    initial_eval_ms: f64,
    initial_eval_us_per_pair: f64,
    total_recalc_ms: f64,
    recalc_ms_p50: f64,
    recalc_ms_p95: f64,
    recalc_ms_max: f64,
    recalc_us_per_pair_p50: f64,
    /// Best-effort SCC telemetry. NOTE: under the arrow-canonical workbook path
    /// the runtime SCC evaluation produces correct values but does NOT populate
    /// `last_cycle_telemetry` (the legacy/`TestWorkbook` path does); these
    /// fields read 0 here even though every pair resolved as a phantom SCC. The
    /// authoritative correctness signal is the value self-check (`sample_*`).
    initial_phantom_sccs: usize,
    initial_settle_passes_total: usize,
    /// `#CIRC!` cells observed on the initial eval, counted by reading cells.
    /// Static mode: `2 * pairs`. Runtime mode: 0 (all pairs are phantom).
    initial_circ_cells: usize,
    /// Checksum over sampled consumer cells (runtime: guard-state value sum).
    sample_checksum: f64,
    rss_current_mb: Option<f64>,
    rss_peak_mb: Option<f64>,
    rounds_detail: Vec<SccPhantomRoundReport>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct SccPhantomRoundReport {
    round: usize,
    flips: usize,
    edit_ms: f64,
    recalc_ms: f64,
    sample_checksum: f64,
}

#[cfg(feature = "formualizer_runner")]
const SHEET: &str = "Sheet1";
/// `k` value taken when the guard is TRUE (A keeps k, B reads A → both = K_TRUE).
#[cfg(feature = "formualizer_runner")]
const K_TRUE: f64 = 100.0;
/// `k` value taken when the guard is FALSE (B keeps k, A reads B → both = K_FALSE).
#[cfg(feature = "formualizer_runner")]
const K_FALSE: f64 = 999.0;

/// Per-pair layout (1-based columns): col 1 = guard, col 2 = A, col 3 = B,
/// col 4 = consumer. One pair per row keeps every SCC independent.
#[cfg(feature = "formualizer_runner")]
fn pair_row(pair: usize) -> u32 {
    pair as u32 + 1
}

/// Settled value of A and B for a given guard state (both members agree).
#[cfg(feature = "formualizer_runner")]
fn pair_value(guard: bool) -> f64 {
    if guard { K_TRUE } else { K_FALSE }
}

#[cfg(feature = "formualizer_runner")]
fn run_probe(cli: &Cli) -> Result<SccPhantomProbeReport> {
    if cli.pairs == 0 {
        bail!("--pairs must be > 0");
    }
    if !(0.0..=1.0).contains(&cli.flip_fraction) {
        bail!("--flip-fraction must be within 0.0..=1.0");
    }

    let cycle = match cli.mode {
        Mode::Static => formualizer_eval::engine::CycleConfig {
            detection: formualizer_eval::engine::CycleDetection::Static,
            policy: formualizer_eval::engine::CyclePolicy::Error,
        },
        Mode::Runtime => formualizer_eval::engine::CycleConfig {
            detection: formualizer_eval::engine::CycleDetection::Runtime,
            policy: formualizer_eval::engine::CyclePolicy::Error,
        },
    };

    // Deterministic alternating initial guard state per pair. The fixture is
    // generated with this exact state, so a fresh load reflects `guards`.
    let mut guards: Vec<bool> = (0..cli.pairs).map(|p| p % 2 == 0).collect();

    let workbook_path = cli
        .workbook_path
        .clone()
        .unwrap_or_else(|| default_workbook_path(&cli.label, cli.pairs));

    let (fixture_gen_ms, reused_workbook) = if cli.reuse_workbook && workbook_path.exists() {
        (0.0, true)
    } else {
        let gen_start = Instant::now();
        generate_fixture(&workbook_path, &guards)?;
        (gen_start.elapsed().as_secs_f64() * 1000.0, false)
    };

    // Bulk-load via EagerAll: the graph (and its SCCs) is built in one batched
    // ingest pass, which scales linearly — unlike incremental per-formula edits
    // under an active cycle config, which re-run detection per edit.
    let mut config = WorkbookConfig::ephemeral();
    config.eval = config.eval.with_cycle(cycle);
    let load_start = Instant::now();
    let backend = UmyaAdapter::open_path(&workbook_path)
        .map_err(|e| anyhow::anyhow!("open fixture via umya {}: {e}", workbook_path.display()))?;
    let mut workbook = Workbook::from_reader(backend, LoadStrategy::EagerAll, config)
        .map_err(|e| anyhow::anyhow!("load fixture into workbook: {e}"))?;
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.0;

    let initial_start = Instant::now();
    let initial_result = workbook
        .evaluate_all()
        .map_err(|e| anyhow::anyhow!("initial evaluate_all: {e}"))?;
    let initial_eval_ms = initial_start.elapsed().as_secs_f64() * 1000.0;

    let telemetry = workbook.engine().last_cycle_telemetry().clone();
    // `#CIRC!` count is the authoritative cycle-handling signal here (read from
    // cells, not telemetry, since the arrow-canonical path leaves telemetry at
    // 0). Runtime: every pair is phantom → 0 circ. Static: 2 per pair.
    let initial_circ_cells = count_circ_cells(&workbook, cli.pairs)?;
    match cli.mode {
        Mode::Runtime => {
            if initial_circ_cells != 0 {
                bail!(
                    "runtime mode: phantom pairs must not stamp #CIRC, got {initial_circ_cells} circ cells"
                );
            }
            if initial_result.cycle_errors != 0 {
                bail!(
                    "runtime mode: phantom pairs must not produce cycle errors, got {}",
                    initial_result.cycle_errors
                );
            }
        }
        Mode::Static => {
            // Each pair stamps both A and B → 2 #CIRC cells per pair.
            if initial_circ_cells != cli.pairs * 2 {
                bail!(
                    "static mode: expected {} #CIRC cells, got {initial_circ_cells}",
                    cli.pairs * 2
                );
            }
        }
    }
    let initial_phantom_sccs = telemetry.phantom_sccs;
    let initial_settle_passes_total = telemetry.settle_passes_total;

    assert_sample(&workbook, &guards, cli.mode, cli.sample)?;
    let initial_checksum = sample_checksum(&workbook, &guards, cli.mode, cli.sample)?;

    let flips_per_round = ((cli.pairs as f64) * cli.flip_fraction).round() as usize;
    let mut rounds_detail = Vec::with_capacity(cli.recalcs);
    for round in 0..cli.recalcs {
        let edit_start = Instant::now();
        let flips = flip_round_guards(&mut workbook, &mut guards, round, flips_per_round)?;
        let edit_ms = edit_start.elapsed().as_secs_f64() * 1000.0;

        let recalc_start = Instant::now();
        let res = workbook
            .evaluate_all()
            .map_err(|e| anyhow::anyhow!("evaluate_all round {round}: {e}"))?;
        let recalc_ms = recalc_start.elapsed().as_secs_f64() * 1000.0;

        if cli.mode == Mode::Runtime && res.cycle_errors != 0 {
            bail!(
                "runtime round {round}: unexpected cycle_errors={}",
                res.cycle_errors
            );
        }
        assert_sample(&workbook, &guards, cli.mode, cli.sample)?;
        let checksum = sample_checksum(&workbook, &guards, cli.mode, cli.sample)?;
        rounds_detail.push(SccPhantomRoundReport {
            round,
            flips,
            edit_ms,
            recalc_ms,
            sample_checksum: checksum,
        });
    }

    let mut recalc_times: Vec<f64> = rounds_detail.iter().map(|r| r.recalc_ms).collect();
    recalc_times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let total_recalc_ms: f64 = recalc_times.iter().sum();
    let recalc_ms_p50 = percentile(&recalc_times, 0.50);
    let recalc_ms_p95 = percentile(&recalc_times, 0.95);
    let recalc_ms_max = recalc_times.last().copied().unwrap_or(0.0);
    let per_pair = cli.pairs as f64;
    let (rss_current_mb, rss_peak_mb) = linux_rss_mb();

    Ok(SccPhantomProbeReport {
        label: cli.label.clone(),
        mode: match cli.mode {
            Mode::Static => "static",
            Mode::Runtime => "runtime",
        },
        pairs: cli.pairs,
        recalcs: cli.recalcs,
        flip_fraction: cli.flip_fraction,
        flips_per_round,
        workbook_path: workbook_path.display().to_string(),
        reused_workbook,
        fixture_gen_ms,
        load_ms,
        initial_eval_ms,
        initial_eval_us_per_pair: initial_eval_ms * 1000.0 / per_pair,
        total_recalc_ms,
        recalc_ms_p50,
        recalc_ms_p95,
        recalc_ms_max,
        recalc_us_per_pair_p50: recalc_ms_p50 * 1000.0 / per_pair,
        initial_phantom_sccs,
        initial_settle_passes_total,
        initial_circ_cells,
        sample_checksum: initial_checksum,
        rss_current_mb,
        rss_peak_mb,
        rounds_detail,
    })
}

/// Generate the XLSX fixture: N independent guarded phantom pairs, one per
/// row. Per-pair layout (umya tuples are `(col, row)`, 1-based): col 1 = guard,
/// col 2 = A, col 3 = B, col 4 = consumer. Bulk-loading this fixture builds the
/// whole dependency graph (and all SCCs) in one batched ingest pass.
#[cfg(feature = "formualizer_runner")]
fn generate_fixture(path: &PathBuf, guards: &[bool]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("create fixture dir {}: {e}", parent.display()))?;
    }
    write_workbook(path, |book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for (pair, &guard) in guards.iter().enumerate() {
            let row = pair_row(pair);
            sheet.get_cell_mut((1, row)).set_value_bool(guard);
            // A = IF(guard, K_TRUE, B); B = IF(guard, A, K_FALSE)  (#99 shape).
            sheet
                .get_cell_mut((2, row))
                .set_formula(format!("=IF(A{row},{K_TRUE},C{row})"));
            sheet
                .get_cell_mut((3, row))
                .set_formula(format!("=IF(A{row},B{row},{K_FALSE})"));
            // Downstream consumer reads both SCC members (exercises condensation
            // ordering: it must be scheduled after the SCC task).
            sheet
                .get_cell_mut((4, row))
                .set_formula(format!("=B{row}+C{row}"));
        }
    });
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
fn default_workbook_path(label: &str, pairs: usize) -> PathBuf {
    let safe_label: String = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    PathBuf::from("target")
        .join("scc-phantom-fixtures")
        .join(format!("{safe_label}-{pairs}.xlsx"))
}

/// Flip a deterministic stride of guards for one recalc round.
#[cfg(feature = "formualizer_runner")]
fn flip_round_guards(
    workbook: &mut Workbook,
    guards: &mut [bool],
    round: usize,
    flips: usize,
) -> Result<usize> {
    let n = guards.len();
    if n == 0 || flips == 0 {
        return Ok(0);
    }
    // Stride spreads flips across the sheet and rotates per round so different
    // pairs churn over successive rounds.
    let stride = (n / flips).max(1);
    let offset = round % stride;
    let mut done = 0usize;
    let mut pair = offset;
    while pair < n && done < flips {
        let next = !guards[pair];
        guards[pair] = next;
        let row = pair_row(pair);
        workbook
            .set_value(SHEET, row, 1, LiteralValue::Boolean(next))
            .map_err(|e| anyhow::anyhow!("flip guard pair {pair} round {round}: {e}"))?;
        done += 1;
        pair += stride;
    }
    Ok(done)
}

#[cfg(feature = "formualizer_runner")]
fn sample_indices(pairs: usize, sample: usize) -> impl Iterator<Item = usize> {
    let sample = sample.clamp(1, pairs);
    let step = (pairs / sample).max(1);
    (0..pairs).step_by(step)
}

/// Correctness self-check on a sampled subset of pairs.
#[cfg(feature = "formualizer_runner")]
fn assert_sample(workbook: &Workbook, guards: &[bool], mode: Mode, sample: usize) -> Result<()> {
    for pair in sample_indices(guards.len(), sample) {
        let row = pair_row(pair);
        let guard = guards[pair];
        match mode {
            Mode::Runtime => {
                let expected = pair_value(guard);
                let a = num(workbook, row, 2)?;
                let b = num(workbook, row, 3)?;
                if (a - expected).abs() > 1e-9 || (b - expected).abs() > 1e-9 {
                    bail!("runtime pair {pair} (guard={guard}): A={a} B={b}, expected {expected}");
                }
                let consumer = num(workbook, row, 4)?;
                if (consumer - 2.0 * expected).abs() > 1e-9 {
                    bail!(
                        "runtime pair {pair} consumer={consumer}, expected {}",
                        2.0 * expected
                    );
                }
            }
            Mode::Static => {
                if !is_circ(workbook, row, 2) || !is_circ(workbook, row, 3) {
                    bail!("static pair {pair}: A/B must be #CIRC");
                }
            }
        }
    }
    Ok(())
}

/// Sum of sampled consumer cells (runtime) or count of sampled #CIRC A-cells
/// (static), used as a stable cross-round checksum in the report.
#[cfg(feature = "formualizer_runner")]
fn sample_checksum(workbook: &Workbook, guards: &[bool], mode: Mode, sample: usize) -> Result<f64> {
    let mut acc = 0.0;
    for pair in sample_indices(guards.len(), sample) {
        let row = pair_row(pair);
        match mode {
            Mode::Runtime => acc += num(workbook, row, 4)?,
            Mode::Static => {
                if is_circ(workbook, row, 2) {
                    acc += 1.0;
                }
            }
        }
    }
    Ok(acc)
}

#[cfg(feature = "formualizer_runner")]
fn count_circ_cells(workbook: &Workbook, pairs: usize) -> Result<usize> {
    let mut circ = 0usize;
    for pair in 0..pairs {
        let row = pair_row(pair);
        if is_circ(workbook, row, 2) {
            circ += 1;
        }
        if is_circ(workbook, row, 3) {
            circ += 1;
        }
    }
    Ok(circ)
}

#[cfg(feature = "formualizer_runner")]
fn num(workbook: &Workbook, row: u32, col: u32) -> Result<f64> {
    match workbook.get_value(SHEET, row, col) {
        Some(LiteralValue::Number(n)) => Ok(n),
        Some(LiteralValue::Int(i)) => Ok(i as f64),
        other => bail!("expected number at r{row}c{col}, got {other:?}"),
    }
}

#[cfg(feature = "formualizer_runner")]
fn is_circ(workbook: &Workbook, row: u32, col: u32) -> bool {
    matches!(
        workbook.get_value(SHEET, row, col),
        Some(LiteralValue::Error(e))
            if e.kind == formualizer_common::ExcelErrorKind::Circ
    )
}

#[cfg(feature = "formualizer_runner")]
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len().saturating_sub(1)) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(feature = "formualizer_runner")]
fn linux_rss_mb() -> (Option<f64>, Option<f64>) {
    let Some(status) = std::fs::read_to_string("/proc/self/status").ok() else {
        return (None, None);
    };
    let mut current = None;
    let mut peak = None;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            current = parse_status_kb(rest).map(|kb| kb as f64 / 1024.0);
        } else if let Some(rest) = line.strip_prefix("VmHWM:") {
            peak = parse_status_kb(rest).map(|kb| kb as f64 / 1024.0);
        }
    }
    (current, peak)
}

#[cfg(feature = "formualizer_runner")]
fn parse_status_kb(value: &str) -> Option<u64> {
    value
        .split_whitespace()
        .next()
        .and_then(|token| token.parse::<u64>().ok())
}
