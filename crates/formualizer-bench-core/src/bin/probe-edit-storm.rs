//! Standing probe for bulk-edit dirty-propagation cost ("edit storm").
//!
//! Shape under test: K input value cells (`A1:AK`) all feed ONE shared
//! downstream component — a fan-in-32 rollup tree of `SUM`s over the inputs
//! plus a K-long arithmetic chain hanging off the rollup root, so the
//! component downstream of every single input is O(K).
//!
//! Each public edit (`Workbook::set_value`) runs a full dirty-propagation
//! BFS over that component, so a per-cell edit loop is O(K × component) =
//! O(K²). The batch APIs (`Workbook::set_values`) historically looped the
//! same single-edit primitive, inheriting the quadratic cost; the deferred-
//! dirty scope collapses the batch to ONE multi-source BFS = O(component).
//!
//! The probe measures, per K and per changelog mode:
//!   (a) per-cell loop:  K × `wb.set_value(...)`        (edit_ms)
//!   (b) batch:          one  `wb.set_values(...)` call (edit_ms)
//! plus the recalc after each edit storm, with value self-checks (rollup
//! root and chain tail must reflect the new inputs).
//!
//! Run (release):
//! ```bash
//! cargo run --release -p formualizer-bench-core --features formualizer_runner \
//!   --bin probe-edit-storm -- --k-list 1000,5000,20000 --changelog both
//! ```

#[cfg(feature = "formualizer_runner")]
use std::time::Instant;

#[cfg(feature = "formualizer_runner")]
use anyhow::{Result, bail};
#[cfg(feature = "formualizer_runner")]
use clap::Parser;
#[cfg(feature = "formualizer_runner")]
use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};
#[cfg(feature = "formualizer_runner")]
use serde::Serialize;

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --bin probe-edit-storm -- ..."
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
#[derive(Debug, Parser)]
#[command(about = "Bulk-edit dirty-propagation probe (per-cell loop vs batch set_values)")]
struct Cli {
    /// Comma-separated input counts to sweep.
    #[arg(long, default_value = "1000,5000,20000")]
    k_list: String,

    /// Changelog modes to measure: `off`, `on`, or `both`.
    /// `off` = `WorkbookConfig::ephemeral()` (engine fast path);
    /// `on`  = ephemeral + changelog (the `edit_with_logger` path).
    #[arg(long, default_value = "both")]
    changelog: String,

    /// Skip the per-cell loop arm (useful once the quadratic baseline is on
    /// record and only the batch arm is being tracked).
    #[arg(long, default_value_t = false)]
    skip_per_cell: bool,

    #[arg(long, default_value = "phase-candidate")]
    label: String,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct EditStormProbeReport {
    label: String,
    fan_in: usize,
    scenarios: Vec<ScenarioReport>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct ScenarioReport {
    k: usize,
    changelog: bool,
    /// Total formula count in the shared component (rollup tree + chain).
    component_formulas: usize,
    arms: Vec<ArmReport>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct ArmReport {
    /// "per_cell" (K × set_value) or "batch" (one set_values).
    arm: &'static str,
    build_ms: f64,
    initial_eval_ms: f64,
    /// Wall time of the edit storm itself (the measured quantity).
    edit_ms: f64,
    /// Wall time of the recalc that consumes the dirtied component.
    recalc_ms: f64,
    /// Root of the rollup tree after the edit recalc (must equal 2K).
    root_after: f64,
    /// Chain tail after the edit recalc (must equal 2K + chain_len - 1).
    chain_tail_after: f64,
}

#[cfg(feature = "formualizer_runner")]
const SHEET: &str = "Sheet1";
#[cfg(feature = "formualizer_runner")]
const FAN_IN: usize = 32;

/// Column layout: inputs in col 1; rollup tree levels in cols 2, 3, ...;
/// chain in `chain_col` (first free column after the tree levels).
#[cfg(feature = "formualizer_runner")]
struct Fixture {
    k: usize,
    chain_col: u32,
    chain_len: usize,
    root_col: u32,
    component_formulas: usize,
}

#[cfg(feature = "formualizer_runner")]
fn col_letter(col: u32) -> String {
    // 1-based column index -> A1 letters.
    let mut c = col;
    let mut out = Vec::new();
    while c > 0 {
        let rem = ((c - 1) % 26) as u8;
        out.push(b'A' + rem);
        c = (c - 1) / 26;
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

/// Build the workbook: K inputs (=1), fan-in-32 SUM rollup tree, K-long
/// chain off the root. Values are inserted before any formula exists and
/// formulas bottom-up, so build cost stays near-linear even without the
/// batch fix (each set_formula propagates into a not-yet-built downstream).
#[cfg(feature = "formualizer_runner")]
fn build_fixture(wb: &mut Workbook, k: usize) -> Result<Fixture> {
    // Inputs.
    let rows: Vec<Vec<LiteralValue>> = (0..k).map(|_| vec![LiteralValue::Int(1)]).collect();
    wb.set_values(SHEET, 1, 1, &rows)
        .map_err(|e| anyhow::anyhow!("seed inputs: {e}"))?;

    // Rollup tree: level L in column 1+L sums fan-in-32 groups of level L-1.
    let mut formula_count = 0usize;
    let mut level_col = 1u32;
    let mut level_count = k;
    while level_count > 1 {
        let next_col = level_col + 1;
        let next_count = level_count.div_ceil(FAN_IN);
        let src = col_letter(level_col);
        for j in 0..next_count {
            let start = j * FAN_IN + 1;
            let end = ((j + 1) * FAN_IN).min(level_count);
            wb.set_formula(
                SHEET,
                j as u32 + 1,
                next_col,
                &format!("=SUM({src}{start}:{src}{end})"),
            )
            .map_err(|e| anyhow::anyhow!("tree formula: {e}"))?;
            formula_count += 1;
        }
        level_col = next_col;
        level_count = next_count;
    }
    let root_col = level_col;

    // Chain off the root, K cells long, in the next free column.
    let chain_col = root_col + 1;
    let chain_len = k;
    let root = format!("{}1", col_letter(root_col));
    let chain = col_letter(chain_col);
    wb.set_formula(SHEET, 1, chain_col, &format!("={root}+0"))
        .map_err(|e| anyhow::anyhow!("chain head: {e}"))?;
    formula_count += 1;
    for i in 2..=chain_len {
        wb.set_formula(SHEET, i as u32, chain_col, &format!("={chain}{}+1", i - 1))
            .map_err(|e| anyhow::anyhow!("chain formula: {e}"))?;
        formula_count += 1;
    }

    Ok(Fixture {
        k,
        chain_col,
        chain_len,
        root_col,
        component_formulas: formula_count,
    })
}

#[cfg(feature = "formualizer_runner")]
fn num(wb: &Workbook, row: u32, col: u32) -> Result<f64> {
    match wb.get_value(SHEET, row, col) {
        Some(LiteralValue::Number(n)) => Ok(n),
        Some(LiteralValue::Int(i)) => Ok(i as f64),
        other => bail!("expected number at r{row}c{col}, got {other:?}"),
    }
}

#[cfg(feature = "formualizer_runner")]
fn check_fixture(wb: &Workbook, fx: &Fixture, input_value: f64) -> Result<(f64, f64)> {
    let expected_root = input_value * fx.k as f64;
    let root = num(wb, 1, fx.root_col)?;
    if (root - expected_root).abs() > 1e-6 {
        bail!("rollup root {root} != expected {expected_root}");
    }
    let expected_tail = expected_root + (fx.chain_len as f64 - 1.0);
    let tail = num(wb, fx.chain_len as u32, fx.chain_col)?;
    if (tail - expected_tail).abs() > 1e-6 {
        bail!("chain tail {tail} != expected {expected_tail}");
    }
    Ok((root, tail))
}

#[cfg(feature = "formualizer_runner")]
fn make_workbook(changelog: bool) -> Workbook {
    let mut config = WorkbookConfig::ephemeral();
    config.enable_changelog = changelog;
    Workbook::new_with_config(config)
}

#[cfg(feature = "formualizer_runner")]
fn run_arm(k: usize, changelog: bool, arm: &'static str) -> Result<(ArmReport, usize)> {
    let mut wb = make_workbook(changelog);
    let build_start = Instant::now();
    let fx = build_fixture(&mut wb, k)?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;

    let eval_start = Instant::now();
    wb.evaluate_all()
        .map_err(|e| anyhow::anyhow!("initial evaluate_all: {e}"))?;
    let initial_eval_ms = eval_start.elapsed().as_secs_f64() * 1000.0;
    check_fixture(&wb, &fx, 1.0)?;

    // Edit storm: rewrite every input to 2.
    let edit_ms = match arm {
        "per_cell" => {
            let start = Instant::now();
            for r in 1..=k as u32 {
                wb.set_value(SHEET, r, 1, LiteralValue::Number(2.0))
                    .map_err(|e| anyhow::anyhow!("per-cell set_value: {e}"))?;
            }
            start.elapsed().as_secs_f64() * 1000.0
        }
        "batch" => {
            let rows: Vec<Vec<LiteralValue>> =
                (0..k).map(|_| vec![LiteralValue::Number(2.0)]).collect();
            let start = Instant::now();
            wb.set_values(SHEET, 1, 1, &rows)
                .map_err(|e| anyhow::anyhow!("batch set_values: {e}"))?;
            start.elapsed().as_secs_f64() * 1000.0
        }
        other => bail!("unknown arm {other}"),
    };

    let recalc_start = Instant::now();
    wb.evaluate_all()
        .map_err(|e| anyhow::anyhow!("post-edit evaluate_all: {e}"))?;
    let recalc_ms = recalc_start.elapsed().as_secs_f64() * 1000.0;
    let (root_after, chain_tail_after) = check_fixture(&wb, &fx, 2.0)?;

    Ok((
        ArmReport {
            arm,
            build_ms,
            initial_eval_ms,
            edit_ms,
            recalc_ms,
            root_after,
            chain_tail_after,
        },
        fx.component_formulas,
    ))
}

#[cfg(feature = "formualizer_runner")]
fn run_probe(cli: &Cli) -> Result<EditStormProbeReport> {
    let ks: Vec<usize> = cli
        .k_list
        .split(',')
        .map(|s| s.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("--k-list parse: {e}"))?;
    if ks.is_empty() || ks.iter().any(|&k| k < FAN_IN) {
        bail!("--k-list needs at least one K >= {FAN_IN}");
    }
    let changelog_modes: Vec<bool> = match cli.changelog.as_str() {
        "off" => vec![false],
        "on" => vec![true],
        "both" => vec![false, true],
        other => bail!("--changelog must be off|on|both, got {other}"),
    };

    let mut scenarios = Vec::new();
    for &k in &ks {
        for &changelog in &changelog_modes {
            let mut arms = Vec::new();
            let mut component_formulas = 0usize;
            let arm_names: &[&'static str] = if cli.skip_per_cell {
                &["batch"]
            } else {
                &["per_cell", "batch"]
            };
            for &arm in arm_names {
                let (report, formulas) = run_arm(k, changelog, arm)?;
                eprintln!(
                    "[edit-storm] k={k} changelog={changelog} arm={arm}: edit {:.1} ms, recalc {:.1} ms (build {:.1} ms, initial eval {:.1} ms)",
                    report.edit_ms, report.recalc_ms, report.build_ms, report.initial_eval_ms
                );
                component_formulas = formulas;
                arms.push(report);
            }
            scenarios.push(ScenarioReport {
                k,
                changelog,
                component_formulas,
                arms,
            });
        }
    }

    Ok(EditStormProbeReport {
        label: cli.label.clone(),
        fan_in: FAN_IN,
        scenarios,
    })
}
