//! Standing probe for iterative-calculation SCC cost (RFC #113, Stage 3).
//!
//! Validates the design-doc cost model (§9 of the Stage 2 SCC design doc):
//! iteration is SCC-scoped, cost = `passes × |SCC|` member evaluations at
//! ~3.3 µs/eval. The probe runs `CyclePolicy::Iterate` (Runtime detection)
//! over two workloads selected by `--workload`:
//!
//! * `pairs` — N independent *convergent* 2-member cycles in the circular-
//!   interest shape `A = 0.5*B + 10`, `B = 0.5*A + 10` (fixed point 20,20;
//!   converges in ~15-25 passes at `max_change` 0.001) plus a downstream
//!   consumer per pair. Measures initial eval and `--recalcs` no-edit recalc
//!   rounds (iterating SCCs self-redirty every recalc by design — #130 — so
//!   recalc re-iterates even without edits) and one small-edit round.
//!
//! * `big-scc` — one SCC of `--members` cells in a convergent ring
//!   (`cell_i = 0.9 * cell_{i-1} + 1`, wraparound — contractive, fixed point
//!   10) plus a downstream consumer. `--divergent` uses factor 1.1 (diverges,
//!   caps at `max_iterations`). `--range-reads K` makes each member ALSO read
//!   a K-wide range of its SCC neighbours, exposing the deferred O(|SCC|)
//!   live-edge range-intersection scaling lever (design §9). Headline metric
//!   is µs-per-member-pass.
//!
//! Run (release):
//! ```bash
//! cargo run --release -p formualizer-bench-core --features formualizer_runner \
//!   --bin probe-scc-iterate -- --workload pairs --pairs 10000
//! cargo run --release -p formualizer-bench-core --features formualizer_runner \
//!   --bin probe-scc-iterate -- --workload big-scc --members 1000
//! cargo run --release -p formualizer-bench-core --features formualizer_runner \
//!   --bin probe-scc-iterate -- --workload big-scc --members 1000 --max-change 1e-300
//! cargo run --release -p formualizer-bench-core --features formualizer_runner \
//!   --bin probe-scc-iterate -- --workload big-scc --members 1000 --divergent
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
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --bin probe-scc-iterate -- ..."
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
enum Workload {
    /// N independent convergent 2-member cycles (circular-interest shape).
    Pairs,
    /// One large convergent (or `--divergent`) ring SCC.
    BigScc,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Parser)]
#[command(about = "Iterative-calculation SCC cost probe (RFC #113 Stage 3 cost-model gate)")]
struct Cli {
    /// Which workload to build/measure.
    #[arg(long, value_enum, default_value_t = Workload::Pairs)]
    workload: Workload,

    /// `pairs` workload: number of independent convergent 2-member cycles.
    #[arg(long, default_value_t = 10_000)]
    pairs: usize,

    /// `big-scc` workload: number of members in the single ring SCC.
    #[arg(long, default_value_t = 1000)]
    members: usize,
    /// `big-scc` workload: use a divergent ring factor (>1) so the SCC never
    /// converges and stops at `--max-iterations` (the at-cap stress number).
    #[arg(long, default_value_t = false)]
    divergent: bool,
    /// `big-scc` workload: each member additionally reads a K-wide range of
    /// its SCC neighbours (0 = scalar refs only). Exposes the deferred
    /// O(|SCC|) per-range live-edge intersection scaling lever (design §9).
    #[arg(long, default_value_t = 0)]
    range_reads: usize,

    /// Iterative-calc cap (Excel `max_iterations`).
    #[arg(long, default_value_t = 100)]
    max_iterations: u32,
    /// Iterative-calc convergence threshold (Excel `max_change`). Use a tiny
    /// value (e.g. 1e-300) to force a true at-cap run; 0.0 is rejected by
    /// config validation for the per-member numeric rule.
    #[arg(long, default_value_t = 0.001)]
    max_change: f64,

    /// Number of no-edit recalc rounds after the initial full evaluation.
    /// Each re-iterates the SCC(s) (self-redirty, #130).
    #[arg(long, default_value_t = 5)]
    recalcs: usize,

    /// Number of SCCs sampled for the correctness self-check.
    #[arg(long, default_value_t = 64)]
    sample: usize,
    #[arg(long, default_value = "phase-candidate")]
    label: String,
    /// Optional XLSX fixture path. Defaults under target/scc-iterate-fixtures/.
    #[arg(long)]
    workbook_path: Option<PathBuf>,
    /// Reuse --workbook-path if it already exists instead of regenerating it.
    #[arg(long)]
    reuse_workbook: bool,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct SccIterateProbeReport {
    label: String,
    workload: &'static str,
    /// Total iterating members across all SCC tasks (pairs: 2*pairs; big-scc:
    /// members). The cost-model unit is `passes × members`.
    members_total: usize,
    /// Number of independent iterating SCCs (pairs: pairs; big-scc: 1).
    scc_count: usize,
    max_iterations: u32,
    max_change: f64,
    divergent: bool,
    range_reads: usize,
    recalcs: usize,
    workbook_path: String,
    reused_workbook: bool,
    fixture_gen_ms: f64,
    load_ms: f64,

    initial_eval_ms: f64,
    /// `settle_passes_total` from the initial eval — summed passes across
    /// every iterating SCC task. For big-scc (one SCC) this equals the SCC's
    /// pass count.
    initial_settle_passes_total: usize,
    /// Largest pass count any single SCC needed on the initial eval.
    initial_max_passes_single_scc: usize,
    initial_iterated_sccs: usize,
    initial_converged_sccs: usize,
    initial_capped_sccs: usize,
    initial_max_abs_delta_at_stop: f64,
    /// Headline cost-model metric: µs per (member × pass) on the initial eval.
    /// Cost model expects ~3.3 µs/member-eval.
    initial_us_per_member_pass: f64,
    /// big-scc convenience: total ms for the single SCC's full iteration.
    initial_us_per_member: f64,

    /// No-edit recalc rounds (each self-redirties and re-iterates).
    total_recalc_ms: f64,
    recalc_ms_p50: f64,
    recalc_ms_p95: f64,
    recalc_ms_max: f64,
    recalc_us_per_member_pass_p50: f64,
    /// Passes on a representative recalc round (the p50 round's SCC task).
    recalc_settle_passes_total_p50: usize,

    /// One small-edit recalc round (pairs: bump one consumer-feeding input;
    /// big-scc: bump the seed of one member). Measures dirty-driven re-iterate.
    edit_recalc_ms: f64,

    /// Checksum over sampled SCCs (fixed-point values), stable across rounds
    /// for convergent workloads.
    sample_checksum: f64,
    rss_current_mb: Option<f64>,
    rss_peak_mb: Option<f64>,
    rounds_detail: Vec<SccIterateRoundReport>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct SccIterateRoundReport {
    round: usize,
    kind: &'static str,
    recalc_ms: f64,
    settle_passes_total: usize,
    converged_sccs: usize,
    capped_sccs: usize,
    sample_checksum: f64,
}

#[cfg(feature = "formualizer_runner")]
const SHEET: &str = "Sheet1";

/* ─────────────────────────── pairs workload ──────────────────────────── */
//
// Circular-interest shape, one pair per row (1-based columns):
//   col 1 = A = 0.5*B + 10  (B is col 2)
//   col 2 = B = 0.5*A + 10
//   col 3 = consumer = A + B
// Fixed point: a = 0.5b + 10, b = 0.5a + 10 ⟹ a = b = 20, consumer = 40.

#[cfg(feature = "formualizer_runner")]
const PAIR_FIXED_POINT: f64 = 20.0;

#[cfg(feature = "formualizer_runner")]
fn pair_row(pair: usize) -> u32 {
    pair as u32 + 1
}

#[cfg(feature = "formualizer_runner")]
fn generate_pairs_fixture(path: &PathBuf, pairs: usize) -> Result<()> {
    ensure_parent(path)?;
    write_workbook(path, |book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for pair in 0..pairs {
            let row = pair_row(pair);
            sheet
                .get_cell_mut((1, row))
                .set_formula(format!("=0.5*B{row}+10"));
            sheet
                .get_cell_mut((2, row))
                .set_formula(format!("=0.5*A{row}+10"));
            sheet
                .get_cell_mut((3, row))
                .set_formula(format!("=A{row}+B{row}"));
        }
    });
    Ok(())
}

/* ────────────────────────── big-scc workload ─────────────────────────── */
//
// One ring SCC of N members in column 1 (rows 1..=N):
//   cell_i = factor * cell_{i-1} + 1   (wraparound: cell_1 reads cell_N)
// Convergent (factor 0.9): fixed point x = factor*x + 1 ⟹ x = 1/(1-factor) = 10.
// Divergent (factor > 1): never settles, caps at max_iterations.
//
// Evaluation is Gauss-Seidel (write-through commit — design §3.3), so values
// propagate around the ring WITHIN one pass: the effective per-pass growth is
// `factor^members`. A naive factor like 1.1 would overflow to +inf on pass 1
// (1.1^1000 = inf), and inf-vs-inf compares EQUAL → the SCC would falsely
// "converge". To keep the divergent ring growing-but-finite for the full
// `max_iterations` cap, we derive the factor from `members` so that one pass
// multiplies the magnitude by a modest target (~1.05): factor = target^(1/N).
// That gives target^max_iterations ≈ 1.05^100 ≈ 130 after 100 passes — large,
// strictly increasing, never < max_change → a genuine at-cap run.
#[cfg(feature = "formualizer_runner")]
const RING_CONVERGENT_FACTOR: f64 = 0.9;
/// Target magnitude growth per FULL pass for the divergent ring.
#[cfg(feature = "formualizer_runner")]
const RING_DIVERGENT_PER_PASS_GROWTH: f64 = 1.05;

#[cfg(feature = "formualizer_runner")]
fn ring_factor_for(divergent: bool, members: usize) -> f64 {
    if divergent {
        RING_DIVERGENT_PER_PASS_GROWTH.powf(1.0 / members as f64)
    } else {
        RING_CONVERGENT_FACTOR
    }
}

/// Analytic fixed point of the scalar ring (range_reads == 0): x = 1/(1-f).
#[cfg(feature = "formualizer_runner")]
fn ring_fixed_point(factor: f64) -> f64 {
    1.0 / (1.0 - factor)
}

#[cfg(feature = "formualizer_runner")]
fn generate_big_scc_fixture(
    path: &PathBuf,
    members: usize,
    divergent: bool,
    range_reads: usize,
) -> Result<()> {
    ensure_parent(path)?;
    let factor = ring_factor_for(divergent, members);
    write_workbook(path, |book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for i in 0..members {
            let row = i as u32 + 1;
            // Predecessor row in the ring (wraparound).
            let prev = if i == 0 { members } else { i } as u32; // 1-based prev row
            let mut formula = format!("={factor}*A{prev}+1");
            if range_reads > 0 {
                // A K-wide window of neighbours strictly after this member,
                // wrapping within the member rows. Adds K live edges/member.
                // Scaled by a tiny coefficient so it perturbs but does not
                // destroy convergence (kept for the at-cap path too).
                let start = (i % members) + 1; // 1-based
                let end = (start + range_reads.saturating_sub(1)).min(members);
                formula.push_str(&format!("+0.0001*SUM(A{start}:A{end})"));
            }
            sheet.get_cell_mut((1, row)).set_formula(formula);
        }
        // Downstream consumer reads the whole member column (condensation
        // ordering: scheduled after the SCC task).
        sheet
            .get_cell_mut((2, 1))
            .set_formula(format!("=SUM(A1:A{members})"));
    });
    Ok(())
}

/* ──────────────────────────────── driver ─────────────────────────────── */

#[cfg(feature = "formualizer_runner")]
fn run_probe(cli: &Cli) -> Result<SccIterateProbeReport> {
    match cli.workload {
        Workload::Pairs => {
            if cli.pairs == 0 {
                bail!("--pairs must be > 0");
            }
        }
        Workload::BigScc => {
            if cli.members < 2 {
                bail!("--members must be >= 2 (a ring needs at least 2 cells)");
            }
        }
    }

    let cycle = formualizer_eval::engine::CycleConfig::iterate(cli.max_iterations, cli.max_change);
    cycle
        .validate()
        .map_err(|e| anyhow::anyhow!("invalid iterate config: {e}"))?;

    let workbook_path = cli
        .workbook_path
        .clone()
        .unwrap_or_else(|| default_workbook_path(cli));

    let (fixture_gen_ms, reused_workbook) = if cli.reuse_workbook && workbook_path.exists() {
        (0.0, true)
    } else {
        let gen_start = Instant::now();
        match cli.workload {
            Workload::Pairs => generate_pairs_fixture(&workbook_path, cli.pairs)?,
            Workload::BigScc => generate_big_scc_fixture(
                &workbook_path,
                cli.members,
                cli.divergent,
                cli.range_reads,
            )?,
        }
        (gen_start.elapsed().as_secs_f64() * 1000.0, false)
    };

    let mut config = WorkbookConfig::ephemeral();
    config.eval = config.eval.with_cycle(cycle);
    let load_start = Instant::now();
    let backend = UmyaAdapter::open_path(&workbook_path)
        .map_err(|e| anyhow::anyhow!("open fixture via umya {}: {e}", workbook_path.display()))?;
    let mut workbook = Workbook::from_reader(backend, LoadStrategy::EagerAll, config)
        .map_err(|e| anyhow::anyhow!("load fixture into workbook: {e}"))?;
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.0;

    let (members_total, scc_count) = match cli.workload {
        Workload::Pairs => (cli.pairs * 2, cli.pairs),
        Workload::BigScc => (cli.members, 1),
    };

    let initial_start = Instant::now();
    workbook
        .evaluate_all()
        .map_err(|e| anyhow::anyhow!("initial evaluate_all: {e}"))?;
    let initial_eval_ms = initial_start.elapsed().as_secs_f64() * 1000.0;

    let t = workbook.engine().last_cycle_telemetry().clone();
    let initial_settle_passes_total = t.settle_passes_total;
    let initial_max_passes_single_scc = t.max_passes_single_scc;
    let initial_iterated_sccs = t.iterated_sccs;
    let initial_converged_sccs = t.converged_sccs;
    let initial_capped_sccs = t.capped_sccs;
    let initial_max_abs_delta_at_stop = t.max_abs_delta_at_stop;

    // Telemetry self-checks: every iterating SCC must be accounted for.
    if t.iterated_sccs != scc_count {
        bail!(
            "expected {scc_count} iterating SCCs, telemetry reports {}",
            t.iterated_sccs
        );
    }
    match cli.workload {
        Workload::Pairs => {
            if t.converged_sccs != scc_count {
                bail!(
                    "pairs are convergent: expected {scc_count} converged SCCs, got {} (capped {})",
                    t.converged_sccs,
                    t.capped_sccs
                );
            }
        }
        Workload::BigScc => {
            if cli.divergent {
                if t.capped_sccs != 1 {
                    bail!(
                        "divergent ring must cap at max_iterations: expected 1 capped SCC, got {} (converged {})",
                        t.capped_sccs,
                        t.converged_sccs
                    );
                }
            } else if t.converged_sccs != 1 {
                bail!(
                    "convergent ring must converge: expected 1 converged SCC, got {} (capped {})",
                    t.converged_sccs,
                    t.capped_sccs
                );
            }
        }
    }

    assert_values(&workbook, cli)?;
    let initial_checksum = sample_checksum(&workbook, cli)?;

    // Cost-model unit: µs per (member × pass). passes here is the summed pass
    // count across all SCC tasks; members_total / scc_count = members/SCC, so
    // members_total * (passes/scc_count) = member-pass count.
    let member_passes = member_pass_count(members_total, scc_count, initial_settle_passes_total);
    let initial_us_per_member_pass = if member_passes > 0.0 {
        initial_eval_ms * 1000.0 / member_passes
    } else {
        0.0
    };
    let initial_us_per_member = initial_eval_ms * 1000.0 / members_total as f64;

    // No-edit recalc rounds: iterating SCCs self-redirty (#130), so each
    // round re-iterates the full SCC even with no edit.
    let mut rounds_detail = Vec::with_capacity(cli.recalcs + 1);
    let mut recalc_times: Vec<f64> = Vec::with_capacity(cli.recalcs);
    for round in 0..cli.recalcs {
        let recalc_start = Instant::now();
        workbook
            .evaluate_all()
            .map_err(|e| anyhow::anyhow!("no-edit recalc round {round}: {e}"))?;
        let recalc_ms = recalc_start.elapsed().as_secs_f64() * 1000.0;
        let rt = workbook.engine().last_cycle_telemetry().clone();
        assert_values(&workbook, cli)?;
        let checksum = sample_checksum(&workbook, cli)?;
        recalc_times.push(recalc_ms);
        rounds_detail.push(SccIterateRoundReport {
            round,
            kind: "no_edit",
            recalc_ms,
            settle_passes_total: rt.settle_passes_total,
            converged_sccs: rt.converged_sccs,
            capped_sccs: rt.capped_sccs,
            sample_checksum: checksum,
        });
    }

    // One small-edit recalc round: bump a single seed and re-evaluate. The
    // fixed point is independent of seeds for these linear systems, so values
    // still settle to the same checksum (convergent workloads).
    let edit_recalc_ms = {
        small_edit(&mut workbook, cli)?;
        let start = Instant::now();
        workbook
            .evaluate_all()
            .map_err(|e| anyhow::anyhow!("small-edit recalc: {e}"))?;
        let ms = start.elapsed().as_secs_f64() * 1000.0;
        let rt = workbook.engine().last_cycle_telemetry().clone();
        assert_values(&workbook, cli)?;
        let checksum = sample_checksum(&workbook, cli)?;
        rounds_detail.push(SccIterateRoundReport {
            round: cli.recalcs,
            kind: "small_edit",
            recalc_ms: ms,
            settle_passes_total: rt.settle_passes_total,
            converged_sccs: rt.converged_sccs,
            capped_sccs: rt.capped_sccs,
            sample_checksum: checksum,
        });
        ms
    };

    recalc_times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let total_recalc_ms: f64 = recalc_times.iter().sum();
    let recalc_ms_p50 = percentile(&recalc_times, 0.50);
    let recalc_ms_p95 = percentile(&recalc_times, 0.95);
    let recalc_ms_max = recalc_times.last().copied().unwrap_or(0.0);
    // Approximate per-member-pass for recalc using the initial pass count
    // (no-edit recalc re-iterates the same SCC shape → same pass count).
    let recalc_member_passes =
        member_pass_count(members_total, scc_count, initial_settle_passes_total);
    let recalc_us_per_member_pass_p50 = if recalc_member_passes > 0.0 {
        recalc_ms_p50 * 1000.0 / recalc_member_passes
    } else {
        0.0
    };
    let recalc_settle_passes_total_p50 = initial_settle_passes_total;

    let (rss_current_mb, rss_peak_mb) = linux_rss_mb();

    Ok(SccIterateProbeReport {
        label: cli.label.clone(),
        workload: match cli.workload {
            Workload::Pairs => "pairs",
            Workload::BigScc => "big-scc",
        },
        members_total,
        scc_count,
        max_iterations: cli.max_iterations,
        max_change: cli.max_change,
        divergent: cli.divergent,
        range_reads: cli.range_reads,
        recalcs: cli.recalcs,
        workbook_path: workbook_path.display().to_string(),
        reused_workbook,
        fixture_gen_ms,
        load_ms,
        initial_eval_ms,
        initial_settle_passes_total,
        initial_max_passes_single_scc,
        initial_iterated_sccs,
        initial_converged_sccs,
        initial_capped_sccs,
        initial_max_abs_delta_at_stop,
        initial_us_per_member_pass,
        initial_us_per_member,
        total_recalc_ms,
        recalc_ms_p50,
        recalc_ms_p95,
        recalc_ms_max,
        recalc_us_per_member_pass_p50,
        recalc_settle_passes_total_p50,
        edit_recalc_ms,
        sample_checksum: initial_checksum,
        rss_current_mb,
        rss_peak_mb,
        rounds_detail,
    })
}

/// member-pass count = members_total × (passes per SCC). For a single SCC
/// (big-scc) passes_total IS the per-SCC pass count. For N SCCs, passes_total
/// is summed, so passes/scc = passes_total/scc_count.
#[cfg(feature = "formualizer_runner")]
fn member_pass_count(members_total: usize, scc_count: usize, passes_total: usize) -> f64 {
    if scc_count == 0 {
        return 0.0;
    }
    let passes_per_scc = passes_total as f64 / scc_count as f64;
    members_total as f64 * passes_per_scc
}

#[cfg(feature = "formualizer_runner")]
fn assert_values(workbook: &Workbook, cli: &Cli) -> Result<()> {
    match cli.workload {
        Workload::Pairs => {
            // Tolerance follows the convergence bound: |value − fixed point| is
            // O(max_change) for a contraction; use a generous multiple.
            let tol = (cli.max_change * 1000.0).max(0.5);
            for pair in sample_indices(cli.pairs, cli.sample) {
                let row = pair_row(pair);
                let a = num(workbook, row, 1)?;
                let b = num(workbook, row, 2)?;
                if (a - PAIR_FIXED_POINT).abs() > tol || (b - PAIR_FIXED_POINT).abs() > tol {
                    bail!(
                        "pairs: pair {pair} A={a} B={b} not near fixed point {PAIR_FIXED_POINT} (tol {tol})"
                    );
                }
                let consumer = num(workbook, row, 3)?;
                if (consumer - 2.0 * PAIR_FIXED_POINT).abs() > 2.0 * tol {
                    bail!(
                        "pairs: pair {pair} consumer={consumer} not near {}",
                        2.0 * PAIR_FIXED_POINT
                    );
                }
            }
            Ok(())
        }
        Workload::BigScc => {
            if cli.divergent {
                // Divergent: values blow up but must stay finite and large.
                // Sample a few members; just assert they are finite numbers
                // (capping keeps the last pass's values).
                for i in sample_indices(cli.members, cli.sample) {
                    let v = num(workbook, i as u32 + 1, 1)?;
                    if !v.is_finite() {
                        bail!("big-scc divergent: member {i} value {v} is not finite");
                    }
                }
                Ok(())
            } else if cli.range_reads == 0 {
                // Convergent scalar ring: all members → analytic fixed point.
                let fp = ring_fixed_point(ring_factor_for(false, cli.members));
                let tol = (cli.max_change * 1000.0).max(0.5);
                for i in sample_indices(cli.members, cli.sample) {
                    let v = num(workbook, i as u32 + 1, 1)?;
                    if (v - fp).abs() > tol {
                        bail!(
                            "big-scc: member {i} value {v} not near fixed point {fp} (tol {tol})"
                        );
                    }
                }
                Ok(())
            } else {
                // Range-reads convergent variant: fixed point shifts slightly
                // from the perturbation term; just assert finiteness and that
                // members are bounded (converged → not diverging).
                for i in sample_indices(cli.members, cli.sample) {
                    let v = num(workbook, i as u32 + 1, 1)?;
                    if !v.is_finite() || v.abs() > 1e6 {
                        bail!("big-scc range-reads: member {i} value {v} out of expected bounds");
                    }
                }
                Ok(())
            }
        }
    }
}

#[cfg(feature = "formualizer_runner")]
fn sample_checksum(workbook: &Workbook, cli: &Cli) -> Result<f64> {
    let mut acc = 0.0;
    match cli.workload {
        Workload::Pairs => {
            for pair in sample_indices(cli.pairs, cli.sample) {
                acc += num(workbook, pair_row(pair), 3)?;
            }
        }
        Workload::BigScc => {
            for i in sample_indices(cli.members, cli.sample) {
                acc += num(workbook, i as u32 + 1, 1)?;
            }
        }
    }
    Ok(acc)
}

/// Apply one small edit to drive a dirty-based recalc. We re-set ONE member's
/// own formula (identical text): this dirties the cell — driving a real
/// structural-edit recalc — without removing the member from its SCC or
/// changing the fixed point, so convergent checksums stay stable.
#[cfg(feature = "formualizer_runner")]
fn small_edit(workbook: &mut Workbook, cli: &Cli) -> Result<()> {
    match cli.workload {
        Workload::Pairs => {
            // Re-set A of pair 0 to its own formula (A = 0.5*B + 10).
            let row = pair_row(0);
            workbook
                .set_formula(SHEET, row, 1, &format!("=0.5*B{row}+10"))
                .map_err(|e| anyhow::anyhow!("small edit pairs: {e}"))?;
        }
        Workload::BigScc => {
            // Re-set member 0 to its own ring formula (reads member N-1, the
            // wraparound predecessor row = members).
            let factor = ring_factor_for(cli.divergent, cli.members);
            let prev = cli.members as u32; // 1-based predecessor of row 1
            let mut formula = format!("={factor}*A{prev}+1");
            if cli.range_reads > 0 {
                let start = 1usize;
                let end = (start + cli.range_reads.saturating_sub(1)).min(cli.members);
                formula.push_str(&format!("+0.0001*SUM(A{start}:A{end})"));
            }
            workbook
                .set_formula(SHEET, 1, 1, &formula)
                .map_err(|e| anyhow::anyhow!("small edit big-scc: {e}"))?;
        }
    }
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
fn default_workbook_path(cli: &Cli) -> PathBuf {
    let safe_label: String = cli
        .label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let stem = match cli.workload {
        Workload::Pairs => format!("{safe_label}-pairs-{}", cli.pairs),
        Workload::BigScc => format!(
            "{safe_label}-big-scc-{}-{}{}",
            cli.members,
            if cli.divergent { "div" } else { "conv" },
            if cli.range_reads > 0 {
                format!("-r{}", cli.range_reads)
            } else {
                String::new()
            }
        ),
    };
    PathBuf::from("target")
        .join("scc-iterate-fixtures")
        .join(format!("{stem}.xlsx"))
}

#[cfg(feature = "formualizer_runner")]
fn ensure_parent(path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("create fixture dir {}: {e}", parent.display()))?;
    }
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
fn sample_indices(total: usize, sample: usize) -> impl Iterator<Item = usize> {
    let sample = sample.clamp(1, total.max(1));
    let step = (total / sample).max(1);
    (0..total).step_by(step)
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
