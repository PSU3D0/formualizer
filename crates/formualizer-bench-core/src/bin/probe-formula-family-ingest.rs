#[cfg(feature = "formualizer_runner")]
use std::{
    collections::BTreeSet,
    fs,
    io::{Cursor, Write},
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

#[cfg(feature = "formualizer_runner")]
use anyhow::{Context, Result, bail};
#[cfg(feature = "formualizer_runner")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "formualizer_runner")]
use formualizer_eval::engine::{FormulaIngestReport, FormulaPlaneMode};
#[cfg(feature = "formualizer_runner")]
use formualizer_workbook::{
    AdapterLoadStats, CalamineAdapter, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};
#[cfg(feature = "formualizer_runner")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "formualizer_runner")]
use sha2::{Digest, Sha256};
#[cfg(feature = "formualizer_runner")]
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("This binary requires feature `formualizer_runner`");
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.child {
        println!("{}", serde_json::to_string(&probe_child(&cli)?)?);
    } else {
        let report = run_matrix(&cli)?;
        let gate_failed = report
            .nested_authoritative_load_gate
            .as_ref()
            .is_some_and(|gate| !gate.passed)
            || report
                .deferred_fragmented_gates
                .as_ref()
                .is_some_and(|gates| {
                    !gates.load_and_build_speedup.passed || !gates.rss_reduction.passed
                });
        println!("{}", serde_json::to_string_pretty(&report)?);
        if gate_failed {
            bail!("one or more formula-family benchmark gates failed");
        }
    }
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Parser)]
#[command(about = "Cold-process matrix for Calamine formula-family ingest")]
struct Cli {
    #[arg(long, value_enum, default_value_t = Scenario::LargeFamily)]
    scenario: Scenario,
    #[arg(long, value_enum, default_value_t = ProbeMode::All)]
    mode: ProbeMode,
    /// Build formulas during load or stage them for an explicit deferred graph build.
    #[arg(long, value_enum, default_value_t = GraphBuild::Eager)]
    graph_build: GraphBuild,
    #[arg(long, default_value_t = 100)]
    members: u32,
    /// Number of evenly spaced ordinary exceptions in `hole-exception`.
    #[arg(long, default_value_t = 1)]
    exclusions: u32,
    #[arg(long)]
    input: Option<PathBuf>,
    #[arg(long)]
    fixture_out: Option<PathBuf>,
    #[arg(long)]
    generate_only: bool,
    /// Number of independently launched cold children per disposition.
    #[arg(long, default_value_t = 5)]
    samples: usize,
    #[arg(long, hide = true)]
    child: bool,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProbeMode {
    All,
    Off,
    Shadow,
    Authoritative,
    ForcedReplay,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GraphBuild {
    Eager,
    Deferred,
}

#[cfg(feature = "formualizer_runner")]
impl GraphBuild {
    fn label(self) -> &'static str {
        match self {
            Self::Eager => "eager",
            Self::Deferred => "deferred",
        }
    }
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum Scenario {
    LargeFamily,
    NestedFunctionFamily,
    ManyTinyFamilies,
    ForwardAnchor,
    HoleException,
    FullSheetTwoPoint,
}

#[cfg(feature = "formualizer_runner")]
impl Scenario {
    fn label(self) -> &'static str {
        match self {
            Self::LargeFamily => "large_family",
            Self::NestedFunctionFamily => "nested_function_family",
            Self::ManyTinyFamilies => "many_tiny_families",
            Self::ForwardAnchor => "forward_anchor",
            Self::HoleException => "hole_exception",
            Self::FullSheetTwoPoint => "full_sheet_two_point",
        }
    }

    fn arg(self) -> &'static str {
        match self {
            Self::LargeFamily => "large-family",
            Self::NestedFunctionFamily => "nested-function-family",
            Self::ManyTinyFamilies => "many-tiny-families",
            Self::ForwardAnchor => "forward-anchor",
            Self::HoleException => "hole-exception",
            Self::FullSheetTwoPoint => "full-sheet-two-point",
        }
    }
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct MatrixReport {
    scenario: Scenario,
    graph_build: GraphBuild,
    members: u32,
    exclusions: u32,
    fixture: String,
    fixture_sha256: String,
    generated: bool,
    generation_ms: f64,
    fixture_bytes: u64,
    samples_per_disposition: usize,
    children: Vec<ChildMeasurement>,
    summaries: Vec<DispositionSummary>,
    arithmetic_direct_baseline: Option<ArithmeticBaseline>,
    nested_authoritative_load_gate: Option<RelativeGate>,
    deferred_fragmented_gates: Option<DeferredFragmentedGates>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct ArithmeticBaseline {
    fixture: String,
    children: Vec<ChildMeasurement>,
    summaries: Vec<DispositionSummary>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct DispositionSummary {
    disposition: String,
    samples: usize,
    maximum_resident_set_kib: MedianMad,
    total_elapsed_ms: MedianMad,
    load_ms: MedianMad,
    graph_build_ms: MedianMad,
    load_and_build_ms: MedianMad,
    evaluate_ms: MedianMad,
    collection_ms: Option<MedianMad>,
    preparation_ms: Option<MedianMad>,
    replay_ms: Option<MedianMad>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Clone, Copy, Debug, Serialize, PartialEq)]
struct MedianMad {
    median: f64,
    mad: f64,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct RelativeGate {
    metric: &'static str,
    limit_percent: f64,
    baseline_median: f64,
    candidate_median: f64,
    overhead_percent: f64,
    passed: bool,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct DeferredFragmentedGates {
    load_and_build_speedup: ReductionGate,
    rss_reduction: ReductionGate,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct ReductionGate {
    metric: &'static str,
    minimum_reduction_percent: f64,
    baseline_median: f64,
    candidate_median: f64,
    reduction_percent: f64,
    passed: bool,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize)]
struct ChildMeasurement {
    disposition: String,
    sample: usize,
    maximum_resident_set_kib: u64,
    report: ModeReport,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize, Deserialize)]
struct ModeReport {
    disposition: String,
    graph_build: GraphBuild,
    total_elapsed_ms: f64,
    load_ms: f64,
    graph_build_ms: f64,
    load_and_build_ms: f64,
    evaluate_ms: f64,
    collection_ms: Option<f64>,
    preparation_ms: Option<f64>,
    replay_ms: Option<f64>,
    adapter: AdapterCounters,
    ingest: IngestCounters,
    storage_kind: String,
    active_spans: usize,
    graph_formula_vertices: usize,
    allocator_or_accounted_high_water: AccountedHighWater,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize, Deserialize)]
struct AccountedHighWater {
    spool_memory_bytes: u64,
    evidence_bytes: u64,
    allocator_formula_heap_bytes: Option<u64>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize, Deserialize)]
struct AdapterCounters {
    formula_cells_observed: Option<u64>,
    formula_cells_handed_to_engine: Option<u64>,
    shared_formula_tags_observed: Option<u64>,
}
#[cfg(feature = "formualizer_runner")]
impl From<Option<AdapterLoadStats>> for AdapterCounters {
    fn from(stats: Option<AdapterLoadStats>) -> Self {
        let s = stats.unwrap_or_default();
        Self {
            formula_cells_observed: s.formula_cells_observed,
            formula_cells_handed_to_engine: s.formula_cells_handed_to_engine,
            shared_formula_tags_observed: s.shared_formula_tags_observed,
        }
    }
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Serialize, Deserialize)]
struct IngestCounters {
    formula_cells_seen: u64,
    graph_formula_cells_materialized: u64,
    source_formula_events: u64,
    source_formula_records_spooled: u64,
    source_spool_encoded_bytes: u64,
    source_spool_peak_memory_bytes: u64,
    source_spool_spilled_bytes: u64,
    source_spool_replays: u64,
    source_ordinary_events: u64,
    source_shared_anchor_events: u64,
    source_shared_descendant_events: u64,
    source_families_seen: u64,
    source_family_cells_seen: u64,
    source_family_promoted: u64,
    source_family_promoted_cells: u64,
    source_family_fallback: u64,
    source_family_fallback_cells: u64,
    source_forward_descendants: u64,
    source_evidence_limit_fallbacks: u64,
    source_evidence_peak_bytes: u64,
    source_anchor_parses: u64,
    source_anchor_asts: u64,
    source_anchor_analyses: u64,
    source_descendant_records_avoided: Option<u64>,
    source_descendant_strings_avoided: u64,
    source_descendant_events_avoided: u64,
    source_descendant_asts_avoided: Option<u64>,
    source_descendant_analyses_avoided: u64,
    source_staging_entries_avoided: Option<u64>,
    source_placement_runs: Option<u64>,
    source_placement_exceptions: Option<u64>,
    fallback_reasons: std::collections::BTreeMap<String, u64>,
}
#[cfg(feature = "formualizer_runner")]
impl From<FormulaIngestReport> for IngestCounters {
    fn from(r: FormulaIngestReport) -> Self {
        Self {
            formula_cells_seen: r.formula_cells_seen,
            graph_formula_cells_materialized: r.graph_formula_cells_materialized,
            source_formula_events: r.source_formula_events,
            source_formula_records_spooled: r.source_formula_records_spooled,
            source_spool_encoded_bytes: r.source_spool_encoded_bytes,
            source_spool_peak_memory_bytes: r.source_spool_peak_memory_bytes,
            source_spool_spilled_bytes: r.source_spool_spilled_bytes,
            source_spool_replays: r.source_spool_replays,
            source_ordinary_events: r.source_ordinary_events,
            source_shared_anchor_events: r.source_shared_anchor_events,
            source_shared_descendant_events: r.source_shared_descendant_events,
            source_families_seen: r.source_families_seen,
            source_family_cells_seen: r.source_family_cells_seen,
            source_family_promoted: r.source_family_promoted,
            source_family_promoted_cells: r.source_family_promoted_cells,
            source_family_fallback: r.source_family_fallback,
            source_family_fallback_cells: r.source_family_fallback_cells,
            source_forward_descendants: r.source_forward_descendants,
            source_evidence_limit_fallbacks: r.source_evidence_limit_fallbacks,
            source_evidence_peak_bytes: r.source_evidence_peak_bytes,
            source_anchor_parses: r.source_anchor_parses,
            source_anchor_asts: r.source_anchor_asts,
            source_anchor_analyses: r.source_anchor_analyses,
            source_descendant_records_avoided: None,
            source_descendant_strings_avoided: r.source_descendant_strings_avoided,
            source_descendant_events_avoided: r.source_descendant_events_avoided,
            source_descendant_asts_avoided: None,
            source_descendant_analyses_avoided: r.source_descendant_analyses_avoided,
            source_staging_entries_avoided: None,
            source_placement_runs: None,
            source_placement_exceptions: None,
            fallback_reasons: r.fallback_reasons,
        }
    }
}

#[cfg(feature = "formualizer_runner")]
fn fixture_path(cli: &Cli) -> PathBuf {
    cli.input
        .clone()
        .or_else(|| cli.fixture_out.clone())
        .unwrap_or_else(|| {
            let exclusions = if matches!(cli.scenario, Scenario::HoleException) {
                format!("-exclusions-{}", cli.exclusions)
            } else {
                String::new()
            };
            PathBuf::from("scratch/formula-family-anchor-once-bench").join(format!(
                "{}-{}{}.xlsx",
                cli.scenario.label(),
                cli.members,
                exclusions,
            ))
        })
}

#[cfg(feature = "formualizer_runner")]
const NESTED_LOAD_GATE_MIN_MEMBERS: u32 = 100_000;

#[cfg(feature = "formualizer_runner")]
fn is_nested_load_gate_run(cli: &Cli) -> bool {
    matches!(cli.scenario, Scenario::NestedFunctionFamily)
        && cli.members >= NESTED_LOAD_GATE_MIN_MEMBERS
        && matches!(cli.mode, ProbeMode::All | ProbeMode::Authoritative)
        && !cli.generate_only
}

#[cfg(feature = "formualizer_runner")]
fn is_deferred_fragmented_gate_run(cli: &Cli) -> bool {
    matches!(cli.scenario, Scenario::HoleException)
        && cli.graph_build == GraphBuild::Deferred
        && cli.members >= 100_000
        && matches!(cli.exclusions, 1 | 8 | 64)
        && cli.mode == ProbeMode::All
        && !cli.generate_only
}

#[cfg(feature = "formualizer_runner")]
fn run_matrix(cli: &Cli) -> Result<MatrixReport> {
    if cli.members == 0 {
        bail!("--members must be at least 1")
    }
    if cli.samples == 0 {
        bail!("--samples must be at least 1")
    }
    if matches!(cli.scenario, Scenario::HoleException)
        && (cli.exclusions == 0 || cli.exclusions >= cli.members.saturating_sub(1))
    {
        bail!("hole-exception requires 1 <= --exclusions < --members - 1")
    }
    if is_nested_load_gate_run(cli) && cli.samples < 5 {
        bail!("nested-function-family 100k gate runs require at least five cold samples")
    }
    if is_deferred_fragmented_gate_run(cli) && cli.samples < 5 {
        bail!("deferred fragmented 100k gate runs require at least five cold samples")
    }
    let fixture = fixture_path(cli);
    let generated = cli.input.is_none();
    let started = Instant::now();
    if generated {
        let bytes = generate_xlsx(cli.scenario, cli.members, cli.exclusions)?;
        if let Some(p) = fixture.parent() {
            fs::create_dir_all(p)?
        }
        fs::write(&fixture, bytes)?;
    }
    let generation_ms = started.elapsed().as_secs_f64() * 1000.0;
    let fixture_bytes = fs::metadata(&fixture)?.len();
    let fixture_sha256 = format!("{:x}", Sha256::digest(fs::read(&fixture)?));
    let mut children = Vec::new();
    if !cli.generate_only {
        for mode in [
            ProbeMode::Off,
            ProbeMode::Shadow,
            ProbeMode::Authoritative,
            ProbeMode::ForcedReplay,
        ] {
            if cli.mode != ProbeMode::All && cli.mode != mode {
                continue;
            }
            for sample in 1..=cli.samples {
                children.push(run_child(cli, &fixture, mode, sample)?);
            }
        }
    }
    let summaries = summarize_dispositions(&children);
    let mut arithmetic_direct_baseline = None;
    let mut nested_authoritative_load_gate = None;
    if is_nested_load_gate_run(cli) {
        let baseline_fixture = fixture.with_file_name(format!(
            "large_family-{}-nested-load-baseline.xlsx",
            cli.members
        ));
        if let Some(parent) = baseline_fixture.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            &baseline_fixture,
            generate_xlsx(Scenario::LargeFamily, cli.members, 0)?,
        )?;
        let mut baseline_children = Vec::with_capacity(cli.samples);
        for sample in 1..=cli.samples {
            baseline_children.push(run_child(
                cli,
                &baseline_fixture,
                ProbeMode::Authoritative,
                sample,
            )?);
        }
        let baseline_summaries = summarize_dispositions(&baseline_children);
        let candidate = summary_for(&summaries, "authoritative")
            .context("nested run did not produce an authoritative summary")?;
        let baseline = summary_for(&baseline_summaries, "authoritative")
            .context("arithmetic baseline did not produce an authoritative summary")?;
        nested_authoritative_load_gate = Some(relative_gate(
            "authoritative_load_ms",
            baseline.load_ms.median,
            candidate.load_ms.median,
            10.0,
        ));
        arithmetic_direct_baseline = Some(ArithmeticBaseline {
            fixture: baseline_fixture.display().to_string(),
            children: baseline_children,
            summaries: baseline_summaries,
        });
    }
    let deferred_fragmented_gates = if is_deferred_fragmented_gate_run(cli) {
        let candidate = summary_for(&summaries, "authoritative")
            .context("deferred fragmented run did not produce an authoritative summary")?;
        let baseline = summary_for(&summaries, "forced-replay")
            .context("deferred fragmented run did not produce a forced-replay summary")?;
        Some(DeferredFragmentedGates {
            load_and_build_speedup: reduction_gate(
                "authoritative_load_and_build_ms",
                baseline.load_and_build_ms.median,
                candidate.load_and_build_ms.median,
                25.0,
            ),
            rss_reduction: reduction_gate(
                "authoritative_maximum_resident_set_kib",
                baseline.maximum_resident_set_kib.median,
                candidate.maximum_resident_set_kib.median,
                40.0,
            ),
        })
    } else {
        None
    };
    Ok(MatrixReport {
        scenario: cli.scenario,
        graph_build: cli.graph_build,
        members: cli.members,
        exclusions: if matches!(cli.scenario, Scenario::HoleException) {
            cli.exclusions
        } else {
            0
        },
        fixture: fixture.display().to_string(),
        fixture_sha256,
        generated,
        generation_ms,
        fixture_bytes,
        samples_per_disposition: cli.samples,
        children,
        summaries,
        arithmetic_direct_baseline,
        nested_authoritative_load_gate,
        deferred_fragmented_gates,
    })
}

#[cfg(feature = "formualizer_runner")]
fn median_mad(mut values: Vec<f64>) -> Option<MedianMad> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let median = median_sorted(&values);
    let mut deviations: Vec<_> = values
        .into_iter()
        .map(|value| (value - median).abs())
        .collect();
    deviations.sort_by(f64::total_cmp);
    Some(MedianMad {
        median,
        mad: median_sorted(&deviations),
    })
}

#[cfg(feature = "formualizer_runner")]
fn median_sorted(values: &[f64]) -> f64 {
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

#[cfg(feature = "formualizer_runner")]
fn summarize_dispositions(children: &[ChildMeasurement]) -> Vec<DispositionSummary> {
    let mut dispositions = Vec::new();
    for child in children {
        if !dispositions.contains(&child.disposition) {
            dispositions.push(child.disposition.clone());
        }
    }
    dispositions
        .into_iter()
        .map(|disposition| {
            let samples: Vec<_> = children
                .iter()
                .filter(|child| child.disposition == disposition)
                .collect();
            let optional_metric = |read: fn(&ModeReport) -> Option<f64>| {
                samples
                    .iter()
                    .map(|sample| read(&sample.report))
                    .collect::<Option<Vec<_>>>()
                    .and_then(median_mad)
            };
            DispositionSummary {
                disposition,
                samples: samples.len(),
                maximum_resident_set_kib: median_mad(
                    samples
                        .iter()
                        .map(|sample| sample.maximum_resident_set_kib as f64)
                        .collect(),
                )
                .expect("non-empty finite RSS samples"),
                total_elapsed_ms: median_mad(
                    samples
                        .iter()
                        .map(|sample| sample.report.total_elapsed_ms)
                        .collect(),
                )
                .expect("non-empty finite total samples"),
                load_ms: median_mad(samples.iter().map(|sample| sample.report.load_ms).collect())
                    .expect("non-empty finite load samples"),
                graph_build_ms: median_mad(
                    samples
                        .iter()
                        .map(|sample| sample.report.graph_build_ms)
                        .collect(),
                )
                .expect("non-empty finite graph-build samples"),
                load_and_build_ms: median_mad(
                    samples
                        .iter()
                        .map(|sample| sample.report.load_and_build_ms)
                        .collect(),
                )
                .expect("non-empty finite load-and-build samples"),
                evaluate_ms: median_mad(
                    samples
                        .iter()
                        .map(|sample| sample.report.evaluate_ms)
                        .collect(),
                )
                .expect("non-empty finite evaluation samples"),
                collection_ms: optional_metric(|report| report.collection_ms),
                preparation_ms: optional_metric(|report| report.preparation_ms),
                replay_ms: optional_metric(|report| report.replay_ms),
            }
        })
        .collect()
}

#[cfg(feature = "formualizer_runner")]
fn summary_for<'a>(
    summaries: &'a [DispositionSummary],
    disposition: &str,
) -> Option<&'a DispositionSummary> {
    summaries
        .iter()
        .find(|summary| summary.disposition == disposition)
}

#[cfg(feature = "formualizer_runner")]
fn relative_gate(
    metric: &'static str,
    baseline_median: f64,
    candidate_median: f64,
    limit_percent: f64,
) -> RelativeGate {
    let overhead_percent = if baseline_median > 0.0 {
        (candidate_median / baseline_median - 1.0) * 100.0
    } else {
        f64::INFINITY
    };
    RelativeGate {
        metric,
        limit_percent,
        baseline_median,
        candidate_median,
        overhead_percent,
        passed: overhead_percent <= limit_percent,
    }
}

#[cfg(feature = "formualizer_runner")]
fn reduction_gate(
    metric: &'static str,
    baseline_median: f64,
    candidate_median: f64,
    minimum_reduction_percent: f64,
) -> ReductionGate {
    let reduction_percent = if baseline_median > 0.0 {
        (1.0 - candidate_median / baseline_median) * 100.0
    } else {
        f64::NEG_INFINITY
    };
    ReductionGate {
        metric,
        minimum_reduction_percent,
        baseline_median,
        candidate_median,
        reduction_percent,
        passed: reduction_percent >= minimum_reduction_percent,
    }
}

#[cfg(feature = "formualizer_runner")]
fn run_child(
    cli: &Cli,
    fixture: &Path,
    mode: ProbeMode,
    sample: usize,
) -> Result<ChildMeasurement> {
    let exe = std::env::current_exe()?;
    let mode_arg = match mode {
        ProbeMode::Off => "off",
        ProbeMode::Shadow => "shadow",
        ProbeMode::Authoritative => "authoritative",
        ProbeMode::ForcedReplay => "forced-replay",
        ProbeMode::All => unreachable!(),
    };
    let output = Command::new("/usr/bin/time")
        .args(["-v"])
        .arg(exe)
        .args(["--child", "--input"])
        .arg(fixture)
        .args([
            "--scenario",
            cli.scenario.arg(),
            "--graph-build",
            cli.graph_build.label(),
            "--members",
            &cli.members.to_string(),
            "--exclusions",
            &cli.exclusions.to_string(),
            "--mode",
            mode_arg,
        ])
        .output()
        .context("launch timed cold child through /usr/bin/time -v")?;
    if !output.status.success() {
        bail!(
            "child {mode_arg} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let rss = parse_max_rss(&stderr).context("/usr/bin/time -v did not report maximum RSS")?;
    let report: ModeReport = serde_json::from_slice(&output.stdout).context("parse child JSON")?;
    Ok(ChildMeasurement {
        disposition: mode_arg.into(),
        sample,
        maximum_resident_set_kib: rss,
        report,
    })
}

#[cfg(feature = "formualizer_runner")]
fn parse_max_rss(s: &str) -> Option<u64> {
    s.lines().find_map(|l| {
        l.split_once("Maximum resident set size (kbytes):")
            .and_then(|(_, v)| v.trim().parse().ok())
    })
}

#[cfg(feature = "formualizer_runner")]
fn probe_child(cli: &Cli) -> Result<ModeReport> {
    let path = fixture_path(cli);
    let (mode, label, forced) = match cli.mode {
        ProbeMode::Off => (FormulaPlaneMode::Off, "off", false),
        ProbeMode::Shadow => (FormulaPlaneMode::Shadow, "shadow", false),
        ProbeMode::Authoritative => (
            FormulaPlaneMode::AuthoritativeExperimental,
            "authoritative",
            false,
        ),
        ProbeMode::ForcedReplay => (
            FormulaPlaneMode::AuthoritativeExperimental,
            "forced_replay",
            true,
        ),
        ProbeMode::All => bail!("child requires one disposition"),
    };
    if forced {
        unsafe { std::env::set_var("FORMUALIZER_BENCH_FORCE_FORMULA_FAMILY_REPLAY", "1") };
    }
    let total = Instant::now();
    let adapter = CalamineAdapter::open_path(&path)?;
    let mut config = WorkbookConfig::ephemeral().with_formula_plane_mode(mode);
    config.eval.defer_graph_building = cli.graph_build == GraphBuild::Deferred;
    let load = Instant::now();
    let (mut workbook, adapter_stats) =
        Workbook::from_reader_with_adapter_stats(adapter, LoadStrategy::EagerAll, config)?;
    let load_ms = load.elapsed().as_secs_f64() * 1000.0;
    let graph_build_ms = if cli.graph_build == GraphBuild::Deferred {
        let graph_build = Instant::now();
        workbook.prepare_graph_all()?;
        graph_build.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };
    let load_and_build_ms = load_ms + graph_build_ms;
    let eval = Instant::now();
    workbook.evaluate_all()?;
    let evaluate_ms = eval.elapsed().as_secs_f64() * 1000.0;
    let stats = workbook.engine().baseline_stats();
    let ingest: IngestCounters = workbook
        .last_formula_ingest_report()
        .context("missing ingest report")?
        .into();
    if forced
        && (stats.formula_plane_active_span_count != 0
            || stats.graph_formula_vertex_count as u64 != ingest.formula_cells_seen)
    {
        bail!(
            "forced replay must materialize every formula as a graph vertex (spans={}, vertices={}, formulas={})",
            stats.formula_plane_active_span_count,
            stats.graph_formula_vertex_count,
            ingest.formula_cells_seen
        );
    }
    if !forced
        && mode == FormulaPlaneMode::AuthoritativeExperimental
        && ingest.source_family_promoted > 0
        && (stats.formula_plane_active_span_count == 0
            || ingest.graph_formula_cells_materialized + ingest.source_family_promoted_cells
                != ingest.formula_cells_seen)
    {
        bail!(
            "authoritative promotion must reconcile every formula (spans={}, graph={}, promoted={}, formulas={})",
            stats.formula_plane_active_span_count,
            ingest.graph_formula_cells_materialized,
            ingest.source_family_promoted_cells,
            ingest.formula_cells_seen
        );
    }
    if !forced
        && mode == FormulaPlaneMode::AuthoritativeExperimental
        && cli.graph_build == GraphBuild::Deferred
        && matches!(cli.scenario, Scenario::HoleException)
        && cli.members >= 100_000
        && cli.exclusions <= 64
    {
        let expected_exclusions = u64::from(cli.exclusions);
        let expected_spans = cli.exclusions as usize + 1;
        if ingest.source_family_promoted != 1
            || ingest.source_family_fallback != 0
            || ingest.graph_formula_cells_materialized != expected_exclusions
            || stats.graph_formula_vertex_count as u64 != expected_exclusions
            || stats.formula_plane_active_span_count != expected_spans
        {
            bail!(
                "deferred fragmented benchmark missed authority path (families={}, fallback={}, graph={}, vertices={}, spans={}, expected exclusions={}, expected spans={})",
                ingest.source_family_promoted,
                ingest.source_family_fallback,
                ingest.graph_formula_cells_materialized,
                stats.graph_formula_vertex_count,
                stats.formula_plane_active_span_count,
                expected_exclusions,
                expected_spans,
            );
        }
    }
    let storage_kind = if ingest.source_spool_spilled_bytes > 0 {
        "native_file"
    } else if ingest.source_formula_records_spooled > 0 {
        "memory"
    } else {
        "none"
    }
    .to_string();
    let high = AccountedHighWater {
        spool_memory_bytes: ingest.source_spool_peak_memory_bytes,
        evidence_bytes: ingest.source_evidence_peak_bytes,
        allocator_formula_heap_bytes: None,
    };
    Ok(ModeReport {
        disposition: label.into(),
        graph_build: cli.graph_build,
        total_elapsed_ms: total.elapsed().as_secs_f64() * 1000.0,
        load_ms,
        graph_build_ms,
        load_and_build_ms,
        evaluate_ms,
        collection_ms: None,
        preparation_ms: None,
        replay_ms: None,
        adapter: adapter_stats.into(),
        storage_kind,
        active_spans: stats.formula_plane_active_span_count,
        graph_formula_vertices: stats.graph_formula_vertex_count,
        allocator_or_accounted_high_water: high,
        ingest,
    })
}

#[cfg(feature = "formualizer_runner")]
fn generate_xlsx(scenario: Scenario, members: u32, exclusions: u32) -> Result<Vec<u8>> {
    let sheet = sheet_xml(scenario, members, exclusions)?;
    let cursor = Cursor::new(Vec::new());
    let mut zip = ZipWriter::new(cursor);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    for (name, body) in [
        ("[Content_Types].xml", CONTENT_TYPES),
        ("_rels/.rels", ROOT_RELS),
        ("xl/workbook.xml", WORKBOOK),
        ("xl/_rels/workbook.xml.rels", WORKBOOK_RELS),
        ("xl/worksheets/sheet1.xml", sheet.as_str()),
    ] {
        zip.start_file(name, options)?;
        zip.write_all(body.as_bytes())?;
    }
    Ok(zip.finish()?.into_inner())
}

#[cfg(feature = "formualizer_runner")]
fn sheet_xml(scenario: Scenario, members: u32, exclusions: u32) -> Result<String> {
    let rows = match scenario {
        Scenario::FullSheetTwoPoint => 2,
        _ => members,
    };
    let exception_rows: BTreeSet<u32> = if matches!(scenario, Scenario::HoleException) {
        let denominator = u64::from(exclusions) + 1;
        (1..=exclusions)
            .map(|index| 1 + ((u64::from(index) * u64::from(rows - 1)) / denominator) as u32)
            .collect()
    } else {
        BTreeSet::new()
    };
    if rows > 1_048_576 {
        bail!("--members exceeds the XLSX row limit");
    }
    let mut xml = String::with_capacity(rows as usize * 100 + 256);
    xml.push_str(r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">"#);
    let dimension_end = if matches!(scenario, Scenario::FullSheetTwoPoint) {
        1_048_576
    } else {
        rows
    };
    xml.push_str(&format!(
        r#"<dimension ref="A1:B{dimension_end}"/><sheetData>"#
    ));

    let tiny_width = 4_u32;
    for row in 1..=rows {
        xml.push_str(&format!(
            r#"<row r="{row}"><c r="A{row}"><v>{row}</v></c><c r="B{row}">"#
        ));
        let formula = format!("A{row}+1");
        match scenario {
            Scenario::ManyTinyFamilies => {
                let family_start = ((row - 1) / tiny_width) * tiny_width + 1;
                let family_end = (family_start + tiny_width - 1).min(rows);
                let si = (row - 1) / tiny_width;
                if row == family_start {
                    xml.push_str(&format!(
                        r#"<f t="shared" si="{si}" ref="B{family_start}:B{family_end}">{formula}</f>"#
                    ));
                } else {
                    xml.push_str(&format!(r#"<f t="shared" si="{si}"/>"#));
                }
            }
            Scenario::ForwardAnchor => {
                if rows == 1 || row == 2 {
                    xml.push_str(&format!(
                        r#"<f t="shared" si="1" ref="B1:B{rows}">{formula}</f>"#
                    ));
                } else {
                    xml.push_str(r#"<f t="shared" si="1"/>"#);
                }
            }
            Scenario::HoleException if exception_rows.contains(&row) => {
                xml.push_str(&format!("<f>{formula}</f>"));
            }
            Scenario::NestedFunctionFamily => {
                if row == 1 {
                    xml.push_str(&format!(
                        r#"<f t="shared" si="1" ref="B1:B{rows}">ROUND(ABS(A1)+SUM(A1:A1)+COUNTIF(A1:A1,"&gt;0")+VLOOKUP(A1,A1:A1,1,FALSE),0)</f>"#
                    ));
                } else {
                    xml.push_str(r#"<f t="shared" si="1"/>"#);
                }
            }
            Scenario::FullSheetTwoPoint | Scenario::LargeFamily | Scenario::HoleException => {
                if row == 1 {
                    let end = if matches!(scenario, Scenario::FullSheetTwoPoint) {
                        1_048_576
                    } else {
                        rows
                    };
                    xml.push_str(&format!(
                        r#"<f t="shared" si="1" ref="B1:B{end}">{formula}</f>"#
                    ));
                } else {
                    xml.push_str(r#"<f t="shared" si="1"/>"#);
                }
            }
        }
        xml.push_str("<v/></c></row>");
    }
    xml.push_str("</sheetData></worksheet>");
    Ok(xml)
}

#[cfg(feature = "formualizer_runner")]
const CONTENT_TYPES: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#;
#[cfg(feature = "formualizer_runner")]
const ROOT_RELS: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#;
#[cfg(feature = "formualizer_runner")]
const WORKBOOK: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>"#;
#[cfg(feature = "formualizer_runner")]
const WORKBOOK_RELS: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#;

#[cfg(all(test, feature = "formualizer_runner"))]
mod tests {
    use super::*;
    use std::io::Read;
    use zip::ZipArchive;

    #[test]
    fn fixture_generation_is_deterministic_and_contains_shared_ooxml() {
        let first = generate_xlsx(Scenario::LargeFamily, 100, 0).unwrap();
        let second = generate_xlsx(Scenario::LargeFamily, 100, 0).unwrap();
        assert_eq!(first, second);
        let mut archive = ZipArchive::new(Cursor::new(first)).unwrap();
        let mut xml = String::new();
        archive
            .by_name("xl/worksheets/sheet1.xml")
            .unwrap()
            .read_to_string(&mut xml)
            .unwrap();
        assert!(xml.contains("t=\"shared\""));
        assert!(xml.contains("ref=\"B1:B100\""));
    }

    #[test]
    fn nested_function_fixture_is_one_genuine_shared_family() {
        let xml = sheet_xml(Scenario::NestedFunctionFamily, 100, 0).unwrap();
        assert!(xml.contains("ROUND(ABS(A1)+SUM(A1:A1)+COUNTIF"));
        assert!(xml.contains("VLOOKUP(A1,A1:A1,1,FALSE)"));
        assert_eq!(xml.matches("t=\"shared\"").count(), 100);
        assert_eq!(xml.matches("ref=\"B1:B100\"").count(), 1);
    }

    #[test]
    fn pathological_fixture_declares_full_sheet_without_materializing_it() {
        let xml = sheet_xml(Scenario::FullSheetTwoPoint, 2, 0).unwrap();
        assert!(xml.contains("ref=\"B1:B1048576\""));
        assert_eq!(xml.matches("<row ").count(), 2);
    }

    #[test]
    fn fragmented_fixture_places_requested_ordinary_exceptions() {
        let xml = sheet_xml(Scenario::HoleException, 100_000, 64).unwrap();
        assert_eq!(xml.matches("<f>A").count(), 64);
        assert_eq!(xml.matches("t=\"shared\"").count(), 100_000 - 64);
        assert_eq!(xml.matches("ref=\"B1:B100000\"").count(), 1);

        let dense = sheet_xml(Scenario::HoleException, 100, 64).unwrap();
        assert_eq!(dense.matches("<f>A").count(), 64);
        assert_eq!(dense.matches("t=\"shared\"").count(), 100 - 64);
        assert_eq!(dense.matches("ref=\"B1:B100\"").count(), 1);
        assert!(dense.contains("<f t=\"shared\" si=\"1\" ref=\"B1:B100\">A1+1</f>"));
    }

    #[test]
    fn nested_load_gate_applies_only_to_required_100k_measurements() {
        let cli = |members| Cli {
            scenario: Scenario::NestedFunctionFamily,
            mode: ProbeMode::Authoritative,
            graph_build: GraphBuild::Eager,
            members,
            exclusions: 1,
            input: None,
            fixture_out: None,
            generate_only: false,
            samples: 5,
            child: false,
        };
        assert!(!is_nested_load_gate_run(&cli(100)));
        assert!(is_nested_load_gate_run(&cli(100_000)));
    }

    #[test]
    fn deferred_fragmented_gate_requires_the_cold_100k_matrix() {
        let cli = |exclusions, graph_build, mode| Cli {
            scenario: Scenario::HoleException,
            mode,
            graph_build,
            members: 100_000,
            exclusions,
            input: None,
            fixture_out: None,
            generate_only: false,
            samples: 5,
            child: false,
        };
        assert!(is_deferred_fragmented_gate_run(&cli(
            1,
            GraphBuild::Deferred,
            ProbeMode::All,
        )));
        assert!(is_deferred_fragmented_gate_run(&cli(
            8,
            GraphBuild::Deferred,
            ProbeMode::All,
        )));
        assert!(is_deferred_fragmented_gate_run(&cli(
            64,
            GraphBuild::Deferred,
            ProbeMode::All,
        )));
        assert!(!is_deferred_fragmented_gate_run(&cli(
            2,
            GraphBuild::Deferred,
            ProbeMode::All,
        )));
        assert!(!is_deferred_fragmented_gate_run(&cli(
            1,
            GraphBuild::Eager,
            ProbeMode::All,
        )));
        assert!(!is_deferred_fragmented_gate_run(&cli(
            1,
            GraphBuild::Deferred,
            ProbeMode::Authoritative,
        )));
    }

    #[test]
    fn median_and_mad_are_stable_for_odd_and_even_samples() {
        assert_eq!(
            median_mad(vec![101.0, 2.0, 3.0, 100.0, 1.0]),
            Some(MedianMad {
                median: 3.0,
                mad: 2.0,
            })
        );
        assert_eq!(
            median_mad(vec![4.0, 1.0, 3.0, 2.0]),
            Some(MedianMad {
                median: 2.5,
                mad: 1.0,
            })
        );
        assert_eq!(median_mad(Vec::new()), None);
        assert_eq!(median_mad(vec![f64::NAN]), None);
    }

    #[test]
    fn relative_load_gate_reports_pass_and_failure_without_using_total_time() {
        let pass = relative_gate("authoritative_load_ms", 100.0, 109.9, 10.0);
        assert!(pass.passed);
        assert_eq!(pass.metric, "authoritative_load_ms");
        let fail = relative_gate("authoritative_load_ms", 100.0, 110.1, 10.0);
        assert!(!fail.passed);
        assert!(fail.overhead_percent > fail.limit_percent);
        assert!(!relative_gate("authoritative_load_ms", 0.0, 1.0, 10.0).passed);

        let pass = reduction_gate("rss", 100.0, 59.9, 40.0);
        assert!(pass.passed);
        assert!(pass.reduction_percent > pass.minimum_reduction_percent);
        let fail = reduction_gate("rss", 100.0, 60.1, 40.0);
        assert!(!fail.passed);
        assert!(!reduction_gate("rss", 0.0, 0.0, 40.0).passed);
    }

    #[test]
    fn parses_gnu_time_maximum_rss_without_combining_children() {
        let output = "\tMaximum resident set size (kbytes): 123456\n";
        assert_eq!(parse_max_rss(output), Some(123_456));
        assert_eq!(parse_max_rss("elapsed 1.0"), None);
    }
}
