#[cfg(feature = "c6_calibration")]
use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "c6_calibration")]
use std::fs;
#[cfg(feature = "c6_calibration")]
use std::io::Read;
#[cfg(feature = "c6_calibration")]
use std::path::{Path, PathBuf};
#[cfg(feature = "c6_calibration")]
use std::process::{Command, Stdio};
#[cfg(feature = "c6_calibration")]
use std::thread;
#[cfg(feature = "c6_calibration")]
use std::time::{Duration, Instant};

#[cfg(feature = "c6_calibration")]
use anyhow::{Context, Result, bail};
#[cfg(feature = "c6_calibration")]
use clap::Parser;
#[cfg(feature = "c6_calibration")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "c6_calibration")]
use formualizer_bench_core::c6_calibration::{
    CalibrationPath, ChildReport, Distribution, TargetScope, distribution, sha256_file,
};

#[cfg(not(feature = "c6_calibration"))]
fn main() {
    eprintln!("This binary requires feature `c6_calibration`");
    std::process::exit(2);
}

#[cfg(feature = "c6_calibration")]
#[derive(Debug, Clone, Parser)]
#[command(about = "Run randomized fresh-process C6 target-locality samples")]
struct Cli {
    /// Total formula count across the deterministic independent branches.
    #[arg(long, default_value_t = 50_000)]
    formulas: u32,

    /// Fresh child-process samples for every path/scope case.
    #[arg(long, default_value_t = 7)]
    samples: usize,

    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_values_t = [CalibrationPath::Full, CalibrationPath::Cells, CalibrationPath::Plan, CalibrationPath::Sheetport]
    )]
    paths: Vec<CalibrationPath>,

    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_values_t = [TargetScope::Tiny, TargetScope::Medium, TargetScope::Full]
    )]
    scopes: Vec<TargetScope>,

    #[arg(long, default_value_t = 3)]
    warm_repeats: usize,

    /// Hard timeout for each generation/sample child process.
    #[arg(long, default_value_t = 600)]
    timeout_seconds: u64,

    /// Deterministic randomization seed recorded in raw output.
    #[arg(long, default_value_t = 0xc6ca_1b7a_u64)]
    seed: u64,

    #[arg(long, default_value = "target/c6-calibration")]
    output_dir: PathBuf,

    /// Reuse an already built release child probe.
    #[arg(long)]
    skip_build: bool,
}

#[cfg(feature = "c6_calibration")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunIdentity {
    commit: String,
    dirty: bool,
    rustc: String,
    host: String,
    os: String,
    architecture: String,
}

#[cfg(feature = "c6_calibration")]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Job {
    path: CalibrationPath,
    scope: TargetScope,
    sample: usize,
}

#[cfg(feature = "c6_calibration")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SampleResult {
    job: Job,
    status: String,
    child: Option<ChildReport>,
    external_max_rss_bytes: Option<u64>,
    wall_milliseconds: f64,
    error: Option<String>,
    stderr_log: String,
}

#[cfg(feature = "c6_calibration")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CaseSummary {
    path: CalibrationPath,
    scope: TargetScope,
    successful_samples: usize,
    cold_total_ms: Option<Distribution>,
    load_ms: Option<Distribution>,
    bind_resolution_ms: Option<Distribution>,
    preparation_plan_ms: Option<Distribution>,
    first_evaluation_ms: Option<Distribution>,
    output_read_ms: Option<Distribution>,
    edit_ms: Option<Distribution>,
    warm_evaluation_ms: Option<Distribution>,
    batch_restore_ms: Option<Distribution>,
    sheetport_batch_plan_telemetry_ms: Option<Distribution>,
    sheetport_batch_execution_telemetry_ms: Option<Distribution>,
    peak_rss_bytes: Option<Distribution>,
    max_prepared_formula_delta: Option<u64>,
    max_staged_selected: Option<u64>,
    max_target_actual_work: Option<u64>,
    locality_oracle_all_passed: bool,
    analytical_output_oracle_all_passed: bool,
    exact_fixture_counts_all_passed: bool,
    exact_locality_counts_all_passed: bool,
    unrelated_staged_all_retained: bool,
    unrelated_dirty_all_retained: bool,
}

#[cfg(feature = "c6_calibration")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MatrixReport {
    schema_version: u32,
    identity: RunIdentity,
    formulas: u32,
    samples_per_case: usize,
    warm_repeats: usize,
    timeout_seconds: u64,
    random_seed: u64,
    fixture_path: String,
    fixture_sha256: String,
    randomized_jobs: Vec<Job>,
    results: Vec<SampleResult>,
    summaries: Vec<CaseSummary>,
    parity_by_scope: BTreeMap<String, bool>,
}

#[cfg(feature = "c6_calibration")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli)
}

#[cfg(feature = "c6_calibration")]
fn run(mut cli: Cli) -> Result<()> {
    if cli.formulas >= 50_000 {
        cli.timeout_seconds = cli.timeout_seconds.max(600);
    }
    if cli.samples == 0 {
        bail!("--samples must be greater than zero");
    }
    if cli.paths.is_empty() || cli.scopes.is_empty() {
        bail!("--paths and --scopes must not be empty");
    }
    let root = repo_root();
    fs::create_dir_all(&cli.output_dir)?;
    if !cli.skip_build {
        build_child(&root)?;
    }
    let probe = child_path(&root);
    if !probe.is_file() {
        bail!(
            "child probe not found at {}; omit --skip-build",
            probe.display()
        );
    }

    let fixture = cli
        .output_dir
        .join(format!("fixture-{}.xlsx", cli.formulas));
    let generation_log = cli.output_dir.join("fixture-generation.stderr.log");
    let generation = run_process(
        command_for_generate(&probe, &fixture, cli.formulas),
        Duration::from_secs(cli.timeout_seconds),
        &generation_log,
    )?;
    if !generation.success {
        bail!("fixture generation failed: {}", generation.error);
    }
    let fixture_sha256 = sha256_file(&fixture)?;

    let mut jobs = Vec::new();
    for sample in 1..=cli.samples {
        for &scope in &cli.scopes {
            for &path in &cli.paths {
                jobs.push(Job {
                    path,
                    scope,
                    sample,
                });
            }
        }
    }
    shuffle(&mut jobs, cli.seed);

    let mut results = Vec::with_capacity(jobs.len());
    for (order, job) in jobs.iter().copied().enumerate() {
        eprintln!(
            "[c6] {}/{} path={} scope={} sample={}",
            order + 1,
            jobs.len(),
            job.path.label(),
            job.scope.label(),
            job.sample
        );
        let stderr_log = cli.output_dir.join(format!(
            "{:03}-{}-{}-sample{}.stderr.log",
            order + 1,
            job.path.label(),
            job.scope.label(),
            job.sample
        ));
        let time_log = cli.output_dir.join(format!(
            "{:03}-{}-{}-sample{}.time.log",
            order + 1,
            job.path.label(),
            job.scope.label(),
            job.sample
        ));
        results.push(run_job(&probe, &fixture, job, &cli, &stderr_log, &time_log));
    }

    let summaries = summarize(&results, &cli);
    let parity_by_scope = parity(&results, &cli.scopes);
    let report = MatrixReport {
        schema_version: 1,
        identity: identity(&root),
        formulas: cli.formulas,
        samples_per_case: cli.samples,
        warm_repeats: cli.warm_repeats,
        timeout_seconds: cli.timeout_seconds,
        random_seed: cli.seed,
        fixture_path: fixture.display().to_string(),
        fixture_sha256,
        randomized_jobs: jobs,
        results,
        summaries,
        parity_by_scope,
    };
    let raw_path = cli.output_dir.join("matrix-raw.json");
    let markdown_path = cli.output_dir.join("matrix-summary.md");
    fs::write(&raw_path, serde_json::to_string_pretty(&report)? + "\n")?;
    fs::write(&markdown_path, render_markdown(&report))?;
    println!("{}", render_markdown(&report));
    eprintln!(
        "[c6] raw={} summary={}",
        raw_path.display(),
        markdown_path.display()
    );

    if report.results.iter().any(|result| result.status != "ok") {
        bail!("one or more child samples failed; inspect raw JSON and stderr logs");
    }
    if report.parity_by_scope.values().any(|parity| !parity) {
        bail!("output/error parity failed");
    }
    Ok(())
}

#[cfg(feature = "c6_calibration")]
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repository root")
        .to_path_buf()
}

#[cfg(feature = "c6_calibration")]
fn target_dir(root: &Path) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"))
}

#[cfg(feature = "c6_calibration")]
fn child_path(root: &Path) -> PathBuf {
    target_dir(root).join("release").join(if cfg!(windows) {
        "probe-c6-target-locality.exe"
    } else {
        "probe-c6-target-locality"
    })
}

#[cfg(feature = "c6_calibration")]
fn build_child(root: &Path) -> Result<()> {
    let status = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "formualizer-bench-core",
            "--features",
            "c6_calibration",
            "--bin",
            "probe-c6-target-locality",
        ])
        .current_dir(root)
        .status()
        .context("build release child probe")?;
    if !status.success() {
        bail!("release child probe build failed with {status}");
    }
    Ok(())
}

#[cfg(feature = "c6_calibration")]
fn command_for_generate(probe: &Path, fixture: &Path, formulas: u32) -> Command {
    let mut command = Command::new(probe);
    command
        .arg("generate")
        .arg("--fixture")
        .arg(fixture)
        .arg("--formulas")
        .arg(formulas.to_string());
    command
}

#[cfg(feature = "c6_calibration")]
fn sample_args(fixture: &Path, job: Job, cli: &Cli) -> Vec<String> {
    vec![
        "sample".to_string(),
        "--fixture".to_string(),
        fixture.display().to_string(),
        "--formulas".to_string(),
        cli.formulas.to_string(),
        "--path".to_string(),
        job.path.label().to_string(),
        "--scope".to_string(),
        match job.scope {
            TargetScope::Tiny => "tiny",
            TargetScope::Medium => "medium",
            TargetScope::Full => "full",
        }
        .to_string(),
        "--warm-repeats".to_string(),
        cli.warm_repeats.to_string(),
    ]
}

#[cfg(feature = "c6_calibration")]
fn run_job(
    probe: &Path,
    fixture: &Path,
    job: Job,
    cli: &Cli,
    stderr_log: &Path,
    time_log: &Path,
) -> SampleResult {
    let args = sample_args(fixture, job, cli);
    let command = if Path::new("/usr/bin/time").is_file() {
        let mut command = Command::new("/usr/bin/time");
        command.arg("-v").arg("-o").arg(time_log).arg(probe);
        command.args(&args);
        command
    } else {
        let mut command = Command::new(probe);
        command.args(&args);
        command
    };
    let process = run_process(
        command,
        Duration::from_secs(cli.timeout_seconds),
        stderr_log,
    );
    match process {
        Ok(process) if process.success => {
            match serde_json::from_slice::<ChildReport>(&process.stdout) {
                Ok(child) => SampleResult {
                    job,
                    status: child.status.clone(),
                    child: Some(child),
                    external_max_rss_bytes: parse_time_max_rss(time_log),
                    wall_milliseconds: process.wall_milliseconds,
                    error: None,
                    stderr_log: stderr_log.display().to_string(),
                },
                Err(error) => SampleResult {
                    job,
                    status: "invalid_json".to_string(),
                    child: None,
                    external_max_rss_bytes: parse_time_max_rss(time_log),
                    wall_milliseconds: process.wall_milliseconds,
                    error: Some(error.to_string()),
                    stderr_log: stderr_log.display().to_string(),
                },
            }
        }
        Ok(process) => SampleResult {
            job,
            status: if process.timed_out {
                "timeout"
            } else {
                "child_error"
            }
            .to_string(),
            child: None,
            external_max_rss_bytes: parse_time_max_rss(time_log),
            wall_milliseconds: process.wall_milliseconds,
            error: Some(process.error),
            stderr_log: stderr_log.display().to_string(),
        },
        Err(error) => SampleResult {
            job,
            status: "spawn_error".to_string(),
            child: None,
            external_max_rss_bytes: None,
            wall_milliseconds: 0.0,
            error: Some(error.to_string()),
            stderr_log: stderr_log.display().to_string(),
        },
    }
}

#[cfg(feature = "c6_calibration")]
struct ProcessResult {
    success: bool,
    timed_out: bool,
    stdout: Vec<u8>,
    wall_milliseconds: f64,
    error: String,
}

#[cfg(feature = "c6_calibration")]
fn run_process(
    mut command: Command,
    timeout: Duration,
    stderr_log: &Path,
) -> Result<ProcessResult> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let started = Instant::now();
    let mut child = command.spawn().context("spawn child process")?;
    let stdout = child.stdout.take().context("capture child stdout")?;
    let stderr = child.stderr.take().context("capture child stderr")?;
    let stdout_drain = thread::spawn(move || drain_pipe(stdout));
    let stderr_drain = thread::spawn(move || drain_pipe(stderr));
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() >= timeout {
            timed_out = true;
            kill_process_group(&mut child).context("kill timed-out child process group")?;
            break child.wait()?;
        }
        thread::sleep(Duration::from_millis(20));
    };
    let stdout = stdout_drain
        .join()
        .map_err(|_| anyhow::anyhow!("stdout drain thread panicked"))??;
    let stderr = stderr_drain
        .join()
        .map_err(|_| anyhow::anyhow!("stderr drain thread panicked"))??;
    if let Some(parent) = stderr_log.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(stderr_log, &stderr)?;
    Ok(ProcessResult {
        success: status.success() && !timed_out,
        timed_out,
        stdout,
        wall_milliseconds: started.elapsed().as_secs_f64() * 1000.0,
        error: String::from_utf8_lossy(&stderr).trim().to_string(),
    })
}

#[cfg(feature = "c6_calibration")]
fn drain_pipe(mut pipe: impl Read) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    pipe.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[cfg(all(feature = "c6_calibration", unix))]
fn kill_process_group(child: &mut std::process::Child) -> Result<()> {
    let status = Command::new("kill")
        .arg("-KILL")
        .arg(format!("-{}", child.id()))
        .status()
        .context("invoke kill for child process group")?;
    if !status.success() {
        child.kill().context("fallback kill of child process")?;
    }
    Ok(())
}

#[cfg(all(feature = "c6_calibration", not(unix)))]
fn kill_process_group(child: &mut std::process::Child) -> Result<()> {
    child.kill().context("kill child process")
}

#[cfg(feature = "c6_calibration")]
fn parse_time_max_rss(path: &Path) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    text.lines().find_map(|line| {
        let value = line
            .trim()
            .strip_prefix("Maximum resident set size (kbytes):")?
            .trim()
            .parse::<u64>()
            .ok()?;
        value.checked_mul(1024)
    })
}

#[cfg(feature = "c6_calibration")]
fn shuffle<T>(values: &mut [T], seed: u64) {
    let mut state = seed.max(1);
    for index in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(index, (state as usize) % (index + 1));
    }
}

#[cfg(feature = "c6_calibration")]
fn summarize(results: &[SampleResult], cli: &Cli) -> Vec<CaseSummary> {
    let mut summaries = Vec::new();
    for &scope in &cli.scopes {
        for &path in &cli.paths {
            let children = results
                .iter()
                .filter(|result| result.job.path == path && result.job.scope == scope)
                .filter_map(|result| result.child.as_ref())
                .filter(|child| child.status == "ok")
                .collect::<Vec<_>>();
            let phase = |extract: fn(&ChildReport) -> Option<f64>| {
                distribution(
                    &children
                        .iter()
                        .filter_map(|child| extract(child))
                        .collect::<Vec<_>>(),
                )
            };
            let flattened = |extract: fn(&ChildReport) -> Vec<f64>| {
                distribution(
                    &children
                        .iter()
                        .flat_map(|child| extract(child))
                        .collect::<Vec<_>>(),
                )
            };
            let cold_total_ms = distribution(
                &children
                    .iter()
                    .map(|child| {
                        [
                            child.phases.load.as_ref(),
                            child.phases.unrelated_dirty_setup.as_ref(),
                            child.phases.bind_target_resolution.as_ref(),
                            if path == CalibrationPath::Sheetport {
                                None
                            } else {
                                child.phases.preparation_plan_build.as_ref()
                            },
                            child.phases.first_evaluation.as_ref(),
                            child.phases.output_read.as_ref(),
                        ]
                        .into_iter()
                        .flatten()
                        .map(|phase| phase.milliseconds)
                        .sum()
                    })
                    .collect::<Vec<_>>(),
            );
            let rss = results
                .iter()
                .filter(|result| {
                    result.job.path == path
                        && result.job.scope == scope
                        && result.status == "ok"
                        && result
                            .child
                            .as_ref()
                            .is_some_and(|child| child.status == "ok")
                })
                .filter_map(|result| {
                    result
                        .external_max_rss_bytes
                        .or_else(|| result.child.as_ref()?.peak_rss_bytes)
                })
                .map(|bytes| bytes as f64)
                .collect::<Vec<_>>();
            let prepared = children
                .iter()
                .filter_map(|child| {
                    Some(
                        child
                            .graph_after_run
                            .as_ref()?
                            .graph_formula_vertices
                            .saturating_sub(
                                child.graph_after_setup.as_ref()?.graph_formula_vertices,
                            ) as u64,
                    )
                })
                .max();
            let max_telemetry =
                |extract: fn(&formualizer_bench_core::c6_calibration::EngineTelemetry) -> u64| {
                    children
                        .iter()
                        .flat_map(|child| child.telemetry.values())
                        .map(extract)
                        .max()
                };
            let telemetry_phase = |key: &str| {
                distribution(
                    &children
                        .iter()
                        .filter_map(|child| child.telemetry.get(key))
                        .map(|telemetry| telemetry.request_total_ms)
                        .collect::<Vec<_>>(),
                )
            };
            summaries.push(CaseSummary {
                path,
                scope,
                successful_samples: children.len(),
                cold_total_ms,
                load_ms: phase(|child| child.phases.load.as_ref().map(|p| p.milliseconds)),
                bind_resolution_ms: phase(|child| {
                    child
                        .phases
                        .bind_target_resolution
                        .as_ref()
                        .map(|p| p.milliseconds)
                }),
                preparation_plan_ms: phase(|child| {
                    child
                        .phases
                        .preparation_plan_build
                        .as_ref()
                        .map(|p| p.milliseconds)
                }),
                first_evaluation_ms: phase(|child| {
                    child
                        .phases
                        .first_evaluation
                        .as_ref()
                        .map(|p| p.milliseconds)
                }),
                output_read_ms: phase(|child| {
                    child.phases.output_read.as_ref().map(|p| p.milliseconds)
                }),
                edit_ms: flattened(|child| {
                    child.phases.edit.iter().map(|p| p.milliseconds).collect()
                }),
                warm_evaluation_ms: flattened(|child| {
                    child
                        .phases
                        .warm_evaluation
                        .iter()
                        .map(|p| p.milliseconds)
                        .collect()
                }),
                batch_restore_ms: phase(|child| {
                    child.phases.batch_restore.as_ref().map(|p| p.milliseconds)
                }),
                sheetport_batch_plan_telemetry_ms: telemetry_phase("sheetport_batch_plan_build"),
                sheetport_batch_execution_telemetry_ms: telemetry_phase(
                    "sheetport_batch_execution",
                ),
                peak_rss_bytes: distribution(&rss),
                max_prepared_formula_delta: prepared,
                max_staged_selected: max_telemetry(|stats| stats.staged_selected),
                max_target_actual_work: max_telemetry(|stats| stats.target_commit_actual_work),
                locality_oracle_all_passed: children
                    .iter()
                    .all(|child| child.oracle_within_one_percent.unwrap_or(true)),
                analytical_output_oracle_all_passed: children
                    .iter()
                    .all(|child| child.analytical_output_oracle_passed.unwrap_or(false)),
                exact_fixture_counts_all_passed: children
                    .iter()
                    .all(|child| child.exact_fixture_counts_passed.unwrap_or(false)),
                exact_locality_counts_all_passed: children
                    .iter()
                    .all(|child| child.exact_locality_counts_passed.unwrap_or(false)),
                unrelated_staged_all_retained: children
                    .iter()
                    .all(|child| child.unrelated_staged_retained.unwrap_or(true)),
                unrelated_dirty_all_retained: children
                    .iter()
                    .all(|child| child.unrelated_dirty_retained.unwrap_or(true)),
            });
        }
    }
    summaries
}

#[cfg(feature = "c6_calibration")]
fn parity(results: &[SampleResult], scopes: &[TargetScope]) -> BTreeMap<String, bool> {
    let mut parity = BTreeMap::new();
    for &scope in scopes {
        let outcomes = results
            .iter()
            .filter(|result| result.job.scope == scope && result.status == "ok")
            .filter_map(|result| result.child.as_ref())
            .filter(|child| child.status == "ok")
            .map(|child| {
                child
                    .typed_error
                    .clone()
                    .unwrap_or_else(|| child.output_checksum.clone().unwrap_or_default())
            })
            .collect::<BTreeSet<_>>();
        parity.insert(scope.label().to_string(), outcomes.len() == 1);
    }
    parity
}

#[cfg(feature = "c6_calibration")]
fn identity(root: &Path) -> RunIdentity {
    let output = |program: &str, args: &[&str]| {
        Command::new(program)
            .args(args)
            .current_dir(root)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
            .unwrap_or_else(|| "unknown".to_string())
    };
    RunIdentity {
        commit: output("git", &["rev-parse", "HEAD"]),
        dirty: !output("git", &["status", "--porcelain"]).is_empty(),
        rustc: output("rustc", &["--version", "--verbose"]),
        host: output("hostname", &[]),
        os: output("uname", &["-srv"]),
        architecture: output("uname", &["-m"]),
    }
}

#[cfg(feature = "c6_calibration")]
fn fmt_stat(value: Option<Distribution>) -> String {
    value.map_or_else(
        || "n/a".to_string(),
        |stats| {
            format!(
                "{:.2} / {:.2} / {:.2} / {:.2}",
                stats.median, stats.p95, stats.mad, stats.max
            )
        },
    )
}

#[cfg(feature = "c6_calibration")]
fn render_markdown(report: &MatrixReport) -> String {
    let mut out = format!(
        "# C6 target-locality calibration\n\n- Commit: `{}`{}\n- Fixture: `{}` formulas, SHA-256 `{}`\n- Samples: {} per case; randomized seed `{:#x}`; child timeout {}s\n- Rust: `{}`\n- Host: `{}` / `{}` / `{}`\n\n",
        report.identity.commit,
        if report.identity.dirty {
            " (dirty)"
        } else {
            ""
        },
        report.formulas,
        report.fixture_sha256,
        report.samples_per_case,
        report.random_seed,
        report.timeout_seconds,
        report.identity.rustc.lines().next().unwrap_or("unknown"),
        report.identity.host,
        report.identity.os,
        report.identity.architecture,
    );
    out.push_str("All timing cells are `median / p95 / MAD / max` in milliseconds, using nearest-rank p95. With seven per-child samples, per-child p95 is the maximum; pooled warm calls contain 21 observations. `cold` is process-cold but normally OS-page-cache-warm and includes load, the explicitly reported unrelated-dirty setup, binding/resolution, first evaluation, and preparation/plan plus separate output read where those precede the first evaluation. SheetPort cold is its one-shot path; its subsequent batch-plan build is shown only in `prepare/plan`. Cells and SheetPort retain combined phases when their public APIs do not expose a split.\n\n");
    out.push_str("| scope | path | n | cold | prepare/plan | first eval | warm API cost | batch restore | peak RSS MiB | prepared formulas | staged selected | actual target work | gates |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
    for summary in &report.summaries {
        let rss = summary.peak_rss_bytes.map(|mut stats| {
            stats.median /= 1_048_576.0;
            stats.p95 /= 1_048_576.0;
            stats.mad /= 1_048_576.0;
            stats.max /= 1_048_576.0;
            stats
        });
        let gates = if summary.locality_oracle_all_passed
            && summary.analytical_output_oracle_all_passed
            && summary.exact_fixture_counts_all_passed
            && summary.exact_locality_counts_all_passed
            && summary.unrelated_staged_all_retained
            && summary.unrelated_dirty_all_retained
        {
            "pass"
        } else {
            "FAIL"
        };
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            summary.scope.label(),
            summary.path.label(),
            summary.successful_samples,
            fmt_stat(summary.cold_total_ms),
            fmt_stat(summary.preparation_plan_ms),
            fmt_stat(summary.first_evaluation_ms),
            fmt_stat(summary.warm_evaluation_ms),
            fmt_stat(summary.batch_restore_ms),
            fmt_stat(rss),
            summary
                .max_prepared_formula_delta
                .map_or_else(|| "n/a".to_string(), |v| v.to_string()),
            summary
                .max_staged_selected
                .map_or_else(|| "n/a".to_string(), |v| v.to_string()),
            summary
                .max_target_actual_work
                .map_or_else(|| "n/a".to_string(), |v| v.to_string()),
            gates,
        ));
    }
    out.push_str("\n## SheetPort telemetry snapshots\n\nThe raw telemetry keys `first_evaluation`, `sheetport_batch_plan_build`, and `sheetport_batch_execution` are distinct and never overwrite one another. The last key is captured after `batch.run`, so its request ledger describes the final baseline-restoration evaluation. Values are engine request-total milliseconds.\n\n");
    out.push_str("| scope | after batch-plan construction | after batch execution/restoration |\n|---|---:|---:|\n");
    for summary in report
        .summaries
        .iter()
        .filter(|summary| summary.path == CalibrationPath::Sheetport)
    {
        out.push_str(&format!(
            "| {} | {} | {} |\n",
            summary.scope.label(),
            fmt_stat(summary.sheetport_batch_plan_telemetry_ms),
            fmt_stat(summary.sheetport_batch_execution_telemetry_ms),
        ));
    }
    out.push_str("\n## Correctness\n\n");
    out.push_str("- Every successful sample passes a closed-form finance-chain output oracle for the initial evaluation and every repeated edit, independent of engine/path parity.\n");
    out.push_str("- Exact deferred/staged/prepared/dirty fixture assertions are included in the table gate.\n");
    for (scope, passed) in &report.parity_by_scope {
        out.push_str(&format!(
            "- `{scope}` output/error parity: {}\n",
            if *passed { "pass" } else { "FAIL" }
        ));
    }
    out.push_str("\n## Interpretation limits\n\n- `warm API cost` means the public API latency after one selected-branch input edit; it is not a claim that 100% of formulas are dirty or recomputed. In `full_100pct`, the fixed edit is `Tiny!A1`, a 0.5% branch, while all terminal outputs are requested.\n- `cells` first/warm timings combine target preparation, ephemeral plan construction, evaluation, and returned output because `evaluate_cells` exposes one public call.\n- SheetPort one-shot combines selector resolution, target preparation, evaluation, and runtime output read; batch iterations combine input edit, plan evaluation, and runtime output read. Checked baseline-restoration accounting is reported separately.\n- SheetPort batch creation follows its one-shot evaluation on the already prepared workbook. Its separately reported batch-plan build is not a second cold latency and is not setup-equivalent to the direct plan build on deferred staging.\n- Prepared formula deltas and public request telemetry prove locality; no allocator-specific counter is available, so RSS/HWM and ledger retained/scratch accounting are reported instead.\n");
    out
}

#[cfg(all(test, feature = "c6_calibration"))]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_smoke_command() {
        let cli = Cli::try_parse_from([
            "matrix",
            "--formulas",
            "500",
            "--samples",
            "1",
            "--paths",
            "full,cells",
            "--scopes",
            "tiny,full",
            "--timeout-seconds",
            "10",
        ])
        .unwrap();
        assert_eq!(cli.formulas, 500);
        assert_eq!(
            cli.paths,
            vec![CalibrationPath::Full, CalibrationPath::Cells]
        );
        assert_eq!(cli.scopes, vec![TargetScope::Tiny, TargetScope::Full]);
    }

    #[test]
    fn shuffle_is_reproducible() {
        let mut left = (0..20).collect::<Vec<_>>();
        let mut right = left.clone();
        shuffle(&mut left, 42);
        shuffle(&mut right, 42);
        assert_eq!(left, right);
        assert_ne!(left, (0..20).collect::<Vec<_>>());
    }
}
