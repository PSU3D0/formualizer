#[cfg(feature = "formualizer_runner")]
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(feature = "formualizer_runner")]
use anyhow::{Context, Result, bail};
#[cfg(feature = "formualizer_runner")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "formualizer_runner")]
use serde::{Deserialize, Serialize};

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --bin probe-load-envelope-matrix -- ..."
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
fn main() -> Result<()> {
    let cli = Cli::parse();
    let root = repo_root();
    build_probe_binary(&root, &cli)?;
    let results = run_matrix(&root, &cli)?;
    let markdown = render_markdown(&results);
    println!("{markdown}");

    if let Some(path) = &cli.json_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_string_pretty(&results)? + "\n")?;
    }

    if let Some(path) = &cli.markdown_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, markdown + "\n")?;
    }

    Ok(())
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Parser)]
#[command(about = "Run load/eval envelope probes across a scenario matrix")]
struct Cli {
    #[arg(long, value_enum, default_value_t = Preset::Smoke)]
    preset: Preset,

    #[arg(long, value_enum, default_value_t = BackendKind::Umya)]
    backend: BackendKind,

    /// Hard timeout applied to generation and load/eval subprocesses independently.
    #[arg(long, default_value_t = 60)]
    timeout_seconds: u64,

    /// Workbook logical-cell budget passed into each probe.
    #[arg(long, default_value_t = 256_000_000)]
    logical_cell_budget: u64,

    /// Sparse-sheet threshold passed into each probe.
    #[arg(long, default_value_t = 250_000)]
    sparse_sheet_threshold: u64,

    /// Max sparse logical/populated ratio passed into each probe.
    #[arg(long, default_value_t = 1_024)]
    max_sparse_ratio: u64,

    /// Enable loader subphase timing/debug logs (`FZ_DEBUG_LOAD=1`).
    #[arg(long)]
    debug_load: bool,

    /// Build the probe with `perf_instrumentation` enabled for fine-grained loader timings.
    #[arg(long)]
    perf_instrumentation: bool,

    /// Experimental Umya lazy-open mode (`FZ_UMYA_LAZY_READ=1`).
    #[arg(long)]
    umya_lazy_read: bool,

    /// Keep generated XLSX artifacts in the output directory.
    #[arg(long)]
    keep_workbooks: bool,

    /// Directory for generated XLSX artifacts and per-case logs.
    #[arg(long)]
    output_dir: Option<PathBuf>,

    /// Optional path to write JSON results.
    #[arg(long)]
    json_out: Option<PathBuf>,

    /// Optional path to write markdown summary.
    #[arg(long)]
    markdown_out: Option<PathBuf>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, ValueEnum)]
enum Preset {
    Smoke,
    Iteration,
    Targeted,
    Envelope,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy, ValueEnum)]
enum BackendKind {
    Umya,
    Calamine,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Copy)]
struct Case {
    scenario: &'static str,
    rows: u32,
    logical_cols: u32,
    report_rows: u32,
    active_cols: u32,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProbeReport {
    backend: String,
    scenario: String,
    workbook_path: String,
    logical_rows: u32,
    logical_cols: u32,
    logical_cells: u64,
    populated_cells_estimate: u64,
    advisory_timeout_seconds: u64,
    status: String,
    generation_ms: f64,
    load_ms: Option<f64>,
    evaluate_ms: Option<f64>,
    load_within_budget: Option<bool>,
    evaluate_within_budget: Option<bool>,
    error: Option<String>,
}

#[cfg(feature = "formualizer_runner")]
#[derive(Debug, Clone, Serialize)]
struct MatrixResult {
    backend: String,
    scenario: String,
    workbook_path: String,
    logical_rows: u32,
    logical_cols: u32,
    logical_cells: u64,
    populated_cells_estimate: Option<u64>,
    advisory_timeout_seconds: u64,
    status: String,
    generation_ms: Option<f64>,
    load_ms: Option<f64>,
    evaluate_ms: Option<f64>,
    load_within_budget: Option<bool>,
    evaluate_within_budget: Option<bool>,
    generate_log_path: String,
    measure_log_path: String,
    error: Option<String>,
}

#[cfg(feature = "formualizer_runner")]
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("repo root")
        .to_path_buf()
}

#[cfg(feature = "formualizer_runner")]
fn target_dir(root: &Path) -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target"))
}

#[cfg(feature = "formualizer_runner")]
fn probe_binary_path(root: &Path) -> PathBuf {
    let exe = if cfg!(windows) {
        "probe-load-envelope.exe"
    } else {
        "probe-load-envelope"
    };
    target_dir(root).join("release").join(exe)
}

#[cfg(feature = "formualizer_runner")]
fn build_probe_binary(root: &Path, cli: &Cli) -> Result<()> {
    let mut features = String::from("formualizer_runner");
    if cli.perf_instrumentation {
        features.push_str(",perf_instrumentation");
    }
    let status = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .arg("-p")
        .arg("formualizer-bench-core")
        .arg("--features")
        .arg(features)
        .arg("--bin")
        .arg("probe-load-envelope")
        .current_dir(root)
        .status()
        .context("build probe-load-envelope binary")?;
    if !status.success() {
        bail!("cargo build for probe-load-envelope failed with status {status}");
    }
    Ok(())
}

#[cfg(feature = "formualizer_runner")]
fn cases_for_preset(preset: Preset) -> &'static [Case] {
    const SMOKE: &[Case] = &[
        Case {
            scenario: "linear-rollup",
            rows: 10_000,
            logical_cols: 20,
            report_rows: 0,
            active_cols: 4,
        },
        Case {
            scenario: "sumifs-report",
            rows: 10_000,
            logical_cols: 20,
            report_rows: 32,
            active_cols: 4,
        },
        Case {
            scenario: "whole-column-summary",
            rows: 10_000,
            logical_cols: 20,
            report_rows: 16,
            active_cols: 4,
        },
    ];

    const ITERATION: &[Case] = &[
        Case {
            scenario: "linear-rollup",
            rows: 25_000,
            logical_cols: 20,
            report_rows: 0,
            active_cols: 4,
        },
        Case {
            scenario: "linear-rollup",
            rows: 50_000,
            logical_cols: 20,
            report_rows: 0,
            active_cols: 4,
        },
    ];

    const TARGETED: &[Case] = &[
        Case {
            scenario: "linear-rollup",
            rows: 700_000,
            logical_cols: 20,
            report_rows: 0,
            active_cols: 4,
        },
        Case {
            scenario: "sumifs-report",
            rows: 100_000,
            logical_cols: 20,
            report_rows: 256,
            active_cols: 4,
        },
        Case {
            scenario: "sumifs-report",
            rows: 250_000,
            logical_cols: 50,
            report_rows: 256,
            active_cols: 4,
        },
        Case {
            scenario: "whole-column-summary",
            rows: 1_000_000,
            logical_cols: 50,
            report_rows: 64,
            active_cols: 4,
        },
    ];

    const ENVELOPE: &[Case] = &[
        Case {
            scenario: "linear-rollup",
            rows: 700_000,
            logical_cols: 20,
            report_rows: 0,
            active_cols: 4,
        },
        Case {
            scenario: "linear-rollup",
            rows: 1_000_000,
            logical_cols: 50,
            report_rows: 0,
            active_cols: 4,
        },
        Case {
            scenario: "linear-rollup",
            rows: 1_000_000,
            logical_cols: 128,
            report_rows: 0,
            active_cols: 4,
        },
        Case {
            scenario: "linear-rollup",
            rows: 1_000_000,
            logical_cols: 256,
            report_rows: 0,
            active_cols: 4,
        },
        Case {
            scenario: "sumifs-report",
            rows: 100_000,
            logical_cols: 20,
            report_rows: 256,
            active_cols: 4,
        },
        Case {
            scenario: "sumifs-report",
            rows: 250_000,
            logical_cols: 50,
            report_rows: 256,
            active_cols: 4,
        },
        Case {
            scenario: "sumifs-report",
            rows: 500_000,
            logical_cols: 128,
            report_rows: 256,
            active_cols: 4,
        },
        Case {
            scenario: "sumifs-report",
            rows: 1_000_000,
            logical_cols: 256,
            report_rows: 256,
            active_cols: 4,
        },
        Case {
            scenario: "whole-column-summary",
            rows: 700_000,
            logical_cols: 20,
            report_rows: 64,
            active_cols: 4,
        },
        Case {
            scenario: "whole-column-summary",
            rows: 1_000_000,
            logical_cols: 50,
            report_rows: 64,
            active_cols: 4,
        },
        Case {
            scenario: "whole-column-summary",
            rows: 1_000_000,
            logical_cols: 128,
            report_rows: 64,
            active_cols: 4,
        },
        Case {
            scenario: "whole-column-summary",
            rows: 1_000_000,
            logical_cols: 256,
            report_rows: 64,
            active_cols: 4,
        },
    ];

    match preset {
        Preset::Smoke => SMOKE,
        Preset::Iteration => ITERATION,
        Preset::Targeted => TARGETED,
        Preset::Envelope => ENVELOPE,
    }
}

#[cfg(feature = "formualizer_runner")]
fn default_output_dir(root: &Path) -> PathBuf {
    root.join("scratch").join("load-eval-envelope")
}

#[cfg(feature = "formualizer_runner")]
impl BackendKind {
    fn label(self) -> &'static str {
        match self {
            BackendKind::Umya => "umya",
            BackendKind::Calamine => "calamine",
        }
    }
}

#[cfg(feature = "formualizer_runner")]
fn workbook_path(output_dir: &Path, case: Case) -> PathBuf {
    output_dir.join(format!(
        "{}-{}x{}.xlsx",
        case.scenario, case.rows, case.logical_cols
    ))
}

#[cfg(feature = "formualizer_runner")]
fn log_path(output_dir: &Path, backend: BackendKind, case: Case, phase: &str) -> PathBuf {
    output_dir.join(format!(
        "{}-{}-{}x{}-{}.log",
        backend.label(),
        case.scenario,
        case.rows,
        case.logical_cols,
        phase,
    ))
}

#[cfg(feature = "formualizer_runner")]
fn run_matrix(root: &Path, cli: &Cli) -> Result<Vec<MatrixResult>> {
    let output_dir = cli
        .output_dir
        .clone()
        .unwrap_or_else(|| default_output_dir(root));
    fs::create_dir_all(&output_dir)?;

    let probe = probe_binary_path(root);
    let mut results = Vec::new();
    for case in cases_for_preset(cli.preset) {
        results.push(run_case(root, &probe, &output_dir, *case, cli)?);
    }
    Ok(results)
}

#[cfg(feature = "formualizer_runner")]
fn run_case(
    root: &Path,
    probe: &Path,
    output_dir: &Path,
    case: Case,
    cli: &Cli,
) -> Result<MatrixResult> {
    let workbook = workbook_path(output_dir, case);
    let generate_log_path = log_path(output_dir, cli.backend, case, "generate");
    let measure_log_path = log_path(output_dir, cli.backend, case, "measure");

    let generate_args = probe_args(case, cli, &workbook, true);
    let generated = run_probe_subprocess(
        root,
        probe,
        &generate_args,
        cli,
        &generate_log_path,
        cli.timeout_seconds,
    )
    .with_context(|| {
        format!(
            "generate case {} {}x{} ({})",
            case.scenario,
            case.rows,
            case.logical_cols,
            cli.backend.label(),
        )
    });

    let generated = match generated {
        Ok(result) => result,
        Err(err) => {
            return Ok(MatrixResult {
                backend: cli.backend.label().to_string(),
                scenario: case.scenario.to_string(),
                workbook_path: workbook.display().to_string(),
                logical_rows: case.rows,
                logical_cols: case.logical_cols,
                logical_cells: u64::from(case.rows) * u64::from(case.logical_cols),
                populated_cells_estimate: None,
                advisory_timeout_seconds: cli.timeout_seconds,
                status: "generation_timeout".to_string(),
                generation_ms: None,
                load_ms: None,
                evaluate_ms: None,
                load_within_budget: None,
                evaluate_within_budget: None,
                generate_log_path: generate_log_path.display().to_string(),
                measure_log_path: measure_log_path.display().to_string(),
                error: Some(err.to_string()),
            });
        }
    };

    let measure_args = probe_args(case, cli, &workbook, false);
    let measured = run_probe_subprocess(
        root,
        probe,
        &measure_args,
        cli,
        &measure_log_path,
        cli.timeout_seconds,
    )
    .with_context(|| {
        format!(
            "measure case {} {}x{} ({})",
            case.scenario,
            case.rows,
            case.logical_cols,
            cli.backend.label(),
        )
    });

    let mut result = match measured {
        Ok(result) => MatrixResult {
            backend: result.backend,
            scenario: result.scenario,
            workbook_path: result.workbook_path,
            logical_rows: result.logical_rows,
            logical_cols: result.logical_cols,
            logical_cells: result.logical_cells,
            populated_cells_estimate: Some(generated.populated_cells_estimate),
            advisory_timeout_seconds: result.advisory_timeout_seconds,
            status: result.status,
            generation_ms: Some(generated.generation_ms),
            load_ms: result.load_ms,
            evaluate_ms: result.evaluate_ms,
            load_within_budget: result.load_within_budget,
            evaluate_within_budget: result.evaluate_within_budget,
            generate_log_path: generate_log_path.display().to_string(),
            measure_log_path: measure_log_path.display().to_string(),
            error: result.error,
        },
        Err(err) => MatrixResult {
            backend: cli.backend.label().to_string(),
            scenario: case.scenario.to_string(),
            workbook_path: workbook.display().to_string(),
            logical_rows: case.rows,
            logical_cols: case.logical_cols,
            logical_cells: u64::from(case.rows) * u64::from(case.logical_cols),
            populated_cells_estimate: Some(generated.populated_cells_estimate),
            advisory_timeout_seconds: cli.timeout_seconds,
            status: "timeout".to_string(),
            generation_ms: Some(generated.generation_ms),
            load_ms: None,
            evaluate_ms: None,
            load_within_budget: Some(false),
            evaluate_within_budget: Some(false),
            generate_log_path: generate_log_path.display().to_string(),
            measure_log_path: measure_log_path.display().to_string(),
            error: Some(err.to_string()),
        },
    };

    if !cli.keep_workbooks {
        let _ = fs::remove_file(&workbook);
    }

    if result.scenario.is_empty() {
        result.scenario = case.scenario.to_string();
    }
    if result.backend.is_empty() {
        result.backend = cli.backend.label().to_string();
    }

    Ok(result)
}

#[cfg(feature = "formualizer_runner")]
fn probe_args(case: Case, cli: &Cli, workbook: &Path, generate_only: bool) -> Vec<String> {
    let mut args = vec![
        "--scenario".to_string(),
        case.scenario.to_string(),
        "--backend".to_string(),
        cli.backend.label().to_string(),
        "--rows".to_string(),
        case.rows.to_string(),
        "--logical-cols".to_string(),
        case.logical_cols.to_string(),
        "--report-rows".to_string(),
        case.report_rows.to_string(),
        "--active-cols".to_string(),
        case.active_cols.to_string(),
        "--logical-cell-budget".to_string(),
        cli.logical_cell_budget.to_string(),
        "--sparse-sheet-threshold".to_string(),
        cli.sparse_sheet_threshold.to_string(),
        "--max-sparse-ratio".to_string(),
        cli.max_sparse_ratio.to_string(),
        "--timeout-seconds".to_string(),
        cli.timeout_seconds.to_string(),
    ];

    if generate_only {
        args.push("--output".to_string());
        args.push(workbook.display().to_string());
        args.push("--generate-only".to_string());
        if cli.keep_workbooks {
            args.push("--keep-workbook".to_string());
        }
    } else {
        args.push("--input".to_string());
        args.push(workbook.display().to_string());
    }
    args
}

#[cfg(feature = "formualizer_runner")]
fn run_probe_subprocess(
    root: &Path,
    probe: &Path,
    args: &[String],
    cli: &Cli,
    log_path: &Path,
    timeout_seconds: u64,
) -> Result<ProbeReport> {
    let mut command = Command::new(probe);
    command
        .args(args)
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if cli.debug_load {
        command.env("FZ_DEBUG_LOAD", "1");
    }
    if cli.umya_lazy_read {
        command.env("FZ_UMYA_LAZY_READ", "1");
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("spawn {:?} {:?}", probe, args))?;

    let start = Instant::now();
    let status = wait_with_timeout(&mut child, Duration::from_secs(timeout_seconds))?;

    let mut stdout = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_string(&mut stdout);
    }
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    let elapsed = start.elapsed();

    let env_summary = format!(
        "FZ_DEBUG_LOAD={} FZ_UMYA_LAZY_READ={} PERF_INSTRUMENTATION={}",
        if cli.debug_load { "1" } else { "0" },
        if cli.umya_lazy_read { "1" } else { "0" },
        if cli.perf_instrumentation { "1" } else { "0" },
    );
    let status_summary = status
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("timeout after {timeout_seconds}s"));
    let log_body = format!(
        "$ {:?} {:?}\n[env] {}\n[status] {}\n[elapsed_s] {:.3}\n\n[stdout]\n{}\n\n[stderr]\n{}\n",
        probe,
        args,
        env_summary,
        status_summary,
        elapsed.as_secs_f64(),
        stdout,
        stderr,
    );
    fs::write(log_path, log_body)?;

    let Some(status) = status else {
        bail!(
            "timed out after {timeout_seconds}s (see {})",
            log_path.display()
        );
    };

    if !status.success() {
        bail!(
            "probe exited with {status} after {:.1}s (see {}): {}",
            elapsed.as_secs_f64(),
            log_path.display(),
            if stderr.trim().is_empty() {
                stdout.trim()
            } else {
                stderr.trim()
            }
        );
    }

    let line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("probe produced no JSON output (see {})", log_path.display())
        })?;
    serde_json::from_str::<ProbeReport>(line)
        .with_context(|| format!("parse probe json from line: {line}"))
}

#[cfg(feature = "formualizer_runner")]
fn wait_with_timeout(
    child: &mut Child,
    timeout: Duration,
) -> Result<Option<std::process::ExitStatus>> {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(feature = "formualizer_runner")]
fn fmt_ms(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{v:.1}"),
        None => "-".to_string(),
    }
}

#[cfg(feature = "formualizer_runner")]
fn fmt_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "yes",
        Some(false) => "no",
        None => "-",
    }
}

#[cfg(feature = "formualizer_runner")]
fn render_markdown(results: &[MatrixResult]) -> String {
    let mut out = String::from(
        "| Backend | Scenario | Shape | Logical cells | Status | Gen ms | Load ms | Eval ms | Load <=60s | Eval <=60s |\n|---|---|---:|---:|---|---:|---:|---:|---|---|\n",
    );
    for item in results {
        let shape = format!("{}x{}", item.logical_rows, item.logical_cols);
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            item.backend,
            item.scenario,
            shape,
            item.logical_cells,
            item.status,
            fmt_ms(item.generation_ms),
            fmt_ms(item.load_ms),
            fmt_ms(item.evaluate_ms),
            fmt_bool(item.load_within_budget),
            fmt_bool(item.evaluate_within_budget)
        ));
    }
    out
}
