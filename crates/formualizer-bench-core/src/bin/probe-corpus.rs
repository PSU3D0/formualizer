#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --release --bin probe-corpus"
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
mod enabled {
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result, bail};
    use clap::Parser;
    use formualizer_bench_core::instrumentation::{
        PhaseMetrics, PhaseReport, Reporter, introspection_notes,
    };
    use formualizer_bench_core::scenarios::common::{fixture_path, set_invariant_scale};
    use formualizer_bench_core::scenarios::{
        FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
        ScenarioPhase, ScenarioRegistry, ScenarioScale,
    };
    use formualizer_common::LiteralValue;
    use formualizer_eval::engine::{EvalConfig, FormulaPlaneMode};
    use formualizer_workbook::{
        LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
    };
    use regex::Regex;
    use serde::Serialize;

    #[derive(Debug, Parser)]
    #[command(about = "Run the FormulaPlane scenario corpus")]
    pub struct Cli {
        /// Required run label, used for default output directory.
        #[arg(long)]
        label: String,
        /// Scale to run: small, medium, or large.
        #[arg(long, default_value = "small")]
        scale: String,
        /// Modes to run: off, auth, or comma-separated off,auth.
        #[arg(long, default_value = "off,auth")]
        modes: String,
        /// Glob/CSV of scenario ids.
        #[arg(long, default_value = "*")]
        include: String,
        /// Fixture directory. Defaults under target/scenario-corpus/<label>/fixtures.
        #[arg(long)]
        fixture_dir: Option<PathBuf>,
        /// Output directory. Defaults under target/scenario-corpus/<label>.
        #[arg(long)]
        output_dir: Option<PathBuf>,
        /// Reuse existing fixture file if present.
        #[arg(long)]
        skip_fixture_rebuild: bool,
        /// Measure only load + first-eval.
        #[arg(long)]
        skip_edit_cycles: bool,
        /// Enable engine parallel evaluation.
        #[arg(long)]
        enable_parallel: Option<bool>,
        /// Per-evaluation phase timeout in milliseconds (load/first_eval/recalc).
        /// 0 disables. Defaults: small=5000, medium=15000, large=60000.
        #[arg(long)]
        phase_timeout_ms: Option<u64>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Mode {
        Off,
        Auth,
    }

    impl Mode {
        fn eval_mode(self) -> FormulaPlaneMode {
            match self {
                Mode::Off => FormulaPlaneMode::Off,
                Mode::Auth => FormulaPlaneMode::AuthoritativeExperimental,
            }
        }

        fn as_str(self) -> &'static str {
            match self {
                Mode::Off => "Off",
                Mode::Auth => "Auth",
            }
        }

        fn file_str(self) -> &'static str {
            match self {
                Mode::Off => "off",
                Mode::Auth => "auth",
            }
        }
    }

    #[derive(Debug, Serialize)]
    struct ScenarioSummaryReport {
        scenario_id: String,
        mode: String,
        scale: String,
        fixture_path: String,
        fixture_size_bytes: u64,
        phases: Vec<PhaseReport>,
        final_invariants_passed: bool,
        notes: Vec<String>,
    }

    #[derive(Debug, Clone)]
    struct SummaryRow {
        scenario: String,
        mode: String,
        scale: String,
        load_ms: f64,
        first_eval_ms: f64,
        recalc_p50_ms: f64,
        peak_rss_mb: Option<f64>,
        span_count: Option<u64>,
    }

    pub fn main() -> Result<()> {
        let _profiler = formualizer_bench_core::instrumentation::dhat::init_profiler();
        let cli = Cli::parse();
        let scale = parse_scale(&cli.scale)?;
        set_invariant_scale(scale);
        let modes = parse_modes(&cli.modes)?;
        let output_dir = cli.output_dir.clone().unwrap_or_else(|| {
            PathBuf::from("target")
                .join("scenario-corpus")
                .join(&cli.label)
        });
        let fixture_dir = cli
            .fixture_dir
            .clone()
            .unwrap_or_else(|| output_dir.join("fixtures"));
        std::fs::create_dir_all(&output_dir)?;
        std::fs::create_dir_all(&fixture_dir)?;

        let include = IncludeMatcher::new(&cli.include)?;
        let scenarios: Vec<Box<dyn Scenario>> = ScenarioRegistry::all()
            .into_iter()
            .filter(|scenario| include.matches(scenario.id()))
            .collect();
        if scenarios.is_empty() {
            bail!("no scenarios matched --include '{}'", cli.include);
        }

        let mut reports = Vec::new();
        let mut summaries = Vec::new();
        for scenario in &scenarios {
            for mode in &modes {
                eprintln!(
                    "[probe-corpus] {} mode={} scale={}",
                    scenario.id(),
                    mode.as_str(),
                    scale.as_str()
                );
                let phase_timeout_ms = cli
                    .phase_timeout_ms
                    .unwrap_or(default_phase_timeout_ms(scale));
                let report = match run_tuple(
                    scenario.as_ref(),
                    *mode,
                    scale,
                    &cli.label,
                    &fixture_dir,
                    cli.skip_fixture_rebuild,
                    cli.skip_edit_cycles,
                    phase_timeout_ms,
                    cli.enable_parallel.unwrap_or(false),
                ) {
                    Ok(r) => r,
                    Err(err) => {
                        eprintln!(
                            "[probe-corpus] {} mode={} scale={} fixture/run error: {err:#}",
                            scenario.id(),
                            mode.as_str(),
                            scale.as_str()
                        );
                        let mut report = ScenarioSummaryReport {
                            scenario_id: scenario.id().to_string(),
                            mode: mode.as_str().to_string(),
                            scale: scale_title(scale).to_string(),
                            fixture_path: String::new(),
                            fixture_size_bytes: 0,
                            phases: Vec::new(),
                            final_invariants_passed: expected_failure_reason(
                                scenario.as_ref(),
                                *mode,
                            )
                            .is_some(),
                            notes: vec![format!("fixture/run error: {err:#}")],
                        };
                        if let Some(reason) = expected_failure_reason(scenario.as_ref(), *mode) {
                            report
                                .notes
                                .push(format!("expected_failure_reason: {reason}"));
                        }
                        report
                    }
                };
                let json_path =
                    output_dir.join(format!("{}-{}.json", scenario.id(), mode.file_str()));
                Reporter::write_json(&json_path, &report)?;
                summaries.push(summary_row(&report));
                reports.push(report);
            }
        }

        let summary_md = render_summary_md(&summaries);
        Reporter::write_text(&output_dir.join("summary.md"), &summary_md)?;
        Reporter::write_text(
            &output_dir.join("summary.csv"),
            &render_summary_csv(&reports),
        )?;
        if modes.len() > 1 {
            Reporter::write_text(&output_dir.join("diff.md"), &render_diff_md(&summaries))?;
        }
        print!("{summary_md}");

        if reports.iter().any(|report| !report.final_invariants_passed) {
            bail!(
                "one or more scenario invariants failed; see JSON reports in {}",
                output_dir.display()
            );
        }
        Ok(())
    }

    fn default_phase_timeout_ms(scale: ScenarioScale) -> u64 {
        match scale {
            ScenarioScale::Small => 5_000,
            ScenarioScale::Medium => 15_000,
            ScenarioScale::Large => 60_000,
        }
    }

    /// Wrap a cancellable evaluate_all with a watchdog thread that flips the
    /// cancel flag after `timeout_ms`. Returns Err(Cancelled) if the timeout
    /// fired and the eval honored it. The cancel checkpoints inside
    /// `evaluate_all_cancellable` are coarse, so the actual return may lag by
    /// the duration of an in-flight scalar eval.
    ///
    /// The watchdog is detached (not joined). It uses a condvar so it returns
    /// promptly when the eval finishes early, so the OS doesn't accumulate
    /// long-running stuck threads across tuples.
    fn evaluate_all_with_timeout(
        workbook: &mut Workbook,
        timeout_ms: u64,
    ) -> std::result::Result<formualizer_eval::engine::EvalResult, formualizer_workbook::IoError>
    {
        if timeout_ms == 0 {
            return workbook.evaluate_all();
        }
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // (mutex<done>, condvar) — watchdog waits on the condvar with timeout.
        let signal = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let watchdog = {
            let cancel = cancel.clone();
            let signal = signal.clone();
            std::thread::spawn(move || {
                let (lock, cvar) = &*signal;
                let mut done = lock.lock().expect("watchdog mutex");
                while !*done {
                    let res = cvar
                        .wait_timeout(done, std::time::Duration::from_millis(timeout_ms))
                        .expect("watchdog cvar");
                    done = res.0;
                    if res.1.timed_out() && !*done {
                        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                        return;
                    }
                }
            })
        };
        let result = workbook.evaluate_all_cancellable(cancel.clone());
        // Wake the watchdog so it exits promptly.
        {
            let (lock, cvar) = &*signal;
            let mut done = lock.lock().expect("watchdog signal lock");
            *done = true;
            cvar.notify_all();
        }
        let _ = watchdog.join();
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn run_tuple(
        scenario: &dyn Scenario,
        mode: Mode,
        scale: ScenarioScale,
        label: &str,
        fixture_dir: &Path,
        skip_fixture_rebuild: bool,
        skip_edit_cycles: bool,
        phase_timeout_ms: u64,
        enable_parallel: bool,
    ) -> Result<ScenarioSummaryReport> {
        set_invariant_scale(scale);
        let ctx = ScenarioBuildCtx {
            scale,
            fixture_dir: fixture_dir.to_path_buf(),
            label: label.to_string(),
        };
        let expected_path = fixture_path(&ctx, scenario.id());
        let mut phases = Vec::new();
        let fixture = if skip_fixture_rebuild && expected_path.exists() {
            ScenarioFixture {
                path: expected_path,
                metadata: metadata_for(scenario.id(), scale)?,
            }
        } else {
            let phase = PhaseMetrics::start("phase_fixture_gen");
            let fixture = scenario
                .build_fixture(&ctx)
                .with_context(|| format!("build fixture for {}", scenario.id()))?;
            phases.push(phase.finish(None));
            fixture
        };
        let fixture_size_bytes = std::fs::metadata(&fixture.path)
            .with_context(|| format!("stat fixture {}", fixture.path.display()))?
            .len();

        let phase = PhaseMetrics::start("phase_load");
        let mut config = WorkbookConfig::ephemeral();
        config.eval = EvalConfig::default().with_formula_plane_mode(mode.eval_mode());
        config.eval.enable_parallel = enable_parallel;
        let backend = UmyaAdapter::open_path(&fixture.path)
            .with_context(|| format!("open fixture {}", fixture.path.display()))?;
        let mut workbook = Workbook::from_reader(backend, LoadStrategy::EagerAll, config)
            .with_context(|| format!("load fixture {}", fixture.path.display()))?;
        phases.push(phase.finish(Some(&workbook)));

        let mut invariant_failures = Vec::new();
        check_invariants(
            scenario,
            ScenarioPhase::AfterLoad,
            &workbook,
            &fixture.metadata,
            &mut invariant_failures,
        );

        let phase = PhaseMetrics::start("phase_first_eval");
        evaluate_all_with_timeout(&mut workbook, phase_timeout_ms)
            .with_context(|| format!("first evaluate_all for {}", scenario.id()))?;
        phases.push(phase.finish(Some(&workbook)));
        check_invariants(
            scenario,
            ScenarioPhase::AfterFirstEval,
            &workbook,
            &fixture.metadata,
            &mut invariant_failures,
        );

        if !skip_edit_cycles && let Some(plan) = scenario.edit_plan() {
            for cycle in 0..plan.cycles {
                let phase = PhaseMetrics::start(format!("phase_edit_{cycle}"));
                let kind = (plan.apply)(&mut workbook, cycle)
                    .with_context(|| format!("apply edit {cycle} for {}", scenario.id()))?;
                phases.push(phase.with_edit(cycle, kind).finish(Some(&workbook)));
                check_invariants(
                    scenario,
                    ScenarioPhase::AfterEdit { cycle, kind },
                    &workbook,
                    &fixture.metadata,
                    &mut invariant_failures,
                );

                let phase = PhaseMetrics::start(format!("phase_recalc_{cycle}"));
                evaluate_all_with_timeout(&mut workbook, phase_timeout_ms).with_context(|| {
                    format!("evaluate_all recalc cycle {cycle} for {}", scenario.id())
                })?;
                phases.push(phase.with_edit(cycle, kind).finish(Some(&workbook)));
                check_invariants(
                    scenario,
                    ScenarioPhase::AfterRecalc { cycle, kind },
                    &workbook,
                    &fixture.metadata,
                    &mut invariant_failures,
                );
            }
        }

        let mut notes = introspection_notes();
        let expected_failure = expected_failure_reason(scenario, mode);
        let failures_known = expected_failure.is_some();
        for failure in &invariant_failures {
            let prefix = if failures_known {
                "expected invariant failure"
            } else {
                "invariant failure"
            };
            notes.push(format!("{prefix}: {failure}"));
        }
        if let Some(reason) = expected_failure {
            notes.push(format!("expected_failure_reason: {reason}"));
        }
        Ok(ScenarioSummaryReport {
            scenario_id: scenario.id().to_string(),
            mode: mode.as_str().to_string(),
            scale: scale_title(scale).to_string(),
            fixture_path: fixture.path.display().to_string(),
            fixture_size_bytes,
            phases,
            final_invariants_passed: invariant_failures.is_empty() || failures_known,
            notes,
        })
    }

    fn expected_failure_reason(scenario: &dyn Scenario, mode: Mode) -> Option<&'static str> {
        use formualizer_bench_core::scenarios::ExpectedFailureMode;
        let target = match mode {
            Mode::Off => ExpectedFailureMode::OffOnly,
            Mode::Auth => ExpectedFailureMode::AuthOnly,
        };
        scenario
            .expected_to_fail_under()
            .iter()
            .find(|ef| ef.mode == target)
            .map(|ef| ef.reason)
    }

    fn check_invariants(
        scenario: &dyn Scenario,
        phase: ScenarioPhase,
        workbook: &Workbook,
        metadata: &FixtureMetadata,
        failures: &mut Vec<String>,
    ) {
        for invariant in scenario.invariants(phase) {
            if let Err(err) = check_invariant(invariant, workbook, metadata) {
                failures.push(format!("{} {:?}: {err}", scenario.id(), phase));
            }
        }
    }

    fn check_invariant(
        invariant: ScenarioInvariant,
        workbook: &Workbook,
        metadata: &FixtureMetadata,
    ) -> Result<()> {
        match invariant {
            ScenarioInvariant::CellEquals {
                sheet,
                row,
                col,
                expected,
            } => {
                let actual = workbook.get_value(&sheet, row, col);
                match actual {
                    Some(actual) if literal_equals(&actual, &expected) => Ok(()),
                    Some(actual) => bail!(
                        "{}!R{}C{} mismatch: got {:?}, expected {:?}",
                        sheet,
                        row,
                        col,
                        actual,
                        expected
                    ),
                    None if literal_equals(&LiteralValue::Empty, &expected) => Ok(()),
                    None => bail!(
                        "{}!R{}C{} missing, expected {:?}",
                        sheet,
                        row,
                        col,
                        expected
                    ),
                }
            }
            ScenarioInvariant::NoErrorCells { sheet } => {
                for row in 1..=metadata.rows {
                    for col in 1..=metadata.cols {
                        if let Some(LiteralValue::Error(error)) =
                            workbook.get_value(&sheet, row, col)
                        {
                            bail!("{}!R{}C{} contains error {:?}", sheet, row, col, error);
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn literal_equals(actual: &LiteralValue, expected: &LiteralValue) -> bool {
        match (actual, expected) {
            (LiteralValue::Number(a), LiteralValue::Number(e)) => (a - e).abs() <= 1e-6,
            _ => actual == expected,
        }
    }

    fn summary_row(report: &ScenarioSummaryReport) -> SummaryRow {
        let load_ms = phase_ms(&report.phases, "phase_load");
        let first_eval_ms = phase_ms(&report.phases, "phase_first_eval");
        let mut recalc: Vec<f64> = report
            .phases
            .iter()
            .filter(|phase| phase.phase.starts_with("phase_recalc_"))
            .map(|phase| phase.wall_ms)
            .collect();
        recalc.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let recalc_p50_ms = percentile(&recalc, 0.50);
        let peak_rss_mb = report
            .phases
            .iter()
            .filter_map(|phase| phase.rss_peak_phase_mb)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let span_count = report
            .phases
            .last()
            .and_then(|phase| phase.plane_span_count);
        SummaryRow {
            scenario: report.scenario_id.clone(),
            mode: report.mode.clone(),
            scale: report.scale.clone(),
            load_ms,
            first_eval_ms,
            recalc_p50_ms,
            peak_rss_mb,
            span_count,
        }
    }

    fn phase_ms(phases: &[PhaseReport], name: &str) -> f64 {
        phases
            .iter()
            .find(|phase| phase.phase == name)
            .map(|phase| phase.wall_ms)
            .unwrap_or(0.0)
    }

    fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let idx = ((sorted.len().saturating_sub(1)) as f64 * p).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn render_summary_md(rows: &[SummaryRow]) -> String {
        let mut out = String::new();
        out.push_str("| scenario | mode | scale | load_ms | first_eval_ms | recalc_p50_ms | peak_rss_mb | span_count |\n");
        out.push_str("|---|---|---|---:|---:|---:|---:|---:|\n");
        for row in rows {
            out.push_str(&format!(
                "| {} | {} | {} | {:.3} | {:.3} | {:.3} | {} | {} |\n",
                row.scenario,
                row.mode,
                row.scale,
                row.load_ms,
                row.first_eval_ms,
                row.recalc_p50_ms,
                fmt_opt_f64(row.peak_rss_mb),
                fmt_opt_u64(row.span_count)
            ));
        }
        out
    }

    fn render_summary_csv(reports: &[ScenarioSummaryReport]) -> String {
        let mut out = String::new();
        out.push_str("scenario_id,mode,scale,fixture_path,fixture_size_bytes,final_invariants_passed,phase,edit_cycle,edit_kind,wall_ms,cpu_ms,rss_start_mb,rss_end_mb,rss_peak_phase_mb,allocs_count,allocs_bytes,allocs_max_bytes,arena_node_count,arena_node_bytes,graph_vertex_count,graph_edge_count,graph_name_count,plane_span_count,plane_template_count,plane_active_span_cells,computed_overlay_cells,delta_overlay_cells,fragments_emitted\n");
        for report in reports {
            for phase in &report.phases {
                out.push_str(&csv_row(&[
                    report.scenario_id.clone(),
                    report.mode.clone(),
                    report.scale.clone(),
                    report.fixture_path.clone(),
                    report.fixture_size_bytes.to_string(),
                    report.final_invariants_passed.to_string(),
                    phase.phase.clone(),
                    phase.edit_cycle.map(|v| v.to_string()).unwrap_or_default(),
                    phase.edit_kind.clone().unwrap_or_default(),
                    format!("{:.6}", phase.wall_ms),
                    format!("{:.6}", phase.cpu_ms),
                    phase
                        .rss_start_mb
                        .map(|v| format!("{v:.6}"))
                        .unwrap_or_default(),
                    phase
                        .rss_end_mb
                        .map(|v| format!("{v:.6}"))
                        .unwrap_or_default(),
                    phase
                        .rss_peak_phase_mb
                        .map(|v| format!("{v:.6}"))
                        .unwrap_or_default(),
                    fmt_csv_opt_u64(phase.allocs_count),
                    fmt_csv_opt_u64(phase.allocs_bytes),
                    fmt_csv_opt_u64(phase.allocs_max_bytes),
                    fmt_csv_opt_u64(phase.arena_node_count),
                    fmt_csv_opt_u64(phase.arena_node_bytes),
                    fmt_csv_opt_u64(phase.graph_vertex_count),
                    fmt_csv_opt_u64(phase.graph_edge_count),
                    fmt_csv_opt_u64(phase.graph_name_count),
                    fmt_csv_opt_u64(phase.plane_span_count),
                    fmt_csv_opt_u64(phase.plane_template_count),
                    fmt_csv_opt_u64(phase.plane_active_span_cells),
                    fmt_csv_opt_u64(phase.computed_overlay_cells),
                    fmt_csv_opt_u64(phase.delta_overlay_cells),
                    fmt_csv_opt_u64(phase.fragments_emitted),
                ]));
            }
        }
        out
    }

    fn render_diff_md(rows: &[SummaryRow]) -> String {
        let mut out = String::new();
        out.push_str("| scenario | load Auth/Off | first_eval Auth/Off | recalc_p50 Auth/Off | peak_rss Auth/Off | spans Auth-Off |\n");
        out.push_str("|---|---:|---:|---:|---:|---:|\n");
        let scenarios: std::collections::BTreeSet<&str> =
            rows.iter().map(|row| row.scenario.as_str()).collect();
        for scenario in scenarios {
            let off = rows
                .iter()
                .find(|row| row.scenario == scenario && row.mode == "Off");
            let auth = rows
                .iter()
                .find(|row| row.scenario == scenario && row.mode == "Auth");
            if let (Some(off), Some(auth)) = (off, auth) {
                out.push_str(&format!(
                    "| {} | {:.3} | {:.3} | {:.3} | {} | {} |\n",
                    scenario,
                    ratio(auth.load_ms, off.load_ms),
                    ratio(auth.first_eval_ms, off.first_eval_ms),
                    ratio(auth.recalc_p50_ms, off.recalc_p50_ms),
                    opt_ratio(auth.peak_rss_mb, off.peak_rss_mb),
                    match (auth.span_count, off.span_count) {
                        (Some(a), Some(o)) => (a as i64 - o as i64).to_string(),
                        _ => "".to_string(),
                    }
                ));
            }
        }
        out
    }

    fn csv_row(fields: &[String]) -> String {
        let mut row = fields
            .iter()
            .map(|field| csv_escape(field))
            .collect::<Vec<_>>()
            .join(",");
        row.push('\n');
        row
    }

    fn csv_escape(field: &str) -> String {
        if field.contains(',') || field.contains('"') || field.contains('\n') {
            format!("\"{}\"", field.replace('"', "\"\""))
        } else {
            field.to_string()
        }
    }

    fn fmt_csv_opt_u64(value: Option<u64>) -> String {
        value.map(|v| v.to_string()).unwrap_or_default()
    }

    fn fmt_opt_f64(value: Option<f64>) -> String {
        value.map(|v| format!("{v:.3}")).unwrap_or_default()
    }

    fn fmt_opt_u64(value: Option<u64>) -> String {
        value.map(|v| v.to_string()).unwrap_or_default()
    }

    fn ratio(numerator: f64, denominator: f64) -> f64 {
        if denominator.abs() <= f64::EPSILON {
            if numerator.abs() <= f64::EPSILON {
                1.0
            } else {
                f64::INFINITY
            }
        } else {
            numerator / denominator
        }
    }

    fn opt_ratio(numerator: Option<f64>, denominator: Option<f64>) -> String {
        match (numerator, denominator) {
            (Some(n), Some(d)) => format!("{:.3}", ratio(n, d)),
            _ => "".to_string(),
        }
    }

    fn parse_scale(scale: &str) -> Result<ScenarioScale> {
        match scale.to_ascii_lowercase().as_str() {
            "small" => Ok(ScenarioScale::Small),
            "medium" => Ok(ScenarioScale::Medium),
            "large" => Ok(ScenarioScale::Large),
            other => bail!("unknown --scale '{other}', expected small|medium|large"),
        }
    }

    fn scale_title(scale: ScenarioScale) -> &'static str {
        match scale {
            ScenarioScale::Small => "Small",
            ScenarioScale::Medium => "Medium",
            ScenarioScale::Large => "Large",
        }
    }

    fn parse_modes(modes: &str) -> Result<Vec<Mode>> {
        let mut parsed = Vec::new();
        for token in modes
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
        {
            let mode = match token.to_ascii_lowercase().as_str() {
                "off" => Mode::Off,
                "auth" => Mode::Auth,
                other => bail!("unknown mode '{other}', expected off|auth"),
            };
            if !parsed.contains(&mode) {
                parsed.push(mode);
            }
        }
        if parsed.is_empty() {
            bail!("--modes must include at least one mode");
        }
        Ok(parsed)
    }

    struct IncludeMatcher {
        patterns: Vec<Regex>,
    }

    impl IncludeMatcher {
        fn new(include: &str) -> Result<Self> {
            let patterns = include
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(glob_to_regex)
                .collect::<Result<Vec<_>>>()?;
            Ok(Self { patterns })
        }

        fn matches(&self, id: &str) -> bool {
            self.patterns.is_empty() || self.patterns.iter().any(|pattern| pattern.is_match(id))
        }
    }

    fn glob_to_regex(pattern: &str) -> Result<Regex> {
        let mut regex = String::from("^");
        for ch in pattern.chars() {
            match ch {
                '*' => regex.push_str(".*"),
                '?' => regex.push('.'),
                _ => regex.push_str(&regex::escape(&ch.to_string())),
            }
        }
        regex.push('$');
        Ok(Regex::new(&regex)?)
    }

    fn metadata_for(id: &str, scale: ScenarioScale) -> Result<FixtureMetadata> {
        let metadata = match id {
            "s001-no-formulas-static-grid" => {
                let (rows, cols) =
                    formualizer_bench_core::scenarios::S001NoFormulasStaticGrid::dimensions(scale);
                FixtureMetadata {
                    rows,
                    cols,
                    sheets: 1,
                    formula_cells: 0,
                    value_cells: rows.saturating_mul(cols),
                    has_named_ranges: false,
                    has_tables: false,
                }
            }
            "s002-single-column-trivial-family" => {
                let rows =
                    formualizer_bench_core::scenarios::S002SingleColumnTrivialFamily::rows(scale);
                FixtureMetadata {
                    rows,
                    cols: 2,
                    sheets: 1,
                    formula_cells: rows,
                    value_cells: rows,
                    has_named_ranges: false,
                    has_tables: false,
                }
            }
            "s003-finance-anchored-arithmetic-family" => {
                let rows =
                    formualizer_bench_core::scenarios::S003FinanceAnchoredArithmeticFamily::rows(
                        scale,
                    );
                FixtureMetadata {
                    rows,
                    cols: 7,
                    sheets: 1,
                    formula_cells: rows + 1,
                    value_cells: rows.saturating_mul(2) + 1,
                    has_named_ranges: false,
                    has_tables: false,
                }
            }
            "s004-five-mixed-families" => {
                let rows = formualizer_bench_core::scenarios::S004FiveMixedFamilies::rows(scale);
                FixtureMetadata {
                    rows,
                    cols: 6,
                    sheets: 1,
                    formula_cells: rows.saturating_mul(5),
                    value_cells: rows,
                    has_named_ranges: false,
                    has_tables: false,
                }
            }
            "s005-long-chain-family" => {
                let rows = formualizer_bench_core::scenarios::S005LongChainFamily::rows(scale);
                FixtureMetadata {
                    rows,
                    cols: 1,
                    sheets: 1,
                    formula_cells: rows.saturating_sub(1),
                    value_cells: 1,
                    has_named_ranges: false,
                    has_tables: false,
                }
            }
            other => bail!("no fixture metadata helper registered for {other}"),
        };
        Ok(metadata)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn probe_corpus_default_disables_parallel() {
            let cli = Cli::try_parse_from(["probe-corpus", "--label", "default-parallel"])
                .expect("parse cli");
            assert!(!cli.enable_parallel.unwrap_or(false));
        }
    }
}

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    enabled::main()
}
