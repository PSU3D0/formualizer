#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!(
        "This binary requires feature `formualizer_runner`: cargo run -p formualizer-bench-core --features formualizer_runner --release --bin probe-corpus-parity"
    );
    std::process::exit(2);
}

#[cfg(feature = "formualizer_runner")]
mod enabled {
    use std::path::PathBuf;

    use anyhow::{Result, bail};
    use clap::Parser;
    use formualizer_bench_core::parity_harness::{
        ParityOptions, ParityScenarioReport, default_phase_timeout_ms, parse_scale,
        report_is_unexpected_failure, run_scenario_parity,
    };
    use formualizer_bench_core::scenarios::common::set_invariant_scale;
    use formualizer_bench_core::scenarios::{ScenarioRegistry, ScenarioScale};
    use regex::Regex;

    #[derive(Debug, Parser)]
    #[command(about = "Run full-cell Off↔Auth parity over the FormulaPlane scenario corpus")]
    pub struct Cli {
        #[arg(long, default_value = "small")]
        scale: String,
        #[arg(long, default_value = "*")]
        include: String,
        #[arg(long)]
        exclude: Option<String>,
        #[arg(long)]
        phase_timeout_ms: Option<u64>,
        #[arg(long)]
        enable_parallel: Option<bool>,
        #[arg(long)]
        fail_fast: bool,
        #[arg(long, default_value_t = 10)]
        max_divergences_per_phase: usize,
        #[arg(long, default_value = "parity")]
        label: String,
        #[arg(long)]
        fixture_dir: Option<PathBuf>,
        #[arg(long)]
        output_dir: Option<PathBuf>,
    }

    pub fn main() -> Result<()> {
        let cli = Cli::parse();
        let scale = parse_scale(&cli.scale)?;
        set_invariant_scale(scale);
        let output_dir = cli.output_dir.clone().unwrap_or_else(|| {
            PathBuf::from("target")
                .join("scenario-corpus")
                .join(&cli.label)
                .join("parity")
        });
        let fixture_dir = cli
            .fixture_dir
            .clone()
            .unwrap_or_else(|| output_dir.join("fixtures"));
        std::fs::create_dir_all(&output_dir)?;
        std::fs::create_dir_all(&fixture_dir)?;

        let include = GlobMatcher::new(&cli.include)?;
        let exclude = match cli.exclude.as_deref() {
            Some(pattern) => Some(GlobMatcher::new(pattern)?),
            None => None,
        };
        let scenarios = ScenarioRegistry::all()
            .into_iter()
            .filter(|scenario| include.matches(scenario.id()))
            .filter(|scenario| !exclude.as_ref().is_some_and(|m| m.matches(scenario.id())))
            .collect::<Vec<_>>();
        if scenarios.is_empty() {
            bail!("no scenarios matched --include '{}'", cli.include);
        }

        let options = ParityOptions {
            phase_timeout_ms: cli
                .phase_timeout_ms
                .unwrap_or(default_phase_timeout_ms(scale)),
            max_divergences_per_phase: cli.max_divergences_per_phase,
            enable_parallel: cli.enable_parallel.unwrap_or(false),
        };
        let mut reports = Vec::new();
        for scenario in scenarios {
            eprintln!(
                "[probe-corpus-parity] {} scale={}",
                scenario.id(),
                scale.as_str()
            );
            let report =
                run_scenario_parity(scenario.as_ref(), scale, &cli.label, &fixture_dir, options);
            let json = serde_json::to_string_pretty(&report)?;
            println!("{json}");
            std::fs::write(output_dir.join(format!("{}.json", scenario.id())), json)?;
            let unexpected = report_is_unexpected_failure(&report);
            reports.push(report);
            if cli.fail_fast && unexpected {
                break;
            }
        }

        let summary = render_summary(&reports, scale);
        std::fs::write(output_dir.join("summary.txt"), &summary)?;
        print!("{summary}");
        if reports.iter().any(report_is_unexpected_failure) {
            std::process::exit(1);
        }
        Ok(())
    }

    fn render_summary(reports: &[ParityScenarioReport], _scale: ScenarioScale) -> String {
        let run = reports.iter().filter(|report| !report.skipped).count();
        let skipped = reports.iter().filter(|report| report.skipped).count();
        let failed = reports
            .iter()
            .filter(|report| report_is_unexpected_failure(report))
            .count();
        let passed = reports
            .iter()
            .filter(|report| {
                !report.skipped
                    && !report_is_unexpected_failure(report)
                    && report.total_divergences == 0
                    && report.phases_failed == 0
            })
            .count();
        let expected = reports
            .iter()
            .filter(|report| {
                !report.skipped && report.expected_divergence.is_some() && report.phases_failed > 0
            })
            .count();
        let total_divergences: usize = reports.iter().map(|report| report.total_divergences).sum();
        let mut out = String::new();
        out.push_str("\n=== PARITY SUMMARY ===\n");
        out.push_str(&format!("Scenarios run:      {run}\n"));
        out.push_str(&format!("Scenarios passed:   {passed}\n"));
        out.push_str(&format!("Scenarios failed:   {failed}\n"));
        out.push_str(&format!(
            "Scenarios skipped:  {skipped} (expected divergence)\n"
        ));
        out.push_str(&format!("Scenarios expected: {expected} (run-and-noted)\n"));
        out.push_str(&format!("Total divergences:  {total_divergences}\n"));
        let failures = reports
            .iter()
            .filter(|report| report_is_unexpected_failure(report))
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            out.push_str("\nFailures:\n");
            for report in failures {
                for phase in &report.phases {
                    if phase.error.is_some() || !phase.divergences.is_empty() || phase.timed_out {
                        out.push_str(&format!(
                            "  {:40} {:35} {} divergences{}\n",
                            report.scenario_id,
                            phase.phase,
                            phase.divergences.len(),
                            phase
                                .error
                                .as_ref()
                                .map(|error| format!(" error={error}"))
                                .unwrap_or_default()
                        ));
                    }
                }
            }
        }
        out
    }

    struct GlobMatcher {
        patterns: Vec<Regex>,
    }

    impl GlobMatcher {
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

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn probe_corpus_parity_default_disables_parallel() {
            let cli = Cli::try_parse_from(["probe-corpus-parity"]).expect("parse cli");
            assert!(!cli.enable_parallel.unwrap_or(false));
        }
    }
}

#[cfg(feature = "formualizer_runner")]
fn main() -> anyhow::Result<()> {
    enabled::main()
}
