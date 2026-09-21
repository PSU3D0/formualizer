//! Structural smoke probe for the unified scenario registry (no timing output).
#[cfg(feature = "formualizer_runner")]
fn main() {
    use formualizer_bench_core::scenarios::unified_registry;
    use formualizer_eval::engine::FormulaPlaneMode;
    use formualizer_testkit::{
        WorkbookRoute,
        run::{Materializer, Recorder, run},
        scenario::Filter,
    };
    use formualizer_workbook::WorkbookConfig;
    use std::{env, str::FromStr};

    let mut filter = None;
    let mut selected_mode = "authoritative".to_owned();
    let mut rows = 256;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--filter" => {
                filter = Some(
                    Filter::from_str(&args.next().expect("--filter value"))
                        .expect("invalid filter"),
                )
            }
            "--mode" => selected_mode = args.next().expect("--mode value"),
            "--rows" => {
                rows = args
                    .next()
                    .expect("--rows value")
                    .parse()
                    .expect("numeric rows")
            }
            _ => panic!("unknown argument {arg}"),
        }
    }
    let modes: Vec<_> = match selected_mode.as_str() {
        "off" => vec![FormulaPlaneMode::Off],
        "authoritative" | "auth" => vec![FormulaPlaneMode::AuthoritativeExperimental],
        "all" => vec![
            FormulaPlaneMode::Off,
            FormulaPlaneMode::AuthoritativeExperimental,
        ],
        _ => panic!("--mode must be off, authoritative, or all"),
    };
    for spec in unified_registry(rows)
        .into_iter()
        .filter(|s| filter.as_ref().is_none_or(|f| f.matches(s)))
    {
        for mode in &modes {
            let mut config = WorkbookConfig::interactive().with_formula_plane_mode(*mode);
            config.eval.enable_parallel = false;
            let recorder = Recorder::default();
            let report = run(
                &spec,
                *mode,
                spec.sizes[0],
                Materializer::workbook_api(WorkbookRoute::SetValuesSetFormulas, config),
                Some(&recorder),
            );
            let span = report
                .steps
                .iter()
                .find(|step| matches!(step.step, formualizer_testkit::scenario::Step::EvaluateAll))
                .and_then(|step| step.stats.as_ref())
                .map(|s| s.formula_plane_active_span_count)
                .unwrap_or(0);
            let state = if let Some(message) = report.failure {
                format!("fail {message}")
            } else if let Some(message) = report.known_failure {
                format!("KNOWN {message}")
            } else {
                "pass".into()
            };
            println!(
                "{} {} {} active_spans={span}",
                spec.id,
                match mode {
                    FormulaPlaneMode::Off => "off",
                    _ => "authoritative",
                },
                state
            );
        }
    }
}
#[cfg(not(feature = "formualizer_runner"))]
fn main() {
    eprintln!("requires --features formualizer_runner");
    std::process::exit(2);
}
